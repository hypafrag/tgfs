use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fuser::{FileAttr, FileType as FuseFileType, Filesystem, Request, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyOpen};
use grammers_client::media::Document;
use tokio::runtime::Handle;

use crate::index::{AppState, ArchiveView, FileEntry, FileType};
use crate::indexer::download_range;
use flate2::read::DeflateDecoder;

fn path_hash(p: &str) -> u64 {
    if p == "/" {
        return fuser::FUSE_ROOT_ID;
    }
    use std::collections::hash_map::DefaultHasher;
    let mut s = DefaultHasher::new();
    p.hash(&mut s);
    let v = s.finish();
    // avoid collisions with root inode and zero
    if v == 0 || v == fuser::FUSE_ROOT_ID { v.wrapping_add(2) } else { v }
}

fn full_for(e: &FileEntry) -> String {
    match &e.path {
        Some(p) => {
            let s = p.to_string_lossy().replace('\\', "/");
            if s.is_empty() { e.name.clone() } else { format!("{}/{}", s, e.name) }
        }
        None => e.name.replace('\\', "/"),
    }
}

fn dir_attr(ino: u64, now: SystemTime) -> FileAttr {
    FileAttr {
        ino, size: 0, blocks: 0,
        atime: now, mtime: now, ctime: now, crtime: now,
        kind: FuseFileType::Directory,
        perm: 0o755, nlink: 2,
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        rdev: 0, flags: 0, blksize: 512,
    }
}

fn file_attr(ino: u64, size: u64, now: SystemTime) -> FileAttr {
    FileAttr {
        ino, size, blocks: (size + 511) / 512,
        atime: now, mtime: now, ctime: now, crtime: now,
        kind: FuseFileType::RegularFile,
        perm: 0o444, nlink: 1,
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        rdev: 0, flags: 0, blksize: 512,
    }
}

/// Add `name` as a child of `parent` if not already present.
fn add_child(children: &mut HashMap<String, Vec<String>>, parent: &str, name: &str) {
    let cv = children.entry(parent.to_string()).or_default();
    if !cv.iter().any(|n| n == name) {
        cv.push(name.to_string());
    }
}

/// Ensure a directory entry exists at `path` with `parent` as its parent.
fn ensure_dir(
    path_to_attr: &mut HashMap<String, FileAttr>,
    children: &mut HashMap<String, Vec<String>>,
    parent: &str,
    name: &str,
    path: &str,
    now: SystemTime,
) {
    add_child(children, parent, name);
    path_to_attr.entry(path.to_string()).or_insert_with(|| dir_attr(path_hash(path), now));
}

/// Ensure all intermediate directories along `base/rel` exist. Returns the leaf
/// path. If `rel` is empty, returns `base`.
fn ensure_dirs_along(
    path_to_attr: &mut HashMap<String, FileAttr>,
    children: &mut HashMap<String, Vec<String>>,
    base: &str,
    rel: &str,
    now: SystemTime,
) -> String {
    let mut parent = base.to_string();
    if rel.is_empty() { return parent; }
    for seg in rel.split('/') {
        let child = format!("{}/{}", parent, seg);
        ensure_dir(path_to_attr, children, &parent, seg, &child, now);
        parent = child;
    }
    parent
}

/// Add a regular file at `<parent>/<name>` of the given size.
/// `perm` overrides the default 0o444 when non-zero.
fn add_file(
    path_to_attr: &mut HashMap<String, FileAttr>,
    children: &mut HashMap<String, Vec<String>>,
    parent: &str,
    name: &str,
    size: u64,
    perm: u16,
    now: SystemTime,
) {
    add_child(children, parent, name);
    let path = format!("{}/{}", parent, name);
    let mut attr = file_attr(path_hash(&path), size, now);
    if perm != 0 { attr.perm = perm & !0o222; } // strip write bits — read-only fs
    path_to_attr.insert(path, attr);
}

pub struct TgfsFS {
    state: Arc<AppState>,
    // mappings built at init
    path_to_attr: HashMap<String, FileAttr>,
    ino_to_path: HashMap<u64, String>,
    children: HashMap<String, Vec<String>>,
    // Captured at construction so blocking FUSE callbacks can drive async ops.
    rt: Handle,
}

impl TgfsFS {
    pub fn new(state: Arc<AppState>) -> Self {
        let mut path_to_attr: HashMap<String, FileAttr> = HashMap::new();
        let mut children: HashMap<String, Vec<String>> = HashMap::new();

        let now = SystemTime::now();

        // root
        path_to_attr.insert("/".to_string(), dir_attr(path_hash("/"), now));

        // channels as top-level dirs
        for (dir, channel) in &state.dir_to_channel {
            let ch_path = format!("/{}", dir);
            ensure_dir(&mut path_to_attr, &mut children, "/", dir, &ch_path, now);

            let archive_view = state.channel_archive_view.get(channel).copied().unwrap_or(ArchiveView::File);

            let files = match state.index.get(channel) {
                Some(f) => f,
                None => continue,
            };

            for f in files.iter() {
                let full = full_for(f);
                let is_browsable_zip = f.file_type == FileType::Zip
                    && f.archive_entries.is_some()
                    && archive_view != ArchiveView::File;
                let show_as_file = !is_browsable_zip || archive_view == ArchiveView::FileAndDirectory;

                // Split `full` into parent dir path (relative) and final name.
                let (parent_rel, fname) = match full.rfind('/') {
                    Some(i) => (&full[..i], &full[i + 1..]),
                    None => ("", full.as_str()),
                };

                if show_as_file {
                    let parent_path = ensure_dirs_along(&mut path_to_attr, &mut children, &ch_path, parent_rel, now);
                    let size = f.size.unwrap_or(0) as u64;
                    add_file(&mut path_to_attr, &mut children, &parent_path, fname, size, 0, now);
                } else {
                    // Directory-only zip: create intermediate dirs only (skip file leaf).
                    ensure_dirs_along(&mut path_to_attr, &mut children, &ch_path, parent_rel, now);
                }

                // Expose archive entries as a virtual directory named after the stem.
                if is_browsable_zip {
                    let ae_list = f.archive_entries.as_ref().unwrap();
                    let stem = std::path::Path::new(&f.name).file_stem().and_then(|s| s.to_str()).unwrap_or(&f.name);
                    let stem_full = match &f.path {
                        Some(p) => {
                            let s = p.to_string_lossy().replace('\\', "/");
                            if s.is_empty() { stem.to_string() } else { format!("{}/{}", s, stem) }
                        }
                        None => stem.to_string(),
                    };
                    let arc_dir = ensure_dirs_along(&mut path_to_attr, &mut children, &ch_path, &stem_full, now);

                    for ae in ae_list.iter() {
                        let (ae_parent_rel, ae_name) = match ae.path.rfind('/') {
                            Some(i) => (&ae.path[..i], &ae.path[i + 1..]),
                            None => ("", ae.path.as_str()),
                        };
                        let ae_parent = ensure_dirs_along(&mut path_to_attr, &mut children, &arc_dir, ae_parent_rel, now);
                        add_file(&mut path_to_attr, &mut children, &ae_parent, ae_name, ae.uncompressed_size as u64, ae.unix_mode.unwrap_or(0), now);
                    }
                }
            }
        }

        let ino_to_path: HashMap<u64, String> = path_to_attr.iter().map(|(p, a)| (a.ino, p.clone())).collect();

        Self { state, path_to_attr, ino_to_path, children, rt: Handle::current() }
    }

    fn lookup_path(&self, path: &str) -> Option<&FileAttr> {
        // Normalize path: ensure no trailing slash except root
        let p = if path == "/" { "/".to_string() } else { path.trim_end_matches('/').to_string() };
        self.path_to_attr.get(&p)
    }

    fn path_for_ino(&self, ino: u64) -> Option<&str> {
        self.ino_to_path.get(&ino).map(|s| s.as_str())
    }

    /// Synchronously download a byte range using the captured tokio runtime handle.
    fn block_download(&self, parts: &[Document], offset: usize, length: usize) -> std::io::Result<Vec<u8>> {
        let client = self.state.client.clone();
        self.rt
            .block_on(async move { download_range(&client, parts, offset, length).await })
            .map_err(|_| std::io::Error::other("download failed"))
    }
}

impl Filesystem for TgfsFS {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &std::ffi::OsStr, reply: ReplyEntry) {
        let parent_path = match self.path_for_ino(parent) {
            Some(p) => p.to_string(),
            None => { reply.error(libc::ENOENT); return; }
        };
        let name_str = name.to_string_lossy();
        let target = if parent_path == "/" { format!("/{}", name_str) } else { format!("{}/{}", parent_path, name_str) };
        if let Some(attr) = self.lookup_path(&target) {
            reply.entry(&Duration::from_secs(1), attr, 0);
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self.path_for_ino(ino).and_then(|p| self.path_to_attr.get(p)) {
            Some(a) => reply.attr(&Duration::from_secs(1), a),
            None => reply.error(libc::ENOENT),
        }
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        let path = match self.path_for_ino(ino) {
            Some(p) => p.to_string(),
            None => { reply.error(libc::ENOENT); return; }
        };
        let mut entries: Vec<(u64, FuseFileType, String)> = Vec::new();
        // . and ..
        entries.push((path_hash(&path), FuseFileType::Directory, ".".to_string()));
        entries.push((path_hash("/"), FuseFileType::Directory, "..".to_string()));

        if let Some(children_vec) = self.children.get(&path) {
            for name in children_vec.iter() {
                let child_path = if path == "/" { format!("/{}", name) } else { format!("{}/{}", path, name) };
                if let Some(attr) = self.path_to_attr.get(&child_path) {
                    entries.push((attr.ino, if attr.kind == FuseFileType::Directory { FuseFileType::Directory } else { FuseFileType::RegularFile }, name.clone()));
                }
            }
        }

        for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(ino, (i + 1) as i64, kind, name) { break; }
        }
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let exists = self.path_for_ino(ino)
            .and_then(|p| self.path_to_attr.get(p))
            .map_or(false, |a| a.kind == FuseFileType::Directory);
        if exists { reply.opened(0, 0); } else { reply.error(libc::ENOENT); }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        // Only allow reading of regular files
        let exists = self.path_for_ino(ino)
            .and_then(|p| self.path_to_attr.get(p))
            .map_or(false, |a| a.kind == FuseFileType::RegularFile);
        if exists { reply.opened(0, 0); } else { reply.error(libc::EISDIR); }
    }

    fn read(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        let path = match self.path_for_ino(ino) {
            Some(p) => p.to_string(),
            None => { reply.error(libc::ENOENT); return; }
        };
        let ptrim = path.trim_start_matches('/');
        let mut parts = ptrim.splitn(2, '/');
        let channel = parts.next().unwrap_or("");
        let rest = parts.next().unwrap_or("");
        let files = match self.state.index.get(channel) {
            Some(f) => f,
            None => { reply.error(libc::ENOENT); return; }
        };

        // Try direct file match
        if let Some(fentry) = files.iter().find(|e| full_for(e) == rest) {
            let parts_docs: Vec<_> = fentry.parts.iter().cloned().collect();
            match self.block_download(&parts_docs, offset as usize, size as usize) {
                Ok(buf) => reply.data(&buf),
                Err(_) => reply.error(libc::EIO),
            }
            return;
        }

        // Try inner archive file
        for f in files.iter() {
            if f.file_type != FileType::Zip { continue; }
            if f.archive_entries.is_none() { continue; }
            let full = full_for(f);
            let stem = std::path::Path::new(&f.name).file_stem().and_then(|s| s.to_str()).unwrap_or(&f.name).to_string();
            let prefix_full = format!("{}/", full);
            let prefix_stem = format!("{}/", stem);
            let inner = if rest.starts_with(&prefix_full) {
                &rest[prefix_full.len()..]
            } else if rest.starts_with(&prefix_stem) {
                &rest[prefix_stem.len()..]
            } else {
                continue;
            };
            let ae = match f.archive_entries.as_ref().unwrap().iter().find(|x| x.path == inner) {
                Some(a) => a,
                None => { reply.error(libc::ENOENT); return; }
            };
            let parts_docs: Vec<_> = f.parts.iter().cloned().collect();
            // read local header to compute data_offset
            let lh = match self.block_download(&parts_docs, ae.local_header_offset as usize, 30) {
                Ok(b) => b,
                Err(_) => { reply.error(libc::EIO); return; }
            };
            if lh.len() < 30 || &lh[0..4] != [0x50, 0x4b, 0x03, 0x04] { reply.error(libc::EIO); return; }
            let name_len = u16::from_le_bytes([lh[26], lh[27]]) as usize;
            let extra_len = u16::from_le_bytes([lh[28], lh[29]]) as usize;
            let data_offset = ae.local_header_offset as usize + 30 + name_len + extra_len;

            match ae.compression_method {
                0 => {
                    let off = data_offset + offset as usize;
                    match self.block_download(&parts_docs, off, size as usize) {
                        Ok(buf) => reply.data(&buf),
                        Err(_) => reply.error(libc::EIO),
                    }
                    return;
                }
                8 => {
                    // deflate: download compressed payload then inflate synchronously
                    let comp_bytes = match self.block_download(&parts_docs, data_offset, ae.compressed_size) {
                        Ok(b) => b,
                        Err(_) => { reply.error(libc::EIO); return; }
                    };
                    let mut decoder = DeflateDecoder::new(&comp_bytes[..]);
                    let mut out = Vec::with_capacity(ae.uncompressed_size);
                    if std::io::copy(&mut decoder, &mut out).is_err() { reply.error(libc::EIO); return; }
                    let off = offset as usize;
                    let end = std::cmp::min(out.len(), off + size as usize);
                    if off >= out.len() { reply.data(&[]); } else { reply.data(&out[off..end]); }
                    return;
                }
                _ => { reply.error(libc::ENOTSUP); return; }
            }
        }

        reply.error(libc::ENOENT);
    }
}
