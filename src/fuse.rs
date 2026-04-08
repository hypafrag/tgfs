use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fuser::{FileAttr, FileType as FuseFileType, Filesystem, Request, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyOpen};

use crate::index::{AppState, FileEntry, FileType};
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

pub struct TgfsFS {
    state: Arc<AppState>,
    // mappings built at init
    path_to_attr: HashMap<String, FileAttr>,
    children: HashMap<String, Vec<String>>,
}

impl TgfsFS {
    pub fn new(state: Arc<AppState>) -> Self {
        let mut path_to_attr: HashMap<String, FileAttr> = HashMap::new();
        let mut children: HashMap<String, Vec<String>> = HashMap::new();

        let now = SystemTime::now();

        // root
        let root_attr = FileAttr {
            ino: path_hash("/"),
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FuseFileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
            rdev: 0,
            flags: 0,
            blksize: 512,
        };
        path_to_attr.insert("/".to_string(), root_attr);

        // channels as top-level dirs
        for channel in state.index.keys() {
            let ch_path = format!("/{}", channel);
            let ch_attr = FileAttr {
                ino: path_hash(&ch_path),
                size: 0,
                blocks: 0,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: FuseFileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: unsafe { libc::geteuid() },
                gid: unsafe { libc::getegid() },
                rdev: 0,
                flags: 0,
                blksize: 512,
            };
            path_to_attr.insert(ch_path.clone(), ch_attr);
            children.entry("/".to_string()).or_default().push(channel.clone());

            if let Some(files) = state.index.get(channel) {
                // add files and directories
                for f in files.iter() {
                    let full = full_for(f);
                    let mut comps: Vec<String> = Vec::new();
                    let ch_prefix = format!("/{}", channel);
                    comps.push(ch_prefix.clone());
                    for seg in full.split('/') {
                        let parent = comps.last().unwrap().clone();
                        let child_path = if parent == "/" { format!("/{}", seg) } else { format!("{}/{}", parent, seg) };
                        // add directory entry for intermediate components (if not leaf)
                        // we'll assume leaf is a file
                        comps.push(child_path.clone());
                    }
                    // iterate comps to create directories and final file
                    for i in 1..comps.len() {
                        let p = comps[i - 1].clone();
                        let c = comps[i].clone();
                        // register child name under parent
                        let name = c.trim_start_matches(&format!("{}/", p)).trim_start_matches('/').to_string();
                        if !children.get(&p).map_or(false, |v| v.contains(&name)) {
                            children.entry(p.clone()).or_default().push(name.clone());
                        }
                        // if last component -> it's a file
                        if i == comps.len() - 1 {
                            let size = f.size.unwrap_or(0) as u64;
                            let fa = FileAttr {
                                ino: path_hash(&c),
                                size,
                                blocks: (size + 511) / 512,
                                atime: now,
                                mtime: now,
                                ctime: now,
                                crtime: now,
                                kind: FuseFileType::RegularFile,
                                perm: 0o444,
                                nlink: 1,
                                uid: unsafe { libc::geteuid() },
                                gid: unsafe { libc::getegid() },
                                rdev: 0,
                                flags: 0,
                                blksize: 512,
                            };
                            path_to_attr.insert(c.clone(), fa);
                        } else {
                            // intermediate directory
                            if !path_to_attr.contains_key(&comps[i]) {
                                let da = FileAttr {
                                    ino: path_hash(&comps[i]),
                                    size: 0,
                                    blocks: 0,
                                    atime: now,
                                    mtime: now,
                                    ctime: now,
                                    crtime: now,
                                    kind: FuseFileType::Directory,
                                    perm: 0o755,
                                    nlink: 2,
                                    uid: unsafe { libc::geteuid() },
                                    gid: unsafe { libc::getegid() },
                                    rdev: 0,
                                    flags: 0,
                                    blksize: 512,
                                };
                                path_to_attr.insert(comps[i].clone(), da);
                            }
                        }
                    }

                    // if archive entries exist, expose inner files under a directory named after stem
                    if f.file_type == FileType::Zip {
                        if let Some(ae_list) = &f.archive_entries {
                            let stem = std::path::Path::new(&f.name).file_stem().and_then(|s| s.to_str()).unwrap_or(&f.name);
                            let arc_dir = if full.ends_with('/') { format!("/{}/{}", channel, stem) } else { format!("/{}/{}", channel, stem) };
                            // ensure arc_dir exists
                            children.entry(format!("/{}", channel)).or_default().push(stem.to_string());
                            for ae in ae_list.iter() {
                                // create full inner path
                                let inner_path = format!("{}/{}", arc_dir, ae.path);
                                // register parent directories for inner entries
                                let mut parent = arc_dir.clone();
                                for seg in ae.path.split('/') {
                                    let child = format!("{}/{}", parent, seg);
                                    let name = seg.to_string();
                                    if !children.get(&parent).map_or(false, |v| v.contains(&name)) {
                                        children.entry(parent.clone()).or_default().push(name.clone());
                                    }
                                    parent = child;
                                }
                                // final file attr
                                let size = ae.uncompressed_size as u64;
                                let fa = FileAttr {
                                    ino: path_hash(&inner_path),
                                    size,
                                    blocks: (size + 511) / 512,
                                    atime: now,
                                    mtime: now,
                                    ctime: now,
                                    crtime: now,
                                    kind: FuseFileType::RegularFile,
                                    perm: 0o444,
                                    nlink: 1,
                                    uid: unsafe { libc::geteuid() },
                                    gid: unsafe { libc::getegid() },
                                    rdev: 0,
                                    flags: 0,
                                    blksize: 512,
                                };
                                path_to_attr.insert(inner_path, fa);
                            }
                        }
                    }
                }
            }
        }

        Self { state, path_to_attr, children }
    }

    fn lookup_path(&self, path: &str) -> Option<&FileAttr> {
        // Normalize path: ensure no trailing slash except root
        let p = if path == "/" { "/".to_string() } else { path.trim_end_matches('/').to_string() };
        self.path_to_attr.get(&p)
    }
}

impl Filesystem for TgfsFS {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &std::ffi::OsStr, reply: ReplyEntry) {
        let parent_path = if parent == path_hash("/") { "/".to_string() } else { 
            // find path by inode
            let mut found = None;
            for (p, a) in self.path_to_attr.iter() {
                if a.ino == parent { found = Some(p.clone()); break; }
            }
            if let Some(fp) = found { fp } else { reply.error(libc::ENOENT); return; }
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
        let mut found: Option<FileAttr> = None;
        for a in self.path_to_attr.values() {
            if a.ino == ino { found = Some(a.clone()); break; }
        }
        if let Some(a) = found {
            reply.attr(&Duration::from_secs(1), &a);
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        // find path for ino
        let path = self.path_to_attr.iter().find(|(_, a)| a.ino == ino).map(|(p, _)| p.clone());
        let path = if let Some(p) = path { p } else { reply.error(libc::ENOENT); return; };
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
        let exists = self.path_to_attr.values().any(|a| a.ino == ino && a.kind == FuseFileType::Directory);
        if exists { reply.opened(0, 0); } else { reply.error(libc::ENOENT); }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        // Only allow reading of regular files
        let exists = self.path_to_attr.values().any(|a| a.ino == ino && a.kind == FuseFileType::RegularFile);
        if exists { reply.opened(0, 0); } else { reply.error(libc::EISDIR); }
    }

    fn read(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        // find path
        let path = self.path_to_attr.iter().find(|(_, a)| a.ino == ino).map(|(p, _)| p.clone());
        let path = if let Some(p) = path { p } else { reply.error(libc::ENOENT); return; };
        // find corresponding FileEntry (if any)
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
            let off = offset as usize;
            let want = size as usize;
            // use tokio runtime to call async download_range
            // build parts slice
            let client = self.state.client.clone();
            let parts_docs: Vec<_> = fentry.parts.iter().cloned().collect();
            let res = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                download_range(&client, &parts_docs, off, want).await
            });
            match res {
                Ok(buf) => { reply.data(&buf); }
                Err(_) => { reply.error(libc::EIO); }
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
            if rest.starts_with(&prefix_full) || rest.starts_with(&prefix_stem) {
                let inner = if rest.starts_with(&prefix_full) { &rest[prefix_full.len()..] } else { &rest[prefix_stem.len()..] };
                let ae_opt = f.archive_entries.as_ref().unwrap().iter().find(|x| x.path == inner);
                if ae_opt.is_none() { reply.error(libc::ENOENT); return; }
                let ae = ae_opt.unwrap();
                // read local header to compute data_offset
                let client = self.state.client.clone();
                let parts_docs: Vec<_> = f.parts.iter().cloned().collect();
                let header = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                    download_range(&client, &parts_docs, ae.local_header_offset as usize, 30).await
                });
                if header.is_err() { reply.error(libc::EIO); return; }
                let lh = header.unwrap();
                if lh.len() < 30 || &lh[0..4] != [0x50, 0x4b, 0x03, 0x04] { reply.error(libc::EIO); return; }
                let name_len = u16::from_le_bytes([lh[26], lh[27]]) as usize;
                let extra_len = u16::from_le_bytes([lh[28], lh[29]]) as usize;
                let data_offset = ae.local_header_offset as usize + 30 + name_len + extra_len;

                match ae.compression_method {
                    0 => {
                        // stored
                        let off = data_offset + offset as usize;
                        let want = size as usize;
                        let res = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                            download_range(&client, &parts_docs, off, want).await
                        });
                        match res { Ok(buf) => reply.data(&buf), Err(_) => reply.error(libc::EIO) }
                        return;
                    }
                    8 => {
                        // deflate: download compressed payload then inflate synchronously
                        let comp = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                            download_range(&client, &parts_docs, data_offset, ae.compressed_size).await
                        });
                        if comp.is_err() { reply.error(libc::EIO); return; }
                        let comp_bytes = comp.unwrap();
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
        }

        reply.error(libc::ENOENT);
    }
}
