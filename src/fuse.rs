use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use fuser::{FileAttr, FileType as FuseFileType, Filesystem, Request, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen};
use grammers_client::Client;
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

// Static read-only FS: entries never change, so use a long TTL for kernel caching.
const ATTR_TTL: Duration = Duration::from_secs(86400);

// 128 KB preferred I/O size. Tells the kernel to batch reads, reducing
// the number of round-trips through the single-threaded FUSE event loop.
const BLKSIZE: u32 = 131_072;

fn dir_attr(ino: u64, now: SystemTime) -> FileAttr {
    FileAttr {
        ino, size: 0, blocks: 0,
        atime: now, mtime: now, ctime: now, crtime: now,
        kind: FuseFileType::Directory,
        perm: 0o755, nlink: 2,
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        rdev: 0, flags: 0, blksize: BLKSIZE,
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
        rdev: 0, flags: 0, blksize: BLKSIZE,
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

const DEFLATE_FETCH_INITIAL: usize = 64 * 1024;
const DEFLATE_FETCH_READAHEAD: usize = 512 * 1024;

/// Streams compressed bytes from Telegram on demand, one chunk at a time.
/// Wrapped by `DeflateDecoder` so only the decoder's 32 KB window is held
/// in memory — not the full compressed payload.
struct TelegramReader {
    client: Client,
    parts: Vec<Document>,
    /// Absolute byte offset in the Telegram file where compressed data begins.
    data_offset: usize,
    compressed_size: usize,
    /// Compressed bytes consumed so far (relative to data_offset).
    consumed: usize,
    buf: Vec<u8>,
    buf_pos: usize,
    next_fetch: usize,
    rt: Handle,
}

impl std::io::Read for TelegramReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.buf_pos >= self.buf.len() {
            let remaining = self.compressed_size.saturating_sub(self.consumed);
            if remaining == 0 {
                return Ok(0);
            }
            let fetch = remaining.min(self.next_fetch);
            self.next_fetch = DEFLATE_FETCH_READAHEAD;
            let abs = self.data_offset + self.consumed;
            // Use a block so borrows of self.rt/client/parts end before we write self.buf.
            let chunk = {
                let rt = &self.rt;
                let client = &self.client;
                let parts = &self.parts;
                rt.block_on(download_range(client, parts, abs, fetch))
                    .map_err(|_| std::io::Error::other("download failed"))?
            };
            if chunk.is_empty() {
                return Ok(0);
            }
            self.consumed += chunk.len();
            self.buf = chunk;
            self.buf_pos = 0;
        }
        let n = out.len().min(self.buf.len() - self.buf_pos);
        out[..n].copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + n]);
        self.buf_pos += n;
        Ok(n)
    }
}

/// Per-open-handle state for a deflate-compressed archive entry.
struct DeflateStream {
    decoder: DeflateDecoder<TelegramReader>,
    /// Byte position in the decompressed output the decoder is currently at.
    pos: usize,
}

pub struct TgfsFS {
    state: Arc<AppState>,
    // mappings built at init
    path_to_attr: HashMap<String, FileAttr>,
    ino_to_path: HashMap<u64, String>,
    children: HashMap<String, Vec<String>>,
    // Captured at construction so blocking FUSE callbacks can drive async ops.
    rt: Handle,
    // Streaming deflate decoders, one per open file handle, keyed by fh.
    // Kept alive between FUSE read chunks so we decompress sequentially
    // rather than re-downloading + re-decompressing from the start each call.
    // Wrapped in Arc<Mutex<>> so per-fh state can be moved into a
    // `spawn_blocking` task and locked there — different fh's (different
    // clients) decode in parallel; concurrent reads on the same fh serialize
    // on the mutex, which is required since the deflate decoder is stateful
    // and must consume bytes sequentially. Removed in release().
    deflate_streams: HashMap<u64, Arc<Mutex<DeflateStream>>>,
    next_fh: u64,
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

            let archive_view = state.channels.get(channel).map(|a| a.archive_view).unwrap_or(ArchiveView::File);

            let files = match state.channels.get(channel).map(|c| &c.files) {
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

        Self { state, path_to_attr, ino_to_path, children, rt: Handle::current(), deflate_streams: HashMap::new(), next_fh: 1 }
    }

    fn lookup_path(&self, path: &str) -> Option<&FileAttr> {
        // Normalize path: ensure no trailing slash except root
        let p = if path == "/" { "/".to_string() } else { path.trim_end_matches('/').to_string() };
        self.path_to_attr.get(&p)
    }

    fn path_for_ino(&self, ino: u64) -> Option<&str> {
        self.ino_to_path.get(&ino).map(|s| s.as_str())
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
            reply.entry(&ATTR_TTL, attr, 0);
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self.path_for_ino(ino).and_then(|p| self.path_to_attr.get(p)) {
            Some(a) => reply.attr(&ATTR_TTL, a),
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
        let exists = self.path_for_ino(ino)
            .and_then(|p| self.path_to_attr.get(p))
            .map_or(false, |a| a.kind == FuseFileType::RegularFile);
        if exists {
            let fh = self.next_fh;
            self.next_fh += 1;
            let path = self.path_for_ino(ino).map(|s| s.to_string()).unwrap_or_else(|| "<unknown>".to_string());
            println!("fuse: open ino={} path='{}' fh={}", ino, path, fh);
            reply.opened(fh, fuser::consts::FOPEN_KEEP_CACHE);
        } else {
            reply.error(libc::EISDIR);
        }
    }

    fn release(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) {
        let had = self.deflate_streams.remove(&fh).is_some();
        // Try to resolve path for logging; not strictly required.
        let path = self.path_for_ino(_ino).map(|s| s.to_string()).unwrap_or_else(|| "<unknown>".to_string());
        println!("fuse: release fh={} ino={} path='{}' had_deflate_stream={}", fh, _ino, path, had);
        reply.ok();
    }

    fn read(&mut self, _req: &Request<'_>, ino: u64, fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        // Clone Arc so borrows for `files`/`ae` come from a local — leaving
        // `self` free to mutate `deflate_streams` below without borrow conflict.
        let state = self.state.clone();
        let path = match self.ino_to_path.get(&ino) {
            Some(p) => p.clone(),
            None => { reply.error(libc::ENOENT); return; }
        };
        let ptrim = path.trim_start_matches('/');
        let mut p_iter = ptrim.splitn(2, '/');
        let dir = p_iter.next().unwrap_or("");
        let rest = p_iter.next().unwrap_or("");
        let channel = match state.dir_to_channel.get(dir) {
            Some(c) => c.clone(),
            None => { reply.error(libc::ENOENT); return; }
        };
        let files = match state.channels.get(&channel).map(|c| &c.files) {
            Some(f) => f,
            None => { reply.error(libc::ENOENT); return; }
        };

        // Try direct file match. Dispatch the download as an async task and
        // return from `read()` immediately so the FUSE event loop is free to
        // service other requests while the download is in flight.
        if let Some(fentry) = files.iter().find(|e| full_for(e) == rest) {
            let parts_docs: Vec<Document> = fentry.parts.iter().cloned().collect();
            let client = state.client.clone();
            self.rt.spawn(async move {
                match download_range(&client, &parts_docs, offset as usize, size as usize).await {
                    Ok(buf) => reply.data(&buf),
                    Err(_) => reply.error(libc::EIO),
                }
            });
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
            let parts_docs: Vec<Document> = f.parts.iter().cloned().collect();
            let data_offset = ae.data_offset as usize;

            match ae.compression_method {
                0 => {
                    let off = data_offset + offset as usize;
                    let client = state.client.clone();
                    self.rt.spawn(async move {
                        match download_range(&client, &parts_docs, off, size as usize).await {
                            Ok(buf) => reply.data(&buf),
                            Err(_) => reply.error(libc::EIO),
                        }
                    });
                    return;
                }
                8 => {
                    let compressed_size = ae.compressed_size;
                    let uncompressed_size = ae.uncompressed_size;
                    let ae_path = ae.path.clone();
                    let off = offset as usize;
                    let sz = size as usize;
                    let channel_skip = state.channels.get(&channel).map(|a| a.skip_deflated_id3v1).unwrap_or(false);

                    // Get-or-create the per-fh streaming decoder. Wrapped in
                    // Arc<Mutex<>> so the actual decode work runs on a blocking
                    // thread and the FUSE event loop returns immediately.
                    let client = state.client.clone();
                    let rt_clone = self.rt.clone();
                    let stream = self.deflate_streams.entry(fh).or_insert_with(|| {
                        let reader = TelegramReader {
                            client,
                            parts: parts_docs,
                            data_offset,
                            compressed_size,
                            consumed: 0,
                            buf: Vec::new(),
                            buf_pos: 0,
                            next_fetch: DEFLATE_FETCH_INITIAL,
                            rt: rt_clone,
                        };
                        Arc::new(Mutex::new(DeflateStream {
                            decoder: DeflateDecoder::new(reader),
                            pos: 0,
                        }))
                    }).clone();

                    self.rt.spawn_blocking(move || {
                        let mut stream = stream.lock().unwrap();

                        // Workaround for players probing for ID3v1 at EOF on deflated inner files.
                        // When enabled per-channel, return zero bytes for small probe reads
                        // near the end to avoid players attempting to parse ID3v1 tags which
                        // would require completing the entire inflated stream.
                        const MAX_TOTAL_READ: usize = 128 * 1024;
                        const DISTANCE_TO_FILE_END: usize = 16 * 1024;
                        const ID3V1_READ_SIZE: usize = 4096;
                        if channel_skip {
                            let path_lower = ae_path.to_lowercase();
                            let looks_like_audio = path_lower.ends_with(".mp3") || path_lower.ends_with(".flac");
                            if looks_like_audio && sz == ID3V1_READ_SIZE && stream.pos < MAX_TOTAL_READ && uncompressed_size.saturating_sub(off) < DISTANCE_TO_FILE_END {
                                println!("fuse: suppressed ID3v1 probe for channel='{}' inner='{}' fh={} off={} stream_pos={}", channel, ae_path, fh, off, stream.pos);
                                let zeros = vec![0u8; sz];
                                reply.data(&zeros);
                                return;
                            }
                        }

                        // Skip forward if the kernel jumped ahead (shouldn't happen in
                        // normal sequential reads; caller should use STORE for random access).
                        if off > stream.pos {
                            use std::io::Read as _;
                            let mut remaining = off - stream.pos;
                            let mut discard = vec![0u8; remaining.min(65536)];
                            while remaining > 0 {
                                let n = remaining.min(discard.len());
                                match stream.decoder.read(&mut discard[..n]) {
                                    Ok(0) => { reply.error(libc::EIO); return; }
                                    Ok(r) => { stream.pos += r; remaining -= r; }
                                    Err(_) => { reply.error(libc::EIO); return; }
                                }
                            }
                        } else if off < stream.pos {
                            // Backward seek: impossible to handle without rewinding.
                            reply.error(libc::EIO);
                            return;
                        }

                        use std::io::Read as _;
                        let mut buf = vec![0u8; sz];
                        let mut total = 0;
                        while total < sz {
                            match stream.decoder.read(&mut buf[total..]) {
                                Ok(0) => break,
                                Ok(n) => total += n,
                                Err(_) => { reply.error(libc::EIO); return; }
                            }
                        }
                        stream.pos += total;
                        reply.data(&buf[..total]);
                    });
                    return;
                }
                _ => { reply.error(libc::ENOTSUP); return; }
            }
        }

        reply.error(libc::ENOENT);
    }
}
