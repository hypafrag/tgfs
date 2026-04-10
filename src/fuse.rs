use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use fuser::{FileAttr, FileType as FuseFileType, Filesystem, FopenFlags, FileHandle, INodeNo, Errno, OpenFlags, Request, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen};
use grammers_client::media::Media;
use grammers_session::types::PeerRef;
use log::{debug, error, trace, warn};
use tokio::runtime::Handle;

use crate::index::{AppState, ArchiveView, FileEntry, FileType};
use crate::indexer::download_range_refresh;
use flate2::read::DeflateDecoder;

fn path_hash(p: &str) -> u64 {
    if p == "/" {
        return INodeNo::ROOT.0;
    }
    use std::collections::hash_map::DefaultHasher;
    let mut s = DefaultHasher::new();
    p.hash(&mut s);
    let v = s.finish();
    // avoid collisions with root inode and zero
    if v == 0 || v == INodeNo::ROOT.0 { v.wrapping_add(2) } else { v }
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

// 1 MB preferred I/O size. Tells the kernel to batch reads, reducing
// the number of round-trips through the FUSE event loop. Must be paired
// with a matching `max_read=` mount option (see main.rs) — otherwise the
// kernel caps individual reads at its default (128 KB on Linux).
pub const BLKSIZE: u32 = 1024 * 1024;

fn dir_attr(ino: u64, now: SystemTime) -> FileAttr {
    FileAttr {
        ino: INodeNo(ino), size: 0, blocks: 0,
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
        ino: INodeNo(ino), size, blocks: (size + 511) / 512,
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

/// Compressed-bytes prefetch chunk size. Big enough to amortize Telegram
/// per-request overhead; small enough to keep the channel responsive.
const DEFLATE_FETCH_CHUNK: usize = 2 * 1024 * 1024;
/// How many fetched chunks the prefetcher may keep buffered ahead of the
/// decoder. The bounded channel applies natural backpressure: when full, the
/// background task awaits on `send().await` until the decoder catches up.
/// Worst-case buffered compressed data ≈ DEFLATE_PREFETCH_DEPTH * DEFLATE_FETCH_CHUNK.
const DEFLATE_PREFETCH_DEPTH: usize = 4;

/// `std::io::Read` adapter over a tokio mpsc receiver fed by a background
/// fetcher task. The decoder calls `read()` from a `spawn_blocking` thread,
/// which blocks on `blocking_recv()` while the fetcher pipelines the next
/// compressed chunks in parallel with decode work.
struct PrefetchingReader {
    rx: tokio::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>,
    buf: Vec<u8>,
    buf_pos: usize,
    eof: bool,
}

impl std::io::Read for PrefetchingReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        while self.buf_pos >= self.buf.len() {
            if self.eof { return Ok(0); }
            match self.rx.blocking_recv() {
                Some(Ok(chunk)) if chunk.is_empty() => {
                    trace!("PrefetchingReader EOF (empty sentinel)");
                    self.eof = true;
                    return Ok(0);
                }
                Some(Ok(chunk)) => {
                    trace!("PrefetchingReader got chunk {} bytes", chunk.len());
                    self.buf = chunk;
                    self.buf_pos = 0;
                }
                Some(Err(e)) => {
                    error!("PrefetchingReader error: {:?}", e);
                    self.eof = true;
                    return Err(e);
                }
                None => {
                    trace!("PrefetchingReader channel closed");
                    self.eof = true;
                    return Ok(0);
                }
            }
        }
        let n = out.len().min(self.buf.len() - self.buf_pos);
        out[..n].copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + n]);
        self.buf_pos += n;
        Ok(n)
    }
}

/// Spawn a background task that pipelines compressed-byte fetches from
/// Telegram into a bounded channel. The receiver is wrapped in
/// `PrefetchingReader` for the deflate decoder. When the receiver is dropped
/// (e.g. on `release()`), `tx.send().await` errors and the task exits.
fn spawn_prefetcher(
    rt: &Handle,
    state: Arc<AppState>,
    parts: Vec<Media>,
    msg_ids: Vec<i32>,
    peer: Option<PeerRef>,
    data_offset: usize,
    compressed_size: usize,
) -> PrefetchingReader {
    let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<Vec<u8>>>(DEFLATE_PREFETCH_DEPTH);
    debug!("spawn_prefetcher data_offset={} compressed_size={}", data_offset, compressed_size);
    rt.spawn(async move {
        let client = state.client.clone();
        let mut consumed: usize = 0;
        while consumed < compressed_size {
            let want = (compressed_size - consumed).min(DEFLATE_FETCH_CHUNK);
            let abs = data_offset + consumed;
            trace!("prefetcher fetch abs={} want={} consumed={}/{}", abs, want, consumed, compressed_size);
            let res = download_range_refresh(&client, &parts, &msg_ids, peer, &state.fresh_docs, abs, want).await;
            match res {
                Ok(chunk) => {
                    if chunk.is_empty() {
                        warn!("prefetcher got empty chunk at consumed={}", consumed);
                        break;
                    }
                    trace!("prefetcher got {} bytes", chunk.len());
                    consumed += chunk.len();
                    if tx.send(Ok(chunk)).await.is_err() {
                        debug!("prefetcher receiver dropped, exiting");
                        return;
                    }
                }
                Err(e) => {
                    error!("prefetcher download error: {:?}", e);
                    let _ = tx.send(Err(std::io::Error::other("download failed"))).await;
                    return;
                }
            }
        }
        debug!("prefetcher done, consumed={}/{}", consumed, compressed_size);
        // Send a final empty chunk to signal clean EOF (channel close also works).
        let _ = tx.send(Ok(Vec::new())).await;
    });
    PrefetchingReader { rx, buf: Vec::new(), buf_pos: 0, eof: false }
}

/// Per-open-handle state for a deflate-compressed archive entry.
struct DeflateStream {
    decoder: DeflateDecoder<PrefetchingReader>,
    /// Byte position in the decompressed output the decoder is currently at.
    pos: usize,
}

/// Try to acquire a permit immediately; if none is available, log that the
/// caller is being throttled by the per-PID fetch limit and await one.
async fn acquire_owned_with_throttle_log(
    sem: Arc<tokio::sync::Semaphore>,
    pid: u32,
    fh: u64,
    site: &str,
) -> tokio::sync::OwnedSemaphorePermit {
    match sem.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            debug!(
                "throttled by max_fetches_per_pid pid={} fh={} site={} available={} waiting...",
                pid, fh, site, sem.available_permits()
            );
            sem.acquire_owned().await.expect("semaphore closed")
        }
    }
}

struct TgfsFSMutable {
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
    /// Per-PID semaphores limiting concurrent Telegram fetches.
    /// Created lazily on first read from a given PID.
    pid_semaphores: HashMap<u32, Arc<tokio::sync::Semaphore>>,
}

pub struct TgfsFS {
    state: Arc<AppState>,
    // mappings built at init
    path_to_attr: HashMap<String, FileAttr>,
    ino_to_path: HashMap<u64, String>,
    children: HashMap<String, Vec<String>>,
    deflated_paths: std::collections::HashSet<String>,
    // Captured at construction so blocking FUSE callbacks can drive async ops.
    rt: Handle,
    /// Mutable state behind a lock (fuser 0.17+ trait requires &self).
    inner: Mutex<TgfsFSMutable>,
    /// Maximum concurrent fetches per PID (None = unlimited).
    max_fetches_per_pid: Option<usize>,
}

impl TgfsFS {
    pub fn new(state: Arc<AppState>) -> Self {
        let mut path_to_attr: HashMap<String, FileAttr> = HashMap::new();
        let mut children: HashMap<String, Vec<String>> = HashMap::new();
        let mut deflated_paths: std::collections::HashSet<String> = std::collections::HashSet::new();

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
                    let file_mtime = f.mtime.unwrap_or(now);
                    add_file(&mut path_to_attr, &mut children, &parent_path, fname, size, 0, file_mtime);
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
                        let file_mtime = f.mtime.unwrap_or(now);
                        add_file(&mut path_to_attr, &mut children, &ae_parent, ae_name, ae.uncompressed_size as u64, ae.unix_mode.unwrap_or(0), file_mtime);
                        if ae.compression_method == 8 {
                            deflated_paths.insert(format!("{}/{}", ae_parent, ae_name));
                        }
                    }
                }
            }
        }

        let ino_to_path: HashMap<u64, String> = path_to_attr.iter().map(|(p, a)| (a.ino.0, p.clone())).collect();

        let max_fetches_per_pid = state.max_fetches_per_pid;
        let inner = Mutex::new(TgfsFSMutable {
            deflate_streams: HashMap::new(),
            next_fh: 1,
            pid_semaphores: HashMap::new(),
        });
        Self { state, path_to_attr, ino_to_path, children, deflated_paths, rt: Handle::current(), inner, max_fetches_per_pid }
    }

    /// Get or create the per-PID semaphore. Returns `None` when limiting is disabled.
    fn pid_semaphore(&self, pid: u32) -> Option<Arc<tokio::sync::Semaphore>> {
        let limit = self.max_fetches_per_pid?;
        let mut inner = self.inner.lock().unwrap();
        Some(inner.pid_semaphores.entry(pid).or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(limit))).clone())
    }

    fn lookup_path(&self, path: &str) -> Option<&FileAttr> {
        // Normalize path: ensure no trailing slash except root
        let p = if path == "/" { "/".to_string() } else { path.trim_end_matches('/').to_string() };
        self.path_to_attr.get(&p)
    }

    fn path_for_ino(&self, ino: u64) -> Option<&str> {
        self.ino_to_path.get(&ino).map(|s| s.as_str())
    }

    /// Returns true if `ino` resolves to a deflate-compressed (method 8) archive entry.
    fn is_deflated_entry(&self, ino: u64) -> bool {
        self.ino_to_path.get(&ino).map_or(false, |p| self.deflated_paths.contains(p))
    }

}

impl Filesystem for TgfsFS {
    fn access(&self, _req: &Request, _ino: INodeNo, _mask: fuser::AccessFlags, reply: ReplyEmpty) {
        reply.ok();
    }

    fn getxattr(&self, _req: &Request, _ino: INodeNo, _name: &std::ffi::OsStr, size: u32, reply: fuser::ReplyXattr) {
        if size == 0 {
            reply.size(0);
        } else {
            reply.error(Errno::NO_XATTR);
        }
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &std::ffi::OsStr, reply: ReplyEntry) {
        let parent_path = match self.path_for_ino(parent.0) {
            Some(p) => p.to_string(),
            None => { reply.error(Errno::ENOENT); return; }
        };
        let name_str = name.to_string_lossy();
        let target = if parent_path == "/" { format!("/{}", name_str) } else { format!("{}/{}", parent_path, name_str) };
        if let Some(attr) = self.lookup_path(&target) {
            reply.entry(&ATTR_TTL, attr, fuser::Generation(0));
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.path_for_ino(ino.0).and_then(|p| self.path_to_attr.get(p)) {
            Some(a) => reply.attr(&ATTR_TTL, a),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn readdir(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        let path = match self.path_for_ino(ino.0) {
            Some(p) => p.to_string(),
            None => { reply.error(Errno::ENOENT); return; }
        };
        let mut entries: Vec<(u64, FuseFileType, String)> = Vec::new();
        // . and ..
        entries.push((path_hash(&path), FuseFileType::Directory, ".".to_string()));
        entries.push((path_hash("/"), FuseFileType::Directory, "..".to_string()));

        if let Some(children_vec) = self.children.get(&path) {
            for name in children_vec.iter() {
                let child_path = if path == "/" { format!("/{}", name) } else { format!("{}/{}", path, name) };
                if let Some(attr) = self.path_to_attr.get(&child_path) {
                    entries.push((attr.ino.0, if attr.kind == FuseFileType::Directory { FuseFileType::Directory } else { FuseFileType::RegularFile }, name.clone()));
                }
            }
        }

        for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(ino), (i + 1) as u64, kind, name) { break; }
        }
        reply.ok();
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let exists = self.path_for_ino(ino.0)
            .and_then(|p| self.path_to_attr.get(p))
            .map_or(false, |a| a.kind == FuseFileType::Directory);
        if exists { reply.opened(FileHandle(0), FopenFlags::empty()); } else { reply.error(Errno::ENOENT); }
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let exists = self.path_for_ino(ino.0)
            .and_then(|p| self.path_to_attr.get(p))
            .map_or(false, |a| a.kind == FuseFileType::RegularFile);
        if exists {
            let fh = {
                let mut inner = self.inner.lock().unwrap();
                let fh = inner.next_fh;
                inner.next_fh += 1;
                fh
            };
            let path = self.path_for_ino(ino.0).map(|s| s.to_string()).unwrap_or_else(|| "<unknown>".to_string());
            let deflated = self.is_deflated_entry(ino.0);
            debug!("open ino={} path='{}' fh={} deflated={}", ino, path, fh, deflated);
            // FOPEN_STREAM: all files are backed by Telegram network requests with
            // no cheap seeking — hint to the kernel to use sequential read-ahead
            // and avoid optimisations that assume O(1) random access.
            //
            // FOPEN_NONSEEKABLE: deflate-compressed archive entries must be
            // decompressed from the start; the per-fh DeflateStream state cannot
            // rewind. Tell the kernel not to issue backward seeks so it never
            // drives read() to an offset behind the decoder's current position,
            // which would return EIO.
            let mut flags = FopenFlags::FOPEN_KEEP_CACHE | FopenFlags::FOPEN_NOFLUSH | FopenFlags::FOPEN_STREAM;
            if deflated { flags |= FopenFlags::FOPEN_NONSEEKABLE; }
            reply.opened(FileHandle(fh), flags);
        } else {
            reply.error(Errno::EISDIR);
        }
    }

    fn release(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, _flags: OpenFlags, _lock_owner: Option<fuser::LockOwner>, _flush: bool, reply: ReplyEmpty) {
        let had = self.inner.lock().unwrap().deflate_streams.remove(&fh.0).is_some();
        // Try to resolve path for logging; not strictly required.
        let path = self.path_for_ino(_ino.0).map(|s| s.to_string()).unwrap_or_else(|| "<unknown>".to_string());
        debug!("release fh={} ino={} path='{}' had_deflate_stream={}", fh, _ino, path, had);
        reply.ok();
    }

    fn read(&self, _req: &Request, ino: INodeNo, fh: FileHandle, offset: u64, size: u32, _flags: OpenFlags, _lock_owner: Option<fuser::LockOwner>, reply: ReplyData) {
        debug!("read enter ino={} fh={} offset={} size={} pid={}", ino, fh, offset, size, _req.pid());
        let pid_sem = self.pid_semaphore(_req.pid());
        // Clone Arc so borrows for `files`/`ae` come from a local — leaving
        // `self` free to mutate `deflate_streams` below without borrow conflict.
        let state = self.state.clone();
        let path = match self.ino_to_path.get(&ino.0) {
            Some(p) => p.clone(),
            None => { reply.error(Errno::ENOENT); return; }
        };
        let ptrim = path.trim_start_matches('/');
        let mut p_iter = ptrim.splitn(2, '/');
        let dir = p_iter.next().unwrap_or("");
        let rest = p_iter.next().unwrap_or("");
        let channel = match state.dir_to_channel.get(dir) {
            Some(c) => c.clone(),
            None => { reply.error(Errno::ENOENT); return; }
        };
        let tchan = match state.channels.get(&channel) {
            Some(c) => c,
            None => { reply.error(Errno::ENOENT); return; }
        };
        let channel_peer = tchan.peer;
        let files = &tchan.files;

        // Try direct file match. Dispatch the download as an async task and
        // return from `read()` immediately so the FUSE event loop is free to
        // service other requests while the download is in flight.
        if let Some(fentry) = files.iter().find(|e| full_for(e) == rest) {
            let parts_docs: Vec<Media> = fentry.parts.iter().cloned().collect();
            let msg_ids: Vec<i32> = fentry.msg_ids.iter().copied().collect();
            let client = state.client.clone();
            let state_for_fetch = state.clone();
            let sem = pid_sem.clone();
            let pid = _req.pid();
            let fh_raw = fh.0;
            debug!("read direct ino={} fh={} parts={} offset={} size={}", ino, fh, parts_docs.len(), offset, size);
            self.rt.spawn(async move {
                let _permit = match sem {
                    Some(s) => Some(acquire_owned_with_throttle_log(s, pid, fh_raw, "direct").await),
                    None => None,
                };
                trace!("read direct fh={} download_range_refresh start", fh_raw);
                match download_range_refresh(&client, &parts_docs, &msg_ids, channel_peer, &state_for_fetch.fresh_docs, offset as usize, size as usize).await {
                    Ok(buf) => {
                        debug!("read direct fh={} got {} bytes", fh_raw, buf.len());
                        reply.data(&buf);
                    }
                    Err(e) => {
                        error!("read direct fh={} download error: {:?}", fh_raw, e);
                        reply.error(Errno::EIO);
                    }
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
            let stem_full = match &f.path {
                Some(p) => {
                    let s = p.to_string_lossy().replace('\\', "/");
                    if s.is_empty() { stem.clone() } else { format!("{}/{}", s, stem) }
                }
                None => stem.clone(),
            };
            let prefix_full = format!("{}/", full);
            let prefix_stem = format!("{}/", stem_full);
            let inner_path = if rest.starts_with(&prefix_full) {
                &rest[prefix_full.len()..]
            } else if rest.starts_with(&prefix_stem) {
                &rest[prefix_stem.len()..]
            } else {
                continue;
            };
            let ae = match f.archive_entries.as_ref().unwrap().iter().find(|x| x.path == inner_path) {
                Some(a) => a,
                None => { reply.error(Errno::ENOENT); return; }
            };
            debug!("read archive entry path='{}' inner='{}' compression={} data_offset={} compressed={} uncompressed={} fh={} offset={} size={}",
                path, inner_path, ae.compression_method, ae.data_offset, ae.compressed_size, ae.uncompressed_size, fh, offset, size);
            let parts_docs: Vec<Media> = f.parts.iter().cloned().collect();
            let msg_ids: Vec<i32> = f.msg_ids.iter().copied().collect();
            let data_offset = ae.data_offset as usize;
            let fh_raw = fh.0;

            match ae.compression_method {
                0 => {
                    let off = data_offset + offset as usize;
                    trace!("stored entry read off={} size={}", off, size);
                    let client = state.client.clone();
                    let state_for_fetch = state.clone();
                    let sem = pid_sem.clone();
                    let pid = _req.pid();
                    self.rt.spawn(async move {
                        let _permit = match sem {
                            Some(s) => Some(acquire_owned_with_throttle_log(s, pid, fh_raw, "stored").await),
                            None => None,
                        };
                        match download_range_refresh(&client, &parts_docs, &msg_ids, channel_peer, &state_for_fetch.fresh_docs, off, size as usize).await {
                            Ok(buf) => { trace!("stored entry got {} bytes", buf.len()); reply.data(&buf); }
                            Err(e) => { error!("stored entry download error: {:?}", e); reply.error(Errno::EIO); }
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
                    // thread and the FUSE event loop returns immediately. The
                    // PrefetchingReader pipelines compressed-byte fetches in a
                    // background task so the decoder rarely blocks on network.
                    let state_for_prefetch = state.clone();
                    let rt_clone = self.rt.clone();
                    let stream = {
                        let mut fs_inner = self.inner.lock().unwrap();
                        fs_inner.deflate_streams.entry(fh_raw).or_insert_with(|| {
                            let reader = spawn_prefetcher(&rt_clone, state_for_prefetch, parts_docs, msg_ids, channel_peer, data_offset, compressed_size);
                            Arc::new(Mutex::new(DeflateStream {
                                decoder: DeflateDecoder::new(reader),
                                pos: 0,
                            }))
                        }).clone()
                    };

                    let sem = pid_sem.clone();
                    let rt_for_sem = self.rt.clone();
                    let pid = _req.pid();
                    self.rt.spawn_blocking(move || {
                        // Acquire per-PID fetch permit (blocks until a slot is free).
                        let _permit = sem.map(|s| rt_for_sem.block_on(acquire_owned_with_throttle_log(s, pid, fh_raw, "deflate")));
                        let mut stream = stream.lock().unwrap();
                        trace!("deflate read fh={} off={} sz={} stream.pos={} uncompressed={} compressed={}",
                            fh_raw, off, sz, stream.pos, uncompressed_size, compressed_size);

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
                                debug!("suppressed ID3v1 probe for channel='{}' inner='{}' fh={} off={} stream_pos={}", channel, ae_path, fh_raw, off, stream.pos);
                                let zeros = vec![0u8; sz];
                                reply.data(&zeros);
                                return;
                            }
                        }

                        // Skip forward if the kernel jumped ahead (shouldn't happen in
                        // normal sequential reads; caller should use STORE for random access).
                        if off > stream.pos {
                            debug!("deflate skip forward from {} to {} (skip {})", stream.pos, off, off - stream.pos);
                            use std::io::Read as _;
                            let mut remaining = off - stream.pos;
                            let mut discard = vec![0u8; remaining.min(65536)];
                            while remaining > 0 {
                                let n = remaining.min(discard.len());
                                match stream.decoder.read(&mut discard[..n]) {
                                    Ok(0) => { reply.error(Errno::EIO); return; }
                                    Ok(r) => { stream.pos += r; remaining -= r; }
                                    Err(_) => { reply.error(Errno::EIO); return; }
                                }
                            }
                        } else if off < stream.pos {
                            // Backward seek: impossible to handle without rewinding.
                            error!("deflate BACKWARD SEEK off={} stream.pos={} — returning EIO", off, stream.pos);
                            reply.error(Errno::EIO);
                            return;
                        }

                        use std::io::Read as _;
                        let mut buf = vec![0u8; sz];
                        let mut total = 0;
                        while total < sz {
                            match stream.decoder.read(&mut buf[total..]) {
                                Ok(0) => { trace!("deflate decoder returned 0 at total={} stream.pos={}", total, stream.pos); break; }
                                Ok(n) => total += n,
                                Err(e) => { error!("deflate decoder error: {:?} at total={} stream.pos={}", e, total, stream.pos); reply.error(Errno::EIO); return; }
                            }
                        }
                        stream.pos += total;
                        trace!("deflate read done fh={} produced={} new_pos={}", fh_raw, total, stream.pos);
                        reply.data(&buf[..total]);
                    });
                    return;
                }
                _ => { reply.error(Errno::ENOTSUP); return; }
            }
        }

        reply.error(Errno::ENOENT);
    }
}
