use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use fuser::{FileAttr, FileType as FuseFileType, Filesystem, FopenFlags, FileHandle, INodeNo, Errno, OpenFlags, Request, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen};
use grammers_client::media::Media;
use grammers_session::types::PeerRef;
use log::{debug, error, info, trace, warn};
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

const ATTR_TTL: Duration = Duration::from_secs(86400);

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

/// Path-keyed tree state. Wrapped in a RwLock so the realtime dispatcher can
/// rebuild a channel's subtree while readers continue to serve other channels.
struct TreeState {
    path_to_attr: HashMap<String, FileAttr>,
    ino_to_path: HashMap<u64, String>,
    children: HashMap<String, Vec<String>>,
    deflated_paths: HashSet<String>,
}

impl TreeState {
    fn new() -> Self {
        let mut path_to_attr = HashMap::new();
        let now = SystemTime::now();
        path_to_attr.insert("/".to_string(), dir_attr(path_hash("/"), now));
        let mut ino_to_path = HashMap::new();
        ino_to_path.insert(INodeNo::ROOT.0, "/".to_string());
        TreeState {
            path_to_attr,
            ino_to_path,
            children: HashMap::new(),
            deflated_paths: HashSet::new(),
        }
    }

    fn add_child(&mut self, parent: &str, name: &str) {
        let cv = self.children.entry(parent.to_string()).or_default();
        if !cv.iter().any(|n| n == name) {
            cv.push(name.to_string());
        }
    }

    fn ensure_dir(&mut self, parent: &str, name: &str, path: &str, now: SystemTime) {
        self.add_child(parent, name);
        let attr = self.path_to_attr.entry(path.to_string())
            .or_insert_with(|| dir_attr(path_hash(path), now));
        self.ino_to_path.insert(attr.ino.0, path.to_string());
    }

    fn ensure_dirs_along(&mut self, base: &str, rel: &str, now: SystemTime) -> String {
        let mut parent = base.to_string();
        if rel.is_empty() { return parent; }
        for seg in rel.split('/') {
            let child = format!("{}/{}", parent, seg);
            self.ensure_dir(&parent, seg, &child, now);
            parent = child;
        }
        parent
    }

    fn add_file(&mut self, parent: &str, name: &str, size: u64, perm: u16, now: SystemTime) {
        self.add_child(parent, name);
        let path = format!("{}/{}", parent, name);
        let mut attr = file_attr(path_hash(&path), size, now);
        if perm != 0 { attr.perm = perm & !0o222; }
        self.ino_to_path.insert(attr.ino.0, path.clone());
        self.path_to_attr.insert(path, attr);
    }

    /// Add a single channel's subtree under `/{dir}` from its current files,
    /// recording every path touched in `touched`.
    fn add_channel(
        &mut self,
        dir: &str,
        archive_view: ArchiveView,
        files: &[FileEntry],
        touched: &mut HashSet<String>,
    ) {
        let now = SystemTime::now();
        let ch_path = format!("/{}", dir);
        self.ensure_dir("/", dir, &ch_path, now);
        touched.insert(ch_path.clone());

        for f in files.iter() {
            let full = full_for(f);
            let is_browsable_zip = f.file_type == FileType::Zip
                && f.archive_entries.is_some()
                && archive_view != ArchiveView::File;
            let show_as_file = !is_browsable_zip || archive_view == ArchiveView::FileAndDirectory;

            let (parent_rel, fname) = match full.rfind('/') {
                Some(i) => (&full[..i], &full[i + 1..]),
                None => ("", full.as_str()),
            };

            if show_as_file {
                let parent_path = self.ensure_dirs_along(&ch_path, parent_rel, now);
                self.record_dirs(&ch_path, parent_rel, touched);
                let size = f.size.unwrap_or(0) as u64;
                let file_mtime = f.mtime.unwrap_or(now);
                self.add_file(&parent_path, fname, size, 0, file_mtime);
                touched.insert(format!("{}/{}", parent_path, fname));
            } else {
                self.ensure_dirs_along(&ch_path, parent_rel, now);
                self.record_dirs(&ch_path, parent_rel, touched);
            }

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
                let arc_dir = self.ensure_dirs_along(&ch_path, &stem_full, now);
                self.record_dirs(&ch_path, &stem_full, touched);

                for ae in ae_list.iter() {
                    let (ae_parent_rel, ae_name) = match ae.path.rfind('/') {
                        Some(i) => (&ae.path[..i], &ae.path[i + 1..]),
                        None => ("", ae.path.as_str()),
                    };
                    let ae_parent = self.ensure_dirs_along(&arc_dir, ae_parent_rel, now);
                    self.record_dirs(&arc_dir, ae_parent_rel, touched);
                    let file_mtime = f.mtime.unwrap_or(now);
                    self.add_file(&ae_parent, ae_name, ae.uncompressed_size as u64, ae.unix_mode.unwrap_or(0), file_mtime);
                    let ae_full = format!("{}/{}", ae_parent, ae_name);
                    touched.insert(ae_full.clone());
                    if ae.compression_method == 8 {
                        self.deflated_paths.insert(ae_full);
                    }
                }
            }
        }
    }

    fn record_dirs(&self, base: &str, rel: &str, touched: &mut HashSet<String>) {
        let mut parent = base.to_string();
        touched.insert(parent.clone());
        if rel.is_empty() { return; }
        for seg in rel.split('/') {
            parent = format!("{}/{}", parent, seg);
            touched.insert(parent.clone());
        }
    }

    /// Strip everything under `/{dir}` (inclusive of the channel dir itself
    /// only if `include_root` is true) and return the set of paths that were
    /// removed together with their inode numbers.
    fn drop_channel(&mut self, dir: &str, include_root: bool) -> HashMap<String, u64> {
        let dir_root = format!("/{}", dir);
        let prefix = format!("{}/", dir_root);
        let mut removed: HashMap<String, u64> = HashMap::new();
        let keys: Vec<String> = self
            .path_to_attr
            .keys()
            .filter(|k| k.starts_with(&prefix) || (include_root && *k == &dir_root))
            .cloned()
            .collect();
        for k in keys {
            if let Some(attr) = self.path_to_attr.remove(&k) {
                self.ino_to_path.remove(&attr.ino.0);
                removed.insert(k.clone(), attr.ino.0);
            }
            self.children.remove(&k);
            self.deflated_paths.remove(&k);
        }
        if include_root {
            if let Some(root_children) = self.children.get_mut("/") {
                root_children.retain(|n| n != dir);
            }
        } else {
            // Reset the channel root's children so add_channel rebuilds them.
            self.children.remove(&dir_root);
        }
        removed
    }
}

/// Outcome of rebuilding a channel subtree: paths added and paths removed,
/// resolved to (parent_ino, child_ino, name) tuples ready for FUSE notifications.
struct TreeDiff {
    added: Vec<(u64, String)>,
    removed: Vec<(u64, u64, String)>,
}

fn split_parent_name(p: &str) -> (String, String) {
    match p.rfind('/') {
        Some(0) => ("/".to_string(), p[1..].to_string()),
        Some(i) => (p[..i].to_string(), p[i + 1..].to_string()),
        None => ("/".to_string(), p.to_string()),
    }
}

/// Pure: compare an old `(path → ino)` snapshot of a channel subtree against
/// the post-rebuild path set, returning the dentries to delete (with their
/// inode numbers) and the names to invalidate. Extracted as a free function
/// so it can be exercised in isolation without spinning up a `TgfsFS`.
fn compute_diff(old: HashMap<String, u64>, new_paths: HashSet<String>) -> TreeDiff {
    let mut added: Vec<(u64, String)> = Vec::new();
    let mut removed: Vec<(u64, u64, String)> = Vec::new();
    for (path, ino) in &old {
        if !new_paths.contains(path) {
            let (parent_path, name) = split_parent_name(path);
            let parent_ino = path_hash(&parent_path);
            removed.push((parent_ino, *ino, name));
        }
    }
    for path in &new_paths {
        if !old.contains_key(path) {
            let (parent_path, name) = split_parent_name(path);
            let parent_ino = path_hash(&parent_path);
            added.push((parent_ino, name));
        }
    }
    TreeDiff { added, removed }
}

const DEFLATE_FETCH_CHUNK: usize = 2 * 1024 * 1024;
const DEFLATE_PREFETCH_DEPTH: usize = 4;
const INFLATE_CACHE_SIZE: usize = 1024 * 1024;
const MAX_FORWARD_SEEK: usize = 32 * 1024 * 1024;

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
        let _ = tx.send(Ok(Vec::new())).await;
    });
    PrefetchingReader { rx, buf: Vec::new(), buf_pos: 0, eof: false }
}

struct DeflateStream {
    decoder: DeflateDecoder<PrefetchingReader>,
    pos: usize,
    cache: std::collections::VecDeque<u8>,
    state: Arc<AppState>,
    parts: Vec<Media>,
    msg_ids: Vec<i32>,
    peer: Option<PeerRef>,
    data_offset: usize,
    compressed_size: usize,
}

impl DeflateStream {
    fn cache_start(&self) -> usize {
        self.pos - self.cache.len()
    }

    fn inflate_to(&mut self, target: usize) -> std::io::Result<()> {
        use std::io::Read;
        let mut tmp = vec![0u8; 65536];
        while self.pos < target {
            let want = (target - self.pos).min(tmp.len());
            let n = self.decoder.read(&mut tmp[..want])?;
            if n == 0 { break; }
            self.cache.extend(&tmp[..n]);
            self.pos += n;
            let excess = self.cache.len().saturating_sub(INFLATE_CACHE_SIZE);
            if excess > 0 { self.cache.drain(..excess); }
        }
        Ok(())
    }

    fn reset(&mut self, rt: &Handle) {
        let reader = spawn_prefetcher(
            rt,
            self.state.clone(),
            self.parts.clone(),
            self.msg_ids.clone(),
            self.peer,
            self.data_offset,
            self.compressed_size,
        );
        self.decoder = DeflateDecoder::new(reader);
        self.pos = 0;
        self.cache.clear();
    }
}

async fn acquire_owned_with_throttle_log(
    sem: Arc<tokio::sync::Semaphore>,
    pid: u32,
    fh: u64,
    site: &str,
) -> tokio::sync::OwnedSemaphorePermit {
    match sem.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            warn!(
                "fetch throttled pid={} fh={} site={} available={} waiting...",
                pid, fh, site, sem.available_permits()
            );
            sem.acquire_owned().await.expect("semaphore closed")
        }
    }
}

struct TgfsFSMutable {
    deflate_streams: HashMap<u64, Arc<Mutex<DeflateStream>>>,
    next_fh: u64,
    pid_semaphores: HashMap<u32, Arc<tokio::sync::Semaphore>>,
}

/// Shared inner state. The `Filesystem` impl wraps an `Arc<TgfsFsInner>` so a
/// clone of the handle can be retained by the realtime dispatcher (which uses
/// it to rebuild channel subtrees and fire kernel notifications) while the
/// fuser session owns its own clone for callback dispatch.
pub struct TgfsFsInner {
    state: Arc<AppState>,
    tree: RwLock<TreeState>,
    rt: Handle,
    inner: Mutex<TgfsFSMutable>,
    max_fetches_per_pid: Option<usize>,
    global_semaphore: Option<Arc<tokio::sync::Semaphore>>,
    /// Set after `Session::new` succeeds and the session's notifier handle is
    /// available. `None` before that, or when running headlessly (HTTP-only).
    notifier: Mutex<Option<fuser::Notifier>>,
}

#[derive(Clone)]
pub struct TgfsFS {
    inner: Arc<TgfsFsInner>,
}

impl TgfsFS {
    pub fn new(state: Arc<AppState>) -> Self {
        let mut tree = TreeState::new();

        // Build channel subtrees. dir_to_channel keys are the directory names
        // we expose; values are the indexing-keys into state.channels.
        for (dir, channel) in &state.dir_to_channel {
            if let Some(lock) = state.channels.get(channel) {
                let g = lock.read().unwrap();
                let mut touched = HashSet::new();
                tree.add_channel(dir, g.archive_view, &g.files, &mut touched);
            }
        }

        let max_fetches_per_pid = state.max_fetches_per_pid;
        let global_semaphore = state.max_fetches_total.map(|n| Arc::new(tokio::sync::Semaphore::new(n)));
        let inner = Mutex::new(TgfsFSMutable {
            deflate_streams: HashMap::new(),
            next_fh: 1,
            pid_semaphores: HashMap::new(),
        });
        Self {
            inner: Arc::new(TgfsFsInner {
                state,
                tree: RwLock::new(tree),
                rt: Handle::current(),
                inner,
                max_fetches_per_pid,
                global_semaphore,
                notifier: Mutex::new(None),
            }),
        }
    }

    /// Plug in the FUSE session's Notifier so realtime updates can deliver
    /// kernel cache invalidations and inotify events.
    pub fn set_notifier(&self, notifier: fuser::Notifier) {
        *self.inner.notifier.lock().unwrap() = Some(notifier);
    }

    /// Rebuild the subtree for `dir` from its channel's current files. Sends
    /// FUSE notifications for created and removed entries. Safe to call from
    /// any async context.
    pub fn rebuild_channel(&self, dir: &str) {
        // Snapshot the channel's current state (Arc clone of files) under the
        // channel lock, then release before touching the tree.
        let snapshot = {
            let channel_name = match self.inner.state.dir_to_channel.get(dir) {
                Some(n) => n.clone(),
                None => return,
            };
            let lock = match self.inner.state.channels.get(&channel_name) {
                Some(l) => l,
                None => return,
            };
            let g = lock.read().unwrap();
            (g.archive_view, g.files.clone())
        };
        let (archive_view, files) = snapshot;

        let diff = {
            let mut tree = self.inner.tree.write().unwrap();
            let old = tree.drop_channel(dir, false);
            let mut new_paths: HashSet<String> = HashSet::new();
            tree.add_channel(dir, archive_view, &files, &mut new_paths);
            compute_diff(old, new_paths)
        };
        self.dispatch_notifications(&diff);
    }

    fn dispatch_notifications(&self, diff: &TreeDiff) {
        let notifier = match self.inner.notifier.lock().unwrap().clone() {
            Some(n) => n,
            None => return,
        };
        // FUSE_NOTIFY_DELETE fires IN_DELETE inotify events on watchers of the
        // parent directory and drops the kernel's dentry cache for the child.
        for (parent_ino, child_ino, name) in &diff.removed {
            let name_os = std::ffi::OsString::from(name);
            if let Err(e) = notifier.delete(INodeNo(*parent_ino), INodeNo(*child_ino), &name_os) {
                debug!("notifier.delete({}, {}, {}) failed: {:?}", parent_ino, child_ino, name, e);
            }
        }
        // FUSE_NOTIFY_INVAL_ENTRY clears any negative-cache miss the kernel
        // recorded for this name. There is no FUSE notify variant that fires
        // IN_CREATE — watchers will see the new file on the next readdir.
        for (parent_ino, name) in &diff.added {
            let name_os = std::ffi::OsString::from(name);
            if let Err(e) = notifier.inval_entry(INodeNo(*parent_ino), &name_os) {
                debug!("notifier.inval_entry({}, {}) failed: {:?}", parent_ino, name, e);
            }
        }
    }

    fn pid_semaphore(&self, pid: u32) -> Option<Arc<tokio::sync::Semaphore>> {
        let limit = self.inner.max_fetches_per_pid?;
        let mut inner = self.inner.inner.lock().unwrap();
        Some(inner.pid_semaphores.entry(pid).or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(limit))).clone())
    }

    fn is_deflated_entry(&self, ino: u64) -> bool {
        let g = self.inner.tree.read().unwrap();
        g.ino_to_path.get(&ino).map_or(false, |p| g.deflated_paths.contains(p))
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
        let g = self.inner.tree.read().unwrap();
        let parent_path = match g.ino_to_path.get(&parent.0) {
            Some(p) => p.clone(),
            None => { reply.error(Errno::ENOENT); return; }
        };
        let name_str = name.to_string_lossy();
        let target = if parent_path == "/" { format!("/{}", name_str) } else { format!("{}/{}", parent_path, name_str) };
        let p = if target == "/" { "/".to_string() } else { target.trim_end_matches('/').to_string() };
        if let Some(attr) = g.path_to_attr.get(&p) {
            reply.entry(&ATTR_TTL, attr, fuser::Generation(0));
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let g = self.inner.tree.read().unwrap();
        match g.ino_to_path.get(&ino.0).and_then(|p| g.path_to_attr.get(p)) {
            Some(a) => reply.attr(&ATTR_TTL, a),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn readdir(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        let g = self.inner.tree.read().unwrap();
        let path = match g.ino_to_path.get(&ino.0) {
            Some(p) => p.clone(),
            None => { reply.error(Errno::ENOENT); return; }
        };
        let mut entries: Vec<(u64, FuseFileType, String)> = Vec::new();
        entries.push((path_hash(&path), FuseFileType::Directory, ".".to_string()));
        entries.push((path_hash("/"), FuseFileType::Directory, "..".to_string()));

        if let Some(children_vec) = g.children.get(&path) {
            for name in children_vec.iter() {
                let child_path = if path == "/" { format!("/{}", name) } else { format!("{}/{}", path, name) };
                if let Some(attr) = g.path_to_attr.get(&child_path) {
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
        let g = self.inner.tree.read().unwrap();
        let exists = g.ino_to_path.get(&ino.0)
            .and_then(|p| g.path_to_attr.get(p))
            .map_or(false, |a| a.kind == FuseFileType::Directory);
        if exists { reply.opened(FileHandle(0), FopenFlags::empty()); } else { reply.error(Errno::ENOENT); }
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let (path, is_file, file_size) = {
            let g = self.inner.tree.read().unwrap();
            let path = g.ino_to_path.get(&ino.0).cloned();
            let attr = path.as_ref().and_then(|p| g.path_to_attr.get(p));
            let is_file = attr.map_or(false, |a| a.kind == FuseFileType::RegularFile);
            let file_size = attr.map_or(0, |a| a.size);
            (path.unwrap_or_else(|| "<unknown>".to_string()), is_file, file_size)
        };
        if is_file {
            let fh = {
                let mut inner = self.inner.inner.lock().unwrap();
                let fh = inner.next_fh;
                inner.next_fh += 1;
                fh
            };
            let deflated = self.is_deflated_entry(ino.0);
            let flags = if deflated && file_size >= MAX_FORWARD_SEEK as u64 {
                info!("open deflated fh={} path='{}' size={} (direct_io, too large to cache)", fh, path, file_size);
                FopenFlags::FOPEN_DIRECT_IO
            } else {
                if deflated {
                    info!("open deflated fh={} path='{}' size={} (keep_cache)", fh, path, file_size);
                } else {
                    debug!("open ino={} path='{}' fh={}", ino, path, fh);
                }
                FopenFlags::FOPEN_KEEP_CACHE
            };
            reply.opened(FileHandle(fh), flags);
        } else {
            reply.error(Errno::EISDIR);
        }
    }

    // Read-only filesystem: there is never dirty state to write back. We reply
    // `ok()` (rather than letting fuser default to `ENOSYS`) so the post-condition
    // each hook promises — "all pending writes are durable" — is truthfully
    // reported, instead of claiming the operation isn't supported. The kernel
    // remaps `ENOSYS` to success here, so behavior is identical either way; the
    // win is semantic accuracy plus quieter logs.
    fn flush(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _lock_owner: fuser::LockOwner, reply: ReplyEmpty) {
        reply.ok();
    }

    fn fsync(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _datasync: bool, reply: ReplyEmpty) {
        reply.ok();
    }

    fn fsyncdir(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _datasync: bool, reply: ReplyEmpty) {
        reply.ok();
    }

    fn release(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, _flags: OpenFlags, _lock_owner: Option<fuser::LockOwner>, _flush: bool, reply: ReplyEmpty) {
        let had = self.inner.inner.lock().unwrap().deflate_streams.remove(&fh.0).is_some();
        let path = self.inner.tree.read().unwrap().ino_to_path.get(&_ino.0).cloned().unwrap_or_else(|| "<unknown>".to_string());
        if had {
            info!("release deflated fh={} path='{}'", fh, path);
        } else {
            debug!("release fh={} ino={} path='{}'", fh, _ino, path);
        }
        reply.ok();
    }

    fn read(&self, _req: &Request, ino: INodeNo, fh: FileHandle, offset: u64, size: u32, _flags: OpenFlags, _lock_owner: Option<fuser::LockOwner>, reply: ReplyData) {
        debug!("read enter ino={} fh={} offset={} size={} pid={}", ino, fh, offset, size, _req.pid());
        let pid_sem = self.pid_semaphore(_req.pid());
        let global_sem = self.inner.global_semaphore.clone();
        let state = self.inner.state.clone();
        let path = {
            let g = self.inner.tree.read().unwrap();
            match g.ino_to_path.get(&ino.0) {
                Some(p) => p.clone(),
                None => { reply.error(Errno::ENOENT); return; }
            }
        };
        let ptrim = path.trim_start_matches('/');
        let mut p_iter = ptrim.splitn(2, '/');
        let dir = p_iter.next().unwrap_or("");
        let rest = p_iter.next().unwrap_or("");
        let channel_name = match state.dir_to_channel.get(dir) {
            Some(c) => c.clone(),
            None => { reply.error(Errno::ENOENT); return; }
        };
        let (channel_peer, files, skip_deflated_id3v1) = {
            let lock = match state.channels.get(&channel_name) {
                Some(l) => l,
                None => { reply.error(Errno::ENOENT); return; }
            };
            let g = lock.read().unwrap();
            (g.peer, g.files.clone(), g.skip_deflated_id3v1)
        };
        let files: &[FileEntry] = &files;

        if let Some(fentry) = files.iter().find(|e| full_for(e) == rest) {
            let parts_docs: Vec<Media> = fentry.parts.iter().cloned().collect();
            let msg_ids: Vec<i32> = fentry.msg_ids.iter().copied().collect();
            let client = state.client.clone();
            let state_for_fetch = state.clone();
            let sem = pid_sem.clone();
            let gsem = global_sem.clone();
            let pid = _req.pid();
            let fh_raw = fh.0;
            debug!("read direct ino={} fh={} parts={} offset={} size={}", ino, fh, parts_docs.len(), offset, size);
            self.inner.rt.spawn(async move {
                let _permit_total = match gsem {
                    Some(s) => Some(acquire_owned_with_throttle_log(s, pid, fh_raw, "direct-total").await),
                    None => None,
                };
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
                    let gsem = global_sem.clone();
                    let pid = _req.pid();
                    self.inner.rt.spawn(async move {
                        let _permit_total = match gsem {
                            Some(s) => Some(acquire_owned_with_throttle_log(s, pid, fh_raw, "stored-total").await),
                            None => None,
                        };
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
                    let channel_skip = skip_deflated_id3v1;

                    let state_for_prefetch = state.clone();
                    let rt_clone = self.inner.rt.clone();
                    let stream = {
                        let mut fs_inner = self.inner.inner.lock().unwrap();
                        fs_inner.deflate_streams.entry(fh_raw).or_insert_with(|| {
                            let reader = spawn_prefetcher(&rt_clone, state_for_prefetch.clone(), parts_docs.clone(), msg_ids.clone(), channel_peer, data_offset, compressed_size);
                            Arc::new(Mutex::new(DeflateStream {
                                decoder: DeflateDecoder::new(reader),
                                pos: 0,
                                cache: std::collections::VecDeque::new(),
                                state: state_for_prefetch,
                                parts: parts_docs,
                                msg_ids,
                                peer: channel_peer,
                                data_offset,
                                compressed_size,
                            }))
                        }).clone()
                    };

                    let sem = pid_sem.clone();
                    let gsem = global_sem.clone();
                    let rt_for_sem = self.inner.rt.clone();
                    let pid = _req.pid();
                    let channel_for_log = channel_name.clone();
                    self.inner.rt.spawn_blocking(move || {
                        let _permit_total = gsem.map(|s| rt_for_sem.block_on(acquire_owned_with_throttle_log(s, pid, fh_raw, "deflate-total")));
                        let _permit = sem.map(|s| rt_for_sem.block_on(acquire_owned_with_throttle_log(s, pid, fh_raw, "deflate")));
                        let mut stream = stream.lock().unwrap();
                        debug!("deflate read fh={} off={} sz={} stream.pos={} entry='{}'",
                            fh_raw, off, sz, stream.pos, ae_path);

                        const MAX_TOTAL_READ: usize = 128 * 1024;
                        const DISTANCE_TO_FILE_END: usize = 16 * 1024;
                        const ID3V1_READ_SIZE: usize = 4096;
                        if channel_skip {
                            let path_lower = ae_path.to_lowercase();
                            let looks_like_audio = path_lower.ends_with(".mp3") || path_lower.ends_with(".flac");
                            if looks_like_audio && sz == ID3V1_READ_SIZE && stream.pos < MAX_TOTAL_READ && uncompressed_size.saturating_sub(off) < DISTANCE_TO_FILE_END {
                                debug!("suppressed ID3v1 probe for channel='{}' inner='{}' fh={} off={} stream_pos={}", channel_for_log, ae_path, fh_raw, off, stream.pos);
                                let zeros = vec![0u8; sz];
                                reply.data(&zeros);
                                return;
                            }
                        }

                        if off < stream.cache_start() {
                            if off < MAX_FORWARD_SEEK {
                                debug!("deflate backward seek channel='{}' entry='{}' fh={} off={} cache_start={} — resetting and re-inflating",
                                    channel_for_log, ae_path, fh_raw, off, stream.cache_start());
                                stream.reset(&rt_for_sem);
                            } else {
                                error!("deflate BACKWARD SEEK channel='{}' entry='{}' fh={} off={} cache_start={} — beyond first {}MB, returning EIO",
                                    channel_for_log, ae_path, fh_raw, off, stream.cache_start(), MAX_FORWARD_SEEK / (1024 * 1024));
                                reply.error(Errno::EIO);
                                return;
                            }
                        }

                        if off > stream.pos && off - stream.pos > MAX_FORWARD_SEEK {
                            warn!("deflate FORWARD SEEK too large channel='{}' entry='{}' fh={} off={} stream.pos={} skip={} — returning EIO",
                                channel_for_log, ae_path, fh_raw, off, stream.pos, off - stream.pos);
                            reply.error(Errno::EIO);
                            return;
                        }

                        if off > stream.pos {
                            debug!("deflate skip forward from {} to {} (+{})", stream.pos, off, off - stream.pos);
                        }

                        let target = (off + sz).min(uncompressed_size);
                        if let Err(e) = stream.inflate_to(target) {
                            error!("deflate inflate error: {e:?}");
                            reply.error(Errno::EIO);
                            return;
                        }
                        if stream.pos < target {
                            warn!("deflate EOF early fh={} entry='{}' expected pos={} actual pos={} off={} sz={}",
                                fh_raw, ae_path, target, stream.pos, off, sz);
                        }

                        let cache_start = stream.cache_start();
                        let idx = off.saturating_sub(cache_start);
                        let available = stream.cache.len().saturating_sub(idx);
                        let to_serve = sz.min(available);
                        let (s1, s2) = stream.cache.as_slices();
                        let mut out = Vec::with_capacity(to_serve);
                        let end = idx + to_serve;
                        if end <= s1.len() {
                            out.extend_from_slice(&s1[idx..end]);
                        } else if idx >= s1.len() {
                            let s2_idx = idx - s1.len();
                            out.extend_from_slice(&s2[s2_idx..s2_idx + to_serve]);
                        } else {
                            out.extend_from_slice(&s1[idx..]);
                            out.extend_from_slice(&s2[..end - s1.len()]);
                        }
                        if to_serve < sz && target < uncompressed_size {
                            warn!("deflate short read fh={} entry='{}' off={} requested={} served={} cache_start={} cache_len={}",
                                fh_raw, ae_path, off, sz, to_serve, cache_start, stream.cache.len());
                        }
                        debug!("deflate read done fh={} entry='{}' off={} served={} stream.pos={}", fh_raw, ae_path, off, out.len(), stream.pos);
                        reply.data(&out);
                    });
                    return;
                }
                _ => { reply.error(Errno::ENOTSUP); return; }
            }
        }

        reply.error(Errno::ENOENT);
    }
}


#[cfg(test)]
#[path = "../tests/fuse.rs"]
mod tests;
