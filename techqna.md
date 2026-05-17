❯ what happens under the hood when I upload new file to telegram channel now?

⏺ End-to-end trace of a new file landing in a watched channel:

  1. Telegram → process
  grammers-mtsender's socket pump receives the Updates envelope and pushes it into the mpsc::UnboundedReceiver<UpdatesLike> we captured from SenderPool::with_configuration (main.rs:73).

  2. Update stream
  Dispatcher::run (realtime.rs:64) is awaiting UpdateStream::next(). The stream applies any pending gap-recovery (getDifference / getChannelDifference), unwraps the raw TL update, and yields an
  Update::NewMessage(msg).

  3. Routing
  Dispatcher::handle reads msg.peer_id(), confirms PeerKind::Channel, and looks up the channel name in channel_id_to_name (built at startup from each TelegramChannel.peer.id.bare_id()).

  4. RawEntry construction — upsert_message (realtime.rs:113)
  - Reads archive_view under a brief read lock.
  - Calls message_to_raw_entry (indexer.rs:617): grabs the Document/Photo, parses name: / type: overrides, classifies file type, interns the MIME string via MimePool::intern (Mutex hop), captures
  mtime. For a single-message zip with archive_view != File, it awaits try_index_zip, which fetches the EOCD tail (~70 KB) and central directory, then one local-header read per inner entry. ZIP
  indexes hit Arc<Mutex<ZipCache>> first to skip the network when possible.

  5. Snapshot → mutate → reassemble
  - Clones raw_entries HashMap and collapse_by_prefix out of the channel under a read lock.
  - Inserts the new RawEntry at msg_id.
  - Awaits assemble_channel_files (indexer.rs:691): rebuilds every FileEntry, runs multipart <base>.NN grouping (await-fetching central directories for any new multipart zips), sorts by lowercased
  name, applies prefix collapse.

  6. Atomic swap
  Takes a write lock on the channel, replaces raw_entries and files: Arc<Vec<FileEntry>>. Drops the lock. HTTP and FUSE readers immediately see the new snapshot via cheap Arc::clone.

  7. FUSE tree rebuild — TgfsFS::rebuild_channel (fuse.rs:329)
  - Re-snapshots (archive_view, files) from the channel.
  - Takes write lock on RwLock<TreeState>.
  - drop_channel(dir, false) walks path_to_attr for every key starting with /{dir}/, removes from path_to_attr / ino_to_path / children / deflated_paths, and remembers each removed (path, ino).
  - add_channel(...) rebuilds the subtree from current files, recording every touched path.
  - Diff: paths in old \ new → removed; new \ old → added. Parent inodes are recomputed from path_hash(parent) (deterministic).

  8. Kernel + inotify notifications — dispatch_notifications (fuse.rs:374)
  - Removed paths: Notifier::delete(parent_ino, child_ino, name) → FUSE_NOTIFY_DELETE → kernel drops the dentry and fires IN_DELETE to inotify watchers on the parent dir.
  - Added paths: Notifier::inval_entry(parent_ino, name) → FUSE_NOTIFY_INVAL_ENTRY → clears any negative-cache miss so the next lookup() sees the file. FUSE has no NOTIFY_CREATE, so inotify won't
  fire IN_CREATE — watchers see the new file on the next readdir.

  9. Next access
  - HTTP GET /{channel}/... does state.channels.get(...).read().files.clone() (Arc) and finds the new FileEntry.
  - FUSE lookup/readdir reads the rebuilt TreeState and serves the new inode.
  - A read() on the new file goes through the same download_range_refresh path, with the fresh file_reference from the incoming message — no re-fetch needed for hours.

  What doesn't happen
  - No re-index of unrelated channels — the dispatcher locks only the affected channel and rebuilds only /{dir} in the FUSE tree.
  - No re-download of file bytes — only metadata moves through this path. Bytes are fetched lazily on the first read().
  - For saved messages: nothing — the dispatcher skips channels whose source != RegularChannel (the realtime feature flag also short-circuits the whole dispatcher).

❯ if i change file's path mapping in message text what happens?

⏺ Editing the name: line in the caption is Update::MessageEdited, which goes through the exact same upsert_message path as a new message — the dispatcher doesn't distinguish "new" vs "edited"
  because the logic is idempotent on msg_id.

  The key moves

  1. Dispatcher receives MessageEdited, routes by peer_id to the channel, calls upsert_message.
  2. message_to_raw_entry parses the new name: value out of msg.text(). Returns a RawEntry whose raw_name carries the new dir/sub/file.ext.
  3. raw_entries.insert(msg_id, new_raw) replaces the prior entry in place (same key, so no stale leftover).
  4. assemble_channel_files rebuilds the file list. raw_entry_to_file calls split_name on the new raw_name, producing a FileEntry with the new path: Option<PathBuf> and possibly a new name.
  Multipart grouping re-runs (and if this message was part .00 of a group, the whole combined entry takes the new exposed name).
  5. Atomic swap of raw_entries + files: Arc<...>. From this point HTTP requests already see the new layout.

  FUSE diff — the interesting part (TgfsFS::rebuild_channel → compute_diff)

  The tree is rebuilt from scratch for /{dir}, then the old path-set is diffed against the new one:

  ┌────────────────────────┬──────────────────────────────────────────────────────┬──────────────────┬────────────────────────────────────────────────────────────────────────────────┐
  │        Scenario        │                       Old set                        │     New set      │                                      Diff                                      │
  ├────────────────────────┼──────────────────────────────────────────────────────┼──────────────────┼────────────────────────────────────────────────────────────────────────────────┤
  │ Pure rename (same dir) │ /ch/a/file.ext                                       │ /ch/a/file2.ext  │ remove old name, add new name                                                  │
  ├────────────────────────┼──────────────────────────────────────────────────────┼──────────────────┼────────────────────────────────────────────────────────────────────────────────┤
  │ Move to sibling dir    │ /ch/a/file.ext                                       │ /ch/b/file.ext   │ remove file.ext under a, add file.ext under b                                  │
  ├────────────────────────┼──────────────────────────────────────────────────────┼──────────────────┼────────────────────────────────────────────────────────────────────────────────┤
  │ Move to new dir        │ /ch/a/file.ext (and /ch/a if file was only resident) │ /ch/b/x/file.ext │ remove /ch/a/file.ext and possibly /ch/a; add /ch/b, /ch/b/x, /ch/b/x/file.ext │
  └────────────────────────┴──────────────────────────────────────────────────────┴──────────────────┴────────────────────────────────────────────────────────────────────────────────┘

  For every removed path the dispatcher fires FUSE_NOTIFY_DELETE(parent_ino, child_ino, name) → kernel drops the dentry and IN_DELETE reaches inotify watchers on the old parent. For every added
  path, FUSE_NOTIFY_INVAL_ENTRY(parent_ino, name) clears the negative cache so the next lookup() finds the new path (no IN_CREATE — FUSE has no notify variant for it).

  Inode identity

  Inodes are path_hash(path), so the file at the new path gets a different inode number than it had at the old path. Consequences:

  - Anyone with an open fh at the old inode: their next read() does ino_to_path.get(&ino) against the rebuilt tree, doesn't find it, and gets ENOENT. Read fails gracefully; the caller must reopen at
   the new path. This is the same semantics as unlink.
  - HTTP is stateless — every request resolves by path, so no leftover state.
  - The underlying Document reference is unchanged (same msg_id → same parts), so a fresh open + read at the new path serves bytes without any re-fetch from Telegram.

  If you also change type: in the same edit

  message_to_raw_entry re-runs classify_file_type. If you flip media ↔ file, only the HTTP Content-Disposition changes (no tree diff). If you flip to/from zip with archive_view != File, a browsable
  virtual subdirectory appears or disappears — diff lists every inner archive entry as added/removed and fires the corresponding notifications.

  If the edit is a no-op for our purposes

  (e.g. you edited the message text but didn't touch name: / type:) — the new RawEntry is byte-identical to the old one, the assembled FileEntry is identical, the new path-set matches the old set,
  and the diff is empty. No notifications fire, no kernel cache is touched.