# tgfs

Read-only filesystem backed by Telegram channels, served over HTTP and/or mounted via FUSE. Indexes document messages in configured channels and exposes them as a browsable directory tree with streaming downloads and in-browser media playback.

## Runtime

- Binary reads `auth.json` (api_id/api_hash/phone — prompts and saves on first run) and a YAML config (default `tgfs.yml`, override with `--config <path>`).
- Session is persisted to `session.sqlite3`; first login prompts for SMS code and 2FA password if required.
- On startup, indexes all configured channels once, then starts whichever services are configured: HTTP server (`http_port`), FUSE mount (`mount_at`), or both concurrently. At least one must be set or the binary errors out.
- HTTP server runs as a `tokio::spawn`ed task; FUSE mount runs in a `spawn_blocking` task; both are awaited via `tokio::try_join!`.

## Config (`tgfs.yml`)

```yaml
http_port: 8080         # optional — serve HTTP index on this port
mount_at: /mnt/tgfs     # optional — mount FUSE filesystem at this path
channels:
  - name: <channel title>
    archive_view: file | directory | file_and_directory
```

`archive_view` controls how `.zip` entries are exposed:
- `file` (default) — raw download only
- `directory` — browse archive contents only (raw download returns 404)
- `file_and_directory` — both

CLI: `tgfs [--config <path>]`. Default config path is `tgfs.yml` in the working directory.

## Features

**Directory listing.** `GET /` lists channels; `GET /{channel}/` lists files. Apache-style HTML index with sizes.

**Virtual paths.** A message may prefix its document with `name: dir/sub/file.ext` to place the file under a virtual directory. Directories are synthesized from these paths and browsable.

**Message overrides** (single-message only, ignored for grouped albums):
- `name: <path/filename>` — override filename and/or place under a virtual path.
- `type: file|media|zip` — override classification. `media` forces inline `Content-Disposition` for in-browser playback; `file` forces attachment; `zip` enables archive indexing.

**Auto classification** (when no `type:` override): `audio/*` or `video/*` MIME → media; `application/zip` or `.zip` extension → zip; otherwise file.

**Multipart files.** Documents whose filenames match `<base>.NN` (two-digit part numbers starting at `.00`, contiguous) are auto-merged into a single logical file. Streaming seamlessly concatenates parts on download. The `.00` message's `name:` override (if any) sets the exposed name.

**ZIP browsing.** For `zip`-classified files, the EOCD + central directory is fetched at index time (only the tail bytes, not the whole archive). Archive entries appear as a virtual subdirectory named after the archive stem. Inner files are extracted by ranged-downloading the relevant local file header + compressed payload and inflating on the fly.

**HTTP Range requests.** Supported for:
- single-doc downloads (delegated to Telegram's chunked downloader),
- multipart concatenations (locates the right part and offset),
- inner-archive files (decompresses from entry start up to the requested offset, then slices).

This enables media seeking in browsers and players.

**MIME pool.** MIME type strings are interned across the index to reduce memory for large channels.

**FUSE mount.** When `mount_at` is set, channels appear as top-level directories under the mountpoint. Files (including virtual paths, multipart concatenations, and browsable archive entries) are exposed as regular read-only files. The path tree, inode table, and reverse inode→path map are built once at mount time. FUSE callbacks are blocking; they drive async `download_range` calls via a captured `tokio::runtime::Handle` cached on `TgfsFS`.

Mount options: `AllowOther`, `AutoUnmount`, `RO`, `kernel_cache`. The `kernel_cache` flag makes the kernel serve repeated reads from its page cache without calling into FUSE — critical for multi-app access on a single-threaded FUSE event loop. Attr/entry TTL is 24 h (`ATTR_TTL`) and `blksize` is 128 KB (`BLKSIZE`), both tuned for a static read-only filesystem.

**FUSE deflate streaming.** Deflated ZIP entries use `TelegramReader` (wraps `DeflateDecoder<TelegramReader>`), one instance per open file handle stored in `TgfsFS::deflate_streams` (keyed by `fh`). `open()` assigns unique file handles via `next_fh`; `release()` removes the stream. `TelegramReader` fetches compressed data from Telegram in 512 KB chunks (`DEFLATE_FETCH`) with sliding-window prefetch: a background tokio task is spawned via `rt.spawn()` as soon as 256 KB of the current buffer is consumed (`DEFLATE_PREFETCH_AT = DEFLATE_FETCH / 2`), so the next chunk is downloading while the decoder reads the back half. At most one 512 KB chunk is in memory at a time; the decoder's 32 KB sliding window is internal to `flate2`. Stored (method 0) ZIP entries use direct ranged downloads per FUSE read — no streaming state needed.

## Source layout

- `src/main.rs` — CLI parsing (`--config`), auth/login, config loading, conditional HTTP + FUSE bootstrap.
- `src/index.rs` — `Config` (with optional `http_port`/`mount_at`), `FileEntry`/`ArchiveFileEntry`/`AppState` types, `dir_listing` HTML renderer.
- `src/indexer.rs` — channel walking, multipart grouping, ZIP central-directory parsing, ranged downloads. ZIP indexing is consolidated in `try_index_zip()`; message-caption directives go through `message_field()`.
- `src/server.rs` — axum router, directory listings (channel root / virtual dir / archive interior), file/multipart/archive streaming with Range support. Archive prefix matching for both directory and file paths goes through `match_archive()`.
- `src/fuse.rs` — `TgfsFS` implementing `fuser::Filesystem`. `FileAttr` construction goes through `dir_attr`/`file_attr` helpers; the path tree is built via `ensure_dir`/`ensure_dirs_along`/`add_file`. Sync FUSE reads call into async download via `block_download()` on the cached runtime handle. `TelegramReader` implements `Read` for streaming deflate decoding with background prefetch; `DeflateStream` pairs a decoder with its decompressed-output position.

## Docker

Multi-stage `Dockerfile`: `rust:bookworm` builder (needs `pkg-config` + `libfuse-dev` to link `fuser`), `debian:bookworm-slim` runtime with only `ca-certificates` and `fuse` (libfuse2 + `fusermount`). Binary is stripped. Container reads config and persistent state from `/data`. FUSE inside the container needs `--cap-add SYS_ADMIN`, `--device /dev/fuse`, and an `rshared` bind-mount on the target directory. See `README.md` for full run commands.
