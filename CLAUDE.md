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

**Virtual paths.** A message may prefix its document with `path: dir/sub/file.ext` to place the file under a virtual directory. Directories are synthesized from these paths and browsable. A trailing `/` (e.g. `path: dir/sub/`) keeps the original document filename and applies the value as the parent directory only.

**Message overrides** (single-message):
- `path: <path/filename>` — override the full virtual path (directory + filename).
- `path: <dir>/` — directory-only override; original document filename is kept. Parsed by `resolve_path_override()` in `src/indexer.rs`.
- `type: file|media|zip` — override classification. `media` forces inline `Content-Disposition` for in-browser playback; `file` forces attachment; `zip` enables archive indexing.

**Grouped-album overrides** (one caption shared across every part of a Telegram album):
- Captions are extracted by `extract_group_caption()` in `src/indexer.rs` and stored on `TelegramChannel.group_captions: HashMap<grouped_id, GroupCaption>`. Applied uniformly in `raw_entry_to_file()` at assembly time.
- `path:` is **always** treated as a directory regardless of trailing slash. Original document filenames are kept on every part.
- `type:` (file/media/zip) is applied to every part. ZIP archive indexes are not fetched retroactively at assembly time, so `type: zip` on a non-zip MIME won't yield browsable inner entries.

**Auto classification** (when no `type:` override): `audio/*` or `video/*` MIME → media; `application/zip` or `.zip` extension → zip; otherwise file.

**Multipart files.** Opt-in per channel via `multipart_policy: none|suffix|album` (default `none`).
- `suffix`: documents matching `<base>.NN` (two-digit, contiguous, starting at `.00`) are merged. `.00`'s `path:` override (if any) sets the exposed name.
- `album`: Telegram-album posts whose caption carries `multipart:` (or `multipart: true`) are concatenated in chronological msg_id order. The album's `path:` directive takes single-message semantics on the combined file (trailing `/` = directory-only).
Streaming seamlessly concatenates parts on download. Logic lives in `assemble_suffix_multipart()` / `assemble_album_multipart()` in `src/indexer.rs`.

**Flag-style caption directives.** A caption like `key:` with no value parses as `key: true` (`parse_bool_value()` in `src/indexer.rs`). Currently `multipart:` is the only flag-style directive; `true`/`yes`/`1`/empty are truthy, anything else falsy.

**ZIP browsing.** For `zip`-classified files, the EOCD + central directory is fetched at index time (only the tail bytes, not the whole archive). Archive entries appear as a virtual subdirectory named after the archive stem. Inner files are extracted by ranged-downloading the relevant local file header + compressed payload and inflating on the fly.

**HTTP Range requests.** Supported for:
- single-doc downloads (delegated to Telegram's chunked downloader),
- multipart concatenations (locates the right part and offset),
- inner-archive files (decompresses from entry start up to the requested offset, then slices).

This enables media seeking in browsers and players.

**MIME pool.** MIME type strings are interned across the index to reduce memory for large channels.

**FUSE mount.** When `mount_at` is set, channels appear as top-level directories under the mountpoint. Files (including virtual paths, multipart concatenations, and browsable archive entries) are exposed as regular read-only files. The path tree, inode table, and reverse inode→path map are built once at mount time. FUSE callbacks are blocking; they drive async `download_range` calls via a captured `tokio::runtime::Handle` cached on `TgfsFS`.

## Source layout

- `src/main.rs` — CLI parsing (`--config`), auth/login, config loading, conditional HTTP + FUSE bootstrap.
- `src/index.rs` — `Config` (with optional `http_port`/`mount_at`), `FileEntry`/`ArchiveFileEntry`/`AppState` types, `dir_listing` HTML renderer.
- `src/indexer.rs` — channel walking, multipart grouping, ZIP central-directory parsing, ranged downloads. ZIP indexing is consolidated in `try_index_zip()`; message-caption directives go through `message_field()`.
- `src/server.rs` — axum router, directory listings (channel root / virtual dir / archive interior), file/multipart/archive streaming with Range support. Archive prefix matching for both directory and file paths goes through `match_archive()`.
- `src/fuse.rs` — `TgfsFS` implementing `fuser::Filesystem`. `FileAttr` construction goes through `dir_attr`/`file_attr` helpers; the path tree is built via `ensure_dir`/`ensure_dirs_along`/`add_file`. Sync FUSE reads call into async download via `block_download()` on the cached runtime handle.

## Troubleshooting via logs

When the integration runner (`cargo run --example integration_test`) misbehaves, re-run with `--log-level debug --log-file integration_test.log` and read that file before stepping through code. The log captures every Telegram RPC, FUSE callback, mount/unmount cycle, and download in chronological order — usually faster than reproducing the failure in a debugger. `--log-file` truncates on each run, so previous noise is gone.

For the production binary, the same `log:` config knob in `tgfs.yml` (or `RUST_LOG=debug`) routes the same instrumentation to stderr. Key targets when narrowing down: `tgfs::indexer` (channel walk + ZIP indexing), `tgfs::fuse` (FUSE callbacks + deflate prefetch), `tgfs::realtime` (update dispatcher), `tgfs::server` (HTTP), `grammers_mtsender` / `grammers_mtproto` (raw RPC).

## tgup (uploader)

`tgup` is the upload-side CLI under `src/bin/tgup/`. Plan construction, ffmpeg encoding, and the Telegram upload primitives live in `plan.rs`, `ffmpeg.rs`, and `upload.rs`.

**Encoded-video upload invariants.** `run_encoded_video()` in `src/bin/tgup/ffmpeg.rs`:
- Must not branch on encoded size — small and large videos take the same code path. Don't probe the source size or buffer the ffmpeg head to pick between upload APIs.
- Must not buffer more than one 512 KB chunk of encoded data before pushing to Telegram. Stream ffmpeg's stdout straight into `upload_one_big_file` (`upload.saveBigFilePart` + `InputFileBig`); peak RAM stays at `TG_CHUNK`.

## Docker

Multi-stage `Dockerfile`: `rust:bookworm` builder (needs `pkg-config` + `libfuse-dev` to link `fuser`), `debian:bookworm-slim` runtime with only `ca-certificates` and `fuse` (libfuse2 + `fusermount`). Binary is stripped. Container reads config and persistent state from `/data`. FUSE inside the container needs `--cap-add SYS_ADMIN`, `--device /dev/fuse`, and an `rshared` bind-mount on the target directory. See `README.md` for full run commands.
