# tgfs

HTTP read-only filesystem backed by Telegram channels. Indexes document messages in configured channels and exposes them as a browsable directory tree with streaming downloads and in-browser media playback.

## Runtime

- Binary reads `auth.json` (api_id/api_hash/phone — prompts and saves on first run) and `tgfs.yml` (channels + options).
- Session is persisted to `session.sqlite3`; first login prompts for SMS code and 2FA password if required.
- On startup, indexes all configured channels once, then serves HTTP on `config.port` (default 8080).

## Config (`tgfs.yml`)

```yaml
port: 8080
channels:
  - name: <channel title>
    archive_view: file | directory | file_and_directory
```

`archive_view` controls how `.zip` entries are exposed:
- `file` (default) — raw download only
- `directory` — browse archive contents only (raw download returns 404)
- `file_and_directory` — both

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

## Source layout

- `src/main.rs` — auth/login, config loading, server bootstrap.
- `src/index.rs` — config, `FileEntry`/`ArchiveFileEntry`/`AppState` types, `dir_listing` HTML renderer.
- `src/indexer.rs` — channel walking, multipart grouping, ZIP central-directory parsing, ranged downloads.
- `src/server.rs` — axum router, directory listings (channel root / virtual dir / archive interior), file/multipart/archive streaming with Range support.
