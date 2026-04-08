# tgfs

Read-only HTTP and FUSE filesystem backed by Telegram channels. Indexes document messages in configured channels and exposes them as a browsable directory tree via a web interface or a local mount point, with streaming downloads and in-browser media playback.

## Features

- **Directory listing** — Apache-style HTML index with file sizes. `GET /` lists channels, `GET /{channel}/` lists files.
- **Virtual paths** — messages with `name: dir/sub/file.ext` in the caption place files under virtual directories.
- **ZIP browsing** — archive contents are browsable as directories. Only the central directory is fetched at index time; inner files are extracted on demand via ranged downloads and DEFLATE inflation.
- **Multipart files** — documents named `<base>.00`, `<base>.01`, … are auto-merged into a single logical file with seamless streaming.
- **HTTP Range requests** — seeking works in browsers and media players for single files, multipart concatenations, and inner-archive entries.
- **Media playback** — audio and video files are served with inline `Content-Disposition` for in-browser playback.
- **FUSE mount** — mount channels as a local read-only filesystem (macOS/Linux) for use with standard file tools.
- **MIME interning** — MIME type strings are pooled across the index to reduce memory on large channels.

## Requirements

- Rust 2024 edition
- A Telegram API ID and API hash ([my.telegram.org](https://my.telegram.org))

## Quick start

```bash
# Build
cargo build --release

# Run (interactive auth on first launch)
cargo run --release
```

On first run the binary will prompt for your phone number, SMS code, and optional 2FA password. Credentials are saved to `auth.json` and the session is persisted in `session.sqlite3`.

Once running, browse `http://localhost:8080/`.

## Configuration

Create `tgfs.yml` in the working directory:

```yaml
port: 8080
channels:
  - name: Audiobooks
    archive_view: directory
  - name: Photos
  - name: ROMs
    archive_view: file_and_directory
```

### `archive_view`

Controls how `.zip` files are exposed:

| Value                | Behavior                                          |
|----------------------|---------------------------------------------------|
| `file` *(default)*   | Raw download only                                 |
| `directory`          | Browse archive contents only (raw download → 404) |
| `file_and_directory` | Both raw download and browsable contents          |

### Message caption overrides

Single-message documents (not grouped albums) support caption directives:

- `name: path/to/file.ext` — override the filename and/or place the file under a virtual directory.
- `type: file|media|zip` — override auto-classification. `media` forces inline playback; `file` forces download; `zip` enables archive indexing.
