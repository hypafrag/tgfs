# tgfs

Read-only HTTP and FUSE filesystem backed by Telegram channels. Indexes document messages in configured channels and exposes them as a browsable directory tree via a web interface or a local mount point, with streaming downloads and in-browser media playback.

## Features

- **Directory listing** — Apache-style HTML index with file sizes. `GET /` lists channels, `GET /{channel}/` lists files.
- **Virtual paths** — messages with `name: dir/sub/file.ext` in the caption place files under virtual directories.
- **ZIP browsing** — archive contents are browsable as directories. Only the central directory is fetched at index time; inner files are extracted on demand via ranged downloads and DEFLATE inflation.
- **Multipart files** — documents named `<base>.00`, `<base>.01`, … are auto-merged into a single logical file with seamless streaming.
- **HTTP Range requests** — seeking works in browsers and media players for single files, multipart concatenations, and inner-archive entries.
- **Media playback** — audio and video files are served with inline `Content-Disposition` for in-browser playback.
- **FUSE mount** — mount channels as a local read-only filesystem (macOS/Linux) for use with standard file tools. Archive entries preserve their original Unix permissions (Info-ZIP `external file attributes`) when available.
- **MIME interning** — MIME type strings are pooled across the index to reduce memory on large channels.

## Requirements

- Rust 2024 edition
- A Telegram API ID and API hash from [my.telegram.org/apps](https://my.telegram.org/apps)

## Quick start

```bash
# Build
cargo build --release

# Configure (see below), then run
cargo run --release
```

On first run the binary will prompt for your SMS code and optional 2FA password. The session is persisted in `session.sqlite3`.

Once running, browse `http://localhost:8080/`.

## Configuration

Create `tgfs.yml` in the working directory (or pass `--config <path>`):

```yaml
api_id: 12345678        # from https://my.telegram.org/apps
api_hash: abc123...     # from https://my.telegram.org/apps
phone: "+12345678900"   # your Telegram phone number

http_port: 8080         # optional — serve HTTP index on this port
mount_at: /mnt/tgfs     # optional — mount FUSE filesystem at this path
channels:
  - name: Audiobooks
    archive_view: directory
  - name: Photos
  - name: ROMs
    archive_view: file_and_directory
```

At least one of `http_port` or `mount_at` must be set; both can run simultaneously.

`$VAR` and `${VAR}` references in the config file are expanded from the environment, so credentials can be kept out of the file:

```yaml
api_id: ${TG_API_ID}
api_hash: ${TG_API_HASH}
phone: "${TG_PHONE}"
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

### Proxy

#### SOCKS5

```yaml
proxy:
  host: proxy.example.com
  port: 1080
  # optional credentials
  user: alice
  password: secret
```

#### MTProxy

```yaml
proxy:
  type: mtproxy
  host: proxy.example.com
  port: 443
  secret: "dd1234567890abcdef1234567890abcdef"
```

The secret is the hex-encoded 16-byte proxy secret. A `dd` prefix (FakeTLS marker) is accepted and stripped automatically. tgfs starts a local SOCKS5 bridge to the MTProxy server and routes all Telegram traffic through it.

### Logging

The default log level is `info`. Override with a `log:` key in `tgfs.yml`:

```yaml
# global level
log: debug
```

```yaml
# per-module levels
log:
  tgfs: debug
  grammers_mtsender: warn
```

Valid levels: `error`, `warn`, `info`, `debug`, `trace`.

The `RUST_LOG` environment variable takes precedence over `log:` when set.

### `max_fetches_per_pid`

Limits how many concurrent Telegram downloads a single process (PID) may have in-flight through the FUSE mount. When the limit is reached, new `read()` calls block until an in-flight fetch completes. File opens are never blocked.

```yaml
max_fetches_per_pid: 3
```

Omit or leave unset for unlimited concurrency (default).

## Docker

A multi-stage `Dockerfile` is provided. The runtime image is based on `debian:bookworm-slim` and contains only the stripped binary plus `ca-certificates` and `libfuse2` — no Rust toolchain or build artifacts.

```bash
docker build -t tgfs .
```

The container reads its config and persistent state from `/data`. Mount your `tgfs.yml` and `session.sqlite3` there:

### HTTP only

```bash
docker run --rm -it \
  -v $PWD/tgfs.yml:/data/tgfs.yml \
  -v $PWD/session.sqlite3:/data/session.sqlite3 \
  -p 8080:8080 \
  tgfs
```

On first run, omit `-v` for `session.sqlite3` and use `-it` so you can complete interactive sign-in; the file will be created in `/data` and you can persist it on subsequent runs.

### With FUSE mount

FUSE inside a container needs `/dev/fuse`, the `SYS_ADMIN` capability, and a bind-mounted target directory with shared propagation so the mount is visible on the host:

```bash
docker run --rm -it \
  --cap-add SYS_ADMIN \
  --device /dev/fuse \
  --security-opt apparmor:unconfined \
  -v $PWD/tgfs.yml:/data/tgfs.yml \
  -v $PWD/session.sqlite3:/data/session.sqlite3 \
  -v $PWD/mnt:/mnt/tgfs:rshared \
  -p 8080:8080 \
  tgfs
```

With `mount_at: /mnt/tgfs` and `http_port: 8080` both set in `tgfs.yml`, the container serves both the HTTP index and the FUSE mount in one process.

### Custom config path

```bash
docker run --rm -it -v $PWD/my.yml:/data/my.yml tgfs --config /data/my.yml
```
