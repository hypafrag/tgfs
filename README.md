# tgfs

Read-only HTTP and FUSE filesystem backed by Telegram channels. Indexes document messages in configured channels and exposes them as a browsable directory tree via a web interface or a local mount point, with streaming downloads and in-browser media playback.

## Features

- **Directory listing** — Apache-style HTML index with file sizes. `GET /` lists channels, `GET /{channel}/` lists files.
- **Virtual paths** — messages with `path: dir/sub/file.ext` in the caption place files under virtual directories. A trailing `/` (e.g. `path: dir/sub/`) keeps the original document filename.
- **ZIP browsing** — archive contents are browsable as directories. Only the central directory is fetched at index time; inner files are extracted on demand via ranged downloads and DEFLATE inflation.
- **Multipart files** — opt-in per channel. Either auto-merge documents named `<base>.00`, `<base>.01`, … or merge every part of an album that carries a `multipart:` caption. See [`multipart_policy`](#multipart_policy).
- **HTTP Range requests** — seeking works in browsers and media players for single files, multipart concatenations, and inner-archive entries.
- **Media playback** — audio and video files are served with inline `Content-Disposition` for in-browser playback.
- **FUSE mount** — mount channels as a local read-only filesystem (macOS/Linux) for use with standard file tools. Archive entries preserve their original Unix permissions (Info-ZIP `external file attributes`) when available.
- **TV-show layout** — per-channel [`tvshow_pattern`](#tvshow_pattern) reroutes episodes into a Plex/Jellyfin-style `Show/Season/Episode` tree, with a fallback chain that gracefully drops the episode title when hunch can't recover one.
- **[`tgup`](#tgup--uploader-cli) uploader** — companion CLI for pushing local files into a tgfs-indexed channel. Handles single files, recursive directory walks, Telegram-album grouping, ffmpeg re-encoding with auto-thumbnails, and a TV-show grouping mode for season uploads.
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

### `multipart_policy`

Controls whether and how separate Telegram messages get fused into one logical file. Per-channel; defaults to `none`.

```yaml
channels:
  - name: Releases
    multipart_policy: suffix
  - name: Backups
    multipart_policy: album
```

| Value              | Behavior                                                                                                                            |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `none` *(default)* | No merging. Each message is its own file.                                                                                           |
| `suffix`           | Documents whose filenames match `<base>.00`, `<base>.01`, … (two-digit, contiguous, starting at `.00`) are merged into `<base>`.    |
| `album`            | Every part of a Telegram album (grouped multi-file upload) whose caption carries a `multipart:` directive is concatenated into one. |

**`album` details.** Add `multipart:` (or `multipart: true`) to the caption when you post an album. Parts are concatenated in chronological order (oldest msg_id first). The combined file's path comes from the caption's `path:`:

- `path: vacation.mp4` → merged file appears at channel root as `vacation.mp4`.
- `path: clips/vacation.mp4` → merged file at `clips/vacation.mp4`.
- `path: clips/` (trailing `/`) → directory-only; merged file keeps the **first part's** original document filename, placed under `clips/`.
- no `path:` directive → merged file uses the first part's original document filename, at channel root.

When `multipart:` is *absent* from an album caption, the album behaves as N independent files placed under the optional `path:` directory (see [Message caption overrides](#message-caption-overrides)).

Bare flag-style directives without a value parse as `true`, so `multipart:` and `multipart: true` are equivalent. `false`/`no`/`0` (and any other value) disables it.

### `skip_deflated_id3v1`

Per-channel workaround for audio players that probe the tail of every file for an ID3v1 tag (last 128 bytes, `TAG` magic). For deflate-compressed entries inside a ZIP, serving even a single byte near EOF requires inflating the **entire** compressed stream from the beginning — which can mean downloading and decoding gigabytes just to satisfy a 4 KB probe before playback starts.

```yaml
channels:
  - name: Audiobooks
    archive_view: directory
    skip_deflated_id3v1: true
```

When enabled, the FUSE read handler short-circuits the probe and returns 4 KB of zeros instead of inflating the archive. The check fires only when **all** of these hold (so normal sequential decoding is unaffected):

- The file lives inside a ZIP archive and is compressed with DEFLATE (method 8).
- Its inner path ends in `.mp3` or `.flac` (case-insensitive).
- The read size is exactly 4096 bytes.
- The decoder has produced fewer than 128 KB so far (i.e. this happens right after open).
- The read offset is within 16 KB of EOF.

Since ID3v1 has been superseded by ID3v2 (which sits at the file start and is reachable without seeking), this trades the legacy tag for instant playback start. Defaults to `false`.

### `tvshow_pattern`

Per-channel template that reroutes TV-show episodes from the channel root into a Plex/Jellyfin-style directory tree. Each document name is parsed with [hunch](https://crates.io/crates/hunch) and substituted into the template; the rendered string becomes the file's virtual `<dir>/<filename>`. Files hunch can't decompose are left at their original locations.

```yaml
channels:
  - name: TV Shows
    tvshow_pattern: "{show_title}/Season {season:02}/Episode {episode:02} - {episode_title}.{ext}|{show_title}/Season {season:02}/Episode {episode:02}.{ext}"
```

Placeholders:

| Token              | Source                                   |
|--------------------|------------------------------------------|
| `{show_title}`     | hunch's detected show title              |
| `{episode_title}`  | hunch's detected episode title *(optional — see fallback chain)* |
| `{season}`         | season number (decimal, no padding)      |
| `{season:N}`       | season number, parsed width but unpadded |
| `{season:0N}`      | season number, zero-padded to width N    |
| `{episode}` / `{episode:N}` / `{episode:0N}` | same forms for episode number |
| `{year}` / `{year:N}` / `{year:0N}`          | year if hunch found one     |
| `{ext}`            | original file extension, lowercased      |

**Width specs.** `:02` zero-pads to width 2 (e.g. `5` → `"05"`). `:2` parses the width for validation but emits the raw decimal (e.g. `5` → `"5"`, `12` → `"12"`) — useful when you want to document the expected width without forcing leading zeros.

**Fallback chain.** A pattern string may contain multiple sub-patterns separated by `|`. Sub-patterns are tried left-to-right; the first one whose placeholders all resolve is used. Today only `{episode_title}` is genuinely optional, so the common shape is "with episode title | without":

```text
…Episode {episode:02} - {episode_title}.{ext}|…Episode {episode:02}.{ext}
```

For a file hunch reads as having an episode title (e.g. `s01e01_Days.Gone.Bye.avi`), the left sub-pattern wins and produces `Episode 01 - Days Gone Bye.avi`. For a file where hunch can't recover one (e.g. `s01e06_TS-19.avi`), the renderer falls through and emits `Episode 06.avi`.

### Message caption overrides

Caption directives are recognized in any message text:

- `path: dir/sub/file.ext` — *(single-file messages only)* override the full virtual path. Both directory and filename are taken from the value.
- `path: dir/sub/` — directory-only override. The original document filename is kept; the value (with the trailing slash stripped) becomes the parent directory. `path: /` is treated the same as omitting the directive.
- `type: file|media|zip` — override auto-classification. `media` forces inline playback; `file` forces download; `zip` enables archive indexing.

**Grouped albums (multi-file uploads)** behave differently because a single caption is attached to the whole group:

- `path:` is **always treated as a directory** for every part of the album, with or without a trailing slash. The original document filenames are preserved. So `path: vacation` and `path: vacation/` are equivalent on an album and place every part under `vacation/`.
- `type:` applies uniformly to every part of the album. `type: media` is the most common — e.g. a photo album captioned `path: vacation/`, where every photo becomes inline-playable under `vacation/`.
- `type: zip` does **not** retroactively fetch archive indexes for parts whose underlying file wasn't already classified as a zip by MIME or `.zip` extension. Use the per-file caption (on a single-message upload) when you need browsable archives.
- `multipart:` flips the album into a **single concatenated file** (requires `multipart_policy: album` on the channel). In that mode `path:` uses single-message semantics again: trailing `/` is directory-only, otherwise the value is the full path of the merged file. See [`multipart_policy`](#multipart_policy).

**Flag-style directives.** Any caption directive without a value (`key:`) parses as `key: true`. Currently only `multipart:` uses this form.

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

### `max_fetches_total`

Limits how many concurrent Telegram downloads may be in-flight across **all** processes combined. Complements `max_fetches_per_pid`: the total cap is the outer bound regardless of how many different PIDs are active.

```yaml
max_fetches_total: 8
```

Omit or leave unset for unlimited concurrency (default).

## tgup — uploader CLI

`tgup` is a general-purpose Telegram uploader for pushing local files into a channel/group/chat. It pairs naturally with tgfs (the binary, configs, and session live in the same workspace), but nothing about it is tgfs-specific — anything you upload is just a regular Telegram message.

Build from the same workspace (`cargo build --release --bin tgup`). On first use it prompts for an SMS code and stores its authenticated session at `~/.config/tgfs/session.sqlite3`; subsequent runs are non-interactive.

```bash
# Upload one file to a named channel
tgup -c MyChannel path/to/file.mkv

# Omit -c to pick the destination interactively. Channels declared in the
# config file are listed at the top, followed by every other dialog this
# account can send to.
tgup path/to/file.mkv

# Walk a directory and upload every contained file as flat siblings
tgup -c Backups -d recursive ./backups/

# Walk a directory but turn each file's parent dir into a `path:` caption,
# so a tgfs-indexed channel reconstructs the local tree
tgup -c Photos -d caption ./trip/

# Group consecutive same-caption files into Telegram albums (≤10 per album)
tgup -c Photos -a ./trip/*.jpg

# Re-encode video files with ffmpeg (uses `ffmpeg.encode_args` from the
# config) and attach a generated thumbnail to each uploaded video
tgup -c HomeVideos --encode-video clip.mov

# Dry-run: print the plan and exit without uploading
tgup -c Photos --dry-run ./trip/
```

Most flags compose: `-d recursive` works alongside `-a`, `--encode-video`, etc. Mutually-exclusive combinations are listed under `tgup --help`.

### Modes

| Flag             | Behavior                                                                                                              |
|------------------|-----------------------------------------------------------------------------------------------------------------------|
| *(none)*         | Each argument is uploaded as its own message.                                                                         |
| `-d recursive`   | Directory args are walked; every contained file is uploaded individually.                                             |
| `-d caption`     | Same as `recursive`, but each file's caption carries `path: <relative dir>/` so tgfs reconstructs the local tree.     |
| `-a / --album`   | Consecutive uploadable files sharing a caption are grouped into Telegram albums (≤10 each).                           |
| `--encode-video` | Videos are streamed through ffmpeg before upload; an auto-generated thumbnail is attached to each one.                |
| `--tvshow`       | Filenames are parsed as TV-show episodes, renamed `<Title> S##E##[ - <Episode Title>].<ext>`, and grouped per-season into albums. See below. |
| `--dry-run`      | Print the upload plan and exit. Composes with any of the above.                                                       |

### `--tvshow`

For libraries of named episodes. Each input file is parsed with [hunch](https://crates.io/crates/hunch) from its **path relative to the arg** (so ancestor directories contribute to the parse), renamed to `<Title> S##E## - <Episode Title>.<ext>` (or the bare `<Title> S##E##.<ext>` when no episode title is recovered), sorted by `(title, season, episode)`, and grouped into per-season Telegram albums of up to 10 items each. Larger seasons split evenly: 11 episodes → albums of 6+5; 21 → 7+7+7. The album caption is `<Title> S## E##-E##` (range) or `<Title> S##E##` (single).

```bash
tgup -c "TV Shows" --tvshow -d recursive "The.Walking.Dead.bdrip_[teko]"
```

Pair with the tgfs [`tvshow_pattern`](#tvshow_pattern) channel option to expose these uploads as a Plex-style tree. Constraints: non-empty extensions only, files >4 GiB rejected, mutually exclusive with `-a`, `--encode-video`, `-d caption`, and `-d zip`.

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
