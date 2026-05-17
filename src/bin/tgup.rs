//! `tgup` — upload files and directories to a tgfs-managed Telegram channel.
//!
//! Reads the same `tgfs.yml` as the daemon, resolves the target channel from
//! `--channel`, and walks positional arguments according to `--dir`. Builds the
//! complete execution plan offline; only after validation does it connect to
//! Telegram and start uploading.

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{ExitCode, Stdio};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _};
use grammers_client::Client;
use grammers_client::media::{InputMedia, Uploaded};
use grammers_client::message::InputMessage;
use grammers_client::peer::Peer;
use grammers_session::types::PeerRef;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, ReadBuf};
use tokio::process::Command;

use tgfs::config::{self, ChannelEntry, Config, FfmpegConfig, MultipartPolicy};
use tgfs::login::connect_and_authorize_with_session;

const PART_MAX: u64 = 4 * 1024 * 1024 * 1024; // 4 GiB per Telegram message

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DirMode {
    Skip,
    Recursive,
    Caption,
    Zip,
}

struct Args {
    /// Explicit `--config <path>`. When `None` we fall back to
    /// `~/.config/tgfs/tgfs.yml`.
    config_path: Option<String>,
    channel: String,
    dir_mode: DirMode,
    dry_run: bool,
    encode_video: bool,
    paths: Vec<PathBuf>,
}

/// Path to `~/.config/tgfs/`. Falls back to the current directory when `HOME`
/// isn't set (unusual environments / containers).
fn default_config_dir() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        let mut p = PathBuf::from(home);
        p.push(".config");
        p.push("tgfs");
        return p;
    }
    PathBuf::from(".")
}

fn default_config_path() -> PathBuf {
    default_config_dir().join("tgfs.yml")
}

fn default_session_path() -> PathBuf {
    default_config_dir().join("session.sqlite3")
}

fn parse_dir_mode(s: &str) -> anyhow::Result<DirMode> {
    match s {
        "skip" => Ok(DirMode::Skip),
        "recursive" => Ok(DirMode::Recursive),
        "caption" => Ok(DirMode::Caption),
        "zip" => Ok(DirMode::Zip),
        _ => Err(anyhow!("invalid --dir mode '{}'; expected skip|recursive|caption|zip", s)),
    }
}

fn print_usage() {
    eprintln!(
        "tgup — upload files to a tgfs-indexed Telegram channel\n\n\
         Usage:\n  \
           tgup [--config <path>] -c <channel> [-d <mode>] [--encode-video] [--dry-run] <path>...\n\n\
         Options:\n  \
           -c, --channel <name>   Target channel name (must exist in config).\n  \
           -d, --dir <mode>       How to handle directory arguments:\n                            \
             skip       — error on directories (default)\n                            \
             recursive  — upload contained files as a flat list\n                            \
             caption    — like recursive, but each file's caption sets\n                                         \
             `path: <relative dir>/` so the tree is recreated\n                            \
             zip        — not implemented (exits with error)\n  \
           --encode-video         Re-encode video files with ffmpeg (using\n                          \
             ffmpeg.encode_args from the config) and attach\n                          \
             an ffmpeg-generated thumbnail to each uploaded\n                          \
             video. Encoded data is piped from ffmpeg —\n                          \
             never written to a temporary file. Requires\n                          \
             ffmpeg on PATH.\n  \
           --dry-run              Print the plan and exit.\n  \
           --config <path>        Config file (default:\n                          \
             ~/.config/tgfs/tgfs.yml). The auth session is\n                          \
             also stored next to the default config at\n                          \
             ~/.config/tgfs/session.sqlite3."
    );
}

fn parse_args() -> anyhow::Result<Args> {
    let mut args = std::env::args().skip(1);
    let mut config_path: Option<String> = None;
    let mut channel: Option<String> = None;
    let mut dir_mode = DirMode::Skip;
    let mut dry_run = false;
    let mut encode_video = false;
    let mut paths: Vec<PathBuf> = Vec::new();
    while let Some(a) = args.next() {
        match a.as_str() {
            "-h" | "--help" => { print_usage(); std::process::exit(0); }
            "--config" => {
                config_path = Some(args.next().ok_or_else(|| anyhow!("--config requires a path"))?);
            }
            "-c" | "--channel" => {
                channel = Some(args.next().ok_or_else(|| anyhow!("-c/--channel requires a value"))?);
            }
            "-d" | "--dir" => {
                let v = args.next().ok_or_else(|| anyhow!("-d/--dir requires a value"))?;
                dir_mode = parse_dir_mode(&v)?;
            }
            "--dry-run" => dry_run = true,
            "--encode-video" => encode_video = true,
            other if other.starts_with("--channel=") => {
                channel = Some(other.trim_start_matches("--channel=").to_string());
            }
            other if other.starts_with("--dir=") => {
                dir_mode = parse_dir_mode(other.trim_start_matches("--dir="))?;
            }
            other if other.starts_with("--config=") => {
                config_path = Some(other.trim_start_matches("--config=").to_string());
            }
            other if other.starts_with('-') => bail!("unknown option: {}", other),
            _ => paths.push(PathBuf::from(a)),
        }
    }
    let channel = channel.ok_or_else(|| anyhow!("-c/--channel is required"))?;
    if paths.is_empty() { bail!("at least one file or directory path is required"); }
    Ok(Args { config_path, channel, dir_mode, dry_run, encode_video, paths })
}

/// Split a config string into argv-style tokens. Honors double and single
/// quotes; backslash escapes the next character (outside single quotes). The
/// surrounding quote characters are stripped from the produced tokens.
fn split_shell_args(s: &str) -> anyhow::Result<Vec<String>> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_dq = false;
    let mut in_sq = false;
    let mut escape = false;
    let mut had_token = false;
    for c in s.chars() {
        if escape {
            cur.push(c);
            escape = false;
            had_token = true;
            continue;
        }
        match c {
            '\\' if !in_sq => { escape = true; }
            '"' if !in_sq => { in_dq = !in_dq; had_token = true; }
            '\'' if !in_dq => { in_sq = !in_sq; had_token = true; }
            c if c.is_whitespace() && !in_dq && !in_sq => {
                if had_token {
                    out.push(std::mem::take(&mut cur));
                    had_token = false;
                }
            }
            c => { cur.push(c); had_token = true; }
        }
    }
    if in_dq || in_sq { bail!("unbalanced quote in ffmpeg args: '{}'", s); }
    if had_token { out.push(cur); }
    Ok(out)
}

fn ffmpeg_in_path() -> bool {
    let path = match std::env::var_os("PATH") { Some(v) => v, None => return false };
    for p in std::env::split_paths(&path) {
        for name in ["ffmpeg", "ffmpeg.exe"] {
            let cand = p.join(name);
            if cand.is_file() { return true; }
        }
    }
    false
}

fn is_video_path(path: &Path) -> bool {
    mime_guess::from_path(path)
        .first()
        .map(|m| m.type_() == mime_guess::mime::VIDEO)
        .unwrap_or(false)
}

fn replace_ext(name: &str, new_ext: &str) -> String {
    let stem = match name.rfind('.') {
        Some(i) => &name[..i],
        None => name,
    };
    if stem.is_empty() { format!(".{}", new_ext) } else { format!("{}.{}", stem, new_ext) }
}

/// A single Telegram document upload (one message).
#[derive(Clone)]
struct UploadPart {
    /// Where to read bytes from.
    src: PartSource,
    offset: u64,
    size: u64,
    /// Filename sent to Telegram (`DocumentAttributeFilename`).
    doc_filename: String,
    /// Caption attached to this part's message.
    caption: String,
}

#[derive(Clone)]
enum PartSource {
    File(PathBuf),
    /// Buffer produced at upload time (e.g. ffmpeg-encoded video).
    Buffer(Arc<Vec<u8>>),
}

/// One logical entry of the upload plan.
#[derive(Clone)]
enum UploadItem {
    /// One message — file <= 4 GiB.
    Single(UploadPart),
    /// File > 4 GiB, channel policy = `suffix`. N independent messages whose
    /// document filenames carry `.NN`. Only `parts[0]` has a `path:` caption.
    SuffixParts { display: String, parts: Vec<UploadPart> },
    /// File > 4 GiB, channel policy = `album`. One Telegram album of N parts
    /// that share `multipart:` + `path:` directives in the caption.
    AlbumParts { display: String, parts: Vec<UploadPart> },
    /// Video that must be re-encoded by ffmpeg before upload. The encoded
    /// stream's size is unknown at plan time — multipart-splitting is deferred
    /// until the encode finishes at upload time.
    EncodedVideo {
        source: PathBuf,
        /// Output filename (extension rewritten to `.mp4`).
        doc_filename: String,
        /// Full virtual path used inside `path:` captions; matches doc_filename
        /// when no relative directory applies.
        virtual_path: String,
        /// Directory-only caption fragment (`path: <rel_dir>/`) used when the
        /// encoded stream stays a single message under `caption` dir mode.
        rel_dir: String,
        /// Channel multipart policy — used to choose suffix vs album if the
        /// encoded stream still ends up exceeding 4 GiB.
        policy: MultipartPolicy,
        /// Source size — for plan-time display only; the actual upload size
        /// comes from ffmpeg's stdout.
        source_size: u64,
    },
}

impl UploadItem {
    fn display_name(&self) -> &str {
        match self {
            UploadItem::Single(p) => &p.doc_filename,
            UploadItem::SuffixParts { display, .. }
            | UploadItem::AlbumParts { display, .. } => display.as_str(),
            UploadItem::EncodedVideo { doc_filename, .. } => doc_filename.as_str(),
        }
    }
    /// Bytes used to size the aggregate progress bar. For encoded videos we
    /// don't know the encoded size yet — use the source size as a rough
    /// estimate; the bar's length is adjusted once encoding completes.
    fn planned_bytes(&self) -> u64 {
        match self {
            UploadItem::Single(p) => p.size,
            UploadItem::SuffixParts { parts, .. }
            | UploadItem::AlbumParts { parts, .. } => parts.iter().map(|p| p.size).sum(),
            UploadItem::EncodedVideo { source_size, .. } => *source_size,
        }
    }
}

fn plan_one_file(
    abs_path: &Path,
    original_name: &str,
    rel_dir: &str,
    size: u64,
    policy: MultipartPolicy,
    dir_mode: DirMode,
    encode_video: bool,
) -> anyhow::Result<UploadItem> {
    let is_video = is_video_path(abs_path);
    let virtual_path_of = |name: &str| -> String {
        if rel_dir.is_empty() { name.to_string() } else { format!("{}/{}", rel_dir, name) }
    };
    let needs_caption_path = dir_mode == DirMode::Caption && !rel_dir.is_empty();

    if encode_video && is_video {
        let doc_filename = replace_ext(original_name, "mp4");
        let virtual_path = virtual_path_of(&doc_filename);
        return Ok(UploadItem::EncodedVideo {
            source: abs_path.to_path_buf(),
            doc_filename,
            virtual_path,
            rel_dir: rel_dir.to_string(),
            policy,
            source_size: size,
        });
    }

    if size <= PART_MAX {
        let caption = if needs_caption_path {
            format!("path: {}/", rel_dir)
        } else {
            String::new()
        };
        return Ok(UploadItem::Single(UploadPart {
            src: PartSource::File(abs_path.to_path_buf()),
            offset: 0,
            size,
            doc_filename: original_name.to_string(),
            caption,
        }));
    }

    if policy == MultipartPolicy::None {
        bail!(
            "file '{}' is {} bytes (> 4 GiB) but the target channel's multipart_policy is `none`",
            abs_path.display(), size
        );
    }

    let virtual_path = virtual_path_of(original_name);
    let parts = build_multipart_parts(
        PartSource::File(abs_path.to_path_buf()),
        original_name,
        &virtual_path,
        size,
        policy,
    );

    Ok(match policy {
        MultipartPolicy::Suffix => UploadItem::SuffixParts { display: original_name.to_string(), parts },
        MultipartPolicy::Album => UploadItem::AlbumParts { display: original_name.to_string(), parts },
        MultipartPolicy::None => unreachable!(),
    })
}

/// Slice `size` bytes drawn from `source` into `.NN`-suffixed `UploadPart`s
/// per `policy`. The first part's caption fixes the assembled file's name.
fn build_multipart_parts(
    source: PartSource,
    base_name: &str,
    virtual_path: &str,
    size: u64,
    policy: MultipartPolicy,
) -> Vec<UploadPart> {
    let mut parts: Vec<UploadPart> = Vec::new();
    let mut offset: u64 = 0;
    let mut idx: usize = 0;
    while offset < size {
        let chunk = std::cmp::min(PART_MAX, size - offset);
        let part_name = format!("{}.{:02}", base_name, idx);
        let caption = match policy {
            MultipartPolicy::Suffix => {
                if idx == 0 { format!("path: {}", virtual_path) } else { String::new() }
            }
            MultipartPolicy::Album => {
                format!("multipart:\npath: {}", virtual_path)
            }
            MultipartPolicy::None => unreachable!(),
        };
        parts.push(UploadPart {
            src: source.clone(),
            offset,
            size: chunk,
            doc_filename: part_name,
            caption,
        });
        offset += chunk;
        idx += 1;
    }
    parts
}

/// Walk one positional argument and accumulate plan items.
fn collect_path(
    arg: &Path,
    cwd: &Path,
    policy: MultipartPolicy,
    dir_mode: DirMode,
    encode_video: bool,
    out: &mut Vec<UploadItem>,
) -> anyhow::Result<()> {
    let meta = std::fs::metadata(arg)
        .with_context(|| format!("can't stat '{}'", arg.display()))?;
    if meta.is_file() {
        let abs = arg.canonicalize()
            .with_context(|| format!("can't canonicalize '{}'", arg.display()))?;
        let name = abs.file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("filename of '{}' is not valid UTF-8", abs.display()))?
            .to_string();
        let rel_dir = match dir_mode {
            DirMode::Caption => {
                let parent = abs.parent().unwrap_or_else(|| Path::new(""));
                relative_dir_from_cwd(parent, cwd)?
            }
            _ => String::new(),
        };
        let item = plan_one_file(&abs, &name, &rel_dir, meta.len(), policy, dir_mode, encode_video)?;
        out.push(item);
        return Ok(());
    }
    if meta.is_dir() {
        match dir_mode {
            DirMode::Skip => bail!(
                "directory '{}' given but --dir mode is `skip`; pass -d recursive|caption|zip",
                arg.display()
            ),
            DirMode::Zip => bail!("--dir zip: not implemented"),
            DirMode::Recursive | DirMode::Caption => {
                collect_dir(arg, cwd, policy, dir_mode, encode_video, out)?;
                return Ok(());
            }
        }
    }
    bail!("'{}' is neither a file nor a directory", arg.display());
}

fn collect_dir(
    dir: &Path,
    cwd: &Path,
    policy: MultipartPolicy,
    dir_mode: DirMode,
    encode_video: bool,
    out: &mut Vec<UploadItem>,
) -> anyhow::Result<()> {
    let mut stack: Vec<PathBuf> = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let entries = std::fs::read_dir(&d)
            .with_context(|| format!("can't read directory '{}'", d.display()))?;
        for e in entries {
            let e = e?;
            let p = e.path();
            let m = e.metadata()?;
            if m.is_dir() {
                stack.push(p);
            } else if m.is_file() {
                let abs = p.canonicalize()
                    .with_context(|| format!("can't canonicalize '{}'", p.display()))?;
                let name = abs.file_name()
                    .and_then(|s| s.to_str())
                    .ok_or_else(|| anyhow!("filename of '{}' is not valid UTF-8", abs.display()))?
                    .to_string();
                let rel_dir = match dir_mode {
                    DirMode::Caption => {
                        let parent = abs.parent().unwrap_or_else(|| Path::new(""));
                        relative_dir_from_cwd(parent, cwd)?
                    }
                    _ => String::new(),
                };
                let item = plan_one_file(&abs, &name, &rel_dir, m.len(), policy, dir_mode, encode_video)?;
                out.push(item);
            }
        }
    }
    Ok(())
}

/// Compute `parent`'s path relative to `cwd`, both canonicalized. Fails when
/// `parent` is not under `cwd`. Returns empty string when `parent == cwd`.
fn relative_dir_from_cwd(parent: &Path, cwd: &Path) -> anyhow::Result<String> {
    let p = if parent.is_absolute() {
        parent.to_path_buf()
    } else {
        parent.canonicalize()
            .with_context(|| format!("can't canonicalize '{}'", parent.display()))?
    };
    let rel = p.strip_prefix(cwd).map_err(|_| {
        anyhow!(
            "path '{}' is outside the current working directory '{}' (would require '..')",
            p.display(), cwd.display()
        )
    })?;
    Ok(rel.to_string_lossy().replace('\\', "/"))
}

fn find_channel<'a>(config: &'a Config, name: &str) -> Option<&'a ChannelEntry> {
    config.channels.iter().find(|c| c.name == name)
}

fn print_plan(plan: &[UploadItem], channel: &str, encode_video: bool) {
    let total: u64 = plan.iter().map(|i| i.planned_bytes()).sum();
    println!("Target channel: {}", channel);
    if encode_video {
        println!("--encode-video: video files will be re-encoded by ffmpeg (sizes below are source bytes)");
    }
    println!("Items: {}, total source bytes: {}", plan.len(), total);
    for (i, item) in plan.iter().enumerate() {
        match item {
            UploadItem::Single(p) => {
                let from = match &p.src {
                    PartSource::File(pb) => pb.display().to_string(),
                    PartSource::Buffer(_) => "<buffer>".to_string(),
                };
                println!("  [{}] {} — {} bytes (from '{}' offset {})",
                    i, p.doc_filename, p.size, from, p.offset);
                if !p.caption.is_empty() {
                    for line in p.caption.lines() {
                        println!("        caption: {}", line);
                    }
                }
            }
            UploadItem::SuffixParts { display, parts } => {
                println!("  [{}] {} — suffix multipart, {} parts, {} bytes",
                    i, display, parts.len(), parts.iter().map(|p| p.size).sum::<u64>());
                for (j, p) in parts.iter().enumerate() {
                    println!("        part .{:02}: {} bytes (offset {})", j, p.size, p.offset);
                    if !p.caption.is_empty() {
                        for line in p.caption.lines() {
                            println!("                  caption: {}", line);
                        }
                    }
                }
            }
            UploadItem::AlbumParts { display, parts } => {
                println!("  [{}] {} — album multipart, {} parts, {} bytes",
                    i, display, parts.len(), parts.iter().map(|p| p.size).sum::<u64>());
                if let Some(c) = parts.first() {
                    for line in c.caption.lines() {
                        println!("        album caption: {}", line);
                    }
                }
                for (j, p) in parts.iter().enumerate() {
                    println!("        part .{:02}: {} bytes (offset {})", j, p.size, p.offset);
                }
            }
            UploadItem::EncodedVideo { source, doc_filename, virtual_path, rel_dir, source_size, .. } => {
                println!("  [{}] {} — re-encode from '{}' ({} source bytes); thumbnail generated",
                    i, doc_filename, source.display(), source_size);
                if !rel_dir.is_empty() {
                    println!("        target virtual path: {}", virtual_path);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// progress + read wrappers
// ---------------------------------------------------------------------------

struct ProgressReader<R> {
    inner: R,
    file_pb: ProgressBar,
    total_pb: ProgressBar,
}

impl<R: AsyncRead + Unpin> AsyncRead for ProgressReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let r = Pin::new(&mut self.inner).poll_read(cx, buf);
        let after = buf.filled().len();
        let delta = (after - before) as u64;
        if delta > 0 {
            self.file_pb.inc(delta);
            self.total_pb.inc(delta);
        }
        r
    }
}

fn set_bar_style(pb: &ProgressBar) {
    pb.disable_steady_tick();
    pb.set_style(
        ProgressStyle::with_template(
            "  {msg:40!} [{bar:30.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
        )
        .unwrap()
        .progress_chars("=>-"),
    );
}

fn set_spinner_style(pb: &ProgressBar) {
    pb.set_style(
        ProgressStyle::with_template("  {msg:40!} {spinner:.cyan} {bytes} ({bytes_per_sec})")
            .unwrap(),
    );
    pb.enable_steady_tick(Duration::from_millis(120));
}

// ---------------------------------------------------------------------------
// upload helpers (operating on UploadPart)
// ---------------------------------------------------------------------------

/// Open the byte stream for a part as an `AsyncRead` limited to `part.size`.
async fn open_part_stream(part: &UploadPart) -> anyhow::Result<Box<dyn AsyncRead + Send + Unpin>> {
    match &part.src {
        PartSource::File(path) => {
            let mut f = File::open(path)
                .await
                .with_context(|| format!("opening '{}'", path.display()))?;
            if part.offset > 0 {
                f.seek(SeekFrom::Start(part.offset)).await?;
            }
            Ok(Box::new(f.take(part.size)))
        }
        PartSource::Buffer(buf) => {
            let start = part.offset as usize;
            let end = start + part.size as usize;
            let slice = buf.clone();
            let cursor = SliceReader { buf: slice, start, end, pos: start };
            Ok(Box::new(cursor))
        }
    }
}

/// AsyncRead view over an `Arc<Vec<u8>>` slice. Used so multiple parts that
/// reference different ranges of the same encoded buffer can stream
/// independently without copying.
struct SliceReader {
    buf: Arc<Vec<u8>>,
    start: usize,
    end: usize,
    pos: usize,
}

impl AsyncRead for SliceReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let _ = self.start;
        let remaining = self.end.saturating_sub(self.pos);
        if remaining == 0 { return Poll::Ready(Ok(())); }
        let n = std::cmp::min(remaining, out.remaining());
        out.put_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Poll::Ready(Ok(()))
    }
}

async fn upload_part_as_message(
    client: &Client,
    peer: PeerRef,
    part: &UploadPart,
    thumb: Option<&Uploaded>,
    file_pb: &ProgressBar,
    total_pb: &ProgressBar,
) -> anyhow::Result<()> {
    set_bar_style(file_pb);
    file_pb.set_length(part.size);
    file_pb.set_position(0);
    file_pb.set_message(part.doc_filename.clone());

    let stream = open_part_stream(part).await?;
    let mut reader = ProgressReader {
        inner: stream,
        file_pb: file_pb.clone(),
        total_pb: total_pb.clone(),
    };
    let uploaded = client
        .upload_stream(&mut reader, part.size as usize, part.doc_filename.clone())
        .await
        .with_context(|| format!("uploading '{}'", part.doc_filename))?;

    let mut msg = InputMessage::new().text(part.caption.clone()).file(uploaded);
    if let Some(t) = thumb {
        msg = msg.thumbnail(t.clone());
    }
    client
        .send_message(peer, msg)
        .await
        .with_context(|| format!("sending message for '{}'", part.doc_filename))?;
    Ok(())
}

async fn upload_album(
    client: &Client,
    peer: PeerRef,
    parts: &[UploadPart],
    thumb: Option<&Uploaded>,
    file_pb: &ProgressBar,
    total_pb: &ProgressBar,
) -> anyhow::Result<()> {
    let mut medias: Vec<InputMedia> = Vec::with_capacity(parts.len());
    for part in parts {
        set_bar_style(file_pb);
        file_pb.set_length(part.size);
        file_pb.set_position(0);
        file_pb.set_message(part.doc_filename.clone());

        let stream = open_part_stream(part).await?;
        let mut reader = ProgressReader {
            inner: stream,
            file_pb: file_pb.clone(),
            total_pb: total_pb.clone(),
        };
        let uploaded = client
            .upload_stream(&mut reader, part.size as usize, part.doc_filename.clone())
            .await
            .with_context(|| format!("uploading album part '{}'", part.doc_filename))?;
        let mut media = InputMedia::new().caption(part.caption.clone()).file(uploaded);
        if let Some(t) = thumb {
            media = media.thumbnail(t.clone());
        }
        medias.push(media);
    }
    client.send_album(peer, medias).await.context("sending album")?;
    Ok(())
}

async fn resolve_channel_peer(client: &Client, channel_name: &str) -> anyhow::Result<PeerRef> {
    let mut dialogs = client.iter_dialogs();
    while let Some(dialog) = dialogs.next().await? {
        if let Peer::Channel(ch) = dialog.peer() {
            if ch.title() == channel_name {
                if let Some(r) = ch.to_ref().await {
                    return Ok(r);
                }
            }
        }
    }
    Err(anyhow!("channel '{}' not found among your dialogs", channel_name))
}

// ---------------------------------------------------------------------------
// ffmpeg invocations (encode + thumbnail)
// ---------------------------------------------------------------------------

/// Run ffmpeg with the given args, capturing all of stdout into memory. The
/// `progress_pb` is updated as bytes arrive on stdout. ffmpeg's stderr is
/// captured into a string and surfaced on failure.
async fn run_ffmpeg_to_buffer(
    input: &Path,
    extra_args: &[String],
    output_format_args: &[&str],
    progress_pb: &ProgressBar,
    progress_label: &str,
) -> anyhow::Result<Vec<u8>> {
    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-y").arg("-nostdin")
        .arg("-loglevel").arg("error")
        .arg("-i").arg(input);
    for a in extra_args { cmd.arg(a); }
    for a in output_format_args { cmd.arg(a); }
    cmd.arg("pipe:1");
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().context("failed to spawn ffmpeg")?;
    let mut stdout = child.stdout.take().expect("piped");
    let mut stderr = child.stderr.take().expect("piped");

    set_spinner_style(progress_pb);
    progress_pb.set_position(0);
    progress_pb.set_message(progress_label.to_string());

    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let n = stdout.read(&mut chunk).await.context("reading ffmpeg stdout")?;
        if n == 0 { break; }
        buf.extend_from_slice(&chunk[..n]);
        progress_pb.set_position(buf.len() as u64);
    }
    let status = child.wait().await.context("waiting for ffmpeg")?;
    if !status.success() {
        let mut err = String::new();
        stderr.read_to_string(&mut err).await.ok();
        bail!("ffmpeg exited with status {}: {}", status, err.trim());
    }
    progress_pb.disable_steady_tick();
    Ok(buf)
}

async fn encode_video_to_buffer(
    input: &Path,
    encode_args: &[String],
    progress_pb: &ProgressBar,
) -> anyhow::Result<Vec<u8>> {
    let label = format!("encoding {}", input.file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default());
    run_ffmpeg_to_buffer(input, encode_args, &["-f", "mp4"], progress_pb, &label).await
}

async fn make_thumbnail_to_buffer(
    input: &Path,
    thumbnail_args: &[String],
    progress_pb: &ProgressBar,
) -> anyhow::Result<Vec<u8>> {
    let label = format!("thumbnail {}", input.file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default());
    // `-f mjpeg pipe:1` emits raw JPEG bytes on stdout; combined with
    // `-frames:v 1` from the thumbnail args this is exactly one image.
    run_ffmpeg_to_buffer(input, thumbnail_args, &["-f", "mjpeg"], progress_pb, &label).await
}

async fn upload_thumb(client: &Client, bytes: Vec<u8>, name: &str) -> anyhow::Result<Uploaded> {
    let size = bytes.len();
    let arc = Arc::new(bytes);
    let mut reader = SliceReader { buf: arc, start: 0, end: size, pos: 0 };
    client
        .upload_stream(&mut reader, size, format!("{}.thumb.jpg", name))
        .await
        .context("uploading thumbnail")
}

/// Run an `EncodedVideo` item: spawn ffmpeg, buffer the encoded MP4 in
/// memory, build the upload parts according to the channel multipart policy
/// (now that we know the encoded size), generate + upload a thumbnail, then
/// send the appropriate message(s).
async fn run_encoded_video(
    client: &Client,
    peer: PeerRef,
    ffmpeg: &FfmpegConfig,
    encode_args: &[String],
    thumbnail_args: &[String],
    item: &UploadItem,
    file_pb: &ProgressBar,
    total_pb: &ProgressBar,
) -> anyhow::Result<()> {
    let (source, doc_filename, virtual_path, rel_dir, policy, source_size) = match item {
        UploadItem::EncodedVideo { source, doc_filename, virtual_path, rel_dir, policy, source_size } => {
            (source.clone(), doc_filename.clone(), virtual_path.clone(), rel_dir.clone(), *policy, *source_size)
        }
        _ => unreachable!(),
    };
    let _ = ffmpeg;

    // 1. Encode video to memory.
    let encoded = encode_video_to_buffer(&source, encode_args, file_pb).await?;
    let encoded_size = encoded.len() as u64;

    // 2. Generate thumbnail to memory.
    let thumb_bytes = make_thumbnail_to_buffer(&source, thumbnail_args, file_pb).await?;

    // 3. Adjust aggregate bar length to reflect the actual encoded size.
    let cur_len = total_pb.length().unwrap_or(0);
    let new_len = cur_len.saturating_sub(source_size).saturating_add(encoded_size);
    total_pb.set_length(new_len);

    // 4. Upload thumbnail (small, no progress tracking).
    let thumb_uploaded = upload_thumb(client, thumb_bytes, &doc_filename).await?;

    // 5. Choose upload shape from encoded size + multipart policy.
    let buffer = Arc::new(encoded);
    if encoded_size <= PART_MAX {
        let caption = if !rel_dir.is_empty() {
            format!("path: {}/", rel_dir)
        } else {
            String::new()
        };
        let part = UploadPart {
            src: PartSource::Buffer(buffer),
            offset: 0,
            size: encoded_size,
            doc_filename: doc_filename.clone(),
            caption,
        };
        upload_part_as_message(client, peer, &part, Some(&thumb_uploaded), file_pb, total_pb).await?;
        return Ok(());
    }

    // Encoded stream is bigger than 4 GiB — needs splitting.
    if policy == MultipartPolicy::None {
        bail!(
            "encoded video '{}' is {} bytes (> 4 GiB) but the target channel's multipart_policy is `none`",
            doc_filename, encoded_size
        );
    }
    let parts = build_multipart_parts(
        PartSource::Buffer(buffer),
        &doc_filename,
        &virtual_path,
        encoded_size,
        policy,
    );
    match policy {
        MultipartPolicy::Suffix => {
            for p in &parts {
                upload_part_as_message(client, peer, p, Some(&thumb_uploaded), file_pb, total_pb).await?;
            }
        }
        MultipartPolicy::Album => {
            upload_album(client, peer, &parts, Some(&thumb_uploaded), file_pb, total_pb).await?;
        }
        MultipartPolicy::None => unreachable!(),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// driver
// ---------------------------------------------------------------------------

async fn run() -> anyhow::Result<()> {
    let args = parse_args()?;

    if args.encode_video && args.dir_mode == DirMode::Zip {
        bail!("--encode-video combined with --dir zip is not supported");
    }

    if args.encode_video && !ffmpeg_in_path() {
        bail!("--encode-video requires ffmpeg on $PATH (not found)");
    }

    if args.dir_mode == DirMode::Zip {
        for p in &args.paths {
            let m = std::fs::metadata(p)
                .with_context(|| format!("can't stat '{}'", p.display()))?;
            if m.is_dir() {
                println!("not implemented");
                return Ok(());
            }
        }
    }

    let config_path: PathBuf = match &args.config_path {
        Some(p) => PathBuf::from(p),
        None => default_config_path(),
    };
    let config: Config = config::load_config(&config_path.to_string_lossy())?;
    let channel_entry = find_channel(&config, &args.channel).ok_or_else(|| {
        anyhow!("channel '{}' is not defined in {}", args.channel, config_path.display())
    })?;
    let policy = channel_entry.multipart_policy;

    // Session lives next to the default config. An explicit --config still
    // points at ~/.config/tgfs/session.sqlite3 — keep one session per host.
    let session_path = default_session_path();
    if let Some(parent) = session_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating '{}'", parent.display()))?;
    }

    let encode_args = split_shell_args(&config.ffmpeg.encode_args)
        .context("parsing ffmpeg.encode_args")?;
    let thumbnail_args = split_shell_args(&config.ffmpeg.thumbnail_args)
        .context("parsing ffmpeg.thumbnail_args")?;

    let cwd = std::env::current_dir()
        .context("can't determine current working directory")?
        .canonicalize()
        .context("can't canonicalize current working directory")?;

    let mut plan: Vec<UploadItem> = Vec::new();
    for p in &args.paths {
        collect_path(p, &cwd, policy, args.dir_mode, args.encode_video, &mut plan)?;
    }
    if plan.is_empty() { bail!("nothing to upload"); }

    if args.dry_run {
        print_plan(&plan, &args.channel, args.encode_video);
        return Ok(());
    }

    print_plan(&plan, &args.channel, args.encode_video);
    println!();
    println!("Connecting to Telegram...");

    let (client, _updates_rx) =
        connect_and_authorize_with_session(&config, &session_path.to_string_lossy()).await?;
    let peer = resolve_channel_peer(&client, &args.channel).await?;

    let total_bytes: u64 = plan.iter().map(|i| i.planned_bytes()).sum();
    let mp = Arc::new(MultiProgress::new());
    let file_pb = mp.add(ProgressBar::new(0));
    set_bar_style(&file_pb);
    let total_pb = mp.add(ProgressBar::new(total_bytes));
    total_pb.set_style(
        ProgressStyle::with_template(
            "TOTAL {prefix:<8} [{bar:30.green/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
        )
        .unwrap()
        .progress_chars("=>-"),
    );
    total_pb.set_prefix(format!("{}/{}", 0, plan.len()));

    for (i, item) in plan.iter().enumerate() {
        total_pb.set_prefix(format!("{}/{}", i, plan.len()));
        match item {
            UploadItem::Single(p) => {
                upload_part_as_message(&client, peer, p, None, &file_pb, &total_pb).await?;
            }
            UploadItem::SuffixParts { parts, .. } => {
                for p in parts {
                    upload_part_as_message(&client, peer, p, None, &file_pb, &total_pb).await?;
                }
            }
            UploadItem::AlbumParts { parts, .. } => {
                upload_album(&client, peer, parts, None, &file_pb, &total_pb).await?;
            }
            UploadItem::EncodedVideo { .. } => {
                run_encoded_video(
                    &client, peer, &config.ffmpeg,
                    &encode_args, &thumbnail_args,
                    item, &file_pb, &total_pb,
                ).await?;
            }
        }
        file_pb.set_message(format!("done: {}", item.display_name()));
    }
    file_pb.finish_with_message("done");
    total_pb.finish();
    println!("All uploads complete.");
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {:#}", e);
            ExitCode::FAILURE
        }
    }
}
