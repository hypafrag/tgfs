//! `tgup` — upload files and directories to a tgfs-managed Telegram channel.
//!
//! Reads the same `tgfs.yml` as the daemon, resolves the target channel from
//! `--channel`, and walks positional arguments according to `--dir`. Builds the
//! complete execution plan offline; only after validation does it connect to
//! Telegram and start uploading.

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::ExitCode;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{anyhow, bail, Context as _};
use grammers_client::Client;
use grammers_client::media::InputMedia;
use grammers_client::message::InputMessage;
use grammers_client::peer::Peer;
use grammers_session::types::PeerRef;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeekExt, ReadBuf};

use tgfs::config::{self, ChannelEntry, Config, MultipartPolicy};
use tgfs::login::connect_and_authorize;

const PART_MAX: u64 = 4 * 1024 * 1024 * 1024; // 4 GiB per Telegram message

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DirMode {
    Skip,
    Recursive,
    Caption,
    Zip,
}

struct Args {
    config_path: String,
    channel: String,
    dir_mode: DirMode,
    dry_run: bool,
    paths: Vec<PathBuf>,
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
           tgup [--config <path>] -c <channel> [-d <mode>] [--dry-run] <path>...\n\n\
         Options:\n  \
           -c, --channel <name>   Target channel name (must exist in config).\n  \
           -d, --dir <mode>       How to handle directory arguments:\n                            \
             skip       — error on directories (default)\n                            \
             recursive  — upload contained files as a flat list\n                            \
             caption    — like recursive, but each file's caption sets\n                                         \
             `path: <relative dir>/` so the tree is recreated\n                            \
             zip        — not implemented (exits with error)\n  \
           --dry-run              Print the plan and exit.\n  \
           --config <path>        Config file (default: tgfs.yml)."
    );
}

fn parse_args() -> anyhow::Result<Args> {
    let mut args = std::env::args().skip(1);
    let mut config_path = "tgfs.yml".to_string();
    let mut channel: Option<String> = None;
    let mut dir_mode = DirMode::Skip;
    let mut dry_run = false;
    let mut paths: Vec<PathBuf> = Vec::new();
    while let Some(a) = args.next() {
        match a.as_str() {
            "-h" | "--help" => { print_usage(); std::process::exit(0); }
            "--config" => {
                config_path = args.next().ok_or_else(|| anyhow!("--config requires a path"))?;
            }
            "-c" | "--channel" => {
                channel = Some(args.next().ok_or_else(|| anyhow!("-c/--channel requires a value"))?);
            }
            "-d" | "--dir" => {
                let v = args.next().ok_or_else(|| anyhow!("-d/--dir requires a value"))?;
                dir_mode = parse_dir_mode(&v)?;
            }
            "--dry-run" => dry_run = true,
            other if other.starts_with("--channel=") => {
                channel = Some(other.trim_start_matches("--channel=").to_string());
            }
            other if other.starts_with("--dir=") => {
                dir_mode = parse_dir_mode(other.trim_start_matches("--dir="))?;
            }
            other if other.starts_with("--config=") => {
                config_path = other.trim_start_matches("--config=").to_string();
            }
            other if other.starts_with('-') => bail!("unknown option: {}", other),
            _ => paths.push(PathBuf::from(a)),
        }
    }
    let channel = channel.ok_or_else(|| anyhow!("-c/--channel is required"))?;
    if paths.is_empty() { bail!("at least one file or directory path is required"); }
    Ok(Args { config_path, channel, dir_mode, dry_run, paths })
}

/// A single Telegram document upload (one message).
#[derive(Clone)]
struct UploadPart {
    source: PathBuf,
    offset: u64,
    size: u64,
    /// Filename sent to Telegram (`DocumentAttributeFilename`).
    doc_filename: String,
    /// Caption attached to this part's message.
    caption: String,
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
}

impl UploadItem {
    fn display_name(&self) -> &str {
        match self {
            UploadItem::Single(p) => &p.doc_filename,
            UploadItem::SuffixParts { display, .. }
            | UploadItem::AlbumParts { display, .. } => display.as_str(),
        }
    }
    fn total_bytes(&self) -> u64 {
        match self {
            UploadItem::Single(p) => p.size,
            UploadItem::SuffixParts { parts, .. }
            | UploadItem::AlbumParts { parts, .. } => parts.iter().map(|p| p.size).sum(),
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
) -> anyhow::Result<UploadItem> {
    let virtual_path = if rel_dir.is_empty() {
        original_name.to_string()
    } else {
        format!("{}/{}", rel_dir, original_name)
    };
    let needs_caption_path = dir_mode == DirMode::Caption && !rel_dir.is_empty();

    if size <= PART_MAX {
        let caption = if needs_caption_path {
            format!("path: {}/", rel_dir)
        } else {
            String::new()
        };
        return Ok(UploadItem::Single(UploadPart {
            source: abs_path.to_path_buf(),
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

    let mut parts: Vec<UploadPart> = Vec::new();
    let mut offset: u64 = 0;
    let mut idx: usize = 0;
    while offset < size {
        let chunk = std::cmp::min(PART_MAX, size - offset);
        let part_name = format!("{}.{:02}", original_name, idx);
        let caption = match policy {
            MultipartPolicy::Suffix => {
                if idx == 0 {
                    // .00's `path:` sets the assembled file's exposed name.
                    format!("path: {}", virtual_path)
                } else {
                    String::new()
                }
            }
            MultipartPolicy::Album => {
                // Same caption is set on every part of the album; the indexer
                // reads one of them via `extract_group_caption`.
                format!("multipart:\npath: {}", virtual_path)
            }
            MultipartPolicy::None => unreachable!(),
        };
        parts.push(UploadPart {
            source: abs_path.to_path_buf(),
            offset,
            size: chunk,
            doc_filename: part_name,
            caption,
        });
        offset += chunk;
        idx += 1;
    }

    Ok(match policy {
        MultipartPolicy::Suffix => UploadItem::SuffixParts {
            display: original_name.to_string(),
            parts,
        },
        MultipartPolicy::Album => UploadItem::AlbumParts {
            display: original_name.to_string(),
            parts,
        },
        MultipartPolicy::None => unreachable!(),
    })
}

/// Walk one positional argument and accumulate plan items.
fn collect_path(
    arg: &Path,
    cwd: &Path,
    policy: MultipartPolicy,
    dir_mode: DirMode,
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
                // Caption-mode on a positional FILE: place it under its dir
                // relative to cwd. The path must not escape cwd.
                let parent = abs.parent().unwrap_or_else(|| Path::new(""));
                relative_dir_from_cwd(parent, cwd)?
            }
            _ => String::new(),
        };
        let item = plan_one_file(&abs, &name, &rel_dir, meta.len(), policy, dir_mode)?;
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
                collect_dir(arg, cwd, policy, dir_mode, out)?;
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
                let item = plan_one_file(&abs, &name, &rel_dir, m.len(), policy, dir_mode)?;
                out.push(item);
            }
        }
    }
    Ok(())
}

/// Compute `parent`'s path relative to `cwd`, both already canonicalized.
/// Fails if the relative path would contain `..` (i.e. `parent` is not under
/// `cwd`). Returns an empty string when `parent == cwd`.
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

fn print_plan(plan: &[UploadItem], channel: &str) {
    let total: u64 = plan.iter().map(|i| i.total_bytes()).sum();
    println!("Target channel: {}", channel);
    println!("Items: {}, total bytes: {}", plan.len(), total);
    for (i, item) in plan.iter().enumerate() {
        match item {
            UploadItem::Single(p) => {
                println!("  [{}] {} — {} bytes (from '{}' offset {})",
                    i, p.doc_filename, p.size, p.source.display(), p.offset);
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
                    println!("        part .{:02}: {} bytes (offset {})",
                        j, p.size, p.offset);
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
                    println!("        part .{:02}: {} bytes (offset {})",
                        j, p.size, p.offset);
                }
            }
        }
    }
}

/// Async-read wrapper that ticks two progress bars.
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

async fn open_part_stream(part: &UploadPart) -> anyhow::Result<tokio::io::Take<File>> {
    let mut f = File::open(&part.source)
        .await
        .with_context(|| format!("opening '{}'", part.source.display()))?;
    if part.offset > 0 {
        f.seek(SeekFrom::Start(part.offset)).await?;
    }
    Ok(tokio::io::AsyncReadExt::take(f, part.size))
}

async fn upload_single(
    client: &Client,
    peer: PeerRef,
    part: &UploadPart,
    file_pb: &ProgressBar,
    total_pb: &ProgressBar,
) -> anyhow::Result<()> {
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

    let msg = InputMessage::new()
        .text(part.caption.clone())
        .file(uploaded);
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
    file_pb: &ProgressBar,
    total_pb: &ProgressBar,
) -> anyhow::Result<()> {
    let mut medias: Vec<InputMedia> = Vec::with_capacity(parts.len());
    for part in parts {
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
        // Every part shares the same caption text (the indexer reads it off
        // any one of them via extract_group_caption).
        let media = InputMedia::new()
            .caption(part.caption.clone())
            .file(uploaded);
        medias.push(media);
    }
    client
        .send_album(peer, medias)
        .await
        .context("sending album")?;
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

async fn run() -> anyhow::Result<()> {
    let args = parse_args()?;
    if args.dir_mode == DirMode::Zip {
        // Only fail with "not implemented" if a directory positional argument
        // would actually trigger the zip path; if all positionals are files,
        // zip mode is harmless. Easier: just refuse up-front per the spec.
        for p in &args.paths {
            let m = std::fs::metadata(p)
                .with_context(|| format!("can't stat '{}'", p.display()))?;
            if m.is_dir() {
                println!("not implemented");
                return Ok(());
            }
        }
    }

    let config: Config = config::load_config(&args.config_path)?;
    let channel_entry = find_channel(&config, &args.channel).ok_or_else(|| {
        anyhow!("channel '{}' is not defined in {}", args.channel, args.config_path)
    })?;
    let policy = channel_entry.multipart_policy;

    let cwd = std::env::current_dir()
        .context("can't determine current working directory")?
        .canonicalize()
        .context("can't canonicalize current working directory")?;

    let mut plan: Vec<UploadItem> = Vec::new();
    for p in &args.paths {
        collect_path(p, &cwd, policy, args.dir_mode, &mut plan)?;
    }
    if plan.is_empty() {
        bail!("nothing to upload");
    }

    if args.dry_run {
        print_plan(&plan, &args.channel);
        return Ok(());
    }

    print_plan(&plan, &args.channel);
    println!();
    println!("Connecting to Telegram...");

    let (client, _updates_rx) = connect_and_authorize(&config).await?;
    let peer = resolve_channel_peer(&client, &args.channel).await?;

    // Progress bars: per-current-file on top, aggregate underneath.
    let total_bytes: u64 = plan.iter().map(|i| i.total_bytes()).sum();
    let mp = Arc::new(MultiProgress::new());
    let file_style = ProgressStyle::with_template(
        "  {msg:40!} [{bar:30.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
    )
    .unwrap()
    .progress_chars("=>-");
    let total_style = ProgressStyle::with_template(
        "TOTAL {prefix:<8} [{bar:30.green/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
    )
    .unwrap()
    .progress_chars("=>-");
    let file_pb = mp.add(ProgressBar::new(0));
    file_pb.set_style(file_style);
    let total_pb = mp.add(ProgressBar::new(total_bytes));
    total_pb.set_style(total_style);
    total_pb.set_prefix(format!("{}/{}", 0, plan.len()));

    for (i, item) in plan.iter().enumerate() {
        total_pb.set_prefix(format!("{}/{}", i, plan.len()));
        match item {
            UploadItem::Single(p) => {
                upload_single(&client, peer, p, &file_pb, &total_pb).await?;
            }
            UploadItem::SuffixParts { parts, .. } => {
                for p in parts {
                    upload_single(&client, peer, p, &file_pb, &total_pb).await?;
                }
            }
            UploadItem::AlbumParts { parts, .. } => {
                upload_album(&client, peer, parts, &file_pb, &total_pb).await?;
            }
        }
        // Show finished item name briefly.
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
