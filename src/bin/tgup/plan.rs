//! Offline upload-plan construction: walk argument paths, classify each file
//! against the channel's multipart policy, and produce a printable list of
//! `UploadItem`s before any network I/O happens.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context as _};

use tgfs::config::{ChannelEntry, Config, MultipartPolicy};

use super::args::DirMode;

pub const PART_MAX: u64 = 4 * 1024 * 1024 * 1024; // 4 GiB per Telegram message

pub fn is_video_path(path: &Path) -> bool {
    mime_guess::from_path(path)
        .first()
        .map(|m| m.type_() == mime_guess::mime::VIDEO)
        .unwrap_or(false)
}

/// Hardcoded extension fallbacks used when `mime_guess` can't classify a
/// file (obscure containers, sparse system MIME tables). Mirrors what the
/// uploader uses at send time so the dry-run preview matches reality.
const VIDEO_EXTS: &[&str] = &["mp4", "mkv", "mov", "webm", "avi", "m4v", "ts", "mpg", "mpeg", "wmv"];
const PHOTO_EXTS: &[&str] = &["jpg", "jpeg", "png", "webp", "heic", "heif", "gif", "bmp"];

/// Classify a path as `"image"`, `"video"`, or `None` using MIME first and
/// extension as fallback. Used by both the dry-run plan printer and the
/// runtime upload classifier so the displayed label can't drift from the
/// actual send behavior.
pub fn media_type_from_path(path: &Path) -> Option<&'static str> {
    if let Some(m) = mime_guess::from_path(path).first() {
        match m.type_().as_str() {
            "image" => return Some("image"),
            "video" => return Some("video"),
            _ => {}
        }
    }
    let ext = path.extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase())?;
    if VIDEO_EXTS.iter().any(|v| *v == ext) { return Some("video"); }
    if PHOTO_EXTS.iter().any(|p| *p == ext) { return Some("image"); }
    None
}

/// One-word upload-kind tag printed in the plan listing.
fn part_kind_label(part: &UploadPart) -> &'static str {
    let PartSource::File(p) = &part.src;
    match media_type_from_path(p) {
        Some("video") => "video",
        Some("image") => "photo",
        _ => "file",
    }
}

/// Returns true when any item in `plan` would benefit from ffprobe metadata
/// (i.e. would be sent as a video). Drives the "ffprobe not on PATH"
/// warning printed before the plan.
pub fn plan_has_video(plan: &[UploadItem]) -> bool {
    plan.iter().any(|item| match item {
        UploadItem::Single(p) => part_kind_label(p) == "video",
        UploadItem::FileAlbum { parts } => parts.iter().all(|p| part_kind_label(p) == "video"),
        UploadItem::EncodedVideo { .. } | UploadItem::EncodedAlbum { .. } => true,
        // Multipart suffix/album splits are never sent as videos — they go
        // out as documents per chunk.
        UploadItem::SuffixParts { .. } | UploadItem::AlbumParts { .. } => false,
    })
}

pub fn replace_ext(name: &str, new_ext: &str) -> String {
    let stem = match name.rfind('.') {
        Some(i) => &name[..i],
        None => name,
    };
    if stem.is_empty() { format!(".{}", new_ext) } else { format!("{}.{}", stem, new_ext) }
}

/// A single Telegram document upload (one message).
#[derive(Clone)]
pub struct UploadPart {
    /// Where to read bytes from.
    pub src: PartSource,
    pub offset: u64,
    pub size: u64,
    /// Filename sent to Telegram (`DocumentAttributeFilename`).
    pub doc_filename: String,
    /// Caption attached to this part's message.
    pub caption: String,
}

#[derive(Clone)]
pub enum PartSource {
    File(PathBuf),
}

/// One logical entry of the upload plan.
#[derive(Clone)]
pub enum UploadItem {
    /// One message — file <= 4 GiB.
    Single(UploadPart),
    /// File > 4 GiB, channel policy = `suffix`. N independent messages whose
    /// document filenames carry `.NN`. Only `parts[0]` has a `path:` caption.
    SuffixParts { display: String, parts: Vec<UploadPart> },
    /// File > 4 GiB, channel policy = `album`. One Telegram album of N parts
    /// that share `multipart:` + `path:` directives in the caption.
    AlbumParts { display: String, parts: Vec<UploadPart> },
    /// Multiple distinct files grouped into a single Telegram album (max 10).
    /// Produced by post-processing when `--album` is passed. All parts share
    /// the same caption so the indexer's `extract_group_caption` applies one
    /// `path:` directive uniformly to every part.
    FileAlbum { parts: Vec<UploadPart> },
    /// Multiple videos produced by `--tvshow --encode-video` that must each
    /// be re-encoded before being grouped into a single Telegram album.
    /// Parts hold the original source paths; `doc_filename` already has the
    /// `.mp4` extension (renamed from the tvshow plan).
    EncodedAlbum { parts: Vec<UploadPart> },
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
    pub fn display_name(&self) -> &str {
        match self {
            UploadItem::Single(p) => &p.doc_filename,
            UploadItem::SuffixParts { display, .. }
            | UploadItem::AlbumParts { display, .. } => display.as_str(),
            UploadItem::FileAlbum { parts }
            | UploadItem::EncodedAlbum { parts } => parts.first().map(|p| p.doc_filename.as_str()).unwrap_or("album"),
            UploadItem::EncodedVideo { doc_filename, .. } => doc_filename.as_str(),
        }
    }
    /// Bytes used to size the aggregate progress bar. For encoded videos we
    /// don't know the encoded size yet — use the source size as a rough
    /// estimate; the bar's length is adjusted once encoding completes.
    pub fn planned_bytes(&self) -> u64 {
        match self {
            UploadItem::Single(p) => p.size,
            UploadItem::SuffixParts { parts, .. }
            | UploadItem::AlbumParts { parts, .. }
            | UploadItem::FileAlbum { parts }
            | UploadItem::EncodedAlbum { parts } => parts.iter().map(|p| p.size).sum(),
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
pub fn collect_path(
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

/// Telegram allows at most 10 media in one album.
pub const ALBUM_MAX: usize = 10;

/// Post-process a plan to merge runs of `Single` items into `FileAlbum`s.
///
/// Only consecutive `Single`s that share the same `caption` are merged so the
/// indexer-side `extract_group_caption` (which applies one `path:` directive
/// uniformly to every part of an album) yields the same virtual paths as the
/// ungrouped upload would have. Non-`Single` items are passed through and act
/// as boundaries that flush any in-progress group.
pub fn group_into_albums(plan: Vec<UploadItem>) -> Vec<UploadItem> {
    let mut out: Vec<UploadItem> = Vec::new();
    let mut buf: Vec<UploadPart> = Vec::new();
    let mut buf_caption: Option<String> = None;
    for item in plan {
        match item {
            UploadItem::Single(part) => {
                let cap_changed = buf_caption.as_ref().map_or(false, |c| c != &part.caption);
                if buf.len() >= ALBUM_MAX || cap_changed {
                    flush_album_buf(&mut buf, &mut buf_caption, &mut out);
                }
                if buf.is_empty() { buf_caption = Some(part.caption.clone()); }
                buf.push(part);
            }
            other => {
                flush_album_buf(&mut buf, &mut buf_caption, &mut out);
                out.push(other);
            }
        }
    }
    flush_album_buf(&mut buf, &mut buf_caption, &mut out);
    out
}

fn flush_album_buf(buf: &mut Vec<UploadPart>, caption: &mut Option<String>, out: &mut Vec<UploadItem>) {
    match buf.len() {
        0 => {}
        1 => out.push(UploadItem::Single(buf.pop().unwrap())),
        _ => out.push(UploadItem::FileAlbum { parts: std::mem::take(buf) }),
    }
    *caption = None;
}

pub fn find_channel<'a>(config: &'a Config, name: &str) -> Option<&'a ChannelEntry> {
    config.channels.iter().find(|c| c.name == name)
}

pub fn print_plan(plan: &[UploadItem], channel: &str, encode_video: bool) {
    let total: u64 = plan.iter().map(|i| i.planned_bytes()).sum();
    println!("Target channel: {}", channel);
    if encode_video {
        println!("--encode-video: video files will be re-encoded by ffmpeg (sizes below are source bytes)");
    }
    println!("Items: {}, total source bytes: {}", plan.len(), total);
    for (i, item) in plan.iter().enumerate() {
        match item {
            UploadItem::Single(p) => {
                let PartSource::File(pb) = &p.src;
                let from = pb.display().to_string();
                println!("  [{}] [{}] {} — {} bytes (from '{}' offset {})",
                    i, part_kind_label(p), p.doc_filename, p.size, from, p.offset);
                if !p.caption.is_empty() {
                    for line in p.caption.lines() {
                        println!("        caption: {}", line);
                    }
                }
            }
            UploadItem::SuffixParts { display, parts } => {
                println!("  [{}] [file] {} — suffix multipart, {} parts, {} bytes",
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
                println!("  [{}] [file] {} — album multipart, {} parts, {} bytes",
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
            UploadItem::FileAlbum { parts } => {
                let total: u64 = parts.iter().map(|p| p.size).sum();
                // Mirror the runtime album-mode decision so the dry-run shows
                // photo album vs. video album vs. file album.
                let kinds: Vec<&'static str> = parts.iter().map(part_kind_label).collect();
                let album_kind = if kinds.iter().all(|k| *k == "photo") { "photo" }
                    else if kinds.iter().all(|k| *k == "video") { "video" }
                    else { "file" };
                println!("  [{}] [{} album] {} items — {} bytes", i, album_kind, parts.len(), total);
                if let Some(c) = parts.first() {
                    if !c.caption.is_empty() {
                        for line in c.caption.lines() {
                            println!("        album caption: {}", line);
                        }
                    }
                }
                for (j, p) in parts.iter().enumerate() {
                    let PartSource::File(pb) = &p.src;
                    println!("        [{}] [{}] {} — {} bytes (from '{}')",
                        j, part_kind_label(p), p.doc_filename, p.size, pb.display());
                }
            }
            UploadItem::EncodedAlbum { parts } => {
                let total: u64 = parts.iter().map(|p| p.size).sum();
                println!("  [{}] [video album] {} items — {} source bytes; each will be re-encoded",
                    i, parts.len(), total);
                if let Some(c) = parts.first() {
                    if !c.caption.is_empty() {
                        for line in c.caption.lines() {
                            println!("        album caption: {}", line);
                        }
                    }
                }
                for (j, p) in parts.iter().enumerate() {
                    let PartSource::File(pb) = &p.src;
                    println!("        [{}] {} — {} bytes (from '{}')",
                        j, p.doc_filename, p.size, pb.display());
                }
            }
            UploadItem::EncodedVideo { source, doc_filename, virtual_path, rel_dir, source_size, .. } => {
                println!("  [{}] [video] {} — re-encode from '{}' ({} source bytes); thumbnail generated",
                    i, doc_filename, source.display(), source_size);
                if !rel_dir.is_empty() {
                    println!("        target virtual path: {}", virtual_path);
                }
            }
        }
    }
}
