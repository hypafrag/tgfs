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
