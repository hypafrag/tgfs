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

/// Convert every video `Single` or `FileAlbum` part of a plan into an encoded
/// upload:
///   - `Single` (video)    → `EncodedVideo`  (individual encoded message)
///   - `FileAlbum` (video) → `EncodedAlbum`  (encode-then-album, grouping preserved)
/// Non-video parts are left unchanged. Used for `--tvshow --encode-video`
/// (applied to the per-season tvshow plan) and for plain `-a/--album
/// --encode-video` (applied after `group_into_albums`, so albums formed from
/// consecutive `Single`s survive the conversion to `EncodedAlbum` instead of
/// being converted to `EncodedVideo` per-file beforehand and left ungrouped).
pub fn apply_encode_video_to_plan(
    plan: Vec<UploadItem>,
    policy: MultipartPolicy,
) -> Vec<UploadItem> {
    let mut out: Vec<UploadItem> = Vec::with_capacity(plan.len());
    for item in plan {
        match item {
            UploadItem::Single(part) => {
                let PartSource::File(ref src) = part.src;
                if is_video_path(src) {
                    let doc_filename = replace_ext(&part.doc_filename, "mp4");
                    out.push(UploadItem::EncodedVideo {
                        source: src.clone(),
                        doc_filename: doc_filename.clone(),
                        virtual_path: doc_filename,
                        rel_dir: String::new(),
                        policy,
                        source_size: part.size,
                    });
                } else {
                    out.push(UploadItem::Single(part));
                }
            }
            UploadItem::FileAlbum { parts } => {
                let renamed: Vec<UploadPart> = parts
                    .into_iter()
                    .map(|p| {
                        let PartSource::File(ref src) = p.src;
                        if is_video_path(src) {
                            UploadPart { doc_filename: replace_ext(&p.doc_filename, "mp4"), ..p }
                        } else {
                            p
                        }
                    })
                    .collect();
                out.push(UploadItem::EncodedAlbum { parts: renamed });
            }
            other => out.push(other),
        }
    }
    out
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

/// How `group_into_albums` splits a run of same-caption files into albums of
/// at most `ALBUM_MAX` items.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AlbumSplitMode {
    /// Greedily pack each album to `ALBUM_MAX` before starting a new one; only
    /// the last album in a run may be smaller. This is the original `-a`
    /// behavior.
    Fill,
    /// Distribute the whole run across `ceil(n / ALBUM_MAX)` albums as evenly
    /// as possible, mirroring the `--tvshow` per-season split.
    Even,
}

/// Distribute `n` items across `ceil(n / ALBUM_MAX)` albums as evenly as
/// possible. e.g. n=11 → [6,5]; n=13 → [7,6]; n=21 → [7,7,7].
pub fn split_album_sizes(n: usize) -> Vec<usize> {
    if n == 0 { return Vec::new(); }
    let k = (n + ALBUM_MAX - 1) / ALBUM_MAX;
    let base = n / k;
    let extra = n % k;
    (0..k).map(|i| if i < extra { base + 1 } else { base }).collect()
}

/// Split `n` items into chunks of at most `ALBUM_MAX`, packing every chunk
/// but the last to full capacity. e.g. n=11 → [10,1]; n=21 → [10,10,1].
fn fill_album_sizes(n: usize) -> Vec<usize> {
    if n == 0 { return Vec::new(); }
    let mut sizes = vec![ALBUM_MAX; n / ALBUM_MAX];
    let rem = n % ALBUM_MAX;
    if rem > 0 { sizes.push(rem); }
    sizes
}

/// Post-process a plan to merge runs of `Single` items into `FileAlbum`s.
///
/// Only consecutive `Single`s that share the same `caption` are merged so the
/// indexer-side `extract_group_caption` (which applies one `path:` directive
/// uniformly to every part of an album) yields the same virtual paths as the
/// ungrouped upload would have. Non-`Single` items are passed through and act
/// as boundaries that flush any in-progress group.
///
/// `mode` controls how a run longer than `ALBUM_MAX` is split into multiple
/// albums — see [`AlbumSplitMode`].
pub fn group_into_albums(plan: Vec<UploadItem>, mode: AlbumSplitMode) -> Vec<UploadItem> {
    let mut out: Vec<UploadItem> = Vec::new();
    let mut buf: Vec<UploadPart> = Vec::new();
    let mut buf_caption: Option<String> = None;
    for item in plan {
        match item {
            UploadItem::Single(part) => {
                let cap_changed = buf_caption.as_ref().map_or(false, |c| c != &part.caption);
                if cap_changed {
                    flush_album_run(&mut buf, &mut buf_caption, &mut out, mode);
                }
                if buf.is_empty() { buf_caption = Some(part.caption.clone()); }
                buf.push(part);
            }
            other => {
                flush_album_run(&mut buf, &mut buf_caption, &mut out, mode);
                out.push(other);
            }
        }
    }
    flush_album_run(&mut buf, &mut buf_caption, &mut out, mode);
    out
}

/// Flush an accumulated same-caption run, splitting it into one or more
/// albums per `mode`. `Fill` packs greedily; `Even` balances the whole run.
fn flush_album_run(
    buf: &mut Vec<UploadPart>,
    caption: &mut Option<String>,
    out: &mut Vec<UploadItem>,
    mode: AlbumSplitMode,
) {
    if buf.is_empty() { return; }
    let parts = std::mem::take(buf);
    let chunk_sizes = match mode {
        AlbumSplitMode::Fill => fill_album_sizes(parts.len()),
        AlbumSplitMode::Even => split_album_sizes(parts.len()),
    };
    let mut iter = parts.into_iter();
    for size in chunk_sizes {
        let chunk: Vec<UploadPart> = iter.by_ref().take(size).collect();
        match chunk.len() {
            0 => {}
            1 => out.push(UploadItem::Single(chunk.into_iter().next().unwrap())),
            _ => out.push(UploadItem::FileAlbum { parts: chunk }),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn part(name: &str, caption: &str) -> UploadPart {
        UploadPart {
            src: PartSource::File(PathBuf::from(format!("/synthetic/{name}"))),
            offset: 0,
            size: 1,
            doc_filename: name.to_string(),
            caption: caption.to_string(),
        }
    }

    fn singles(n: usize, caption: &str) -> Vec<UploadItem> {
        (0..n).map(|i| UploadItem::Single(part(&format!("f{i}"), caption))).collect()
    }

    /// Pull the size of each grouped chunk out of a `group_into_albums` result,
    /// for a plan that only ever produces `Single`/`FileAlbum` items.
    fn chunk_sizes(out: &[UploadItem]) -> Vec<usize> {
        out.iter().map(|i| match i {
            UploadItem::Single(_) => 1,
            UploadItem::FileAlbum { parts } => parts.len(),
            _ => panic!("unexpected item kind in test plan"),
        }).collect()
    }

    #[test]
    fn split_album_sizes_balances_evenly() {
        assert_eq!(split_album_sizes(0), Vec::<usize>::new());
        assert_eq!(split_album_sizes(1), vec![1]);
        assert_eq!(split_album_sizes(10), vec![10]);
        assert_eq!(split_album_sizes(11), vec![6, 5]);
        assert_eq!(split_album_sizes(13), vec![7, 6]);
        assert_eq!(split_album_sizes(21), vec![7, 7, 7]);
    }

    #[test]
    fn fill_album_sizes_packs_greedily() {
        assert_eq!(fill_album_sizes(0), Vec::<usize>::new());
        assert_eq!(fill_album_sizes(1), vec![1]);
        assert_eq!(fill_album_sizes(10), vec![10]);
        assert_eq!(fill_album_sizes(11), vec![10, 1]);
        assert_eq!(fill_album_sizes(21), vec![10, 10, 1]);
    }

    #[test]
    fn group_into_albums_fill_packs_full_chunks_then_remainder() {
        let plan = singles(11, "same caption");
        let out = group_into_albums(plan, AlbumSplitMode::Fill);
        assert_eq!(chunk_sizes(&out), vec![10, 1]);
    }

    #[test]
    fn group_into_albums_even_balances_the_whole_run() {
        let plan = singles(11, "same caption");
        let out = group_into_albums(plan, AlbumSplitMode::Even);
        assert_eq!(chunk_sizes(&out), vec![6, 5]);
    }

    #[test]
    fn group_into_albums_even_matches_fill_under_album_max() {
        // Runs that fit in a single album behave identically under both modes.
        let plan = singles(7, "same caption");
        let fill = group_into_albums(plan.clone(), AlbumSplitMode::Fill);
        let even = group_into_albums(plan, AlbumSplitMode::Even);
        assert_eq!(chunk_sizes(&fill), vec![7]);
        assert_eq!(chunk_sizes(&even), vec![7]);
    }

    #[test]
    fn group_into_albums_caption_change_flushes_the_run() {
        let mut plan = singles(3, "caption A");
        plan.extend(singles(4, "caption B"));
        let out = group_into_albums(plan, AlbumSplitMode::Even);
        assert_eq!(chunk_sizes(&out), vec![3, 4]);
    }

    #[test]
    fn group_into_albums_non_single_item_is_a_boundary() {
        let mut plan = singles(2, "caption A");
        plan.push(UploadItem::SuffixParts {
            display: "big.mkv".to_string(),
            parts: vec![part("big.mkv.00", ""), part("big.mkv.01", "")],
        });
        plan.extend(singles(3, "caption A"));
        let out = group_into_albums(plan, AlbumSplitMode::Even);
        // The SuffixParts boundary forces two separate same-caption runs even
        // though the caption string is identical on both sides.
        assert_eq!(out.len(), 3);
        assert!(matches!(&out[0], UploadItem::FileAlbum { parts } if parts.len() == 2));
        assert!(matches!(&out[1], UploadItem::SuffixParts { .. }));
        assert!(matches!(&out[2], UploadItem::FileAlbum { parts } if parts.len() == 3));
    }

    #[test]
    fn group_into_albums_single_item_run_stays_single() {
        let plan = singles(1, "only one");
        let out = group_into_albums(plan, AlbumSplitMode::Even);
        assert_eq!(out.len(), 1);
        assert!(matches!(&out[0], UploadItem::Single(_)));
    }
}
