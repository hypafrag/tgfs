//! Telegram upload primitives: streaming `UploadPart` messages and the
//! lower-level `SaveBigFilePart` plumbing used by the encoded-video path.

use std::io::SeekFrom;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context as _};
use log::{debug, warn};
use grammers_client::Client;
use grammers_client::media::{Attribute, InputMedia, Uploaded};
use grammers_client::message::InputMessage;
use grammers_client::peer::Peer;
use grammers_client::tl;
use grammers_session::types::PeerRef;
use indicatif::ProgressBar;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};

use super::ffmpeg::{ffmpeg_in_path, ffprobe_in_path, make_thumbnail_silent, probe_video_file};
use super::plan::{media_type_from_path, PartSource, UploadPart};

/// What kind of message a whole-file upload should be sent as. Per-chunk
/// uploads (multipart files split into 4 GiB pieces) always use
/// `Document` since a single chunk isn't a self-contained media file.
enum MediaKind {
    Document,
    Video(VideoInfo),
    Photo,
}

/// Best-effort thumbnail for a video file. Returns `None` when ffmpeg
/// isn't installed (we already warned about ffprobe earlier — silently
/// skip here) or when extraction failed for any other reason. Telegram
/// is happy without a thumbnail; we just won't get a preview frame.
async fn make_and_upload_thumb(
    client: &Client,
    path: &std::path::Path,
    doc_filename: &str,
) -> Option<Uploaded> {
    if !ffmpeg_in_path() { return None; }
    let bytes = make_thumbnail_silent(path).await.ok()?;
    if bytes.is_empty() { return None; }
    upload_thumb(client, bytes, doc_filename).await.ok()
}

/// Decide how to upload `part`. Returns `Document` when the part isn't a
/// whole-file upload (multipart chunk), when the MIME/extension isn't a
/// recognized image/video, or — for videos — when ffprobe isn't installed
/// and we have no way to render an inline preview safely.
async fn classify_whole_file(part: &UploadPart) -> MediaKind {
    let PartSource::File(path) = &part.src;
    let meta = match tokio::fs::metadata(path).await {
        Ok(m) => m,
        Err(_) => return MediaKind::Document,
    };
    if part.offset != 0 || part.size != meta.len() {
        return MediaKind::Document;
    }
    match media_type_from_path(path) {
        Some("image") => MediaKind::Photo,
        Some("video") => {
            // Prefer ffprobe-derived duration / width / height for inline
            // previews. When ffprobe isn't on PATH (or it fails on this
            // file), fall back to a zero-metadata VideoInfo derived from the
            // extension — Telegram still gets the `video` attribute and will
            // route it through the video player UI, just without a known
            // duration or aspect ratio.
            if ffprobe_in_path() {
                if let Some(info) = probe_video_file(path).await {
                    return MediaKind::Video(info);
                }
            }
            MediaKind::Video(VideoInfo {
                duration: std::time::Duration::ZERO,
                width: 0,
                height: 0,
                streamable: false,
            })
        }
        _ => MediaKind::Document,
    }
}
use super::progress::{fmt_mib, fmt_speed, set_bar_style, set_prefix_label, spawn_speed_ticker, ProgressReader, SliceReader, LABEL_WIDTH};

pub const TG_CHUNK: usize = 512 * 1024; // MTProto SaveBigFilePart chunk size
/// Maximum number of `SaveBigFilePart` tasks allowed in flight at once.
/// Caps the encode-buffer fill at `UPLOAD_CONCURRENCY × TG_CHUNK` bytes.
pub const UPLOAD_CONCURRENCY: usize = 4;

/// Metadata used to upload a part as a playable video message instead of a
/// document attachment.
#[derive(Clone)]
pub struct VideoInfo {
    pub duration: Duration,
    pub width: i32,
    pub height: i32,
    pub streamable: bool,
}

pub fn video_attribute(info: &VideoInfo) -> Attribute {
    Attribute::Video {
        round_message: false,
        supports_streaming: info.streamable,
        duration: info.duration,
        w: info.width,
        h: info.height,
    }
}

/// Open the byte stream for a part as an `AsyncRead` limited to `part.size`.
pub async fn open_part_stream(part: &UploadPart) -> anyhow::Result<Box<dyn AsyncRead + Send + Unpin>> {
    let PartSource::File(path) = &part.src;
    let mut f = File::open(path)
        .await
        .with_context(|| format!("opening '{}'", path.display()))?;
    if part.offset > 0 {
        f.seek(SeekFrom::Start(part.offset)).await?;
    }
    Ok(Box::new(f.take(part.size)))
}

pub async fn upload_part_as_message(
    client: &Client,
    peer: PeerRef,
    part: &UploadPart,
    thumb: Option<&Uploaded>,
    video: Option<&VideoInfo>,
    file_pb: &ProgressBar,
    total_pb: &ProgressBar,
) -> anyhow::Result<()> {
    set_bar_style(file_pb);
    file_pb.set_length(part.size);
    file_pb.set_position(0);
    set_prefix_label(file_pb, part.doc_filename.clone());
    file_pb.set_message(String::new());
    file_pb.reset_elapsed();
    let speed = spawn_speed_ticker(file_pb.clone());

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
    speed.abort();

    // Caller-supplied `video` (re-encode path) wins; otherwise auto-classify
    // the source as video/photo/document based on MIME + ffprobe so users
    // get inline previews instead of generic attachments.
    let auto = if video.is_none() {
        Some(classify_whole_file(part).await)
    } else {
        None
    };
    // For auto-classified videos, also generate a thumbnail with ffmpeg so
    // the Telegram client can show a preview frame. Caller-supplied `thumb`
    // wins (re-encode path already produced one); for everything else we
    // try to make one, silently degrading on failure.
    let auto_thumb = match (video, &auto) {
        (None, Some(MediaKind::Video(_))) => {
            let PartSource::File(path) = &part.src;
            make_and_upload_thumb(client, path, &part.doc_filename).await
        }
        _ => None,
    };
    let mut msg = InputMessage::new().text(part.caption.clone());
    msg = match (video, &auto) {
        (Some(info), _) => msg.document(uploaded).attribute(video_attribute(info)),
        (None, Some(MediaKind::Video(info))) => msg.document(uploaded).attribute(video_attribute(info)),
        (None, Some(MediaKind::Photo)) => msg.photo(uploaded),
        (None, _) => msg.file(uploaded),
    };
    if let Some(t) = thumb.or(auto_thumb.as_ref()) {
        msg = msg.thumbnail(t.clone());
    }
    client
        .send_message(peer, msg)
        .await
        .with_context(|| format!("sending message for '{}'", part.doc_filename))?;
    Ok(())
}

pub async fn upload_album(
    client: &Client,
    peer: PeerRef,
    parts: &[UploadPart],
    thumb: Option<&Uploaded>,
    video: Option<&VideoInfo>,
    file_pb: &ProgressBar,
    total_pb: &ProgressBar,
) -> anyhow::Result<()> {
    // Album-level pre-classification: probe every part up front. When the
    // caller didn't already pin the kind (re-encode path), and *all* parts
    // are videos or *all* parts are photos, render the whole album as that
    // single media type. Anything mixed (or other content) keeps the legacy
    // document-album behavior — Telegram rejects albums that mix documents
    // with photos/videos, so the "uniform or nothing" rule is the safe one.
    let auto_kinds: Option<Vec<MediaKind>> = if video.is_none() {
        let mut v = Vec::with_capacity(parts.len());
        for part in parts { v.push(classify_whole_file(part).await); }
        Some(v)
    } else { None };
    let album_mode = match &auto_kinds {
        Some(kinds) if kinds.iter().all(|k| matches!(k, MediaKind::Photo)) => AlbumMode::Photos,
        Some(kinds) if kinds.iter().all(|k| matches!(k, MediaKind::Video(_))) => AlbumMode::Videos,
        _ => AlbumMode::Documents,
    };

    let mut medias: Vec<InputMedia> = Vec::with_capacity(parts.len());
    for (i, part) in parts.iter().enumerate() {
        set_bar_style(file_pb);
        file_pb.set_length(part.size);
        file_pb.set_position(0);
        set_prefix_label(file_pb, part.doc_filename.clone());
        file_pb.set_message(String::new());
        file_pb.reset_elapsed();
        let speed = spawn_speed_ticker(file_pb.clone());

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
        speed.abort();
        let mut media = InputMedia::new().caption(part.caption.clone());
        let per_part_thumb = if matches!(album_mode, AlbumMode::Videos) && video.is_none() {
            let PartSource::File(p) = &part.src;
            make_and_upload_thumb(client, p, &part.doc_filename).await
        } else { None };
        media = match (video, album_mode) {
            (Some(info), _) => media.document(uploaded).attribute(video_attribute(info)),
            (None, AlbumMode::Photos) => media.photo(uploaded),
            (None, AlbumMode::Videos) => {
                let info = match &auto_kinds.as_ref().unwrap()[i] {
                    MediaKind::Video(v) => v,
                    _ => unreachable!("AlbumMode::Videos implies every kind is Video"),
                };
                media.document(uploaded).attribute(video_attribute(info))
            }
            (None, AlbumMode::Documents) => media.file(uploaded),
        };
        if let Some(t) = thumb.or(per_part_thumb.as_ref()) {
            media = media.thumbnail(t.clone());
        }
        medias.push(media);
    }
    client.send_album(peer, medias).await.context("sending album")?;
    Ok(())
}

#[derive(Clone, Copy)]
enum AlbumMode { Photos, Videos, Documents }

pub async fn resolve_channel_peer(client: &Client, channel_name: &str) -> anyhow::Result<PeerRef> {
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

pub async fn upload_thumb(client: &Client, bytes: Vec<u8>, name: &str) -> anyhow::Result<Uploaded> {
    let size = bytes.len();
    let arc = Arc::new(bytes);
    let mut reader = SliceReader { buf: arc, start: 0, end: size, pos: 0 };
    client
        .upload_stream(&mut reader, size, format!("{}.thumb.jpg", name))
        .await
        .context("uploading thumbnail")
}

/// One Telegram big-file's pre-`SendMedia` state: identified by `file_id`,
/// `parts` 512 KB chunks have been pushed via `SaveBigFilePart`. The display
/// name is chosen later, once we know whether this file is the only one
/// (clean `doc_filename`) or part N of a multipart upload (`doc.NN`).
pub struct RawBigFile {
    pub file_id: i64,
    pub parts: i32,
    #[allow(dead_code)]
    pub size: u64,
}

pub fn random_file_id() -> i64 {
    let mut b = [0u8; 8];
    getrandom::getrandom(&mut b).expect("getrandom");
    i64::from_le_bytes(b)
}

pub fn finalize_big_file(raw: &RawBigFile, name: String) -> Uploaded {
    Uploaded::from_raw(
        tl::types::InputFileBig {
            id: raw.file_id,
            parts: raw.parts,
            name,
        }
        .into(),
    )
}

/// Per-file sub-bars for the encoded-video upload path.
/// Passed to `upload_one_big_file` to track buffer fill and upload throughput.
pub struct VideoUploadBars {
    /// Bar showing combined bytes currently held in encode buffers
    /// (0 – UPLOAD_CONCURRENCY × TG_CHUNK = 4 MB).
    pub buf_pb: ProgressBar,
    /// Counter used purely for its `bytes_per_sec` readout: Telegram upload speed.
    pub upload_pb: ProgressBar,
    /// Bytes in complete (fully-filled) buffers waiting for SaveBigFilePart to finish.
    pub buf_fill: Arc<AtomicU64>,
    /// Bytes accumulated so far into the buffer currently being filled from the pipe.
    /// Reset to 0 each time a buffer is handed off for upload.
    pub partial_fill: Arc<AtomicU64>,
    /// Cumulative bytes confirmed uploaded to Telegram for this file.
    pub total_uploaded: Arc<AtomicU64>,
}

/// Read up to `max_bytes` from `reader` and push them to Telegram in
/// `TG_CHUNK`-sized parts via `upload.saveBigFilePart`. Returns the raw file
/// handle (file_id + part count) and whether the reader is at EOF.
///
/// `peek` carries a one-byte lookahead between successive calls: when this
/// function stops because `max_bytes` was reached (not EOF), it peeks one
/// extra byte from the stream so the next call knows there is more data;
/// that byte is prepended to the next file's first part.
///
/// `file_total_parts` on each `SaveBigFilePart` is set to the current 1-based
/// part index (i.e. "this is the last one so far"). Telegram only cross-checks
/// `InputFileBig.parts` at `SendMedia` time, so the final value attached when
/// the file is sent matches the parts we actually pushed.
pub async fn upload_one_big_file<R: AsyncRead + Unpin>(
    client: &Client,
    reader: &mut R,
    peek: &mut Option<u8>,
    max_bytes: u64,
    file_pb: &ProgressBar,
    total_pb: &ProgressBar,
    // When true, the uploader increments both file_pb and total_pb from
    // uploaded bytes. When false, an external driver (e.g. the ffmpeg
    // progress task) owns the bars and the uploader stays silent.
    update_progress: bool,
    video_bars: Option<&VideoUploadBars>,
) -> anyhow::Result<(Option<RawBigFile>, bool)> {
    let file_id = random_file_id();
    let mut part_idx: i32 = 0;
    let mut uploaded: u64 = 0;
    let mut eof = false;
    let mut scratch = [0u8; 64 * 1024];

    // Cap in-flight upload tasks to UPLOAD_CONCURRENCY: when the queue is full
    // we await the oldest task before spawning a new one, providing back-pressure
    // so the encoder cannot queue more than UPLOAD_CONCURRENCY × TG_CHUNK bytes.
    let mut pending: std::collections::VecDeque<tokio::task::JoinHandle<anyhow::Result<()>>> =
        std::collections::VecDeque::new();
    // Two alternating buffers: `cur_buf` holds the part we're about to send,
    // `next_buf` is filled while `cur_buf` is uploading.
    let mut cur_buf: Option<Vec<u8>> = None;

    loop {
        // Ensure cur_buf is filled.
        if cur_buf.is_none() {
            if uploaded >= max_bytes { break; }
            let remaining = max_bytes - uploaded;
            let target = std::cmp::min(TG_CHUNK as u64, remaining) as usize;
            let mut buf: Vec<u8> = Vec::with_capacity(target);
            if let Some(b) = peek.take() { buf.push(b); }
            while buf.len() < target {
                let want = std::cmp::min(scratch.len(), target - buf.len());
                let n = reader
                    .read(&mut scratch[..want])
                    .await
                    .context("reading ffmpeg stdout")?;
                if n == 0 { eof = true; break; }
                buf.extend_from_slice(&scratch[..n]);
                if let Some(vb) = video_bars {
                    vb.partial_fill.store(buf.len() as u64, Ordering::Relaxed);
                }
            }
            if buf.is_empty() { break; }
            cur_buf = Some(buf);
        }

        // Decide whether this cur_buf is the final part for the file.
        let buf = cur_buf.take().unwrap();
        let n = buf.len() as u64;
        let file_total_parts_param: i32;

        if eof {
            file_total_parts_param = part_idx + 1;
        } else if uploaded + n >= max_bytes {
            // At the per-file cap: peek one byte to determine if there's
            // more data (another file) or this is the final part.
            let mut one = [0u8; 1];
            let m = reader.read(&mut one).await.context("peeking ffmpeg stdout")?;
            if m == 0 {
                file_total_parts_param = part_idx + 1;
                eof = true;
            } else {
                // There's more data for the next file; stash the byte for
                // the caller and mark this part non-final.
                *peek = Some(one[0]);
                file_total_parts_param = -1;
            }
        } else {
            // Not at file cap: this cannot be the final part for the file.
            file_total_parts_param = -1;
        }

        // Track buffer fill: add this part's bytes before upload starts.
        // partial_fill is reset to 0 because this buffer is now complete.
        if let Some(vb) = video_bars {
            vb.partial_fill.store(0, Ordering::Relaxed);
            let new_fill = vb.buf_fill.fetch_add(n, Ordering::Relaxed) + n;
            vb.buf_pb.set_position(new_fill);
        }
        // Spawn upload for this part. Move `buf` into the task so we can
        // continue filling `next_buf` concurrently.
        let client_clone = client.clone();
        let file_id_c = file_id;
        let part_idx_c = part_idx;
        let tparam = file_total_parts_param;
        let vb_buf_fill = video_bars.map(|vb| vb.buf_fill.clone());
        let vb_buf_pb   = video_bars.map(|vb| vb.buf_pb.clone());
        let vb_upload_pb = video_bars.map(|vb| vb.upload_pb.clone());
        let vb_total_uploaded = video_bars.map(|vb| vb.total_uploaded.clone());
        let buf_max = (TG_CHUNK * UPLOAD_CONCURRENCY) as u64;
        let part_len = n;
        // Timestamp taken just before the RPC is dispatched so that elapsed
        // time measures pure network round-trip, not buffer-fill idle time.
        let t0 = Instant::now();
        let handle = tokio::spawn(async move {
            let res = client_clone
                .invoke(&tl::functions::upload::SaveBigFilePart {
                    file_id: file_id_c,
                    file_part: part_idx_c,
                    file_total_parts: tparam,
                    bytes: buf,
                })
                .await
                .with_context(|| format!("saveBigFilePart {}", part_idx_c))
                .and_then(|ok| {
                    if ok {
                        debug!("saveBigFilePart {} ok (file_id={}, total_parts={})", part_idx_c, file_id_c, tparam);
                        Ok(())
                    } else {
                        warn!("saveBigFilePart {} returned false (file_id={}, total_parts={})", part_idx_c, file_id_c, tparam);
                        Err(anyhow::anyhow!("saveBigFilePart {} returned false", part_idx_c))
                    }
                });
            if res.is_ok() {
                if let (Some(bf), Some(bp), Some(up), Some(tu)) =
                    (vb_buf_fill, vb_buf_pb, vb_upload_pb, vb_total_uploaded)
                {
                    let prev = bf.fetch_sub(part_len, Ordering::Relaxed);
                    let new_fill = prev.saturating_sub(part_len);
                    let processed = tu.fetch_add(part_len, Ordering::Relaxed) + part_len;
                    bp.set_position(new_fill);
                    bp.set_message(format!(
                        "{} / {}  (Σ {})",
                        fmt_mib(new_fill),
                        fmt_mib(buf_max),
                        fmt_mib(processed),
                    ));
                    let elapsed = t0.elapsed().as_secs_f64();
                    if elapsed > 0.0 {
                        up.set_message(format!(
                            "{:<w$} {}",
                            "upload speed",
                            fmt_speed(part_len as f64 / elapsed),
                            w = LABEL_WIDTH,
                        ));
                    }
                }
            }
            res
        });
        pending.push_back(handle);

        // Back-pressure: drain the oldest pending task if we've hit the cap.
        if pending.len() >= UPLOAD_CONCURRENCY {
            let oldest = pending.pop_front().unwrap();
            let res = oldest.await.context("join upload task")?;
            res?;
        }

        uploaded += n;
        if update_progress {
            total_pb.inc(n);
            if let Some(cur_len) = file_pb.length() {
                if uploaded > cur_len {
                    // Extend by 10 % headroom so the bar stays below 100 %
                    // for the simple-file fallback path.
                    file_pb.set_length(uploaded * 11 / 10);
                }
            }
            file_pb.set_position(uploaded);
        }
        part_idx += 1;

        if eof { break; }

        // Fill next_buf while the previous part is being uploaded to
        // overlap IO and network. Only read up to the remaining bytes for
        // this file to avoid stealing bytes from the next file.
        if uploaded < max_bytes {
            let remaining = max_bytes - uploaded;
            let target = std::cmp::min(TG_CHUNK as u64, remaining) as usize;
            let mut buf: Vec<u8> = Vec::with_capacity(target);
            while buf.len() < target {
                let want = std::cmp::min(scratch.len(), target - buf.len());
                let m = reader
                    .read(&mut scratch[..want])
                    .await
                    .context("reading ffmpeg stdout")?;
                if m == 0 { eof = true; break; }
                buf.extend_from_slice(&scratch[..m]);
                if let Some(vb) = video_bars {
                    vb.partial_fill.store(buf.len() as u64, Ordering::Relaxed);
                }
            }
            if buf.is_empty() {
                // No data for next part; continue loop which will see EOF.
                cur_buf = None;
            } else {
                // Place the freshly-read buffer into `cur_buf` for the next
                // iteration instead of using an intermediate `next_buf`.
                cur_buf = Some(buf);
            }
        }
    }

    // Await any remaining pending upload tasks and propagate their errors.
    for h in pending {
        let res = h.await.context("join upload task")?;
        res?;
    }

    

    if part_idx == 0 {
        return Ok((None, eof));
    }
    debug!("upload_one_big_file done: file_id={} parts={} bytes={}", file_id, part_idx, uploaded);
    Ok((Some(RawBigFile { file_id, parts: part_idx, size: uploaded }), eof))
}

// Testable helper: read parts from `reader` up to `max_bytes`, applying the
// same peek & last-part detection semantics used by the uploader. Returns
// a vector of (part_len, file_total_parts_param) and whether EOF was seen.
#[cfg(test)]
pub async fn collect_parts_for_test<R: AsyncRead + Unpin>(
    reader: &mut R,
    peek: &mut Option<u8>,
    max_bytes: u64,
) -> anyhow::Result<(Vec<(u64, i32)>, bool)> {
    let mut part_idx: i32 = 0;
    let mut uploaded: u64 = 0;
    let mut eof = false;
    let mut scratch = [0u8; 64 * 1024];
    let mut parts: Vec<(u64, i32)> = Vec::new();

    while uploaded < max_bytes {
        let remaining = max_bytes - uploaded;
        let target = std::cmp::min(TG_CHUNK as u64, remaining) as usize;
        let mut buf_len: usize = 0;
        if let Some(_) = peek.take() { buf_len = 1; }
        while buf_len < target {
            let want = std::cmp::min(scratch.len(), target - buf_len);
            let n = reader.read(&mut scratch[..want]).await.context("reading test input")?;
            if n == 0 { eof = true; break; }
            buf_len += n;
        }
        if buf_len == 0 { break; }
        let n = buf_len as u64;

        let mut file_total_parts_param: i32 = -1;
        if eof {
            file_total_parts_param = part_idx + 1;
        } else if uploaded + n >= max_bytes {
            let mut one = [0u8; 1];
            let m = reader.read(&mut one).await.context("peeking test input")?;
            if m == 0 {
                file_total_parts_param = part_idx + 1;
                eof = true;
            } else {
                *peek = Some(one[0]);
                file_total_parts_param = -1;
            }
        }

        parts.push((n, file_total_parts_param));
        uploaded += n;
        part_idx += 1;
        if eof { break; }
    }

    Ok((parts, eof))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_collect_single_exact_chunk() {
        let data = vec![0u8; TG_CHUNK];
        let mut cur = Cursor::new(data);
        let mut peek = None;
        let (parts, eof) = collect_parts_for_test(&mut cur, &mut peek, TG_CHUNK as u64).await.unwrap();
        assert!(eof);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].0, TG_CHUNK as u64);
        assert_eq!(parts[0].1, 1); // final part index = 1
        assert!(peek.is_none());
    }

    #[tokio::test]
    async fn test_collect_multiple_parts() {
        let total = TG_CHUNK * 3 + 1234;
        let data = vec![0u8; total];
        let mut cur = Cursor::new(data);
        let mut peek = None;
        let (parts, eof) = collect_parts_for_test(&mut cur, &mut peek, total as u64).await.unwrap();
        assert!(eof);
        assert_eq!(parts.len(), 4);
        for i in 0..3 {
            assert_eq!(parts[i].0, TG_CHUNK as u64);
            assert_eq!(parts[i].1, -1);
        }
        assert_eq!(parts[3].0, 1234);
        assert_eq!(parts[3].1, 4);
        assert!(peek.is_none());
    }

    #[tokio::test]
    async fn test_peek_across_file_cap() {
        // Create data longer than max_bytes to trigger peek at cap boundary.
        let max_bytes = TG_CHUNK as u64 * 2;
        let data = vec![0u8; (max_bytes + 10) as usize];
        let mut cur = Cursor::new(data);
        let mut peek = None;
        let (parts, eof) = collect_parts_for_test(&mut cur, &mut peek, max_bytes).await.unwrap();
        assert!(!eof);
        // Should have filled exactly max_bytes across parts.
        let sum: u64 = parts.iter().map(|p| p.0).sum();
        assert_eq!(sum, max_bytes);
        // The last part should have file_total_parts = -1 because more data exists
        assert_eq!(parts.last().unwrap().1, -1);
        assert!(peek.is_some());
    }
}

