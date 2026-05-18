//! Telegram upload primitives: streaming `UploadPart` messages and the
//! lower-level `SaveBigFilePart` plumbing used by the encoded-video path.

use std::io::SeekFrom;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context as _};
use grammers_client::Client;
use grammers_client::media::{Attribute, InputMedia, Uploaded};
use grammers_client::message::InputMessage;
use grammers_client::peer::Peer;
use grammers_client::tl;
use grammers_session::types::PeerRef;
use indicatif::ProgressBar;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};

use super::plan::{PartSource, UploadPart};
use super::progress::{set_bar_style, set_label, ProgressReader, SliceReader};

pub const TG_CHUNK: usize = 512 * 1024; // MTProto SaveBigFilePart chunk size

/// Metadata used to upload a part as a playable video message instead of a
/// document attachment.
#[derive(Clone)]
pub struct VideoInfo {
    pub duration: Duration,
    pub width: i32,
    pub height: i32,
}

pub fn video_attribute(info: &VideoInfo) -> Attribute {
    Attribute::Video {
        round_message: false,
        supports_streaming: true,
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
    set_label(file_pb, part.doc_filename.clone());

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

    let mut msg = InputMessage::new().text(part.caption.clone());
    msg = match video {
        Some(info) => msg.document(uploaded).attribute(video_attribute(info)),
        None => msg.file(uploaded),
    };
    if let Some(t) = thumb {
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
    let mut medias: Vec<InputMedia> = Vec::with_capacity(parts.len());
    for part in parts {
        set_bar_style(file_pb);
        file_pb.set_length(part.size);
        file_pb.set_position(0);
        set_label(file_pb, part.doc_filename.clone());

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
        let mut media = InputMedia::new().caption(part.caption.clone());
        media = match video {
            Some(info) => media.document(uploaded).attribute(video_attribute(info)),
            None => media.file(uploaded),
        };
        if let Some(t) = thumb {
            media = media.thumbnail(t.clone());
        }
        medias.push(media);
    }
    client.send_album(peer, medias).await.context("sending album")?;
    Ok(())
}

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
    pb: &ProgressBar,
    total_pb: &ProgressBar,
) -> anyhow::Result<(Option<RawBigFile>, bool)> {
    let file_id = random_file_id();
    let mut part_idx: i32 = 0;
    let mut uploaded: u64 = 0;
    let mut eof = false;
    let mut scratch = [0u8; 64 * 1024];

    while uploaded < max_bytes {
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
        }
        if buf.is_empty() { break; }

        let n = buf.len() as u64;
        client
            .invoke(&tl::functions::upload::SaveBigFilePart {
                file_id,
                file_part: part_idx,
                file_total_parts: part_idx + 1,
                bytes: buf,
            })
            .await
            .with_context(|| format!("saveBigFilePart {}", part_idx))?;
        uploaded += n;
        pb.inc(n);
        total_pb.inc(n);
        part_idx += 1;
        if eof { break; }
    }

    // Filled the file without seeing EOF — peek one byte so the caller can
    // tell whether there's another file coming.
    if !eof && uploaded >= max_bytes {
        let mut one = [0u8; 1];
        let n = reader.read(&mut one).await.context("peeking ffmpeg stdout")?;
        if n == 0 { eof = true; } else { *peek = Some(one[0]); }
    }

    if part_idx == 0 {
        return Ok((None, eof));
    }
    Ok((Some(RawBigFile { file_id, parts: part_idx }), eof))
}

