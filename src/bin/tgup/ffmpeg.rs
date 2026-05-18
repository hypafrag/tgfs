//! ffmpeg/ffprobe integration: argv parsing, PATH lookup, single-shot encodes
//! (thumbnails), and the streaming encode-then-upload pipeline for videos.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context as _};
use grammers_client::Client;
use grammers_client::media::InputMedia;
use grammers_client::message::InputMessage;
use grammers_session::types::PeerRef;
use indicatif::ProgressBar;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use tgfs::config::MultipartPolicy;

use super::plan::{UploadItem, PART_MAX};
use super::progress::{set_label, set_spinner_style};
use super::upload::{
    finalize_big_file, upload_one_big_file, upload_thumb, video_attribute, RawBigFile, VideoInfo,
};

/// Split a config string into argv-style tokens. Honors double and single
/// quotes; backslash escapes the next character (outside single quotes). The
/// surrounding quote characters are stripped from the produced tokens.
pub fn split_shell_args(s: &str) -> anyhow::Result<Vec<String>> {
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

pub fn ffmpeg_in_path() -> bool {
    let path = match std::env::var_os("PATH") { Some(v) => v, None => return false };
    for p in std::env::split_paths(&path) {
        for name in ["ffmpeg", "ffmpeg.exe"] {
            let cand = p.join(name);
            if cand.is_file() { return true; }
        }
    }
    false
}

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
    set_label(progress_pb, progress_label.to_string());

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

async fn make_thumbnail_to_buffer(
    input: &Path,
    thumbnail_args: &[String],
    progress_pb: &ProgressBar,
) -> anyhow::Result<Vec<u8>> {
    let label = format!(
        "thumbnail {}",
        input.file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default(),
    );
    // `-f mjpeg pipe:1` emits raw JPEG bytes on stdout; combined with
    // `-frames:v 1` from the thumbnail args this is exactly one image.
    run_ffmpeg_to_buffer(input, thumbnail_args, &["-f", "mjpeg"], progress_pb, &label).await
}

/// Run `ffprobe` against a source video file and extract `(duration, width,
/// height)`. We deliberately probe the source rather than the encoded buffer:
/// fragmented MP4 over a pipe doesn't carry the total duration (ffprobe only
/// sees the first fragment), and our encoder preserves duration anyway.
/// Dimensions are sent purely as a preview hint — Telegram re-derives the
/// authoritative values from the uploaded bytes server-side.
async fn probe_video_file(path: &Path) -> Option<VideoInfo> {
    let out = Command::new("ffprobe")
        .args([
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height:format=duration",
            "-of", "default=nw=1",
        ])
        .arg(path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .ok()?;
    if !out.status.success() { return None; }
    let txt = String::from_utf8_lossy(&out.stdout);
    let mut w = 0i32;
    let mut h = 0i32;
    let mut d = 0.0f64;
    for line in txt.lines() {
        if let Some(v) = line.strip_prefix("width=") { w = v.trim().parse().unwrap_or(0); }
        else if let Some(v) = line.strip_prefix("height=") { h = v.trim().parse().unwrap_or(0); }
        else if let Some(v) = line.strip_prefix("duration=") { d = v.trim().parse().unwrap_or(0.0); }
    }
    if w <= 0 || h <= 0 { return None; }
    Some(VideoInfo {
        duration: Duration::from_secs_f64(d.max(0.0)),
        width: w,
        height: h,
    })
}

/// Run an `EncodedVideo` item: probe source, generate thumbnail, spawn ffmpeg,
/// then stream encoded bytes directly into `upload.saveBigFilePart` calls so
/// uploading begins as soon as the first 512 KB has been encoded.
pub async fn run_encoded_video(
    client: &Client,
    peer: PeerRef,
    encode_args: &[String],
    thumbnail_args: &[String],
    item: &UploadItem,
    file_pb: &ProgressBar,
    total_pb: &ProgressBar,
) -> anyhow::Result<()> {
    let (source, doc_filename, virtual_path, rel_dir, policy) = match item {
        UploadItem::EncodedVideo {
            source, doc_filename, virtual_path, rel_dir, policy, ..
        } => (
            source.clone(),
            doc_filename.clone(),
            virtual_path.clone(),
            rel_dir.clone(),
            *policy,
        ),
        _ => unreachable!(),
    };

    // Probe source for duration/dimensions (fragmented MP4 over pipe doesn't
    // report total duration, and Telegram re-derives dimensions anyway).
    let video_info = probe_video_file(&source).await;

    // Thumbnail.
    let thumb_bytes = make_thumbnail_to_buffer(&source, thumbnail_args, file_pb).await?;
    let thumb = upload_thumb(client, thumb_bytes, &doc_filename).await?;

    // Spawn ffmpeg; stdout = fragmented MP4 stream.
    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-y").arg("-nostdin")
        .arg("-loglevel").arg("error")
        .arg("-i").arg(&source);
    for a in encode_args { cmd.arg(a); }
    cmd.args(["-f", "mp4"]).arg("pipe:1");
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().context("failed to spawn ffmpeg")?;
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");

    // Spinner with bytes uploaded — total is unknown until ffmpeg finishes.
    // Peak buffered encoded data is one 512 KB SaveBigFilePart chunk.
    set_spinner_style(file_pb);
    set_label(file_pb, format!("uploading {}", doc_filename));
    file_pb.set_position(0);

    let mut files: Vec<RawBigFile> = Vec::new();
    let mut peek: Option<u8> = None;
    loop {
        let (raw, eof) =
            upload_one_big_file(client, &mut stdout, &mut peek, PART_MAX, file_pb, total_pb)
                .await?;
        if let Some(r) = raw { files.push(r); }
        if eof { break; }
        if files.len() > 1 && policy == MultipartPolicy::None {
            let _ = child.kill().await;
            bail!(
                "encoded '{}' exceeded 4 GiB but multipart_policy is `none`",
                doc_filename
            );
        }
    }

    // Verify ffmpeg exited cleanly.
    let status = child.wait().await.context("waiting for ffmpeg")?;
    if !status.success() {
        let mut err = String::new();
        stderr.read_to_string(&mut err).await.ok();
        bail!("ffmpeg exited with {}: {}", status, err.trim());
    }

    if files.is_empty() {
        bail!("ffmpeg produced no output for '{}'", source.display());
    }

    // Single-message case.
    if files.len() == 1 {
        let raw = files.into_iter().next().unwrap();
        let uploaded = finalize_big_file(&raw, doc_filename.clone());
        let caption = if !rel_dir.is_empty() {
            format!("path: {}/", rel_dir)
        } else {
            String::new()
        };
        let mut msg = InputMessage::new().text(caption).document(uploaded);
        if let Some(ref info) = video_info { msg = msg.attribute(video_attribute(info)); }
        msg = msg.thumbnail(thumb.clone());
        client.send_message(peer, msg).await.context("sending video message")?;
        return Ok(());
    }

    // Multipart case.
    if policy == MultipartPolicy::None {
        bail!(
            "encoded '{}' exceeded 4 GiB but multipart_policy is `none`",
            doc_filename
        );
    }
    let mut album_medias: Vec<InputMedia> = Vec::new();
    for (idx, raw) in files.iter().enumerate() {
        let part_name = format!("{}.{:02}", doc_filename, idx);
        let uploaded = finalize_big_file(raw, part_name.clone());
        let caption = match policy {
            MultipartPolicy::Suffix => {
                if idx == 0 { format!("path: {}", virtual_path) } else { String::new() }
            }
            MultipartPolicy::Album => format!("multipart:\npath: {}", virtual_path),
            MultipartPolicy::None => unreachable!(),
        };
        match policy {
            MultipartPolicy::Suffix => {
                let mut msg = InputMessage::new().text(caption).document(uploaded);
                if let Some(ref info) = video_info { msg = msg.attribute(video_attribute(info)); }
                msg = msg.thumbnail(thumb.clone());
                client
                    .send_message(peer, msg)
                    .await
                    .with_context(|| format!("sending '{}'", part_name))?;
            }
            MultipartPolicy::Album => {
                let mut media = InputMedia::new().caption(caption).document(uploaded);
                if let Some(ref info) = video_info { media = media.attribute(video_attribute(info)); }
                media = media.thumbnail(thumb.clone());
                album_medias.push(media);
            }
            MultipartPolicy::None => unreachable!(),
        }
    }
    if !album_medias.is_empty() {
        client.send_album(peer, album_medias).await.context("sending video album")?;
    }

    Ok(())
}
