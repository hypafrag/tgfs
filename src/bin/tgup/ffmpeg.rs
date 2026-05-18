//! ffmpeg/ffprobe integration: argv parsing, PATH lookup, single-shot encodes
//! (thumbnails), and the streaming encode-then-upload pipeline for videos.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use anyhow::{bail, Context as _};
use grammers_client::Client;
use grammers_client::media::InputMedia;
use grammers_client::message::InputMessage;
use grammers_session::types::PeerRef;
use indicatif::{MultiProgress, ProgressBar};
use tokio::io::{AsyncReadExt, AsyncBufReadExt, BufReader};
use tokio::sync::Mutex as AsyncMutex;
use tokio::process::Command;

use tgfs::config::{EncodeArgs, MultipartPolicy, Threads};

use super::plan::{UploadItem, PART_MAX};
use super::progress::{fmt_mib, set_label, set_bar_style, set_buffer_bar_style, set_spinner_style, set_throughput_style, set_manual_speed_style, SpeedReader};
use super::upload::{
    finalize_big_file, upload_one_big_file, upload_thumb, video_attribute, RawBigFile, TG_CHUNK,
    UPLOAD_CONCURRENCY, VideoInfo, VideoUploadBars,
};

/// Build the ffmpeg argv list (everything between `-i <input>` and `pipe:1`)
/// from the structured config. Tuned for streaming and seekability:
///
/// * Fragmented MP4 (`+frag_keyframe+empty_moov+default_base_moof`) — moov
///   atom up front, no seek-back required, works over `pipe:1`.
/// * 24 fps with a 2-second GOP (`-g 48`) — coarse enough to keep size
///   sensible, fine enough for usable seeking.
/// * libx264: `-sc_threshold 0` disables scene-change keyframe insertion so
///   the GOP cadence stays predictable. CRF 23 / profile main / level 4.1
///   are baseline H.264 settings widely playable in browsers.
/// * Width: `scale=-2:H:force_original_aspect_ratio=decrease` aims for an
///   even width via the `-2` hint, but `force_original_aspect_ratio=decrease`
///   can override that and produce odd widths (e.g. 1088x1080 → 725x720)
///   which libx264 rejects. The trailing
///   `scale=trunc(iw/2)*2:trunc(ih/2)*2` re-evens both dimensions.
pub fn build_encode_args(cfg: &EncodeArgs) -> Vec<String> {
    let mut a: Vec<String> = Vec::new();

    let threads = match cfg.threads {
        Threads::Auto => std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2),
        Threads::Count(n) => n as usize,
    };
    a.extend(["-threads".into(), threads.to_string()]);

    let codec = if cfg.video.codec == "auto" {
        if cfg!(target_os = "macos") { "h264_videotoolbox" } else { "libx264" }.to_string()
    } else {
        cfg.video.codec.clone()
    };
    a.extend(["-c:v".into(), codec.clone()]);

    match codec.as_str() {
        "libx264" => {
            a.extend(["-preset".into(), cfg.video.libx264preset.clone()]);
            a.extend(["-profile:v".into(), "main".into()]);
            a.extend(["-level".into(), "4.1".into()]);
            a.extend(["-crf".into(), "23".into()]);
            if cfg.video.streamable {
                // Keep keyframes only at the regular GOP boundary so seeking
                // lands on predictable offsets.
                a.extend(["-sc_threshold".into(), "0".into()]);
            }
        }
        "h264_videotoolbox" => {
            // videotoolbox has no CRF; -q:v 50 is a reasonable mid-quality
            // VBR target. Profile main keeps it browser-friendly.
            a.extend(["-profile:v".into(), "main".into()]);
            a.extend(["-q:v".into(), "50".into()]);
        }
        _ => {}
    }

    a.extend(["-pix_fmt".into(), "yuv420p".into()]);
    if cfg.video.streamable {
        a.extend([
            "-movflags".into(),
            "+frag_keyframe+empty_moov+default_base_moof".into(),
        ]);
    }

    a.extend([
        "-vf".into(),
        format!(
            "scale=-2:{vres}:force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2",
            vres = cfg.video.vres,
        ),
    ]);

    a.extend(["-r".into(), "24".into()]);
    if cfg.video.streamable {
        a.extend(["-g".into(), "48".into()]);
    }

    a.extend(["-c:a".into(), cfg.audio.codec.clone()]);
    a.extend(["-b:a".into(), cfg.audio.bitrate.clone()]);
    a.extend(["-ar".into(), cfg.audio.sample_rate.to_string()]);

    a
}

/// Hardcoded thumbnail-extraction args. Picks a representative frame
/// (`thumbnail=100` evaluates 100 frames and keeps the most distinctive)
/// then downscales to fit a 320x320 box without upscaling.
pub fn thumbnail_args() -> Vec<String> {
    [
        "-vf",
        "thumbnail=100,scale=320:320:force_original_aspect_ratio=decrease,hue=s=0",
        "-frames:v",
        "1",
        "-q:v",
        "5",
    ]
    .into_iter()
    .map(String::from)
    .collect()
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
        streamable: true, // filled in by caller
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
    mp: &MultiProgress,
    file_pb: &ProgressBar,
    total_pb: &ProgressBar,
) -> anyhow::Result<()> {
    let (source, doc_filename, virtual_path, rel_dir, policy, source_size) = match item {
        UploadItem::EncodedVideo {
            source, doc_filename, virtual_path, rel_dir, policy, source_size, ..
        } => (
            source.clone(),
            doc_filename.clone(),
            virtual_path.clone(),
            rel_dir.clone(),
            *policy,
            *source_size,
        ),
        _ => unreachable!(),
    };

    // Probe source for duration/dimensions.
    let video_info = probe_video_file(&source).await.map(|mut v| {
        v.streamable = encode_args.iter().any(|a| a.contains("frag_keyframe"));
        v
    });

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
    // -progress pipe:2 makes ffmpeg emit key=value progress lines on stderr
    // so we can map encoding time to source-file position without touching
    // the encoded output stream.
    cmd.arg("-progress").arg("pipe:2");
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().context("failed to spawn ffmpeg")?;
    let raw_stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");

    // Duration in microseconds — denominator for mapping out_time_us to a
    // fraction of the source file. Note: despite the _ms suffix, ffmpeg's
    // out_time_ms carries the same microsecond value as out_time_us.
    let duration_us = video_info.as_ref()
        .map(|v| v.duration.as_micros() as u64)
        .filter(|&d| d > 0);

    // file_pb length = source_size; position = (out_time_us / duration_us)
    // * source_size, giving "how far through the source file ffmpeg has read".
    set_bar_style(file_pb);
    set_label(file_pb, format!("uploading {}", doc_filename));
    file_pb.set_length(source_size.max(1));
    file_pb.set_position(0);

    // Sub-bars inserted between file_pb and total_pb for the duration of the
    // encode+upload: buffer fill level, encode throughput, upload throughput.
    let buf_pb = mp.insert_after(file_pb, ProgressBar::new((TG_CHUNK * UPLOAD_CONCURRENCY) as u64));
    set_buffer_bar_style(&buf_pb);
    let encode_pb = mp.insert_after(&buf_pb, ProgressBar::new(0));
    set_throughput_style(&encode_pb, "encode speed");
    let upload_pb = mp.insert_after(&encode_pb, ProgressBar::new(0));
    set_manual_speed_style(&upload_pb, "upload speed");
    let video_bars = VideoUploadBars {
        buf_pb: buf_pb.clone(),
        upload_pb: upload_pb.clone(),
        buf_fill: Arc::new(AtomicU64::new(0)),
        partial_fill: Arc::new(AtomicU64::new(0)),
        total_uploaded: Arc::new(AtomicU64::new(0)),
    };
    // Periodically refresh the buffer-fill bar so it updates at ~1 Hz even
    // when no fill/drain events fire (e.g. encoder is slower than uploader).
    let buf_tick_fill     = video_bars.buf_fill.clone();
    let buf_tick_partial  = video_bars.partial_fill.clone();
    let buf_tick_uploaded = video_bars.total_uploaded.clone();
    let buf_tick_pb       = buf_pb.clone();
    let buf_max = (TG_CHUNK * UPLOAD_CONCURRENCY) as u64;
    let buf_tick_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let fill = buf_tick_fill.load(std::sync::atomic::Ordering::Relaxed)
                + buf_tick_partial.load(std::sync::atomic::Ordering::Relaxed);
            let processed = buf_tick_uploaded.load(std::sync::atomic::Ordering::Relaxed);
            buf_tick_pb.set_position(fill);
            buf_tick_pb.set_message(format!(
                "{} / {}  (processed: {})",
                fmt_mib(fill),
                fmt_mib(buf_max),
                fmt_mib(processed),
            ));
        }
    });
    // Wrap raw_stdout so each byte read from ffmpeg also increments encode_pb.
    let mut tracked_stdout = SpeedReader { inner: raw_stdout, pb: encode_pb.clone() };

    // Shared stderr buffer for error reporting after child.wait().
    let stderr_buf = Arc::new(AsyncMutex::new(String::new()));
    // Bytes credited to total_pb by the progress task; used to top up the
    // remainder so this file always contributes exactly source_size.
    let total_advanced = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let progress_handle: Option<tokio::task::JoinHandle<()>> =
        if let Some(dur_us) = duration_us {
            let file_pb_c = file_pb.clone();
            let total_pb_c = total_pb.clone();
            let stderr_buf_c = stderr_buf.clone();
            let total_adv_c = total_advanced.clone();
            let ss = source_size;
            Some(tokio::spawn(async move {
                let mut last_t: u64 = 0;
                let mut collected = String::new();
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    collected.push_str(&line);
                    collected.push('\n');
                    if let Some(v) = line.strip_prefix("out_time_us=") {
                        if let Ok(t) = v.trim().parse::<u64>() {
                            if t > last_t {
                                let delta_us = t - last_t;
                                // Absolute position avoids fp drift.
                                let pos = ((t as f64 / dur_us as f64) * ss as f64) as u64;
                                let delta_bytes =
                                    (delta_us as f64 / dur_us as f64 * ss as f64) as u64;
                                file_pb_c.set_position(pos.min(ss));
                                total_pb_c.inc(delta_bytes);
                                total_adv_c.fetch_add(
                                    delta_bytes,
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                                last_t = t;
                            }
                        }
                    }
                }
                *stderr_buf_c.lock().await = collected;
            }))
        } else {
            // Duration unknown: drain stderr for error reporting; uploader
            // drives the bars as a best-effort fallback.
            let stderr_buf_c = stderr_buf.clone();
            tokio::spawn(async move {
                let mut s = String::new();
                let mut r = stderr;
                let _ = r.read_to_string(&mut s).await;
                *stderr_buf_c.lock().await = s;
            });
            None
        };

    // When the progress task owns the bars, tell the uploader not to touch
    // them. When duration is unknown, the uploader drives them as fallback.
    let uploader_drives = progress_handle.is_none();

    let mut files: Vec<RawBigFile> = Vec::new();
    let mut peek: Option<u8> = None;
    loop {
        let (raw, eof) = upload_one_big_file(
            client, &mut tracked_stdout, &mut peek, PART_MAX,
            file_pb, total_pb, uploader_drives, Some(&video_bars),
        ).await?;
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

    buf_tick_handle.abort();
    let _ = buf_tick_handle.await;

    // Reap ffmpeg. After it exits its stderr pipe closes, so the progress
    // task drains to completion.
    let status = child.wait().await.context("waiting for ffmpeg")?;

    if let Some(h) = progress_handle {
        // Consume any final progress lines (e.g. the progress=end block).
        let _ = h.await;
        // Top up total_pb so this file contributes exactly source_size bytes,
        // regardless of fp rounding or out_time_us not reaching duration_us.
        let advanced = total_advanced.load(std::sync::atomic::Ordering::Relaxed);
        total_pb.inc(source_size.saturating_sub(advanced));
    }
    // Snap file bar to 100 %.
    file_pb.set_length(source_size.max(1));
    file_pb.set_position(source_size);
    encode_pb.finish_and_clear();
    buf_pb.finish_and_clear();
    upload_pb.finish_and_clear();

    if !status.success() {
        let err = stderr_buf.lock().await.clone();
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
