//! ffmpeg/ffprobe integration: argv parsing, PATH lookup, single-shot encodes
//! (thumbnails), and the streaming encode-then-upload pipeline for videos.

use std::path::{Path, PathBuf};
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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncBufReadExt, BufReader};
use tokio::sync::Mutex as AsyncMutex;
use tokio::process::Command;

use tgfs::config::{EncodeArgs, MultipartPolicy, Streamification, Threads};

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

    let streamification = cfg.video.streamification;
    match codec.as_str() {
        "libx264" => {
            a.extend(["-preset".into(), cfg.video.libx264preset.clone()]);
            a.extend(["-profile:v".into(), "main".into()]);
            a.extend(["-level".into(), "4.1".into()]);
            a.extend(["-crf".into(), "23".into()]);
            if streamification != Streamification::None {
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
    match streamification {
        Streamification::Fmp4 => a.extend([
            "-movflags".into(),
            "+frag_keyframe+empty_moov+default_base_moof".into(),
        ]),
        Streamification::LeadingMoov => a.extend([
            "-movflags".into(),
            "+faststart".into(),
        ]),
        Streamification::None => {}
    }

    a.extend(["-r".into(), "24".into()]);
    if streamification != Streamification::None {
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

// fn shell_escape(s: &std::ffi::OsStr) -> String {
//     let s = s.to_string_lossy();
//     if s.chars().all(|c| c.is_ascii_alphanumeric() || "-_./=:+,@%".contains(c)) {
//         s.into_owned()
//     } else {
//         format!("'{}'", s.replace('\'', "'\\''"))
//     }
// }

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


/// Directory under which `LeadingMoov` encodes drop their scratch file before
/// uploading. Created on demand; the scratch file is deleted after upload.
const SCRATCH_DIR: &str = "/tmp/tgup";

fn scratch_path(name: &str) -> PathBuf {
    PathBuf::from(SCRATCH_DIR).join(name)
}

/// Spawn `ffmpeg` to encode `source` into the given output (file path or
/// `pipe:1`), parsing `-progress pipe:2` lines on stderr to drive `file_pb`
/// and `total_pb`. Returns the spawned child (with `stdout` already taken if
/// piped) plus a handle to the progress-task and shared stderr buffer for
/// error reporting after `child.wait()`.
struct FfmpegRun {
    child: tokio::process::Child,
    stdout: Option<tokio::process::ChildStdout>,
    progress: Option<tokio::task::JoinHandle<()>>,
    stderr_buf: Arc<AsyncMutex<String>>,
    total_advanced: Arc<AtomicU64>,
}

fn spawn_ffmpeg(
    source: &Path,
    encode_args: &[String],
    scale_args: &[String],
    output_arg: &str,
    pipe_stdout: bool,
    file_pb: &ProgressBar,
    total_pb: &ProgressBar,
    source_size: u64,
    duration_us: Option<u64>,
) -> anyhow::Result<FfmpegRun> {
    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-y").arg("-nostdin")
        .arg("-loglevel").arg("error")
        .arg("-i").arg(source);
    for a in encode_args.iter().chain(scale_args.iter()) { cmd.arg(a); }
    cmd.args(["-f", "mp4"]).arg(output_arg);
    cmd.arg("-progress").arg("pipe:2");
    if pipe_stdout { cmd.stdout(Stdio::piped()); } else { cmd.stdout(Stdio::null()); }
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().context("failed to spawn ffmpeg")?;
    let stdout = if pipe_stdout { Some(child.stdout.take().expect("piped stdout")) } else { None };
    let stderr = child.stderr.take().expect("piped stderr");

    let stderr_buf = Arc::new(AsyncMutex::new(String::new()));
    let total_advanced = Arc::new(AtomicU64::new(0));

    let progress = if let Some(dur_us) = duration_us {
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
        // Duration unknown: just drain stderr for error reporting.
        let stderr_buf_c = stderr_buf.clone();
        tokio::spawn(async move {
            let mut s = String::new();
            let mut r = stderr;
            let _ = r.read_to_string(&mut s).await;
            *stderr_buf_c.lock().await = s;
        });
        None
    };

    Ok(FfmpegRun { child, stdout, progress, stderr_buf, total_advanced })
}

/// Run an `EncodedVideo` item: probe source, generate thumbnail, encode with
/// ffmpeg, then upload.
///
/// - `Fmp4` / `None`: ffmpeg writes to `pipe:1` and the uploader streams bytes
///   concurrently with encoding.
/// - `LeadingMoov`: ffmpeg needs a seekable output for the `+faststart`
///   shuffle, so it writes to a scratch file under [`SCRATCH_DIR`]; once
///   ffmpeg exits, tgup opens that file and uploads, then deletes it.
pub async fn run_encoded_video(
    client: &Client,
    peer: PeerRef,
    streamification: Streamification,
    encode_args: &[String],
    vres: u32,
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

    let video_info = probe_video_file(&source).await.map(|mut v| {
        v.streamable = streamification != Streamification::None;
        v
    });

    // Compute scale filter from probed dimensions. Only downscale; if the
    // source height is already ≤ vres, pass no -vf filter at all.
    let scale_args: Vec<String> = match &video_info {
        Some(vi) if vi.height > vres as i32 => {
            let new_h = (vres / 2) * 2;
            let new_w = (vi.width as u64 * vres as u64 / vi.height as u64) as u32;
            let new_w = (new_w / 2) * 2;
            vec!["-vf".into(), format!("scale={new_w}:{new_h}")]
        }
        _ => vec![],
    };

    let thumb_bytes = make_thumbnail_to_buffer(&source, thumbnail_args, file_pb).await?;
    let thumb = upload_thumb(client, thumb_bytes, &doc_filename).await?;

    let duration_us: Option<u64> = video_info.as_ref()
        .map(|v| v.duration.as_micros() as u64)
        .filter(|&d| d > 0);

    // Prepare the file_pb for the encode/upload phase.
    set_bar_style(file_pb);
    set_label(file_pb, format!("encoding {}", doc_filename));
    file_pb.set_length(source_size.max(1));
    file_pb.set_position(0);

    // Sub-bars for encode-buffer fill, encode throughput, upload throughput.
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

    // Branch on streamification to build the upload reader.
    // - Pipe stream (Fmp4 / None): encode and upload concurrently — ffmpeg
    //   writes to pipe:1, the uploader reads it as bytes arrive.
    // - Scratch file (LeadingMoov): faststart needs a seekable output, so
    //   ffmpeg writes to a scratch file under SCRATCH_DIR; once it exits we
    //   open that file and stream it to the uploader, then delete it.
    let scratch: Option<PathBuf> = match streamification {
        Streamification::LeadingMoov => {
            std::fs::create_dir_all(SCRATCH_DIR)
                .with_context(|| format!("create_dir_all {SCRATCH_DIR}"))?;
            Some(scratch_path(&doc_filename))
        }
        _ => None,
    };

    let (output_arg, pipe_stdout) = match &scratch {
        Some(p) => (p.to_string_lossy().into_owned(), false),
        None    => ("pipe:1".to_string(), true),
    };

    let mut run = spawn_ffmpeg(
        &source, encode_args, &scale_args, &output_arg, pipe_stdout,
        file_pb, total_pb, source_size, duration_us,
    )?;

    let mut files: Vec<RawBigFile> = Vec::new();

    if pipe_stdout {
        // Streamed path: upload concurrently with encoding.
        let upload_reader: Box<dyn AsyncRead + Unpin + Send> =
            Box::new(run.stdout.take().expect("pipe:1 stdout"));
        let mut tracked = SpeedReader { inner: upload_reader, pb: encode_pb.clone() };

        let uploader_drives = run.progress.is_none();
        let mut peek: Option<u8> = None;
        loop {
            let (raw, eof) = upload_one_big_file(
                client, &mut tracked, &mut peek, PART_MAX,
                file_pb, total_pb, uploader_drives, Some(&video_bars),
            ).await?;
            if let Some(r) = raw { files.push(r); }
            if eof { break; }
            if files.len() > 1 && policy == MultipartPolicy::None {
                let _ = run.child.kill().await;
                bail!("encoded '{}' exceeded 4 GiB but multipart_policy is `none`", doc_filename);
            }
        }
    }

    let status = run.child.wait().await.context("waiting for ffmpeg")?;
    if let Some(h) = run.progress {
        let _ = h.await;
        let advanced = run.total_advanced.load(std::sync::atomic::Ordering::Relaxed);
        total_pb.inc(source_size.saturating_sub(advanced));
    }
    if !status.success() {
        buf_tick_handle.abort();
        let _ = buf_tick_handle.await;
        encode_pb.finish_and_clear();
        buf_pb.finish_and_clear();
        upload_pb.finish_and_clear();
        if let Some(p) = scratch.as_ref() { let _ = tokio::fs::remove_file(p).await; }
        let err = run.stderr_buf.lock().await.clone();
        bail!("ffmpeg exited with {}: {}", status, err.trim());
    }

    // Scratch-file path: encode is done, now open the file and upload it.
    if let Some(ref path) = scratch {
        set_label(file_pb, format!("uploading {}", doc_filename));
        file_pb.set_position(0);
        let f = tokio::fs::File::open(path).await
            .with_context(|| format!("opening scratch file {}", path.display()))?;
        let upload_reader: Box<dyn AsyncRead + Unpin + Send> = Box::new(f);
        let mut tracked = SpeedReader { inner: upload_reader, pb: encode_pb.clone() };
        // Uploader drives file_pb (encode phase already credited total_pb).
        let mut peek: Option<u8> = None;
        loop {
            let (raw, eof) = upload_one_big_file(
                client, &mut tracked, &mut peek, PART_MAX,
                file_pb, total_pb, true, Some(&video_bars),
            ).await?;
            if let Some(r) = raw { files.push(r); }
            if eof { break; }
            if files.len() > 1 && policy == MultipartPolicy::None {
                let _ = tokio::fs::remove_file(path).await;
                bail!("encoded '{}' exceeded 4 GiB but multipart_policy is `none`", doc_filename);
            }
        }
        // Cleanup scratch regardless of further outcomes.
        let _ = tokio::fs::remove_file(path).await;
    }

    buf_tick_handle.abort();
    let _ = buf_tick_handle.await;

    file_pb.set_length(source_size.max(1));
    file_pb.set_position(source_size);
    encode_pb.finish_and_clear();
    buf_pb.finish_and_clear();
    upload_pb.finish_and_clear();

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
