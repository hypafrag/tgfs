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
use super::progress::{
    fmt_mib, fmt_speed, set_label, set_prefix_label, set_bar_style, spawn_speed_ticker,
    set_buffer_bar_style, set_spinner_style, set_throughput_style, set_manual_speed_style,
    ScaledProgressReader, SpeedReader,
};
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

pub fn ffmpeg_in_path() -> bool { tool_in_path("ffmpeg") }
pub fn ffprobe_in_path() -> bool { tool_in_path("ffprobe") }

fn tool_in_path(name: &str) -> bool {
    let path = match std::env::var_os("PATH") { Some(v) => v, None => return false };
    let exe = format!("{name}.exe");
    for p in std::env::split_paths(&path) {
        if p.join(name).is_file() || p.join(&exe).is_file() { return true; }
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

/// Headless thumbnail extraction — same `-f mjpeg pipe:1` invocation as the
/// re-encode path but without touching any progress bar. Used by the plain
/// upload path to attach a thumbnail to videos that weren't going through
/// ffmpeg in the first place. Returns the raw JPEG bytes (suitable for
/// `upload_thumb`).
pub async fn make_thumbnail_silent(input: &Path) -> anyhow::Result<Vec<u8>> {
    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-y").arg("-nostdin")
        .arg("-loglevel").arg("error")
        .arg("-i").arg(input);
    for a in thumbnail_args() { cmd.arg(a); }
    cmd.arg("-f").arg("mjpeg").arg("pipe:1");
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().context("failed to spawn ffmpeg for thumbnail")?;
    let mut stdout = child.stdout.take().expect("piped");
    let mut stderr = child.stderr.take().expect("piped");
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let n = stdout.read(&mut chunk).await.context("reading ffmpeg thumbnail stdout")?;
        if n == 0 { break; }
        buf.extend_from_slice(&chunk[..n]);
    }
    let status = child.wait().await.context("waiting for ffmpeg thumbnail")?;
    if !status.success() {
        let mut err = String::new();
        stderr.read_to_string(&mut err).await.ok();
        bail!("ffmpeg thumbnail exited with status {}: {}", status, err.trim());
    }
    Ok(buf)
}

/// Run `ffprobe` against a source video file and extract `(duration, width,
/// height)`. We deliberately probe the source rather than the encoded buffer:
/// fragmented MP4 over a pipe doesn't carry the total duration (ffprobe only
/// sees the first fragment), and our encoder preserves duration anyway.
/// Dimensions are sent purely as a preview hint — Telegram re-derives the
/// authoritative values from the uploaded bytes server-side.
pub async fn probe_video_file(path: &Path) -> Option<VideoInfo> {
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
pub const SCRATCH_DIR: &str = "/tmp/tgup";

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
    file_budget: u64,
    total_budget: u64,
    duration_us: Option<u64>,
    encoded_size: Option<Arc<AtomicU64>>,
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
        let encoded_size = encoded_size.clone();
        let fb = file_budget;
        let tb = total_budget;
        Some(tokio::spawn(async move {
            let mut last_t: u64 = 0;
            let mut last_total_size: u64 = 0;
            let mut collected = String::new();
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                collected.push_str(&line);
                collected.push('\n');
                if let Some(v) = line.strip_prefix("out_time_us=") {
                    if let Ok(t) = v.trim().parse::<u64>() {
                        if t > last_t {
                            let delta_us = t - last_t;
                            // file_pb: 0 → file_budget (per-file progress shown as 0–100%).
                            let pos_file = ((t as f64 / dur_us as f64) * fb as f64) as u64;
                            file_pb_c.set_position(pos_file.min(fb));
                            // total_pb: scaled to total_budget (which can be less than
                            // file_budget for LeadingMoov, where encode = half the file).
                            let delta_total =
                                (delta_us as f64 / dur_us as f64 * tb as f64) as u64;
                            total_pb_c.inc(delta_total);
                            total_adv_c.fetch_add(
                                delta_total,
                                std::sync::atomic::Ordering::Relaxed,
                            );
                            last_t = t;
                        }
                    }
                } else if let Some(v) = line.strip_prefix("total_size=") {
                    // ffmpeg emits "total_size=N" on each progress block. For
                    // pipe outputs this is "N/A" — parsing fails and we leave
                    // the counter alone, which is fine.
                    if let Ok(n) = v.trim().parse::<u64>() {
                        // Track delta locally so the shared counter stays
                        // monotonic across files in the pipeline (each
                        // ffmpeg invocation restarts total_size at 0).
                        let delta = n.saturating_sub(last_total_size);
                        last_total_size = n;
                        if delta > 0 {
                            if let Some(ref c) = encoded_size {
                                c.fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
                            }
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
    set_prefix_label(file_pb, format!("encoding {}", doc_filename));
    file_pb.set_message(String::new());
    file_pb.reset_elapsed();
    let file_speed_h = spawn_speed_ticker(file_pb.clone());
    file_pb.set_length(source_size.max(1));
    file_pb.set_position(0);

    // LeadingMoov is encode-then-upload. file_pb shows 0–100% of each phase
    // (the same bar is reused for both); total_pb gets source_size/2 from
    // each phase so the file contributes source_size overall. For Fmp4 the
    // two budgets coincide.
    let two_phase = matches!(streamification, Streamification::LeadingMoov);
    let total_budget_encode: u64 = if two_phase { source_size / 2 } else { source_size };

    // Sub-bars for encode-buffer fill, encode throughput, upload throughput.
    let buf_pb = mp.insert_after(file_pb, ProgressBar::new((TG_CHUNK * UPLOAD_CONCURRENCY) as u64));
    set_buffer_bar_style(&buf_pb);
    // Encode-speed / upload-speed sub-bars only make sense when encode and
    // upload run concurrently (Fmp4). For LeadingMoov they're hidden.
    let (encode_pb, upload_pb) = if two_phase {
        (ProgressBar::hidden(), ProgressBar::hidden())
    } else {
        let e = mp.insert_after(&buf_pb, ProgressBar::new(0));
        set_throughput_style(&e, "encode speed");
        let u = mp.insert_after(&e, ProgressBar::new(0));
        set_manual_speed_style(&u, "upload speed");
        (e, u)
    };
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
                "{} / {}  (Σ {})",
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
        file_pb, total_pb,
        source_size, total_budget_encode, duration_us,
        None,
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
        total_pb.inc(total_budget_encode.saturating_sub(advanced));
        file_pb.set_position(source_size);
    }
    if !status.success() {
        buf_tick_handle.abort();
        let _ = buf_tick_handle.await;
        file_speed_h.abort();
        encode_pb.finish_and_clear();
        buf_pb.finish_and_clear();
        upload_pb.finish_and_clear();
        if let Some(p) = scratch.as_ref() { let _ = tokio::fs::remove_file(p).await; }
        let err = run.stderr_buf.lock().await.clone();
        bail!("ffmpeg exited with {}: {}", status, err.trim());
    }

    // Scratch-file path: encode is done, now open the file and upload it.
    if let Some(ref path) = scratch {
        // Re-use file_pb as the upload bar (single-file LeadingMoov, e.g.
        // through this entry point — for multi-file pipelining there's a
        // separate code path with its own upload bar).
        let scratch_size = tokio::fs::metadata(path).await
            .with_context(|| format!("stat scratch file {}", path.display()))?
            .len();
        set_prefix_label(file_pb, format!("uploading {}", doc_filename));
        file_pb.set_length(scratch_size.max(1));
        file_pb.set_position(0);
        // Restart the speed window so upload throughput doesn't get diluted
        // by the (much-slower) preceding encode phase.
        file_pb.reset_elapsed();
        let upload_total_budget = source_size.saturating_sub(total_budget_encode);
        let f = tokio::fs::File::open(path).await
            .with_context(|| format!("opening scratch file {}", path.display()))?;
        let mut tracked = ScaledProgressReader {
            inner: f,
            file_pb: file_pb.clone(),
            total_pb: total_pb.clone(),
            inner_size: scratch_size,
            total_budget: upload_total_budget,
            bytes_read: 0,
            total_credited: 0,
            shared_total: None,
        };
        let mut peek: Option<u8> = None;
        loop {
            let (raw, eof) = upload_one_big_file(
                client, &mut tracked, &mut peek, PART_MAX,
                file_pb, total_pb, false, Some(&video_bars),
            ).await?;
            if let Some(r) = raw { files.push(r); }
            if eof { break; }
            if files.len() > 1 && policy == MultipartPolicy::None {
                let _ = tokio::fs::remove_file(path).await;
                bail!("encoded '{}' exceeded 4 GiB but multipart_policy is `none`", doc_filename);
            }
        }
        let credited = tracked.total_credited;
        total_pb.inc(upload_total_budget.saturating_sub(credited));
        let _ = tokio::fs::remove_file(path).await;
    }

    buf_tick_handle.abort();
    let _ = buf_tick_handle.await;
    file_speed_h.abort();

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

// ─── LeadingMoov pipeline ───────────────────────────────────────────────────
//
// For multi-file LeadingMoov plans we don't want to encode-then-upload one
// file at a time — the upload phase doesn't saturate while the encoder is
// running, and vice versa. Instead we run two tasks bridged by an
// mpsc::channel(1): the encoder iterates the plan and produces an
// `EncodedJob` per file; the uploader consumes them. The channel's capacity
// of 1 means the encoder blocks on `send` until the uploader has picked up
// the previous file, guaranteeing at most two scratch files exist on disk at
// any moment (the one being encoded, the one being uploaded).

/// Everything the uploader needs to send a finished encode to Telegram.
pub struct EncodedJob {
    pub scratch_path: PathBuf,
    pub scratch_size: u64,
    pub doc_filename: String,
    pub virtual_path: String,
    pub rel_dir: String,
    pub policy: MultipartPolicy,
    pub source_size: u64,
    pub video_info: Option<VideoInfo>,
    pub thumb: grammers_client::media::Uploaded,
}

/// Phase 1 of the pipeline: probe → thumbnail → ffmpeg encode to scratch.
/// `encode_pb` shows 0–100% of the current file; `total_pb` is credited
/// `source_size / 2` over the duration of the encode.
pub async fn encode_to_scratch(
    client: &Client,
    encode_args: &[String],
    vres: u32,
    thumbnail_args: &[String],
    item: &UploadItem,
    encode_pb: &ProgressBar,
    total_pb: &ProgressBar,
    encoded_bytes: Arc<AtomicU64>,
) -> anyhow::Result<EncodedJob> {
    let (source, doc_filename, virtual_path, rel_dir, policy, source_size) = match item {
        UploadItem::EncodedVideo {
            source, doc_filename, virtual_path, rel_dir, policy, source_size, ..
        } => (
            source.clone(), doc_filename.clone(), virtual_path.clone(),
            rel_dir.clone(), *policy, *source_size,
        ),
        _ => bail!("encode_to_scratch: not an EncodedVideo item"),
    };

    let video_info = probe_video_file(&source).await.map(|mut v| {
        v.streamable = true;
        v
    });

    let scale_args: Vec<String> = match &video_info {
        Some(vi) if vi.height > vres as i32 => {
            let new_h = (vres / 2) * 2;
            let new_w = (vi.width as u64 * vres as u64 / vi.height as u64) as u32;
            let new_w = (new_w / 2) * 2;
            vec!["-vf".into(), format!("scale={new_w}:{new_h}")]
        }
        _ => vec![],
    };

    // Thumbnail is uploaded eagerly (small, cheap) so the job carries an
    // Uploaded handle ready for the message send.
    let thumb_bytes = make_thumbnail_to_buffer(&source, thumbnail_args, encode_pb).await?;
    let thumb = upload_thumb(client, thumb_bytes, &doc_filename).await?;

    let duration_us: Option<u64> = video_info.as_ref()
        .map(|v| v.duration.as_micros() as u64)
        .filter(|&d| d > 0);

    // Pipeline-style bar: {prefix} = label, {msg} = speed (updated by tick).
    set_bar_style(encode_pb);
    set_prefix_label(encode_pb, format!("encoding {}", doc_filename));
    encode_pb.set_length(source_size.max(1));
    encode_pb.set_position(0);

    std::fs::create_dir_all(SCRATCH_DIR)
        .with_context(|| format!("create_dir_all {SCRATCH_DIR}"))?;
    let scratch_path_buf = scratch_path(&doc_filename);
    let output_arg = scratch_path_buf.to_string_lossy().into_owned();

    // Encode contributes source_size / 2 to total_pb; encode_pb advances the
    // full source_size to show 0–100% on the per-file bar.
    let total_budget_encode = source_size / 2;

    let mut run = spawn_ffmpeg(
        &source, encode_args, &scale_args, &output_arg, false,
        encode_pb, total_pb,
        source_size, total_budget_encode, duration_us,
        Some(encoded_bytes),
    )?;
    let status = run.child.wait().await.context("waiting for ffmpeg")?;
    if let Some(h) = run.progress {
        let _ = h.await;
        let advanced = run.total_advanced.load(std::sync::atomic::Ordering::Relaxed);
        total_pb.inc(total_budget_encode.saturating_sub(advanced));
        encode_pb.set_position(source_size);
    }
    if !status.success() {
        let _ = tokio::fs::remove_file(&scratch_path_buf).await;
        let err = run.stderr_buf.lock().await.clone();
        bail!("ffmpeg exited with {}: {}", status, err.trim());
    }

    let scratch_size = tokio::fs::metadata(&scratch_path_buf).await
        .with_context(|| format!("stat scratch file {}", scratch_path_buf.display()))?
        .len();

    Ok(EncodedJob {
        scratch_path: scratch_path_buf,
        scratch_size,
        doc_filename,
        virtual_path,
        rel_dir,
        policy,
        source_size,
        video_info,
        thumb,
    })
}

/// Phase 2: open the scratch file, stream it to Telegram, send the message,
/// delete the scratch. `upload_pb` shows 0–100% of the current file's upload;
/// `total_pb` is credited `source_size - source_size/2 = source_size/2` over
/// `scratch_size` bytes.
pub async fn upload_scratch(
    client: &Client,
    peer: PeerRef,
    job: EncodedJob,
    upload_pb: &ProgressBar,
    buf_pb: &ProgressBar,
    total_pb: &ProgressBar,
    video_bars: &VideoUploadBars,
    uploaded_bytes: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    let EncodedJob {
        scratch_path,
        scratch_size,
        doc_filename,
        virtual_path,
        rel_dir,
        policy,
        source_size,
        video_info,
        thumb,
    } = job;

    set_bar_style(upload_pb);
    set_prefix_label(upload_pb, format!("uploading {}", doc_filename));
    upload_pb.set_length(scratch_size.max(1));
    upload_pb.set_position(0);
    let _ = buf_pb; // referenced indirectly through video_bars

    let upload_total_budget = source_size.saturating_sub(source_size / 2);
    let f = tokio::fs::File::open(&scratch_path).await
        .with_context(|| format!("opening scratch file {}", scratch_path.display()))?;
    let mut tracked = ScaledProgressReader {
        inner: f,
        file_pb: upload_pb.clone(),
        total_pb: total_pb.clone(),
        inner_size: scratch_size,
        total_budget: upload_total_budget,
        bytes_read: 0,
        total_credited: 0,
        shared_total: Some(uploaded_bytes),
    };
    let mut files: Vec<RawBigFile> = Vec::new();
    let mut peek: Option<u8> = None;
    loop {
        let (raw, eof) = upload_one_big_file(
            client, &mut tracked, &mut peek, PART_MAX,
            upload_pb, total_pb, false, Some(video_bars),
        ).await?;
        if let Some(r) = raw { files.push(r); }
        if eof { break; }
        if files.len() > 1 && policy == MultipartPolicy::None {
            let _ = tokio::fs::remove_file(&scratch_path).await;
            bail!("encoded '{}' exceeded 4 GiB but multipart_policy is `none`", doc_filename);
        }
    }
    let credited = tracked.total_credited;
    total_pb.inc(upload_total_budget.saturating_sub(credited));
    let _ = tokio::fs::remove_file(&scratch_path).await;

    // Snap the upload bar to 100 % for the just-finished file.
    upload_pb.set_position(scratch_size);

    if files.is_empty() {
        bail!("no upload parts produced for '{}'", doc_filename);
    }

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

    if policy == MultipartPolicy::None {
        bail!("encoded '{}' exceeded 4 GiB but multipart_policy is `none`", doc_filename);
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
                client.send_message(peer, msg).await
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

/// Run the LeadingMoov plan with the encode/upload pipeline. Sets up a
/// background buffer-tick task and orchestrates the encoder/uploader split.
pub async fn run_leading_moov_pipeline(
    client: &Client,
    peer: PeerRef,
    encode_args: &[String],
    vres: u32,
    thumbnail_args: &[String],
    plan: &[UploadItem],
    mp: &MultiProgress,
    encode_pb: &ProgressBar,
    upload_pb: &ProgressBar,
    total_pb: &ProgressBar,
) -> anyhow::Result<()> {
    // The single buffer bar (rendered between encode_pb and upload_pb is
    // ideal, but indicatif insert_after with two reference bars complicates
    // the order — we just insert it after upload_pb).
    // Pipeline bars use the with-speed style: speed is rendered inline on the
    // same line as the progress bar (via {msg}), so no separate speed bars.
    set_bar_style(encode_pb);
    set_bar_style(upload_pb);
    set_prefix_label(upload_pb, "pending upload");

    let buf_pb = mp.insert_after(upload_pb, ProgressBar::new((TG_CHUNK * UPLOAD_CONCURRENCY) as u64));
    set_buffer_bar_style(&buf_pb);

    let video_bars = VideoUploadBars {
        buf_pb: buf_pb.clone(),
        upload_pb: ProgressBar::hidden(),
        buf_fill: Arc::new(AtomicU64::new(0)),
        partial_fill: Arc::new(AtomicU64::new(0)),
        total_uploaded: Arc::new(AtomicU64::new(0)),
    };
    // Cumulative byte counters powering the inline speed strings.
    let encoded_bytes = Arc::new(AtomicU64::new(0));
    let uploaded_bytes = Arc::new(AtomicU64::new(0));

    let buf_tick_fill     = video_bars.buf_fill.clone();
    let buf_tick_partial  = video_bars.partial_fill.clone();
    let buf_tick_uploaded = video_bars.total_uploaded.clone();
    let buf_tick_pb       = buf_pb.clone();
    let enc_b_tick        = encoded_bytes.clone();
    let up_b_tick         = uploaded_bytes.clone();
    let encode_pb_tick    = encode_pb.clone();
    let upload_pb_tick    = upload_pb.clone();
    let buf_tick_handle = tokio::spawn(async move {
        use std::collections::VecDeque;
        // Rolling window so ffmpeg's 256 KB avio-flush bursts (and likewise
        // bursty TG part-uploads) average out into a stable readout.
        const WINDOW_SECS: usize = 5;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut enc_history: VecDeque<u64> = VecDeque::with_capacity(WINDOW_SECS + 1);
        let mut up_history:  VecDeque<u64> = VecDeque::with_capacity(WINDOW_SECS + 1);
        let speed = |hist: &VecDeque<u64>| -> u64 {
            if hist.len() < 2 { return 0; }
            let oldest = *hist.front().unwrap();
            let newest = *hist.back().unwrap();
            let span   = (hist.len() - 1) as u64;
            newest.saturating_sub(oldest) / span
        };
        loop {
            interval.tick().await;
            let fill = buf_tick_fill.load(std::sync::atomic::Ordering::Relaxed)
                + buf_tick_partial.load(std::sync::atomic::Ordering::Relaxed);
            let processed = buf_tick_uploaded.load(std::sync::atomic::Ordering::Relaxed);
            buf_tick_pb.set_position(fill);
            buf_tick_pb.set_message(format!("Σ {}", fmt_mib(processed)));

            let enc = enc_b_tick.load(std::sync::atomic::Ordering::Relaxed);
            let up = up_b_tick.load(std::sync::atomic::Ordering::Relaxed);
            enc_history.push_back(enc);
            up_history.push_back(up);
            while enc_history.len() > WINDOW_SECS + 1 { enc_history.pop_front(); }
            while up_history.len()  > WINDOW_SECS + 1 { up_history.pop_front();  }
            encode_pb_tick.set_message(fmt_speed(speed(&enc_history) as f64));
            upload_pb_tick.set_message(fmt_speed(speed(&up_history)  as f64));
        }
    });

    let n = plan.len();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<EncodedJob>(1);

    // Uploader task.
    let uploader = {
        let client = client.clone();
        let upload_pb = upload_pb.clone();
        let buf_pb_c = buf_pb.clone();
        let total_pb = total_pb.clone();
        let uploaded_bytes = uploaded_bytes.clone();
        tokio::spawn(async move {
            let mut idx: usize = 0;
            while let Some(job) = rx.recv().await {
                upload_scratch(
                    &client, peer, job,
                    &upload_pb, &buf_pb_c, &total_pb, &video_bars,
                    uploaded_bytes.clone(),
                ).await?;
                idx += 1;
                total_pb.set_message(format!("TOTAL {idx}/{n}"));
                // Reset to "pending upload" until the next job arrives. If
                // one is already in the channel the next loop iteration will
                // re-label immediately; otherwise the pipeline-tick keeps
                // updating the upload-speed string against this idle bar.
                set_prefix_label(&upload_pb, "pending upload");
                upload_pb.set_length(1);
                upload_pb.set_position(0);
            }
            // No more jobs — clear the upload bar.
            set_prefix_label(&upload_pb, "no more uploads");
            upload_pb.set_length(1);
            upload_pb.set_position(1);
            anyhow::Ok(())
        })
    };

    // Encoder loop runs on this task.
    let mut encoder_err: Option<anyhow::Error> = None;
    for item in plan.iter() {
        match encode_to_scratch(
            client, encode_args, vres, thumbnail_args, item, encode_pb, total_pb,
            encoded_bytes.clone(),
        ).await {
            Ok(job) => {
                // `send` blocks if the uploader hasn't picked up the previous
                // job yet — that's the at-most-two-files-on-disk guarantee.
                if tx.send(job).await.is_err() {
                    // Uploader closed early (likely errored). Surface that.
                    encoder_err = Some(anyhow::anyhow!("uploader closed unexpectedly"));
                    break;
                }
            }
            Err(e) => { encoder_err = Some(e); break; }
        }
    }
    set_prefix_label(encode_pb, "done encoding");
    encode_pb.set_position(encode_pb.length().unwrap_or(0));

    drop(tx);
    let uploader_res = uploader.await;
    buf_tick_handle.abort();
    let _ = buf_tick_handle.await;
    buf_pb.finish_and_clear();
    // Clear any stale speed string on the inline bars.
    encode_pb.set_message("");
    upload_pb.set_message("");

    if let Some(e) = encoder_err { return Err(e); }
    uploader_res.context("uploader join")??;
    Ok(())
}
