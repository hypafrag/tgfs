//! `tgup` — upload files and directories to a tgfs-managed Telegram channel.
//!
//! Reads the same `tgfs.yml` as the daemon, resolves the target channel from
//! `--channel`, and walks positional arguments according to `--dir`. Builds the
//! complete execution plan offline; only after validation does it connect to
//! Telegram and start uploading.

mod args;
mod ffmpeg;
mod plan;
mod progress;
mod tvshow;
mod upload;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context as _};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use tgfs::config::{self, Config};
use tgfs::login::connect_and_authorize_with_session;

use args::{default_config_path, default_session_path, parse_args, DirMode};
use ffmpeg::{
    build_encode_args, ffmpeg_in_path, run_encoded_video, run_leading_moov_pipeline,
    thumbnail_args,
};
use tgfs::config::Streamification;
use plan::{collect_path, find_channel, group_into_albums, print_plan, UploadItem};
use progress::{set_bar_style, set_label, LABEL_WIDTH};
use upload::{resolve_channel_peer, upload_album, upload_part_as_message};

/// A `Write` adapter that routes log lines through `MultiProgress::println` so
/// they appear above the progress bars without corrupting the cursor-up redraw
/// arithmetic indicatif uses to overwrite them in place.
struct MpLogWriter {
    mp: Arc<MultiProgress>,
    buf: Vec<u8>,
}

impl std::io::Write for MpLogWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        let s = String::from_utf8_lossy(&self.buf).into_owned();
        for line in s.split_inclusive('\n') {
            let trimmed = line.trim_end_matches('\n');
            if !trimmed.is_empty() {
                let _ = self.mp.println(trimmed);
            }
        }
        self.buf.clear();
        Ok(())
    }
}

async fn run(mp: Arc<MultiProgress>) -> anyhow::Result<()> {
    // Wipe leftover scratch files from any prior crashed run before we start.
    // remove_dir_all errors when the dir doesn't exist — fine, ignore.
    let _ = std::fs::remove_dir_all(ffmpeg::SCRATCH_DIR);

    let args = parse_args()?;

    if args.encode_video && args.dir_mode == DirMode::Zip {
        bail!("--encode-video combined with --dir zip is not supported");
    }

    if args.encode_video && !ffmpeg_in_path() {
        bail!("--encode-video requires ffmpeg on $PATH (not found)");
    }

    if args.dir_mode == DirMode::Zip {
        for p in &args.paths {
            let m = std::fs::metadata(p)
                .with_context(|| format!("can't stat '{}'", p.display()))?;
            if m.is_dir() {
                println!("not implemented");
                return Ok(());
            }
        }
    }

    let config_path: PathBuf = match &args.config_path {
        Some(p) => PathBuf::from(p),
        None => default_config_path(),
    };
    let config: Config = config::load_config(&config_path.to_string_lossy())?;
    let channel_entry = find_channel(&config, &args.channel).ok_or_else(|| {
        anyhow!("channel '{}' is not defined in {}", args.channel, config_path.display())
    })?;
    let policy = channel_entry.multipart_policy;

    // Session lives next to the default config. An explicit --config still
    // points at ~/.config/tgfs/session.sqlite3 — keep one session per host.
    let session_path = default_session_path();
    if let Some(parent) = session_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating '{}'", parent.display()))?;
    }

    let encode_args = build_encode_args(&config.ffmpeg.encode_args);
    let thumb_args = thumbnail_args();

    let cwd = std::env::current_dir()
        .context("can't determine current working directory")?
        .canonicalize()
        .context("can't canonicalize current working directory")?;

    let mut plan: Vec<UploadItem> = Vec::new();
    if args.tvshow {
        plan = tvshow::build_tvshow_plan(&args.paths, args.dir_mode)?;
    } else {
        for p in &args.paths {
            collect_path(p, &cwd, policy, args.dir_mode, args.encode_video, &mut plan)?;
        }
    }
    if plan.is_empty() { bail!("nothing to upload"); }

    if args.album {
        plan = group_into_albums(plan);
    }

    if args.dry_run {
        print_plan(&plan, &args.channel, args.encode_video);
        return Ok(());
    }

    print_plan(&plan, &args.channel, args.encode_video);
    println!();
    println!("Connecting to Telegram...");

    let (client, _updates_rx) =
        connect_and_authorize_with_session(&config, &session_path.to_string_lossy()).await?;
    let peer = resolve_channel_peer(&client, &args.channel).await?;

    let total_bytes: u64 = plan.iter().map(|i| i.planned_bytes()).sum();
    let streamification = config.ffmpeg.encode_args.video.streamification;
    let pipeline_eligible = streamification == Streamification::LeadingMoov
        && plan.iter().all(|i| matches!(i, UploadItem::EncodedVideo { .. }));

    // For the LeadingMoov pipeline we want TWO file-level bars (encode +
    // upload). For everything else, one is enough.
    let file_pb = mp.add(ProgressBar::new(0));
    set_bar_style(&file_pb);
    let upload_pb: Option<ProgressBar> = if pipeline_eligible {
        let pb = mp.add(ProgressBar::new(1));
        set_bar_style(&pb);
        set_label(&pb, "pending upload");
        Some(pb)
    } else { None };
    let total_pb = mp.add(ProgressBar::new(total_bytes));
    total_pb.set_style(
        ProgressStyle::with_template(&format!(
            "{{msg:<{w}.{w}}} [{{bar:20.green/blue}}] {{percent:>3}}% ({{eta}})",
            w = LABEL_WIDTH,
        ))
        .unwrap()
        .progress_chars("=>-"),
    );
    total_pb.set_message(format!("TOTAL 0/{}", plan.len()));

    if let Some(ref upload_pb) = upload_pb {
        run_leading_moov_pipeline(
            &client, peer, &encode_args, config.ffmpeg.encode_args.video.vres,
            &thumb_args, &plan, &mp, &file_pb, upload_pb, &total_pb,
        ).await?;
    } else {
        for (i, item) in plan.iter().enumerate() {
            match item {
                UploadItem::Single(p) => {
                    upload_part_as_message(&client, peer, p, None, None, &file_pb, &total_pb).await?;
                }
                UploadItem::SuffixParts { parts, .. } => {
                    for p in parts {
                        upload_part_as_message(&client, peer, p, None, None, &file_pb, &total_pb).await?;
                    }
                }
                UploadItem::AlbumParts { parts, .. } => {
                    upload_album(&client, peer, parts, None, None, &file_pb, &total_pb).await?;
                }
                UploadItem::FileAlbum { parts } => {
                    upload_album(&client, peer, parts, None, None, &file_pb, &total_pb).await?;
                }
                UploadItem::EncodedVideo { .. } => {
                    run_encoded_video(
                        &client, peer,
                        streamification,
                        &encode_args, config.ffmpeg.encode_args.video.vres,
                        &thumb_args,
                        item, &mp, &file_pb, &total_pb,
                    ).await?;
                }
            }
            set_label(&file_pb, format!("done: {}", item.display_name()));
            total_pb.set_message(format!("TOTAL {}/{}", i + 1, plan.len()));
        }
    }
    file_pb.finish_with_message("done");
    if let Some(pb) = upload_pb { pb.finish_with_message("done"); }
    total_pb.finish();
    println!("All uploads complete.");
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    // Create the MultiProgress first so log output can be routed through its
    // `println` — that way `grammers` (or any other `log::warn!`) writes don't
    // interleave with the bar redraws and desync the cursor-up arithmetic.
    let mp = Arc::new(MultiProgress::new());
    let log_writer = MpLogWriter { mp: mp.clone(), buf: Vec::new() };
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("warn,grammers_mtsender=error"),
    )
        .target(env_logger::Target::Pipe(Box::new(log_writer)))
        .init();

    // SIGINT / SIGTERM / SIGHUP: wipe the scratch directory and exit so a
    // mid-encode Ctrl-C doesn't leak GB-scale temp files.
    tokio::spawn(async {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint  = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut sighup  = signal(SignalKind::hangup()).expect("install SIGHUP handler");
        let signo = tokio::select! {
            _ = sigint.recv()  => 2,
            _ = sigterm.recv() => 15,
            _ = sighup.recv()  => 1,
        };
        let _ = std::fs::remove_dir_all(ffmpeg::SCRATCH_DIR);
        std::process::exit(128 + signo);
    });

    match run(mp).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {:#}", e);
            ExitCode::FAILURE
        }
    }
}
