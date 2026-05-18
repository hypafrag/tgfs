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
mod upload;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context as _};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use tgfs::config::{self, Config};
use tgfs::login::connect_and_authorize_with_session;

use args::{default_config_path, default_session_path, parse_args, DirMode};
use ffmpeg::{ffmpeg_in_path, run_encoded_video, split_shell_args};
use plan::{collect_path, find_channel, print_plan, UploadItem};
use progress::{set_bar_style, set_label, LABEL_WIDTH};
use upload::{resolve_channel_peer, upload_album, upload_part_as_message};

async fn run() -> anyhow::Result<()> {
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

    let encode_args = split_shell_args(&config.ffmpeg.encode_args)
        .context("parsing ffmpeg.encode_args")?;
    let thumbnail_args = split_shell_args(&config.ffmpeg.thumbnail_args)
        .context("parsing ffmpeg.thumbnail_args")?;

    let cwd = std::env::current_dir()
        .context("can't determine current working directory")?
        .canonicalize()
        .context("can't canonicalize current working directory")?;

    let mut plan: Vec<UploadItem> = Vec::new();
    for p in &args.paths {
        collect_path(p, &cwd, policy, args.dir_mode, args.encode_video, &mut plan)?;
    }
    if plan.is_empty() { bail!("nothing to upload"); }

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
    let mp = Arc::new(MultiProgress::new());
    let file_pb = mp.add(ProgressBar::new(0));
    set_bar_style(&file_pb);
    let total_pb = mp.add(ProgressBar::new(total_bytes));
    // Compose a single fixed-width "TOTAL i/N" label so the bar column
    // lines up with the file_pb above it (which left-pads `{msg}` to
    // LABEL_WIDTH).
    total_pb.set_style(
        ProgressStyle::with_template(&format!(
            "{{msg:<{w}.{w}}} [{{bar:30.green/blue}}] {{bytes}}/{{total_bytes}} ({{bytes_per_sec}}, {{eta}})",
            w = LABEL_WIDTH,
        ))
        .unwrap()
        .progress_chars("=>-"),
    );
    total_pb.set_message(format!("TOTAL {}/{}", 0, plan.len()));

    for (i, item) in plan.iter().enumerate() {
        total_pb.set_message(format!("TOTAL {}/{}", i, plan.len()));
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
            UploadItem::EncodedVideo { .. } => {
                run_encoded_video(
                    &client, peer,
                    &encode_args, &thumbnail_args,
                    item, &file_pb, &total_pb,
                ).await?;
            }
        }
        set_label(&file_pb, format!("done: {}", item.display_name()));
    }
    file_pb.finish_with_message("done");
    total_pb.finish();
    println!("All uploads complete.");
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {:#}", e);
            ExitCode::FAILURE
        }
    }
}
