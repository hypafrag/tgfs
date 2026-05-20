//! `tgup` — upload files and directories to a tgfs-managed Telegram channel.
//!
//! Reads the same `tgfs.yml` as the daemon, resolves the target channel from
//! `--channel`, and walks positional arguments according to `--dir`. Builds the
//! complete execution plan offline; only after validation does it connect to
//! Telegram and start uploading.

mod args;
mod ffmpeg;
mod picker;
mod plan;
mod progress;
mod tvmaze;
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
    build_encode_args, encode_and_upload_for_album, ffmpeg_in_path, ffprobe_in_path,
    run_encoded_video, run_leading_moov_pipeline, run_leading_moov_album_pipeline,
    thumbnail_args,
};
use tgfs::config::Streamification;
use plan::{collect_path, find_channel, group_into_albums, plan_has_video, print_plan, PartSource, UploadItem};
use plan::replace_ext;
use progress::{set_bar_style, set_prefix_label, LABEL_WIDTH};
use upload::{resolve_channel_peer, upload_album, upload_part_as_message};

/// When `--tvshow` and `--encode-video` are combined, convert every video
/// `Single` or `FileAlbum` part into an encoded upload:
///   - `Single` (video)    → `EncodedVideo`  (individual encoded message)
///   - `FileAlbum` (video) → `EncodedAlbum`  (encode-then-album, grouping preserved)
/// Non-video parts are left unchanged.
fn apply_encode_video_to_tvshow_plan(
    plan: Vec<UploadItem>,
    policy: tgfs::config::MultipartPolicy,
) -> Vec<UploadItem> {
    let mut out: Vec<UploadItem> = Vec::with_capacity(plan.len());
    for item in plan {
        match item {
            UploadItem::Single(part) => {
                let PartSource::File(ref src) = part.src;
                if plan::is_video_path(src) {
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
                // Keep the album grouping; rename video doc_filenames to .mp4.
                let renamed: Vec<plan::UploadPart> = parts
                    .into_iter()
                    .map(|p| {
                        let PartSource::File(ref src) = p.src;
                        if plan::is_video_path(src) {
                            plan::UploadPart {
                                doc_filename: replace_ext(&p.doc_filename, "mp4"),
                                ..p
                            }
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

    // Session lives next to the default config. An explicit --config still
    // points at ~/.config/tgfs/session.sqlite3 — keep one session per host.
    let session_path = default_session_path();
    if let Some(parent) = session_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating '{}'", parent.display()))?;
    }

    // Resolve the destination. When `-c/--channel` was given, look up the
    // matching ChannelEntry in the config for its multipart_policy; we still
    // connect lazily (after planning) so a dry-run never touches the network.
    // When omitted, connect now and show the interactive picker — its return
    // value gives us both the display name and the resolved PeerRef, so we
    // skip the redundant title-walk in resolve_channel_peer afterwards.
    let (channel_name, channel_entry, picked_peer, picked_client): (
        String,
        Option<&tgfs::config::ChannelEntry>,
        Option<_>,
        Option<_>,
    ) = match &args.channel {
        Some(name) => {
            let entry = find_channel(&config, name).ok_or_else(|| {
                anyhow!("channel '{}' is not defined in {}", name, config_path.display())
            })?;
            (name.clone(), Some(entry), None, None)
        }
        None => {
            if args.dry_run {
                bail!("--dry-run requires -c/--channel; the interactive picker needs a live Telegram connection");
            }
            println!("Connecting to Telegram to populate destination picker...");
            let (client, _rx) =
                connect_and_authorize_with_session(&config, &session_path.to_string_lossy()).await?;
            let (name, peer) = picker::pick_destination(&client, &config).await?;
            let entry = find_channel(&config, &name);
            (name, entry, Some(peer), Some(client))
        }
    };
    let policy = channel_entry
        .map(|e| e.multipart_policy)
        .unwrap_or_default();

    let encode_args = build_encode_args(&config.ffmpeg.encode_args);
    let thumb_args = thumbnail_args();

    let cwd = std::env::current_dir()
        .context("can't determine current working directory")?
        .canonicalize()
        .context("can't canonicalize current working directory")?;

    let mut plan: Vec<UploadItem> = Vec::new();
    if args.tvshow {
        plan = tvshow::build_tvshow_plan(&args.paths, args.dir_mode).await?;
        if args.encode_video {
            plan = apply_encode_video_to_tvshow_plan(plan, policy);
        }
    } else {
        for p in &args.paths {
            collect_path(p, &cwd, policy, args.dir_mode, args.encode_video, &mut plan)?;
        }
    }
    if plan.is_empty() { bail!("nothing to upload"); }

    if args.album {
        plan = group_into_albums(plan);
    }

    // Warn once before the plan: when the plan contains video files but
    // ffprobe isn't on PATH, those videos will be uploaded as videos but
    // without duration / width / height — Telegram clients can't render an
    // inline preview without those, so they'll appear as generic
    // attachments. Suppress for the re-encode path: ffmpeg/ffprobe was
    // already validated up top.
    if !args.encode_video && plan_has_video(&plan) && !ffprobe_in_path() {
        eprintln!(
            "warning: ffprobe not found on PATH — video files will be uploaded \
             without duration/dimensions, so Telegram clients won't show inline \
             previews. Install ffmpeg (which ships ffprobe) to enable previews."
        );
        eprintln!();
    }

    if args.dry_run {
        print_plan(&plan, &channel_name, args.encode_video);
        return Ok(());
    }

    print_plan(&plan, &channel_name, args.encode_video);
    println!();

    // Reuse the picker's already-authenticated client when available; only
    // connect now if the user passed an explicit -c (skipped the picker).
    let (client, peer) = match (picked_client, picked_peer) {
        (Some(c), Some(p)) => (c, p),
        _ => {
            println!("Connecting to Telegram...");
            let (client, _updates_rx) =
                connect_and_authorize_with_session(&config, &session_path.to_string_lossy()).await?;
            let peer = resolve_channel_peer(&client, &channel_name).await?;
            (client, peer)
        }
    };

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
        set_prefix_label(&pb, "pending upload");
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
                UploadItem::EncodedAlbum { parts } => {
                    if streamification == Streamification::LeadingMoov {
                        // Pipeline: encode queue → upload queue, ≤2 scratch
                        // files on disk. Collects InputMedia, then send_album.
                        run_leading_moov_album_pipeline(
                            &client, peer, parts,
                            &encode_args, config.ffmpeg.encode_args.video.vres,
                            &thumb_args, &mp, &file_pb, &total_pb,
                        ).await?;
                    } else {
                        use grammers_client::media::InputMedia;
                        // Fmp4 / None: stream each part from ffmpeg pipe:1
                        // directly to Telegram upload — no scratch files.
                        let vres = config.ffmpeg.encode_args.video.vres;
                        let mut medias: Vec<InputMedia> = Vec::with_capacity(parts.len());
                        for part in parts {
                            let PartSource::File(src) = &part.src;
                            let (uploaded, video_info, thumb) = encode_and_upload_for_album(
                                &client, src, &part.doc_filename,
                                &encode_args, &thumb_args,
                                vres, &file_pb, &total_pb, part.size,
                            ).await.with_context(|| format!("encoding '{}'", src.display()))?;
                            let mut media = InputMedia::new()
                                .caption(part.caption.clone())
                                .document(uploaded);
                            if let Some(ref info) = video_info {
                                media = media.attribute(upload::video_attribute(info));
                            }
                            media = media.thumbnail(thumb);
                            medias.push(media);
                        }
                        client.send_album(peer, medias).await
                            .context("sending encoded album")?;
                    }
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
            set_prefix_label(&file_pb, format!("done: {}", item.display_name()));
            file_pb.set_message(String::new());
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
