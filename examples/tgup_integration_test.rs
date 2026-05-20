//! tgup integration test.
//!
//! Generates 5-second test-pattern .mp4 videos with ffmpeg and uploads them to
//! the same Telegram channel as `integration_test` (resolved from the spec
//! file's `channel.name` field), exercising several `tgup` CLI options
//! end-to-end:
//!
//!   * plain upload of a single file
//!   * `-a/--album` grouping
//!   * `--tvshow` show/season grouping + filename normalization
//!   * `--encode-video` re-encode
//!
//! Each scenario wipes every message in the channel first, then runs `tgup`
//! as a subprocess, then re-fetches the channel and asserts the resulting
//! filenames / captions / grouped_ids.
//!
//! Usage:
//!   cargo run --example tgup_integration_test -- \
//!     --config tgfs.yml --spec test_channels.yml \
//!     [--tgup target/debug/tgup]
//!
//! Prerequisites:
//!   1. Authenticated tgfs session at `./session.sqlite3` (same one
//!      `integration_test` uses).
//!   2. Authenticated tgup session at `~/.config/tgfs/session.sqlite3` — log
//!      tgup in once manually before running this test.
//!   3. `ffmpeg` on `$PATH`.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use grammers_client::media::Media;
use grammers_client::peer::Peer;
use grammers_client::tl;
use grammers_client::Client;
use grammers_session::types::PeerRef;
use log::{info, warn};
use serde::Deserialize;

use tgfs::config::{self, Config};
use tgfs::login::connect_and_authorize;

#[derive(Debug)]
struct Args {
    config: String,
    spec: String,
    tgup: Option<String>,
    log_level: String,
    log_file: Option<String>,
}

fn parse_args() -> Args {
    let mut config = "tgfs.yml".to_string();
    let mut spec = "test_channels.yml".to_string();
    let mut tgup: Option<String> = None;
    let mut log_level = "info,tgfs=info".to_string();
    let mut log_file: Option<String> = None;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--config" => config = it.next().expect("--config requires a path"),
            "--spec" => spec = it.next().expect("--spec requires a path"),
            "--tgup" => tgup = Some(it.next().expect("--tgup requires a path")),
            "--log-level" => log_level = it.next().expect("--log-level requires a value"),
            "--log-file" => log_file = Some(it.next().expect("--log-file requires a path")),
            "-h" | "--help" => {
                eprintln!(
                    "Usage: tgup_integration_test [--config tgfs.yml] \
                     [--spec test_channels.yml] [--tgup target/debug/tgup] \
                     [--log-level info,tgfs=debug] [--log-file test.log]\n\
                     \n\
                     --log-file is truncated at startup (one-off debugging mode). \
                     When --log-file is omitted, logs append to ./test.log so a \
                     single `./test` run aggregates output from all integration \
                     runners into one file."
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }
    Args { config, spec, tgup, log_level, log_file }
}

fn init_logger(level: &str, log_file: Option<&str>) -> anyhow::Result<()> {
    let mut builder = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(level),
    );
    builder.format(|buf, record| {
        let ts = buf.timestamp_millis();
        writeln!(
            buf,
            "{ts} {:5} {}: {}",
            record.level(),
            record.target(),
            record.args()
        )
    });
    let (path, truncate) = match log_file {
        Some(p) => (p.to_string(), true),
        None => ("test.log".to_string(), false),
    };
    let mut opts = fs::OpenOptions::new();
    opts.create(true).write(true);
    if truncate { opts.truncate(true); } else { opts.append(true); }
    let file = opts.open(&path)
        .with_context(|| format!("opening log file {}", path))?;
    builder.target(env_logger::Target::Pipe(Box::new(file)));
    builder.write_style(env_logger::WriteStyle::Never);
    builder.init();
    Ok(())
}

/// Tiny cargo-test-look-alike harness: prints `test <name> ... ok|FAILED`
/// to stdout while all `log!` output goes to test.log. Process exits
/// non-zero if any test fails.
struct TestRunner {
    passed: usize,
    failed: Vec<(String, String)>,
}

impl TestRunner {
    fn new() -> Self { Self { passed: 0, failed: Vec::new() } }

    async fn run<F, Fut>(&mut self, name: &str, body: F)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        print!("test {name} ... ");
        let _ = std::io::stdout().flush();
        info!("=== test {name} START ===");
        match body().await {
            Ok(()) => {
                println!("{}", console::style("ok").green());
                info!("=== test {name} OK ===");
                self.passed += 1;
            }
            Err(e) => {
                println!("{}", console::style("FAILED").red());
                info!("=== test {name} FAILED: {e:?} ===");
                self.failed.push((name.to_string(), format!("{e:?}")));
            }
        }
    }

    fn finish(self) -> bool {
        println!();
        if self.failed.is_empty() {
            println!(
                "test result: {}. {} passed; 0 failed",
                console::style("ok").green(), self.passed,
            );
            true
        } else {
            println!("failures:");
            for (name, err) in &self.failed {
                println!();
                println!("---- {} ----", console::style(name).red());
                println!("{err}");
            }
            println!();
            println!(
                "test result: {}. {} passed; {} failed",
                console::style("FAILED").red(), self.passed, self.failed.len(),
            );
            false
        }
    }
}

#[derive(Deserialize)]
struct SpecRoot { channel: ChannelSpec }
#[derive(Deserialize)]
struct ChannelSpec { name: String }

const SCRATCH_DIR: &str = "/tmp/tgfs/tgup-test";

fn reset_scratch() -> anyhow::Result<()> {
    let _ = fs::remove_dir_all(SCRATCH_DIR);
    fs::create_dir_all(SCRATCH_DIR)?;
    Ok(())
}

/// Render a 5-second test-pattern .mp4 (libx264 + AAC, faststart-friendly).
/// `label` is overlaid as text so files are visually distinguishable when
/// played back from the channel.
fn generate_video(path: &Path, label: &str) -> anyhow::Result<()> {
    let drawtext = format!(
        "drawtext=text='{}':fontsize=24:fontcolor=white:x=10:y=10",
        label.replace('\'', "")
    );
    let status = Command::new("ffmpeg")
        .args(["-y", "-hide_banner", "-loglevel", "error"])
        .args(["-f", "lavfi", "-i", "testsrc=duration=5:size=320x240:rate=30"])
        .args(["-f", "lavfi", "-i", "sine=frequency=440:duration=5"])
        .args(["-vf", &drawtext])
        .args(["-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p"])
        .args(["-c:a", "aac", "-b:a", "96k"])
        .args(["-movflags", "+faststart"])
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("running ffmpeg")?;
    if !status.success() {
        bail!("ffmpeg failed for {}", path.display());
    }
    Ok(())
}

async fn find_channel(client: &Client, name: &str) -> anyhow::Result<PeerRef> {
    let mut dialogs = client.iter_dialogs();
    while let Some(d) = dialogs.next().await? {
        if let Peer::Channel(ch) = d.peer() {
            if ch.title() == name {
                return ch.to_ref().await
                    .ok_or_else(|| anyhow!("channel '{name}' ref unresolvable"));
            }
        }
    }
    Err(anyhow!(
        "channel '{name}' not found in this account's dialogs — \
         create it (and join with the test account) before running this test"
    ))
}

/// Render a 5-second test-pattern AVI with the xvid codec (libxvid + mp3
/// audio). Named after a TV-show episode so hunch can parse it locally.
fn generate_avi_xvid(path: &Path, label: &str) -> anyhow::Result<()> {
    let drawtext = format!(
        "drawtext=text='{}':fontsize=24:fontcolor=white:x=10:y=10",
        label.replace('\'', "")
    );
    let status = Command::new("ffmpeg")
        .args(["-y", "-hide_banner", "-loglevel", "error"])
        .args(["-f", "lavfi", "-i", "testsrc=duration=5:size=320x240:rate=30"])
        .args(["-f", "lavfi", "-i", "sine=frequency=440:duration=5"])
        .args(["-vf", &drawtext])
        .args(["-c:v", "libxvid", "-q:v", "10"])
        .args(["-c:a", "libmp3lame", "-b:a", "96k"])
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("running ffmpeg for xvid")?;
    if !status.success() {
        bail!("ffmpeg (libxvid) failed for {} — ensure ffmpeg is built with --enable-libxvid", path.display());
    }
    Ok(())
}

async fn clear_about(client: &Client, peer: PeerRef) -> anyhow::Result<()> {
    // Wipe whatever `integration_test` (or a previous run) wrote to the
    // channel description — otherwise the tgfs runner will see its sentinel
    // hash next time and skip re-population, leaving stale spec data behind.
    client.invoke(&tl::functions::messages::EditChatAbout {
        peer: peer.into(),
        about: String::new(),
    }).await?;
    Ok(())
}

async fn delete_all_messages(client: &Client, peer: PeerRef) -> anyhow::Result<usize> {
    let mut ids: Vec<i32> = Vec::new();
    let mut it = client.iter_messages(peer);
    while let Some(m) = it.next().await? { ids.push(m.id()); }
    if ids.is_empty() { return Ok(0); }
    for chunk in ids.chunks(100) {
        client.invoke(&tl::functions::channels::DeleteMessages {
            channel: peer.into(),
            id: chunk.to_vec(),
        }).await?;
    }
    Ok(ids.len())
}

#[derive(Debug)]
struct ChannelMsg {
    #[allow(dead_code)]
    id: i32,
    text: String,
    doc_name: Option<String>,
    grouped_id: Option<i64>,
}

async fn fetch_messages(client: &Client, peer: PeerRef) -> anyhow::Result<Vec<ChannelMsg>> {
    let mut out: Vec<ChannelMsg> = Vec::new();
    let mut it = client.iter_messages(peer);
    while let Some(m) = it.next().await? {
        // Message id=1 is the channel-creation service message; Telegram
        // refuses to delete it even from the official client, so skip it
        // unconditionally to keep per-scenario assertions honest.
        if m.id() == 1 { continue; }
        let doc_name = match m.media() {
            Some(Media::Document(d)) => d.name().map(|s| s.to_string()),
            _ => None,
        };
        out.push(ChannelMsg {
            id: m.id(),
            text: m.text().to_string(),
            doc_name,
            grouped_id: m.grouped_id(),
        });
    }
    // `iter_messages` returns newest first; reverse so positions match upload order.
    out.reverse();
    Ok(out)
}

/// Write a minimal tgfs.yml that contains just enough for tgup to authenticate
/// and resolve the test channel. Copies api_id/api_hash/phone from `base`.
fn write_tgup_config(base: &Config, channel_name: &str) -> anyhow::Result<PathBuf> {
    let yaml = format!(
        "api_id: {api_id}\n\
         api_hash: \"{api_hash}\"\n\
         phone: \"{phone}\"\n\
         channels:\n  - name: \"{name}\"\n",
        api_id = base.api_id,
        api_hash = base.api_hash,
        phone = base.phone,
        name = channel_name,
    );
    let path = PathBuf::from(SCRATCH_DIR).join("tgup-config.yml");
    fs::write(&path, yaml).context("writing tgup test config")?;
    Ok(path)
}

fn run_tgup(
    tgup_bin: &Path,
    config: &Path,
    channel: &str,
    extra: &[&str],
    log_path: &Path,
) -> anyhow::Result<()> {
    let mut cmd = Command::new(tgup_bin);
    cmd.arg("--config").arg(config)
       .args(["-c", channel])
       .args(extra)
       .stdin(Stdio::null());
    // Route tgup's stdout/stderr into the shared test.log so the
    // cargo-test-style line on the parent's stdout stays clean.
    let stdout = fs::OpenOptions::new().create(true).append(true).open(log_path)
        .with_context(|| format!("opening {} for child stdout", log_path.display()))?;
    let stderr = fs::OpenOptions::new().create(true).append(true).open(log_path)
        .with_context(|| format!("opening {} for child stderr", log_path.display()))?;
    cmd.stdout(Stdio::from(stdout)).stderr(Stdio::from(stderr));
    info!("running: {:?}", cmd);
    let status = cmd.status().context("spawning tgup")?;
    if !status.success() {
        bail!("tgup exited with status {:?}", status.code());
    }
    Ok(())
}

fn locate_tgup_bin(cli_override: Option<&str>) -> anyhow::Result<PathBuf> {
    if let Some(p) = cli_override {
        let pb = PathBuf::from(p);
        if !pb.exists() { bail!("--tgup path '{}' doesn't exist", p); }
        return Ok(pb);
    }
    info!("building tgup binary…");
    let status = Command::new("cargo")
        .args(["build", "--bin", "tgup"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("cargo build --bin tgup")?;
    if !status.success() { bail!("cargo build --bin tgup failed"); }
    let p = PathBuf::from("target/debug/tgup");
    if !p.exists() { bail!("tgup binary not found at {}", p.display()); }
    Ok(p)
}

async fn settle() {
    tokio::time::sleep(Duration::from_secs(3)).await;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = parse_args();
    let log_path = PathBuf::from(args.log_file.clone().unwrap_or_else(|| "test.log".to_string()));
    init_logger(&args.log_level, args.log_file.as_deref())?;

    let cfg = config::load_config(&args.config)?;
    let spec_bytes = fs::read(&args.spec)
        .with_context(|| format!("reading {}", args.spec))?;
    let spec: SpecRoot = serde_yaml::from_slice(&spec_bytes).context("parsing spec")?;
    let channel_name = spec.channel.name.clone();
    info!("test channel: {}", channel_name);

    reset_scratch()?;
    let scratch = Path::new(SCRATCH_DIR);

    info!("generating test-pattern videos…");
    let plain_video = scratch.join("plain.mp4");
    generate_video(&plain_video, "PLAIN")?;

    let album_dir = scratch.join("album");
    fs::create_dir_all(&album_dir)?;
    let mut album_files: Vec<PathBuf> = Vec::new();
    for i in 1..=5 {
        let p = album_dir.join(format!("clip-{}.mp4", i));
        generate_video(&p, &format!("ALBUM {}", i))?;
        album_files.push(p);
    }

    let tvshow_dir = scratch.join("tvshow");
    fs::create_dir_all(&tvshow_dir)?;
    // Episode titles are embedded in the filenames so hunch can extract them
    // locally without a TVmaze network call.
    let tv_episode_titles = ["One", "Two", "Three", "Four", "Five"];
    let mut tv_files: Vec<PathBuf> = Vec::new();
    for (ep, title) in (1..=5u32).zip(tv_episode_titles.iter()) {
        let p = tvshow_dir.join(format!("Some.Show.S01E{:02}.{}.mp4", ep, title));
        generate_video(&p, &format!("S01E{:02}", ep))?;
        tv_files.push(p);
    }

    let encode_source = scratch.join("encodeme.mov");
    generate_video(&encode_source, "ENCODE")?;

    // Six AVI files (xvid) across two seasons, each with an embedded episode
    // title so hunch can parse the full name locally without hitting TVmaze.
    let tvshow_encode_dir = scratch.join("tvshow_encode");
    fs::create_dir_all(&tvshow_encode_dir)?;
    let tvshow_encode_files: Vec<PathBuf> = [
        ("Some.Show.S01E01.Pilot.avi",   "S01E01"),
        ("Some.Show.S01E02.Orbit.avi",   "S01E02"),
        ("Some.Show.S01E03.Crater.avi",  "S01E03"),
        ("Some.Show.S02E01.Surge.avi",   "S02E01"),
        ("Some.Show.S02E02.Blaze.avi",   "S02E02"),
        ("Some.Show.S02E03.Finale.avi",  "S02E03"),
    ]
    .iter()
    .map(|(name, label)| {
        let p = tvshow_encode_dir.join(name);
        generate_avi_xvid(&p, label)?;
        Ok::<_, anyhow::Error>(p)
    })
    .collect::<anyhow::Result<Vec<_>>>()?;

    let tgup_bin = locate_tgup_bin(args.tgup.as_deref())?;
    let tgup_config = write_tgup_config(&cfg, &channel_name)?;

    let (client, _rx) = connect_and_authorize(&cfg).await?;
    let peer = find_channel(&client, &channel_name).await?;

    // Pre-flight: wipe the channel once and confirm nothing is left behind.
    let deleted = delete_all_messages(&client, peer).await?;
    info!("cleared {deleted} pre-existing messages");
    clear_about(&client, peer).await.context("clearing channel description")?;
    info!("cleared channel description");
    settle().await;
    let residue = fetch_messages(&client, peer).await?;
    if !residue.is_empty() {
        warn!(
            "{} message(s) remain in '{}' after a full delete pass — these are \
             almost certainly service messages (channel creation, pinned-message \
             notifications, etc.) that the Bot API cannot remove. Open the channel \
             in the Telegram client and delete them by hand, then re-run the test. \
             Residue: {:#?}",
            residue.len(), channel_name, residue,
        );
    }

    let mut runner = TestRunner::new();

    runner.run("tgup::plain_upload", || async {
        run_tgup(&tgup_bin, &tgup_config, &channel_name,
            &[plain_video.to_str().unwrap()], &log_path)?;
        settle().await;
        let msgs = fetch_messages(&client, peer).await?;
        if msgs.len() != 1 { bail!("expected 1 message, got {}: {msgs:#?}", msgs.len()); }
        if msgs[0].doc_name.as_deref() != Some("plain.mp4") {
            bail!("unexpected doc name: {:?}", msgs[0].doc_name);
        }
        if !msgs[0].text.is_empty() { bail!("caption should be empty, got {:?}", msgs[0].text); }
        if msgs[0].grouped_id.is_some() { bail!("should not be in a Telegram album"); }
        Ok(())
    }).await;

    runner.run("tgup::album", || async {
        delete_all_messages(&client, peer).await?;
        settle().await;
        let mut argv: Vec<&str> = vec!["-a"];
        let strs: Vec<String> = album_files.iter().map(|p| p.to_string_lossy().into_owned()).collect();
        for s in &strs { argv.push(s); }
        run_tgup(&tgup_bin, &tgup_config, &channel_name, &argv, &log_path)?;
        settle().await;
        let msgs = fetch_messages(&client, peer).await?;
        if msgs.len() != 5 { bail!("expected 5 messages, got {}", msgs.len()); }
        let gid = msgs[0].grouped_id;
        if gid.is_none() { bail!("messages should share a grouped_id"); }
        if !msgs.iter().all(|m| m.grouped_id == gid) {
            bail!("not all messages share the same grouped_id: {msgs:#?}");
        }
        let mut names: Vec<String> = msgs.iter().filter_map(|m| m.doc_name.clone()).collect();
        names.sort();
        let expected_names: Vec<String> = (1..=5).map(|i| format!("clip-{}.mp4", i)).collect();
        if names != expected_names { bail!("unexpected filenames: {names:?}"); }
        Ok(())
    }).await;

    runner.run("tgup::tvshow", || async {
        delete_all_messages(&client, peer).await?;
        settle().await;
        let mut argv: Vec<&str> = vec!["--tvshow"];
        let strs: Vec<String> = tv_files.iter().map(|p| p.to_string_lossy().into_owned()).collect();
        for s in &strs { argv.push(s); }
        run_tgup(&tgup_bin, &tgup_config, &channel_name, &argv, &log_path)?;
        settle().await;
        let msgs = fetch_messages(&client, peer).await?;
        if msgs.len() != 5 { bail!("expected 5 messages, got {}", msgs.len()); }
        let gid = msgs[0].grouped_id;
        if gid.is_none() { bail!("messages should share a grouped_id"); }
        if !msgs.iter().all(|m| m.grouped_id == gid) {
            bail!("not all messages share the same grouped_id");
        }
        let mut names: Vec<String> = msgs.iter().filter_map(|m| m.doc_name.clone()).collect();
        names.sort();
        // Episode titles are in filenames so hunch fills them without a TVmaze call.
        let mut expected: Vec<String> = [
            ("01","One"), ("02","Two"), ("03","Three"), ("04","Four"), ("05","Five"),
        ].iter().map(|(n, t)| format!("Some Show S01E{} - {}.mp4", n, t)).collect();
        expected.sort();
        if names != expected { bail!("filenames not renamed as expected: {names:?}"); }
        let captions: Vec<&str> = msgs.iter().map(|m| m.text.as_str()).collect();
        if !captions.iter().any(|t| t.contains("Some Show S01 E01-E05")) {
            bail!("missing album caption; captions were {captions:?}");
        }
        Ok(())
    }).await;

    runner.run("tgup::encode_video", || async {
        delete_all_messages(&client, peer).await?;
        settle().await;
        run_tgup(&tgup_bin, &tgup_config, &channel_name,
            &["--encode-video", encode_source.to_str().unwrap()], &log_path)?;
        settle().await;
        let msgs = fetch_messages(&client, peer).await?;
        if msgs.len() != 1 { bail!("expected 1 message, got {}", msgs.len()); }
        if msgs[0].doc_name.as_deref() != Some("encodeme.mp4") {
            bail!("filename should be re-extensioned to .mp4, got {:?}", msgs[0].doc_name);
        }
        Ok(())
    }).await;

    // --tvshow + --encode-video: 3 S01 + 3 S02 AVI files → 2 albums of 3 mp4s.
    runner.run("tgup::tvshow_encode_video", || async {
        delete_all_messages(&client, peer).await?;
        settle().await;
        let mut argv: Vec<&str> = vec!["--tvshow", "--encode-video"];
        let strs: Vec<String> = tvshow_encode_files.iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        for s in &strs { argv.push(s.as_str()); }
        run_tgup(&tgup_bin, &tgup_config, &channel_name, &argv, &log_path)?;
        settle().await;

        let msgs = fetch_messages(&client, peer).await?;
        if msgs.len() != 6 { bail!("expected 6 messages, got {}: {msgs:#?}", msgs.len()); }

        // All messages must have a grouped_id (all are in albums).
        if msgs.iter().any(|m| m.grouped_id.is_none()) {
            bail!("every message should be in a Telegram album: {msgs:#?}");
        }

        // Exactly 2 distinct grouped_ids — one per season.
        let mut gids: Vec<i64> = msgs.iter().filter_map(|m| m.grouped_id).collect();
        gids.sort();
        gids.dedup();
        if gids.len() != 2 {
            bail!("expected 2 distinct album grouped_ids (one per season), got {}: {msgs:#?}", gids.len());
        }

        // Filenames: extension changed to .mp4, tvshow naming with episode title.
        let mut names: Vec<String> = msgs.iter().filter_map(|m| m.doc_name.clone()).collect();
        names.sort();
        let mut expected: Vec<String> = vec![
            "Some Show S01E01 - Pilot.mp4".to_string(),
            "Some Show S01E02 - Orbit.mp4".to_string(),
            "Some Show S01E03 - Crater.mp4".to_string(),
            "Some Show S02E01 - Surge.mp4".to_string(),
            "Some Show S02E02 - Blaze.mp4".to_string(),
            "Some Show S02E03 - Finale.mp4".to_string(),
        ];
        expected.sort();
        if names != expected {
            bail!("unexpected filenames.\n  got:      {names:?}\n  expected: {expected:?}");
        }

        // Each season forms exactly one album: the 3 S01 messages share one
        // grouped_id, and the 3 S02 messages share the other.
        let s1_gid = {
            let m = msgs.iter().find(|m| {
                m.doc_name.as_deref().map_or(false, |n| n.starts_with("Some Show S01"))
            }).unwrap();
            m.grouped_id.unwrap()
        };
        let s2_gid = {
            let m = msgs.iter().find(|m| {
                m.doc_name.as_deref().map_or(false, |n| n.starts_with("Some Show S02"))
            }).unwrap();
            m.grouped_id.unwrap()
        };
        if s1_gid == s2_gid {
            bail!("S01 and S02 should be in different albums but share grouped_id {s1_gid}");
        }
        let s1_count = msgs.iter().filter(|m| m.grouped_id == Some(s1_gid)).count();
        let s2_count = msgs.iter().filter(|m| m.grouped_id == Some(s2_gid)).count();
        if s1_count != 3 {
            bail!("expected 3 messages in the S01 album, got {s1_count}");
        }
        if s2_count != 3 {
            bail!("expected 3 messages in the S02 album, got {s2_count}");
        }

        // Album captions: first message of each album carries the season caption.
        let captions: Vec<&str> = msgs.iter().map(|m| m.text.as_str()).collect();
        if !captions.iter().any(|t| t.contains("Some Show S01 E01-E03")) {
            bail!("missing S01 album caption; captions were {captions:?}");
        }
        if !captions.iter().any(|t| t.contains("Some Show S02 E01-E03")) {
            bail!("missing S02 album caption; captions were {captions:?}");
        }
        Ok(())
    }).await;

    info!("all scenarios completed");
    if runner.finish() {
        Ok(())
    } else {
        std::process::exit(1);
    }
}
