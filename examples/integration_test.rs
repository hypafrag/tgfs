//! Integration test runner.
//!
//! Connects to a real Telegram account using credentials from `tgfs.yml`,
//! populates a dedicated test channel from the spec in `test_channels.yml`,
//! then mounts it under several settings and asserts the layout/contents.
//!
//! Invoke as:
//!
//! ```text
//! cargo run --example integration_test -- \
//!     --config tgfs.yml \
//!     --spec test_channels.yml
//! ```
//!
//! The runner is *destructive* against the channel named in the spec: when
//! the spec hash recorded in the channel "about" field doesn't match the
//! current spec, every message in the channel is deleted and the spec is
//! re-uploaded.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context};
use base64::Engine;
use grammers_client::media::Uploaded;
use grammers_client::message::InputMessage;
use grammers_client::peer::Peer;
use grammers_client::Client;
use grammers_client::tl;
use grammers_session::types::PeerRef;
use grammers_session::updates::UpdatesLike;
use log::{info, warn};
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
#[cfg(not(target_os = "macos"))]
use notify::EventKind;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use tokio::sync::mpsc as tokio_mpsc;
use zip::write::{FileOptions, ZipWriter};
use zip::CompressionMethod;

use tgfs::config::{self, ArchiveView, ChannelEntry, Config, MultipartPolicy};
use tgfs::index::{AppState, MimePool};
use tgfs::{indexer, realtime};
use tgfs::login::connect_and_authorize;
use tgfs::zip_cache::ZipCache;
use tgfs::fuse as tgfs_fuse;

// ------------------------ CLI -----------------------------------------------

#[derive(Debug)]
struct Args {
    config: String,
    spec: String,
    log_level: String,
    log_file: Option<String>,
}

fn parse_args() -> Args {
    let mut config = "tgfs.yml".to_string();
    let mut spec = "test_channels.yml".to_string();
    let mut log_level = "info,tgfs=info".to_string();
    let mut log_file: Option<String> = None;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--config" => config = it.next().expect("--config requires a path"),
            "--spec" => spec = it.next().expect("--spec requires a path"),
            "--log-level" => log_level = it.next().expect("--log-level requires a value"),
            "--log-file" => log_file = Some(it.next().expect("--log-file requires a path")),
            "-h" | "--help" => {
                eprintln!(
                    "Usage: integration_test [--config tgfs.yml] [--spec test_channels.yml] \
                     [--log-level info,tgfs=debug] [--log-file integration_test.log]\n\
                     \n\
                     --log-file is truncated (created from scratch) at every run.\n\
                     When --log-file is omitted, logs go to stderr."
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }
    Args { config, spec, log_level, log_file }
}

/// REMINDER FOR FUTURE SELF: when integration tests fail or behave oddly,
/// re-run with `--log-level debug --log-file integration_test.log` and read
/// the resulting file. The log captures every Telegram RPC, FUSE callback,
/// mount/unmount cycle, and download in chronological order — that's where
/// to look first before stepping through code.
fn init_logger(level: &str, log_file: Option<&str>) -> anyhow::Result<()> {
    let mut builder = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level));
    builder.format(|buf, record| {
        let ts = buf.timestamp_millis();
        let level_style = buf.default_level_style(record.level());
        writeln!(
            buf,
            "{ts} {level_style}{:5}{level_style:#} {}: {}",
            record.level(),
            record.target(),
            record.args()
        )
    });
    if let Some(path) = log_file {
        // Created from scratch on every run so each test invocation starts
        // with a clean log file — no need to manually rotate between runs.
        let file = fs::OpenOptions::new()
            .create(true).write(true).truncate(true)
            .open(path)
            .with_context(|| format!("opening log file {}", path))?;
        builder.target(env_logger::Target::Pipe(Box::new(file)));
        // Force-disable ANSI styles since they don't render usefully in files.
        builder.write_style(env_logger::WriteStyle::Never);
    }
    builder.init();
    Ok(())
}

// ------------------------ Spec types ----------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SpecRoot {
    channel: ChannelSpec,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ChannelSpec {
    name: String,
    #[serde(default)]
    messages: Vec<MessageSpec>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MessageSpec {
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    files: Vec<FileSpec>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct FileSpec {
    name: String,
    #[serde(default)]
    text: Option<String>,
    /// Base64-encoded binary content (whitespace tolerated).
    #[serde(default)]
    blob: Option<String>,
    /// Recursive zip contents. Mutually exclusive with text/blob.
    #[serde(default)]
    zip: Option<Vec<ZipEntry>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ZipEntry {
    name: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    blob: Option<String>,
    /// When `Some`, this entry is a directory whose children are the listed entries.
    #[serde(default)]
    content: Option<Vec<ZipEntry>>,
    /// Per-file flag for zip entries. `Some(true)` → DEFLATE (method 8),
    /// `Some(false)` → stored (method 0). `None` defaults to `true`.
    /// Ignored for directory entries.
    #[serde(default)]
    deflated: Option<bool>,
}

impl FileSpec {
    /// Materialize the file's raw bytes (text/blob/zip → concrete buffer).
    fn build_bytes(&self) -> anyhow::Result<Vec<u8>> {
        match (&self.text, &self.blob, &self.zip) {
            (Some(t), None, None) => Ok(t.as_bytes().to_vec()),
            (None, Some(b), None) => decode_blob(b),
            (None, None, Some(entries)) => build_zip(entries),
            _ => bail!("file '{}' must specify exactly one of text/blob/zip", self.name),
        }
    }
}

fn decode_blob(b: &str) -> anyhow::Result<Vec<u8>> {
    let cleaned: String = b.chars().filter(|c| !c.is_whitespace()).collect();
    Ok(base64::engine::general_purpose::STANDARD.decode(cleaned.as_bytes())?)
}

/// Build a ZIP archive in memory from a recursive `ZipEntry` tree.
fn build_zip(entries: &[ZipEntry]) -> anyhow::Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let cursor = std::io::Cursor::new(&mut buf);
        let mut w = ZipWriter::new(cursor);
        walk_zip(&mut w, "", entries)?;
        w.finish()?;
    }
    Ok(buf)
}

fn walk_zip<W: std::io::Write + std::io::Seek>(
    w: &mut ZipWriter<W>,
    prefix: &str,
    entries: &[ZipEntry],
) -> anyhow::Result<()> {
    for e in entries {
        let path = if prefix.is_empty() { e.name.clone() } else { format!("{}/{}", prefix, e.name) };
        match (&e.content, &e.text, &e.blob) {
            (Some(children), None, None) => {
                w.add_directory::<_, ()>(format!("{}/", path), FileOptions::default())?;
                walk_zip(w, &path, children)?;
            }
            (None, _, _) => {
                let bytes = match (&e.text, &e.blob) {
                    (Some(t), None) => t.as_bytes().to_vec(),
                    (None, Some(b)) => decode_blob(b)?,
                    _ => bail!("zip entry '{}' must specify exactly one of text/blob/content", path),
                };
                let method = if e.deflated.unwrap_or(true) {
                    CompressionMethod::Deflated
                } else {
                    CompressionMethod::Stored
                };
                let opts: FileOptions<()> = FileOptions::default().compression_method(method);
                w.start_file(path, opts)?;
                w.write_all(&bytes)?;
            }
            _ => bail!("zip entry '{}' has both content and text/blob", path),
        }
    }
    Ok(())
}

// ------------------------ Sentinel hash -------------------------------------

const SENTINEL_PREFIX: &str = "tgfs-integration-test-spec=";

fn spec_hash(yaml_bytes: &[u8]) -> String {
    let mut h = sha2::Sha256::new();
    h.update(yaml_bytes);
    hex::encode(h.finalize())
}

fn parse_sentinel(about: &str) -> Option<&str> {
    about.lines()
        .find_map(|line| line.trim().strip_prefix(SENTINEL_PREFIX))
}

// ------------------------ Channel resolution --------------------------------

async fn find_channel(client: &Client, name: &str) -> anyhow::Result<PeerRef> {
    let mut dialogs = client.iter_dialogs();
    while let Some(d) = dialogs.next().await? {
        if let Peer::Channel(ch) = d.peer() {
            if ch.title() == name {
                return ch.to_ref().await.ok_or_else(||
                    anyhow!("channel '{name}' found but ref unresolvable"));
            }
        }
    }
    Err(anyhow!(
        "channel '{name}' not found in this account's dialogs. Create it manually (the runner won't auto-create channels)."
    ))
}

async fn read_about(client: &Client, peer: PeerRef) -> anyhow::Result<String> {
    let resp = client.invoke(&tl::functions::channels::GetFullChannel {
        channel: peer.into(),
    }).await?;
    let tl::enums::messages::ChatFull::Full(full) = resp;
    match full.full_chat {
        tl::enums::ChatFull::ChannelFull(cf) => Ok(cf.about),
        _ => Ok(String::new()),
    }
}

async fn set_about(client: &Client, peer: PeerRef, about: String) -> anyhow::Result<()> {
    client.invoke(&tl::functions::messages::EditChatAbout {
        peer: peer.into(),
        about,
    }).await?;
    Ok(())
}

// ------------------------ Population ----------------------------------------

async fn delete_all_messages(client: &Client, peer: PeerRef) -> anyhow::Result<usize> {
    let mut ids: Vec<i32> = Vec::new();
    let mut messages = client.iter_messages(peer);
    while let Some(m) = messages.next().await? {
        ids.push(m.id());
    }
    if ids.is_empty() { return Ok(0); }
    info!("deleting {} existing messages", ids.len());
    // Telegram caps delete batches at 100 ids per call. Use channels.deleteMessages
    // directly since `Client::delete_messages` may dispatch through the wrong helper.
    for chunk in ids.chunks(100) {
        client.invoke(&tl::functions::channels::DeleteMessages {
            channel: peer.into(),
            id: chunk.to_vec(),
        }).await?;
    }
    Ok(ids.len())
}

const SCRATCH_DIR: &str = "/tmp/tgfs/test";

fn ensure_scratch_dir() -> anyhow::Result<()> {
    fs::create_dir_all(SCRATCH_DIR)?;
    Ok(())
}

fn scratch_path(idx: usize, name: &str) -> PathBuf {
    // Each message gets its own subdirectory so we can keep the original
    // filename — grammers derives the Telegram document name from the path.
    let dir = Path::new(SCRATCH_DIR).join(format!("msg{:03}", idx));
    fs::create_dir_all(&dir).expect("create scratch subdir");
    dir.join(name)
}

async fn upload_message(
    client: &Client,
    peer: PeerRef,
    msg_idx: usize,
    msg: &MessageSpec,
) -> anyhow::Result<()> {
    let caption = msg.text.clone().unwrap_or_default();

    if msg.files.is_empty() {
        // Text-only message.
        client.send_message(peer, InputMessage::new().text(caption)).await?;
        return Ok(());
    }

    // Materialize each file to disk and upload.
    let mut uploaded: Vec<(String, Uploaded)> = Vec::with_capacity(msg.files.len());
    for f in &msg.files {
        let bytes = f.build_bytes()
            .with_context(|| format!("building bytes for {}", f.name))?;
        let path = scratch_path(msg_idx, &f.name);
        fs::write(&path, &bytes)?;
        let up = client.upload_file(&path).await?;
        uploaded.push((f.name.clone(), up));
    }

    if uploaded.len() == 1 {
        let (_name, up) = uploaded.into_iter().next().unwrap();
        client.send_message(peer, InputMessage::new().text(caption).file(up)).await?;
    } else {
        // Album: first InputMedia carries the caption; subsequent ones must be
        // bare media. grammers' `send_album` sends them as a grouped multi-media
        // message, which is how Telegram exposes multi-file uploads.
        use grammers_client::media::InputMedia;
        let mut medias: Vec<InputMedia> = Vec::with_capacity(uploaded.len());
        for (i, (_, up)) in uploaded.into_iter().enumerate() {
            let mut im = InputMedia::new().file(up);
            if i == 0 { im = im.caption(caption.clone()); }
            medias.push(im);
        }
        client.send_album(peer, medias).await?;
    }

    Ok(())
}

async fn populate_channel(
    client: &Client,
    peer: PeerRef,
    spec: &ChannelSpec,
    hash: &str,
) -> anyhow::Result<()> {
    ensure_scratch_dir()?;
    delete_all_messages(client, peer).await?;
    // Clear the sentinel so a crash mid-upload doesn't leave us thinking
    // the channel matches the spec.
    set_about(client, peer, String::new()).await.ok();

    info!("uploading {} messages…", spec.messages.len());
    for (i, m) in spec.messages.iter().enumerate() {
        info!("  msg {}/{} ({} files)", i + 1, spec.messages.len(), m.files.len());
        upload_message(client, peer, i, m).await
            .with_context(|| format!("uploading message {}", i))?;
        // Mild pacing to keep below Telegram's flood thresholds for a fresh account.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    set_about(client, peer, format!("{SENTINEL_PREFIX}{hash}")).await?;
    info!("wrote spec hash to channel description");
    Ok(())
}

// ------------------------ Mount + assertions --------------------------------

const MOUNT_PATH: &str = "/tmp/tgfs/test/mount";

fn ensure_mount_dir() -> anyhow::Result<()> {
    fs::create_dir_all(MOUNT_PATH)?;
    Ok(())
}

/// Build a Config that only contains the test channel, with the requested
/// per-variant settings. Used by `mount_variant`.
fn variant_config(
    base: &Config,
    channel_name: &str,
    archive_view: ArchiveView,
    multipart_policy: MultipartPolicy,
) -> Config {
    Config {
        api_id: base.api_id,
        api_hash: base.api_hash.clone(),
        phone: base.phone.clone(),
        log: base.log.clone(),
        http_port: None,
        mount_at: Some(MOUNT_PATH.to_string()),
        saved_messages: None,
        proxy: base.proxy.clone(),
        max_fetches_per_pid: None,
        max_fetches_total: None,
        realtime: false,
        channels: vec![ChannelEntry {
            name: channel_name.to_string(),
            directory: None,
            archive_view,
            skip_deflated_id3v1: false,
            collapse_by_prefix: None,
            multipart_policy,
        }],
    }
}

/// Mount the channel under MOUNT_PATH using `cfg`, run `body` with the
/// mount visible to std::fs, then unmount and join.
async fn mount_variant<F>(
    client: Client,
    cfg: Config,
    zip_cache: Arc<Mutex<ZipCache>>,
    body: F,
) -> anyhow::Result<()>
where
    F: FnOnce(&Path) -> anyhow::Result<()> + Send + 'static,
{
    ensure_mount_dir()?;
    let mime_pool = MimePool::new();
    let result = indexer::build_index(client.clone(), &cfg, &mime_pool, &zip_cache).await?;
    let state = Arc::new(AppState {
        client: client.clone(),
        mime_pool,
        channels: result.channels,
        dir_to_channel: result.dir_to_channel,
        max_fetches_per_pid: None,
        max_fetches_total: None,
        fresh_docs: Mutex::new(HashMap::new()),
    });

    let fs_handle = tgfs_fuse::TgfsFS::new(Arc::clone(&state));
    let mut fuse_config = fuser::Config::default();
    fuse_config.acl = fuser::SessionACL::All;
    fuse_config.mount_options = vec![
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::RO,
        fuser::MountOption::CUSTOM(format!("max_read={}", tgfs_fuse::BLKSIZE)),
    ];
    let session = fuser::Session::new(fs_handle.clone(), MOUNT_PATH, &fuse_config)
        .context("FUSE session creation failed")?;
    fs_handle.set_notifier(session.notifier());
    let bg = session.spawn().context("FUSE session spawn failed")?;

    // Tiny pause so the kernel publishes the mount.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let body_result = tokio::task::spawn_blocking(move || body(Path::new(MOUNT_PATH))).await?;

    shutdown_fuse(bg, MOUNT_PATH).await;
    body_result
}

/// Compute the expected on-disk layout from the spec (for `archive_view: file`).
/// Maps virtual path → expected bytes.
fn expected_layout_file_view(channel_name: &str, spec: &ChannelSpec) -> anyhow::Result<HashMap<String, Vec<u8>>> {
    let mut out = HashMap::new();
    for msg in &spec.messages {
        let dir_override = msg.text.as_deref().and_then(parse_path_directive);
        for f in &msg.files {
            let bytes = f.build_bytes()?;
            let prefix = match &dir_override {
                Some(d) => format!("{}/{}/", channel_name, d.trim_end_matches('/')),
                None => format!("{}/", channel_name),
            };
            out.insert(format!("{prefix}{}", f.name), bytes);
        }
    }
    Ok(out)
}

fn parse_path_directive(text: &str) -> Option<String> {
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("path:") {
            let v = v.trim();
            if !v.is_empty() { return Some(v.to_string()); }
        }
    }
    None
}

/// Build a `name → raw bytes` map of every leaf in every zip in the spec.
/// Keys are `<archive-stem>/<inner-path>` (channel-dir relative).
fn expected_zip_inner_bytes(spec: &ChannelSpec) -> anyhow::Result<HashMap<String, Vec<u8>>> {
    let mut out = HashMap::new();
    for msg in &spec.messages {
        let dir_override = msg.text.as_deref().and_then(parse_path_directive);
        for f in &msg.files {
            if let Some(zip_entries) = &f.zip {
                let stem = Path::new(&f.name).file_stem().and_then(|s| s.to_str()).unwrap_or(&f.name);
                let dir = dir_override.as_deref().map(|d| d.trim_end_matches('/')).unwrap_or("");
                let prefix = if dir.is_empty() { stem.to_string() } else { format!("{}/{}", dir, stem) };
                collect_zip(&prefix, zip_entries, &mut out)?;
            }
        }
    }
    Ok(out)
}

fn collect_zip(prefix: &str, entries: &[ZipEntry], out: &mut HashMap<String, Vec<u8>>) -> anyhow::Result<()> {
    for e in entries {
        let path = format!("{}/{}", prefix, e.name);
        match (&e.content, &e.text, &e.blob) {
            (Some(children), None, None) => collect_zip(&path, children, out)?,
            (None, Some(t), None) => { out.insert(path, t.as_bytes().to_vec()); }
            (None, None, Some(b)) => { out.insert(path, decode_blob(b)?); }
            _ => bail!("invalid zip entry '{}'", path),
        }
    }
    Ok(())
}

fn assert_layout_matches(root: &Path, expected: &HashMap<String, Vec<u8>>) -> anyhow::Result<()> {
    for (rel, expected_bytes) in expected {
        let p = root.join(rel);
        let got = fs::read(&p)
            .with_context(|| format!("reading {}", p.display()))?;
        if got != *expected_bytes {
            bail!(
                "content mismatch at {}: expected {} bytes, got {} bytes",
                p.display(), expected_bytes.len(), got.len()
            );
        }
        info!("✓ {} ({} bytes)", rel, got.len());
    }
    Ok(())
}

// ------------------------ Multipart helpers ---------------------------------

/// Parse `<stem>.NN` suffix filenames (exactly two decimal digits).
fn parse_suffix_part(name: &str) -> Option<(String, u8)> {
    let dot = name.rfind('.')?;
    let ext = &name[dot + 1..];
    if ext.len() == 2 && ext.bytes().all(|b| b.is_ascii_digit()) {
        Some((name[..dot].to_string(), ext.parse().ok()?))
    } else {
        None
    }
}

fn has_multipart_directive(text: &str) -> bool {
    text.lines().any(|line| {
        line.strip_prefix("multipart:")
            .map(|v| matches!(v.trim().to_lowercase().as_str(), "" | "true" | "yes" | "1"))
            .unwrap_or(false)
    })
}

/// Expected layout for `multipart_policy=suffix`: `.NN` parts are merged into
/// their base name; every other file appears individually.
fn expected_layout_suffix_multipart(
    channel_name: &str,
    spec: &ChannelSpec,
) -> anyhow::Result<HashMap<String, Vec<u8>>> {
    let mut suffix_groups: HashMap<String, BTreeMap<u8, Vec<u8>>> = HashMap::new();
    let mut out = HashMap::new();

    for msg in &spec.messages {
        let dir_override = msg.text.as_deref().and_then(parse_path_directive);
        for f in &msg.files {
            if let Some((base, num)) = parse_suffix_part(&f.name) {
                suffix_groups.entry(base).or_default().insert(num, f.build_bytes()?);
            } else {
                let bytes = f.build_bytes()?;
                let prefix = match &dir_override {
                    Some(d) => format!("{}/{}/", channel_name, d.trim_end_matches('/')),
                    None => format!("{}/", channel_name),
                };
                out.insert(format!("{prefix}{}", f.name), bytes);
            }
        }
    }

    for (base, parts) in suffix_groups {
        let merged: Vec<u8> = parts.into_values().flatten().collect();
        out.insert(format!("{}/{}", channel_name, base), merged);
    }

    Ok(out)
}

/// Expected layout for `multipart_policy=album`: albums whose caption carries
/// `multipart: true` are merged into the first file's name; everything else
/// (including suffix-named files) appears individually.
fn expected_layout_album_multipart(
    channel_name: &str,
    spec: &ChannelSpec,
) -> anyhow::Result<HashMap<String, Vec<u8>>> {
    let mut out = HashMap::new();

    for msg in &spec.messages {
        let is_album_multipart = msg.files.len() > 1
            && msg.text.as_deref().map(has_multipart_directive).unwrap_or(false);
        let dir_override = msg.text.as_deref().and_then(parse_path_directive);

        if is_album_multipart {
            let first_name = &msg.files[0].name;
            let mut merged: Vec<u8> = Vec::new();
            for f in &msg.files {
                merged.extend(f.build_bytes()?);
            }
            let prefix = match &dir_override {
                Some(d) => format!("{}/{}/", channel_name, d.trim_end_matches('/')),
                None => format!("{}/", channel_name),
            };
            out.insert(format!("{prefix}{first_name}"), merged);
        } else {
            for f in &msg.files {
                let bytes = f.build_bytes()?;
                let prefix = match &dir_override {
                    Some(d) => format!("{}/{}/", channel_name, d.trim_end_matches('/')),
                    None => format!("{}/", channel_name),
                };
                out.insert(format!("{prefix}{}", f.name), bytes);
            }
        }
    }

    Ok(out)
}

/// Tear down the FUSE mount between variants.
///
/// On macOS, `bg.umount_and_join()` works reliably and is the cleanest path —
/// AutoUnmount + libfuse_destroy unwind the mount and the worker thread exits.
///
/// On Linux, that same call consistently hangs in our environment: fusermount
/// blocks (likely because the AutoUnmount helper and an explicit unmount race
/// for the same mount), and `join()` then waits forever on a worker thread
/// that's still blocked in `read(fuse_fd)`. We skip it and go straight to
/// `fusermount -u -z` (lazy) — the kernel detaches the mount immediately, the
/// FUSE fd closes, the worker thread's read returns 0, and the bg session can
/// then be dropped on a blocking thread without hanging.
async fn shutdown_fuse(bg: fuser::BackgroundSession, mount_path: &str) {
    #[cfg(target_os = "linux")]
    {
        // Detach the mount first so the worker thread's read on the FUSE fd
        // returns and it becomes joinable.
        if let Err(e) = std::process::Command::new("fusermount")
            .args(["-u", "-z", mount_path])
            .status()
        {
            warn!("fusermount -u -z {mount_path} failed: {e}");
        }
        // Drop the session on a blocking thread — its Drop joins the worker.
        // Bound the wait so a stray hang can't stall the whole test run.
        let task = tokio::task::spawn_blocking(move || drop(bg));
        if tokio::time::timeout(Duration::from_secs(5), task).await.is_err() {
            warn!("FUSE bg session drop timed out after 5s — leaking the session");
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = mount_path;
        let task = tokio::task::spawn_blocking(move || bg.umount_and_join());
        match tokio::time::timeout(Duration::from_secs(10), task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(e))) => warn!("FUSE umount returned error: {e}"),
            Ok(Err(e)) => warn!("FUSE umount task panicked: {e}"),
            Err(_) => warn!("FUSE umount_and_join timed out after 10s"),
        }
    }
}

// ------------------------ Mutation-test helpers -----------------------------

const MUTABLE_TAG: &str = "mutable:";

/// True if a Telegram message caption carries the `mutable:` test-runner tag.
fn caption_is_mutable(text: &str) -> bool {
    text.lines().any(|l| l.trim_start().starts_with(MUTABLE_TAG))
}

/// Delete every message in the channel whose caption carries `mutable:`.
/// Returns the IDs that were removed. Safe to call before the realtime
/// dispatcher is running — it operates straight against Telegram.
async fn cleanup_mutable_messages(client: &Client, peer: PeerRef) -> anyhow::Result<Vec<i32>> {
    let mut ids: Vec<i32> = Vec::new();
    let mut messages = client.iter_messages(peer);
    while let Some(m) = messages.next().await? {
        if caption_is_mutable(m.text()) { ids.push(m.id()); }
    }
    if ids.is_empty() { return Ok(ids); }
    for chunk in ids.chunks(100) {
        client.invoke(&tl::functions::channels::DeleteMessages {
            channel: peer.into(),
            id: chunk.to_vec(),
        }).await?;
    }
    Ok(ids)
}

/// Upload one mutable file as a single-file message. The caption is exactly
/// `"mutable:"` so cleanup can identify it. Returns the Telegram message id.
async fn upload_mutable_message(
    client: &Client,
    peer: PeerRef,
    name: &str,
    content: &[u8],
) -> anyhow::Result<i32> {
    let dir = Path::new(SCRATCH_DIR).join("mutable");
    fs::create_dir_all(&dir)?;
    let path = dir.join(name);
    fs::write(&path, content)?;
    let up: Uploaded = client.upload_file(&path).await?;
    let msg = client.send_message(
        peer,
        InputMessage::new().text(MUTABLE_TAG).file(up),
    ).await?;
    Ok(msg.id())
}

/// Block until `path` reaches the requested existence state. Polls via
/// `metadata()` (which round-trips through FUSE getattr) on a 100ms cadence.
fn wait_for_file_state(path: &Path, want_exists: bool, timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let exists = fs::metadata(path).is_ok();
        if exists == want_exists { return Ok(()); }
        if Instant::now() >= deadline {
            bail!("timeout waiting for {} (want exists={}, got {})",
                  path.display(), want_exists, exists);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Read a directory through FUSE and return the entry-name set. Tests
/// `readdir` independently from `getattr`/`lookup` — both must agree on the
/// new tree state for a mutation to count as fully propagated.
fn list_dir(dir: &Path) -> anyhow::Result<HashSet<String>> {
    let mut names = HashSet::new();
    for entry in fs::read_dir(dir).with_context(|| format!("readdir {}", dir.display()))? {
        names.insert(entry?.file_name().to_string_lossy().into_owned());
    }
    Ok(names)
}

/// Assert `name` is present in `dir`'s readdir listing (or absent if `want`
/// is false). Used after each mutation to verify the directory enumeration
/// matches the per-path lookups.
fn assert_listing(dir: &Path, name: &str, want_present: bool) -> anyhow::Result<()> {
    let listing = list_dir(dir)?;
    let present = listing.contains(name);
    if present != want_present {
        bail!(
            "{} listing of {}: '{}' presence={}, expected={}. Full listing: {:?}",
            if want_present { "missing" } else { "stale" },
            dir.display(), name, present, want_present, listing,
        );
    }
    Ok(())
}

/// Drain the recv channel for `dur` and return everything we saw. Used after
/// each mutation to collect whatever the OS-native watcher delivered.
fn drain_events(rx: &std_mpsc::Receiver<notify::Result<Event>>, dur: Duration) -> Vec<Event> {
    let deadline = Instant::now() + dur;
    let mut out = Vec::new();
    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        match rx.recv_timeout(remaining) {
            Ok(Ok(ev)) => out.push(ev),
            Ok(Err(e)) => warn!("watcher error: {e:?}"),
            Err(std_mpsc::RecvTimeoutError::Timeout) => break,
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    out
}

/// True if `events` contains a Remove event whose paths include `target`.
/// Only used by `collect_remove_events`, which is itself macOS-skipped.
#[cfg(not(target_os = "macos"))]
fn events_contain_remove(events: &[Event], target: &Path) -> bool {
    events.iter().any(|e| {
        matches!(e.kind, EventKind::Remove(_))
            && e.paths.iter().any(|p| p == target)
    })
}

/// Drain any pending updates already buffered in the receiver. Used before
/// spawning the realtime dispatcher so it starts on a clean slate instead of
/// burning cycles replaying the populate-phase backlog.
fn drain_updates_backlog(rx: &mut tokio_mpsc::UnboundedReceiver<UpdatesLike>) -> usize {
    let mut n = 0;
    while rx.try_recv().is_ok() { n += 1; }
    n
}

/// The mutation test variant. Spawns the realtime dispatcher, installs an
/// OS-native filesystem watcher on the mount, then performs add/delete
/// operations through Telegram and asserts the FUSE state — and watcher
/// stream — reflect them.
async fn run_mutation_test(
    client: Client,
    base_cfg: &Config,
    spec_name: &str,
    peer: PeerRef,
    zip_cache: Arc<Mutex<ZipCache>>,
    mut updates_rx: tokio_mpsc::UnboundedReceiver<UpdatesLike>,
) -> anyhow::Result<()> {
    // Build a normal `archive_view=file, multipart_policy=none` variant.
    let cfg = variant_config(base_cfg, spec_name, ArchiveView::File, MultipartPolicy::None);

    ensure_mount_dir()?;
    let mime_pool = MimePool::new();
    let result = indexer::build_index(client.clone(), &cfg, &mime_pool, &zip_cache).await?;
    let state = Arc::new(AppState {
        client: client.clone(),
        mime_pool,
        channels: result.channels,
        dir_to_channel: result.dir_to_channel,
        max_fetches_per_pid: None,
        max_fetches_total: None,
        fresh_docs: Mutex::new(HashMap::new()),
    });

    let fs_handle = tgfs_fuse::TgfsFS::new(Arc::clone(&state));
    let mut fuse_config = fuser::Config::default();
    fuse_config.acl = fuser::SessionACL::All;
    fuse_config.mount_options = vec![
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::RO,
        fuser::MountOption::CUSTOM(format!("max_read={}", tgfs_fuse::BLKSIZE)),
    ];
    let session = fuser::Session::new(fs_handle.clone(), MOUNT_PATH, &fuse_config)
        .context("FUSE session creation failed")?;
    fs_handle.set_notifier(session.notifier());
    let bg = session.spawn().context("FUSE session spawn failed")?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Drop whatever piled up during indexing + earlier variants, then start
    // the dispatcher so subsequent mutations route through it cleanly.
    let drained = drain_updates_backlog(&mut updates_rx);
    info!("drained {drained} buffered updates before starting dispatcher");

    let dispatcher = realtime::Dispatcher::new(
        client.clone(),
        Arc::clone(&state),
        Arc::clone(&zip_cache),
        Some(fs_handle.clone()),
    );
    let dispatcher_task = tokio::spawn(dispatcher.run(updates_rx));

    // Install the OS-native watcher (inotify on Linux, FSEvents on macOS).
    // notify's `recommended_watcher` never falls back to polling.
    let (ev_tx, ev_rx) = std_mpsc::channel::<notify::Result<Event>>();
    let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |res| {
        let _ = ev_tx.send(res);
    }).context("creating recommended watcher")?;
    watcher.watch(Path::new(MOUNT_PATH), RecursiveMode::Recursive)
        .context("starting watch on mount")?;
    info!("watching {} with OS-native backend", MOUNT_PATH);

    let mount_root = Path::new(MOUNT_PATH);
    let chan_dir = mount_root.join(spec_name);

    // Tracks mutable msg_ids so we can clean up on the way out even if an
    // assertion fails mid-test.
    let mut mutable_ids: Vec<i32> = Vec::new();

    let outcome: anyhow::Result<()> = async {
        // -- Mutation 1: add a single mutable file ---------------------------
        info!("mutation: add mut_one.txt");
        let id1 = upload_mutable_message(&client, peer, "mut_one.txt", b"one").await?;
        mutable_ids.push(id1);
        let p1 = chan_dir.join("mut_one.txt");
        wait_for_file_state(&p1, true, Duration::from_secs(15))?;
        // Adds only fire FUSE_NOTIFY_INVAL_ENTRY (no fsnotify event); we
        // log whatever the watcher saw but don't gate on it.
        let ev = drain_events(&ev_rx, Duration::from_millis(500));
        // Cross-check readdir + content against the per-path metadata probe.
        assert_listing(&chan_dir, "mut_one.txt", true)?;
        let got = fs::read(&p1)?;
        if got != b"one" { bail!("mut_one.txt content mismatch: got {:?}", got); }
        info!("✓ add mut_one.txt: lookup+readdir+content all consistent ({} watcher event(s))", ev.len());

        // -- Mutation 2: add a second mutable file ---------------------------
        info!("mutation: add mut_two.bin");
        let payload2: Vec<u8> = (0u8..32).collect();
        let id2 = upload_mutable_message(&client, peer, "mut_two.bin", &payload2).await?;
        mutable_ids.push(id2);
        let p2 = chan_dir.join("mut_two.bin");
        wait_for_file_state(&p2, true, Duration::from_secs(15))?;
        let ev = drain_events(&ev_rx, Duration::from_millis(500));
        assert_listing(&chan_dir, "mut_two.bin", true)?;
        // The earlier add must still be visible in the listing — verifies
        // the second rebuild didn't accidentally drop unrelated entries.
        assert_listing(&chan_dir, "mut_one.txt", true)?;
        let got = fs::read(&p2)?;
        if got != payload2 { bail!("mut_two.bin content mismatch"); }
        info!("✓ add mut_two.bin: listing keeps both mutables ({} watcher event(s))", ev.len());

        // -- Mutation 3: delete one message ---------------------------------
        info!("mutation: delete mut_one.txt (msg {id1})");
        client.invoke(&tl::functions::channels::DeleteMessages {
            channel: peer.into(),
            id: vec![id1],
        }).await?;
        mutable_ids.retain(|&i| i != id1);
        verify_telegram_message_absent(&client, peer, id1).await?;
        wait_for_file_state(&p1, false, Duration::from_secs(5))?;
        // Listing must drop the entry — readdir and lookup agreeing on
        // absence is the real proof of a clean delete.
        assert_listing(&chan_dir, "mut_one.txt", false)?;
        assert_listing(&chan_dir, "mut_two.bin", true)?;
        check_watcher_saw_delete("mut_one.txt", &ev_rx, &p1)?;

        // -- Mutation 4: delete the remaining mutable message ----------------
        info!("mutation: delete mut_two.bin (msg {id2})");
        client.invoke(&tl::functions::channels::DeleteMessages {
            channel: peer.into(),
            id: vec![id2],
        }).await?;
        mutable_ids.retain(|&i| i != id2);
        verify_telegram_message_absent(&client, peer, id2).await?;
        wait_for_file_state(&p2, false, Duration::from_secs(5))?;
        // Both mutables should now be gone from the channel listing.
        assert_listing(&chan_dir, "mut_two.bin", false)?;
        assert_listing(&chan_dir, "mut_one.txt", false)?;
        check_watcher_saw_delete("mut_two.bin", &ev_rx, &p2)?;

        Ok(())
    }.await;

    // Always try to clean up the mutables, regardless of test outcome.
    if !mutable_ids.is_empty() {
        info!("post-test cleanup: removing {} leftover mutable msg(s)", mutable_ids.len());
        if let Err(e) = client.invoke(&tl::functions::channels::DeleteMessages {
            channel: peer.into(),
            id: mutable_ids.clone(),
        }).await {
            warn!("cleanup of leftover mutable messages failed: {e:?}");
        }
    }
    drop(watcher);
    dispatcher_task.abort();
    shutdown_fuse(bg, MOUNT_PATH).await;
    outcome
}

/// Watcher-event check after a confirmed delete.
///
/// Linux: `FUSE_NOTIFY_DELETE` reaches inotify, so the watcher MUST observe a
/// Remove event — anything else is a regression and we hard-fail. Logs "✓" on
/// pass.
///
/// macOS: macFUSE-emitted notify messages don't propagate into FSEvents (the
/// notify crate's macOS backend). Asserting Remove events here would be testing
/// the kernel-extension/FSEvents gap rather than tgfs behavior, so we skip the
/// check — but **loudly**, at INFO level next to the "✓" pass lines, so the
/// skip is impossible to miss when reading the log. The earlier
/// `wait_for_file_state` + `assert_listing` checks have already validated that
/// the deletion fully propagated through Telegram → grammers → dispatcher →
/// FUSE tree; only the downstream "tell external watchers" hop is what we're
/// declining to assert on macOS.
fn check_watcher_saw_delete(
    name: &str,
    _rx: &std_mpsc::Receiver<notify::Result<Event>>,
    _path: &Path,
) -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        info!(
            "⊘ delete {name}: FUSE + listing clean; SKIPPED watcher Remove-event \
             check on macOS (macFUSE doesn't deliver FUSE_NOTIFY_DELETE to FSEvents)"
        );
        Ok(())
    }
    #[cfg(not(target_os = "macos"))]
    {
        if collect_remove_events(_rx, _path, Duration::from_secs(2)) {
            info!("✓ delete {name}: FUSE + listing clean; watcher saw Remove event");
            Ok(())
        } else {
            bail!(
                "delete {name}: FUSE state updated but the native fs watcher saw \
                 no Remove event — FUSE_NOTIFY_DELETE didn't propagate to inotify"
            );
        }
    }
}

/// Confirm a Telegram message is gone server-side. Used by the delete
/// assertions to distinguish "Telegram didn't process the delete" from
/// "the delete update never reached the dispatcher".
async fn verify_telegram_message_absent(client: &Client, peer: PeerRef, id: i32) -> anyhow::Result<()> {
    let mut messages = client.iter_messages(peer);
    while let Some(m) = messages.next().await? {
        if m.id() == id {
            bail!("Telegram message {id} still present after DeleteMessages call");
        }
        // Early-out: iter_messages goes newest-first, so once we pass `id`
        // we know it isn't there.
        if m.id() < id { break; }
    }
    Ok(())
}

/// Wait up to `timeout` for a Remove event referencing `target`. Returns as
/// soon as one shows up; otherwise returns false at deadline. Only used on
/// platforms where the OS-native watcher receives FUSE delete events
/// (currently: not macOS — see `check_watcher_saw_delete`).
#[cfg(not(target_os = "macos"))]
fn collect_remove_events(
    rx: &std_mpsc::Receiver<notify::Result<Event>>,
    target: &Path,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut seen_paths: HashSet<PathBuf> = HashSet::new();
    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        match rx.recv_timeout(remaining) {
            Ok(Ok(ev)) => {
                for p in &ev.paths { seen_paths.insert(p.clone()); }
                if events_contain_remove(&[ev], target) { return true; }
            }
            Ok(Err(e)) => warn!("watcher error: {e:?}"),
            Err(_) => break,
        }
    }
    false
}

// ------------------------ main ----------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = parse_args();
    init_logger(&args.log_level, args.log_file.as_deref())?;

    info!("loading {} and {}", args.config, args.spec);
    let cfg = config::load_config(&args.config)?;
    let spec_bytes = fs::read(&args.spec).with_context(|| format!("reading {}", args.spec))?;
    let spec_root: SpecRoot = serde_yaml::from_slice(&spec_bytes)?;
    let spec = spec_root.channel.clone();
    let hash = spec_hash(&spec_bytes);

    info!("connecting to Telegram…");
    let (client, updates_rx) = connect_and_authorize(&cfg).await?;

    info!("resolving channel '{}'", spec.name);
    let peer = find_channel(&client, &spec.name).await?;

    // Remove any leftover `mutable:` messages from a previous mutation test
    // run BEFORE the sentinel check, so the channel matches the saved spec
    // hash exactly. Mutables aren't in the spec; the runner manages them
    // out-of-band.
    let cleaned = cleanup_mutable_messages(&client, peer).await?;
    if !cleaned.is_empty() {
        info!("startup cleanup: removed {} leftover mutable msg(s): {:?}", cleaned.len(), cleaned);
    }

    let about = read_about(&client, peer).await?;
    let current_hash = parse_sentinel(&about);
    info!("channel sentinel: {:?} (want {})", current_hash, &hash[..16]);

    if current_hash == Some(hash.as_str()) {
        info!("spec hash matches — skipping repopulation");
    } else {
        info!("spec changed — clearing channel and repopulating");
        populate_channel(&client, peer, &spec, &hash).await?;
    }

    // Re-index will pick up the just-uploaded messages.
    let zip_cache = Arc::new(Mutex::new(ZipCache::load("zip_index_cache.json.gz")));

    // ----- Variant A: archive_view = file -----------------------------------
    info!("== variant: archive_view=file, multipart_policy=none ==");
    let expected_files = expected_layout_file_view(&spec.name, &spec)?;
    let var_cfg = variant_config(&cfg, &spec.name, ArchiveView::File, MultipartPolicy::None);
    let exp_a = expected_files.clone();
    let client_a = client.clone();
    let zc_a = Arc::clone(&zip_cache);
    mount_variant(client_a, var_cfg, zc_a, move |root| {
        assert_layout_matches(root, &exp_a)?;
        // Verify the zip file itself is readable as a flat document and its
        // bytes round-trip the locally-built archive.
        for (rel, expected_bytes) in &exp_a {
            if rel.ends_with(".zip") {
                let p = root.join(rel);
                let got = fs::read(&p)?;
                if got.len() != expected_bytes.len() {
                    bail!("zip-as-file size mismatch for {rel}: expected {}, got {}",
                        expected_bytes.len(), got.len());
                }
            }
        }
        Ok(())
    }).await?;

    // ----- Variant B: archive_view = directory ------------------------------
    info!("== variant: archive_view=directory ==");
    let expected_inner = expected_zip_inner_bytes(&spec)?;
    let channel_dir = spec.name.clone();
    let exp_b_inner: HashMap<String, Vec<u8>> = expected_inner.iter()
        .map(|(k, v)| (format!("{}/{}", channel_dir, k), v.clone())).collect();
    // Plus non-zip top-level files (zip files themselves are hidden under `directory`).
    let exp_b_top: HashMap<String, Vec<u8>> = expected_files.iter()
        .filter(|(k, _)| !k.ends_with(".zip"))
        .map(|(k, v)| (k.clone(), v.clone())).collect();
    let var_cfg = variant_config(&cfg, &spec.name, ArchiveView::Directory, MultipartPolicy::None);
    let client_b = client.clone();
    let zc_b = Arc::clone(&zip_cache);
    mount_variant(client_b, var_cfg, zc_b, move |root| {
        assert_layout_matches(root, &exp_b_top)?;
        assert_layout_matches(root, &exp_b_inner)?;
        Ok(())
    }).await?;

    // ----- Variant C: archive_view = file_and_directory ---------------------
    info!("== variant: archive_view=file_and_directory ==");
    let mut exp_c: HashMap<String, Vec<u8>> = expected_files.clone();
    for (k, v) in expected_inner.iter() {
        exp_c.insert(format!("{}/{}", spec.name, k), v.clone());
    }
    let var_cfg = variant_config(&cfg, &spec.name, ArchiveView::FileAndDirectory, MultipartPolicy::None);
    let client_c = client.clone();
    let zc_c = Arc::clone(&zip_cache);
    mount_variant(client_c, var_cfg, zc_c, move |root| {
        assert_layout_matches(root, &exp_c)?;
        Ok(())
    }).await?;

    // ----- Variant D: multipart_policy = suffix ---------------------------------
    info!("== variant: archive_view=file, multipart_policy=suffix ==");
    let expected_suffix = expected_layout_suffix_multipart(&spec.name, &spec)?;
    let var_cfg = variant_config(&cfg, &spec.name, ArchiveView::File, MultipartPolicy::Suffix);
    let exp_d = expected_suffix;
    let client_d = client.clone();
    let zc_d = Arc::clone(&zip_cache);
    mount_variant(client_d, var_cfg, zc_d, move |root| {
        assert_layout_matches(root, &exp_d)
    }).await?;

    // ----- Variant E: multipart_policy = album ----------------------------------
    info!("== variant: archive_view=file, multipart_policy=album ==");
    let expected_album = expected_layout_album_multipart(&spec.name, &spec)?;
    let var_cfg = variant_config(&cfg, &spec.name, ArchiveView::File, MultipartPolicy::Album);
    let exp_e = expected_album;
    let client_e = client.clone();
    let zc_e = Arc::clone(&zip_cache);
    mount_variant(client_e, var_cfg, zc_e, move |root| {
        assert_layout_matches(root, &exp_e)
    }).await?;

    // ----- Variant F: realtime mutations + OS-native filesystem watcher ----
    info!("== variant: realtime mutations + native fs watcher ==");
    run_mutation_test(
        client.clone(),
        &cfg,
        &spec.name,
        peer,
        Arc::clone(&zip_cache),
        updates_rx,
    ).await?;

    info!("all variants OK");
    Ok(())
}
