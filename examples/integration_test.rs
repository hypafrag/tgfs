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

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use base64::Engine;
use grammers_client::media::Uploaded;
use grammers_client::message::InputMessage;
use grammers_client::peer::Peer;
use grammers_client::Client;
use grammers_client::tl;
use grammers_session::types::PeerRef;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use zip::write::{FileOptions, ZipWriter};
use zip::CompressionMethod;

use tgfs::config::{self, ArchiveView, ChannelEntry, Config, MultipartPolicy};
use tgfs::index::{AppState, MimePool};
use tgfs::indexer;
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

    if let Err(e) = bg.umount_and_join() {
        warn!("FUSE umount returned error: {e}");
    }
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
    let (client, _updates_rx) = connect_and_authorize(&cfg).await?;

    info!("resolving channel '{}'", spec.name);
    let peer = find_channel(&client, &spec.name).await?;

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

    info!("all variants OK");
    Ok(())
}
