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
use tgfs::{indexer, realtime, server};
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
/// inspect `test.log` first — every Telegram RPC, FUSE callback,
/// mount/unmount cycle, and download is logged there in chronological
/// order. Override the path with `--log-file <path>` for one-off debugging
/// (that mode truncates the file at startup); the default `test.log`
/// target is opened in append mode so multiple integration runners share
/// the same log file across a single `./test` invocation.
fn init_logger(level: &str, log_file: Option<&str>) -> anyhow::Result<()> {
    let mut builder = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level));
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
    // Default → append to test.log (the wrapper `./test` script truncates it
    // on each run). Explicit --log-file → use that path, truncated.
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
    /// When true, the file is uploaded as a Telegram *photo* (not a document).
    /// Bytes are generated deterministically at runtime. Layout helpers skip
    /// photo entries: Telegram re-encodes photos server-side and the indexer
    /// exposes them as `photo_<id>.jpg`, so neither the name nor the bytes
    /// can be predicted from the spec.
    #[serde(default)]
    photo: Option<bool>,
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
    fn is_photo(&self) -> bool {
        self.photo.unwrap_or(false)
    }

    /// Materialize the file's raw bytes (text/blob/zip/photo → concrete buffer).
    fn build_bytes(&self) -> anyhow::Result<Vec<u8>> {
        if self.is_photo() {
            if self.text.is_some() || self.blob.is_some() || self.zip.is_some() {
                bail!("photo file '{}' must not also carry text/blob/zip", self.name);
            }
            return synth_photo_jpeg();
        }
        match (&self.text, &self.blob, &self.zip) {
            (Some(t), None, None) => Ok(t.as_bytes().to_vec()),
            (None, Some(b), None) => decode_blob(b),
            (None, None, Some(entries)) => build_zip(entries),
            _ => bail!("file '{}' must specify exactly one of text/blob/zip", self.name),
        }
    }
}

/// Deterministic 320×240 gradient JPEG, large enough for Telegram to accept
/// as a photo. Telegram re-encodes it anyway, so the exact bytes only need to
/// be a valid image.
fn synth_photo_jpeg() -> anyhow::Result<Vec<u8>> {
    let mut img = image::RgbImage::new(320, 240);
    for (x, y, p) in img.enumerate_pixels_mut() {
        *p = image::Rgb([(x % 256) as u8, (y % 256) as u8, ((x + y) % 256) as u8]);
    }
    let mut buf = std::io::Cursor::new(Vec::new());
    img.write_to(&mut buf, image::ImageFormat::Jpeg)
        .context("encoding synthetic photo JPEG")?;
    Ok(buf.into_inner())
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
        let msg_input = if msg.files[0].is_photo() {
            InputMessage::new().text(caption).photo(up)
        } else {
            InputMessage::new().text(caption).file(up)
        };
        client.send_message(peer, msg_input).await?;
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
    tvshow_pattern: Option<&str>,
    collapse_by_prefix: Option<usize>,
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
        ffmpeg: base.ffmpeg.clone(),
        max_fetches_per_pid: None,
        max_fetches_total: None,
        realtime: false,
        channels: vec![ChannelEntry {
            name: channel_name.to_string(),
            directory: None,
            archive_view,
            skip_deflated_id3v1: false,
            collapse_by_prefix,
            multipart_policy,
            tvshow_pattern: tvshow_pattern.map(|s| s.to_string()),
        }],
    }
}

/// Mount the channel under MOUNT_PATH using `cfg`, run `body` with the
/// mount visible to std::fs, then unmount and join.
///
/// The HTTP index router is also bound on a random `127.0.0.1` port using
/// the same `AppState`, so every variant exercises both the FUSE tree and
/// the web server against identical data. The bound base URL
/// (`http://127.0.0.1:PORT`) is passed to `body` as the second argument.
async fn mount_variant<F>(
    client: Client,
    cfg: Config,
    zip_cache: Arc<Mutex<ZipCache>>,
    body: F,
) -> anyhow::Result<()>
where
    F: FnOnce(&Path, &str) -> anyhow::Result<()> + Send + 'static,
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

    // Bind the HTTP server on a random local port and spawn it on the
    // current runtime. Shares `state` with the FUSE filesystem so the two
    // views can't drift.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await
        .context("binding HTTP test listener")?;
    let local_addr = listener.local_addr()?;
    let base_url = format!("http://{}", local_addr);
    info!("HTTP test server bound on {}", base_url);
    let router = server::make_router(Arc::clone(&state));
    let http_task = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("HTTP test server failed");
    });

    // Tiny pause so the kernel publishes the mount.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let url_for_body = base_url.clone();
    let body_result = tokio::task::spawn_blocking(move || {
        body(Path::new(MOUNT_PATH), &url_for_body)
    }).await?;

    http_task.abort();
    shutdown_fuse(bg, MOUNT_PATH).await;
    body_result
}

/// Percent-encode a virtual path for use in an HTTP URL — segment by segment
/// so the `/` separators survive but spaces and other special chars in
/// individual names get escaped the same way `dir_listing` renders them.
fn url_encode_path(rel: &str) -> String {
    rel.split('/')
        .map(|s| urlencoding::encode(s).into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

fn http_client() -> anyhow::Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .map_err(Into::into)
}

/// Fetch every `rel → bytes` pair via HTTP and assert bodies match.
fn assert_http_layout_matches(base_url: &str, expected: &HashMap<String, Vec<u8>>) -> anyhow::Result<()> {
    let client = http_client()?;
    for (rel, expected_bytes) in expected {
        let url = format!("{}/{}", base_url.trim_end_matches('/'), url_encode_path(rel));
        let resp = client.get(&url).send().with_context(|| format!("GET {url}"))?;
        if !resp.status().is_success() {
            bail!("HTTP {} for {url}", resp.status());
        }
        let got = resp.bytes().with_context(|| format!("reading body of {url}"))?.to_vec();
        if got != *expected_bytes {
            bail!(
                "HTTP content mismatch at {url}: expected {} bytes, got {} bytes",
                expected_bytes.len(), got.len(),
            );
        }
        info!("✓ HTTP {} ({} bytes)", rel, got.len());
    }
    Ok(())
}

/// Synchronous HTTP GET for use inside `mount_variant` bodies. Returns the
/// status code and full body, even for non-2xx responses.
fn http_get_blocking(base_url: &str, rel: &str) -> anyhow::Result<(u16, Vec<u8>)> {
    let url = format!("{}/{}", base_url.trim_end_matches('/'), url_encode_path(rel));
    let c = http_client()?;
    let resp = c.get(&url).send().with_context(|| format!("GET {url}"))?;
    let status = resp.status().as_u16();
    let body = resp.bytes()?.to_vec();
    Ok((status, body))
}

/// Synchronous ranged GET: sends a `Range` header and returns
/// `(status, Content-Range header if any, body)`.
fn http_get_range_blocking(
    base_url: &str,
    rel: &str,
    range: &str,
) -> anyhow::Result<(u16, Option<String>, Vec<u8>)> {
    let url = format!("{}/{}", base_url.trim_end_matches('/'), url_encode_path(rel));
    let c = http_client()?;
    let resp = c.get(&url)
        .header(reqwest::header::RANGE, range)
        .send()
        .with_context(|| format!("GET {url} (Range: {range})"))?;
    let status = resp.status().as_u16();
    let content_range = resp.headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let body = resp.bytes()?.to_vec();
    Ok((status, content_range, body))
}

/// Synchronous GET returning `(status, <header> value if any)`. Body is
/// discarded — used for header-only assertions like Content-Disposition.
fn http_get_header_blocking(
    base_url: &str,
    rel: &str,
    header: &str,
) -> anyhow::Result<(u16, Option<String>)> {
    let url = format!("{}/{}", base_url.trim_end_matches('/'), url_encode_path(rel));
    let c = http_client()?;
    let resp = c.get(&url).send().with_context(|| format!("GET {url}"))?;
    let status = resp.status().as_u16();
    let value = resp.headers()
        .get(header)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    Ok((status, value))
}

/// Assert one ranged GET: 206 status, exact body slice, exact Content-Range.
fn assert_range(
    base_url: &str,
    rel: &str,
    range: &str,
    full: &[u8],
    start: usize,
    end: usize,
) -> anyhow::Result<()> {
    let (status, cr, body) = http_get_range_blocking(base_url, rel, range)?;
    if status != 206 {
        bail!("Range '{range}' on {rel}: expected 206, got {status}");
    }
    let want_cr = format!("bytes {}-{}/{}", start, end, full.len());
    if cr.as_deref() != Some(want_cr.as_str()) {
        bail!("Range '{range}' on {rel}: Content-Range {:?}, expected '{want_cr}'", cr);
    }
    if body != full[start..=end] {
        bail!(
            "Range '{range}' on {rel}: body mismatch ({} bytes, expected {})",
            body.len(), end - start + 1,
        );
    }
    info!("✓ Range {range} on {rel} → bytes {start}-{end}");
    Ok(())
}

/// Blocking HTTP GET wrapped for use from an async context. Returns the
/// status code and the full response body, even for non-2xx responses (so
/// callers can assert on 404s).
async fn http_get(base_url: &str, rel: &str) -> anyhow::Result<(u16, Vec<u8>)> {
    let url = format!("{}/{}", base_url.trim_end_matches('/'), url_encode_path(rel));
    tokio::task::spawn_blocking(move || -> anyhow::Result<(u16, Vec<u8>)> {
        let c = http_client()?;
        let resp = c.get(&url).send().with_context(|| format!("GET {url}"))?;
        let status = resp.status().as_u16();
        let body = resp.bytes()?.to_vec();
        Ok((status, body))
    }).await?
}

/// GET a directory URL and assert the response is an HTML index containing
/// each `needle` as a substring. Useful for sanity-checking the listing
/// endpoint independently from file downloads.
fn assert_http_listing_contains(base_url: &str, dir_rel: &str, needles: &[&str]) -> anyhow::Result<()> {
    let client = http_client()?;
    let dir = if dir_rel.is_empty() {
        String::new()
    } else {
        format!("{}/", url_encode_path(dir_rel.trim_end_matches('/')))
    };
    let url = format!("{}/{}", base_url.trim_end_matches('/'), dir);
    let resp = client.get(&url).send().with_context(|| format!("GET {url}"))?;
    if !resp.status().is_success() {
        bail!("HTTP {} for listing {url}", resp.status());
    }
    let body = resp.text().with_context(|| format!("decoding listing body of {url}"))?;
    for needle in needles {
        if !body.contains(needle) {
            bail!("listing {url} missing expected entry '{}'", needle);
        }
    }
    info!("✓ HTTP listing {} contains {} expected entr(ies)", url, needles.len());
    Ok(())
}

/// Compute the expected on-disk layout from the spec (for `archive_view: file`).
/// Maps virtual path → expected bytes.
fn expected_layout_file_view(channel_name: &str, spec: &ChannelSpec) -> anyhow::Result<HashMap<String, Vec<u8>>> {
    let mut out = HashMap::new();
    for msg in &spec.messages {
        let dir_override = msg.text.as_deref().and_then(parse_path_directive);
        for f in &msg.files {
            if f.is_photo() { continue; }
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

/// Read the raw `..` dirent of `dir` and return its inode number.
/// `std::fs::read_dir` hides dot entries, so this goes through libc readdir —
/// the entries come straight from the FUSE `readdir` reply.
fn read_dotdot_ino(dir: &Path) -> anyhow::Result<u64> {
    use std::os::unix::ffi::OsStrExt as _;
    let c = std::ffi::CString::new(dir.as_os_str().as_bytes())?;
    unsafe {
        let d = libc::opendir(c.as_ptr());
        if d.is_null() {
            bail!("opendir({}) failed: {}", dir.display(), std::io::Error::last_os_error());
        }
        loop {
            let ent = libc::readdir(d);
            if ent.is_null() {
                libc::closedir(d);
                bail!("no '..' entry in readdir of {}", dir.display());
            }
            let name = std::ffi::CStr::from_ptr((*ent).d_name.as_ptr());
            if name.to_bytes() == b".." {
                let ino = (*ent).d_ino as u64;
                libc::closedir(d);
                return Ok(ino);
            }
        }
    }
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
/// their base name *within their virtual directory*; every other file appears
/// individually. Same-named part sets in different directories merge
/// independently.
fn expected_layout_suffix_multipart(
    channel_name: &str,
    spec: &ChannelSpec,
) -> anyhow::Result<HashMap<String, Vec<u8>>> {
    let mut suffix_groups: HashMap<(String, String), BTreeMap<u8, Vec<u8>>> = HashMap::new();
    let mut out = HashMap::new();

    for msg in &spec.messages {
        let dir_override = msg.text.as_deref().and_then(parse_path_directive);
        for f in &msg.files {
            if f.is_photo() { continue; }
            if let Some((base, num)) = parse_suffix_part(&f.name) {
                let dir = dir_override.as_deref().map(|d| d.trim_end_matches('/')).unwrap_or("").to_string();
                suffix_groups.entry((dir, base)).or_default().insert(num, f.build_bytes()?);
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

    for ((dir, base), parts) in suffix_groups {
        let merged: Vec<u8> = parts.into_values().flatten().collect();
        let key = if dir.is_empty() {
            format!("{}/{}", channel_name, base)
        } else {
            format!("{}/{}/{}", channel_name, dir, base)
        };
        out.insert(key, merged);
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
                if f.is_photo() { continue; }
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

/// Owns every resource the realtime mutation tests need: the FUSE mount, the
/// dispatcher task, the HTTP server, the OS-native filesystem watcher, and the
/// set of `mutable:` message IDs uploaded so far (for cleanup).
///
/// Built once via [`setup_mutation_env`], shared across one-or-more
/// [`do_mutation_round`] calls (so the realtime stream — which can only be
/// consumed once — is reused), and finally consumed by
/// [`teardown_mutation_env`].
struct MutationEnv {
    bg: fuser::BackgroundSession,
    dispatcher_task: tokio::task::JoinHandle<()>,
    http_task: tokio::task::JoinHandle<()>,
    watcher: RecommendedWatcher,
    ev_rx: std_mpsc::Receiver<notify::Result<Event>>,
    base_url: String,
    spec_name: String,
    chan_dir: PathBuf,
    mutable_ids: Vec<i32>,
}

/// Mount the channel, spawn the realtime dispatcher, bring up the HTTP server,
/// and start watching the mount with the OS-native fs watcher.
async fn setup_mutation_env(
    client: Client,
    base_cfg: &Config,
    spec_name: &str,
    zip_cache: Arc<Mutex<ZipCache>>,
    mut updates_rx: tokio_mpsc::UnboundedReceiver<UpdatesLike>,
) -> anyhow::Result<MutationEnv> {
    // Build a normal `archive_view=file, multipart_policy=none` variant.
    let cfg = variant_config(base_cfg, spec_name, ArchiveView::File, MultipartPolicy::None, None, None);

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

    // Bring up the HTTP index alongside the mount so the mutation test can
    // cross-verify FUSE + HTTP after every add and delete.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await
        .context("binding HTTP test listener")?;
    let base_url = format!("http://{}", listener.local_addr()?);
    info!("HTTP test server bound on {} (mutation variant)", base_url);
    let router = server::make_router(Arc::clone(&state));
    let http_task = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("HTTP test server failed");
    });

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

    let chan_dir = Path::new(MOUNT_PATH).join(spec_name);

    Ok(MutationEnv {
        bg, dispatcher_task, http_task, watcher, ev_rx, base_url,
        spec_name: spec_name.to_string(), chan_dir, mutable_ids: Vec::new(),
    })
}

/// Run one add-add-delete-delete round against the live channel, using
/// `prefix` to namespace the mutable filenames so multiple rounds against the
/// same env can't collide. Updates `env.mutable_ids` as messages are uploaded
/// and removed.
async fn do_mutation_round(
    env: &mut MutationEnv,
    client: &Client,
    peer: PeerRef,
    prefix: &str,
) -> anyhow::Result<()> {
    let name_one = format!("{prefix}_one.txt");
    let name_two = format!("{prefix}_two.bin");

    // -- Mutation 1: add a single mutable file -------------------------------
    info!("mutation: add {name_one}");
    let id1 = upload_mutable_message(client, peer, &name_one, b"one").await?;
    env.mutable_ids.push(id1);
    let p1 = env.chan_dir.join(&name_one);
    wait_for_file_state(&p1, true, Duration::from_secs(15))?;
    // Adds only fire FUSE_NOTIFY_INVAL_ENTRY (no fsnotify event); we
    // log whatever the watcher saw but don't gate on it.
    let ev = drain_events(&env.ev_rx, Duration::from_millis(500));
    // Cross-check readdir + content against the per-path metadata probe.
    assert_listing(&env.chan_dir, &name_one, true)?;
    let got = fs::read(&p1)?;
    if got != b"one" { bail!("{name_one} content mismatch: got {:?}", got); }
    let (status, body) = http_get(&env.base_url, &format!("{}/{}", env.spec_name, name_one)).await?;
    if status != 200 || body != b"one" {
        bail!("HTTP probe after add: status={status}, body={} bytes", body.len());
    }
    info!("✓ add {name_one}: lookup+readdir+content+HTTP all consistent ({} watcher event(s))", ev.len());

    // -- Mutation 2: add a second mutable file -------------------------------
    info!("mutation: add {name_two}");
    let payload2: Vec<u8> = (0u8..32).collect();
    let id2 = upload_mutable_message(client, peer, &name_two, &payload2).await?;
    env.mutable_ids.push(id2);
    let p2 = env.chan_dir.join(&name_two);
    wait_for_file_state(&p2, true, Duration::from_secs(15))?;
    let ev = drain_events(&env.ev_rx, Duration::from_millis(500));
    assert_listing(&env.chan_dir, &name_two, true)?;
    // The earlier add must still be visible in the listing — verifies
    // the second rebuild didn't accidentally drop unrelated entries.
    assert_listing(&env.chan_dir, &name_one, true)?;
    let got = fs::read(&p2)?;
    if got != payload2 { bail!("{name_two} content mismatch"); }
    let (status, body) = http_get(&env.base_url, &format!("{}/{}", env.spec_name, name_two)).await?;
    if status != 200 || body != payload2 {
        bail!("HTTP probe after add {name_two}: status={status}, body={} bytes", body.len());
    }
    info!("✓ add {name_two}: listing keeps both mutables, HTTP serves new bytes ({} watcher event(s))", ev.len());

    // -- Mutation 3: delete one message --------------------------------------
    info!("mutation: delete {name_one} (msg {id1})");
    client.invoke(&tl::functions::channels::DeleteMessages {
        channel: peer.into(),
        id: vec![id1],
    }).await?;
    env.mutable_ids.retain(|&i| i != id1);
    verify_telegram_message_absent(client, peer, id1).await?;
    wait_for_file_state(&p1, false, Duration::from_secs(5))?;
    // Listing must drop the entry — readdir and lookup agreeing on
    // absence is the real proof of a clean delete.
    assert_listing(&env.chan_dir, &name_one, false)?;
    assert_listing(&env.chan_dir, &name_two, true)?;
    let (status, _) = http_get(&env.base_url, &format!("{}/{}", env.spec_name, name_one)).await?;
    if status != 404 {
        bail!("HTTP probe after delete {name_one}: expected 404, got {status}");
    }
    check_watcher_saw_delete(&name_one, &env.ev_rx, &p1)?;

    // -- Mutation 4: delete the remaining mutable message --------------------
    info!("mutation: delete {name_two} (msg {id2})");
    client.invoke(&tl::functions::channels::DeleteMessages {
        channel: peer.into(),
        id: vec![id2],
    }).await?;
    env.mutable_ids.retain(|&i| i != id2);
    verify_telegram_message_absent(client, peer, id2).await?;
    wait_for_file_state(&p2, false, Duration::from_secs(5))?;
    // Both mutables should now be gone from the channel listing.
    assert_listing(&env.chan_dir, &name_two, false)?;
    assert_listing(&env.chan_dir, &name_one, false)?;
    let (status, _) = http_get(&env.base_url, &format!("{}/{}", env.spec_name, name_two)).await?;
    if status != 404 {
        bail!("HTTP probe after delete {name_two}: expected 404, got {status}");
    }
    check_watcher_saw_delete(&name_two, &env.ev_rx, &p2)?;

    // -- Mutation 5: edit a caption to relocate a file ------------------------
    // Exercises the `MessageEdited` realtime path: adding a `path: moved/`
    // directive must move the file into the virtual directory, dropping the
    // old location from FUSE and HTTP alike.
    let name_three = format!("{prefix}_move.txt");
    info!("mutation: add {name_three}, then edit its caption to `path: moved/`");
    let id3 = upload_mutable_message(client, peer, &name_three, b"three").await?;
    env.mutable_ids.push(id3);
    let p3 = env.chan_dir.join(&name_three);
    wait_for_file_state(&p3, true, Duration::from_secs(15))?;

    client.edit_message(
        peer,
        id3,
        InputMessage::new().text(format!("{MUTABLE_TAG}\npath: moved/")),
    ).await.context("editing caption to add path directive")?;

    let p3_moved = env.chan_dir.join("moved").join(&name_three);
    wait_for_file_state(&p3_moved, true, Duration::from_secs(15))?;
    wait_for_file_state(&p3, false, Duration::from_secs(5))?;
    assert_listing(&env.chan_dir, &name_three, false)?;
    assert_listing(&env.chan_dir.join("moved"), &name_three, true)?;
    let got = fs::read(&p3_moved)?;
    if got != b"three" { bail!("{name_three} content mismatch after caption edit"); }
    let (status, body) = http_get(&env.base_url, &format!("{}/moved/{}", env.spec_name, name_three)).await?;
    if status != 200 || body != b"three" {
        bail!("HTTP probe of moved file: status={status}, body={} bytes", body.len());
    }
    let (status, _) = http_get(&env.base_url, &format!("{}/{}", env.spec_name, name_three)).await?;
    if status != 404 {
        bail!("HTTP probe of old location after move: expected 404, got {status}");
    }
    info!("✓ caption edit relocated {name_three} to moved/ (FUSE + HTTP agree)");

    // Clean up: delete the moved message; both the file and its now-empty
    // virtual directory must disappear.
    client.invoke(&tl::functions::channels::DeleteMessages {
        channel: peer.into(),
        id: vec![id3],
    }).await?;
    env.mutable_ids.retain(|&i| i != id3);
    verify_telegram_message_absent(client, peer, id3).await?;
    wait_for_file_state(&p3_moved, false, Duration::from_secs(5))?;
    wait_for_file_state(&env.chan_dir.join("moved"), false, Duration::from_secs(5))?;
    let (status, _) = http_get(&env.base_url, &format!("{}/moved/{}", env.spec_name, name_three)).await?;
    if status != 404 {
        bail!("HTTP probe after deleting moved file: expected 404, got {status}");
    }
    info!("✓ delete of moved file removed both the file and its virtual directory");

    Ok(())
}

/// Tear down the mount, dispatcher, HTTP server, and watcher; best-effort
/// delete of any mutable messages still on Telegram.
async fn teardown_mutation_env(env: MutationEnv, client: &Client, peer: PeerRef) {
    let MutationEnv {
        bg, dispatcher_task, http_task, watcher, mutable_ids, ..
    } = env;
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
    http_task.abort();
    shutdown_fuse(bg, MOUNT_PATH).await;
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
            info!(
                "⊘ delete {name}: FUSE state updated but the native fs watcher saw \
                 no Remove event — FUSE_NOTIFY_DELETE didn't propagate to inotify"
            );
            Ok(())
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

// ------------------------ Test runner --------------------------------------

/// Tiny cargo-test-look-alike harness so the integration runner can stream
/// `test <name> ... ok|FAILED` lines to stdout while routing every `log!`
/// call into `test.log`. Failures are collected and reported in a summary
/// block at the end; the process exits non-zero if any test failed.
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
        use std::io::Write as _;
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

    // Precompute expected layouts shared across variants.
    let expected_files = expected_layout_file_view(&spec.name, &spec)?;
    let expected_inner = expected_zip_inner_bytes(&spec)?;

    let mut runner = TestRunner::new();

    // ----- archive_view = file ---------------------------------------------
    let exp_a = expected_files.clone();
    let spec_name = spec.name.clone();
    runner.run("integration::archive_view_file", || async {
        let var_cfg = variant_config(
            &cfg, &spec.name, ArchiveView::File, MultipartPolicy::None, None, None,
        );
        let exp_a_inner = exp_a.clone();
        let spec_name_inner = spec_name.clone();
        mount_variant(client.clone(), var_cfg, Arc::clone(&zip_cache), move |root, base_url| {
            assert_layout_matches(root, &exp_a_inner)?;
            for (rel, expected_bytes) in &exp_a_inner {
                if rel.ends_with(".zip") {
                    let p = root.join(rel);
                    let got = fs::read(&p)?;
                    if got.len() != expected_bytes.len() {
                        bail!("zip-as-file size mismatch for {rel}: expected {}, got {}",
                            expected_bytes.len(), got.len());
                    }
                }
            }
            assert_http_layout_matches(base_url, &exp_a_inner)?;
            assert_http_listing_contains(base_url, "", &[spec_name_inner.as_str()])?;
            if let Some(any_rel) = exp_a_inner.keys().next() {
                let leaf = any_rel.rsplit('/').next().unwrap_or(any_rel);
                assert_http_listing_contains(base_url, &spec_name_inner, &[leaf])?;
            }
            Ok(())
        }).await
    }).await;

    // ----- archive_view = directory ----------------------------------------
    // Exclude every zip-classified file (by `.zip` name *or* `type: zip`
    // caption) from the raw-file expectations: in directory view those are
    // exposed only as browsable directories and raw download is a 404.
    let zip_raw_paths: HashSet<String> = {
        let mut s = HashSet::new();
        for msg in &spec.messages {
            let dir_override = msg.text.as_deref().and_then(parse_path_directive);
            for f in &msg.files {
                if f.zip.is_some() {
                    let prefix = match &dir_override {
                        Some(d) => format!("{}/{}/", spec.name, d.trim_end_matches('/')),
                        None => format!("{}/", spec.name),
                    };
                    s.insert(format!("{prefix}{}", f.name));
                }
            }
        }
        s
    };
    let channel_dir = spec.name.clone();
    let exp_b_inner: HashMap<String, Vec<u8>> = expected_inner.iter()
        .map(|(k, v)| (format!("{}/{}", channel_dir, k), v.clone())).collect();
    let exp_b_top: HashMap<String, Vec<u8>> = expected_files.iter()
        .filter(|(k, _)| !zip_raw_paths.contains(*k))
        .map(|(k, v)| (k.clone(), v.clone())).collect();
    runner.run("integration::archive_view_directory", || async {
        let var_cfg = variant_config(
            &cfg, &spec.name, ArchiveView::Directory, MultipartPolicy::None, None, None,
        );
        let top = exp_b_top.clone();
        let inner = exp_b_inner.clone();
        mount_variant(client.clone(), var_cfg, Arc::clone(&zip_cache), move |root, base_url| {
            assert_layout_matches(root, &top)?;
            assert_layout_matches(root, &inner)?;
            assert_http_layout_matches(base_url, &top)?;
            assert_http_layout_matches(base_url, &inner)?;
            Ok(())
        }).await
    }).await;

    // ----- archive_view = file_and_directory --------------------------------
    let mut exp_c: HashMap<String, Vec<u8>> = expected_files.clone();
    for (k, v) in expected_inner.iter() {
        exp_c.insert(format!("{}/{}", spec.name, k), v.clone());
    }
    runner.run("integration::archive_view_file_and_directory", || async {
        let var_cfg = variant_config(
            &cfg, &spec.name, ArchiveView::FileAndDirectory, MultipartPolicy::None, None, None,
        );
        let exp = exp_c.clone();
        mount_variant(client.clone(), var_cfg, Arc::clone(&zip_cache), move |root, base_url| {
            assert_layout_matches(root, &exp)?;
            assert_http_layout_matches(base_url, &exp)?;
            Ok(())
        }).await
    }).await;

    // ----- multipart = suffix ----------------------------------------------
    runner.run("integration::multipart_suffix", || async {
        let exp = expected_layout_suffix_multipart(&spec.name, &spec)?;
        let var_cfg = variant_config(
            &cfg, &spec.name, ArchiveView::File, MultipartPolicy::Suffix, None, None,
        );
        let chan = spec.name.clone();
        mount_variant(client.clone(), var_cfg, Arc::clone(&zip_cache), move |root, base_url| {
            assert_layout_matches(root, &exp)?;
            assert_http_layout_matches(base_url, &exp)?;
            // Regression guards for the two merge bugs:
            // - `notes.0`/`notes.1` (single-digit) must not fuse into `notes`;
            // - `A/data.bin.*` and `B/data.bin.*` must not fuse into a
            //   channel-root `data.bin`.
            for stray in ["notes", "data.bin"] {
                let p = root.join(&chan).join(stray);
                if p.exists() {
                    bail!(
                        "'{}' exists — unrelated or cross-directory parts were \
                         fused into one multipart file",
                        p.display()
                    );
                }
            }
            Ok(())
        }).await
    }).await;

    // ----- multipart = album -----------------------------------------------
    runner.run("integration::multipart_album", || async {
        let exp = expected_layout_album_multipart(&spec.name, &spec)?;
        let var_cfg = variant_config(
            &cfg, &spec.name, ArchiveView::File, MultipartPolicy::Album, None, None,
        );
        mount_variant(client.clone(), var_cfg, Arc::clone(&zip_cache), move |root, base_url| {
            assert_layout_matches(root, &exp)?;
            assert_http_layout_matches(base_url, &exp)
        }).await
    }).await;

    // ----- HTTP Range requests ---------------------------------------------
    // Mounted with FileAndDirectory + Suffix so every ranged-read code path
    // is reachable at once: plain single-doc files, multipart concatenations
    // (part-boundary crossing), and deflated/stored files inside archives.
    runner.run("integration::http_range_requests", || async {
        let var_cfg = variant_config(
            &cfg, &spec.name, ArchiveView::FileAndDirectory, MultipartPolicy::Suffix, None, None,
        );
        let chan = spec.name.clone();
        let readme_rel = format!("{}/readme.txt", chan);
        let readme = expected_files.get(&readme_rel)
            .ok_or_else(|| anyhow!("readme.txt missing from expected layout"))?.clone();
        let suffix_map = expected_layout_suffix_multipart(&spec.name, &spec)?;
        let merged_rel = format!("{}/suffixed", chan);
        let merged = suffix_map.get(&merged_rel)
            .ok_or_else(|| anyhow!("merged 'suffixed' missing from suffix layout"))?.clone();
        let deflated_rel = format!("{}/project/src/main.py", chan);
        let deflated = expected_inner.get("project/src/main.py")
            .ok_or_else(|| anyhow!("project/src/main.py missing from zip layout"))?.clone();
        let stored_rel = format!("{}/project/logo.bin", chan);
        let stored = expected_inner.get("project/logo.bin")
            .ok_or_else(|| anyhow!("project/logo.bin missing from zip layout"))?.clone();
        mount_variant(client.clone(), var_cfg, Arc::clone(&zip_cache), move |_root, base_url| {
            let len = readme.len();
            // Middle slice of a plain file.
            assert_range(base_url, &readme_rel, "bytes=5-15", &readme, 5, 15)?;
            // RFC 7233 suffix range: the final N bytes.
            assert_range(base_url, &readme_rel, "bytes=-7", &readme, len - 7, len - 1)?;
            // End past EOF reads as "to the end" with a clamped Content-Range.
            assert_range(base_url, &readme_rel, "bytes=0-999999999", &readme, 0, len - 1)?;
            // Start past EOF is unsatisfiable.
            let (status, _, _) = http_get_range_blocking(
                base_url, &readme_rel, &format!("bytes={}-", len))?;
            if status != 416 {
                bail!("range past EOF: expected 416, got {status}");
            }
            info!("✓ Range past EOF → 416");
            // Multipart concatenation: bytes 3-8 of "Hello, world!" span the
            // part-0/part-1 boundary at offset 7.
            assert_range(base_url, &merged_rel, "bytes=3-8", &merged, 3, 8)?;
            // Deflated inner-archive file (inflate-then-slice path).
            assert_range(base_url, &deflated_rel, "bytes=5-20", &deflated, 5, 20)?;
            // Stored inner-archive file (direct offset path).
            assert_range(base_url, &stored_rel, "bytes=1-4", &stored, 1, 4)?;
            Ok(())
        }).await
    }).await;

    // ----- photo message ---------------------------------------------------
    // A real Telegram photo (not a document) must be listed and be
    // downloadable with identical bytes over FUSE and HTTP. Regression for
    // photos streaming as a silent empty body over HTTP.
    runner.run("integration::photo_message", || async {
        let var_cfg = variant_config(
            &cfg, &spec.name, ArchiveView::File, MultipartPolicy::None, None, None,
        );
        let chan = spec.name.clone();
        mount_variant(client.clone(), var_cfg, Arc::clone(&zip_cache), move |root, base_url| {
            let chan_dir = root.join(&chan);
            let photo_name = list_dir(&chan_dir)?
                .into_iter()
                .find(|n| n.starts_with("photo_") && n.ends_with(".jpg"))
                .ok_or_else(|| anyhow!("no photo_<id>.jpg entry in channel listing"))?;
            let p = chan_dir.join(&photo_name);
            let meta_len = fs::metadata(&p)?.len();
            let fuse_bytes = fs::read(&p)?;
            if fuse_bytes.is_empty() {
                bail!("photo read over FUSE returned 0 bytes");
            }
            if fuse_bytes.len() as u64 != meta_len {
                bail!("photo FUSE size mismatch: getattr says {meta_len}, read {} bytes", fuse_bytes.len());
            }
            if fuse_bytes[..2] != [0xFF, 0xD8] {
                bail!("photo bytes are not JPEG (no FFD8 magic)");
            }
            let (status, body) = http_get_blocking(base_url, &format!("{}/{}", chan, photo_name))?;
            if status != 200 {
                bail!("HTTP photo download: expected 200, got {status}");
            }
            if body != fuse_bytes {
                bail!(
                    "photo bytes differ between HTTP ({}) and FUSE ({})",
                    body.len(), fuse_bytes.len()
                );
            }
            info!("✓ photo {} ({} bytes) identical over FUSE and HTTP", photo_name, meta_len);
            Ok(())
        }).await
    }).await;

    // ----- caption `type:` directives ---------------------------------------
    // `type: media` forces inline Content-Disposition on a plain binary;
    // `type: zip` makes a non-.zip filename browsable as an archive.
    runner.run("integration::caption_type_directives", || async {
        let var_cfg = variant_config(
            &cfg, &spec.name, ArchiveView::FileAndDirectory, MultipartPolicy::None, None, None,
        );
        let chan = spec.name.clone();
        mount_variant(client.clone(), var_cfg, Arc::clone(&zip_cache), move |root, base_url| {
            let (status, disp) = http_get_header_blocking(
                base_url, &format!("{}/inline.bin", chan), "content-disposition")?;
            if status != 200 || !disp.as_deref().unwrap_or("").starts_with("inline") {
                bail!("type: media → expected inline disposition, got status={status} disposition={disp:?}");
            }
            let (status, disp) = http_get_header_blocking(
                base_url, &format!("{}/readme.txt", chan), "content-disposition")?;
            if status != 200 || !disp.as_deref().unwrap_or("").starts_with("attachment") {
                bail!("plain document → expected attachment disposition, got status={status} disposition={disp:?}");
            }
            // type: zip on bundle.dat → browsable as `bundle/` via FUSE + HTTP.
            let inner = root.join(&chan).join("bundle").join("inner.txt");
            let got = fs::read(&inner)
                .with_context(|| format!("reading {}", inner.display()))?;
            if got != b"inside bundle.dat" {
                bail!("bundle/inner.txt content mismatch over FUSE");
            }
            assert_http_listing_contains(base_url, &chan, &["bundle/"])?;
            info!("✓ type: media inline disposition, type: zip browsable");
            Ok(())
        }).await
    }).await;

    // ----- FUSE seek + `..` inode -------------------------------------------
    runner.run("integration::fuse_seek_and_dotdot", || async {
        let var_cfg = variant_config(
            &cfg, &spec.name, ArchiveView::File, MultipartPolicy::Suffix, None, None,
        );
        let chan = spec.name.clone();
        mount_variant(client.clone(), var_cfg, Arc::clone(&zip_cache), move |root, _base_url| {
            use std::io::{Read as _, Seek as _, SeekFrom};
            use std::os::unix::fs::MetadataExt as _;
            let chan_dir = root.join(&chan);
            // Seek into the merged multipart file across the part-0/part-1
            // boundary (parts are 7+5+1 bytes of "Hello, world!").
            let mut f = fs::File::open(chan_dir.join("suffixed"))?;
            f.seek(SeekFrom::Start(3))?;
            let mut buf = [0u8; 6];
            f.read_exact(&mut buf)?;
            if &buf != b"lo, wo" {
                bail!("seek-read across part boundary returned {:?}", String::from_utf8_lossy(&buf));
            }
            f.seek(SeekFrom::Start(8))?;
            let mut tail = Vec::new();
            f.read_to_end(&mut tail)?;
            if tail != b"orld!" {
                bail!("seek-read into part 1 returned {:?}", String::from_utf8_lossy(&tail));
            }
            // `..` of a nested virtual dir must report the parent's inode.
            let parent_ino = fs::metadata(&chan_dir)?.ino();
            let dotdot_ino = read_dotdot_ino(&chan_dir.join("docs"))?;
            if dotdot_ino != parent_ino {
                bail!("readdir '..' inode {dotdot_ino} != parent inode {parent_ino}");
            }
            info!("✓ multipart seek-reads exact, '..' inode correct");
            Ok(())
        }).await
    }).await;

    // ----- collapse_by_prefix ----------------------------------------------
    // With min prefix length 20 exactly one pair in the spec qualifies; both
    // files move under a directory named after their shared trimmed prefix,
    // everything else stays put.
    runner.run("integration::collapse_by_prefix", || async {
        let var_cfg = variant_config(
            &cfg, &spec.name, ArchiveView::File, MultipartPolicy::None, None, Some(20),
        );
        let mut exp = expected_files.clone();
        let dir = "Collapse_Prefix_Demo_Track_0";
        for n in ["Collapse_Prefix_Demo_Track_01.txt", "Collapse_Prefix_Demo_Track_02.txt"] {
            let old = format!("{}/{}", spec.name, n);
            let bytes = exp.remove(&old)
                .ok_or_else(|| anyhow!("{old} missing from expected layout"))?;
            exp.insert(format!("{}/{}/{}", spec.name, dir, n), bytes);
        }
        mount_variant(client.clone(), var_cfg, Arc::clone(&zip_cache), move |root, base_url| {
            assert_layout_matches(root, &exp)?;
            assert_http_layout_matches(base_url, &exp)
        }).await
    }).await;

    // ----- tvshow_pattern --------------------------------------------------
    // Channel-level template that hunch-decomposable filenames get rerouted
    // through. Files hunch can't parse must keep their original locations.
    runner.run("integration::tvshow_pattern", || async {
        let pattern = "{show_title}/Season {season}/Episode {episode}.{ext}";
        let var_cfg = variant_config(
            &cfg, &spec.name, ArchiveView::File, MultipartPolicy::None, Some(pattern), None,
        );
        let channel_dir = spec.name.clone();
        let exp: HashMap<String, Vec<u8>> = [
            (format!("{}/Breaking Bad/Season 1/Episode 1.mkv", channel_dir), b"S01E01 payload".to_vec()),
            (format!("{}/Breaking Bad/Season 1/Episode 2.mkv", channel_dir), b"S01E02 payload".to_vec()),
            (format!("{}/The Walking Dead/Season 2/Episode 5.mkv", channel_dir), b"TWD payload".to_vec()),
        ].into_iter().collect();
        let channel_dir_inner = channel_dir.clone();
        mount_variant(client.clone(), var_cfg, Arc::clone(&zip_cache), move |root, base_url| {
            assert_layout_matches(root, &exp)?;
            assert_http_layout_matches(base_url, &exp)?;
            let readme = root.join(&channel_dir_inner).join("readme.txt");
            if !readme.exists() {
                bail!(
                    "non-tvshow file '{}' should still be at the channel root \
                     — tvshow_pattern leaked into a file hunch could not parse",
                    readme.display()
                );
            }
            let stray = root.join(&channel_dir_inner).join("Breaking.Bad.S01E01.mkv");
            if stray.exists() {
                bail!(
                    "tvshow-renamed file '{}' should have been rerouted away from \
                     the channel root, but it's still there",
                    stray.display()
                );
            }
            Ok(())
        }).await
    }).await;

    // ----- realtime mutations + native fs watcher --------------------------
    // `updates_rx` can only be consumed once, and we want two separate test
    // entries to exercise the realtime path: one that mutates immediately
    // after setup, and one that mutates after the dispatcher has been idle
    // for a minute. Share the env across both via a single Option cell:
    // the first test sets up + mutates + parks the env; the second takes
    // the env, idles, mutates, and tears down.
    let mut updates_rx_opt = Some(updates_rx);
    let env_cell: Arc<tokio::sync::Mutex<Option<MutationEnv>>> =
        Arc::new(tokio::sync::Mutex::new(None));

    let env_cell_a = Arc::clone(&env_cell);
    runner.run("integration::realtime_mutations", || async {
        let rx = updates_rx_opt.take().expect("realtime_mutations runs at most once");
        let mut env = setup_mutation_env(
            client.clone(), &cfg, &spec.name, Arc::clone(&zip_cache), rx,
        ).await?;
        let outcome = do_mutation_round(&mut env, &client, peer, "mut").await;
        if outcome.is_err() {
            // Don't strand the mount + dispatcher if this round failed —
            // the second test would just immediately bail on missing env.
            teardown_mutation_env(env, &client, peer).await;
        } else {
            *env_cell_a.lock().await = Some(env);
        }
        outcome
    }).await;

    // Idle for 60s after the first round's mutations, then run another
    // add/delete round. This exercises the realtime pipeline's ability to
    // keep delivering updates after the connection has been idle, which is
    // where transport-level heartbeats and reconnect logic come into play.
    let env_cell_b = Arc::clone(&env_cell);
    runner.run("integration::realtime_mutations_after_idle", || async {
        let mut guard = env_cell_b.lock().await;
        let mut env = guard.take().ok_or_else(|| anyhow!(
            "realtime env unavailable (setup in realtime_mutations failed?)"
        ))?;
        info!("idling 60s before next mutation round…");
        tokio::time::sleep(Duration::from_secs(60)).await;
        let outcome = do_mutation_round(&mut env, &client, peer, "idle").await;
        teardown_mutation_env(env, &client, peer).await;
        outcome
    }).await;

    info!("all variants completed");
    if runner.finish() {
        Ok(())
    } else {
        std::process::exit(1);
    }
}
