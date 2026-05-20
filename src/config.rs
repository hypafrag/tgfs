use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize)]
pub struct ChannelEntry {
    pub name: String,
    #[serde(default)]
    pub directory: Option<String>,
    #[serde(default = "default_archive_view")]
    pub archive_view: ArchiveView,
    #[serde(default)]
    pub skip_deflated_id3v1: bool,
    /// Minimum length of a shared name prefix required to collapse files at
    /// the same virtual directory level into a new sub-directory named after
    /// that prefix (trailing whitespace stripped). 0 / absent = disabled.
    #[serde(default)]
    pub collapse_by_prefix: Option<usize>,
    /// How to merge several Telegram messages into one logical file. Default
    /// `none` (no merging). See `MultipartPolicy` for the available strategies.
    #[serde(default)]
    pub multipart_policy: MultipartPolicy,
    /// Optional virtual-path template applied to every file whose filename
    /// hunch can decompose into show title + season + episode. Supports the
    /// placeholders `{show_title}`, `{season}`, `{season:0N}`, `{episode}`,
    /// `{episode:0N}`, `{year}`, `{year:0N}`, `{ext}`. Files that don't parse
    /// (movies, generic uploads) are unaffected.
    #[serde(default)]
    pub tvshow_pattern: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Default, Debug)]
#[serde(rename_all = "snake_case")]
pub enum MultipartPolicy {
    /// No multipart detection — every message is its own file.
    #[default]
    None,
    /// Auto-merge documents whose filenames match `<base>.NN` (two-digit part
    /// numbers starting at `.00`, contiguous).
    Suffix,
    /// Merge every file attached to a Telegram album whose caption carries a
    /// `multipart:` (or `multipart: true`) directive. Parts are concatenated
    /// in chronological (msg_id ascending) order.
    Album,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveView {
    File,
    Directory,
    FileAndDirectory,
}

fn default_archive_view() -> ArchiveView {
    ArchiveView::File
}

#[derive(Deserialize, Default, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum ProxyType {
    #[default]
    Socks5,
    Mtproxy,
}

#[derive(Deserialize, Clone)]
pub struct ProxyConfig {
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// Hex-encoded MTProxy secret (16 bytes = 32 hex chars). A leading `dd` prefix is
    /// accepted and stripped (it marks FakeTLS on the proxy side; the underlying 16-byte
    /// secret is used for standard obfuscated transport on the client side).
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(rename = "type", default)]
    pub proxy_type: ProxyType,
}

/// Logging configuration from `tgfs.yml`.
///
/// Either a bare level string (`log: debug`) or a per-module map:
/// ```yaml
/// log:
///   tgfs: debug
///   grammers_mtsender: warn
/// ```
#[derive(Deserialize, Clone)]
#[serde(untagged)]
pub enum LogConfig {
    /// Global level (e.g. `"debug"`, `"trace"`).
    Level(String),
    /// Per-module directives, each value a level string.
    Modules(HashMap<String, String>),
}

impl LogConfig {
    /// Convert to an `env_logger` filter string (e.g. `"tgfs=debug,grammers_mtsender=warn"`).
    pub fn to_filter_string(&self) -> String {
        match self {
            LogConfig::Level(l) => l.clone(),
            LogConfig::Modules(map) => map.iter()
                .map(|(module, level)| format!("{}={}", module, level))
                .collect::<Vec<_>>()
                .join(","),
        }
    }
}

/// ffmpeg invocation parameters used by `tgup --encode-video`. The actual
/// ffmpeg argv list is built in `src/bin/tgup/ffmpeg.rs::build_encode_args`
/// from the structured fields below; thumbnail args are hardcoded.
#[derive(Deserialize, Clone, Default)]
pub struct FfmpegConfig {
    #[serde(default)]
    pub encode_args: EncodeArgs,
}

#[derive(Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct EncodeArgs {
    #[serde(default)]
    pub threads: Threads,
    #[serde(default)]
    pub video: VideoArgs,
    #[serde(default)]
    pub audio: AudioArgs,
}

/// ffmpeg `-threads`. `auto` (default) resolves to
/// `std::thread::available_parallelism()` at run time; an integer is passed
/// through verbatim.
#[derive(Clone, Debug)]
pub enum Threads {
    Auto,
    Count(u32),
}

impl Default for Threads {
    fn default() -> Self { Threads::Auto }
}

impl<'de> Deserialize<'de> for Threads {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let v = serde_yaml::Value::deserialize(d)?;
        if let Some(s) = v.as_str() {
            if s == "auto" {
                Ok(Threads::Auto)
            } else {
                Err(D::Error::custom(format!(
                    "ffmpeg.encode_args.threads: expected integer or \"auto\", got \"{}\"",
                    s
                )))
            }
        } else if let Some(n) = v.as_u64() {
            if n == 0 {
                Err(D::Error::custom("ffmpeg.encode_args.threads: must be > 0"))
            } else {
                Ok(Threads::Count(n as u32))
            }
        } else {
            Err(D::Error::custom(
                "ffmpeg.encode_args.threads: expected integer or \"auto\"",
            ))
        }
    }
}

/// Controls how the encoded MP4 output is made streamable/seekable.
#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Streamification {
    /// No streaming flags — plain MP4, moov at end. Use for archival encodes
    /// where streaming is not required.
    None,
    /// Fragmented MP4: `-movflags +frag_keyframe+empty_moov+default_base_moof`.
    /// moov atoms are spread across fragments so playback can start from the
    /// first byte. Works over a pipe — no scratch file needed.
    #[default]
    Fmp4,
    /// Plain MP4 with `+faststart`: ffmpeg writes a regular MP4 then shuffles
    /// the moov atom to the front. Requires a seekable output, so the encode
    /// goes to a scratch file under `/tmp/tgup/` first; tgup uploads that file
    /// and deletes it on completion.
    LeadingMoov,
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct VideoArgs {
    /// `auto` (default) picks `h264_videotoolbox` on macOS and `libx264`
    /// elsewhere. Any other value is passed straight to `-c:v`.
    #[serde(default = "default_video_codec")]
    pub codec: String,
    /// libx264 `-preset` value. Ignored when `codec` is not libx264.
    #[serde(default = "default_libx264_preset")]
    pub libx264preset: String,
    /// Target vertical resolution in lines. Width is computed by the scale
    /// filter to preserve aspect ratio (and then forced to an even value
    /// because `force_original_aspect_ratio=decrease` overrides the `-2`
    /// divisibility hint and can produce odd widths libx264 rejects).
    #[serde(default = "default_vres")]
    pub vres: u32,
    /// How to make the output seekable/streamable. Default is `fmp4`.
    /// See [`Streamification`] for details on each mode.
    #[serde(default)]
    pub streamification: Streamification,
}

impl Default for VideoArgs {
    fn default() -> Self {
        Self {
            codec: default_video_codec(),
            libx264preset: default_libx264_preset(),
            vres: default_vres(),
            streamification: Streamification::default(),
        }
    }
}

fn default_video_codec() -> String { "auto".into() }
fn default_libx264_preset() -> String { "slow".into() }
fn default_vres() -> u32 { 720 }

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct AudioArgs {
    #[serde(default = "default_audio_codec")]
    pub codec: String,
    #[serde(default = "default_audio_bitrate")]
    pub bitrate: String,
    #[serde(default = "default_audio_sample_rate")]
    pub sample_rate: u32,
}

impl Default for AudioArgs {
    fn default() -> Self {
        Self {
            codec: default_audio_codec(),
            bitrate: default_audio_bitrate(),
            sample_rate: default_audio_sample_rate(),
        }
    }
}

fn default_audio_codec() -> String { "aac".into() }
fn default_audio_bitrate() -> String { "128k".into() }
fn default_audio_sample_rate() -> u32 { 48000 }

#[derive(Deserialize)]
pub struct Config {
    pub api_id: i32,
    pub api_hash: String,
    pub phone: String,
    #[serde(default)]
    pub log: Option<LogConfig>,
    #[serde(default)]
    pub http_port: Option<u16>,
    #[serde(default)]
    pub mount_at: Option<String>,
    /// ffmpeg invocation parameters. Used only by `tgup --encode-video`; the
    /// daemon itself never shells out to ffmpeg.
    #[serde(default)]
    pub ffmpeg: FfmpegConfig,
    /// If set, expose Saved Messages as a top-level directory.
    /// Use `directory` to set the top-level directory name; `archive_view`
    /// controls how ZIP archives are exposed (`file|directory|file_and_directory`).
    #[serde(default)]
    pub saved_messages: Option<SavedMessagesConfig>,
    #[serde(default)]
    pub proxy: Option<ProxyConfig>,
    /// Maximum number of concurrent Telegram fetches a single PID may have
    /// in-flight through the FUSE mount. Extra reads block until a slot opens.
    #[serde(default)]
    pub max_fetches_per_pid: Option<usize>,
    /// Maximum number of concurrent Telegram fetches across all PIDs combined.
    #[serde(default)]
    pub max_fetches_total: Option<usize>,
    /// Subscribe to Telegram updates and mutate the in-memory index when
    /// channel messages are added, edited, or deleted. When mounted via FUSE,
    /// kernel cache and inotify watchers are notified on each change. Default true.
    #[serde(default = "default_realtime")]
    pub realtime: bool,
    #[serde(default)]
    pub channels: Vec<ChannelEntry>,
}

fn default_realtime() -> bool { true }

#[derive(Deserialize, Clone)]
pub struct SavedMessagesConfig {
    #[serde(default)]
    pub directory: Option<String>,
    #[serde(default = "default_archive_view")]
    pub archive_view: ArchiveView,
}

/// Substitute `$VAR` and `${VAR}` references in `s`. `dotenv` (typically the
/// contents of `~/.config/tgfs/.env`) wins over the process environment; unset
/// variables expand to an empty string.
fn expand_env_with(s: &str, dotenv: &HashMap<String, String>) -> String {
    let lookup = |name: &str| -> String {
        if let Some(v) = dotenv.get(name) { return v.clone(); }
        std::env::var(name).unwrap_or_default()
    };
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '$' { out.push(c); continue; }
        match chars.peek() {
            Some(&'{') => {
                chars.next();
                let name: String = chars.by_ref().take_while(|&c| c != '}').collect();
                out.push_str(&lookup(&name));
            }
            Some(&c2) if c2.is_ascii_alphanumeric() || c2 == '_' => {
                let mut name = String::new();
                while let Some(&nc) = chars.peek() {
                    if nc.is_ascii_alphanumeric() || nc == '_' { name.push(nc); chars.next(); }
                    else { break; }
                }
                let _ = c2;
                out.push_str(&lookup(&name));
            }
            _ => out.push('$'),
        }
    }
    out
}

/// Path to the optional dotenv file. `$HOME/.config/tgfs/.env`. Absent /
/// unreadable file → empty map; never errors.
fn dotenv_path() -> Option<std::path::PathBuf> {
    let home = std::env::var_os("HOME")?;
    Some(std::path::PathBuf::from(home).join(".config/tgfs/.env"))
}

fn load_dotenv() -> HashMap<String, String> {
    match dotenv_path().and_then(|p| std::fs::read_to_string(&p).ok()) {
        Some(data) => parse_dotenv(&data),
        None => HashMap::new(),
    }
}

/// Minimal dotenv parser: `KEY=VALUE` per line, `#` comments, blank lines
/// ignored, surrounding single/double quotes around values stripped. No
/// shell expansion or `export` prefix support — keep the format obvious so
/// `${VAR}` resolution stays predictable.
fn parse_dotenv(data: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for raw in data.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') { continue; }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let (k, v) = match line.split_once('=') {
            Some(pair) => pair,
            None => continue,
        };
        let key = k.trim().to_string();
        if key.is_empty() { continue; }
        let val = strip_matched_quotes(v.trim()).to_string();
        out.insert(key, val);
    }
    out
}

fn strip_matched_quotes(s: &str) -> &str {
    let bytes = s.as_bytes();
    if bytes.len() >= 2
        && ((bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\''))
    {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

#[cfg(test)]
fn expand_env(s: &str) -> String {
    expand_env_with(s, &HashMap::new())
}

pub fn load_config(path: &str) -> anyhow::Result<Config> {
    let data = std::fs::read_to_string(path)
        .map_err(|_| anyhow::anyhow!("{} not found", path))?;
    let dotenv = load_dotenv();
    Ok(serde_yaml::from_str(&expand_env_with(&data, &dotenv))?)
}


#[cfg(test)]
#[path = "../tests/config.rs"]
mod tests;
