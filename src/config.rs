use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
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

#[derive(Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ProxyType {
    #[default]
    Socks5,
    Mtproxy,
}

#[derive(Deserialize)]
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
    #[serde(default)]
    pub channels: Vec<ChannelEntry>,
}

#[derive(Deserialize, Clone)]
pub struct SavedMessagesConfig {
    #[serde(default)]
    pub directory: Option<String>,
    #[serde(default = "default_archive_view")]
    pub archive_view: ArchiveView,
}

/// Substitute `$VAR` and `${VAR}` references in `s` with values from the environment.
/// Unset variables are replaced with an empty string.
fn expand_env(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '$' { out.push(c); continue; }
        match chars.peek() {
            Some(&'{') => {
                chars.next();
                let name: String = chars.by_ref().take_while(|&c| c != '}').collect();
                out.push_str(&std::env::var(&name).unwrap_or_default());
            }
            Some(&c2) if c2.is_ascii_alphanumeric() || c2 == '_' => {
                let mut name = String::new();
                while let Some(&nc) = chars.peek() {
                    if nc.is_ascii_alphanumeric() || nc == '_' { name.push(nc); chars.next(); }
                    else { break; }
                }
                let _ = c2;
                out.push_str(&std::env::var(&name).unwrap_or_default());
            }
            _ => out.push('$'),
        }
    }
    out
}

pub fn load_config(path: &str) -> anyhow::Result<Config> {
    let data = std::fs::read_to_string(path)
        .map_err(|_| anyhow::anyhow!("{} not found", path))?;
    Ok(serde_yaml::from_str(&expand_env(&data))?)
}
