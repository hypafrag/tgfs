use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use grammers_client::media::Document;
use std::path::PathBuf;
use grammers_client::Client;
use smallvec::SmallVec;

pub type DocParts = SmallVec<[Document; 1]>;

#[derive(Deserialize)]
pub struct ChannelEntry {
    pub name: String,
    #[serde(default)]
    pub directory: Option<String>,
    #[serde(default = "default_archive_view")]
    pub archive_view: ArchiveView,
    #[serde(default)]
    pub skip_deflated_id3v1: bool,
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

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProxyType {
    Socks5,
}

#[derive(Deserialize)]
pub struct ProxyConfig {
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(rename = "type", default)]
    pub _proxy_type: Option<ProxyType>,
}

#[derive(Deserialize)]
pub struct Config {
    pub api_id: i32,
    pub api_hash: String,
    pub phone: String,
    #[serde(default)]
    pub http_port: Option<u16>,
    #[serde(default)]
    pub mount_at: Option<String>,
    /// If set, expose Saved Messages as a top-level directory of this name.
    /// Files tagged via Telegram saved-message reaction tags are grouped under
    /// `<saved_messages>/<tag-name>/`. Untagged files live at the root.
    #[serde(default)]
    pub saved_messages: Option<String>,
    #[serde(default)]
    pub proxy: Option<ProxyConfig>,
    #[serde(default)]
    pub channels: Vec<ChannelEntry>,
}

#[derive(Clone)]
pub struct ArchiveFileEntry {
    pub path: String,
    pub compressed_size: usize,
    pub uncompressed_size: usize,
    pub data_offset: u64,
    pub compression_method: u16,
    /// Unix permission bits (lower 12 bits of st_mode), present when the archive
    /// was created on a Unix system (Info-ZIP "version made by" OS byte == 3).
    pub unix_mode: Option<u16>,
}

// Make ArchiveFileEntry serializable so it can be persisted to disk
impl serde::Serialize for ArchiveFileEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("ArchiveFileEntry", 6)?;
        s.serialize_field("path", &self.path)?;
        s.serialize_field("compressed_size", &self.compressed_size)?;
        s.serialize_field("uncompressed_size", &self.uncompressed_size)?;
        s.serialize_field("data_offset", &self.data_offset)?;
        s.serialize_field("compression_method", &self.compression_method)?;
        s.serialize_field("unix_mode", &self.unix_mode)?;
        s.end()
    }
}

impl<'de> serde::Deserialize<'de> for ArchiveFileEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        #[derive(serde::Deserialize)]
        struct _AFE { path: String, compressed_size: usize, uncompressed_size: usize, data_offset: u64, compression_method: u16, unix_mode: Option<u16> }
        let v = _AFE::deserialize(deserializer)?;
        Ok(ArchiveFileEntry { path: v.path, compressed_size: v.compressed_size, uncompressed_size: v.uncompressed_size, data_offset: v.data_offset, compression_method: v.compression_method, unix_mode: v.unix_mode })
    }
}

#[derive(Clone)]
pub struct FileEntry {
    pub name: String,
    // Optional directory path (relative virtual path) where the file is served.
    // Parsed at indexing time from the `name:` message override if it contains
    // path separators. Example: `dir/subdir/file.ext` -> path=Some("dir/subdir"), name="file.ext".
    pub path: Option<PathBuf>,
    // One or more concatenated document parts. A simple file has a single element
    // stored inline (no heap allocation); multipart files (`<base>.NN` detection)
    // spill to the heap.
    pub parts: DocParts,
    pub size: Option<usize>,
    // Index into the global MIME pool in `AppState`.
    pub mime_idx: usize,
    pub archive_entries: Option<Vec<ArchiveFileEntry>>,
    // Final computed type for the entry: "file", "media", or "zip".
    // This is either parsed from a `type:` message override or derived from
    // the document MIME/type and the original document filename (not the
    // message `name:` override).
    pub file_type: FileType,
}

#[derive(Clone, PartialEq, Eq)]
pub enum FileType {
    File,
    Media,
    Zip,
}

impl FileEntry {
    pub fn first_doc(&self) -> &Document {
        &self.parts[0]
    }

    pub fn doc_name(&self) -> &str {
        self.first_doc().name().unwrap_or(&self.name)
    }
}

pub struct AppState {
    pub client: Client,
    // Deduplicated pool of MIME type strings shared across entries.
    pub mime_pool: Vec<String>,
    // Channel-level runtime attributes populated from config channels.
    pub channels: HashMap<String, TelegramChannel>,
    // Maps directory name (used in URLs / FUSE paths) → channel name (index key).
    // When no `directory:` override is set, dir name == channel name.
    pub dir_to_channel: HashMap<String, String>,
}

#[derive(Clone)]
pub struct TelegramChannel {
    pub archive_view: ArchiveView,
    pub skip_deflated_id3v1: bool,
    pub files: Vec<FileEntry>,
}

pub fn human_size(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "K", "M", "G", "T"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{}B", bytes)
    } else {
        format!("{:.1}{}", value, UNITS[unit])
    }
}

pub struct Entry {
    pub href: String,
    pub label: String,
    pub size: Option<usize>,
}

pub fn dir_listing(title: &str, parent: Option<&str>, entries: &[Entry]) -> String {
    use std::fmt::Write;
    let mut body = String::new();
    write!(body, "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 3.2 Final//EN\">\n<html>\n<head><title>{}</title></head>\n<body>\n<h1>{}</h1>\n<pre>\n", title, title).unwrap();
    if let Some(p) = parent {
        write!(body, "<a href=\"{}\">../</a>\n", p).unwrap();
    }
    for e in entries {
        let size_str = e.size.map_or("-".to_string(), |s| human_size(s));
        write!(body, "<a href=\"{}\">{}</a>  {}\n", e.href, e.label, size_str).unwrap();
    }
    write!(body, "</pre>\n</body>\n</html>").unwrap();
    body
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
                let _ = c2; // already consumed via peek-driven loop above
                out.push_str(&std::env::var(&name).unwrap_or_default());
            }
            _ => out.push('$'),
        }
    }
    out
}

// convenience loader used by main
pub fn load_config(path: &str) -> anyhow::Result<Config> {
    let data = std::fs::read_to_string(path)
        .map_err(|_| anyhow::anyhow!("{} not found", path))?;
    Ok(serde_yaml::from_str(&expand_env(&data))?)
}
