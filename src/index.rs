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
    #[serde(default)]
    pub proxy: Option<ProxyConfig>,
    pub channels: Vec<ChannelEntry>,
}

pub struct ArchiveFileEntry {
    pub path: String,
    pub compressed_size: usize,
    pub uncompressed_size: usize,
    pub local_header_offset: u64,
    pub compression_method: u16,
    /// Unix permission bits (lower 12 bits of st_mode), present when the archive
    /// was created on a Unix system (Info-ZIP "version made by" OS byte == 3).
    pub unix_mode: Option<u16>,
}
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
    pub index: HashMap<String, Vec<FileEntry>>,
    // Deduplicated pool of MIME type strings shared across entries.
    pub mime_pool: Vec<String>,
    // Channel-level archive view settings (from `tgfs.yml`).
    pub channel_archive_view: HashMap<String, ArchiveView>,
    // Maps directory name (used in URLs / FUSE paths) → channel name (index key).
    // When no `directory:` override is set, dir name == channel name.
    pub dir_to_channel: HashMap<String, String>,
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
