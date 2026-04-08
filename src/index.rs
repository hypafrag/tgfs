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
pub struct Config {
    #[serde(default)]
    pub http_port: Option<u16>,
    #[serde(default)]
    pub mount_at: Option<String>,
    pub channels: Vec<ChannelEntry>,
}

pub struct ArchiveFileEntry {
    pub path: String,
    pub compressed_size: usize,
    pub uncompressed_size: usize,
    pub local_header_offset: u64,
    pub compression_method: u16,
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

// convenience loader used by main
pub fn load_config(path: &str) -> anyhow::Result<Config> {
    let data = std::fs::read_to_string(path)
        .map_err(|_| anyhow::anyhow!("{} not found", path))?;
    Ok(serde_yaml::from_str(&data)?)
}
