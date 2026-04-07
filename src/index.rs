use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use grammers_client::media::Document;
use grammers_client::Client;

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
    #[serde(default = "default_port")]
    pub port: u16,
    pub channels: Vec<ChannelEntry>,
}

fn default_port() -> u16 { 8080 }

#[allow(dead_code)]
pub struct ArchiveFileEntry {
    pub path: String,
    pub compressed_size: usize,
    pub uncompressed_size: usize,
    pub compression_method: u16,
    pub local_header_offset: u64,
}

#[allow(dead_code)]
pub struct FileEntry {
    pub name: String,
    pub doc: Document,
    pub size: Option<usize>,
    pub mime_type: String,
    pub is_zip: bool,
    pub archive_entries: Option<Vec<ArchiveFileEntry>>,
    pub archive_view: ArchiveView,
    // If this entry represents a concatenation of multiple parts, `parts` holds
    // the ordered list of `Document`s and `doc` is the first part's document.
    pub parts: Option<Vec<Document>>,
}

#[allow(dead_code)]
pub struct AppState {
    pub client: Client,
    pub index: HashMap<String, Vec<FileEntry>>,
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
    let mut body = format!(
        "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 3.2 Final//EN\">\n\
         <html>\n<head><title>{title}</title></head>\n<body>\n\
         <h1>{title}</h1>\n<pre>\n"
    );
    if let Some(p) = parent {
        body.push_str(&format!("<a href=\"{p}\">../</a>\n"));
    }
    for e in entries {
        let size_str = e.size.map_or("-".to_string(), |s| human_size(s));
        body.push_str(&format!("<a href=\"{}\">{}</a>  {}\n", e.href, e.label, size_str));
    }
    body.push_str("</pre>\n</body>\n</html>");
    body
}

// convenience loader used by main
pub fn load_config(path: &str) -> anyhow::Result<Config> {
    let data = std::fs::read_to_string(path)
        .map_err(|_| anyhow::anyhow!("{} not found", path))?;
    Ok(serde_yaml::from_str(&data)?)
}
