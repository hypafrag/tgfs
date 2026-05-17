use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use grammers_client::media::{Document, Media};
use std::path::PathBuf;
use std::time::SystemTime;
use grammers_client::Client;
use grammers_session::types::PeerRef;
use smallvec::SmallVec;

pub use crate::config::ArchiveView;

pub type DocParts = SmallVec<[Media; 1]>;
pub type MsgIds = SmallVec<[i32; 1]>;

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
    /// Source Telegram message id for each part, parallel to `parts`. Used to
    /// re-fetch fresh `Document` objects when Telegram returns
    /// `FILE_REFERENCE_EXPIRED` (file references are short-lived).
    pub msg_ids: MsgIds,
    pub size: Option<usize>,
    // Index into the global MIME pool in `AppState`.
    pub mime_idx: usize,
    pub archive_entries: Option<Vec<ArchiveFileEntry>>,
    // Final computed type for the entry: "file", "media", or "zip".
    // This is either parsed from a `type:` message override or derived from
    // the document MIME/type and the original document filename (not the
    // message `name:` override).
    pub file_type: FileType,
    // Modification time derived from the originating Telegram message.
    pub mtime: Option<SystemTime>,
}

#[derive(Clone, PartialEq, Eq)]
pub enum FileType {
    File,
    Media,
    Zip,
}

impl FileEntry {
    pub fn first_doc(&self) -> &Media {
        &self.parts[0]
    }

    pub fn doc_name(&self) -> &str {
        match &self.first_doc() {
            Media::Document(d) => d.name().unwrap_or(&self.name),
            Media::Photo(_) => &self.name,
            _ => &self.name,
        }
    }
}

/// Per-message snapshot used to rebuild the file index incrementally on updates.
/// One `RawEntry` per accepted Telegram message; the assembler folds multipart
/// parts together and runs prefix collapse to produce the final `FileEntry` list.
#[derive(Clone)]
pub struct RawEntry {
    pub msg_id: i32,
    pub media: Media,
    /// Name as it should be exposed, possibly including a `dir/sub/` path prefix.
    /// Honors the message `name:` override; otherwise the document filename, or
    /// `photo_<id>.jpg` for photos.
    pub raw_name: String,
    /// Original document filename without override. Used for multipart suffix
    /// detection (`<base>.NN`). `None` for photos.
    pub doc_name: Option<String>,
    pub size: Option<usize>,
    pub mime_idx: usize,
    pub mtime: Option<SystemTime>,
    pub type_override: Option<FileType>,
    pub mime_type: String,
    /// Pre-fetched ZIP central directory for zip-classified single-message
    /// entries. Multipart zips index archive_entries on the combined parts at
    /// assembly time (where the full Document list is available).
    pub archive_entries: Option<Vec<ArchiveFileEntry>>,
}

/// Interned MIME-type pool. Behind a Mutex so background update tasks can
/// intern new types concurrently with serving requests.
pub struct MimePool {
    inner: Mutex<MimePoolInner>,
}

struct MimePoolInner {
    vec: Vec<String>,
    map: HashMap<String, usize>,
}

impl MimePool {
    pub fn new() -> Self {
        Self { inner: Mutex::new(MimePoolInner { vec: Vec::new(), map: HashMap::new() }) }
    }

    /// Insert `s` if absent and return its stable index. Cloned MIME strings
    /// stay short, so the underlying allocation cost is negligible.
    pub fn intern(&self, s: &str) -> usize {
        let mut g = self.inner.lock().unwrap();
        if let Some(&idx) = g.map.get(s) { return idx; }
        let idx = g.vec.len();
        g.vec.push(s.to_string());
        g.map.insert(s.to_string(), idx);
        idx
    }

    pub fn get(&self, idx: usize) -> Option<String> {
        self.inner.lock().unwrap().vec.get(idx).cloned()
    }
}

impl Default for MimePool {
    fn default() -> Self { Self::new() }
}

pub struct AppState {
    pub client: Client,
    pub mime_pool: MimePool,
    /// Per-channel state behind a RwLock so the realtime dispatcher can swap
    /// in updated file lists without blocking concurrent reads of unrelated
    /// channels. The outer `HashMap` itself is built once at startup and never
    /// grows, so it doesn't need its own lock.
    pub channels: HashMap<String, RwLock<TelegramChannel>>,
    // Maps directory name (used in URLs / FUSE paths) → channel name (index key).
    // When no `directory:` override is set, dir name == channel name.
    pub dir_to_channel: HashMap<String, String>,
    /// Per-PID concurrent fetch limit for FUSE (None = unlimited).
    pub max_fetches_per_pid: Option<usize>,
    /// Process-wide concurrent fetch limit for FUSE (None = unlimited).
    pub max_fetches_total: Option<usize>,
    /// Process-wide cache of refreshed `Document`s, keyed by document id.
    /// Populated on `FILE_REFERENCE_EXPIRED` recovery so subsequent reads of
    /// the same file reuse the fresh reference instead of re-fetching the
    /// source message every time.
    pub fresh_docs: Mutex<HashMap<i64, Document>>,
}

pub struct TelegramChannel {
    pub archive_view: ArchiveView,
    pub skip_deflated_id3v1: bool,
    pub collapse_by_prefix: Option<usize>,
    /// Cheap-to-clone snapshot of the assembled file list. Swapped atomically
    /// when raw_entries change; readers clone the Arc and drop the lock.
    pub files: Arc<Vec<FileEntry>>,
    /// One RawEntry per accepted Telegram message that contributed to the
    /// index. Mutated by the realtime dispatcher; assembled into `files` on
    /// each change.
    pub raw_entries: HashMap<i32, RawEntry>,
    /// Resolved peer reference for the channel (or self peer for Saved Messages).
    /// Used to re-fetch messages when refreshing expired file references and to
    /// route incoming updates to the correct channel.
    pub peer: Option<PeerRef>,
    /// Channel kind. Realtime updates apply to `RegularChannel` only.
    pub source: ChannelSource,
}

#[derive(Clone, PartialEq, Eq)]
pub enum ChannelSource {
    /// Backed by a Telegram channel; realtime updates are processed.
    RegularChannel,
    /// Backed by the Saved Messages chat; built once at startup, not updated
    /// in realtime (tag reactions and album-grouping would need their own
    /// streaming pipeline).
    SavedMessages,
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
    pub modified: Option<std::time::SystemTime>,
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

fn fmt_system_time(t: std::time::SystemTime) -> String {
    let secs = match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(_) => return String::new(),
    };
    let time_of_day = secs % 86400;
    let hour = time_of_day / 3600;
    let min = (time_of_day % 3600) / 60;
    // civil_from_days (Howard Hinnant)
    let z = secs / 86400 + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02} {:02}:{:02}", y, m, d, hour, min)
}

pub fn dir_listing(title: &str, _parent: Option<&str>, entries: &[Entry]) -> String {
    use std::fmt::Write;
    let mut body = String::new();
    let te = html_escape(title);
    write!(body, concat!(
        "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n",
        "<title>{te}</title>\n",
        "<style>\n",
        "  body {{ font-family: monaco; }}\n",
        "  table {{ width: 100%; border-collapse: collapse; }}\n",
        "  th, td {{ padding: 4px 8px; }}\n",
        "  th {{ text-align: left; border-bottom: 1px solid #aaa; }}\n",
        "  td.size, td.modified {{ text-align: right; white-space: nowrap; }}\n",
        "  a {{ text-decoration: none; color: #0366d6; }}\n",
        "</style>\n</head>\n<body>\n",
        "<h1>{te}</h1>\n",
        "<table>\n<thead>\n<tr>\n",
        "  <th>Name</th>\n",
        "  <th class=\"modified\">Last modified</th>\n",
        "  <th class=\"size\">Size</th>\n",
        "</tr>\n</thead>\n<tbody>\n"
    ), te = te).unwrap();
    for e in entries {
        let size_str = e.size.map_or("-".to_string(), |s| human_size(s));
        let mod_str = e.modified.map(fmt_system_time).unwrap_or_default();
        write!(body,
            "<tr>\n  <td>\n    <a href=\"{}\">{}</a>\n  </td>\n  <td class=\"modified\">{}</td>\n  <td class=\"size\">{}</td>\n</tr>\n",
            e.href, html_escape(&e.label), mod_str, size_str
        ).unwrap();
    }
    write!(body, "</tbody>\n</table>\n</body>\n</html>").unwrap();
    body
}
