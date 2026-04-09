use std::collections::HashMap;
use std::io::{self, Write};
use std::time::{SystemTime, Duration};
use grammers_client::media::{Document, Media};
use grammers_client::peer::Peer;
use grammers_client::Client;
use grammers_client::tl;
use crate::index::{Config, FileEntry, FileType, ArchiveFileEntry, ArchiveView, DocParts, TelegramChannel};
use crate::zip_cache::{ZipCache, ZipCacheKey};
use smallvec::smallvec;

fn u16le(b: &[u8], o: usize) -> u16 { u16::from_le_bytes(b[o..o+2].try_into().unwrap()) }
fn u32le(b: &[u8], o: usize) -> u32 { u32::from_le_bytes(b[o..o+4].try_into().unwrap()) }
fn u64le(b: &[u8], o: usize) -> u64 { u64::from_le_bytes(b[o..o+8].try_into().unwrap()) }

/// Detect a multipart-file suffix: `<base>.<digits>`. Returns `(base, part_num)`.
/// Requires at least one digit after the final `.`.
fn split_part_suffix(name: &str) -> Option<(&str, usize)> {
    let dot = name.rfind('.')?;
    let (base, rest) = (&name[..dot], &name[dot + 1..]);
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) { return None; }
    Some((base, rest.parse().ok()?))
}

fn common_prefix_len(a: &str, b: &str) -> usize {
    a.chars().zip(b.chars()).take_while(|(ca, cb)| ca == cb).map(|(c, _)| c.len_utf8()).sum()
}

/// Trim a prefix candidate: remove trailing whitespace and underscores, then
/// cut at the earliest unmatched `[` or `(` (e.g. `"Into the Breach [010057"`
/// → `"Into the Breach"`). Returns a &str into the original slice.
fn trim_prefix_name(s: &str) -> &str {
    // Find the first position of an unmatched opening bracket/paren.
    let mut bracket_opens: Vec<usize> = Vec::new();
    let mut paren_opens: Vec<usize> = Vec::new();
    for (i, c) in s.char_indices() {
        match c {
            '[' => bracket_opens.push(i),
            ']' => { bracket_opens.pop(); }
            '(' => paren_opens.push(i),
            ')' => { paren_opens.pop(); }
            _ => {}
        }
    }
    let mut cut = s.len();
    if let Some(&pos) = bracket_opens.first() { cut = cut.min(pos); }
    if let Some(&pos) = paren_opens.first() { cut = cut.min(pos); }
    s[..cut].trim_end_matches(|c: char| c.is_whitespace() || c == '_')
}

/// Collapse files at the same virtual-directory level into sub-directories
/// when two or more files share a common name prefix of at least `min_len`
/// characters (after trimming trailing whitespace from the prefix).
/// Modifies `path` on affected entries in-place; the rest of the server /
/// FUSE path logic requires no changes.
fn apply_prefix_collapse(files: &mut Vec<FileEntry>, min_len: usize) {
    if min_len == 0 || files.len() < 2 { return; }

    // Bucket file indices by their current virtual path so we only collapse
    // files that already live at the same directory level.
    let mut buckets: std::collections::BTreeMap<String, Vec<usize>> = Default::default();
    for (i, f) in files.iter().enumerate() {
        let key = f.path.as_ref().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default();
        buckets.entry(key).or_default().push(i);
    }

    let mut updates: Vec<(usize, std::path::PathBuf)> = Vec::new();

    for (_bucket_key, mut indices) in buckets {
        if indices.len() < 2 { continue; }
        // Sort indices by lowercase name so adjacent entries share maximum prefix.
        indices.sort_by_key(|&i| files[i].name.to_lowercase());
        let names: Vec<&str> = indices.iter().map(|&i| files[i].name.as_str()).collect();

        // Forward greedy scan: extend a group as long as the accumulated
        // common prefix of the whole group stays >= min_len bytes.
        let mut start = 0;
        while start + 1 < names.len() {
            let mut prefix_bytes = common_prefix_len(names[start], names[start + 1]);
            if prefix_bytes < min_len { start += 1; continue; }

            let mut end = start + 2;
            while end < names.len() {
                let new_len = common_prefix_len(&names[start][..prefix_bytes], names[end]);
                if new_len >= min_len { prefix_bytes = new_len; end += 1; } else { break; }
            }

            // The directory name is the shared prefix, right-trimmed of whitespace.
            let dir_name = trim_prefix_name(&names[start][..prefix_bytes]).to_string();
            if dir_name.len() >= min_len {
                for &idx in &indices[start..end] {
                    let new_path = match &files[idx].path {
                        Some(p) => p.join(&dir_name),
                        None => std::path::PathBuf::from(&dir_name),
                    };
                    updates.push((idx, new_path));
                }
            }
            start = end;
        }
    }

    for (idx, new_path) in updates {
        files[idx].path = Some(new_path);
    }
}

fn split_name(raw: &str) -> (String, Option<std::path::PathBuf>) {
    if raw.contains('/') {
        let p = std::path::Path::new(raw);
        let fname = p.file_name().and_then(|s| s.to_str()).unwrap_or(raw).to_string();
        let dir = p.parent().map(|pp| pp.to_path_buf());
        (fname, dir)
    } else {
        (raw.to_string(), None)
    }
}

fn msg_mtime(msg: &grammers_client::message::Message) -> Option<SystemTime> {
    if let tl::enums::Message::Message(m) = &msg.raw {
        return Some(SystemTime::UNIX_EPOCH + Duration::from_secs(m.date as u64));
    }
    None
}

pub fn photo_largest_size(p: &grammers_client::media::Photo) -> usize {
    // Pick the largest *separately downloadable* variant. Inline thumbnails
    // (`PhotoCachedSize`/`PhotoPathSize`/`PhotoStrippedSize`/`Empty`) are
    // embedded in the TL Photo and never returned by `iter_download`, so
    // including their byte length here would cause a size/stream mismatch
    // for photos that happen to have only inline variants.
    if let Some(tl::enums::Photo::Photo(inner)) = &p.raw.photo {
        let mut best: usize = 0;
        for sz in inner.sizes.iter() {
            match sz {
                tl::enums::PhotoSize::Size(s) => best = best.max(s.size as usize),
                // `Progressive.sizes` is cumulative bytes per encoded layer;
                // the last entry is the full image.
                tl::enums::PhotoSize::Progressive(p) => {
                    if let Some(&last) = p.sizes.last() {
                        best = best.max(last as usize);
                    }
                }
                _ => {}
            }
        }
        return best;
    }
    0
}

fn classify_file_type(type_override: &Option<FileType>, mime_type: &str, doc_name: &str) -> FileType {
    if let Some(t) = type_override { return t.clone(); }
    if mime_type.starts_with("audio/") || mime_type.starts_with("video/") || mime_type.starts_with("image/") {
        FileType::Media
    } else if mime_type == "application/zip" || doc_name.to_lowercase().ends_with(".zip") {
        FileType::Zip
    } else {
        FileType::File
    }
}

pub struct IndexBuildResult {
    pub mime_vec: Vec<String>,
    pub channels: HashMap<String, TelegramChannel>,
    pub dir_to_channel: HashMap<String, String>,
}

/// Fetch and parse the ZIP central directory for `docs` (one or more concatenated parts).
/// Returns `None` if the archive can't be indexed (no EOCD, download failure, etc.).
async fn try_index_zip(client: &Client, docs: &[Document], label: &str, cache: &mut ZipCache) -> Option<Vec<ArchiveFileEntry>> {
    let total: usize = docs.iter().map(|d| d.size().unwrap_or(0) as usize).sum();
    // check cache first
    // Use the document's original name but strip a trailing multipart
    // suffix like `.00` so multipart parts map to the same cache key.
    let raw_name = docs.get(0).and_then(|d| d.name()).map(|s| s.to_string()).unwrap_or_else(|| label.to_string());
    let name_key = if let Some((base, _)) = split_part_suffix(&raw_name) { base.to_string() } else { raw_name.clone() };
    let key = ZipCacheKey { name: name_key.clone(), size: total };
    if let Some(cached) = cache.get(&key) {
        println!("  zip index cache hit for {} ({} bytes)", key.name, key.size);
        return Some(cached);
    }
    // Wrap once so we can reuse the unified `download_range` instead of
    // maintaining a near-duplicate `Document`-only variant.
    let media_parts: Vec<Media> = docs.iter().cloned().map(Media::Document).collect();
    println!("  fetching EOCD tail of '{}' for indexing", label);
    io::stdout().flush().ok();
    let tail_len = std::cmp::min(total, 70_000);
    let tail_offset = total.saturating_sub(tail_len);
    let tail = download_range(client, &media_parts, tail_offset, tail_len).await.ok()?;
    let (cd_off, cd_size) = match find_eocd(&tail, tail_offset as u64) {
        Some(v) => v,
        None => {
            println!("  EOCD not found for '{}', skipping archive index", label);
            return None;
        }
    };
    println!("    central directory at {} ({} bytes)", cd_off, cd_size);
    let cd_bytes = download_range(client, &media_parts, cd_off as usize, cd_size as usize).await.ok()?;
    let (mut ae_list, lh_offsets) = parse_central_directory(&cd_bytes).ok()?;
    // Fetch each local file header to resolve the true data offset.
    // The local extra field length can differ from the central directory,
    // so we must read it — but only once per entry, here at index time.
    for (ae, lh_offset) in ae_list.iter_mut().zip(lh_offsets.iter()) {
        let lh = download_range(client, &media_parts, *lh_offset as usize, 30).await.ok()?;
        if lh.len() < 30 || &lh[0..4] != [0x50, 0x4b, 0x03, 0x04] { return None; }
        let name_len = u16::from_le_bytes([lh[26], lh[27]]) as usize;
        let extra_len = u16::from_le_bytes([lh[28], lh[29]]) as usize;
        ae.data_offset = lh_offset + 30 + name_len as u64 + extra_len as u64;
    }
    println!("  zip entries read: {}", ae_list.len());
    cache.insert(ZipCacheKey { name: name_key, size: total }, ae_list.clone());
    Some(ae_list)
}

/// Telegram chunk size for `iter_download`. Must be a power of 2; 64 KB is the
/// standard MTProto chunk granularity.
const TG_CHUNK_SIZE: usize = 64 * 1024;
/// How many parallel `iter_download` streams to spawn per `download_part_range`
/// call. Each stream walks a disjoint slice of contiguous 64 KB chunks. The
/// `SenderPool` in `make_client` multiplexes these onto its connection pool.
const PARALLEL_DOWNLOAD_STREAMS: usize = 4;

// Download a byte range [offset, offset+length) from Telegram, spanning one or
// more concatenated document parts. A single-document file is just a 1-element
// slice. Used by both the indexer (central-directory parsing, local-header reads)
// and the server (archive inner-file header reads).
pub async fn download_range(
    client: &Client,
    parts: &[Media],
    offset: usize,
    length: usize,
) -> anyhow::Result<Vec<u8>> {
    let sizes: Vec<usize> = parts
        .iter()
        .map(|m| match m {
            Media::Document(d) => d.size().unwrap_or(0) as usize,
            Media::Photo(p) => photo_largest_size(p),
            _ => 0,
        })
        .collect();
    let total: usize = sizes.iter().sum();
    if offset >= total { return Ok(Vec::new()); }
    let to_read = std::cmp::min(length, total - offset);

    // locate starting part and in-part offset
    let mut pos = 0usize;
    let mut i = 0usize;
    while i < sizes.len() && pos + sizes[i] <= offset { pos += sizes[i]; i += 1; }
    let mut start_in_part = offset - pos;

    let mut buf: Vec<u8> = Vec::with_capacity(to_read);
    let mut need = to_read;
    while i < parts.len() && need > 0 {
        let avail = sizes[i].saturating_sub(start_in_part);
        if avail == 0 { i += 1; start_in_part = 0; continue; }
        let read_len = std::cmp::min(avail, need);
        let doc = match &parts[i] {
            Media::Document(d) => d,
            _ => { return Err(anyhow::anyhow!("download_part_range requires a Document, got {:?}", parts[i])); }
        };
        let part_buf = download_part_range(client, doc, start_in_part, read_len).await?;
        if part_buf.len() < read_len {
            return Err(anyhow::anyhow!("failed to download requested range"));
        }
        buf.extend_from_slice(&part_buf);
        need = need.saturating_sub(read_len);
        i += 1;
        start_in_part = 0;
    }

    Ok(buf)
}

/// Download `[offset, offset+length)` from a single document part by splitting
/// the covering chunk range across `PARALLEL_DOWNLOAD_STREAMS` concurrent
/// `iter_download` streams. Network latency hides behind itself, giving
/// near-linear speedup up to the underlying connection-pool concurrency.
async fn download_part_range(
    client: &Client,
    doc: &Document,
    offset: usize,
    length: usize,
) -> anyhow::Result<Vec<u8>> {
    if length == 0 { return Ok(Vec::new()); }
    let first_chunk = offset / TG_CHUNK_SIZE;
    let last_chunk = (offset + length - 1) / TG_CHUNK_SIZE;
    let total_chunks = last_chunk - first_chunk + 1;
    let in_first_chunk = offset % TG_CHUNK_SIZE;

    let n_tasks = std::cmp::min(PARALLEL_DOWNLOAD_STREAMS, total_chunks);
    let chunks_per_task = total_chunks.div_ceil(n_tasks);

    let mut handles = Vec::with_capacity(n_tasks);
    for t in 0..n_tasks {
        let start_chunk = first_chunk + t * chunks_per_task;
        if start_chunk > last_chunk { break; }
        let end_chunk = std::cmp::min(first_chunk + (t + 1) * chunks_per_task - 1, last_chunk);
        let take_n = end_chunk - start_chunk + 1;
        let doc = doc.clone();
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            let mut out: Vec<u8> = Vec::with_capacity(take_n * TG_CHUNK_SIZE);
            for chunk_idx in 0..take_n {
                let mut retries = 0u32;
                loop {
                    let mut dl = client
                        .iter_download(&doc)
                        .chunk_size(TG_CHUNK_SIZE as i32)
                        .skip_chunks((start_chunk + chunk_idx) as i32);
                    match dl.next().await {
                        Ok(Some(chunk)) => { out.extend_from_slice(&chunk); break; }
                        Ok(None) => break,
                        Err(grammers_client::InvocationError::Rpc(ref rpc)) if rpc.name == "FLOOD_WAIT" => {
                            let wait = rpc.value.unwrap_or(1);
                            retries += 1;
                            if retries > 5 {
                                return Err::<Vec<u8>, anyhow::Error>(anyhow::anyhow!("FLOOD_WAIT retry limit exceeded"));
                            }
                            eprintln!("indexer: FLOOD_WAIT {} at chunk {}, retry {}/5", wait, start_chunk + chunk_idx, retries);
                            tokio::time::sleep(std::time::Duration::from_secs(wait as u64)).await;
                        }
                        Err(e) => return Err::<Vec<u8>, anyhow::Error>(e.into()),
                    }
                }
            }
            Ok(out)
        }));
    }

    // Concatenate in order so the returned bytes are contiguous.
    let mut combined: Vec<u8> = Vec::with_capacity(total_chunks * TG_CHUNK_SIZE);
    for h in handles {
        let part = h.await??;
        combined.extend_from_slice(&part);
    }

    if combined.len() <= in_first_chunk {
        return Err(anyhow::anyhow!("failed to download requested range"));
    }
    let end = std::cmp::min(combined.len(), in_first_chunk + length);
    Ok(combined[in_first_chunk..end].to_vec())
}

fn find_eocd(tail: &[u8], tail_offset: u64) -> Option<(u64, u64)> {
    if tail.len() < 22 { return None; }
    for i in (0..=tail.len() - 22).rev() {
        if &tail[i..i+4] == [0x50,0x4b,0x05,0x06] {
            let cd_size = u32le(tail, i+12) as u64;
            let cd_offset = u32le(tail, i+16) as u64;
            // If either value is 0xFFFFFFFF, look for ZIP64 EOCD
            if cd_size == 0xFFFF_FFFF || cd_offset == 0xFFFF_FFFF {
                if let Some((z64_off, z64_sz)) = find_zip64_eocd(tail, i, tail_offset) {
                    return Some((z64_off, z64_sz));
                }
                // ZIP64 lookup failed; the 0xFFFFFFFF values are unusable
                return None;
            }
            return Some((cd_offset, cd_size));
        }
    }
    None
}

/// Search for the ZIP64 End of Central Directory Locator (20 bytes, immediately
/// before the standard EOCD) and then read the ZIP64 EOCD record from the tail.
fn find_zip64_eocd(tail: &[u8], eocd_pos: usize, tail_offset: u64) -> Option<(u64, u64)> {
    // The ZIP64 EOCD locator is 20 bytes and sits right before the standard EOCD.
    if eocd_pos < 20 { return None; }
    let loc = eocd_pos - 20;
    if &tail[loc..loc+4] != [0x50, 0x4b, 0x06, 0x07] { return None; }
    // Locator field at offset 8: absolute offset of the ZIP64 EOCD record.
    let zip64_eocd_abs = u64le(tail, loc + 8);
    // Check if the ZIP64 EOCD record is within our downloaded tail.
    if zip64_eocd_abs < tail_offset { return None; }
    let z64_pos = (zip64_eocd_abs - tail_offset) as usize;
    // ZIP64 EOCD record is at least 56 bytes.
    if z64_pos + 56 > tail.len() { return None; }
    if &tail[z64_pos..z64_pos+4] != [0x50, 0x4b, 0x06, 0x06] { return None; }
    let cd_size = u64le(tail, z64_pos + 40);
    let cd_offset = u64le(tail, z64_pos + 48);
    Some((cd_offset, cd_size))
}

/// Returns parsed entries alongside their local header offsets.
/// `try_index_zip` uses the offsets to fetch each local header once and
/// compute the true data offset (local extra_len can differ from the CD).
fn parse_central_directory(cd: &[u8]) -> anyhow::Result<(Vec<ArchiveFileEntry>, Vec<u64>)> {
    let mut i: usize = 0;
    let mut entries = Vec::new();
    let mut lh_offsets: Vec<u64> = Vec::new();
    while i + 46 <= cd.len() {
        if &cd[i..i+4] != [0x50,0x4b,0x01,0x02] { break; }
        let version_made_by = u16le(cd, i+4);
        let mut compressed_size = u32le(cd, i+20) as u64;
        let mut uncompressed_size = u32le(cd, i+24) as u64;
        let compression_method = u16le(cd, i+10);
        let name_len = u16le(cd, i+28) as usize;
        let extra_len = u16le(cd, i+30) as usize;
        let comment_len = u16le(cd, i+32) as usize;
        let external_attrs = u32le(cd, i+38);
        let mut local_header_offset = u32le(cd, i+42) as u64;
        let var_start = i + 46;
        if var_start + name_len > cd.len() { break; }
        let name = String::from_utf8_lossy(&cd[var_start..var_start+name_len]).to_string();

        // Parse ZIP64 extended information extra field if any value is 0xFFFFFFFF
        if uncompressed_size == 0xFFFF_FFFF || compressed_size == 0xFFFF_FFFF || local_header_offset == 0xFFFF_FFFF {
            let extra_start = var_start + name_len;
            let extra_end = std::cmp::min(extra_start + extra_len, cd.len());
            parse_zip64_extra(&cd[extra_start..extra_end], &mut uncompressed_size, &mut compressed_size, &mut local_header_offset);
        }

        i = var_start + name_len + extra_len + comment_len;
        if name.ends_with('/') { continue; }

        let unix_mode = if (version_made_by >> 8) == 3 {
            let mode = (external_attrs >> 16) as u16;
            if mode != 0 { Some(mode & 0o7777) } else { None }
        } else {
            None
        };

        lh_offsets.push(local_header_offset);
        entries.push(ArchiveFileEntry {
            path: name,
            compressed_size: compressed_size as usize,
            uncompressed_size: uncompressed_size as usize,
            data_offset: 0, // filled in by try_index_zip after fetching local headers
            compression_method,
            unix_mode,
        });
    }
    Ok((entries, lh_offsets))
}

/// Parse ZIP64 extended information extra field (tag 0x0001). Values appear in
/// order: uncompressed_size, compressed_size, local_header_offset, disk_number —
/// but only for fields whose standard-header value is 0xFFFFFFFF.
fn parse_zip64_extra(extra: &[u8], uncompressed: &mut u64, compressed: &mut u64, header_offset: &mut u64) {
    let mut pos = 0;
    while pos + 4 <= extra.len() {
        let tag = u16le(extra, pos);
        let sz = u16le(extra, pos + 2) as usize;
        let data_start = pos + 4;
        if data_start + sz > extra.len() { break; }
        if tag == 0x0001 {
            let mut o = data_start;
            if *uncompressed == 0xFFFF_FFFF && o + 8 <= data_start + sz {
                *uncompressed = u64le(extra, o); o += 8;
            }
            if *compressed == 0xFFFF_FFFF && o + 8 <= data_start + sz {
                *compressed = u64le(extra, o); o += 8;
            }
            if *header_offset == 0xFFFF_FFFF && o + 8 <= data_start + sz {
                *header_offset = u64le(extra, o);
            }
            return;
        }
        pos = data_start + sz;
    }
}

/// Look up a `key:` field in a single (non-grouped) message's text.
fn message_field(msg: &grammers_client::message::Message, key: &str) -> Option<String> {
    if msg.grouped_id().is_some() { return None; }
    for line in msg.text().lines() {
        if let Some(value) = line.strip_prefix(key) {
            return Some(value.trim().to_string());
        }
    }
    None
}

fn resolve_type_override(msg: &grammers_client::message::Message) -> Option<FileType> {
    let v = message_field(msg, "type:")?.to_lowercase();
    match v.as_str() {
        "file" => Some(FileType::File),
        "media" => Some(FileType::Media),
        "zip" => Some(FileType::Zip),
        _ => None,
    }
}

pub async fn build_index(client: Client, config: &Config) -> anyhow::Result<IndexBuildResult> {
    let mut channel_peers = HashMap::new();
    let mut dialogs = client.iter_dialogs();
    while let Some(dialog) = dialogs.next().await? {
        if let Peer::Channel(ch) = dialog.peer() {
            if config.channels.iter().any(|c| c.name == ch.title()) {
                if let Some(r) = ch.to_ref().await {
                    channel_peers.insert(ch.title().to_string(), r);
                }
            }
        }
    }

    // MIME interning structures
    let mut mime_map: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut mime_vec: Vec<String> = Vec::new();
    // build channel -> TelegramChannel map (files populated later)
    let mut index: HashMap<String, TelegramChannel> = HashMap::new();
    let mut dir_to_channel: HashMap<String, String> = config.channels.iter().map(|c| {
        let dir = c.directory.clone().unwrap_or_else(|| c.name.clone());
        (dir, c.name.clone())
    }).collect();

    // load persistent zip index cache (keyed by document original name + total size)
    let mut zip_cache = ZipCache::load("zip_index_cache.json.gz");

    for entry in &config.channels {
        let name = &entry.name;
        let peer_ref = match channel_peers.get(name) {
            Some(r) => r.clone(),
            None => { eprintln!("Channel '{name}' not found, skipping."); continue; }
        };

        println!("Indexing {name}...");
        let mut files = Vec::new();
        let mut messages = client.iter_messages(peer_ref);
        let mut processed_msgs: usize = 0;
        while let Some(msg) = messages.next().await? {
            processed_msgs += 1;
            match msg.media() {
                Some(Media::Document(doc)) => {
                    let raw_name = message_field(&msg, "name:").unwrap_or_else(|| doc.name().unwrap_or("<unnamed>").to_string());
                    // parse out optional path prefix from message `name:` override
                    let (file_name, path_opt) = split_name(&raw_name);
                    let type_override = resolve_type_override(&msg);
                    // Skip entries with empty or placeholder names
                    if file_name.trim().is_empty() || file_name == "<unnamed>" {
                        continue;
                    }
                    println!("Processing file: {}", file_name);
                    io::stdout().flush().ok();
                    let size = doc.size();
                    let mime_type = doc.mime_type().unwrap_or("application/octet-stream").to_string();
                    // Determine the final `file_type` for this entry. Preference order:
                    // 1) explicit `type:` override in the message,
                    // 2) MIME/type (audio/video => media, application/zip => zip),
                    // 3) document filename extension (using the underlying
                    //    `Document` name, not a `name:` override).
                    let final_type = classify_file_type(&type_override, &mime_type, doc.name().unwrap_or(&file_name));
                    // Multipart parts (`*.NN`) are classified as `File` (the
                    // part name doesn't end in `.zip`), so this branch is
                    // naturally skipped for them — no extra guard needed.
                    let mut archive_entries: Option<Vec<ArchiveFileEntry>> = None;
                    if final_type == FileType::Zip && entry.archive_view != ArchiveView::File {
                        archive_entries = try_index_zip(&client, &[doc.clone()], &file_name, &mut zip_cache).await;
                    }
                    // intern mime_type
                    let mime_idx = *mime_map.entry(mime_type.clone()).or_insert_with(|| { mime_vec.push(mime_type.clone()); mime_vec.len() - 1 });
                    let mtime = msg_mtime(&msg);
                    files.push(FileEntry { name: file_name, path: path_opt, parts: smallvec![Media::Document(doc.clone())], size, mime_idx, archive_entries, file_type: final_type.clone(), mtime });
                }
                Some(Media::Photo(photo)) => {
                    // Photos: expose as files. Use `name:` override if present, otherwise synthesize a filename.
                    let raw_name = message_field(&msg, "name:").unwrap_or_else(|| format!("photo_{}.jpg", photo.id()));
                    let (file_name, path_opt) = split_name(&raw_name);
                    let type_override = resolve_type_override(&msg);
                    // Skip entries with empty or placeholder names
                    if file_name.trim().is_empty() || file_name == "<unnamed>" {
                        continue;
                    }
                    println!("Processing photo: {}", file_name);
                    io::stdout().flush().ok();
                    let size = Some(photo_largest_size(&photo));
                    let mime_type = "image/jpeg".to_string();
                    let final_type = classify_file_type(&type_override, &mime_type, &file_name);
                    let mime_idx = *mime_map.entry(mime_type.clone()).or_insert_with(|| { mime_vec.push(mime_type.clone()); mime_vec.len() - 1 });
                    let mtime = msg_mtime(&msg);
                    files.push(FileEntry { name: file_name, path: path_opt, parts: smallvec![Media::Photo(photo.clone())], size, mime_idx, archive_entries: None, file_type: final_type, mtime });
                }
                _ => {}
            }
        }
        println!("Finished indexing messages for '{}', processed {} messages", name, processed_msgs);
        println!(" {} files (pre-group)", files.len());

        // Detect multipart files by inspecting the document filename (not message overrides).
        // Pattern: <base>.<N digits> e.g. foo.00, foo.01, ... Numbering must start at 0
        // and be contiguous.
        let mut groups: std::collections::HashMap<String, Vec<(usize, usize)>> = std::collections::HashMap::new();
        for (i, f) in files.iter().enumerate() {
            if let Some((base, part)) = split_part_suffix(f.doc_name()) {
                groups.entry(base.to_string()).or_default().push((i, part));
            }
        }

        // Build new files vector merging detected groups of >= 2 parts
        let mut removed = std::collections::BTreeSet::new();
        let mut new_files: Vec<FileEntry> = Vec::new();
        for (base, mut entries) in groups.into_iter() {
            if entries.len() < 2 { continue; }
            entries.sort_by_key(|&(_, p)| p);
            if !entries.iter().enumerate().all(|(i, &(_, p))| p == i) { continue; }
            // collect docs and sizes
            let mut docs: DocParts = DocParts::new();
            let mut total_size: Option<usize> = Some(0);
            let parts_indices: Vec<usize> = entries.iter().map(|(idx, _)| *idx).collect();
            for idx in &parts_indices {
                let f = &files[*idx];
                docs.extend(f.parts.iter().cloned());
                match (total_size, f.size) {
                    (Some(acc), Some(s)) => total_size = Some(acc + s),
                    _ => total_size = None,
                }
            }

            // Determine exposed name: if the .00 part (part 0) has a message override name, use it.
            // Otherwise use the doc-derived base name.
            let mut exposed_name = base.clone();
            // find index with part == 0
                if let Some((first_idx, _first_part)) = entries.iter().find(|&&(_, p)| p == 0) {
                let f0 = &files[*first_idx];
                let doc_name = f0.doc_name().to_string();
                if f0.name != doc_name { // message override present
                    exposed_name = f0.name.clone();
                }
            }

            // Build a combined FileEntry using the first part as representative
            let first = &files[parts_indices[0]];
            // If the base name looks like a .zip, attempt to parse its central directory
            // across the concatenated parts so we can expose inner entries.
            let mut archive_entries_combined: Option<Vec<ArchiveFileEntry>> = None;
            if base.to_lowercase().ends_with(".zip") && !docs.is_empty() {
                // try_index_zip expects Documents; only attempt if all parts are Documents
                let doc_vec: Vec<Document> = docs.iter().cloned().filter_map(|m| if let Media::Document(d) = m { Some(d) } else { None }).collect();
                if doc_vec.len() == docs.len() {
                    println!("Processing multipart archive: {}", exposed_name);
                    archive_entries_combined = try_index_zip(&client, &doc_vec, &exposed_name, &mut zip_cache).await;
                }
            }

            // Determine combined file_type: prefer the first part's resolved file_type.
            let combined_file_type = first.file_type.clone();

            // For combined entries the `exposed_name` may include a path.
            let (exposed_base, exposed_path) = split_name(&exposed_name);

            let combined = FileEntry {
                name: exposed_base,
                path: exposed_path,
                parts: docs,
                size: total_size,
                mime_idx: first.mime_idx,
                archive_entries: archive_entries_combined,
                file_type: combined_file_type,
                mtime: first.mtime,
            };

            // mark removed indices
            for idx in parts_indices { removed.insert(idx); }
            // Skip combined entries with empty or placeholder names
            if combined.name.trim().is_empty() || combined.name == "<unnamed>" {
                continue;
            }
            new_files.push(combined);
        }

        // Append non-removed original entries
        for (i, f) in files.into_iter().enumerate() {
            if removed.contains(&i) { continue; }
            if f.name.trim().is_empty() || f.name == "<unnamed>" { continue; }
            new_files.push(f);
        }

        // sort files by exposed name
        new_files.sort_by_key(|f| f.name.to_lowercase());
        println!(" {} files (post-group)", new_files.len());
        // collapse files sharing a common name prefix into virtual subdirectories
        if let Some(min_len) = entry.collapse_by_prefix {
            apply_prefix_collapse(&mut new_files, min_len);
        }
        // create TelegramChannel and insert
        let tchan = TelegramChannel { archive_view: entry.archive_view, skip_deflated_id3v1: entry.skip_deflated_id3v1, files: new_files };
        index.insert(name.clone(), tchan);
    }

    // Index Saved Messages if configured. Files tagged via Telegram saved-message
    // reaction tags are exposed as a directory per tag; untagged files live at the
    // root of the saved_messages directory.
    if let Some(saved_cfg) = &config.saved_messages {
        let saved_dir = saved_cfg.directory.clone().unwrap_or_else(|| "saved_messages".to_string());
        if dir_to_channel.contains_key(&saved_dir) || index.contains_key(&saved_dir) {
            return Err(anyhow::anyhow!(
                "saved_messages directory '{}' collides with a configured channel name",
                saved_dir
            ));
        }
        match index_saved_messages(&client, &mut mime_map, &mut mime_vec, &mut zip_cache, saved_cfg.archive_view).await {
            Ok(channel) => {
                index.insert(saved_dir.clone(), channel);
                dir_to_channel.insert(saved_dir.clone(), saved_dir.clone());
            }
            Err(e) => eprintln!("failed to index saved messages: {}", e),
        }
    }

    // Persist the zip index cache once after the full index is built.
    if let Err(e) = zip_cache.save() {
        eprintln!("failed to save zip cache: {}", e);
    }

    Ok(IndexBuildResult { mime_vec, channels: index, dir_to_channel })
}

/// Hashable key derived from a Reaction so we can map reactions to their
/// saved-tag titles. The Reaction enum itself is generated without `Hash`,
/// so we project the discriminating fields ourselves.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
enum ReactionKey {
    Emoji(String),
    Custom(i64),
    Other,
}

fn reaction_key(r: &tl::enums::Reaction) -> ReactionKey {
    match r {
        tl::enums::Reaction::Emoji(e) => ReactionKey::Emoji(e.emoticon.clone()),
        tl::enums::Reaction::CustomEmoji(c) => ReactionKey::Custom(c.document_id),
        _ => ReactionKey::Other,
    }
}

/// Fallback display label for a reaction that has no title set on its
/// `messages.SavedReactionTags` entry — use the emoji glyph itself, or
/// `custom_<id>` for custom-emoji tags.
fn reaction_label(r: &tl::enums::Reaction) -> String {
    match r {
        tl::enums::Reaction::Emoji(e) => e.emoticon.clone(),
        tl::enums::Reaction::CustomEmoji(c) => format!("custom_{}", c.document_id),
        _ => String::new(),
    }
}

/// Walk a message's reactions and resolve which ones map to a known
/// saved-reaction tag title. Avoids materialising an intermediate
/// `Vec<Reaction>` for messages that have no reactions at all (the common
/// case in Saved Messages). The `if let` over `ReactionCount` is currently
/// irrefutable but kept that way so a future TL variant doesn't break the
/// build — hence the `#[allow]`.
#[allow(irrefutable_let_patterns)]
fn extract_tag_titles(
    msg: &grammers_client::message::Message,
    tag_titles: &HashMap<ReactionKey, String>,
) -> Vec<String> {
    let mut out = Vec::new();
    if let tl::enums::Message::Message(m) = &msg.raw {
        if let Some(tl::enums::MessageReactions::Reactions(r)) = &m.reactions {
            for rc in &r.results {
                if let tl::enums::ReactionCount::Count(c) = rc {
                    if let Some(t) = tag_titles.get(&reaction_key(&c.reaction)) {
                        out.push(t.clone());
                    }
                }
            }
        }
    }
    out
}

/// Compact per-message record built during the saved-messages stream pass.
/// We keep just enough to emit `FileEntry` rows in the second pass without
/// holding raw `Message` objects around (which carry text, peer info, etc.).
struct SavedRecord {
    media: Media,
    name: String,
    size: Option<usize>,
    mime_idx: usize,
    file_type: FileType,
    archive_entries: Option<Vec<ArchiveFileEntry>>,
    mtime: Option<SystemTime>,
    grouped_id: Option<i64>,
    /// Tag titles resolved from this individual message's own reactions.
    /// For grouped (album) messages this is ignored at emit time; we use
    /// the per-`grouped_id` union instead so a tag applied to any single
    /// part fans the whole album out under that tag.
    own_tag_titles: Vec<String>,
}

/// Index the user's Saved Messages dialog. Each document/photo message becomes
/// one or more `FileEntry` rows: untagged files at the root, tagged files
/// fanned out under one virtual directory per matching saved reaction tag.
/// For album (grouped) messages, the tag set is the union over all album
/// parts so a single reaction on any part propagates to every part.
async fn index_saved_messages(
    client: &Client,
    mime_map: &mut HashMap<String, usize>,
    mime_vec: &mut Vec<String>,
    zip_cache: &mut ZipCache,
    archive_view: crate::index::ArchiveView,
) -> anyhow::Result<TelegramChannel> {
    println!("Indexing Saved Messages...");

    // Resolve self peer for `iter_messages`. `Saved Messages` is the cloud
    // chat with yourself; its peer is just your own User.
    let me = client.get_me().await?;
    let me_ref = me
        .to_ref()
        .await
        .ok_or_else(|| anyhow::anyhow!("could not resolve self peer"))?;

    // Pull the list of saved reaction tags (with titles) once. The result
    // gives us reaction → title mapping; reactions on messages that don't
    // appear here are not user-defined tags and are ignored.
    let tags_resp = client
        .invoke(&tl::functions::messages::GetSavedReactionTags { peer: None, hash: 0 })
        .await?;
    let mut tag_titles: HashMap<ReactionKey, String> = HashMap::new();
    if let tl::enums::messages::SavedReactionTags::Tags(tags) = tags_resp {
        for tag in tags.tags {
            let tl::enums::SavedReactionTag::Tag(t) = tag;
            let key = reaction_key(&t.reaction);
            let title = t.title.unwrap_or_else(|| reaction_label(&t.reaction));
            if !title.is_empty() {
                tag_titles.insert(key, title);
            }
        }
    }
    println!("  saved reaction tags: {}", tag_titles.len());

    // Stream messages once, building compact per-message records and
    // accumulating per-album tag unions on the fly. We never need to keep
    // the raw `Message` objects around after this loop.
    let mut records: Vec<SavedRecord> = Vec::new();
    let mut group_tag_titles: HashMap<i64, Vec<String>> = HashMap::new();
    let mut messages = client.iter_messages(me_ref);
    let mut processed: usize = 0;
    while let Some(msg) = messages.next().await? {
        processed += 1;

        // Materialise media-specific fields into a uniform tuple so the
        // rest of the per-message logic can run once instead of being
        // duplicated across the Document/Photo arms.
        let (media, name, size, mime_type) = match msg.media() {
            Some(Media::Document(doc)) => {
                let n = doc.name().unwrap_or("<unnamed>").to_string();
                let s = doc.size();
                let mt = doc.mime_type().unwrap_or("application/octet-stream").to_string();
                (Media::Document(doc), n, s, mt)
            }
            Some(Media::Photo(photo)) => {
                let n = format!("photo_{}.jpg", photo.id());
                let s = Some(photo_largest_size(&photo));
                (Media::Photo(photo), n, s, "image/jpeg".to_string())
            }
            _ => continue,
        };

        // Skip placeholder/empty names early so we don't waste a zip-index
        // round-trip on something we'd discard anyway.
        if name.trim().is_empty() || name == "<unnamed>" { continue; }

        // No `type:`/`name:` overrides in saved messages — classify purely
        // from MIME and filename. Multipart parts (`*.NN`) classify as
        // `File` because the part name doesn't end in `.zip`, so the zip
        // branch below is naturally skipped for them.
        let file_type = classify_file_type(&None, &mime_type, &name);
        let mime_idx = *mime_map.entry(mime_type.clone()).or_insert_with(|| {
            mime_vec.push(mime_type.clone());
            mime_vec.len() - 1
        });

        // Index ZIP central directory exactly once per source message. The
        // previous implementation called `try_index_zip` inside the per-tag
        // fan-out, re-doing the work (or at least re-hitting the cache) N
        // times for an N-tagged archive.
        let archive_entries = if file_type == FileType::Zip {
            if let Media::Document(doc) = &media {
                try_index_zip(client, &[doc.clone()], &name, zip_cache).await
            } else {
                None
            }
        } else {
            None
        };

        let mtime = msg_mtime(&msg);
        let grouped_id = msg.grouped_id();
        let own_tag_titles = extract_tag_titles(&msg, &tag_titles);

        // Accumulate per-album tag union as we stream so we don't need a
        // second pass over messages just to compute it.
        if let Some(gid) = grouped_id {
            let bucket = group_tag_titles.entry(gid).or_default();
            for t in &own_tag_titles {
                if !bucket.contains(t) { bucket.push(t.clone()); }
            }
        }

        records.push(SavedRecord {
            media,
            name,
            size,
            mime_idx,
            file_type,
            archive_entries,
            mtime,
            grouped_id,
            own_tag_titles,
        });
    }

    // Now turn the records into `FileEntry` rows. Tag selection rule:
    //   - grouped: use the per-album union (per-message reactions are
    //     already folded into that union, so taking both would just
    //     duplicate).
    //   - non-grouped: use the message's own reactions.
    let mut files: Vec<FileEntry> = Vec::new();
    for rec in records.into_iter() {
        let titles: Vec<String> = match rec.grouped_id {
            Some(gid) => group_tag_titles.get(&gid).cloned().unwrap_or_default(),
            None => rec.own_tag_titles.clone(),
        };

        let make_entry = |path: Option<std::path::PathBuf>| FileEntry {
            name: rec.name.clone(),
            path,
            parts: smallvec![rec.media.clone()],
            size: rec.size,
            mime_idx: rec.mime_idx,
            archive_entries: rec.archive_entries.clone(),
            file_type: rec.file_type.clone(),
            mtime: rec.mtime,
        };

        if titles.is_empty() {
            files.push(make_entry(None));
        } else {
            for tag in &titles {
                files.push(make_entry(Some(std::path::PathBuf::from(tag))));
            }
        }
    }
    println!(
        "Saved Messages indexed: {} entries from {} messages",
        files.len(),
        processed
    );

    files.sort_by_key(|f| f.name.to_lowercase());
    // Detect multipart files among saved messages and merge contiguous parts.
    // Only merge parts that share the same tag `path` (i.e. same saved reaction
    // tag placement). This avoids grouping parts that were fanned out under
    // different tags.
    let mut groups: std::collections::HashMap<String, Vec<(usize, usize)>> = std::collections::HashMap::new();
    for (i, f) in files.iter().enumerate() {
        if let Some((base, part)) = split_part_suffix(f.doc_name()) {
            groups.entry(base.to_string()).or_default().push((i, part));
        }
    }

    let mut removed = std::collections::BTreeSet::new();
    let mut new_files: Vec<FileEntry> = Vec::new();
    for (base, mut entries) in groups.into_iter() {
        if entries.len() < 2 { continue; }
        entries.sort_by_key(|&(_, p)| p);
        if !entries.iter().enumerate().all(|(i, &(_, p))| p == i) { continue; }
        // Ensure all parts share the same tag path (both None or equal Some)
        let parts_indices: Vec<usize> = entries.iter().map(|(idx, _)| *idx).collect();
        let first_path = files[parts_indices[0]].path.clone();
        if !parts_indices.iter().all(|&idx| files[idx].path == first_path) { continue; }

        // collect docs and sizes
        let mut docs: DocParts = DocParts::new();
        let mut total_size: Option<usize> = Some(0);
        for idx in &parts_indices {
            let f = &files[*idx];
            docs.extend(f.parts.iter().cloned());
            match (total_size, f.size) {
                (Some(acc), Some(s)) => total_size = Some(acc + s),
                _ => total_size = None,
            }
        }

        // Exposed name: use the base (no message overrides in saved messages)
        let exposed_name = base.clone();

        // Representative from first part
        let first = &files[parts_indices[0]];

        let mut archive_entries_combined: Option<Vec<ArchiveFileEntry>> = None;
        if base.to_lowercase().ends_with(".zip") && !docs.is_empty() {
            // try_index_zip expects Documents; only attempt if all parts are Documents.
            let doc_vec: Vec<Document> = docs
                .iter()
                .cloned()
                .filter_map(|m| if let Media::Document(d) = m { Some(d) } else { None })
                .collect();
            if doc_vec.len() == docs.len() {
                println!("Processing saved-message multipart archive: {}", exposed_name);
                archive_entries_combined = try_index_zip(client, &doc_vec, &exposed_name, zip_cache).await;
            }
        }

        let combined = FileEntry {
            name: exposed_name,
            path: first.path.clone(),
            parts: docs,
            size: total_size,
            mime_idx: first.mime_idx,
            archive_entries: archive_entries_combined,
            file_type: first.file_type.clone(),
            mtime: first.mtime,
        };

        for idx in parts_indices { removed.insert(idx); }
        if combined.name.trim().is_empty() || combined.name == "<unnamed>" {
            continue;
        }
        new_files.push(combined);
    }

    // Append non-removed original entries
    for (i, f) in files.into_iter().enumerate() {
        if removed.contains(&i) { continue; }
        if f.name.trim().is_empty() || f.name == "<unnamed>" { continue; }
        new_files.push(f);
    }

    new_files.sort_by_key(|f| f.name.to_lowercase());

    Ok(TelegramChannel {
        archive_view,
        skip_deflated_id3v1: false,
        files: new_files,
    })
}
