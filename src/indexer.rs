use std::collections::HashMap;
use std::io::{self, Write};
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

// Document-only downloader adapter. Some internal code (ZIP central-directory
// parsing and local-header reads) works with `Document` slices and expects
// a downloader that accepts `&[Document]`. Provide a thin copy of the
// media-aware `download_range` that operates on `Document` specifically so
// callers don't need to convert types.
pub async fn download_range_docs(
    client: &Client,
    parts: &[Document],
    offset: usize,
    length: usize,
) -> anyhow::Result<Vec<u8>> {
    let sizes: Vec<usize> = parts.iter().map(|d| d.size().unwrap_or(0) as usize).collect();
    let total: usize = sizes.iter().sum();
    if offset >= total { return Ok(Vec::new()); }
    let to_read = std::cmp::min(length, total - offset);

    // locate starting part and in-part offset
    let mut pos = 0usize;
    let mut i = 0usize;
    while i < sizes.len() && pos + sizes[i] <= offset { pos += sizes[i]; i += 1; }
    let mut start_in_part = offset - pos;

    let chunk_size: usize = 64 * 1024;
    let mut buf: Vec<u8> = Vec::with_capacity(to_read);
    let mut need = to_read;
    while i < parts.len() && need > 0 {
        let avail = sizes[i].saturating_sub(start_in_part);
        if avail == 0 { i += 1; start_in_part = 0; continue; }
        let read_len = std::cmp::min(avail, need);
        let first_chunk = (start_in_part / chunk_size) as i32;
        let offset_in_first = start_in_part % chunk_size;
        let mut dl = client
            .iter_download(&parts[i])
            .chunk_size(chunk_size as i32)
            .skip_chunks(first_chunk);
        let mut part_buf: Vec<u8> = Vec::with_capacity(offset_in_first + read_len);
        while let Ok(Some(chunk)) = dl.next().await {
            part_buf.extend_from_slice(&chunk);
            if part_buf.len() >= offset_in_first + read_len { break; }
        }
        if part_buf.len() <= offset_in_first {
            return Err(anyhow::anyhow!("failed to download requested range"));
        }
        let end = std::cmp::min(part_buf.len(), offset_in_first + read_len);
        buf.extend_from_slice(&part_buf[offset_in_first..end]);
        need = need.saturating_sub(end - offset_in_first);
        i += 1;
        start_in_part = 0;
    }

    Ok(buf)
}

pub fn photo_largest_size(p: &grammers_client::media::Photo) -> usize {
    // `p.raw.photo` is an Option<tl::enums::Photo>. Drill down to the actual
    // TL Photo (`tl::enums::Photo::Photo`) and inspect its `sizes` Vector.
    if let Some(inner_photo_enum) = &p.raw.photo {
        if let tl::enums::Photo::Photo(inner_photo) = inner_photo_enum {
            let mut best: usize = 0;
            for sz in inner_photo.sizes.iter() {
                match sz {
                    tl::enums::PhotoSize::Size(s) => best = best.max(s.size as usize),
                    tl::enums::PhotoSize::PhotoCachedSize(s) => best = best.max(s.bytes.len()),
                    tl::enums::PhotoSize::PhotoPathSize(s) => best = best.max(s.bytes.len()),
                    tl::enums::PhotoSize::PhotoStrippedSize(s) => best = best.max(s.bytes.len()),
                    tl::enums::PhotoSize::Empty(_) => {}
                    tl::enums::PhotoSize::Progressive(p) => best = best.max(p.sizes.iter().max().cloned().unwrap_or(0) as usize),
                }
            }
            return best;
        }
    }
    0
}

fn classify_file_type(type_override: &Option<FileType>, mime_type: &str, doc_name: &str) -> FileType {
    if let Some(t) = type_override { return t.clone(); }
    if mime_type.starts_with("audio/") || mime_type.starts_with("video/") {
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
    println!("  fetching EOCD tail of '{}' for indexing", label);
    io::stdout().flush().ok();
    let tail_len = std::cmp::min(total, 70_000);
    let tail_offset = total.saturating_sub(tail_len);
    let tail = download_range_docs(client, docs, tail_offset, tail_len).await.ok()?;
    let (cd_off, cd_size) = match find_eocd(&tail, tail_offset as u64) {
        Some(v) => v,
        None => {
            println!("  EOCD not found for '{}', skipping archive index", label);
            return None;
        }
    };
    println!("    central directory at {} ({} bytes)", cd_off, cd_size);
    let cd_bytes = download_range_docs(client, docs, cd_off as usize, cd_size as usize).await.ok()?;
    let (mut ae_list, lh_offsets) = parse_central_directory(&cd_bytes).ok()?;
    // Fetch each local file header to resolve the true data offset.
    // The local extra field length can differ from the central directory,
    // so we must read it — but only once per entry, here at index time.
    for (ae, lh_offset) in ae_list.iter_mut().zip(lh_offsets.iter()) {
        let lh = download_range_docs(client, docs, *lh_offset as usize, 30).await.ok()?;
        if lh.len() < 30 || &lh[0..4] != [0x50, 0x4b, 0x03, 0x04] { return None; }
        let name_len = u16::from_le_bytes([lh[26], lh[27]]) as usize;
        let extra_len = u16::from_le_bytes([lh[28], lh[29]]) as usize;
        ae.data_offset = lh_offset + 30 + name_len as u64 + extra_len as u64;
    }
    println!("  zip entries read: {}", ae_list.len());
    cache.insert(ZipCacheKey { name: name_key, size: total }, ae_list.clone());
    Some(ae_list)
}

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

    let chunk_size: usize = 64 * 1024;
    let mut buf: Vec<u8> = Vec::with_capacity(to_read);
    let mut need = to_read;
    while i < parts.len() && need > 0 {
        let avail = sizes[i].saturating_sub(start_in_part);
        if avail == 0 { i += 1; start_in_part = 0; continue; }
        let read_len = std::cmp::min(avail, need);
        let first_chunk = (start_in_part / chunk_size) as i32;
        let offset_in_first = start_in_part % chunk_size;
        let mut dl = client
            .iter_download(&parts[i])
            .chunk_size(chunk_size as i32)
            .skip_chunks(first_chunk);
        let mut part_buf: Vec<u8> = Vec::with_capacity(offset_in_first + read_len);
        while let Ok(Some(chunk)) = dl.next().await {
            part_buf.extend_from_slice(&chunk);
            if part_buf.len() >= offset_in_first + read_len { break; }
        }
        if part_buf.len() <= offset_in_first {
            return Err(anyhow::anyhow!("failed to download requested range"));
        }
        let end = std::cmp::min(part_buf.len(), offset_in_first + read_len);
        buf.extend_from_slice(&part_buf[offset_in_first..end]);
        need = need.saturating_sub(end - offset_in_first);
        i += 1;
        start_in_part = 0;
    }

    Ok(buf)
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

#[allow(dead_code)]
fn resolve_filename(msg: &grammers_client::message::Message, doc: &Document) -> String {
    message_field(msg, "name:").unwrap_or_else(|| doc.name().unwrap_or("<unnamed>").to_string())
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
    let dir_to_channel: HashMap<String, String> = config.channels.iter().map(|c| {
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
                    let mut archive_entries: Option<Vec<ArchiveFileEntry>> = None;
                    let is_multipart_part = split_part_suffix(doc.name().unwrap_or("")).is_some();
                    if final_type == FileType::Zip && entry.archive_view != ArchiveView::File && !is_multipart_part {
                        archive_entries = try_index_zip(&client, &[doc.clone()], &file_name, &mut zip_cache).await;
                    }
                    // intern mime_type
                    let mime_idx = *mime_map.entry(mime_type.clone()).or_insert_with(|| { mime_vec.push(mime_type.clone()); mime_vec.len() - 1 });
                    files.push(FileEntry { name: file_name, path: path_opt, parts: smallvec![Media::Document(doc.clone())], size, mime_idx, archive_entries, file_type: final_type.clone() });
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
                    files.push(FileEntry { name: file_name, path: path_opt, parts: smallvec![Media::Photo(photo.clone())], size, mime_idx, archive_entries: None, file_type: final_type });
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
        // create TelegramChannel and insert
        let tchan = TelegramChannel { archive_view: entry.archive_view, skip_deflated_id3v1: entry.skip_deflated_id3v1, files: new_files };
        index.insert(name.clone(), tchan);
    }

    // Index Saved Messages if configured. Files tagged via Telegram saved-message
    // reaction tags are exposed as a directory per tag; untagged files live at the
    // root of the saved_messages directory.
    let mut dir_to_channel = dir_to_channel;
    if let Some(dir) = &config.saved_messages {
        if dir_to_channel.contains_key(dir) || index.contains_key(dir) {
            return Err(anyhow::anyhow!(
                "saved_messages directory '{}' collides with a configured channel name",
                dir
            ));
        }
        match index_saved_messages(&client, &mut mime_map, &mut mime_vec).await {
            Ok(channel) => {
                index.insert(dir.clone(), channel);
                dir_to_channel.insert(dir.clone(), dir.clone());
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

/// Read the per-message reactions from the raw `tl::enums::Message`. In
/// Saved Messages these are the user's own tag reactions on each message.
fn extract_message_reactions(msg: &grammers_client::message::Message) -> Vec<tl::enums::Reaction> {
    if let tl::enums::Message::Message(m) = &msg.raw {
        if let Some(tl::enums::MessageReactions::Reactions(r)) = &m.reactions {
            return r
                .results
                .iter()
                .map(|rc| {
                    let tl::enums::ReactionCount::Count(c) = rc;
                    c.reaction.clone()
                })
                .collect();
        }
    }
    Vec::new()
}

/// Index the user's Saved Messages dialog. Each document message becomes one
/// or more `FileEntry` rows: untagged files at the root, tagged files
/// fanned out under one virtual directory per matching saved reaction tag
/// (a single message tagged `music` + `ambient` produces two entries).
///
/// Skips zip indexing and multipart-grouping intentionally — saved messages
/// are treated as a flat tag-grouped catalogue. The existing fan-out via
/// `FileEntry.path` lets fuse.rs / server.rs render tag dirs without any
/// additional logic.
async fn index_saved_messages(
    client: &Client,
    mime_map: &mut HashMap<String, usize>,
    mime_vec: &mut Vec<String>,
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

    let mut files: Vec<FileEntry> = Vec::new();
    // Collect saved messages first so we can aggregate reaction tags across
    // grouped (album) messages. This ensures tags applied to one album part
    // are propagated to all parts in the same group.
    let mut saved_msgs: Vec<grammers_client::message::Message> = Vec::new();
    let mut messages = client.iter_messages(me_ref);
    let mut processed: usize = 0;
    while let Some(msg) = messages.next().await? {
        processed += 1;
        saved_msgs.push(msg);
    }

    // Build a mapping of grouped_id -> aggregated tag titles for that group.
    let mut group_tag_titles: std::collections::HashMap<i64, Vec<String>> = std::collections::HashMap::new();
    for msg in &saved_msgs {
        if let Some(gid) = msg.grouped_id() {
            let entry = group_tag_titles.entry(gid).or_default();
            for r in extract_message_reactions(msg) {
                if let Some(title) = tag_titles.get(&reaction_key(&r)).cloned() {
                    entry.push(title);
                }
            }
        }
    }
    for v in group_tag_titles.values_mut() {
        v.sort();
        v.dedup();
    }

    // Now iterate collected messages and build `FileEntry` items, merging
    // per-message tags with any group-level tags we discovered above.
    for msg in saved_msgs.into_iter() {
        match msg.media() {
            Some(Media::Document(doc)) => {
                let name = doc.name().unwrap_or("<unnamed>").to_string();
                let size = doc.size();
                let mime_type = doc
                    .mime_type()
                    .unwrap_or("application/octet-stream")
                    .to_string();
                // No `type:`/`name:` overrides in saved messages — classify purely from
                // the document MIME and filename.
                // Photos should be treated as media so browsers display them inline.
                let file_type = FileType::Media;
                let mime_idx = *mime_map.entry(mime_type.clone()).or_insert_with(|| {
                    mime_vec.push(mime_type.clone());
                    mime_vec.len() - 1
                });

                // Resolve which of this message's reactions are user-defined tags.
                let mut titles: Vec<String> = extract_message_reactions(&msg)
                    .iter()
                    .filter_map(|r| tag_titles.get(&reaction_key(r)).cloned())
                    .collect();
                if let Some(gid) = msg.grouped_id() {
                    if let Some(group_titles) = group_tag_titles.get(&gid) {
                        titles.extend(group_titles.clone());
                    }
                }
                titles.sort();
                titles.dedup();

                if titles.is_empty() {
                        // Skip entries with empty or placeholder names
                        if name.trim().is_empty() || name == "<unnamed>" { continue; }
                        files.push(FileEntry {
                            name: name.clone(),
                            path: None,
                            parts: smallvec![Media::Document(doc.clone())],
                            size,
                            mime_idx,
                            archive_entries: None,
                            file_type: file_type.clone(),
                        });
                } else {
                    for tag in titles {
                        // skip pushing tagged entries with empty names
                        if name.trim().is_empty() || name == "<unnamed>" { continue; }
                        files.push(FileEntry {
                            name: name.clone(),
                            path: Some(std::path::PathBuf::from(&tag)),
                            parts: smallvec![Media::Document(doc.clone())],
                            size,
                            mime_idx,
                            archive_entries: None,
                            file_type: file_type.clone(),
                        });
                    }
                }
            }
            Some(Media::Photo(photo)) => {
                let name = format!("photo_{}.jpg", photo.id());
                let size = Some(photo_largest_size(&photo));
                let mime_type = "image/jpeg".to_string();
                let file_type = classify_file_type(&None, &mime_type, &name);
                let mime_idx = *mime_map.entry(mime_type.clone()).or_insert_with(|| {
                    mime_vec.push(mime_type.clone());
                    mime_vec.len() - 1
                });
                // Resolve which of this message's reactions are user-defined tags.
                let mut titles: Vec<String> = extract_message_reactions(&msg)
                    .iter()
                    .filter_map(|r| tag_titles.get(&reaction_key(r)).cloned())
                    .collect();
                if let Some(gid) = msg.grouped_id() {
                    if let Some(group_titles) = group_tag_titles.get(&gid) {
                        titles.extend(group_titles.clone());
                    }
                }
                titles.sort();
                titles.dedup();

                if titles.is_empty() {
                        // Skip entries with empty or placeholder names
                        if name.trim().is_empty() || name == "<unnamed>" { continue; }
                        files.push(FileEntry {
                            name: name.clone(),
                            path: None,
                            parts: smallvec![Media::Photo(photo.clone())],
                            size,
                            mime_idx,
                            archive_entries: None,
                            file_type: file_type.clone(),
                        });
                } else {
                    for tag in titles {
                        if name.trim().is_empty() || name == "<unnamed>" { continue; }
                        files.push(FileEntry {
                            name: name.clone(),
                            path: Some(std::path::PathBuf::from(&tag)),
                            parts: smallvec![Media::Photo(photo.clone())],
                            size,
                            mime_idx,
                            archive_entries: None,
                            file_type: file_type.clone(),
                        });
                    }
                }
            }
            _ => {}
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

        let combined = FileEntry {
            name: exposed_name,
            path: first.path.clone(),
            parts: docs,
            size: total_size,
            mime_idx: first.mime_idx,
            archive_entries: None,
            file_type: first.file_type.clone(),
        };

        for idx in parts_indices { removed.insert(idx); }
        new_files.push(combined);
    }

    // Append non-removed original entries
    for (i, f) in files.into_iter().enumerate() {
        if removed.contains(&i) { continue; }
        new_files.push(f);
    }

    new_files.sort_by_key(|f| f.name.to_lowercase());

    Ok(TelegramChannel {
        archive_view: ArchiveView::File,
        skip_deflated_id3v1: false,
        files: new_files,
    })
}
