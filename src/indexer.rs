use std::collections::HashMap;
use std::io::{self, Write};
use grammers_client::media::{Document, Media};
use grammers_client::peer::Peer;
use grammers_client::Client;
use crate::index::{Config, FileEntry, ArchiveFileEntry, ArchiveView, DocParts};
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

fn classify_file_type(type_override: &Option<crate::index::FileType>, mime_type: &str, doc_name: &str) -> crate::index::FileType {
    if let Some(t) = type_override { return t.clone(); }
    if mime_type.starts_with("audio/") || mime_type.starts_with("video/") {
        crate::index::FileType::Media
    } else if mime_type == "application/zip" || doc_name.to_lowercase().ends_with(".zip") {
        crate::index::FileType::Zip
    } else {
        crate::index::FileType::File
    }
}

pub struct IndexBuildResult {
    pub index: HashMap<String, Vec<FileEntry>>,
    pub mime_vec: Vec<String>,
    pub channel_view_map: std::collections::HashMap<String, crate::index::ArchiveView>,
}

// Download a byte range [offset, offset+length) from Telegram, spanning one or
// more concatenated document parts. A single-document file is just a 1-element
// slice. Used by both the indexer (central-directory parsing, local-header reads)
// and the server (archive inner-file header reads).
pub async fn download_range(
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

fn parse_central_directory(cd: &[u8]) -> anyhow::Result<Vec<ArchiveFileEntry>> {
    let mut i: usize = 0;
    let mut entries = Vec::new();
    while i + 46 <= cd.len() {
        if &cd[i..i+4] != [0x50,0x4b,0x01,0x02] { break; }
        let mut compressed_size = u32le(cd, i+20) as u64;
        let mut uncompressed_size = u32le(cd, i+24) as u64;
        let name_len = u16le(cd, i+28) as usize;
        let extra_len = u16le(cd, i+30) as usize;
        let comment_len = u16le(cd, i+32) as usize;
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
        entries.push(ArchiveFileEntry {
            path: name,
            compressed_size: compressed_size as usize,
            uncompressed_size: uncompressed_size as usize,
            local_header_offset,
        });
    }
    Ok(entries)
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

fn resolve_filename(msg: &grammers_client::message::Message, doc: &Document) -> String {
    if msg.grouped_id().is_none() {
        for line in msg.text().lines() {
            if let Some(value) = line.strip_prefix("name:") {
                return value.trim().to_string();
            }
        }
    }
    doc.name().unwrap_or("<unnamed>").to_string()
}

fn resolve_type_override(msg: &grammers_client::message::Message) -> Option<crate::index::FileType> {
    if msg.grouped_id().is_none() {
        for line in msg.text().lines() {
            if let Some(value) = line.strip_prefix("type:") {
                let v = value.trim().to_lowercase();
                return match v.as_str() {
                    "file" => Some(crate::index::FileType::File),
                    "media" => Some(crate::index::FileType::Media),
                    "zip" => Some(crate::index::FileType::Zip),
                    _ => None,
                };
            }
        }
    }
    None
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

    let mut index: HashMap<String, Vec<FileEntry>> = HashMap::new();
    // MIME interning structures
    let mut mime_map: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut mime_vec: Vec<String> = Vec::new();
    // build channel -> archive_view map
    let channel_view_map: std::collections::HashMap<String, crate::index::ArchiveView> = config.channels.iter().map(|c| (c.name.clone(), c.archive_view)).collect();

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
            if let Some(Media::Document(doc)) = msg.media() {
                let raw_name = resolve_filename(&msg, &doc);
                // parse out optional path prefix from message `name:` override
                let (file_name, path_opt) = split_name(&raw_name);
                let type_override = resolve_type_override(&msg);
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
                if final_type == crate::index::FileType::Zip && entry.archive_view != ArchiveView::File && !is_multipart_part {
                    println!("  fetching EOCD tail of '{}' for indexing", file_name);
                    io::stdout().flush().ok();
                    let size_usize = doc.size().unwrap_or(0) as usize;
                    let tail_len = std::cmp::min(size_usize, 70_000);
                    let tail_offset = size_usize.saturating_sub(tail_len);
                    let parts_one = [doc.clone()];
                    let tail = download_range(&client, &parts_one, tail_offset, tail_len).await?;
                    if let Some((cd_off, cd_size)) = find_eocd(&tail, tail_offset as u64) {
                        println!("    central directory at {} ({} bytes)", cd_off, cd_size);
                        let cd_bytes = download_range(&client, &parts_one, cd_off as usize, cd_size as usize).await?;
                        let ae_list = parse_central_directory(&cd_bytes)?;
                        println!("  zip entries read: {}", ae_list.len());
                        archive_entries = Some(ae_list);
                    } else {
                        println!("  EOCD not found for '{}', skipping archive index", file_name);
                    }
                }
                // intern mime_type
                let mime_idx = *mime_map.entry(mime_type.clone()).or_insert_with(|| { mime_vec.push(mime_type.clone()); mime_vec.len() - 1 });
                files.push(FileEntry { name: file_name, path: path_opt, parts: smallvec![doc], size, mime_idx, archive_entries, file_type: final_type.clone() });
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
                println!("Processing multipart archive: {}", exposed_name);
                println!("  fetching EOCD tail of '{}' for indexing", exposed_name);
                io::stdout().flush().ok();
                let total_bytes = docs.iter().map(|d| d.size().unwrap_or(0) as usize).sum::<usize>();
                let tail_len = std::cmp::min(total_bytes, 70_000);
                let tail_offset = total_bytes.saturating_sub(tail_len);
                if let Ok(tail) = download_range(&client, &docs, tail_offset, tail_len).await {
                    if let Some((cd_off, cd_size)) = find_eocd(&tail, tail_offset as u64) {
                        println!("    central directory at {} ({} bytes)", cd_off, cd_size);
                        if let Ok(cd_bytes) = download_range(&client, &docs, cd_off as usize, cd_size as usize).await {
                            if let Ok(ae_list) = parse_central_directory(&cd_bytes) {
                                println!("  zip entries read: {}", ae_list.len());
                                archive_entries_combined = Some(ae_list);
                            }
                        }
                    } else {
                        println!("  EOCD not found for '{}', skipping archive index", exposed_name);
                    }
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
            new_files.push(combined);
        }

        // Append non-removed original entries
        for (i, f) in files.into_iter().enumerate() {
            if removed.contains(&i) { continue; }
            new_files.push(f);
        }

        // sort files by exposed name
        new_files.sort_by_key(|f| f.name.to_lowercase());
        println!(" {} files (post-group)", new_files.len());
        index.insert(name.clone(), new_files);
    }

    Ok(IndexBuildResult { index, mime_vec, channel_view_map })
}
