use std::collections::HashMap;
use std::io::{self, Write};
use std::convert::TryInto;
use grammers_client::media::{Document, Media};
use grammers_client::peer::Peer;
use grammers_client::Client;
// tokio_stream::StreamExt not required; `.next()` is available on the iterator types used
use crate::index::{Config, FileEntry, ArchiveFileEntry, ArchiveView};

// Download a byte range [offset, offset+length) from Telegram using the chunked
// downloader. Reused by server for ranged extraction as well.
pub async fn download_range(
    client: &Client,
    doc: &Document,
    offset: usize,
    length: usize,
) -> anyhow::Result<Vec<u8>> {
    let size = doc.size().unwrap_or(0) as usize;
    if offset >= size {
        return Ok(Vec::new());
    }
    let to_read = std::cmp::min(length, size - offset);

    let chunk_size: usize = 64 * 1024;
    let first_chunk = (offset / chunk_size) as i32;
    let offset_in_first = offset % chunk_size;

    let mut dl = client
        .iter_download(doc)
        .chunk_size(chunk_size as i32)
        .skip_chunks(first_chunk);

    let mut buf: Vec<u8> = Vec::with_capacity(offset_in_first + to_read);
    while let Ok(Some(chunk)) = dl.next().await {
        buf.extend_from_slice(&chunk);
        if buf.len() >= offset_in_first + to_read {
            break;
        }
    }

    if buf.len() <= offset_in_first {
        return Err(anyhow::anyhow!("failed to download requested range"));
    }

    Ok(buf[offset_in_first..offset_in_first + to_read].to_vec())
}

fn find_eocd(tail: &[u8]) -> Option<(u64, u64)> {
    if tail.len() < 22 { return None; }
    for i in (0..=tail.len() - 22).rev() {
        if &tail[i..i+4] == [0x50,0x4b,0x05,0x06] {
            let cd_size = u32::from_le_bytes(tail[i+12..i+16].try_into().unwrap()) as u64;
            let cd_offset = u32::from_le_bytes(tail[i+16..i+20].try_into().unwrap()) as u64;
            return Some((cd_offset, cd_size));
        }
    }
    None
}

fn parse_central_directory(cd: &[u8]) -> anyhow::Result<Vec<ArchiveFileEntry>> {
    let mut i: usize = 0;
    let mut entries = Vec::new();
    while i + 46 <= cd.len() {
        if &cd[i..i+4] != [0x50,0x4b,0x01,0x02] { break; }
        let compression_method = u16::from_le_bytes(cd[i+10..i+12].try_into().unwrap());
        let compressed_size = u32::from_le_bytes(cd[i+20..i+24].try_into().unwrap()) as usize;
        let uncompressed_size = u32::from_le_bytes(cd[i+24..i+28].try_into().unwrap()) as usize;
        let name_len = u16::from_le_bytes(cd[i+28..i+30].try_into().unwrap()) as usize;
        let extra_len = u16::from_le_bytes(cd[i+30..i+32].try_into().unwrap()) as usize;
        let comment_len = u16::from_le_bytes(cd[i+32..i+34].try_into().unwrap()) as usize;
        let local_header_offset = u32::from_le_bytes(cd[i+42..i+46].try_into().unwrap()) as u64;
        let var_start = i + 46;
        if var_start + name_len > cd.len() { break; }
        let name = String::from_utf8_lossy(&cd[var_start..var_start+name_len]).to_string();
        i = var_start + name_len + extra_len + comment_len;
        if name.ends_with('/') { continue; }
        entries.push(ArchiveFileEntry {
            path: name,
            compressed_size,
            uncompressed_size,
            compression_method,
            local_header_offset,
        });
    }
    Ok(entries)
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

pub async fn build_index(client: Client, config: &Config) -> anyhow::Result<HashMap<String, Vec<FileEntry>>> {
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
            println!("Indexed {} messages so far for channel '{}'...", processed_msgs, name);
            if let Some(Media::Document(doc)) = msg.media() {
                let file_name = resolve_filename(&msg, &doc);
                println!("Processing file: {}", file_name);
                io::stdout().flush().ok();
                let size = doc.size();
                let mime_type = doc.mime_type().unwrap_or("application/octet-stream").to_string();
                let is_zip = mime_type == "application/zip" || file_name.to_lowercase().ends_with(".zip");
                let mut archive_entries: Option<Vec<ArchiveFileEntry>> = None;
                if is_zip && entry.archive_view != ArchiveView::File {
                    println!("  fetching EOCD tail of '{}' for indexing", file_name);
                    io::stdout().flush().ok();
                    let size_usize = doc.size().unwrap_or(0) as usize;
                    let tail_len = std::cmp::min(size_usize, 70_000);
                    let tail_offset = size_usize.saturating_sub(tail_len);
                    let tail = download_range(&client, &doc, tail_offset, tail_len).await?;
                    if let Some((cd_off, cd_size)) = find_eocd(&tail) {
                        println!("    central directory at {} ({} bytes)", cd_off, cd_size);
                        let cd_bytes = download_range(&client, &doc, cd_off as usize, cd_size as usize).await?;
                        let ae_list = parse_central_directory(&cd_bytes)?;
                        println!("  zip entries read: {}", ae_list.len());
                        archive_entries = Some(ae_list);
                    } else {
                        println!("  EOCD not found for '{}', skipping archive index", file_name);
                    }
                }
                files.push(FileEntry { name: file_name, doc, size, mime_type, is_zip, archive_entries, archive_view: entry.archive_view });
            }
        }
        println!("Finished indexing messages for '{}', processed {} messages", name, processed_msgs);
        println!(" {} files", files.len());
        index.insert(name.clone(), files);
    }

    Ok(index)
}
