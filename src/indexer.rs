use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, Duration};
use grammers_client::media::{Document, Downloadable, Media};
use grammers_client::peer::Peer;
use grammers_client::Client;
use grammers_client::InvocationError;
use grammers_client::tl;
use grammers_session::types::PeerRef;
use log::{debug, error, info, warn};
use crate::config::{Config, ArchiveView};
use crate::index::{
    FileEntry, FileType, ArchiveFileEntry, DocParts, MsgIds, TelegramChannel, RawEntry,
    MimePool, ChannelSource,
};
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

fn apply_prefix_collapse(files: &mut Vec<FileEntry>, min_len: usize) {
    if min_len == 0 || files.len() < 2 { return; }

    let mut buckets: std::collections::BTreeMap<String, Vec<usize>> = Default::default();
    for (i, f) in files.iter().enumerate() {
        let key = f.path.as_ref().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default();
        buckets.entry(key).or_default().push(i);
    }

    let mut updates: Vec<(usize, std::path::PathBuf)> = Vec::new();

    for (_bucket_key, mut indices) in buckets {
        if indices.len() < 2 { continue; }
        indices.sort_by_key(|&i| files[i].name.to_lowercase());
        let names: Vec<&str> = indices.iter().map(|&i| files[i].name.as_str()).collect();

        let mut start = 0;
        while start + 1 < names.len() {
            let mut prefix_bytes = common_prefix_len(names[start], names[start + 1]);
            if prefix_bytes < min_len { start += 1; continue; }

            let mut end = start + 2;
            while end < names.len() {
                let new_len = common_prefix_len(&names[start][..prefix_bytes], names[end]);
                if new_len >= min_len { prefix_bytes = new_len; end += 1; } else { break; }
            }

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
    if let Some(tl::enums::Photo::Photo(inner)) = &p.raw.photo {
        let mut best: usize = 0;
        for sz in inner.sizes.iter() {
            match sz {
                tl::enums::PhotoSize::Size(s) => best = best.max(s.size as usize),
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
    pub channels: HashMap<String, RwLock<TelegramChannel>>,
    pub dir_to_channel: HashMap<String, String>,
}

/// Fetch and parse the ZIP central directory for `docs` (one or more concatenated parts).
/// Returns `None` if the archive can't be indexed (no EOCD, download failure, etc.).
async fn try_index_zip(
    client: &Client,
    docs: &[Document],
    label: &str,
    cache: &Mutex<ZipCache>,
) -> Option<Vec<ArchiveFileEntry>> {
    let total: usize = docs.iter().map(|d| d.size().unwrap_or(0) as usize).sum();
    let raw_name = docs.get(0).and_then(|d| d.name()).map(|s| s.to_string()).unwrap_or_else(|| label.to_string());
    let name_key = if let Some((base, _)) = split_part_suffix(&raw_name) { base.to_string() } else { raw_name.clone() };
    let key = ZipCacheKey { name: name_key.clone(), size: total };
    {
        let g = cache.lock().unwrap();
        if let Some(cached) = g.get(&key) {
            debug!("zip index cache hit for {} ({} bytes)", key.name, key.size);
            return Some(cached);
        }
    }
    let media_parts: Vec<Media> = docs.iter().cloned().map(Media::Document).collect();
    debug!("fetching EOCD tail of '{}' for indexing", label);
    let tail_len = std::cmp::min(total, 70_000);
    let tail_offset = total.saturating_sub(tail_len);
    let tail = download_range(client, &media_parts, tail_offset, tail_len).await.ok()?;
    let (cd_off, cd_size) = match find_eocd(&tail, tail_offset as u64) {
        Some(v) => v,
        None => {
            warn!("EOCD not found for '{}', skipping archive index", label);
            return None;
        }
    };
    debug!("central directory at {} ({} bytes)", cd_off, cd_size);
    let cd_bytes = download_range(client, &media_parts, cd_off as usize, cd_size as usize).await.ok()?;
    let (mut ae_list, lh_offsets) = parse_central_directory(&cd_bytes).ok()?;
    for (ae, lh_offset) in ae_list.iter_mut().zip(lh_offsets.iter()) {
        let lh = download_range(client, &media_parts, *lh_offset as usize, 30).await.ok()?;
        if lh.len() < 30 || &lh[0..4] != [0x50, 0x4b, 0x03, 0x04] { return None; }
        let name_len = u16::from_le_bytes([lh[26], lh[27]]) as usize;
        let extra_len = u16::from_le_bytes([lh[28], lh[29]]) as usize;
        ae.data_offset = lh_offset + 30 + name_len as u64 + extra_len as u64;
    }
    debug!("zip entries read: {}", ae_list.len());
    cache.lock().unwrap().insert(ZipCacheKey { name: name_key, size: total }, ae_list.clone());
    Some(ae_list)
}

/// Telegram chunk size for `iter_download`. Must be a power of 2; 64 KB is the
/// standard MTProto chunk granularity.
const TG_CHUNK_SIZE: usize = 64 * 1024;
const PARALLEL_DOWNLOAD_STREAMS: usize = 4;

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
        let part_buf = match &parts[i] {
            Media::Document(d) => download_part_range(client, d, start_in_part, read_len).await?,
            Media::Photo(p) => download_part_range_photo(client, p, start_in_part, read_len).await?,
            _ => return Err(anyhow::anyhow!("download_range: unsupported media type {:?}", parts[i])),
        };
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

pub fn is_file_ref_expired(e: &anyhow::Error) -> bool {
    for cause in e.chain() {
        if let Some(InvocationError::Rpc(rpc)) = cause.downcast_ref::<InvocationError>() {
            if rpc.name == "FILE_REFERENCE_EXPIRED" { return true; }
        }
    }
    false
}

pub async fn refresh_part_documents(
    client: &Client,
    peer: PeerRef,
    msg_ids: &[i32],
) -> anyhow::Result<Vec<Document>> {
    let messages = client.get_messages_by_id(peer, msg_ids).await?;
    let mut out = Vec::with_capacity(messages.len());
    for (i, mopt) in messages.into_iter().enumerate() {
        let m = mopt.ok_or_else(|| anyhow::anyhow!("message {} missing on file-reference refresh", msg_ids[i]))?;
        match m.media() {
            Some(Media::Document(d)) => out.push(d),
            _ => anyhow::bail!("expected document media on refreshed message {}", msg_ids[i]),
        }
    }
    Ok(out)
}

pub fn apply_fresh_docs(parts: &[Media], fresh_docs: &Mutex<HashMap<i64, Document>>) -> Vec<Media> {
    let cache = fresh_docs.lock().unwrap();
    parts
        .iter()
        .map(|m| match m {
            Media::Document(d) => match cache.get(&d.id()) {
                Some(fresh) => Media::Document(fresh.clone()),
                None => Media::Document(d.clone()),
            },
            other => other.clone(),
        })
        .collect()
}

pub async fn download_range_refresh(
    client: &Client,
    parts: &[Media],
    msg_ids: &[i32],
    peer: Option<PeerRef>,
    fresh_docs: &Mutex<HashMap<i64, Document>>,
    offset: usize,
    length: usize,
) -> anyhow::Result<Vec<u8>> {
    let effective = apply_fresh_docs(parts, fresh_docs);
    match download_range(client, &effective, offset, length).await {
        Ok(buf) => Ok(buf),
        Err(e) => {
            if !is_file_ref_expired(&e) { return Err(e); }
            let peer = match peer {
                Some(p) => p,
                None => return Err(e),
            };
            if msg_ids.len() != parts.len() || msg_ids.is_empty() {
                return Err(e);
            }
            warn!(
                "download_range: FILE_REFERENCE_EXPIRED, refreshing {} part(s)",
                msg_ids.len()
            );
            let docs = refresh_part_documents(client, peer, msg_ids).await?;
            {
                let mut cache = fresh_docs.lock().unwrap();
                for d in &docs {
                    cache.insert(d.id(), d.clone());
                }
            }
            let refreshed: Vec<Media> = docs.into_iter().map(Media::Document).collect();
            download_range(client, &refreshed, offset, length).await
        }
    }
}

async fn download_part_range_inner(
    client: &Client,
    file_dc: i32,
    location: tl::enums::InputFileLocation,
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
        let location = location.clone();
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            let mut out: Vec<u8> = Vec::with_capacity(take_n * TG_CHUNK_SIZE);
            for chunk_idx in 0..take_n {
                let chunk_num = start_chunk + chunk_idx;
                let chunk_off = (chunk_num * TG_CHUNK_SIZE) as i64;
                let mut retries = 0u32;
                let chunk = loop {
                    let req = tl::functions::upload::GetFile {
                        precise: true,
                        cdn_supported: true,
                        location: location.clone(),
                        offset: chunk_off,
                        limit: TG_CHUNK_SIZE as i32,
                    };
                    match client.invoke_in_dc(file_dc, &req).await {
                        Ok(tl::enums::upload::File::File(f)) => break f.bytes,
                        Ok(tl::enums::upload::File::CdnRedirect(r)) => {
                            let enc_key: [u8; 32] = r.encryption_key.try_into()
                                .map_err(|_| anyhow::anyhow!("CDN key must be 32 bytes"))?;
                            let enc_iv: [u8; 16] = r.encryption_iv.try_into()
                                .map_err(|_| anyhow::anyhow!("CDN iv must be 16 bytes"))?;
                            let cdn_dc = r.dc_id;
                            let file_token = r.file_token;
                            let mut cdn_retries = 0u32;
                            let mut bytes = loop {
                                let cdn_req = tl::functions::upload::GetCdnFile {
                                    file_token: file_token.clone(),
                                    offset: chunk_off,
                                    limit: TG_CHUNK_SIZE as i32,
                                };
                                match client.invoke_in_dc(cdn_dc, &cdn_req).await {
                                    Ok(tl::enums::upload::CdnFile::File(f)) => break f.bytes,
                                    Ok(tl::enums::upload::CdnFile::ReuploadNeeded(n)) => {
                                        let reup = tl::functions::upload::ReuploadCdnFile {
                                            file_token: file_token.clone(),
                                            request_token: n.request_token,
                                        };
                                        if let Err(e) = client.invoke_in_dc(file_dc, &reup).await {
                                            warn!("reuploadCdnFile failed: {e:?}");
                                        }
                                    }
                                    Err(grammers_client::InvocationError::Rpc(ref rpc)) if rpc.name == "FLOOD_WAIT" => {
                                        let wait = rpc.value.unwrap_or(1);
                                        cdn_retries += 1;
                                        if cdn_retries > 5 {
                                            return Err::<Vec<u8>, anyhow::Error>(anyhow::anyhow!("CDN FLOOD_WAIT exceeded"));
                                        }
                                        warn!("CDN FLOOD_WAIT {} at chunk {}, retry {}/5", wait, chunk_num, cdn_retries);
                                        tokio::time::sleep(std::time::Duration::from_secs(wait as u64)).await;
                                    }
                                    Err(e) => return Err::<Vec<u8>, anyhow::Error>(e.into()),
                                }
                            };
                            use aes::cipher::{generic_array::GenericArray, KeyIvInit, StreamCipher, StreamCipherSeek};
                            #[allow(deprecated)]
                            let mut cipher = ctr::Ctr128LE::<aes::Aes256>::new(
                                GenericArray::from_slice(&enc_key),
                                GenericArray::from_slice(&enc_iv),
                            );
                            cipher.seek(chunk_off as u64);
                            cipher.apply_keystream(&mut bytes);
                            break bytes;
                        }
                        Err(grammers_client::InvocationError::Rpc(ref rpc)) if rpc.name == "FLOOD_WAIT" => {
                            let wait = rpc.value.unwrap_or(1);
                            retries += 1;
                            if retries > 5 {
                                return Err::<Vec<u8>, anyhow::Error>(anyhow::anyhow!("FLOOD_WAIT retry limit exceeded"));
                            }
                            warn!("FLOOD_WAIT {} at chunk {}, retry {}/5", wait, chunk_num, retries);
                            tokio::time::sleep(std::time::Duration::from_secs(wait as u64)).await;
                        }
                        Err(e) => return Err::<Vec<u8>, anyhow::Error>(e.into()),
                    }
                };
                out.extend_from_slice(&chunk);
            }
            Ok(out)
        }));
    }

    let mut combined: Vec<u8> = Vec::with_capacity(total_chunks * TG_CHUNK_SIZE);
    for h in handles {
        combined.extend_from_slice(&h.await??);
    }

    if combined.len() <= in_first_chunk {
        return Err(anyhow::anyhow!("failed to download requested range"));
    }
    let end = std::cmp::min(combined.len(), in_first_chunk + length);
    Ok(combined[in_first_chunk..end].to_vec())
}

async fn download_part_range_photo(
    client: &Client,
    photo: &grammers_client::media::Photo,
    offset: usize,
    length: usize,
) -> anyhow::Result<Vec<u8>> {
    let file_dc = match &photo.raw.photo {
        Some(tl::enums::Photo::Photo(p)) => p.dc_id,
        _ => return Err(anyhow::anyhow!("photo not available")),
    };
    let location = photo.to_raw_input_location()
        .ok_or_else(|| anyhow::anyhow!("photo not downloadable"))?;
    download_part_range_inner(client, file_dc, location, offset, length).await
}

async fn download_part_range(
    client: &Client,
    doc: &Document,
    offset: usize,
    length: usize,
) -> anyhow::Result<Vec<u8>> {
    let file_dc = match &doc.raw.document {
        Some(tl::enums::Document::Document(d)) => d.dc_id,
        _ => return Err(anyhow::anyhow!("document not available")),
    };
    let location = doc.to_raw_input_location()
        .ok_or_else(|| anyhow::anyhow!("document not downloadable"))?;
    download_part_range_inner(client, file_dc, location, offset, length).await
}

fn find_eocd(tail: &[u8], tail_offset: u64) -> Option<(u64, u64)> {
    if tail.len() < 22 { return None; }
    for i in (0..=tail.len() - 22).rev() {
        if &tail[i..i+4] == [0x50,0x4b,0x05,0x06] {
            let cd_size = u32le(tail, i+12) as u64;
            let cd_offset = u32le(tail, i+16) as u64;
            if cd_size == 0xFFFF_FFFF || cd_offset == 0xFFFF_FFFF {
                if let Some((z64_off, z64_sz)) = find_zip64_eocd(tail, i, tail_offset) {
                    return Some((z64_off, z64_sz));
                }
                return None;
            }
            return Some((cd_offset, cd_size));
        }
    }
    None
}

fn find_zip64_eocd(tail: &[u8], eocd_pos: usize, tail_offset: u64) -> Option<(u64, u64)> {
    if eocd_pos < 20 { return None; }
    let loc = eocd_pos - 20;
    if &tail[loc..loc+4] != [0x50, 0x4b, 0x06, 0x07] { return None; }
    let zip64_eocd_abs = u64le(tail, loc + 8);
    if zip64_eocd_abs < tail_offset { return None; }
    let z64_pos = (zip64_eocd_abs - tail_offset) as usize;
    if z64_pos + 56 > tail.len() { return None; }
    if &tail[z64_pos..z64_pos+4] != [0x50, 0x4b, 0x06, 0x06] { return None; }
    let cd_size = u64le(tail, z64_pos + 40);
    let cd_offset = u64le(tail, z64_pos + 48);
    Some((cd_offset, cd_size))
}

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
            data_offset: 0,
            compression_method,
            unix_mode,
        });
    }
    Ok((entries, lh_offsets))
}

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

/// Distill a single Telegram message into a `RawEntry`. Returns `None` for
/// non-media messages or those with empty/placeholder names. Used at both
/// startup-indexing time and by the realtime dispatcher.
///
/// For single-message zip files this also fetches the ZIP central directory
/// when `archive_view` requires browsable inner entries. Multipart zips have
/// to be indexed at assembly time, where the concatenated part list is known.
pub async fn message_to_raw_entry(
    client: &Client,
    msg: &grammers_client::message::Message,
    archive_view: ArchiveView,
    mime_pool: &MimePool,
    zip_cache: &Mutex<ZipCache>,
) -> Option<RawEntry> {
    match msg.media() {
        Some(Media::Document(doc)) => {
            let raw_name = message_field(msg, "name:").unwrap_or_else(|| doc.name().unwrap_or("<unnamed>").to_string());
            let (file_name, _path_opt) = split_name(&raw_name);
            if file_name.trim().is_empty() || file_name == "<unnamed>" { return None; }
            let size = doc.size().map(|s| s as usize);
            let mime_type = doc.mime_type().unwrap_or("application/octet-stream").to_string();
            let type_override = resolve_type_override(msg);
            let final_type = classify_file_type(&type_override, &mime_type, doc.name().unwrap_or(&file_name));
            let archive_entries = if final_type == FileType::Zip && archive_view != ArchiveView::File {
                try_index_zip(client, &[doc.clone()], &file_name, zip_cache).await
            } else {
                None
            };
            let mime_idx = mime_pool.intern(&mime_type);
            let mtime = msg_mtime(msg);
            let doc_name = doc.name().map(|s| s.to_string());
            Some(RawEntry {
                msg_id: msg.id(),
                media: Media::Document(doc),
                raw_name,
                doc_name,
                size,
                mime_idx,
                mtime,
                type_override,
                mime_type,
                archive_entries,
            })
        }
        Some(Media::Photo(photo)) => {
            let raw_name = message_field(msg, "name:").unwrap_or_else(|| format!("photo_{}.jpg", photo.id()));
            let (file_name, _path_opt) = split_name(&raw_name);
            if file_name.trim().is_empty() || file_name == "<unnamed>" { return None; }
            let size = Some(photo_largest_size(&photo));
            let mime_type = "image/jpeg".to_string();
            let type_override = resolve_type_override(msg);
            let mime_idx = mime_pool.intern(&mime_type);
            let mtime = msg_mtime(msg);
            Some(RawEntry {
                msg_id: msg.id(),
                media: Media::Photo(photo),
                raw_name,
                doc_name: None,
                size,
                mime_idx,
                mtime,
                type_override,
                mime_type,
                archive_entries: None,
            })
        }
        _ => None,
    }
}

/// Materialize a `RawEntry` into the (pre-grouping) `FileEntry` that one
/// message produces. The assembler runs this for every raw entry, then fuses
/// multipart parts together.
fn raw_entry_to_file(rec: &RawEntry) -> FileEntry {
    let (file_name, path_opt) = split_name(&rec.raw_name);
    let doc_name_for_classify = rec.doc_name.as_deref().unwrap_or(&file_name);
    let file_type = classify_file_type(&rec.type_override, &rec.mime_type, doc_name_for_classify);
    FileEntry {
        name: file_name,
        path: path_opt,
        parts: smallvec![rec.media.clone()],
        msg_ids: smallvec![rec.msg_id],
        size: rec.size,
        mime_idx: rec.mime_idx,
        archive_entries: rec.archive_entries.clone(),
        file_type,
        mtime: rec.mtime,
    }
}

/// Assemble the public `FileEntry` list for a channel from its per-message
/// `RawEntry` map. Runs multipart grouping, fetches ZIP indexes for multipart
/// archives, sorts, and applies prefix collapse.
pub async fn assemble_channel_files(
    client: &Client,
    raw_entries: &HashMap<i32, RawEntry>,
    archive_view: ArchiveView,
    collapse_by_prefix: Option<usize>,
    zip_cache: &Mutex<ZipCache>,
) -> Vec<FileEntry> {
    let mut files: Vec<FileEntry> = raw_entries.values().map(raw_entry_to_file).collect();

    // Detect multipart files by inspecting the document filename (not message overrides).
    let mut groups: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
    for (i, f) in files.iter().enumerate() {
        if let Some((base, part)) = split_part_suffix(f.doc_name()) {
            groups.entry(base.to_string()).or_default().push((i, part));
        }
    }

    let mut removed: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
    let mut new_files: Vec<FileEntry> = Vec::new();
    for (base, mut entries) in groups.into_iter() {
        if entries.len() < 2 { continue; }
        entries.sort_by_key(|&(_, p)| p);
        if !entries.iter().enumerate().all(|(i, &(_, p))| p == i) { continue; }

        let mut docs: DocParts = DocParts::new();
        let mut combined_msg_ids: MsgIds = MsgIds::new();
        let mut total_size: Option<usize> = Some(0);
        let parts_indices: Vec<usize> = entries.iter().map(|(idx, _)| *idx).collect();
        for idx in &parts_indices {
            let f = &files[*idx];
            docs.extend(f.parts.iter().cloned());
            combined_msg_ids.extend(f.msg_ids.iter().copied());
            match (total_size, f.size) {
                (Some(acc), Some(s)) => total_size = Some(acc + s),
                _ => total_size = None,
            }
        }

        let mut exposed_name = base.clone();
        if let Some((first_idx, _first_part)) = entries.iter().find(|&&(_, p)| p == 0) {
            let f0 = &files[*first_idx];
            let doc_name = f0.doc_name().to_string();
            if f0.name != doc_name { exposed_name = f0.name.clone(); }
        }

        let first = &files[parts_indices[0]];

        let mut archive_entries_combined: Option<Vec<ArchiveFileEntry>> = None;
        if base.to_lowercase().ends_with(".zip")
            && archive_view != ArchiveView::File
            && !docs.is_empty()
        {
            let doc_vec: Vec<Document> = docs.iter().cloned().filter_map(|m| if let Media::Document(d) = m { Some(d) } else { None }).collect();
            if doc_vec.len() == docs.len() {
                debug!("Processing multipart archive: {}", exposed_name);
                archive_entries_combined = try_index_zip(client, &doc_vec, &exposed_name, zip_cache).await;
            }
        }

        let combined_file_type = first.file_type.clone();
        let (exposed_base, exposed_path) = split_name(&exposed_name);

        let combined = FileEntry {
            name: exposed_base,
            path: exposed_path,
            parts: docs,
            msg_ids: combined_msg_ids,
            size: total_size,
            mime_idx: first.mime_idx,
            archive_entries: archive_entries_combined,
            file_type: combined_file_type,
            mtime: first.mtime,
        };

        for idx in parts_indices { removed.insert(idx); }
        if combined.name.trim().is_empty() || combined.name == "<unnamed>" { continue; }
        new_files.push(combined);
    }

    for (i, f) in files.drain(..).enumerate() {
        if removed.contains(&i) { continue; }
        if f.name.trim().is_empty() || f.name == "<unnamed>" { continue; }
        new_files.push(f);
    }

    new_files.sort_by_key(|f| f.name.to_lowercase());
    if let Some(min_len) = collapse_by_prefix {
        apply_prefix_collapse(&mut new_files, min_len);
    }
    new_files
}

pub async fn build_index(
    client: Client,
    config: &Config,
    mime_pool: &MimePool,
    zip_cache: &Mutex<ZipCache>,
) -> anyhow::Result<IndexBuildResult> {
    let mut channel_peers: HashMap<String, PeerRef> = HashMap::new();
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

    let mut index: HashMap<String, RwLock<TelegramChannel>> = HashMap::new();
    let mut dir_to_channel: HashMap<String, String> = config.channels.iter().map(|c| {
        let dir = c.directory.clone().unwrap_or_else(|| c.name.clone());
        (dir, c.name.clone())
    }).collect();

    for entry in &config.channels {
        let name = &entry.name;
        let peer_ref = match channel_peers.get(name) {
            Some(r) => r.clone(),
            None => { warn!("Channel '{name}' not found, skipping."); continue; }
        };

        info!("Indexing {name}...");
        let mut raw_entries: HashMap<i32, RawEntry> = HashMap::new();
        let mut messages = client.iter_messages(peer_ref);
        let mut processed_msgs: usize = 0;
        while let Some(msg) = messages.next().await? {
            processed_msgs += 1;
            if let Some(raw) = message_to_raw_entry(&client, &msg, entry.archive_view, mime_pool, zip_cache).await {
                raw_entries.insert(raw.msg_id, raw);
            }
        }
        info!("Finished indexing messages for '{}', processed {} messages", name, processed_msgs);
        debug!("{} raw entries", raw_entries.len());

        let files = assemble_channel_files(&client, &raw_entries, entry.archive_view, entry.collapse_by_prefix, zip_cache).await;
        info!("{} files (post-assembly)", files.len());

        let tchan = TelegramChannel {
            archive_view: entry.archive_view,
            skip_deflated_id3v1: entry.skip_deflated_id3v1,
            collapse_by_prefix: entry.collapse_by_prefix,
            files: Arc::new(files),
            raw_entries,
            peer: Some(peer_ref),
            source: ChannelSource::RegularChannel,
        };
        index.insert(name.clone(), RwLock::new(tchan));
    }

    if let Some(saved_cfg) = &config.saved_messages {
        let saved_dir = saved_cfg.directory.clone().unwrap_or_else(|| "saved_messages".to_string());
        if dir_to_channel.contains_key(&saved_dir) || index.contains_key(&saved_dir) {
            return Err(anyhow::anyhow!(
                "saved_messages directory '{}' collides with a configured channel name",
                saved_dir
            ));
        }
        match index_saved_messages(&client, mime_pool, zip_cache, saved_cfg.archive_view).await {
            Ok(channel) => {
                index.insert(saved_dir.clone(), RwLock::new(channel));
                dir_to_channel.insert(saved_dir.clone(), saved_dir.clone());
            }
            Err(e) => error!("failed to index saved messages: {}", e),
        }
    }

    Ok(IndexBuildResult { channels: index, dir_to_channel })
}

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

fn reaction_label(r: &tl::enums::Reaction) -> String {
    match r {
        tl::enums::Reaction::Emoji(e) => e.emoticon.clone(),
        tl::enums::Reaction::CustomEmoji(c) => format!("custom_{}", c.document_id),
        _ => String::new(),
    }
}

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

struct SavedRecord {
    media: Media,
    msg_id: i32,
    name: String,
    size: Option<usize>,
    mime_idx: usize,
    file_type: FileType,
    archive_entries: Option<Vec<ArchiveFileEntry>>,
    mtime: Option<SystemTime>,
    grouped_id: Option<i64>,
    own_tag_titles: Vec<String>,
}

async fn index_saved_messages(
    client: &Client,
    mime_pool: &MimePool,
    zip_cache: &Mutex<ZipCache>,
    archive_view: ArchiveView,
) -> anyhow::Result<TelegramChannel> {
    info!("Indexing Saved Messages...");

    let me = client.get_me().await?;
    let me_ref = me
        .to_ref()
        .await
        .ok_or_else(|| anyhow::anyhow!("could not resolve self peer"))?;

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
    debug!("saved reaction tags: {}", tag_titles.len());

    let mut records: Vec<SavedRecord> = Vec::new();
    let mut group_tag_titles: HashMap<i64, Vec<String>> = HashMap::new();
    let mut messages = client.iter_messages(me_ref);
    let mut processed: usize = 0;
    while let Some(msg) = messages.next().await? {
        processed += 1;

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

        if name.trim().is_empty() || name == "<unnamed>" { continue; }

        let file_type = classify_file_type(&None, &mime_type, &name);
        let mime_idx = mime_pool.intern(&mime_type);

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

        if let Some(gid) = grouped_id {
            let bucket = group_tag_titles.entry(gid).or_default();
            for t in &own_tag_titles {
                if !bucket.contains(t) { bucket.push(t.clone()); }
            }
        }

        records.push(SavedRecord {
            media,
            msg_id: msg.id(),
            name,
            size: size.map(|s| s as usize),
            mime_idx,
            file_type,
            archive_entries,
            mtime,
            grouped_id,
            own_tag_titles,
        });
    }

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
            msg_ids: smallvec![rec.msg_id],
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
    info!(
        "Saved Messages indexed: {} entries from {} messages",
        files.len(),
        processed
    );

    files.sort_by_key(|f| f.name.to_lowercase());
    let mut groups: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
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
        let parts_indices: Vec<usize> = entries.iter().map(|(idx, _)| *idx).collect();
        let first_path = files[parts_indices[0]].path.clone();
        if !parts_indices.iter().all(|&idx| files[idx].path == first_path) { continue; }

        let mut docs: DocParts = DocParts::new();
        let mut combined_msg_ids: MsgIds = MsgIds::new();
        let mut total_size: Option<usize> = Some(0);
        for idx in &parts_indices {
            let f = &files[*idx];
            docs.extend(f.parts.iter().cloned());
            combined_msg_ids.extend(f.msg_ids.iter().copied());
            match (total_size, f.size) {
                (Some(acc), Some(s)) => total_size = Some(acc + s),
                _ => total_size = None,
            }
        }

        let exposed_name = base.clone();
        let first = &files[parts_indices[0]];

        let mut archive_entries_combined: Option<Vec<ArchiveFileEntry>> = None;
        if base.to_lowercase().ends_with(".zip") && !docs.is_empty() {
            let doc_vec: Vec<Document> = docs
                .iter()
                .cloned()
                .filter_map(|m| if let Media::Document(d) = m { Some(d) } else { None })
                .collect();
            if doc_vec.len() == docs.len() {
                debug!("Processing saved-message multipart archive: {}", exposed_name);
                archive_entries_combined = try_index_zip(client, &doc_vec, &exposed_name, zip_cache).await;
            }
        }

        let combined = FileEntry {
            name: exposed_name,
            path: first.path.clone(),
            parts: docs,
            msg_ids: combined_msg_ids,
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

    for (i, f) in files.into_iter().enumerate() {
        if removed.contains(&i) { continue; }
        if f.name.trim().is_empty() || f.name == "<unnamed>" { continue; }
        new_files.push(f);
    }

    new_files.sort_by_key(|f| f.name.to_lowercase());

    Ok(TelegramChannel {
        archive_view,
        skip_deflated_id3v1: false,
        collapse_by_prefix: None,
        files: Arc::new(new_files),
        raw_entries: HashMap::new(),
        peer: Some(me_ref),
        source: ChannelSource::SavedMessages,
    })
}
