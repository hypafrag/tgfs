use std::sync::Arc;
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{header, StatusCode, HeaderMap};
use axum::response::{Html, Response};
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::io::{StreamReader, ReaderStream};
use tokio::io::BufReader;
use async_compression::tokio::bufread::DeflateDecoder as AsyncDeflateDecoder;
use mime_guess::from_path as guess_mime;

use crate::index::{AppState, Entry, dir_listing};
use crate::indexer::download_range;

pub async fn handle_root(State(state): State<Arc<AppState>>) -> Html<String> {
    let mut channels: Vec<&String> = state.index.keys().collect();
    channels.sort();
    let entries: Vec<Entry> = channels
        .iter()
        .map(|k| Entry {
            href: format!("{}/", urlencoding::encode(k)),
            label: format!("{}/", k),
            size: None,
        })
        .collect();
    Html(dir_listing("Index of /", None, &entries))
}

pub async fn handle_channel_root(
    State(state): State<Arc<AppState>>,
    AxumPath(channel): AxumPath<String>,
) -> Result<Response, StatusCode> {
    let channel = channel.trim_end_matches('/').to_string();
    handle_channel_path(State(state), HeaderMap::new(), AxumPath((channel, String::new()))).await
}

pub async fn handle_channel_path(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    AxumPath((channel, path)): AxumPath<(String, String)>,
) -> Result<Response, StatusCode> {
    let channel = channel.trim_end_matches('/').to_string();
    let files = state.index.get(&channel).ok_or(StatusCode::NOT_FOUND)?;
    let orig_path = path;
    let is_dir_request = orig_path.is_empty() || orig_path.ends_with('/');
    let trimmed = orig_path.trim_end_matches('/');

    if is_dir_request {
        if trimmed.is_empty() {
            let mut entries: Vec<Entry> = Vec::new();
            for f in files.iter() {
                if f.is_zip && f.archive_entries.is_some() {
                    let stem = std::path::Path::new(&f.name)
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or(&f.name)
                        .to_string();
                    entries.push(Entry { href: format!("{}/", urlencoding::encode(&f.name)), label: format!("{}/", stem), size: None });
                    if f.archive_entries.is_some() && f.archive_view == crate::index::ArchiveView::FileAndDirectory {
                        entries.push(Entry { href: urlencoding::encode(&f.name).into_owned(), label: f.name.clone(), size: f.size });
                    }
                } else {
                    entries.push(Entry { href: urlencoding::encode(&f.name).into_owned(), label: f.name.clone(), size: f.size });
                }
            }
            let title = format!("Index of /{channel}/");
            let body = dir_listing(&title, Some("/"), &entries);
            return Ok(Response::builder().header(header::CONTENT_TYPE, "text/html; charset=utf-8").body(Body::from(body)).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?);
        }

        // inner listing logic (kept simple)
        let mut parts = trimmed.splitn(2, '/');
        let archive_name = parts.next().unwrap();
        let inner_path = parts.next().unwrap_or("");
        let entry = files.iter().find(|e| e.name == archive_name && e.is_zip).ok_or(StatusCode::NOT_FOUND)?;
        let archive_entries = entry.archive_entries.as_ref().ok_or(StatusCode::NOT_FOUND)?;
        use std::collections::BTreeSet;
        let mut seen = BTreeSet::new();
        let mut listing: Vec<Entry> = Vec::new();
        // Build relative links for items inside the archive (do not prefix archive name again)
        let prefix = if inner_path.is_empty() { String::new() } else { format!("{}/", inner_path) };
        for ae in archive_entries.iter() {
            if !ae.path.starts_with(prefix.as_str()) { continue; }
            let rest = &ae.path[prefix.len()..];
            if rest.is_empty() { continue; }
            let mut seg_iter = rest.splitn(2, '/');
            let name = seg_iter.next().unwrap();
            let is_dir = seg_iter.next().is_some();
            if !seen.insert((name.to_string(), is_dir)) { continue; }
            let combined = if inner_path.is_empty() { name.to_string() } else { format!("{}/{}", inner_path, name) };
            if is_dir {
                listing.push(Entry { href: format!("{}/", urlencoding::encode(&combined)), label: format!("{}/", name), size: None });
            } else {
                let ae_size = archive_entries.iter().find(|x| x.path == format!("{}{}", prefix, name)).map(|x| x.uncompressed_size);
                listing.push(Entry { href: urlencoding::encode(&combined).into_owned(), label: name.to_string(), size: ae_size });
            }
        }
        let title = format!("Index of /{channel}/{trimmed}/");
        let body = dir_listing(&title, Some(&format!("/{channel}/")), &listing);
        return Ok(Response::builder().header(header::CONTENT_TYPE, "text/html; charset=utf-8").body(Body::from(body)).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?);
    }

    // File download path (orig_path does not end with '/')
    // either a top-level file or an inner archive file
    if !trimmed.contains('/') {
        // top-level file download
        let files_vec = files;
        let fentry = files_vec.iter().find(|e| e.name == trimmed).ok_or(StatusCode::NOT_FOUND)?;
        // if zip and archive_view is Directory-only, then we do not offer raw download; return 404
        if fentry.is_zip && fentry.archive_entries.is_some() && fentry.archive_view == crate::index::ArchiveView::Directory {
            return Err(StatusCode::NOT_FOUND);
        }

        let client = state.client.clone();
        let mime = fentry.mime_type.clone();
        let size = fentry.size;
        let encoded_name = urlencoding::encode(&fentry.name).into_owned();

        // Multipart support: if this FileEntry is composed of parts, allow seeking across
        // the concatenated uncompressed stream when all part sizes are known.
        let is_media = mime.starts_with("audio/") || mime.starts_with("video/");
        let content_disposition = if is_media {
            format!("inline; filename*=UTF-8''{encoded_name}")
        } else {
            format!("attachment; filename*=UTF-8''{encoded_name}")
        };

        if let Some(parts) = &fentry.parts {
            // gather sizes
            let sizes: Vec<Option<usize>> = parts.iter().map(|d| d.size().map(|s| s as usize)).collect();
            let all_known = sizes.iter().all(|s| s.is_some());
            let total = if all_known { Some(sizes.iter().map(|s| s.unwrap()).sum::<usize>()) } else { None };

            // If there's a Range header and sizes are known, satisfy the range by locating
            // the part containing the start offset and streaming from there.
            if let Some(range_hdr) = headers.get(header::RANGE) {
                if let (Some(total_size), Ok(range_str)) = (total, range_hdr.to_str()) {
                    if let Some(r) = range_str.strip_prefix("bytes=") {
                        let mut parts_r = r.splitn(2, '-');
                        let start_opt = parts_r.next().and_then(|s| if s.is_empty() { None } else { s.parse::<usize>().ok() });
                        let end_opt = parts_r.next().and_then(|s| if s.is_empty() { None } else { s.parse::<usize>().ok() });
                        let start = start_opt.unwrap_or(0);
                        let end = end_opt.unwrap_or(total_size.saturating_sub(1));
                        if start > end || start >= total_size {
                            return Err(StatusCode::RANGE_NOT_SATISFIABLE);
                        }
                        let wanted = end - start + 1;

                        // find starting part index and in-part offset
                        let mut accum = 0usize;
                        let mut part_idx = 0usize;
                        let mut offset_in_part = 0usize;
                        for (i, s) in sizes.iter().enumerate() {
                            let s = s.unwrap();
                            if start < accum + s {
                                part_idx = i;
                                offset_in_part = start - accum;
                                break;
                            }
                            accum += s;
                        }

                        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);
                        let client_clone = client.clone();
                        let parts_clone = parts.clone();
                        tokio::spawn(async move {
                            let chunk_size: usize = 64 * 1024;
                            let mut remaining = wanted;

                            for idx in part_idx..parts_clone.len() {
                                if remaining == 0 { break; }
                                let doc = parts_clone[idx].clone();
                                let _part_size = doc.size().unwrap_or(0) as usize;
                                // determine start offset in this part
                                let start_off = if idx == part_idx { offset_in_part } else { 0 };
                                let first_chunk = (start_off / chunk_size) as i32;
                                let offset_in_first = start_off % chunk_size;

                                let mut dl = client_clone.iter_download(&doc).chunk_size(chunk_size as i32).skip_chunks(first_chunk);
                                let mut bytes_sent: usize = 0;
                                while let Ok(Some(chunk)) = dl.next().await {
                                    let mut slice = &chunk[..];
                                    if bytes_sent == 0 && offset_in_first > 0 {
                                        if slice.len() <= offset_in_first {
                                            // still in the skipped prefix
                                            continue;
                                        }
                                        slice = &slice[offset_in_first..];
                                    }
                                    let take = std::cmp::min(remaining, slice.len());
                                    if tx.send(Ok(Bytes::from(slice[..take].to_vec()))).await.is_err() { return; }
                                    remaining = remaining.saturating_sub(take);
                                    bytes_sent += take;
                                    if remaining == 0 { break; }
                                }
                            }
                        });

                        let body = Body::from_stream(ReceiverStream::new(rx));
                        let builder = Response::builder()
                            .status(StatusCode::PARTIAL_CONTENT)
                            .header(header::ACCEPT_RANGES, "bytes")
                            .header(header::CONTENT_TYPE, mime)
                            .header(header::CONTENT_DISPOSITION, content_disposition)
                            .header(header::CONTENT_RANGE, format!("bytes {}-{}/{}", start, start + wanted - 1, total_size))
                            .header(header::CONTENT_LENGTH, wanted.to_string());
                        return Ok(builder.body(body).unwrap());
                    }
                }
            }

            // No range requested or sizes unknown: stream full concatenated parts
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            let parts_clone = parts.clone();
            let client_clone = client.clone();
            tokio::spawn(async move {
                for doc_part in parts_clone {
                    let mut dl = client_clone.iter_download(&doc_part);
                    while let Ok(Some(chunk)) = dl.next().await {
                        if tx.send(Ok::<Bytes, std::io::Error>(Bytes::from(chunk))).await.is_err() {
                            return;
                        }
                    }
                }
            });

            let mut builder = Response::builder()
                .header(header::CONTENT_TYPE, mime)
                .header(header::CONTENT_DISPOSITION, content_disposition)
                .header(header::ACCEPT_RANGES, "bytes");
            if let Some(t) = total { builder = builder.header(header::CONTENT_LENGTH, t.to_string()); }
            let body = Body::from_stream(ReceiverStream::new(rx));
            return Ok(builder.body(body).unwrap());
        }

        // single-part: fallthrough to existing single-doc streaming
        let doc = fentry.doc.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let mut dl = client.iter_download(&doc);
            while let Ok(Some(chunk)) = dl.next().await {
                if tx
                    .send(Ok::<Bytes, std::io::Error>(Bytes::from(chunk)))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        let body = Body::from_stream(ReceiverStream::new(rx));
        let mut builder = Response::builder()
            .header(header::CONTENT_TYPE, mime)
            .header(header::CONTENT_DISPOSITION, content_disposition);
        if let Some(s) = size {
            builder = builder.header(header::CONTENT_LENGTH, s);
        }
        return Ok(builder.body(body).unwrap());
    }

    // inner archive file download: "archive.zip/path/to/file"
    let mut parts = trimmed.splitn(2, '/');
    let archive_name = parts.next().unwrap();
    let inner = parts.next().unwrap_or("");

    let archive_entry = files
        .iter()
        .find(|e| e.name == archive_name && e.is_zip && e.archive_entries.is_some())
        .ok_or(StatusCode::NOT_FOUND)?;

    // ranged-extract the requested file from the archive without downloading the whole archive
    let client = state.client.clone();
    let doc = archive_entry.doc.clone();

    let archive_entries = archive_entry.archive_entries.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    let ae = archive_entries.iter().find(|x| x.path == inner).ok_or(StatusCode::NOT_FOUND)?;

    // read local file header to determine extra field length and filename length
    let lh = download_range(&client, &doc, ae.local_header_offset as usize, 30)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if lh.len() < 30 || &lh[0..4] != [0x50, 0x4b, 0x03, 0x04] {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    let name_len = u16::from_le_bytes(lh[26..28].try_into().unwrap()) as usize;
    let extra_len = u16::from_le_bytes(lh[28..30].try_into().unwrap()) as usize;
    let data_offset = ae.local_header_offset as usize + 30 + name_len + extra_len;

    // stream compressed data only and asynchronously inflate on-the-fly
    let chunk_size: usize = 64 * 1024;
    let first_chunk = data_offset / chunk_size;
    let offset_in_first = data_offset % chunk_size;
    let to_read = ae.compressed_size;

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);
    let client_feed = client.clone();
    let doc_feed = doc.clone();
    tokio::spawn(async move {
        let mut dl = client_feed
            .iter_download(&doc_feed)
            .chunk_size(chunk_size as i32)
            .skip_chunks(first_chunk as i32);
        let mut bytes_sent: usize = 0;
        while let Ok(Some(chunk)) = dl.next().await {
            let mut slice = &chunk[..];
            if bytes_sent == 0 && offset_in_first > 0 {
                if slice.len() <= offset_in_first {
                    // still in the skipped prefix
                    continue;
                }
                slice = &slice[offset_in_first..];
            }
            let remaining = to_read.saturating_sub(bytes_sent);
            if remaining == 0 { break; }
            let take = std::cmp::min(remaining, slice.len());
            if tx.send(Ok(Bytes::from(slice[..take].to_vec()))).await.is_err() { break; }
            bytes_sent += take;
            if bytes_sent >= to_read { break; }
        }
        // close sender
    });

    let byte_stream = ReceiverStream::new(rx);
    let reader = StreamReader::new(byte_stream);
    let buf = BufReader::new(reader);
    let deflate = AsyncDeflateDecoder::new(buf);
    let decompressed = ReaderStream::new(deflate);

    let mime = guess_mime(inner).first_or_octet_stream().essence_str().to_string();
    let encoded_name = urlencoding::encode(inner).into_owned();
    let is_media = mime.starts_with("audio/") || mime.starts_with("video/");
    let content_disposition = if is_media {
        format!("inline; filename*=UTF-8''{encoded_name}")
    } else {
        format!("attachment; filename*=UTF-8''{encoded_name}")
    };

    // Support HTTP Range requests for media seeking. We decompress the requested entry
    // and stream only the requested uncompressed byte range. Note: seeking still requires
    // decompressing from the start of the entry up to the requested offset.
    let total = ae.uncompressed_size as usize;
    if let Some(range_hdr) = headers.get(header::RANGE) {
        if let Ok(range_str) = range_hdr.to_str() {
            if let Some(r) = range_str.strip_prefix("bytes=") {
                let mut parts = r.splitn(2, '-');
                let start_opt = parts.next().and_then(|s| if s.is_empty() { None } else { s.parse::<usize>().ok() });
                let end_opt = parts.next().and_then(|s| if s.is_empty() { None } else { s.parse::<usize>().ok() });
                let start = start_opt.unwrap_or(0);
                let end = end_opt.unwrap_or(total.saturating_sub(1));
                if start > end || start >= total {
                    return Err(StatusCode::RANGE_NOT_SATISFIABLE);
                }
                let wanted = end - start + 1;

                let (tx_partial, rx_partial) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);
                let mut dec_stream = decompressed;
                tokio::spawn(async move {
                    let mut sent: usize = 0;
                    let mut to_skip = start;
                    while let Some(chunk_res) = dec_stream.next().await {
                        match chunk_res {
                            Ok(chunk) => {
                                let mut offset = 0usize;
                                let chunk_len = chunk.len();
                                if to_skip > 0 {
                                    if to_skip >= chunk_len {
                                        to_skip -= chunk_len;
                                        continue;
                                    } else {
                                        offset = to_skip;
                                        to_skip = 0;
                                    }
                                }
                                let remaining = wanted.saturating_sub(sent);
                                if remaining == 0 { break; }
                                let take = std::cmp::min(remaining, chunk_len - offset);
                                let slice = chunk.slice(offset..offset + take);
                                if tx_partial.send(Ok(slice)).await.is_err() { break; }
                                sent += take;
                                if sent >= wanted { break; }
                            }
                            Err(e) => {
                                let _ = tx_partial.send(Err(e)).await;
                                break;
                            }
                        }
                    }
                });

                let body = Body::from_stream(ReceiverStream::new(rx_partial));
                let builder = Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(header::ACCEPT_RANGES, "bytes")
                    .header(header::CONTENT_TYPE, mime)
                    .header(header::CONTENT_DISPOSITION, content_disposition)
                    .header(header::CONTENT_RANGE, format!("bytes {}-{}/{}", start, start + wanted - 1, total))
                    .header(header::CONTENT_LENGTH, wanted.to_string());
                return Ok(builder.body(body).unwrap());
            }
        }
    }

    let builder = Response::builder()
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_TYPE, mime)
        .header(header::CONTENT_DISPOSITION, content_disposition)
        .header(header::CONTENT_LENGTH, total.to_string());
    let body = Body::from_stream(decompressed);
    Ok(builder.body(body).unwrap())
}

pub fn make_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(handle_root))
        .route("/:channel", get(handle_channel_root))
        .route("/:channel/", get(handle_channel_root))
        .route("/:channel/*path", get(handle_channel_path))
        .with_state(state)
}
