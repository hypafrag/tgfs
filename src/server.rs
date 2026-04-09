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

use crate::index::{AppState, Entry, dir_listing, FileType, FileEntry, DocParts};
use grammers_client::media::Media;
use grammers_client::Client;
use tokio_stream::Stream;

fn normalize_path(p: &std::path::Path) -> String {
    p.to_string_lossy().replace('\\', "/").trim_start_matches("./").trim_start_matches('/').to_string()
}

fn full_for(e: &FileEntry) -> String {
    match &e.path {
        Some(p) => {
            let s = normalize_path(p);
            if s.is_empty() { e.name.clone() } else { format!("{s}/{}", e.name) }
        }
        None => e.name.replace('\\', "/"),
    }
}

fn stem_full_for(e: &FileEntry) -> String {
    let stem = std::path::Path::new(&e.name).file_stem().and_then(|s| s.to_str()).unwrap_or(&e.name).to_string();
    match &e.path {
        Some(p) => {
            let s = normalize_path(p);
            if s.is_empty() { stem } else { format!("{}/{}", s, stem) }
        }
        None => stem,
    }
}


fn content_disposition(ft: &FileType, encoded_name: &str) -> String {
    match ft {
        FileType::Media => format!("inline; filename*=UTF-8''{}", encoded_name),
        _ => format!("attachment; filename*=UTF-8''{}", encoded_name),
    }
}

fn html_response(body: String) -> Result<Response, StatusCode> {
    Response::builder()
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(body))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn parse_range(headers: &HeaderMap, total: usize) -> Result<Option<(usize, usize)>, StatusCode> {
    use axum::http::header::RANGE;
    if let Some(range_hdr) = headers.get(RANGE) {
        let range_str = range_hdr.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
        if let Some(r) = range_str.strip_prefix("bytes=") {
            let mut parts = r.splitn(2, '-');
            let start_opt = parts.next().and_then(|s| if s.is_empty() { None } else { s.parse::<usize>().ok() });
            let end_opt = parts.next().and_then(|s| if s.is_empty() { None } else { s.parse::<usize>().ok() });
            let start = start_opt.unwrap_or(0);
            let end = end_opt.unwrap_or(total.saturating_sub(1));
            if start > end || start >= total { return Err(StatusCode::RANGE_NOT_SATISFIABLE); }
            return Ok(Some((start, end)));
        }
    }
    Ok(None)
}

fn encode_segments(p: &str) -> String {
    p.split('/').map(|seg| urlencoding::encode(seg).into_owned()).collect::<Vec<_>>().join("/")
}

const DOWNLOAD_CHUNK_SIZE: usize = 64 * 1024;

/// Single streaming primitive: stream `length` bytes starting at `start` across
/// a list of concatenated document parts. `length = None` means stream to EOF.
/// Handles single-doc files, multipart files, and ranged reads over archive parts
/// (used for inner-file extraction, where `parts` is the archive's own part list).
fn stream_parts_range(
    client: Client,
    parts: DocParts,
    start: usize,
    length: Option<usize>,
) -> ReceiverStream<Result<Bytes, std::io::Error>> {
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        // locate starting part index and in-part offset
        let sizes: Vec<usize> = parts
            .iter()
            .map(|m| match m {
                Media::Document(d) => d.size().unwrap_or(0) as usize,
                Media::Photo(p) => crate::indexer::photo_largest_size(p),
                _ => 0,
            })
            .collect();
        let mut accum = 0usize;
        let mut part_idx = 0usize;
        let mut offset_in_part = 0usize;
        for (i, s) in sizes.iter().enumerate() {
            if start < accum + s { part_idx = i; offset_in_part = start - accum; break; }
            accum += s;
            part_idx = i + 1;
        }
        if part_idx >= parts.len() { return; }

        let mut remaining: Option<usize> = length;
        for idx in part_idx..parts.len() {
            if remaining == Some(0) { break; }
            let doc = &parts[idx];
            let start_off = if idx == part_idx { offset_in_part } else { 0 };
            let first_chunk = (start_off / DOWNLOAD_CHUNK_SIZE) as i32;
            let offset_in_first = start_off % DOWNLOAD_CHUNK_SIZE;
            let mut dl = client.iter_download(doc).chunk_size(DOWNLOAD_CHUNK_SIZE as i32).skip_chunks(first_chunk);
            let mut bytes_sent: usize = 0;
            while let Ok(Some(chunk)) = dl.next().await {
                let mut slice = &chunk[..];
                if bytes_sent == 0 && offset_in_first > 0 {
                    if slice.len() <= offset_in_first { continue; }
                    slice = &slice[offset_in_first..];
                }
                let take = match remaining {
                    Some(r) => std::cmp::min(r, slice.len()),
                    None => slice.len(),
                };
                if tx.send(Ok(Bytes::from(slice[..take].to_vec()))).await.is_err() { return; }
                bytes_sent += take;
                if let Some(r) = remaining.as_mut() {
                    *r -= take;
                    if *r == 0 { break; }
                }
            }
        }
    });
    ReceiverStream::new(rx)
}

fn spawn_range_slicer<S>(mut stream: S, start: usize, wanted: usize) -> ReceiverStream<Result<Bytes, std::io::Error>>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        let mut sent: usize = 0;
        let mut to_skip = start;
        while let Some(chunk_res) = stream.next().await {
            match chunk_res {
                Ok(chunk) => {
                    let mut offset = 0usize;
                    let chunk_len = chunk.len();
                    if to_skip > 0 {
                        if to_skip >= chunk_len { to_skip -= chunk_len; continue; }
                        offset = to_skip;
                        to_skip = 0;
                    }
                    let remaining = wanted.saturating_sub(sent);
                    if remaining == 0 { break; }
                    let take = std::cmp::min(remaining, chunk_len - offset);
                    let slice = chunk.slice(offset..offset + take);
                    if tx.send(Ok(slice)).await.is_err() { break; }
                    sent += take;
                    if sent >= wanted { break; }
                }
                Err(e) => { let _ = tx.send(Err(e)).await; break; }
            }
        }
    });
    ReceiverStream::new(rx)
}

fn full_download_response(mime: &str, disposition: &str, body: Body, content_length: Option<usize>) -> Response {
    let mut b = Response::builder()
        .header(header::CONTENT_TYPE, mime)
        .header(header::CONTENT_DISPOSITION, disposition)
        .header(header::ACCEPT_RANGES, "bytes");
    if let Some(len) = content_length {
        b = b.header(header::CONTENT_LENGTH, len.to_string());
    }
    b.body(body).unwrap()
}

fn partial_response(mime: &str, disposition: &str, body: Body, start: usize, end: usize, total: usize) -> Response {
    let wanted = end - start + 1;
    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_TYPE, mime)
        .header(header::CONTENT_DISPOSITION, disposition)
        .header(header::CONTENT_RANGE, format!("bytes {}-{}/{}", start, end, total))
        .header(header::CONTENT_LENGTH, wanted.to_string())
        .body(body)
        .unwrap()
}

fn parent_href(channel: &str, trimmed: &str) -> String {
    if let Some(idx) = trimmed.rfind('/') {
        format!("/{}/{}/", urlencoding::encode(channel), encode_segments(&trimmed[..idx]))
    } else {
        format!("/{}/", urlencoding::encode(channel))
    }
}

/// Find the archive entry whose virtual path (or stem-path, with `.zip` stripped)
/// matches `trimmed`. Returns `(entry, inner_prefix)`. When `allow_exact` is true,
/// an exact match returns `inner_prefix = ""`; otherwise only prefix matches are
/// considered (used for inner-file downloads, where the archive root itself is
/// served as a directory listing instead).
fn match_archive<'a>(files: &'a [FileEntry], trimmed: &str, allow_exact: bool) -> Option<(&'a FileEntry, String)> {
    for e in files.iter() {
        if e.file_type != FileType::Zip || e.archive_entries.is_none() { continue; }
        let full = full_for(e);
        let stem = stem_full_for(e);
        if allow_exact && (trimmed == full || trimmed == stem) {
            return Some((e, String::new()));
        }
        let full_pref = format!("{}/", full);
        if trimmed.starts_with(&full_pref) {
            return Some((e, trimmed[full_pref.len()..].to_string()));
        }
        let stem_pref = format!("{}/", stem);
        if trimmed.starts_with(&stem_pref) {
            return Some((e, trimmed[stem_pref.len()..].to_string()));
        }
    }
    None
}

fn entries_for_root(files: &[FileEntry], channel: &str, archive_view: crate::index::ArchiveView) -> (Vec<Entry>, String) {
    use std::collections::BTreeSet;
    let mut dirs: BTreeSet<String> = BTreeSet::new();
    let mut top_files: Vec<&FileEntry> = Vec::new();
    for f in files.iter() {
        let full = full_for(f);
        if full.contains('/') {
            dirs.insert(full.splitn(2, '/').next().unwrap().to_string());
        } else {
            top_files.push(f);
        }
    }

    let ch = urlencoding::encode(channel);
    let mut entries: Vec<Entry> = Vec::new();
    for d in dirs.iter() {
        entries.push(Entry { href: format!("/{}/{}/", ch, encode_segments(d)), label: format!("{}/", d), size: None });
    }
    for f in top_files.iter() {
        let full = full_for(f);
        if f.file_type == FileType::Zip && f.archive_entries.is_some() {
            let stem_full = stem_full_for(f);
            let stem = std::path::Path::new(&f.name).file_stem().and_then(|s| s.to_str()).unwrap_or(&f.name).to_string();
            entries.push(Entry { href: format!("/{}/{}/", ch, encode_segments(&stem_full)), label: format!("{}/", stem), size: None });
            if archive_view == crate::index::ArchiveView::FileAndDirectory {
                entries.push(Entry { href: format!("/{}/{}", ch, encode_segments(&full)), label: f.name.clone(), size: f.size });
            }
        } else {
            entries.push(Entry { href: format!("/{}/{}", ch, encode_segments(&full)), label: f.name.clone(), size: f.size });
        }
    }
    entries.sort_by_key(|e| e.label.to_lowercase());
    (entries, format!("Index of /{channel}/"))
}

fn entries_for_archive_listing(archive_entry: &FileEntry, channel: &str, trimmed: &str, inner_prefix: &str) -> Vec<Entry> {
    use std::collections::BTreeSet;
    let archive_entries = archive_entry.archive_entries.as_ref().unwrap();
    let mut seen = BTreeSet::new();
    let mut listing: Vec<Entry> = Vec::new();
    let prefix = if inner_prefix.is_empty() { String::new() } else { format!("{}/", inner_prefix) };
    let base_path = format!("/{}/{}", urlencoding::encode(channel), encode_segments(trimmed));
    for ae in archive_entries.iter() {
        if !ae.path.starts_with(prefix.as_str()) { continue; }
        let rest = &ae.path[prefix.len()..];
        if rest.is_empty() { continue; }
        let mut seg_iter = rest.splitn(2, '/');
        let name = seg_iter.next().unwrap();
        let is_dir = seg_iter.next().is_some();
        if !seen.insert((name.to_string(), is_dir)) { continue; }
        if is_dir {
            listing.push(Entry { href: format!("{}/{}/", base_path, encode_segments(name)), label: format!("{}/", name), size: None });
        } else {
            let ae_size = archive_entries.iter().find(|x| x.path == format!("{}{}", prefix, name)).map(|x| x.uncompressed_size);
            listing.push(Entry { href: format!("{}/{}", base_path, encode_segments(name)), label: name.to_string(), size: ae_size });
        }
    }
    listing.sort_by_key(|e| e.label.to_lowercase());
    listing
}

fn entries_for_virtual_dir(files: &[FileEntry], channel: &str, trimmed: &str, archive_view: crate::index::ArchiveView) -> Vec<Entry> {
    use std::collections::BTreeSet;
    let mut seen = BTreeSet::new();
    let mut listing: Vec<Entry> = Vec::new();
    let prefix = format!("{}/", trimmed);
    let ch = urlencoding::encode(channel);
    for f in files.iter() {
        let full = full_for(f);
        if !full.starts_with(&prefix) { continue; }
        let rest = &full[prefix.len()..];
        if rest.is_empty() { continue; }
        let mut seg_iter = rest.splitn(2, '/');
        let name = seg_iter.next().unwrap();
        let is_dir = seg_iter.next().is_some();
        if !seen.insert((name.to_string(), is_dir)) { continue; }
        let combined = format!("{}/{}", trimmed, name);
        if is_dir {
            listing.push(Entry { href: format!("/{}/{}/", ch, encode_segments(&combined)), label: format!("{}/", name), size: None });
        } else {
            // Check for browsable zip inside a virtual directory and expose as directory
            if let Some(e) = files.iter().find(|x| full_for(x) == combined) {
                if e.file_type == FileType::Zip && e.archive_entries.is_some() && archive_view != crate::index::ArchiveView::File {
                    // expose stem as a directory listing
                    let stem = std::path::Path::new(&e.name).file_stem().and_then(|s| s.to_str()).unwrap_or(&e.name).to_string();
                    let stem_full = format!("{}/{}", trimmed, stem);
                    listing.push(Entry { href: format!("/{}/{}/", ch, encode_segments(&stem_full)), label: format!("{}/", stem), size: None });
                    if archive_view == crate::index::ArchiveView::FileAndDirectory {
                        listing.push(Entry { href: format!("/{}/{}", ch, encode_segments(&combined)), label: name.to_string(), size: e.size });
                    }
                    continue;
                }
            }
            let size = files.iter().find(|x| full_for(x) == combined).and_then(|x| x.size);
            listing.push(Entry { href: format!("/{}/{}", ch, encode_segments(&combined)), label: name.to_string(), size });
        }
    }
    listing.sort_by_key(|e| e.label.to_lowercase());
    listing
}

pub async fn handle_root(State(state): State<Arc<AppState>>) -> Html<String> {
    let mut dirs: Vec<&String> = state.dir_to_channel.keys().collect();
    dirs.sort();
    let entries: Vec<Entry> = dirs
        .iter()
        .map(|d| Entry {
            href: format!("{}/", urlencoding::encode(d)),
            label: format!("{}/", d),
            size: None,
        })
        .collect();
    Html(dir_listing("Index of /", None, &entries))
}

pub async fn handle_channel_root(
    State(state): State<Arc<AppState>>,
    AxumPath(dir): AxumPath<String>,
) -> Result<Response, StatusCode> {
    let dir = dir.trim_end_matches('/').to_string();
    handle_channel_path(State(state), HeaderMap::new(), AxumPath((dir, String::new()))).await
}

pub async fn handle_channel_path(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    AxumPath((channel, path)): AxumPath<(String, String)>,
) -> Result<Response, StatusCode> {
    let dir = channel.trim_end_matches('/').to_string();
    let channel = state.dir_to_channel.get(&dir).cloned().ok_or(StatusCode::NOT_FOUND)?;
    let files = state.channels.get(&channel).map(|c| &c.files).ok_or(StatusCode::NOT_FOUND)?;
    let orig_path = path;
    let is_dir_request = orig_path.is_empty() || orig_path.ends_with('/');
    let trimmed = orig_path.trim_end_matches('/');

    if is_dir_request {
        if trimmed.is_empty() {
            let archive_view = state.channels.get(&channel).map(|a| a.archive_view).unwrap_or(crate::index::ArchiveView::File);
            let (entries, title) = entries_for_root(files, &dir, archive_view);
            return html_response(dir_listing(&title, Some("/"), &entries));
        }

        // Determine whether this directory refers to an archive (either the archive root
        // or a path inside the archive). If so, render the archive internal listing
        // for the matching inner prefix.
        if let Some((archive_entry, inner_prefix)) = match_archive(files, trimmed, true) {
            let listing = entries_for_archive_listing(archive_entry, &dir, trimmed, &inner_prefix);
            let title = format!("Index of /{dir}/{trimmed}/");
            return html_response(dir_listing(&title, Some(&parent_href(&dir, trimmed)), &listing));
        }

        // Virtual directory listing: list immediate children (dirs and files) under trimmed/
        let archive_view = state.channels.get(&channel).map(|a| a.archive_view).unwrap_or(crate::index::ArchiveView::File);
        let listing = entries_for_virtual_dir(files, &dir, trimmed, archive_view);
        let title = format!("Index of /{dir}/{trimmed}/");
        return html_response(dir_listing(&title, Some(&parent_href(&dir, trimmed)), &listing));
    }

    // File download path (orig_path does not end with '/')
    // either a direct file (top-level or at a virtual path) or a file inside an archive
    if let Some(fentry) = files.iter().find(|e| full_for(e) == trimmed) {
        // if zip and archive_view is Directory-only, then we do not offer raw download; return 404
        if fentry.file_type == FileType::Zip && fentry.archive_entries.is_some() && state.channels.get(&channel).map(|a| a.archive_view).unwrap_or(crate::index::ArchiveView::File) == crate::index::ArchiveView::Directory {
            return Err(StatusCode::NOT_FOUND);
        }

        let client = state.client.clone();
        let mime = state.mime_pool.get(fentry.mime_idx).cloned().unwrap_or_else(|| "application/octet-stream".to_string());
        let encoded_name = urlencoding::encode(&fentry.name).into_owned();
        let disposition = content_disposition(&fentry.file_type, &encoded_name);

        if let Some(total_size) = fentry.size {
            if let Some((start, end)) = parse_range(&headers, total_size)? {
                let wanted = end - start + 1;
                let stream = stream_parts_range(client, fentry.parts.clone(), start, Some(wanted));
                let body = Body::from_stream(stream);
                return Ok(partial_response(&mime, &disposition, body, start, end, total_size));
            }
        }

        let body = Body::from_stream(stream_parts_range(client, fentry.parts.clone(), 0, None));
        return Ok(full_download_response(&mime, &disposition, body, fentry.size));
    }

    // inner archive file download: match an archive whose virtual full path or stem-full
    // path is a prefix of the request path: "<archive_full_path>/path/to/file" or
    // "<archive_stem_path>/path/to/file" (where stem_path omits the .zip extension).
    let (archive_entry, inner) = match_archive(files, trimmed, false).ok_or(StatusCode::NOT_FOUND)?;
    let inner = inner.as_str();

    // ranged-extract the requested file from the archive without downloading the whole archive.
    // The archive itself may be multipart, so we thread its full `parts` list through every read.
    let client = state.client.clone();
    let archive_parts = archive_entry.parts.clone();

    let archive_entries = archive_entry.archive_entries.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    let ae = archive_entries.iter().find(|x| x.path == inner).ok_or(StatusCode::NOT_FOUND)?;

    let data_offset = ae.data_offset as usize;

    // Choose handling based on compression method: 0 = stored (no compression),
    // 8 = deflate. For stored entries we stream the raw bytes directly; for
    // deflate entries we inflate on-the-fly as before.
    let mime = guess_mime(inner).first_or_octet_stream().essence_str().to_string();
    // inner-archive filename should be just the basename for the download header
    let inner_base = std::path::Path::new(inner).file_name().and_then(|s| s.to_str()).unwrap_or(inner);
    let encoded_name = urlencoding::encode(inner_base).into_owned();
    // disposition based on the inner file's MIME, not the archive's file_type
    let inner_type = if mime.starts_with("audio/") || mime.starts_with("video/") || mime.starts_with("image/") {
        FileType::Media
    } else {
        FileType::File
    };
    let disposition = content_disposition(&inner_type, &encoded_name);
    let total = ae.uncompressed_size as usize;

    match ae.compression_method {
        0 => {
            // Stored (no compression): compressed_size == uncompressed_size normally.
            if let Some((start, end)) = parse_range(&headers, total)? {
                let wanted = end - start + 1;
                let stream = stream_parts_range(client, archive_parts, data_offset + start, Some(wanted));
                let body = Body::from_stream(stream);
                return Ok(partial_response(&mime, &disposition, body, start, end, total));
            }
            let stream = stream_parts_range(client, archive_parts, data_offset, Some(ae.compressed_size));
            let body = Body::from_stream(stream);
            return Ok(full_download_response(&mime, &disposition, body, Some(total)));
        }
        8 => {
            // Deflated: inflate on-the-fly (existing behavior).
            let compressed = stream_parts_range(client, archive_parts, data_offset, Some(ae.compressed_size));
            let buf = BufReader::new(StreamReader::new(compressed));
            let decompressed = ReaderStream::new(AsyncDeflateDecoder::new(buf));

            if let Some((start, end)) = parse_range(&headers, total)? {
                let wanted = end - start + 1;
                let body = Body::from_stream(spawn_range_slicer(decompressed, start, wanted));
                return Ok(partial_response(&mime, &disposition, body, start, end, total));
            }

            let body = Body::from_stream(decompressed);
            return Ok(full_download_response(&mime, &disposition, body, Some(total)));
        }
        _ => {
            // Unsupported compression method
            return Err(StatusCode::NOT_IMPLEMENTED);
        }
    }
}

pub fn make_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(handle_root))
        .route("/:channel", get(handle_channel_root))
        .route("/:channel/", get(handle_channel_root))
        .route("/:channel/*path", get(handle_channel_path))
        .with_state(state)
}
