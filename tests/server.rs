use super::*;
use axum::http::HeaderValue;

// -------- parse_range --------

fn header_map_with_range(v: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(axum::http::header::RANGE, HeaderValue::from_str(v).unwrap());
    h
}

#[test]
fn parse_range_returns_none_when_header_absent() {
    let h = HeaderMap::new();
    assert_eq!(parse_range(&h, 100).unwrap(), None);
}

#[test]
fn parse_range_simple_range() {
    let h = header_map_with_range("bytes=0-9");
    assert_eq!(parse_range(&h, 100).unwrap(), Some((0, 9)));
}

#[test]
fn parse_range_open_ended_defaults_to_total_minus_one() {
    let h = header_map_with_range("bytes=10-");
    assert_eq!(parse_range(&h, 100).unwrap(), Some((10, 99)));
}

#[test]
fn parse_range_suffix_longer_than_file_is_whole_file() {
    // RFC 7233 §2.1: "If the selected representation is shorter than the
    // specified suffix-length, the entire representation is used."
    let h = header_map_with_range("bytes=-500");
    assert_eq!(parse_range(&h, 100).unwrap(), Some((0, 99)));
}

#[test]
fn parse_range_zero_length_suffix_is_unsatisfiable() {
    // `bytes=-0` requests zero bytes; RFC 7233 defines it as unsatisfiable.
    let h = header_map_with_range("bytes=-0");
    assert_eq!(parse_range(&h, 100).unwrap_err(), StatusCode::RANGE_NOT_SATISFIABLE);
}

#[test]
fn parse_range_out_of_range_returns_416() {
    let h = header_map_with_range("bytes=200-300");
    assert_eq!(parse_range(&h, 100).unwrap_err(), StatusCode::RANGE_NOT_SATISFIABLE);
}

// -------- bug demonstrations (these assert the CORRECT behavior and fail
// until the corresponding bug is fixed) --------

/// RFC 7233 §2.1: `bytes=-N` is a *suffix* range — the final N bytes of the
/// representation. For total=100, `bytes=-50` must be bytes 50..=99.
///
/// Today `parse_range` treats the empty start as 0 and the suffix length as
/// an end offset, serving the FIRST 51 bytes with a Content-Range header
/// claiming otherwise. Players fetching an MP4's trailing moov atom (or any
/// tool grabbing a file tail) silently receive the head of the file.
///
/// NOTE: `parse_range_implicit_start_is_zero` above blesses the buggy
/// behavior; it must be removed when this is fixed.
#[test]
fn parse_range_suffix_form_means_last_n_bytes() {
    let h = header_map_with_range("bytes=-50");
    assert_eq!(parse_range(&h, 100).unwrap(), Some((50, 99)));
}

/// RFC 7233 §2.1: "if the last-byte-pos value is greater than or equal to the
/// current length of the representation data, the byte range is interpreted as
/// the remainder of the representation" — i.e. `end` must be clamped to
/// total-1. Today the oversized end passes through, so `partial_response`
/// emits a Content-Length/Content-Range far larger than the stream delivers
/// and clients see a truncated response.
#[test]
fn parse_range_clamps_end_to_last_byte() {
    let h = header_map_with_range("bytes=0-999999");
    assert_eq!(parse_range(&h, 100).unwrap(), Some((0, 99)));
}

/// Photos are indexed as regular file entries (channels and saved messages),
/// but `stream_parts_range` skips `Media::Photo` parts entirely, so an HTTP
/// download of a photo terminates after 0 bytes with no error — while the
/// response header advertises the photo's full size. Clients hang or report
/// a truncated body.
///
/// Correct behavior is anything *other* than a silent empty success: either
/// bytes are produced or an error item is surfaced (offline, an attempted
/// download stalls/errors — both count as "the photo was handled").
#[tokio::test]
async fn photo_stream_must_not_be_silently_empty() {
    use grammers_client::tl;

    let session = std::sync::Arc::new(grammers_session::storages::MemorySession::default());
    let pool = grammers_mtsender::SenderPool::new(session, 1);
    let state = Arc::new(crate::index::AppState {
        client: grammers_client::Client::new(pool.handle),
        mime_pool: crate::index::MimePool::new(),
        channels: std::collections::HashMap::new(),
        dir_to_channel: std::collections::HashMap::new(),
        max_fetches_per_pid: None,
        max_fetches_total: None,
        fresh_docs: std::sync::Mutex::new(std::collections::HashMap::new()),
    });

    let photo = grammers_client::media::Photo::from_raw(tl::enums::Photo::Photo(tl::types::Photo {
        has_stickers: false,
        id: 1,
        access_hash: 2,
        file_reference: Vec::new(),
        date: 0,
        sizes: vec![tl::enums::PhotoSize::Size(tl::types::PhotoSize {
            r#type: "y".to_string(),
            w: 100,
            h: 100,
            size: 1000,
        })],
        video_sizes: None,
        dc_id: 2,
    }));
    // Sanity: the indexer sees this photo as a 1000-byte file, so the HTTP
    // response for it carries Content-Length: 1000.
    assert_eq!(crate::indexer::photo_largest_size(&photo), 1000);

    let ctx = StreamCtx {
        state,
        parts: smallvec::smallvec![Media::Photo(photo)],
        msg_ids: vec![10],
        peer: None,
    };
    let mut stream = stream_parts_range(ctx, 0, None);
    match tokio::time::timeout(std::time::Duration::from_secs(2), stream.next()).await {
        // Stream produced data or an error: the photo part was handled.
        Ok(Some(_)) => {}
        // Timed out: a real download was attempted (we're offline) — handled.
        Err(_) => {}
        // Immediate clean EOF: the photo part was skipped and the client gets
        // an empty body against a nonzero Content-Length.
        Ok(None) => panic!(
            "streaming a 1000-byte photo produced an empty body with no error \
             (Media::Photo parts are skipped by stream_parts_range)"
        ),
    }
}

// -------- encode_segments --------

#[test]
fn encode_segments_preserves_slashes_between_segments() {
    assert_eq!(encode_segments("a/b/c"), "a/b/c");
}

#[test]
fn encode_segments_percent_encodes_special_chars() {
    assert_eq!(encode_segments("foo bar/baz.txt"), "foo%20bar/baz.txt");
}

#[test]
fn encode_segments_handles_unicode() {
    let out = encode_segments("файл/test.bin");
    assert!(out.contains("/test.bin"));
    assert!(out.starts_with("%"));
}

// -------- content_disposition --------

#[test]
fn content_disposition_media_is_inline() {
    assert_eq!(content_disposition(&FileType::Media, "x.mp3"), "inline; filename*=UTF-8''x.mp3");
}

#[test]
fn content_disposition_file_is_attachment() {
    assert_eq!(content_disposition(&FileType::File, "x.bin"), "attachment; filename*=UTF-8''x.bin");
}

#[test]
fn content_disposition_zip_is_attachment() {
    assert_eq!(content_disposition(&FileType::Zip, "x.zip"), "attachment; filename*=UTF-8''x.zip");
}

// -------- normalize_path --------

#[test]
fn normalize_path_converts_backslashes_and_strips_leading_slashes() {
    let p = std::path::PathBuf::from("/a/b\\c");
    assert_eq!(normalize_path(&p), "a/b/c");
}

#[test]
fn normalize_path_strips_dot_slash_prefix() {
    let p = std::path::PathBuf::from("./foo");
    assert_eq!(normalize_path(&p), "foo");
}

// -------- parent_href --------

#[test]
fn parent_href_at_channel_root() {
    assert_eq!(parent_href("MyChannel", "file.bin"), "/MyChannel/");
}

#[test]
fn parent_href_in_subdir() {
    assert_eq!(parent_href("MyChannel", "dir/sub/file.bin"), "/MyChannel/dir/sub/");
}

#[test]
fn parent_href_url_encodes_channel_name() {
    assert_eq!(parent_href("Channel Name", "x"), "/Channel%20Name/");
}
