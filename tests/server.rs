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
fn parse_range_implicit_start_is_zero() {
    let h = header_map_with_range("bytes=-50");
    // Note: parser uses splitn(2, '-'), so "bytes=-50" yields start_opt=None,
    // end_opt=Some(50). Means [0..50] under our semantics.
    assert_eq!(parse_range(&h, 100).unwrap(), Some((0, 50)));
}

#[test]
fn parse_range_out_of_range_returns_416() {
    let h = header_map_with_range("bytes=200-300");
    assert_eq!(parse_range(&h, 100).unwrap_err(), StatusCode::RANGE_NOT_SATISFIABLE);
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
