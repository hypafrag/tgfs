use super::*;

// -------- multipart suffix detection --------

#[test]
fn split_part_suffix_basic_two_digit() {
    assert_eq!(split_part_suffix("archive.zip.00"), Some(("archive.zip", 0)));
    assert_eq!(split_part_suffix("archive.zip.99"), Some(("archive.zip", 99)));
}

#[test]
fn split_part_suffix_three_digit_still_matches() {
    // Spec talks about two-digit parts, but split_part_suffix only requires
    // "all digits"; the contiguity check in assemble enforces the rest.
    assert_eq!(split_part_suffix("dump.bin.123"), Some(("dump.bin", 123)));
}

#[test]
fn split_part_suffix_rejects_non_digit_suffix() {
    assert_eq!(split_part_suffix("foo.zip"), None);
    assert_eq!(split_part_suffix("foo.bar"), None);
    assert_eq!(split_part_suffix("readme.txt"), None);
}

#[test]
fn split_part_suffix_rejects_no_dot() {
    assert_eq!(split_part_suffix("noextension"), None);
}

#[test]
fn split_part_suffix_rejects_empty_suffix() {
    assert_eq!(split_part_suffix("foo."), None);
}

// -------- caption directive parsing --------

#[test]
fn parse_bool_value_truthy() {
    for v in ["", "true", "TRUE", "yes", "Yes", "1", "  ", "  true  "] {
        assert!(parse_bool_value(v), "expected truthy: {:?}", v);
    }
}

#[test]
fn parse_bool_value_falsy() {
    for v in ["false", "no", "0", "off", "garbage"] {
        assert!(!parse_bool_value(v), "expected falsy: {:?}", v);
    }
}

#[test]
fn parse_type_value_known() {
    assert_eq!(parse_type_value("file"), Some(FileType::File));
    assert_eq!(parse_type_value("media"), Some(FileType::Media));
    assert_eq!(parse_type_value("zip"), Some(FileType::Zip));
}

#[test]
fn parse_type_value_unknown() {
    assert_eq!(parse_type_value(""), None);
    assert_eq!(parse_type_value("bogus"), None);
}

#[test]
fn resolve_path_override_passthrough_when_absent() {
    assert_eq!(resolve_path_override(None, "doc.bin"), "doc.bin");
}

#[test]
fn resolve_path_override_full_path_overrides_filename() {
    assert_eq!(
        resolve_path_override(Some("dir/sub/other.bin".into()), "doc.bin"),
        "dir/sub/other.bin"
    );
}

#[test]
fn resolve_path_override_trailing_slash_keeps_original_filename() {
    assert_eq!(
        resolve_path_override(Some("dir/sub/".into()), "doc.bin"),
        "dir/sub/doc.bin"
    );
}

#[test]
fn resolve_path_override_bare_slash_is_noop() {
    assert_eq!(resolve_path_override(Some("/".into()), "doc.bin"), "doc.bin");
}

#[test]
fn parse_field_in_text_first_match_wins() {
    assert_eq!(
        parse_field_in_text("path: foo\npath: bar\n", "path:"),
        Some("foo".to_string())
    );
}

#[test]
fn parse_field_in_text_trims_whitespace() {
    assert_eq!(
        parse_field_in_text("type:   media  ", "type:"),
        Some("media".to_string())
    );
}

#[test]
fn parse_field_in_text_missing_returns_none() {
    assert_eq!(parse_field_in_text("hello world", "path:"), None);
}

#[test]
fn parse_caption_directives_none_when_empty() {
    assert!(parse_caption_directives("").is_none());
    assert!(parse_caption_directives("just a plain caption").is_none());
}

#[test]
fn parse_caption_directives_combined() {
    let cap = parse_caption_directives("path: vacation/\ntype: media\nmultipart: true").unwrap();
    assert_eq!(cap.path_override.as_deref(), Some("vacation/"));
    assert_eq!(cap.type_override, Some(FileType::Media));
    assert!(cap.multipart);
}

#[test]
fn parse_caption_directives_bare_multipart_is_true() {
    let cap = parse_caption_directives("multipart:").unwrap();
    assert!(cap.multipart);
    assert!(cap.path_override.is_none());
    assert!(cap.type_override.is_none());
}

#[test]
fn parse_caption_directives_multipart_false_returns_none() {
    // No other directive present, multipart=false → no caption.
    assert!(parse_caption_directives("multipart: false").is_none());
}

// -------- classification --------

#[test]
fn classify_file_type_overrides_take_precedence() {
    assert_eq!(
        classify_file_type(&Some(FileType::Media), "application/zip", "x.zip"),
        FileType::Media
    );
}

#[test]
fn classify_file_type_audio_video_image_to_media() {
    assert_eq!(classify_file_type(&None, "audio/mpeg", "a.mp3"), FileType::Media);
    assert_eq!(classify_file_type(&None, "video/mp4", "v.mp4"), FileType::Media);
    assert_eq!(classify_file_type(&None, "image/jpeg", "p.jpg"), FileType::Media);
}

#[test]
fn classify_file_type_zip_mime_or_extension() {
    assert_eq!(classify_file_type(&None, "application/zip", "x.bin"), FileType::Zip);
    assert_eq!(classify_file_type(&None, "application/octet-stream", "x.ZIP"), FileType::Zip);
}

#[test]
fn classify_file_type_fallback_is_file() {
    assert_eq!(classify_file_type(&None, "application/octet-stream", "x.bin"), FileType::File);
}

// -------- filename splitting --------

#[test]
fn split_name_no_slash_is_filename_only() {
    let (n, p) = split_name("foo.bin");
    assert_eq!(n, "foo.bin");
    assert!(p.is_none());
}

#[test]
fn split_name_with_slash_extracts_dir() {
    let (n, p) = split_name("a/b/c.bin");
    assert_eq!(n, "c.bin");
    assert_eq!(p.unwrap().to_string_lossy(), "a/b");
}

// -------- prefix-collapse helpers --------

#[test]
fn common_prefix_len_basic() {
    assert_eq!(common_prefix_len("abcdef", "abcxyz"), 3);
    assert_eq!(common_prefix_len("abc", "abc"), 3);
    assert_eq!(common_prefix_len("", "abc"), 0);
}

#[test]
fn common_prefix_len_utf8_boundary() {
    // Multi-byte characters: ensure we count bytes (matching the algorithm)
    // and don't split mid-character.
    let a = "αβγδ";
    let b = "αβxx";
    // α and β are 2 bytes each → 4 bytes shared.
    assert_eq!(common_prefix_len(a, b), 4);
}

#[test]
fn trim_prefix_name_strips_trailing_ws_and_underscore() {
    assert_eq!(trim_prefix_name("Foo Bar  __"), "Foo Bar");
}

#[test]
fn trim_prefix_name_cuts_at_unmatched_bracket() {
    assert_eq!(trim_prefix_name("Into the Breach [010057"), "Into the Breach");
}

#[test]
fn trim_prefix_name_keeps_balanced_brackets() {
    assert_eq!(trim_prefix_name("Title [tag] suffix"), "Title [tag] suffix");
}

#[test]
fn trim_prefix_name_cuts_at_unmatched_paren() {
    assert_eq!(trim_prefix_name("Album (mp3"), "Album");
}

// -------- apply_prefix_collapse --------

fn fe(name: &str, path: Option<&str>) -> FileEntry {
    FileEntry {
        name: name.to_string(),
        path: path.map(std::path::PathBuf::from),
        parts: DocParts::new(),
        msg_ids: MsgIds::new(),
        size: None,
        mime_idx: 0,
        archive_entries: None,
        file_type: FileType::File,
        mtime: None,
    }
}

#[test]
fn apply_prefix_collapse_disabled_when_min_len_zero() {
    let mut v = vec![fe("aaa_one.bin", None), fe("aaa_two.bin", None)];
    apply_prefix_collapse(&mut v, 0);
    assert!(v[0].path.is_none() && v[1].path.is_none());
}

#[test]
fn apply_prefix_collapse_groups_shared_prefix() {
    let mut v = vec![fe("aaa_one.bin", None), fe("aaa_two.bin", None), fe("zzz.bin", None)];
    apply_prefix_collapse(&mut v, 3);
    // Sort by name so the test is index-stable regardless of bucket order.
    v.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(v[0].path.as_ref().unwrap().to_string_lossy(), "aaa");
    assert_eq!(v[1].path.as_ref().unwrap().to_string_lossy(), "aaa");
    assert!(v[2].path.is_none());
}

#[test]
fn apply_prefix_collapse_below_threshold_does_nothing() {
    let mut v = vec![fe("ab_one.bin", None), fe("ab_two.bin", None)];
    apply_prefix_collapse(&mut v, 5);
    assert!(v[0].path.is_none() && v[1].path.is_none());
}

// -------- ZIP parsing --------

/// Build a minimal valid EOCD record (no comment).
fn make_eocd(cd_off: u32, cd_size: u32) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&[0x50, 0x4b, 0x05, 0x06]); // signature
    b.extend_from_slice(&[0, 0, 0, 0]); // disk numbers, entries on disk, total entries
    b.extend_from_slice(&[0, 0, 0, 0]);
    b.extend_from_slice(&cd_size.to_le_bytes()); // central dir size
    b.extend_from_slice(&cd_off.to_le_bytes()); // central dir offset
    b.extend_from_slice(&[0, 0]); // comment length
    b
}

#[test]
fn find_eocd_simple_match() {
    let eocd = make_eocd(0xDEAD, 0xBEEF);
    let (off, sz) = find_eocd(&eocd, 0).unwrap();
    assert_eq!(off, 0xDEAD);
    assert_eq!(sz, 0xBEEF);
}

#[test]
fn find_eocd_returns_none_when_too_small() {
    let buf = [0u8; 10];
    assert!(find_eocd(&buf, 0).is_none());
}

#[test]
fn find_eocd_returns_none_for_zip64_sentinel_without_locator() {
    // EOCD with 0xFFFFFFFF for offset triggers ZIP64 lookup, which fails
    // because there's no locator → None.
    let eocd = make_eocd(0xFFFF_FFFF, 0xFFFF_FFFF);
    assert!(find_eocd(&eocd, 0).is_none());
}

/// Synthetic single-entry central directory header.
fn make_cd_entry(name: &str, comp_size: u32, uncomp_size: u32, local_off: u32) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&[0x50, 0x4b, 0x01, 0x02]); // signature
    b.extend_from_slice(&[0x14, 0x00]); // version made by (0 = MS-DOS)
    b.extend_from_slice(&[0x14, 0x00]); // version needed
    b.extend_from_slice(&[0x00, 0x00]); // flags
    b.extend_from_slice(&[0x08, 0x00]); // method (deflate)
    b.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // mod time + date
    b.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // crc32
    b.extend_from_slice(&comp_size.to_le_bytes());
    b.extend_from_slice(&uncomp_size.to_le_bytes());
    b.extend_from_slice(&(name.len() as u16).to_le_bytes()); // name len
    b.extend_from_slice(&[0x00, 0x00]); // extra len
    b.extend_from_slice(&[0x00, 0x00]); // comment len
    b.extend_from_slice(&[0x00, 0x00]); // disk number
    b.extend_from_slice(&[0x00, 0x00]); // internal attrs
    b.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // external attrs
    b.extend_from_slice(&local_off.to_le_bytes());
    b.extend_from_slice(name.as_bytes());
    b
}

#[test]
fn parse_central_directory_single_entry() {
    let cd = make_cd_entry("hello.txt", 100, 200, 50);
    let (entries, offs) = parse_central_directory(&cd).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path, "hello.txt");
    assert_eq!(entries[0].compressed_size, 100);
    assert_eq!(entries[0].uncompressed_size, 200);
    assert_eq!(entries[0].compression_method, 8);
    assert_eq!(offs, vec![50]);
}

#[test]
fn parse_central_directory_skips_directory_entries() {
    let mut cd = make_cd_entry("dir/", 0, 0, 0);
    cd.extend(make_cd_entry("file.bin", 10, 20, 30));
    let (entries, _) = parse_central_directory(&cd).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path, "file.bin");
}

#[test]
fn parse_zip64_extra_substitutes_only_sentinel_fields() {
    // tag=0x0001, size=24 (3 u64), then uncompressed, compressed, header_offset.
    let mut extra = Vec::new();
    extra.extend_from_slice(&[0x01, 0x00, 0x18, 0x00]);
    extra.extend_from_slice(&7u64.to_le_bytes());
    extra.extend_from_slice(&5u64.to_le_bytes());
    extra.extend_from_slice(&3u64.to_le_bytes());
    let mut un = 0xFFFF_FFFF_u64;
    let mut co = 0xFFFF_FFFF_u64;
    let mut off = 0xFFFF_FFFF_u64;
    parse_zip64_extra(&extra, &mut un, &mut co, &mut off);
    assert_eq!((un, co, off), (7, 5, 3));
}

#[test]
fn parse_zip64_extra_only_overwrites_sentinel_values() {
    // If un/co/off are already concrete (not 0xFFFFFFFF), they should not
    // be substituted even if the extra field contains values.
    let mut extra = Vec::new();
    extra.extend_from_slice(&[0x01, 0x00, 0x18, 0x00]);
    extra.extend_from_slice(&7u64.to_le_bytes());
    extra.extend_from_slice(&5u64.to_le_bytes());
    extra.extend_from_slice(&3u64.to_le_bytes());
    let mut un = 42u64;
    let mut co = 24u64;
    let mut off = 9u64;
    parse_zip64_extra(&extra, &mut un, &mut co, &mut off);
    assert_eq!((un, co, off), (42, 24, 9));
}
