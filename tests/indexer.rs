use super::*;

// -------- multipart suffix detection --------

#[test]
fn split_part_suffix_basic_two_digit() {
    assert_eq!(split_part_suffix("archive.zip.00"), Some(("archive.zip", 0)));
    assert_eq!(split_part_suffix("archive.zip.99"), Some(("archive.zip", 99)));
}

#[test]
fn split_part_suffix_requires_exactly_two_digits() {
    // The spec is two-digit `.NN` parts; anything looser would treat
    // ordinary numeric extensions (versions, indices) as multipart parts.
    assert_eq!(split_part_suffix("notes.0"), None);
    assert_eq!(split_part_suffix("dump.bin.123"), None);
    assert_eq!(split_part_suffix("v1.5"), None);
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

// -------- bug demonstrations (these assert the CORRECT behavior and fail
// until the corresponding bug is fixed) --------

/// Offline `Client` — never connects; enough to satisfy the `&Client`
/// parameter of assembly functions on paths that don't perform RPCs.
fn offline_client() -> Client {
    let session = std::sync::Arc::new(grammers_session::storages::MemorySession::default());
    let pool = grammers_mtsender::SenderPool::new(session, 1);
    Client::new(pool.handle)
}

/// A `FileEntry` whose single part is a synthetic Telegram document carrying
/// a real filename attribute, so `doc_name()` resolves like production data.
fn fe_doc(doc_name: &str, path: Option<&str>, size: usize, msg_id: i32) -> FileEntry {
    let doc = Document::from_raw_media(tl::types::MessageMediaDocument {
        nopremium: false,
        spoiler: false,
        video: false,
        round: false,
        voice: false,
        document: Some(tl::enums::Document::Document(tl::types::Document {
            id: msg_id as i64,
            access_hash: 0,
            file_reference: Vec::new(),
            date: 0,
            mime_type: "application/octet-stream".to_string(),
            size: size as i64,
            thumbs: None,
            video_thumbs: None,
            dc_id: 2,
            attributes: vec![tl::enums::DocumentAttribute::Filename(
                tl::types::DocumentAttributeFilename { file_name: doc_name.to_string() },
            )],
        })),
        alt_documents: None,
        video_cover: None,
        video_timestamp: None,
        ttl_seconds: None,
    });
    FileEntry {
        name: doc_name.to_string(),
        path: path.map(std::path::PathBuf::from),
        parts: smallvec![Media::Document(doc)],
        msg_ids: smallvec![msg_id],
        size: Some(size),
        mime_idx: 0,
        archive_entries: None,
        file_type: FileType::File,
        mtime: None,
    }
}

fn test_zip_cache() -> Mutex<ZipCache> {
    Mutex::new(ZipCache::load("/nonexistent/tgfs_test_zip_cache.json"))
}

/// The docs specify multipart parts as two-digit `.NN` suffixes. But
/// `split_part_suffix` accepts ANY all-digit suffix, and the assembler only
/// checks contiguity from 0 — so two ordinary, unrelated files that happen to
/// be named `notes.0` and `notes.1` are silently fused into one bogus
/// concatenated file called `notes`.
#[tokio::test]
async fn single_digit_numeric_extensions_are_not_multipart() {
    let client = offline_client();
    let zip_cache = test_zip_cache();
    let files = vec![
        fe_doc("notes.0", None, 5, 1),
        fe_doc("notes.1", None, 5, 2),
    ];
    let mut removed = std::collections::BTreeSet::new();
    let mut new_files: Vec<FileEntry> = Vec::new();
    assemble_suffix_multipart(&files, ArchiveView::File, &client, &zip_cache, &mut removed, &mut new_files).await;
    assert!(
        new_files.is_empty() && removed.is_empty(),
        "unrelated files 'notes.0' and 'notes.1' were merged into a single \
         multipart file {:?}",
        new_files.first().map(|f| &f.name)
    );
}

/// Suffix-multipart grouping buckets by document-name base only, ignoring the
/// entries' virtual directories. The saved-messages assembler guards against
/// this (it requires all parts to share the same `path`); the channel
/// assembler does not — so a part set under `A/` gets fused with a
/// same-named part under `B/` into one broken concatenation.
#[tokio::test]
async fn suffix_multipart_does_not_merge_across_virtual_directories() {
    let client = offline_client();
    let zip_cache = test_zip_cache();
    let files = vec![
        fe_doc("data.bin.00", Some("A"), 10, 1),
        fe_doc("data.bin.01", Some("A"), 10, 2),
        fe_doc("data.bin.02", Some("B"), 10, 3),
    ];
    let mut removed = std::collections::BTreeSet::new();
    let mut new_files: Vec<FileEntry> = Vec::new();
    assemble_suffix_multipart(&files, ArchiveView::File, &client, &zip_cache, &mut removed, &mut new_files).await;
    for f in &new_files {
        let from_a = f.msg_ids.iter().any(|id| *id == 1 || *id == 2);
        let from_b = f.msg_ids.iter().any(|id| *id == 3);
        assert!(
            !(from_a && from_b),
            "'{}' concatenates parts from virtual directory A/ (msgs 1,2) \
             with a part from B/ (msg 3)",
            f.name
        );
    }
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
