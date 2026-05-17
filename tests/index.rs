use super::*;

// -------- MimePool --------

#[test]
fn mimepool_intern_is_idempotent() {
    let p = MimePool::new();
    let a = p.intern("text/plain");
    let b = p.intern("text/plain");
    assert_eq!(a, b);
}

#[test]
fn mimepool_intern_returns_distinct_indices() {
    let p = MimePool::new();
    let a = p.intern("text/plain");
    let b = p.intern("application/json");
    assert_ne!(a, b);
}

#[test]
fn mimepool_get_roundtrips_interned_string() {
    let p = MimePool::new();
    let idx = p.intern("application/zip");
    assert_eq!(p.get(idx).as_deref(), Some("application/zip"));
}

#[test]
fn mimepool_get_missing_returns_none() {
    let p = MimePool::new();
    assert_eq!(p.get(42), None);
}

// -------- human_size --------

#[test]
fn human_size_below_unit_boundary_keeps_bytes() {
    assert_eq!(human_size(0), "0B");
    assert_eq!(human_size(1023), "1023B");
}

#[test]
fn human_size_crosses_unit_boundaries() {
    assert_eq!(human_size(1024), "1.0K");
    assert_eq!(human_size(1024 * 1024), "1.0M");
    assert_eq!(human_size(1024 * 1024 * 1024), "1.0G");
}

#[test]
fn human_size_one_decimal_place() {
    assert_eq!(human_size(1536), "1.5K"); // 1.5 KiB
}

// -------- fmt_system_time --------

#[test]
fn fmt_system_time_unix_epoch() {
    assert_eq!(fmt_system_time(std::time::UNIX_EPOCH), "1970-01-01 00:00");
}

#[test]
fn fmt_system_time_known_date() {
    // 2026-05-25 12:34 UTC
    let t = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_779_712_440);
    assert_eq!(fmt_system_time(t), "2026-05-25 12:34");
}
