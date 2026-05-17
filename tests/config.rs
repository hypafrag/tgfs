use super::*;

// -------- expand_env --------

#[test]
fn expand_env_no_substitution_when_no_dollar() {
    assert_eq!(expand_env("hello world"), "hello world");
}

#[test]
fn expand_env_braces_substitutes() {
    // SAFETY: tests run single-threaded by default in cargo if needed; even so,
    // env var name is unique enough to avoid clashing with other tests.
    unsafe { std::env::set_var("TGFS_TEST_BRACE", "hello"); }
    assert_eq!(expand_env("${TGFS_TEST_BRACE}!"), "hello!");
    unsafe { std::env::remove_var("TGFS_TEST_BRACE"); }
}

#[test]
fn expand_env_unbraced_substitutes_alphanumeric_run() {
    unsafe { std::env::set_var("TGFS_TEST_BARE", "yes"); }
    assert_eq!(expand_env("a=$TGFS_TEST_BARE,b"), "a=yes,b");
    unsafe { std::env::remove_var("TGFS_TEST_BARE"); }
}

#[test]
fn expand_env_missing_var_is_empty() {
    unsafe { std::env::remove_var("TGFS_TEST_MISSING"); }
    assert_eq!(expand_env("[${TGFS_TEST_MISSING}]"), "[]");
}

#[test]
fn expand_env_dollar_alone_is_kept() {
    // A `$` not followed by a name or brace is passed through verbatim.
    assert_eq!(expand_env("price: $"), "price: $");
}

// -------- LogConfig::to_filter_string --------

#[test]
fn log_config_level_round_trips() {
    let cfg = LogConfig::Level("debug".into());
    assert_eq!(cfg.to_filter_string(), "debug");
}

#[test]
fn log_config_modules_emits_directive_list() {
    let mut m = HashMap::new();
    m.insert("tgfs".to_string(), "debug".to_string());
    let cfg = LogConfig::Modules(m);
    // Single entry so HashMap ordering doesn't matter.
    assert_eq!(cfg.to_filter_string(), "tgfs=debug");
}

// -------- MultipartPolicy default --------

#[test]
fn multipart_policy_default_is_none() {
    assert_eq!(MultipartPolicy::default(), MultipartPolicy::None);
}
