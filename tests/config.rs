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

// -------- dotenv --------

#[test]
fn dotenv_parses_basic_pairs() {
    let m = parse_dotenv("FOO=bar\nBAZ=qux\n");
    assert_eq!(m.get("FOO").map(String::as_str), Some("bar"));
    assert_eq!(m.get("BAZ").map(String::as_str), Some("qux"));
}

#[test]
fn dotenv_strips_quotes_and_comments_and_export_prefix() {
    let m = parse_dotenv(
        "# a comment\n\n\
         export FOO=\"hello world\"\n\
         BAR='one two'\n\
         BAZ=  spacy  \n\
         INVALID_LINE_NO_EQUALS\n",
    );
    assert_eq!(m.get("FOO").map(String::as_str), Some("hello world"));
    assert_eq!(m.get("BAR").map(String::as_str), Some("one two"));
    assert_eq!(m.get("BAZ").map(String::as_str), Some("spacy"));
    assert!(!m.contains_key("INVALID_LINE_NO_EQUALS"));
}

#[test]
fn expand_env_with_prefers_dotenv_over_environment() {
    let mut dotenv = std::collections::HashMap::new();
    dotenv.insert("TGFS_TEST_PREC".to_string(), "from_dotenv".to_string());
    unsafe { std::env::set_var("TGFS_TEST_PREC", "from_env"); }
    assert_eq!(
        expand_env_with("X=${TGFS_TEST_PREC}", &dotenv),
        "X=from_dotenv"
    );
    unsafe { std::env::remove_var("TGFS_TEST_PREC"); }
}

#[test]
fn expand_env_with_falls_back_to_environment_when_dotenv_missing() {
    let dotenv = std::collections::HashMap::new();
    unsafe { std::env::set_var("TGFS_TEST_FALLBACK", "from_env"); }
    assert_eq!(
        expand_env_with("X=${TGFS_TEST_FALLBACK}", &dotenv),
        "X=from_env"
    );
    unsafe { std::env::remove_var("TGFS_TEST_FALLBACK"); }
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
