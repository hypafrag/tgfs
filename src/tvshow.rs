//! `tvshow_pattern` channel option support.
//!
//! Given a channel-level format string (e.g.
//! `{show_title}/Season {season}/Episode {episode}.{ext}`) and a raw file
//! name, parse the name via `hunch` and substitute each `{key}` placeholder.
//! If every referenced placeholder resolves, the file is rerouted to the
//! rendered virtual path; otherwise it stays where it was.
//!
//! Supported placeholders:
//!   * `{show_title}`     — episode title from hunch
//!   * `{season}` / `{season:0N}`   — season number, optionally zero-padded
//!   * `{episode}` / `{episode:0N}` — episode number, optionally zero-padded
//!   * `{year}` / `{year:0N}`       — year if hunch detected one
//!   * `{ext}`            — original file extension, lowercased

use std::path::PathBuf;

struct EpisodeSubst {
    title: Option<String>,
    season: Option<i32>,
    episode: Option<i32>,
    year: Option<i32>,
    ext: String,
}

fn parse_for_pattern(name: &str) -> EpisodeSubst {
    let r = hunch::hunch(name);
    let ext = std::path::Path::new(name)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();
    EpisodeSubst {
        title: r.title().map(|s| s.to_string()),
        season: r.season(),
        episode: r.episode(),
        year: r.year(),
        ext,
    }
}

/// Render `pattern` against `subst`. Returns `Some(rendered)` only when every
/// referenced placeholder has a value (and the format spec, if any, is well-
/// formed). Returns `None` if any placeholder can't be resolved — the caller
/// should then leave the original path untouched.
fn render_pattern(pattern: &str, subst: &EpisodeSubst) -> Option<String> {
    let mut out = String::with_capacity(pattern.len());
    let mut rest = pattern;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let close = after.find('}')?;
        let token = &after[..close];
        let (key, spec) = match token.split_once(':') {
            Some((k, s)) => (k.trim(), Some(s.trim())),
            None => (token.trim(), None),
        };
        let val = match key {
            "show_title" => subst.title.clone()?,
            "season" => format_int(subst.season?, spec)?,
            "episode" => format_int(subst.episode?, spec)?,
            "year" => format_int(subst.year?, spec)?,
            "ext" => {
                if subst.ext.is_empty() { return None; }
                subst.ext.clone()
            }
            _ => return None,
        };
        out.push_str(&val);
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    Some(out)
}

fn format_int(n: i32, spec: Option<&str>) -> Option<String> {
    match spec {
        None => Some(n.to_string()),
        Some(s) => {
            // Only `0N` (zero-pad to width N) is supported.
            let width = s.strip_prefix('0')?.parse::<usize>().ok()?;
            Some(format!("{:0w$}", n, w = width))
        }
    }
}

/// Compute a new `(name, path)` pair for `original_name` according to
/// `pattern`. Returns `None` when hunch can't fill every placeholder, in
/// which case the caller must leave the file's current location alone.
pub fn rewrite_name_and_path(
    pattern: &str,
    original_name: &str,
) -> Option<(String, Option<PathBuf>)> {
    let subst = parse_for_pattern(original_name);
    let rendered = render_pattern(pattern, &subst)?;
    let (dir, file) = match rendered.rfind('/') {
        Some(i) => (Some(rendered[..i].to_string()), rendered[i + 1..].to_string()),
        None => (None, rendered),
    };
    if file.trim().is_empty() { return None; }
    Some((file, dir.filter(|d| !d.is_empty()).map(PathBuf::from)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PATTERN: &str = "{show_title}/Season {season}/Episode {episode}.{ext}";

    #[test]
    fn rewrites_typical_episode() {
        let (name, path) = rewrite_name_and_path(
            PATTERN,
            "Breaking.Bad.S05E03.1080p.BluRay.x264-DEMAND.mkv",
        ).unwrap();
        assert_eq!(name, "Episode 3.mkv");
        assert_eq!(path.unwrap().to_string_lossy(), "Breaking Bad/Season 5");
    }

    #[test]
    fn supports_zero_padding() {
        let pat = "{show_title} S{season:02}E{episode:02}.{ext}";
        let (name, path) = rewrite_name_and_path(
            pat,
            "Breaking.Bad.S05E03.mkv",
        ).unwrap();
        assert_eq!(name, "Breaking Bad S05E03.mkv");
        assert!(path.is_none());
    }

    #[test]
    fn returns_none_when_placeholder_missing() {
        // A movie filename has no season/episode → pattern can't render.
        assert!(rewrite_name_and_path(PATTERN, "Inception.2010.1080p.mkv").is_none());
    }

    #[test]
    fn returns_none_on_unknown_placeholder() {
        let pat = "{show_title}/{whatever}.{ext}";
        assert!(rewrite_name_and_path(pat, "Breaking.Bad.S01E01.mkv").is_none());
    }

    #[test]
    fn unmatched_brace_is_treated_as_failure() {
        // No closing `}` after `{show_title` → can't parse; return None
        // rather than silently truncating.
        assert!(rewrite_name_and_path("{show_title", "Breaking.Bad.S01E01.mkv").is_none());
    }

    #[test]
    fn preserves_literal_text_around_placeholders() {
        let pat = "shows/[{show_title}] S{season}E{episode}.{ext}";
        let (name, path) = rewrite_name_and_path(pat, "Breaking.Bad.S01E02.mkv").unwrap();
        assert_eq!(path.unwrap().to_string_lossy(), "shows");
        assert_eq!(name, "[Breaking Bad] S1E2.mkv");
    }
}
