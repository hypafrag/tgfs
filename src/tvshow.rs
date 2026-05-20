//! `tvshow_pattern` channel option support.
//!
//! Given a channel-level format string (e.g.
//! `{show_title}/Season {season:02}/Episode {episode:02} - {episode_title}.{ext}`)
//! and a raw file name, parse the name via `hunch` and substitute each
//! `{key}` placeholder. If every referenced placeholder resolves, the file
//! is rerouted to the rendered virtual path; otherwise it stays where it
//! was.
//!
//! Supported placeholders:
//!   * `{show_title}`     — show title from hunch
//!   * `{episode_title}`  — episode title from hunch (optional — see fallback chain)
//!   * `{season}` / `{season:N}` / `{season:0N}`     — season number (raw, fixed-width, or zero-padded)
//!   * `{episode}` / `{episode:N}` / `{episode:0N}`  — episode number, same forms
//!   * `{year}` / `{year:N}` / `{year:0N}`           — year if hunch detected one
//!   * `{ext}`            — original file extension, lowercased
//!
//! ## Format specs
//!
//! Integer placeholders accept an optional width spec after `:`:
//!   * `:0N` — fixed width N, zero-padded (e.g. `:02` makes 5 → `"05"`)
//!   * `:N`  — width is parsed (for validation/documentation) but no
//!             padding is applied; output is just the decimal form
//!             (`:2` makes 5 → `"5"`)
//!
//! ## Fallback chain
//!
//! A pattern string may contain multiple sub-patterns separated by `|`. The
//! sub-patterns are tried left-to-right; the first one that fully resolves
//! is used. Today only `{episode_title}` is optional, so the common shape
//! is "with episode title | without":
//!
//! ```text
//! {show_title}/Season {season:02}/Episode {episode:02} - {episode_title}.{ext}|{show_title}/Season {season:02}/Episode {episode:02}.{ext}
//! ```
//!
//! If hunch can't recover `episode_title`, the left sub-pattern fails and
//! the renderer falls through to the right one.

use std::path::PathBuf;

struct EpisodeSubst {
    title: Option<String>,
    season: Option<i32>,
    episode: Option<i32>,
    episode_title: Option<String>,
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
        episode_title: r.episode_title().map(|s| s.to_string()),
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
            "episode_title" => subst.episode_title.clone()?,
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
        Some(s) if s.is_empty() => Some(n.to_string()),
        Some(s) => {
            if let Some(rest) = s.strip_prefix('0') {
                // `:0N` → zero-pad to width N.
                let width = rest.parse::<usize>().ok()?;
                Some(format!("{:0w$}", n, w = width))
            } else {
                // `:N` → width is parsed for validation but no padding is
                // applied; output is just the decimal form. Lets callers
                // signal intent ("this number is normally two digits")
                // without forcing leading zeros.
                let _width = s.parse::<usize>().ok()?;
                Some(n.to_string())
            }
        }
    }
}

/// Compute a new `(name, path)` pair for `original_name` according to
/// `pattern`. The pattern may be a `|`-separated chain — the first
/// sub-pattern whose placeholders all resolve wins. Returns `None` when
/// none of the sub-patterns render fully, in which case the caller must
/// leave the file's current location alone.
pub fn rewrite_name_and_path(
    pattern: &str,
    original_name: &str,
) -> Option<(String, Option<PathBuf>)> {
    let subst = parse_for_pattern(original_name);
    for sub in pattern.split('|') {
        let Some(rendered) = render_pattern(sub, &subst) else { continue };
        let (dir, file) = match rendered.rfind('/') {
            Some(i) => (Some(rendered[..i].to_string()), rendered[i + 1..].to_string()),
            None => (None, rendered),
        };
        if file.trim().is_empty() { continue; }
        return Some((file, dir.filter(|d| !d.is_empty()).map(PathBuf::from)));
    }
    None
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

    #[test]
    fn width_spec_without_leading_zero_does_not_pad() {
        // `:2` parses but emits the raw decimal — no zero-pad, no space pad.
        let pat = "{show_title} S{season:2}E{episode:2}.{ext}";
        let (name, _) = rewrite_name_and_path(pat, "Breaking.Bad.S05E03.mkv").unwrap();
        assert_eq!(name, "Breaking Bad S5E3.mkv");
        // Two-digit numbers are unchanged by either spec.
        let (name, _) = rewrite_name_and_path(pat, "Breaking.Bad.S12E15.mkv").unwrap();
        assert_eq!(name, "Breaking Bad S12E15.mkv");
    }

    #[test]
    fn episode_title_placeholder_renders_when_present() {
        let pat = "{show_title}/Season {season:02}/Episode {episode:02} - {episode_title}.{ext}";
        let (name, path) = rewrite_name_and_path(
            pat,
            "The.Walking.Dead.bdrip_[teko]/Season_01/s01e01_Days.Gone.Bye.avi",
        ).unwrap();
        assert_eq!(name, "Episode 01 - Days Gone Bye.avi");
        assert_eq!(
            path.unwrap().to_string_lossy(),
            "The Walking Dead/Season 01",
        );
    }

    #[test]
    fn fallback_chain_picks_first_pattern_that_resolves() {
        // Two-pattern chain: prefer the form with episode_title, fall back to
        // the bare form when hunch can't recover one (the `TS-19` case).
        let pat = "{show_title}/Season {season:02}/Episode {episode:02} - \
                   {episode_title}.{ext}|\
                   {show_title}/Season {season:02}/Episode {episode:02}.{ext}";

        // Hunch returns episode_title="Days Gone Bye" → first sub-pattern wins.
        let (name, path) = rewrite_name_and_path(
            pat,
            "The.Walking.Dead.bdrip_[teko]/Season_01/s01e01_Days.Gone.Bye.avi",
        ).unwrap();
        assert_eq!(name, "Episode 01 - Days Gone Bye.avi");
        assert_eq!(path.unwrap().to_string_lossy(), "The Walking Dead/Season 01");

        // Hunch can't extract an episode title from `s01e06_TS-19.avi` →
        // first sub-pattern fails; second one is used.
        let (name, path) = rewrite_name_and_path(
            pat,
            "The.Walking.Dead.bdrip_[teko]/Season_01/s01e06_TS-19.avi",
        ).unwrap();
        assert_eq!(name, "Episode 06.avi");
        assert_eq!(path.unwrap().to_string_lossy(), "The Walking Dead/Season 01");
    }

    #[test]
    fn fallback_chain_returns_none_when_every_subpattern_fails() {
        // Each sub-pattern requires hunch data that a plain-document name lacks.
        let pat = "{show_title}/{episode_title}.{ext}|{show_title}/S{season:02}.{ext}";
        assert!(rewrite_name_and_path(pat, "random-document.txt").is_none());
    }
}
