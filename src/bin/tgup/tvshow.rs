//! `--tvshow` plan builder. Parses input file paths with `hunch`, renames each
//! file to `<Title> S##E##[ - <Episode Title>].<ext>`, sorts by (title, season,
//! episode), and splits each season's episodes into Telegram albums (≤10 per
//! album, balanced as evenly as possible).
//!
//! Hunch sees the **full path relative to the arg** (e.g.
//! `The.Walking.Dead.bdrip_[teko]/Season_01/s01e01_Days.Gone.Bye.avi`), not
//! just the filename, so the show title and episode title can be recovered
//! from directory components when they're missing from the leaf name.

use std::path::{Component, Path, PathBuf};

use anyhow::{anyhow, bail, Context as _};

use super::args::DirMode;
use super::plan::{PartSource, UploadItem, UploadPart, ALBUM_MAX, PART_MAX};

#[derive(Debug)]
struct Episode {
    abs_path: PathBuf,
    size: u64,
    title: String,
    season: i32,
    episode: i32,
    episode_title: Option<String>,
    ext: String,
}

fn parse_episode(abs: PathBuf, size: u64, hunch_input: &str) -> anyhow::Result<Episode> {
    let result = hunch::hunch(hunch_input);
    let title = result
        .title()
        .ok_or_else(|| anyhow!("hunch: could not extract show title from '{}'", hunch_input))?
        .to_string();
    let season = result
        .season()
        .ok_or_else(|| anyhow!("hunch: could not extract season from '{}'", hunch_input))?;
    let episode = result
        .episode()
        .ok_or_else(|| anyhow!("hunch: could not extract episode number from '{}'", hunch_input))?;
    let episode_title = result.episode_title().map(|s| s.to_string());
    let ext = Path::new(hunch_input)
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("file '{}' has no extension", abs.display()))?;
    if ext.is_empty() {
        bail!("file '{}' has an empty extension", abs.display());
    }
    Ok(Episode { abs_path: abs, size, title, season, episode, episode_title, ext })
}

/// Distribute `n` items across `ceil(n / ALBUM_MAX)` albums as evenly as
/// possible. e.g. n=11 → [6,5]; n=13 → [7,6]; n=21 → [7,7,7].
pub fn split_album_sizes(n: usize) -> Vec<usize> {
    if n == 0 { return Vec::new(); }
    let k = (n + ALBUM_MAX - 1) / ALBUM_MAX;
    let base = n / k;
    let extra = n % k;
    (0..k).map(|i| if i < extra { base + 1 } else { base }).collect()
}

fn tv_filename(
    title: &str,
    season: i32,
    episode: i32,
    episode_title: Option<&str>,
    ext: &str,
) -> String {
    match episode_title {
        Some(et) if !et.is_empty() => format!("{} S{:02}E{:02} - {}.{}", title, season, episode, et, ext),
        _ => format!("{} S{:02}E{:02}.{}", title, season, episode, ext),
    }
}

fn album_caption(title: &str, season: i32, episodes: &[i32]) -> String {
    if episodes.len() == 1 {
        format!("{} S{:02}E{:02}", title, season, episodes[0])
    } else {
        let first = *episodes.first().unwrap();
        let last = *episodes.last().unwrap();
        format!("{} S{:02} E{:02}-E{:02}", title, season, first, last)
    }
}

/// Join the components of `under` with forward slashes — hunch was designed
/// against Unix-style paths, so emit `/` regardless of platform separator.
fn rel_to_slash(under: &Path) -> String {
    under.components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Each entry: `(abs_path, size, hunch_input)`. `hunch_input` is the full
/// path relative to the user-supplied arg, with the arg's own basename as
/// the first component — that's what lets hunch see ancestor directory
/// names like `The.Walking.Dead.bdrip_[teko]` when the leaf filename
/// doesn't carry the show title.
fn collect_files(
    arg: &Path,
    dir_mode: DirMode,
    out: &mut Vec<(PathBuf, u64, String)>,
) -> anyhow::Result<()> {
    let meta = std::fs::metadata(arg)
        .with_context(|| format!("can't stat '{}'", arg.display()))?;
    if meta.is_file() {
        let abs = arg.canonicalize()
            .with_context(|| format!("can't canonicalize '{}'", arg.display()))?;
        let rel = abs.file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("filename of '{}' is not valid UTF-8", arg.display()))?
            .to_string();
        out.push((abs, meta.len(), rel));
        return Ok(());
    }
    if meta.is_dir() {
        if dir_mode == DirMode::Skip {
            bail!(
                "directory '{}' given but --dir mode is `skip`; pass -d recursive",
                arg.display()
            );
        }
        let arg_canon = arg.canonicalize()
            .with_context(|| format!("can't canonicalize '{}'", arg.display()))?;
        let arg_root_name = arg_canon.file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!(
                "can't determine a basename for arg '{}'", arg.display()
            ))?
            .to_string();
        let mut stack: Vec<PathBuf> = vec![arg_canon.clone()];
        while let Some(d) = stack.pop() {
            let entries = std::fs::read_dir(&d)
                .with_context(|| format!("can't read directory '{}'", d.display()))?;
            for e in entries {
                let e = e?;
                let p = e.path();
                let m = e.metadata()?;
                if m.is_dir() {
                    stack.push(p);
                } else if m.is_file() {
                    let under = p.strip_prefix(&arg_canon)
                        .with_context(|| format!(
                            "walked path '{}' is not under arg root '{}'",
                            p.display(), arg_canon.display(),
                        ))?;
                    let rel_under = rel_to_slash(under);
                    let rel = if rel_under.is_empty() {
                        arg_root_name.clone()
                    } else {
                        format!("{}/{}", arg_root_name, rel_under)
                    };
                    out.push((p, m.len(), rel));
                }
            }
        }
        return Ok(());
    }
    bail!("'{}' is neither a file nor a directory", arg.display())
}

/// Walk `paths`, parse every file via `hunch`, group per (title, season), and
/// emit `UploadItem`s. Multi-episode seasons become `FileAlbum`s split across
/// chunks of ≤ALBUM_MAX as evenly as possible; single-episode seasons become a
/// lone `Single`.
pub fn build_tvshow_plan(paths: &[PathBuf], dir_mode: DirMode) -> anyhow::Result<Vec<UploadItem>> {
    let mut files: Vec<(PathBuf, u64, String)> = Vec::new();
    for p in paths {
        collect_files(p, dir_mode, &mut files)?;
    }
    if files.is_empty() { return Ok(Vec::new()); }

    let mut episodes: Vec<Episode> = Vec::with_capacity(files.len());
    for (abs, size, rel) in files {
        if size > PART_MAX {
            bail!(
                "file '{}' is {} bytes (> 4 GiB); --tvshow does not support multipart files",
                abs.display(), size
            );
        }
        episodes.push(parse_episode(abs, size, &rel)?);
    }
    Ok(assemble_plan(episodes))
}

/// Pure plan-assembly: sort episodes by (title, season, episode) and partition
/// each season-run into albums. Split from the filesystem walk so the
/// grouping/splitting logic can be unit-tested without touching disk.
fn assemble_plan(mut episodes: Vec<Episode>) -> Vec<UploadItem> {
    episodes.sort_by(|a, b| {
        a.title.cmp(&b.title)
            .then(a.season.cmp(&b.season))
            .then(a.episode.cmp(&b.episode))
    });

    let mut out: Vec<UploadItem> = Vec::new();
    let mut i = 0;
    while i < episodes.len() {
        let title = episodes[i].title.clone();
        let season = episodes[i].season;
        let mut j = i + 1;
        while j < episodes.len()
            && episodes[j].title == title
            && episodes[j].season == season
        {
            j += 1;
        }
        let season_eps = &episodes[i..j];
        let sizes = split_album_sizes(season_eps.len());

        let mut cursor = 0;
        for chunk_size in sizes {
            let chunk = &season_eps[cursor..cursor + chunk_size];
            cursor += chunk_size;

            let ep_nums: Vec<i32> = chunk.iter().map(|e| e.episode).collect();
            let caption = album_caption(&title, season, &ep_nums);

            let parts: Vec<UploadPart> = chunk
                .iter()
                .enumerate()
                .map(|(k, e)| UploadPart {
                    src: PartSource::File(e.abs_path.clone()),
                    offset: 0,
                    size: e.size,
                    doc_filename: tv_filename(
                        &e.title, e.season, e.episode, e.episode_title.as_deref(), &e.ext,
                    ),
                    // Caption only on the first part — Telegram surfaces one
                    // caption per album, and the indexer's group-caption
                    // extractor picks up any non-empty one.
                    caption: if k == 0 { caption.clone() } else { String::new() },
                })
                .collect();

            if parts.len() == 1 {
                out.push(UploadItem::Single(parts.into_iter().next().unwrap()));
            } else {
                out.push(UploadItem::FileAlbum { parts });
            }
        }

        i = j;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn ep(title: &str, season: i32, episode: i32, ext: &str) -> Episode {
        ep_with_title(title, season, episode, None, ext)
    }

    fn ep_with_title(
        title: &str,
        season: i32,
        episode: i32,
        episode_title: Option<&str>,
        ext: &str,
    ) -> Episode {
        Episode {
            // Path content is irrelevant for assemble_plan tests — it is just
            // forwarded into PartSource::File. Use a synthetic, unique-ish path.
            abs_path: PathBuf::from(format!(
                "/synthetic/{}.S{:02}E{:02}.{}",
                title.replace(' ', "."), season, episode, ext
            )),
            size: 100,
            title: title.to_string(),
            season,
            episode,
            episode_title: episode_title.map(|s| s.to_string()),
            ext: ext.to_string(),
        }
    }

    fn names(item: &UploadItem) -> Vec<String> {
        match item {
            UploadItem::Single(p) => vec![p.doc_filename.clone()],
            UploadItem::FileAlbum { parts } => parts.iter().map(|p| p.doc_filename.clone()).collect(),
            _ => panic!("unexpected UploadItem variant in tvshow plan"),
        }
    }

    fn captions(item: &UploadItem) -> Vec<String> {
        match item {
            UploadItem::Single(p) => vec![p.caption.clone()],
            UploadItem::FileAlbum { parts } => parts.iter().map(|p| p.caption.clone()).collect(),
            _ => panic!("unexpected UploadItem variant in tvshow plan"),
        }
    }

    #[test]
    fn split_balances_evenly() {
        assert_eq!(split_album_sizes(0), Vec::<usize>::new());
        assert_eq!(split_album_sizes(1), vec![1]);
        assert_eq!(split_album_sizes(10), vec![10]);
        assert_eq!(split_album_sizes(11), vec![6, 5]);
        assert_eq!(split_album_sizes(13), vec![7, 6]);
        assert_eq!(split_album_sizes(20), vec![10, 10]);
        assert_eq!(split_album_sizes(21), vec![7, 7, 7]);
        assert_eq!(split_album_sizes(23), vec![8, 8, 7]);
    }

    #[test]
    fn tv_filename_formats_pad() {
        assert_eq!(tv_filename("Show", 1, 2, None, "mkv"), "Show S01E02.mkv");
        assert_eq!(tv_filename("Show", 12, 134, None, "mp4"), "Show S12E134.mp4");
    }

    #[test]
    fn tv_filename_appends_episode_title_when_present() {
        assert_eq!(
            tv_filename("The Walking Dead", 1, 1, Some("Days Gone Bye"), "avi"),
            "The Walking Dead S01E01 - Days Gone Bye.avi",
        );
        // Empty episode_title collapses back to the bare form.
        assert_eq!(
            tv_filename("Show", 1, 2, Some(""), "mkv"),
            "Show S01E02.mkv",
        );
    }

    #[test]
    fn album_caption_range() {
        assert_eq!(album_caption("Show", 5, &[3]), "Show S05E03");
        assert_eq!(album_caption("Show", 5, &[1, 2, 3]), "Show S05 E01-E03");
    }

    #[test]
    fn assemble_empty_yields_empty_plan() {
        assert!(assemble_plan(Vec::new()).is_empty());
    }

    #[test]
    fn assemble_single_episode_becomes_single() {
        let plan = assemble_plan(vec![ep("Show", 1, 1, "mkv")]);
        assert_eq!(plan.len(), 1);
        assert!(matches!(plan[0], UploadItem::Single(_)));
        assert_eq!(names(&plan[0]), vec!["Show S01E01.mkv"]);
        assert_eq!(captions(&plan[0]), vec!["Show S01E01"]);
    }

    #[test]
    fn assemble_one_full_season_one_album() {
        let plan = assemble_plan((1..=5).map(|n| ep("Show", 2, n, "mp4")).collect());
        assert_eq!(plan.len(), 1);
        assert!(matches!(plan[0], UploadItem::FileAlbum { .. }));
        assert_eq!(
            names(&plan[0]),
            (1..=5).map(|n| format!("Show S02E{:02}.mp4", n)).collect::<Vec<_>>(),
        );
        // Only the first part carries the album caption; the rest are blank.
        let caps = captions(&plan[0]);
        assert_eq!(caps[0], "Show S02 E01-E05");
        assert!(caps[1..].iter().all(|c| c.is_empty()));
    }

    #[test]
    fn assemble_eleven_episodes_split_six_then_five() {
        let plan = assemble_plan((1..=11).map(|n| ep("Show", 1, n, "mkv")).collect());
        assert_eq!(plan.len(), 2);
        assert_eq!(names(&plan[0]).len(), 6);
        assert_eq!(names(&plan[1]).len(), 5);
        assert_eq!(captions(&plan[0])[0], "Show S01 E01-E06");
        assert_eq!(captions(&plan[1])[0], "Show S01 E07-E11");
    }

    #[test]
    fn assemble_twenty_one_episodes_split_three_sevens() {
        let plan = assemble_plan((1..=21).map(|n| ep("Show", 1, n, "mkv")).collect());
        assert_eq!(plan.len(), 3);
        for item in &plan { assert_eq!(names(item).len(), 7); }
        assert_eq!(captions(&plan[0])[0], "Show S01 E01-E07");
        assert_eq!(captions(&plan[1])[0], "Show S01 E08-E14");
        assert_eq!(captions(&plan[2])[0], "Show S01 E15-E21");
    }

    #[test]
    fn assemble_sorts_and_separates_seasons_and_titles() {
        // Deliberately scrambled input across two shows and two seasons.
        let plan = assemble_plan(vec![
            ep("Bravo", 1, 2, "mkv"),
            ep("Alpha", 2, 1, "mkv"),
            ep("Alpha", 1, 3, "mkv"),
            ep("Alpha", 1, 1, "mkv"),
            ep("Bravo", 1, 1, "mkv"),
            ep("Alpha", 1, 2, "mkv"),
        ]);
        // Expected ordering after sort: Alpha S1 (3 eps), Alpha S2 (1 ep), Bravo S1 (2 eps).
        assert_eq!(plan.len(), 3);
        assert!(matches!(plan[0], UploadItem::FileAlbum { .. }));
        assert_eq!(captions(&plan[0])[0], "Alpha S01 E01-E03");
        assert!(matches!(plan[1], UploadItem::Single(_)));
        assert_eq!(names(&plan[1]), vec!["Alpha S02E01.mkv"]);
        assert!(matches!(plan[2], UploadItem::FileAlbum { .. }));
        assert_eq!(captions(&plan[2])[0], "Bravo S01 E01-E02");
    }

    #[test]
    fn assemble_preserves_extension_per_file() {
        let plan = assemble_plan(vec![
            ep("Show", 1, 1, "mkv"),
            ep("Show", 1, 2, "mp4"),
        ]);
        assert_eq!(plan.len(), 1);
        assert_eq!(names(&plan[0]), vec!["Show S01E01.mkv", "Show S01E02.mp4"]);
    }

    #[test]
    fn parse_episode_extracts_title_season_episode() {
        let abs = PathBuf::from("/x/Breaking.Bad.S05E03.1080p.BluRay.x264-DEMAND.mkv");
        let e = parse_episode(
            abs, 1234, "Breaking.Bad.S05E03.1080p.BluRay.x264-DEMAND.mkv",
        ).unwrap();
        assert_eq!(e.title, "Breaking Bad");
        assert_eq!(e.season, 5);
        assert_eq!(e.episode, 3);
        assert_eq!(e.ext, "mkv");
        assert_eq!(e.size, 1234);
    }

    #[test]
    fn parse_episode_uses_full_rel_path_to_recover_title_and_ep_title() {
        // Show title lives only in the directory component; episode title in
        // the leaf. The combined relative path is what makes both reachable.
        let abs = PathBuf::from("/x/y/s01e01_Days.Gone.Bye.avi");
        let rel = "The.Walking.Dead.bdrip_[teko]/Season_01/s01e01_Days.Gone.Bye.avi";
        let e = parse_episode(abs, 100, rel).unwrap();
        assert_eq!(e.title, "The Walking Dead");
        assert_eq!(e.season, 1);
        assert_eq!(e.episode, 1);
        assert_eq!(e.episode_title.as_deref(), Some("Days Gone Bye"));
        assert_eq!(e.ext, "avi");
    }

    #[test]
    fn parse_episode_rejects_unparseable_name() {
        let abs = PathBuf::from("/x/random-document.txt");
        assert!(parse_episode(abs, 100, "random-document.txt").is_err());
    }

    #[test]
    fn parse_episode_rejects_empty_extension() {
        // Trailing dot makes `Path::extension()` return `Some("")`.
        let abs = PathBuf::from("/x/Breaking.Bad.S01E01.");
        let err = parse_episode(abs, 100, "Breaking.Bad.S01E01.").unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("empty extension"), "got: {}", msg);
    }

    // ---- filesystem-touching end-to-end tests --------------------------------

    static UNIQ: AtomicU32 = AtomicU32::new(0);

    fn tempdir(label: &str) -> PathBuf {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let n = UNIQ.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("tgup-tvshow-{}-{}-{}", label, nanos, n));
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn touch(dir: &Path, name: &str) -> PathBuf {
        let p = dir.join(name);
        fs::write(&p, b"x").unwrap();
        p
    }

    #[test]
    fn build_plan_end_to_end_groups_by_season() {
        let dir = tempdir("e2e-groups");
        let mut paths = Vec::new();
        for n in 1..=11 {
            paths.push(touch(&dir, &format!("Breaking.Bad.S05E{:02}.1080p.x264.mkv", n)));
        }
        for n in 1..=3 {
            paths.push(touch(&dir, &format!("Breaking.Bad.S04E{:02}.720p.x264.mkv", n)));
        }
        paths.push(touch(&dir, "The.Walking.Dead.S01E01.720p.x264.mkv"));

        let plan = build_tvshow_plan(&paths, DirMode::Skip).unwrap();
        // S04 (1 album of 3) + S05 (split 6+5 = 2 albums) + TWD S01 (1 Single).
        assert_eq!(plan.len(), 4);
        assert_eq!(captions(&plan[0])[0], "Breaking Bad S04 E01-E03");
        assert_eq!(captions(&plan[1])[0], "Breaking Bad S05 E01-E06");
        assert_eq!(captions(&plan[2])[0], "Breaking Bad S05 E07-E11");
        assert!(matches!(plan[3], UploadItem::Single(_)));
        assert_eq!(names(&plan[3]), vec!["The Walking Dead S01E01.mkv"]);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn build_plan_walks_directory_with_dir_recursive() {
        let dir = tempdir("e2e-recursive");
        let nested = dir.join("season-5");
        fs::create_dir(&nested).unwrap();
        for n in 1..=3 {
            touch(&nested, &format!("Breaking.Bad.S05E{:02}.x264.mkv", n));
        }

        let plan = build_tvshow_plan(&[dir.clone()], DirMode::Recursive).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(names(&plan[0]).len(), 3);
        assert_eq!(captions(&plan[0])[0], "Breaking Bad S05 E01-E03");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn build_plan_rejects_directory_under_skip() {
        let dir = tempdir("e2e-skip");
        touch(&dir, "Breaking.Bad.S01E01.mkv");
        let err = match build_tvshow_plan(&[dir.clone()], DirMode::Skip) {
            Err(e) => e,
            Ok(_) => panic!("expected error for directory under DirMode::Skip"),
        };
        let msg = format!("{:#}", err);
        assert!(msg.contains("--dir mode is `skip`"), "got: {}", msg);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn build_plan_empty_input_yields_empty_plan() {
        let plan = build_tvshow_plan(&[], DirMode::Skip).unwrap();
        assert!(plan.is_empty());
    }
}
