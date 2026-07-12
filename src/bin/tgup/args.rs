//! CLI argument parsing and default config-path helpers for `tgup`.

use std::path::PathBuf;

use anyhow::{anyhow, bail};

use super::plan::AlbumSplitMode;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DirMode {
    Skip,
    Recursive,
    Caption,
    Zip,
}

pub struct Args {
    /// Explicit `--config <path>`. When `None` we fall back to
    /// `~/.config/tgfs/tgfs.yml`.
    pub config_path: Option<String>,
    /// `None` when the user omitted `-c/--channel`; in that case `tgup`
    /// connects to Telegram and shows an interactive picker.
    pub channel: Option<String>,
    pub dir_mode: DirMode,
    pub dry_run: bool,
    pub encode_video: bool,
    /// `None` when `-a/--album` was omitted; `Some(mode)` selects how runs
    /// longer than `ALBUM_MAX` are split into multiple albums.
    pub album: Option<AlbumSplitMode>,
    pub tvshow: bool,
    pub paths: Vec<PathBuf>,
    /// `--test-thumbnails <dir>`: write thumbnail candidates to this directory
    /// and exit without connecting to Telegram.
    pub test_thumbnails: Option<PathBuf>,
}

/// Path to `~/.config/tgfs/`. Falls back to the current directory when `HOME`
/// isn't set (unusual environments / containers).
fn default_config_dir() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        let mut p = PathBuf::from(home);
        p.push(".config");
        p.push("tgfs");
        return p;
    }
    PathBuf::from(".")
}

pub fn default_config_path() -> PathBuf {
    default_config_dir().join("tgfs.yml")
}

pub fn default_session_path() -> PathBuf {
    default_config_dir().join("session.sqlite3")
}

fn parse_dir_mode(s: &str) -> anyhow::Result<DirMode> {
    match s {
        "skip" => Ok(DirMode::Skip),
        "recursive" => Ok(DirMode::Recursive),
        "caption" => Ok(DirMode::Caption),
        "zip" => Ok(DirMode::Zip),
        _ => Err(anyhow!("invalid --dir mode '{}'; expected skip|recursive|caption|zip", s)),
    }
}

fn parse_album_mode(s: &str) -> anyhow::Result<AlbumSplitMode> {
    match s {
        "fill" => Ok(AlbumSplitMode::Fill),
        "even" => Ok(AlbumSplitMode::Even),
        _ => Err(anyhow!("invalid --album mode '{}'; expected fill|even", s)),
    }
}

fn print_usage() {
    eprintln!(
        "tgup — upload files to a tgfs-indexed Telegram channel\n\n\
         Usage:\n  \
           tgup [--config <path>] [-c <channel>] [-d <mode>] [-a <mode>] [--encode-video] [--dry-run] <path>...\n\n\
         Options:\n  \
           -c, --channel <name>   Target channel/group/chat name. When omitted,\n                          \
             tgup connects to Telegram and shows an interactive\n                          \
             picker; channels declared in the config file are\n                          \
             listed at the top, followed by every other dialog\n                          \
             this account can send to.\n  \
           -d, --dir <mode>       How to handle directory arguments:\n                            \
             skip       — error on directories (default)\n                            \
             recursive  — upload contained files as a flat list\n                            \
             caption    — like recursive, but each file's caption sets\n                                         \
             `path: <relative dir>/` so the tree is recreated\n                            \
             zip        — not implemented (exits with error)\n  \
           -a, --album <mode>     Group consecutive uploadable files into Telegram\n                          \
             albums (up to 10 items each). Only files sharing the\n                          \
             same caption are grouped together; multipart items\n                          \
             are passed through unchanged. Compatible with\n                          \
             --encode-video: each file in the resulting album(s)\n                          \
             is re-encoded before upload. <mode> is one of:\n                            \
             fill — pack each album to 10 items before starting a\n                                         \
             new one; only the last album in a run may be smaller\n                            \
             even — split a run evenly across the minimum number\n                                         \
             of albums, like --tvshow\n  \
           --tvshow               Treat inputs as TV-show episodes. Filenames are\n                          \
             parsed via hunch to extract show title, season, and\n                          \
             episode; files are renamed to\n                          \
             `<title> S##E##.<ext>` and grouped per-season into\n                          \
             Telegram albums (≤10 per album, split as evenly as\n                          \
             possible). Mutually exclusive with --album,\n                          \
             -d caption, and -d zip. Compatible with\n                          \
             --encode-video; season albums are preserved and\n                          \
             each episode is re-encoded before upload.\n  \
           --encode-video         Re-encode video files with ffmpeg (using\n                          \
             ffmpeg.encode_args from the config) and attach\n                          \
             an ffmpeg-generated thumbnail to each uploaded\n                          \
             video. Encoded data is piped from ffmpeg —\n                          \
             never written to a temporary file. Requires\n                          \
             ffmpeg on PATH.\n  \
           --dry-run              Print the plan and exit.\n  \
           --test-thumbnails <dir>\n                          \
             Build the plan, extract thumbnail candidates for\n                          \
             all video items, write them to <dir> with the\n                          \
             selected (sharpest) one labeled, then exit without\n                          \
             connecting to Telegram.\n  \
           --config <path>        Config file (default:\n                          \
             ~/.config/tgfs/tgfs.yml). The auth session is\n                          \
             also stored next to the default config at\n                          \
             ~/.config/tgfs/session.sqlite3."
    );
}

pub fn parse_args() -> anyhow::Result<Args> {
    parse_args_from(std::env::args().skip(1))
}

/// Core of `parse_args`, taking an explicit argument iterator so it can be
/// exercised directly in tests without touching the real process argv.
fn parse_args_from(mut args: impl Iterator<Item = String>) -> anyhow::Result<Args> {
    let mut config_path: Option<String> = None;
    let mut channel: Option<String> = None;
    let mut dir_mode = DirMode::Skip;
    let mut dry_run = false;
    let mut encode_video = false;
    let mut album: Option<AlbumSplitMode> = None;
    let mut tvshow = false;
    let mut test_thumbnails: Option<PathBuf> = None;
    let mut paths: Vec<PathBuf> = Vec::new();
    while let Some(a) = args.next() {
        match a.as_str() {
            "-h" | "--help" => { print_usage(); std::process::exit(0); }
            "--config" => {
                config_path = Some(args.next().ok_or_else(|| anyhow!("--config requires a path"))?);
            }
            "-c" | "--channel" => {
                channel = Some(args.next().ok_or_else(|| anyhow!("-c/--channel requires a value"))?);
            }
            "-d" | "--dir" => {
                let v = args.next().ok_or_else(|| anyhow!("-d/--dir requires a value"))?;
                dir_mode = parse_dir_mode(&v)?;
            }
            "--dry-run" => dry_run = true,
            "--encode-video" => encode_video = true,
            "-a" | "--album" => {
                let v = args.next().ok_or_else(|| anyhow!("-a/--album requires a value"))?;
                album = Some(parse_album_mode(&v)?);
            }
            "--tvshow" => tvshow = true,
            "--test-thumbnails" => {
                let v = args.next().ok_or_else(|| anyhow!("--test-thumbnails requires a directory path"))?;
                test_thumbnails = Some(PathBuf::from(v));
            }
            other if other.starts_with("--channel=") => {
                channel = Some(other.trim_start_matches("--channel=").to_string());
            }
            other if other.starts_with("--dir=") => {
                dir_mode = parse_dir_mode(other.trim_start_matches("--dir="))?;
            }
            other if other.starts_with("--album=") => {
                album = Some(parse_album_mode(other.trim_start_matches("--album="))?);
            }
            other if other.starts_with("--config=") => {
                config_path = Some(other.trim_start_matches("--config=").to_string());
            }
            other if other.starts_with('-') => bail!("unknown option: {}", other),
            _ => paths.push(PathBuf::from(a)),
        }
    }
    if paths.is_empty() { bail!("at least one file or directory path is required"); }
    if tvshow {
        if album.is_some() { bail!("--tvshow is incompatible with --album"); }
        if matches!(dir_mode, DirMode::Caption | DirMode::Zip) {
            bail!("--tvshow is incompatible with -d caption|zip");
        }
    }
    Ok(Args { config_path, channel, dir_mode, dry_run, encode_video, album, tvshow, paths, test_thumbnails })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> anyhow::Result<Args> {
        parse_args_from(args.iter().map(|s| s.to_string()))
    }

    #[test]
    fn album_fill_is_parsed() {
        let a = parse(&["-a", "fill", "file.mp4"]).unwrap();
        assert_eq!(a.album, Some(AlbumSplitMode::Fill));
    }

    #[test]
    fn album_even_is_parsed() {
        let a = parse(&["--album", "even", "file.mp4"]).unwrap();
        assert_eq!(a.album, Some(AlbumSplitMode::Even));
    }

    #[test]
    fn album_equals_form_is_parsed() {
        let a = parse(&["--album=even", "file.mp4"]).unwrap();
        assert_eq!(a.album, Some(AlbumSplitMode::Even));
    }

    #[test]
    fn album_omitted_is_none() {
        let a = parse(&["file.mp4"]).unwrap();
        assert_eq!(a.album, None);
    }

    #[test]
    fn album_rejects_unknown_mode() {
        assert!(parse(&["-a", "bogus", "file.mp4"]).is_err());
    }

    #[test]
    fn album_requires_a_value() {
        assert!(parse(&["-a"]).is_err());
    }

    #[test]
    fn album_is_incompatible_with_tvshow() {
        assert!(parse(&["--tvshow", "-a", "fill", "file.mp4"]).is_err());
    }
}
