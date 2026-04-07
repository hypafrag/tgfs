### Repository overview

This repository is a small Rust HTTP frontend for Telegram file shares (tgfs). It indexes configured channels and exposes a simple directory-style HTTP index. It supports showing `.zip` attachments as directories and extracting files on-demand using ranged downloads and DEFLATE inflation.

### How I should behave as Copilot in this repo

- Prefer minimal, focused edits that match the existing coding style (Rust 2021, small helper modules).
- Keep changes async-friendly: avoid blocking calls in async contexts. If a blocking operation is necessary, use `tokio::task::spawn_blocking`.
- Do not add the `zip` crate — the project intentionally parses ZIP central directories and uses ranged downloads and `flate2` for DEFLATE inflation.
- When modifying archive logic, preserve the EOCD-first partial-download approach: download EOCD tail, fetch central directory, store metadata, and defer payload downloads until HTTP request time.
- Preserve in-memory processing; avoid writing temporary files to disk unless a clear benefit is documented.

### Important files and responsibilities

- `src/main.rs`: program entry, auth/session handling and wiring of modules.
- `src/index.rs`: data models, config loader, and HTML directory generator.
- `src/indexer.rs`: logic that walks Telegram dialogs/messages and builds the file index. Responsible for ranged `download_range`, EOCD search, and central-directory parsing.
- `src/server.rs`: axum HTTP handlers; responsible for directory listings, top-level file streaming, and on-demand archive entry extraction.

### How to build and run locally

1. Build:

```
cargo build
```

2. Run (interactive auth may be required on first run):

```
cargo run
```

3. Test via `curl` or browser against `http://localhost:8080/`.

Example: `curl -I http://localhost:8080/Audiobooks/` to get the channel index.

### Configuration notes

- `tgfs.yml` contains `channels` entries. Each entry may include `archive_view: file|directory|file_and_directory`.
- Do not change the config schema without updating `src/index.rs::Config`.

### Tests and quality

- Run `cargo test` after adding behavior-critical changes.
- Keep code changes small and unit-testable; prefer refactors that separate parsing, I/O, and HTTP layer.

### Commit guidance

- Use concise commit messages describing intent, e.g. "server: fix inner-archive links".
- For larger changes, open a branch and include a short README or PR description explaining tradeoffs (especially around archive handling and caching).

### Troubleshooting

- If the server fails to start, check `auth.json` and `session.sqlite3`. Re-run auth if the session is invalidated.
- For archive-related bugs, reproduce with a small sample `.zip` and log EOCD/central-directory offsets; avoid downloading entire archives during indexing.

If you want a stricter or more detailed policy (e.g., lint rules, additional tests, or caching strategy), tell me and I'll add it.
