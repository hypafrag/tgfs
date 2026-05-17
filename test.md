# Testing

## Running

```bash
cargo test            # full suite
cargo test indexer    # one module's tests
cargo test parse_     # by test-name substring
cargo test -- --nocapture   # show log output / debug prints
```

The binary is compiled in test mode (`cargo test` builds `tgfs` with `#[cfg(test)]` enabled), so every `#[cfg(test)] #[path = "../tests/<module>.rs"] mod tests;` stub in `src/*.rs` is collected into a single binary's test list.

## Implemented unit tests

Test code lives in `tests/<module>.rs` at the project root and is attached to its source module via a four-line stub at the bottom of the corresponding `src/<module>.rs`:

```rust
#[cfg(test)]
#[path = "../tests/<module>.rs"]
mod tests;
```

The `#[path]` attribute makes the file a private submodule of the source module — `use super::*;` at the top of each test file grants access to the source module's private items, so nothing needs to be made `pub` for testability. `Cargo.toml` sets `autotests = false` so Cargo doesn't try to compile these files as standalone integration-test binaries (which would fail because they reference private items in `super`). As of the latest run: **86 tests**.

### `tests/indexer.rs` (42 tests)

Caption directive parsing, multipart suffix detection, ZIP central-directory parsing, and prefix-collapse helpers — the highest-churn surface in the codebase.

- **Multipart suffix detection** — `split_part_suffix_*`: two-digit / three-digit / non-digit / no-dot / empty-suffix cases.
- **Boolean directive parsing** — `parse_bool_value_*`: truthy (`""` / `true` / `yes` / `1` / whitespace) and falsy (`false`, `no`, `0`, unknown).
- **Type directive parsing** — `parse_type_value_*`: known (`file`/`media`/`zip`) and unknown.
- **Path override resolution** — `resolve_path_override_*`: absent, full path, trailing-slash directory-only, bare `/` (no-op).
- **Field-from-text helper** — `parse_field_in_text_*`: first-match wins, whitespace trim, missing key.
- **Group caption parser** — `parse_caption_directives_*`: empty/plain text → `None`; combined `path:` + `type:` + `multipart:`; bare `multipart:` → true; `multipart: false` alone → `None`.
- **File classification** — `classify_file_type_*`: override precedence, audio/video/image → media, `.zip`/`application/zip` → zip, default fallback.
- **Filename splitting** — `split_name_*`: no slash, with slash.
- **Prefix-collapse helpers** — `common_prefix_len_*` (basic + UTF-8 boundary), `trim_prefix_name_*` (whitespace/underscore tail, unmatched `[`/`(`, balanced brackets).
- **`apply_prefix_collapse`** — disabled when `min_len == 0`, shared-prefix grouping, sub-threshold no-op. Uses a private `fe()` helper that constructs a `FileEntry` with an empty `parts: DocParts` (the collapse logic reads only `name` and `path`).
- **ZIP parsing** — `find_eocd_*` (basic, too-small buffer, ZIP64 sentinel without locator), `parse_central_directory_*` (single entry, directory entries skipped), `parse_zip64_extra_*` (substitutes only `0xFFFFFFFF` sentinels).

### `tests/index.rs` (8 tests)

- **`MimePool`** — `intern_is_idempotent`, distinct indices, `get` round-trip, missing index.
- **`human_size`** — boundary at 1024, decimal formatting, sub-unit bytes.
- **`fmt_system_time`** — Unix epoch, a known 2026 date.

### `tests/fuse.rs` (11 tests)

- **`path_hash`** — root constant, non-root differs from root/zero, deterministic, distinct inputs → distinct hashes.
- **`split_parent_name`** — root child, deep path, no-slash defensive case.
- **`compute_diff`** — pure addition, pure removal (preserves stored ino), overlap is a no-op, rename emits both add and remove.

### `tests/server.rs` (12 tests)

- **`parse_range`** — absent header, simple range, open-ended (`10-`), implicit start (`-50`), out-of-range → `416`.
- **`encode_segments`** — preserves slashes, percent-encodes spaces, handles Unicode.
- **`content_disposition`** — `media` → `inline`, `file`/`zip` → `attachment`.
- **`normalize_path`** — backslash conversion, leading `./` and `/` strip.
- **`parent_href`** — channel root, sub-directory, URL-encoded channel name.

### `tests/config.rs` (7 tests)

- **`expand_env`** — no `$`, `${VAR}`, bare `$VAR`, missing var → empty, bare `$` passed through.
- **`LogConfig::to_filter_string`** — `Level` round-trip, `Modules` single-entry directive.
- **`MultipartPolicy::default()`** — defaults to `None`.

## How to improve

- **Mutation testing.** Run [`cargo-mutants`](https://github.com/sourcefrog/cargo-mutants) on `src/indexer.rs` to catch tests that aren't actually exercising the branches they look like they cover (especially `apply_prefix_collapse` — the bracket/whitespace cutoff is easy to off-by-one).
- **Property tests for ZIP byte-buffer parsers.** `find_eocd` / `parse_central_directory` / `parse_zip64_extra` are pure byte-in → struct-out functions, ideal for `proptest` or `quickcheck` to fuzz against arbitrary byte buffers and verify they never panic (only return `None` / fail gracefully).
- **`apply_prefix_collapse` coverage expansion.** Current tests cover the happy path; add cases for: nested existing `path`, prefix that ends mid-UTF-8, prefix containing only whitespace, three-way groups with diverging tails.
- **`fmt_system_time` regression coverage.** Currently two data points. The algorithm is Howard-Hinnant `civil_from_days` — worth adding pre-1970 (negative epoch), leap-year boundaries (2024-02-29, 2100-02-28), and end-of-month rollovers.
- **`parse_range` edge cases.** Add `bytes=0-0` (single byte), `bytes=99-99` at exact end, malformed (`bytes=abc`), missing `bytes=` prefix.
- **Async assembler tests.** `assemble_channel_files` is testable in principle with a stubbed `Client`, but constructing a `grammers_client::Client` in-test is impractical. The realistic path is integration-style coverage against a recorded fixture — see TODO below.

## Snapshot tests (potential)

- `dir_listing` HTML output is stable and renders deterministically; a `insta` snapshot would catch unintentional HTML changes.
- Sorted channel listings and `fmt_system_time` round-trips would also fit.

## TODO: integration tests

Defer to a future commit. These need infrastructure that unit tests intentionally avoid:

- **End-to-end multipart assembly.** Construct a fake channel via `assemble_channel_files` with synthetic `RawEntry` fixtures (Photo media — easier to construct than Document) and verify multipart `suffix` vs `album` produce the expected `FileEntry` layout. Requires mocking or constructing `grammers_client::media::Media`, which has no constructor — needs a test fixture loaded from a saved `tl::types::*` payload or a thin wrapper trait inserted around media access.
- **FUSE tree rebuild + Notifier dispatch.** Run `TgfsFS::rebuild_channel` against a fixture `AppState` (no real Telegram), then either inspect `tree` state directly or stub the `Notifier` to record calls. Verifies add/remove path diff is correctly translated into `delete` / `inval_entry` notifications. Needs a way to construct a `TgfsFS` without `Handle::current()` (e.g. with a `tokio::test` runtime) and without the `Notifier` to actually send messages over a fuse fd.
- **HTTP smoke tests.** `axum::Router` can be exercised via `tower::ServiceExt::oneshot` — but every interesting endpoint hits `state.client` which is a real `grammers_client::Client`. Either add a `ClientLike` trait abstraction or build a recorded fixture state where `download_range` etc. are not called (e.g. directory listings only). Reasonable first integration test would be just the channel-listing path that doesn't touch Telegram.
- **Realtime dispatcher.** Inject a `(tx, rx)` pair instead of `Client::stream_updates`, push synthetic `Update::NewMessage` / `Update::MessageEdited` / `Update::MessageDeleted` events, and verify the channel state mutates correctly. Currently the dispatcher constructs `UpdateStream` internally via `Client::stream_updates`, so this needs either a feature flag for test injection or a trait abstraction over the update source.
- **ZIP fixture round-trip.** Generate a real ZIP file in-memory (e.g. with the `zip` crate as a `dev-dependency`), feed its bytes to `find_eocd` + `parse_central_directory`, and assert the parsed entries match the source. This catches encoder/parser divergence.
- **`download_range` across multi-part fixtures.** Same shape: synthetic part list, mock client returning recorded bytes, verify the concatenation/offset math.
- **`message_to_raw_entry` + `extract_group_caption`** against recorded `Message` fixtures (JSON dump of TL types?), covering each branch: non-grouped doc with `path:`, grouped with combined caption, photo with `name:` override, message with no media.
- **CDN download path.** The `upload.getCdnFile` / `reuploadCdnFile` retry loop is tricky to exercise without a real CDN. A mock `Client` returning `CdnRedirect` then `CdnFile::ReuploadNeeded` then `CdnFile::File` would verify the state machine.
- **Realtime + FUSE Notifier together.** Higher-level: inject an update, observe that the tree mutates and the right `Notifier` calls were enqueued. Combines two of the above.
- **Concurrent dispatcher + reader.** Verify that an HTTP request reading a file via `state.channels.get(...).read()` doesn't deadlock or panic when the dispatcher swaps `files: Arc<...>` mid-flight.
