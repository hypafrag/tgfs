❯ estimate changing indexing product from plain file entries list to tree-like directory/file structure to avoid virtual paths overhead after indexing

⏺ Estimate: flat list → directory tree

  Motivation

  Current design stores index: HashMap<String, Vec<FileEntry>> — flat lists per channel. Virtual paths from name: dir/sub/file overrides are encoded as
  FileEntry::path: Option<PathBuf> and reconstructed per-request via full_for(), which:

  - allocates a fresh String every call,
  - is called ~2-6× per file per request (listing + lookup),
  - drives three separate O(N_in_channel) scans in server.rs (root listing, virtual-dir listing, archive matching).

  Archive internals sit in a parallel Vec<ArchiveFileEntry> on each zip FileEntry, matched via stem/full-path prefix loops — another O(N_archives) scan per
  request.

  Proposed model

  pub enum Node {
      Dir(BTreeMap<String, Node>),
      File(FileEntry),
  }
  pub struct AppState {
      pub channels: BTreeMap<String, Node>, // each root is always Dir
      // ...
  }

  Key points:
  - FileEntry::path deleted — position in the tree encodes the virtual directory.
  - Archive interiors are materialized into the same tree at index time. Inner "files" become a new FileEntry flavor that references the outer archive's
  DocParts + local header offset + compressed/uncompressed sizes. This kills the last special case.
  - ArchiveView::FileAndDirectory — the archive occupies two sibling keys: name (file) and stem (dir). Clean, no enum gymnastics.
  - ArchiveView::Directory — only the stem dir node is inserted.

  Request path becomes

  fn walk(root: &Node, segments: &[&str]) -> Option<&Node>;  // O(depth)
  match walk(channel_root, &segments) {
      Some(Node::Dir(children)) => list_children(children),
      Some(Node::File(f))       => stream_file(f),
      None                      => 404,
  }

  One traversal, one listing function, one download function. The three entries_for_* helpers and the archive-matching loops all collapse.

  Line-count estimate

  ┌────────────┬──────┬──────┬──────┐
  │    File    │  +   │  −   │ net  │
  ├────────────┼──────┼──────┼──────┤
  │ index.rs   │ ~45  │ ~15  │ +30  │
  ├────────────┼──────┼──────┼──────┤
  │ indexer.rs │ ~60  │ ~25  │ +35  │
  ├────────────┼──────┼──────┼──────┤
  │ server.rs  │ ~50  │ ~200 │ −150 │
  ├────────────┼──────┼──────┼──────┤
  │ total      │ ~155 │ ~240 │ ~−85 │
  └────────────┴──────┴──────┴──────┘

  Projected server.rs: ~480 → ~330 lines.

  Runtime wins

  - Directory listing: O(direct children) instead of O(channel size) — matters for channels with many files at root.
  - File lookup: O(path depth) instead of linear scan.
  - Zero per-request String allocations for path reconstruction.
  - Archive inner-file lookup: same tree walk, no second scan.

  Memory

  - Remove Option<PathBuf> per entry (~32 bytes).
  - Add BTreeMap<String, Node> per directory (~48 bytes header + entry overhead).
  - For M files across K directories where M >> K: small net win.
  - Archive materialization: one FileEntry-shaped node per zip entry. For a 10k-file zip this is ~1-2 MB. For config profile archive_view: file (the default),
   no materialization — archive is a plain leaf. Only directory / file_and_directory pay this cost.

  Complexity & risk

  Medium. Touches every file but the changes are localized and mostly mechanical.

  The few non-trivial bits:

  1. Multipart grouping still runs on the flat list first. Part detection (foo.00, foo.01, ...) needs to see all siblings. The current logic builds the flat
  files: Vec<FileEntry>, groups, then produces a smaller flat list of combined entries. Only after that do we insert into the tree. No logic change, just an
  extra final step.
  2. Two FileEntry flavors. Regular files own DocParts; archive inner files need (archive_parts: DocParts, local_header_offset, compressed_size,
  uncompressed_size, mime, name). Either a new enum variant (File(RegularFile) vs File(ArchiveInner)) or a sum type on FileEntry itself. Adds maybe 20 lines,
  but kills the download_range + stream_parts_range special-casing at the server download path.
  3. Dual node for FileAndDirectory view. Requires that both "archive.zip" and "archive" exist as siblings. Trivial if we key the BTreeMap on display name and
   insert twice — but listing would need to dedup. An alternative: only the dir node exists in the tree, and the "download the archive raw" URL re-uses the
  parent's lookup via a query param or .zip suffix match. The two-sibling approach is probably cleanest.
  4. name: dir/sub/file.ext path collisions. Currently if two messages declare the same virtual path, both end up as separate FileEntrys in the flat list and
  the listing logic silently picks one. In the tree, the second insert has to decide: overwrite, skip, or rename. Pick a policy (skip + warn) and log once.
  Same effective behavior, more explicit.

  When it's worth doing

  Code-clarity motivation is strong now. The three prefix-matching loops in server.rs are the ugliest code left after the previous refactors. Collapsing them
  into one tree walk is a big legibility win, independent of perf.

  Perf motivation depends on channel size. For channels with tens of files, the current O(N) scans are invisible. For a channel with thousands of files + deep
   virtual-path trees, the current design will start showing up in request latency and allocator pressure. You mentioned an Audiobooks channel — if those are
  organized into virtual directories, this is the one that benefits.

  Risk of introducing regressions: moderate. Worth having a small set of manual smoke tests (root listing, nested virtual path, multipart file, zip dir
  listing, zip inner file, zip-over-multipart inner file, FileAndDirectory view) to walk through once the refactor is done.

  Recommendation

  Do it, but scope it into three commits for reviewability:

  1. Commit A: introduce Node tree and build it from the existing flat index (new code, old code still runs in parallel). No server changes yet.
  2. Commit B: switch handle_channel_path to use the tree. Delete full_for, stem_full_for, and the three entries_for_* helpers.
  3. Commit C: materialize zip interiors into the tree. Delete the archive-matching loops and the separate archive_entries render path.