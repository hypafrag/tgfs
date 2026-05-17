use super::*;

// -------- path_hash --------

#[test]
fn path_hash_root_is_root_inode() {
    assert_eq!(path_hash("/"), INodeNo::ROOT.0);
}

#[test]
fn path_hash_non_root_differs_from_root_and_zero() {
    let h = path_hash("/channels/foo");
    assert_ne!(h, 0);
    assert_ne!(h, INodeNo::ROOT.0);
}

#[test]
fn path_hash_is_deterministic() {
    assert_eq!(path_hash("/a/b/c"), path_hash("/a/b/c"));
}

#[test]
fn path_hash_distinguishes_different_paths() {
    assert_ne!(path_hash("/a/b"), path_hash("/a/c"));
}

// -------- split_parent_name --------

#[test]
fn split_parent_name_root_child() {
    let (parent, name) = split_parent_name("/foo");
    assert_eq!(parent, "/");
    assert_eq!(name, "foo");
}

#[test]
fn split_parent_name_deep_path() {
    let (parent, name) = split_parent_name("/a/b/c.txt");
    assert_eq!(parent, "/a/b");
    assert_eq!(name, "c.txt");
}

#[test]
fn split_parent_name_no_slash_defaults_to_root_parent() {
    // Defensive: real callers always pass absolute paths, but the helper
    // should not panic if it sees a bare name.
    let (parent, name) = split_parent_name("orphan");
    assert_eq!(parent, "/");
    assert_eq!(name, "orphan");
}

// -------- compute_diff --------

fn old_map(pairs: &[(&str, u64)]) -> HashMap<String, u64> {
    pairs.iter().map(|(p, ino)| (p.to_string(), *ino)).collect()
}

fn new_set(paths: &[&str]) -> HashSet<String> {
    paths.iter().map(|s| s.to_string()).collect()
}

#[test]
fn compute_diff_pure_addition() {
    let diff = compute_diff(old_map(&[]), new_set(&["/ch/a"]));
    assert!(diff.removed.is_empty());
    assert_eq!(diff.added.len(), 1);
    assert_eq!(diff.added[0].1, "a");
    assert_eq!(diff.added[0].0, path_hash("/ch"));
}

#[test]
fn compute_diff_pure_removal_preserves_inode() {
    let diff = compute_diff(old_map(&[("/ch/a", 7)]), new_set(&[]));
    assert!(diff.added.is_empty());
    assert_eq!(diff.removed.len(), 1);
    let (parent_ino, child_ino, name) = &diff.removed[0];
    assert_eq!(*parent_ino, path_hash("/ch"));
    assert_eq!(*child_ino, 7);
    assert_eq!(name, "a");
}

#[test]
fn compute_diff_overlap_is_noop() {
    let diff = compute_diff(old_map(&[("/ch/a", 1)]), new_set(&["/ch/a"]));
    assert!(diff.added.is_empty());
    assert!(diff.removed.is_empty());
}

#[test]
fn compute_diff_rename_emits_add_and_remove() {
    // Same content moved from /ch/a → /ch/b
    let diff = compute_diff(old_map(&[("/ch/a", 1)]), new_set(&["/ch/b"]));
    assert_eq!(diff.added.len(), 1);
    assert_eq!(diff.removed.len(), 1);
    assert_eq!(diff.added[0].1, "b");
    assert_eq!(diff.removed[0].2, "a");
}
