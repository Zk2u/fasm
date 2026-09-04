//! The directory operations over a [`RawKv`] view: the version check,
//! node lookup, lazy creation, child listing, and recursive removal.
//!
//! Each function ports the corresponding `DirectoryLayer` algorithm
//! (pinned `foundationdb-0.11.0`) onto the flat layout's row forms. The
//! flat single-writer view makes the layer's transactional structure
//! trivial: every operation below is one flat transaction; FDB's
//! version-vector and conflict-range machinery has no flat analogue.
//!
//! Paths are the layer's paths — `&[&[u8]]` of segments, UTF-8 enforced
//! by the caller at the fasm API boundary (`validate_dir`) and re-checked
//! here where a segment is packed as a string element.

use core::ops::Bound;

use rand::RngExt;

use crate::flatdir::error::FlatError;
use crate::flatdir::hca::allocate;
use crate::flatdir::layout::{
    COUNTERS_BASE, LAYER_TAG, LAYOUT_VERSION, NODE_PREFIX, RECENT_BASE, ROOT_NODE, VERSION_KEY,
    child_key, children_key_range, data_region, layer_key, node_key_range, node_subspace,
    parse_version,
};
use crate::flatdir::tuple::{self, unpack_child_name, unpack_i64_full};
use crate::keyspace::next_prefix;
use crate::rawkv::RawKv;

/// Convert an owned bound pair into references (the `RawKv` bound type).
pub(crate) fn bounds_ref(b: &(Bound<Vec<u8>>, Bound<Vec<u8>>)) -> (Bound<&[u8]>, Bound<&[u8]>) {
    (bound_ref1(&b.0), bound_ref1(&b.1))
}

fn bound_ref1(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
    }
}

/// Read and enforce the store's version row (the layer's `check_version`,
/// verbatim):
///
/// * missing → fresh: succeed, and write the version row iff
///   `allow_create` (lazy initialisation — the first create writes it);
/// * present → must parse as a 12-byte version; a newer major (or a
///   read-only newer minor — this layout has no read-only mode, so it
///   fails closed) is [`FlatError::Foreign`]; a malformed value is
///   [`FlatError::Foreign`] (the layer calls it "incorrect version
///   length").
pub fn check_version<R: RawKv>(raw: &mut R, allow_create: bool) -> Result<(), FlatError<R::Error>> {
    check_version_read(raw)?;
    if allow_create {
        // The fresh-store branch: the layer's `initialize_directory`
        // (1.0.0, little-endian).
        if raw.get(VERSION_KEY)?.is_none() {
            raw.insert(VERSION_KEY, LAYOUT_VERSION)?;
        }
    }
    Ok(())
}

/// The read-only half of [`check_version`]: enforce the version row
/// without writing it. Read operations pay this one point read per
/// call (the layer's per-entry-point cost).
pub fn check_version_read<R: RawKv>(raw: &R) -> Result<(), FlatError<R::Error>> {
    match raw.get(VERSION_KEY)? {
        None => Ok(()),
        Some(v) => match parse_version(&v) {
            None => Err(FlatError::Foreign),
            Some((major, minor, _)) => {
                if major > 1 || minor > 0 {
                    Err(FlatError::Foreign)
                } else {
                    Ok(())
                }
            }
        },
    }
}

/// Find the allocated prefix of the node at `path` (the layer's `find`,
/// verbatim): walk from the root node; a missing child row stops the walk
/// with `Ok(None)` — missing parents are not errors.
///
/// An empty path is the layer's root: it always exists, and its
/// "prefix" is the node prefix itself (never used as a data prefix — the
/// fasm mapping always carries the anchor segment).
pub fn find<R: RawKv>(raw: &R, path: &[&[u8]]) -> Result<Option<Vec<u8>>, FlatError<R::Error>> {
    if path.is_empty() {
        return Ok(Some(NODE_PREFIX.to_vec()));
    }
    let mut node = ROOT_NODE.to_vec();
    let mut prefix = NODE_PREFIX.to_vec();
    for seg in path {
        let name = core::str::from_utf8(seg).map_err(|_| FlatError::Corrupt)?;
        let key = child_key(&node, name);
        let Some(value) = raw.get(&key)? else {
            return Ok(None);
        };
        // The value is the child's allocated prefix (a packed i64); a
        // malformed value is a corrupt node row.
        if unpack_i64_full(&value).is_none() {
            return Err(FlatError::Corrupt);
        }
        node = node_subspace(&value);
        prefix = value;
    }
    Ok(Some(prefix))
}

/// The raw key that owns a data key (the layer's `node_containing_key`):
/// for a data key `P ‖ k`, the prefix `P` is the packed `i64` at the
/// front of the key.
///
/// Returns the prefix (`P`), or `None` when the key is not a data key
/// with a well-formed allocated prefix at its front (meta keys, foreign
/// content, reserved-region rows).
pub fn data_key_prefix(key: &[u8]) -> Option<&[u8]> {
    if key.is_empty() {
        return None;
    }
    // A packed i64 first byte is 0x0c..=0x1c; anything else (0x00..0x0b,
    // 0x1d..=0xff) is not a data key.
    match key[0] {
        0x0c..=0x1c => {
            let n = (key[0] as i8 - 0x14).unsigned_abs() as usize;
            let len = 1 + n;
            if key.len() < len {
                return None;
            }
            let packed = &key[..len];
            unpack_i64_full(packed)?;
            Some(packed)
        }
        _ => None,
    }
}

/// Whether an allocated prefix is free (the layer's `is_prefix_free`,
/// verbatim): the prefix is free when (a) no existing node's child rows
/// could own a key in the prefix's data region — checked by the
/// `node_containing_key` reverse scan over the node region, and (b) the
/// prefix's own node subspace holds no rows.
///
/// Both checks hold for every HCA-allocated prefix; a violation here is
/// an allocator-invariant failure ([`FlatError::Corrupt`]), reported by
/// the caller.
pub fn is_prefix_free<R: RawKv>(raw: &R, prefix: &[u8]) -> Result<bool, FlatError<R::Error>> {
    if prefix.is_empty() {
        return Ok(false);
    }
    if prefix.starts_with(NODE_PREFIX) {
        return Ok(false);
    }
    // (a) node_containing_key: the largest node row strictly below the
    // candidate's position in the node region. FDB reverse-scans with
    // limit 1; the flat view scans ascending and takes the last.
    {
        let mut key_after = prefix.to_vec();
        key_after.push(0);
        let range_end = {
            let mut k = NODE_PREFIX.to_vec();
            k.extend_from_slice(&tuple::pack_bytes(&key_after));
            k
        };
        let mut scan_start = NODE_PREFIX.to_vec();
        scan_start.push(0);
        let rows = raw.scan(
            Bound::Included(scan_start.as_slice()),
            Bound::Excluded(range_end.as_slice()),
            true,
        )?;
        if let Some(row) = rows.last() {
            let key = &row.key;
            let rel = &key[NODE_PREFIX.len()..];
            if let Some(prev) = tuple::first_element_payload(rel)
                && prefix.starts_with(&prev)
            {
                return Ok(false);
            }
        }
    }
    // (b) the candidate's own node subspace must be empty, over the
    // layer's exact range [BYTES(P), BYTES(strinc(P))).
    {
        let ns_start = {
            let mut k = NODE_PREFIX.to_vec();
            k.extend_from_slice(&tuple::pack_bytes(prefix));
            k
        };
        let ns_end = {
            let mut k = NODE_PREFIX.to_vec();
            k.extend_from_slice(&tuple::pack_bytes(&next_prefix(prefix).expect(
                "next_prefix fails only on all-0xFF; allocated prefixes start 0x0c..=0x1c",
            )));
            k
        };
        let rows = raw.scan(
            Bound::Included(ns_start.as_slice()),
            Bound::Excluded(ns_end.as_slice()),
            true,
        )?;
        Ok(rows.is_empty())
    }
}

/// The `get_prefix` range probe: whether the candidate's data region
/// `[prefix ‖ 0x00, prefix ‖ 0xFF]` holds any row (verbatim: a limit-1
/// range read; the flat view counts).
pub fn prefix_data_region_occupied<R: RawKv>(
    raw: &R,
    prefix: &[u8],
) -> Result<bool, FlatError<R::Error>> {
    let mut start = prefix.to_vec();
    start.push(0x00);
    let mut end = prefix.to_vec();
    end.push(0xFF);
    end.push(0x00);
    Ok(!raw
        .scan(
            Bound::Included(start.as_slice()),
            Bound::Excluded(end.as_slice()),
            true,
        )?
        .is_empty())
}

/// Open (and optionally create) the directory at `path`, returning its
/// allocated prefix.
///
/// This composes the layer's `create_or_open_internal` + `create_internal`
/// over the flat view: version check (read mode), the node walk
/// (`find`), and on creation — the version check (create mode, which
/// lazily writes the version row on a fresh store), the HCA allocation,
/// the two prefix-freedom checks, the lazy parent creation, and the
/// child-row + layer-row writes.
pub fn create_or_open<R: RawKv, RNG: RngExt>(
    raw: &mut R,
    path: &[&[u8]],
    allow_create: bool,
    rng: &mut RNG,
) -> Result<Option<Vec<u8>>, FlatError<R::Error>> {
    check_version(raw, false)?;
    if let Some(prefix) = find(raw, path)? {
        return Ok(Some(prefix));
    }
    if !allow_create {
        return Ok(None);
    }
    // create_internal (verbatim order): version check with creation,
    // allocation, both prefix checks, parent creation, row writes.
    check_version(raw, true)?;
    let new_prefix = tuple::pack_i64(allocate(raw, rng)?);
    if prefix_data_region_occupied(raw, &new_prefix)? {
        // PrefixNotEmpty: the allocator produced a used prefix — an
        // invariant failure, not a retryable one.
        return Err(FlatError::Corrupt);
    }
    if !is_prefix_free(raw, &new_prefix)? {
        return Err(FlatError::Corrupt);
    }
    let (last, parent_path) = path
        .split_last()
        .expect("find said the path is missing; it is non-empty");
    let parent_node = if parent_path.is_empty() {
        ROOT_NODE.to_vec()
    } else {
        // get_parent_node (verbatim): the parent is opened (created)
        // lazily — intermediate nodes appear only on demand.
        let parent_prefix =
            create_or_open(raw, parent_path, true, rng)?.expect("the parent path was just created");
        node_subspace(&parent_prefix)
    };
    let name = core::str::from_utf8(last).map_err(|_| FlatError::Corrupt)?;
    raw.insert(&child_key(&parent_node, name), &new_prefix)?;
    raw.insert(&layer_key(&node_subspace(&new_prefix)), &[])?;
    Ok(Some(new_prefix))
}

/// List the immediate children of the directory with allocated prefix
/// `prefix`, sorted by name (the layer's `list`).
pub fn list_children<R: RawKv>(raw: &R, prefix: &[u8]) -> Result<Vec<String>, FlatError<R::Error>> {
    let node = node_subspace(prefix);
    let r = children_key_range(&node);
    let (s, e) = bounds_ref(&r);
    let rows = raw.scan(s, e, true)?;
    let mut names = Vec::with_capacity(rows.len());
    for row in &rows {
        let key = &row.key;
        if !key.starts_with(&node) {
            return Err(FlatError::Corrupt);
        }
        let rel = &key[node.len()..];
        names.push(unpack_child_name(rel).ok_or(FlatError::Corrupt)?);
    }
    names.sort();
    Ok(names)
}

/// Remove the directory at `path` and its entire subtree (the layer's
/// `remove_internal` + `remove_recursive` + `remove_from_parent`,
/// verbatim operation order): recurse into every child, clear the
/// directory's data region, clear the node's own rows, then delete the
/// child row in the (re-found) parent, tolerating a missing parent.
///
/// `Ok(false)` when the path does not exist. An empty path is the layer
/// root and is not removable; the fasm mapping never passes one (the
/// fasm root directory is rejected before reaching this function).
pub fn remove_dir<R: RawKv>(raw: &mut R, path: &[&[u8]]) -> Result<bool, FlatError<R::Error>> {
    check_version(raw, true)?;
    if path.is_empty() {
        // CannotModifyRootDirectory (the layer): unreachable in the fasm
        // mapping; a precondition violation, not a structural fault.
        return Err(FlatError::Corrupt);
    }
    let Some(prefix) = find(raw, path)? else {
        return Ok(false);
    };
    remove_recursive(raw, &prefix)?;
    // remove_from_parent (verbatim): re-resolve the parent and tolerate
    // its absence. `find` of the empty path answers the root node, so a
    // single-segment path clears the child row the layer holds under the
    // root node — the same row the create path wrote.
    let (last, parent_path) = path.split_last().unwrap();
    if let Some(parent_prefix) = find(raw, parent_path)? {
        let name = core::str::from_utf8(last).map_err(|_| FlatError::Corrupt)?;
        let parent_node = node_subspace(&parent_prefix);
        raw.delete(&child_key(&parent_node, name))?;
    }
    Ok(true)
}

/// The recursive removal of a directory node (verbatim): scan the
/// children subspace, recurse into each child (deepest-first via the
/// recursion), then clear the data region, then the node's own rows.
fn remove_recursive<R: RawKv>(raw: &mut R, prefix: &[u8]) -> Result<(), FlatError<R::Error>> {
    let node = node_subspace(prefix);
    let r = children_key_range(&node);
    let (s, e) = bounds_ref(&r);
    let rows = raw.scan(s, e, true)?;
    for row in &rows {
        // The child row's value is the child's allocated prefix (packed
        // i64); a malformed value is corrupt structure.
        if unpack_i64_full(&row.value).is_none() {
            return Err(FlatError::Corrupt);
        }
        remove_recursive(raw, &row.value)?;
    }
    let dr = data_region(prefix);
    let (ds, de) = bounds_ref(&dr);
    raw.clear_range(ds, de)?;
    let nr = node_key_range(&node);
    let (ns, ne) = bounds_ref(&nr);
    raw.clear_range(ns, ne)?;
    Ok(())
}

/// A whole-store walk that validates every row against the layout.
///
/// Every row must classify into exactly one kind (version, HCA
/// counter, HCA recent, node row, data row) with well-formed fields;
/// every node that is referenced as a child must have its layer row;
/// every data key must resolve to such a live directory. Returns the
/// counted (data rows, child rows); `Err(FlatError::Corrupt)` on a
/// malformed row, `Err(FlatError::Foreign)` on content the layout does
/// not own (a row that classifies into no kind).
pub fn validate<R: RawKv>(raw: &R) -> Result<(usize, usize), FlatError<R::Error>> {
    let mut data_rows = 0usize;
    let mut child_rows = 0usize;
    let mut layer_rows: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
    let mut child_targets: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
    let mut all_live: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
    let mut node_scan_start = NODE_PREFIX.to_vec();
    node_scan_start.push(0);
    // Unbounded end: the walk also covers node keys at or above
    // `[FE FF 00]` and the `0xFF` reserved region, so a foreign row
    // anywhere in the store is caught, not just below `0xFF`.
    let node_rows = raw.scan(
        Bound::Included(node_scan_start.as_slice()),
        Bound::Unbounded,
        true,
    )?;
    for row in &node_rows {
        let key = &row.key;
        let value = &row.value;
        if key == VERSION_KEY {
            let (major, minor, _patch) = parse_version(value).ok_or(FlatError::Foreign)?;
            if major > 1 || minor > 0 {
                // The same gate as `check_version_read`: a foreign
                // version row is not a healthy layout, even when it
                // parses.
                return Err(FlatError::Foreign);
            }
            continue;
        }
        if key.starts_with(COUNTERS_BASE) {
            unpack_i64_full(&key[COUNTERS_BASE.len()..]).ok_or(FlatError::Corrupt)?;
            if value.len() != 8 {
                return Err(FlatError::Corrupt);
            }
            continue;
        }
        if key.starts_with(RECENT_BASE) {
            unpack_i64_full(&key[RECENT_BASE.len()..]).ok_or(FlatError::Corrupt)?;
            if !value.is_empty() {
                return Err(FlatError::Corrupt);
            }
            continue;
        }
        // A node row: NODE_PREFIX ‖ BYTES(P) ‖ row. The root node's P is
        // the reserved byte [0xFE] (not an HCA allocation); every other
        // node's P is a packed i64. A key that does not start with the
        // node prefix is not a node row: foreign content.
        if !key.starts_with(NODE_PREFIX) {
            return Err(FlatError::Foreign);
        }
        let rel = &key[NODE_PREFIX.len()..];
        let (escaped, row_rel) = split_node_row(rel).ok_or(FlatError::Foreign)?;
        // `split_node_row` returns the escaped payload for the row-offset
        // arithmetic; the prefix itself must be unescaped before
        // unpacking — a packed prefix whose big-endian payload contains a
        // `0x00` byte (e.g. 256, 512, 65536) escapes to `0x00 0xFF`
        // inside the node key.
        let prefix: Vec<u8> = if escaped == [0xFE] {
            vec![0xFE]
        } else {
            let raw_prefix = tuple::first_element_payload(rel).ok_or(FlatError::Foreign)?;
            unpack_i64_full(&raw_prefix)
                .map(tuple::pack_i64)
                .ok_or(FlatError::Corrupt)?
        };
        // The BYTES element re-packs canonically: a node row's leading
        // element is the canonical packing of P.
        let expected_node = node_subspace(&prefix);
        if key.len() < expected_node.len() || !key.starts_with(&expected_node) {
            return Err(FlatError::Corrupt);
        }
        if row_rel == LAYER_TAG {
            if !value.is_empty() {
                return Err(FlatError::Corrupt);
            }
            layer_rows.insert(prefix.clone());
            all_live.insert(prefix);
            continue;
        }
        // A child row: INT(0) ‖ STRING(name), value = child prefix.
        let _name = unpack_child_name(row_rel).ok_or(FlatError::Corrupt)?;
        let child_prefix = unpack_i64_full(value)
            .map(tuple::pack_i64)
            .ok_or(FlatError::Corrupt)?;
        child_rows += 1;
        all_live.insert(prefix);
        all_live.insert(child_prefix.clone());
        child_targets.insert(child_prefix);
    }
    // Every node that is a child's target must have its layer row.
    for target in &child_targets {
        if !layer_rows.contains(target) {
            return Err(FlatError::Corrupt);
        }
    }
    // The data region: every key below the node region must be a data
    // key resolving to a live directory. The unbounded start also covers
    // the empty key, which no data key can be.
    let data_end = vec![NODE_PREFIX[0], 0x00];
    let data_rows_scan = raw.scan(Bound::Unbounded, Bound::Excluded(data_end.as_slice()), true)?;
    for row in &data_rows_scan {
        let key = &row.key;
        let Some(p) = data_key_prefix(key) else {
            return Err(FlatError::Foreign);
        };
        if !all_live.contains(p) {
            return Err(FlatError::Corrupt);
        }
        data_rows += 1;
    }
    Ok((data_rows, child_rows))
}

/// Split a node-region relative key into (escaped BYTES payload, row
/// remainder). The relative key is `[code] ‖ escaped ‖ [00] ‖ row`;
/// `first_bytes_element` returns the escaped payload (escapes included,
/// terminator excluded), so the row starts after the terminator.
fn split_node_row(rel: &[u8]) -> Option<(&[u8], &[u8])> {
    let escaped = tuple::first_bytes_element(rel)?;
    let row_start = 1 + escaped.len() + 1; // code + escaped + terminator
    if rel.len() < row_start {
        return None;
    }
    Some((escaped, &rel[row_start..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flatdir::hca::Lcg;
    use crate::flatengine::Mem;

    /// Create the directory `dir` (fasm path form) under the anchor,
    /// returning its allocated prefix.
    fn create<R: RawKv, RNG: RngExt>(
        raw: &mut R,
        rng: &mut RNG,
        dir: &[&str],
    ) -> Result<Vec<u8>, FlatError<R::Error>> {
        let segs = path_refs(dir);
        create_or_open(raw, &as_refs(&segs), true, rng).map(|p| p.expect("created"))
    }

    /// The layer path for a fasm directory, owned: the anchor segment
    /// plus the directory segments.
    fn path_refs(dir: &[&str]) -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = vec![
            crate::flatdir::layout::ROOT_PATH_SEGMENT
                .as_bytes()
                .to_vec(),
        ];
        for d in dir {
            v.push(d.as_bytes().to_vec());
        }
        v
    }

    /// Borrow the owned segments as the `&[&[u8]]` form the ops take.
    fn as_refs(segs: &[Vec<u8>]) -> Vec<&[u8]> {
        segs.iter().map(|s| s.as_slice()).collect()
    }

    /// Resolve (without creating) the directory `dir`.
    fn resolve<R: RawKv, RNG: RngExt>(
        raw: &mut R,
        rng: &mut RNG,
        dir: &[&str],
    ) -> Result<Option<Vec<u8>>, FlatError<R::Error>> {
        let segs = path_refs(dir);
        create_or_open(raw, &as_refs(&segs), false, rng)
    }

    fn open_anchor<R: RawKv, RNG: RngExt>(
        raw: &mut R,
        rng: &mut RNG,
    ) -> Result<Vec<u8>, FlatError<R::Error>> {
        resolve(raw, rng, &[]).map(|p| p.expect("anchor exists"))
    }

    #[test]
    fn a_fresh_store_is_fresh_until_the_first_create() {
        let mut raw = Mem::default();
        // Reads on a fresh store succeed (verbatim FDB): find is None,
        // the version row is absent.
        let segs = path_refs(&[]);
        assert_eq!(find(&raw, &as_refs(&segs)).unwrap(), None);
        assert_eq!(raw.get(VERSION_KEY).unwrap(), None);
        // The first create writes the version row, the anchor node's
        // layer row, and the HCA rows.
        let prefix = create(&mut raw, &mut Lcg::new(1), &[]).unwrap();
        assert_eq!(raw.get(VERSION_KEY).unwrap(), Some(LAYOUT_VERSION.to_vec()));
        let anchor_node = node_subspace(&prefix);
        assert!(raw.get(&layer_key(&anchor_node)).unwrap().is_some());
        // The fasm root now exists (its anchor node row is present).
        assert_eq!(open_anchor(&mut raw, &mut Lcg::new(2)).unwrap(), prefix);
    }

    #[test]
    fn create_builds_the_node_tree_lazy() {
        let mut raw = Mem::default();
        let mut rng = Lcg::new(3);
        let a = create(&mut raw, &mut rng, &["a"]).unwrap();
        let ab = create(&mut raw, &mut rng, &["a", "b"]).unwrap();
        let abc = create(&mut raw, &mut rng, &["a", "b", "c"]).unwrap();
        assert!(!a.is_empty() && !ab.is_empty() && !abc.is_empty());
        assert_ne!(a, ab);
        assert_ne!(ab, abc);
        // Every node has its layer row.
        for p in [&a, &ab, &abc] {
            let node = node_subspace(p);
            assert!(raw.get(&layer_key(&node)).unwrap().is_some());
        }
        // Reopening finds the same prefixes (no re-allocation).
        assert_eq!(
            resolve(&mut raw, &mut Lcg::new(99), &["a"]).unwrap(),
            Some(a)
        );
        assert_eq!(
            resolve(&mut raw, &mut Lcg::new(99), &["a", "b", "c"]).unwrap(),
            Some(abc)
        );
    }

    #[test]
    fn list_children_is_sorted_and_exact() {
        let mut raw = Mem::default();
        let mut rng = Lcg::new(4);
        for name in ["c", "a", "b", "A", "aa"] {
            create(&mut raw, &mut rng, &[name]).unwrap();
        }
        let root = open_anchor(&mut raw, &mut Lcg::new(5)).unwrap();
        let names = list_children(&raw, &root).unwrap();
        assert_eq!(names, vec!["A", "a", "aa", "b", "c"]);
        // An empty directory lists empty (no error).
        let empty = create(&mut raw, &mut rng, &["empty"]).unwrap();
        assert!(list_children(&raw, &empty).unwrap().is_empty());
    }

    #[test]
    fn remove_deletes_the_whole_subtree() {
        let mut raw = Mem::default();
        let mut rng = Lcg::new(5);
        let a = create(&mut raw, &mut rng, &["a"]).unwrap();
        let ab = create(&mut raw, &mut rng, &["a", "b"]).unwrap();
        let ac = create(&mut raw, &mut rng, &["a", "c"]).unwrap();
        // A data row under each leaf (the data key is prefix || key).
        let data = |p: &[u8]| {
            let mut k = p.to_vec();
            k.extend_from_slice(b"key");
            k
        };
        raw.insert(&data(&a), b"v1").unwrap();
        raw.insert(&data(&ab), b"v2").unwrap();
        raw.insert(&data(&ac), b"v3").unwrap();

        assert!(remove_dir(&mut raw, &as_refs(&path_refs(&["a"]))).unwrap());
        // The whole subtree is gone: layer rows and data rows.
        for p in [&a, &ab, &ac] {
            let node = node_subspace(p);
            assert_eq!(raw.get(&layer_key(&node)).unwrap(), None);
        }
        assert_eq!(raw.get(&data(&a)).unwrap(), None);
        assert_eq!(raw.get(&data(&ab)).unwrap(), None);
        assert_eq!(raw.get(&data(&ac)).unwrap(), None);
        // The parent's child row is deleted.
        let root = open_anchor(&mut raw, &mut Lcg::new(6)).unwrap();
        assert!(list_children(&raw, &root).unwrap().is_empty());
        // Removing a missing directory is Ok(false).
        assert!(!remove_dir(&mut raw, &as_refs(&path_refs(&["a"]))).unwrap());
        // The store still validates.
        assert!(validate(&raw).is_ok());
    }

    #[test]
    fn remove_tolerates_a_missing_parent() {
        let mut raw = Mem::default();
        let mut rng = Lcg::new(6);
        let _a = create(&mut raw, &mut rng, &["a"]).unwrap();
        let _ab = create(&mut raw, &mut rng, &["a", "b"]).unwrap();
        // Removing "a" clears the "a/b" subtree with it; removing "a/b"
        // afterwards fails the walk at "a" and is Ok(false), not an
        // error.
        assert!(remove_dir(&mut raw, &as_refs(&path_refs(&["a"]))).unwrap());
        assert!(!remove_dir(&mut raw, &as_refs(&path_refs(&["a", "b"]))).unwrap());
        assert!(validate(&raw).is_ok());
    }

    #[test]
    fn a_foreign_version_fails_closed() {
        let mut raw = Mem::default();
        // major = 2: newer than this layout reads.
        let mut v = [0u8; 12];
        v[0..4].copy_from_slice(&2u32.to_le_bytes());
        raw.insert(VERSION_KEY, &v).unwrap();
        let mut rng = Lcg::new(1);
        let segs = path_refs(&["a"]);
        assert_eq!(
            create_or_open(&mut raw, &as_refs(&segs), true, &mut rng).unwrap_err(),
            FlatError::Foreign
        );
        // minor > 0: a same-major foreign store (the read-only rule).
        let mut raw = Mem::default();
        let mut v = [0u8; 12];
        v[0..4].copy_from_slice(&1u32.to_le_bytes());
        v[4..8].copy_from_slice(&1u32.to_le_bytes());
        raw.insert(VERSION_KEY, &v).unwrap();
        let mut rng = Lcg::new(3);
        let segs = path_refs(&["a"]);
        assert_eq!(
            create_or_open(&mut raw, &as_refs(&segs), true, &mut rng).unwrap_err(),
            FlatError::Foreign
        );
        // A short version value is Foreign (incorrect version length).
        let mut raw = Mem::default();
        raw.insert(VERSION_KEY, &[1, 0]).unwrap();
        let mut rng = Lcg::new(2);
        let segs = path_refs(&["a"]);
        assert_eq!(
            create_or_open(&mut raw, &as_refs(&segs), true, &mut rng).unwrap_err(),
            FlatError::Foreign
        );
    }

    #[test]
    fn an_allocated_prefix_is_not_free() {
        let mut raw = Mem::default();
        let mut rng = Lcg::new(1);
        let p = create(&mut raw, &mut rng, &[]).unwrap();
        // The layer row occupies the prefix's own node subspace: an
        // allocated prefix is never free for a new allocation.
        assert!(!is_prefix_free(&raw, &p).unwrap());
        // A candidate inside the node region (0xFE ...) is never free
        // either.
        assert!(!is_prefix_free(&raw, &[0xFE, 0x01, 0x7F]).unwrap());
    }

    #[test]
    fn validate_walks_a_populated_store() {
        let mut raw = Mem::default();
        let mut rng = Lcg::new(11);
        for name in ["x", "y"] {
            create(&mut raw, &mut rng, &[name]).unwrap();
        }
        let x = resolve(&mut raw, &mut Lcg::new(12), &["x"])
            .unwrap()
            .unwrap();
        let mut dk = x;
        dk.extend_from_slice(b"some-key");
        raw.insert(&dk, b"some-value").unwrap();
        let (data_rows, child_rows) = validate(&raw).unwrap();
        assert_eq!(data_rows, 1);
        assert!(child_rows >= 2);
    }

    #[test]
    fn validate_rejects_unknown_content() {
        let mut raw = Mem::default();
        let mut rng = Lcg::new(12);
        let _ = create(&mut raw, &mut rng, &[]).unwrap();
        // A row in the data region that does not start with a packed i64.
        raw.insert(&[0x03, 0x00], b"foreign").unwrap();
        assert_eq!(validate(&raw).unwrap_err(), FlatError::Foreign);
    }

    #[test]
    fn two_stores_same_ops_resolve_the_same_paths() {
        // The structural equivalence invariant (the determinism
        // invariant's replacement): two stores, same operation
        // sequence, may allocate different prefixes (allocation is
        // random) but must resolve every path to a working node with
        // the same data.
        let mut s1 = Mem::default();
        let mut s2 = Mem::default();
        let mut r1 = Lcg::new(101);
        let mut r2 = Lcg::new(202);
        for name in ["a", "b", "c"] {
            create(&mut s1, &mut r1, &[name]).unwrap();
            create(&mut s2, &mut r2, &[name]).unwrap();
        }
        for name in ["a", "b", "c"] {
            let p1 = resolve(&mut s1, &mut r1, &[name]).unwrap().unwrap();
            let p2 = resolve(&mut s2, &mut r2, &[name]).unwrap().unwrap();
            // A data row under each store's own prefix: same data,
            // different raw keys (different prefixes).
            let mut k1 = p1;
            k1.extend_from_slice(b"k");
            let mut k2 = p2;
            k2.extend_from_slice(b"k");
            s1.insert(&k1, b"v").unwrap();
            s2.insert(&k2, b"v").unwrap();
            assert_eq!(s1.get(&k1).unwrap(), Some(b"v".to_vec()));
            assert_eq!(s2.get(&k2).unwrap(), Some(b"v".to_vec()));
            // Both stores validate.
            assert!(validate(&s1).is_ok());
            assert!(validate(&s2).is_ok());
        }
    }

    /// Validate a healthy store after 129 allocations: three full HCA
    /// windows, advanced counter/recent rows, and 129 nodes.
    #[test]
    fn validate_passes_on_a_healthy_store_past_the_first_hca_window() {
        let mut raw = Mem::default();
        let mut rng = Lcg::new(7);
        for i in 0..129 {
            let name = format!("d{i}");
            create(&mut raw, &mut rng, &[name.as_str()]).expect("create the directory");
        }
        validate(&raw).expect("a healthy store must validate");
    }

    /// A node key whose prefix's packed form contains a `0x00` byte
    /// carries that byte escaped (`0x00 0xFF`) inside the BYTES
    /// element: prefix 256 packs as `[0x16, 0x01, 0x00]`, so the node
    /// key's escaped payload is `[0x16, 0x01, 0x00, 0xFF]`. The walk
    /// must unescape the payload before unpacking the prefix — a
    /// healthy store validates.
    #[test]
    fn validate_unescapes_a_node_prefix_with_an_embedded_zero() {
        let prefix = tuple::pack_i64(256);
        let node = node_subspace(&prefix);
        let mut raw = Mem::default();
        // The version row (1.0.0), the node's layer row, and a child
        // row targeting the node from the root node.
        let version = [1u32.to_le_bytes(), 0u32.to_le_bytes(), 0u32.to_le_bytes()].concat();
        raw.insert(VERSION_KEY, &version).unwrap();
        raw.insert(&layer_key(&node), b"").unwrap();
        raw.insert(&child_key(ROOT_NODE, "d"), &prefix).unwrap();
        validate(&raw).expect("a healthy store must validate");
    }
}
