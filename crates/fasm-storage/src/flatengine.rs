//! The flat directory layout engine over a [`RawKv`] view.
//!
//! One layout, written and tested once here; the flat backends
//! (btreemap, redb) implement [`RawKv`] over their transaction views and
//! route every operation through [`FlatEngine`]. FDB does not use this —
//! it uses FDB's native directory layer over the same row layouts (the
//! [`crate::flatdir`] module documents the shared layouts).
//!
//! The engine's layout is a verbatim port of FoundationDB's
//! `DirectoryLayer` (see [`crate::flatdir`]): a node tree of allocated
//! prefixes (packed `i64`s from the High Contention Allocator), a version
//! row, and HCA counter/recent rows, all under the `0xFE` node region.
//! Data keys are `prefix ‖ key` in the `0x00`–`0xFD` region; `0xFF` is
//! reserved.
//!
//! Stores are opened lazily: a fresh store (no version row) answers
//! empty on reads, and the first create writes the version row. A store
//! whose version row this layout does not read fails every operation
//! with [`FlatError::Foreign`].

use core::ops::Bound;
use rand::rng as thread_rng;

use crate::flatdir::layout::{ROOT_PATH_SEGMENT, data_region};
use crate::flatdir::ops;
use crate::key::validate_dir;
use crate::rawkv::RawKv;
use crate::stream::KvPair;

/// Re-exported for the backends' error types (defined with the layout
/// machinery it guards).
pub use crate::flatdir::error::FlatError;

/// The flat directory layout over a raw byte view.
///
/// Not itself a [`KvStore`](crate::KvStore): it has no async boundary and
/// no engine error identity. The flat backends wrap it.
///
/// Directory paths are the fasm form (`&[&[u8]]`, UTF-8, validated with
/// [`validate_dir`]); each fasm directory maps to the layer path
/// `[ROOT_PATH_SEGMENT] ‖ dir`, so the fasm root `[]` is the layer's
/// anchor directory — a real, allocated node, never the layer root.
pub struct FlatEngine<R: RawKv> {
    raw: R,
}

impl<R: RawKv> FlatEngine<R> {
    /// Wrap a raw view.
    pub fn new(raw: R) -> Self {
        Self { raw }
    }

    /// The underlying view, for backends that need to reach it directly
    /// (streaming, raw-handle accessors, the open-time probe).
    pub fn raw(&self) -> &R {
        &self.raw
    }

    /// The mutable underlying view.
    pub fn raw_mut(&mut self) -> &mut R {
        &mut self.raw
    }

    /// The layer path for a fasm directory: the anchor segment plus the
    /// directory's segments (already UTF-8-validated by the caller).
    fn layer_path<'a>(dir: &[&'a [u8]]) -> Vec<&'a [u8]> {
        let mut v: Vec<&'a [u8]> = Vec::with_capacity(dir.len() + 1);
        v.push(ROOT_PATH_SEGMENT.as_bytes());
        v.extend_from_slice(dir);
        v
    }

    // -- directory resolution ---------------------------------------------

    /// Resolve `dir` to its allocated prefix.
    ///
    /// `Ok(None)` when the store is fresh or the directory does not
    /// exist. `Err(FlatError::Foreign)` when the store's version row is
    /// one this layout does not read; `Err(FlatError::Corrupt)` when a
    /// node row's value is malformed. Write operations use
    /// [`allocate_dir`](Self::allocate_dir) instead.
    pub fn prefix_of(&self, dir: &[&[u8]]) -> Result<Option<Vec<u8>>, FlatError<R::Error>> {
        validate_dir(dir).map_err(FlatError::Key)?;
        ops::check_version_read(&self.raw)?;
        ops::find(&self.raw, &Self::layer_path(dir))
    }

    /// Resolve `dir`, allocating every missing directory (and only
    /// those) along the way — the anchor included, which is what writes
    /// the version row on a fresh store. Returns the resolved prefix.
    ///
    /// Creation is recursive on the *path*: creating `[a, b]` creates
    /// both `a` and `b` (lazy parent creation, verbatim from the layer).
    /// Each new node takes a candidate from the HCA; the allocation and
    /// the node rows land in the same transaction view, so a rolled-back
    /// creation leaves no trace. Idempotent: an existing directory
    /// resolves to its prefix without a new allocation.
    pub fn allocate_dir(&mut self, dir: &[&[u8]]) -> Result<Vec<u8>, FlatError<R::Error>> {
        validate_dir(dir).map_err(FlatError::Key)?;
        let mut rng = thread_rng();
        ops::create_or_open(&mut self.raw, &Self::layer_path(dir), true, &mut rng)
            .map(|p| p.expect("create_or_open with creation returns Some"))
    }

    // -- data operations ----------------------------------------------------

    /// Get `key` within `dir`. A missing directory yields `Ok(None)`.
    pub fn get(&self, dir: &[&[u8]], key: &[u8]) -> Result<Option<Vec<u8>>, FlatError<R::Error>> {
        let Some(prefix) = self.prefix_of(dir)? else {
            return Ok(None);
        };
        let mut raw_key = prefix;
        raw_key.extend_from_slice(key);
        Ok(self.raw.get(&raw_key)?)
    }

    /// Scan within `dir` between `start` and `end` (bounds over the key
    /// part), ascending or descending. A missing directory yields no rows.
    /// The returned keys are the within-directory keys.
    pub fn scan(
        &self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        forward: bool,
    ) -> Result<Vec<KvPair>, FlatError<R::Error>> {
        let Some(prefix) = self.prefix_of(dir)? else {
            return Ok(Vec::new());
        };
        let owned = map_bounds(&prefix, start, end);
        if raw_range_empty(&owned.0, &owned.1) {
            return Ok(Vec::new());
        }
        let (rs, re) = bounds_ref(&owned);
        let rows = self.raw.scan(rs, re, forward)?;
        let plen = prefix.len();
        Ok(rows
            .into_iter()
            .map(|pair| KvPair {
                key: pair.key[plen..].to_vec(),
                value: pair.value,
            })
            .collect())
    }

    /// The raw engine range holding `dir`'s data for `start <= k < end`
    /// (bounds over the within-directory key part): the resolved prefix
    /// plus the raw start/end byte bounds, or `Ok(None)` when `dir` does
    /// not exist or the bound pair names no keys.
    ///
    /// The seam for backends that stream a lazy cursor over the engine's
    /// raw range (one page or one reseek at a time) instead of
    /// [`scan`](Self::scan)'s materialized merge. The raw range is
    /// `[start, end)` in the engine's whole keyspace; the keys a cursor
    /// yields there carry the prefix, which the caller strips
    /// (`prefix.len()` bytes) to get the within-directory key.
    pub fn data_bounds(
        &self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<Option<DataBounds>, FlatError<R::Error>> {
        let Some(prefix) = self.prefix_of(dir)? else {
            return Ok(None);
        };
        let (rs, re) = map_bounds(&prefix, start, end);
        if raw_range_empty(&rs, &re) {
            return Ok(None);
        }
        Ok(Some(DataBounds {
            prefix,
            start: rs,
            end: re,
        }))
    }

    /// Set `key` within `dir`, allocating `dir` (and any missing
    /// ancestor, and the anchor) if needed.
    pub fn set(
        &mut self,
        dir: &[&[u8]],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), FlatError<R::Error>> {
        let prefix = self.allocate_dir(dir)?;
        let mut raw_key = prefix;
        raw_key.extend_from_slice(key);
        self.raw.insert(&raw_key, value)?;
        Ok(())
    }

    /// Delete `key` within `dir` (a no-op if the directory or the key
    /// does not exist).
    pub fn delete(&mut self, dir: &[&[u8]], key: &[u8]) -> Result<(), FlatError<R::Error>> {
        let Some(prefix) = self.prefix_of(dir)? else {
            return Ok(());
        };
        let mut raw_key = prefix;
        raw_key.extend_from_slice(key);
        self.raw.delete(&raw_key)?;
        Ok(())
    }

    /// Clear `start <= k < end` within `dir` (a no-op if the directory
    /// does not exist).
    pub fn clear_range(
        &mut self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), FlatError<R::Error>> {
        let Some(prefix) = self.prefix_of(dir)? else {
            return Ok(());
        };
        let owned = map_bounds(&prefix, start, end);
        if raw_range_empty(&owned.0, &owned.1) {
            return Ok(());
        }
        let (rs, re) = bounds_ref(&owned);
        self.raw.clear_range(rs, re)?;
        Ok(())
    }

    // -- directory navigation ----------------------------------------------

    /// The immediate child segment names under `dir`, sorted ascending.
    /// A missing directory yields an empty list.
    pub fn list_dirs(&self, dir: &[&[u8]]) -> Result<Vec<Vec<u8>>, FlatError<R::Error>> {
        validate_dir(dir).map_err(FlatError::Key)?;
        ops::check_version_read(&self.raw)?;
        let Some(prefix) = ops::find(&self.raw, &Self::layer_path(dir))? else {
            return Ok(Vec::new());
        };
        let names = ops::list_children(&self.raw, &prefix)?;
        Ok(names.into_iter().map(|s| s.into_bytes()).collect())
    }

    /// Whether `dir` exists. The fasm root `[]` exists iff its anchor
    /// node has been created (the store has been written); any other
    /// directory exists iff its node row is present — a directory with
    /// zero data keys still exists.
    pub fn dir_exists(&self, dir: &[&[u8]]) -> Result<bool, FlatError<R::Error>> {
        validate_dir(dir).map_err(FlatError::Key)?;
        ops::check_version_read(&self.raw)?;
        Ok(ops::find(&self.raw, &Self::layer_path(dir))?.is_some())
    }

    /// Remove `dir` recursively: its subdirectories (node rows and data)
    /// and its own key data. `Ok(true)` if it existed, `Ok(false)`
    /// otherwise. The root is not removable.
    pub fn remove_dir(&mut self, dir: &[&[u8]]) -> Result<bool, FlatError<R::Error>> {
        validate_dir(dir).map_err(FlatError::Key)?;
        if dir.is_empty() {
            // The fasm root maps to the layer's anchor directory — a
            // real node. Removing it would strand every directory under
            // it; the layer's own empty-path refusal is a second,
            // unreachable barrier.
            return Err(FlatError::Key(crate::key::KeyError::RootNotRemovable));
        }
        ops::check_version_read(&self.raw)?;
        ops::remove_dir(&mut self.raw, &Self::layer_path(dir))
    }
}

/// The raw engine range of one directory's data: the resolved prefix and
/// the raw byte bounds over the engine's whole keyspace. The start bound
/// carries its own inclusion (`Included`/`Excluded`); the end bound is
/// always `Excluded`. Cursor-yielding keys carry the prefix; strip
/// `prefix.len()` bytes to get the within-directory key.
pub struct DataBounds {
    /// The directory's allocated prefix.
    pub prefix: Vec<u8>,
    /// Raw start bound.
    pub start: Bound<Vec<u8>>,
    /// Raw end bound, exclusive.
    pub end: Bound<Vec<u8>>,
}

impl core::fmt::Debug for DataBounds {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Lengths only: a `Debug` panic line must not dump the raw bound
        // bytes, which embed the key material of a live directory.
        f.debug_struct("DataBounds")
            .field("prefix_len", &self.prefix.len())
            .field("start", &bound_len(&self.start))
            .field("end", &bound_len(&self.end))
            .finish()
    }
}

fn bound_len(b: &Bound<Vec<u8>>) -> String {
    match b {
        Bound::Unbounded => "unbounded".to_owned(),
        Bound::Included(v) => format!("incl({} bytes)", v.len()),
        Bound::Excluded(v) => format!("excl({} bytes)", v.len()),
    }
}

/// Borrow the owned bounds of a mapped range as slice bounds, for
/// [`RawKv::scan`]/[`RawKv::clear_range`].
fn bounds_ref(b: &(Bound<Vec<u8>>, Bound<Vec<u8>>)) -> (Bound<&[u8]>, Bound<&[u8]>) {
    (bound_ref(&b.0), bound_ref(&b.1))
}

fn bound_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
    }
}

/// The exclusive raw end of `prefix`'s data region.
///
/// [`data_region`] returns a `Bound`; this is its `Excluded` byte vector,
/// for callers that need the owned form.
fn data_end_exclusive(prefix: &[u8]) -> Vec<u8> {
    match data_region(prefix).1 {
        Bound::Excluded(b) => b,
        _ => unreachable!("data_region end is always Excluded"),
    }
}

/// Map key-part bounds onto the raw range under `prefix`.
///
/// Within one directory the mapping `prefix ‖ k` is monotone, so the raw
/// range stays inside the directory's data region:
///
/// - start `Included(k)`  -> raw `Included(prefix ‖ k)`
/// - start `Excluded(k)`  -> raw `Excluded(prefix ‖ k)`
/// - start unbounded      -> raw `Included(prefix)` (the region start: the
///   directory's empty key sits at the prefix itself)
/// - end `Excluded(k)`    -> raw `Excluded(prefix ‖ k)`
/// - end `Included(k)`    -> raw `Excluded(prefix ‖ k ‖ [0x00])`:
///   `prefix ‖ k ‖ [0x00]` is the raw lexicographic immediate successor of
///   the key's raw form (a byte string's smallest greater byte string is
///   itself with a `0x00` appended), and it always lies inside the region
///   because it is an extension of `prefix`
/// - end unbounded        -> raw `Excluded` of the region end
///   (`next_prefix(prefix)`)
///
/// Note the two successor notions: the end bound uses the raw immediate
/// successor (`‖ [0x00]`), while region ends use the prefix-block successor
/// ([`crate::keyspace::next_prefix`]).
///
/// The range may name no keys (for example `Excluded(k)` to
/// `Excluded(k)`); callers check with [`raw_range_empty`].
fn map_bounds(
    prefix: &[u8],
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let mk = |k: &[u8]| {
        let mut raw = prefix.to_vec();
        raw.extend_from_slice(k);
        raw
    };
    let rs = match start {
        Bound::Unbounded => Bound::Included(prefix.to_vec()),
        Bound::Included(k) => Bound::Included(mk(k)),
        Bound::Excluded(k) => Bound::Excluded(mk(k)),
    };
    let re = match end {
        Bound::Unbounded => Bound::Excluded(data_end_exclusive(prefix)),
        Bound::Excluded(k) => Bound::Excluded(mk(k)),
        Bound::Included(k) => {
            let mut raw = mk(k);
            raw.push(0x00);
            Bound::Excluded(raw)
        }
    };
    (rs, re)
}

/// Whether a raw `(start, end)` bound pair names no keys at all.
///
/// Only `Excluded` end bounds occur (see [`map_bounds`]), so the range is
/// empty exactly when the start bound reaches or passes the end bound.
fn raw_range_empty(start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    match (start, end) {
        (Bound::Included(a), Bound::Excluded(b)) => a >= b,
        (Bound::Excluded(a), Bound::Excluded(b)) => a >= b,
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => false,
        _ => unreachable!("end bounds are always Excluded"),
    }
}

// -- test double -----------------------------------------------------------

/// An in-memory [`RawKv`] for testing the layout independently of any
/// backend. Shared by the flatdir module's test suites.
#[cfg(test)]
pub(crate) struct Mem {
    map: core::cell::RefCell<std::collections::BTreeMap<Vec<u8>, Vec<u8>>>,
}

#[cfg(test)]
impl Default for Mem {
    fn default() -> Self {
        Self {
            map: core::cell::RefCell::new(std::collections::BTreeMap::new()),
        }
    }
}

#[cfg(test)]
impl RawKv for Mem {
    type Error = std::convert::Infallible;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.map.borrow().get(key).cloned())
    }

    fn scan(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        forward: bool,
    ) -> Result<Vec<KvPair>, Self::Error> {
        let map = self.map.borrow();
        let rows: Vec<KvPair> = map
            .iter()
            .filter(|(k, _)| {
                let k = &k[..];
                let above = match start {
                    Bound::Unbounded => true,
                    Bound::Included(s) => k >= s,
                    Bound::Excluded(s) => k > s,
                };
                let below = match end {
                    Bound::Unbounded => true,
                    Bound::Excluded(e) => k < e,
                    Bound::Included(e) => k <= e,
                };
                above && below
            })
            .map(|(k, v)| KvPair {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();
        if forward {
            Ok(rows)
        } else {
            Ok(rows.into_iter().rev().collect())
        }
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.map.borrow_mut().insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        self.map.borrow_mut().remove(key);
        Ok(())
    }

    fn clear_range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<(), Self::Error> {
        let mut map = self.map.borrow_mut();
        let keys: Vec<Vec<u8>> = map
            .keys()
            .filter(|k| {
                let k = &k[..];
                let above = match start {
                    Bound::Unbounded => true,
                    Bound::Included(s) => k >= s,
                    Bound::Excluded(s) => k > s,
                };
                let below = match end {
                    Bound::Unbounded => true,
                    Bound::Excluded(e) => k < e,
                    Bound::Included(e) => k <= e,
                };
                above && below
            })
            .cloned()
            .collect();
        for k in keys {
            map.remove(&k);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::KeyError;

    fn engine() -> FlatEngine<Mem> {
        FlatEngine::new(Mem::default())
    }

    fn ok<T>(r: Result<T, FlatError<std::convert::Infallible>>) -> T {
        r.unwrap()
    }

    // -- fresh stores ------------------------------------------------------

    #[test]
    fn fresh_reads_return_empty_and_first_write_initialises() {
        let mut e = engine();
        assert_eq!(ok(e.prefix_of(&[])), None);
        assert_eq!(ok(e.get(&[], b"k")), None);
        assert!(ok(e.scan(&[], Bound::Unbounded, Bound::Unbounded, true)).is_empty());
        assert!(ok(e.list_dirs(&[])).is_empty());
        assert!(!ok(e.dir_exists(&[])));
        // Removing from a fresh store is a miss, not an error.
        assert!(!ok(e.remove_dir(&[b"a"])));
        // The first write initialises the store lazily (version row,
        // anchor node).
        ok(e.set(&[], b"k", b"v"));
        assert!(ok(e.dir_exists(&[])));
        assert_eq!(ok(e.get(&[], b"k")), Some(b"v".to_vec()));
        // The version row is the layout's 12-byte 1.0.0.
        let version = e
            .raw()
            .map
            .borrow()
            .get(crate::flatdir::layout::VERSION_KEY)
            .cloned();
        assert_eq!(
            version,
            Some(crate::flatdir::layout::LAYOUT_VERSION.to_vec())
        );
    }

    /// A first write in a one-segment directory produces exactly nine
    /// raw rows: the version row, the anchor node's child and layer
    /// rows, the directory node's child and layer rows, the HCA
    /// counter row, two HCA recent rows, and the data row. The root
    /// node carries no layer row.
    #[test]
    fn a_first_set_produces_exactly_nine_raw_rows() {
        let mut e = engine();
        ok(e.set(&[b"a"], b"k", b"v"));
        let map = e.raw().map.borrow();
        assert_eq!(map.len(), 9, "unexpected raw rows: {map:?}");
    }

    // -- foreign stores ------------------------------------------------------

    #[test]
    fn reads_on_a_foreign_store_fail_closed() {
        // A version row this layout does not read (major = 2): every
        // operation fails with `Foreign` rather than answering over
        // someone else's data.
        let mut e = engine();
        let mut v = [0u8; 12];
        v[0..4].copy_from_slice(&2u32.to_le_bytes());
        e.raw_mut()
            .insert(crate::flatdir::layout::VERSION_KEY, &v)
            .unwrap();
        assert_eq!(e.prefix_of(&[]), Err(FlatError::Foreign));
        assert_eq!(e.get(&[], b"k"), Err(FlatError::Foreign));
        assert_eq!(
            e.scan(&[], Bound::Unbounded, Bound::Unbounded, true),
            Err(FlatError::Foreign)
        );
        assert!(matches!(
            e.data_bounds(&[], Bound::Unbounded, Bound::Unbounded),
            Err(FlatError::Foreign)
        ));
        assert_eq!(e.dir_exists(&[]), Err(FlatError::Foreign));
        assert_eq!(e.list_dirs(&[]), Err(FlatError::Foreign));
        assert_eq!(e.set(&[], b"k", b"v"), Err(FlatError::Foreign));
        assert_eq!(e.remove_dir(&[b"a"]), Err(FlatError::Foreign));
    }

    #[test]
    fn a_malformed_version_row_fails_closed() {
        // A short version value is not a version: fail closed.
        let mut e = engine();
        e.raw_mut()
            .insert(crate::flatdir::layout::VERSION_KEY, &[1, 0])
            .unwrap();
        assert_eq!(e.dir_exists(&[]), Err(FlatError::Foreign));
    }

    #[test]
    fn a_corrupt_node_value_is_corrupt_not_a_panic() {
        // A healthy store whose anchor child row's value is then flipped
        // to a non-packing byte string: the node walk reports `Corrupt`
        // rather than panicking.
        let mut e = engine();
        e.set(&[b"dir"], b"k", b"v").unwrap();
        // The anchor child row sits under the root node and is the only
        // row there tagged with INT(0) (0x14): the HCA and version rows
        // are tagged 0x01 (BYTES) and sort before it.
        let child_tag = crate::flatdir::layout::ROOT_NODE
            .iter()
            .copied()
            .chain(std::iter::once(0x14))
            .collect::<Vec<u8>>();
        let row_key = e
            .raw()
            .map
            .borrow()
            .iter()
            .find(|(k, _)| k.starts_with(&child_tag))
            .expect("the anchor child row")
            .0
            .clone();
        e.raw_mut()
            .map
            .borrow_mut()
            .insert(row_key, vec![0x99, 0x00]);
        assert_eq!(e.get(&[b"dir"], b"k"), Err(FlatError::Corrupt));
        assert_eq!(
            e.scan(&[b"dir"], Bound::Unbounded, Bound::Unbounded, true),
            Err(FlatError::Corrupt)
        );
        assert_eq!(e.remove_dir(&[b"dir"]), Err(FlatError::Corrupt));
    }

    // -- allocation ---------------------------------------------------------

    #[test]
    fn allocation_is_recursive_and_stable() {
        let mut e = engine();
        let p = ok(e.allocate_dir(&[b"a", b"b"]));
        // Re-resolving finds the same prefix (no new allocation).
        assert_eq!(ok(e.prefix_of(&[b"a", b"b"])), Some(p.clone()));
        // Siblings get different prefixes; the ancestor resolves too.
        let q = ok(e.allocate_dir(&[b"a", b"c"]));
        assert_ne!(p, q);
        assert!(ok(e.prefix_of(&[b"a"])).is_some());
        // Every allocated prefix starts in the packed-i64 band
        // (0x0c..=0x1c): never the meta (0xFE) or reserved (0xFF) region.
        let dirs: [[&[u8]; 2]; 2] = [[b"a" as &[u8], b"b"], [b"a" as &[u8], b"c"]];
        for dir in dirs {
            let p = ok(e.prefix_of(&dir)).unwrap();
            assert!((0x0c..=0x1c).contains(&p[0]), "prefix {p:?}");
        }
    }

    #[test]
    fn many_allocations_stay_in_the_data_region() {
        // 300 directories (past the first HCA window advance): every
        // allocated prefix stays in the data region and every data key
        // lands outside the meta region.
        let mut e = engine();
        for i in 0..300u32 {
            let d = format!("d{i:03}").into_bytes();
            let d = [d.as_slice()];
            let p = ok(e.allocate_dir(&d));
            assert!((0x0c..=0x1c).contains(&p[0]), "prefix {p:?} at {i}");
        }
    }

    #[test]
    fn resolve_missing_dir_is_none() {
        let mut e = engine();
        ok(e.set(&[], b"k", b"v"));
        assert_eq!(ok(e.prefix_of(&[b"nope"])), None);
        assert_eq!(ok(e.prefix_of(&[b"nope", b"deeper"])), None);
    }

    // -- data operations ----------------------------------------------------

    #[test]
    fn set_get_delete_round_trips_per_dir() {
        let mut e = engine();
        ok(e.set(&[b"asset", b"btc"], b"k", b"v"));
        assert_eq!(ok(e.get(&[b"asset", b"btc"], b"k")), Some(b"v".to_vec()));
        // Same key, different directory: isolated.
        assert_eq!(ok(e.get(&[b"asset"], b"k")), None);
        assert_eq!(ok(e.get(&[b"btc"], b"k")), None);
        ok(e.delete(&[b"asset", b"btc"], b"k"));
        assert_eq!(ok(e.get(&[b"asset", b"btc"], b"k")), None);
    }

    #[test]
    fn scan_orders_within_dir_only() {
        let mut e = engine();
        for k in [b"b", b"a", b"c"] {
            ok(e.set(&[b"d"], k, k));
            ok(e.set(&[b"other"], k, k));
        }
        let rows = ok(e.scan(&[b"d"], Bound::Unbounded, Bound::Unbounded, true));
        let keys: Vec<Vec<u8>> = rows.iter().map(|p| p.key.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        let rows = ok(e.scan(&[b"d"], Bound::Unbounded, Bound::Unbounded, false));
        assert_eq!(rows.first().unwrap().key, b"c".to_vec());
    }

    #[test]
    fn scan_bound_matrix() {
        let mut e = engine();
        for k in [b"a", b"b", b"c", b"\xff"] {
            ok(e.set(&[b"d"], k, b"v"));
        }
        let get = |s: core::ops::Bound<&[u8]>,
                   en: core::ops::Bound<&[u8]>,
                   e: &FlatEngine<Mem>|
         -> Vec<Vec<u8>> {
            ok(e.scan(&[b"d"], s, en, true))
                .into_iter()
                .map(|pair| pair.key)
                .collect()
        };
        assert_eq!(
            get(Bound::Included(b"b"), Bound::Excluded(b"c"), &e),
            vec![b"b".to_vec()]
        );
        assert_eq!(
            get(Bound::Excluded(b"b"), Bound::Included(b"c"), &e),
            vec![b"c".to_vec()]
        );
        assert_eq!(
            get(Bound::Excluded(&[0xFFu8]), Bound::Unbounded, &e),
            Vec::<Vec<u8>>::new()
        );
        assert_eq!(
            get(Bound::Unbounded, Bound::Included(&[0xFFu8]), &e),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), vec![0xFF]]
        );
        assert_eq!(
            get(Bound::Included(&[0xFFu8]), Bound::Unbounded, &e),
            vec![vec![0xFF]]
        );
        // An Excluded bound at all-0xFF: the range is empty.
        assert_eq!(
            get(Bound::Excluded(&[0xFFu8]), Bound::Excluded(&[0xFFu8]), &e),
            Vec::<Vec<u8>>::new()
        );
    }

    #[test]
    fn clear_range_clears_within_dir_only() {
        let mut e = engine();
        for k in [b"a", b"b", b"c"] {
            ok(e.set(&[b"d"], k, b"v"));
            ok(e.set(&[b"e"], k, b"v"));
        }
        ok(e.clear_range(&[b"d"], Bound::Included(b"b"), Bound::Excluded(b"c")));
        assert_eq!(ok(e.get(&[b"d"], b"a")), Some(b"v".to_vec()));
        assert_eq!(ok(e.get(&[b"d"], b"b")), None);
        assert_eq!(ok(e.get(&[b"d"], b"c")), Some(b"v".to_vec()));
        assert_eq!(ok(e.get(&[b"e"], b"b")), Some(b"v".to_vec()));
        // Whole directory.
        ok(e.clear_range(&[b"d"], Bound::Unbounded, Bound::Unbounded));
        assert!(ok(e.scan(&[b"d"], Bound::Unbounded, Bound::Unbounded, true)).is_empty());
        // The directory still exists (data-free directories are entities).
        assert!(ok(e.dir_exists(&[b"d"])));
    }

    // -- navigation ---------------------------------------------------------

    #[test]
    fn nav_list_exists_remove() {
        let mut e = engine();
        ok(e.set(&[b"a", b"1"], b"k", b"v"));
        ok(e.set(&[b"a", b"2"], b"k", b"v"));
        ok(e.set(&[b"b"], b"k", b"v"));
        assert_eq!(ok(e.list_dirs(&[])), vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(ok(e.list_dirs(&[b"a"])), vec![b"1".to_vec(), b"2".to_vec()]);
        assert!(ok(e.dir_exists(&[b"a"])));
        assert!(ok(e.dir_exists(&[b"a", b"1"])));
        assert!(!ok(e.dir_exists(&[b"a", b"3"])));
        // A directory with no data keys still exists.
        ok(e.allocate_dir(&[b"empty"]));
        assert!(ok(e.dir_exists(&[b"empty"])));
        assert!(ok(e.list_dirs(&[b"empty"])).is_empty());
        // Missing: empty list, not an error.
        assert!(ok(e.list_dirs(&[b"nope"])).is_empty());
        assert!(!ok(e.dir_exists(&[b"nope"])));
    }

    #[test]
    fn nav_remove_is_recursive() {
        let mut e = engine();
        ok(e.set(&[b"a", b"b", b"c"], b"k1", b"v1"));
        ok(e.set(&[b"a", b"b"], b"k2", b"v2"));
        ok(e.set(&[b"a", b"d"], b"k3", b"v3"));
        ok(e.set(&[b"z"], b"k4", b"v4"));
        // Remove the middle directory: b/c and b go, a/d stays.
        assert!(ok(e.remove_dir(&[b"a", b"b"])));
        assert_eq!(ok(e.get(&[b"a", b"b", b"c"], b"k1")), None);
        assert_eq!(ok(e.get(&[b"a", b"b"], b"k2")), None);
        assert_eq!(ok(e.get(&[b"a", b"d"], b"k3")), Some(b"v3".to_vec()));
        assert!(ok(e.dir_exists(&[b"a"])));
        assert!(!ok(e.dir_exists(&[b"a", b"b"])));
        // Removing a leaf directory removes it and its data.
        assert!(ok(e.remove_dir(&[b"a", b"d"])));
        assert_eq!(ok(e.get(&[b"a", b"d"], b"k3")), None);
        // Missing: false.
        assert!(!ok(e.remove_dir(&[b"a", b"b"])));
        // Unaffected directories keep their data.
        assert_eq!(ok(e.get(&[b"z"], b"k4")), Some(b"v4".to_vec()));
    }

    #[test]
    fn nav_root_is_not_removable() {
        let mut e = engine();
        ok(e.set(&[], b"k", b"v"));
        assert_eq!(
            e.remove_dir(&[]),
            Err(FlatError::Key(KeyError::RootNotRemovable))
        );
        // The root survived.
        assert!(ok(e.dir_exists(&[])));
    }

    #[test]
    fn nav_rejects_non_utf8() {
        let mut e = engine();
        let seg: &[u8] = b"\xFF\xFE";
        let bad = [seg];
        assert_eq!(
            e.list_dirs(&bad),
            Err(FlatError::Key(KeyError::DirSegmentNotUtf8 { segment: 0 }))
        );
        assert_eq!(
            e.remove_dir(&bad),
            Err(FlatError::Key(KeyError::DirSegmentNotUtf8 { segment: 0 }))
        );
    }

    #[test]
    fn remove_and_recreate_work() {
        // Removal leaves no trace that blocks re-creation: the HCA may
        // even re-claim the same candidate value (FDB semantics — the
        // allocator tracks windows, not used prefixes); what matters is
        // that the recreated directory is fully functional and no stale
        // row of the old one is read.
        let mut e = engine();
        ok(e.set(&[b"x"], b"k", b"v1"));
        let p1 = ok(e.prefix_of(&[b"x"])).unwrap();
        assert!(ok(e.remove_dir(&[b"x"])));
        assert!(!ok(e.dir_exists(&[b"x"])));
        assert_eq!(ok(e.get(&[b"x"], b"k")), None);
        ok(e.set(&[b"x"], b"k", b"v2"));
        let p2 = ok(e.prefix_of(&[b"x"])).unwrap();
        assert_eq!(ok(e.get(&[b"x"], b"k")), Some(b"v2".to_vec()));
        // If the allocator re-claimed the same prefix, the old data row
        // was cleared by the removal (same raw key, new value wins).
        if p1 == p2 {
            assert_eq!(
                ok(e.scan(&[b"x"], Bound::Unbounded, Bound::Unbounded, true)).len(),
                1
            );
        }
        // The store still validates as a whole layout.
        assert!(ops::validate(e.raw()).is_ok());
    }
}
