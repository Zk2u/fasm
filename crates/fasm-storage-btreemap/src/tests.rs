//! Store-backed tests for the in-memory [`BTreeMapStore`] backend and the
//! [`ScopedKvStore`](fasm_storage::ScopedKvStore) adapter built on it.
//!
//! This backend is **transactional**. [`BTreeMapStore`] is the committed base
//! (raw layout rows) and the opener; every test's store handle is a fresh
//! [`BTreeMapTransaction`] whose buffered writes are read-your-writes, are
//! applied to the base with [`Commit`], and roll back on drop. The state
//! machines built on `fasm-storage` rely on exactly that contract, so the
//! conformance suite runs against the transaction handle: an in-memory test
//! and a durable-backend test exercise the same semantics.
//!
//! The suite covers the conformance tests, the scoped-store behaviour, and
//! the transaction/reference-model property tests: this crate holds the
//! in-process store the whole workspace is tested against.
//!
//! `fasm-storage` keeps its pure-logic tests and deliberately has no
//! dependency on this crate: a dev-dependency cycle would make `KvStore`
//! compile as two distinct instances.

use core::future::Future;
use core::ops::Bound;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::collections::BTreeMap;

use proptest::prelude::*;
use proptest::test_runner::Config as ProptestConfig;

use fasm_storage::{
    Commit, FlatError, KvDirNav, KvPair, KvStore, RawKv, RetryableStorageError, ScopedKvStore,
    bound_as_slice, flatdir,
};

use crate::{BTreeMapError, BTreeMapStore, BTreeMapTransaction, BufViewError};

/// A read-only [`RawKv`] view over an owned raw-row map, for running the
/// layout's `validate` against a dumped table.
struct MapRawKv(BTreeMap<Vec<u8>, Vec<u8>>);

impl MapRawKv {
    fn bounds_hit(bound: Bound<&[u8]>, k: &[u8]) -> bool {
        match bound {
            Bound::Unbounded => true,
            Bound::Included(x) => k >= x,
            Bound::Excluded(x) => k < x,
        }
    }
}

impl RawKv for MapRawKv {
    type Error = fasm_storage::FlatError<core::convert::Infallible>;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.0.get(key).cloned())
    }

    fn scan(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        forward: bool,
    ) -> Result<Vec<KvPair>, Self::Error> {
        let rows = self
            .0
            .iter()
            .filter(|(k, _)| {
                Self::bounds_hit(start, k.as_slice()) && Self::bounds_hit(end, k.as_slice())
            })
            .map(|(k, v)| KvPair {
                key: k.clone(),
                value: v.clone(),
            })
            .collect::<Vec<_>>();
        Ok(if forward {
            rows
        } else {
            rows.into_iter().rev().collect()
        })
    }

    fn insert(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        unreachable!("validate is read-only")
    }

    fn delete(&mut self, _key: &[u8]) -> Result<(), Self::Error> {
        unreachable!("validate is read-only")
    }

    fn clear_range(&mut self, _start: Bound<&[u8]>, _end: Bound<&[u8]>) -> Result<(), Self::Error> {
        unreachable!("validate is read-only")
    }
}

// ============================================================================
// Shared test support
//
// `fasm-storage` carries its own private copy of these helpers; a separate
// crate cannot reach a `pub(crate)` item, so this crate keeps its own copy of
// the executor, the key/value alphabets, and the reference bound predicate.
// ============================================================================

/// Minimal executor for this crate's own tests.
///
/// Hand-rolled rather than pulling in `futures` as a dev-dependency: this
/// crate is the workspace's in-memory storage layer and its only reason to
/// grow a dependency would be a real one. `Waker::noop()` has been stable
/// since 1.85 (which edition 2024 already requires), so this needs no
/// `unsafe`.
///
/// Every future produced by [`BTreeMapTransaction`] and [`ScopedKvStore`] is
/// pure CPU work over an in-process `BTreeMap`: it has nothing to wait on and
/// must complete on the first poll. So `Pending` is not a scheduling event to
/// spin on, it is a bug — and panicking on it makes this executor an
/// assertion rather than a busy loop. A backend crate with real I/O supplies
/// its own runtime through the conformance macro's `block_on` parameter.
pub(crate) fn block_on<F: Future>(fut: F) -> F::Output {
    let fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    match fut.poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => {
            panic!("btreemap test future returned Pending; in-memory stores must never yield")
        }
    }
}

/// A fresh empty store (the committed base plus its opener).
fn open_store() -> BTreeMapStore {
    BTreeMapStore::new()
}

/// A fresh write transaction over a fresh empty store: the default test
/// handle. The store is unreachable from the transaction, so tests that need
/// to inspect the committed base build it themselves via [`open_store`].
fn open_transaction() -> BTreeMapTransaction {
    open_store().transaction()
}

/// Arbitrary key bytes, drawn from a deliberately tiny alphabet.
///
/// Uniform random bytes would make every generated key distinct and every
/// prefix relationship impossible, which is exactly the structure these
/// properties are about. `0xFF` is over-represented because the successor-less
/// prefix is the interesting edge in
/// [`next_prefix`](fasm_storage::next_prefix), and `0x00`/`0x01` and
/// `0xFE`/`0xFF` are adjacent so that one key can sit exactly one step above
/// another. The empty key is reachable because it is a legal key that sorts
/// first.
fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    let byte = prop_oneof![
        3 => Just(0xFFu8),
        3 => Just(0x00u8),
        2 => Just(0x01u8),
        1 => Just(0xFEu8),
        1 => any::<u8>(),
    ];
    prop::collection::vec(byte, 0..5)
}

/// Arbitrary value bytes. Values are opaque to the store, so the only thing
/// worth varying is the length, including empty.
fn arb_value() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..4)
}

/// Arbitrary directory segments: short UTF-8 names so that generated
/// directories stay tiny and can be compared. The alphabet is ASCII (a
/// subset of UTF-8): the trait contract rejects non-UTF-8 segments, so the
/// model never generates one.
fn arb_seg() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(0x61u8..=0x63u8, 1..3).prop_map(|bytes| {
        String::from_utf8(bytes)
            .expect("ASCII is UTF-8")
            .into_bytes()
    })
}

/// Arbitrary directory paths: 0..2 segments.
fn arb_dir() -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::vec(arb_seg(), 0..2)
}

/// Arbitrary range bounds, including inverted and equal endpoints.
///
/// The reference model naturally selects no keys for an inverted interval, so
/// generating both key orders keeps every backend honest about the normative
/// empty-range contract.
fn arb_bounds() -> impl Strategy<Value = (Bound<Vec<u8>>, Bound<Vec<u8>>)> {
    (arb_key(), arb_key(), 0u8..3, 0u8..3).prop_map(|(a, b, start_kind, end_kind)| {
        let start = match start_kind {
            0 => Bound::Unbounded,
            1 => Bound::Included(a),
            _ => Bound::Excluded(a),
        };
        let end = match end_kind {
            0 => Bound::Unbounded,
            1 => Bound::Included(b),
            _ => Bound::Excluded(b),
        };
        (start, end)
    })
}

/// Whether `key` lies inside `bounds`, by plain lexicographic byte comparison.
///
/// The reference answer the store's own range logic is checked against.
fn bounds_contain(bounds: &(Bound<Vec<u8>>, Bound<Vec<u8>>), key: &[u8]) -> bool {
    let above_start = match &bounds.0 {
        Bound::Included(start) => key >= start.as_slice(),
        Bound::Excluded(start) => key > start.as_slice(),
        Bound::Unbounded => true,
    };
    let below_end = match &bounds.1 {
        Bound::Included(end) => key <= end.as_slice(),
        Bound::Excluded(end) => key < end.as_slice(),
        Bound::Unbounded => true,
    };
    above_start && below_end
}

/// Collect a full scan of `dir` as `(key, value)` pairs.
async fn scan_all<KV: KvStore>(
    store: &KV,
    dir: &[&[u8]],
    reverse: bool,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    store
        .range(dir, Bound::Unbounded, Bound::Unbounded, reverse)
        .collect()
        .await
        .expect("scan must succeed")
        .into_iter()
        .map(|pair| (pair.key, pair.value))
        .collect()
}

/// Collect a bounded range scan within `dir` as `(key, value)` pairs.
async fn scan_bounded<KV: KvStore>(
    store: &KV,
    dir: &[&[u8]],
    bounds: &(Bound<Vec<u8>>, Bound<Vec<u8>>),
    reverse: bool,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    store
        .range(
            dir,
            bound_as_slice(&bounds.0),
            bound_as_slice(&bounds.1),
            reverse,
        )
        .collect()
        .await
        .expect("scan must succeed")
        .into_iter()
        .map(|pair| (pair.key, pair.value))
        .collect()
}

// ============================================================================
// Conformance: the scopes the suite must pass
// ============================================================================

mod btreemap_conformance {
    // The store handle is a write transaction: the conformance suite must
    // pass on the same transactional surface the durable backends expose.
    fasm_storage::kv_store_tests!(
        store = super::open_transaction(),
        block_on = super::block_on,
    );
    // Its caller-space root is the engine root, so navigation conformance
    // applies too.
    fasm_storage::kv_nav_tests!(
        store = super::open_transaction(),
        block_on = super::block_on,
    );
}

mod scoped_conformance {
    use fasm_storage::ScopedKvStore;

    fasm_storage::kv_store_tests!(
        store = ScopedKvStore::new(
            super::open_transaction(),
            vec![b"a".to_vec(), b"b".to_vec()]
        ),
        block_on = super::block_on,
    );
}

mod scoped_deep_conformance {
    use fasm_storage::ScopedKvStore;

    fasm_storage::kv_store_tests!(
        store = ScopedKvStore::new(
            super::open_transaction(),
            vec![b"x".to_vec(), b"y".to_vec(), b"z".to_vec()]
        ),
        block_on = super::block_on,
    );
}

mod root_pinned_conformance {
    use fasm_storage::ScopedKvStore;

    // An empty pin is a transparent view of the whole store, root and all.
    fasm_storage::kv_store_tests!(
        store = ScopedKvStore::new(super::open_transaction(), Vec::new()),
        block_on = super::block_on,
    );
    fasm_storage::kv_nav_tests!(
        store = ScopedKvStore::new(super::open_transaction(), Vec::new()),
        block_on = super::block_on,
    );
}

// ============================================================================
// BTreeMapStore / BTreeMapTransaction
// ============================================================================

#[test]
fn btreemap_store_reports_committed_rows_and_raw_keys() {
    block_on(async {
        let store = open_store();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);

        let mut tx = store.transaction();
        tx.set(&[b"a"], b"k", b"v").await.expect("set k");
        // Uncommitted: the base still reports nothing.
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);

        tx.commit().await.expect("commit");
        // One data key in one directory materialises the lazily written
        // layout: version row, anchor child + layer rows, the directory
        // child + layer rows, HCA counter + two recent rows, and the data
        // row.
        assert!(!store.is_empty());
        assert_eq!(store.len(), 9);
        let rows = raw_map(&store);
        assert_eq!(
            flatdir::parse_version(
                rows.get(flatdir::VERSION_KEY)
                    .expect("version row")
                    .as_slice()
            ),
            Some((1, 0, 0))
        );
        // The anchor's child row is the reserved first element under the
        // root node.
        let anchor_child =
            flatdir::layout::child_key(flatdir::layout::ROOT_NODE, flatdir::ROOT_PATH_SEGMENT);
        assert!(rows.contains_key(&anchor_child), "anchor child row missing");
        // The committed raw map holds exactly one data value.
        let keys = store.keys();
        let data_rows: Vec<&Vec<u8>> = keys
            .iter()
            .filter(|k| (0x0c..=0x1c).contains(&k.first().copied().unwrap_or(0xFF)))
            .collect();
        assert_eq!(data_rows.len(), 1);
        assert_eq!(
            store.get_committed(data_rows[0].as_slice()),
            Some(b"v".to_vec())
        );
        // The layout is self-consistent.
        flatdir::ops::validate(&MapRawKv(rows)).expect("the layout must validate");

        // A second directory with one data row adds exactly four rows:
        // the child row, the layer row, the data row, and the HCA recent
        // row for the new allocation.
        let mut tx2 = store.transaction();
        tx2.set(&[b"b"], b"k2", b"v2").await.expect("set b");
        tx2.commit().await.expect("commit b");
        assert_eq!(store.len(), 13);

        store.clear();
        assert!(store.is_empty());
    });
}

/// The reference operation sequence for the deterministic-layout test: a
/// mixed write/delete/clear pattern across the root and two nested
/// directories.
async fn run_reference_ops(tx: &mut BTreeMapTransaction) {
    tx.set(&[], b"k", b"v1").await.expect("set k");
    tx.set(&[b"a"], b"x", b"y").await.expect("set x");
    tx.set(&[b"a", b"b"], b"m", b"n").await.expect("set m");
    tx.delete(&[], b"k").await.expect("delete k");
    tx.set(&[b"a"], b"x", b"y2").await.expect("set x2");
    tx.clear_range(&[b"a"], Bound::Unbounded, Bound::Unbounded)
        .await
        .expect("clear a");
}

fn raw_map(store: &BTreeMapStore) -> std::collections::BTreeMap<Vec<u8>, Vec<u8>> {
    store
        .keys()
        .into_iter()
        .map(|k| (k.clone(), store.get_committed(&k).expect("committed row")))
        .collect()
}

#[test]
fn the_version_entry_is_1_0_0_after_any_write() {
    block_on(async {
        let store = open_store();
        let mut tx = store.transaction();
        tx.set(&[b"swap"], b"k", b"v").await.expect("set");
        tx.commit().await.expect("commit");
        let rows = raw_map(&store);
        assert_eq!(
            flatdir::parse_version(
                rows.get(flatdir::VERSION_KEY)
                    .expect("version row")
                    .as_slice()
            ),
            Some((1, 0, 0))
        );
    });
}

#[test]
fn the_same_operations_resolve_the_same_paths_and_validate() {
    block_on(async {
        let s1 = open_store();
        let s2 = open_store();
        let mut t1 = s1.transaction();
        let mut t2 = s2.transaction();
        run_reference_ops(&mut t1).await;
        run_reference_ops(&mut t2).await;
        t1.commit().await.expect("commit 1");
        t2.commit().await.expect("commit 2");

        // The HCA samples its candidate within the window (the verbatim
        // FDB contention-distribution design), so the two stores' raw
        // layouts are not byte-identical. What is guaranteed: the same
        // visible data, and a self-consistent layout on both.
        for s in [&s1, &s2] {
            let rows = raw_map(s);
            // The reference ops' end state is a deterministic 12-row
            // layout: the version row, the HCA counter row, one child
            // row and one layer row per directory (the harness root
            // segment, `a`, and `a.b`), one HCA recent row per
            // allocation (three in total), and the one surviving data
            // row.
            assert_eq!(rows.len(), 12, "unexpected raw rows: {rows:?}");
            flatdir::ops::validate(&MapRawKv(rows)).expect("the layout must validate");
        }
        let r1 = s1.transaction();
        let r2 = s2.transaction();
        // The reference ops deleted the root's only key.
        assert_eq!(r1.get(&[], b"k").await.unwrap(), None);
        assert_eq!(r2.get(&[], b"k").await.unwrap(), None);
        // And cleared directory `[a]`'s keys; `[a, b]` survives with `m`.
        assert_eq!(r1.get(&[b"a"], b"x").await.unwrap(), None);
        assert_eq!(
            r1.get(&[b"a", b"b"], b"m").await.unwrap(),
            Some(b"n".to_vec())
        );
        assert_eq!(r2.get(&[b"a"], b"x").await.unwrap(), None);
        assert_eq!(
            r2.get(&[b"a", b"b"], b"m").await.unwrap(),
            Some(b"n".to_vec())
        );
    });
}

/// A transaction sees its own uncommitted writes; the committed base and any
/// other transaction do not.
#[test]
fn a_transaction_sees_its_own_uncommitted_writes() {
    block_on(async {
        let store = open_store();
        let mut tx = store.transaction();

        tx.set(&[b"a"], b"k", b"n").await.expect("set k");
        assert_eq!(
            tx.get(&[b"a"], b"k").await.expect("get k"),
            Some(b"n".to_vec())
        );
        // Not visible in the committed base.
        assert_eq!(store.len(), 0);

        // A fresh transaction sees only the committed base.
        let other = store.transaction();
        assert_eq!(other.get(&[b"a"], b"k").await.expect("get other"), None);
    });
}

/// A buffered delete shadows the committed value until commit, and the commit
/// is what actually removes the key from the base.
#[test]
fn a_transaction_delete_shadows_the_committed_value() {
    block_on(async {
        let store = open_store();
        {
            let mut seed = store.transaction();
            seed.set(&[b"a"], b"k", b"old").await.expect("seed");
            seed.commit().await.expect("seed commit");
        }

        let mut tx = store.transaction();
        tx.delete(&[b"a"], b"k").await.expect("delete");
        assert_eq!(tx.get(&[b"a"], b"k").await.expect("get"), None);
        assert!(!tx.exists(&[b"a"], b"k").await.expect("exists"));
        // The committed value is untouched until the commit applies.
        assert_eq!(store.len(), 9);

        tx.commit().await.expect("commit");
        // The data row is gone; the layout rows and the (empty) directory
        // remain.
        assert_eq!(store.len(), 8);
    });
}

/// The rollback FASM depends on: a dropped transaction — failed state
/// transition or not — leaves the base exactly as it was.
#[test]
fn an_uncommitted_transaction_rolls_back_on_drop() {
    block_on(async {
        let store = open_store();
        {
            let mut seed = store.transaction();
            seed.set(&[b"a"], b"k", b"kept").await.expect("seed");
            seed.commit().await.expect("seed commit");
        }

        {
            let mut ephemeral = store.transaction();
            ephemeral.set(&[b"a"], b"k", b"gone").await.expect("set");
            ephemeral.set(&[b"b"], b"k2", b"new").await.expect("set b");
            ephemeral.remove_dir(&[b"a"]).await.expect("remove a");
            // Dropping the transaction without a commit discards all of it.
        }

        assert_eq!(store.len(), 9, "the committed base must be untouched");
        let check = store.transaction();
        assert_eq!(
            check.get(&[b"a"], b"k").await.expect("get"),
            Some(b"kept".to_vec())
        );
        assert_eq!(check.get(&[b"b"], b"k2").await.expect("get"), None);
    });
}

/// `range` merges the committed map and the buffered writes: a buffered value
/// overrides the base, a tombstone suppresses it, and new keys appear — in
/// both directions.
#[test]
fn a_range_scan_merges_buffered_writes_over_the_committed_map() {
    block_on(async {
        let store = open_store();
        {
            let mut seed = store.transaction();
            for key in [&b"a"[..], &b"b"[..], &b"c"[..], &b"d"[..]] {
                seed.set(&[], key, b"committed").await.expect("seed");
            }
            seed.commit().await.expect("seed commit");
        }

        let mut tx = store.transaction();
        tx.set(&[], b"b", b"overridden").await.expect("override b");
        tx.set(&[], b"e", b"new").await.expect("add e");
        tx.delete(&[], b"c").await.expect("delete c");

        let unbounded = (Bound::Unbounded, Bound::Unbounded);
        let pairs = scan_bounded(&tx, &[], &unbounded, false).await;
        assert_eq!(
            pairs,
            vec![
                (b"a".to_vec(), b"committed".to_vec()),
                (b"b".to_vec(), b"overridden".to_vec()),
                (b"d".to_vec(), b"committed".to_vec()),
                (b"e".to_vec(), b"new".to_vec()),
            ]
        );

        // The same set, descending.
        let reversed = scan_bounded(&tx, &[], &unbounded, true).await;
        assert_eq!(reversed.into_iter().rev().collect::<Vec<_>>(), pairs);
    });
}

#[test]
fn btreemap_errors_are_not_retryable() {
    let key_err = fasm_storage::KeyError::RootNotRemovable;
    for e in &[
        BTreeMapError::from(FlatError::Foreign),
        BTreeMapError::from(FlatError::Corrupt),
        BTreeMapError::from(FlatError::Key(key_err)),
        BTreeMapError::from(FlatError::Engine(BufViewError)),
    ] {
        assert!(!e.is_retryable());
    }
}

#[test]
fn debug_output_redacts_stored_keys_and_values() {
    block_on(async {
        let store = open_store();
        let mut tx = store.transaction();
        tx.set(&[b"a"], b"secret-key", b"secret-value")
            .await
            .expect("set");
        // One set in one directory lazily initialises the layout (version
        // row, anchor + directory child rows, anchor + directory layer
        // rows, HCA counter + two recent rows) and writes the data row:
        // nine buffered raw rows.
        assert_eq!(format!("{tx:?}"), "BTreeMapTransaction { pending: 9 }");
        // Nothing is committed yet, and the base count never shows the bytes.
        assert_eq!(format!("{store:?}"), "BTreeMapStore { rows: 0 }");
        tx.commit().await.expect("commit");
        assert_eq!(format!("{store:?}"), "BTreeMapStore { rows: 9 }");

        let scoped = ScopedKvStore::new(store.transaction(), vec![b"a".to_vec()]);
        assert_eq!(format!("{scoped:?}"), "ScopedKvStore { dir_segments: 1 }");
    });
}

// ============================================================================
// BTreeMapTransaction as a reference model
// ============================================================================

/// The reference model: a map from `(dir, key)` to value with the semantics
/// the [`KvStore`] documentation describes.
type Model = BTreeMap<(Vec<Vec<u8>>, Vec<u8>), Vec<u8>>;

/// One mutation applied to both the transaction and the reference model.
#[derive(Debug, Clone)]
enum Op {
    /// Write a value, overwriting any existing one.
    Set(Vec<Vec<u8>>, Vec<u8>, Vec<u8>),
    /// Remove a key, present or not.
    Delete(Vec<Vec<u8>>, Vec<u8>),
    /// Remove every key in a range within one directory.
    ClearRange(Vec<Vec<u8>>, Bound<Vec<u8>>, Bound<Vec<u8>>),
}

/// Arbitrary mutation. `set` is weighted up so sequences actually accumulate
/// state instead of thrashing an almost-empty map.
fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (arb_dir(), arb_key(), arb_value()).prop_map(|(dir, key, value)| Op::Set(dir, key, value)),
        2 => (arb_dir(), arb_key()).prop_map(|(dir, key)| Op::Delete(dir, key)),
        1 => (arb_dir(), arb_bounds())
            .prop_map(|(dir, bounds)| Op::ClearRange(dir, bounds.0, bounds.1)),
    ]
}

fn dir_slice(dir: &[Vec<u8>]) -> Vec<&[u8]> {
    dir.iter().map(|s| s.as_slice()).collect()
}

/// Apply `op` to the reference model.
fn apply_to_model(model: &mut Model, op: &Op) {
    match op {
        Op::Set(dir, key, value) => {
            model.insert((dir.clone(), key.clone()), value.clone());
        }
        Op::Delete(dir, key) => {
            model.remove(&(dir.clone(), key.clone()));
        }
        Op::ClearRange(dir, start, end) => {
            let bounds = (start.clone(), end.clone());
            let d = dir.clone();
            model.retain(|(d2, key), _| !(d2 == &d && bounds_contain(&bounds, key)));
        }
    }
}

/// Apply `op` to the transaction under test.
async fn apply_to_store(tx: &mut BTreeMapTransaction, op: &Op) {
    match op {
        Op::Set(dir, key, value) => {
            tx.set(&dir_slice(dir), key, value)
                .await
                .expect("set must succeed");
        }
        Op::Delete(dir, key) => {
            tx.delete(&dir_slice(dir), key)
                .await
                .expect("delete must succeed");
        }
        Op::ClearRange(dir, start, end) => {
            tx.clear_range(&dir_slice(dir), bound_as_slice(start), bound_as_slice(end))
                .await
                .expect("clear_range must succeed");
        }
    }
}

/// Scan the model's one directory as `(key, value)` pairs.
fn model_dir_pairs(model: &Model, dir: &[Vec<u8>]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let d = dir.to_vec();
    model
        .iter()
        .filter(|((d2, _), _)| d2 == &d)
        .map(|((_, key), value)| (key.clone(), value.clone()))
        .collect()
}

proptest! {
    // Each case replays a whole op sequence and then runs scans.
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// A `BTreeMapTransaction` buffers every write, so its merged view must
    /// agree with the `(dir, key) -> value` model op for op. Everything built
    /// on this crate is tested against this store, which makes any divergence
    /// from the obvious model a divergence the layers above would inherit.
    #[test]
    fn prop_btreemap_transaction_tracks_a_model(
        ops in prop::collection::vec(arb_op(), 0..24),
        query_dir in arb_dir(),
        query in arb_bounds(),
    ) {
        block_on(async {
            let mut tx = open_transaction();
            let mut model: Model = Model::new();

            for op in &ops {
                apply_to_store(&mut tx, op).await;
                apply_to_model(&mut model, op);

                // Point reads agree after every single step, so a
                // divergence is reported at the op that caused it.
                if let Op::Set(dir, key, _) | Op::Delete(dir, key) = op {
                    let stored = tx.get(&dir_slice(dir), key).await.expect("get must succeed");
                    let want = model.get(&(dir.clone(), key.clone())).cloned();
                    prop_assert_eq!(stored, want.clone());
                    prop_assert_eq!(
                        tx.exists(&dir_slice(dir), key).await.expect("exists must succeed"),
                        want.is_some(),
                    );
                }
            }

            // The full directory scan agrees in both directions.
            let everything = model_dir_pairs(&model, &query_dir);
            let unbounded = (Bound::Unbounded, Bound::Unbounded);
            prop_assert_eq!(
                scan_bounded(&tx, &dir_slice(&query_dir), &unbounded, false).await,
                everything.clone(),
            );
            let mut descending = everything.clone();
            descending.reverse();
            prop_assert_eq!(
                scan_bounded(&tx, &dir_slice(&query_dir), &unbounded, true).await,
                descending,
            );

            // ...and an arbitrary bounded query selects exactly the model's
            // keys inside those bounds, in both directions.
            let bounds = (query.0.clone(), query.1.clone());
            let selected: Vec<(Vec<u8>, Vec<u8>)> = everything
                .into_iter()
                .filter(|(key, _)| bounds_contain(&bounds, key))
                .collect();
            prop_assert_eq!(
                scan_bounded(&tx, &dir_slice(&query_dir), &bounds, false).await,
                selected.clone(),
            );
            let mut selected_descending = selected;
            selected_descending.reverse();
            prop_assert_eq!(
                scan_bounded(&tx, &dir_slice(&query_dir), &bounds, true).await,
                selected_descending,
            );

            Ok(())
        })?;
    }
}

// ============================================================================
// ScopedKvStore
// ============================================================================

#[test]
fn scoped_store_pins_the_directory() {
    block_on(async {
        let store = open_store();
        let mut scope =
            ScopedKvStore::new(store.transaction(), vec![b"c1".to_vec(), b"swap".to_vec()]);
        assert_eq!(scope.dir(), &[b"c1".to_vec(), b"swap".to_vec()]);

        scope.set(&[], b"key", b"value").await.expect("set");
        scope.set(&[], b"", b"root").await.expect("set empty key");

        // Commit, then inspect the committed base through the opener.
        scope.commit().await.expect("commit must not fail");
        // The pin directory's own empty key and `key` land in the pin, and
        // nothing else: the layout rows plus the pin's two data rows.
        let check = store.transaction();
        assert_eq!(
            check.get(&[b"c1", b"swap"], b"key").await.expect("get key"),
            Some(b"value".to_vec())
        );
        assert_eq!(
            check
                .get(&[b"c1", b"swap"], b"")
                .await
                .expect("get empty key"),
            Some(b"root".to_vec())
        );
        // The pin's parent directory exists (allocated on the way) but holds
        // no keys of its own.
        let visible: Vec<Vec<u8>> = check
            .range(&[b"c1"], Bound::Unbounded, Bound::Unbounded, false)
            .collect()
            .await
            .expect("scan parent")
            .into_iter()
            .map(|pair| pair.key)
            .collect();
        assert!(visible.is_empty(), "the parent holds no data of its own");
    });
}

#[test]
fn scoped_stores_do_not_see_each_others_keys() {
    block_on(async {
        let store = open_store();
        let mut first = ScopedKvStore::new(store.transaction(), vec![b"a".to_vec()]);
        first.set(&[], b"key", b"first").await.expect("set first");
        first
            .set(&[], b"other", b"first-other")
            .await
            .expect("set first other");
        first.commit().await.expect("commit first");

        let mut second = ScopedKvStore::new(store.transaction(), vec![b"b".to_vec()]);
        second
            .set(&[], b"key", b"second")
            .await
            .expect("set second");
        second.commit().await.expect("commit second");

        // Same caller-space key, different directories, no collision. A fresh
        // scoped handle reads the committed write through its pin.
        let second = ScopedKvStore::new(store.transaction(), vec![b"b".to_vec()]);
        assert_eq!(
            second.get(&[], b"key").await.expect("get second"),
            Some(b"second".to_vec())
        );
        let visible: Vec<Vec<u8>> = second
            .range(&[], Bound::Unbounded, Bound::Unbounded, false)
            .collect()
            .await
            .expect("scan second")
            .into_iter()
            .map(|pair| pair.key)
            .collect();
        assert_eq!(visible, vec![b"key".to_vec()]);

        let mut back = ScopedKvStore::new(store.transaction(), vec![b"a".to_vec()]);
        assert_eq!(
            back.get(&[], b"key").await.expect("get first"),
            Some(b"first".to_vec())
        );

        // Clearing one directory wholesale leaves the other intact.
        back.clear_range(&[], Bound::Unbounded, Bound::Unbounded)
            .await
            .expect("clear first");
        back.commit().await.expect("commit first clear");

        // The base still holds the second scope's key.
        let check = store.transaction();
        assert_eq!(
            check.get(&[b"b"], b"key").await.expect("get"),
            Some(b"second".to_vec())
        );
        assert_eq!(check.get(&[b"a"], b"key").await.expect("get"), None);
    });
}

#[test]
fn scoped_relative_paths_name_subdirectories_of_the_pin() {
    block_on(async {
        let store = open_store();
        let mut scope = ScopedKvStore::new(store.transaction(), vec![b"a".to_vec()]);

        // The empty relative path names the pin itself.
        scope.set(&[], b"top", b"t").await.expect("set at pin");
        // A relative path names a subdirectory of the pin.
        scope.set(&[b"sub"], b"k", b"v").await.expect("set in sub");

        scope.commit().await.expect("commit");

        let check = store.transaction();
        assert_eq!(
            check.get(&[b"a"], b"top").await.expect("pin key"),
            Some(b"t".to_vec())
        );
        assert_eq!(
            check.get(&[b"a", b"sub"], b"k").await.expect("sub key"),
            Some(b"v".to_vec())
        );
        // A scan at the pin's level never yields the subdirectory's keys.
        let visible: Vec<Vec<u8>> = check
            .range(&[b"a"], Bound::Unbounded, Bound::Unbounded, false)
            .collect()
            .await
            .expect("scan pin")
            .into_iter()
            .map(|pair| pair.key)
            .collect();
        assert_eq!(visible, vec![b"top".to_vec()]);
    });
}

#[test]
fn scoped_nesting_appends_one_segment() {
    block_on(async {
        let store = open_store();
        let mut nested =
            ScopedKvStore::new(store.transaction(), vec![b"c1".to_vec()]).nested(b"swap");
        assert_eq!(nested.dir(), &[b"c1".to_vec(), b"swap".to_vec()]);

        nested.set(&[], b"status", b"funded").await.expect("set");
        assert_eq!(
            nested
                .inner()
                .get(&[b"c1", b"swap"], b"status")
                .await
                .expect("raw get"),
            Some(b"funded".to_vec())
        );

        nested
            .inner_mut()
            .clear_range(&[b"c1", b"swap"], Bound::Unbounded, Bound::Unbounded)
            .await
            .expect("clear the pin");
        assert_eq!(nested.get(&[], b"status").await.expect("get"), None);
    });
}

#[test]
fn scoped_commit_forwards_unwrapped() {
    block_on(async {
        let scope = ScopedKvStore::new(open_transaction(), vec![b"p".to_vec()]);
        // The commit error type is the backend's own.
        let result: Result<(), BTreeMapError> = scope.commit().await;
        result.expect("commit must not fail");
    });
}

// ============================================================================
// ScopedKvStore properties
// ============================================================================

/// Arbitrary caller-space contents of one scope.
fn arb_entries() -> impl Strategy<Value = BTreeMap<Vec<u8>, Vec<u8>>> {
    prop::collection::btree_map(arb_key(), arb_value(), 0..8)
}

proptest! {
    // Each case builds a store and runs several scans.
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// A scope is a faithful view of the directory written through it: what
    /// goes in comes back out, point reads and full scans agree, and the scan
    /// order is the store's lexicographic order in both directions.
    #[test]
    fn prop_scope_round_trips_every_entry(
        pin in arb_dir(),
        entries in arb_entries(),
    ) {
        block_on(async {
            let mut scope = ScopedKvStore::new(open_transaction(), pin.clone());
            for (key, value) in &entries {
                scope.set(&[], key, value).await.expect("set must succeed");
            }

            for (key, value) in &entries {
                let stored = scope.get(&[], key).await.expect("get must succeed");
                prop_assert_eq!(stored, Some(value.clone()));
                prop_assert!(scope.exists(&[], key).await.expect("exists must succeed"));
            }

            let forward = scan_all(&scope, &[], false).await;
            let expected: Vec<(Vec<u8>, Vec<u8>)> = entries
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            prop_assert_eq!(&forward, &expected);

            let mut reversed = scan_all(&scope, &[], true).await;
            reversed.reverse();
            prop_assert_eq!(&reversed, &expected);

            Ok(())
        })?;
    }

    /// Two scopes pinned to the same base with different pins cannot see each
    /// other's keys — including a wholesale `clear_range` at one pin, which
    /// must not reach past its directory.
    #[test]
    fn prop_disjoint_pins_cannot_see_each_other(
        pin_a in arb_dir(),
        pin_b in arb_dir(),
        first_entries in arb_entries(),
        second_entries in arb_entries(),
    ) {
        prop_assume!(pin_a != pin_b);
        block_on(async {
            let mut first = ScopedKvStore::new(open_transaction(), pin_a.clone());
            for (key, value) in &first_entries {
                first.set(&[], key, value).await.expect("set must succeed");
            }

            // Both scopes share one transaction (one rollback unit); the
            // directory pins are what confine each scope's visibility.
            let mut second = ScopedKvStore::new(first.into_inner(), pin_b.clone());
            for (key, value) in &second_entries {
                second.set(&[], key, value).await.expect("set must succeed");
            }

            // The second scope sees exactly its own writes, whatever the
            // first one put in the same transaction.
            let seen = scan_all(&second, &[], false).await;
            let expected: Vec<(Vec<u8>, Vec<u8>)> = second_entries
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            prop_assert_eq!(seen, expected);

            // Clearing the second directory wholesale leaves the first
            // intact.
            second
                .clear_range(&[], Bound::Unbounded, Bound::Unbounded)
                .await
                .expect("clear must succeed");

            let first = ScopedKvStore::new(second.into_inner(), pin_a.clone());
            let survivors = scan_all(&first, &[], false).await;
            let expected: Vec<(Vec<u8>, Vec<u8>)> = first_entries
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            prop_assert_eq!(survivors, expected);

            Ok(())
        })?;
    }
}
