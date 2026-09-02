//! Store-backed tests for the in-memory [`BTreeMapStore`] backend and the
//! [`ScopedKvStore`](fasm_storage::ScopedKvStore) adapter built on it.
//!
//! Faithful port of the `sb-kv` crate's test suite — its `mem`, `tests`,
//! `keyspace`, and `scoped` test modules — applied to this backend, with one
//! structural difference: this backend is **transactional**. [`BTreeMapStore`]
//! is the committed base and the opener; every test's store handle is a fresh
//! [`BTreeMapTransaction`] whose buffered writes are read-your-writes, are
//! applied to the base with [`Commit`], and roll back on drop. The state
//! machines built on `fasm-storage` rely on exactly that contract, so the
//! conformance suite runs against the transaction handle: an in-memory test
//! and a durable-backend test exercise the same semantics.
//!
//! The only other changes from the `sb-kv` original are the
//! `MemKvStore`/`MemKvError` rename to `BTreeMapStore`/`BTreeMapError`, the
//! crate-root paths becoming `fasm_storage::` / `crate::`, and two same-named
//! `scan` helpers being disambiguated (`scan_bounded`, `scan_all`). The
//! conformance suite, the scoped-store behaviour, the fail-closed tests, and
//! the keyspace/scoped property tests all run here because this crate holds
//! the in-process store the whole workspace is tested against.
//!
//! `fasm-storage` keeps its pure-logic tests and deliberately has no
//! dependency on this crate: a dev-dependency cycle would make `KvStore`
//! compile as two distinct instances.

use core::future::Future;
use core::ops::Bound;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::collections::BTreeMap;

use async_trait::async_trait;
use proptest::prelude::*;
use proptest::test_runner::Config as ProptestConfig;
use thiserror::Error;

use fasm_storage::{
    Commit, KvPair, KvStore, KvStream, RetryableStorageError, ScopedKvError, ScopedKvStore,
    bound_as_slice,
};

use crate::{BTreeMapError, BTreeMapStore, BTreeMapTransaction};

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

/// Collect a full scan of `store` as `(key, value)` pairs.
async fn scan_all<KV: KvStore>(store: &KV, reverse: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
    store
        .range(Bound::Unbounded, Bound::Unbounded, reverse)
        .collect()
        .await
        .expect("scan must succeed")
        .into_iter()
        .map(|pair| (pair.key, pair.value))
        .collect()
}

/// Collect a bounded range scan as `(key, value)` pairs.
async fn scan_bounded<KV: KvStore>(
    store: &KV,
    bounds: &(Bound<Vec<u8>>, Bound<Vec<u8>>),
    reverse: bool,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    store
        .range(
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
// Conformance: the four scopes the suite must pass
// ============================================================================

mod btreemap_conformance {
    // The store handle is a write transaction: the conformance suite must
    // pass on the same transactional surface the durable backends expose.
    fasm_storage::kv_store_tests!(
        store = super::open_transaction(),
        block_on = super::block_on,
    );
}

mod scoped_conformance {
    use fasm_storage::ScopedKvStore;

    fasm_storage::kv_store_tests!(
        store = ScopedKvStore::new(super::open_store().transaction(), b"c1/swap/".to_vec()),
        block_on = super::block_on,
    );
}

mod scoped_all_ff_conformance {
    use fasm_storage::ScopedKvStore;

    // The prefix with no successor: `end_bound(Unbounded)` must degrade to
    // `Bound::Unbounded` or every high key falls out of the namespace.
    fasm_storage::kv_store_tests!(
        store = ScopedKvStore::new(super::open_store().transaction(), vec![0xFF, 0xFF]),
        block_on = super::block_on,
    );
}

mod empty_prefix_conformance {
    use fasm_storage::ScopedKvStore;

    // The other successor-less prefix: an empty scope is a transparent view.
    fasm_storage::kv_store_tests!(
        store = ScopedKvStore::new(super::open_store().transaction(), Vec::new()),
        block_on = super::block_on,
    );
}

// ============================================================================
// BTreeMapStore / BTreeMapTransaction
// ============================================================================

#[test]
fn btreemap_store_reports_committed_size_and_raw_keys() {
    block_on(async {
        let store = open_store();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);

        let mut tx = store.transaction();
        tx.set(b"b", b"2").await.expect("set b");
        tx.set(b"a", b"1").await.expect("set a");
        // Uncommitted: the base still reports nothing.
        assert!(store.is_empty());
        assert_eq!(store.keys().len(), 0);

        tx.commit().await.expect("commit");
        assert!(!store.is_empty());
        assert_eq!(store.len(), 2);
        assert_eq!(store.keys(), vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(store.get_committed(b"a"), Some(b"1".to_vec()));

        store.clear();
        assert!(store.is_empty());
    });
}

/// A transaction sees its own uncommitted writes; the committed base and any
/// other transaction do not.
#[test]
fn a_transaction_sees_its_own_uncommitted_writes() {
    block_on(async {
        let store = open_store();
        let mut tx = store.transaction();

        tx.set(b"new", b"n").await.expect("set new");
        assert_eq!(tx.get(b"new").await.expect("get new"), Some(b"n".to_vec()));
        // Not visible in the committed base.
        assert_eq!(store.get_committed(b"new"), None);

        // A fresh transaction sees only the committed base.
        let other = store.transaction();
        assert_eq!(other.get(b"new").await.expect("get other"), None);
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
            seed.set(b"k", b"old").await.expect("seed");
            seed.commit().await.expect("seed commit");
        }

        let mut tx = store.transaction();
        tx.delete(b"k").await.expect("delete");
        assert_eq!(tx.get(b"k").await.expect("get"), None);
        assert!(!tx.exists(b"k").await.expect("exists"));
        // The committed value is untouched until the commit applies.
        assert_eq!(store.get_committed(b"k"), Some(b"old".to_vec()));

        tx.commit().await.expect("commit");
        assert_eq!(store.get_committed(b"k"), None);
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
            seed.set(b"seed", b"kept").await.expect("seed");
            seed.commit().await.expect("seed commit");
        }

        {
            let mut ephemeral = store.transaction();
            ephemeral.set(b"ephemeral", b"gone").await.expect("set");
            ephemeral.delete(b"seed").await.expect("delete");
            // Dropping the transaction without a commit discards both writes.
        }

        assert_eq!(
            store.get_committed(b"seed"),
            Some(b"kept".to_vec()),
            "the committed seed must survive the rolled-back delete"
        );
        assert_eq!(store.get_committed(b"ephemeral"), None);
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
            for key in [b"a", b"b", b"c", b"d"] {
                seed.set(key, b"committed").await.expect("seed");
            }
            seed.commit().await.expect("seed commit");
        }

        let mut tx = store.transaction();
        tx.set(b"b", b"overridden").await.expect("override b");
        tx.set(b"e", b"new").await.expect("add e");
        tx.delete(b"c").await.expect("delete c");

        let unbounded = (Bound::Unbounded, Bound::Unbounded);
        let pairs = scan_bounded(&tx, &unbounded, false).await;
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
        let reversed = scan_bounded(&tx, &unbounded, true).await;
        assert_eq!(reversed.into_iter().rev().collect::<Vec<_>>(), pairs);
    });
}

#[test]
fn btreemap_error_is_not_retryable() {
    assert!(!BTreeMapError.is_retryable());
}

#[test]
fn debug_output_redacts_stored_keys_values_and_prefixes() {
    block_on(async {
        let store = open_store();
        let mut tx = store.transaction();
        tx.set(b"secret-key", b"secret-value").await.expect("set");
        assert_eq!(format!("{tx:?}"), "BTreeMapTransaction { pending: 1 }");
        // Nothing is committed yet, and the base count never shows the bytes.
        assert_eq!(format!("{store:?}"), "BTreeMapStore { keys: 0 }");
        tx.commit().await.expect("commit");
        assert_eq!(format!("{store:?}"), "BTreeMapStore { keys: 1 }");

        let pair = KvPair {
            key: b"secret-key".to_vec(),
            value: b"secret-value".to_vec(),
        };
        assert_eq!(
            format!("{pair:?}"),
            "KvPair { key: <elided>, value: <12 bytes> }"
        );

        let scoped = ScopedKvStore::new(store.transaction(), b"secret-prefix/".to_vec());
        assert_eq!(format!("{scoped:?}"), "ScopedKvStore(..)");

        // The one error that is handed a key from *another* namespace. It is
        // what a retry loop logs, so neither the foreign key nor this scope's
        // own prefix may survive into `Display` or `Debug`.
        let violation: ScopedKvError<BTreeMapError> =
            ScopedKvError::prefix_violation(b"secret-prefix/", b"foreign/secret-key");
        for rendered in [format!("{violation}"), format!("{violation:?}")] {
            assert!(
                !rendered.contains("secret-key") && !rendered.contains("secret-prefix"),
                "key material must not reach the error text: {rendered}"
            );
            assert!(
                !rendered.contains("foreign"),
                "the foreign namespace must not reach the error text: {rendered}"
            );
        }

        // Lengths still make the report actionable.
        let message = format!("{violation}");
        assert!(message.contains("18-byte key"), "{message}");
        assert!(message.contains("14-byte prefix"), "{message}");

        // The fingerprints are stable, so two reports of the same violation
        // correlate, and distinct keys do not collide into one report.
        assert_eq!(
            ScopedKvError::<BTreeMapError>::prefix_violation(b"p/", b"foreign/a"),
            ScopedKvError::<BTreeMapError>::prefix_violation(b"p/", b"foreign/a"),
        );
        assert_ne!(
            ScopedKvError::<BTreeMapError>::prefix_violation(b"p/", b"foreign/a"),
            ScopedKvError::<BTreeMapError>::prefix_violation(b"p/", b"foreign/b"),
        );
    });
}

// ============================================================================
// BTreeMapTransaction as a reference model
// ============================================================================

/// One mutation applied to both the transaction and the reference model.
#[derive(Debug, Clone)]
enum Op {
    /// Write a value, overwriting any existing one.
    Set(Vec<u8>, Vec<u8>),
    /// Remove a key, present or not.
    Delete(Vec<u8>),
    /// Remove every key in a range.
    ClearRange(Bound<Vec<u8>>, Bound<Vec<u8>>),
}

/// Arbitrary mutation. `set` is weighted up so sequences actually accumulate
/// state instead of thrashing an almost-empty map.
fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (arb_key(), arb_value()).prop_map(|(key, value)| Op::Set(key, value)),
        2 => arb_key().prop_map(Op::Delete),
        1 => arb_bounds().prop_map(|(start, end)| Op::ClearRange(start, end)),
    ]
}

/// Apply `op` to the reference model: a plain `BTreeMap` with the semantics
/// the [`KvStore`] documentation describes, written out directly.
fn apply_to_model(model: &mut BTreeMap<Vec<u8>, Vec<u8>>, op: &Op) {
    match op {
        Op::Set(key, value) => {
            model.insert(key.clone(), value.clone());
        }
        Op::Delete(key) => {
            model.remove(key);
        }
        Op::ClearRange(start, end) => {
            let bounds = (start.clone(), end.clone());
            model.retain(|key, _| !bounds_contain(&bounds, key));
        }
    }
}

/// Apply `op` to the transaction under test.
async fn apply_to_store(tx: &mut BTreeMapTransaction, op: &Op) {
    match op {
        Op::Set(key, value) => tx.set(key, value).await.expect("set must succeed"),
        Op::Delete(key) => tx.delete(key).await.expect("delete must succeed"),
        Op::ClearRange(start, end) => tx
            .clear_range(bound_as_slice(start), bound_as_slice(end))
            .await
            .expect("clear_range must succeed"),
    }
}

#[test]
fn invalid_btree_ranges_are_empty_in_both_directions() {
    block_on(async {
        let mut tx = open_transaction();
        tx.set(b"k", b"value").await.expect("set must succeed");

        for (start, end) in [
            (
                Bound::Excluded(b"k".as_slice()),
                Bound::Excluded(b"k".as_slice()),
            ),
            (
                Bound::Included(b"z".as_slice()),
                Bound::Excluded(b"a".as_slice()),
            ),
            (
                Bound::Excluded(b"z".as_slice()),
                Bound::Included(b"a".as_slice()),
            ),
        ] {
            for reverse in [false, true] {
                let pairs = tx
                    .range(start, end, reverse)
                    .collect()
                    .await
                    .expect("scan must succeed");
                assert!(
                    pairs.is_empty(),
                    "bounds {start:?}..{end:?}, reverse={reverse}"
                );
            }
        }
    });
}

#[test]
fn invalid_btree_clear_ranges_are_noops() {
    block_on(async {
        for (start, end) in [
            (
                Bound::Excluded(b"k".as_slice()),
                Bound::Excluded(b"k".as_slice()),
            ),
            (
                Bound::Included(b"z".as_slice()),
                Bound::Excluded(b"a".as_slice()),
            ),
            (
                Bound::Excluded(b"z".as_slice()),
                Bound::Included(b"a".as_slice()),
            ),
        ] {
            let mut tx = open_transaction();
            tx.set(b"k", b"value").await.expect("set must succeed");
            tx.clear_range(start, end)
                .await
                .expect("clear_range must succeed");

            assert_eq!(
                tx.get(b"k").await.expect("get must succeed"),
                Some(b"value".to_vec()),
                "bounds {start:?}..{end:?}",
            );
        }
    });
}

proptest! {
    // Each case replays a whole op sequence and then runs four scans.
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// A `BTreeMapTransaction` buffers every write, so its merged view must
    /// agree with a plain `BTreeMap` model op for op. Everything built on this
    /// crate is tested against this store, which makes any divergence from the
    /// obvious model a divergence the layers above would inherit.
    #[test]
    fn prop_btreemap_transaction_tracks_a_btreemap_model(
        ops in prop::collection::vec(arb_op(), 0..24),
        query in arb_bounds(),
    ) {
        block_on(async {
            let mut tx = open_transaction();
            let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

            for op in &ops {
                apply_to_store(&mut tx, op).await;
                apply_to_model(&mut model, op);

                // Point reads agree after every single step, so a
                // divergence is reported at the op that caused it.
                if let Op::Set(key, _) | Op::Delete(key) = op {
                    let stored = tx.get(key).await.expect("get must succeed");
                    prop_assert_eq!(stored.as_ref(), model.get(key));
                    prop_assert_eq!(
                        tx.exists(key).await.expect("exists must succeed"),
                        model.contains_key(key),
                    );
                }
            }

            let everything: Vec<(Vec<u8>, Vec<u8>)> = model
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            let unbounded = (Bound::Unbounded, Bound::Unbounded);

            prop_assert_eq!(scan_bounded(&tx, &unbounded, false).await, everything.clone());

            let mut descending = everything.clone();
            descending.reverse();
            prop_assert_eq!(scan_bounded(&tx, &unbounded, true).await, descending);

            // ...and an arbitrary bounded query selects exactly the model's
            // keys inside those bounds, in both directions.
            let selected: Vec<(Vec<u8>, Vec<u8>)> = everything
                .into_iter()
                .filter(|(key, _)| bounds_contain(&query, key))
                .collect();
            prop_assert_eq!(scan_bounded(&tx, &query, false).await, selected.clone());

            let mut selected_descending = selected;
            selected_descending.reverse();
            prop_assert_eq!(scan_bounded(&tx, &query, true).await, selected_descending);

            Ok(())
        })?;
    }
}

// ============================================================================
// ScopedKvStore
// ============================================================================

#[test]
fn scoped_store_prefixes_the_backing_keys() {
    block_on(async {
        let store = open_store();
        let mut scope = ScopedKvStore::new(store.transaction(), b"p/".to_vec());
        assert_eq!(scope.prefix(), b"p/");

        scope.set(b"key", b"value").await.expect("set");
        scope.set(b"", b"root").await.expect("set empty key");

        // Commit, then inspect the committed base through the opener.
        scope.commit().await.expect("commit must not fail");
        assert_eq!(store.keys(), vec![b"p/".to_vec(), b"p/key".to_vec()]);
        assert_eq!(store.get_committed(b"p/key"), Some(b"value".to_vec()));
        assert_eq!(store.get_committed(b"p/"), Some(b"root".to_vec()));
    });
}

#[test]
fn scoped_stores_do_not_see_each_others_keys() {
    block_on(async {
        let store = open_store();
        let mut first = ScopedKvStore::new(store.transaction(), vec![0x10]);
        first.set(b"key", b"first").await.expect("set first");
        first
            .set(b"other", b"first-other")
            .await
            .expect("set first other");
        first.commit().await.expect("commit first");

        let mut second = ScopedKvStore::new(store.transaction(), vec![0x20]);
        second.set(b"key", b"second").await.expect("set second");
        second.commit().await.expect("commit second");

        // Same caller-space key, different namespaces, no collision. A fresh
        // scoped handle reads the committed write through the namespace.
        let second = ScopedKvStore::new(store.transaction(), vec![0x20]);
        assert_eq!(
            second.get(b"key").await.expect("get second"),
            Some(b"second".to_vec())
        );
        let visible: Vec<Vec<u8>> = second
            .range(Bound::Unbounded, Bound::Unbounded, false)
            .collect()
            .await
            .expect("scan second")
            .into_iter()
            .map(|pair| pair.key)
            .collect();
        assert_eq!(visible, vec![b"key".to_vec()]);

        let mut back = ScopedKvStore::new(store.transaction(), vec![0x10]);
        assert_eq!(
            back.get(b"key").await.expect("get first"),
            Some(b"first".to_vec())
        );

        // Clearing one namespace wholesale leaves the other intact.
        back.clear_range(Bound::Unbounded, Bound::Unbounded)
            .await
            .expect("clear first");
        back.commit().await.expect("commit first clear");

        assert_eq!(store.keys(), vec![[0x20, b'k', b'e', b'y'].to_vec()]);
    });
}

#[test]
fn scoped_range_bounds_never_leak_past_the_namespace() {
    block_on(async {
        // Neighbouring prefixes chosen so a missing upper bound would spill:
        // `[0x10, 0xFF]`'s successor is `[0x11]`, and `[0x11]` is populated.
        let store = open_store();
        {
            let mut seed = store.transaction();
            for key in [
                [0x10, 0xFE].as_slice(),
                [0x10, 0xFF].as_slice(),
                [0x10, 0xFF, 0x00].as_slice(),
                [0x10, 0xFF, 0xFF].as_slice(),
                [0x11].as_slice(),
                [0x11, 0x00].as_slice(),
            ] {
                seed.set(key, b"v").await.expect("seed");
            }
            seed.commit().await.expect("seed commit");
        }

        let mut scope = ScopedKvStore::new(store.transaction(), vec![0x10, 0xFF]);
        let keys: Vec<Vec<u8>> = scope
            .range(Bound::Unbounded, Bound::Unbounded, false)
            .collect()
            .await
            .expect("scan")
            .into_iter()
            .map(|pair| pair.key)
            .collect();
        assert_eq!(keys, vec![Vec::new(), vec![0x00], vec![0xFF]]);

        // ...and the same holds for a range delete.
        scope
            .clear_range(Bound::Unbounded, Bound::Unbounded)
            .await
            .expect("clear");
        scope.commit().await.expect("commit clear");

        assert_eq!(
            store.keys(),
            vec![vec![0x10, 0xFE], vec![0x11], vec![0x11, 0x00],]
        );
    });
}

#[test]
fn scoped_nesting_concatenates_prefixes() {
    block_on(async {
        let store = open_store();
        let mut nested = ScopedKvStore::new(store.transaction(), b"c1/".to_vec()).scoped(b"swap/");
        assert_eq!(nested.prefix(), b"c1/swap/");

        nested.set(b"status", b"funded").await.expect("set");
        assert_eq!(
            nested
                .inner()
                .get(b"c1/swap/status")
                .await
                .expect("raw get"),
            Some(b"funded".to_vec())
        );

        nested
            .inner_mut()
            .clear_range(Bound::Unbounded, Bound::Unbounded)
            .await
            .expect("clear inner");
        assert_eq!(nested.get(b"status").await.expect("get"), None);
    });
}

#[test]
fn scoped_commit_forwards_unwrapped() {
    block_on(async {
        let scope = ScopedKvStore::new(open_store().transaction(), b"p/".to_vec());
        // The commit error type is the backend's own, not `ScopedKvError`.
        let result: Result<(), BTreeMapError> = scope.commit().await;
        result.expect("commit must not fail");
    });
}

// ============================================================================
// Fail-closed behaviour
// ============================================================================

/// Error type for [`RogueStore`], deliberately reported as retryable so the
/// forwarding in [`ScopedKvError::is_retryable`] is observable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("rogue backend error")]
struct RogueError;

impl RetryableStorageError for RogueError {
    fn is_retryable(&self) -> bool {
        true
    }
}

/// A backend that ignores range bounds and fails every point read.
///
/// A correct backend can never trigger [`ScopedKvError::PrefixViolation`],
/// because [`ScopedKvStore`] confines every bound it hands down. This store
/// exists to prove the scoped wrapper fails closed anyway rather than trusting
/// what comes back.
struct RogueStore;

#[async_trait]
impl KvStore for RogueStore {
    type Error = RogueError;

    async fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Err(RogueError)
    }

    async fn set(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        Err(RogueError)
    }

    async fn delete(&mut self, _key: &[u8]) -> Result<(), Self::Error> {
        Err(RogueError)
    }

    fn range<'a>(
        &'a self,
        _start: Bound<&[u8]>,
        _end: Bound<&[u8]>,
        _reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        KvStream::new(async {
            let pair = KvPair {
                key: b"somewhere/else".to_vec(),
                value: b"not yours".to_vec(),
            };
            Ok(Some((pair, KvStream::empty())))
        })
    }

    async fn clear_range(
        &mut self,
        _start: Bound<&[u8]>,
        _end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        Err(RogueError)
    }
}

/// A backend that delegates reads and writes but ignores clear-range bounds.
struct OverDeletingStore {
    inner: BTreeMapTransaction,
}

#[async_trait]
impl KvStore for OverDeletingStore {
    type Error = BTreeMapError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get(key).await
    }

    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.inner.set(key, value).await
    }

    async fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key).await
    }

    async fn exists(&self, key: &[u8]) -> Result<bool, Self::Error> {
        self.inner.exists(key).await
    }

    fn range<'a>(
        &'a self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        self.inner.range(start, end, reverse)
    }

    /// The rogue behaviour: ignore the scoped bounds and clear everything.
    async fn clear_range(
        &mut self,
        _start: Bound<&[u8]>,
        _end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        self.inner
            .clear_range(Bound::Unbounded, Bound::Unbounded)
            .await
    }
}

#[test]
fn scoped_scan_rejects_keys_outside_its_prefix() {
    block_on(async {
        let scope = ScopedKvStore::new(RogueStore, b"p/".to_vec());
        let err = scope
            .range(Bound::Unbounded, Bound::Unbounded, false)
            .collect()
            .await
            .expect_err("out-of-prefix key must not be returned to the caller");

        assert_eq!(
            err,
            ScopedKvError::prefix_violation(b"p/", b"somewhere/else")
        );
        assert!(!err.is_retryable(), "corruption must not look retryable");
    });
}

#[test]
fn scoped_backend_errors_keep_their_retryability() {
    block_on(async {
        let scope = ScopedKvStore::new(RogueStore, b"p/".to_vec());
        let err = scope.get(b"k").await.expect_err("backend fails");

        assert_eq!(err, ScopedKvError::Backend(RogueError));
        assert!(
            err.is_retryable(),
            "wrapping must not erase the backend's own answer"
        );
    });
}

#[test]
fn scoped_clear_range_cannot_detect_a_backend_that_over_deletes() {
    block_on(async {
        let mut tx = open_transaction();
        tx.set(b"p/inside", b"inside").await.expect("seed scope");
        tx.set(b"foreign/outside", b"outside")
            .await
            .expect("seed foreign");

        let rogue = OverDeletingStore { inner: tx };
        let mut scope = ScopedKvStore::new(rogue, b"p/".to_vec());
        scope
            .clear_range(Bound::Unbounded, Bound::Unbounded)
            .await
            .expect("backend reports success");

        // The rogue backend ignored the scoped bounds and cleared the foreign
        // key too. The scoped wrapper cannot detect it — the view is simply
        // empty now.
        let inner = scope.into_inner();
        let pairs = scan_all(&inner, false).await;
        assert!(
            pairs.is_empty(),
            "the scoped wrapper cannot verify a keyless clear-range response"
        );
    });
}

// ============================================================================
// ScopedKvStore properties
// ============================================================================

/// Arbitrary caller-space contents of one scope.
fn arb_entries() -> impl Strategy<Value = BTreeMap<Vec<u8>, Vec<u8>>> {
    prop::collection::btree_map(arb_key(), arb_value(), 0..8)
}

/// Two prefixes where neither is a prefix of the other.
///
/// Nesting is a deliberate feature ([`ScopedKvStore::scoped`]), so a scope
/// under another scope's prefix is *supposed* to see the parent's keys.
/// Isolation is only claimed for genuinely disjoint namespaces, and the empty
/// prefix — a prefix of everything — is excluded by the same filter.
fn arb_disjoint_prefixes() -> impl Strategy<Value = (Vec<u8>, Vec<u8>)> {
    (arb_key(), arb_key()).prop_filter("neither prefix may contain the other", |(a, b)| {
        !a.starts_with(b) && !b.starts_with(a)
    })
}

proptest! {
    // Each case builds a store and runs several scans.
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// A scope is a faithful view of the map written through it: what goes in
    /// comes back out, point reads and full scans agree, and the scan order is
    /// the store's lexicographic order in both directions.
    #[test]
    fn prop_scope_round_trips_every_entry(
        prefix in arb_key(),
        entries in arb_entries(),
    ) {
        block_on(async {
            let mut scope = ScopedKvStore::new(open_store().transaction(), prefix.clone());
            for (key, value) in &entries {
                scope.set(key, value).await.expect("set must succeed");
            }

            for (key, value) in &entries {
                let stored = scope.get(key).await.expect("get must succeed");
                prop_assert_eq!(stored.as_ref(), Some(value));
                prop_assert!(scope.exists(key).await.expect("exists must succeed"));
            }

            let forward = scan_all(&scope, false).await;
            let expected: Vec<(Vec<u8>, Vec<u8>)> = entries
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            prop_assert_eq!(&forward, &expected);

            let mut reversed = scan_all(&scope, true).await;
            reversed.reverse();
            prop_assert_eq!(&reversed, &expected);

            Ok(())
        })?;
    }

    /// Operations through the `KvStore` surface prefix every caller key before
    /// handing it to the backend.
    #[test]
    fn prop_backend_keys_are_all_prefixed(
        prefix in arb_key(),
        entries in arb_entries(),
    ) {
        block_on(async {
            let mut scope = ScopedKvStore::new(open_store().transaction(), prefix.clone());
            for (key, value) in &entries {
                scope.set(key, value).await.expect("set must succeed");
            }

            // The transaction's merged view holds exactly the prefixed keys.
            let backend = scope.into_inner();
            let raw: Vec<(Vec<u8>, Vec<u8>)> = scan_all(&backend, false).await;
            let raw_keys: Vec<Vec<u8>> = raw.into_iter().map(|(key, _)| key).collect();
            let expected: Vec<Vec<u8>> = entries
                .keys()
                .map(|key| {
                    let mut full = prefix.clone();
                    full.extend_from_slice(key);
                    full
                })
                .collect();
            prop_assert_eq!(raw_keys, expected);
            Ok(())
        })?;
    }

    /// Writes through one scope are invisible to a disjoint one — including a
    /// wholesale `clear_range`, which must not reach past its namespace.
    #[test]
    fn prop_disjoint_scopes_cannot_see_each_other(
        (first_prefix, second_prefix) in arb_disjoint_prefixes(),
        first_entries in arb_entries(),
        second_entries in arb_entries(),
    ) {
        block_on(async {
            let mut first = ScopedKvStore::new(open_store().transaction(), first_prefix.clone());
            for (key, value) in &first_entries {
                first.set(key, value).await.expect("set must succeed");
            }

            // Both scopes share one transaction (one rollback unit); the
            // scoped bounds are what confine each scope's visibility.
            let mut second = ScopedKvStore::new(first.into_inner(), second_prefix.clone());
            for (key, value) in &second_entries {
                second.set(key, value).await.expect("set must succeed");
            }

            // The second scope sees exactly its own writes, whatever the
            // first one put in the same transaction.
            let seen = scan_all(&second, false).await;
            let expected: Vec<(Vec<u8>, Vec<u8>)> = second_entries
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            prop_assert_eq!(seen, expected);

            // Clearing the second namespace wholesale leaves the first intact.
            second
                .clear_range(Bound::Unbounded, Bound::Unbounded)
                .await
                .expect("clear must succeed");

            let first = ScopedKvStore::new(second.into_inner(), first_prefix.clone());
            let survivors = scan_all(&first, false).await;
            let expected: Vec<(Vec<u8>, Vec<u8>)> = first_entries
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            prop_assert_eq!(survivors, expected);

            Ok(())
        })?;
    }
}

// ============================================================================
// Key-space vs scoped-store agreement
// ============================================================================

/// A prefix together with backend contents concentrated around it.
fn arb_prefix_and_entries() -> impl Strategy<Value = (Vec<u8>, BTreeMap<Vec<u8>, Vec<u8>>)> {
    arb_key().prop_flat_map(|prefix| {
        let entries = prop::collection::btree_map(arb_key_near(prefix.clone()), arb_value(), 0..8);
        (Just(prefix), entries)
    })
}

/// A key from a prefix's neighbourhood: the prefix itself, an extension of it,
/// or an unrelated key.
fn arb_key_near(prefix: Vec<u8>) -> impl Strategy<Value = Vec<u8>> {
    let extension = (Just(prefix.clone()), arb_key()).prop_map(|(mut key, tail)| {
        key.extend_from_slice(&tail);
        key
    });
    prop_oneof![1 => extension, 2 => arb_key()]
}

proptest! {
    // Each case builds a store and runs two scans; 64 is enough to cover the
    // prefix relationships the alphabet can produce.
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// The keyspace helpers and [`ScopedKvStore`] must agree, because the
    /// scoped store *is* the production consumer of these bounds. Scanning the
    /// backend over `prefix_range(p)` has to return exactly what a scope at
    /// `p` reports, prefix re-attached.
    #[test]
    fn prop_prefix_range_agrees_with_the_scoped_store(
        (prefix, entries) in arb_prefix_and_entries(),
    ) {
        block_on(async {
            let mut tx = open_transaction();
            for (key, value) in &entries {
                tx.set(key, value).await.expect("seed must succeed");
            }

            let (start, end) = fasm_storage::prefix_range(&prefix);
            let via_bounds: Vec<Vec<u8>> = tx
                .range(bound_as_slice(&start), bound_as_slice(&end), false)
                .collect()
                .await
                .expect("scan must succeed")
                .into_iter()
                .map(|pair| pair.key)
                .collect();

            let scope = ScopedKvStore::new(tx, prefix.clone());
            let via_scope: Vec<Vec<u8>> = scope
                .range(Bound::Unbounded, Bound::Unbounded, false)
                .collect()
                .await
                .expect("scoped scan must succeed")
                .into_iter()
                .map(|pair| {
                    let mut full = prefix.clone();
                    full.extend_from_slice(&pair.key);
                    full
                })
                .collect();

            prop_assert_eq!(via_bounds, via_scope);
            Ok(())
        })?;
    }
}
