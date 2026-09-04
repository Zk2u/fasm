//! Tests for the FoundationDB backend.
//!
//! Gated behind the `fdb-storage-tests` feature: the conformance suite drives
//! real FDB round-trips, which need a live cluster and `libfdb_c` to link the
//! test binary. With the feature off (the local compile gate) this module
//! compiles to nothing. CI enables the feature and runs it against a cluster.
//!
//! FDB is a shared cluster, not an isolated per-test database. Every
//! per-test store is pinned to a unique directory (`run <id> / test
//! <counter>`); each conformance test's keyspace is therefore empty and
//! independent, and uncommitted transactions drop their writes, so no
//! cleanup is needed. All committing tests share the one top-level `run
//! <id>` segment, so the root listing can gain at most one child per
//! binary run; the root-level nav suite asserts exact listings against a
//! baseline it captures at its own start, and the binary runs
//! single-threaded (CI passes `--test-threads=1`) so no commit lands
//! inside a listing window.

#![cfg(feature = "fdb-storage-tests")]

use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Once},
};

use fasm_storage::{Commit, KvDirNav, KvPair, KvStore, RawKv, ScopedKvStore};
use foundationdb::{KeySelector, RangeOption, directory::Directory};
use tokio::runtime::Runtime;

use crate::{FdbReadOnlyStore, FdbStorageError, FdbTransaction};

static FDB_BOOT: Once = Once::new();
static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
static RUN_ID: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
});
static RUNTIME: std::sync::LazyLock<Runtime> = std::sync::LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
});

/// Start the FDB network at most once, then warm the cluster.
///
/// The network boot is a synchronous C call (`FDB_network_init`), and the
/// warm-up runs on its own thread with its own runtime: the store factories
/// this guards are evaluated inside the `kv_store_tests!` macro's
/// block-on'd future, where a nested `block_on` on the shared current-thread
/// runtime would panic. A fresh CI cluster can answer the first
/// transactions with transient errors, so the warm-up retries any
/// officially retryable error with backoff before the suite is allowed
/// to run.
fn ensure_booted() {
    FDB_BOOT.call_once(|| {
        // SAFETY: `foundationdb::boot` must be called at most once before
        // any FDB operation; the `Once` guarantees single execution.
        // The returned `NetworkAutoStop` is leaked, not dropped: its
        // `Drop` implementation calls `fdb_stop_network`, after which
        // every FDB operation in this process fails with
        // broken_promise (1100).
        std::mem::forget(unsafe { foundationdb::boot() });
        std::thread::spawn(warm_up_cluster)
            .join()
            .expect("warm-up thread");
    });
}

fn warm_up_cluster() {
    let mut delay = std::time::Duration::from_millis(100);
    for _ in 0..30 {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("warm-up runtime");
        match rt.block_on(async {
            let db = foundationdb::Database::default()?;
            let tx = db.create_trx()?;
            tx.get(b"warm-up", false).await
        }) {
            Ok(_) => return,
            Err(e) if e.is_retryable() => std::thread::sleep(delay),
            Err(e) => panic!("cluster warm-up failed: {e}"),
        }
        delay = (delay * 2).min(std::time::Duration::from_secs(1));
    }
    panic!("cluster warm-up: retryable errors persisted after 30 attempts");
}

fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    ensure_booted();
    RUNTIME.block_on(fut)
}

fn unique_dir() -> Vec<Vec<u8>> {
    let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    vec![
        format!("run {}", *RUN_ID).into_bytes(),
        format!("t{}", n).into_bytes(),
    ]
}

/// Borrow a unique directory as `&[&[u8]]` for `KvStore` calls.
fn dref(d: &[Vec<u8>]) -> Vec<&[u8]> {
    d.iter().map(|s| s.as_slice()).collect()
}

/// Open a transaction handle. Fully synchronous: `Database::default()` and
/// `create_trx()` are synchronous C calls, and this factory is evaluated
/// inside the conformance macro's block-on'd future, where a nested
/// `block_on` would panic.
fn open_transaction() -> FdbTransaction {
    ensure_booted();
    let db = foundationdb::Database::default().expect("database");
    FdbTransaction::new(db.create_trx().expect("transaction"))
}

fn open_scoped_transaction() -> ScopedKvStore<FdbTransaction> {
    ScopedKvStore::new(open_transaction(), unique_dir())
}

fn open_reader() -> FdbReadOnlyStore {
    ensure_booted();
    let db = foundationdb::Database::default().expect("database");
    FdbReadOnlyStore::new(Arc::new(db))
}

mod fdb_conformance {
    use super::*;

    fasm_storage::kv_store_tests! {
        store = open_transaction(),
        block_on = block_on,
    }

    // The engine root is the engine root, so the nav suite (including the
    // root-not-removable assertion) applies.
    fasm_storage::kv_nav_tests! {
        store = open_transaction(),
        block_on = block_on,
    }
}

mod scoped_conformance {
    use super::*;

    fasm_storage::kv_store_tests! {
        store = open_scoped_transaction(),
        block_on = block_on,
    }
}

mod root_pinned_conformance {
    use super::*;

    fasm_storage::kv_store_tests! {
        store = ScopedKvStore::new(open_transaction(), vec![]),
        block_on = block_on,
    }

    fasm_storage::kv_nav_tests! {
        store = ScopedKvStore::new(open_transaction(), vec![]),
        block_on = block_on,
    }
}

#[test]
fn committed_writes_are_visible_to_a_fresh_reader() {
    let dir = unique_dir();
    let d = dref(&dir);
    let mut tx = open_transaction();
    block_on(async {
        tx.set(&d, b"k1", b"v1").await.expect("set");
    });
    block_on(async { tx.commit().await.expect("commit") });

    let reader = open_reader();
    let scoped = ScopedKvStore::new(reader, dir);
    block_on(async {
        let v = scoped.get(&[], b"k1").await.expect("get").expect("value");
        assert_eq!(v, b"v1");
    });
}

#[test]
fn uncommitted_writes_are_not_visible_to_a_fresh_reader() {
    let dir = unique_dir();
    let d = dref(&dir);
    let mut tx = open_transaction();
    block_on(async {
        tx.set(&d, b"k1", b"v1").await.expect("set");
    });
    // Drop the transaction without committing: the writes vanish.
    drop(tx);

    let reader = open_reader();
    let scoped = ScopedKvStore::new(reader, dir);
    block_on(async {
        assert!(scoped.get(&[], b"k1").await.expect("get").is_none());
    });
}

#[test]
fn read_only_store_rejects_mutations() {
    let dir = unique_dir();
    let reader = open_reader();
    let mut scoped = ScopedKvStore::new(reader, dir);
    block_on(async {
        let err = scoped.set(&[], b"k", b"v").await.expect_err("must fail");
        assert!(
            matches!(err, FdbStorageError::ReadOnlyMutation),
            "expected ReadOnlyMutation, got {err:?}"
        );
        let err = scoped.delete(&[], b"k").await.expect_err("must fail");
        assert!(matches!(err, FdbStorageError::ReadOnlyMutation));
        let err = scoped
            .clear_range(&[], Bound::Unbounded, Bound::Unbounded)
            .await
            .expect_err("must fail");
        assert!(matches!(err, FdbStorageError::ReadOnlyMutation));
    });
}

#[test]
fn read_only_store_rejects_remove_dir() {
    let dir = unique_dir();
    let reader = open_reader();
    let mut scoped = ScopedKvStore::new(reader, dir);
    block_on(async {
        let err = scoped.remove_dir(&[]).await.expect_err("must fail");
        assert!(matches!(err, FdbStorageError::ReadOnlyMutation));
    });
}

#[test]
fn read_only_nav_works_on_created_directories() {
    let dir = unique_dir();
    let mut tx = open_transaction();
    block_on(async {
        tx.set(&[dir[0].as_slice(), dir[1].as_slice(), b"n"], b"k", b"v")
            .await
            .expect("set");
    });
    block_on(async { tx.commit().await.expect("commit") });

    let reader = open_reader();
    let scoped = ScopedKvStore::new(reader, dir);
    block_on(async {
        assert!(scoped.dir_exists(&[b"n"]).await.expect("exists"));
        assert!(!scoped.dir_exists(&[b"missing"]).await.expect("exists"));
        let children = scoped.list_dirs(&[]).await.expect("list");
        assert_eq!(children, vec![b"n".to_vec()]);
    });
}

/// Resolve a committed directory's allocated prefix through a fresh default
/// layer (the same placement the store uses).
fn resolve_prefix(dir: &[Vec<u8>]) -> Vec<u8> {
    let fut = async {
        let db = foundationdb::Database::default()?;
        let tx = db.create_trx()?;
        let layer = foundationdb::directory::DirectoryLayer::default();
        // The same anchor mapping the store uses: every layer path is
        // rooted under the reserved segment.
        let mut path: Vec<String> = vec![crate::ROOT_PATH_SEGMENT.to_string()];
        path.extend(
            dir.iter()
                .map(|seg| String::from_utf8(seg.clone()).expect("utf-8 test segment")),
        );
        let out = layer.open(&tx, &path, None).await.map_err(crate::dir_err)?;
        Ok::<Vec<u8>, FdbStorageError>(out.bytes().map_err(crate::dir_err)?.to_vec())
    };
    block_on(fut).expect("resolve the prefix")
}

#[test]
fn allocated_prefixes_stay_out_of_the_meta_region() {
    // FDB's native layer allocates content prefixes from `0x00`..=`0xFD`;
    // `0xFE`/`0xFF` are reserved for the layout and must never be handed
    // out as a directory prefix.
    let dir = unique_dir();
    let d = dref(&dir);
    let mut tx = open_transaction();
    block_on(async {
        tx.set(&d, b"k", b"v").await.expect("set");
    });
    block_on(async { tx.commit().await.expect("commit") });

    let prefix = resolve_prefix(&dir);
    assert!(
        prefix.first().is_some_and(|&b| b <= 0xFD),
        "the allocated prefix enters the meta/reserved region: {prefix:02x?}"
    );
}

/// A read-only [`RawKv`] view over an owned raw-row map, for running the
/// layout's `validate` against a raw-handle dump (the flat backends do
/// the same over their own tables).
struct DumpRawKv(BTreeMap<Vec<u8>, Vec<u8>>);

impl RawKv for DumpRawKv {
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
        let in_start = |k: &[u8]| match start {
            Bound::Unbounded => true,
            Bound::Included(x) => k >= x,
            Bound::Excluded(x) => k > x,
        };
        let in_end = |k: &[u8]| match end {
            Bound::Unbounded => true,
            Bound::Included(x) => k <= x,
            Bound::Excluded(x) => k < x,
        };
        let rows: Vec<KvPair> = self
            .0
            .iter()
            .filter(|(k, _)| in_start(k.as_slice()) && in_end(k.as_slice()))
            .map(|(k, v)| KvPair {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();
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

#[test]
fn the_committed_layout_validates_over_the_raw_handle() {
    // The layout-validation walk runs on every backend (plan §6): the
    // flat backends walk their raw tables; here it walks every row a
    // non-system transaction can read, through a raw database
    // transaction (no directory layer in the read path). That is the
    // whole user keyspace `[start, 0xFF)` — the FDB system keyspace
    // under `0xFF/` is invisible to a non-system transaction, and the
    // layout writes nothing there. The cluster is shared across the
    // suite's tests, so the walk validates the whole accumulated
    // layout (the shared `run <id>` tree and its children), not just
    // this test's directory. It must run on a cluster whose content
    // this layout wrote — `validate` fails on foreign rows by design,
    // which is why this test is for CI's fresh runner, not a shared
    // development cluster. The binary runs single-threaded in CI
    // (`--test-threads=1`), so no commit lands inside the dump.
    let dir = unique_dir();
    let d = dref(&dir);
    let mut tx = open_transaction();
    block_on(async {
        tx.set(&d, b"k", b"v").await.expect("set");
    });
    block_on(async { tx.commit().await.expect("commit") });

    let db = foundationdb::Database::default().expect("database");
    let raw_tx = db.create_trx().expect("transaction");
    // The range streams in pages (`StreamingMode::Iterator`, the
    // `RangeOption` default): the first call must pass `iteration = 1`
    // — the C API rejects `0` with a `process_error` — and the walk
    // follows `next_range` continuations to the end of the range; a
    // partial dump is a structurally incomplete tree and fails
    // `validate`.
    let begin = KeySelector::first_greater_or_equal(&[] as &[u8]);
    let end = KeySelector::first_greater_or_equal(&[0xFFu8]);
    let mut opt = RangeOption::from((begin, end));
    let mut iteration = 1;
    let mut rows: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    block_on(async {
        loop {
            let values = raw_tx
                .get_range(&opt, iteration, false)
                .await
                .expect("raw range");
            let next = opt.next_range(&values);
            for kv in values.into_iter() {
                rows.insert(kv.key().to_vec(), kv.value().to_vec());
            }
            match next {
                Some(o) => {
                    opt = o;
                    iteration += 1;
                }
                None => break,
            }
        }
    });
    fasm_storage::flatdir::ops::validate(&DumpRawKv(rows))
        .expect("the committed layout must validate");
}

#[test]
fn concurrent_same_dir_creations_are_opportunistic() {
    // Two transactions lazily create the same new directory. The backend
    // is opportunistic, not a coordinator: the first commit wins, and the
    // second either lands on the same allocation or loses to a retryable
    // conflict. Either way the path resolves to exactly one prefix and the
    // winner's data is present.
    let dir = unique_dir();
    let d = dref(&dir);
    let mut t1 = open_transaction();
    let mut t2 = open_transaction();
    block_on(async {
        t1.set(&d, b"k1", b"v1").await.expect("set t1");
        t2.set(&d, b"k2", b"v2").await.expect("set t2");
    });
    block_on(async { t1.commit().await.expect("commit t1") });
    let second = block_on(async { t2.commit().await });
    if let Err(e) = second {
        assert!(
            fasm_storage::RetryableStorageError::is_retryable(&e),
            "a second-creation conflict must be retryable: {e:?}"
        );
    }

    let reader = open_reader();
    let scoped = ScopedKvStore::new(reader, dir.clone());
    block_on(async {
        assert!(scoped.dir_exists(&[]).await.expect("exists"));
        assert_eq!(
            scoped.get(&[], b"k1").await.expect("get k1").as_deref(),
            Some(b"v1" as &[u8])
        );
    });
    // Two fresh resolutions agree: one prefix for the path.
    let p1 = resolve_prefix(&dir);
    let p2 = resolve_prefix(&dir);
    assert_eq!(p1, p2);
}

#[test]
fn scan_pages_past_one_page_worth_of_keys() {
    let dir = unique_dir();
    let d = dref(&dir);
    let mut tx = open_transaction();
    let n = 200u32;
    block_on(async {
        for i in 0..n {
            let key = format!("k{i:04}").into_bytes();
            tx.set(&d, &key, b"v").await.expect("set");
        }
    });
    block_on(async { tx.commit().await.expect("commit") });

    let reader = open_reader();
    let scoped = ScopedKvStore::new(reader, dir);
    block_on(async {
        let mut got = 0;
        let mut stream = scoped.range(&[], Bound::Unbounded, Bound::Unbounded, false);
        while let Some((pair, rest)) = stream.next().await.expect("step") {
            let _ = pair;
            got += 1;
            stream = rest;
        }
        assert_eq!(got, n as usize);
    });
}
