//! Tests for the redb backend.
//!
//! The full [`fasm_storage::kv_store_tests!`] conformance suite runs against the
//! write transaction and against a root-pinned [`ScopedKvStore`], so redb's
//! ordering, bound, reverse, paging and range-delete answers are held to the
//! same answer key as every other backend. The redb-specific behaviours the
//! shared suite cannot cover — the commit handed to the dedicated db thread,
//! rollback on drop, the read-only reader, and the non-parking open — are
//! tested here.
//!
//! A real executor is required rather than a poll-once block: a commit hands
//! its fsync to a dedicated std thread and awaits the reply over a oneshot, so
//! the future parks until that thread answers. A single-threaded tokio runtime
//! drives that park/resume.

use std::ops::Bound;
use std::time::Duration;

use fasm_storage::{Commit, KvDirNav, KvPair, KvStore, RawKv, ScopedKvStore};

use crate::{RedbReader, RedbStore, RedbTransaction};

/// A read-only [`RawKv`] view over an owned raw-row map, for running the
/// layout's `validate` against a dumped table.
struct MapRawKv(std::collections::BTreeMap<Vec<u8>, Vec<u8>>);

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

/// Drive a future to completion on a single-threaded runtime. The future need
/// not be `Send`: it runs on the calling thread, which is what lets a commit
/// await a reply from the store's dedicated db thread.
fn block_on<F: core::future::Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build the test runtime")
        .block_on(fut)
}

fn open_store() -> RedbStore {
    RedbStore::in_memory().expect("in-memory store")
}

/// The conformance suite, run against a redb write transaction.
mod redb_conformance {
    use super::{block_on, open_store};

    fasm_storage::kv_store_tests!(
        store = open_store().transaction().expect("write transaction"),
        block_on = block_on,
    );
    fasm_storage::kv_nav_tests!(
        store = open_store().transaction().expect("write transaction"),
        block_on = block_on,
    );
}

/// The conformance suite, run against a scoped store pinned at the engine
/// root: for a root pin the caller-space root is the engine root, so the nav
/// conformance applies too.
mod root_pinned_redb_conformance {
    use super::{block_on, open_store};

    use fasm_storage::ScopedKvStore;

    fn store() -> ScopedKvStore<crate::RedbTransaction> {
        ScopedKvStore::new(
            open_store().transaction().expect("write transaction"),
            vec![],
        )
    }

    fasm_storage::kv_store_tests!(store = store(), block_on = block_on,);
    fasm_storage::kv_nav_tests!(store = store(), block_on = block_on,);
}

/// The conformance suite, run against a scoped store over a redb write
/// transaction pinned to a non-root directory.
mod scoped_redb_conformance {
    use super::{block_on, open_store};

    use fasm_storage::ScopedKvStore;

    fasm_storage::kv_store_tests!(
        store = ScopedKvStore::new(
            open_store().transaction().expect("write transaction"),
            vec![b"r1".to_vec()],
        ),
        block_on = block_on,
    );
}

/// A commit is handed to the dedicated db thread; a fresh reader then sees the
/// committed data.
#[test]
fn a_commit_persists_for_a_fresh_reader() {
    let store = open_store();
    let mut tx = store.transaction().expect("write transaction");
    block_on(async move {
        tx.set(&[b"note"], b"1", b"a").await.expect("set");
        tx.set(&[b"note"], b"2", b"b").await.expect("set");
        tx.commit().await.expect("commit");
    });

    let reader = store.reader().expect("reader");
    assert_eq!(
        block_on(reader.get(&[b"note"], b"1")).unwrap().as_deref(),
        Some(b"a".as_slice())
    );
    assert_eq!(
        block_on(reader.get(&[b"note"], b"2")).unwrap().as_deref(),
        Some(b"b".as_slice())
    );
}

/// An uncommitted transaction rolls back on drop: its writes are not visible to
/// a fresh reader, and the committed data it opened over is untouched.
#[test]
fn an_uncommitted_transaction_rolls_back_on_drop() {
    let store = open_store();

    let mut tx = store.transaction().expect("seed transaction");
    block_on(async move {
        tx.set(&[b"s"], b"seed", b"committed").await.expect("set");
        tx.commit().await.expect("commit");
    });

    let mut tx = store.transaction().expect("uncommitted transaction");
    block_on(async move {
        tx.set(&[b"e"], b"ephemeral", b"gone").await.expect("set");
        // tx is dropped here, uncommitted.
    });

    let reader = store.reader().expect("reader");
    assert_eq!(
        block_on(reader.get(&[b"e"], b"ephemeral")).unwrap(),
        None,
        "an uncommitted write must not persist"
    );
    assert_eq!(
        block_on(reader.get(&[b"s"], b"seed")).unwrap().as_deref(),
        Some(b"committed".as_slice()),
        "the committed seed must survive the rolled-back transaction"
    );
}

/// The reader is a consistent read-only view: reads and scans work, and every
/// mutation is rejected. An existing directory's `remove_dir` surfaces the
/// engine's first raw write as a `ReadOnly` error; a missing directory reports
/// `Ok(false)` without attempting a write.
#[test]
fn a_reader_serves_reads_and_rejects_mutations() {
    let store = open_store();
    let mut tx = store.transaction().expect("seed transaction");
    block_on(async move {
        tx.set(&[], b"0", b"v0").await.expect("set");
        tx.set(&[], b"1", b"v1").await.expect("set");
        tx.set(&[b"note"], b"n", b"vn").await.expect("set");
        tx.commit().await.expect("commit");
    });

    let mut reader: RedbReader = store.reader().expect("reader");

    // Reads and scans work.
    assert_eq!(
        block_on(reader.get(&[], b"1")).unwrap().as_deref(),
        Some(b"v1".as_slice())
    );
    let scan = block_on(async {
        let mut stream = reader.range(&[], Bound::Unbounded, Bound::Unbounded, false);
        let mut out = Vec::new();
        while let Some((pair, rest)) = stream.next().await.expect("scan step") {
            out.push(pair.key);
            stream = rest;
        }
        out
    });
    assert_eq!(scan, vec![b"0".to_vec(), b"1".to_vec()]);
    assert_eq!(
        block_on(reader.list_dirs(&[])).unwrap(),
        vec![b"note".to_vec()]
    );

    // Every mutation is rejected.
    let err = block_on(reader.set(&[], b"k", b"v")).expect_err("set on reader");
    assert!(
        err.to_string().contains("read-only"),
        "set surfaces the read-only error: {err}"
    );
    let err = block_on(reader.delete(&[], b"k")).expect_err("delete on reader");
    assert!(
        err.to_string().contains("read-only"),
        "delete surfaces the read-only error: {err}"
    );
    let err = block_on(reader.clear_range(&[], Bound::Unbounded, Bound::Unbounded))
        .expect_err("clear on reader");
    assert!(
        err.to_string().contains("read-only"),
        "clear surfaces the read-only error: {err}"
    );
    let err =
        block_on(reader.remove_dir(&[b"note"])).expect_err("remove of an existing dir on a reader");
    assert!(
        err.to_string().contains("read-only"),
        "remove_dir surfaces the read-only error: {err}"
    );
    // A missing directory needs no write.
    assert!(!block_on(reader.remove_dir(&[b"absent"])).expect("remove of missing dir"));
}

/// A scoped commit commits the underlying redb transaction.
#[test]
fn a_scoped_commit_forwards_to_the_transaction() {
    let store = open_store();
    let mut scoped = ScopedKvStore::new(
        store.transaction().expect("write transaction"),
        vec![b"scope".to_vec()],
    );
    block_on(async move {
        scoped.set(&[], b"a", b"1").await.expect("set");
        scoped.commit().await.expect("commit");
    });

    // The committed key lives in the pinned directory in the backing store.
    let reader = store.reader().expect("reader");
    assert_eq!(
        block_on(reader.get(&[b"scope"], b"a")).unwrap().as_deref(),
        Some(b"1".as_slice())
    );
}

/// `transaction_nonparking` must suspend, not park the calling thread, while a
/// writer is held: on this single-threaded runtime a parked thread would stop
/// the timer branch of the `select!` from firing, and the test would hang
/// rather than fail fast — which is exactly what a regression to the blocking
/// open does.
#[tokio::test]
async fn a_nonparking_open_suspends_while_a_writer_is_held() {
    let store = open_store();
    let (release, held) = std::sync::mpsc::channel::<()>();
    let holder = {
        let store = store.clone();
        std::thread::spawn(move || {
            let tx: RedbTransaction = store.transaction().expect("holder transaction");
            // Hold the database's one writer until the test says otherwise.
            held.recv().expect("release signal");
            drop(tx);
        })
    };
    // Give the holder time to actually acquire the writer.
    std::thread::sleep(Duration::from_millis(50));

    let open = store.transaction_nonparking();
    tokio::pin!(open);
    tokio::select! {
        _ = &mut open => panic!("the open completed while the writer was still held"),
        _ = tokio::time::sleep(Duration::from_millis(100)) => {
            // The timer fired, so this thread kept running while the open
            // waited — the wait is a suspension, not a parked thread.
        }
    }

    release.send(()).expect("release the holder");
    let tx = open
        .await
        .expect("the open completes once the writer is released");
    drop(tx);
    holder.join().expect("join the holder thread");
}

/// A `RedbStore` is cloneable; clones share the one database and the one
/// dedicated commit thread.
#[test]
fn a_store_clone_shares_the_database() {
    let store = open_store();
    let clone = store.clone();

    let mut tx = store.transaction().expect("write transaction");
    block_on(async move {
        tx.set(&[b"sh"], b"shared", b"1").await.expect("set");
        tx.commit().await.expect("commit");
    });

    // The clone sees the same committed data.
    let reader = clone.reader().expect("reader on the clone");
    assert_eq!(
        block_on(reader.get(&[b"sh"], b"shared"))
            .unwrap()
            .as_deref(),
        Some(b"1".as_slice())
    );
}

/// The `KvStream` surface over a transaction: `keys` and paged `next` on a
/// bounded forward scan.
#[test]
fn keys_and_take_on_a_bounded_scan() {
    let store = open_store();
    let mut tx = store.transaction().expect("write transaction");
    block_on(async move {
        for i in 0..10u8 {
            let key = i.to_be_bytes();
            tx.set(&[b"k"], &key, &[i]).await.expect("set");
        }
        let keys: Vec<Vec<u8>> =
            collect_keys(tx.range(&[b"k"], Bound::Unbounded, Bound::Unbounded, false)).await;
        assert_eq!(keys.len(), 10);
        // A paged scan takes the three smallest keys, stepping through the
        // stream handles.
        let mut got = Vec::new();
        let mut stream = tx.range(&[b"k"], Bound::Unbounded, Bound::Unbounded, false);
        for _ in 0..3 {
            let Some((pair, rest)) = stream.next().await.expect("step") else {
                panic!("the stream ended before three keys")
            };
            got.push(pair.key);
            stream = rest;
        }
        assert_eq!(
            got,
            vec![0u8.to_be_bytes(), 1u8.to_be_bytes(), 2u8.to_be_bytes()]
        );
        // A missing directory yields an empty stream.
        let empty = tx.range(&[b"absent"], Bound::Unbounded, Bound::Unbounded, false);
        assert!(empty.next().await.expect("empty step").is_none());
    });
}

/// A bounded range with an `Included` end includes the key at the end; an
/// `Excluded` start excludes the key at the start. (Spot checks on top of the
/// shared conformance suite.)
#[test]
fn bound_inclusion_spot_checks() {
    let store = open_store();
    let mut tx = store.transaction().expect("write transaction");
    block_on(async move {
        tx.set(&[b"b"], b"1", b"v1").await.expect("set");
        tx.set(&[b"b"], b"2", b"v2").await.expect("set");
        tx.set(&[b"b"], b"3", b"v3").await.expect("set");

        let keys: Vec<Vec<u8>> =
            collect_keys(tx.range(&[b"b"], Bound::Included(b"1"), Bound::Included(b"2"), false))
                .await;
        assert_eq!(keys, vec![b"1".to_vec(), b"2".to_vec()]);

        let keys: Vec<Vec<u8>> =
            collect_keys(tx.range(&[b"b"], Bound::Excluded(b"1"), Bound::Unbounded, false)).await;
        assert_eq!(keys, vec![b"2".to_vec(), b"3".to_vec()]);
    });
}

// ============================================================================
// File-backed layout tests
//
// The persistent-backend specifics the in-memory tests cannot cover: the
// open-time layout-version check, the raw rows a fresh file is
// initialised with, and the structural layout equivalence (the same
// operations on two files resolve the same paths, validate, and expose
// the same visible data).

use fasm_storage::RetryableStorageError;
use fasm_storage::flatdir as fdir;
use std::collections::BTreeMap;

static FILE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static RUN_ID: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
});

/// A unique file path in the system temp dir.
fn unique_file() -> std::path::PathBuf {
    let n = FILE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("fasm-redb-test-{}-{}", *RUN_ID, n))
}

fn table_name() -> redb::TableDefinition<'static, &'static [u8], &'static [u8]> {
    redb::TableDefinition::new("kv")
}

/// Dump a file's raw rows directly through redb (the store must be dropped
/// first so no writer is outstanding). redb allows one handle per file per
/// process, and the store's dedicated open thread still holds an
/// `Arc<Database>` until it unwinds, so retry briefly until the last
/// handle drops.
fn dump_raw_rows(path: &std::path::Path) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut attempt = 0;
    loop {
        match open_raw_rows(path) {
            Ok(rows) => return rows,
            Err(e) => {
                attempt += 1;
                assert!(attempt < 100, "re-open never succeeded over 5s: {e}");
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn open_raw_rows(path: &std::path::Path) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, Box<redb::Error>> {
    let db = redb::Database::open(path).map_err(|e| Box::new(e.into()))?;
    let rtx = db.begin_read().map_err(|e| Box::new(e.into()))?;
    let table = rtx
        .open_table(table_name())
        .map_err(|e| Box::new(e.into()))?;
    let mut rows = BTreeMap::new();
    let start = Bound::<&[u8]>::Unbounded;
    let end = Bound::<&[u8]>::Unbounded;
    let iter = table
        .range::<&[u8]>((start, end))
        .map_err(|e| Box::new(e.into()))?;
    for pair in iter {
        let (k, v) = pair.map_err(|e| Box::new(e.into()))?;
        rows.insert(k.value().to_vec(), v.value().to_vec());
    }
    Ok(rows)
}

async fn run_reference_ops(tx: &mut RedbTransaction) {
    tx.set(&[], b"k", b"v1").await.expect("set k");
    tx.set(&[b"a"], b"x", b"y").await.expect("set x");
    tx.set(&[b"a", b"b"], b"m", b"n").await.expect("set m");
    tx.delete(&[], b"k").await.expect("delete k");
    tx.set(&[b"a"], b"x", b"y2").await.expect("set x2");
    tx.clear_range(&[b"a"], Bound::Unbounded, Bound::Unbounded)
        .await
        .expect("clear a");
}

#[test]
fn open_fails_on_a_foreign_version_entry() {
    // A major = 2 row (newer major), a minor = 1 row (the read-only
    // rule), and a truncated value all fail closed.
    for (major, minor, len) in [(2u32, 0u32, 12usize), (1, 1, 12), (1, 0, 8)] {
        let path = unique_file();
        {
            let db = redb::Database::create(&path).expect("create the file");
            let wtx = db.begin_write().expect("write transaction");
            {
                let mut table = wtx.open_table(table_name()).expect("kv table");
                let mut v = Vec::new();
                v.extend_from_slice(&major.to_le_bytes());
                v.extend_from_slice(&minor.to_le_bytes());
                v.extend_from_slice(&0u32.to_le_bytes());
                table
                    .insert(fdir::VERSION_KEY, &v[..len])
                    .expect("insert the foreign version");
            }
            wtx.commit().expect("commit");
        }
        let err = RedbStore::open(&path).expect_err("a mismatched version must fail the open");
        assert!(!RetryableStorageError::is_retryable(&err));
        assert!(
            err.to_string().contains("different layout version"),
            "unexpected error: {err}"
        );
        let _ = std::fs::remove_file(&path);
    }
}

/// A version row the engine accepts (major <= 1, minor == 0, any patch)
/// opens; a table without a version row opens fresh.
#[test]
fn open_accepts_supported_version_rows() {
    for (major, minor, patch) in [(1u32, 0u32, 7u32), (0, 0, 0)] {
        let path = unique_file();
        {
            let db = redb::Database::create(&path).expect("create the file");
            let wtx = db.begin_write().expect("write transaction");
            {
                let mut table = wtx.open_table(table_name()).expect("kv table");
                let mut v = Vec::new();
                v.extend_from_slice(&major.to_le_bytes());
                v.extend_from_slice(&minor.to_le_bytes());
                v.extend_from_slice(&patch.to_le_bytes());
                table
                    .insert(fdir::VERSION_KEY, v.as_slice())
                    .expect("insert the version");
            }
            wtx.commit().expect("commit");
        }
        RedbStore::open(&path).expect("a supported version must open");
        let _ = std::fs::remove_file(&path);
    }
}

#[test]
fn open_accepts_a_table_without_a_version_row() {
    let path = unique_file();
    {
        let db = redb::Database::create(&path).expect("create the file");
        let wtx = db.begin_write().expect("write transaction");
        {
            let mut table = wtx.open_table(table_name()).expect("kv table");
            table
                .insert(b"other".as_slice(), b"row".as_slice())
                .expect("insert an unrelated row");
        }
        wtx.commit().expect("commit");
    }
    RedbStore::open(&path).expect("a table without a version row opens fresh");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn a_fresh_file_is_initialised_by_the_first_committed_write() {
    let path = unique_file();
    {
        let store = RedbStore::open(&path).expect("open the fresh file");
        let mut tx = store.transaction().expect("write transaction");
        block_on(async move {
            tx.set(&[b"swap"], b"k", b"v").await.expect("set");
            tx.commit().await.expect("commit");
        });
        drop(store);
    }
    let rows = dump_raw_rows(&path);
    // The lazily written layout: version row, the anchor child + layer
    // rows, the directory child + layer rows, the HCA counter + two recent
    // rows, and the one data row.
    assert_eq!(rows.len(), 9, "unexpected raw rows: {rows:?}");
    assert_eq!(
        fdir::parse_version(rows.get(fdir::VERSION_KEY).expect("version row").as_slice()),
        Some((1, 0, 0))
    );
    // The data row's prefix sits in the packed-i64 band and the whole
    // layout is self-consistent.
    let data = rows
        .iter()
        .find(|(k, _)| (0x0c..=0x1c).contains(&k[0]))
        .expect("a data row");
    assert_eq!(&data.1[..], b"v");
    fdir::ops::validate(&MapRawKv(rows)).expect("the layout must validate");
    // A second directory with one data row adds exactly four rows:
    // the child row, the layer row, the data row, and the HCA recent
    // row for the new allocation.
    {
        let store = RedbStore::open(&path).expect("re-open");
        let mut tx = store.transaction().expect("write transaction");
        block_on(async move {
            tx.set(&[b"b"], b"k2", b"v2").await.expect("set b");
            tx.commit().await.expect("commit b");
        });
        drop(store);
    }
    let rows = dump_raw_rows(&path);
    assert_eq!(rows.len(), 13, "unexpected raw rows: {rows:?}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn the_version_entry_is_1_0_0_after_any_write() {
    let path = unique_file();
    {
        let store = RedbStore::open(&path).expect("open the fresh file");
        let mut tx = store.transaction().expect("write transaction");
        block_on(async move {
            tx.set(&[b"swap"], b"k", b"v").await.expect("set");
            tx.commit().await.expect("commit");
        });
        drop(store);
    }
    let rows = dump_raw_rows(&path);
    assert_eq!(
        fdir::parse_version(rows.get(fdir::VERSION_KEY).expect("version row").as_slice()),
        Some((1, 0, 0))
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn the_same_operations_resolve_the_same_paths_and_validate() {
    let p1 = unique_file();
    let p2 = unique_file();
    for path in [&p1, &p2] {
        let store = RedbStore::open(path).expect("open the fresh file");
        let mut tx = store.transaction().expect("write transaction");
        block_on(async move {
            run_reference_ops(&mut tx).await;
            tx.commit().await.expect("commit");
        });
        drop(store);
    }
    let raw1 = dump_raw_rows(&p1);
    let raw2 = dump_raw_rows(&p2);
    // Data rows only: keep the rows whose prefix sits in the packed-i64
    // band (the meta regions start at 0xFE or live under the root node).
    let data_rows = |rows: &BTreeMap<Vec<u8>, Vec<u8>>| -> Vec<(Vec<u8>, Vec<u8>)> {
        rows.iter()
            .filter(|(k, _)| (0x0c..=0x1c).contains(&k[0]))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };
    // The HCA samples its candidate within the window (the verbatim FDB
    // contention-distribution design), so the two layouts are not
    // byte-identical. What is guaranteed: a self-consistent layout, and
    // the same visible data.
    // The reference ops' end state is a deterministic 12-row layout:
    // the version row, the HCA counter row, one child row and one layer
    // row per directory (the harness root segment, `a`, and `a.b`), one
    // HCA recent row per allocation (three in total), and the one
    // surviving data row.
    assert_eq!(raw1.len(), 12, "unexpected raw rows: {raw1:?}");
    assert_eq!(raw2.len(), 12, "unexpected raw rows: {raw2:?}");
    let d1 = data_rows(&raw1);
    let d2 = data_rows(&raw2);
    fdir::ops::validate(&MapRawKv(raw1.clone())).expect("layout 1 must validate");
    fdir::ops::validate(&MapRawKv(raw2.clone())).expect("layout 2 must validate");
    assert_eq!(d1.len(), 1, "unexpected data rows: {raw1:?}");
    assert_eq!(d2.len(), 1);
    // The surviving row is `[a, b]`'s `m -> n`: relative key `m`.
    assert_eq!(&d1[0].1[..], b"n");
    assert_eq!(&d2[0].1[..], b"n");
    // Both relative keys decode to `m` (the last byte of the data key).
    assert_eq!(d1[0].0.last(), Some(&b'm'));
    assert_eq!(d2[0].0.last(), Some(&b'm'));
    // Re-open and read through the store API: the resolved path must
    // expose the surviving key (path-to-prefix resolution, not just raw
    // row shapes).
    {
        let store = RedbStore::open(&p1).expect("re-open layout 1");
        let r = store.transaction().expect("read transaction");
        block_on(async move {
            assert_eq!(
                r.get(&[b"a", b"b"], b"m").await.unwrap(),
                Some(b"n".to_vec())
            );
        });
    }
    let _ = std::fs::remove_file(&p1);
    let _ = std::fs::remove_file(&p2);
}

/// Drain a `KvStream`, keeping each pair's key.
async fn collect_keys(
    mut stream: fasm_storage::KvStream<'_, crate::RedbStorageError>,
) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while let Some((pair, rest)) = stream.next().await.expect("scan step") {
        out.push(pair.key);
        stream = rest;
    }
    out
}
