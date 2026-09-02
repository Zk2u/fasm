//! Tests for the FoundationDB backend.
//!
//! Gated behind the `fdb-storage-tests` feature: the conformance suite drives
//! real FDB round-trips, which need a live cluster and `libfdb_c` to link the
//! test binary. With the feature off (the local compile gate) this module
//! compiles to nothing. CI enables the feature and runs it against a cluster.
//!
//! FDB is a shared cluster, not an isolated per-test database, so every store
//! handle is scoped under a unique prefix. That keeps each conformance test's
//! keyspace empty and independent, and uncommitted transactions drop their
//! writes, so no cleanup is needed.

#![cfg(feature = "fdb-storage-tests")]

use std::{
    ops::Bound,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Once},
};

use fasm_storage::{Commit, KvStore};

use crate::{FdbReadOnlyStore, FdbStorageError, FdbTransaction};

static FDB_BOOT: Once = Once::new();
static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
static RUN_ID: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
});

/// Initialize the FoundationDB network thread (idempotent).
///
/// The returned `NetworkAutoStop` is intentionally leaked so the network thread
/// stays alive for the duration of the test process.
fn fdb_boot() {
    FDB_BOOT.call_once(|| {
        // SAFETY: `foundationdb::boot` must be called at most once before any FDB
        // operations; `Once` guarantees single execution. The handle is leaked so
        // the network thread is never torn down mid-test.
        let handle = unsafe { foundationdb::boot() };
        std::mem::forget(handle);
    });
}

/// A unique scope prefix per store handle, so each test's keyspace is empty and
/// independent on the shared cluster.
fn unique_prefix() -> Vec<u8> {
    let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("fasm-fdb-test-{}-{id}/", *RUN_ID).into_bytes()
}

/// A fresh, empty read-write transaction scoped under a unique prefix.
async fn create_transaction() -> FdbTransaction {
    fdb_boot();
    let db = foundationdb::Database::default().expect("open the FDB database");
    let tx = db.create_trx().expect("create a transaction");
    FdbTransaction::new(tx, unique_prefix())
}

/// Drive a future to completion on a single-threaded runtime. FDB's futures are
/// completed by FDB's own network thread, so the executor only needs to park and
/// resume.
fn block_on<F: core::future::Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build the test runtime")
        .block_on(fut)
}

/// The shared conformance suite, run against a fresh FDB write transaction.
mod fdb_conformance {
    use super::create_transaction;

    fasm_storage::kv_store_tests!(
        store = create_transaction().await,
        block_on = super::block_on,
    );
}

/// A commit is durable: a fresh reader with the same prefix sees the committed
/// key, and a reader under a different prefix does not.
#[test]
fn a_commit_persists_for_a_fresh_reader() {
    block_on(async {
        fdb_boot();
        let db = foundationdb::Database::default().expect("open the FDB database");
        let prefix = unique_prefix();

        let mut tx =
            FdbTransaction::new(db.create_trx().expect("write transaction"), prefix.clone());
        tx.set(b"note/1", b"a").await.expect("set");
        tx.set(b"note/2", b"b").await.expect("set");
        tx.commit().await.expect("commit");

        let reader = FdbReadOnlyStore::new(Arc::new(db), prefix);
        assert_eq!(
            reader.get(b"note/1").await.expect("get").as_deref(),
            Some(b"a".as_slice())
        );
        assert_eq!(
            reader.get(b"note/2").await.expect("get").as_deref(),
            Some(b"b".as_slice())
        );
    });
}

/// A drop of an uncommitted transaction discards its writes.
#[test]
fn an_uncommitted_transaction_rolls_back_on_drop() {
    block_on(async {
        fdb_boot();
        let db = foundationdb::Database::default().expect("open the FDB database");
        let prefix = unique_prefix();

        // Seed a committed key.
        let mut seed =
            FdbTransaction::new(db.create_trx().expect("seed transaction"), prefix.clone());
        seed.set(b"seed", b"committed").await.expect("set");
        seed.commit().await.expect("commit");

        // An uncommitted write in a fresh transaction under the same prefix.
        let mut ephemeral = FdbTransaction::new(
            db.create_trx().expect("ephemeral transaction"),
            prefix.clone(),
        );
        ephemeral.set(b"ephemeral", b"gone").await.expect("set");
        drop(ephemeral); // uncommitted

        let reader = FdbReadOnlyStore::new(Arc::new(db), prefix);
        assert_eq!(reader.get(b"ephemeral").await.expect("get"), None);
        assert_eq!(
            reader.get(b"seed").await.expect("get").as_deref(),
            Some(b"committed".as_slice())
        );
    });
}

/// The read-only handle serves reads and scans, and rejects every mutation with
/// `ReadOnlyMutation`.
#[test]
fn a_reader_serves_reads_and_rejects_mutations() {
    block_on(async {
        fdb_boot();
        let db = foundationdb::Database::default().expect("open the FDB database");
        let prefix = unique_prefix();

        // Seed committed data.
        let mut seed =
            FdbTransaction::new(db.create_trx().expect("seed transaction"), prefix.clone());
        seed.set(b"0", b"v0").await.expect("set");
        seed.set(b"1", b"v1").await.expect("set");
        seed.commit().await.expect("commit");

        let mut reader = FdbReadOnlyStore::new(Arc::new(db), prefix);

        // Reads and scans work.
        assert_eq!(
            reader.get(b"1").await.expect("get").as_deref(),
            Some(b"v1".as_slice())
        );
        let mut scan = reader.range(Bound::Unbounded, Bound::Unbounded, false);
        let mut keys = Vec::new();
        while let Some((pair, rest)) = scan.next().await.expect("scan step") {
            keys.push(pair.key);
            scan = rest;
        }
        assert_eq!(keys, vec![b"0".to_vec(), b"1".to_vec()]);

        // Every mutation is rejected.
        assert!(matches!(
            reader.set(b"k", b"v").await,
            Err(FdbStorageError::ReadOnlyMutation)
        ));
        assert!(matches!(
            reader.delete(b"k").await,
            Err(FdbStorageError::ReadOnlyMutation)
        ));
        assert!(matches!(
            reader.clear_range(Bound::Unbounded, Bound::Unbounded).await,
            Err(FdbStorageError::ReadOnlyMutation)
        ));
    });
}
