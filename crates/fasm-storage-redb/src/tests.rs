//! Tests for the redb backend.
//!
//! The full [`fasm_storage::kv_store_tests!`] conformance suite runs against the
//! write transaction and against a [`ScopedKvStore`] over one, so redb's
//! ordering, bound, reverse, paging and range-delete answers are held to the
//! same answer key as every other backend. The redb-specific behaviours that the
//! shared suite cannot cover — the commit handed to the dedicated db thread,
//! rollback on drop, the read-only reader, and the non-parking open — are tested
//! here.
//!
//! A real executor is required rather than a poll-once block: a commit hands its
//! fsync to a dedicated std thread and awaits the reply over a oneshot, so the
//! future parks until that thread answers. A single-threaded tokio runtime drives
//! that park/resume.

use std::ops::Bound;
use std::time::Duration;

use fasm_storage::{Commit, KvStore, ScopedKvStore};

use crate::{RedbReader, RedbStorageError, RedbStore, RedbTransaction};

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
    use super::open_store;

    fasm_storage::kv_store_tests!(
        store = open_store().transaction().expect("write transaction"),
        block_on = super::block_on,
    );
}

/// The conformance suite, run against a scoped store over a redb write
/// transaction.
mod scoped_redb_conformance {
    use super::open_store;

    use fasm_storage::ScopedKvStore;

    fasm_storage::kv_store_tests!(
        store = ScopedKvStore::new(
            open_store().transaction().expect("write transaction"),
            b"redb/swap/".to_vec(),
        ),
        block_on = super::block_on,
    );
}

/// A commit is handed to the dedicated db thread; a fresh reader then sees the
/// committed data.
#[test]
fn a_commit_persists_for_a_fresh_reader() {
    let store = open_store();
    let mut tx = store.transaction().expect("write transaction");
    block_on(async move {
        tx.set(b"note/1", b"a").await.expect("set");
        tx.set(b"note/2", b"b").await.expect("set");
        tx.commit().await.expect("commit");
    });

    let reader = store.reader().expect("reader");
    let got = block_on(reader.get(b"note/1"));
    assert_eq!(got.unwrap().as_deref(), Some(b"a".as_slice()));
    let got = block_on(reader.get(b"note/2"));
    assert_eq!(got.unwrap().as_deref(), Some(b"b".as_slice()));
}

/// An uncommitted transaction rolls back on drop: its writes are not visible to
/// a fresh reader, and the committed data it opened over is untouched.
#[test]
fn an_uncommitted_transaction_rolls_back_on_drop() {
    let store = open_store();

    let mut tx = store.transaction().expect("seed transaction");
    block_on(async move {
        tx.set(b"seed", b"committed").await.expect("set");
        tx.commit().await.expect("commit");
    });

    let mut tx = store.transaction().expect("uncommitted transaction");
    block_on(async move {
        tx.set(b"ephemeral", b"gone").await.expect("set");
        // tx is dropped here, uncommitted.
    });

    let reader = store.reader().expect("reader");
    assert_eq!(
        block_on(reader.get(b"ephemeral")).unwrap(),
        None,
        "an uncommitted write must not persist"
    );
    assert_eq!(
        block_on(reader.get(b"seed")).unwrap().as_deref(),
        Some(b"committed".as_slice()),
        "the committed seed must survive the rolled-back transaction"
    );
}

/// The reader is a consistent read-only view: reads and scans work, and every
/// mutation is rejected with `ReadOnly`.
#[test]
fn a_reader_serves_reads_and_rejects_mutations() {
    let store = open_store();
    let mut tx = store.transaction().expect("seed transaction");
    block_on(async move {
        tx.set(b"0", b"v0").await.expect("set");
        tx.set(b"1", b"v1").await.expect("set");
        tx.commit().await.expect("commit");
    });

    let mut reader: RedbReader = store.reader().expect("reader");

    // Reads and scans work.
    assert_eq!(
        block_on(reader.get(b"1")).unwrap().as_deref(),
        Some(b"v1".as_slice())
    );
    let scan = block_on(async {
        let mut stream = reader.range(Bound::Unbounded, Bound::Unbounded, false);
        let mut out = Vec::new();
        while let Some((pair, rest)) = stream.next().await.expect("scan step") {
            out.push(pair.key);
            stream = rest;
        }
        out
    });
    assert_eq!(scan, vec![b"0".to_vec(), b"1".to_vec()]);

    // Every mutation is rejected.
    assert!(matches!(
        block_on(reader.set(b"k", b"v")),
        Err(RedbStorageError::ReadOnly)
    ));
    assert!(matches!(
        block_on(reader.delete(b"k")),
        Err(RedbStorageError::ReadOnly)
    ));
    assert!(matches!(
        block_on(reader.clear_range(Bound::Unbounded, Bound::Unbounded)),
        Err(RedbStorageError::ReadOnly)
    ));
}

/// A scoped commit commits the underlying redb transaction.
#[test]
fn a_scoped_commit_forwards_to_the_transaction() {
    let store = open_store();
    let mut scoped = ScopedKvStore::new(
        store.transaction().expect("write transaction"),
        b"scope/".to_vec(),
    );
    block_on(async move {
        scoped.set(b"a", b"1").await.expect("set");
        scoped.commit().await.expect("commit");
    });

    // The committed key carries the prefix in the backing store.
    let reader = store.reader().expect("reader");
    assert_eq!(
        block_on(reader.get(b"scope/a")).unwrap().as_deref(),
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
        tx.set(b"shared", b"1").await.expect("set");
        tx.commit().await.expect("commit");
    });

    // The clone sees the same committed data.
    let reader = clone.reader().expect("reader on the clone");
    assert_eq!(
        block_on(reader.get(b"shared")).unwrap().as_deref(),
        Some(b"1".as_slice())
    );
}
