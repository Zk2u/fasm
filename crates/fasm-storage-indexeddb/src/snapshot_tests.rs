//! Isolation, capture ordering, and cancellation regressions in a real browser.

use std::{
    future::Future,
    ops::Bound,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use fasm_storage::{Commit, KvDirNav, KvStore, flatdir::ops};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::wasm_bindgen_test;
use web_sys::IdbTransactionMode;

use crate::{
    IndexedDbError, IndexedDbStore,
    commit_tests::join2,
    idb::{
        KV_STORE, TransactionOutcome, bytes_to_js, dom_error,
        fixture::{await_complete, seed_root_rows, unique_name},
    },
    store::Scope,
};

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[wasm_bindgen_test]
async fn sessions_and_readers_keep_data_and_directories_after_another_connection_commits()
-> Result<(), IndexedDbError> {
    let name = unique_name("snapshot-isolation");
    let store = IndexedDbStore::open(&name).await?;
    let other = IndexedDbStore::open(&name).await?;
    let mut seed = store.transaction().await?;
    seed.set(&[b"old"], b"a", b"before").await?;
    seed.set(&[b"old"], b"b", b"before").await?;
    seed.commit().await?;

    let mut session = store.transaction().await?;
    let reader = store.reader().await?;
    let (first, rest) = reader
        .range(&[b"old"], Bound::Unbounded, Bound::Unbounded, false)
        .next()
        .await?
        .expect("seeded first row");
    assert_eq!(first.key, b"a");

    let mut writer = other.transaction().await?;
    writer.remove_dir(&[b"old"]).await?;
    writer.set(&[b"new"], b"c", b"after").await?;
    writer.commit().await?;

    assert_eq!(
        session.get(&[b"old"], b"a").await?,
        Some(b"before".to_vec())
    );
    assert!(session.dir_exists(&[b"old"]).await?);
    assert!(!session.dir_exists(&[b"new"]).await?);
    assert_eq!(reader.list_dirs(&[]).await?, vec![b"old".to_vec()]);
    let remaining = rest.collect().await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].value, b"before");
    // Starting a NEW scan through the same reader also uses the old snapshot.
    assert_eq!(
        reader
            .range(&[b"old"], Bound::Unbounded, Bound::Unbounded, true)
            .collect()
            .await?
            .len(),
        2
    );
    let fresh = store.reader().await?;
    assert_eq!(fresh.list_dirs(&[]).await?, vec![b"new".to_vec()]);
    ops::validate(fresh.engine.raw()).expect("persisted removal keeps a valid layout");

    session.set(&[b"old"], b"a", b"local").await?;
    assert_eq!(session.get(&[b"old"], b"a").await?, Some(b"local".to_vec()));
    assert!(matches!(
        session.commit().await,
        Err(IndexedDbError::Conflict)
    ));
    drop(reader);
    drop(fresh);
    drop(store);
    drop(other);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn capture_pairs_revision_and_all_rows_on_either_side_of_a_queued_commit()
-> Result<(), IndexedDbError> {
    for snapshot_first in [true, false] {
        let name = unique_name("snapshot-capture-order");
        let store = IndexedDbStore::open(&name).await?;
        let other = IndexedDbStore::open(&name).await?;
        let rows: Vec<_> = (0_u32..1100)
            .map(|i| (i.to_be_bytes().to_vec(), b"before".to_vec()))
            .collect();
        let refs: Vec<_> = rows
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        seed_root_rows(&store, &refs).await?;
        let mut writer = other.transaction().await?;
        for (key, _) in &rows {
            writer.set(&[], key, b"after").await?;
        }
        let mut capture = Box::pin(store.transaction());
        let mut commit = Box::pin(writer.commit());
        if snapshot_first {
            assert!(poll_once(capture.as_mut()).is_pending());
            assert!(poll_once(commit.as_mut()).is_pending());
        } else {
            assert!(poll_once(commit.as_mut()).is_pending());
            assert!(poll_once(capture.as_mut()).is_pending());
        }
        let (captured, committed) = join2(capture, commit).await;
        committed?;
        let captured = captured?;
        let expected_revision = if snapshot_first { 0 } else { 1 };
        let expected_value = if snapshot_first {
            b"before".as_slice()
        } else {
            b"after".as_slice()
        };
        assert_eq!(captured.expected_revision().get(), expected_revision);
        let actual = captured
            .range(&[], Bound::Unbounded, Bound::Unbounded, false)
            .collect()
            .await?;
        assert_eq!(actual.len(), 1100);
        assert!(actual.iter().all(|p| p.value == expected_value));
        let result = captured.commit().await;
        if snapshot_first {
            assert!(matches!(result, Err(IndexedDbError::Conflict)));
        } else {
            result?;
        }
        drop(store);
        drop(other);
        IndexedDbStore::delete(&name).await?;
    }
    Ok(())
}

#[wasm_bindgen_test]
async fn dropped_snapshot_capture_does_not_write_or_block_later_commits()
-> Result<(), IndexedDbError> {
    let name = unique_name("drop-snapshot");
    let store = IndexedDbStore::open(&name).await?;
    seed_root_rows(&store, &[(b"a", b"old")]).await?;
    let mut capture = Box::pin(store.transaction());
    assert!(poll_once(capture.as_mut()).is_pending());
    drop(capture);
    let mut next = store.transaction().await?;
    assert_eq!(next.expected_revision().get(), 0);
    assert_eq!(next.get(&[], b"a").await?, Some(b"old".to_vec()));
    next.set(&[], b"a", b"new").await?;
    next.commit().await?;
    assert_eq!(
        store.reader().await?.get(&[], b"a").await?,
        Some(b"new".to_vec())
    );
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn malformed_binary_rows_reject_the_entire_snapshot() -> Result<(), IndexedDbError> {
    for bad_key in [true, false] {
        let name = unique_name("snapshot-corrupt-row");
        let store = IndexedDbStore::open(&name).await?;
        let transaction = store.begin(IdbTransactionMode::Readwrite, Scope::Kv)?;
        let outcome = TransactionOutcome::new(transaction.clone());
        let kv = transaction
            .object_store(KV_STORE)
            .map_err(|v| dom_error(&v))?;
        let key = if bad_key {
            JsValue::from_str("not binary")
        } else {
            bytes_to_js(b"key")
        };
        let value = if bad_key {
            bytes_to_js(b"value")
        } else {
            JsValue::from_str("not binary")
        };
        kv.put_with_key(&value, &key).map_err(|v| dom_error(&v))?;
        await_complete(outcome).await?;
        assert!(matches!(
            store.transaction().await,
            Err(IndexedDbError::Corrupt { .. })
        ));
        assert!(matches!(
            store.reader().await,
            Err(IndexedDbError::Corrupt { .. })
        ));
        drop(store);
        IndexedDbStore::delete(&name).await?;
    }
    Ok(())
}
