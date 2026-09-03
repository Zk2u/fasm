//! Browser tests for fenced, detached IndexedDB commits.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::{Context, Poll, Waker},
};

use fasm_storage::{Commit, KvStore, RetryableStorageError, ScopedKvStore};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;
use web_sys::{IdbDatabase, IdbFactory, IdbRequest, IdbTransactionMode};

use crate::{
    IndexedDbError, IndexedDbStore, Revision,
    idb::{
        KV_STORE, META_STORE, REVISION_KEY, RequestFuture, TransactionOutcome, bytes_from_js,
        bytes_to_js, dom_error, fixture::await_complete, fixture::unique_name, fixture::wait_until,
        global_factory, revision_to_js,
    },
    session::FaultInjection,
    store::{Scope, readonly_result},
};

fn from_js<T>(result: Result<T, JsValue>) -> Result<T, IndexedDbError> {
    result.map_err(|value| dom_error(&value))
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    future.poll(&mut context)
}

async fn join2<F, G>(mut first: Pin<Box<F>>, mut second: Pin<Box<G>>) -> (F::Output, G::Output)
where
    F: Future,
    G: Future,
{
    let mut first_output = None;
    let mut second_output = None;
    poll_fn(|context| {
        if first_output.is_none()
            && let Poll::Ready(output) = first.as_mut().poll(context)
        {
            first_output = Some(output);
        }
        if second_output.is_none()
            && let Poll::Ready(output) = second.as_mut().poll(context)
        {
            second_output = Some(output);
        }

        match (first_output.take(), second_output.take()) {
            (Some(first), Some(second)) => Poll::Ready((first, second)),
            (first, second) => {
                first_output = first;
                second_output = second;
                Poll::Pending
            }
        }
    })
    .await
}

async fn raw_open(
    factory: &IdbFactory,
    name: &str,
    version: u32,
) -> Result<IdbDatabase, IndexedDbError> {
    let request = from_js(factory.open_with_u32(name, version))?;
    let request: IdbRequest = request.unchecked_into();
    RequestFuture::new(request)
        .await?
        .dyn_into::<IdbDatabase>()
        .map_err(|_| IndexedDbError::Corrupt {
            detail: "raw open returned a non-database value".to_owned(),
        })
}

async fn put_raw_meta(store: &IndexedDbStore, value: &JsValue) -> Result<(), IndexedDbError> {
    let transaction = store.begin(IdbTransactionMode::Readwrite, Scope::Meta)?;
    let outcome = TransactionOutcome::new(transaction.clone());
    let metadata = from_js(transaction.object_store(META_STORE))?;
    from_js(metadata.put_with_key(value, &JsValue::from_str(REVISION_KEY)))?;
    await_complete(outcome).await
}

async fn delete_raw_meta(store: &IndexedDbStore) -> Result<(), IndexedDbError> {
    let transaction = store.begin(IdbTransactionMode::Readwrite, Scope::Meta)?;
    let outcome = TransactionOutcome::new(transaction.clone());
    let metadata = from_js(transaction.object_store(META_STORE))?;
    from_js(metadata.delete(&JsValue::from_str(REVISION_KEY)))?;
    await_complete(outcome).await
}

async fn revision(store: &IndexedDbStore) -> Result<Revision, IndexedDbError> {
    Ok(store.transaction().await?.expected_revision())
}

fn root_raw_key(key: &[u8]) -> Vec<u8> {
    let mut raw = fasm_storage::flatdir::ROOT_PREFIX.to_vec();
    raw.extend_from_slice(key);
    raw
}

async fn raw_value(store: &IndexedDbStore, key: &[u8]) -> Result<Option<Vec<u8>>, IndexedDbError> {
    let transaction = store.begin(IdbTransactionMode::Readonly, Scope::Kv)?;
    let kv = from_js(transaction.object_store(KV_STORE))?;
    let request = from_js(kv.get(&bytes_to_js(key)))?;
    let outcome = TransactionOutcome::new(transaction);
    let value = RequestFuture::new(request).await;
    readonly_result(outcome.await)?;
    let value = value?;
    if value.is_null() || value.is_undefined() {
        Ok(None)
    } else {
        bytes_from_js(&value, "value").map(Some)
    }
}

#[wasm_bindgen_test]
async fn commit_persists_rows_revision_and_reopen() -> Result<(), IndexedDbError> {
    let name = unique_name("commit-persists");
    let store = IndexedDbStore::open(&name).await?;
    let mut session = store.transaction().await?;
    session.set(&[], b"a", b"va").await?;
    session.set(&[], b"b", b"vb").await?;
    session.commit().await?;

    let reader = store.reader();
    assert_eq!(reader.get(&[], b"a").await?, Some(b"va".to_vec()));
    assert_eq!(reader.get(&[], b"b").await?, Some(b"vb".to_vec()));
    assert_eq!(revision(&store).await?.get(), 1);
    drop(reader);
    drop(store);

    let reopened = IndexedDbStore::open(&name).await?;
    let reader = reopened.reader();
    assert_eq!(reader.get(&[], b"a").await?, Some(b"va".to_vec()));
    assert_eq!(reader.get(&[], b"b").await?, Some(b"vb".to_vec()));
    drop(reader);
    drop(reopened);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn dropping_session_before_commit_rolls_back() -> Result<(), IndexedDbError> {
    let name = unique_name("drop-session");
    let store = IndexedDbStore::open(&name).await?;
    let mut session = store.transaction().await?;
    session.set(&[], b"k", b"v").await?;
    drop(session);

    assert_eq!(store.reader().get(&[], b"k").await?, None);
    assert_eq!(revision(&store).await?, Revision::ZERO);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn dropping_unpolled_commit_future_rolls_back() -> Result<(), IndexedDbError> {
    let name = unique_name("drop-unpolled-commit");
    let store = IndexedDbStore::open(&name).await?;
    let mut session = store.transaction().await?;
    session.set(&[], b"k", b"v").await?;

    let commit = session.commit();
    drop(commit);

    assert_eq!(store.reader().get(&[], b"k").await?, None);
    assert_eq!(revision(&store).await?, Revision::ZERO);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn dropping_polled_commit_future_still_commits() -> Result<(), IndexedDbError> {
    let name = unique_name("drop-commit");
    let store = IndexedDbStore::open(&name).await?;
    let mut session = store.transaction().await?;
    session.set(&[], b"k", b"v").await?;

    let mut commit = Box::pin(session.commit());
    assert!(poll_once(commit.as_mut()).is_pending());
    drop(commit);

    let observed_store = store.clone();
    wait_until(2_000, move || {
        let store = observed_store.clone();
        async move { Ok(store.reader().get(&[], b"k").await? == Some(b"v".to_vec())) }
    })
    .await?;

    assert_eq!(store.reader().get(&[], b"k").await?, Some(b"v".to_vec()));
    assert_eq!(revision(&store).await?.get(), 1);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn scoped_commit_forwards_to_indexeddb_session() -> Result<(), IndexedDbError> {
    let name = unique_name("scoped-commit");
    let store = IndexedDbStore::open(&name).await?;
    let session = store.transaction().await?;
    let mut scoped = ScopedKvStore::new(session, vec![b"scope".to_vec()]);
    if let Err(error) = scoped.set(&[], b"a", b"v").await {
        panic!("scoped set failed: {error}");
    }
    scoped.commit().await?;

    assert_eq!(
        store.reader().get(&[b"scope"], b"a").await?,
        Some(b"v".to_vec())
    );
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn stale_session_conflicts_then_fresh_session_commits() -> Result<(), IndexedDbError> {
    let name = unique_name("session-conflict");
    let store = IndexedDbStore::open(&name).await?;
    let mut first = store.transaction().await?;
    let mut stale = store.transaction().await?;
    first.set(&[], b"x", b"vx").await?;
    stale.set(&[], b"y", b"vy").await?;
    first.commit().await?;

    let error = stale
        .commit()
        .await
        .err()
        .ok_or_else(|| IndexedDbError::Corrupt {
            detail: "stale session unexpectedly committed".to_owned(),
        })?;
    assert!(matches!(error, IndexedDbError::Conflict));
    assert!(error.is_retryable());
    assert_eq!(store.reader().get(&[], b"x").await?, Some(b"vx".to_vec()));
    assert_eq!(store.reader().get(&[], b"y").await?, None);
    assert_eq!(revision(&store).await?.get(), 1);

    let mut fresh = store.transaction().await?;
    fresh.set(&[], b"y", b"vy").await?;
    fresh.commit().await?;
    assert_eq!(revision(&store).await?.get(), 2);
    drop(store);
    IndexedDbStore::delete(&name).await
}

async fn assert_connection_conflict(
    name: &str,
    first_connection_wins: bool,
) -> Result<(), IndexedDbError> {
    let first_store = IndexedDbStore::open(name).await?;
    let second_store = IndexedDbStore::open(name).await?;
    let mut first = first_store.transaction().await?;
    let mut second = second_store.transaction().await?;
    first.set(&[], b"first", b"v").await?;
    second.set(&[], b"second", b"v").await?;

    let error = if first_connection_wins {
        first.commit().await?;
        second.commit().await.err()
    } else {
        second.commit().await?;
        first.commit().await.err()
    }
    .ok_or_else(|| IndexedDbError::Corrupt {
        detail: "cross-connection stale session unexpectedly committed".to_owned(),
    })?;
    assert!(matches!(error, IndexedDbError::Conflict));
    assert_eq!(revision(&first_store).await?.get(), 1);
    drop(first_store);
    drop(second_store);
    Ok(())
}

#[wasm_bindgen_test]
async fn two_connections_conflict_in_both_commit_orders() -> Result<(), IndexedDbError> {
    let first_name = unique_name("connections-first");
    assert_connection_conflict(&first_name, true).await?;
    IndexedDbStore::delete(&first_name).await?;

    let second_name = unique_name("connections-second");
    assert_connection_conflict(&second_name, false).await?;
    IndexedDbStore::delete(&second_name).await
}

#[wasm_bindgen_test]
async fn two_connections_racing_commits_have_one_winner() -> Result<(), IndexedDbError> {
    let name = unique_name("connections-race");
    let first_store = IndexedDbStore::open(&name).await?;
    let second_store = IndexedDbStore::open(&name).await?;
    let mut first = first_store.transaction().await?;
    let mut second = second_store.transaction().await?;
    first.set(&[], b"first", b"v").await?;
    second.set(&[], b"second", b"v").await?;

    let mut first_commit = Box::pin(first.commit());
    let mut second_commit = Box::pin(second.commit());
    assert!(poll_once(first_commit.as_mut()).is_pending());
    assert!(poll_once(second_commit.as_mut()).is_pending());
    let (first_result, second_result) = join2(first_commit, second_commit).await;

    match (&first_result, &second_result) {
        (Ok(()), Err(error)) | (Err(error), Ok(())) => {
            assert!(matches!(error, IndexedDbError::Conflict));
            assert!(error.is_retryable());
        }
        _ => panic!(
            "racing commits did not produce exactly one success and one conflict: \
             first={first_result:?}, second={second_result:?}"
        ),
    }
    assert_eq!(revision(&first_store).await?.get(), 1);
    drop(first_store);
    drop(second_store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn asynchronous_request_failure_rolls_back_every_write() -> Result<(), IndexedDbError> {
    let name = unique_name("request-failure");
    let store = IndexedDbStore::open(&name).await?;
    let mut seed = store.transaction().await?;
    seed.set(&[], b"k", b"old").await?;
    seed.commit().await?;

    let mut session = store.transaction().await?;
    session.set(&[], b"k", b"new").await?;
    session.set(&[], b"m", b"value").await?;
    session.inject_faults(FaultInjection {
        fail_request_of: Some(root_raw_key(b"k")),
        ..FaultInjection::default()
    });
    let error = session
        .commit()
        .await
        .err()
        .ok_or_else(|| IndexedDbError::Corrupt {
            detail: "injected request failure unexpectedly committed".to_owned(),
        })?;
    match &error {
        IndexedDbError::CommitAborted { reason } => assert!(reason.contains("ConstraintError")),
        other => panic!("unexpected request failure: {other}"),
    }
    assert!(error.is_retryable());
    assert_eq!(store.reader().get(&[], b"k").await?, Some(b"old".to_vec()));
    assert_eq!(store.reader().get(&[], b"m").await?, None);
    assert_eq!(revision(&store).await?.get(), 1);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn synchronous_enqueue_throw_aborts_without_writes() -> Result<(), IndexedDbError> {
    let name = unique_name("enqueue-throw");
    let store = IndexedDbStore::open(&name).await?;
    let mut session = store.transaction().await?;
    session.set(&[], b"k", b"v").await?;
    session.set(&[], b"m", b"v").await?;
    session.inject_faults(FaultInjection {
        fail_enqueue_of: Some(root_raw_key(b"k")),
        ..FaultInjection::default()
    });

    match session.commit().await {
        Err(IndexedDbError::Backend { name, .. }) => assert_eq!(name, "DataError"),
        Err(error) => panic!("unexpected enqueue error: {error}"),
        Ok(()) => panic!("injected enqueue throw unexpectedly committed"),
    }
    assert_eq!(store.reader().get(&[], b"k").await?, None);
    assert_eq!(store.reader().get(&[], b"m").await?, None);
    assert_eq!(revision(&store).await?, Revision::ZERO);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn failed_abort_after_conflict_still_returns_conflict() -> Result<(), IndexedDbError> {
    let name = unique_name("abort-failure-conflict");
    let store = IndexedDbStore::open(&name).await?;
    let mut stale = store.transaction().await?;
    stale.set(&[], b"stale", b"v").await?;
    stale.inject_faults(FaultInjection {
        fail_abort: true,
        ..FaultInjection::default()
    });

    let mut winner = store.transaction().await?;
    winner.set(&[], b"winner", b"v").await?;
    winner.commit().await?;

    let error = stale
        .commit()
        .await
        .err()
        .ok_or_else(|| IndexedDbError::Corrupt {
            detail: "conflicting commit with failed abort unexpectedly succeeded".to_owned(),
        })?;
    assert!(matches!(error, IndexedDbError::Conflict));
    assert!(error.is_retryable());
    assert_eq!(store.reader().get(&[], b"stale").await?, None);
    assert_eq!(revision(&store).await?.get(), 1);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn failed_abort_after_enqueue_throw_has_unknown_outcome() -> Result<(), IndexedDbError> {
    let name = unique_name("abort-failure-enqueue");
    let store = IndexedDbStore::open(&name).await?;
    let mut session = store.transaction().await?;
    session.set(&[], b"k", b"v").await?;
    session.inject_faults(FaultInjection {
        fail_enqueue_of: Some(root_raw_key(b"k")),
        fail_abort: true,
        ..FaultInjection::default()
    });

    let error = session
        .commit()
        .await
        .err()
        .ok_or_else(|| IndexedDbError::Corrupt {
            detail: "enqueue throw with failed abort unexpectedly succeeded".to_owned(),
        })?;
    match &error {
        IndexedDbError::Backend { name, .. } => assert_eq!(name, "UnexpectedComplete"),
        other => panic!("unexpected enqueue/abort failure: {other}"),
    }
    assert!(!error.is_retryable());
    assert_eq!(store.reader().get(&[], b"k").await?, None);
    assert_eq!(revision(&store).await?, Revision::ZERO);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn partial_enqueue_with_failed_abort_has_unknown_outcome() -> Result<(), IndexedDbError> {
    let name = unique_name("abort-failure-partial-enqueue");
    let store = IndexedDbStore::open(&name).await?;
    let mut session = store.transaction().await?;
    session.set(&[], b"a", b"written").await?;
    session.set(&[], b"k", b"not-written").await?;
    session.inject_faults(FaultInjection {
        fail_enqueue_of: Some(root_raw_key(b"k")),
        fail_abort: true,
        ..FaultInjection::default()
    });

    let error = session
        .commit()
        .await
        .err()
        .ok_or_else(|| IndexedDbError::Corrupt {
            detail: "partial enqueue with failed abort unexpectedly succeeded".to_owned(),
        })?;
    match &error {
        IndexedDbError::Backend { name, .. } => assert_eq!(name, "UnexpectedComplete"),
        other => panic!("unexpected partial-enqueue/abort failure: {other}"),
    }
    assert!(!error.is_retryable());
    assert_eq!(
        raw_value(&store, &root_raw_key(b"a")).await?,
        Some(b"written".to_vec())
    );
    assert_eq!(raw_value(&store, &root_raw_key(b"k")).await?, None);
    assert!(matches!(
        store.reader().get(&[], b"a").await,
        Err(IndexedDbError::Foreign)
    ));
    assert_eq!(revision(&store).await?, Revision::ZERO);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn conversion_failure_happens_before_transaction() -> Result<(), IndexedDbError> {
    let name = unique_name("conversion-failure");
    let store = IndexedDbStore::open(&name).await?;
    let mut session = store.transaction().await?;
    session.set(&[], b"k", b"v").await?;
    session.inject_faults(FaultInjection {
        fail_conversion_of: Some(root_raw_key(b"k")),
        ..FaultInjection::default()
    });

    match session.commit().await {
        Err(IndexedDbError::Corrupt { detail }) => assert!(detail.contains("injected")),
        Err(error) => panic!("unexpected conversion error: {error}"),
        Ok(()) => panic!("injected conversion failure unexpectedly committed"),
    }
    assert_eq!(store.reader().get(&[], b"k").await?, None);
    assert_eq!(revision(&store).await?, Revision::ZERO);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn corrupt_revision_aborts_instead_of_false_success() -> Result<(), IndexedDbError> {
    let name = unique_name("corrupt-revision");
    let store = IndexedDbStore::open(&name).await?;
    let mut session = store.transaction().await?;
    session.set(&[], b"k", b"v").await?;
    put_raw_meta(&store, &JsValue::from_str("x")).await?;

    assert!(matches!(
        session.commit().await,
        Err(IndexedDbError::Corrupt { .. })
    ));
    assert_eq!(store.reader().get(&[], b"k").await?, None);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn missing_revision_record_aborts_commit() -> Result<(), IndexedDbError> {
    let name = unique_name("missing-revision");
    let store = IndexedDbStore::open(&name).await?;
    let mut session = store.transaction().await?;
    session.set(&[], b"k", b"v").await?;
    delete_raw_meta(&store).await?;

    assert!(matches!(
        session.commit().await,
        Err(IndexedDbError::Corrupt { .. })
    ));
    assert_eq!(store.reader().get(&[], b"k").await?, None);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn empty_session_commit_does_not_advance_revision() -> Result<(), IndexedDbError> {
    let name = unique_name("empty-commit");
    let store = IndexedDbStore::open(&name).await?;
    store.transaction().await?.commit().await?;
    assert_eq!(revision(&store).await?, Revision::ZERO);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn stale_empty_session_conflicts() -> Result<(), IndexedDbError> {
    let name = unique_name("stale-empty-commit");
    let store = IndexedDbStore::open(&name).await?;
    let mut winner = store.transaction().await?;
    let stale = store.transaction().await?;
    winner.set(&[], b"winner", b"value").await?;
    winner.commit().await?;

    let error = stale
        .commit()
        .await
        .err()
        .ok_or_else(|| IndexedDbError::Corrupt {
            detail: "stale empty session unexpectedly committed".to_owned(),
        })?;
    assert!(matches!(error, IndexedDbError::Conflict));
    assert!(error.is_retryable());
    assert_eq!(revision(&store).await?.get(), 1);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn empty_session_at_max_revision_does_not_advance() -> Result<(), IndexedDbError> {
    let name = unique_name("empty-commit-max-revision");
    let store = IndexedDbStore::open(&name).await?;
    put_raw_meta(&store, &revision_to_js(Revision::MAX)).await?;

    store.transaction().await?.commit().await?;

    assert_eq!(revision(&store).await?, Revision::MAX);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn closed_store_fails_commit_before_transaction() -> Result<(), IndexedDbError> {
    let name = unique_name("closed-commit");
    let store = IndexedDbStore::open(&name).await?;
    let mut session = store.transaction().await?;
    session.set(&[], b"k", b"v").await?;
    let factory = global_factory()?;
    let upgraded = raw_open(&factory, &name, 2).await?;

    assert!(matches!(
        session.commit().await,
        Err(IndexedDbError::Closed)
    ));
    upgraded.close();
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn exhausted_revision_fails_before_opening_commit_transaction() -> Result<(), IndexedDbError>
{
    let name = unique_name("revision-exhausted");
    let store = IndexedDbStore::open(&name).await?;
    put_raw_meta(&store, &revision_to_js(Revision::MAX)).await?;
    let mut session = store.transaction().await?;
    session.set(&[], b"k", b"v").await?;

    assert!(matches!(
        session.commit().await,
        Err(IndexedDbError::Corrupt { .. })
    ));
    assert_eq!(store.reader().get(&[], b"k").await?, None);
    assert_eq!(revision(&store).await?, Revision::MAX);
    drop(store);
    IndexedDbStore::delete(&name).await
}
