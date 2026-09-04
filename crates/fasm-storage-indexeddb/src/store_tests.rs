//! Browser tests for database creation, deletion, and connection ownership.
//!
//! A blocked open is unreachable through schema-version-one `IndexedDbStore::open`;
//! IndexedDB only blocks an open when it needs a version change.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use fasm_storage::KvStore;
use js_sys::Reflect;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;
use web_sys::{IdbDatabase, IdbFactory, IdbRequest, IdbTransactionMode};

use crate::{
    IndexedDbError, IndexedDbStore, Revision,
    idb::{
        KV_STORE, META_STORE, REVISION_KEY, RequestFuture, TransactionOutcome, bytes_to_js,
        dom_error, fixture::await_complete, fixture::root_rows, fixture::seed_root_rows,
        fixture::sleep_ms, fixture::unique_name, fixture::wait_until, global_factory,
        revision_from_js,
    },
    store::{Scope, transaction_durability},
};

fn from_js<T>(result: Result<T, JsValue>) -> Result<T, IndexedDbError> {
    result.map_err(|value| dom_error(&value))
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    future.poll(&mut context)
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

async fn put_raw_row(store: &IndexedDbStore) -> Result<(), IndexedDbError> {
    seed_root_rows(store, &[(b"key", b"value")]).await
}

async fn kv_count(store: &IndexedDbStore) -> Result<u32, IndexedDbError> {
    let transaction = store.begin(IdbTransactionMode::Readonly, Scope::Kv)?;
    let outcome = TransactionOutcome::new(transaction.clone());
    let object_store = from_js(transaction.object_store(KV_STORE))?;
    let count = RequestFuture::new(from_js(object_store.count())?).await?;
    await_complete(outcome).await?;
    count
        .as_f64()
        .map(|count| count as u32)
        .ok_or_else(|| IndexedDbError::Corrupt {
            detail: "object store count was not a number".to_owned(),
        })
}

fn object_store_names(database: &IdbDatabase) -> Result<Vec<String>, IndexedDbError> {
    let names = Reflect::get(database.as_ref(), &JsValue::from_str("objectStoreNames"))
        .map_err(|value| dom_error(&value))?;
    let length = Reflect::get(&names, &JsValue::from_str("length"))
        .map_err(|value| dom_error(&value))?
        .as_f64()
        .ok_or_else(|| IndexedDbError::Corrupt {
            detail: "objectStoreNames length was not a number".to_owned(),
        })? as u32;
    (0..length)
        .map(|index| {
            Reflect::get(&names, &JsValue::from_f64(f64::from(index)))
                .map_err(|value| dom_error(&value))?
                .as_string()
                .ok_or_else(|| IndexedDbError::Corrupt {
                    detail: "objectStoreNames entry was not a string".to_owned(),
                })
        })
        .collect()
}

fn browser_user_agent() -> Option<String> {
    let navigator = Reflect::get(&js_sys::global(), &JsValue::from_str("navigator")).ok()?;
    Reflect::get(&navigator, &JsValue::from_str("userAgent"))
        .ok()?
        .as_string()
}

#[wasm_bindgen_test]
async fn open_twice_creates_schema_and_initial_revision() -> Result<(), IndexedDbError> {
    let name = unique_name("open-schema");
    let first = IndexedDbStore::open(&name).await?;
    let second = IndexedDbStore::open(&name).await?;

    assert_eq!(first.name(), name);
    assert!(!first.is_closed());
    assert!(!second.is_closed());
    first.database()?;
    second.database()?;
    assert_eq!(
        object_store_names(first.database()?)?,
        vec![KV_STORE.to_owned(), META_STORE.to_owned()]
    );

    let transaction = first.begin(IdbTransactionMode::Readonly, Scope::KvAndMeta)?;
    let outcome = TransactionOutcome::new(transaction.clone());
    let metadata = from_js(transaction.object_store(META_STORE))?;
    let revision =
        RequestFuture::new(from_js(metadata.get(&JsValue::from_str(REVISION_KEY)))?).await?;
    await_complete(outcome).await?;
    assert_eq!(revision_from_js(&revision)?, Revision::ZERO);

    drop(first);
    drop(second);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn delete_removes_rows_and_recreates_revision() -> Result<(), IndexedDbError> {
    let name = unique_name("delete-removes");
    let store = IndexedDbStore::open(&name).await?;
    put_raw_row(&store).await?;
    assert!(kv_count(&store).await? > 1);
    drop(store);

    IndexedDbStore::delete(&name).await?;
    let reopened = IndexedDbStore::open(&name).await?;
    assert_eq!(kv_count(&reopened).await?, 0);

    let transaction = reopened.begin(IdbTransactionMode::Readonly, Scope::KvAndMeta)?;
    let outcome = TransactionOutcome::new(transaction.clone());
    let metadata = from_js(transaction.object_store(META_STORE))?;
    let revision =
        RequestFuture::new(from_js(metadata.get(&JsValue::from_str(REVISION_KEY)))?).await?;
    await_complete(outcome).await?;
    assert_eq!(revision_from_js(&revision)?, Revision::ZERO);

    drop(reopened);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn durable_transaction_requests_strict_and_completes_write() -> Result<(), IndexedDbError> {
    let name = unique_name("strict-durability");
    let store = IndexedDbStore::open(&name).await?;
    let transaction = store.begin_durable(Scope::KvAndMeta)?;
    let durability = transaction_durability(&transaction);
    if browser_user_agent().is_some_and(|agent| agent.contains("Chrome/")) {
        assert_eq!(durability.as_deref(), Some("strict"));
    } else {
        assert!(matches!(
            durability.as_deref(),
            None | Some("default") | Some("strict")
        ));
    }

    let outcome = TransactionOutcome::new(transaction.clone());
    let object_store = from_js(transaction.object_store(KV_STORE))?;
    for (key, value) in root_rows(&[(b"key", b"value")])? {
        from_js(object_store.put_with_key(&bytes_to_js(&value), &bytes_to_js(&key)))?;
    }
    await_complete(outcome).await?;
    assert_eq!(
        store.reader().await?.get(&[], b"key").await?,
        Some(b"value".to_vec())
    );

    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn versionchange_closes_handle_and_unblocks_upgrade() -> Result<(), IndexedDbError> {
    let name = unique_name("versionchange");
    let store = IndexedDbStore::open(&name).await?;
    let factory = global_factory()?;
    let upgraded = raw_open(&factory, &name, 2).await?;

    assert!(store.is_closed());
    assert!(matches!(store.database(), Err(IndexedDbError::Closed)));
    assert!(matches!(
        store.begin(IdbTransactionMode::Readonly, Scope::Kv),
        Err(IndexedDbError::Closed)
    ));

    upgraded.close();
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn dropped_open_future_closes_detached_connection() -> Result<(), IndexedDbError> {
    let name = unique_name("drop-open");
    let mut open = Box::pin(IndexedDbStore::open(&name));
    assert!(poll_once(open.as_mut()).is_pending());
    drop(open);

    let factory = global_factory()?;
    let request = from_js(factory.open_with_u32(&name, 2))?;
    let request: IdbRequest = request.unchecked_into();
    let mut upgrade = Box::pin(RequestFuture::new(request));
    let mut upgrade_result = None;
    wait_until(2_000, || {
        let ready = if upgrade_result.is_some() {
            true
        } else if let Poll::Ready(result) = poll_once(upgrade.as_mut()) {
            upgrade_result = Some(result);
            true
        } else {
            false
        };
        async move { Ok(ready) }
    })
    .await?;
    let Some(result) = upgrade_result else {
        panic!("version-two open reported ready without a result");
    };
    let database = result?
        .dyn_into::<IdbDatabase>()
        .map_err(|_| IndexedDbError::Corrupt {
            detail: "version-two open returned a non-database value".to_owned(),
        })?;
    database.close();
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn dropped_delete_future_still_deletes_database() -> Result<(), IndexedDbError> {
    let name = unique_name("drop-delete");
    let store = IndexedDbStore::open(&name).await?;
    put_raw_row(&store).await?;
    drop(store);

    let mut delete = Box::pin(IndexedDbStore::delete(&name));
    assert!(poll_once(delete.as_mut()).is_pending());
    drop(delete);

    let mut reopen = Box::pin(IndexedDbStore::open(&name));
    let mut reopen_result = None;
    wait_until(2_000, || {
        let ready = if reopen_result.is_some() {
            true
        } else if let Poll::Ready(result) = poll_once(reopen.as_mut()) {
            reopen_result = Some(result);
            true
        } else {
            false
        };
        async move { Ok(ready) }
    })
    .await?;
    let Some(reopened) = reopen_result else {
        panic!("reopen reported ready without a result");
    };
    let reopened = reopened?;
    assert_eq!(kv_count(&reopened).await?, 0);
    drop(reopened);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn blocked_delete_waits_for_connection_to_close() -> Result<(), IndexedDbError> {
    let name = unique_name("blocked-delete");
    let factory = global_factory()?;
    let database = raw_open(&factory, &name, 1).await?;
    let mut delete = Box::pin(IndexedDbStore::delete(&name));

    assert!(poll_once(delete.as_mut()).is_pending());
    for _ in 0..5 {
        sleep_ms(20).await?;
        assert!(poll_once(delete.as_mut()).is_pending());
    }

    database.close();
    let mut delete_result = None;
    wait_until(2_000, || {
        let ready = if delete_result.is_some() {
            true
        } else if let Poll::Ready(result) = poll_once(delete.as_mut()) {
            delete_result = Some(result);
            true
        } else {
            false
        };
        async move { Ok(ready) }
    })
    .await?;
    let Some(result) = delete_result else {
        panic!("delete reported ready without a result");
    };
    result
}

#[wasm_bindgen_test]
async fn opening_newer_database_reports_version_error() -> Result<(), IndexedDbError> {
    let name = unique_name("newer-version");
    let factory = global_factory()?;
    let database = raw_open(&factory, &name, 2).await?;
    database.close();

    match IndexedDbStore::open(&name).await {
        Err(IndexedDbError::Backend { name, .. }) => assert_eq!(name, "VersionError"),
        Err(error) => panic!("newer database returned unexpected error: {error}"),
        Ok(_) => panic!("newer database unexpectedly opened at schema version one"),
    }
    IndexedDbStore::delete(&name).await
}
