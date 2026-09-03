//! Browser tests for the request, cursor and transaction adapters.
//!
//! There is no store type yet, so these tests open a throwaway database per
//! test with plain web-sys (`raw_database`) and drive the adapters directly.
//! They run only under `wasm-pack test --headless --chrome|--firefox`; Node has
//! no IndexedDB.

use std::sync::atomic::{AtomicU32, Ordering};

use js_sys::{Object, Uint8Array};
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use wasm_bindgen_test::wasm_bindgen_test;
use web_sys::{
    DomException, Event, IdbCursorDirection, IdbDatabase, IdbObjectStore, IdbRequest,
    IdbTransaction, IdbTransactionMode,
};

use super::{
    CursorPage, RequestFuture, TransactionEnd, TransactionOutcome, dom_error, factory_from,
    global_factory, read_cursor_page,
};
use crate::IndexedDbError;

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

static NEXT_DATABASE: AtomicU32 = AtomicU32::new(1);

fn from_js<T>(result: Result<T, JsValue>) -> Result<T, IndexedDbError> {
    result.map_err(|value| dom_error(&value))
}

fn log_fixture_failure(context: &str, value: &JsValue) {
    let error = dom_error(value);
    web_sys::console::error_1(&JsValue::from_str(&format!(
        "IndexedDB test fixture {context}: {error}"
    )));
}

async fn raw_database(test_name: &str) -> Result<IdbDatabase, IndexedDbError> {
    let serial = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
    let name = format!("fasm-storage-indexeddb-{test_name}-{serial}");
    let factory = global_factory()?;
    let open = from_js(factory.open_with_u32(&name, 1))?;

    let upgrade_request = open.clone();
    let upgrade: Closure<dyn FnMut(Event)> = Closure::new(move |_event: Event| {
        let value = match upgrade_request.result() {
            Ok(value) => value,
            Err(value) => {
                log_fixture_failure("could not read upgrade result", &value);
                return;
            }
        };
        let database = match value.dyn_into::<IdbDatabase>() {
            Ok(database) => database,
            Err(value) => {
                log_fixture_failure("upgrade result was not a database", &value);
                return;
            }
        };
        if let Err(value) = database.create_object_store("t") {
            log_fixture_failure("could not create object store", &value);
        }
    });
    open.set_onupgradeneeded(Some(upgrade.as_ref().unchecked_ref()));

    let request: IdbRequest = open.clone().unchecked_into();
    let result = RequestFuture::new(request).await;
    open.set_onupgradeneeded(None);
    drop(upgrade);
    let value = result?;
    value
        .dyn_into::<IdbDatabase>()
        .map_err(|_| IndexedDbError::Corrupt {
            detail: "open request returned a non-database value".to_owned(),
        })
}

fn object_store(
    database: &IdbDatabase,
    mode: IdbTransactionMode,
) -> Result<(IdbTransaction, IdbObjectStore), IndexedDbError> {
    let transaction = from_js(database.transaction_with_str_and_mode("t", mode))?;
    let store = from_js(transaction.object_store("t"))?;
    Ok((transaction, store))
}

fn binary_key(byte: u8) -> Uint8Array {
    Uint8Array::from(&[byte][..])
}

fn first_key_byte(row: &(JsValue, JsValue)) -> u8 {
    Uint8Array::new(&row.0).get_index(0)
}

async fn seed(database: &IdbDatabase, keys: &[u8]) -> Result<(), IndexedDbError> {
    let (transaction, store) = object_store(database, IdbTransactionMode::Readwrite)?;
    let outcome = TransactionOutcome::new(transaction);
    for key in keys {
        let key_array = binary_key(*key);
        from_js(store.put_with_key(&JsValue::from_f64(f64::from(*key)), key_array.as_ref()))?;
    }

    match outcome.await {
        TransactionEnd::Complete => Ok(()),
        TransactionEnd::Aborted { error } => {
            let reason = match error {
                Some(exception) => exception.message(),
                None => "test seed transaction aborted".to_owned(),
            };
            Err(IndexedDbError::CommitAborted { reason })
        }
    }
}

async fn cursor_page(
    database: &IdbDatabase,
    page_size: usize,
    direction: Option<IdbCursorDirection>,
) -> Result<CursorPage, IndexedDbError> {
    let (transaction, store) = object_store(database, IdbTransactionMode::Readonly)?;
    let outcome = TransactionOutcome::new(transaction);
    let request = match direction {
        Some(direction) => {
            from_js(store.open_cursor_with_range_and_direction(&JsValue::UNDEFINED, direction))?
        }
        None => from_js(store.open_cursor())?,
    };
    let page = read_cursor_page(request, page_size).await?;
    match outcome.await {
        TransactionEnd::Complete => Ok(page),
        TransactionEnd::Aborted { error } => {
            let reason = match error {
                Some(exception) => exception.message(),
                None => "test cursor transaction aborted".to_owned(),
            };
            Err(IndexedDbError::CommitAborted { reason })
        }
    }
}

#[wasm_bindgen_test]
async fn discovers_factory_from_browser_and_rejects_plain_object() -> Result<(), IndexedDbError> {
    let plain: JsValue = Object::new().into();
    assert!(matches!(
        factory_from(&plain),
        Err(IndexedDbError::Unavailable)
    ));
    global_factory()?;
    Ok(())
}

#[wasm_bindgen_test]
async fn request_future_resolves_and_clears_handlers() -> Result<(), IndexedDbError> {
    let database = raw_database("request-success").await?;
    let (transaction, store) = object_store(&database, IdbTransactionMode::Readwrite)?;
    let outcome = TransactionOutcome::new(transaction);
    let request =
        from_js(store.put_with_key(&JsValue::from_str("value"), &JsValue::from_str("key")))?;
    RequestFuture::new(request.clone()).await?;

    assert!(request.onsuccess().is_none());
    assert!(request.onerror().is_none());
    assert!(matches!(outcome.await, TransactionEnd::Complete));
    database.close();
    Ok(())
}

#[wasm_bindgen_test]
async fn dropping_pending_request_future_clears_handlers() -> Result<(), IndexedDbError> {
    let database = raw_database("request-drop").await?;
    let (_transaction, store) = object_store(&database, IdbTransactionMode::Readonly)?;
    let request = from_js(store.get(&JsValue::from_str("missing")))?;
    let future = RequestFuture::new(request.clone());
    drop(future);

    assert!(request.onsuccess().is_none());
    assert!(request.onerror().is_none());
    database.close();
    Ok(())
}

#[wasm_bindgen_test]
async fn cursor_pages_cover_full_partial_empty_and_reverse_scans() -> Result<(), IndexedDbError> {
    let database = raw_database("cursor-pages").await?;
    seed(&database, &[1, 2, 3, 4, 5]).await?;

    let full = cursor_page(&database, 2, None).await?;
    assert_eq!(full.rows.len(), 2);
    assert!(!full.exhausted);
    assert_eq!(
        full.rows.iter().map(first_key_byte).collect::<Vec<_>>(),
        vec![1, 2]
    );

    let partial = cursor_page(&database, 8, None).await?;
    assert_eq!(partial.rows.len(), 5);
    assert!(partial.exhausted);

    let reverse = cursor_page(&database, 8, Some(IdbCursorDirection::Prev)).await?;
    assert!(reverse.exhausted);
    assert_eq!(
        reverse.rows.iter().map(first_key_byte).collect::<Vec<_>>(),
        vec![5, 4, 3, 2, 1]
    );
    database.close();

    let empty_database = raw_database("cursor-empty").await?;
    let empty = cursor_page(&empty_database, 2, None).await?;
    assert!(empty.rows.is_empty());
    assert!(empty.exhausted);
    empty_database.close();
    Ok(())
}

#[wasm_bindgen_test]
async fn duplicate_add_rejects_request_then_aborts_transaction() -> Result<(), IndexedDbError> {
    let database = raw_database("constraint-abort").await?;
    let (transaction, store) = object_store(&database, IdbTransactionMode::Readwrite)?;
    let outcome = TransactionOutcome::new(transaction);
    let key = binary_key(1);
    let first = from_js(store.add_with_key(&JsValue::from_str("first"), key.as_ref()))?;
    let second = from_js(store.add_with_key(&JsValue::from_str("second"), key.as_ref()))?;
    let first = RequestFuture::new(first);
    let second = RequestFuture::new(second);

    first.await?;
    match second.await {
        Err(IndexedDbError::Backend { name, .. }) => assert_eq!(name, "ConstraintError"),
        Err(error) => panic!("unexpected duplicate-add error: {error}"),
        Ok(_) => panic!("duplicate add unexpectedly succeeded"),
    }
    match outcome.await {
        TransactionEnd::Aborted { error: Some(_) } => {}
        TransactionEnd::Aborted { error: None } => {
            panic!("constraint failure aborted without a transaction error")
        }
        TransactionEnd::Complete => panic!("constraint failure transaction committed"),
    }
    database.close();
    Ok(())
}

#[wasm_bindgen_test]
async fn successful_writes_complete_transaction() -> Result<(), IndexedDbError> {
    let database = raw_database("transaction-complete").await?;
    let (transaction, store) = object_store(&database, IdbTransactionMode::Readwrite)?;
    let outcome = TransactionOutcome::new(transaction);
    from_js(store.put_with_key(&JsValue::from_str("one"), &JsValue::from_str("one")))?;
    from_js(store.put_with_key(&JsValue::from_str("two"), &JsValue::from_str("two")))?;

    assert!(matches!(outcome.await, TransactionEnd::Complete));
    database.close();
    Ok(())
}

#[wasm_bindgen_test]
async fn maps_dom_and_non_dom_errors() -> Result<(), IndexedDbError> {
    let exception = from_js(DomException::new_with_message_and_name(
        "storage is full",
        "QuotaExceededError",
    ))?;
    assert!(matches!(
        dom_error(exception.as_ref()),
        IndexedDbError::QuotaExceeded
    ));

    match dom_error(&JsValue::from_str("boom")) {
        IndexedDbError::Backend { name, message } => {
            assert_eq!(name, "UnknownError");
            assert_eq!(message, "boom");
        }
        error => panic!("unexpected non-DOM mapping: {error}"),
    }
    Ok(())
}
