//! Browser tests for the request, cursor and transaction adapters.
//!
//! There is no store type yet, so these tests open a throwaway database per
//! test with plain web-sys (`raw_database`) and drive the adapters directly.
//! They run only under `wasm-pack test --headless --chrome|--firefox`; Node has
//! no IndexedDB.

use js_sys::{Object, Uint8Array};
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use wasm_bindgen_test::wasm_bindgen_test;
use web_sys::{DomException, Event, IdbCursorDirection, IdbDatabase, IdbTransactionMode};

use super::{
    CursorPage, RequestFuture, TransactionEnd, TransactionOutcome, dom_error, factory_from,
    global_factory, read_cursor_page,
};
use crate::IndexedDbError;
use crate::idb::fixture::{await_complete, object_store, raw_database};

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

fn from_js<T>(result: Result<T, JsValue>) -> Result<T, IndexedDbError> {
    result.map_err(|value| dom_error(&value))
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

    await_complete(outcome).await
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
    await_complete(outcome).await?;
    Ok(page)
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
    let raw = raw_database("request-success").await?;
    let (transaction, store) = object_store(&raw.database, IdbTransactionMode::Readwrite)?;
    let outcome = TransactionOutcome::new(transaction);
    let request =
        from_js(store.put_with_key(&JsValue::from_str("value"), &JsValue::from_str("key")))?;
    RequestFuture::new(request.clone()).await?;

    assert!(request.onsuccess().is_none());
    assert!(request.onerror().is_none());
    assert!(matches!(outcome.await, TransactionEnd::Complete));
    raw.close_and_delete().await
}

#[wasm_bindgen_test]
async fn dropping_pending_request_future_clears_handlers() -> Result<(), IndexedDbError> {
    let raw = raw_database("request-drop").await?;
    let (_transaction, store) = object_store(&raw.database, IdbTransactionMode::Readonly)?;
    let request = from_js(store.get(&JsValue::from_str("missing")))?;
    let future = RequestFuture::new(request.clone());
    drop(future);

    assert!(request.onsuccess().is_none());
    assert!(request.onerror().is_none());
    raw.close_and_delete().await
}

#[wasm_bindgen_test]
async fn cursor_pages_cover_full_partial_empty_and_reverse_scans() -> Result<(), IndexedDbError> {
    let raw = raw_database("cursor-pages").await?;
    seed(&raw.database, &[1, 2, 3, 4, 5]).await?;

    let full = cursor_page(&raw.database, 2, None).await?;
    assert_eq!(full.rows.len(), 2);
    assert!(!full.exhausted);
    assert_eq!(
        full.rows.iter().map(first_key_byte).collect::<Vec<_>>(),
        vec![1, 2]
    );

    let partial = cursor_page(&raw.database, 8, None).await?;
    assert_eq!(partial.rows.len(), 5);
    assert!(partial.exhausted);

    let reverse = cursor_page(&raw.database, 8, Some(IdbCursorDirection::Prev)).await?;
    assert!(reverse.exhausted);
    assert_eq!(
        reverse.rows.iter().map(first_key_byte).collect::<Vec<_>>(),
        vec![5, 4, 3, 2, 1]
    );
    raw.close_and_delete().await?;

    let empty_raw = raw_database("cursor-empty").await?;
    let empty = cursor_page(&empty_raw.database, 2, None).await?;
    assert!(empty.rows.is_empty());
    assert!(empty.exhausted);
    empty_raw.close_and_delete().await
}

#[wasm_bindgen_test]
async fn duplicate_add_rejects_request_then_aborts_transaction() -> Result<(), IndexedDbError> {
    let raw = raw_database("constraint-abort").await?;
    let (transaction, store) = object_store(&raw.database, IdbTransactionMode::Readwrite)?;
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
    raw.close_and_delete().await
}

#[wasm_bindgen_test]
async fn prevented_request_error_does_not_settle_transaction_outcome() -> Result<(), IndexedDbError>
{
    let raw = raw_database("prevented-constraint").await?;
    let (transaction, store) = object_store(&raw.database, IdbTransactionMode::Readwrite)?;
    let outcome = TransactionOutcome::new(transaction);
    let key = binary_key(1);
    let first = from_js(store.add_with_key(&JsValue::from_str("first"), key.as_ref()))?;
    let duplicate = from_js(store.add_with_key(&JsValue::from_str("second"), key.as_ref()))?;
    let prevent_abort: Closure<dyn FnMut(Event)> = Closure::new(move |event: Event| {
        event.prevent_default();
    });
    duplicate.set_onerror(Some(prevent_abort.as_ref().unchecked_ref()));

    RequestFuture::new(first).await?;
    let end = outcome.await;
    duplicate.set_onerror(None);
    drop(prevent_abort);

    assert!(matches!(end, TransactionEnd::Complete));
    raw.close_and_delete().await
}

#[wasm_bindgen_test]
async fn successful_writes_complete_transaction() -> Result<(), IndexedDbError> {
    let raw = raw_database("transaction-complete").await?;
    let (transaction, store) = object_store(&raw.database, IdbTransactionMode::Readwrite)?;
    let outcome = TransactionOutcome::new(transaction);
    from_js(store.put_with_key(&JsValue::from_str("one"), &JsValue::from_str("one")))?;
    from_js(store.put_with_key(&JsValue::from_str("two"), &JsValue::from_str("two")))?;

    assert!(matches!(outcome.await, TransactionEnd::Complete));
    raw.close_and_delete().await
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
