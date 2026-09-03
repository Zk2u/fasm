//! Shared raw-IndexedDB fixtures for browser tests.
//!
//! These helpers deliberately use `web-sys` directly so conversion, adapter,
//! and store tests can exercise the browser without depending on a higher-level
//! store implementation.

use std::{
    future::Future,
    sync::atomic::{AtomicU32, Ordering},
};

use js_sys::{Function, Promise, Reflect};
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use wasm_bindgen_futures::JsFuture;
use web_sys::{Event, IdbDatabase, IdbObjectStore, IdbRequest, IdbTransaction, IdbTransactionMode};

use super::{
    KV_STORE, RequestFuture, TransactionEnd, TransactionOutcome, bytes_to_js, dom_error,
    global_factory,
};
use crate::{IndexedDbError, IndexedDbStore, store::Scope};

static NEXT_DATABASE: AtomicU32 = AtomicU32::new(1);

/// A raw test database paired with the name needed to delete it.
pub(crate) struct RawDatabase {
    pub(crate) database: IdbDatabase,
    name: String,
}

impl RawDatabase {
    /// Closes the connection and deletes the database created by the fixture.
    pub(crate) async fn close_and_delete(self) -> Result<(), IndexedDbError> {
        self.database.close();
        let factory = global_factory()?;
        let delete = from_js(factory.delete_database(&self.name))?;
        let request: IdbRequest = delete.unchecked_into();
        RequestFuture::new(request).await?;
        Ok(())
    }
}

/// Returns a process-unique database name labelled with `test_name`.
pub(crate) fn unique_name(test_name: &str) -> String {
    let serial = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
    format!("fasm-storage-indexeddb-{test_name}-{serial}")
}

/// Seeds committed root-directory rows in a valid flat-layout store.
///
/// Tests that bypass the public session surface must still stamp the exact
/// layout metadata written by the directory driver. Otherwise any subsequent
/// read correctly classifies their raw data as foreign.
pub(crate) async fn seed_root_rows(
    store: &IndexedDbStore,
    rows: &[(&[u8], &[u8])],
) -> Result<(), IndexedDbError> {
    use fasm_storage::flatdir::{
        COUNTER_KEY, LAYOUT_VERSION, ROOT_PREFIX, ROOT_PREFIX_KEY, VERSION_KEY, encode_varint,
    };

    let transaction = store.begin(IdbTransactionMode::Readwrite, Scope::Kv)?;
    let outcome = TransactionOutcome::new(transaction.clone());
    let object_store = from_js(transaction.object_store(KV_STORE))?;
    let counter = encode_varint(1);
    for (key, value) in [
        (VERSION_KEY, LAYOUT_VERSION),
        (ROOT_PREFIX_KEY, ROOT_PREFIX),
        (COUNTER_KEY, counter.as_slice()),
    ] {
        from_js(object_store.put_with_key(&bytes_to_js(value), &bytes_to_js(key)))?;
    }
    for (key, value) in rows {
        let mut raw_key = ROOT_PREFIX.to_vec();
        raw_key.extend_from_slice(key);
        from_js(object_store.put_with_key(&bytes_to_js(value), &bytes_to_js(&raw_key)))?;
    }
    await_complete(outcome).await
}

/// Yields to the browser event loop and resolves after at least `milliseconds`.
pub(crate) async fn sleep_ms(milliseconds: u32) -> Result<(), IndexedDbError> {
    let global = js_sys::global();
    let set_timeout = Reflect::get(&global, &JsValue::from_str("setTimeout"))
        .map_err(|value| dom_error(&value))?
        .dyn_into::<Function>()
        .map_err(|_| IndexedDbError::Unavailable)?;
    let promise = Promise::new(&mut |resolve, reject| {
        if let Err(value) = set_timeout.call2(
            &global,
            resolve.as_ref(),
            &JsValue::from_f64(f64::from(milliseconds)),
        ) {
            let _ = reject.call1(&JsValue::UNDEFINED, &value);
        }
    });
    JsFuture::from(promise)
        .await
        .map(|_| ())
        .map_err(|value| dom_error(&value))
}

/// Rechecks an asynchronous condition until it succeeds or the deadline passes.
///
/// Browser storage work completes on later event-loop turns, so detached-
/// operation tests must observe the promised effect rather than assume a fixed
/// delay is long enough on every test runner.
pub(crate) async fn wait_until<F, Fut>(
    timeout_ms: u32,
    mut predicate: F,
) -> Result<(), IndexedDbError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, IndexedDbError>>,
{
    let mut elapsed_ms = 0;
    loop {
        if predicate().await? {
            return Ok(());
        }
        if elapsed_ms >= timeout_ms {
            return Err(IndexedDbError::Backend {
                name: "TimeoutError".to_owned(),
                message: format!(
                    "test condition was not satisfied within {timeout_ms} milliseconds"
                ),
            });
        }
        sleep_ms(10).await?;
        elapsed_ms += 10;
    }
}

/// Opens a fresh version-one database containing the object store `t`.
pub(crate) async fn raw_database(test_name: &str) -> Result<RawDatabase, IndexedDbError> {
    let name = unique_name(test_name);
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
    let database = value
        .dyn_into::<IdbDatabase>()
        .map_err(|_| IndexedDbError::Corrupt {
            detail: "open request returned a non-database value".to_owned(),
        })?;
    Ok(RawDatabase { name, database })
}

/// Opens object store `t` in a transaction with the requested mode.
pub(crate) fn object_store(
    database: &IdbDatabase,
    mode: IdbTransactionMode,
) -> Result<(IdbTransaction, IdbObjectStore), IndexedDbError> {
    let transaction = from_js(database.transaction_with_str_and_mode("t", mode))?;
    let store = from_js(transaction.object_store("t"))?;
    Ok((transaction, store))
}

/// Awaits a transaction and maps rollback into the backend's commit error.
pub(crate) async fn await_complete(outcome: TransactionOutcome) -> Result<(), IndexedDbError> {
    match outcome.await {
        TransactionEnd::Complete => Ok(()),
        TransactionEnd::Aborted { error } => {
            let reason = match error {
                Some(exception) => exception.message(),
                None => "test transaction aborted".to_owned(),
            };
            Err(IndexedDbError::CommitAborted { reason })
        }
    }
}

fn from_js<T>(result: Result<T, JsValue>) -> Result<T, IndexedDbError> {
    result.map_err(|value| dom_error(&value))
}

fn log_fixture_failure(context: &str, value: &JsValue) {
    let error = dom_error(value);
    web_sys::console::error_1(&JsValue::from_str(&format!(
        "IndexedDB test fixture {context}: {error}"
    )));
}
