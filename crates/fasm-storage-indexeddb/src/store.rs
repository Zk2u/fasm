//! Named IndexedDB connections and detached database operations.

use std::{
    cell::{Cell, RefCell},
    fmt,
    rc::{Rc, Weak},
};

use js_sys::{Array, Function, Object, Reflect};
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use web_sys::{
    Event, IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbRequest, IdbTransaction,
    IdbTransactionMode,
};

use crate::{
    IndexedDbError, IndexedDbReader, IndexedDbTransaction, Revision,
    idb::{
        DetachedId, KV_STORE, META_STORE, REVISION_KEY, RequestFuture, SCHEMA_VERSION,
        TransactionEnd, TransactionOutcome, bytes_from_js, detach, dom_error, global_factory,
        log_detached_failure, read_cursor_page, release, revision_from_js, revision_to_js,
    },
    operation::{OperationReceiver, OperationSender},
    overlay::Rows,
};

type EventHandler = RefCell<Option<Closure<dyn FnMut(Event)>>>;

/// A cloneable handle to one named browser IndexedDB connection.
///
/// Clones share the same underlying connection and therefore observe the same
/// permanent closed state. Open the name again to obtain a new connection after
/// [`is_closed`](Self::is_closed) becomes true.
#[derive(Clone)]
pub struct IndexedDbStore {
    inner: Rc<Connection>,
}

impl fmt::Debug for IndexedDbStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexedDbStore")
            .field("name", &self.name())
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl IndexedDbStore {
    /// Opens the version-one FASM database named `name` for the current storage key.
    ///
    /// IndexedDB names are local to a storage key, normally the page's origin,
    /// and each name identifies one database there. A new database contains an
    /// application `kv` object store and a `meta` object store holding its
    /// revision fence. If another connection blocks creation or upgrade, this
    /// future keeps waiting and emits one browser-console warning.
    ///
    /// The returned handle is cloneable; all clones share one connection. A
    /// version change requested by another tab or an abnormal browser close
    /// permanently closes that shared handle. The caller must call `open` again
    /// rather than retrying through a handle that reports [`IndexedDbError::Closed`].
    pub async fn open(name: &str) -> Result<Self, IndexedDbError> {
        Self::open_with(global_factory()?, name).await
    }

    pub(crate) async fn open_with(factory: IdbFactory, name: &str) -> Result<Self, IndexedDbError> {
        let request = factory
            .open_with_u32(name, SCHEMA_VERSION)
            .map_err(|value| dom_error(&value))?;
        OpenOperation::start(request, name.to_owned()).await
    }

    /// Deletes the database named `name` from the current storage key.
    ///
    /// Existing connections can block deletion. The operation waits for those
    /// connections to close and continues in the browser even if its Rust
    /// future is dropped after the request has been issued.
    pub async fn delete(name: &str) -> Result<(), IndexedDbError> {
        Self::delete_with(global_factory()?, name).await
    }

    pub(crate) async fn delete_with(factory: IdbFactory, name: &str) -> Result<(), IndexedDbError> {
        let request = factory
            .delete_database(name)
            .map_err(|value| dom_error(&value))?;
        DeleteOperation::start(request, name.to_owned()).await
    }

    /// Returns the database name supplied to [`open`](Self::open).
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Reports whether this shared connection has closed permanently.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.get()
    }

    /// Captures all raw rows and their revision for one state transition.
    ///
    /// This costs a full database read and an owned copy in memory. Reads stay
    /// stable for the session's lifetime; commit checks the captured revision
    /// before applying only the buffered changes. Concurrent commits, even to
    /// unrelated directories, make this session's commit return `Conflict`.
    pub async fn transaction(&self) -> Result<IndexedDbTransaction, IndexedDbError> {
        let (rows, expected) = self.snapshot().await?;
        Ok(IndexedDbTransaction::new(self.clone(), expected, rows))
    }

    /// Captures a read-only snapshot of the whole database.
    ///
    /// Reads and scans remain stable across later commits. Call this method
    /// again to refresh; opening a reader performs a full database read and
    /// retains an owned copy until the reader is dropped.
    pub async fn reader(&self) -> Result<IndexedDbReader, IndexedDbError> {
        let (rows, _) = self.snapshot().await?;
        Ok(IndexedDbReader::new(self.clone(), rows))
    }

    async fn snapshot(&self) -> Result<(Rows, Revision), IndexedDbError> {
        // Enqueue BOTH requests and install ALL handlers before the first
        // await. They share one readonly transaction over kv + meta, so the
        // returned rows can never be paired with a different revision.
        let transaction = self.begin(IdbTransactionMode::Readonly, Scope::KvAndMeta)?;
        let outcome = TransactionOutcome::new(transaction.clone());
        let metadata = transaction
            .object_store(META_STORE)
            .map_err(|v| dom_error(&v))?;
        let kv = transaction
            .object_store(KV_STORE)
            .map_err(|v| dom_error(&v))?;
        let revision = RequestFuture::new(
            metadata
                .get(&JsValue::from_str(REVISION_KEY))
                .map_err(|v| dom_error(&v))?,
        );
        let cursor = read_cursor_page(kv.open_cursor().map_err(|v| dom_error(&v))?, usize::MAX);
        let revision = revision.await;
        let page = cursor.await;
        readonly_result(outcome.await)?;
        let expected = revision_from_js(&revision?)?;
        let page = page?;
        if !page.exhausted {
            return Err(IndexedDbError::Corrupt {
                detail: "snapshot cursor did not exhaust the database".to_owned(),
            });
        }
        let mut rows = Rows::new();
        for (key, value) in page.rows {
            let key = bytes_from_js(&key, "key")?;
            let value = bytes_from_js(&value, "value")?;
            if rows.insert(key, value).is_some() {
                return Err(IndexedDbError::Corrupt {
                    detail: "duplicate binary key in snapshot".to_owned(),
                });
            }
        }
        Ok((rows, expected))
    }

    pub(crate) fn database(&self) -> Result<&IdbDatabase, IndexedDbError> {
        if self.is_closed() {
            Err(IndexedDbError::Closed)
        } else {
            Ok(&self.inner.database)
        }
    }

    pub(crate) fn begin(
        &self,
        mode: IdbTransactionMode,
        scope: Scope,
    ) -> Result<IdbTransaction, IndexedDbError> {
        let database = self.database()?;
        let transaction = match scope {
            #[cfg(test)]
            Scope::Kv => database.transaction_with_str_and_mode(KV_STORE, mode),
            Scope::Meta => database.transaction_with_str_and_mode(META_STORE, mode),
            Scope::KvAndMeta => {
                let stores = Array::new();
                stores.push(&JsValue::from_str(KV_STORE));
                stores.push(&JsValue::from_str(META_STORE));
                database.transaction_with_str_sequence_and_mode(stores.as_ref(), mode)
            }
        };

        transaction.map_err(|value| self.begin_error(&value))
    }

    /// Starts a readwrite transaction that requests strict browser durability.
    ///
    /// The options overload is called reflectively because web-sys exposes it
    /// only under `web_sys_unstable_apis`. Browsers that do not implement the
    /// hint may ignore it and create a transaction with their default
    /// durability instead.
    pub(crate) fn begin_durable(&self, scope: Scope) -> Result<IdbTransaction, IndexedDbError> {
        let database = self.database()?;
        let store_names = match scope {
            #[cfg(test)]
            Scope::Kv => JsValue::from_str(KV_STORE),
            Scope::Meta => JsValue::from_str(META_STORE),
            Scope::KvAndMeta => {
                let stores = Array::new();
                stores.push(&JsValue::from_str(KV_STORE));
                stores.push(&JsValue::from_str(META_STORE));
                stores.into()
            }
        };
        let options = Object::new();
        Reflect::set(
            options.as_ref(),
            &JsValue::from_str("durability"),
            &JsValue::from_str("strict"),
        )
        .map_err(|value| self.begin_error(&value))?;
        let transaction = Reflect::get(database.as_ref(), &JsValue::from_str("transaction"))
            .map_err(|value| self.begin_error(&value))?
            .dyn_into::<Function>()
            .map_err(|value| self.begin_error(&value))?;
        transaction
            .call3(
                database.as_ref(),
                &store_names,
                &JsValue::from_str("readwrite"),
                options.as_ref(),
            )
            .map_err(|value| self.begin_error(&value))?
            .dyn_into::<IdbTransaction>()
            .map_err(|value| self.begin_error(&value))
    }

    fn begin_error(&self, value: &JsValue) -> IndexedDbError {
        let error = dom_error(value);
        if matches!(
            &error,
            IndexedDbError::Backend { name, .. } if name == "InvalidStateError"
        ) {
            self.inner.closed.set(true);
            IndexedDbError::Closed
        } else {
            error
        }
    }
}

/// Object stores included in an IndexedDB transaction.
pub(crate) enum Scope {
    /// The application key/value object store.
    #[cfg(test)]
    Kv,
    /// The revision metadata object store.
    Meta,
    /// The application and revision metadata object stores.
    KvAndMeta,
}

/// Returns the browser-reported durability mode when that property is exposed.
#[cfg(test)]
pub(crate) fn transaction_durability(transaction: &IdbTransaction) -> Option<String> {
    Reflect::get(transaction.as_ref(), &JsValue::from_str("durability"))
        .ok()?
        .as_string()
}

pub(crate) fn readonly_result(outcome: TransactionEnd) -> Result<(), IndexedDbError> {
    match outcome {
        TransactionEnd::Complete => Ok(()),
        TransactionEnd::Aborted { error } => match error {
            Some(exception) => Err(IndexedDbError::Backend {
                name: exception.name(),
                message: exception.message(),
            }),
            None => Err(IndexedDbError::Backend {
                name: "AbortError".to_owned(),
                message: "readonly IndexedDB transaction aborted".to_owned(),
            }),
        },
    }
}

struct Connection {
    name: String,
    database: IdbDatabase,
    closed: Cell<bool>,
    versionchange: Option<Closure<dyn FnMut(Event)>>,
    close: Option<Closure<dyn FnMut(Event)>>,
}

impl Connection {
    fn new(name: String, database: IdbDatabase) -> Rc<Self> {
        Rc::new_cyclic(|weak: &Weak<Self>| {
            let versionchange_weak = weak.clone();
            let versionchange = Closure::new(move |_event: Event| {
                if let Some(connection) = versionchange_weak.upgrade() {
                    connection.closed.set(true);
                    connection.database.close();
                }
            });

            let close_weak = weak.clone();
            let close = Closure::new(move |_event: Event| {
                if let Some(connection) = close_weak.upgrade() {
                    connection.closed.set(true);
                }
            });

            database.set_onversionchange(Some(versionchange.as_ref().unchecked_ref()));
            database.set_onclose(Some(close.as_ref().unchecked_ref()));

            Self {
                name,
                database,
                closed: Cell::new(false),
                versionchange: Some(versionchange),
                close: Some(close),
            }
        })
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.database.set_onversionchange(None);
        self.database.set_onclose(None);
        self.versionchange.take();
        self.close.take();
        self.closed.set(true);
        self.database.close();
    }
}

struct OpenOperation {
    request: IdbOpenDbRequest,
    name: String,
    receiver: OperationSender<Result<IndexedDbStore, IndexedDbError>>,
    anchor: Cell<Option<DetachedId>>,
    upgrade_error: RefCell<Option<IndexedDbError>>,
    blocked_warned: Cell<bool>,
    upgradeneeded: EventHandler,
    blocked: EventHandler,
    success: EventHandler,
    error: EventHandler,
}

impl OpenOperation {
    fn start(
        request: IdbOpenDbRequest,
        name: String,
    ) -> OperationReceiver<Result<IndexedDbStore, IndexedDbError>> {
        let (receiver, sender) = OperationReceiver::new();
        let operation = Rc::new(Self {
            request,
            name,
            receiver: sender,
            anchor: Cell::new(None),
            upgrade_error: RefCell::new(None),
            blocked_warned: Cell::new(false),
            upgradeneeded: RefCell::new(None),
            blocked: RefCell::new(None),
            success: RefCell::new(None),
            error: RefCell::new(None),
        });
        operation.install_handlers();
        operation.anchor.set(Some(detach(Rc::clone(&operation))));
        receiver
    }

    fn install_handlers(self: &Rc<Self>) {
        let weak = Rc::downgrade(self);
        let upgradeneeded = Closure::new(move |_event: Event| {
            if let Some(operation) = weak.upgrade() {
                operation.upgrade();
            }
        });

        let weak = Rc::downgrade(self);
        let blocked = Closure::new(move |_event: Event| {
            if let Some(operation) = weak.upgrade()
                && !operation.blocked_warned.replace(true)
            {
                web_sys::console::warn_1(&JsValue::from_str(&format!(
                    "open of {} is blocked by another connection; waiting",
                    operation.name
                )));
            }
        });

        let weak = Rc::downgrade(self);
        let success = Closure::new(move |_event: Event| {
            if let Some(operation) = weak.upgrade() {
                operation.open_succeeded();
            }
        });

        let weak = Rc::downgrade(self);
        let error = Closure::new(move |_event: Event| {
            if let Some(operation) = weak.upgrade() {
                operation.open_failed();
            }
        });

        self.request
            .set_onupgradeneeded(Some(upgradeneeded.as_ref().unchecked_ref()));
        self.request
            .set_onblocked(Some(blocked.as_ref().unchecked_ref()));
        self.request
            .set_onsuccess(Some(success.as_ref().unchecked_ref()));
        self.request
            .set_onerror(Some(error.as_ref().unchecked_ref()));

        *self.upgradeneeded.borrow_mut() = Some(upgradeneeded);
        *self.blocked.borrow_mut() = Some(blocked);
        *self.success.borrow_mut() = Some(success);
        *self.error.borrow_mut() = Some(error);
    }

    fn upgrade(&self) {
        match self.upgrade_error.try_borrow() {
            Ok(error) if error.is_some() => return,
            Ok(_) => {}
            Err(_) => {
                log_callback_conflict("schema upgrade");
                return;
            }
        }
        if let Err(error) = self.create_schema() {
            match self.upgrade_error.try_borrow_mut() {
                Ok(mut upgrade_error) => *upgrade_error = Some(error),
                Err(_) => {
                    log_callback_conflict("schema upgrade error");
                    return;
                }
            }
            if let Some(transaction) = self.request.transaction()
                && let Err(value) = transaction.abort()
            {
                web_sys::console::error_1(&JsValue::from_str(&format!(
                    "IndexedDB schema upgrade abort failed: {}",
                    dom_error(&value)
                )));
            }
        }
    }

    fn create_schema(&self) -> Result<(), IndexedDbError> {
        let database = self
            .request
            .result()
            .map_err(|value| dom_error(&value))?
            .dyn_into::<IdbDatabase>()
            .map_err(|_| IndexedDbError::Corrupt {
                detail: "open upgrade returned a non-database value".to_owned(),
            })?;
        database
            .create_object_store(KV_STORE)
            .map_err(|value| dom_error(&value))?;
        database
            .create_object_store(META_STORE)
            .map_err(|value| dom_error(&value))?;
        let transaction = self
            .request
            .transaction()
            .ok_or_else(|| IndexedDbError::Corrupt {
                detail: "open upgrade did not expose its versionchange transaction".to_owned(),
            })?;
        let metadata = transaction
            .object_store(META_STORE)
            .map_err(|value| dom_error(&value))?;
        metadata
            .put_with_key(
                &revision_to_js(Revision::ZERO),
                &JsValue::from_str(REVISION_KEY),
            )
            .map_err(|value| dom_error(&value))?;
        Ok(())
    }

    fn open_succeeded(&self) {
        let database = self.database_result();
        let upgrade_error = match self.upgrade_error.try_borrow_mut() {
            Ok(mut error) => error.take(),
            Err(_) => {
                log_callback_conflict("open success");
                self.finish();
                return;
            }
        };
        match (upgrade_error, database) {
            (Some(error), Ok(database)) => {
                database.close();
                self.deliver_error(error);
            }
            (Some(error), Err(_)) | (None, Err(error)) => self.deliver_error(error),
            (None, Ok(database)) => {
                if !self.receiver.is_attached() {
                    database.close();
                } else {
                    let store = IndexedDbStore {
                        inner: Connection::new(self.name.clone(), database),
                    };
                    self.receiver.settle(Ok(store));
                }
            }
        }
        self.finish();
    }

    fn open_failed(&self) {
        let upgrade_error = match self.upgrade_error.try_borrow_mut() {
            Ok(mut error) => error.take(),
            Err(_) => {
                log_callback_conflict("open error");
                None
            }
        };
        let error = match upgrade_error {
            Some(error) => error,
            None => request_error(&self.request),
        };
        self.deliver_error(error);
        self.finish();
    }

    fn deliver_error(&self, error: IndexedDbError) {
        if self.receiver.is_attached() {
            self.receiver.settle(Err(error));
        } else {
            log_detached_failure("open", &error);
        }
    }

    fn database_result(&self) -> Result<IdbDatabase, IndexedDbError> {
        self.request
            .result()
            .map_err(|value| dom_error(&value))?
            .dyn_into::<IdbDatabase>()
            .map_err(|_| IndexedDbError::Corrupt {
                detail: "open request returned a non-database value".to_owned(),
            })
    }

    fn finish(&self) {
        self.request.set_onupgradeneeded(None);
        self.request.set_onblocked(None);
        self.request.set_onsuccess(None);
        self.request.set_onerror(None);
        if let Some(anchor) = self.anchor.take() {
            release(anchor);
        }
    }
}

struct DeleteOperation {
    request: IdbOpenDbRequest,
    name: String,
    receiver: OperationSender<Result<(), IndexedDbError>>,
    anchor: Cell<Option<DetachedId>>,
    blocked_warned: Cell<bool>,
    blocked: EventHandler,
    success: EventHandler,
    error: EventHandler,
}

impl DeleteOperation {
    fn start(
        request: IdbOpenDbRequest,
        name: String,
    ) -> OperationReceiver<Result<(), IndexedDbError>> {
        let (receiver, sender) = OperationReceiver::new();
        let operation = Rc::new(Self {
            request,
            name,
            receiver: sender,
            anchor: Cell::new(None),
            blocked_warned: Cell::new(false),
            blocked: RefCell::new(None),
            success: RefCell::new(None),
            error: RefCell::new(None),
        });
        operation.install_handlers();
        operation.anchor.set(Some(detach(Rc::clone(&operation))));
        receiver
    }

    fn install_handlers(self: &Rc<Self>) {
        let weak = Rc::downgrade(self);
        let blocked = Closure::new(move |_event: Event| {
            if let Some(operation) = weak.upgrade()
                && !operation.blocked_warned.replace(true)
            {
                web_sys::console::warn_1(&JsValue::from_str(&format!(
                    "delete of {} is blocked by another connection; waiting",
                    operation.name
                )));
            }
        });

        let weak = Rc::downgrade(self);
        let success = Closure::new(move |_event: Event| {
            if let Some(operation) = weak.upgrade() {
                operation.delete_succeeded();
            }
        });

        let weak = Rc::downgrade(self);
        let error = Closure::new(move |_event: Event| {
            if let Some(operation) = weak.upgrade() {
                operation.delete_failed();
            }
        });

        self.request
            .set_onblocked(Some(blocked.as_ref().unchecked_ref()));
        self.request
            .set_onsuccess(Some(success.as_ref().unchecked_ref()));
        self.request
            .set_onerror(Some(error.as_ref().unchecked_ref()));

        *self.blocked.borrow_mut() = Some(blocked);
        *self.success.borrow_mut() = Some(success);
        *self.error.borrow_mut() = Some(error);
    }

    fn delete_succeeded(&self) {
        self.receiver.settle(Ok(()));
        self.finish();
    }

    fn delete_failed(&self) {
        let error = request_error(&self.request);
        if self.receiver.is_attached() {
            self.receiver.settle(Err(error));
        } else {
            log_detached_failure("delete", &error);
        }
        self.finish();
    }

    fn finish(&self) {
        self.request.set_onblocked(None);
        self.request.set_onsuccess(None);
        self.request.set_onerror(None);
        if let Some(anchor) = self.anchor.take() {
            release(anchor);
        }
    }
}

fn request_error(request: &IdbRequest) -> IndexedDbError {
    match request.error() {
        Ok(Some(exception)) => dom_error(exception.as_ref()),
        Ok(None) => IndexedDbError::Backend {
            name: "UnknownError".to_owned(),
            message: "IndexedDB request failed without a DOM exception".to_owned(),
        },
        Err(value) => dom_error(&value),
    }
}

fn log_callback_conflict(context: &str) {
    web_sys::console::error_1(&JsValue::from_str(&format!(
        "IndexedDB {context} callback could not borrow operation state"
    )));
}
