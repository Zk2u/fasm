//! Named IndexedDB connections and detached database operations.

use std::{
    cell::{Cell, RefCell},
    fmt,
    future::Future,
    pin::Pin,
    rc::{Rc, Weak},
    task::{Context, Poll, Waker},
};

use js_sys::Array;
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use web_sys::{
    Event, IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbRequest, IdbTransaction,
    IdbTransactionMode,
};

use crate::{
    IndexedDbError, IndexedDbReader, IndexedDbTransaction, Revision,
    idb::{
        DetachedId, KV_STORE, META_STORE, REVISION_KEY, RequestFuture, SCHEMA_VERSION,
        TransactionEnd, TransactionOutcome, detach, dom_error, global_factory,
        log_detached_failure, release, revision_from_js, revision_to_js,
    },
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

    /// Starts one buffered state transition fenced by the current revision.
    ///
    /// A session represents exactly one state transition and can be committed
    /// once. Its initial revision is the optimistic fence checked during that
    /// commit. Opening a session early and committing it much later is safe,
    /// but increases the chance of a false-positive [`IndexedDbError::Conflict`]
    /// after an unrelated session advances the fence.
    ///
    /// A permanently closed store returns [`IndexedDbError::Closed`].
    pub async fn transaction(&self) -> Result<IndexedDbTransaction, IndexedDbError> {
        let transaction = self.begin(IdbTransactionMode::Readonly, Scope::Meta)?;
        let metadata = transaction
            .object_store(META_STORE)
            .map_err(|value| dom_error(&value))?;
        let request = metadata
            .get(&JsValue::from_str(REVISION_KEY))
            .map_err(|value| dom_error(&value))?;
        let outcome = TransactionOutcome::new(transaction);
        let revision = RequestFuture::new(request).await;
        readonly_result(outcome.await)?;
        let expected = revision_from_js(&revision?)?;

        Ok(IndexedDbTransaction::new(self.clone(), expected))
    }

    /// Returns a read-only view over committed data.
    ///
    /// Each point read, and later each range page, uses its own readonly
    /// IndexedDB transaction. A page is internally consistent, but a scan can
    /// observe commits made between pages. This is weaker than redb's single
    /// read transaction and FDB's single-transaction scan; the handle exists
    /// to provide API parity where that per-page consistency is sufficient.
    pub fn reader(&self) -> IndexedDbReader {
        IndexedDbReader::new(self.clone())
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
            Scope::Kv => database.transaction_with_str_and_mode(KV_STORE, mode),
            Scope::Meta => database.transaction_with_str_and_mode(META_STORE, mode),
            Scope::KvAndMeta => {
                let stores = Array::new();
                stores.push(&JsValue::from_str(KV_STORE));
                stores.push(&JsValue::from_str(META_STORE));
                database.transaction_with_str_sequence_and_mode(stores.as_ref(), mode)
            }
        };

        transaction.map_err(|value| {
            let error = dom_error(&value);
            if matches!(
                &error,
                IndexedDbError::Backend { name, .. } if name == "InvalidStateError"
            ) {
                self.inner.closed.set(true);
                IndexedDbError::Closed
            } else {
                error
            }
        })
    }
}

/// Object stores included in an IndexedDB transaction.
pub(crate) enum Scope {
    /// The application key/value object store.
    #[allow(dead_code)] // used from commit 7 (trait impl)
    Kv,
    /// The revision metadata object store.
    Meta,
    /// The application and revision metadata object stores.
    #[allow(dead_code)] // used from commit 8 (commit)
    KvAndMeta,
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

struct ReceiverState<T> {
    outcome: Option<T>,
    waker: Option<Waker>,
}

impl<T> ReceiverState<T> {
    fn pending() -> Self {
        Self {
            outcome: None,
            waker: None,
        }
    }
}

struct OperationReceiver<T> {
    state: Rc<RefCell<ReceiverState<T>>>,
}

impl<T> Future for OperationReceiver<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.state.try_borrow_mut() {
            Ok(mut state) => match state.outcome.take() {
                Some(outcome) => Poll::Ready(outcome),
                None => {
                    state.waker = Some(context.waker().clone());
                    Poll::Pending
                }
            },
            Err(_) => {
                log_callback_conflict("receiver poll");
                Poll::Pending
            }
        }
    }
}

fn settle_receiver<T>(receiver: &Weak<RefCell<ReceiverState<T>>>, outcome: T) -> bool {
    let Some(receiver) = receiver.upgrade() else {
        return false;
    };
    let waker = match receiver.try_borrow_mut() {
        Ok(mut state) => {
            state.outcome = Some(outcome);
            state.waker.take()
        }
        Err(_) => {
            log_callback_conflict("receiver settlement");
            return true;
        }
    };
    if let Some(waker) = waker {
        waker.wake();
    }
    true
}

struct OpenOperation {
    request: IdbOpenDbRequest,
    name: String,
    receiver: Weak<RefCell<ReceiverState<Result<IndexedDbStore, IndexedDbError>>>>,
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
        let receiver = Rc::new(RefCell::new(ReceiverState::pending()));
        let operation = Rc::new(Self {
            request,
            name,
            receiver: Rc::downgrade(&receiver),
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
        OperationReceiver { state: receiver }
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
                if self.receiver.upgrade().is_none() {
                    database.close();
                } else {
                    let store = IndexedDbStore {
                        inner: Connection::new(self.name.clone(), database),
                    };
                    settle_receiver(&self.receiver, Ok(store));
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
        if self.receiver.upgrade().is_some() {
            settle_receiver(&self.receiver, Err(error));
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
    receiver: Weak<RefCell<ReceiverState<Result<(), IndexedDbError>>>>,
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
        let receiver = Rc::new(RefCell::new(ReceiverState::pending()));
        let operation = Rc::new(Self {
            request,
            name,
            receiver: Rc::downgrade(&receiver),
            anchor: Cell::new(None),
            blocked_warned: Cell::new(false),
            blocked: RefCell::new(None),
            success: RefCell::new(None),
            error: RefCell::new(None),
        });
        operation.install_handlers();
        operation.anchor.set(Some(detach(Rc::clone(&operation))));
        OperationReceiver { state: receiver }
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
        settle_receiver(&self.receiver, Ok(()));
        self.finish();
    }

    fn delete_failed(&self) {
        let error = request_error(&self.request);
        if self.receiver.upgrade().is_some() {
            settle_receiver(&self.receiver, Err(error));
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
