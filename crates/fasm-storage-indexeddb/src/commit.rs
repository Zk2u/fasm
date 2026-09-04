//! Fenced, detached commits for buffered IndexedDB sessions.

use std::{
    cell::{Cell, RefCell},
    mem,
    rc::Rc,
};

use fasm_storage::Commit;
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use web_sys::{Event, IdbObjectStore, IdbRequest, IdbTransaction, IdbTransactionMode, console};

use crate::{
    IndexedDbError, IndexedDbTransaction, Revision,
    idb::{
        DetachedId, KV_STORE, META_STORE, REVISION_KEY, RequestFuture, TransactionOutcome,
        bytes_to_js, detach, dom_error, log_detached_failure, release, revision_from_js,
        revision_to_js,
    },
    operation::{OperationReceiver, OperationSender},
    overlay::WriteBuffer,
    store::{Scope, readonly_result},
};

#[cfg(test)]
use crate::session::FaultInjection;

type EventHandler = RefCell<Option<Closure<dyn FnMut(Event)>>>;
type CommitResult = Result<(), IndexedDbError>;

struct PreparedOp {
    key: JsValue,
    value: Option<JsValue>,
    #[cfg(test)]
    use_add: bool,
}

enum RequestedAbort {
    None,
    Conflict,
    Return(IndexedDbError),
}

/// Finalizes exactly one buffered session in one readwrite transaction.
///
/// The session's observed revision is compared and advanced in the same
/// transaction as every buffered set and tombstone. A mismatch aborts before
/// writes are enqueued, so two sessions cannot both commit from one revision.
/// The resulting errors have these replay guarantees:
///
/// | Result | Safe to retry from a fresh session? |
/// | --- | --- |
/// | [`IndexedDbError::Conflict`] | Yes; no writes were enqueued. |
/// | [`IndexedDbError::CommitAborted`] | Yes; IndexedDB rolled back atomically. |
/// | Any other error | No; reconcile persisted state first. |
///
/// Calling `commit` creates no browser work until its returned future is first
/// polled. Once polled, the IndexedDB operation is detached: dropping the Rust
/// future cannot cancel requests already submitted to the browser. Dropping a
/// session without polling commit remains the rollback path. An empty session
/// still validates the fence, but writes nothing and does not advance the
/// revision.
///
/// A write commit requests strict durability. Chrome honours that hint; other
/// browsers may ignore it and use their default durability. `Ok(())` therefore
/// means the transaction committed and, where the browser honours the hint,
/// was flushed durably. A store already closed by a version change fails before
/// a transaction can open.
impl Commit for IndexedDbTransaction {
    type Error = IndexedDbError;

    async fn commit(mut self) -> CommitResult {
        if self.engine.raw().buffer.is_empty() {
            return validate_empty_fence(&self.store, self.expected).await;
        }
        let next = self.expected.next()?;

        let buffer = mem::take(&mut self.engine.raw_mut().buffer);
        drop(self.engine);
        #[cfg(test)]
        let prepared = prepare_ops(buffer, &self.faults)?;
        #[cfg(not(test))]
        let prepared = prepare_ops(buffer);

        let transaction = self.store.begin_durable(Scope::KvAndMeta)?;
        CommitOperation::start(
            transaction,
            prepared,
            self.expected,
            next,
            #[cfg(test)]
            self.faults.fail_abort,
        )
        .await
    }
}

async fn validate_empty_fence(store: &crate::IndexedDbStore, expected: Revision) -> CommitResult {
    let transaction = store.begin(IdbTransactionMode::Readonly, Scope::Meta)?;
    let metadata = transaction
        .object_store(META_STORE)
        .map_err(|value| dom_error(&value))?;
    let request = metadata
        .get(&JsValue::from_str(REVISION_KEY))
        .map_err(|value| dom_error(&value))?;
    let outcome = TransactionOutcome::new(transaction);
    let revision = RequestFuture::new(request).await;
    readonly_result(outcome.await)?;
    let revision = revision_from_js(&revision?)?;

    if revision == expected {
        Ok(())
    } else {
        Err(IndexedDbError::Conflict)
    }
}

struct CommitOperation {
    transaction: IdbTransaction,
    revision_request: RefCell<Option<IdbRequest>>,
    prepared: RefCell<Option<Vec<PreparedOp>>>,
    expected: Revision,
    next: Revision,
    requested_abort: RefCell<RequestedAbort>,
    abort_failed: Cell<bool>,
    enqueue_started: Cell<bool>,
    request_diagnostic: RefCell<Option<String>>,
    receiver: OperationSender<CommitResult>,
    anchor: Cell<Option<DetachedId>>,
    revision_success: EventHandler,
    revision_error: EventHandler,
    complete: EventHandler,
    abort: EventHandler,
    transaction_error: EventHandler,
    #[cfg(test)]
    fail_abort: bool,
}

impl CommitOperation {
    fn start(
        transaction: IdbTransaction,
        prepared: Vec<PreparedOp>,
        expected: Revision,
        next: Revision,
        #[cfg(test)] fail_abort: bool,
    ) -> OperationReceiver<CommitResult> {
        let (receiver, sender) = OperationReceiver::new();
        let operation = Rc::new(Self {
            transaction,
            revision_request: RefCell::new(None),
            prepared: RefCell::new(Some(prepared)),
            expected,
            next,
            requested_abort: RefCell::new(RequestedAbort::None),
            abort_failed: Cell::new(false),
            enqueue_started: Cell::new(false),
            request_diagnostic: RefCell::new(None),
            receiver: sender,
            anchor: Cell::new(None),
            revision_success: RefCell::new(None),
            revision_error: RefCell::new(None),
            complete: RefCell::new(None),
            abort: RefCell::new(None),
            transaction_error: RefCell::new(None),
            #[cfg(test)]
            fail_abort,
        });
        operation.install_transaction_handlers();
        operation.anchor.set(Some(detach(Rc::clone(&operation))));
        operation.issue_revision_request();
        receiver
    }

    fn install_transaction_handlers(self: &Rc<Self>) {
        let weak = Rc::downgrade(self);
        let complete = Closure::new(move |_event: Event| {
            if let Some(operation) = weak.upgrade() {
                operation.completed();
            }
        });

        let weak = Rc::downgrade(self);
        let abort = Closure::new(move |_event: Event| {
            if let Some(operation) = weak.upgrade() {
                operation.aborted();
            }
        });

        let weak = Rc::downgrade(self);
        let transaction_error = Closure::new(move |event: Event| {
            if let Some(operation) = weak.upgrade() {
                operation.transaction_failed(event);
            }
        });

        self.transaction
            .set_oncomplete(Some(complete.as_ref().unchecked_ref()));
        self.transaction
            .set_onabort(Some(abort.as_ref().unchecked_ref()));
        self.transaction
            .set_onerror(Some(transaction_error.as_ref().unchecked_ref()));
        *self.complete.borrow_mut() = Some(complete);
        *self.abort.borrow_mut() = Some(abort);
        *self.transaction_error.borrow_mut() = Some(transaction_error);
    }

    fn issue_revision_request(self: &Rc<Self>) {
        let request = self
            .transaction
            .object_store(META_STORE)
            .map_err(|value| dom_error(&value))
            .and_then(|metadata| {
                metadata
                    .get(&JsValue::from_str(REVISION_KEY))
                    .map_err(|value| dom_error(&value))
            });
        let request = match request {
            Ok(request) => request,
            Err(error) => {
                self.abort_with(RequestedAbort::Return(error));
                return;
            }
        };

        let weak = Rc::downgrade(self);
        let success = Closure::new(move |_event: Event| {
            if let Some(operation) = weak.upgrade() {
                operation.revision_succeeded();
            }
        });

        let weak = Rc::downgrade(self);
        let error = Closure::new(move |_event: Event| {
            if let Some(operation) = weak.upgrade() {
                operation.revision_failed();
            }
        });

        request.set_onsuccess(Some(success.as_ref().unchecked_ref()));
        request.set_onerror(Some(error.as_ref().unchecked_ref()));
        *self.revision_success.borrow_mut() = Some(success);
        *self.revision_error.borrow_mut() = Some(error);
        *self.revision_request.borrow_mut() = Some(request);
    }

    fn revision_succeeded(&self) {
        let revision = match self.revision_result() {
            Ok(revision) => revision,
            Err(error) => {
                self.abort_with(RequestedAbort::Return(error));
                return;
            }
        };
        if revision != self.expected {
            self.abort_with(RequestedAbort::Conflict);
            return;
        }

        if let Err(error) = self.enqueue_all() {
            self.abort_with(RequestedAbort::Return(error));
        }
    }

    fn revision_result(&self) -> Result<Revision, IndexedDbError> {
        let request = self
            .revision_request
            .try_borrow()
            .map_err(|_| callback_state_error("revision request"))?;
        let request = request
            .as_ref()
            .ok_or_else(|| callback_state_error("missing revision request"))?;
        let value = request.result().map_err(|value| dom_error(&value))?;
        revision_from_js(&value)
    }

    fn enqueue_all(&self) -> Result<(), IndexedDbError> {
        let kv = self
            .transaction
            .object_store(KV_STORE)
            .map_err(|value| dom_error(&value))?;
        let metadata = self
            .transaction
            .object_store(META_STORE)
            .map_err(|value| dom_error(&value))?;
        let prepared = self
            .prepared
            .try_borrow_mut()
            .map_err(|_| callback_state_error("prepared writes"))?
            .take()
            .ok_or_else(|| callback_state_error("missing prepared writes"))?;

        for op in prepared {
            self.enqueue_started.set(true);
            enqueue(&kv, op)?;
        }
        metadata
            .put_with_key(&revision_to_js(self.next), &JsValue::from_str(REVISION_KEY))
            .map_err(|value| dom_error(&value))?;
        Ok(())
    }

    fn revision_failed(&self) {
        let diagnostic = self
            .revision_request
            .try_borrow()
            .ok()
            .and_then(|request| request.as_ref().cloned())
            .and_then(|request| request.error().ok().flatten())
            .map(|exception| format!("{}: {}", exception.name(), exception.message()))
            .unwrap_or_else(|| "revision request failed without an error".to_owned());
        match self.request_diagnostic.try_borrow_mut() {
            Ok(mut slot) => *slot = Some(diagnostic),
            Err(_) => log_callback_conflict("revision error diagnostic"),
        }
    }

    fn transaction_failed(&self, event: Event) {
        let diagnostic = event
            .target()
            .and_then(|target| target.dyn_into::<IdbRequest>().ok())
            .and_then(|request| request.error().ok().flatten())
            .map(|exception| format!("{}: {}", exception.name(), exception.message()))
            .unwrap_or_else(|| "transaction error event had no request error".to_owned());
        match self.request_diagnostic.try_borrow_mut() {
            Ok(mut slot) => *slot = Some(diagnostic),
            Err(_) => log_callback_conflict("transaction error diagnostic"),
        }
    }

    fn abort_with(&self, cause: RequestedAbort) {
        match self.requested_abort.try_borrow_mut() {
            Ok(mut requested) => {
                if matches!(*requested, RequestedAbort::None) {
                    *requested = cause;
                }
            }
            Err(_) => log_callback_conflict("requested abort"),
        }

        if let Err(value) = self.request_abort() {
            let error = dom_error(&value);
            console::error_1(&JsValue::from_str(&format!(
                "IndexedDB requested commit abort failed: {error}"
            )));
            self.abort_failed.set(true);
        }
    }

    fn request_abort(&self) -> Result<(), JsValue> {
        #[cfg(test)]
        if self.fail_abort {
            return Err(JsValue::from_str("injected abort failure"));
        }

        self.transaction.abort()
    }

    fn completed(&self) {
        let result = match self.take_requested_abort() {
            Ok(RequestedAbort::None) if self.abort_failed.get() => Err(IndexedDbError::Backend {
                name: "AbortError".to_owned(),
                message: "transaction completed after an abort failed without a recorded cause"
                    .to_owned(),
            }),
            Ok(RequestedAbort::None) => Ok(()),
            Ok(RequestedAbort::Conflict) => Err(IndexedDbError::Conflict),
            Ok(RequestedAbort::Return(error)) if !self.enqueue_started.get() => Err(error),
            Ok(RequestedAbort::Return(error)) => Err(IndexedDbError::Backend {
                name: "UnexpectedComplete".to_owned(),
                message: format!(
                    "transaction completed after a requested abort; outcome unknown: {error}"
                ),
            }),
            Err(error) => Err(error),
        };
        self.deliver(result);
        self.finish();
    }

    fn aborted(&self) {
        let result = match self.take_requested_abort() {
            Ok(RequestedAbort::Conflict) => Err(IndexedDbError::Conflict),
            Ok(RequestedAbort::Return(error)) => Err(error),
            Ok(RequestedAbort::None) => Err(self.unrequested_abort_error()),
            Err(error) => Err(error),
        };
        self.deliver(result);
        self.finish();
    }

    fn take_requested_abort(&self) -> Result<RequestedAbort, IndexedDbError> {
        self.requested_abort
            .try_borrow_mut()
            .map(|mut requested| mem::replace(&mut *requested, RequestedAbort::None))
            .map_err(|_| callback_state_error("terminal abort state"))
    }

    fn unrequested_abort_error(&self) -> IndexedDbError {
        match self.transaction.error() {
            Some(exception) if exception.name() == "QuotaExceededError" => {
                IndexedDbError::QuotaExceeded
            }
            Some(exception) => IndexedDbError::CommitAborted {
                reason: format!("{}: {}", exception.name(), exception.message()),
            },
            None => {
                if let Ok(diagnostic) = self.request_diagnostic.try_borrow()
                    && let Some(diagnostic) = diagnostic.as_ref()
                {
                    console::warn_1(&JsValue::from_str(&format!(
                        "IndexedDB commit abort diagnostic: {diagnostic}"
                    )));
                }
                IndexedDbError::CommitAborted {
                    reason: "transaction aborted without an error".to_owned(),
                }
            }
        }
    }

    fn deliver(&self, result: CommitResult) {
        if self.receiver.is_attached() {
            self.receiver.settle(result);
        } else if let Err(error) = result {
            log_detached_failure("commit", &error);
        }
    }

    fn finish(&self) {
        if let Ok(request) = self.revision_request.try_borrow()
            && let Some(request) = request.as_ref()
        {
            request.set_onsuccess(None);
            request.set_onerror(None);
        }
        self.transaction.set_oncomplete(None);
        self.transaction.set_onabort(None);
        self.transaction.set_onerror(None);
        if let Some(anchor) = self.anchor.take() {
            release(anchor);
        }
    }
}

#[cfg(not(test))]
fn prepare_ops(buffer: WriteBuffer) -> Vec<PreparedOp> {
    buffer
        .drain_ops()
        .into_iter()
        .map(|(key, value)| PreparedOp {
            key: bytes_to_js(&key),
            value: value.map(|value| bytes_to_js(&value)),
        })
        .collect()
}

#[cfg(test)]
fn prepare_ops(
    buffer: WriteBuffer,
    faults: &FaultInjection,
) -> Result<Vec<PreparedOp>, IndexedDbError> {
    buffer
        .drain_ops()
        .into_iter()
        .map(|(key, value)| {
            if faults.fail_conversion_of.as_deref() == Some(key.as_slice()) {
                return Err(IndexedDbError::Corrupt {
                    detail: "injected conversion failure".to_owned(),
                });
            }
            let use_add = faults.fail_request_of.as_deref() == Some(key.as_slice());
            let key = if faults.fail_enqueue_of.as_deref() == Some(key.as_slice()) {
                JsValue::UNDEFINED
            } else {
                bytes_to_js(&key)
            };
            Ok(PreparedOp {
                key,
                value: value.map(|value| bytes_to_js(&value)),
                use_add,
            })
        })
        .collect()
}

#[cfg(not(test))]
fn enqueue(store: &IdbObjectStore, op: PreparedOp) -> Result<(), IndexedDbError> {
    match op.value {
        Some(value) => store.put_with_key(&value, &op.key),
        None => store.delete(&op.key),
    }
    .map(|_| ())
    .map_err(|value| dom_error(&value))
}

#[cfg(test)]
fn enqueue(store: &IdbObjectStore, op: PreparedOp) -> Result<(), IndexedDbError> {
    match (op.value, op.use_add) {
        (Some(value), true) => store.add_with_key(&value, &op.key),
        (Some(value), false) => store.put_with_key(&value, &op.key),
        (None, _) => store.delete(&op.key),
    }
    .map(|_| ())
    .map_err(|value| dom_error(&value))
}

fn callback_state_error(context: &str) -> IndexedDbError {
    IndexedDbError::Backend {
        name: "CallbackStateError".to_owned(),
        message: format!("commit callback could not access {context}"),
    }
}

fn log_callback_conflict(context: &str) {
    console::error_1(&JsValue::from_str(&format!(
        "IndexedDB {context} callback could not borrow operation state"
    )));
}
