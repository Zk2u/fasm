//! Future adapters for requests, cursors, and transaction termination.

use std::{
    cell::RefCell,
    future::Future,
    mem,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use web_sys::{DomException, Event, IdbCursorWithValue, IdbFactory, IdbRequest, IdbTransaction};

use crate::IndexedDbError;

const INDEXED_DB_PROPERTY: &str = "indexedDB";

/// Reads an IndexedDB factory from a browser or worker global object.
pub(crate) fn factory_from(global: &JsValue) -> Result<IdbFactory, IndexedDbError> {
    let value = js_sys::Reflect::get(global, &JsValue::from_str(INDEXED_DB_PROPERTY))
        .map_err(|_| IndexedDbError::Unavailable)?;
    if value.is_null() || value.is_undefined() {
        return Err(IndexedDbError::Unavailable);
    }

    value
        .dyn_into::<IdbFactory>()
        .map_err(|_| IndexedDbError::Unavailable)
}

/// Finds IndexedDB without assuming the global is a `Window`.
pub(crate) fn global_factory() -> Result<IdbFactory, IndexedDbError> {
    factory_from(&js_sys::global())
}

/// Classifies a JavaScript exception without retaining application bytes.
pub(crate) fn dom_error(value: &JsValue) -> IndexedDbError {
    if let Some(exception) = value.dyn_ref::<DomException>() {
        let name = exception.name();
        if name == "QuotaExceededError" {
            return IndexedDbError::QuotaExceeded;
        }
        return IndexedDbError::Backend {
            name,
            message: exception.message(),
        };
    }

    let message = match value.as_string() {
        Some(message) => message,
        None => "<non-string error>".to_owned(),
    };
    IndexedDbError::Backend {
        name: "UnknownError".to_owned(),
        message,
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

fn log_borrow_conflict(context: &str) {
    web_sys::console::error_1(&JsValue::from_str(&format!(
        "IndexedDB {context} callback could not borrow adapter state"
    )));
}

struct RequestState {
    settled: bool,
    outcome: Option<Result<JsValue, IndexedDbError>>,
    waker: Option<Waker>,
}

impl RequestState {
    fn pending() -> Self {
        Self {
            settled: false,
            outcome: None,
            waker: None,
        }
    }
}

fn settle_request(
    state: &Rc<RefCell<RequestState>>,
    outcome: Result<JsValue, IndexedDbError>,
) -> bool {
    let waker = match state.try_borrow_mut() {
        Ok(mut state) => {
            if state.settled {
                return false;
            }
            state.settled = true;
            state.outcome = Some(outcome);
            state.waker.take()
        }
        Err(_) => {
            log_borrow_conflict("request");
            return false;
        }
    };

    if let Some(waker) = waker {
        waker.wake();
    }
    true
}

/// A single-settlement future for one-shot IndexedDB requests.
///
/// The adapter owns both installed closures. Terminal callbacks clear the DOM
/// properties immediately; the closures themselves are dropped when the ready
/// future is polled or when the future is abandoned.
pub(crate) struct RequestFuture {
    request: IdbRequest,
    state: Rc<RefCell<RequestState>>,
    success: Option<Closure<dyn FnMut(Event)>>,
    error: Option<Closure<dyn FnMut(Event)>>,
}

impl RequestFuture {
    /// Installs owned success and error handlers for `request`.
    pub(crate) fn new(request: IdbRequest) -> Self {
        let state = Rc::new(RefCell::new(RequestState::pending()));

        let success_state = Rc::clone(&state);
        let success_request = request.clone();
        let success = Closure::new(move |_event: Event| {
            let outcome = success_request.result().map_err(|value| dom_error(&value));
            if settle_request(&success_state, outcome) {
                success_request.set_onsuccess(None);
                success_request.set_onerror(None);
            }
        });

        let error_state = Rc::clone(&state);
        let error_request = request.clone();
        let error = Closure::new(move |_event: Event| {
            let outcome = Err(request_error(&error_request));
            if settle_request(&error_state, outcome) {
                error_request.set_onsuccess(None);
                error_request.set_onerror(None);
            }
        });

        request.set_onsuccess(Some(success.as_ref().unchecked_ref()));
        request.set_onerror(Some(error.as_ref().unchecked_ref()));

        Self {
            request,
            state,
            success: Some(success),
            error: Some(error),
        }
    }

    fn clear_handlers(&mut self) {
        self.request.set_onsuccess(None);
        self.request.set_onerror(None);
        self.success.take();
        self.error.take();
    }
}

impl Future for RequestFuture {
    type Output = Result<JsValue, IndexedDbError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let outcome = match this.state.try_borrow_mut() {
            Ok(mut state) => match state.outcome.take() {
                Some(outcome) => Some(outcome),
                None => {
                    state.waker = Some(context.waker().clone());
                    None
                }
            },
            Err(_) => {
                log_borrow_conflict("request poll");
                None
            }
        };

        match outcome {
            Some(outcome) => {
                this.clear_handlers();
                Poll::Ready(outcome)
            }
            None => Poll::Pending,
        }
    }
}

impl Drop for RequestFuture {
    fn drop(&mut self) {
        self.clear_handlers();
    }
}

/// Rows produced by one uninterrupted cursor callback chain.
pub(crate) struct CursorPage {
    /// JavaScript keys and values in cursor order.
    pub(crate) rows: Vec<(JsValue, JsValue)>,
    /// Whether the cursor reached its terminal null result.
    pub(crate) exhausted: bool,
}

struct CursorState {
    settled: bool,
    rows: Vec<(JsValue, JsValue)>,
    outcome: Option<Result<CursorPage, IndexedDbError>>,
    waker: Option<Waker>,
}

impl CursorState {
    fn pending() -> Self {
        Self {
            settled: false,
            rows: Vec::new(),
            outcome: None,
            waker: None,
        }
    }
}

fn settle_cursor(
    state: &Rc<RefCell<CursorState>>,
    outcome: Result<CursorPage, IndexedDbError>,
) -> bool {
    let waker = match state.try_borrow_mut() {
        Ok(mut state) => {
            if state.settled {
                return false;
            }
            state.settled = true;
            state.outcome = Some(outcome);
            state.waker.take()
        }
        Err(_) => {
            log_borrow_conflict("cursor");
            return false;
        }
    };

    if let Some(waker) = waker {
        waker.wake();
    }
    true
}

fn settle_cursor_page(state: &Rc<RefCell<CursorState>>, exhausted: bool) -> bool {
    let waker = match state.try_borrow_mut() {
        Ok(mut state) => {
            if state.settled {
                return false;
            }
            let page = CursorPage {
                rows: mem::take(&mut state.rows),
                exhausted,
            };
            state.settled = true;
            state.outcome = Some(Ok(page));
            state.waker.take()
        }
        Err(_) => {
            log_borrow_conflict("cursor");
            return false;
        }
    };

    if let Some(waker) = waker {
        waker.wake();
    }
    true
}

struct CursorPageFuture {
    request: IdbRequest,
    state: Rc<RefCell<CursorState>>,
    success: Option<Closure<dyn FnMut(Event)>>,
    error: Option<Closure<dyn FnMut(Event)>>,
}

impl CursorPageFuture {
    fn new(request: IdbRequest, page_size: usize) -> Self {
        let state = Rc::new(RefCell::new(CursorState::pending()));

        let success_state = Rc::clone(&state);
        let success_request = request.clone();
        let success = Closure::new(move |_event: Event| {
            let result = match success_request.result() {
                Ok(result) => result,
                Err(value) => {
                    if settle_cursor(&success_state, Err(dom_error(&value))) {
                        success_request.set_onsuccess(None);
                        success_request.set_onerror(None);
                    }
                    return;
                }
            };

            if result.is_null() || result.is_undefined() {
                if settle_cursor_page(&success_state, true) {
                    success_request.set_onsuccess(None);
                    success_request.set_onerror(None);
                }
                return;
            }

            let cursor = match result.dyn_into::<IdbCursorWithValue>() {
                Ok(cursor) => cursor,
                Err(_) => {
                    let settled = settle_cursor(
                        &success_state,
                        Err(IndexedDbError::Corrupt {
                            detail: "cursor request returned a non-cursor value".to_owned(),
                        }),
                    );
                    if settled {
                        success_request.set_onsuccess(None);
                        success_request.set_onerror(None);
                    }
                    return;
                }
            };
            let key = match cursor.key() {
                Ok(key) => key,
                Err(value) => {
                    if settle_cursor(&success_state, Err(dom_error(&value))) {
                        success_request.set_onsuccess(None);
                        success_request.set_onerror(None);
                    }
                    return;
                }
            };
            let value = match cursor.value() {
                Ok(value) => value,
                Err(value) => {
                    if settle_cursor(&success_state, Err(dom_error(&value))) {
                        success_request.set_onsuccess(None);
                        success_request.set_onerror(None);
                    }
                    return;
                }
            };

            let full_page = match success_state.try_borrow_mut() {
                Ok(mut state) => {
                    if state.settled {
                        return;
                    }
                    state.rows.push((key, value));
                    state.rows.len() >= page_size
                }
                Err(_) => {
                    log_borrow_conflict("cursor success");
                    return;
                }
            };

            if full_page {
                if settle_cursor_page(&success_state, false) {
                    success_request.set_onsuccess(None);
                    success_request.set_onerror(None);
                }
            } else if let Err(value) = cursor.continue_()
                && settle_cursor(&success_state, Err(dom_error(&value)))
            {
                success_request.set_onsuccess(None);
                success_request.set_onerror(None);
            }
        });

        let error_state = Rc::clone(&state);
        let error_request = request.clone();
        let error = Closure::new(move |_event: Event| {
            let outcome = Err(request_error(&error_request));
            if settle_cursor(&error_state, outcome) {
                error_request.set_onsuccess(None);
                error_request.set_onerror(None);
            }
        });

        request.set_onsuccess(Some(success.as_ref().unchecked_ref()));
        request.set_onerror(Some(error.as_ref().unchecked_ref()));

        Self {
            request,
            state,
            success: Some(success),
            error: Some(error),
        }
    }

    fn clear_handlers(&mut self) {
        self.request.set_onsuccess(None);
        self.request.set_onerror(None);
        self.success.take();
        self.error.take();
    }
}

impl Future for CursorPageFuture {
    type Output = Result<CursorPage, IndexedDbError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let outcome = match this.state.try_borrow_mut() {
            Ok(mut state) => match state.outcome.take() {
                Some(outcome) => Some(outcome),
                None => {
                    state.waker = Some(context.waker().clone());
                    None
                }
            },
            Err(_) => {
                log_borrow_conflict("cursor poll");
                None
            }
        };

        match outcome {
            Some(outcome) => {
                this.clear_handlers();
                Poll::Ready(outcome)
            }
            None => Poll::Pending,
        }
    }
}

impl Drop for CursorPageFuture {
    fn drop(&mut self) {
        self.clear_handlers();
    }
}

/// Drives one cursor page entirely from callbacks while its transaction is active.
pub(crate) fn read_cursor_page(
    request: IdbRequest,
    page_size: usize,
) -> impl Future<Output = Result<CursorPage, IndexedDbError>> {
    CursorPageFuture::new(request, page_size)
}

/// The only two terminal states of an IndexedDB transaction.
pub(crate) enum TransactionEnd {
    /// Every request completed and the browser committed the transaction.
    Complete,
    /// The browser rolled the transaction back.
    Aborted {
        /// The transaction's error at abort time; explicit `abort()` has none.
        error: Option<DomException>,
    },
}

struct TransactionState {
    settled: bool,
    outcome: Option<TransactionEnd>,
    waker: Option<Waker>,
    last_request_error: Option<DomException>,
}

impl TransactionState {
    fn pending() -> Self {
        Self {
            settled: false,
            outcome: None,
            waker: None,
            last_request_error: None,
        }
    }
}

fn settle_transaction(state: &Rc<RefCell<TransactionState>>, outcome: TransactionEnd) -> bool {
    let waker = match state.try_borrow_mut() {
        Ok(mut state) => {
            if state.settled {
                return false;
            }
            state.settled = true;
            state.outcome = Some(outcome);
            state.waker.take()
        }
        Err(_) => {
            log_borrow_conflict("transaction");
            return false;
        }
    };

    if let Some(waker) = waker {
        waker.wake();
    }
    true
}

/// A future that distinguishes transaction commit from rollback.
///
/// An `error` event can be a bubbled request failure and is deliberately only
/// diagnostic. The later `abort` event is what settles the adapter.
pub(crate) struct TransactionOutcome {
    transaction: IdbTransaction,
    state: Rc<RefCell<TransactionState>>,
    complete: Option<Closure<dyn FnMut(Event)>>,
    abort: Option<Closure<dyn FnMut(Event)>>,
    error: Option<Closure<dyn FnMut(Event)>>,
}

impl TransactionOutcome {
    /// Installs owned handlers for all transaction lifecycle events.
    pub(crate) fn new(transaction: IdbTransaction) -> Self {
        let state = Rc::new(RefCell::new(TransactionState::pending()));

        let complete_state = Rc::clone(&state);
        let complete_transaction = transaction.clone();
        let complete = Closure::new(move |_event: Event| {
            if settle_transaction(&complete_state, TransactionEnd::Complete) {
                complete_transaction.set_oncomplete(None);
                complete_transaction.set_onabort(None);
                complete_transaction.set_onerror(None);
            }
        });

        let abort_state = Rc::clone(&state);
        let abort_transaction = transaction.clone();
        let abort = Closure::new(move |_event: Event| {
            let error = abort_transaction.error();
            if settle_transaction(&abort_state, TransactionEnd::Aborted { error }) {
                abort_transaction.set_oncomplete(None);
                abort_transaction.set_onabort(None);
                abort_transaction.set_onerror(None);
            }
        });

        let error_state = Rc::clone(&state);
        let error = Closure::new(move |event: Event| {
            let error = event
                .target()
                .and_then(|target| target.dyn_into::<IdbRequest>().ok())
                .and_then(|request| request.error().ok().flatten());
            match error_state.try_borrow_mut() {
                Ok(mut state) => {
                    if !state.settled {
                        state.last_request_error = error;
                    }
                }
                Err(_) => log_borrow_conflict("transaction error"),
            }
        });

        transaction.set_oncomplete(Some(complete.as_ref().unchecked_ref()));
        transaction.set_onabort(Some(abort.as_ref().unchecked_ref()));
        transaction.set_onerror(Some(error.as_ref().unchecked_ref()));

        Self {
            transaction,
            state,
            complete: Some(complete),
            abort: Some(abort),
            error: Some(error),
        }
    }

    fn clear_handlers(&mut self) {
        self.transaction.set_oncomplete(None);
        self.transaction.set_onabort(None);
        self.transaction.set_onerror(None);
        self.complete.take();
        self.abort.take();
        self.error.take();
    }
}

impl Future for TransactionOutcome {
    type Output = TransactionEnd;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let outcome = match this.state.try_borrow_mut() {
            Ok(mut state) => match state.outcome.take() {
                Some(outcome) => Some(outcome),
                None => {
                    state.waker = Some(context.waker().clone());
                    None
                }
            },
            Err(_) => {
                log_borrow_conflict("transaction poll");
                None
            }
        };

        match outcome {
            Some(outcome) => {
                this.clear_handlers();
                Poll::Ready(outcome)
            }
            None => Poll::Pending,
        }
    }
}

impl Drop for TransactionOutcome {
    fn drop(&mut self) {
        self.clear_handlers();
    }
}
