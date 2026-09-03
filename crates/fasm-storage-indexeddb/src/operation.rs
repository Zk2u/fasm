//! Shared receiver for browser operations that outlive their initiating future.

use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::{Rc, Weak},
    task::{Context, Poll, Waker},
};

use wasm_bindgen::JsValue;
use web_sys::console;

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

/// The future-owned half of a detached browser operation.
///
/// The operation retains only a weak reference to this receiver. Dropping the
/// future therefore cannot cancel browser work that has already started, and
/// lets the terminal callback detect that an error must be logged instead of
/// delivered.
pub(crate) struct OperationReceiver<T> {
    state: Rc<RefCell<ReceiverState<T>>>,
}

impl<T> OperationReceiver<T> {
    /// Creates a receiver and the weak handle retained by its operation.
    pub(crate) fn new() -> (Self, OperationSender<T>) {
        let state = Rc::new(RefCell::new(ReceiverState::pending()));
        let sender = OperationSender {
            state: Rc::downgrade(&state),
        };
        (Self { state }, sender)
    }
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

/// The operation-owned weak half used to deliver one terminal outcome.
pub(crate) struct OperationSender<T> {
    state: Weak<RefCell<ReceiverState<T>>>,
}

impl<T> OperationSender<T> {
    /// Returns whether the initiating future still exists.
    pub(crate) fn is_attached(&self) -> bool {
        self.state.strong_count() != 0
    }

    /// Delivers an outcome, returning false when the receiver was dropped.
    pub(crate) fn settle(&self, outcome: T) -> bool {
        let Some(receiver) = self.state.upgrade() else {
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
}

fn log_callback_conflict(context: &str) {
    console::error_1(&JsValue::from_str(&format!(
        "IndexedDB {context} callback could not borrow operation state"
    )));
}
