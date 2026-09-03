//! Lifetime anchoring for operations that must survive a dropped Rust future.

use std::{
    any::Any,
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
};

use wasm_bindgen::JsValue;

use crate::IndexedDbError;

thread_local! {
    /// Strong references keeping browser operations alive until a terminal event.
    static LIVE_OPERATIONS: RefCell<HashMap<u64, Rc<dyn Any>>> = RefCell::new(HashMap::new());
    static NEXT_DETACHED_ID: Cell<u64> = const { Cell::new(1) };
}

/// Registry key for an operation detached from the future that started it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct DetachedId(u64);

/// Anchors `op` independently of the awaiting Rust future.
///
/// The operation stores the returned identifier in its shared state. Its
/// terminal `complete`, `abort`, `success`, or `error` handler must call
/// [`release`] on itself after recording the outcome and clearing handlers.
/// Non-terminal events such as an open request's `blocked` event must retain
/// the anchor.
pub(crate) fn detach<T: 'static>(op: Rc<T>) -> DetachedId {
    let id = NEXT_DETACHED_ID.with(|next| {
        let id = next.get();
        let following = id.wrapping_add(1).max(1);
        next.set(following);
        DetachedId(id)
    });
    let erased: Rc<dyn Any> = op;

    LIVE_OPERATIONS.with(|operations| match operations.try_borrow_mut() {
        Ok(mut operations) => {
            operations.insert(id.0, erased);
        }
        Err(_) => web_sys::console::error_1(&JsValue::from_str(
            "IndexedDB detached-operation registry is already borrowed",
        )),
    });

    id
}

/// Releases the registry anchor after a detached operation reaches a terminal event.
pub(crate) fn release(id: DetachedId) {
    LIVE_OPERATIONS.with(|operations| match operations.try_borrow_mut() {
        Ok(mut operations) => {
            operations.remove(&id.0);
        }
        Err(_) => web_sys::console::error_1(&JsValue::from_str(
            "IndexedDB detached-operation registry is already borrowed during release",
        )),
    });
}

/// Reports a detached failure that no longer has an awaiting caller.
pub(crate) fn log_detached_failure(context: &str, error: &IndexedDbError) {
    web_sys::console::error_1(&JsValue::from_str(&format!(
        "detached IndexedDB {context} failed: {error}"
    )));
}
