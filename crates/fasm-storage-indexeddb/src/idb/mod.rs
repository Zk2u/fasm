//! Panic-free adapters for IndexedDB's callback-driven primitives.
//!
//! IndexedDB requests and transactions report completion through mutable DOM
//! event-handler properties. Each adapter therefore owns every closure it
//! installs, clears the corresponding properties on settlement or drop, and
//! keeps a shared settled bit so duplicate or late events cannot produce a
//! second result. In particular, cursor success handlers remain alive across
//! every row event instead of using one-shot closures.
//!
//! The callbacks use fallible `RefCell` borrowing. A borrow conflict is logged
//! and ignored because unwinding through a wasm callback would trap JavaScript
//! and could let an active transaction auto-commit in a partially driven state.

mod detached;
mod request;

pub(crate) use detached::{DetachedId, detach, log_detached_failure, release};
pub(crate) use request::{
    CursorPage, RequestFuture, TransactionEnd, TransactionOutcome, dom_error, factory_from,
    global_factory, read_cursor_page,
};

/// Maximum number of committed rows driven by one cursor callback chain.
pub(crate) const PAGE_SIZE: usize = 256;

#[cfg(test)]
mod tests;
