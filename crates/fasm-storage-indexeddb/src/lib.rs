//! Browser [`KvStore`](fasm_storage::KvStore) storage backed by IndexedDB.
//!
//! This backend uses raw binary IndexedDB keys. IndexedDB orders binary keys
//! bytewise, with a shorter prefix before the longer key, exactly matching the
//! storage trait's ordering contract. No key encoding layer is needed, so the
//! bytes chosen by a state machine remain the bytes visible to the database.
//!
//! # Buffered sessions
//!
//! A session buffers writes in Rust as set values and tombstones. Reads overlay
//! that buffer on short readonly IndexedDB transactions; commit applies the
//! complete buffer in one readwrite transaction, and dropping an uncommitted
//! session is a rollback. The buffering is necessary because IndexedDB
//! transactions auto-commit when control returns to the event loop, while a
//! state transition may await non-IndexedDB work before it is ready to commit.
//! Holding one browser transaction across that work would therefore provide a
//! misleading atomicity boundary.
//!
//! Reads made at different times within a session are not one database
//! snapshot. To make that weaker read model safe for writes, a `meta` object
//! store holds a checked-u53 `revision`. A session remembers the revision it
//! observed, then compares and increments it inside its commit transaction. A
//! mismatch returns [`IndexedDbError::Conflict`], making the entire transition
//! retryable. Concurrent sessions remain safe across tabs and workers: a
//! session may observe a mixed view, but such a session can never commit after
//! another writer moves the fence.
//!
//! Once commit starts, dropping its Rust future does not cancel the IndexedDB
//! transaction. The browser operation continues detached because abandoning
//! the future cannot synchronously roll back work already submitted to
//! IndexedDB.
//!
//! A reader handle uses a fresh readonly transaction for each page. Every page
//! is internally consistent, but changes committed between pages can appear in
//! or disappear from the overall scan. Callers that require a stable logical
//! read must use their own revision or application-level validation.
//!
//! # Database identity
//!
//! The caller always supplies the database name; there is no default. IndexedDB
//! names are scoped to a browser storage key (normally the origin), so the same
//! name under a different storage key denotes a different database. One backend
//! instance uses one such named database.
//!
//! # Tests and native builds
//!
//! Browser tests run with `wasm-pack test --headless --chrome` or
//! `wasm-pack test --headless --firefox`. Node does not provide IndexedDB, so
//! `wasm-pack test --node` is unsupported. Native targets compile this
//! documentation, [`IndexedDbError`], [`Revision`], and the pure
//! buffered-overlay implementation; browser API code stays behind the exact
//! `wasm32-unknown-unknown` predicate.

mod error;
// The browser I/O layer arrives in later commits. Keep its pure, crate-local
// dependency compiled on every target in the meantime.
#[allow(dead_code)]
pub(crate) mod overlay;
mod revision;

pub use error::IndexedDbError;
pub use revision::Revision;
