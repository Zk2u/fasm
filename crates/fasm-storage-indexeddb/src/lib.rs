//! Browser [`KvStore`](fasm_storage::KvStore) storage backed by IndexedDB.
//!
//! IndexedDB stores the composite `flatdir` rows (`prefix ‖ key`) as binary
//! keys. Its bytewise ordering of those rows preserves the caller's key order
//! within one resolved directory.
//!
//! # Directories
//!
//! Directories use the same `fasm_storage::flatdir` byte layout as the
//! btreemap and redb backends: version, counter, root prefix, mapping rows, and
//! directory-prefixed data rows are identical on disk. IndexedDB reads are
//! asynchronous, so this crate drives those shared byte rules through its own
//! async directory layer instead of the synchronous `FlatEngine`. The FDB
//! backend is the other backend with a native directory-layer driver.
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
//! Empty sessions still validate that fence, but write nothing and do not
//! advance the revision. Non-empty commits request strict durability. Chrome
//! honours that hint; other browsers may ignore it and use their default
//! durability. A successful commit therefore means the transaction committed
//! and, where the browser honours the hint, was flushed durably.
//!
//! Once commit starts, dropping its Rust future does not cancel the IndexedDB
//! transaction. The browser operation continues detached because abandoning
//! the future cannot synchronously roll back work already submitted to
//! IndexedDB.
//!
//! # Range scans
//!
//! Range scans resolve one exact directory, then fetch at most 256 committed
//! rows per page from that directory's data interval, using a fresh readonly
//! transaction for every page. Each cursor window ends at its last
//! committed seam key (or at the caller's terminal bound when exhausted), and
//! buffered values and tombstones are merged only inside that window; a full
//! page hidden entirely by tombstones advances to the next page rather than
//! ending the stream.
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
//! On browser targets, `IndexedDbStore` opens and deletes those databases and
//! owns the shared connection lifecycle. It creates one-transition
//! `IndexedDbTransaction` sessions and committed-data `IndexedDbReader`
//! views. Clones of a store handle share one browser connection rather than
//! opening independent connections.
//!
//! # Tests and native builds
//!
//! Browser tests run with `wasm-pack test --headless --chrome` or
//! `wasm-pack test --headless --firefox`. Node does not provide IndexedDB, so
//! `wasm-pack test --node` is unsupported. Native targets compile this
//! documentation, [`IndexedDbError`], [`Revision`], and the pure buffered-overlay
//! implementation. `IndexedDbStore` and all browser API code stay behind the
//! exact `wasm32-unknown-unknown` predicate.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod commit;
mod error;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod flat;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod idb;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod operation;
#[cfg(any(test, all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod overlay;
mod revision;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod scan;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod session;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod store;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod traits;

pub use error::IndexedDbError;
pub use revision::Revision;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use session::{IndexedDbReader, IndexedDbTransaction};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use store::IndexedDbStore;

#[cfg(all(test, all(target_arch = "wasm32", target_os = "unknown")))]
mod commit_tests;
#[cfg(all(test, all(target_arch = "wasm32", target_os = "unknown")))]
mod conformance_tests;
#[cfg(all(test, all(target_arch = "wasm32", target_os = "unknown")))]
mod flat_tests;
#[cfg(all(test, all(target_arch = "wasm32", target_os = "unknown")))]
mod scan_tests;
#[cfg(all(test, all(target_arch = "wasm32", target_os = "unknown")))]
mod session_tests;
#[cfg(all(test, all(target_arch = "wasm32", target_os = "unknown")))]
mod store_tests;
