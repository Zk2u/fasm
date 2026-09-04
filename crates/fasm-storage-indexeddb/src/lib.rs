//! Browser [`KvStore`](fasm_storage::KvStore) storage backed by IndexedDB.
//!
//! IndexedDB stores the composite `flatdir` rows (`prefix ‖ key`) as binary
//! keys. Its bytewise ordering of those rows preserves the caller's key order
//! within one resolved directory.
//!
//! # Directories and snapshots
//!
//! The backend reuses [`fasm_storage::FlatEngine`] over an owned byte map.
//! Directory metadata, the allocator, and data prefixes therefore use the same
//! FoundationDB DirectoryLayer layout as btreemap and redb, without a second
//! asynchronous implementation of those algorithms.
//!
//! `store.transaction().await?` reads all raw rows and the revision fence in
//! **one readonly IndexedDB transaction** before returning a session. It keeps
//! that snapshot in Rust and applies local writes to it synchronously. Reads
//! and directory navigation remain stable across other tabs' commits, while
//! observing the session's own pending writes. No browser transaction stays
//! open across arbitrary application awaits.
//!
//! `store.reader().await?` also captures a full snapshot. All reads and scans
//! through that handle stay on the captured state; acquire a new reader to see
//! later commits. Closing the shared connection invalidates existing handles.
//!
//! # Commit and rollback
//!
//! A separate write buffer records changed keys and tombstones. Commit writes
//! only this delta in one readwrite transaction over `kv` and `meta`; it never
//! replaces the entire database. The revision captured with the snapshot must
//! still match. Otherwise [`IndexedDbError::Conflict`] rejects the complete
//! write set, including directory allocations, and the caller must retry from
//! a fresh session. All writers must use this protocol: raw external writes
//! that bypass the revision fence are unsupported.
//!
//! Empty sessions validate the fence without advancing it. Concurrent commits
//! to unrelated directories also invalidate the session; the fence covers the
//! whole named database. Dropping an uncommitted session discards its changes.
//! Once a commit future has been polled, the browser operation is detached so
//! dropping the future cannot leave partially queued work to auto-commit.
//!
//! Write commits request strict durability. `Ok(())` confirms browser commit
//! and, where the browser honours that hint, a durable flush. The existing
//! error contract distinguishes retryable rollbacks from unknown outcomes.
//!
//! # Memory and range scans
//!
//! Opening each session or reader reads the **entire database**, including
//! directory metadata, and holds its own copy until dropped. Capture also
//! temporarily holds JavaScript cursor results before conversion into Rust.
//! Memory and startup I/O therefore grow with total stored bytes, even for a
//! single-key operation; this design targets bounded client state.
//!
//! Range scans materialize the selected rows from the in-memory snapshot in
//! either direction, then expose them through [`fasm_storage::KvStream`].
//! `take(n)` limits consumption, not the initial database read or range
//! materialization. Scans perform no further IndexedDB I/O and cannot mix
//! revisions between continuations.
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
mod snapshot_tests;
#[cfg(all(test, all(target_arch = "wasm32", target_os = "unknown")))]
mod store_tests;
