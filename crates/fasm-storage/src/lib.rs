//! Ordered async byte-oriented key-value storage.
//!
//! This is the lowest storage layer for FASM state machines: an ordered map
//! from byte keys to byte values, plus the two things a state machine needs on
//! top of one — continuation-based range scans and an atomic commit boundary.
//! It knows nothing about swaps, rows, or serialization; those live in the
//! crates above it.
//!
//! ## The layering
//!
//! ```text
//! state machine               typed rows, indexes, transitions
//!         ▼
//! high-level state traits     Result<Option<T>> getters over a key schema
//!         ▼
//! KvStore + Commit            ← this crate: ordered async bytes
//!         ▼
//! BTreeMapStore · redb tx · FoundationDB tx   (backends, sibling crates)
//! ```
//!
//! ## What this crate provides
//!
//! - [`KvStore`] — the backend trait: async `get`/`set`/`delete`/`exists`, a
//!   continuation-based [`range`](KvStore::range) scan, and `clear_range`. Keys
//!   are ordered lexicographically by byte value, always.
//! - [`KvStream`] — the consuming continuation a scan returns, which enables
//!   incremental backends without requiring a particular fetching strategy.
//! - [`Commit`] — a *separate*, consuming trait marking the atomic boundary. One
//!   state-transition invocation is one session is one commit.
//! - [`RetryableStorageError`] — the contract every storage error answers, so
//!   the layer above can tell "rerun the transition" from "fail closed".
//! - [`ScopedKvStore`] — prefix plumbing with fail-closed scan verification, not
//!   a capability boundary; its inner-store escape hatches are deliberate.
//! - the keyspace helpers [`next_prefix`], [`prefix_range`], [`is_empty_range`],
//!   [`bound_to_owned`], [`bound_as_slice`] — the pure bound arithmetic every
//!   backend and key schema shares.
//! - `kv_store_tests!` (feature `test-utils`) — the conformance suite every
//!   backend must pass, so key schemas can be written once against one set of
//!   ordering and bound answers.
//!
//! ## Backends
//!
//! The trait and the plumbing live here; the concrete backends live in sibling
//! crates: `fasm-storage-btreemap` (in-memory, tests and simulations only),
//! `fasm-storage-redb`, and `fasm-storage-fdb`. A browser backend is deferred:
//! it needs a `?Send` formulation and an async test mode.
//!
//! ## Example
//!
//! The shape below is what every backend — and therefore every state machine
//! on top — must provide. It is written generically over the backend so this
//! documentation does not depend on any particular one; the concrete in-memory
//! usage lives in `fasm-storage-btreemap`.
//!
//! ```
//! use std::ops::Bound;
//!
//! use fasm_storage::{Commit, KvStore, ScopedKvStore};
//!
//! /// One invocation of a state transition is one session is one commit.
//! async fn transition<S>(mut swap: ScopedKvStore<S>) -> Result<(), <S as Commit>::Error>
//! where
//!     S: KvStore + Commit,
//! {
//!     // Multi-key writes under a single session.
//!     let _ = swap.set(b"status", b"funded").await;
//!     let _ = swap.range(Bound::Unbounded, Bound::Unbounded, false).collect().await;
//!
//!     // Finalize the session atomically.
//!     swap.commit().await?;
//!     Ok(())
//! }
//! # fn main() {}
//! ```

mod commit;
mod error;
mod keyspace;
mod scoped;
mod store;
mod stream;

#[cfg(any(feature = "test-utils", test))]
mod conformance;

#[cfg(test)]
mod tests;

pub use commit::Commit;
pub use error::RetryableStorageError;
pub use keyspace::{bound_as_slice, bound_to_owned, is_empty_range, next_prefix, prefix_range};
pub use scoped::{ScopedKvError, ScopedKvStore};
pub use store::KvStore;
pub use stream::{KvPair, KvStream};

/// Re-exports for [`kv_store_tests!`]. Not part of the public API.
///
/// The macro expands at the call site, where nothing it needs is guaranteed to
/// be in scope; every path it emits goes through here so that invoking crates
/// need no imports of their own.
#[cfg(any(feature = "test-utils", test))]
#[doc(hidden)]
pub mod __private {
    pub use core::ops::Bound;
}
