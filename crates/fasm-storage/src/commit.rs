//! The [`Commit`] trait: finalizing one storage session.

use core::error::Error;
use core::future::Future;

use crate::error::RetryableStorageError;
use crate::maybe_send::{MaybeSend, MaybeSync};

/// Finalizes the writes performed through a mutable storage handle.
///
/// `Commit` is deliberately **separate from [`KvStore`]**. A read-only handle
/// and a write handle can both be `KvStore`s; only the write handle is a
/// *session* that has to be closed out. Keeping the two apart lets a caller
/// accept `KV: KvStore` where it only reads, and `KV: KvStore + Commit` where
/// it owns the session, and have the compiler enforce the difference.
///
/// # The atomicity rule
///
/// **One state-transition invocation = one storage session = one atomic
/// commit.** Everything a transition writes lands together or not at all.
/// Dropping a session before invoking `commit` is the rollback. Once `commit`
/// has been invoked, however, a non-retryable error may leave the caller unable
/// to tell whether the atomic commit happened.
///
/// `commit` **consumes `self`**, which is the whole point. A session cannot be
/// committed twice, cannot be written to after commit, and cannot be
/// accidentally shared across two transitions — the type system says so rather
/// than the documentation.
///
/// # In-memory backends
///
/// The in-memory `fasm-storage-btreemap` backend provides the same buffering
/// and rollback as the durable backends: `commit` applies the buffered writes
/// to the in-process base map, and dropping the transaction rolls them back.
/// It provides no crash atomicity — the base map is memory, so a crash loses
/// it — and it belongs in tests and simulations only.
///
/// [`KvStore`]: crate::KvStore
pub trait Commit {
    /// Error type produced by the commit itself.
    ///
    /// An error for which [`RetryableStorageError::is_retryable`] is `true`
    /// guarantees that this commit attempt did not commit and is safe to replay
    /// from a fresh session. An error for which it is `false` makes no such
    /// guarantee: the outcome may be unknown, as with a lost acknowledgement
    /// after a durable commit. Callers must reconcile persisted state before
    /// replaying such an attempt.
    ///
    /// FoundationDB's `retryable_not_committed` distinction is the precedent:
    /// backends should map the equivalent guarantee onto `is_retryable`.
    type Error: Error + RetryableStorageError + MaybeSend + MaybeSync + 'static;

    /// Finalize every write performed through this handle, atomically.
    ///
    /// On `Ok(())` all writes are durable and visible. On `Err(err)`, replay is
    /// safe only when [`RetryableStorageError::is_retryable`] returns `true`,
    /// which guarantees that the attempt did not commit. A `false` result may
    /// mean the outcome is unknown; reconcile persisted state before replaying.
    ///
    /// On native targets [`MaybeSend`] has `Send` as a supertrait, so trait
    /// elaboration still gives callers a `Send` future that can cross task
    /// boundaries. In a browser it does not require `Send`, allowing a commit
    /// future to retain thread-local Web API handles.
    fn commit(self) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;
}
