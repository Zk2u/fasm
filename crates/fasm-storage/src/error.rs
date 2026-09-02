//! The retryability contract every storage error must answer.

/// Error contract for storage failures that can be safely retried by discarding
/// the current logical attempt and rerunning it from the start.
///
/// "Retryable" here is a statement about the *whole* logical operation, not
/// about a single round-trip. A transactional backend that rejects a commit
/// because of a read/write conflict is retryable: nothing was written, and
/// rerunning the state transition from its beginning is sound. A serialization
/// failure, a key-schema violation, or a prefix violation is not: the input is
/// wrong and will be just as wrong the second time.
///
/// # The snapshot-before-erasure rule
///
/// Retryability lives on the *concrete* error value. Any wrapper that
/// type-erases a storage error — boxing it as `dyn Error`, rendering it to a
/// `String`, folding it into a variant that no longer names the backend type —
/// **must** call [`is_retryable`] on the concrete error *before* erasing it and
/// carry the resulting `bool` alongside the erased value:
///
/// ```
/// use std::error::Error;
///
/// use fasm_storage::{Commit, RetryableStorageError};
///
/// enum ClientError {
///     Storage {
///         /// Snapshotted *before* the source was boxed.
///         retryable: bool,
///         source: Box<dyn Error + Send + Sync>,
///     },
/// }
///
/// impl ClientError {
///     fn storage(err: impl Error + RetryableStorageError + Send + Sync + 'static) -> Self {
///         Self::Storage { retryable: err.is_retryable(), source: Box::new(err) }
///     }
/// }
///
/// impl RetryableStorageError for ClientError {
///     fn is_retryable(&self) -> bool {
///         match self {
///             Self::Storage { retryable, .. } => *retryable,
///         }
///     }
/// }
///
/// async fn commit<C: Commit>(session: C) -> Result<(), ClientError> {
///     session.commit().await.map_err(ClientError::storage)
/// }
/// ```
///
/// Once the type is gone the answer is unrecoverable, and the tempting default
/// is exactly the wrong one in both directions: reporting `false` silently
/// converts a transient commit conflict into a permanent failure, while
/// reporting `true` retries operations that can never succeed.
///
/// [`is_retryable`]: RetryableStorageError::is_retryable
pub trait RetryableStorageError {
    /// Returns `true` when the caller may safely rerun the entire logical
    /// operation from the beginning.
    ///
    /// Implementations must be conservative: return `false` whenever it is not
    /// certain that the attempt left no partial effect behind.
    fn is_retryable(&self) -> bool;
}
