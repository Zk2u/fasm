//! Errors produced by the IndexedDB storage backend.

use fasm_storage::{KeyError, RetryableStorageError};
use thiserror::Error;

/// An error from the IndexedDB storage backend.
///
/// Error text may describe the browser failure or revision state, but must
/// never include key or value bytes. The variants intentionally carry no such
/// bytes; future variants must preserve that rule for both [`Debug`] and
/// [`Display`](core::fmt::Display) output.
#[derive(Debug, Error)]
pub enum IndexedDbError {
    /// The application object store contains data from another layout.
    #[error("store content does not match this layout version")]
    Foreign,

    /// A directory path failed the storage trait's validation rules.
    #[error(transparent)]
    Key(#[from] KeyError),

    /// The global environment does not expose IndexedDB.
    ///
    /// This is expected in Node, sandboxed frames without storage access, and
    /// some private-browsing modes. The caller should choose another backend or
    /// tell the user that durable browser storage is unavailable.
    #[error("IndexedDB is unavailable in this environment")]
    Unavailable,

    /// The database connection was closed and can no longer issue requests.
    ///
    /// Another tab may have requested a version change, or the browser may have
    /// closed the connection abnormally. Discard this handle and reopen the
    /// named database before starting a new logical operation.
    #[error("IndexedDB connection is closed")]
    Closed,

    /// IndexedDB rejected a write because the storage quota was exhausted.
    ///
    /// Retrying unchanged data is not useful. The caller should free storage,
    /// reduce its retained data, or ask the user to make more space available.
    #[error("IndexedDB storage quota exceeded")]
    QuotaExceeded,

    /// The revision fence changed after this session began.
    ///
    /// Another session committed first. No buffered writes from this session
    /// were applied, so the caller should retry the entire state transition
    /// from a fresh session.
    #[error("IndexedDB session conflicted with another commit")]
    Conflict,

    /// The readwrite transaction aborted before any of its writes took effect.
    ///
    /// IndexedDB rolls an aborted transaction back atomically. The caller may
    /// retry the entire logical operation; `reason` is suitable for diagnostics
    /// and contains no application key or value bytes.
    #[error("IndexedDB commit aborted: {reason}")]
    CommitAborted {
        /// A browser-provided or synthesized description of the abort.
        reason: String,
    },

    /// Persisted metadata or a JavaScript value violated the backend schema.
    ///
    /// Examples include a key or value with an unexpected JavaScript type and
    /// a missing, non-integral, or out-of-range revision. Retrying cannot repair
    /// persisted data; the caller should report or recover the database.
    #[error("IndexedDB data is corrupt: {detail}")]
    Corrupt {
        /// A schema-level explanation that omits application key/value bytes.
        detail: String,
    },

    /// A mutation was attempted through a read-only handle.
    ///
    /// This is a caller programming error. Use a writable session rather than
    /// retrying the same operation through the reader.
    #[error("write attempted on a read-only IndexedDB store")]
    ReadOnly,

    /// IndexedDB returned a DOM exception not represented by another variant.
    ///
    /// The exception name and message are retained for diagnostics. Because the
    /// backend cannot prove that an arbitrary failure is transient and rolled
    /// back, the caller must not automatically retry it.
    #[error("IndexedDB backend error ({name}): {message}")]
    Backend {
        /// The DOM exception name.
        name: String,
        /// The DOM exception message, without application key/value bytes.
        message: String,
    },
}

impl RetryableStorageError for IndexedDbError {
    /// Reports whether the whole logical operation can be safely replayed.
    ///
    /// A fence conflict happens before this session applies its writes, and an
    /// observed transaction abort means IndexedDB rolled every write back
    /// atomically. Those are the only variants with a no-partial-effect
    /// guarantee; all other failures are conservatively non-retryable.
    fn is_retryable(&self) -> bool {
        matches!(self, Self::Conflict | Self::CommitAborted { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryability_matches_atomic_rollback_guarantees() {
        let cases = [
            (IndexedDbError::Foreign, false),
            (IndexedDbError::Key(KeyError::RootNotRemovable), false),
            (IndexedDbError::Unavailable, false),
            (IndexedDbError::Closed, false),
            (IndexedDbError::QuotaExceeded, false),
            (IndexedDbError::Conflict, true),
            (
                IndexedDbError::CommitAborted {
                    reason: "transaction aborted".to_owned(),
                },
                true,
            ),
            (
                IndexedDbError::Corrupt {
                    detail: "invalid revision".to_owned(),
                },
                false,
            ),
            (IndexedDbError::ReadOnly, false),
            (
                IndexedDbError::Backend {
                    name: "UnknownError".to_owned(),
                    message: "browser failure".to_owned(),
                },
                false,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.is_retryable(), expected, "{error:?}");
        }
    }

    #[test]
    fn every_display_is_nonempty_and_does_not_suggest_a_panic() {
        let errors = [
            IndexedDbError::Foreign,
            IndexedDbError::Key(KeyError::RootNotRemovable),
            IndexedDbError::Unavailable,
            IndexedDbError::Closed,
            IndexedDbError::QuotaExceeded,
            IndexedDbError::Conflict,
            IndexedDbError::CommitAborted {
                reason: "transaction aborted".to_owned(),
            },
            IndexedDbError::Corrupt {
                detail: "invalid revision".to_owned(),
            },
            IndexedDbError::ReadOnly,
            IndexedDbError::Backend {
                name: "UnknownError".to_owned(),
                message: "browser failure".to_owned(),
            },
        ];

        for error in errors {
            let display = error.to_string();
            assert!(!display.is_empty(), "{error:?}");
            assert!(!display.contains("unwrap"), "{display}");
        }
    }
}
