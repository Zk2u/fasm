//! The error type shared by the layout machinery and the flat backends.

use std::error::Error;

use crate::error::RetryableStorageError;
use crate::key::KeyError;

/// A flat-layout store failed a structural precondition.
///
/// Every variant is non-retryable except `Engine`, which delegates to the
/// engine view's own classification; for today's backends that is also
/// non-retryable.
#[derive(Debug, thiserror::Error)]
pub enum FlatError<E> {
    /// The store's version row names a layout this build does not read
    /// (a newer major version, a read-only newer minor version, or a
    /// value that is not a 12-byte version at all).
    #[error("store content does not match this layout version")]
    Foreign,
    /// A structural row (version, HCA counter or recent, node row) is
    /// malformed, or the allocator produced a state the layout cannot
    /// have produced (a prefix already in use, a corrupt counter).
    #[error("store content is malformed")]
    Corrupt,
    /// A directory path failed validation.
    #[error(transparent)]
    Key(KeyError),
    /// The engine view failed.
    #[error("{0}")]
    Engine(#[from] E),
}

impl<E: std::fmt::Debug + PartialEq> PartialEq for FlatError<E> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Foreign, Self::Foreign) | (Self::Corrupt, Self::Corrupt) => true,
            (Self::Key(a), Self::Key(b)) => a == b,
            (Self::Engine(a), Self::Engine(b)) => a == b,
            _ => false,
        }
    }
}

impl<E: Error + RetryableStorageError> RetryableStorageError for FlatError<E> {
    fn is_retryable(&self) -> bool {
        match self {
            FlatError::Engine(e) => e.is_retryable(),
            _ => false,
        }
    }
}
