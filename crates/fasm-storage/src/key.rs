//! Directory-key validation and the owned key form.

use core::str;

use thiserror::Error;

use crate::error::RetryableStorageError;

/// The owned `(dir, key)` form, for callers that hold a key across async
/// boundaries, in contrast to the borrowed forms the store operations
/// take. `dir` must pass [`validate_dir`](crate::validate_dir); `key` is
/// arbitrary bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Key {
    /// The directory path (UTF-8 segments).
    pub dir: Vec<Vec<u8>>,
    /// The key within the directory (arbitrary bytes).
    pub key: Vec<u8>,
}

/// A directory path failed validation.
///
/// Both variants are input errors: the operation is just as wrong on a
/// rerun, so neither is retryable.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum KeyError {
    /// Segment `segment` is not valid UTF-8.
    ///
    /// Directory segments are the trait-level UTF-8 contract (FDB's layer
    /// takes `&[String]` paths; the flat backends enforce the same for
    /// uniformity). Keys are not subject to it.
    #[error("directory segment {segment} is not valid UTF-8")]
    DirSegmentNotUtf8 {
        /// The 0-based index of the offending segment.
        segment: usize,
    },
    /// A directory-removal operation was applied to the root `[]`.
    #[error("the root directory cannot be removed")]
    RootNotRemovable,
}

impl RetryableStorageError for KeyError {
    fn is_retryable(&self) -> bool {
        false
    }
}

/// Validate a directory path: every segment must be valid UTF-8.
///
/// Every backend calls this before touching its engine, so an invalid path
/// fails with one answer on every backend. An empty segment is valid (the
/// empty string is valid UTF-8), and the empty path (the root) is valid.
pub fn validate_dir(dir: &[&[u8]]) -> Result<(), KeyError> {
    for (i, seg) in dir.iter().enumerate() {
        if str::from_utf8(seg).is_err() {
            return Err(KeyError::DirSegmentNotUtf8 { segment: i });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    #[test]
    fn validate_dir_accepts_utf8_and_empty() {
        assert!(validate_dir(&[]).is_ok());
        assert!(validate_dir(&[b"", b"asset", b"btc"]).is_ok());
        // UTF-8 multi-byte content is legal.
        assert!(validate_dir(&["€".as_bytes()]).is_ok());
    }

    #[test]
    fn validate_dir_rejects_non_utf8_with_index() {
        assert_eq!(
            validate_dir(&[b"ok", b"\xFF\xFE"]),
            Err(KeyError::DirSegmentNotUtf8 { segment: 1 })
        );
        assert_eq!(
            validate_dir(&[b"\x00\xFF"]),
            Err(KeyError::DirSegmentNotUtf8 { segment: 0 })
        );
    }

    #[test]
    fn key_error_is_not_retryable() {
        assert!(!KeyError::RootNotRemovable.is_retryable());
        assert!(!KeyError::DirSegmentNotUtf8 { segment: 0 }.is_retryable());
    }

    proptest! {
        #[test]
        fn prop_validate_dir_matches_str_from_utf8(
            dir in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..8), 0..4),
        ) {
            let slices: Vec<&[u8]> = dir.iter().map(|v| v.as_slice()).collect();
            let expected: Option<usize> =
                dir.iter().position(|seg| str::from_utf8(seg).is_err());
            match (validate_dir(&slices), expected) {
                (Ok(()), None) => {}
                (Err(KeyError::DirSegmentNotUtf8 { segment }), Some(i)) => {
                    prop_assert_eq!(segment, i);
                }
                (other, _) => prop_assert!(false, "unexpected answer: {other:?}"),
            }
        }
    }
}
