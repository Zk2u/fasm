//! The JavaScript-safe revision used to fence concurrent sessions.

use crate::IndexedDbError;

/// A database revision exactly representable by a JavaScript number.
///
/// IndexedDB stores structured-clone values, and a JavaScript `Number` is the
/// simplest interoperable representation for this monotonically increasing
/// fence. A `BigInt` would be unnecessary for any practical commit count. This
/// type reproduces `Number.isSafeInteger` semantics in Rust by admitting only
/// integral values from zero through [`MAX`](Self::MAX), inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Revision(u64);

impl Revision {
    /// The initial revision of a newly created database.
    pub const ZERO: Revision = Self(0);

    /// The largest integer exactly representable by a JavaScript number.
    pub const MAX: Revision = Self((1_u64 << 53) - 1);

    /// Converts a JavaScript number into a checked revision.
    ///
    /// Non-finite, fractional, negative, and larger-than-u53 values indicate
    /// corrupt metadata rather than a recoverable browser error.
    pub fn from_f64(value: f64) -> Result<Self, IndexedDbError> {
        if !value.is_finite() {
            return Err(IndexedDbError::Corrupt {
                detail: "revision is not finite".to_owned(),
            });
        }
        if value < 0.0 {
            return Err(IndexedDbError::Corrupt {
                detail: "revision is negative".to_owned(),
            });
        }
        if value.fract() != 0.0 {
            return Err(IndexedDbError::Corrupt {
                detail: "revision is not an integer".to_owned(),
            });
        }
        if value > Self::MAX.to_f64() {
            return Err(IndexedDbError::Corrupt {
                detail: "revision exceeds the JavaScript safe-integer range".to_owned(),
            });
        }

        Ok(Self(value as u64))
    }

    /// Converts this revision to its exact JavaScript number representation.
    pub fn to_f64(self) -> f64 {
        self.0 as f64
    }

    /// Returns the following revision.
    ///
    /// Exhaustion after 2^53 commits is treated as corrupt metadata and never
    /// wraps the fence, because wrapping could let a stale session commit.
    pub fn next(self) -> Result<Self, IndexedDbError> {
        if self == Self::MAX {
            return Err(IndexedDbError::Corrupt {
                detail: "revision exhausted the JavaScript safe-integer range".to_owned(),
            });
        }

        Ok(Self(self.0 + 1))
    }

    /// Returns the revision as a native integer.
    pub fn get(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_integers_round_trip() {
        for expected in [
            Revision::ZERO,
            Revision(1),
            Revision(1_u64 << 52),
            Revision::MAX,
        ] {
            let Ok(actual) = Revision::from_f64(expected.to_f64()) else {
                panic!("a safe integer was rejected: {expected:?}");
            };
            assert_eq!(actual, expected);
            assert_eq!(actual.get(), expected.0);
        }
    }

    #[test]
    fn rejects_non_finite_values() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(matches!(
                Revision::from_f64(value),
                Err(IndexedDbError::Corrupt { .. })
            ));
        }
    }

    #[test]
    fn rejects_fractional_values() {
        assert!(matches!(
            Revision::from_f64(1.5),
            Err(IndexedDbError::Corrupt { .. })
        ));
    }

    #[test]
    fn rejects_negative_values() {
        assert!(matches!(
            Revision::from_f64(-1.0),
            Err(IndexedDbError::Corrupt { .. })
        ));
    }

    #[test]
    fn rejects_values_above_maximum() {
        assert!(matches!(
            Revision::from_f64(Revision::MAX.to_f64() + 1.0),
            Err(IndexedDbError::Corrupt { .. })
        ));
    }

    #[test]
    fn next_never_wraps() {
        assert!(matches!(Revision::ZERO.next(), Ok(revision) if revision.get() == 1));
        assert!(matches!(
            Revision::MAX.next(),
            Err(IndexedDbError::Corrupt { .. })
        ));
    }

    #[test]
    fn revisions_order_by_value() {
        assert!(Revision::ZERO < Revision(1));
        assert!(Revision(1) < Revision::MAX);
    }
}
