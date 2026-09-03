//! Byte-keyspace helpers: prefix successors, prefix ranges, bound conversion.
//!
//! Everything here is pure arithmetic on the lexicographic byte ordering that
//! [`KvStore`](crate::KvStore) mandates. Backend implementors need it to map
//! bounds; key-schema authors need it to turn a prefix into a scan range.

use core::ops::Bound;

/// Whether a range is empty solely from its bounds.
///
/// Every [`KvStore`](crate::KvStore) backend must normalize these ranges to an
/// empty result before calling an engine whose native range API rejects them.
/// A range is empty when both bounds are keyed and the start sorts after the
/// end, or when equal keyed bounds exclude that one possible key.
///
/// ```
/// use std::ops::Bound;
///
/// use fasm_storage::is_empty_range;
///
/// assert!(is_empty_range(
///     &Bound::Included(b"z"),
///     &Bound::Excluded(b"a"),
/// ));
/// assert!(is_empty_range(
///     &Bound::Excluded(b"k"),
///     &Bound::Included(b"k"),
/// ));
/// assert!(!is_empty_range(
///     &Bound::Included(b"k"),
///     &Bound::Included(b"k"),
/// ));
/// ```
pub fn is_empty_range(start: &Bound<&[u8]>, end: &Bound<&[u8]>) -> bool {
    match (start, end) {
        (
            Bound::Included(start_key) | Bound::Excluded(start_key),
            Bound::Included(end_key) | Bound::Excluded(end_key),
        ) => {
            start_key > end_key
                || (start_key == end_key
                    && (matches!(start, Bound::Excluded(_)) || matches!(end, Bound::Excluded(_))))
        }
        _ => false,
    }
}

/// Returns the shortest byte string that sorts strictly after every key
/// beginning with `prefix`, or `None` when no such string exists.
///
/// This is the "successor" of a prefix: incrementing the last byte below `0xFF`
/// and truncating everything after it. `Excluded(next_prefix(p))` is therefore
/// the exact upper bound of the `p` namespace.
///
/// # The all-`0xFF` case
///
/// A prefix consisting entirely of `0xFF` bytes has **no** successor — every
/// byte is already at its maximum, so there is nothing to carry into. `None`
/// means *unbounded*: the namespace runs to the end of the keyspace, and the
/// caller must use [`Bound::Unbounded`] rather than inventing a sentinel. An
/// implementation that "solves" this by appending `0xFF` bytes is wrong: it
/// would exclude keys such as `[0xFF, 0xFF, 0xFF]` from the `[0xFF, 0xFF]`
/// namespace.
///
/// The empty prefix is the degenerate case of the same rule: it matches the
/// entire keyspace, so it has no successor either.
///
/// ```
/// use fasm_storage::next_prefix;
///
/// assert_eq!(next_prefix(&[0x10, 0x20]), Some(vec![0x10, 0x21]));
/// // The carry truncates: everything after the incremented byte is dropped.
/// assert_eq!(next_prefix(&[0x10, 0xFF]), Some(vec![0x11]));
/// // No successor exists.
/// assert_eq!(next_prefix(&[0xFF, 0xFF]), None);
/// assert_eq!(next_prefix(&[]), None);
/// ```
pub fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next = prefix.to_vec();
    for i in (0..next.len()).rev() {
        if next[i] != 0xFF {
            next[i] += 1;
            next.truncate(i + 1);
            return Some(next);
        }
    }
    None
}

/// Returns the bound pair that matches exactly the keys carrying `prefix`.
///
/// The upper bound is `Excluded(next_prefix(prefix))`, falling back to
/// [`Bound::Unbounded`] when the prefix has no successor (see
/// [`next_prefix`]).
///
/// ```
/// use std::ops::Bound;
///
/// use fasm_storage::prefix_range;
///
/// let (start, end) = prefix_range(&[0x10, 0x20]);
/// assert_eq!(start, Bound::Included(vec![0x10, 0x20]));
/// assert_eq!(end, Bound::Excluded(vec![0x10, 0x21]));
///
/// let (_, end) = prefix_range(&[0xFF, 0xFF]);
/// assert_eq!(end, Bound::Unbounded);
/// ```
pub fn prefix_range(prefix: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start = Bound::Included(prefix.to_vec());
    let end = match next_prefix(prefix) {
        Some(next) => Bound::Excluded(next),
        None => Bound::Unbounded,
    };
    (start, end)
}

/// Copy a borrowed bound into an owned one.
///
/// Backends that hand bounds to an owning collection (a `BTreeMap` range, a
/// query builder) need this because [`KvStore::range`](crate::KvStore::range)
/// borrows its bounds for the call only.
pub fn bound_to_owned(bound: Bound<&[u8]>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Included(bytes) => Bound::Included(bytes.to_vec()),
        Bound::Excluded(bytes) => Bound::Excluded(bytes.to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Borrow an owned bound as a slice bound.
///
/// The inverse of [`bound_to_owned`], for handing a computed bound back down to
/// an inner [`KvStore`](crate::KvStore).
pub fn bound_as_slice(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(bytes) => Bound::Included(bytes.as_slice()),
        Bound::Excluded(bytes) => Bound::Excluded(bytes.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    use super::{bound_as_slice, bound_to_owned};
    use super::{is_empty_range, next_prefix, prefix_range};
    use core::ops::Bound;

    // `proptest` reaches `wait-timeout`, which does not support browser wasm.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    use proptest::prelude::*;

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    use crate::tests::{arb_key, bounds_contain};

    #[test]
    fn next_prefix_increments_last_non_ff_byte() {
        assert_eq!(next_prefix(&[0x00]), Some(vec![0x01]));
        assert_eq!(next_prefix(&[0x10, 0x20]), Some(vec![0x10, 0x21]));
        assert_eq!(next_prefix(&[0xFE]), Some(vec![0xFF]));
    }

    #[test]
    fn next_prefix_carries_and_truncates() {
        assert_eq!(next_prefix(&[0x10, 0xFF]), Some(vec![0x11]));
        assert_eq!(next_prefix(&[0x10, 0xFF, 0xFF]), Some(vec![0x11]));
        assert_eq!(next_prefix(&[0x00, 0xFF, 0xFF]), Some(vec![0x01]));
    }

    #[test]
    fn next_prefix_has_no_successor_for_all_ff_or_empty() {
        assert_eq!(next_prefix(&[0xFF]), None);
        assert_eq!(next_prefix(&[0xFF, 0xFF, 0xFF]), None);
        assert_eq!(next_prefix(&[]), None);
    }

    #[test]
    fn prefix_range_bounds_the_namespace() {
        let (start, end) = prefix_range(&[0x10, 0x20]);
        assert_eq!(start, Bound::Included(vec![0x10, 0x20]));
        assert_eq!(end, Bound::Excluded(vec![0x10, 0x21]));
    }

    #[test]
    fn prefix_range_is_unbounded_above_for_all_ff() {
        let (start, end) = prefix_range(&[0xFF, 0xFF]);
        assert_eq!(start, Bound::Included(vec![0xFF, 0xFF]));
        assert_eq!(end, Bound::Unbounded);

        // The `Unbounded` answer is the correct one: longer all-0xFF keys are
        // inside the namespace and must not be excluded.
        let inside: &[u8] = &[0xFF, 0xFF, 0xFF];
        assert!(inside.starts_with(&[0xFF, 0xFF]));
    }

    #[test]
    fn empty_range_covers_inverted_and_equal_exclusive_bounds() {
        assert!(is_empty_range(
            &Bound::Included(b"z"),
            &Bound::Excluded(b"a")
        ));
        assert!(is_empty_range(
            &Bound::Excluded(b"z"),
            &Bound::Included(b"a")
        ));
        assert!(is_empty_range(
            &Bound::Excluded(b"k"),
            &Bound::Excluded(b"k")
        ));
        assert!(is_empty_range(
            &Bound::Included(b"k"),
            &Bound::Excluded(b"k")
        ));
        assert!(is_empty_range(
            &Bound::Excluded(b"k"),
            &Bound::Included(b"k")
        ));

        assert!(!is_empty_range(
            &Bound::Included(b"k"),
            &Bound::Included(b"k")
        ));
        assert!(!is_empty_range(&Bound::Unbounded, &Bound::Excluded(b"k")));
        assert!(!is_empty_range(&Bound::Included(b"k"), &Bound::Unbounded));
    }

    /// Arbitrary owned bound over [`arb_key`] bytes.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn arb_bound() -> impl Strategy<Value = Bound<Vec<u8>>> {
        prop_oneof![
            Just(Bound::Unbounded),
            arb_key().prop_map(Bound::Included),
            arb_key().prop_map(Bound::Excluded),
        ]
    }

    /// A key drawn from the *neighbourhood* of `prefix`.
    ///
    /// An independently drawn key essentially never lands on the boundary a
    /// prefix range is defined by, so a bound that is off by one key would go
    /// unnoticed. The keys that matter are the prefix's own extensions, the
    /// first key past it (some byte carried, everything after it dropped — the
    /// shape a successor has), and its truncations. Those are generated
    /// deliberately, with unrelated keys mixed in.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn arb_key_near(prefix: Vec<u8>) -> impl Strategy<Value = Vec<u8>> {
        let extension = (Just(prefix.clone()), arb_key()).prop_map(|(mut key, tail)| {
            key.extend_from_slice(&tail);
            key
        });
        let successor = (
            Just(prefix.clone()),
            arb_key(),
            any::<prop::sample::Index>(),
        )
            .prop_map(|(prefix, tail, index)| {
                if prefix.is_empty() {
                    return tail;
                }
                let at = index.index(prefix.len());
                let mut key = prefix[..=at].to_vec();
                key[at] = key[at].wrapping_add(1);
                key.extend_from_slice(&tail);
                key
            });
        let truncation =
            (Just(prefix), any::<prop::sample::Index>()).prop_map(|(prefix, index)| {
                if prefix.is_empty() {
                    return prefix;
                }
                prefix[..index.index(prefix.len())].to_vec()
            });

        prop_oneof![
            3 => extension,
            3 => successor,
            2 => truncation,
            2 => arb_key(),
        ]
    }

    /// A prefix together with a key from its neighbourhood.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn arb_prefix_and_key() -> impl Strategy<Value = (Vec<u8>, Vec<u8>)> {
        arb_key().prop_flat_map(|prefix| (Just(prefix.clone()), arb_key_near(prefix)))
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    proptest! {
        /// The defining property of [`prefix_range`]: its bounds select exactly
        /// the keys carrying the prefix, no more and no less. Every key schema
        /// in the SDK turns a prefix into a scan this way, so an off-by-one here
        /// would either hide rows or leak a neighbouring namespace's.
        #[test]
        fn prop_prefix_range_selects_exactly_the_prefixed_keys(
            (prefix, key) in arb_prefix_and_key(),
        ) {
            prop_assert_eq!(
                key.starts_with(&prefix),
                bounds_contain(&prefix_range(&prefix), &key),
            );
        }

        /// The same law stated where it bites: an arbitrary *extension* of the
        /// prefix is always inside the range. Drawing the key independently
        /// above rarely produces one.
        #[test]
        fn prop_prefix_range_contains_every_extension(prefix in arb_key(), tail in arb_key()) {
            let mut key = prefix.clone();
            key.extend_from_slice(&tail);
            prop_assert!(bounds_contain(&prefix_range(&prefix), &key));
        }

        /// `next_prefix` is a *strict* upper bound: every key beginning with the
        /// prefix sorts before it. An implementation that appended `0xFF` bytes
        /// instead of carrying would fail this for long all-`0xFF` extensions.
        #[test]
        fn prop_next_prefix_strictly_exceeds_every_extension(
            prefix in arb_key(),
            tail in arb_key(),
        ) {
            let Some(next) = next_prefix(&prefix) else {
                return Ok(());
            };
            let mut key = prefix.clone();
            key.extend_from_slice(&tail);
            prop_assert!(
                key.as_slice() < next.as_slice(),
                "{:02x?} must sort before {:02x?}",
                key,
                next
            );
        }

        /// There is no successor exactly when every byte is already at its
        /// maximum — which the empty prefix satisfies vacuously.
        #[test]
        fn prop_next_prefix_is_none_exactly_for_all_ff(prefix in arb_key()) {
            prop_assert_eq!(
                next_prefix(&prefix).is_none(),
                prefix.iter().all(|byte| *byte == 0xFF),
            );
        }

        #[test]
        fn prop_bound_conversions_round_trip(bound in arb_bound()) {
            prop_assert_eq!(bound_to_owned(bound_as_slice(&bound)), bound);
        }
    }
}
