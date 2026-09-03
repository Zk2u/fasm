//! Pure write-buffer and cursor-window logic for the IndexedDB backend.
//!
//! IndexedDB access is deliberately absent from this module. Browser cursors
//! supply committed pages; this module overlays the session's buffered sets
//! and tombstones and performs the bound arithmetic that joins those pages
//! without gaps. Keeping that boundary pure makes the transaction semantics
//! property-testable on native targets, where no IndexedDB implementation is
//! available.

use std::{collections::BTreeMap, ops::Bound};

use fasm_storage::{bound_to_owned, is_empty_range};

/// A session's pending writes, ordered exactly as raw IndexedDB byte keys are
/// ordered.
///
/// A present value is a buffered set. `None` is a tombstone that must continue
/// to shadow any committed value until the complete buffer is committed or
/// discarded.
#[derive(Clone)]
pub(crate) struct WriteBuffer {
    entries: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

/// The answer supplied by the write buffer for a point read.
///
/// [`Lookup::Miss`] is distinct from [`Lookup::Tombstone`]: only a miss allows
/// the browser layer to fall back to the committed object store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lookup<'a> {
    /// The session has set this value.
    Set(&'a [u8]),
    /// The session has deleted this key.
    Tombstone,
    /// The session has not touched this key.
    Miss,
}

impl WriteBuffer {
    /// Creates an empty session buffer.
    pub(crate) fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Buffers a value, replacing either an earlier value or a tombstone.
    pub(crate) fn set(&mut self, key: &[u8], value: &[u8]) {
        self.entries.insert(key.to_vec(), Some(value.to_vec()));
    }

    /// Buffers a tombstone, replacing any earlier value for the key.
    pub(crate) fn delete(&mut self, key: &[u8]) {
        self.entries.insert(key.to_vec(), None);
    }

    /// Looks up a key without consulting the committed object store.
    pub(crate) fn lookup(&self, key: &[u8]) -> Lookup<'_> {
        match self.entries.get(key) {
            Some(Some(value)) => Lookup::Set(value),
            Some(None) => Lookup::Tombstone,
            None => Lookup::Miss,
        }
    }

    /// Tombstones a range in the session view.
    ///
    /// `committed_keys_in_range` comes from the browser cursor and is already
    /// restricted to the requested range. Every such committed key needs an
    /// explicit tombstone so commit removes it. Buffered keys are selected
    /// here as well: this makes a clear after a set hide that set, while a set
    /// performed after the clear can deliberately reinsert the key.
    pub(crate) fn tombstone_keys(
        &mut self,
        committed_keys_in_range: impl IntoIterator<Item = Vec<u8>>,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) {
        for key in committed_keys_in_range {
            self.entries.insert(key, None);
        }

        let buffered_keys = self
            .entries
            .keys()
            .filter(|key| key_in_range(key, start, end))
            .cloned()
            .collect::<Vec<_>>();
        for key in buffered_keys {
            self.entries.insert(key, None);
        }
    }

    /// Whether the session has no pending sets or tombstones.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of pending keys, counting tombstones.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Consumes the buffer into the deterministic key order used by commit.
    pub(crate) fn drain_ops(self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.entries.into_iter().collect()
    }
}

/// Merges one committed cursor page with the buffered session view.
///
/// `committed` must already be ordered in the requested direction and every
/// pair must belong to `window`. Buffered values replace committed values,
/// tombstones suppress them, and buffered-only keys are inserted in bytewise
/// order. The ordering precondition is a debug assertion because malformed
/// browser input should aid development without introducing a new release
/// panic path.
///
/// A full committed page can merge to an empty result when every row is
/// tombstoned. That empty page must **not** end the stream: the cursor caller
/// must advance with [`next_bounds`] until a visible row appears or the
/// committed cursor is exhausted.
pub(crate) fn merge_page(
    buffer: &WriteBuffer,
    committed: Vec<(Vec<u8>, Vec<u8>)>,
    window: (Bound<&[u8]>, Bound<&[u8]>),
    reverse: bool,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    debug_assert!(committed.windows(2).all(|pair| {
        if reverse {
            pair[0].0 >= pair[1].0
        } else {
            pair[0].0 <= pair[1].0
        }
    }));

    let mut visible = committed.into_iter().collect::<BTreeMap<_, _>>();
    for (key, value) in &buffer.entries {
        if !key_in_range(key, window.0, window.1) {
            continue;
        }
        match value {
            Some(value) => {
                visible.insert(key.clone(), value.clone());
            }
            None => {
                visible.remove(key);
            }
        }
    }

    if reverse {
        visible.into_iter().rev().collect()
    } else {
        visible.into_iter().collect()
    }
}

/// Returns the exact key window represented by one committed cursor page.
///
/// A forward full page covers the current lower bound through its last
/// committed key; a reverse full page covers that last key through the current
/// upper bound. Including the seam key lets a buffered overwrite or tombstone
/// at that key participate in this page. When the cursor is exhausted, the
/// window instead extends to the caller's terminal bound so buffered-only keys
/// after the last committed row are still emitted. Consequently the first
/// window begins at the caller's bound, not at the first committed row.
pub(crate) fn page_window(
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    last_committed: Option<&[u8]>,
    exhausted: bool,
    reverse: bool,
) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    if exhausted {
        return (bound_to_owned(lower), bound_to_owned(upper));
    }

    debug_assert!(last_committed.is_some());
    match (reverse, last_committed) {
        (false, Some(last)) => (bound_to_owned(lower), Bound::Included(last.to_vec())),
        (true, Some(last)) => (Bound::Included(last.to_vec()), bound_to_owned(upper)),
        (_, None) => (bound_to_owned(lower), bound_to_owned(upper)),
    }
}

/// Advances the committed cursor bounds past the seam of a full page.
///
/// The seam is excluded from the next page because [`page_window`] assigns it
/// to the page that just completed. Forward scans advance the lower bound;
/// reverse scans retreat the upper bound. The opposite, caller-supplied bound
/// remains unchanged.
pub(crate) fn next_bounds(
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    last_committed: &[u8],
    reverse: bool,
) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    if reverse {
        (
            bound_to_owned(lower),
            Bound::Excluded(last_committed.to_vec()),
        )
    } else {
        (
            Bound::Excluded(last_committed.to_vec()),
            bound_to_owned(upper),
        )
    }
}

/// Whether `key` lies inside the caller's bytewise range.
///
/// Empty and inverted ranges are normalized before comparing either endpoint,
/// matching the storage trait's cross-backend range contract.
pub(crate) fn key_in_range(key: &[u8], start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    if is_empty_range(&start, &end) {
        return false;
    }

    let above_start = match start {
        Bound::Included(start) => key >= start,
        Bound::Excluded(start) => key > start,
        Bound::Unbounded => true,
    };
    let below_end = match end {
        Bound::Included(end) => key <= end,
        Bound::Excluded(end) => key < end,
        Bound::Unbounded => true,
    };
    above_start && below_end
}

#[cfg(all(test, not(all(target_arch = "wasm32", target_os = "unknown"))))]
mod tests {
    use std::{collections::BTreeMap, ops::Bound};

    use fasm_storage::{bound_as_slice, bound_to_owned};
    use proptest::{
        collection::{btree_map, vec},
        prelude::*,
        test_runner::{Config as ProptestConfig, TestCaseResult},
    };

    use super::{Lookup, WriteBuffer, key_in_range, merge_page, next_bounds, page_window};

    type Oracle = BTreeMap<Vec<u8>, Vec<u8>>;

    #[derive(Debug, Clone)]
    enum Op {
        Set(Vec<u8>, Vec<u8>),
        Delete(Vec<u8>),
        ClearRange(Bound<Vec<u8>>, Bound<Vec<u8>>),
        Get(Vec<u8>),
        Exists(Vec<u8>),
        Range(Bound<Vec<u8>>, Bound<Vec<u8>>, bool),
    }

    fn arb_key() -> impl Strategy<Value = Vec<u8>> {
        vec(
            prop_oneof![
                Just(0x00_u8),
                Just(0x01_u8),
                Just(0x7f_u8),
                Just(0xfe_u8),
                Just(0xff_u8),
            ],
            0..4,
        )
    }

    fn arb_value() -> impl Strategy<Value = Vec<u8>> {
        vec(any::<u8>(), 0..5)
    }

    fn arb_bounds() -> impl Strategy<Value = (Bound<Vec<u8>>, Bound<Vec<u8>>)> {
        (arb_key(), arb_key(), 0_u8..3, 0_u8..3).prop_map(|(start, end, start_kind, end_kind)| {
            let start = match start_kind {
                0 => Bound::Unbounded,
                1 => Bound::Included(start),
                _ => Bound::Excluded(start),
            };
            let end = match end_kind {
                0 => Bound::Unbounded,
                1 => Bound::Included(end),
                _ => Bound::Excluded(end),
            };
            (start, end)
        })
    }

    fn arb_op() -> impl Strategy<Value = Op> {
        prop_oneof![
            4 => (arb_key(), arb_value()).prop_map(|(key, value)| Op::Set(key, value)),
            2 => arb_key().prop_map(Op::Delete),
            1 => arb_bounds().prop_map(|(start, end)| Op::ClearRange(start, end)),
            1 => arb_key().prop_map(Op::Get),
            1 => arb_key().prop_map(Op::Exists),
            2 => (arb_bounds(), any::<bool>())
                .prop_map(|((start, end), reverse)| Op::Range(start, end, reverse)),
        ]
    }

    fn overlay_get(buffer: &WriteBuffer, committed: &Oracle, key: &[u8]) -> Option<Vec<u8>> {
        match buffer.lookup(key) {
            Lookup::Set(value) => Some(value.to_vec()),
            Lookup::Tombstone => None,
            Lookup::Miss => committed.get(key).cloned(),
        }
    }

    fn oracle_range(
        oracle: &Oracle,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut pairs = oracle
            .iter()
            .filter(|(key, _)| key_in_range(key, start, end))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        if reverse {
            pairs.reverse();
        }
        pairs
    }

    /// Simulates the browser cursor loop, including its extra exhausted page
    /// when the final committed page exactly fills the requested page size.
    fn scan_overlay(
        buffer: &WriteBuffer,
        committed: &Oracle,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        reverse: bool,
        page_size: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut bounds = (bound_to_owned(lower), bound_to_owned(upper));
        let mut result = Vec::new();

        loop {
            let mut remaining = committed
                .iter()
                .filter(|(key, _)| {
                    key_in_range(key, bound_as_slice(&bounds.0), bound_as_slice(&bounds.1))
                })
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>();
            if reverse {
                remaining.reverse();
            }

            let exhausted = remaining.len() < page_size;
            let committed_page = remaining.into_iter().take(page_size).collect::<Vec<_>>();
            let last_committed = committed_page.last().map(|(key, _)| key.clone());
            let window = page_window(
                bound_as_slice(&bounds.0),
                bound_as_slice(&bounds.1),
                last_committed.as_deref(),
                exhausted,
                reverse,
            );
            result.extend(merge_page(
                buffer,
                committed_page,
                (bound_as_slice(&window.0), bound_as_slice(&window.1)),
                reverse,
            ));

            if exhausted {
                break;
            }

            let last = last_committed.expect("a full cursor page must contain a row");
            bounds = next_bounds(
                bound_as_slice(&bounds.0),
                bound_as_slice(&bounds.1),
                &last,
                reverse,
            );
        }

        result
    }

    fn assert_view_matches(
        buffer: &WriteBuffer,
        committed: &Oracle,
        session: &Oracle,
        page_size: usize,
    ) -> TestCaseResult {
        let mut candidate_keys = committed.keys().cloned().collect::<Vec<_>>();
        candidate_keys.extend(session.keys().cloned());
        candidate_keys.extend(buffer.entries.keys().cloned());
        candidate_keys.sort();
        candidate_keys.dedup();

        for key in candidate_keys {
            let actual = overlay_get(buffer, committed, &key);
            prop_assert_eq!(actual.as_ref(), session.get(&key));
            prop_assert_eq!(actual.is_some(), session.contains_key(&key));
        }

        for reverse in [false, true] {
            prop_assert_eq!(
                scan_overlay(
                    buffer,
                    committed,
                    Bound::Unbounded,
                    Bound::Unbounded,
                    reverse,
                    page_size,
                ),
                oracle_range(session, Bound::Unbounded, Bound::Unbounded, reverse,),
            );
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// The pure overlay must remain equivalent to a materialized session
        /// view across point reads, range reads, clears, and final commit.
        #[test]
        fn prop_write_buffer_tracks_a_btreemap_session(
            committed in btree_map(arb_key(), arb_value(), 1..12),
            ops in vec(arb_op(), 0..41),
            page_size in 1_usize..=4,
        ) {
            let mut buffer = WriteBuffer::new();
            let mut session = committed.clone();

            for op in ops {
                match op {
                    Op::Set(key, value) => {
                        buffer.set(&key, &value);
                        session.insert(key, value);
                    }
                    Op::Delete(key) => {
                        buffer.delete(&key);
                        session.remove(&key);
                    }
                    Op::ClearRange(start, end) => {
                        let committed_keys = committed
                            .keys()
                            .filter(|key| {
                                key_in_range(
                                    key,
                                    bound_as_slice(&start),
                                    bound_as_slice(&end),
                                )
                            })
                            .cloned()
                            .collect::<Vec<_>>();
                        buffer.tombstone_keys(
                            committed_keys,
                            bound_as_slice(&start),
                            bound_as_slice(&end),
                        );
                        session.retain(|key, _| {
                            !key_in_range(
                                key,
                                bound_as_slice(&start),
                                bound_as_slice(&end),
                            )
                        });
                    }
                    Op::Get(key) => {
                        let actual = overlay_get(&buffer, &committed, &key);
                        prop_assert_eq!(actual.as_ref(), session.get(&key));
                    }
                    Op::Exists(key) => {
                        prop_assert_eq!(
                            overlay_get(&buffer, &committed, &key).is_some(),
                            session.contains_key(&key),
                        );
                    }
                    Op::Range(start, end, reverse) => {
                        prop_assert_eq!(
                            scan_overlay(
                                &buffer,
                                &committed,
                                bound_as_slice(&start),
                                bound_as_slice(&end),
                                reverse,
                                page_size,
                            ),
                            oracle_range(
                                &session,
                                bound_as_slice(&start),
                                bound_as_slice(&end),
                                reverse,
                            ),
                        );
                    }
                }
                assert_view_matches(&buffer, &committed, &session, page_size)?;
            }

            let mut committed_after_apply = committed.clone();
            for (key, value) in buffer.drain_ops() {
                match value {
                    Some(value) => {
                        committed_after_apply.insert(key, value);
                    }
                    None => {
                        committed_after_apply.remove(&key);
                    }
                }
            }
            prop_assert_eq!(committed_after_apply, session);
        }
    }

    #[test]
    fn merge_pages_places_buffered_keys_across_every_seam_in_both_directions() {
        let committed = Oracle::from([
            (b"b".to_vec(), b"base-b".to_vec()),
            (b"d".to_vec(), b"base-d".to_vec()),
            (b"f".to_vec(), b"base-f".to_vec()),
        ]);
        let mut buffer = WriteBuffer::new();
        for key in [b"a", b"c", b"e", b"g"] {
            buffer.set(key, key);
        }

        let expected = Oracle::from([
            (b"a".to_vec(), b"a".to_vec()),
            (b"b".to_vec(), b"base-b".to_vec()),
            (b"c".to_vec(), b"c".to_vec()),
            (b"d".to_vec(), b"base-d".to_vec()),
            (b"e".to_vec(), b"e".to_vec()),
            (b"f".to_vec(), b"base-f".to_vec()),
            (b"g".to_vec(), b"g".to_vec()),
        ]);
        for reverse in [false, true] {
            assert_eq!(
                scan_overlay(
                    &buffer,
                    &committed,
                    Bound::Unbounded,
                    Bound::Unbounded,
                    reverse,
                    1,
                ),
                oracle_range(&expected, Bound::Unbounded, Bound::Unbounded, reverse,),
            );
        }
    }

    #[test]
    fn seam_overwrites_and_tombstones_apply_on_the_completed_page() {
        let committed = Oracle::from([
            (b"a".to_vec(), b"base-a".to_vec()),
            (b"c".to_vec(), b"base-c".to_vec()),
            (b"e".to_vec(), b"base-e".to_vec()),
        ]);

        let mut overwritten = WriteBuffer::new();
        overwritten.set(b"c", b"buffer-c");
        for reverse in [false, true] {
            let mut expected = vec![
                (b"a".to_vec(), b"base-a".to_vec()),
                (b"c".to_vec(), b"buffer-c".to_vec()),
                (b"e".to_vec(), b"base-e".to_vec()),
            ];
            if reverse {
                expected.reverse();
            }
            assert_eq!(
                scan_overlay(
                    &overwritten,
                    &committed,
                    Bound::Unbounded,
                    Bound::Unbounded,
                    reverse,
                    2,
                ),
                expected,
            );
        }

        let mut tombstoned = WriteBuffer::new();
        tombstoned.delete(b"c");
        for reverse in [false, true] {
            let mut expected = vec![
                (b"a".to_vec(), b"base-a".to_vec()),
                (b"e".to_vec(), b"base-e".to_vec()),
            ];
            if reverse {
                expected.reverse();
            }
            assert_eq!(
                scan_overlay(
                    &tombstoned,
                    &committed,
                    Bound::Unbounded,
                    Bound::Unbounded,
                    reverse,
                    2,
                ),
                expected,
            );
        }
    }

    #[test]
    fn an_empty_merged_full_page_does_not_end_the_scan() {
        let committed = Oracle::from([
            (b"a".to_vec(), b"a".to_vec()),
            (b"b".to_vec(), b"b".to_vec()),
            (b"c".to_vec(), b"c".to_vec()),
        ]);
        let mut buffer = WriteBuffer::new();
        buffer.delete(b"a");
        buffer.delete(b"b");

        assert_eq!(
            scan_overlay(
                &buffer,
                &committed,
                Bound::Unbounded,
                Bound::Unbounded,
                false,
                2,
            ),
            vec![(b"c".to_vec(), b"c".to_vec())],
        );

        let mut reverse_buffer = WriteBuffer::new();
        reverse_buffer.delete(b"b");
        reverse_buffer.delete(b"c");
        assert_eq!(
            scan_overlay(
                &reverse_buffer,
                &committed,
                Bound::Unbounded,
                Bound::Unbounded,
                true,
                2,
            ),
            vec![(b"a".to_vec(), b"a".to_vec())],
        );
    }

    #[test]
    fn buffered_only_empty_and_extreme_keys_obey_bounds_and_direction() {
        let committed = Oracle::new();
        let mut buffer = WriteBuffer::new();
        for key in [Vec::new(), vec![0xff], vec![0xff, 0xff]] {
            buffer.set(&key, &key);
        }

        for reverse in [false, true] {
            let expected = oracle_range(
                &Oracle::from([
                    (Vec::new(), Vec::new()),
                    (vec![0xff], vec![0xff]),
                    (vec![0xff, 0xff], vec![0xff, 0xff]),
                ]),
                Bound::Unbounded,
                Bound::Unbounded,
                reverse,
            );
            assert_eq!(
                scan_overlay(
                    &buffer,
                    &committed,
                    Bound::Unbounded,
                    Bound::Unbounded,
                    reverse,
                    1,
                ),
                expected,
            );
        }

        for reverse in [false, true] {
            assert!(
                scan_overlay(
                    &buffer,
                    &committed,
                    Bound::Included(b""),
                    Bound::Excluded(b""),
                    reverse,
                    1,
                )
                .is_empty()
            );
            assert!(
                scan_overlay(
                    &buffer,
                    &committed,
                    Bound::Excluded(&[0xff]),
                    Bound::Included(&[0xff]),
                    reverse,
                    1,
                )
                .is_empty()
            );
        }
    }

    #[test]
    fn clear_after_set_hides_it_and_set_after_clear_reinserts_it() {
        let mut cleared = WriteBuffer::new();
        cleared.set(b"k", b"first");
        cleared.tombstone_keys(Vec::new(), Bound::Included(b"k"), Bound::Included(b"k"));
        assert_eq!(cleared.lookup(b"k"), Lookup::Tombstone);

        cleared.set(b"k", b"second");
        assert_eq!(cleared.lookup(b"k"), Lookup::Set(b"second"));
        assert_eq!(cleared.len(), 1);
        assert!(!cleared.is_empty());
    }

    #[test]
    fn drain_ops_is_in_key_order_and_preserves_tombstones() {
        let mut buffer = WriteBuffer::new();
        assert!(buffer.is_empty());
        buffer.set(b"z", b"last");
        buffer.delete(b"a");
        buffer.set(b"m", b"middle");

        assert_eq!(
            buffer.drain_ops(),
            vec![
                (b"a".to_vec(), None),
                (b"m".to_vec(), Some(b"middle".to_vec())),
                (b"z".to_vec(), Some(b"last".to_vec())),
            ],
        );
    }

    #[test]
    fn page_window_follows_each_direction_and_exhaustion_row() {
        let lower = Bound::Included(b"a".as_slice());
        let upper = Bound::Excluded(b"z".as_slice());

        assert_eq!(
            page_window(lower, upper, Some(b"m"), false, false),
            (
                Bound::Included(b"a".to_vec()),
                Bound::Included(b"m".to_vec()),
            ),
        );
        assert_eq!(
            page_window(lower, upper, Some(b"m"), false, true),
            (
                Bound::Included(b"m".to_vec()),
                Bound::Excluded(b"z".to_vec()),
            ),
        );
        assert_eq!(
            page_window(lower, upper, Some(b"m"), true, false),
            (
                Bound::Included(b"a".to_vec()),
                Bound::Excluded(b"z".to_vec()),
            ),
        );
        assert_eq!(
            page_window(lower, upper, Some(b"m"), true, true),
            (
                Bound::Included(b"a".to_vec()),
                Bound::Excluded(b"z".to_vec()),
            ),
        );
        assert_eq!(
            page_window(lower, upper, None, true, false),
            (
                Bound::Included(b"a".to_vec()),
                Bound::Excluded(b"z".to_vec()),
            ),
        );
    }

    #[test]
    fn next_bounds_excludes_the_completed_seam_in_each_direction() {
        let lower = Bound::Included(b"a".as_slice());
        let upper = Bound::Excluded(b"z".as_slice());

        assert_eq!(
            next_bounds(lower, upper, b"m", false),
            (
                Bound::Excluded(b"m".to_vec()),
                Bound::Excluded(b"z".to_vec()),
            ),
        );
        assert_eq!(
            next_bounds(lower, upper, b"m", true),
            (
                Bound::Included(b"a".to_vec()),
                Bound::Excluded(b"m".to_vec()),
            ),
        );
    }
}
