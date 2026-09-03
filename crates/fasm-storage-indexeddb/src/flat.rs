//! Async driver for the shared flat directory byte layout.
//!
//! IndexedDB point reads and cursors complete asynchronously, so its raw view
//! cannot implement the synchronous [`fasm_storage::RawKv`] seam used by the
//! in-memory and redb backends. This module ports `FlatEngine`'s algorithms
//! one-for-one over [`RawAsync`] while continuing to delegate every encoded
//! key, range, and allocation rule to [`fasm_storage::flatdir`]. The resulting
//! persisted rows are therefore byte-for-byte compatible with those flat
//! backends.

use std::ops::Bound;

use fasm_storage::{
    KeyError, KvPair,
    flatdir::{
        COUNTER_KEY, LAYOUT_VERSION, ROOT_PREFIX, ROOT_PREFIX_KEY, VERSION_KEY, allocated_prefix,
        children_range, data_range, dec_seg, decode_varint, encode_varint, is_skipped,
        mapping_base, mapping_row, next_alloc,
    },
    validate_dir,
};

use crate::IndexedDbError;

/// The asynchronous raw map needed by the directory driver.
///
/// `scan_all` is used only for the small structural mapping ranges and for the
/// one-time fresh/foreign classification. Application-data range scans retain
/// the backend's lazy paged cursor in `scan.rs`.
#[allow(async_fn_in_trait)]
pub(crate) trait RawAsync {
    /// Read one raw layout or data row.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, IndexedDbError>;

    /// Materialise a raw interval for structural directory operations.
    async fn scan_all(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<Vec<KvPair>, IndexedDbError>;

    /// Buffer one raw set operation.
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), IndexedDbError>;

    /// Buffer one raw tombstone.
    fn delete(&mut self, key: &[u8]) -> Result<(), IndexedDbError>;

    /// Buffer tombstones for an entire raw interval.
    async fn clear_range(
        &mut self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), IndexedDbError>;
}

enum StoreState {
    Fresh,
    Initialised,
    Foreign,
}

/// Owned raw bounds that can outlive the caller's within-directory bounds.
pub(crate) type RawBounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);

async fn store_state<R: RawAsync + ?Sized>(raw: &R) -> Result<StoreState, IndexedDbError> {
    match raw.get(VERSION_KEY).await? {
        Some(version) if version == LAYOUT_VERSION => Ok(StoreState::Initialised),
        Some(_) => Ok(StoreState::Foreign),
        None => {
            let content = raw
                .scan_all(Bound::Unbounded, Bound::Unbounded, false)
                .await?;
            if content.is_empty() {
                Ok(StoreState::Fresh)
            } else {
                Ok(StoreState::Foreign)
            }
        }
    }
}

async fn is_initialised<R: RawAsync + ?Sized>(raw: &R) -> Result<bool, IndexedDbError> {
    match store_state(raw).await? {
        StoreState::Fresh => Ok(false),
        StoreState::Initialised => Ok(true),
        StoreState::Foreign => Err(IndexedDbError::Foreign),
    }
}

/// Lazily claim a fresh raw store for this layout.
pub(crate) async fn ensure_init<R: RawAsync + ?Sized>(raw: &mut R) -> Result<(), IndexedDbError> {
    match store_state(raw).await? {
        StoreState::Initialised => Ok(()),
        StoreState::Foreign => Err(IndexedDbError::Foreign),
        StoreState::Fresh => {
            raw.insert(VERSION_KEY, LAYOUT_VERSION)?;
            raw.insert(ROOT_PREFIX_KEY, ROOT_PREFIX)?;
            raw.insert(COUNTER_KEY, &encode_varint(1))?;
            Ok(())
        }
    }
}

fn check_prefix(prefix: &[u8]) -> Result<(), IndexedDbError> {
    if prefix.last().is_some_and(|byte| *byte == 0) {
        Ok(())
    } else {
        Err(corrupt("directory mapping contains an invalid prefix"))
    }
}

/// Resolve a directory without allocating it.
pub(crate) async fn prefix_of<R: RawAsync + ?Sized>(
    raw: &R,
    dir: &[&[u8]],
) -> Result<Option<Vec<u8>>, IndexedDbError> {
    validate_dir(dir)?;
    match store_state(raw).await? {
        StoreState::Fresh => return Ok(None),
        StoreState::Foreign => return Err(IndexedDbError::Foreign),
        StoreState::Initialised => {}
    }

    let mut prefix = ROOT_PREFIX.to_vec();
    for segment in dir {
        match raw.get(&mapping_row(&prefix, segment)).await? {
            Some(child) => {
                check_prefix(&child)?;
                prefix = child;
            }
            None => return Ok(None),
        }
    }
    Ok(Some(prefix))
}

/// Resolve a directory, allocating every missing ancestor along the path.
pub(crate) async fn allocate_dir<R: RawAsync + ?Sized>(
    raw: &mut R,
    dir: &[&[u8]],
) -> Result<Vec<u8>, IndexedDbError> {
    validate_dir(dir)?;
    ensure_init(raw).await?;

    let mut prefix = ROOT_PREFIX.to_vec();
    for segment in dir {
        let row = mapping_row(&prefix, segment);
        match raw.get(&row).await? {
            Some(child) => {
                check_prefix(&child)?;
                prefix = child;
            }
            None => {
                let encoded = raw
                    .get(COUNTER_KEY)
                    .await?
                    .ok_or_else(|| corrupt("directory allocation counter is missing"))?;
                let candidate = decode_varint(&encoded)
                    .ok_or_else(|| corrupt("directory allocation counter is malformed"))?;
                let allocated = next_alloc(candidate)
                    .ok_or_else(|| corrupt("directory allocation counter is exhausted"))?;
                if is_skipped(allocated) {
                    return Err(corrupt("directory allocator entered the metadata region"));
                }
                let child = allocated_prefix(allocated);
                let next = allocated
                    .checked_add(1)
                    .ok_or_else(|| corrupt("directory allocation counter is exhausted"))?;
                raw.insert(&row, &child)?;
                raw.insert(COUNTER_KEY, &encode_varint(next))?;
                prefix = child;
            }
        }
    }
    Ok(prefix)
}

/// Convert caller key bounds into the raw interval for `prefix`.
pub(crate) fn data_bounds(
    prefix: &[u8],
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
) -> Option<RawBounds> {
    let with_prefix = |key: &[u8]| {
        let mut raw = prefix.to_vec();
        raw.extend_from_slice(key);
        raw
    };
    let raw_start = match start {
        Bound::Unbounded => Bound::Included(prefix.to_vec()),
        Bound::Included(key) => Bound::Included(with_prefix(key)),
        Bound::Excluded(key) => Bound::Excluded(with_prefix(key)),
    };
    let raw_end = match end {
        Bound::Unbounded => {
            let Bound::Excluded(end) = data_range(prefix).1 else {
                return None;
            };
            Bound::Excluded(end.to_vec())
        }
        Bound::Excluded(key) => Bound::Excluded(with_prefix(key)),
        Bound::Included(key) => {
            let mut successor = with_prefix(key);
            successor.push(0);
            Bound::Excluded(successor)
        }
    };

    if raw_range_empty(&raw_start, &raw_end) {
        None
    } else {
        Some((raw_start, raw_end))
    }
}

/// List the immediate child segments of `dir` in deterministic byte order.
pub(crate) async fn list_dirs<R: RawAsync + ?Sized>(
    raw: &R,
    dir: &[&[u8]],
) -> Result<Vec<Vec<u8>>, IndexedDbError> {
    validate_dir(dir)?;
    let Some(prefix) = prefix_of(raw, dir).await? else {
        return Ok(Vec::new());
    };
    let base = mapping_base(&prefix);
    let (start, end) = children_range(&prefix);
    let rows = raw
        .scan_all(bound_ref(&start), bound_ref(&end), false)
        .await?;
    let mut names = rows
        .into_iter()
        .map(|pair| {
            pair.key
                .get(base.len()..)
                .and_then(dec_seg)
                .map(ToOwned::to_owned)
                .ok_or_else(|| corrupt("directory mapping row is malformed"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    names.sort();
    Ok(names)
}

/// Report whether a directory has been materialised.
pub(crate) async fn dir_exists<R: RawAsync + ?Sized>(
    raw: &R,
    dir: &[&[u8]],
) -> Result<bool, IndexedDbError> {
    validate_dir(dir)?;
    if dir.is_empty() {
        is_initialised(raw).await
    } else {
        Ok(prefix_of(raw, dir).await?.is_some())
    }
}

/// Remove a directory, its data, and all descendants.
pub(crate) async fn remove_dir<R: RawAsync + ?Sized>(
    raw: &mut R,
    dir: &[&[u8]],
) -> Result<bool, IndexedDbError> {
    validate_dir(dir)?;
    let Some((segment, parent)) = dir.split_last() else {
        return Err(KeyError::RootNotRemovable.into());
    };
    if !is_initialised(raw).await? {
        return Ok(false);
    }
    let Some(prefix) = prefix_of(raw, dir).await? else {
        return Ok(false);
    };
    let parent_prefix = prefix_of(raw, parent)
        .await?
        .ok_or_else(|| corrupt("directory parent mapping is missing"))?;
    let row = mapping_row(&parent_prefix, segment);

    let mut stack = vec![(prefix, row, false)];
    while let Some((prefix, row, visited)) = stack.pop() {
        if visited {
            let (data_start, data_end) = data_range(&prefix);
            raw.clear_range(bound_ref(&data_start), bound_ref(&data_end))
                .await?;
            raw.delete(&row)?;
            continue;
        }

        let (child_start, child_end) = children_range(&prefix);
        let children = raw
            .scan_all(bound_ref(&child_start), bound_ref(&child_end), false)
            .await?;
        stack.push((prefix, row, true));
        for child in children.into_iter().rev() {
            check_prefix(&child.value)?;
            stack.push((child.value, child.key, false));
        }
    }
    Ok(true)
}

fn bound_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(value) => Bound::Included(value),
        Bound::Excluded(value) => Bound::Excluded(value),
    }
}

fn raw_range_empty(start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    match (start, end) {
        (Bound::Included(start), Bound::Excluded(end))
        | (Bound::Excluded(start), Bound::Excluded(end)) => start >= end,
        _ => false,
    }
}

fn corrupt(detail: &str) -> IndexedDbError {
    IndexedDbError::Corrupt {
        detail: detail.to_owned(),
    }
}
