//! In-memory [`KvStore`] backend for tests and single-process hosts.
//!
//! The backend mirrors the redb backend's shape: [`BTreeMapStore`] is the base
//! (the committed raw map) and [`BTreeMapTransaction`] is a read-write handle
//! over it that **buffers its writes** rather than applying them in place.
//! Reads inside a transaction see the transaction's own uncommitted writes;
//! [`Commit`] applies the buffer to the base atomically, and dropping the
//! transaction discards it. That buffer is the rollback FASM depends on: a
//! state transition that fails partway drops its transaction and the base
//! store is left exactly as it was.
//!
//! - [`BTreeMapStore`] — the base. Holds the committed map. Opens transactions.
//!   Not itself a [`KvStore`]: it is the thing a transaction commits into.
//! - [`BTreeMapTransaction`] — the [`KvStore`] + [`Commit`] +
//!   [`KvDirNav`] write handle. One open transaction at
//!   a time per store is the caller's responsibility.
//!
//! # Layout
//!
//! The committed map holds the flat directory layout's raw rows: data keys,
//! the directory-mapping rows, and the layout's own meta rows (version,
//! counter, root prefix). [`BTreeMapTransaction`] routes every operation
//! through [`FlatEngine`] over a raw view of the
//! merged base-plus-buffer, so the layout logic is written once in
//! `fasm-storage` and shared with the redb backend.
//!
//! # Rollback is by drop, not by an explicit undo
//!
//! A [`BTreeMapTransaction`] owns its write buffer. Committing moves the
//! buffer into the base under a single lock (all buffered keys applied, or
//! none on a panic mid-apply is not possible — the apply is a plain loop with
//! no fallible step). Dropping the transaction, committed or not, simply frees
//! the buffer: nothing reaches the base. There is no `rollback()` method
//! because the buffer never leaves the transaction until [`Commit`].
//!
//! # Single-threaded, no real I/O
//!
//! Every operation is CPU work over an in-process `BTreeMap`; no future
//! yields. A test executor may therefore panic on `Pending` (see the crate's
//! tests) — a backend with real I/O supplies its own runtime instead.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::{Arc, Mutex},
};

use thiserror::Error;

use fasm_storage::{
    Commit, FlatEngine, FlatError, KvDirNav, KvPair, KvStore, KvStream, RawKv,
    RetryableStorageError,
};

/// The committed in-memory key-value map and the opener for transactions over
/// it. Cloneable: every clone shares the one map.
///
/// This is the redb `RedbStore` role — a handle to the store, not a store
/// view. It is deliberately *not* a [`KvStore`]: the only write path is
/// through a [`BTreeMapTransaction`], so there is no way to bypass the
/// commit/rollback boundary.
#[derive(Clone)]
pub struct BTreeMapStore {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl std::fmt::Debug for BTreeMapStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Report only the committed row count, never the bytes: this store
        // may hold key material, and a panic `Debug` output must not leak it.
        let n = self.data.lock().expect("in-memory store mutex").len();
        f.debug_struct("BTreeMapStore").field("rows", &n).finish()
    }
}

impl Default for BTreeMapStore {
    fn default() -> Self {
        Self::new()
    }
}

impl BTreeMapStore {
    /// An empty store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// A store over a consumer-provided raw map, sharing it with every clone.
    /// The map holds the layout's raw rows (meta and data); the store owns no
    /// other copy of it.
    pub fn from_map(map: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>) -> Self {
        Self { data: map }
    }

    /// The raw committed map this store backs.
    pub fn raw(&self) -> &Mutex<BTreeMap<Vec<u8>, Vec<u8>>> {
        &self.data
    }

    /// Opens a write transaction over this store. Uncommitted on drop.
    ///
    /// The transaction buffers its writes and reads its own buffer first, so a
    /// fresh transaction opened while another is outstanding sees only the
    /// committed map. Committing one is the only way its writes reach the
    /// base.
    pub fn transaction(&self) -> BTreeMapTransaction {
        BTreeMapTransaction {
            base: Arc::clone(&self.data),
            buffer: BTreeMap::new(),
        }
    }

    /// Number of committed raw rows (data and layout rows).
    pub fn len(&self) -> usize {
        self.data.lock().expect("in-memory store mutex").len()
    }

    /// Whether the committed map is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Committed raw keys, ascending. A point inspection of the base; the
    /// transactional read path is [`BTreeMapTransaction`].
    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.data
            .lock()
            .expect("in-memory store mutex")
            .keys()
            .cloned()
            .collect()
    }

    /// A committed raw point read of the base, ignoring any outstanding
    /// transaction's buffer.
    pub fn get_committed(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data
            .lock()
            .expect("in-memory store mutex")
            .get(key)
            .cloned()
    }

    /// Remove all committed rows.
    pub fn clear(&self) {
        self.data.lock().expect("in-memory store mutex").clear();
    }
}

/// A read-write view over one [`BTreeMapStore`] that buffers its writes.
///
/// Reads see the transaction's own uncommitted writes (a buffered value, or a
/// tombstone for a deletion, taking precedence over the base); [`Commit`]
/// applies the whole buffer to the base under a single lock, and dropping the
/// transaction discards it. The buffered writes are the rollback: a failed
/// state transition drops its transaction and the base is unchanged.
///
/// Every operation runs through a [`FlatEngine`] over a raw view of the
/// merged base-plus-buffer, so directory resolution, bounds mapping and
/// navigation are the shared `fasm-storage` layout logic, not a second copy.
pub struct BTreeMapTransaction {
    base: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    /// Pending writes. `Some(v)` is a set, `None` a deletion (tombstone).
    ///
    /// A tombstone matters when the key exists in the base: a plain "skip"
    /// would let the base value leak through on a later read.
    buffer: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl std::fmt::Debug for BTreeMapTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Report only the pending write count, never the buffered bytes.
        f.debug_struct("BTreeMapTransaction")
            .field("pending", &self.buffer.len())
            .finish()
    }
}

impl BTreeMapTransaction {
    /// Number of pending (uncommitted) raw writes, including tombstones.
    pub fn pending_len(&self) -> usize {
        self.buffer.len()
    }
}

// ============================================================================
// Raw views over the merged base-plus-buffer
// ============================================================================

/// A read-only raw view of the merged base-plus-buffer.
struct BufReadView<'b> {
    base: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    buffer: &'b BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

/// A read-write raw view of the merged base-plus-buffer: reads merge as in
/// [`BufReadView`], writes go to the buffer.
struct BufWriteView<'b> {
    base: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    buffer: &'b mut BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

/// Whether `key` lies inside `start..end` by plain lexicographic byte order.
/// The shared bound predicate the buffer/base merge relies on.
fn key_in_range(key: &[u8], start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    let above_start = match start {
        Bound::Included(s) => key >= s,
        Bound::Excluded(s) => key > s,
        Bound::Unbounded => true,
    };
    let below_end = match end {
        Bound::Included(e) => key <= e,
        Bound::Excluded(e) => key < e,
        Bound::Unbounded => true,
    };
    above_start && below_end
}

/// Read one raw key: the buffer wins over the base (a buffered set or a
/// tombstone suppresses the base value).
fn merged_get(
    base: &Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    buffer: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    key: &[u8],
) -> Option<Vec<u8>> {
    if let Some(value) = buffer.get(key) {
        return value.clone();
    }
    base.lock()
        .expect("in-memory store mutex")
        .get(key)
        .cloned()
}

/// Scan raw rows in `start..end`, merging the committed map and the buffer:
/// a buffered value wins, a tombstone suppresses, ascending.
///
/// The merge is materialized into a small `Vec` because this is the
/// in-memory reference backend.
fn merged_scan(
    base: &Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    buffer: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
) -> Vec<KvPair> {
    // Snapshot the committed rows inside the range under one lock.
    let committed: BTreeMap<Vec<u8>, Vec<u8>> = base
        .lock()
        .expect("in-memory store mutex")
        .iter()
        .filter(|(k, _)| key_in_range(k, start, end))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    // The buffered rows inside the range (values or tombstones).
    let buffered: BTreeMap<Vec<u8>, Option<Vec<u8>>> = buffer
        .iter()
        .filter(|(k, _)| key_in_range(k.as_slice(), start, end))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // The union of candidate keys, ascending.
    let mut candidates: BTreeSet<Vec<u8>> = BTreeSet::new();
    candidates.extend(committed.keys().cloned());
    candidates.extend(buffered.keys().cloned());

    let mut rows = Vec::new();
    for key in &candidates {
        match buffered.get(key) {
            // A buffered set wins.
            Some(Some(value)) => rows.push(KvPair {
                key: key.clone(),
                value: value.clone(),
            }),
            // A tombstone suppresses the base value.
            Some(None) => {}
            // No buffered write: fall back to the base if present.
            None => {
                if let Some(value) = committed.get(key) {
                    rows.push(KvPair {
                        key: key.clone(),
                        value: value.clone(),
                    });
                }
            }
        }
    }
    rows
}

/// Tombstone every base-plus-buffered key in `start..end` into `buffer`.
fn merged_clear(
    base: &Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    buffer: &mut BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
) {
    let committed_keys = base
        .lock()
        .expect("in-memory store mutex")
        .keys()
        .filter(|k| key_in_range(k, start, end))
        .cloned()
        .collect::<Vec<_>>();
    for key in committed_keys {
        buffer.insert(key, None);
    }
    let buffered_keys = buffer
        .keys()
        .filter(|k| key_in_range(k, start, end))
        .cloned()
        .collect::<Vec<_>>();
    for key in buffered_keys {
        buffer.insert(key, None);
    }
}

/// The raw-view error. The merged view cannot fail to read; only a write
/// routed through a read-only view can.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("in-memory raw view is read-only")]
pub struct BufViewError;

impl RetryableStorageError for BufViewError {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl RawKv for BufReadView<'_> {
    type Error = BufViewError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(merged_get(&self.base, self.buffer, key))
    }

    fn scan(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        forward: bool,
    ) -> Result<Vec<KvPair>, Self::Error> {
        let mut rows = merged_scan(&self.base, self.buffer, start, end);
        if !forward {
            rows.reverse();
        }
        Ok(rows)
    }

    fn insert(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        Err(BufViewError)
    }

    fn delete(&mut self, _key: &[u8]) -> Result<(), Self::Error> {
        Err(BufViewError)
    }

    fn clear_range(&mut self, _start: Bound<&[u8]>, _end: Bound<&[u8]>) -> Result<(), Self::Error> {
        Err(BufViewError)
    }
}

impl RawKv for BufWriteView<'_> {
    type Error = BufViewError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(merged_get(&self.base, self.buffer, key))
    }

    fn scan(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        forward: bool,
    ) -> Result<Vec<KvPair>, Self::Error> {
        let mut rows = merged_scan(&self.base, self.buffer, start, end);
        if !forward {
            rows.reverse();
        }
        Ok(rows)
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.buffer.insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        self.buffer.insert(key.to_vec(), None);
        Ok(())
    }

    fn clear_range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<(), Self::Error> {
        merged_clear(&self.base, self.buffer, start, end);
        Ok(())
    }
}

// ============================================================================
// The KvStore / KvDirNav / Commit surface
// ============================================================================

/// The error type for the in-memory backend: the flat-layout error with the
/// merged raw view's error as its engine source.
///
/// `Display` is transparent, so a core [`KeyError`](fasm_storage::KeyError)
/// (an invalid directory segment, a root removal) renders with its own
/// message. Nothing here is retryable: the view cannot fail to read, and a
/// layout precondition on an in-memory store would be a bug, not a transient
/// condition.
#[derive(Debug, Error)]
#[error(transparent)]
#[repr(transparent)]
pub struct BTreeMapError(FlatError<BufViewError>);

impl From<FlatError<BufViewError>> for BTreeMapError {
    fn from(e: FlatError<BufViewError>) -> Self {
        Self(e)
    }
}

impl RetryableStorageError for BTreeMapError {
    fn is_retryable(&self) -> bool {
        self.0.is_retryable()
    }
}

impl KvStore for BTreeMapTransaction {
    type Error = BTreeMapError;

    async fn get(&self, dir: &[&[u8]], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let view = BufReadView {
            base: Arc::clone(&self.base),
            buffer: &self.buffer,
        };
        Ok(FlatEngine::new(view).get(dir, key)?)
    }

    async fn set(&mut self, dir: &[&[u8]], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let view = BufWriteView {
            base: Arc::clone(&self.base),
            buffer: &mut self.buffer,
        };
        FlatEngine::new(view)
            .set(dir, key, value)
            .map_err(BTreeMapError)
    }

    async fn delete(&mut self, dir: &[&[u8]], key: &[u8]) -> Result<(), Self::Error> {
        let view = BufWriteView {
            base: Arc::clone(&self.base),
            buffer: &mut self.buffer,
        };
        FlatEngine::new(view)
            .delete(dir, key)
            .map_err(BTreeMapError)
    }

    fn range<'a>(
        &'a self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        let view = BufReadView {
            base: Arc::clone(&self.base),
            buffer: &self.buffer,
        };
        match FlatEngine::new(view).scan(dir, start, end, !reverse) {
            Ok(pairs) => {
                // `scan` already returned the rows in the requested
                // direction; emit them as-is.
                stream_from_pairs(pairs, false)
            }
            // `scan` validates `dir` and can fail with a key error: surface
            // it as a failed stream rather than panicking on a sync path.
            Err(e) => KvStream::failed(BTreeMapError(e)),
        }
    }

    async fn clear_range(
        &mut self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        let view = BufWriteView {
            base: Arc::clone(&self.base),
            buffer: &mut self.buffer,
        };
        FlatEngine::new(view)
            .clear_range(dir, start, end)
            .map_err(BTreeMapError)
    }
}

impl KvDirNav for BTreeMapTransaction {
    async fn list_dirs(&self, dir: &[&[u8]]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let view = BufReadView {
            base: Arc::clone(&self.base),
            buffer: &self.buffer,
        };
        FlatEngine::new(view).list_dirs(dir).map_err(BTreeMapError)
    }

    async fn dir_exists(&self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        let view = BufReadView {
            base: Arc::clone(&self.base),
            buffer: &self.buffer,
        };
        FlatEngine::new(view).dir_exists(dir).map_err(BTreeMapError)
    }

    async fn remove_dir(&mut self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        let view = BufWriteView {
            base: Arc::clone(&self.base),
            buffer: &mut self.buffer,
        };
        FlatEngine::new(view).remove_dir(dir).map_err(BTreeMapError)
    }
}

impl Commit for BTreeMapTransaction {
    type Error = BTreeMapError;

    /// Apply the whole buffer to the base under one lock: every buffered raw
    /// row is set or removed, so the committed view is exactly the
    /// transaction's view at the moment of commit. The apply is a plain loop
    /// with no fallible step, so it is atomic in practice (a panic mid-loop
    /// would leave the base partially applied, but there is nothing in the
    /// loop that panics).
    async fn commit(self) -> Result<(), Self::Error> {
        let mut base = self.base.lock().expect("in-memory store mutex");
        for (key, value) in self.buffer {
            match value {
                Some(value) => {
                    base.insert(key, value);
                }
                None => {
                    base.remove(&key);
                }
            }
        }
        Ok(())
    }
}

/// Drive a list of pairs as a [`KvStream`], one per `next()` call.
///
/// The stream owns its data (the `Vec` is moved in), so it needs no borrow of
/// a store and can outlive any transaction that produced it. Each `next()`
/// pulls the next pair off the front (or back, if `reverse`) and re-wraps the
/// tail as the continuation, so the list is consumed lazily one step at a
/// time.
///
/// The lifetime is a parameter rather than `'static` because
/// [`KvStore::range`] hands back a `KvStream` tied to the `&self` borrow, and
/// [`KvStream`] is invariant in its lifetime: the caller's (shorter) lifetime
/// is what the recursive tail is built with.
pub fn stream_from_pairs<'a>(pairs: Vec<KvPair>, reverse: bool) -> KvStream<'a, BTreeMapError> {
    stream_from_deque(std::collections::VecDeque::from(pairs), reverse)
}

/// The recursive tail: a deque drains from either end in O(1), where a
/// `Vec` front removal is O(n) and would make a full forward drain O(n^2).
fn stream_from_deque<'a>(
    mut pairs: std::collections::VecDeque<KvPair>,
    reverse: bool,
) -> KvStream<'a, BTreeMapError> {
    if pairs.is_empty() {
        return KvStream::empty();
    }
    let first = if reverse {
        pairs.pop_back().expect("pairs is non-empty")
    } else {
        pairs.pop_front().expect("pairs is non-empty")
    };
    KvStream::new(async move { Ok(Some((first, stream_from_deque(pairs, reverse)))) })
}

#[cfg(test)]
mod tests;
