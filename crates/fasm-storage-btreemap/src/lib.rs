//! In-memory [`KvStore`] backend for tests and single-process hosts.
//!
//! The backend mirrors the redb backend's shape: [`BTreeMapStore`] is the base
//! (the committed key-value map) and [`BTreeMapTransaction`] is a read-write
//! handle over it that **buffers its writes** rather than applying them in
//! place. Reads inside a transaction see the transaction's own uncommitted
//! writes; [`Commit`] applies the buffer to the base atomically, and dropping
//! the transaction discards it. That buffer is the rollback FASM depends on: a
//! state transition that fails partway drops its transaction and the base store
//! is left exactly as it was.
//!
//! - [`BTreeMapStore`] — the base. Holds the committed map. Opens transactions.
//!   Not itself a [`KvStore`]: it is the thing a transaction commits into.
//! - [`BTreeMapTransaction`] — the [`KvStore`] + [`Commit`] write handle. One
//!   open transaction at a time per store is the caller's responsibility.
//!
//! # Rollback is by drop, not by an explicit undo
//!
//! A [`BTreeMapTransaction`] owns its write buffer. Committing moves the buffer
//! into the base under a single lock (all buffered keys applied, or none on a
//! panic mid-apply is not possible — the apply is a plain loop with no fallible
//! step). Dropping the transaction, committed or not, simply frees the buffer:
//! nothing reaches the base. There is no `rollback()` method because the buffer
//! never leaves the transaction until [`Commit`].
//!
//! # Single-threaded, no real I/O
//!
//! Every operation is CPU work over an in-process `BTreeMap`; no future yields.
//! A test executor may therefore panic on `Pending` (see the crate's tests) —
//! a backend with real I/O supplies its own runtime instead.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::{Arc, Mutex},
};

use fasm_storage::{Commit, KvPair, KvStore, KvStream, RetryableStorageError};

/// The committed in-memory key-value map and the opener for transactions over
/// it. Cloneable: every clone shares the one map.
///
/// This is the redb `RedbStore` role — a handle to the store, not a store view.
/// It is deliberately *not* a [`KvStore`]: the only write path is through a
/// [`BTreeMapTransaction`], so there is no way to bypass the commit/rollback
/// boundary.
#[derive(Clone)]
pub struct BTreeMapStore {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl std::fmt::Debug for BTreeMapStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Report only the committed key count, never the bytes: this store may
        // hold key material, and a panic `Debug` output must not leak it.
        let n = self.data.lock().expect("in-memory store mutex").len();
        f.debug_struct("BTreeMapStore").field("keys", &n).finish()
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

    /// Opens a write transaction over this store. Uncommitted on drop.
    ///
    /// The transaction buffers its writes and reads its own buffer first, so a
    /// fresh transaction opened while another is outstanding sees only the
    /// committed map. Committing one is the only way its writes reach the base.
    pub fn transaction(&self) -> BTreeMapTransaction {
        BTreeMapTransaction {
            base: Arc::clone(&self.data),
            buffer: BTreeMap::new(),
        }
    }

    /// Number of committed keys.
    pub fn len(&self) -> usize {
        self.data.lock().expect("in-memory store mutex").len()
    }

    /// Whether the committed map is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Committed keys, ascending. A point inspection of the base; the
    /// transactional read path is [`BTreeMapTransaction`].
    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.data
            .lock()
            .expect("in-memory store mutex")
            .keys()
            .cloned()
            .collect()
    }

    /// A committed point read of the base, ignoring any outstanding
    /// transaction's buffer.
    pub fn get_committed(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data
            .lock()
            .expect("in-memory store mutex")
            .get(key)
            .cloned()
    }

    /// Remove all committed keys.
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
    /// Number of pending (uncommitted) writes, including tombstones.
    pub fn pending_len(&self) -> usize {
        self.buffer.len()
    }
}

/// Whether `key` lies inside `start..end` by plain lexicographic byte order.
/// The shared bound predicate the buffer/base merge and the reference-model
/// tests both rely on.
fn key_in_range(key: &Vec<u8>, start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    let above_start = match start {
        Bound::Included(s) => key.as_slice() >= s,
        Bound::Excluded(s) => key.as_slice() > s,
        Bound::Unbounded => true,
    };
    let below_end = match end {
        Bound::Included(e) => key.as_slice() <= e,
        Bound::Excluded(e) => key.as_slice() < e,
        Bound::Unbounded => true,
    };
    above_start && below_end
}

#[async_trait::async_trait]
impl KvStore for BTreeMapTransaction {
    type Error = BTreeMapError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let owned = key.to_vec();
        // Read-your-writes: the transaction's own buffer wins over the base.
        if let Some(value) = self.buffer.get(&owned) {
            return Ok(value.clone());
        }
        Ok(self
            .base
            .lock()
            .expect("in-memory store mutex")
            .get(&owned)
            .cloned())
    }

    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.buffer.insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        self.buffer.insert(key.to_vec(), None);
        Ok(())
    }

    /// Stream a range of keys, merging the committed map and the transaction's
    /// buffered writes: a buffered value or tombstone wins over the base, and
    /// tombstoned keys are omitted. The merge is materialized into a small
    /// `Vec` because this is the in-memory reference backend.
    fn range<'a>(
        &'a self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        // Snapshot the committed keys/values inside the range under one lock.
        let committed: BTreeMap<Vec<u8>, Vec<u8>> = self
            .base
            .lock()
            .expect("in-memory store mutex")
            .iter()
            .filter(|(k, _)| key_in_range(k, start, end))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        // The buffered keys inside the range (values or tombstones).
        let buffered: BTreeMap<Vec<u8>, Option<Vec<u8>>> = self
            .buffer
            .iter()
            .filter(|(k, _)| key_in_range(k, start, end))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // The union of candidate keys, ascending.
        let mut candidates: BTreeSet<Vec<u8>> = BTreeSet::new();
        candidates.extend(committed.keys().cloned());
        candidates.extend(buffered.keys().cloned());

        let mut pairs = Vec::new();
        for key in &candidates {
            match buffered.get(key) {
                // A buffered set wins.
                Some(Some(value)) => pairs.push(KvPair {
                    key: key.clone(),
                    value: value.clone(),
                }),
                // A tombstone suppresses the base value.
                Some(None) => {}
                // No buffered write: fall back to the base if present.
                None => {
                    if let Some(value) = committed.get(key) {
                        pairs.push(KvPair {
                            key: key.clone(),
                            value: value.clone(),
                        });
                    }
                }
            }
        }

        // `stream_from_pairs` owns the direction: the list stays ascending here.
        stream_from_pairs(pairs, reverse)
    }

    /// Delete every key in `start..end` by tombstoning it in the buffer: both
    /// the committed keys and the buffered keys in the range. Committing then
    /// removes them from the base; dropping discards the tombstones.
    async fn clear_range(
        &mut self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        let committed_keys = self
            .base
            .lock()
            .expect("in-memory store mutex")
            .keys()
            .filter(|k| key_in_range(k, start, end))
            .cloned()
            .collect::<Vec<_>>();
        for key in committed_keys {
            self.buffer.insert(key, None);
        }
        let buffered_keys = self
            .buffer
            .keys()
            .filter(|k| key_in_range(k, start, end))
            .cloned()
            .collect::<Vec<_>>();
        for key in buffered_keys {
            self.buffer.insert(key, None);
        }
        Ok(())
    }
}

impl Commit for BTreeMapTransaction {
    type Error = BTreeMapError;

    /// Apply the whole buffer to the base under one lock: every buffered key is
    /// set or removed, so the committed view is exactly the transaction's view
    /// at the moment of commit. The apply is a plain loop with no fallible step,
    /// so it is atomic in practice (a panic mid-loop would leave the base
    /// partially applied, but there is nothing in the loop that panics).
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

/// The error type for the in-memory backend.
///
/// A `BTreeMap` cannot fail: there is no disk, no lock to time out, and no
/// concurrency to reject. The type exists because [`KvStore::Error`] is a
/// required associated type, and it is reported as not retryable because an
/// "error" here would be a real bug, not a transient condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BTreeMapError;

impl std::fmt::Display for BTreeMapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "in-memory store: no failures are possible")
    }
}

impl std::error::Error for BTreeMapError {}

impl RetryableStorageError for BTreeMapError {
    fn is_retryable(&self) -> bool {
        false
    }
}

/// Drive a list of pairs as a [`KvStream`], one per `next()` call.
///
/// The stream owns its data (the `Vec` is moved in), so it needs no borrow of
/// a store and can outlive any transaction that produced it. Each `next()` pulls
/// the next pair off the front (or back, if `reverse`) and re-wraps the tail as
/// the continuation, so the list is consumed lazily one step at a time.
///
/// The lifetime is a parameter rather than `'static` because [`KvStore::range`]
/// hands back a `KvStream` tied to the `&self` borrow, and [`KvStream`] is
/// invariant in its lifetime: the caller's (shorter) lifetime is what the
/// recursive tail is built with.
pub fn stream_from_pairs<'a>(mut pairs: Vec<KvPair>, reverse: bool) -> KvStream<'a, BTreeMapError> {
    if pairs.is_empty() {
        return KvStream::empty();
    }
    let first = if reverse {
        pairs.pop().expect("pairs is non-empty")
    } else {
        pairs.remove(0)
    };
    KvStream::new(async move { Ok(Some((first, stream_from_pairs(pairs, reverse)))) })
}

#[cfg(test)]
mod tests;
