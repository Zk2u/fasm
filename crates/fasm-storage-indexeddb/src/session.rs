//! Buffered state-transition sessions and committed-data readers.

use std::{fmt, ops::Bound};

use js_sys::Array;
use web_sys::IdbTransactionMode;

use crate::{
    IndexedDbError, IndexedDbStore, Revision,
    idb::{
        KV_STORE, KeyRange, RequestFuture, TransactionOutcome, bytes_from_js, bytes_to_js,
        dom_error, key_range,
    },
    overlay::{Lookup, WriteBuffer},
    store::{Scope, readonly_result},
};

/// One buffered state transition against a named IndexedDB database.
///
/// Point reads overlay pending writes and tombstones on committed data. The
/// buffer remains private to this session until the later commit operation
/// applies it atomically after checking [`expected_revision`](Self::expected_revision).
pub struct IndexedDbTransaction {
    #[allow(dead_code)] // used from commit 7 (trait impl)
    store: IndexedDbStore,
    buffer: WriteBuffer,
    expected: Revision,
}

impl fmt::Debug for IndexedDbTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexedDbTransaction")
            .field("pending_len", &self.pending_len())
            .field("expected_revision", &self.expected)
            .finish()
    }
}

impl IndexedDbTransaction {
    pub(crate) fn new(store: IndexedDbStore, expected: Revision) -> Self {
        Self {
            store,
            buffer: WriteBuffer::new(),
            expected,
        }
    }

    /// Returns the number of keys with a pending value or tombstone.
    pub fn pending_len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the optimistic revision fence observed when the session opened.
    pub fn expected_revision(&self) -> Revision {
        self.expected
    }

    #[allow(dead_code)] // used from commit 7 (trait impl)
    pub(crate) async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, IndexedDbError> {
        match self.buffer.lookup(key) {
            Lookup::Set(value) => Ok(Some(value.to_vec())),
            Lookup::Tombstone => Ok(None),
            Lookup::Miss => get_committed(&self.store, key).await,
        }
    }

    #[allow(dead_code)] // used from commit 7 (trait impl)
    pub(crate) async fn contains(&self, key: &[u8]) -> Result<bool, IndexedDbError> {
        match self.buffer.lookup(key) {
            Lookup::Set(_) => Ok(true),
            Lookup::Tombstone => Ok(false),
            Lookup::Miss => contains_committed(&self.store, key).await,
        }
    }

    #[allow(dead_code)] // used from commit 7 (trait impl)
    pub(crate) fn write(&mut self, key: &[u8], value: &[u8]) {
        self.buffer.set(key, value);
    }

    #[allow(dead_code)] // used from commit 7 (trait impl)
    pub(crate) fn remove(&mut self, key: &[u8]) {
        self.buffer.delete(key);
    }

    #[allow(dead_code)] // used from commit 7 (trait impl)
    pub(crate) async fn clear(
        &mut self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), IndexedDbError> {
        let committed = committed_keys_in_range(&self.store, start, end).await?;
        self.buffer.tombstone_keys(committed, start, end);
        Ok(())
    }
}

/// A read-only view over data already committed to IndexedDB.
///
/// Each operation opens a short readonly browser transaction. The handle does
/// not include a buffered session and therefore never exposes pending writes.
pub struct IndexedDbReader {
    store: IndexedDbStore,
}

impl fmt::Debug for IndexedDbReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexedDbReader")
            .field("store", &self.store)
            .finish()
    }
}

impl IndexedDbReader {
    pub(crate) fn new(store: IndexedDbStore) -> Self {
        Self { store }
    }

    #[allow(dead_code)] // used from commit 7 (trait impl)
    pub(crate) async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, IndexedDbError> {
        get_committed(&self.store, key).await
    }

    #[allow(dead_code)] // used from commit 7 (trait impl)
    pub(crate) async fn contains(&self, key: &[u8]) -> Result<bool, IndexedDbError> {
        contains_committed(&self.store, key).await
    }
}

#[allow(dead_code)] // used from commit 7 (trait impl)
pub(crate) async fn get_committed(
    store: &IndexedDbStore,
    key: &[u8],
) -> Result<Option<Vec<u8>>, IndexedDbError> {
    let transaction = store.begin(IdbTransactionMode::Readonly, Scope::Kv)?;
    let object_store = transaction
        .object_store(KV_STORE)
        .map_err(|value| dom_error(&value))?;
    let request = object_store
        .get(&bytes_to_js(key))
        .map_err(|value| dom_error(&value))?;
    let outcome = TransactionOutcome::new(transaction);
    let value = RequestFuture::new(request).await;
    readonly_result(outcome.await)?;
    let value = value?;

    if value.is_undefined() || value.is_null() {
        Ok(None)
    } else {
        bytes_from_js(&value, "value").map(Some)
    }
}

#[allow(dead_code)] // used from commit 7 (trait impl)
async fn contains_committed(store: &IndexedDbStore, key: &[u8]) -> Result<bool, IndexedDbError> {
    let transaction = store.begin(IdbTransactionMode::Readonly, Scope::Kv)?;
    let object_store = transaction
        .object_store(KV_STORE)
        .map_err(|value| dom_error(&value))?;
    let request = object_store
        .get_key(&bytes_to_js(key))
        .map_err(|value| dom_error(&value))?;
    let outcome = TransactionOutcome::new(transaction);
    let value = RequestFuture::new(request).await;
    readonly_result(outcome.await)?;
    let value = value?;
    Ok(!value.is_undefined() && !value.is_null())
}

/// Returns all committed keys selected by `start` and `end`.
///
/// This materialises every committed key in the range before the caller adds
/// tombstones. That is intentionally acceptable for the small per-swap
/// prefixes this backend clears; large arbitrary ranges should use a paged
/// design instead.
#[allow(dead_code)] // used from commit 7 (trait impl)
pub(crate) async fn committed_keys_in_range(
    store: &IndexedDbStore,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
) -> Result<Vec<Vec<u8>>, IndexedDbError> {
    let range = key_range(start, end)?;
    if matches!(range, KeyRange::Empty) {
        return Ok(Vec::new());
    }

    let transaction = store.begin(IdbTransactionMode::Readonly, Scope::Kv)?;
    let object_store = transaction
        .object_store(KV_STORE)
        .map_err(|value| dom_error(&value))?;
    let request = match range {
        KeyRange::Empty => return Ok(Vec::new()),
        KeyRange::All => object_store.get_all_keys(),
        KeyRange::Bounded(range) => object_store.get_all_keys_with_key(range.as_ref()),
    }
    .map_err(|value| dom_error(&value))?;
    let outcome = TransactionOutcome::new(transaction);
    let keys = RequestFuture::new(request).await;
    readonly_result(outcome.await)?;
    let keys = keys?;
    if !Array::is_array(&keys) {
        return Err(IndexedDbError::Corrupt {
            detail: "getAllKeys result is not an array".to_owned(),
        });
    }

    Array::from(&keys)
        .iter()
        .map(|key| bytes_from_js(&key, "key"))
        .collect()
}
