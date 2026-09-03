//! Buffered state-transition sessions and committed-data readers.

use std::{fmt, ops::Bound};

use fasm_storage::KvPair;
use wasm_bindgen::JsValue;
use web_sys::{IdbCursorDirection, IdbRequest, IdbTransactionMode};

use crate::{
    IndexedDbError, IndexedDbStore, Revision,
    flat::{self, RawAsync},
    idb::{
        KV_STORE, KeyRange, RequestFuture, TransactionOutcome, bytes_from_js, bytes_to_js,
        dom_error, key_range, read_cursor_page,
    },
    overlay::{Lookup, WriteBuffer, merge_page},
    store::{Scope, readonly_result},
};

/// One buffered state transition against a named IndexedDB database.
///
/// Directory-and-key reads overlay pending writes and tombstones on committed data. The
/// buffer remains private to this session until the later commit operation
/// applies it atomically after checking [`expected_revision`](Self::expected_revision).
pub struct IndexedDbTransaction {
    pub(crate) store: IndexedDbStore,
    pub(crate) buffer: WriteBuffer,
    pub(crate) expected: Revision,
    #[cfg(test)]
    pub(crate) faults: FaultInjection,
}

/// Test-only failures injected at distinct commit phases.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct FaultInjection {
    pub fail_conversion_of: Option<Vec<u8>>,
    pub fail_enqueue_of: Option<Vec<u8>>,
    pub fail_request_of: Option<Vec<u8>>,
    pub fail_abort: bool,
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
            #[cfg(test)]
            faults: FaultInjection::default(),
        }
    }

    /// Installs deterministic commit failures for browser tests.
    #[cfg(test)]
    pub(crate) fn inject_faults(&mut self, faults: FaultInjection) {
        self.faults = faults;
    }

    /// Returns the number of keys with a pending value or tombstone.
    pub fn pending_len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the optimistic revision fence observed when the session opened.
    pub fn expected_revision(&self) -> Revision {
        self.expected
    }

    /// Read a key from one exact directory through the overlay-aware raw view.
    pub(crate) async fn read(
        &self,
        dir: &[&[u8]],
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, IndexedDbError> {
        let Some(mut raw_key) = flat::prefix_of(self, dir).await? else {
            return Ok(None);
        };
        raw_key.extend_from_slice(key);
        self.raw_read(&raw_key).await
    }

    /// Read a raw layout or data row with buffered writes taking precedence.
    pub(crate) async fn raw_read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, IndexedDbError> {
        match self.buffer.lookup(key) {
            Lookup::Set(value) => Ok(Some(value.to_vec())),
            Lookup::Tombstone => Ok(None),
            Lookup::Miss => get_committed(&self.store, key).await,
        }
    }

    /// Test a key in one exact directory without transferring a committed value.
    pub(crate) async fn contains(&self, dir: &[&[u8]], key: &[u8]) -> Result<bool, IndexedDbError> {
        let Some(mut raw_key) = flat::prefix_of(self, dir).await? else {
            return Ok(false);
        };
        raw_key.extend_from_slice(key);
        self.raw_contains(&raw_key).await
    }

    async fn raw_contains(&self, key: &[u8]) -> Result<bool, IndexedDbError> {
        match self.buffer.lookup(key) {
            Lookup::Set(_) => Ok(true),
            Lookup::Tombstone => Ok(false),
            Lookup::Miss => contains_committed(&self.store, key).await,
        }
    }

    /// Buffer a key in one directory, allocating missing ancestors first.
    pub(crate) async fn write(
        &mut self,
        dir: &[&[u8]],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), IndexedDbError> {
        let mut raw_key = flat::allocate_dir(self, dir).await?;
        raw_key.extend_from_slice(key);
        self.buffer.set(&raw_key, value);
        Ok(())
    }

    /// Buffer deletion of a key from one directory.
    pub(crate) async fn remove(&mut self, dir: &[&[u8]], key: &[u8]) -> Result<(), IndexedDbError> {
        let Some(mut raw_key) = flat::prefix_of(self, dir).await? else {
            return Ok(());
        };
        raw_key.extend_from_slice(key);
        self.buffer.delete(&raw_key);
        Ok(())
    }

    /// Buffer deletion of a caller-key range within one directory.
    pub(crate) async fn clear(
        &mut self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), IndexedDbError> {
        let Some(prefix) = flat::prefix_of(self, dir).await? else {
            return Ok(());
        };
        let Some((start, end)) = flat::data_bounds(&prefix, start, end) else {
            return Ok(());
        };
        self.raw_clear(bound_ref(&start), bound_ref(&end)).await?;
        Ok(())
    }

    /// List immediate directory children through the async layout driver.
    pub(crate) async fn list_directories(
        &self,
        dir: &[&[u8]],
    ) -> Result<Vec<Vec<u8>>, IndexedDbError> {
        flat::list_dirs(self, dir).await
    }

    /// Report whether the directory mapping has been materialised.
    pub(crate) async fn directory_exists(&self, dir: &[&[u8]]) -> Result<bool, IndexedDbError> {
        flat::dir_exists(self, dir).await
    }

    /// Buffer recursive removal of a directory subtree.
    pub(crate) async fn remove_directory(&mut self, dir: &[&[u8]]) -> Result<bool, IndexedDbError> {
        flat::remove_dir(self, dir).await
    }

    async fn raw_clear(
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
    pub(crate) store: IndexedDbStore,
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

    /// Read committed data from one exact directory.
    pub(crate) async fn read(
        &self,
        dir: &[&[u8]],
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, IndexedDbError> {
        let Some(mut raw_key) = flat::prefix_of(self, dir).await? else {
            return Ok(None);
        };
        raw_key.extend_from_slice(key);
        get_committed(&self.store, &raw_key).await
    }

    /// Test committed data in one directory without transferring its value.
    pub(crate) async fn contains(&self, dir: &[&[u8]], key: &[u8]) -> Result<bool, IndexedDbError> {
        let Some(mut raw_key) = flat::prefix_of(self, dir).await? else {
            return Ok(false);
        };
        raw_key.extend_from_slice(key);
        contains_committed(&self.store, &raw_key).await
    }
}

async fn get_committed(
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
async fn committed_keys_in_range(
    store: &IndexedDbStore,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
) -> Result<Vec<Vec<u8>>, IndexedDbError> {
    committed_rows_in_range(store, start, end, false)
        .await
        .map(|rows| rows.into_iter().map(|(key, _)| key).collect())
}

impl RawAsync for IndexedDbTransaction {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, IndexedDbError> {
        self.raw_read(key).await
    }

    async fn scan_all(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<Vec<KvPair>, IndexedDbError> {
        let committed = committed_rows_in_range(&self.store, start, end, reverse).await?;
        Ok(merge_page(&self.buffer, committed, (start, end), reverse)
            .into_iter()
            .map(|(key, value)| KvPair { key, value })
            .collect())
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), IndexedDbError> {
        self.buffer.set(key, value);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), IndexedDbError> {
        self.buffer.delete(key);
        Ok(())
    }

    async fn clear_range(
        &mut self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), IndexedDbError> {
        self.raw_clear(start, end).await
    }
}

impl RawAsync for IndexedDbReader {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, IndexedDbError> {
        get_committed(&self.store, key).await
    }

    async fn scan_all(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<Vec<KvPair>, IndexedDbError> {
        committed_rows_in_range(&self.store, start, end, reverse)
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|(key, value)| KvPair { key, value })
                    .collect()
            })
    }

    fn insert(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), IndexedDbError> {
        Err(IndexedDbError::ReadOnly)
    }

    fn delete(&mut self, _key: &[u8]) -> Result<(), IndexedDbError> {
        Err(IndexedDbError::ReadOnly)
    }

    async fn clear_range(
        &mut self,
        _start: Bound<&[u8]>,
        _end: Bound<&[u8]>,
    ) -> Result<(), IndexedDbError> {
        Err(IndexedDbError::ReadOnly)
    }
}

/// Materialise committed raw rows for directory metadata and clear operations.
///
/// User-facing scans use the paged implementation in `scan.rs`; this helper is
/// intentionally limited to structural work that needs an owned merged view.
pub(crate) async fn committed_rows_in_range(
    store: &IndexedDbStore,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    reverse: bool,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, IndexedDbError> {
    let range = key_range(start, end)?;
    if matches!(range, KeyRange::Empty) {
        return Ok(Vec::new());
    }
    let transaction = store.begin(IdbTransactionMode::Readonly, Scope::Kv)?;
    let object_store = transaction
        .object_store(KV_STORE)
        .map_err(|value| dom_error(&value))?;
    let request: IdbRequest = match (&range, reverse) {
        (KeyRange::All, false) => object_store.open_cursor(),
        (KeyRange::Bounded(range), false) => object_store.open_cursor_with_range(range.as_ref()),
        (KeyRange::All, true) => object_store
            .open_cursor_with_range_and_direction(&JsValue::UNDEFINED, IdbCursorDirection::Prev),
        (KeyRange::Bounded(range), true) => object_store
            .open_cursor_with_range_and_direction(range.as_ref(), IdbCursorDirection::Prev),
        (KeyRange::Empty, _) => return Ok(Vec::new()),
    }
    .map_err(|value| dom_error(&value))?;
    let outcome = TransactionOutcome::new(transaction);
    let page = read_cursor_page(request, usize::MAX).await;
    readonly_result(outcome.await)?;
    page?
        .rows
        .into_iter()
        .map(|(key, value)| Ok((bytes_from_js(&key, "key")?, bytes_from_js(&value, "value")?)))
        .collect()
}

fn bound_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(value) => Bound::Included(value),
        Bound::Excluded(value) => Bound::Excluded(value),
    }
}
