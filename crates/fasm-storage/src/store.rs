//! The [`KvStore`] trait: an async byte-to-byte map scoped by directory.

use core::error::Error;
use core::future::Future;
use core::ops::Bound;

use crate::error::RetryableStorageError;
use crate::maybe_send::{MaybeSend, MaybeSync};
use crate::stream::KvStream;

/// Async ordered key-value store, scoped by directory.
///
/// This trait abstracts over in-memory maps and the near-term backend targets,
/// redb and FoundationDB. Store types and method futures use [`MaybeSend`]
/// (and store types also use [`MaybeSync`]): these mean `Send`/`Sync` on native
/// targets but impose no thread-safety requirement in a browser, where storage
/// handles may hold `JsValue`s. The browser exception deliberately targets only
/// `wasm32-unknown-unknown`, rather than `target_family = "wasm"`, because targets
/// such as `wasm32-wasip1-threads` have real threads. Every operation is async so
/// that network-backed and transactional engines fit without a blocking shim.
///
/// # Directories and keys
///
/// Every operation names a **directory** and a **key**. A directory is a
/// sequence of segments (`&[&[u8]]`; the empty slice is the root directory);
/// a key is an arbitrary byte sequence. The pair is the store's real key:
/// the same key bytes in different directories are distinct entries, and a
/// scan of one directory never yields another's keys.
///
/// **Directory segments are UTF-8** (the trait-level contract): every
/// operation validates its directory with [`validate_dir`](crate::validate_dir) and rejects a
/// non-UTF-8 segment with [`KeyError::DirSegmentNotUtf8`](crate::KeyError)
/// before touching the engine. Keys have no such restriction.
///
/// Directories are created **lazily by the first write**: a `set` into a
/// never-written directory allocates it (and any missing ancestor). Reads on
/// a missing directory are empty, not an error — `get` answers `None`,
/// `range` an empty stream. Existence is a query, [`KvDirNav::dir_exists`](crate::nav::KvDirNav::dir_exists)
/// (crate root) — a directory with zero keys still exists.
///
/// # Ordering
///
/// Keys are **arbitrary byte sequences** ordered **lexicographically by byte
/// value**, exactly as [`Ord for [u8]`](slice) defines it, within their
/// directory. Shorter keys sort before longer keys that extend them, so
/// `[1] < [1, 0] < [1, 0, 0] < [2]`. This is what makes a resolved
/// directory prefix a usable namespacing scheme and is not negotiable per
/// backend: a store whose engine orders differently must translate.
///
/// The empty key is a legal key and sorts first.
///
/// # Range scans
///
/// [`range`](KvStore::range) scans **one exact directory** between two key
/// bounds and returns a continuation-based [`KvStream`] of
/// [`KvPair`](crate::KvPair) whose `key` is the within-directory key — the
/// directory is implicit in the scan. This enables backends to fetch
/// incrementally, but does not require it: a backend may prefetch pages or
/// materialize a snapshot. [`KvStream::take`] limits continuation polling,
/// not necessarily backend fetches.
///
/// Bounds are over the within-directory key. A range whose keyed `start`
/// sorts after its keyed `end` is empty. Equal keyed bounds are empty when
/// either bound is excluded; two included equal bounds select that one key.
///
/// Scans across directories (subtree scans, multi-directory scans) are not
/// expressible in one call; the directory dimension is exact-match only.
///
/// # Atomicity
///
/// Transactional backends **buffer writes until commit**. Reads issued through
/// a handle must observe that handle's own buffered writes (read-your-writes),
/// but nothing becomes visible to other handles, and nothing is durable, until
/// the atomic commit is applied. `Ok(())` confirms that outcome; a
/// non-retryable commit error may leave it unknown.
///
/// This matters because the state machines built on top perform multi-key
/// updates — a swap row plus its registry summary, an index insertion plus the
/// row it points at — whose exactly-once guarantees depend on all-or-nothing
/// application. A partially applied session can double-credit, duplicate a
/// withdrawal, or strand a reservation forever.
/// The in-memory backend (the `fasm-storage-btreemap` crate) provides the same
/// buffering, read-your-writes, and drop-rollback contract with no durability:
/// a crash loses the base map, so it is test/simulation-only — the
/// transaction contract itself does not differ.
///
/// # Errors
///
/// The associated error type must be a real [`Error`] and must answer
/// [`RetryableStorageError::is_retryable`], because the layer above needs to
/// distinguish "the transaction conflicted, rerun the transition" from "this
/// data is corrupt, fail closed".
///
/// # Why `Sync`
///
/// Every method future is explicitly [`MaybeSend`]: it is `Send` on native
/// targets, so a generic caller can move (for example) `store.get(..)` across a
/// task boundary when `S: KvStore + Sync`. In a browser the marker does not
/// require `Send`, allowing both the store and its futures to remain
/// thread-local. [`MaybeSync`] supplies the native `Sync` requirement needed by
/// futures that capture `&Self`.
pub trait KvStore: MaybeSend + MaybeSync {
    /// The error type for this store.
    type Error: Error + RetryableStorageError + MaybeSend + MaybeSync + 'static;

    /// Get a value by directory and key.
    ///
    /// Returns `Ok(Some(value))` if the key is present in the directory,
    /// `Ok(None)` if it is not or the directory does not exist.
    fn get(
        &self,
        dir: &[&[u8]],
        key: &[u8],
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + MaybeSend;

    /// Set a key-value pair in a directory, overwriting any existing value
    /// for the key. The directory (and any missing ancestor) is allocated
    /// if needed.
    fn set(
        &mut self,
        dir: &[&[u8]],
        key: &[u8],
        value: &[u8],
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;

    /// Delete a key from a directory.
    ///
    /// Deleting an absent key or a key in a missing directory is a no-op,
    /// not an error.
    fn delete(
        &mut self,
        dir: &[&[u8]],
        key: &[u8],
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;

    /// Check whether a key exists in a directory.
    ///
    /// The default implementation goes through [`get`](KvStore::get) and
    /// therefore pays for transferring the value. Backends with a cheaper
    /// existence probe should override it.
    fn exists(
        &self,
        dir: &[&[u8]],
        key: &[u8],
    ) -> impl Future<Output = Result<bool, Self::Error>> + MaybeSend {
        async move { Ok(self.get(dir, key).await?.is_some()) }
    }

    /// Scan the keys of one directory between two key bounds, in
    /// lexicographic order.
    ///
    /// Results are exposed through the returned continuation-based
    /// [`KvStream`]; each pair's key is the within-directory key.
    ///
    /// # Arguments
    ///
    /// * `dir` — the exact directory to scan (validated UTF-8 segments).
    /// * `start` — start bound over the within-directory key
    ///   (included / excluded / unbounded)
    /// * `end` — end bound (included / excluded / unbounded)
    /// * `reverse` — if `true`, produce the same set of pairs in descending
    ///   key order. `reverse` changes the iteration direction only; it never
    ///   reinterprets which bound is which.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ops::Bound;
    ///
    /// use fasm_storage::KvStore;
    ///
    /// async fn demo<S: KvStore>(store: &S) -> Result<(), S::Error> {
    ///     // In directory `["pool"]`, all keys from "a" (inclusive) to
    ///     // "z" (exclusive).
    ///     let _ = store
    ///         .range(&[b"pool"], Bound::Included(b"a"), Bound::Excluded(b"z"), false)
    ///         .collect()
    ///         .await?;
    ///
    ///     // In the root directory, all keys starting from "start".
    ///     let _ = store.range(&[], Bound::Included(b"start"), Bound::Unbounded, false);
    ///
    ///     // The whole root directory, newest key first.
    ///     let _ = store.range(&[], Bound::Unbounded, Bound::Unbounded, true);
    ///     Ok(())
    /// }
    /// # fn main() {}
    /// ```
    fn range<'a>(
        &'a self,
        dir: &[&[u8]],
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error>;

    /// Delete every key in a range within one directory.
    ///
    /// Semantically identical to scanning the range and deleting each key,
    /// but backends with a native range delete should use it. Clearing a
    /// range that matches nothing is a no-op, not an error — including a
    /// missing directory. The same empty-bound rules as
    /// [`range`](KvStore::range) apply, including inverted keyed bounds.
    ///
    /// Clearing a directory's whole range (`Unbounded` to `Unbounded`)
    /// removes all of its keys but **not** the directory itself:
    /// [`KvDirNav::dir_exists`](crate::nav::KvDirNav::dir_exists) (crate root) still answers `true` after.
    fn clear_range(
        &mut self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;
}
