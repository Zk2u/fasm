//! The [`KvStore`] trait: an ordered async byte-to-byte map.

use core::error::Error;
use core::ops::Bound;

use async_trait::async_trait;

use crate::error::RetryableStorageError;
use crate::stream::KvStream;

/// Async ordered key-value store.
///
/// This trait abstracts over in-memory maps and the near-term backend targets,
/// redb and FoundationDB. A browser backend is deferred: it needs a `?Send`
/// formulation and an async test mode. Every operation is async so that
/// network-backed and transactional engines fit without a blocking shim.
///
/// # Ordering
///
/// Keys are **arbitrary byte sequences** ordered **lexicographically by byte
/// value**, exactly as [`Ord for [u8]`](slice) defines it. Shorter keys sort
/// before longer keys that extend them, so `[1] < [1, 0] < [1, 0, 0] < [2]`.
/// This is what makes `prefix ++ key` a usable namespacing scheme (see
/// [`prefix_range`](crate::prefix_range)) and is not negotiable per backend:
/// a store whose engine orders differently must translate.
///
/// The empty key is a legal key and sorts first.
///
/// # Range scans
///
/// [`range`](KvStore::range) returns a continuation-based [`KvStream`]. This
/// enables backends to fetch incrementally, but does not require it: a backend
/// may prefetch pages or materialize a snapshot. [`KvStream::take`] limits
/// continuation polling, not necessarily backend fetches.
///
/// Bounds are given in the caller's key space. A range whose keyed `start`
/// sorts after its keyed `end` is empty. Equal keyed bounds are empty when
/// either bound is excluded; two included equal bounds select that one key.
/// Backends whose native range API rejects empty bounds should normalize them
/// with [`is_empty_range`](crate::is_empty_range).
///
/// # Namespacing
///
/// The trait deliberately has no notion of tables, column families or
/// namespaces. **Implementations are responsible for any key prefixing**;
/// [`ScopedKvStore`](crate::ScopedKvStore) is the generic prefix-plumbing
/// adapter for any backend; its public access to the inner store means it is not
/// itself a capability boundary.
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
/// The `&self` methods return `Send` futures that capture `&Self`, which is
/// only `Send` when `Self: Sync`. Every implementor therefore already had to be
/// `Sync`; stating it as a supertrait is what lets generic wrappers such as
/// [`ScopedKvStore`](crate::ScopedKvStore) be written over an arbitrary
/// `KV: KvStore` at all.
#[async_trait]
pub trait KvStore: Send + Sync {
    /// The error type for this store.
    type Error: Error + RetryableStorageError + Send + Sync + 'static;

    /// Get a value by key.
    ///
    /// Returns `Ok(Some(value))` if the key is present, `Ok(None)` if it is not.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Set a key-value pair, overwriting any existing value for the key.
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    /// Delete a key.
    ///
    /// Deleting an absent key is a no-op, not an error.
    async fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error>;

    /// Check whether a key exists.
    ///
    /// The default implementation goes through [`get`](KvStore::get) and
    /// therefore pays for transferring the value. Backends with a cheaper
    /// existence probe should override it.
    async fn exists(&self, key: &[u8]) -> Result<bool, Self::Error> {
        Ok(self.get(key).await?.is_some())
    }

    /// Scan a range of keys in lexicographic order.
    ///
    /// Results are exposed through the returned continuation-based [`KvStream`].
    ///
    /// # Arguments
    ///
    /// * `start` — start bound (included / excluded / unbounded)
    /// * `end` — end bound (included / excluded / unbounded)
    /// * `reverse` — if `true`, produce the same set of pairs in descending key
    ///   order. `reverse` changes the iteration direction only; it never
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
    ///     // All keys from "a" (inclusive) to "z" (exclusive).
    ///     let _ = store
    ///         .range(Bound::Included(b"a"), Bound::Excluded(b"z"), false)
    ///         .collect()
    ///         .await?;
    ///
    ///     // All keys starting from "start".
    ///     let _ = store
    ///         .range(Bound::Included(b"start"), Bound::Unbounded, false)
    ///         .collect()
    ///         .await?;
    ///
    ///     // Everything, newest key first.
    ///     let _ = store
    ///         .range(Bound::Unbounded, Bound::Unbounded, true)
    ///         .collect()
    ///         .await?;
    ///     Ok(())
    /// }
    /// # fn main() {}
    /// ```
    fn range<'a>(
        &'a self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error>;

    /// Delete every key in a range.
    ///
    /// Semantically identical to scanning the range and deleting each key, but
    /// backends with a native range delete should use it. Clearing a range that
    /// matches nothing is a no-op, not an error. The same empty-bound rules as
    /// [`range`](KvStore::range) apply, including inverted keyed bounds.
    async fn clear_range(
        &mut self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), Self::Error>;
}
