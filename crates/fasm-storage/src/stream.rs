//! The consuming key-value continuation returned by range scans.

use core::fmt;
use core::future::Future;
use core::pin::Pin;

/// A key-value pair produced by a range scan.
///
/// Keys are always reported in the caller's own key space: a [`ScopedKvStore`]
/// strips its configured prefix before handing the pair back.
///
/// [`ScopedKvStore`]: crate::ScopedKvStore
#[derive(Clone, PartialEq, Eq)]
pub struct KvPair {
    /// The key bytes.
    pub key: Vec<u8>,
    /// The value bytes.
    pub value: Vec<u8>,
}

impl fmt::Debug for KvPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KvPair {{ key: <elided>, value: <{} bytes> }}",
            self.value.len()
        )
    }
}

/// The boxed future type inside a [`KvStream`].
type KvStreamFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<Option<(KvPair, KvStream<'a, E>)>, E>> + Send + 'a>>;

/// An async stream of key-value pairs.
///
/// This is a *consuming cons-list*: awaiting the stream yields either `None`
/// (exhausted) or one [`KvPair`] together with the stream carrying the remainder.
/// Consuming rather than borrowing means a backend can keep a live cursor, a
/// page buffer, or a transaction read handle inside the future without any of
/// it leaking into the caller's borrow graph.
///
/// The continuation shape enables lazy backends, but does not force a fetching
/// strategy. A backend may prefetch pages or materialize the range up front;
/// [`take`] stops polling continuations after its limit, but cannot undo such
/// backend work. The in-memory backend, for example, materializes its
/// snapshot up front.
///
/// # Cancellation
///
/// This stream and its async methods are consuming. Dropping a returned future,
/// as can happen when it loses a `select` or timeout race, loses both the
/// in-flight page and the continuation. To resume, reissue
/// [`KvStore::range`](crate::KvStore::range) from the last processed key; the
/// caller owns deduplication at that seam.
///
/// ```
/// use std::ops::Bound;
///
/// use fasm_storage::KvStore;
///
/// async fn demo<S: KvStore>(store: &S) -> Result<(), S::Error> {
///     let mut cursor = store.range(Bound::Unbounded, Bound::Unbounded, false);
///     while let Some((pair, rest)) = cursor.next().await? {
///         println!("key has {} bytes", pair.key.len());
///         cursor = rest;
///     }
///     Ok(())
/// }
/// # fn main() {}
/// ```
///
/// [`take`]: KvStream::take
#[must_use = "a KvStream does nothing unless awaited"]
pub struct KvStream<'a, E> {
    inner: KvStreamFuture<'a, E>,
}

impl<E> fmt::Debug for KvStream<'_, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("KvStream(..)")
    }
}

impl<'a, E> KvStream<'a, E> {
    /// Create a new stream from a future producing the head pair and the tail.
    ///
    /// The future must be `Send` so that scans can cross task boundaries; it is
    /// boxed, so backends are free to return `async` blocks of any shape.
    pub fn new<F>(fut: F) -> Self
    where
        F: Future<Output = Result<Option<(KvPair, KvStream<'a, E>)>, E>> + Send + 'a,
    {
        Self {
            inner: Box::pin(fut),
        }
    }

    /// Create an empty stream (no items).
    pub fn empty() -> Self
    where
        E: 'a,
    {
        Self {
            inner: Box::pin(async { Ok(None) }),
        }
    }

    /// Create a stream that fails on its first poll.
    ///
    /// Useful for backends whose `range` signature cannot return a `Result` but
    /// which can fail while *setting up* the scan (acquiring a snapshot,
    /// mapping bounds); the failure is deferred into the stream instead of
    /// being swallowed.
    pub fn failed(err: E) -> Self
    where
        E: Send + 'a,
    {
        Self {
            inner: Box::pin(async move { Err(err) }),
        }
    }

    /// Get the next item in the stream.
    ///
    /// Returns `Ok(Some((pair, next)))` if there is another item, or
    /// `Ok(None)` if the stream is exhausted.
    ///
    /// This consumes the stream. Dropping the returned future loses the
    /// in-flight page and continuation; resume by reissuing the range from the
    /// last processed key and deduplicate at the seam.
    pub async fn next(self) -> Result<Option<(KvPair, KvStream<'a, E>)>, E> {
        self.inner.await
    }

    /// Collect all remaining items into a vector.
    ///
    /// **Warning**: this loads the entire remaining range into memory. Prefer
    /// [`take`](Self::take) or [`for_each`](Self::for_each) for unbounded scans.
    ///
    /// This consumes the stream. Dropping the returned future loses the
    /// in-flight page and continuation; resume by reissuing the range from the
    /// last processed key and deduplicate at the seam.
    pub async fn collect(self) -> Result<Vec<KvPair>, E> {
        let mut results = Vec::new();
        let mut current = self;

        while let Some((pair, next)) = current.next().await? {
            results.push(pair);
            current = next;
        }

        Ok(results)
    }

    /// Collect up to `limit` items into a vector.
    ///
    /// Stops polling continuations as soon as `limit` items have been produced.
    /// A backend may already have prefetched or materialized additional items.
    ///
    /// This consumes the stream. Dropping the returned future loses the
    /// in-flight page and continuation; resume by reissuing the range from the
    /// last processed key and deduplicate at the seam.
    pub async fn take(self, limit: usize) -> Result<Vec<KvPair>, E> {
        let mut results = Vec::with_capacity(limit.min(64));
        let mut current = self;

        for _ in 0..limit {
            match current.next().await? {
                Some((pair, next)) => {
                    results.push(pair);
                    current = next;
                }
                None => break,
            }
        }

        Ok(results)
    }

    /// Apply a function to each item, stopping on the first *stream* error.
    ///
    /// The visitor is infallible and synchronous, so it can neither stop the
    /// walk early nor report a failure of its own. A consumer that needs either
    /// — a parse that can fail, a search that ends at the first match — should
    /// drive [`next`](Self::next) directly rather than reaching for this.
    ///
    /// This consumes the stream. Dropping the returned future loses the
    /// in-flight page and continuation; resume by reissuing the range from the
    /// last processed key and deduplicate at the seam.
    pub async fn for_each<F>(self, mut f: F) -> Result<(), E>
    where
        F: FnMut(KvPair),
    {
        let mut current = self;

        while let Some((pair, next)) = current.next().await? {
            f(pair);
            current = next;
        }

        Ok(())
    }
}
