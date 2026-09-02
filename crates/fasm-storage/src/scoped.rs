//! [`ScopedKvStore`]: prefix plumbing over any [`KvStore`].

use core::fmt;
use core::future::Future;
use core::hash::{Hash, Hasher};
use core::ops::Bound;
use std::collections::hash_map::DefaultHasher;

use async_trait::async_trait;
use thiserror::Error;

use crate::commit::Commit;
use crate::error::RetryableStorageError;
use crate::keyspace::{bound_as_slice, next_prefix};
use crate::store::KvStore;
use crate::stream::{KvPair, KvStream};

/// Errors produced by a [`ScopedKvStore`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ScopedKvError<E> {
    /// The underlying store failed.
    #[error("scoped store backend: {0}")]
    Backend(#[source] E),

    /// The backend returned a key that does not start with this store's prefix.
    ///
    /// A correct backend cannot do this: every bound handed down is confined to
    /// the prefix namespace. Seeing it means the backend ignored the bounds, or
    /// the keyspace is corrupt. Either way the scoped store refuses to guess
    /// what the caller-space key would have been and **fails closed**, because
    /// the alternative — silently returning a foreign key, or truncating it to
    /// whatever survives the strip — would expose data from a different prefix
    /// through the scoped surface.
    ///
    /// This is the one place a key belonging to a *different* namespace is
    /// guaranteed to be in hand, and callers log storage errors from retry
    /// loops. So the variant carries lengths and fingerprints rather than the
    /// bytes: enough to correlate two reports of the same violation, not enough
    /// to reconstruct the foreign key or this scope's prefix. Build it with
    /// [`ScopedKvError::prefix_violation`], which is the only thing that knows
    /// how a fingerprint is derived.
    ///
    /// The fingerprints are non-cryptographic and deliberately truncated: a
    /// short key can be recovered from one by brute force, so this is a
    /// redaction of convenience for logs, not a secrecy guarantee.
    #[error(
        "scoped store received a {key_len}-byte key (fingerprint {key_fingerprint:08x}) \
         outside its {prefix_len}-byte prefix (fingerprint {prefix_fingerprint:08x}); \
         backend ignored range bounds or keyspace is corrupt"
    )]
    PrefixViolation {
        /// Length of the offending key, as the backend returned it.
        key_len: usize,
        /// Truncated fingerprint of the offending key.
        key_fingerprint: u32,
        /// Length of the prefix the key was required to carry.
        prefix_len: usize,
        /// Truncated fingerprint of that prefix.
        prefix_fingerprint: u32,
    },
}

/// A short, non-cryptographic fingerprint of some key material.
///
/// See [`ScopedKvError::PrefixViolation`] for what it is and is not good for.
fn fingerprint(bytes: &[u8]) -> u32 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    // The low half is as good as the whole for correlation, and half as much
    // material to work backwards from.
    hasher.finish() as u32
}

impl<E> ScopedKvError<E> {
    /// Build a [`ScopedKvError::PrefixViolation`] without retaining either
    /// byte string.
    pub fn prefix_violation(prefix: &[u8], key: &[u8]) -> Self {
        Self::PrefixViolation {
            key_len: key.len(),
            key_fingerprint: fingerprint(key),
            prefix_len: prefix.len(),
            prefix_fingerprint: fingerprint(prefix),
        }
    }
}

impl<E: RetryableStorageError> RetryableStorageError for ScopedKvError<E> {
    fn is_retryable(&self) -> bool {
        match self {
            // Snapshot-before-erasure does not apply here: the concrete backend
            // error is preserved, so its own answer is still reachable.
            Self::Backend(err) => err.is_retryable(),
            // Corruption does not heal on retry.
            Self::PrefixViolation { .. } => false,
        }
    }
}

/// A prefix-scoped view of another [`KvStore`].
///
/// Every key the caller supplies is written and read as `prefix ++ key`, and
/// every key handed back has the prefix stripped. The prefix never appears in
/// the [`KvStore`] surface, and scan results are verified to carry the expected
/// prefix before they are exposed to the caller. A mismatch fails closed with
/// [`ScopedKvError::PrefixViolation`].
///
/// The fail-closed check covers **returned scan keys only**. `clear_range`
/// returns no keys to verify, so the wrapper cannot detect a backend that
/// ignores its bounds and over-deletes. Honouring range-delete bounds remains
/// the backend's [`KvStore`] obligation.
///
/// This type is prefix plumbing, **not a capability boundary**. [`Self::inner`],
/// [`Self::inner_mut`], and [`Self::into_inner`] deliberately expose the wrapped
/// store, through which code may access keys outside this prefix. Callers that
/// need structural isolation must not hand out those escape hatches. Higher
/// layers own that restriction; the scoped handles held by state-machine crates
/// are the actual capability boundary.
///
/// # Range bound mapping
///
/// Bounds arrive in caller space and are mapped into prefixed space:
///
/// | caller bound | mapped bound |
/// |---|---|
/// | `Included(k)` | `Included(prefix ++ k)` |
/// | `Excluded(k)` | `Excluded(prefix ++ k)` |
/// | `Unbounded` (start) | `Included(prefix)` |
/// | `Unbounded` (end) | `Excluded(next_prefix(prefix))`, or `Unbounded` |
///
/// An unbounded *start* becomes `Included(prefix)` because the prefix itself is
/// the smallest key in the namespace — it is what the empty caller key maps to.
///
/// An unbounded *end* becomes the prefix successor. When the prefix is all
/// `0xFF` bytes (or empty) there is no successor and the namespace genuinely
/// runs to the end of the keyspace, so the mapped bound is
/// [`Bound::Unbounded`] — see [`crate::keyspace::next_prefix`]. Appending sentinel `0xFF` bytes
/// instead would silently drop keys such as `[0xFF, 0xFF, 0xFF]` from the
/// `[0xFF, 0xFF]` namespace.
///
/// # Prefix-freeness
///
/// Prefixes used over the same backend must be prefix-free. For example, `a`
/// and `ab` are not independent scopes: caller keys under `a` can enter the
/// `ab` namespace. Use fixed-width prefix components or terminate each
/// component with a byte that cannot occur inside it. This crate does not
/// enforce prefix-freeness.
///
/// # Commit
///
/// [`Commit`] forwards straight through, unwrapped: prefixing cannot fail at
/// commit time, so there is no scoped error to introduce and the caller keeps
/// the backend's own commit error type.
#[derive(Clone, PartialEq, Eq)]
pub struct ScopedKvStore<KV> {
    inner: KV,
    prefix: Vec<u8>,
}

impl<KV> fmt::Debug for ScopedKvStore<KV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ScopedKvStore(..)")
    }
}

impl<KV> ScopedKvStore<KV> {
    /// Wrap `inner`, namespacing every key under `prefix`.
    ///
    /// An empty prefix is legal and yields a transparent pass-through view.
    /// Prefixes sharing one backend must be prefix-free; see the type-level
    /// "Prefix-freeness" section.
    pub fn new(inner: KV, prefix: impl Into<Vec<u8>>) -> Self {
        Self {
            inner,
            prefix: prefix.into(),
        }
    }

    /// The prefix every key in this scope carries.
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// Borrow the wrapped store, which still sees prefixed keys.
    pub fn inner(&self) -> &KV {
        &self.inner
    }

    /// Mutably borrow the wrapped store, which still sees prefixed keys.
    pub fn inner_mut(&mut self) -> &mut KV {
        &mut self.inner
    }

    /// Unwrap, returning the underlying store.
    pub fn into_inner(self) -> KV {
        self.inner
    }

    /// Build a further-nested scope by appending `suffix` to this prefix.
    ///
    /// Consumes `self`, since the outer view and the nested view would
    /// otherwise both claim ownership of the same backend.
    pub fn scoped(self, suffix: impl AsRef<[u8]>) -> Self {
        let mut prefix = self.prefix;
        prefix.extend_from_slice(suffix.as_ref());
        Self {
            inner: self.inner,
            prefix,
        }
    }

    /// `prefix ++ key`.
    fn prefixed(&self, key: &[u8]) -> Vec<u8> {
        let mut full = Vec::with_capacity(self.prefix.len() + key.len());
        full.extend_from_slice(&self.prefix);
        full.extend_from_slice(key);
        full
    }

    /// Map a caller-space start bound into prefixed space.
    fn start_bound(&self, bound: Bound<&[u8]>) -> Bound<Vec<u8>> {
        match bound {
            Bound::Included(key) => Bound::Included(self.prefixed(key)),
            Bound::Excluded(key) => Bound::Excluded(self.prefixed(key)),
            // The prefix itself is the first key in the namespace: it is what
            // the empty caller key maps to, and the empty key sorts first.
            Bound::Unbounded => Bound::Included(self.prefix.clone()),
        }
    }

    /// Map a caller-space end bound into prefixed space.
    fn end_bound(&self, bound: Bound<&[u8]>) -> Bound<Vec<u8>> {
        match bound {
            Bound::Included(key) => Bound::Included(self.prefixed(key)),
            Bound::Excluded(key) => Bound::Excluded(self.prefixed(key)),
            // One past the last key in the namespace — unless there is no such
            // key, in which case the namespace runs to the end of the keyspace.
            Bound::Unbounded => match next_prefix(&self.prefix) {
                Some(next) => Bound::Excluded(next),
                None => Bound::Unbounded,
            },
        }
    }
}

/// Strip `prefix` from a backend key, failing closed if it is not there.
fn strip<E>(prefix: &[u8], key: Vec<u8>) -> Result<Vec<u8>, ScopedKvError<E>> {
    match key.strip_prefix(prefix) {
        Some(stripped) => Ok(stripped.to_vec()),
        None => Err(ScopedKvError::prefix_violation(prefix, &key)),
    }
}

/// Lazily adapt an inner stream: strip the prefix off each key as it arrives.
///
/// Recursion is fine because [`KvStream`] boxes its future, so the adapter's
/// own future never contains itself.
fn scoped_stream<'a, E>(inner: KvStream<'a, E>, prefix: Vec<u8>) -> KvStream<'a, ScopedKvError<E>>
where
    E: Send + 'a,
{
    KvStream::new(async move {
        match inner.next().await.map_err(ScopedKvError::Backend)? {
            Some((pair, rest)) => {
                let key = strip(&prefix, pair.key)?;
                let pair = KvPair {
                    key,
                    value: pair.value,
                };
                let next = scoped_stream(rest, prefix);
                Ok(Some((pair, next)))
            }
            None => Ok(None),
        }
    })
}

#[async_trait]
impl<KV: KvStore> KvStore for ScopedKvStore<KV> {
    type Error = ScopedKvError<KV::Error>;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner
            .get(&self.prefixed(key))
            .await
            .map_err(ScopedKvError::Backend)
    }

    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let full = self.prefixed(key);
        self.inner
            .set(&full, value)
            .await
            .map_err(ScopedKvError::Backend)
    }

    async fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        let full = self.prefixed(key);
        self.inner
            .delete(&full)
            .await
            .map_err(ScopedKvError::Backend)
    }

    async fn exists(&self, key: &[u8]) -> Result<bool, Self::Error> {
        self.inner
            .exists(&self.prefixed(key))
            .await
            .map_err(ScopedKvError::Backend)
    }

    fn range<'a>(
        &'a self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        let start = self.start_bound(start);
        let end = self.end_bound(end);
        let inner = self
            .inner
            .range(bound_as_slice(&start), bound_as_slice(&end), reverse);
        scoped_stream(inner, self.prefix.clone())
    }

    async fn clear_range(
        &mut self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        let start = self.start_bound(start);
        let end = self.end_bound(end);
        self.inner
            .clear_range(bound_as_slice(&start), bound_as_slice(&end))
            .await
            .map_err(ScopedKvError::Backend)
    }
}

impl<KV: Commit> Commit for ScopedKvStore<KV> {
    type Error = KV::Error;

    fn commit(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.inner.commit()
    }
}
