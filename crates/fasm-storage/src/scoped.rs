//! [`ScopedKvStore`]: a directory-pinned view over any [`KvStore`].

use core::fmt;
use core::future::Future;
use core::ops::Bound;

use crate::commit::Commit;
use crate::nav::KvDirNav;
use crate::store::KvStore;
use crate::stream::KvStream;

/// A directory-pinned view of another [`KvStore`].
///
/// The store is pinned to a directory, and every operation's directory
/// argument is a **relative path from the pin**: the empty path names the
/// pinned directory itself, and a non-empty path names a subdirectory of it.
/// The inner store sees the joined absolute path; the caller never types the
/// pinned part again. Bounds pass through unchanged — they are always over
/// the within-directory key, so a scan yields keys of the directory the call
/// names.
///
/// The pinned directory (and any relative subdirectory) is created lazily
/// like any other (the first write allocates it); reads on a never-written
/// directory are empty.
///
/// This type is scope plumbing, **not a capability boundary**.
/// [`Self::inner`], [`Self::inner_mut`], and [`Self::into_inner`] deliberately
/// expose the wrapped store, through which code may access any directory.
/// Callers that need structural isolation must not hand out those escape
/// hatches. Higher layers own that restriction; the scoped handles held by
/// state-machine crates are the actual capability boundary.
///
/// # Commit
///
/// [`Commit`] forwards straight through, unwrapped: pinning a directory
/// cannot fail at commit time, so there is no scoped error to introduce and
/// the caller keeps the backend's own commit error type.
#[derive(Clone, PartialEq, Eq)]
pub struct ScopedKvStore<KV> {
    inner: KV,
    dir: Vec<Vec<u8>>,
}

impl<KV> fmt::Debug for ScopedKvStore<KV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedKvStore")
            .field("dir_segments", &self.dir.len())
            .finish()
    }
}

impl<KV> ScopedKvStore<KV> {
    /// Wrap `inner`, pinning every operation under the directory `dir`:
    /// a caller path `p` is issued to the inner store as `dir ++ p`.
    ///
    /// An empty directory pins the root and yields a transparent
    /// pass-through view.
    pub fn new(inner: KV, dir: impl Into<Vec<Vec<u8>>>) -> Self {
        Self {
            inner,
            dir: dir.into(),
        }
    }

    /// The pinned directory, as owned segments.
    pub fn dir(&self) -> &[Vec<u8>] {
        &self.dir
    }

    /// Borrow the wrapped store, which still sees any directory.
    pub fn inner(&self) -> &KV {
        &self.inner
    }

    /// Mutably borrow the wrapped store, which still sees any directory.
    pub fn inner_mut(&mut self) -> &mut KV {
        &mut self.inner
    }

    /// Unwrap, returning the underlying store.
    pub fn into_inner(self) -> KV {
        self.inner
    }

    /// Build a further-nested scope by appending one segment to this
    /// directory.
    ///
    /// Consumes `self`, since the outer view and the nested view would
    /// otherwise both claim ownership of the same backend.
    pub fn nested(self, seg: impl Into<Vec<u8>>) -> Self {
        let seg = seg.into();
        Self {
            inner: self.inner,
            dir: {
                let mut dir = self.dir;
                dir.push(seg);
                dir
            },
        }
    }
}

/// Join a pinned directory with a caller-supplied relative path, as a
/// per-op borrow slice.
///
/// A free function so the pin borrow is a `self.dir` *field* borrow, kept
/// disjoint from the `self.inner` borrow the same statement takes for the
/// forwarded call.
fn join_dir<'a>(pin: &'a [Vec<u8>], rel: &'a [&'a [u8]]) -> Vec<&'a [u8]> {
    pin.iter()
        .map(|s| s.as_slice())
        .chain(rel.iter().copied())
        .collect()
}

impl<KV: KvStore> KvStore for ScopedKvStore<KV> {
    type Error = KV::Error;

    async fn get(&self, dir: &[&[u8]], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get(&join_dir(&self.dir, dir), key).await
    }

    async fn set(&mut self, dir: &[&[u8]], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let d = join_dir(&self.dir, dir);
        self.inner.set(&d, key, value).await
    }

    async fn delete(&mut self, dir: &[&[u8]], key: &[u8]) -> Result<(), Self::Error> {
        let d = join_dir(&self.dir, dir);
        self.inner.delete(&d, key).await
    }

    async fn exists(&self, dir: &[&[u8]], key: &[u8]) -> Result<bool, Self::Error> {
        self.inner.exists(&join_dir(&self.dir, dir), key).await
    }

    fn range<'a>(
        &'a self,
        dir: &[&[u8]],
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        let d = join_dir(&self.dir, dir);
        self.inner.range(&d, start, end, reverse)
    }

    async fn clear_range(
        &mut self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        let d = join_dir(&self.dir, dir);
        self.inner.clear_range(&d, start, end).await
    }
}

impl<KV: KvDirNav> KvDirNav for ScopedKvStore<KV> {
    async fn list_dirs(&self, dir: &[&[u8]]) -> Result<Vec<Vec<u8>>, Self::Error> {
        KvDirNav::list_dirs(&self.inner, &join_dir(&self.dir, dir)).await
    }

    async fn dir_exists(&self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        KvDirNav::dir_exists(&self.inner, &join_dir(&self.dir, dir)).await
    }

    async fn remove_dir(&mut self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        KvDirNav::remove_dir(&mut self.inner, &join_dir(&self.dir, dir)).await
    }
}

impl<KV: Commit> Commit for ScopedKvStore<KV> {
    type Error = KV::Error;

    fn commit(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.inner.commit()
    }
}
