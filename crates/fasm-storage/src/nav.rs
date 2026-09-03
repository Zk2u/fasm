//! Directory navigation: the extension trait over [`KvStore`].

use core::future::Future;

use crate::store::KvStore;

/// Directory navigation over a [`KvStore`].
///
/// The data plane addresses data within a directory; this trait
/// addresses the directories themselves. An implementation lists
/// directories from the store's own mapping: the flat backends scan one
/// contiguous mapping range (O(children), not O(subtree keys)), the FDB
/// backend asks its native directory layer.
///
/// All three methods validate their directory path with
/// [`validate_dir`](crate::validate_dir) before touching the engine.
// See `KvStore`: auto-trait bounds are carried by the supertraits' markers.
#[allow(async_fn_in_trait)]
pub trait KvDirNav: KvStore {
    /// List the immediate child directory names under `dir`, as segment
    /// bytes, sorted ascending.
    ///
    /// A missing `dir` yields an empty list, not an error —
    /// [`dir_exists`](crate::nav::KvDirNav::dir_exists) is how to tell "no children"
    /// from "not there". Output is sorted at this boundary so the ordering
    /// is deterministic regardless of the engine's.
    fn list_dirs(&self, dir: &[&[u8]]) -> impl Future<Output = Result<Vec<Vec<u8>>, Self::Error>>;

    /// Whether the directory exists.
    ///
    /// "Exists" means the directory is materialised: on the flat backends
    /// its child row is present (a directory with zero data keys still
    /// exists); the root exists iff the store's anchor node exists, so a
    /// fresh, never-written store's root does not; on FDB the layer's
    /// mapping entry is present.
    fn dir_exists(&self, dir: &[&[u8]]) -> impl Future<Output = Result<bool, Self::Error>>;

    /// Remove `dir` **recursively**: its subdirectories (child rows and
    /// data) and all of `dir`'s own key data.
    ///
    /// A data-destruction operation: everything under `dir` is gone, and it
    /// cannot be recovered from the store. `Ok(true)` if a directory
    /// existed and was removed, `Ok(false)` if it did not exist. The root
    /// `[]` is not removable — [`KeyError::RootNotRemovable`](crate::KeyError::RootNotRemovable).
    ///
    /// The whole removal is one transaction on transactional backends.
    fn remove_dir(&mut self, dir: &[&[u8]]) -> impl Future<Output = Result<bool, Self::Error>>;
}
