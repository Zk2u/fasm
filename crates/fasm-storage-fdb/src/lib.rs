//! FoundationDB-backed [`KvStore`] backend with native directory support.
//!
//! Directories are FDB's own: this backend uses FoundationDB's
//! [`DirectoryLayer`] (the default layer: directory metadata under `0xFE`,
//! content prefixes allocated by the high-contention allocator as packed
//! `i64`s), so a directory's keyspace is whatever prefix the layer allocated
//! to it. The flat backends (`fasm-storage-btreemap`, `fasm-storage-redb`)
//! port this same layout verbatim on top of their raw storage. Two handles
//! are offered over the same database:
//!
//! - [`FdbTransaction`] — a read-write view over one FoundationDB
//!   transaction. Reads see the transaction's own buffered writes; [`Commit`]
//!   commits it. A directory is created (by the layer, inside this
//!   transaction) on the first `set` in it.
//! - [`FdbReadOnlyStore`] — a read-only view that opens a fresh transaction
//!   per operation. It never creates anything: a missing directory is empty,
//!   and mutations are rejected with [`FdbStorageError::ReadOnlyMutation`].
//!
//! All keys are relative to their directory; the layer's prefix is added and
//! stripped so callers never see it.
//!
//! # Directory paths and data regions
//!
//! The layer refuses to open or create an empty path, and the layer's own
//! root has no content prefix of its own — so the fasm root directory is a
//! single reserved child of the layer root, and every fasm directory is a
//! descendant of it: fasm `[]` maps to the layer path
//! `[ROOT_PATH_SEGMENT]`, and fasm `[s₀, …]` maps to
//! `[ROOT_PATH_SEGMENT, s₀, …]`. The anchor is always the first element, so
//! no user directory can collide with it: reaching exactly the anchor alone
//! requires the empty fasm directory, which is the root itself.
//!
//! A directory's data region is `[prefix, next_prefix(prefix))` — the shared
//! `fasm_storage::next_prefix` successor, **not** the layer's own `range()`
//! end (`prefix‖0xFF`): fasm keys are arbitrary bytes, and a key whose first
//! byte is `0xFF` would otherwise sort at or past the layer's end and fall
//! outside the directory's own range.
//!
//! # Single-transaction scans
//!
//! One `range()` call corresponds to exactly one FoundationDB transaction (for
//! the read-only handle, opened lazily inside the stream and held for the
//! duration of the iteration). FDB transactions have a soft limit of ~5
//! seconds, after which reads return `transaction_too_old`. Because every page
//! in one `range()` call shares that transaction's read version, the budget
//! covers the whole scan, not each page: a caller iterating a very large range
//! (or slowly enough that the total scan would exceed the window) must split
//! the range itself and reissue from a cursor key. The read-only handle owns
//! its transaction and retries a retryable page-fetch error via
//! `Transaction::on_error` (FDB's backoff, then the same cursor again); pages
//! observed after a retry are at the post-reset snapshot. The write handle
//! borrows its transaction from the caller, so a page-fetch error is surfaced
//! to the caller, which owns the retry policy for the whole transaction.
//!
//! # Multiple directory creations
//!
//! The layer's `create_or_open` documents that creating several new paths
//! in one transaction requires care (each new prefix is claimed from a
//! high-contention allocator, and a racing claim surfaces as a commit
//! conflict). In this backend a single [`FdbTransaction`] may lazily
//! create several new directories — one `set` per missing directory — and
//! the outcome is still well-defined: a colliding creation makes the
//! commit fail with a retryable conflict error, and the caller retries the
//! transaction (this backend does not auto-retry commits). Opening a path
//! that already exists within the transaction is an idempotent open, not a
//! creation.
//!
//! # Empty and inverted ranges
//!
//! A bound pair that names no keys — an empty window, or a start that sorts
//! after the end — is answered locally and never reaches FoundationDB: `range`
//! returns an empty stream (the read-only handle opens no transaction for it)
//! and `clear_range` is a no-op. FoundationDB reads such a range leniently and
//! returns nothing, but an inverted `clear_range` raises `inverted_range`
//! into the transaction's deferred error, which the next operation on that
//! transaction re-raises.

use std::{collections::VecDeque, ops::Bound, sync::Arc};

use foundationdb::{
    Database, FdbError, KeySelector, RangeOption, TransactOption, Transaction,
    TransactionCommitError,
    directory::{Directory, DirectoryError, DirectoryLayer},
    options,
    tuple::hca::HcaError,
};
use thiserror::Error;

use fasm_storage::{
    Commit, KeyError, KvDirNav, KvPair, KvStore, KvStream, RetryableStorageError, bound_to_owned,
    flatdir::ROOT_PATH_SEGMENT, is_empty_range, next_prefix, validate_dir,
};

/// An error from the FoundationDB storage backend.
#[derive(Debug, Error)]
pub enum FdbStorageError {
    /// FoundationDB API error.
    #[error("foundationdb: {0}")]
    Fdb(#[from] FdbError),
    /// FoundationDB commit error.
    #[error("transaction commit: {0}")]
    Commit(#[from] TransactionCommitError),
    /// A directory path failed validation (a non-UTF-8 segment, a root
    /// removal).
    #[error(transparent)]
    Key(#[from] KeyError),
    /// FoundationDB directory-layer error. The layer's error type implements
    /// neither `Display` nor `std::error::Error`, so it is carried as its
    /// debug rendering; missing-directory cases never reach this variant —
    /// reads and clears translate them to empty results.
    #[error("foundationdb directory: {0}")]
    Directory(String),
    /// A returned key escaped the resolved directory's range. This is a
    /// backend invariant failure, not a caller condition: the layer bounds
    /// every scan by the directory's own range.
    #[error("range returned a key outside the resolved directory")]
    PrefixViolation,
    /// A mutation was attempted through a read-only handle.
    #[error("mutation attempted through read-only foundationdb state handle")]
    ReadOnlyMutation,
}

impl TryFrom<FdbStorageError> for FdbError {
    type Error = FdbStorageError;

    fn try_from(value: FdbStorageError) -> Result<Self, Self::Error> {
        match value {
            FdbStorageError::Fdb(err) => Ok(err),
            other => Err(other),
        }
    }
}

impl RetryableStorageError for FdbStorageError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::Fdb(err) => err.is_retryable(),
            Self::Commit(err) => err.is_retryable_not_committed(),
            Self::Key(_) | Self::Directory(_) | Self::PrefixViolation | Self::ReadOnlyMutation => {
                false
            }
        }
    }
}

/// A directory resolved inside one transaction: its content prefix and the
/// exclusive raw end of its data region — the `next_prefix` successor of
/// the prefix, which bounds every raw key the layer can write under it.
struct ResolvedDir {
    prefix: Vec<u8>,
    end: Vec<u8>,
}

/// Validate a directory path and turn it into the layer's `Vec<String>`
/// form under the backend's reserved anchor. The segment comes from the
/// core so all backends agree on it (see the module docs).
fn dir_path(dir: &[&[u8]]) -> Result<Vec<String>, FdbStorageError> {
    validate_dir(dir)?;
    // `validate_dir` accepted the path, so every segment is valid UTF-8;
    // fail loudly if that invariant regresses instead of aliasing a
    // non-UTF-8 segment to a replacement character.
    let mut path = Vec::with_capacity(dir.len() + 1);
    path.push(ROOT_PATH_SEGMENT.to_string());
    path.extend(dir.iter().map(|seg| {
        core::str::from_utf8(seg)
            .expect("segment validated above")
            .to_string()
    }));
    Ok(path)
}

/// Whether a directory-layer error means "the directory is not there" —
/// which reads and clears translate to empty results, not failures.
fn is_missing(e: &DirectoryError) -> bool {
    matches!(
        e,
        DirectoryError::DirectoryDoesNotExists | DirectoryError::PathDoesNotExists
    )
}

/// Wrap a directory-layer error. The two variants that carry the
/// underlying `FdbError` keep it: the layer's operations perform real
/// transaction reads and writes, so their retryable failures (1020
/// `not_committed`, 1037 `process_behind`, ...) must stay retryable for
/// the caller's retry loop and for `transact_boxed`. Every other variant
/// is structural and non-retryable; the layer's error type is neither
/// `Display` nor `std::error::Error`, so those are carried as their debug
/// rendering.
fn dir_err(e: DirectoryError) -> FdbStorageError {
    match e {
        DirectoryError::FdbError(fdb) => FdbStorageError::Fdb(fdb),
        DirectoryError::HcaError(HcaError::FdbError(fdb)) => FdbStorageError::Fdb(fdb),
        other => FdbStorageError::Directory(format!("{other:?}")),
    }
}

fn join(prefix: &[u8], key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + key.len());
    out.extend_from_slice(prefix);
    out.extend_from_slice(key);
    out
}

/// The immediate raw successor of a data key: the key with a `0x00`
/// appended. Every raw key strictly greater than `prefix‖key` is either an
/// extension of it — hence greater than `prefix‖key‖[0x00]` — or differs at
/// a byte at or after the key's end — hence not less than
/// `prefix‖key‖[0x00]`. So a range ending at `Excluded(just_after(…))`
/// includes the key and nothing after it.
fn just_after(prefix: &[u8], key: &[u8]) -> Vec<u8> {
    let mut out = join(prefix, key);
    out.push(0x00);
    out
}

fn start_selector(prefix: &[u8], dir_start: &[u8], bound: Bound<Vec<u8>>) -> KeySelector<'static> {
    match bound {
        Bound::Included(key) => KeySelector::first_greater_or_equal(join(prefix, &key)),
        Bound::Excluded(key) => KeySelector::first_greater_than(join(prefix, &key)),
        // Unbounded: the directory's data region starts at its prefix.
        Bound::Unbounded => KeySelector::first_greater_or_equal(dir_start.to_vec()),
    }
}

fn end_selector(prefix: &[u8], dir_end: &[u8], bound: Bound<Vec<u8>>) -> KeySelector<'static> {
    match bound {
        Bound::Included(key) => KeySelector::first_greater_than(join(prefix, &key)),
        Bound::Excluded(key) => KeySelector::first_greater_or_equal(join(prefix, &key)),
        // Unbounded: the directory's data region ends at next_prefix.
        Bound::Unbounded => KeySelector::first_greater_or_equal(dir_end.to_vec()),
    }
}

/// The raw `clear_range` bounds for a key bound pair inside `d`. `None` is a
/// no-op: the pair names no keys (empty or inverted).
fn clear_bounds(
    d: &ResolvedDir,
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let lo = match start {
        Bound::Included(key) => join(&d.prefix, &key),
        Bound::Excluded(key) => just_after(&d.prefix, &key),
        // Unbounded: the directory's content starts at its prefix.
        Bound::Unbounded => d.prefix.clone(),
    };
    let hi = match end {
        Bound::Included(key) => just_after(&d.prefix, &key),
        Bound::Excluded(key) => join(&d.prefix, &key),
        // Unbounded: the region end is exclusive and reserved-free.
        Bound::Unbounded => d.end.clone(),
    };
    if lo >= hi {
        // An inverted pair would raise `inverted_range` into the
        // transaction's deferred error.
        None
    } else {
        Some((lo, hi))
    }
}

/// A read-write KV view over one FoundationDB transaction. Reads see the
/// transaction's own buffered writes; [`Commit`] commits it.
///
/// The caller owns the transaction's lifetime: uncommitted on drop, and the
/// transaction is single-use (commit or drop, not both).
#[derive(Debug)]
pub struct FdbTransaction {
    tx: Transaction,
    layer: DirectoryLayer,
}

/// A read-only KV view that opens a fresh FoundationDB transaction per
/// operation. It never creates directories.
#[derive(Clone)]
pub struct FdbReadOnlyStore {
    db: Arc<Database>,
    layer: DirectoryLayer,
}

impl std::fmt::Debug for FdbReadOnlyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FdbReadOnlyStore").finish_non_exhaustive()
    }
}

/// Build the FDB range option for one directory scan. `prefix` is the
/// directory's raw data prefix, `region_end` the raw region end (exclusive).
/// Returns `None` when the mapped bounds are empty or inverted — FDB rejects
/// such requests, so the stream yields nothing instead of failing.
fn range_option(
    prefix: &[u8],
    region_end: &[u8],
    start: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    reverse: bool,
) -> Option<RangeOption<'static>> {
    // Effective raw bounds, for the empty/inverted check.
    let start_raw: Vec<u8> = match &start {
        Bound::Unbounded => prefix.to_vec(),
        Bound::Included(k) | Bound::Excluded(k) => join(prefix, k),
    };
    let end_raw: Vec<u8> = match &end_bound {
        Bound::Unbounded => region_end.to_vec(),
        Bound::Included(k) | Bound::Excluded(k) => join(prefix, k),
    };
    let start_b = match start {
        Bound::Unbounded | Bound::Included(_) => Bound::Included(start_raw.as_slice()),
        Bound::Excluded(_) => Bound::Excluded(start_raw.as_slice()),
    };
    // The effective range always ends *before* `end_raw`: `Excluded` and
    // `Unbounded` end selectors are first_greater_or_equal (an exclusive
    // boundary), and `Included` is first_greater_than, whose boundary sits
    // after the key — but the raw key itself is the inclusive boundary.
    let end_b = match end_bound {
        Bound::Unbounded => Bound::Excluded(end_raw.as_slice()),
        Bound::Included(_) => Bound::Included(end_raw.as_slice()),
        Bound::Excluded(_) => Bound::Excluded(end_raw.as_slice()),
    };
    if is_empty_range(&start_b, &end_b) {
        return None;
    }
    let mut opt = RangeOption::from((
        start_selector(prefix, prefix, start),
        end_selector(prefix, region_end, end_bound),
    ));
    opt.reverse = reverse;
    opt.mode = options::StreamingMode::Iterator;
    Some(opt)
}

impl FdbTransaction {
    /// Wraps an open transaction. Directories are allocated by the default
    /// directory layer (metadata under `0xFE`, content prefixes in
    /// `0x00`–`0xFD`); see the module docs.
    pub fn new(tx: Transaction) -> Self {
        Self::with_layer(tx, DirectoryLayer::default())
    }

    /// Wraps an open transaction with a consumer-provided directory layer.
    /// A custom placement must keep the metadata region and the data
    /// regions disjoint the way the default does, or the layout's
    /// disjointness assumptions no longer hold.
    pub fn with_layer(tx: Transaction, layer: DirectoryLayer) -> Self {
        Self { tx, layer }
    }

    /// The wrapped FDB transaction, for consumers that work the engine
    /// directly around fasm operations.
    pub fn tx(&self) -> &Transaction {
        &self.tx
    }

    /// Resolve `path` inside this transaction. With `create`, the layer
    /// creates the directory (and any missing parents) when absent — the lazy
    /// materialisation a first `set` relies on. Without it, an absent
    /// directory is reported as `None` (the empty-result contract) rather than
    /// an error.
    async fn resolve(
        &self,
        path: &[String],
        create: bool,
    ) -> Result<Option<ResolvedDir>, FdbStorageError> {
        let out = if create {
            self.layer
                .create_or_open(&self.tx, path, None, None)
                .await
                .map_err(dir_err)?
        } else {
            match self.layer.open(&self.tx, path, None).await {
                Ok(out) => out,
                Err(e) if is_missing(&e) => return Ok(None),
                Err(e) => return Err(dir_err(e)),
            }
        };
        // `bytes` borrows the output; copy the prefix before it is dropped.
        let prefix = out.bytes().map_err(dir_err)?.to_vec();
        // The region end is the shared `next_prefix` successor, not the
        // layer's `range()` end (`prefix‖0xFF`): a data key starting with
        // `0xFF` would otherwise sit outside the directory's own range.
        // An allocated prefix (whose first byte is a tuple int code
        // below `0xFF`) always has one.
        let end = region_end(&prefix)?;
        Ok(Some(ResolvedDir { prefix, end }))
    }
}

/// The exclusive raw end of a directory's data region: the `next_prefix`
/// successor of its content prefix. `None` from `next_prefix` means the
/// prefix is entirely `0xFF` (or empty) — a state the layer's allocator
/// does not produce — reported as a structural error rather than an
/// unbounded region.
fn region_end(prefix: &[u8]) -> Result<Vec<u8>, FdbStorageError> {
    next_prefix(prefix).ok_or_else(|| {
        FdbStorageError::Directory(
            "directory content prefix has no bounded region end (it is all 0xFF)".into(),
        )
    })
}

/// The boxed future type inside a write-handle [`KvStream`].
type ScanFuture<'a> = core::pin::Pin<
    Box<
        dyn core::future::Future<
                Output = Result<Option<(KvPair, KvStream<'a, FdbStorageError>)>, FdbStorageError>,
            > + Send
            + 'a,
    >,
>;

/// One step of the write-handle scan: drain the buffer, else fetch the next
/// page of the directory range. No retry: the transaction is the caller's,
/// and its retry policy is the caller's (see the module docs).
///
/// A plain function (not `async fn`) mirroring the read-only path: the
/// stream's continuation lifetime is the caller's, and `KvStream` is
/// invariant in its lifetime.
fn scan_head<'a>(
    tx: &'a Transaction,
    prefix: Arc<Vec<u8>>,
    opt: Option<RangeOption<'static>>,
    iteration: usize,
    buffered: VecDeque<KvPair>,
) -> ScanFuture<'a> {
    Box::pin(async move {
        let mut opt = opt;
        let mut iteration = iteration;
        let mut buffered = buffered;
        loop {
            if let Some(pair) = buffered.pop_front() {
                let next =
                    KvStream::new(scan_head(tx, Arc::clone(&prefix), opt, iteration, buffered));
                return Ok(Some((pair, next)));
            }

            let Some(current) = opt.take() else {
                return Ok(None);
            };

            let values = tx.get_range(&current, iteration, false).await?;
            opt = current.clone().next_range(&values);
            iteration += 1;

            for kv in values.into_iter() {
                let key = kv
                    .key()
                    .strip_prefix(prefix.as_slice())
                    .ok_or(FdbStorageError::PrefixViolation)?
                    .to_vec();
                buffered.push_back(KvPair {
                    key,
                    value: kv.value().to_vec(),
                });
            }
        }
    })
}

impl FdbReadOnlyStore {
    /// Wraps a database handle. Reads open a fresh transaction per operation;
    /// nothing is ever created.
    pub fn new(db: Arc<Database>) -> Self {
        Self::with_layer(db, DirectoryLayer::default())
    }

    /// Wraps a database handle with a consumer-provided directory layer;
    /// see [`FdbTransaction::with_layer`] for the placement contract.
    pub fn with_layer(db: Arc<Database>, layer: DirectoryLayer) -> Self {
        Self { db, layer }
    }

    /// The wrapped FDB database handle.
    pub fn db(&self) -> &Database {
        &self.db
    }

    /// Resolve `path` inside `tx` without creating (a missing directory is
    /// `None`).
    async fn resolve(
        tx: &Transaction,
        layer: &DirectoryLayer,
        path: &[String],
    ) -> Result<Option<ResolvedDir>, FdbStorageError> {
        match layer.open(tx, path, None).await {
            Ok(out) => {
                let prefix = out.bytes().map_err(dir_err)?.to_vec();
                let end = region_end(&prefix)?;
                Ok(Some(ResolvedDir { prefix, end }))
            }
            Err(e) if is_missing(&e) => Ok(None),
            Err(e) => Err(dir_err(e)),
        }
    }
}

/// The boxed future type inside a read-only [`KvStream`].
type OwnedScanFuture<'a> = core::pin::Pin<
    Box<
        dyn core::future::Future<
                Output = Result<Option<(KvPair, KvStream<'a, FdbStorageError>)>, FdbStorageError>,
            > + Send
            + 'a,
    >,
>;

/// One step of a read-only scan, owning the transaction by value. On a
/// retryable error during a page fetch, `Transaction::on_error` applies FDB's
/// backoff and returns the transaction reset; the same cursor is retried.
/// Pages observed after a retry are at the post-reset snapshot.
///
/// A plain function (not `async fn`) because the stream's continuation
/// lifetime is the caller's: `KvStream` is invariant in its lifetime, and the
/// owned transaction inside borrows nothing, so any `'a` works.
fn owned_scan_head<'a>(
    tx: Transaction,
    prefix: Arc<Vec<u8>>,
    opt: Option<RangeOption<'static>>,
    iteration: usize,
    buffered: VecDeque<KvPair>,
) -> OwnedScanFuture<'a> {
    Box::pin(async move {
        let mut tx = tx;
        let mut opt = opt;
        let mut iteration = iteration;
        let mut buffered = buffered;
        loop {
            if let Some(pair) = buffered.pop_front() {
                let next = KvStream::new(owned_scan_head(
                    tx,
                    Arc::clone(&prefix),
                    opt,
                    iteration,
                    buffered,
                ));
                return Ok(Some((pair, next)));
            }

            let Some(current) = opt.take() else {
                return Ok(None);
            };

            let values = loop {
                match tx.get_range(&current, iteration, false).await {
                    Ok(v) => break v,
                    Err(e) => match tx.on_error(e).await {
                        Ok(reset) => {
                            tx = reset;
                            // The transaction is reset; retry the same cursor.
                        }
                        // `on_error` rejected the retry: report the
                        // original page-fetch error (the first cause).
                        Err(_) => return Err(FdbStorageError::Fdb(e)),
                    },
                }
            };
            // `next_range` consumes `self`; clone the cursor (cheap — it is
            // two key selectors and a few flags) so it can be advanced.
            opt = current.clone().next_range(&values);
            iteration += 1;

            for kv in values.into_iter() {
                let key = kv
                    .key()
                    .strip_prefix(prefix.as_slice())
                    .ok_or(FdbStorageError::PrefixViolation)?
                    .to_vec();
                buffered.push_back(KvPair {
                    key,
                    value: kv.value().to_vec(),
                });
            }
        }
    })
}

impl KvStore for FdbTransaction {
    type Error = FdbStorageError;

    async fn get(&self, dir: &[&[u8]], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let path = dir_path(dir)?;
        let Some(d) = self.resolve(&path, false).await? else {
            return Ok(None);
        };
        Ok(self
            .tx
            .get(&join(&d.prefix, key), false)
            .await?
            .map(|value| value.to_vec()))
    }

    async fn set(&mut self, dir: &[&[u8]], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let path = dir_path(dir)?;
        // The layer creates the directory (and missing parents) inside this
        // transaction; `create_or_open` always yields a directory.
        let d = self
            .resolve(&path, true)
            .await?
            .expect("create_or_open yields a directory");
        self.tx.set(&join(&d.prefix, key), value);
        Ok(())
    }

    async fn delete(&mut self, dir: &[&[u8]], key: &[u8]) -> Result<(), Self::Error> {
        let path = dir_path(dir)?;
        // A missing directory has nothing to delete; no directory is created.
        if let Some(d) = self.resolve(&path, false).await? {
            self.tx.clear(&join(&d.prefix, key));
        }
        Ok(())
    }

    fn range<'a>(
        &'a self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        let path = match dir_path(dir) {
            Ok(path) => path,
            Err(e) => return KvStream::failed(e),
        };
        if is_empty_range(&start, &end) {
            // Answered locally; no FDB scan is opened for a range that names
            // no keys.
            return KvStream::empty();
        }
        let start = bound_to_owned(start);
        let end = bound_to_owned(end);
        KvStream::new(async move {
            // Resolve the directory in this transaction. A read never
            // creates: a missing directory is an empty scan, not an error.
            let Some(d) = self.resolve(&path, false).await? else {
                return Ok(None);
            };
            let prefix = Arc::new(d.prefix);
            let opt = range_option(&prefix, &d.end, start, end, reverse);
            scan_head(&self.tx, prefix, opt, 1, VecDeque::new()).await
        })
    }

    async fn clear_range(
        &mut self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        let path = dir_path(dir)?;
        let Some(d) = self.resolve(&path, false).await? else {
            // A missing directory has nothing to clear.
            return Ok(());
        };
        let start = bound_to_owned(start);
        let end = bound_to_owned(end);
        let Some((lo, hi)) = clear_bounds(&d, start, end) else {
            return Ok(());
        };
        self.tx.clear_range(&lo, &hi);
        Ok(())
    }
}

impl KvDirNav for FdbTransaction {
    async fn list_dirs(&self, dir: &[&[u8]]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let path = dir_path(dir)?;
        // `list` errors on a missing directory; the contract is an empty list.
        match self.layer.list(&self.tx, &path).await {
            Ok(mut names) => {
                names.sort();
                Ok(names.into_iter().map(|name| name.into_bytes()).collect())
            }
            Err(e) if is_missing(&e) => Ok(Vec::new()),
            Err(e) => Err(dir_err(e)),
        }
    }

    async fn dir_exists(&self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        let path = dir_path(dir)?;
        self.layer.exists(&self.tx, &path).await.map_err(dir_err)
    }

    async fn remove_dir(&mut self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        if dir.is_empty() {
            return Err(KeyError::RootNotRemovable.into());
        }
        let path = dir_path(dir)?;
        // `remove_if_exists` is natively recursive (subdirectories and all
        // contents) and reports `false` for a missing directory.
        self.layer
            .remove_if_exists(&self.tx, &path)
            .await
            .map_err(dir_err)
    }
}

impl Commit for FdbTransaction {
    type Error = FdbStorageError;

    async fn commit(self) -> Result<(), Self::Error> {
        self.tx.commit().await?;
        Ok(())
    }
}

impl KvStore for FdbReadOnlyStore {
    type Error = FdbStorageError;

    async fn get(&self, dir: &[&[u8]], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let path = dir_path(dir)?;
        self.db
            .transact_boxed(
                (path, key.to_vec()),
                |tx, data| {
                    let layer = self.layer.clone();
                    Box::pin(async move {
                        let Some(d) = Self::resolve(tx, &layer, &data.0).await? else {
                            return Ok(None);
                        };
                        Ok(tx
                            .get(&join(&d.prefix, &data.1), false)
                            .await?
                            .map(|value| value.to_vec()))
                    })
                },
                TransactOption::idempotent(),
            )
            .await
    }

    async fn set(&mut self, _dir: &[&[u8]], _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        Err(FdbStorageError::ReadOnlyMutation)
    }

    async fn delete(&mut self, _dir: &[&[u8]], _key: &[u8]) -> Result<(), Self::Error> {
        Err(FdbStorageError::ReadOnlyMutation)
    }

    /// Scan a range of keys, streaming each page lazily.
    ///
    /// # Single-transaction semantics
    ///
    /// **One `range()` call corresponds to exactly one FoundationDB
    /// transaction**, opened lazily inside the stream and held open for the
    /// duration of the iteration. Every page read shares that transaction's
    /// read version, so a scan that completes without retries observes a
    /// single consistent FoundationDB snapshot.
    ///
    /// FDB transactions have a soft limit of ~5 seconds (after which reads
    /// start returning `transaction_too_old`). Because *all* pages in one
    /// `range()` call run inside the same transaction, that budget covers the
    /// entire scan, not each page. Callers iterating very large key ranges (or
    /// processing each item slowly enough that the total scan time would
    /// exceed FDB's transaction window) **must split the range up
    /// themselves** — pick an intermediate cursor key from the last item of
    /// one batch, then issue a fresh `range()` starting from that key.
    ///
    /// On retryable errors during a page fetch (`transaction_too_old`,
    /// `process_behind`, ...) the implementation calls `Transaction::on_error`,
    /// which applies FDB's recommended backoff and resets the transaction.
    /// Pages observed after a retry are at the post-reset snapshot.
    fn range<'a>(
        &'a self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        let path = match dir_path(dir) {
            Ok(path) => path,
            Err(e) => return KvStream::failed(e),
        };
        if is_empty_range(&start, &end) {
            // Answered locally; no FoundationDB transaction is opened for a
            // range that names no keys.
            return KvStream::empty();
        }
        let db = Arc::clone(&self.db);
        let layer = self.layer.clone();
        let start = bound_to_owned(start);
        let end = bound_to_owned(end);
        KvStream::new(async move {
            let tx = match db.create_trx() {
                Ok(tx) => tx,
                Err(e) => return Err(FdbStorageError::from(e)),
            };
            let d = match Self::resolve(&tx, &layer, &path).await? {
                Some(d) => d,
                // A missing directory is an empty scan; the transaction is
                // dropped without committing anything.
                None => return Ok(None),
            };
            // The raw-level empty/inverted check: user bounds can be
            // non-empty yet map outside the allocated region (a start that
            // sorts past the region end). `None` answers the scan locally.
            let Some(opt) = range_option(&d.prefix, &d.end, start, end, reverse) else {
                return Ok(None);
            };
            owned_scan_head(tx, Arc::new(d.prefix), Some(opt), 1, VecDeque::new()).await
        })
    }

    async fn clear_range(
        &mut self,
        _dir: &[&[u8]],
        _start: Bound<&[u8]>,
        _end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        Err(FdbStorageError::ReadOnlyMutation)
    }
}

impl KvDirNav for FdbReadOnlyStore {
    async fn list_dirs(&self, dir: &[&[u8]]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let path = dir_path(dir)?;
        let layer = self.layer.clone();
        self.db
            .transact_boxed(
                path,
                move |tx, path| {
                    let layer = layer.clone();
                    Box::pin(async move {
                        match layer.list(tx, path).await {
                            Ok(mut names) => {
                                names.sort();
                                Ok(names
                                    .into_iter()
                                    .map(|name| name.into_bytes())
                                    .collect::<Vec<_>>())
                            }
                            Err(e) if is_missing(&e) => Ok(Vec::new()),
                            Err(e) => Err(dir_err(e)),
                        }
                    })
                },
                TransactOption::idempotent(),
            )
            .await
    }

    async fn dir_exists(&self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        let path = dir_path(dir)?;
        let layer = self.layer.clone();
        self.db
            .transact_boxed(
                path,
                move |tx, path| {
                    let layer = layer.clone();
                    Box::pin(async move { layer.exists(tx, path).await.map_err(dir_err) })
                },
                TransactOption::idempotent(),
            )
            .await
    }

    /// Removing through a read-only handle is rejected, even for a missing
    /// directory: the rejection is about the handle, not the directory.
    async fn remove_dir(&mut self, _dir: &[&[u8]]) -> Result<bool, Self::Error> {
        Err(FdbStorageError::ReadOnlyMutation)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod dir_err_tests {
    use super::*;

    /// Layer-wrapped FDB errors keep their retryability; structural
    /// layer errors stay non-retryable. No cluster is needed: the FDB
    /// error predicates are a C-level lookup.
    #[test]
    fn dir_err_keeps_retryable_fdb_errors_retryable() {
        let e = dir_err(DirectoryError::FdbError(FdbError::from_code(1020)));
        assert!(matches!(e, FdbStorageError::Fdb(_)));
        assert!(e.is_retryable());
        // The `transact_boxed` path extracts the inner error.
        let back = FdbError::try_from(e).expect("the inner FDB error");
        assert_eq!(back.code(), 1020);

        let e = dir_err(DirectoryError::HcaError(HcaError::FdbError(
            FdbError::from_code(1037),
        )));
        assert!(matches!(e, FdbStorageError::Fdb(_)));
        assert!(e.is_retryable());

        let e = dir_err(DirectoryError::DirAlreadyExists);
        assert!(matches!(e, FdbStorageError::Directory(_)));
        assert!(!e.is_retryable());
    }
}
