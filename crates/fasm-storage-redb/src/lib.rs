//! On-disk [`KvStore`] backend, backed by [redb](https://docs.rs/redb).
//!
//! A flat keyspace: every raw row lives in one redb table. The flat directory
//! layout from [`fasm_storage::flatdir`] sits on top of the table through the
//! [`RawKv`] seam, so the directory semantics are the shared engine's, not
//! this backend's. redb keeps its own page cache, and `get`/`set`/`range`
//! only touch in-memory B-tree/mmap state, so they run directly on the async
//! executor (wrapped in ready futures) — the database is the single source of
//! truth and reads hit it live.
//!
//! The commit fsync is handed off rather than run inline. Rather than spawn a
//! thread per commit, [`RedbStore`] owns **two long-lived threads** and no
//! per-operation pool: a commit thread that [`Commit`] hands the
//! [`WriteTransaction`] (which is `Send`) over a channel and awaits the reply
//! on, and an open thread that takes the non-parking open's blocking
//! `begin_write`. On constrained devices that is two predictable background
//! threads, not a pool.
//!
//! `range` is a stateless cursor: each pulled item re-seeks the B-tree for the
//! next key past the previous one, so `take(1)` (the last key) and paging read
//! only what they consume.
//!
//! # One write transaction at a time — [`RedbStore::transaction`] also blocks
//!
//! redb allows a single writer: [`Database::begin_write`] **blocks the calling
//! thread** until any outstanding write transaction commits or is dropped. So
//! opening a transaction is a second blocking operation, not just the commit.
//! [`RedbStore::transaction`] takes that wait on the calling thread;
//! [`RedbStore::transaction_nonparking`] takes it on the dedicated open thread
//! and suspends the caller instead, which is what a single-threaded executor
//! must use. Two rules follow, and neither can be enforced here, because a
//! blocked `begin_write` is indistinguishable from a legitimately contended
//! one:
//!
//! - **Never open a second write transaction while one is still outstanding on
//!   the same thread.** Waiting parks the only thread that could finish the
//!   transaction being waited on: a deadlock, not a delay.
//! - **Never hold a write transaction across an `await` on anything else** — no
//!   chain call, no UI hop. A transition is `stf(&mut tx).await` then
//!   `tx.commit().await`, and nothing in between. Every `await` a transaction is
//!   held across is a window in which some other thread's `begin_write` stalls,
//!   and that window includes the commit's fsync.

use core::ops::Bound;
use std::sync::Arc;

use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use fasm_storage::flatdir::{VERSION_KEY, parse_version};
use fasm_storage::{
    Commit, DataBounds, FlatEngine, FlatError, KvDirNav, KvPair, KvStore, KvStream, RawKv,
    RetryableStorageError, bound_as_slice,
};

/// The single flat table holding every raw row.
const TABLE: TableDefinition<'static, &'static [u8], &'static [u8]> = TableDefinition::new("kv");

fn backend<E: core::fmt::Display>(e: E) -> RedbBackendError {
    RedbBackendError::Backend(e.to_string())
}

/// Wrap a displayable failure as a full layout error (engine source), for
/// the store's public error type.
fn wrap<E: core::fmt::Display>(e: E) -> RedbStorageError {
    RedbStorageError(FlatError::Engine(backend(e)))
}

/// A backend-level failure, before the layout wraps it: a disk, transaction,
/// or lock error, or a write attempted through a read-only view.
#[derive(Debug, Error)]
pub enum RedbBackendError {
    /// A backend (disk, transaction, lock) failure. Carries human-readable
    /// detail for logs; not intended for direct display to end users.
    #[error("storage backend error: {0}")]
    Backend(String),
    /// A mutation was attempted through a read-only view.
    #[error("write attempted on a read-only store")]
    ReadOnly,
    /// The file holds content under a different layout version
    /// (checked at open; no migration).
    #[error("the store carries a different layout version")]
    LayoutVersionMismatch,
}

impl RetryableStorageError for RedbBackendError {
    fn is_retryable(&self) -> bool {
        // redb failures (a failed commit, an access error) are persistent, not
        // transient: retrying the same operation does not make it valid. Report
        // rather than loop.
        false
    }
}

/// The error type for the redb backend: the flat-layout error with the
/// backend's own failures as its engine source.
///
/// `Display` is transparent, so a core [`KeyError`](fasm_storage::KeyError)
/// (an invalid directory segment, a root removal) renders with its own
/// message.
#[derive(Debug, Error)]
#[error(transparent)]
#[repr(transparent)]
pub struct RedbStorageError(FlatError<RedbBackendError>);

impl From<FlatError<RedbBackendError>> for RedbStorageError {
    fn from(e: FlatError<RedbBackendError>) -> Self {
        Self(e)
    }
}

impl From<RedbBackendError> for RedbStorageError {
    fn from(e: RedbBackendError) -> Self {
        Self(FlatError::Engine(e))
    }
}

impl RetryableStorageError for RedbStorageError {
    fn is_retryable(&self) -> bool {
        self.0.is_retryable()
    }
}

/// A single decoded raw key/value entry from a cursor seek, or `None` at the
/// end of the range.
type Entry = Option<(Vec<u8>, Vec<u8>)>;

/// A commit handed to the dedicated db thread: run the write transaction's fsync
/// (the one blocking db operation) and reply with its result.
struct CommitJob {
    tx: WriteTransaction,
    reply: oneshot::Sender<Result<(), RedbStorageError>>,
}

/// An open handed to the dedicated open thread: run `begin_write` — which can
/// block until an outstanding writer ends — and reply with the transaction.
///
/// A separate thread from the commits, necessarily: an open parked in
/// `begin_write` is waiting for some transaction to end, and if that
/// transaction's commit sat queued behind it on the same thread, neither could
/// ever proceed.
struct OpenJob {
    reply: oneshot::Sender<Result<WriteTransaction, RedbStorageError>>,
}

/// A handle to an on-disk redb store. Cloneable; every clone shares the one
/// database and the one dedicated commit thread.
#[derive(Debug, Clone)]
pub struct RedbStore {
    db: Arc<Database>,
    committer: mpsc::UnboundedSender<CommitJob>,
    opener: mpsc::UnboundedSender<OpenJob>,
}

/// Check the layout version at open (fail-closed; no migration). A
/// fresh file (table absent, or no version row) opens; the engine
/// writes the version row lazily on the first directory creation.
/// A version row present with a version the engine does not support
/// (major > 1 or minor > 0, or not a 12-byte triple) rejects the open.
fn probe_layout_version(db: &Database) -> Result<(), RedbStorageError> {
    let tx = db.begin_read().map_err(wrap)?;
    let table = match tx.open_table(TABLE) {
        Ok(table) => Some(table),
        Err(redb::TableError::TableDoesNotExist(_)) => None,
        Err(e) => return Err(wrap(e)),
    };
    let Some(table) = table else {
        return Ok(());
    };
    match table.get(VERSION_KEY).map_err(wrap)? {
        Some(v) => match parse_version(v.value()) {
            Some((major, minor, _patch)) if major <= 1 && minor == 0 => Ok(()),
            _ => Err(RedbBackendError::LayoutVersionMismatch.into()),
        },
        None => Ok(()),
    }
}

impl RedbStore {
    /// Wraps a consumer-opened [`Database`] and spawns its dedicated threads.
    ///
    /// Pass a `Database` you opened yourself (your own `Database::builder()`
    /// options, growth/compression settings) to control the engine the way you
    /// would natively; [`RedbStore::open`] and [`RedbStore::in_memory`] are thin
    /// wrappers over this. The layout version is still probed here, so a file
    /// carrying a different layout version is rejected either way.
    ///
    /// Fails rather than panics if the threads cannot be spawned: a host may be
    /// built with `panic = "abort"`, where an `expect` here would take the whole
    /// process down over a recoverable resource shortage.
    pub fn with_db(db: Database) -> Result<Self, RedbStorageError> {
        probe_layout_version(&db)?;
        let db = Arc::new(db);
        let (committer, mut jobs) = mpsc::unbounded_channel::<CommitJob>();
        // One long-lived thread performs every commit's fsync — the blocking step
        // that must stay off the async executor. It exits when the last store
        // handle drops (the channel closes). No per-transaction threads are ever
        // spawned.
        std::thread::Builder::new()
            .name("fasm-redb-commit".to_string())
            .spawn(move || {
                while let Some(job) = jobs.blocking_recv() {
                    let outcome = job.tx.commit().map_err(wrap);
                    // A send failure means the caller stopped awaiting (its
                    // future was dropped), so the reply has nowhere to go. A
                    // *failed* commit still has to be reported: writes the
                    // caller believes landed did not, and this is the last place
                    // that knows.
                    if let Err(Err(e)) = job.reply.send(outcome) {
                        tracing::error!(
                            error = %e,
                            "a commit failed after its caller stopped awaiting the result",
                        );
                    }
                }
            })
            .map_err(wrap)?;
        // A second long-lived thread performs every non-parking open's
        // `begin_write`. Kept apart from the commit thread — see [`OpenJob`].
        let (opener, mut opens) = mpsc::unbounded_channel::<OpenJob>();
        let open_db = Arc::clone(&db);
        std::thread::Builder::new()
            .name("fasm-redb-open".to_string())
            .spawn(move || {
                while let Some(job) = opens.blocking_recv() {
                    let outcome = open_db.begin_write().map_err(wrap);
                    // A dropped reply means the caller stopped awaiting; the
                    // transaction it asked for is dropped uncommitted, which is
                    // a rollback of nothing.
                    let _ = job.reply.send(outcome);
                }
            })
            .map_err(wrap)?;
        Ok(Self {
            db,
            committer,
            opener,
        })
    }

    /// Opens (creating if absent) a store at `path`.
    ///
    /// Fails if the file holds a different layout version.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, RedbStorageError> {
        Self::with_db(Database::create(path).map_err(wrap)?)
    }

    /// Creates an ephemeral in-memory store (for tests).
    pub fn in_memory() -> Result<Self, RedbStorageError> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(wrap)?;
        Self::with_db(db)
    }

    /// The underlying redb [`Database`] this store wraps, for consumers that
    /// work the engine directly around fasm operations.
    pub fn raw(&self) -> &Database {
        &self.db
    }

    /// Opens a write transaction. Uncommitted on drop.
    ///
    /// **Blocks** until any write transaction already outstanding on this
    /// database ends — see the module docs. Not safe to call while this thread
    /// holds one.
    pub fn transaction(&self) -> Result<RedbTransaction, RedbStorageError> {
        Ok(RedbTransaction {
            tx: self.db.begin_write().map_err(wrap)?,
            committer: self.committer.clone(),
        })
    }

    /// Opens a write transaction without parking the calling thread.
    ///
    /// The wait for any outstanding writer happens on the dedicated open
    /// thread; this caller suspends instead of blocking, so a single-threaded
    /// executor keeps driving its other work while it waits its turn.
    pub async fn transaction_nonparking(&self) -> Result<RedbTransaction, RedbStorageError> {
        let (reply, answer) = oneshot::channel();
        self.opener.send(OpenJob { reply }).map_err(|_| {
            RedbStorageError(FlatError::Engine(RedbBackendError::Backend(
                "the database's open thread has exited".into(),
            )))
        })?;
        let tx = answer.await.map_err(|_| {
            RedbStorageError(FlatError::Engine(RedbBackendError::Backend(
                "the database's open thread dropped the request".into(),
            )))
        })??;
        Ok(RedbTransaction {
            tx,
            committer: self.committer.clone(),
        })
    }

    /// Opens a reader (a consistent read-only view of the committed database).
    pub fn reader(&self) -> Result<RedbReader, RedbStorageError> {
        Ok(RedbReader {
            tx: self.db.begin_read().map_err(wrap)?,
        })
    }
}

// ============================================================================
// RawKv views
// ============================================================================

/// Read the rows of one raw range, in the requested direction.
fn read_rows<T: ReadableTable<&'static [u8], &'static [u8]>>(
    table: &T,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    forward: bool,
) -> Result<Vec<KvPair>, RedbBackendError> {
    let mut iter = table.range::<&[u8]>((start, end)).map_err(backend)?;
    let mut rows = Vec::new();
    if forward {
        for row in iter.by_ref() {
            let (k, v) = row.map_err(backend)?;
            rows.push(KvPair {
                key: k.value().to_vec(),
                value: v.value().to_vec(),
            });
        }
    } else {
        while let Some(row) = iter.next_back() {
            let (k, v) = row.map_err(backend)?;
            rows.push(KvPair {
                key: k.value().to_vec(),
                value: v.value().to_vec(),
            });
        }
    }
    Ok(rows)
}

/// A raw read-only view over a committed reader.
///
/// A table that has never been written simply has no rows: `get` returns
/// `None` and `scan` yields nothing.
struct RedbReadView<'a> {
    tx: &'a ReadTransaction,
}

impl RawKv for RedbReadView<'_> {
    type Error = RedbBackendError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        match self.tx.open_table(TABLE) {
            Ok(table) => Ok(table.get(key).map_err(backend)?.map(|g| g.value().to_vec())),
            // A never-written table simply has no rows.
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(backend(e)),
        }
    }

    fn scan(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        forward: bool,
    ) -> Result<Vec<KvPair>, Self::Error> {
        match self.tx.open_table(TABLE) {
            Ok(table) => read_rows(&table, start, end, forward),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(Vec::new()),
            Err(e) => Err(backend(e)),
        }
    }

    fn insert(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        Err(RedbBackendError::ReadOnly)
    }

    fn delete(&mut self, _key: &[u8]) -> Result<(), Self::Error> {
        Err(RedbBackendError::ReadOnly)
    }

    fn clear_range(&mut self, _start: Bound<&[u8]>, _end: Bound<&[u8]>) -> Result<(), Self::Error> {
        Err(RedbBackendError::ReadOnly)
    }
}

/// A raw view over a write transaction: reads see the transaction's buffered
/// writes, and writes go into the transaction.
///
/// `WriteTransaction::open_table` borrows from the transaction (it takes a
/// shared reference), so one view can read and write without exclusivity:
/// the table handles are short-lived, scoped to one raw operation.
struct RedbWriteView<'a> {
    tx: &'a WriteTransaction,
}

impl RawKv for RedbWriteView<'_> {
    type Error = RedbBackendError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let table = self.tx.open_table(TABLE).map_err(backend)?;
        Ok(table.get(key).map_err(backend)?.map(|g| g.value().to_vec()))
    }

    fn scan(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        forward: bool,
    ) -> Result<Vec<KvPair>, Self::Error> {
        let table = self.tx.open_table(TABLE).map_err(backend)?;
        read_rows(&table, start, end, forward)
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let mut table = self.tx.open_table(TABLE).map_err(backend)?;
        table.insert(key, value).map_err(backend)?;
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        let mut table = self.tx.open_table(TABLE).map_err(backend)?;
        table.remove(key).map_err(backend)?;
        Ok(())
    }

    fn clear_range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<(), Self::Error> {
        let mut table = self.tx.open_table(TABLE).map_err(backend)?;
        // redb has no bulk range clear; `retain_in` with a `false` predicate is
        // the native primitive — it removes exactly the in-range entries in one
        // pass, with no per-key allocation or per-key point-delete. An empty or
        // inverted range is a no-op: its range iterator yields nothing.
        //
        // The explicit `::<&[u8], fn(..)>` turbofish is required: `retain_in`'s
        // `KR` type parameter cannot be inferred from a `(Bound, Bound)` range
        // tuple, and `F` must be nameable as a `fn` item (not a closure) so the
        // full turbofish can name both parameters.
        table
            .retain_in::<&[u8], fn(&[u8], &[u8]) -> bool>((start, end), clear_predicate)
            .map_err(backend)?;
        Ok(())
    }
}

/// The constant-`false` predicate for a `retain_in` range clear: every entry in
/// the range fails the predicate, so every entry in the range is removed.
/// A `fn` item (not a closure) so its type is nameable in `retain_in`'s
/// turbofish.
fn clear_predicate(_key: &[u8], _value: &[u8]) -> bool {
    false
}

// ============================================================================
// The lazy range cursor
// ============================================================================

/// Seeks the single first (or last, if `reverse`) raw row in `[start, end)`.
trait RawCursorStep {
    fn first_in_range(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<Entry, RedbBackendError>;
}

fn first_of<T: ReadableTable<&'static [u8], &'static [u8]>>(
    table: &T,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    reverse: bool,
) -> Result<Entry, RedbBackendError> {
    let mut iter = table.range::<&[u8]>((start, end)).map_err(backend)?;
    let entry = if reverse {
        iter.next_back()
    } else {
        iter.next()
    };
    match entry {
        Some(row) => {
            let (k, v) = row.map_err(backend)?;
            Ok(Some((k.value().to_vec(), v.value().to_vec())))
        }
        None => Ok(None),
    }
}

/// One lazy step of a directory scan: seek the first (or last) raw row in the
/// remaining raw range, strip the directory's prefix from the raw key, and
/// resume just past (or before) that row.
///
/// The raw range is a [`DataBounds`]: the resolved prefix plus owned raw
/// bounds over the engine's whole keyspace. Each step re-seeks the B-tree, so
/// `take(1)` and paging read only what they consume.
fn redb_dir_cursor<'a, T: RawCursorStep + Sync + 'a>(
    step: &'a T,
    bounds: DataBounds,
    reverse: bool,
) -> KvStream<'a, RedbStorageError> {
    KvStream::new(async move {
        match step.first_in_range(
            bound_as_slice(&bounds.start),
            bound_as_slice(&bounds.end),
            reverse,
        )? {
            None => Ok(None),
            Some((raw_key, value)) => {
                let plen = bounds.prefix.len();
                let key = raw_key[plen..].to_vec();
                let (next_start, next_end) = if reverse {
                    (bounds.start, Bound::Excluded(raw_key))
                } else {
                    (Bound::Excluded(raw_key), bounds.end)
                };
                let next = DataBounds {
                    prefix: bounds.prefix.clone(),
                    start: next_start,
                    end: next_end,
                };
                Ok(Some((
                    KvPair { key, value },
                    redb_dir_cursor(step, next, reverse),
                )))
            }
        }
    })
}

// ============================================================================
// The write transaction
// ============================================================================

/// A redb write transaction. Reads see its own buffered writes; [`Commit`]
/// hands the fsync to the store's dedicated db thread.
pub struct RedbTransaction {
    tx: WriteTransaction,
    committer: mpsc::UnboundedSender<CommitJob>,
}

impl core::fmt::Debug for RedbTransaction {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RedbTransaction").finish_non_exhaustive()
    }
}

impl RawCursorStep for RedbTransaction {
    fn first_in_range(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<Entry, RedbBackendError> {
        let table = self.tx.open_table(TABLE).map_err(backend)?;
        first_of(&table, start, end, reverse)
    }
}

impl KvStore for RedbTransaction {
    type Error = RedbStorageError;

    async fn get(&self, dir: &[&[u8]], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let view = RedbWriteView { tx: &self.tx };
        Ok(FlatEngine::new(view).get(dir, key)?)
    }

    async fn set(&mut self, dir: &[&[u8]], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let view = RedbWriteView { tx: &mut self.tx };
        FlatEngine::new(view)
            .set(dir, key, value)
            .map_err(RedbStorageError)
    }

    async fn delete(&mut self, dir: &[&[u8]], key: &[u8]) -> Result<(), Self::Error> {
        let view = RedbWriteView { tx: &mut self.tx };
        FlatEngine::new(view)
            .delete(dir, key)
            .map_err(RedbStorageError)
    }

    fn range<'a>(
        &'a self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        // Resolve the raw range synchronously. A missing directory, or a bound
        // pair that names no keys, is an empty stream; a layout error surfaces
        // as a failed stream rather than a panic on a sync path.
        let view = RedbWriteView { tx: &self.tx };
        match FlatEngine::new(view).data_bounds(dir, start, end) {
            Ok(Some(b)) => redb_dir_cursor(self, b, reverse),
            Ok(None) => KvStream::empty(),
            Err(e) => KvStream::failed(RedbStorageError(e)),
        }
    }

    async fn clear_range(
        &mut self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        let view = RedbWriteView { tx: &mut self.tx };
        FlatEngine::new(view)
            .clear_range(dir, start, end)
            .map_err(RedbStorageError)
    }
}

impl KvDirNav for RedbTransaction {
    async fn list_dirs(&self, dir: &[&[u8]]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let view = RedbWriteView { tx: &self.tx };
        Ok(FlatEngine::new(view).list_dirs(dir)?)
    }

    async fn dir_exists(&self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        let view = RedbWriteView { tx: &self.tx };
        Ok(FlatEngine::new(view).dir_exists(dir)?)
    }

    async fn remove_dir(&mut self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        let view = RedbWriteView { tx: &mut self.tx };
        Ok(FlatEngine::new(view).remove_dir(dir)?)
    }
}

impl Commit for RedbTransaction {
    type Error = RedbStorageError;

    async fn commit(self) -> Result<(), Self::Error> {
        // The fsync is the blocking step that must not run on the async
        // executor; hand it to the dedicated db thread and await the result,
        // rather than spawning a thread per commit. (Opening the transaction
        // blocks too, but on the caller's thread — see the module docs.)
        let (reply, result) = oneshot::channel();
        self.committer
            .send(CommitJob { tx: self.tx, reply })
            .map_err(|_| {
                RedbStorageError(FlatError::Engine(RedbBackendError::Backend(
                    "db commit thread has stopped".to_string(),
                )))
            })?;
        result.await.map_err(|_| {
            RedbStorageError(FlatError::Engine(RedbBackendError::Backend(
                "db commit thread dropped the reply".to_string(),
            )))
        })?
    }
}

// ============================================================================
// The reader
// ============================================================================

/// A redb reader: a consistent read-only view of the committed database.
/// Mutations return [`RedbBackendError::ReadOnly`] wrapped in the layout
/// error.
#[derive(Debug)]
pub struct RedbReader {
    tx: ReadTransaction,
}

impl RawCursorStep for RedbReader {
    fn first_in_range(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<Entry, RedbBackendError> {
        match self.tx.open_table(TABLE) {
            Ok(table) => first_of(&table, start, end, reverse),
            // A never-written table simply has no rows.
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(backend(e)),
        }
    }
}

impl KvStore for RedbReader {
    type Error = RedbStorageError;

    async fn get(&self, dir: &[&[u8]], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let view = RedbReadView { tx: &self.tx };
        Ok(FlatEngine::new(view).get(dir, key)?)
    }

    async fn set(&mut self, _dir: &[&[u8]], _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        Err(RedbStorageError(FlatError::Engine(
            RedbBackendError::ReadOnly,
        )))
    }

    async fn delete(&mut self, _dir: &[&[u8]], _key: &[u8]) -> Result<(), Self::Error> {
        Err(RedbStorageError(FlatError::Engine(
            RedbBackendError::ReadOnly,
        )))
    }

    fn range<'a>(
        &'a self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        let view = RedbReadView { tx: &self.tx };
        match FlatEngine::new(view).data_bounds(dir, start, end) {
            Ok(Some(b)) => redb_dir_cursor(self, b, reverse),
            Ok(None) => KvStream::empty(),
            Err(e) => KvStream::failed(RedbStorageError(e)),
        }
    }

    async fn clear_range(
        &mut self,
        _dir: &[&[u8]],
        _start: Bound<&[u8]>,
        _end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        Err(RedbStorageError(FlatError::Engine(
            RedbBackendError::ReadOnly,
        )))
    }
}

impl KvDirNav for RedbReader {
    async fn list_dirs(&self, dir: &[&[u8]]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let view = RedbReadView { tx: &self.tx };
        Ok(FlatEngine::new(view).list_dirs(dir)?)
    }

    async fn dir_exists(&self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        let view = RedbReadView { tx: &self.tx };
        Ok(FlatEngine::new(view).dir_exists(dir)?)
    }

    /// Removing through a reader fails with `ReadOnly` for an existing
    /// directory (the engine reaches its first raw delete); a missing
    /// directory still reports `Ok(false)` — no write is attempted.
    async fn remove_dir(&mut self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        let view = RedbReadView { tx: &self.tx };
        Ok(FlatEngine::new(view).remove_dir(dir)?)
    }
}

#[cfg(test)]
mod tests;
