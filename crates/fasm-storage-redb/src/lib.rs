//! On-disk [`KvStore`] backend, backed by [redb](https://docs.rs/redb).
//!
//! A flat keyspace: every key lives in one redb table. redb keeps its own page
//! cache, and `get`/`set`/`range` only touch in-memory B-tree/mmap state, so they
//! run directly on the async executor (wrapped in ready futures) — the database
//! is the single source of truth and reads hit it live.
//!
//! The commit fsync is handed off rather than run inline. Rather than spawn a
//! thread per commit, [`RedbStore`] owns **one dedicated db thread**: [`Commit`]
//! hands it the [`WriteTransaction`] (which is `Send`) over a channel and awaits
//! the reply. On constrained devices that is one predictable background thread,
//! not a pool.
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

use fasm_storage::{
    Commit, KvPair, KvStore, KvStream, RetryableStorageError, bound_as_slice, bound_to_owned,
};

/// The single flat table holding every key.
const TABLE: TableDefinition<'static, &'static [u8], &'static [u8]> = TableDefinition::new("kv");

fn backend<E: core::fmt::Display>(e: E) -> RedbStorageError {
    RedbStorageError::Backend(e.to_string())
}

/// An error from the redb storage backend.
#[derive(Debug, Error)]
pub enum RedbStorageError {
    /// A backend (disk, transaction, lock) failure. Carries human-readable
    /// detail for logs; not intended for direct display to end users.
    #[error("storage backend error: {0}")]
    Backend(String),
    /// A mutation was attempted through a read-only view.
    #[error("write attempted on a read-only store")]
    ReadOnly,
}

impl RetryableStorageError for RedbStorageError {
    fn is_retryable(&self) -> bool {
        // redb failures (a failed commit, an access error) are persistent, not
        // transient: retrying the same operation does not make it valid. Report
        // rather than loop.
        false
    }
}

/// A single decoded key/value entry from a range, or `None` at the end.
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

impl RedbStore {
    /// Wraps an opened database and spawns its dedicated commit thread.
    ///
    /// Fails rather than panics if the thread cannot be spawned: a host may be
    /// built with `panic = "abort"`, where an `expect` here would take the whole
    /// process down over a recoverable resource shortage.
    fn with_db(db: Database) -> Result<Self, RedbStorageError> {
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
                    let outcome = job.tx.commit().map_err(backend);
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
            .map_err(backend)?;
        // A second long-lived thread performs every non-parking open's
        // `begin_write`. Kept apart from the commit thread — see [`OpenJob`].
        let (opener, mut opens) = mpsc::unbounded_channel::<OpenJob>();
        let open_db = Arc::clone(&db);
        std::thread::Builder::new()
            .name("fasm-redb-open".to_string())
            .spawn(move || {
                while let Some(job) = opens.blocking_recv() {
                    let outcome = open_db.begin_write().map_err(backend);
                    // A dropped reply means the caller stopped awaiting; the
                    // transaction it asked for is dropped uncommitted, which is
                    // a rollback of nothing.
                    let _ = job.reply.send(outcome);
                }
            })
            .map_err(backend)?;
        Ok(Self {
            db,
            committer,
            opener,
        })
    }

    /// Opens (creating if absent) a store at `path`.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, RedbStorageError> {
        Self::with_db(Database::create(path).map_err(backend)?)
    }

    /// Creates an ephemeral in-memory store (for tests).
    pub fn in_memory() -> Result<Self, RedbStorageError> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(backend)?;
        Self::with_db(db)
    }

    /// Opens a write transaction. Uncommitted on drop.
    ///
    /// **Blocks** until any write transaction already outstanding on this
    /// database ends — see the module docs. Not safe to call while this thread
    /// holds one.
    pub fn transaction(&self) -> Result<RedbTransaction, RedbStorageError> {
        Ok(RedbTransaction {
            tx: self.db.begin_write().map_err(backend)?,
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
            RedbStorageError::Backend("the database's open thread has exited".into())
        })?;
        let tx = answer.await.map_err(|_| {
            RedbStorageError::Backend("the database's open thread dropped the request".into())
        })??;
        Ok(RedbTransaction {
            tx,
            committer: self.committer.clone(),
        })
    }

    /// Opens a reader (a consistent read-only view of the committed database).
    pub fn reader(&self) -> Result<RedbReader, RedbStorageError> {
        Ok(RedbReader {
            tx: self.db.begin_read().map_err(backend)?,
        })
    }
}

/// Seeks the single first (or last, if `reverse`) key/value in `[start, end)`.
trait FirstInRange {
    fn first_in_range(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<Entry, RedbStorageError>;
}

fn first_of<T: ReadableTable<&'static [u8], &'static [u8]>>(
    table: &T,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    reverse: bool,
) -> Result<Entry, RedbStorageError> {
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

/// One lazy step of a redb scan.
fn redb_cursor<'a, T: FirstInRange + Sync + 'a>(
    tx: &'a T,
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    reverse: bool,
) -> KvStream<'a, RedbStorageError> {
    KvStream::new(async move {
        match tx.first_in_range(bound_as_slice(&start), bound_as_slice(&end), reverse)? {
            None => Ok(None),
            Some((key, value)) => {
                let (next_start, next_end) = if reverse {
                    (start, Bound::Excluded(key.clone()))
                } else {
                    (Bound::Excluded(key.clone()), end)
                };
                Ok(Some((
                    KvPair { key, value },
                    redb_cursor(tx, next_start, next_end, reverse),
                )))
            }
        }
    })
}

/// A redb write transaction. Reads see its own buffered writes; [`Commit`] hands
/// the fsync to the store's dedicated db thread.
pub struct RedbTransaction {
    tx: WriteTransaction,
    committer: mpsc::UnboundedSender<CommitJob>,
}

impl core::fmt::Debug for RedbTransaction {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RedbTransaction").finish_non_exhaustive()
    }
}

impl FirstInRange for RedbTransaction {
    fn first_in_range(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<Entry, RedbStorageError> {
        let table = self.tx.open_table(TABLE).map_err(backend)?;
        first_of(&table, start, end, reverse)
    }
}

#[async_trait::async_trait]
impl KvStore for RedbTransaction {
    type Error = RedbStorageError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let table = self.tx.open_table(TABLE).map_err(backend)?;
        Ok(table.get(key).map_err(backend)?.map(|g| g.value().to_vec()))
    }

    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let mut table = self.tx.open_table(TABLE).map_err(backend)?;
        table.insert(key, value).map_err(backend)?;
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        let mut table = self.tx.open_table(TABLE).map_err(backend)?;
        table.remove(key).map_err(backend)?;
        Ok(())
    }

    fn range<'a>(
        &'a self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        redb_cursor(self, bound_to_owned(start), bound_to_owned(end), reverse)
    }

    async fn clear_range(
        &mut self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        let mut table = self.tx.open_table(TABLE).map_err(backend)?;
        // redb has no bulk range clear; `retain_in` with a `false` predicate is
        // the native primitive — it removes exactly the in-range entries in one
        // pass, with no per-key allocation or per-key point-delete. An empty or
        // inverted range is a no-op: its range iterator yields nothing.
        //
        // The explicit `::<&[u8], fn(..)>` turbofish is required: `retain_in`'s
        // `KR` type parameter cannot be inferred from a `(Bound, Bound)` range
        // tuple, and `F` must be a nameable `fn` item (not a closure) so the
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
            .map_err(|_| RedbStorageError::Backend("db commit thread has stopped".to_string()))?;
        result.await.map_err(|_| {
            RedbStorageError::Backend("db commit thread dropped the reply".to_string())
        })?
    }
}

/// A redb reader: a consistent read-only view of the committed database.
/// Mutations return [`RedbStorageError::ReadOnly`].
#[derive(Debug)]
pub struct RedbReader {
    tx: ReadTransaction,
}

impl FirstInRange for RedbReader {
    fn first_in_range(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<Entry, RedbStorageError> {
        match self.tx.open_table(TABLE) {
            Ok(table) => first_of(&table, start, end, reverse),
            // A never-written table simply has no rows.
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(backend(e)),
        }
    }
}

#[async_trait::async_trait]
impl KvStore for RedbReader {
    type Error = RedbStorageError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        match self.tx.open_table(TABLE) {
            Ok(table) => Ok(table.get(key).map_err(backend)?.map(|g| g.value().to_vec())),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(backend(e)),
        }
    }

    async fn set(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        Err(RedbStorageError::ReadOnly)
    }

    async fn delete(&mut self, _key: &[u8]) -> Result<(), Self::Error> {
        Err(RedbStorageError::ReadOnly)
    }

    fn range<'a>(
        &'a self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        redb_cursor(self, bound_to_owned(start), bound_to_owned(end), reverse)
    }

    async fn clear_range(
        &mut self,
        _start: Bound<&[u8]>,
        _end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        Err(RedbStorageError::ReadOnly)
    }
}

#[cfg(test)]
mod tests;
