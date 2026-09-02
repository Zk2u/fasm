//! FoundationDB-backed [`KvStore`] backend.
//!
//! This is the pure key-value half of the FDB storage: a flat keyspace scoped by a
//! single byte prefix. Two handles are offered over the same keyspace:
//!
//! - [`FdbTransaction`] — a read-write view over one FoundationDB transaction.
//!   Reads see the transaction's own buffered writes; [`Commit`] commits it.
//! - [`FdbReadOnlyStore`] — a read-only view that opens a fresh transaction per
//!   scan. Mutations are rejected with [`FdbStorageError::ReadOnlyMutation`].
//!
//! A caller scopes all of its keys under one prefix and hands that prefix to a
//! handle; the handle stores and reports keys with the prefix added and stripped
//! so callers never see it. A returned key that somehow escapes the prefix is
//! reported as [`FdbStorageError::PrefixViolation`] rather than leaked.
//!
//! The FoundationDB directory layer (per-peer, per-role keyspace allocation) is
//! **not** part of this crate: it is a consumer concern. This crate assumes the
//! caller has already resolved the prefix to scope under and provides only the
//! flat KV store beneath it.
//!
//! # Single-transaction scans
//!
//! One `range()` call corresponds to exactly one FoundationDB transaction (for
//! the read-only handle, opened lazily inside the stream and held for the
//! duration of the iteration). FDB transactions have a soft limit of ~5 seconds,
//! after which reads return `transaction_too_old`. Because every page in one
//! `range()` call shares that transaction's read version, the budget covers the
//! whole scan, not each page: a caller iterating a very large range (or slowly
//! enough that the total scan would exceed the window) must split the range
//! itself and reissue from a cursor key. On retryable errors during a page fetch
//! the implementation calls `Transaction::on_error`, applying FDB's recommended
//! backoff and resetting the transaction; pages observed after a retry are at the
//! post-reset snapshot.

use std::{collections::VecDeque, ops::Bound, pin::Pin, sync::Arc};

use foundationdb::{
    Database, FdbError, KeySelector, RangeOption, TransactOption, Transaction,
    TransactionCommitError, options,
};
use thiserror::Error;

use fasm_storage::{
    Commit, KvPair, KvStore, KvStream, RetryableStorageError, bound_to_owned, next_prefix,
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
    /// A returned key escaped the scoped prefix.
    #[error("scoped range returned a key outside the expected prefix")]
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
            Self::PrefixViolation | Self::ReadOnlyMutation => false,
        }
    }
}

/// A read-write KV view over one FoundationDB transaction, scoped to one prefix.
/// Reads see the transaction's own buffered writes; [`Commit`] commits it.
#[derive(Debug)]
pub struct FdbTransaction {
    tx: Transaction,
    prefix: Vec<u8>,
}

/// A read-only KV view that opens a fresh FoundationDB transaction per scan,
/// scoped to one prefix.
#[derive(Clone)]
pub struct FdbReadOnlyStore {
    db: Arc<Database>,
    prefix: Vec<u8>,
}

impl std::fmt::Debug for FdbReadOnlyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FdbReadOnlyStore").finish_non_exhaustive()
    }
}

/// A buffered, multi-page cursor over a single FDB range scan.
#[derive(Debug)]
struct RangeCursor {
    opt: Option<RangeOption<'static>>,
    iteration: usize,
    buffered: VecDeque<KvPair>,
}

impl FdbTransaction {
    /// Wraps an open transaction and scopes it under `prefix`.
    ///
    /// The caller owns the transaction's lifetime: uncommitted on drop, and the
    /// transaction is single-use (commit or drop, not both).
    pub fn new(tx: Transaction, prefix: Vec<u8>) -> Self {
        Self { tx, prefix }
    }

    fn make_cursor(
        &self,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        reverse: bool,
    ) -> RangeCursor {
        let mut opt = RangeOption::from((
            start_selector(&self.prefix, start),
            end_selector(&self.prefix, end),
        ));
        opt.reverse = reverse;
        opt.mode = options::StreamingMode::Iterator;

        RangeCursor {
            opt: Some(opt),
            iteration: 1,
            buffered: VecDeque::new(),
        }
    }

    fn stream_from_cursor<'a>(&'a self, mut cursor: RangeCursor) -> KvStream<'a, FdbStorageError> {
        KvStream::new(async move {
            loop {
                if let Some(pair) = cursor.buffered.pop_front() {
                    return Ok(Some((pair, self.stream_from_cursor(cursor))));
                }

                let Some(opt) = cursor.opt.take() else {
                    return Ok(None);
                };
                let values = self.tx.get_range(&opt, cursor.iteration, false).await?;
                cursor.iteration += 1;
                cursor.opt = opt.next_range(&values);

                for kv in values.into_iter() {
                    let key = kv
                        .key()
                        .strip_prefix(self.prefix.as_slice())
                        .ok_or(FdbStorageError::PrefixViolation)?
                        .to_vec();
                    cursor.buffered.push_back(KvPair {
                        key,
                        value: kv.value().to_vec(),
                    });
                }
            }
        })
    }
}

impl FdbReadOnlyStore {
    /// Wraps a database handle and scopes it under `prefix`.
    pub fn new(db: Arc<Database>, prefix: Vec<u8>) -> Self {
        Self { db, prefix }
    }
}

fn prefix_key(prefix: &[u8], key: &[u8]) -> Vec<u8> {
    let mut prefixed = Vec::with_capacity(prefix.len() + key.len());
    prefixed.extend_from_slice(prefix);
    prefixed.extend_from_slice(key);
    prefixed
}

fn start_selector(prefix: &[u8], bound: Bound<Vec<u8>>) -> KeySelector<'static> {
    match bound {
        Bound::Included(key) => KeySelector::first_greater_or_equal(prefix_key(prefix, &key)),
        Bound::Excluded(key) => KeySelector::first_greater_than(prefix_key(prefix, &key)),
        Bound::Unbounded => KeySelector::first_greater_or_equal(prefix.to_vec()),
    }
}

fn end_selector(prefix: &[u8], bound: Bound<Vec<u8>>) -> KeySelector<'static> {
    match bound {
        Bound::Included(key) => KeySelector::first_greater_than(prefix_key(prefix, &key)),
        Bound::Excluded(key) => KeySelector::first_greater_or_equal(prefix_key(prefix, &key)),
        Bound::Unbounded => {
            let next = next_prefix(prefix).unwrap_or_else(|| vec![0xFF]);
            KeySelector::first_greater_or_equal(next)
        }
    }
}

fn first_greater_than_key(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 1);
    out.extend_from_slice(key);
    out.push(0x00);
    out
}

fn clear_range_start_key(prefix: &[u8], bound: Bound<Vec<u8>>) -> Vec<u8> {
    match bound {
        Bound::Included(key) => prefix_key(prefix, &key),
        Bound::Excluded(key) => first_greater_than_key(&prefix_key(prefix, &key)),
        Bound::Unbounded => prefix.to_vec(),
    }
}

fn clear_range_end_key(prefix: &[u8], bound: Bound<Vec<u8>>) -> Vec<u8> {
    match bound {
        Bound::Included(key) => first_greater_than_key(&prefix_key(prefix, &key)),
        Bound::Excluded(key) => prefix_key(prefix, &key),
        Bound::Unbounded => next_prefix(prefix).unwrap_or_else(|| vec![0xFF]),
    }
}

#[async_trait::async_trait]
impl KvStore for FdbTransaction {
    type Error = FdbStorageError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let key = prefix_key(&self.prefix, key);
        Ok(self.tx.get(&key, false).await?.map(|value| value.to_vec()))
    }

    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let key = prefix_key(&self.prefix, key);
        self.tx.set(&key, value);
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        let key = prefix_key(&self.prefix, key);
        self.tx.clear(&key);
        Ok(())
    }

    fn range<'a>(
        &'a self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        let start = bound_to_owned(start);
        let end = bound_to_owned(end);
        self.stream_from_cursor(self.make_cursor(start, end, reverse))
    }

    async fn clear_range(
        &mut self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        let start = bound_to_owned(start);
        let end = bound_to_owned(end);
        let begin = clear_range_start_key(&self.prefix, start);
        let end = clear_range_end_key(&self.prefix, end);
        self.tx.clear_range(&begin, &end);
        Ok(())
    }
}

#[async_trait::async_trait]
impl KvStore for FdbReadOnlyStore {
    type Error = FdbStorageError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.db
            .transact_boxed(
                (self.prefix.clone(), key.to_vec()),
                |tx, data| {
                    Box::pin(async move {
                        let key = prefix_key(&data.0, &data.1);
                        Ok(tx.get(&key, false).await?.map(|value| value.to_vec()))
                    })
                },
                TransactOption::idempotent(),
            )
            .await
    }

    async fn set(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        Err(FdbStorageError::ReadOnlyMutation)
    }

    async fn delete(&mut self, _key: &[u8]) -> Result<(), Self::Error> {
        Err(FdbStorageError::ReadOnlyMutation)
    }

    /// Scan a range of keys, streaming each page lazily.
    ///
    /// # Single-transaction semantics
    ///
    /// **One `range()` call corresponds to exactly one FoundationDB
    /// transaction**, opened lazily inside the stream and held open for the
    /// duration of the iteration. Every page read shares that transaction's read
    /// version, so a scan that completes without retries observes a single
    /// consistent FoundationDB snapshot.
    ///
    /// FDB transactions have a soft limit of ~5 seconds (after which reads start
    /// returning `transaction_too_old`). Because *all* pages in one `range()`
    /// call run inside the same transaction, that budget covers the entire scan,
    /// not each page. Callers iterating very large key ranges (or processing
    /// each item slowly enough that the total scan time would exceed FDB's
    /// transaction window) **must split the range up themselves** — pick an
    /// intermediate cursor key from the last item of one batch, then issue a
    /// fresh `range()` starting from that key.
    ///
    /// On retryable errors during a page fetch (`transaction_too_old`,
    /// `process_behind`, ...) the implementation calls `Transaction::on_error`,
    /// which applies FDB's recommended backoff and resets the transaction. Pages
    /// observed after a retry are at the post-reset snapshot.
    fn range<'a>(
        &'a self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        let db = Arc::clone(&self.db);
        let prefix = Arc::new(self.prefix.clone());
        let start = bound_to_owned(start);
        let end = bound_to_owned(end);
        KvStream::new(async move {
            let tx = match db.create_trx() {
                Ok(tx) => tx,
                Err(e) => return Err(FdbStorageError::from(e)),
            };
            let initial_opt = build_range_option(prefix.as_slice(), start, end, reverse);
            stream_next_page(tx, prefix, Some(initial_opt), 1, VecDeque::new()).await
        })
    }

    async fn clear_range(
        &mut self,
        _start: Bound<&[u8]>,
        _end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        Err(FdbStorageError::ReadOnlyMutation)
    }
}

fn build_range_option(
    prefix: &[u8],
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    reverse: bool,
) -> RangeOption<'static> {
    let mut opt = RangeOption::from((start_selector(prefix, start), end_selector(prefix, end)));
    opt.reverse = reverse;
    opt.mode = options::StreamingMode::Iterator;
    opt
}

type RangeStreamFuture<'a> = Pin<
    Box<
        dyn core::future::Future<
                Output = Result<Option<(KvPair, KvStream<'a, FdbStorageError>)>, FdbStorageError>,
            > + Send
            + 'a,
    >,
>;

/// Drive a paginated read-only range scan against a single transaction,
/// retrying transient FDB errors via `Transaction::on_error`.
///
/// The transaction is moved by value through each recursion of the stream
/// future. As long as no retryable error is hit, every page reads at the same
/// FDB snapshot. On a retryable error during `get_range`, `tx.on_error(e)`
/// consumes the transaction, applies FDB's recommended backoff, and returns it
/// reset and ready to retry from the same cursor.
fn stream_next_page<'a>(
    tx: Transaction,
    prefix: Arc<Vec<u8>>,
    cursor_opt: Option<RangeOption<'static>>,
    iteration: usize,
    buffered: VecDeque<KvPair>,
) -> RangeStreamFuture<'a> {
    Box::pin(async move {
        let mut tx = tx;
        let mut cursor_opt = cursor_opt;
        let mut iteration = iteration;
        let mut buffered = buffered;
        loop {
            if let Some(pair) = buffered.pop_front() {
                let next_prefix = Arc::clone(&prefix);
                let next_cursor = cursor_opt;
                let next_iteration = iteration;
                let next_buffered = buffered;
                let next_stream = KvStream::new(async move {
                    stream_next_page(tx, next_prefix, next_cursor, next_iteration, next_buffered)
                        .await
                });
                return Ok(Some((pair, next_stream)));
            }

            let Some(opt) = cursor_opt.take() else {
                return Ok(None);
            };

            // Retry loop for transient FDB errors. `on_error` consumes the
            // transaction, applies FDB's exponential backoff, and returns a
            // fresh transaction ready to retry. If the error is non-retryable,
            // `on_error` returns Err and we propagate it.
            let values = loop {
                match tx.get_range(&opt, iteration, false).await {
                    Ok(v) => break v,
                    Err(e) => {
                        tx = tx.on_error(e).await.map_err(FdbStorageError::from)?;
                    }
                }
            };
            // `next_range` consumes `self`; clone the cursor (cheap — it is two
            // key selectors and a few flags) so we can advance it.
            cursor_opt = opt.clone().next_range(&values);
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

impl Commit for FdbTransaction {
    type Error = FdbStorageError;

    async fn commit(self) -> Result<(), Self::Error> {
        self.tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
