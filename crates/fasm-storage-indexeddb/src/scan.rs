//! Continuations over an owned range from a session or reader snapshot.

use std::collections::VecDeque;

use fasm_storage::{KvPair, KvStream};

use crate::{IndexedDbError, IndexedDbStore};

pub(crate) fn snapshot_stream(
    store: &IndexedDbStore,
    rows: Result<Vec<KvPair>, IndexedDbError>,
) -> KvStream<'_, IndexedDbError> {
    match rows {
        Ok(rows) => next(store, rows.into()),
        Err(error) => KvStream::failed(error),
    }
}

fn next(store: &IndexedDbStore, mut rows: VecDeque<KvPair>) -> KvStream<'_, IndexedDbError> {
    KvStream::new(async move {
        store.database()?;
        Ok(rows.pop_front().map(|row| (row, next(store, rows))))
    })
}
