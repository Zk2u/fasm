//! Snapshot state-transition sessions and read-only snapshot handles.

use std::fmt;

#[cfg(test)]
use fasm_storage::RawKv;
use fasm_storage::{FlatEngine, flatengine::FlatError};

use crate::{
    IndexedDbError, IndexedDbStore, Revision,
    overlay::{Rows, Snapshot},
};

/// One state transition over a consistent, owned database snapshot.
///
/// Opening the session copies the whole raw database and its revision in one
/// readonly IndexedDB transaction. Later reads use this snapshot plus the
/// session's writes, even when another tab commits. Only its buffered changes
/// are submitted at commit; a changed revision rejects the entire write set.
/// Dropping the session before polling commit discards every change.
pub struct IndexedDbTransaction {
    pub(crate) store: IndexedDbStore,
    pub(crate) engine: FlatEngine<Snapshot>,
    pub(crate) expected: Revision,
    #[cfg(test)]
    pub(crate) faults: FaultInjection,
}

/// Test-only failures injected at distinct commit phases.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct FaultInjection {
    pub fail_conversion_of: Option<Vec<u8>>,
    pub fail_enqueue_of: Option<Vec<u8>>,
    pub fail_request_of: Option<Vec<u8>>,
    pub fail_abort: bool,
}

impl fmt::Debug for IndexedDbTransaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedDbTransaction")
            .field("pending_len", &self.pending_len())
            .field("expected_revision", &self.expected)
            .finish()
    }
}

impl IndexedDbTransaction {
    pub(crate) fn new(store: IndexedDbStore, expected: Revision, rows: Rows) -> Self {
        Self {
            store,
            engine: FlatEngine::new(Snapshot::new(rows)),
            expected,
            #[cfg(test)]
            faults: FaultInjection::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn inject_faults(&mut self, faults: FaultInjection) {
        self.faults = faults;
    }

    /// Number of raw keys with a pending value or tombstone, including layout rows.
    pub fn pending_len(&self) -> usize {
        self.engine.raw().buffer.len()
    }

    /// Revision captured atomically with this session's snapshot.
    pub fn expected_revision(&self) -> Revision {
        self.expected
    }

    #[cfg(test)]
    pub(crate) async fn raw_read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, IndexedDbError> {
        self.engine.raw().get(key)
    }
}

/// A read-only snapshot of committed data, captured by `store.reader().await`.
///
/// All operations, including scans, see the same data. Create a new reader to
/// observe later commits. Each reader holds a full database copy in memory.
/// Closing its connection invalidates this handle just as it does a session.
pub struct IndexedDbReader {
    pub(crate) store: IndexedDbStore,
    pub(crate) engine: FlatEngine<Snapshot>,
}

impl fmt::Debug for IndexedDbReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedDbReader")
            .field("store", &self.store)
            .finish()
    }
}

impl IndexedDbReader {
    pub(crate) fn new(store: IndexedDbStore, rows: Rows) -> Self {
        Self {
            store,
            engine: FlatEngine::new(Snapshot::new(rows)),
        }
    }
}

pub(crate) fn layout_error(error: FlatError<IndexedDbError>) -> IndexedDbError {
    match error {
        FlatError::Foreign => IndexedDbError::Foreign,
        FlatError::Corrupt => IndexedDbError::Corrupt {
            detail: "directory layout is malformed".to_owned(),
        },
        FlatError::Key(error) => IndexedDbError::Key(error),
        FlatError::Engine(error) => error,
    }
}
