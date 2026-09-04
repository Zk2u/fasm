//! An owned session snapshot and the write set applied by fenced commit.
//!
//! The current view is updated synchronously so the shared directory engine
//! can use its ordinary `RawKv` interface. Only changed keys enter the write
//! buffer; committing never replaces the whole database.

use std::{collections::BTreeMap, ops::Bound};

use fasm_storage::{KvPair, RawKv, is_empty_range};

use crate::IndexedDbError;

pub(crate) type Rows = BTreeMap<Vec<u8>, Vec<u8>>;

/// Pending sets and tombstones, in deterministic raw-key order.
#[derive(Default)]
pub(crate) struct WriteBuffer {
    entries: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl WriteBuffer {
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn drain_ops(self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.entries.into_iter().collect()
    }
}

/// One consistent committed snapshot, updated with this session's own writes.
pub(crate) struct Snapshot {
    rows: Rows,
    pub(crate) buffer: WriteBuffer,
}

impl Snapshot {
    pub(crate) fn new(rows: Rows) -> Self {
        Self {
            rows,
            buffer: WriteBuffer::default(),
        }
    }
}

impl RawKv for Snapshot {
    type Error = IndexedDbError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.rows.get(key).cloned())
    }

    fn scan(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        forward: bool,
    ) -> Result<Vec<KvPair>, Self::Error> {
        if is_empty_range(&start, &end) {
            return Ok(Vec::new());
        }
        let mut rows: Vec<_> = self
            .rows
            .range::<[u8], _>((start, end))
            .map(|(key, value)| KvPair {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        if !forward {
            rows.reverse();
        }
        Ok(rows)
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.rows.insert(key.to_vec(), value.to_vec());
        self.buffer
            .entries
            .insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        self.rows.remove(key);
        self.buffer.entries.insert(key.to_vec(), None);
        Ok(())
    }

    fn clear_range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<(), Self::Error> {
        if is_empty_range(&start, &end) {
            return Ok(());
        }
        let keys: Vec<_> = self
            .rows
            .range::<[u8], _>((start, end))
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            self.delete(&key)?;
        }
        Ok(())
    }
}

#[cfg(all(test, not(all(target_arch = "wasm32", target_os = "unknown"))))]
mod tests {
    use super::*;
    use proptest::{
        collection::{btree_map, vec},
        prelude::*,
    };

    fn contains(key: &[u8], lo: &[u8], hi: &[u8], excluded: bool) -> bool {
        key >= lo && if excluded { key < hi } else { key <= hi }
    }

    proptest! {
        /// Compare both the visible view and replayed write set with an
        /// independent map after overlapping sets, deletes and range clears.
        #[test]
        fn snapshot_and_commit_delta_match_the_model(
            initial in btree_map(vec(any::<u8>(), 0..4), vec(any::<u8>(), 0..8), 1..30),
            ops in vec((0_u8..4, vec(any::<u8>(), 0..4), vec(any::<u8>(), 0..4), any::<bool>()), 0..100),
        ) {
            let mut snapshot = Snapshot::new(initial.clone());
            let mut model = initial.clone();
            prop_assert!(snapshot.buffer.is_empty());
            prop_assert_eq!(snapshot.buffer.len(), 0);
            for (kind, key, value, excluded) in ops {
                match kind {
                    0 => { snapshot.insert(&key, &value).unwrap(); model.insert(key, value); }
                    1 => { snapshot.delete(&key).unwrap(); model.remove(&key); }
                    2 => {
                        let end = if excluded { Bound::Excluded(value.as_slice()) } else { Bound::Included(value.as_slice()) };
                        snapshot.clear_range(Bound::Included(&key), end).unwrap();
                        model.retain(|k, _| !contains(k, &key, &value, excluded));
                    }
                    _ => { prop_assert_eq!(snapshot.get(&key).unwrap(), model.get(&key).cloned()); }
                }
                for forward in [false, true] {
                    let actual: Vec<_> = snapshot.scan(Bound::Unbounded, Bound::Unbounded, forward).unwrap()
                        .into_iter().map(|p| (p.key, p.value)).collect();
                    let mut expected: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    if !forward { expected.reverse(); }
                    prop_assert_eq!(actual, expected);
                }
            }
            let mut replay = initial;
            for (key, value) in snapshot.buffer.drain_ops() {
                match value { Some(value) => { replay.insert(key, value); }, None => { replay.remove(&key); } }
            }
            prop_assert_eq!(replay, model);
        }
    }
}
