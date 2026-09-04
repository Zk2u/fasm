//! Directory operations delegated to the shared synchronous engine.

use std::ops::Bound;

use fasm_storage::{KvDirNav, KvStore, KvStream, validate_dir};

use crate::{
    IndexedDbError, IndexedDbReader, IndexedDbTransaction, scan::snapshot_stream,
    session::layout_error,
};

impl KvStore for IndexedDbTransaction {
    type Error = IndexedDbError;

    async fn get(&self, dir: &[&[u8]], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.store.database()?;
        self.engine.get(dir, key).map_err(layout_error)
    }

    async fn exists(&self, dir: &[&[u8]], key: &[u8]) -> Result<bool, Self::Error> {
        self.store.database()?;
        self.engine
            .get(dir, key)
            .map(|value| value.is_some())
            .map_err(layout_error)
    }

    async fn set(&mut self, dir: &[&[u8]], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.store.database()?;
        self.engine.set(dir, key, value).map_err(layout_error)
    }

    async fn delete(&mut self, dir: &[&[u8]], key: &[u8]) -> Result<(), Self::Error> {
        self.store.database()?;
        self.engine.delete(dir, key).map_err(layout_error)
    }

    async fn clear_range(
        &mut self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        self.store.database()?;
        self.engine
            .clear_range(dir, start, end)
            .map_err(layout_error)
    }

    fn range<'a>(
        &'a self,
        dir: &[&[u8]],
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        let rows = self.store.database().and_then(|_| {
            self.engine
                .scan(dir, start, end, !reverse)
                .map_err(layout_error)
        });
        snapshot_stream(&self.store, rows)
    }
}

impl KvDirNav for IndexedDbTransaction {
    async fn list_dirs(&self, dir: &[&[u8]]) -> Result<Vec<Vec<u8>>, Self::Error> {
        self.store.database()?;
        self.engine.list_dirs(dir).map_err(layout_error)
    }

    async fn dir_exists(&self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        self.store.database()?;
        self.engine.dir_exists(dir).map_err(layout_error)
    }

    async fn remove_dir(&mut self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        self.store.database()?;
        self.engine.remove_dir(dir).map_err(layout_error)
    }
}

impl KvStore for IndexedDbReader {
    type Error = IndexedDbError;

    async fn get(&self, dir: &[&[u8]], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.store.database()?;
        self.engine.get(dir, key).map_err(layout_error)
    }

    async fn exists(&self, dir: &[&[u8]], key: &[u8]) -> Result<bool, Self::Error> {
        self.store.database()?;
        self.engine
            .get(dir, key)
            .map(|value| value.is_some())
            .map_err(layout_error)
    }

    async fn set(&mut self, dir: &[&[u8]], _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        validate_dir(dir)?;
        Err(IndexedDbError::ReadOnly)
    }

    async fn delete(&mut self, dir: &[&[u8]], _key: &[u8]) -> Result<(), Self::Error> {
        validate_dir(dir)?;
        Err(IndexedDbError::ReadOnly)
    }

    async fn clear_range(
        &mut self,
        dir: &[&[u8]],
        _start: Bound<&[u8]>,
        _end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        validate_dir(dir)?;
        Err(IndexedDbError::ReadOnly)
    }

    fn range<'a>(
        &'a self,
        dir: &[&[u8]],
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        let rows = self.store.database().and_then(|_| {
            self.engine
                .scan(dir, start, end, !reverse)
                .map_err(layout_error)
        });
        snapshot_stream(&self.store, rows)
    }
}

impl KvDirNav for IndexedDbReader {
    async fn list_dirs(&self, dir: &[&[u8]]) -> Result<Vec<Vec<u8>>, Self::Error> {
        self.store.database()?;
        self.engine.list_dirs(dir).map_err(layout_error)
    }

    async fn dir_exists(&self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        self.store.database()?;
        self.engine.dir_exists(dir).map_err(layout_error)
    }

    async fn remove_dir(&mut self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        validate_dir(dir)?;
        Err(IndexedDbError::ReadOnly)
    }
}
