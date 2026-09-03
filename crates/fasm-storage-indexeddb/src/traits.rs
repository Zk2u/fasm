//! Directory-native `KvStore` and `KvDirNav` implementations for the browser handles.

use std::ops::Bound;

use fasm_storage::{KvDirNav, KvStore, KvStream, validate_dir};

use crate::{
    IndexedDbError, IndexedDbReader, IndexedDbTransaction,
    scan::{reader_scan, transaction_scan},
};

impl KvStore for IndexedDbTransaction {
    type Error = IndexedDbError;

    async fn get(&self, dir: &[&[u8]], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.read(dir, key).await
    }

    async fn set(&mut self, dir: &[&[u8]], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.write(dir, key, value).await
    }

    async fn delete(&mut self, dir: &[&[u8]], key: &[u8]) -> Result<(), Self::Error> {
        self.remove(dir, key).await
    }

    async fn exists(&self, dir: &[&[u8]], key: &[u8]) -> Result<bool, Self::Error> {
        self.contains(dir, key).await
    }

    fn range<'a>(
        &'a self,
        dir: &[&[u8]],
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        transaction_scan(self, dir, start, end, reverse)
    }

    async fn clear_range(
        &mut self,
        dir: &[&[u8]],
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        self.clear(dir, start, end).await
    }
}

impl KvDirNav for IndexedDbTransaction {
    async fn list_dirs(&self, dir: &[&[u8]]) -> Result<Vec<Vec<u8>>, Self::Error> {
        self.list_directories(dir).await
    }

    async fn dir_exists(&self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        self.directory_exists(dir).await
    }

    async fn remove_dir(&mut self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        self.remove_directory(dir).await
    }
}

impl KvStore for IndexedDbReader {
    type Error = IndexedDbError;

    async fn get(&self, dir: &[&[u8]], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.read(dir, key).await
    }

    async fn set(&mut self, dir: &[&[u8]], _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        validate_dir(dir)?;
        Err(IndexedDbError::ReadOnly)
    }

    async fn delete(&mut self, dir: &[&[u8]], _key: &[u8]) -> Result<(), Self::Error> {
        validate_dir(dir)?;
        Err(IndexedDbError::ReadOnly)
    }

    async fn exists(&self, dir: &[&[u8]], key: &[u8]) -> Result<bool, Self::Error> {
        self.contains(dir, key).await
    }

    fn range<'a>(
        &'a self,
        dir: &[&[u8]],
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
        reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        reader_scan(self, dir, start, end, reverse)
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
}

impl KvDirNav for IndexedDbReader {
    async fn list_dirs(&self, dir: &[&[u8]]) -> Result<Vec<Vec<u8>>, Self::Error> {
        crate::flat::list_dirs(self, dir).await
    }

    async fn dir_exists(&self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        crate::flat::dir_exists(self, dir).await
    }

    async fn remove_dir(&mut self, dir: &[&[u8]]) -> Result<bool, Self::Error> {
        validate_dir(dir)?;
        Err(IndexedDbError::ReadOnly)
    }
}
