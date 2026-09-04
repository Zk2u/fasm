//! The raw-byte view the flat directory layout works over.
//!
//! A [`RawKv`] is a read/write view over one store's raw (unstructured)
//! byte map — no keyspace semantics, just bytes. The flat backends
//! implement it over their transaction views (the btreemap backend's
//! buffered base+delta view; the redb backend's read-write transaction)
//! and route every directory-layout operation through
//! [`FlatEngine`](crate::flatengine::FlatEngine), so the layout logic is
//! written and tested once.
//!
//! All reads return **owned** bytes: the layout machinery decodes and
//! collects before returning, and the backends stream from that owned
//! result (which they keep alive in their stream handle), so no borrow
//! from the engine view escapes.

use core::ops::Bound;

use crate::stream::KvPair;

/// A read/write view over a raw byte map.
///
/// Implementations must honour the empty-range contract: an `is_empty_range`
/// range (see [`crate::is_empty_range`]) yields no rows and performs no
/// mutation.
pub trait RawKv {
    /// The engine's own error type.
    type Error;

    /// Get one row.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Scan `start <= k < end` (both bounds per `Bound`), ascending or
    /// descending, returning owned raw rows as pairs.
    fn scan(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        forward: bool,
    ) -> Result<Vec<KvPair>, Self::Error>;

    /// Insert (or overwrite) one row.
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    /// Delete one row (a no-op if absent).
    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error>;

    /// Delete every row in `start <= k < end`.
    fn clear_range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<(), Self::Error>;
}
