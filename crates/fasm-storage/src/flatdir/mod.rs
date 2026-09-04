//! The flat directory layout: a verbatim port of FoundationDB's
//! `DirectoryLayer` (`foundationdb` crate, pinned `0.11.0`).
//!
//! The layout and every algorithm here replicate what
//! `DirectoryLayer::default()` does in a real FoundationDB keyspace —
//! the node tree, the High Contention Allocator, the version row, the
//! tuple byte encoding, the default layer placement — so that a store
//! written by this layout and one written by the native layer agree on
//! every row. The flat backends (btreemap, redb) execute it over a
//! [`crate::rawkv::RawKv`] view; the FDB backend uses the native layer
//! over the identical layouts.
//!
//! ## Row layouts
//!
//! | Row | Key | Value |
//! |-----|-----|-------|
//! | version | [`layout::VERSION_KEY`] | 12 bytes: major, minor, patch, little-endian `u32` (1.0.0) |
//! | HCA counter | [`layout::COUNTERS_BASE`] ‖ packed `i64` window start | 8-byte LE allocation count |
//! | HCA recent | [`layout::RECENT_BASE`] ‖ packed `i64` candidate | empty |
//! | child | [`layout::child_key`] = node ‖ `i64(0)` ‖ STRING(segment) | the child's allocated prefix (raw bytes) |
//! | layer | [`layout::layer_key`] = node ‖ BYTES("layer") | layer bytes (always empty here) |
//! | data | allocated prefix ‖ key | the value |
//!
//! Key regions: `0x00`–`0xFD` data, `0xFE` directory nodes
//! ([`layout::NODE_PREFIX`], root node [`layout::ROOT_NODE`]), `0xFF`
//! reserved. Allocated prefixes are packed `i64`s (prefix-free, first
//! byte `0x0c..=0x1c`), so data keys need no separator and cannot
//! collide with meta or reserved rows.
//!
//! ## Single-writer adaptations
//!
//! The flat backends have exactly one writer, so the layer's
//! transactional machinery degrades in documented ways (per module:
//! [`hca`], [`ops`]): no write-conflict ranges, the counter increment is
//! a read-modify-write landing at the atomic op's byte values, the RNG
//! is caller-supplied (`thread_rng` in production), and a candidate
//! claimed by a leftover recent row is retried rather than version-vector
//! resolved. The row bytes and the operation order are unchanged.
//!
//! ## Fresh stores and versions
//!
//! A store with no version row is fresh: reads answer empty, and the
//! first create writes the version row lazily (the layer's
//! `check_version` behaviour). A store whose version row this layout
//! does not read (newer major or minor) fails closed with
//! [`error::FlatError::Foreign`].

pub mod error;
pub mod hca;
pub mod layout;
pub mod ops;
pub mod tuple;

/// Re-exports for the backends' direct touches (the open-time probe and
/// the anchor mapping).
pub use error::FlatError;
pub use layout::{LAYOUT_VERSION, ROOT_PATH_SEGMENT, VERSION_KEY, parse_version};
