//! The exact row layouts of the flat directory layout.
//!
//! This module holds the layout constants and the builders that turn a
//! directory node, a segment name, or an allocated prefix into the raw
//! row keys and regions the layout operates on. Every row form replicates
//! what the FoundationDB directory layer (`DirectoryLayer::default()`)
//! writes into a real FoundationDB keyspace:
//!
//! | Row | Key | Value |
//! |-----|-----|-------|
//! | version | [`VERSION_KEY`] | 12 bytes: major, minor, patch as little-endian `u32` (1.0.0) |
//! | HCA counter | [`COUNTERS_BASE`] ‖ packed `i64` window start | 8-byte little-endian allocation count |
//! | HCA recent | [`RECENT_BASE`] ‖ packed `i64` candidate | empty |
//! | child | [`child_key`]: node ‖ `i64(0)` ‖ STRING(segment) | the child's allocated prefix (raw bytes) |
//! | layer | [`layer_key`]: node ‖ BYTES("layer") | layer bytes (always empty in this layout) |
//! | data | allocated prefix ‖ key | the value |
//!
//! Key regions by first byte:
//!
//! | First byte | Content |
//! |---|---|
//! | `0x00`–`0xFD` | Data keys: an allocated prefix ‖ key |
//! | `0xFE` | Directory nodes: [`NODE_PREFIX`] ‖ BYTES(allocated prefix), each holding its child rows and layer row; the version key and the HCA counter/recent rows live under the root node in this region |
//! | `0xFF` | Reserved (empty forever) |
//!
//! Allocated prefixes are packed `i64`s: prefix-free (no one is a
//! byte-prefix of another) and always starting `0x0c..=0x1c`, so a data
//! key needs no separator byte and no data key can collide with another
//! directory's data keys, the meta region, or the reserved region.
//!
//! The `0xFE`/`0xFF` node and reserved regions, and the content range
//! `0x00..0xFE`, match `DirectoryLayer::default()` exactly (its
//! `DEFAULT_NODE_PREFIX` is `0xFE`, its content subspace is the whole
//! keyspace).

use core::ops::Bound;

use crate::flatdir::tuple::{CODE_BYTES, pack_bytes, pack_i64, pack_string};
use crate::keyspace::next_prefix;

/// The raw first byte of every directory-node key.
pub const NODE_PREFIX: &[u8] = &[0xFE];
/// The reserved first byte: no row of this layout starts with it.
pub const RESERVED_BYTE: u8 = 0xFF;

/// The root node: `NODE_PREFIX ‖ BYTES([0xFE])` — verbatim from the layer
/// constructor (`root_node = node_subspace.subspace(&node_subspace.bytes())`
/// with the default node prefix `0xFE`).
pub const ROOT_NODE: &[u8] = &[0xFE, 0x01, 0xFE, 0x00];

/// The HCA subspace: root node ‖ BYTES("hca") — `DEFAULT_HCA_PREFIX` is a
/// byte string in the layer's own code.
pub const HCA_SUBSPACE: &[u8] = &[0xFE, 0x01, 0xFE, 0x00, 0x01, b'h', b'c', b'a', 0x00];

/// HCA counters subspace: HCA ‖ INT(0).
pub const COUNTERS_BASE: &[u8] = &[0xFE, 0x01, 0xFE, 0x00, 0x01, b'h', b'c', b'a', 0x00, 0x14];

/// HCA recents subspace: HCA ‖ INT(1).
pub const RECENT_BASE: &[u8] = &[
    0xFE, 0x01, 0xFE, 0x00, 0x01, b'h', b'c', b'a', 0x00, 0x15, 0x01,
];

/// The version key: root node ‖ BYTES("version").
pub const VERSION_KEY: &[u8] = &[
    0xFE, 0x01, 0xFE, 0x00, 0x01, b'v', b'e', b'r', b's', b'i', b'o', b'n', 0x00,
];

/// The layer-row tag: a node's layer row is `node ‖ BYTES("layer")`.
pub const LAYER_TAG: &[u8] = &[CODE_BYTES, b'l', b'a', b'y', b'e', b'r', 0x00];

/// The version this layout writes and reads: 1.0.0 as three little-endian
/// `u32`s.
pub const LAYOUT_VERSION: &[u8] = &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

/// Parse a version value: three little-endian `u32`s (major, minor,
/// patch). `None` when the value is shorter than 12 bytes (trailing bytes
/// beyond 12 are tolerated, matching the layer's `len < 12` check).
pub fn parse_version(value: &[u8]) -> Option<(u32, u32, u32)> {
    if value.len() < 12 {
        return None;
    }
    Some((
        u32::from_le_bytes(value[0..4].try_into().ok()?),
        u32::from_le_bytes(value[4..8].try_into().ok()?),
        u32::from_le_bytes(value[8..12].try_into().ok()?),
    ))
}

/// The fasm root directory maps to the layer path starting with this
/// segment; every fasm directory path is `[ROOT_PATH_SEGMENT] ‖ dir`.
/// The segment is valid UTF-8 and contains a NUL, which no user segment
/// validation would reject but which keeps the anchor unforgeable from a
/// single user path level. The fdb backend shares this constant, so the
/// flat layout and the native layer agree on the anchor mapping.
pub const ROOT_PATH_SEGMENT: &str = "\0fasm-storage";

/// The node subspace of an allocated prefix: `NODE_PREFIX ‖ BYTES(prefix)`.
pub fn node_subspace(prefix: &[u8]) -> Vec<u8> {
    let mut node = NODE_PREFIX.to_vec();
    node.extend_from_slice(&pack_bytes(prefix));
    node
}

/// The child-row key under `node` for segment `name` (UTF-8):
/// `node ‖ INT(0) ‖ STRING(name)`.
pub fn child_key(node: &[u8], name: &str) -> Vec<u8> {
    let mut key = node.to_vec();
    key.extend_from_slice(&pack_i64(0));
    key.extend_from_slice(&pack_string(name));
    key
}

/// The layer-row key of a node: `node ‖ BYTES("layer")`.
pub fn layer_key(node: &[u8]) -> Vec<u8> {
    let mut key = node.to_vec();
    key.extend_from_slice(LAYER_TAG);
    key
}

/// The HCA counter key for a window start: `COUNTERS_BASE ‖ INT(start)`.
pub fn counter_key(window_start: i64) -> Vec<u8> {
    let mut key = COUNTERS_BASE.to_vec();
    key.extend_from_slice(&pack_i64(window_start));
    key
}

/// The HCA recent key for a candidate: `RECENT_BASE ‖ INT(candidate)`.
pub fn recent_key(candidate: i64) -> Vec<u8> {
    let mut key = RECENT_BASE.to_vec();
    key.extend_from_slice(&pack_i64(candidate));
    key
}

/// The raw keyspace range of a node's own rows (children rows + layer
/// row): `[node ‖ 0x00, node ‖ 0xFF]` inclusive at both ends, expressed
/// with the flat engine's half-open end.
pub fn node_key_range(node: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start = {
        let mut k = node.to_vec();
        k.push(0x00);
        k
    };
    let mut end = node.to_vec();
    end.push(0xFF);
    end.push(0x00);
    (Bound::Included(start), Bound::Excluded(end))
}

/// The raw keyspace range of a node's child rows:
/// `[node ‖ INT(0) ‖ 0x00, node ‖ INT(0) ‖ 0xFF]` inclusive at both ends.
pub fn children_key_range(node: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let base = {
        let mut k = node.to_vec();
        k.extend_from_slice(&pack_i64(0));
        k
    };
    let start = {
        let mut k = base.clone();
        k.push(0x00);
        k
    };
    let mut end = base;
    end.push(0xFF);
    end.push(0x00);
    (Bound::Included(start), Bound::Excluded(end))
}

/// The raw keyspace range of a directory's data:
/// `[prefix, strinc(prefix))`, where `strinc` is the FoundationDB
/// successor (increment the last non-`0xFF` byte, drop trailing `0xFF`s)
/// — exactly the crate's [`next_prefix`], pinned by test.
///
/// An allocated prefix (a packed `i64`) never ends in `0xFF` at its last
/// non-`0xFF` position in a way that makes `strinc` fail: `next_prefix`
/// returns `Some` for every packed `i64`.
pub fn data_region(prefix: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let end = next_prefix(prefix).expect(
        "next_prefix fails only on an all-0xFF input; allocated prefixes start 0x0c..=0x1c",
    );
    (Bound::Included(prefix.to_vec()), Bound::Excluded(end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flatdir::tuple::{unpack_child_name, unpack_i64_full};

    #[test]
    fn the_constants_are_the_layer_row_bytes() {
        // Pinned from the `foundationdb` crate's default layer:
        // node prefix 0xFE, root node = 0xFE || BYTES(0xFE), hca/version/
        // layer tags as byte strings, counters/recents as INT subspace.
        assert_eq!(node_subspace(&[0xFE]), ROOT_NODE.to_vec());
        assert_eq!(
            HCA_SUBSPACE,
            &[0xFE, 0x01, 0xFE, 0x00, 0x01, b'h', b'c', b'a', 0x00]
        );
        assert_eq!(COUNTERS_BASE[..HCA_SUBSPACE.len()], HCA_SUBSPACE[..]);
        assert_eq!(RECENT_BASE[..HCA_SUBSPACE.len()], HCA_SUBSPACE[..]);
        assert_eq!(VERSION_KEY[..ROOT_NODE.len()], ROOT_NODE[..]);
        assert_eq!(parse_version(LAYOUT_VERSION), Some((1, 0, 0)));
        // Version key and HCA rows sit under the root node; data and the
        // reserved region are disjoint.
        assert!(VERSION_KEY.starts_with(ROOT_NODE));
        assert!(COUNTERS_BASE.starts_with(ROOT_NODE));
        assert!(RECENT_BASE.starts_with(ROOT_NODE));
        // The allocated-prefix data range (first byte 0x0c..=0x1c) never
        // touches the 0xFE node region or the 0xFF reserved region.
        for v in [0i64, 1, 255, 256, -1, i64::MAX] {
            let p = pack_i64(v);
            assert!((0x0c..=0x1c).contains(&p[0]), "{v}");
            let (start, end) = data_region(&p);
            match (start, end) {
                (Bound::Included(s), Bound::Excluded(e)) => {
                    // Both ends of the data region stay below the 0xFE
                    // node region: the prefix starts 0x0c..=0x1c and
                    // `next_prefix` only increments within that band.
                    assert!(s[0] < 0xFE && e[0] < 0xFE);
                }
                _ => panic!("data_region must be Included/Excluded"),
            }
        }
    }

    #[test]
    fn strinc_is_next_prefix() {
        // The layout's region end is FDB's `strinc`; the crate's
        // property-tested `next_prefix` is that function.
        for v in [0i64, 1, 254, 255, 256, 65534, 65535, 65536, 1_000_000] {
            let p = pack_i64(v);
            let (start, end) = data_region(&p);
            match end {
                Bound::Excluded(e) => assert_eq!(Some(e), next_prefix(&p)),
                _ => panic!(),
            }
            assert_eq!(start, Bound::Included(p));
        }
        // Region arithmetic: [prefix, strinc(prefix)) holds exactly the
        // keys prefix || k, and the end is the successor of the whole
        // prefix block.
        let p = pack_i64(255); // [0x15, 0xFF] - ends in 0xFF.
        let (start, end) = data_region(&p);
        assert_eq!(
            (start, end),
            (Bound::Included(p.clone()), Bound::Excluded(vec![0x16]))
        );
        for k in [
            b"".as_slice(),
            b"k".as_slice(),
            b"k\0".as_slice(),
            &[0xFF][..],
        ] {
            let mut raw = p.clone();
            raw.extend_from_slice(k);
            assert!(
                raw.as_slice() >= p.as_slice() && raw.as_slice() < [0x16].as_slice(),
                "key {k:?}"
            );
        }
    }

    #[test]
    fn child_key_round_trips() {
        for name in ["a", "hca", "version", "\u{00d4}\0"] {
            let key = child_key(ROOT_NODE, name);
            let rel = &key[ROOT_NODE.len()..];
            assert_eq!(unpack_child_name(rel), Some(name.to_string()));
        }
        // The children range holds the child rows and nothing else from
        // other row kinds: the layer row sits outside it.
        let (s, e) = children_key_range(ROOT_NODE);
        match (s, e) {
            (Bound::Included(s), Bound::Excluded(e)) => {
                let ck = child_key(ROOT_NODE, "a");
                assert!(ck >= s && ck < e);
                let lk = layer_key(ROOT_NODE);
                assert!(
                    lk < s || lk >= e,
                    "layer row must sit outside the children range"
                );
            }
            _ => panic!(),
        }
    }

    #[test]
    fn node_range_holds_child_and_layer_rows() {
        let node = node_subspace(&[0x15, 0x01]);
        let (s, e) = node_key_range(&node);
        match (s, e) {
            (Bound::Included(s), Bound::Excluded(e)) => {
                let ck = child_key(&node, "x");
                assert!(ck >= s && ck < e);
                let lk = layer_key(&node);
                assert!(lk >= s && lk < e);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn hca_keys_decode() {
        for (start, key) in [
            (0i64, counter_key(0)),
            (64, counter_key(64)),
            (-7, counter_key(-7)),
        ] {
            assert_eq!(unpack_i64_full(&key[COUNTERS_BASE.len()..]), Some(start));
        }
        for (cand, key) in [(0i64, recent_key(0)), (4096, recent_key(4096))] {
            assert_eq!(unpack_i64_full(&key[RECENT_BASE.len()..]), Some(cand));
        }
    }
}
