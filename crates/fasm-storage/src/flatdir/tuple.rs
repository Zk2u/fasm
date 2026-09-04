//! The tuple byte encoding the flat directory layout uses for its row
//! keys.
//!
//! This is a port of the subset of the FoundationDB tuple packing format
//! that the directory layout produces and consumes: integer elements,
//! byte-string elements, string (UTF-8) elements, and depth-0 tuples of
//! them. It replicates the encoding of the `foundationdb-tuple` crate at
//! the version the fdb backend is built against, so that keys produced
//! here are byte-identical to the keys the FoundationDB directory layer
//! would write for the same paths.
//!
//! What is ported, and why it is sufficient:
//!
//! - `i64` — the allocated prefixes, the HCA window starts and candidate
//!   values, and the fixed `0` first element of child-row keys.
//! - bytes (`&[u8]`) — node subspaces are addressed by the raw allocated
//!   prefix bytes, and the `version` / `layer` / `hca` row tags are byte
//!   strings in the layer's own code.
//! - string (`&str`) — directory segment names in child-row keys.
//! - depth-0 tuples — the only multi-element key the layout builds is
//!   `(0i64, name)` for child rows.
//!
//! What is not ported: `None`, `bool`, `f64`, versionstamps, and nested
//! (depth > 0) elements. The layout never produces or consumes them; a
//! row whose encoding needs one of them cannot be a valid row of this
//! layout, and the strict decoders here reject it.
//!
//! Element codes (matching the `foundationdb-tuple` crate):
//!
//! | Code | Meaning |
//! |------|---------|
//! | `0x00` | terminator of a byte/string payload (after escaping) |
//! | `0x01` | BYTES element |
//! | `0x02` | STRING element |
//! | `0x0b` | start of the integer band (`NEGINTSTART`); `i64` values pack into `0x14±len` with `len` ≤ 8, so `i64` codes stay within `0x0c..=0x1c` |
//! | `0x14` | INT zero; `0x14+len` / `0x14-len` carry positive / negative integers |
//! | `0x1d` | end of the integer band (`POSINTEND`; not produced by `i64` packing) |
//! | `0xff` | escape: inside a byte/string payload, `0x00` is written as `0x00 0xff` |
//!
//! Byte/string payloads carry no length prefix: they end at the first
//! unescaped `0x00`. An element is therefore self-delimiting, which is
//! what makes the row keys decodable one element at a time.

/// Code for a BYTES element.
pub const CODE_BYTES: u8 = 0x01;
/// Code for a STRING element.
pub const CODE_STRING: u8 = 0x02;
/// Code for the integer `0`; `0x14 ± len` carries other integers.
pub const CODE_INT_ZERO: u8 = 0x14;
/// Escape byte: inside a payload, `0x00 0xff` stands for one `0x00`.
pub const ESCAPE: u8 = 0xff;

/// Pack an `i64` as a top-level tuple element.
///
/// `0` packs to the single byte `0x14`. `n > 0` packs to `0x14 + len`
/// followed by the `len` most significant big-endian bytes of `n`.
/// `n < 0` packs to `0x14 - len` followed by the `len` most significant
/// big-endian bytes of `n - 1`. `len` is the smallest count in 1..=8
/// that holds the value's magnitude representation.
///
/// Every `i64` packs to 1..=9 bytes and the first byte alone determines
/// the length (two integers of different lengths have different first
/// bytes), so no packed `i64` is a byte-prefix of another.
pub fn pack_i64(value: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    if value == 0 {
        out.push(CODE_INT_ZERO);
        return out;
    }
    // Ported from `foundationdb-tuple`'s `impl_ix!(i64, u64)`: `n` is the
    // minimal byte count holding the value's magnitude.
    let magnitude = value.wrapping_abs() as u64;
    let n = 8 - (magnitude.leading_zeros() as usize) / 8;
    if value > 0 {
        out.push(CODE_INT_ZERO + n as u8);
        out.extend_from_slice(&(value as u64).to_be_bytes()[8 - n..]);
    } else {
        // FDB encodes a negative n as the magnitude of n - 1: the payload
        // is the low `n` bytes of (value - 1) in two's complement.
        out.push(CODE_INT_ZERO - n as u8);
        out.extend_from_slice(&value.wrapping_sub(1).to_be_bytes()[8 - n..]);
    }
    out
}

/// Pack a byte string as a top-level BYTES element.
///
/// `0x01`, the payload with every `0x00` written as `0x00 0xff`, and a
/// terminating `0x00`. No length prefix.
pub fn pack_bytes(payload: &[u8]) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(2 + payload.len() + payload.iter().filter(|&&b| b == 0).count());
    out.push(CODE_BYTES);
    for &b in payload {
        out.push(b);
        if b == 0 {
            out.push(ESCAPE);
        }
    }
    out.push(0x00);
    out
}

/// Pack a string as a top-level STRING element (UTF-8 bytes with the
/// same escaping as [`pack_bytes`]).
pub fn pack_string(payload: &str) -> Vec<u8> {
    let mut out = pack_bytes(payload.as_bytes());
    out[0] = CODE_STRING;
    out
}

/// Decode a top-level integer element that fills `bytes` exactly.
///
/// `None` when the bytes are not a well-formed single integer element
/// (bad code byte, truncated payload, or trailing bytes after it).
pub fn unpack_i64_full(bytes: &[u8]) -> Option<i64> {
    let (&code, rest) = bytes.split_first()?;
    if code == CODE_INT_ZERO {
        return rest.is_empty().then_some(0);
    }
    let code = code as i8;
    let (positive, n) = if code > CODE_INT_ZERO as i8 {
        (true, (code - CODE_INT_ZERO as i8) as usize)
    } else if code < CODE_INT_ZERO as i8 {
        (false, (CODE_INT_ZERO as i8 - code) as usize)
    } else {
        return None;
    };
    if !(1..=8).contains(&n) || rest.len() != n {
        return None;
    }
    // Sign-extend exactly as `foundationdb-tuple` does (zero for
    // positives, 0xFF for negatives). Like the FDB decoder, non-minimal
    // encodings (a leading zero payload byte) decode to the same value
    // rather than being rejected.
    let mut arr = [0u8; 8];
    if !positive {
        for b in arr.iter_mut() {
            *b = 0xFF;
        }
    }
    arr[8 - n..].copy_from_slice(rest);
    let v = i64::from_be_bytes(arr);
    if positive {
        (v >= 0).then_some(v)
    } else {
        // FDB stores the magnitude of value - 1, so add one back
        // (wrapping: the all-0xFF 8-byte payload decodes to `i64::MIN`).
        let x = v.wrapping_add(1);
        (x <= 0).then_some(x)
    }
}

/// Walk an element payload from the start of `rest` (the bytes after
/// the code byte). Returns the extent of the element in `rest` — up to
/// and including the terminating 0x00 — or `None` when the payload runs
/// off the end without a terminator.
fn element_extent(rest: &[u8]) -> Option<usize> {
    let mut i = 0;
    while i < rest.len() {
        let b = rest[i];
        if b == 0 {
            if i + 1 < rest.len() && rest[i + 1] == ESCAPE {
                i += 2;
            } else {
                return Some(i + 1);
            }
        } else {
            i += 1;
        }
    }
    None
}

/// Unescape a payload (the element bytes without the terminator).
pub fn unescape_payload(escaped: &[u8]) -> Option<Vec<u8>> {
    let mut payload = Vec::with_capacity(escaped.len());
    let mut i = 0;
    while i < escaped.len() {
        let b = escaped[i];
        if b == 0 {
            if i + 1 < escaped.len() && escaped[i + 1] == ESCAPE {
                payload.push(0);
                i += 2;
            } else {
                return None;
            }
        } else {
            payload.push(b);
            i += 1;
        }
    }
    Some(payload)
}

/// Decode a top-level BYTES or STRING element that fills `bytes`
/// exactly, returning the unescaped payload (owned: escapes shorten the
/// payload, so it is not a slice of the input).
///
/// `None` on a bad code byte, an unterminated payload, or trailing bytes
/// after the terminator.
pub fn unpack_element_full(bytes: &[u8]) -> Option<Vec<u8>> {
    let (&code, rest) = bytes.split_first()?;
    if code != CODE_BYTES && code != CODE_STRING {
        return None;
    }
    let extent = element_extent(rest)?;
    if extent != rest.len() {
        return None;
    }
    unescape_payload(&rest[..extent - 1])
}

/// Decode the leading BYTES or STRING element of `key` (up to its
/// terminator) and return the unescaped payload (owned); the bytes after
/// the terminator are ignored. `None` on a bad code byte or an
/// unterminated payload.
pub fn first_element_payload(key: &[u8]) -> Option<Vec<u8>> {
    let (&code, rest) = key.split_first()?;
    if code != CODE_BYTES && code != CODE_STRING {
        return None;
    }
    let extent = element_extent(rest)?;
    unescape_payload(&rest[..extent - 1])
}

/// Decode a top-level STRING element that fills `bytes` exactly, as UTF-8.
pub fn unpack_string_full(bytes: &[u8]) -> Option<String> {
    let (&code, _rest) = bytes.split_first()?;
    if code != CODE_STRING {
        return None;
    }
    core::str::from_utf8(&unpack_element_full(bytes)?)
        .ok()
        .map(str::to_owned)
}
/// Decode the child-row key trailing a node subspace: a depth-0 tuple
/// `(0i64, STRING(name))` filling `bytes` exactly.
///
/// Returns the segment name (owned). `None` when the first element is
/// not the integer `0` or the second is not a well-formed string element.
pub fn unpack_child_name(bytes: &[u8]) -> Option<String> {
    let int_zero = pack_i64(0);
    let (first, rest) = bytes.split_at_checked(int_zero.len())?;
    if first != int_zero.as_slice() {
        return None;
    }
    unpack_string_full(rest)
}

/// Decode the first element of a key and return its payload, without
/// requiring the rest to be well-formed.
///
/// Used by the node-region "does an existing node contain this key"
/// probe, where only the leading element (the node's allocated prefix)
/// matters. The element must be BYTES or STRING; a key starting with any
/// other element (an integer, a nested tuple) has no leading byte-string
/// and returns `None`.
pub fn first_bytes_element(key: &[u8]) -> Option<&[u8]> {
    let (&code, rest) = key.split_first()?;
    if code != CODE_BYTES && code != CODE_STRING {
        return None;
    }
    // Walk to the terminating unescaped 0x00.
    let mut i = 0;
    while i < rest.len() {
        if rest[i] == 0 {
            if i + 1 < rest.len() && rest[i + 1] == ESCAPE {
                i += 2;
            } else {
                return Some(&rest[..i]);
            }
        } else {
            i += 1;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // Vectors pinned from the `foundationdb-tuple` crate's own test
    // suite (the crate the fdb backend builds against): these are the
    // byte-identity ground truth for the port.

    #[test]
    fn integer_vectors() {
        assert_eq!(pack_i64(0), vec![0x14]);
        assert_eq!(pack_i64(1), vec![0x15, 0x01]);
        assert_eq!(pack_i64(-1), vec![0x13, 0xfe]);
        assert_eq!(pack_i64(100), vec![0x15, 100]);
        assert_eq!(pack_i64(255), vec![0x15, 0xff]);
        assert_eq!(pack_i64(256), vec![0x16, 0x01, 0x00]);
        assert_eq!(pack_i64(-256), vec![0x12, 0xfe, 0xff]);
        assert_eq!(pack_i64(-257), vec![0x12, 0xfe, 0xfe]);
        assert_eq!(
            pack_i64(i64::MAX),
            vec![0x1c, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
        );
        assert_eq!(
            pack_i64(i64::MIN),
            vec![0x0c, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
        );
    }

    #[test]
    fn byte_and_string_vectors() {
        assert_eq!(pack_bytes(b""), vec![0x01, 0x00]);
        assert_eq!(pack_bytes(b"\0"), vec![0x01, 0x00, 0xff, 0x00]);
        assert_eq!(
            pack_bytes(b"foo\0bar"),
            vec![0x01, 0x66, 0x6f, 0x6f, 0x00, 0xff, 0x62, 0x61, 0x72, 0x00]
        );
        assert_eq!(pack_string(""), vec![0x02, 0x00]);
        // "FÔO\0bar" (Ô = U+00D4 = c3 94 in UTF-8).
        assert_eq!(
            pack_string("F\u{00d4}O\0bar"),
            vec![
                0x02, 0x46, 0xc3, 0x94, 0x4f, 0x00, 0xff, 0x62, 0x61, 0x72, 0x00
            ]
        );
        assert_eq!(pack_string("hca"), vec![0x02, 0x68, 0x63, 0x61, 0x00]);
    }

    #[test]
    fn integer_round_trips() {
        let values: [i64; 18] = [
            0,
            1,
            -1,
            2,
            -2,
            127,
            128,
            -128,
            129,
            -129,
            255,
            256,
            -256,
            -257,
            0x7fff_ffff,
            i64::MAX,
            i64::MIN,
            -i64::MAX,
        ];
        for v in values {
            let packed = pack_i64(v);
            assert_eq!(unpack_i64_full(&packed), Some(v), "round trip {v}");
        }
    }

    #[test]
    fn byte_round_trips() {
        let payloads: [&[u8]; 6] = [
            b"",
            b"\0",
            b"\0\0",
            b"foo",
            b"foo\0bar\0",
            &[0xff, 0x00, 0xff, 0x01, 0x00, 0x02],
        ];
        for p in payloads {
            assert_eq!(
                unpack_element_full(&pack_bytes(p)),
                Some(p.to_vec()),
                "round trip {p:?}"
            );
        }
        for p in ["", "a", "hca", "version", "layer", "\u{00d4}\0"] {
            assert_eq!(
                unpack_string_full(&pack_string(p)),
                Some(p.to_string()),
                "round trip {p:?}"
            );
        }
    }

    #[test]
    fn packed_integers_are_prefix_free() {
        // The layout's core disjointness property: no packed i64 is a
        // byte-prefix of another, so `prefix ‖ key` needs no separator.
        let values: Vec<i64> = (0..4000).chain(-4000..0).collect();
        for (i, &a) in values.iter().enumerate() {
            let pa = pack_i64(a);
            for &b in values.iter().skip(i + 1) {
                let pb = pack_i64(b);
                assert!(
                    !pa.starts_with(&pb) && !pb.starts_with(&pa),
                    "{a} and {b} must not prefix each other"
                );
            }
        }
    }

    #[test]
    fn strict_decoders_reject_malformed_input() {
        assert_eq!(unpack_i64_full(&[]), None);
        assert_eq!(unpack_i64_full(&[0x14, 0x00]), None); // trailing after zero
        assert_eq!(unpack_i64_full(&[0x15]), None); // truncated payload
        assert_eq!(unpack_i64_full(&[0x00]), None); // not an integer code
        assert_eq!(unpack_i64_full(&[0x01, 0x00]), None); // an element, not an int
        // Band edges: codes 0x0b and 0x1d imply a 9-byte payload, which
        // no i64 encoding emits (i64 packs within `0x0c..=0x1c`).
        assert_eq!(unpack_i64_full(&[0x0b; 10]), None);
        assert_eq!(unpack_i64_full(&[0x1d, 0, 0, 0, 0, 0, 0, 0, 0, 0]), None);
        assert_eq!(unpack_element_full(&[]), None);
        assert_eq!(unpack_element_full(&[0x01]), None); // unterminated
        assert_eq!(unpack_element_full(&[0x01, 0x00, 0x00]), None); // trailing
        assert_eq!(unpack_element_full(&[0x14]), None); // integer code
        // A string element with invalid UTF-8 payload.
        assert_eq!(unpack_string_full(&[0x02, 0xff, 0x00]), None);
        assert_eq!(
            unpack_string_full(&[0x02, 0x41, 0x00]).unwrap(),
            "A".to_string()
        );
    }

    /// Non-minimal integer encodings (a leading zero payload byte)
    /// decode to the same value rather than being rejected — verbatim
    /// FDB.
    #[test]
    fn non_minimal_integers_decode_to_the_same_value() {
        // 5 minimally packs as [0x15, 0x05]; the 2-byte form must
        // decode identically.
        assert_eq!(unpack_i64_full(&[0x15, 0x05]), Some(5));
        assert_eq!(unpack_i64_full(&[0x16, 0x00, 0x05]), Some(5));
        // -1 minimally packs as [0x13, 0xFE]; the 2-byte form must
        // decode identically.
        assert_eq!(unpack_i64_full(&[0x13, 0xFE]), Some(-1));
        assert_eq!(unpack_i64_full(&[0x12, 0xFF, 0xFE]), Some(-1));
    }

    #[test]
    fn child_name_round_trips() {
        for name in ["", "a", "hca", "version", "\u{00d4}\0", "z z"] {
            let mut key = pack_i64(0);
            key.extend_from_slice(&pack_string(name));
            assert_eq!(unpack_child_name(&key), Some(name.to_string()));
        }
        // First element not 0.
        let mut key = pack_i64(1);
        key.extend_from_slice(&pack_string("a"));
        assert_eq!(unpack_child_name(&key), None);
        // Missing the name element.
        assert_eq!(unpack_child_name(&pack_i64(0)), None);
    }

    #[test]
    fn first_element_of_a_node_key() {
        // A node row: NODE_PREFIX ‖ BYTES(prefix) ‖ child/layer tail.
        // Only the leading element is needed by the containment probe.
        let mut key = pack_bytes(&[0x15, 0x01]);
        key.extend_from_slice(&pack_i64(0));
        key.extend_from_slice(&pack_string("child"));
        assert_eq!(first_bytes_element(&key), Some(&[0x15, 0x01][..]));
        // An integer-led key has no leading byte-string.
        assert_eq!(first_bytes_element(&pack_i64(5)), None);
        // Unterminated payload.
        assert_eq!(first_bytes_element(&[0x01, 0x41]), None);
    }
}
