//! Checked conversions between Rust storage types and IndexedDB values.

#[cfg(test)]
use std::ops::Bound;

#[cfg(test)]
use fasm_storage::is_empty_range;
use js_sys::{ArrayBuffer, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};
#[cfg(test)]
use web_sys::IdbKeyRange;

#[cfg(test)]
use super::dom_error;
use crate::{IndexedDbError, Revision};

/// The IndexedDB cursor range corresponding to a Rust pair of bounds.
#[cfg(test)]
pub(crate) enum KeyRange {
    /// The bounds contain no key, so callers must not issue a cursor or delete.
    Empty,
    /// Neither side is bounded, so callers should use an unbounded operation.
    All,
    /// IndexedDB can enforce the non-empty bounds with this native key range.
    Bounded(IdbKeyRange),
}

/// Copies Rust bytes into JavaScript-owned memory.
///
/// A copied [`Uint8Array`] remains valid if WebAssembly memory grows or moves;
/// a view over Rust memory would not. IndexedDB compares `BufferSource` keys
/// bytewise, placing a shorter key before a longer key when one is a prefix of
/// the other, so this conversion is the complete key encoding and also works
/// for values.
pub(crate) fn bytes_to_js(bytes: &[u8]) -> JsValue {
    Uint8Array::from(bytes).into()
}

/// Copies a JavaScript binary value into Rust-owned memory.
///
/// Cursors return binary keys as [`ArrayBuffer`] values, while other browser
/// paths can retain a [`Uint8Array`]. The error deliberately reports only the
/// schema role and JavaScript type, never application bytes.
pub(crate) fn bytes_from_js(value: &JsValue, what: &str) -> Result<Vec<u8>, IndexedDbError> {
    if value.is_instance_of::<ArrayBuffer>() {
        return Ok(Uint8Array::new(value).to_vec());
    }
    if let Some(array) = value.dyn_ref::<Uint8Array>() {
        return Ok(array.to_vec());
    }

    Err(IndexedDbError::Corrupt {
        detail: format!(
            "{what} is not a binary value (JavaScript type: {})",
            javascript_type(value)
        ),
    })
}

/// Converts Rust range semantics into IndexedDB's native key-range semantics.
///
/// Empty and inverted ranges are identified before calling JavaScript because
/// IndexedDB would throw `DataError` for some of them. In particular, the empty
/// byte string remains a valid bound and is never treated as unbounded.
#[cfg(test)]
pub(crate) fn key_range(
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
) -> Result<KeyRange, IndexedDbError> {
    if is_empty_range(&start, &end) {
        return Ok(KeyRange::Empty);
    }

    match (start, end) {
        (Bound::Unbounded, Bound::Unbounded) => Ok(KeyRange::All),
        (Bound::Included(lower), Bound::Unbounded) | (Bound::Excluded(lower), Bound::Unbounded) => {
            let open = matches!(start, Bound::Excluded(_));
            let lower = bytes_to_js(lower);
            IdbKeyRange::lower_bound_with_open(&lower, open)
                .map(KeyRange::Bounded)
                .map_err(|value| dom_error(&value))
        }
        (Bound::Unbounded, Bound::Included(upper)) | (Bound::Unbounded, Bound::Excluded(upper)) => {
            let open = matches!(end, Bound::Excluded(_));
            let upper = bytes_to_js(upper);
            IdbKeyRange::upper_bound_with_open(&upper, open)
                .map(KeyRange::Bounded)
                .map_err(|value| dom_error(&value))
        }
        (Bound::Included(lower), Bound::Included(upper))
        | (Bound::Included(lower), Bound::Excluded(upper))
        | (Bound::Excluded(lower), Bound::Included(upper))
        | (Bound::Excluded(lower), Bound::Excluded(upper)) => {
            let lower_open = matches!(start, Bound::Excluded(_));
            let upper_open = matches!(end, Bound::Excluded(_));
            let lower = bytes_to_js(lower);
            let upper = bytes_to_js(upper);
            IdbKeyRange::bound_with_lower_open_and_upper_open(
                &lower, &upper, lower_open, upper_open,
            )
            .map(KeyRange::Bounded)
            .map_err(|value| dom_error(&value))
        }
    }
}

/// Converts a checked revision into its exact JavaScript number form.
pub(crate) fn revision_to_js(revision: Revision) -> JsValue {
    JsValue::from_f64(revision.to_f64())
}

/// Validates a JavaScript revision record before constructing a [`Revision`].
pub(crate) fn revision_from_js(value: &JsValue) -> Result<Revision, IndexedDbError> {
    if value.is_undefined() || value.is_null() {
        return Err(IndexedDbError::Corrupt {
            detail: "revision record is missing".to_owned(),
        });
    }

    let number = value.as_f64().ok_or_else(|| IndexedDbError::Corrupt {
        detail: format!(
            "revision is not a number (JavaScript type: {})",
            javascript_type(value)
        ),
    })?;
    Revision::from_f64(number)
}

fn javascript_type(value: &JsValue) -> String {
    match value.js_typeof().as_string() {
        Some(value_type) => value_type,
        None => "unknown".to_owned(),
    }
}
