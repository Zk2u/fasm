//! Browser tests for binary conversions, ordering, ranges, and revisions.

use std::ops::Bound;

use wasm_bindgen::JsValue;
use wasm_bindgen_test::wasm_bindgen_test;
use web_sys::{IdbDatabase, IdbTransactionMode};

use super::{
    KeyRange, RequestFuture, TransactionOutcome, bytes_from_js, bytes_to_js, dom_error, key_range,
    read_cursor_page, revision_from_js, revision_to_js,
};
use crate::idb::fixture::{await_complete, object_store, raw_database};
use crate::{IndexedDbError, Revision};

fn from_js<T>(result: Result<T, JsValue>) -> Result<T, IndexedDbError> {
    result.map_err(|value| dom_error(&value))
}

async fn put_binary_rows(
    database: &IdbDatabase,
    rows: &[(&[u8], &[u8])],
) -> Result<(), IndexedDbError> {
    let (transaction, store) = object_store(database, IdbTransactionMode::Readwrite)?;
    let outcome = TransactionOutcome::new(transaction);
    for (key, value) in rows {
        from_js(store.put_with_key(&bytes_to_js(value), &bytes_to_js(key)))?;
    }
    await_complete(outcome).await
}

async fn scan_range(
    database: &IdbDatabase,
    range: KeyRange,
) -> Result<Vec<Vec<u8>>, IndexedDbError> {
    let range = match range {
        KeyRange::Empty => return Ok(Vec::new()),
        KeyRange::All => None,
        KeyRange::Bounded(range) => Some(range),
    };

    let (transaction, store) = object_store(database, IdbTransactionMode::Readonly)?;
    let outcome = TransactionOutcome::new(transaction);
    let request = match range {
        Some(range) => from_js(store.open_cursor_with_range(range.as_ref()))?,
        None => from_js(store.open_cursor())?,
    };
    let page = read_cursor_page(request, 16).await?;
    await_complete(outcome).await?;
    page.rows
        .iter()
        .map(|(key, _)| bytes_from_js(key, "cursor key"))
        .collect()
}

#[wasm_bindgen_test]
async fn binary_values_round_trip_and_cursor_keys_follow_byte_order() -> Result<(), IndexedDbError>
{
    let raw = raw_database("binary-order").await?;
    let keys: &[&[u8]] = &[&[], &[0], &[0, 0], &[1], &[0xff], &[0xff, 0xff]];
    let rows = keys.iter().map(|key| (*key, *key)).collect::<Vec<_>>();
    put_binary_rows(&raw.database, &rows).await?;

    let (transaction, store) = object_store(&raw.database, IdbTransactionMode::Readonly)?;
    let outcome = TransactionOutcome::new(transaction);
    let requests = keys
        .iter()
        .map(|key| from_js(store.get(&bytes_to_js(key))).map(RequestFuture::new))
        .collect::<Result<Vec<_>, _>>()?;
    for (request, expected) in requests.into_iter().zip(keys) {
        let value = request.await?;
        assert_eq!(bytes_from_js(&value, "value")?, *expected);
    }
    await_complete(outcome).await?;

    assert_eq!(
        scan_range(&raw.database, KeyRange::All).await?,
        keys.iter().map(|key| key.to_vec()).collect::<Vec<_>>()
    );
    raw.close_and_delete().await
}

#[wasm_bindgen_test]
async fn key_ranges_match_rust_bounds_without_constructing_empty_ranges()
-> Result<(), IndexedDbError> {
    let raw = raw_database("key-ranges").await?;
    let keys: &[&[u8]] = &[b"a", b"b", b"c", b"d", b"e"];
    let rows = keys.iter().map(|key| (*key, *key)).collect::<Vec<_>>();
    put_binary_rows(&raw.database, &rows).await?;

    let cases = [
        (Bound::Unbounded, Bound::Unbounded, keys),
        (Bound::Unbounded, Bound::Included(&b"d"[..]), &keys[..4]),
        (Bound::Unbounded, Bound::Excluded(&b"d"[..]), &keys[..3]),
        (Bound::Included(&b"b"[..]), Bound::Unbounded, &keys[1..]),
        (Bound::Excluded(&b"b"[..]), Bound::Unbounded, &keys[2..]),
        (
            Bound::Included(&b"b"[..]),
            Bound::Included(&b"d"[..]),
            &keys[1..4],
        ),
        (
            Bound::Included(&b"b"[..]),
            Bound::Excluded(&b"d"[..]),
            &keys[1..3],
        ),
        (
            Bound::Excluded(&b"b"[..]),
            Bound::Included(&b"d"[..]),
            &keys[2..4],
        ),
        (
            Bound::Excluded(&b"b"[..]),
            Bound::Excluded(&b"d"[..]),
            &keys[2..3],
        ),
    ];

    for (start, end, expected) in cases {
        let actual = scan_range(&raw.database, key_range(start, end)?).await?;
        assert_eq!(
            actual,
            expected.iter().map(|key| key.to_vec()).collect::<Vec<_>>()
        );
    }

    assert!(matches!(
        key_range(Bound::Included(b"c"), Bound::Included(b"a"))?,
        KeyRange::Empty
    ));
    assert_eq!(
        scan_range(
            &raw.database,
            key_range(Bound::Included(b"c"), Bound::Included(b"c"))?
        )
        .await?,
        vec![b"c".to_vec()]
    );
    assert!(matches!(
        key_range(Bound::Excluded(b"c"), Bound::Included(b"c"))?,
        KeyRange::Empty
    ));
    assert!(matches!(
        key_range(Bound::Included(b"c"), Bound::Excluded(b"c"))?,
        KeyRange::Empty
    ));
    raw.close_and_delete().await
}

#[wasm_bindgen_test]
fn invalid_binary_value_reports_role_and_type_without_contents() {
    let error = bytes_from_js(&JsValue::from_str("nope"), "key");
    let Err(IndexedDbError::Corrupt { detail }) = error else {
        panic!("non-binary value did not produce a corruption error");
    };
    assert!(detail.contains("key"), "{detail}");
    assert!(detail.contains("string"), "{detail}");
    assert!(!detail.contains("nope"), "{detail}");
}

#[wasm_bindgen_test]
fn revisions_are_checked_at_the_javascript_boundary() -> Result<(), IndexedDbError> {
    let missing = revision_from_js(&JsValue::UNDEFINED);
    let Err(IndexedDbError::Corrupt { detail }) = missing else {
        panic!("missing revision did not produce a corruption error");
    };
    assert!(detail.contains("missing"), "{detail}");

    assert!(matches!(
        revision_from_js(&JsValue::from_f64(1.5)),
        Err(IndexedDbError::Corrupt { .. })
    ));
    assert!(matches!(
        revision_from_js(&JsValue::from_str("3")),
        Err(IndexedDbError::Corrupt { .. })
    ));
    assert_eq!(revision_from_js(&JsValue::from_f64(3.0))?.get(), 3);
    assert_eq!(
        revision_from_js(&revision_to_js(Revision::ZERO))?,
        Revision::ZERO
    );
    Ok(())
}
