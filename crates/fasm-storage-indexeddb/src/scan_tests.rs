//! Browser tests for paged committed cursors and buffered range overlays.

use std::{collections::BTreeMap, ops::Bound};

use fasm_storage::{KvPair, KvStore};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;
use web_sys::{IdbDatabase, IdbFactory, IdbRequest};

use crate::{
    IndexedDbError, IndexedDbStore,
    idb::{
        RequestFuture, dom_error, fixture::seed_root_rows, fixture::unique_name, global_factory,
    },
    overlay::key_in_range,
};

fn from_js<T>(result: Result<T, JsValue>) -> Result<T, IndexedDbError> {
    result.map_err(|value| dom_error(&value))
}

fn key(index: u32) -> Vec<u8> {
    index.to_be_bytes().to_vec()
}

fn value(index: u32) -> Vec<u8> {
    format!("value-{index}").into_bytes()
}

async fn seed_rows(
    store: &IndexedDbStore,
    count: u32,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, IndexedDbError> {
    let mut oracle = BTreeMap::new();

    for index in 0..count {
        let key = key(index);
        let value = value(index);
        oracle.insert(key, value);
    }
    let rows = oracle
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect::<Vec<_>>();
    seed_root_rows(store, &rows).await?;
    Ok(oracle)
}

fn expected_range(
    oracle: &BTreeMap<Vec<u8>, Vec<u8>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    reverse: bool,
) -> Vec<KvPair> {
    let mut rows = oracle
        .iter()
        .filter(|(key, _)| key_in_range(key, start, end))
        .map(|(key, value)| KvPair {
            key: key.clone(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();
    if reverse {
        rows.reverse();
    }
    rows
}

async fn raw_open(
    factory: &IdbFactory,
    name: &str,
    version: u32,
) -> Result<IdbDatabase, IndexedDbError> {
    let request = from_js(factory.open_with_u32(name, version))?;
    let request: IdbRequest = request.unchecked_into();
    RequestFuture::new(request)
        .await?
        .dyn_into::<IdbDatabase>()
        .map_err(|_| IndexedDbError::Corrupt {
            detail: "raw open returned a non-database value".to_owned(),
        })
}

#[wasm_bindgen_test]
async fn mixed_overlay_crosses_committed_page_seams_in_both_directions()
-> Result<(), IndexedDbError> {
    let name = unique_name("scan-seams");
    let store = IndexedDbStore::open(&name).await?;
    let mut oracle = seed_rows(&store, 1_100).await?;
    let mut session = store.transaction().await?;

    for seam in [255_u32, 511, 767] {
        let mut inserted = key(seam);
        inserted.push(0);
        let inserted_value = format!("inserted-{seam}").into_bytes();
        session.set(&[], &inserted, &inserted_value).await?;
        oracle.insert(inserted, inserted_value);
    }

    session.set(&[], &key(256), b"overwritten-256").await?;
    oracle.insert(key(256), b"overwritten-256".to_vec());
    for deleted in [0_u32, 512, 1_099] {
        session.delete(&[], &key(deleted)).await?;
        oracle.remove(&key(deleted));
    }

    let forward = session
        .range(&[], Bound::Unbounded, Bound::Unbounded, false)
        .collect()
        .await?;
    assert_eq!(
        forward,
        expected_range(&oracle, Bound::Unbounded, Bound::Unbounded, false)
    );

    let reverse = session
        .range(&[], Bound::Unbounded, Bound::Unbounded, true)
        .collect()
        .await?;
    assert_eq!(
        reverse,
        expected_range(&oracle, Bound::Unbounded, Bound::Unbounded, true)
    );

    let lower = key(250);
    let upper = key(265);
    let bounded = session
        .range(
            &[],
            Bound::Included(lower.as_slice()),
            Bound::Excluded(upper.as_slice()),
            false,
        )
        .collect()
        .await?;
    assert_eq!(
        bounded,
        expected_range(
            &oracle,
            Bound::Included(lower.as_slice()),
            Bound::Excluded(upper.as_slice()),
            false,
        )
    );

    assert_eq!(
        session
            .range(&[], Bound::Unbounded, Bound::Unbounded, false)
            .take(1)
            .await?,
        expected_range(&oracle, Bound::Unbounded, Bound::Unbounded, false)[..1]
    );
    assert_eq!(
        session
            .range(&[], Bound::Unbounded, Bound::Unbounded, true)
            .take(1)
            .await?,
        expected_range(&oracle, Bound::Unbounded, Bound::Unbounded, true)[..1]
    );

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn all_tombstoned_pages_continue_to_visible_rows() -> Result<(), IndexedDbError> {
    let name = unique_name("scan-tombstoned-pages");
    let store = IndexedDbStore::open(&name).await?;
    let oracle = seed_rows(&store, 300).await?;

    let mut forward_session = store.transaction().await?;
    for index in 0..256 {
        forward_session.delete(&[], &key(index)).await?;
    }
    let forward = forward_session
        .range(&[], Bound::Unbounded, Bound::Unbounded, false)
        .collect()
        .await?;
    assert_eq!(
        forward,
        (256..300)
            .map(|index| KvPair {
                key: key(index),
                value: oracle[&key(index)].clone(),
            })
            .collect::<Vec<_>>()
    );

    let mut reverse_session = store.transaction().await?;
    for index in 44..300 {
        reverse_session.delete(&[], &key(index)).await?;
    }
    let reverse = reverse_session
        .range(&[], Bound::Unbounded, Bound::Unbounded, true)
        .collect()
        .await?;
    assert_eq!(
        reverse,
        (0..44)
            .rev()
            .map(|index| KvPair {
                key: key(index),
                value: oracle[&key(index)].clone(),
            })
            .collect::<Vec<_>>()
    );

    drop(forward_session);
    drop(reverse_session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn reader_range_ignores_session_buffer_and_rejects_mutations() -> Result<(), IndexedDbError> {
    let name = unique_name("reader-range");
    let store = IndexedDbStore::open(&name).await?;
    let oracle = seed_rows(&store, 3).await?;
    let mut session = store.transaction().await?;
    session.set(&[], &key(0), b"buffered-overwrite").await?;
    session.delete(&[], &key(1)).await?;
    session.set(&[], b"buffered-only", b"value").await?;
    session.set(&[b"nested"], b"only", b"nested-value").await?;

    assert_eq!(
        session
            .range(&[b"nested"], Bound::Unbounded, Bound::Unbounded, false)
            .collect()
            .await?,
        vec![KvPair {
            key: b"only".to_vec(),
            value: b"nested-value".to_vec(),
        }]
    );

    let mut reader = store.reader();
    assert_eq!(
        reader
            .range(&[], Bound::Unbounded, Bound::Unbounded, false)
            .collect()
            .await?,
        expected_range(&oracle, Bound::Unbounded, Bound::Unbounded, false)
    );
    assert!(matches!(
        reader.set(&[], b"key", b"value").await,
        Err(IndexedDbError::ReadOnly)
    ));
    assert!(matches!(
        reader.delete(&[], b"key").await,
        Err(IndexedDbError::ReadOnly)
    ));
    assert!(matches!(
        reader
            .clear_range(&[], Bound::Unbounded, Bound::Unbounded)
            .await,
        Err(IndexedDbError::ReadOnly)
    ));

    drop(reader);
    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn closed_store_range_defers_failure_to_first_next() -> Result<(), IndexedDbError> {
    let name = unique_name("closed-range");
    let store = IndexedDbStore::open(&name).await?;
    let factory = global_factory()?;
    let upgraded = raw_open(&factory, &name, 2).await?;

    let reader = store.reader();
    let result = reader
        .range(&[], Bound::Unbounded, Bound::Unbounded, false)
        .next()
        .await;
    assert!(matches!(&result, Err(IndexedDbError::Closed)));
    drop(result);

    upgraded.close();
    drop(reader);
    drop(store);
    IndexedDbStore::delete(&name).await
}
