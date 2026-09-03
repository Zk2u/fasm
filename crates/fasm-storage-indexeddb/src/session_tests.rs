//! Browser tests for buffered point operations and committed-data readers.

use std::ops::Bound;

use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;
use web_sys::{IdbDatabase, IdbFactory, IdbRequest, IdbTransactionMode};

use crate::{
    IndexedDbError, IndexedDbStore, Revision,
    idb::{
        KV_STORE, META_STORE, REVISION_KEY, RequestFuture, TransactionOutcome, bytes_to_js,
        dom_error, fixture::await_complete, fixture::unique_name, global_factory, revision_to_js,
    },
    store::Scope,
};

fn from_js<T>(result: Result<T, JsValue>) -> Result<T, IndexedDbError> {
    result.map_err(|value| dom_error(&value))
}

async fn seed_committed(
    store: &IndexedDbStore,
    rows: &[(&[u8], &[u8])],
) -> Result<(), IndexedDbError> {
    let transaction = store.begin(IdbTransactionMode::Readwrite, Scope::Kv)?;
    let outcome = TransactionOutcome::new(transaction.clone());
    let object_store = from_js(transaction.object_store(KV_STORE))?;
    for (key, value) in rows {
        from_js(object_store.put_with_key(&bytes_to_js(value), &bytes_to_js(key)))?;
    }
    await_complete(outcome).await
}

async fn put_raw_meta(store: &IndexedDbStore, value: &JsValue) -> Result<(), IndexedDbError> {
    let transaction = store.begin(IdbTransactionMode::Readwrite, Scope::Meta)?;
    let outcome = TransactionOutcome::new(transaction.clone());
    let metadata = from_js(transaction.object_store(META_STORE))?;
    from_js(metadata.put_with_key(value, &JsValue::from_str(REVISION_KEY)))?;
    await_complete(outcome).await
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
async fn session_reads_its_writes_without_changing_committed_data() -> Result<(), IndexedDbError> {
    let name = unique_name("read-writes");
    let store = IndexedDbStore::open(&name).await?;
    let mut session = store.transaction().await?;
    session.write(b"k", b"v");

    assert_eq!(session.read(b"k").await?, Some(b"v".to_vec()));
    assert!(session.contains(b"k").await?);
    assert_eq!(store.reader().read(b"k").await?, None);

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn tombstone_hides_committed_key_only_in_session() -> Result<(), IndexedDbError> {
    let name = unique_name("tombstone");
    let store = IndexedDbStore::open(&name).await?;
    seed_committed(&store, &[(&b"k"[..], &b"v"[..])]).await?;
    let mut session = store.transaction().await?;
    session.remove(b"k");

    assert_eq!(session.read(b"k").await?, None);
    assert!(!session.contains(b"k").await?);
    assert_eq!(store.reader().read(b"k").await?, Some(b"v".to_vec()));

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn reader_sees_only_committed_data() -> Result<(), IndexedDbError> {
    let name = unique_name("reader-committed");
    let store = IndexedDbStore::open(&name).await?;
    seed_committed(&store, &[(&b"a"[..], &b"va"[..])]).await?;
    let mut session = store.transaction().await?;
    session.write(b"b", b"vb");
    let reader = store.reader();

    assert_eq!(reader.read(b"a").await?, Some(b"va".to_vec()));
    assert!(reader.contains(b"a").await?);
    assert_eq!(reader.read(b"b").await?, None);
    assert!(!reader.contains(b"b").await?);

    drop(session);
    drop(reader);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn unbounded_clear_tombstones_every_committed_key() -> Result<(), IndexedDbError> {
    let name = unique_name("clear-all");
    let store = IndexedDbStore::open(&name).await?;
    seed_committed(
        &store,
        &[
            (&b"a"[..], &b"va"[..]),
            (&b"b"[..], &b"vb"[..]),
            (&b"c"[..], &b"vc"[..]),
        ],
    )
    .await?;
    let mut session = store.transaction().await?;
    session.clear(Bound::Unbounded, Bound::Unbounded).await?;

    assert_eq!(session.read(b"a").await?, None);
    assert_eq!(session.read(b"b").await?, None);
    assert_eq!(session.read(b"c").await?, None);
    assert_eq!(session.pending_len(), 3);

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn write_after_clear_reinserts_key() -> Result<(), IndexedDbError> {
    let name = unique_name("clear-reinsert");
    let store = IndexedDbStore::open(&name).await?;
    seed_committed(&store, &[(&b"a"[..], &b"v1"[..])]).await?;
    let mut session = store.transaction().await?;
    session
        .clear(Bound::Included(b"a"), Bound::Included(b"a"))
        .await?;
    session.write(b"a", b"v2");

    assert_eq!(session.read(b"a").await?, Some(b"v2".to_vec()));

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn clear_tombstones_buffered_and_committed_keys() -> Result<(), IndexedDbError> {
    let name = unique_name("clear-overlay");
    let store = IndexedDbStore::open(&name).await?;
    seed_committed(&store, &[(&b"a"[..], &b"va"[..])]).await?;
    let mut session = store.transaction().await?;
    session.write(b"b", b"vb");
    session
        .clear(Bound::Included(b"a"), Bound::Included(b"b"))
        .await?;

    assert_eq!(session.read(b"a").await?, None);
    assert_eq!(session.read(b"b").await?, None);
    assert_eq!(session.pending_len(), 2);

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn empty_and_inverted_clears_are_no_ops() -> Result<(), IndexedDbError> {
    let name = unique_name("clear-empty");
    let store = IndexedDbStore::open(&name).await?;
    seed_committed(&store, &[(&b"a"[..], &b"va"[..])]).await?;
    let mut session = store.transaction().await?;
    session
        .clear(Bound::Included(b"c"), Bound::Included(b"a"))
        .await?;
    session
        .clear(Bound::Excluded(b"a"), Bound::Included(b"a"))
        .await?;

    assert_eq!(session.pending_len(), 0);
    assert_eq!(session.read(b"a").await?, Some(b"va".to_vec()));

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn mutation_order_around_clear_is_preserved() -> Result<(), IndexedDbError> {
    let name = unique_name("clear-order");
    let store = IndexedDbStore::open(&name).await?;
    seed_committed(&store, &[(&b"d"[..], &b"vd"[..])]).await?;
    let mut session = store.transaction().await?;
    session.write(b"a", b"va");
    session
        .clear(Bound::Included(b"a"), Bound::Included(b"c"))
        .await?;
    session.write(b"b", b"vb");

    assert_eq!(session.read(b"a").await?, None);
    assert_eq!(session.read(b"b").await?, Some(b"vb".to_vec()));
    assert_eq!(session.read(b"d").await?, Some(b"vd".to_vec()));

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn session_captures_and_validates_revision() -> Result<(), IndexedDbError> {
    let name = unique_name("session-revision");
    let store = IndexedDbStore::open(&name).await?;
    assert_eq!(
        store.transaction().await?.expected_revision(),
        Revision::ZERO
    );

    let five = Revision::from_f64(5.0)?;
    put_raw_meta(&store, &revision_to_js(five)).await?;
    assert_eq!(store.transaction().await?.expected_revision(), five);

    put_raw_meta(&store, &JsValue::from_str("x")).await?;
    assert!(matches!(
        store.transaction().await,
        Err(IndexedDbError::Corrupt { .. })
    ));

    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn closed_store_rejects_sessions_and_reader_operations() -> Result<(), IndexedDbError> {
    let name = unique_name("session-closed");
    let store = IndexedDbStore::open(&name).await?;
    let factory = global_factory()?;
    let upgraded = raw_open(&factory, &name, 2).await?;

    assert!(matches!(
        store.transaction().await,
        Err(IndexedDbError::Closed)
    ));
    assert!(matches!(
        store.reader().read(b"k").await,
        Err(IndexedDbError::Closed)
    ));

    upgraded.close();
    drop(store);
    IndexedDbStore::delete(&name).await
}
