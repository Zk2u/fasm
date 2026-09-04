//! Browser integration tests for the shared directory engine over snapshots.

use fasm_storage::{
    KeyError, KvDirNav, KvStore,
    flatdir::{LAYOUT_VERSION, VERSION_KEY, ops},
};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::wasm_bindgen_test;
use web_sys::IdbTransactionMode;

use crate::{
    IndexedDbError, IndexedDbStore,
    idb::{
        KV_STORE, TransactionOutcome, bytes_to_js, dom_error, fixture::await_complete,
        fixture::unique_name,
    },
    session::IndexedDbTransaction,
    store::Scope,
};

fn from_js<T>(result: Result<T, JsValue>) -> Result<T, IndexedDbError> {
    result.map_err(|value| dom_error(&value))
}

async fn put_raw(store: &IndexedDbStore, key: &[u8], value: &[u8]) -> Result<(), IndexedDbError> {
    let transaction = store.begin(IdbTransactionMode::Readwrite, Scope::Kv)?;
    let outcome = TransactionOutcome::new(transaction.clone());
    let kv = from_js(transaction.object_store(KV_STORE))?;
    from_js(kv.put_with_key(&bytes_to_js(value), &bytes_to_js(key)))?;
    await_complete(outcome).await
}

async fn fresh(
    test: &str,
) -> Result<(String, IndexedDbStore, IndexedDbTransaction), IndexedDbError> {
    let name = unique_name(test);
    let store = IndexedDbStore::open(&name).await?;
    let session = store.transaction().await?;
    Ok((name, store, session))
}

#[wasm_bindgen_test]
async fn fresh_reads_are_empty_without_initialising() -> Result<(), IndexedDbError> {
    let (name, store, session) = fresh("flat-fresh").await?;

    assert_eq!(session.get(&[], b"key").await?, None);
    assert!(session.list_dirs(&[]).await?.is_empty());
    assert!(!session.dir_exists(&[]).await?);
    assert_eq!(session.raw_read(VERSION_KEY).await?, None);
    assert_eq!(session.pending_len(), 0);

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn first_write_initialises_all_root_metadata() -> Result<(), IndexedDbError> {
    let (name, store, mut session) = fresh("flat-init").await?;
    session.set(&[], b"key", b"value").await?;

    assert_eq!(
        session.raw_read(VERSION_KEY).await?,
        Some(LAYOUT_VERSION.to_vec())
    );
    ops::validate(session.engine.raw()).expect("shared directory layout validates");
    assert_eq!(session.get(&[], b"key").await?, Some(b"value".to_vec()));

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn foreign_content_fails_reads_and_writes_closed() -> Result<(), IndexedDbError> {
    let name = unique_name("flat-foreign");
    let store = IndexedDbStore::open(&name).await?;
    let mut version = LAYOUT_VERSION.to_vec();
    version[..4].copy_from_slice(&2_u32.to_le_bytes());
    put_raw(&store, VERSION_KEY, &version).await?;
    let mut session = store.transaction().await?;

    assert!(matches!(
        session.get(&[], b"key").await,
        Err(IndexedDbError::Foreign)
    ));
    assert!(matches!(
        session.set(&[], b"key", b"value").await,
        Err(IndexedDbError::Foreign)
    ));

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn nested_allocation_creates_ancestors_and_lists_sorted_children()
-> Result<(), IndexedDbError> {
    let (name, store, mut session) = fresh("flat-nested").await?;
    session.set(&[b"z"], b"k", b"z").await?;
    session.set(&[b"a", b"leaf"], b"k", b"a").await?;
    session.set(&[b"m"], b"k", b"m").await?;

    assert!(session.dir_exists(&[b"a"]).await?);
    assert!(session.dir_exists(&[b"a", b"leaf"]).await?);
    assert_eq!(
        session.list_dirs(&[]).await?,
        vec![b"a".to_vec(), b"m".to_vec(), b"z".to_vec()]
    );
    assert_eq!(session.list_dirs(&[b"a"]).await?, vec![b"leaf".to_vec()]);

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn remove_directory_is_recursive_and_missing_is_false() -> Result<(), IndexedDbError> {
    let (name, store, mut session) = fresh("flat-remove").await?;
    session.set(&[b"a"], b"parent", b"value").await?;
    session.set(&[b"a", b"b", b"c"], b"child", b"value").await?;

    assert!(session.remove_dir(&[b"a"]).await?);
    assert!(!session.remove_dir(&[b"a"]).await?);
    assert!(!session.dir_exists(&[b"a"]).await?);
    assert_eq!(session.get(&[b"a", b"b", b"c"], b"child").await?, None);
    assert!(session.list_dirs(&[]).await?.is_empty());

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn root_directory_is_not_removable() -> Result<(), IndexedDbError> {
    let (name, store, mut session) = fresh("flat-root-remove").await?;

    assert!(matches!(
        session.remove_dir(&[]).await,
        Err(IndexedDbError::Key(KeyError::RootNotRemovable))
    ));

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}

#[wasm_bindgen_test]
async fn non_utf8_directory_is_rejected_before_raw_io() -> Result<(), IndexedDbError> {
    let (name, store, mut session) = fresh("flat-invalid-dir").await?;

    assert!(matches!(
        session.set(&[&[0xff]], b"key", b"value").await,
        Err(IndexedDbError::Key(KeyError::DirSegmentNotUtf8 { .. }))
    ));
    assert_eq!(session.pending_len(), 0);
    assert_eq!(session.raw_read(VERSION_KEY).await?, None);

    drop(session);
    drop(store);
    IndexedDbStore::delete(&name).await
}
