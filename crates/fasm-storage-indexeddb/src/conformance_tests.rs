//! Shared `KvStore` conformance tests for browser IndexedDB sessions.
//!
//! The `kv_store_tests!` macro owns each store expression and exposes no
//! per-test teardown hook, so each evaluation best-effort deletes databases
//! left by earlier evaluations; the final database remains for the test run.

use std::cell::RefCell;

use wasm_bindgen_futures::spawn_local;

use crate::{IndexedDbStore, IndexedDbTransaction, idb::fixture::unique_name};

thread_local! {
    static CREATED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

async fn fresh_transaction() -> IndexedDbTransaction {
    let previous = CREATED.with(|created| created.borrow_mut().drain(..).collect::<Vec<_>>());
    for name in previous {
        spawn_local(async move {
            let _ = IndexedDbStore::delete(&name).await;
        });
    }

    let name = unique_name("conformance");
    CREATED.with(|created| created.borrow_mut().push(name.clone()));
    let store = IndexedDbStore::open(&name)
        .await
        .expect("open fresh conformance database");
    store
        .transaction()
        .await
        .expect("open fresh conformance session")
}

mod session_conformance {
    fasm_storage::kv_store_tests!(
        store = super::fresh_transaction().await,
        test_attr = wasm_bindgen_test::wasm_bindgen_test,
    );
    fasm_storage::kv_nav_tests!(
        store = super::fresh_transaction().await,
        test_attr = wasm_bindgen_test::wasm_bindgen_test,
    );
}

mod scoped_session_conformance {
    fasm_storage::kv_store_tests!(
        store = fasm_storage::ScopedKvStore::new(
            super::fresh_transaction().await,
            vec![b"idb".to_vec(), b"swap".to_vec()],
        ),
        test_attr = wasm_bindgen_test::wasm_bindgen_test,
    );
}

mod scoped_deep_conformance {
    fasm_storage::kv_store_tests!(
        store = fasm_storage::ScopedKvStore::new(
            super::fresh_transaction().await,
            vec![b"x".to_vec(), b"y".to_vec(), b"z".to_vec()],
        ),
        test_attr = wasm_bindgen_test::wasm_bindgen_test,
    );
}

mod root_pinned_conformance {
    fasm_storage::kv_store_tests!(
        store = fasm_storage::ScopedKvStore::new(super::fresh_transaction().await, Vec::new(),),
        test_attr = wasm_bindgen_test::wasm_bindgen_test,
    );
    fasm_storage::kv_nav_tests!(
        store = fasm_storage::ScopedKvStore::new(super::fresh_transaction().await, Vec::new(),),
        test_attr = wasm_bindgen_test::wasm_bindgen_test,
    );
}
