//! The [`kv_store_tests!`](crate::kv_store_tests) conformance macro.
//!
//! Gated behind the `test-utils` feature (and always available inside this
//! crate's own test build).

/// Generate the [`KvStore`](crate::KvStore) conformance suite for one backend.
///
/// Every backend must produce identical answers for ordering, bound semantics,
/// reverse scans and range deletes, because the key schemas above them are
/// written once against those answers. The near-term backend targets are
/// FoundationDB and redb, with an in-memory store for tests and simulations.
/// The same async bodies can be emitted either as synchronous `#[test]`
/// wrappers or through a caller-provided async test attribute, including a
/// browser runner whose futures are not `Send`. This macro is the shared
/// answer key.
///
/// # Invocation
///
/// ```ignore
/// #[cfg(test)]
/// mod conformance {
///     use my_backend::MyStore;
///
///     fasm_storage::kv_store_tests!(
///         store = MyStore::new(),
///         block_on = |fut| tokio::runtime::Builder::new_current_thread()
///             .build()
///             .expect("runtime")
///             .block_on(fut),
///     );
/// }
/// ```
///
/// A bare path works too when the runner is already a function:
///
/// ```ignore
/// fasm_storage::kv_store_tests!(
///     store = MyStore::new(),
///     block_on = futures::executor::block_on,
/// );
/// ```
///
/// An async test runner can instead be applied directly:
///
/// ```ignore
/// fasm_storage::kv_store_tests!(
///     store = MyStore::new(),
///     test_attr = tokio::test,
/// );
/// ```
///
/// The browser form uses `wasm-bindgen-test`:
///
/// ```ignore
/// fasm_storage::kv_store_tests!(
///     store = MyIndexedDbStore::open().await,
///     test_attr = wasm_bindgen_test::wasm_bindgen_test,
/// );
/// ```
///
/// A browser backend must call
/// `wasm_bindgen_test_configure!(run_in_browser)` itself: IndexedDB is a
/// browser API and is unavailable in wasm-bindgen-test's default Node runner.
///
/// The expansion defines free items (helper functions, a trait import) at the
/// invocation site, so **invoke it inside a dedicated module**. Invoke it once
/// per backend configuration you want covered.
///
/// # The `store` contract
///
/// `store` may be evaluated more than once per generated test. Every evaluation
/// must produce a **fresh, empty, independent** store. Tests write freely and
/// never clean up, so a builder that hands back shared state will
/// cross-contaminate. The expression is evaluated **inside** the generated
/// async body, so it may `.await` — `store = MyStore::open().await` is legal.
/// It must not block on a runtime of its own. If your backend needs setup that
/// can fail, panic in the builder.
///
/// # The `block_on` runner contract
///
/// `block_on` exists because this crate cannot know, and refuses to dictate,
/// which async runtime a backend uses. It is an expression evaluated **fresh
/// inside every generated test**, which must evaluate to something callable as
/// `runner(fut)` where:
///
/// - `fut` is a single future, passed by value, taking no arguments;
/// - the call **blocks** until that future completes and returns its output;
/// - the future is *not* required to be `Send` or `'static`, so a runner must
///   not impose either (`tokio::runtime::Runtime::block_on` and
///   `futures::executor::block_on` both qualify);
/// - it is called **exactly once** per test, with exactly one future.
///
/// Being evaluated per test means a closure works even though closures are not
/// generic: each test instantiates its own. A generic `fn` item such as
/// `futures::executor::block_on` works too, and so does any path to one.
///
/// Runners that require `Send + 'static` futures (for example a bare
/// `Handle::spawn` wrapper) do not satisfy the contract and will fail to
/// compile against some of the generated tests.
///
/// # The `test_attr` contract
///
/// `test_attr` is a path to an attribute macro that turns an `async fn` into a
/// test. The path is applied verbatim to every generated wrapper, and each
/// wrapper awaits the same body used by `block_on` mode. The attribute owns
/// runtime setup and any future bounds it imposes. `tokio::test` qualifies for
/// native tests; `wasm_bindgen_test::wasm_bindgen_test` qualifies in browser
/// builds and permits the thread-local futures browser storage requires.
///
/// # What is covered
///
/// Round-trips (`get`/`set`/`delete`/`exists`, overwrite, absent-key deletes)
/// across four directories (root, one-segment, two-segment, and a disjoint
/// one); cross-directory isolation — the same key bytes in different
/// directories never leak into each other's scans; lexicographic ordering
/// including prefix-shadowing keys (`[1]` vs `[1, 0]`) and `0xFF`-heavy keys
/// that stress successor arithmetic, within each directory; every combination
/// of `Included` / `Excluded` / `Unbounded` on both ends over the
/// within-directory key; reverse scans; empty and inverted ranges;
/// `clear_range` including the fully unbounded form (which empties a
/// directory without removing it), point-read visibility after a clear, and
/// re-inserting into a range that was just cleared; overwrites and deletes
/// observed *through a range scan* rather than only through `get`; paged
/// traversal over more than 1,000 rows in both directions, bounded and
/// unbounded; the empty key. It also covers [`KvStream`](crate::KvStream)'s
/// `next`/`take`/`collect`/`for_each`. The directory-navigation contract
/// (`list_dirs`, `dir_exists`, recursive `remove_dir`, root not removable,
/// missing directories empty, non-UTF-8 segments rejected) is covered by
/// [`kv_nav_tests!`] on the same store.
///
/// The store must implement [`KvDirNav`](crate::KvDirNav) even though the
/// data plane's own calls are `KvStore` calls: the suite observes
/// `clear_range`'s does-not-remove-the-directory contract through
/// `dir_exists`, and no other observation distinguishes an emptied
/// directory from a removed one.
///
/// Several of those exist for backends that buffer mutations and merge them
/// over a committed snapshot at read time, which a hand-rolled SQLite or
/// IndexedDB session does and FoundationDB does for you. Such a layer can emit
/// an overwritten key twice in one scan, leak a buffered tombstone into a range
/// read, or apply queued range tombstones after point writes and so lose a key
/// re-inserted into a just-cleared range. None of that is visible through `get`.
///
/// # What is not covered
///
/// Durability, concurrency, and [`Commit`](crate::Commit) semantics — a no-op
/// commit and a transactional commit cannot share a test body. Stream tests
/// check returned values and continuation behaviour, not backend fetch or
/// prefetch counts. The suite never pre-creates directories: a directory
/// exists from its first naming write, and the missing-directory cases cover
/// everything before that.
#[macro_export]
macro_rules! kv_store_tests {
    (store = $store:expr, block_on = $block_on:expr $(,)?) => {
        $crate::kv_store_tests!(@emit (block_on $block_on), $store);
    };
    (store = $store:expr, test_attr = $attr:path $(,)?) => {
        $crate::kv_store_tests!(@emit (test_attr $attr), $store);
    };
    (@emit $mode:tt, $store:expr) => {
        use $crate::__private::Bound;
        #[allow(unused_imports)]
        use $crate::KvDirNav as _;
        use $crate::KvStore as _;
        #[allow(unused_imports)]
        use $crate::RetryableStorageError as _;

        /// Bound over borrowed key bytes, as `KvStore::range` takes them.
        type ConformanceBound<'k> = $crate::__private::Bound<&'k [u8]>;

        /// The directories the suite writes: root, one segment, two segments,
        /// and a disjoint one.
        const D0: &[&[u8]] = &[];
        const D1: &[&[u8]] = &[b"a"];
        const D2: &[&[u8]] = &[b"a", b"b"];
        const D3: &[&[u8]] = &[b"c"];

        /// Ordering-stress keys, already in lexicographic order: the empty
        /// key, prefix-shadowing pairs, and `0xFF`-heavy keys.
        const K1: &[u8] = &[1u8];
        const K10: &[u8] = &[1u8, 0u8];
        const K2: &[u8] = &[2u8];
        const KFF: &[u8] = &[0xFFu8];
        const KFF0: &[u8] = &[0xFFu8, 0u8];
        const KFFFF: &[u8] = &[0xFFu8, 0xFFu8];
        const ORDER_KEYS: &[&[u8]] = &[b"", K1, K10, K2, KFF, KFF0, KFFFF];

        /// Write `key -> key` for each key in `dir`, so values identify keys.
        async fn seed<S: $crate::KvStore>(store: &mut S, dir: &[&[u8]], keys: &[&[u8]]) {
            for key in keys {
                store
                    .set(dir, key, key)
                    .await
                    .expect("seed write must succeed");
            }
        }

        /// Collect a range within `dir` as `(key, value)` pairs.
        async fn scan<S: $crate::KvStore>(
            store: &S,
            dir: &[&[u8]],
            start: ConformanceBound<'_>,
            end: ConformanceBound<'_>,
            reverse: bool,
        ) -> Vec<(Vec<u8>, Vec<u8>)> {
            store
                .range(dir, start, end, reverse)
                .collect()
                .await
                .expect("range scan must succeed")
                .into_iter()
                .map(|pair| (pair.key, pair.value))
                .collect()
        }

        /// Collect just the keys of a forward range within `dir`.
        async fn keys<S: $crate::KvStore>(
            store: &S,
            dir: &[&[u8]],
            start: ConformanceBound<'_>,
            end: ConformanceBound<'_>,
        ) -> Vec<Vec<u8>> {
            scan(store, dir, start, end, false)
                .await
                .into_iter()
                .map(|(key, _)| key)
                .collect()
        }

        fn owned(keys: &[&[u8]]) -> Vec<Vec<u8>> {
            keys.iter().map(|key| key.to_vec()).collect()
        }

        fn owned_pairs(pairs: &[(&[u8], &[u8])]) -> Vec<(Vec<u8>, Vec<u8>)> {
            pairs
                .iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect()
        }

        async fn kv_set_get_delete_round_trips_body() {
                let mut store = $store;

                for dir in &[D0, D1, D2, D3] {
                    seed(&mut store, dir, &[b"", b"one", b"two"]).await;
                    for key in [&b""[..], &b"one"[..], &b"two"[..]] {
                        assert_eq!(
                            $crate::KvStore::get(&store, dir, key).await.expect("get"),
                            Some(key.to_vec())
                        );
                        assert!(
                            $crate::KvStore::exists(&store, dir, key)
                                .await
                                .expect("exists")
                        );
                    }
                    // Overwrite.
                    store
                        .set(dir, b"one", b"replaced")
                        .await
                        .expect("overwrite");
                    assert_eq!(
                        $crate::KvStore::get(&store, dir, b"one")
                            .await
                            .expect("get overwritten"),
                        Some(b"replaced".to_vec())
                    );
                    // Delete; deleting an absent key is a no-op.
                    $crate::KvStore::delete(&mut store, dir, b"two")
                        .await
                        .expect("delete");
                    store
                        .delete(dir, b"never-was")
                        .await
                        .expect("delete absent is a no-op");
                    assert_eq!(
                        $crate::KvStore::get(&store, dir, b"two")
                            .await
                            .expect("get deleted"),
                        None
                    );
                    assert!(
                        !$crate::KvStore::exists(&store, dir, b"never-was")
                            .await
                            .expect("exists absent")
                    );
                }
        }

        async fn kv_scan_of_one_directory_never_yields_another_body() {
                let mut store = $store;

                // The same key bytes in three different directories, with
                // directory-identifying values.
                $crate::KvStore::set(&mut store, D1, b"k", b"from-a")
                    .await
                    .expect("set in a");
                $crate::KvStore::set(&mut store, D2, b"k", b"from-a-b")
                    .await
                    .expect("set in a/b");
                $crate::KvStore::set(&mut store, D3, b"k", b"from-c")
                    .await
                    .expect("set in c");
                $crate::KvStore::set(&mut store, D1, b"only-in-a", b"x")
                    .await
                    .expect("set a extra");
                $crate::KvStore::set(&mut store, D3, b"only-in-c", b"y")
                    .await
                    .expect("set c extra");

                assert_eq!(
                    scan(&store, D1, Bound::Unbounded, Bound::Unbounded, false).await,
                    owned_pairs(&[(b"k", b"from-a"), (b"only-in-a", b"x")])
                );
                assert_eq!(
                    scan(&store, D2, Bound::Unbounded, Bound::Unbounded, false).await,
                    owned_pairs(&[(b"k", b"from-a-b")])
                );
                assert_eq!(
                    scan(&store, D3, Bound::Unbounded, Bound::Unbounded, false).await,
                    owned_pairs(&[(b"k", b"from-c"), (b"only-in-c", b"y")])
                );
                // Nothing was written to the root.
                assert!(
                    scan(&store, D0, Bound::Unbounded, Bound::Unbounded, false)
                        .await
                        .is_empty()
                );
        }

        async fn kv_orders_keys_lexicographically_within_each_directory_body() {
                let mut store = $store;

                for dir in &[D0, D1, D2] {
                    seed(&mut store, dir, ORDER_KEYS).await;
                }
                let expected = owned(ORDER_KEYS);
                for dir in &[D0, D1, D2] {
                    assert_eq!(
                        keys(&store, dir, Bound::Unbounded, Bound::Unbounded).await,
                        expected,
                        "directory {dir:?}"
                    );
                }
        }

        async fn kv_bounds_matrix_selects_exactly_the_bounded_keys_body() {
                let mut store = $store;
                seed(&mut store, D1, &[b"a", b"b", b"c", b"d"]).await;

                let cases: [(ConformanceBound<'_>, ConformanceBound<'_>, &[&[u8]]); 9] = [
                    (
                        Bound::Unbounded,
                        Bound::Unbounded,
                        &[b"a", b"b", b"c", b"d"],
                    ),
                    (Bound::Unbounded, Bound::Included(b"c"), &[b"a", b"b", b"c"]),
                    (Bound::Unbounded, Bound::Excluded(b"c"), &[b"a", b"b"]),
                    (Bound::Included(b"b"), Bound::Unbounded, &[b"b", b"c", b"d"]),
                    (Bound::Excluded(b"b"), Bound::Unbounded, &[b"c", b"d"]),
                    (Bound::Included(b"b"), Bound::Included(b"c"), &[b"b", b"c"]),
                    (Bound::Included(b"b"), Bound::Excluded(b"c"), &[b"b"]),
                    (Bound::Excluded(b"b"), Bound::Included(b"c"), &[b"c"]),
                    (Bound::Excluded(b"b"), Bound::Excluded(b"c"), &[]),
                ];
                for (start, end, expected) in cases {
                    let got = scan(&store, D1, start, end, false).await;
                    let expected_pairs: Vec<(Vec<u8>, Vec<u8>)> =
                        expected.iter().map(|k| (k.to_vec(), k.to_vec())).collect();
                    assert_eq!(got, expected_pairs, "start {start:?}, end {end:?}");
                }

                // Inverted bounds select nothing, in either direction; equal
                // excluded bounds are the empty range.
                for reverse in [false, true].into_iter() {
                    assert!(
                        scan(
                            &store,
                            D1,
                            Bound::Included(b"c"),
                            Bound::Included(b"b"),
                            reverse
                        )
                        .await
                        .is_empty()
                    );
                    assert!(
                        scan(
                            &store,
                            D1,
                            Bound::Excluded(b"c"),
                            Bound::Excluded(b"c"),
                            reverse
                        )
                        .await
                        .is_empty()
                    );
                }
        }

        async fn kv_reverse_scan_returns_the_same_set_descending_body() {
                let mut store = $store;
                for dir in &[D1, D2] {
                    seed(&mut store, dir, ORDER_KEYS).await;
                }
                for dir in &[D1, D2] {
                    let fwd = scan(&store, dir, Bound::Unbounded, Bound::Unbounded, false).await;
                    let rev = scan(&store, dir, Bound::Unbounded, Bound::Unbounded, true).await;
                    assert_eq!(
                        rev.into_iter().rev().collect::<Vec<_>>(),
                        fwd,
                        "whole directory, {dir:?}"
                    );
                }
                // Bounded reverse: the same subset, descending.
                let fwd = scan(&store, D1, Bound::Included(b""), Bound::Excluded(K2), false).await;
                let expected_pairs = owned_pairs(&[(b"", b""), (K1, K1), (K10, K10)]);
                assert_eq!(fwd, expected_pairs);
                let rev = scan(&store, D1, Bound::Included(b""), Bound::Excluded(K2), true).await;
                assert_eq!(rev, fwd.into_iter().rev().collect::<Vec<_>>());
        }

        async fn kv_scan_sees_overwrites_and_deletes_body() {
                let mut store = $store;
                seed(&mut store, D1, &[b"a", b"b", b"c", b"d"]).await;
                $crate::KvStore::set(&mut store, D1, b"b", b"overridden")
                    .await
                    .expect("override");
                $crate::KvStore::set(&mut store, D1, b"e", b"new")
                    .await
                    .expect("insert");
                $crate::KvStore::delete(&mut store, D1, b"c")
                    .await
                    .expect("delete");

                let fwd = scan(&store, D1, Bound::Unbounded, Bound::Unbounded, false).await;
                assert_eq!(
                    fwd,
                    owned_pairs(&[
                        (b"a", b"a"),
                        (b"b", b"overridden"),
                        (b"d", b"d"),
                        (b"e", b"new"),
                    ])
                );
                // The same merged view, descending.
                let rev = scan(&store, D1, Bound::Unbounded, Bound::Unbounded, true).await;
                assert_eq!(rev, fwd.into_iter().rev().collect::<Vec<_>>());
        }

        async fn kv_clear_range_bounded_and_reinsert_body() {
                let mut store = $store;
                seed(&mut store, D1, &[b"a", b"b", b"c", b"d", b"e"]).await;

                store
                    .clear_range(D1, Bound::Included(b"b"), Bound::Excluded(b"d"))
                    .await
                    .expect("clear b..d");
                assert_eq!(
                    keys(&store, D1, Bound::Unbounded, Bound::Unbounded).await,
                    owned(&[b"a", b"d", b"e"])
                );
                // Point reads agree with the scan.
                assert_eq!(
                    $crate::KvStore::get(&store, D1, b"b").await.expect("get b"),
                    None
                );
                assert_eq!(
                    $crate::KvStore::get(&store, D1, b"c").await.expect("get c"),
                    None
                );
                assert_eq!(
                    $crate::KvStore::get(&store, D1, b"d").await.expect("get d"),
                    Some(b"d".to_vec())
                );

                // Re-insert into the just-cleared range.
                $crate::KvStore::set(&mut store, D1, b"c", b"again")
                    .await
                    .expect("reinsert");
                assert_eq!(
                    keys(&store, D1, Bound::Unbounded, Bound::Unbounded).await,
                    owned(&[b"a", b"c", b"d", b"e"])
                );

                // A clear that matches nothing is a no-op.
                store
                    .clear_range(D1, Bound::Included(b"z"), Bound::Excluded(b"zz"))
                    .await
                    .expect("clear nothing");
                assert_eq!(
                    keys(&store, D1, Bound::Unbounded, Bound::Unbounded).await,
                    owned(&[b"a", b"c", b"d", b"e"])
                );
        }

        // The Excluded start and Included end arms pin the bound
        // mapping on every backend (the FDB backend maps those arms by
        // hand, so a wrong `just_after` — e.g. clearing the excluded
        // start key itself — would otherwise ship unseen).
        async fn kv_clear_range_excluded_start_included_end_body() {
                let mut store = $store;
                seed(&mut store, D1, &[b"a", b"b", b"c", b"d"]).await;

                store
                    .clear_range(D1, Bound::Excluded(b"a"), Bound::Included(b"c"))
                    .await
                    .expect("clear (a, c]");
                // Exactly `a` and `d` survive: the excluded start and
                // the key past the included end are untouched, the keys
                // inside the range (`b`, `c`) are gone.
                assert_eq!(
                    keys(&store, D1, Bound::Unbounded, Bound::Unbounded).await,
                    owned(&[b"a", b"d"])
                );
                assert_eq!(
                    $crate::KvStore::get(&store, D1, b"a").await.expect("get a"),
                    Some(b"a".to_vec())
                );
                assert_eq!(
                    $crate::KvStore::get(&store, D1, b"c").await.expect("get c"),
                    None
                );
        }

        async fn kv_clear_range_whole_directory_keeps_the_directory_body() {
                let mut store = $store;
                seed(&mut store, D2, &[b"x", b"y"]).await;
                assert!(
                    $crate::KvDirNav::dir_exists(&store, D2)
                        .await
                        .expect("a/b exists")
                );

                store
                    .clear_range(D2, Bound::Unbounded, Bound::Unbounded)
                    .await
                    .expect("clear the whole directory");
                assert!(
                    scan(&store, D2, Bound::Unbounded, Bound::Unbounded, false)
                        .await
                        .is_empty()
                );
                // A data-only clear does not remove the directory.
                assert!(
                    $crate::KvDirNav::dir_exists(&store, D2)
                        .await
                        .expect("a/b still exists")
                );

                // A neighbouring directory is untouched.
                seed(&mut store, D1, &[b"n"]).await;
                assert_eq!(
                    keys(&store, D1, Bound::Unbounded, Bound::Unbounded).await,
                    owned(&[b"n"])
                );
        }

        async fn kv_clear_range_missing_directory_is_a_noop_body() {
                let mut store = $store;
                store
                    .clear_range(&[b"never"], Bound::Unbounded, Bound::Unbounded)
                    .await
                    .expect("clearing a missing directory is a no-op");
                assert!(
                    !$crate::KvDirNav::dir_exists(&store, &[b"never"])
                        .await
                        .expect("still absent")
                );
        }

        async fn kv_paged_traversal_over_a_thousand_rows_body() {
                let mut store = $store;

                // 1024 keys, zero-padded so lexicographic order is numeric.
                let n: usize = 1024;
                let make = |i: usize| format!("k{:04}", i).into_bytes();
                for i in 0..n {
                    let key = make(i);
                    store.set(D0, &key, &key).await.expect("seed must succeed");
                }

                // Page forward: five via next, one hundred via take, the rest
                // via collect.
                let mut stream =
                    $crate::KvStore::range(&store, D0, Bound::Unbounded, Bound::Unbounded, false);
                let mut got: Vec<Vec<u8>> = Vec::new();
                for _ in 0..5 {
                    let (pair, rest) = stream
                        .next()
                        .await
                        .expect("next must succeed")
                        .expect("rows remain");
                    got.push(pair.key);
                    stream = rest;
                }
                assert_eq!(got, (0..5).map(make).collect::<Vec<_>>());
                let mid = stream.take(100).await.expect("take must succeed");
                assert_eq!(
                    mid.into_iter().map(|pair| pair.key).collect::<Vec<_>>(),
                    (5..105).map(make).collect::<Vec<_>>()
                );
                // `take` consumes its stream: a fresh, bounded scan for the
                // tail.
                let from = make(105);
                let tail = keys(
                    &store,
                    D0,
                    Bound::Included(from.as_slice()),
                    Bound::Unbounded,
                )
                .await;
                assert_eq!(tail, (105..n).map(make).collect::<Vec<_>>());

                // The whole set descending, in one scan.
                let rev = scan(&store, D0, Bound::Unbounded, Bound::Unbounded, true).await;
                assert_eq!(
                    rev.into_iter().map(|(k, _)| k).collect::<Vec<_>>(),
                    (0..n)
                        .map(make)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect::<Vec<_>>()
                );

                // A bounded window selects exactly that window.
                let lo = make(100);
                let hi = make(200);
                let window = keys(&store, D0, Bound::Included(&lo), Bound::Excluded(&hi)).await;
                assert_eq!(window, (100..200).map(make).collect::<Vec<_>>());
        }

        async fn kv_take_limits_what_the_caller_polls_body() {
                let mut store = $store;
                seed(&mut store, D1, &[b"a", b"b", b"c", b"d", b"e"]).await;

                let got = store
                    .range(D1, Bound::Unbounded, Bound::Unbounded, false)
                    .take(3)
                    .await
                    .expect("take must succeed");
                assert_eq!(
                    got.into_iter().map(|pair| pair.key).collect::<Vec<_>>(),
                    owned(&[b"a", b"b", b"c"])
                );

                // A take past the end of the range returns what is there.
                let got = store
                    .range(D1, Bound::Unbounded, Bound::Unbounded, false)
                    .take(100)
                    .await
                    .expect("take past the end must succeed");
                assert_eq!(got.len(), 5);

                // for_each drains a bounded range into the caller's sink.
                let mut sink: Vec<Vec<u8>> = Vec::new();
                store
                    .range(D1, Bound::Included(b"b"), Bound::Excluded(b"e"), false)
                    .for_each(|pair| sink.push(pair.key))
                    .await
                    .expect("for_each must succeed");
                assert_eq!(sink, owned(&[b"b", b"c", b"d"]));
        }

        async fn kv_unknown_directory_reads_are_empty_body() {
                let mut store = $store;
                seed(&mut store, D1, &[b"present"]).await;

                let missing: &[&[u8]] = &[b"zz", b"deep"];
                assert_eq!(
                    $crate::KvStore::get(&store, missing, b"present")
                        .await
                        .expect("get"),
                    None
                );
                assert!(
                    scan(&store, missing, Bound::Unbounded, Bound::Unbounded, false)
                        .await
                        .is_empty()
                );
        }

        $crate::kv_store_tests!(@case $mode, kv_set_get_delete_round_trips, kv_set_get_delete_round_trips_body);
        $crate::kv_store_tests!(@case $mode, kv_scan_of_one_directory_never_yields_another, kv_scan_of_one_directory_never_yields_another_body);
        $crate::kv_store_tests!(@case $mode, kv_orders_keys_lexicographically_within_each_directory, kv_orders_keys_lexicographically_within_each_directory_body);
        $crate::kv_store_tests!(@case $mode, kv_bounds_matrix_selects_exactly_the_bounded_keys, kv_bounds_matrix_selects_exactly_the_bounded_keys_body);
        $crate::kv_store_tests!(@case $mode, kv_reverse_scan_returns_the_same_set_descending, kv_reverse_scan_returns_the_same_set_descending_body);
        $crate::kv_store_tests!(@case $mode, kv_scan_sees_overwrites_and_deletes, kv_scan_sees_overwrites_and_deletes_body);
        $crate::kv_store_tests!(@case $mode, kv_clear_range_bounded_and_reinsert, kv_clear_range_bounded_and_reinsert_body);
        $crate::kv_store_tests!(@case $mode, kv_clear_range_excluded_start_included_end, kv_clear_range_excluded_start_included_end_body);
        $crate::kv_store_tests!(@case $mode, kv_clear_range_whole_directory_keeps_the_directory, kv_clear_range_whole_directory_keeps_the_directory_body);
        $crate::kv_store_tests!(@case $mode, kv_clear_range_missing_directory_is_a_noop, kv_clear_range_missing_directory_is_a_noop_body);
        $crate::kv_store_tests!(@case $mode, kv_paged_traversal_over_a_thousand_rows, kv_paged_traversal_over_a_thousand_rows_body);
        $crate::kv_store_tests!(@case $mode, kv_take_limits_what_the_caller_polls, kv_take_limits_what_the_caller_polls_body);
        $crate::kv_store_tests!(@case $mode, kv_unknown_directory_reads_are_empty, kv_unknown_directory_reads_are_empty_body);
    };
    (@case (block_on $block_on:expr), $name:ident, $body:ident) => {
        #[test]
        fn $name() {
            ($block_on)($body())
        }
    };
    (@case (test_attr $attr:path), $name:ident, $body:ident) => {
        #[$attr]
        async fn $name() {
            $body().await
        }
    };
}
#[macro_export]
#[doc(hidden)]
macro_rules! __kv_nav_helpers {
    () => {
        const ND0: &[&[u8]] = &[];
        const ND1: &[&[u8]] = &[b"a"];
        const ND2: &[&[u8]] = &[b"a", b"b"];
        const ND3: &[&[u8]] = &[b"c"];

        fn owned_dirs(dirs: &[&[u8]]) -> Vec<Vec<u8>> {
            dirs.iter().map(|d| d.to_vec()).collect()
        }

        async fn nav_seed<S: $crate::KvStore>(store: &mut S, dir: &[&[u8]], keys: &[&[u8]]) {
            for key in keys {
                $crate::KvStore::set(store, dir, key, key)
                    .await
                    .expect("seed write must succeed");
            }
        }

        /// The immediate child segment names under `dir`, sorted.
        async fn nav_list<S: $crate::KvDirNav>(store: &S, dir: &[&[u8]]) -> Vec<Vec<u8>> {
            $crate::KvDirNav::list_dirs(store, dir)
                .await
                .expect("list_dirs must succeed")
        }

        /// The sorted union of two sorted directory-segment lists.
        fn nav_union(a: &[Vec<u8>], b: &[Vec<u8>]) -> Vec<Vec<u8>> {
            let mut out = Vec::with_capacity(a.len() + b.len());
            let (mut i, mut j) = (0usize, 0usize);
            while i < a.len() && j < b.len() {
                match a[i].cmp(&b[j]) {
                    core::cmp::Ordering::Less => {
                        out.push(a[i].clone());
                        i += 1;
                    }
                    core::cmp::Ordering::Greater => {
                        out.push(b[j].clone());
                        j += 1;
                    }
                    core::cmp::Ordering::Equal => {
                        out.push(a[i].clone());
                        i += 1;
                        j += 1;
                    }
                }
            }
            out.extend_from_slice(&a[i..]);
            out.extend_from_slice(&b[j..]);
            out
        }
    };
}

/// The directory-navigation conformance tests.
///
/// Every backend whose caller-space root **is the engine root** must pass
/// these: the raw stores of each backend, and a [`ScopedKvStore`]
/// ([`fasm_storage::ScopedKvStore`]) pinned at that root. A scope pinned
/// elsewhere is covered by the scoped data-plane tests instead: its "root"
/// is the pin, which is a removable directory, so the root-removal
/// assertion below does not apply to it.
///
/// # Root listing baseline
///
/// The root-listing assertions are exact against a **baseline** captured at
/// the start of each test, not absolute listings: a shared live engine
/// (a live FDB cluster) may already hold top-level directories committed by
/// other tests in the same binary. A private per-test engine (empty
/// baseline) still asserts exact listings. A test binary that shares its
/// engine root must run single-threaded so that no commit lands between a
/// test's baseline capture and its assertions.
///
/// # The error contract
///
/// Two generated tests check that an invalid directory (a non-UTF-8 segment)
/// and a root `remove_dir` fail with the core [`KeyError`](crate::KeyError)
/// answers. Backend error types wrap [`KeyError`], so the macro asserts the
/// uniform surface every backend must provide: the error is **not retryable**,
/// and its `Display` text carries the [`KeyError`]'s own message verbatim
/// (wrap it `#[error(transparent)]`-style or include the source).
///
/// # Invocation
///
/// The synchronous form accepts the same per-test blocking runner as
/// [`kv_store_tests!`]:
///
/// ```ignore
/// fasm_storage::kv_nav_tests!(
///     store = MyStore::new(),
///     block_on = futures::executor::block_on,
/// );
/// ```
///
/// Or apply an async test attribute directly:
///
/// ```ignore
/// fasm_storage::kv_nav_tests!(
///     store = MyStore::new(),
///     test_attr = tokio::test,
/// );
/// ```
///
/// A browser backend uses the browser attribute:
///
/// ```ignore
/// fasm_storage::kv_nav_tests!(
///     store = MyIndexedDbStore::open().await,
///     test_attr = wasm_bindgen_test::wasm_bindgen_test,
/// );
/// ```
///
/// The backend crate must call
/// `wasm_bindgen_test_configure!(run_in_browser)` itself because IndexedDB is
/// unavailable in wasm-bindgen-test's default Node runner.
///
/// # The `test_attr` contract
///
/// The path is applied verbatim to each generated `async fn` and must turn it
/// into a test. It therefore controls the runtime and future bounds:
/// `tokio::test` qualifies natively, while
/// `wasm_bindgen_test::wasm_bindgen_test` qualifies in the browser and accepts
/// the thread-local futures used by browser storage.
#[macro_export]
macro_rules! kv_nav_tests {
    (store = $store:expr, block_on = $block_on:expr $(,)?) => {
        $crate::kv_nav_tests!(@emit (block_on $block_on), $store);
    };
    (store = $store:expr, test_attr = $attr:path $(,)?) => {
        $crate::kv_nav_tests!(@emit (test_attr $attr), $store);
    };
    (@emit $mode:tt, $store:expr) => {
        #[allow(unused_imports)]
        use $crate::KvDirNav as _;
        #[allow(unused_imports)]
        use $crate::KvStore as _;
        #[allow(unused_imports)]
        use $crate::RetryableStorageError as _;
        $crate::__kv_nav_helpers!();
        async fn kv_nav_lists_immediate_children_sorted_body() {
                let mut store = $store;
                // The baseline root listing at suite start: children that
                // exist before this suite runs (a committed top-level
                // directory on a shared live cluster, for instance). The
                // root assertion below is exact against the union with
                // this baseline, so a private engine root (empty
                // baseline) still asserts exact listings.
                let base = nav_list(&store, ND0).await;
                nav_seed(&mut store, ND1, &[b"x"]).await;
                nav_seed(&mut store, ND2, &[b"y"]).await;
                nav_seed(&mut store, ND3, &[b"z"]).await;

                // The root's children are the baseline plus the top-level
                // segments, sorted.
                assert_eq!(
                    nav_list(&store, ND0).await,
                    nav_union(&base, &owned_dirs(&[b"a", b"c"]))
                );
                // `a`'s child is `b`; deeper nesting is not listed here.
                assert_eq!(nav_list(&store, ND1).await, owned_dirs(&[b"b"]));
                // Directories with children but no keys of their own still
                // exist and list their children: existence and listing are
                // mapping facts, independent of data keys.
                $crate::KvStore::set(&mut store, &[b"n", b"o", b"p"], b"k", b"v")
                    .await
                    .expect("set deep");
                assert!(
                    $crate::KvDirNav::dir_exists(&store, &[b"n"])
                        .await
                        .expect("n exists")
                );
                assert!(
                    $crate::KvDirNav::dir_exists(&store, &[b"n", b"o"])
                        .await
                        .expect("n/o exists")
                );
                assert_eq!(nav_list(&store, &[b"n"]).await, owned_dirs(&[b"o"]));
                assert_eq!(nav_list(&store, &[b"n", b"o"]).await, owned_dirs(&[b"p"]));
                // Listing a missing directory is empty, not an error.
                assert!(nav_list(&store, &[b"nope"]).await.is_empty());
                // Existence under a missing intermediate segment is
                // false: the layer answers `Ok(false)`, not a
                // missing-path error.
                assert!(
                    !$crate::KvDirNav::dir_exists(&store, &[b"nope", b"x"])
                        .await
                        .expect("deep missing dir_exists")
                );
        }

        async fn kv_nav_remove_dir_is_recursive_body() {
                let mut store = $store;
                let base = nav_list(&store, ND0).await;
                nav_seed(&mut store, ND1, &[b"top"]).await;
                nav_seed(&mut store, ND2, &[b"mid"]).await;
                nav_seed(&mut store, &[b"a", b"b", b"x"], &[b"deep"]).await;
                nav_seed(&mut store, ND3, &[b"keep"]).await;

                // Removing `a` removes `a`, `a/b` and `a/b/x` — data and
                // mapping rows alike.
                assert!(
                    $crate::KvDirNav::remove_dir(&mut store, ND1)
                        .await
                        .expect("remove a")
                );
                assert_eq!(
                    $crate::KvStore::get(&store, ND1, b"top")
                        .await
                        .expect("a gone"),
                    None
                );
                assert_eq!(
                    $crate::KvStore::get(&store, ND2, b"mid")
                        .await
                        .expect("a/b gone"),
                    None
                );
                assert_eq!(
                    $crate::KvStore::get(&store, &[b"a", b"b", b"x"], b"deep")
                        .await
                        .expect("a/b/x gone"),
                    None
                );
                assert!(
                    !$crate::KvDirNav::dir_exists(&store, ND1)
                        .await
                        .expect("a removed")
                );
                assert!(
                    !$crate::KvDirNav::dir_exists(&store, ND2)
                        .await
                        .expect("a/b removed")
                );
                assert!(nav_list(&store, ND0).await == nav_union(&base, &owned_dirs(&[b"c"])));

                // A sibling directory survives.
                assert_eq!(
                    $crate::KvStore::get(&store, ND3, b"keep")
                        .await
                        .expect("c kept"),
                    Some(b"keep".to_vec())
                );

                // Removing a missing directory is `Ok(false)`.
                assert!(
                    !$crate::KvDirNav::remove_dir(&mut store, ND1)
                        .await
                        .expect("remove missing")
                );

                // The root is not removable: the core KeyError answer,
                // wrapped by the backend's error type.
                let err = $crate::KvDirNav::remove_dir(&mut store, ND0)
                    .await
                    .expect_err("the root must not be removable");
                assert!(
                    !$crate::RetryableStorageError::is_retryable(&err),
                    "a key error is not retryable"
                );
                assert!(
                    err.to_string()
                        .contains("the root directory cannot be removed"),
                    "expected the RootNotRemovable message, got: {err}"
                );
        }

        async fn kv_non_utf8_dir_segment_is_rejected_body() {
                let mut store = $store;
                let bad_segment: &[u8] = &[0xFFu8, 0xFEu8];
                let bad: &[&[u8]] = &[bad_segment];

                let err = $crate::KvStore::get(&store, bad, b"k")
                    .await
                    .expect_err("must reject");
                assert!(
                    !$crate::RetryableStorageError::is_retryable(&err),
                    "a key error is not retryable"
                );
                assert!(
                    err.to_string().contains("is not valid UTF-8"),
                    "expected the DirSegmentNotUtf8 message, got: {err}"
                );
                assert!($crate::KvDirNav::dir_exists(&store, bad).await.is_err());
                assert!(
                    $crate::KvDirNav::remove_dir(&mut store, bad)
                        .await
                        .is_err_and(|e| e.to_string().contains("is not valid UTF-8"))
                );
        }

        $crate::kv_nav_tests!(@case $mode, kv_nav_lists_immediate_children_sorted, kv_nav_lists_immediate_children_sorted_body);
        $crate::kv_nav_tests!(@case $mode, kv_nav_remove_dir_is_recursive, kv_nav_remove_dir_is_recursive_body);
        $crate::kv_nav_tests!(@case $mode, kv_non_utf8_dir_segment_is_rejected, kv_non_utf8_dir_segment_is_rejected_body);
    };
    (@case (block_on $block_on:expr), $name:ident, $body:ident) => {
        #[test]
        fn $name() {
            ($block_on)($body())
        }
    };
    (@case (test_attr $attr:path), $name:ident, $body:ident) => {
        #[$attr]
        async fn $name() {
            $body().await
        }
    };
}
