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
/// A browser backend is deferred: it needs a `?Send` formulation and an async
/// test mode. This macro is the shared answer key.
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
/// The expansion defines free items (helper functions, a trait import) at the
/// invocation site, so **invoke it inside a dedicated module**. Invoke it once
/// per backend configuration you want covered.
///
/// # The `store` contract
///
/// `store` may be evaluated more than once per generated test. Every evaluation
/// must produce a **fresh, empty, independent** store. Tests write freely and
/// never clean up, so a builder that hands back shared state will
/// cross-contaminate. The expression is evaluated **inside** the future handed
/// to `block_on`, so it may `.await` — `store = MyStore::open().await` is legal.
/// It must not block on a runtime of its own, for the same reason. If your
/// backend needs setup that can fail, panic in the builder.
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
/// # What is covered
///
/// Round-trips (`get`/`set`/`delete`/`exists`, overwrite, absent-key deletes);
/// lexicographic ordering including prefix-shadowing keys (`[1]` vs `[1, 0]`);
/// every combination of `Included` / `Excluded` / `Unbounded` on both ends;
/// reverse scans; empty and inverted ranges; `clear_range` including the fully
/// unbounded form, point-read visibility after a clear, and re-inserting into a
/// range that was just cleared; overwrites and deletes observed *through a range
/// scan* rather than only through `get`; paged traversal over more than 1,000
/// rows in both directions, bounded and unbounded; the empty key; `0xFF`-heavy
/// keys that stress prefix successor arithmetic; and
/// [`KvStream`](crate::KvStream)'s `next`/`take`/`collect`/`for_each`.
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
/// prefetch counts.
#[macro_export]
macro_rules! kv_store_tests {
    (store = $store:expr, block_on = $block_on:expr $(,)?) => {
        use $crate::KvStore as _;

        /// Bound over borrowed key bytes, as `KvStore::range` takes them.
        type ConformanceBound<'k> = $crate::__private::Bound<&'k [u8]>;

        /// Write `key -> key` for each key, so values identify their key.
        async fn seed<S: $crate::KvStore>(store: &mut S, keys: &[&[u8]]) {
            for key in keys {
                store.set(key, key).await.expect("seed write must succeed");
            }
        }

        /// Collect a range as `(key, value)` pairs.
        async fn scan<S: $crate::KvStore>(
            store: &S,
            start: ConformanceBound<'_>,
            end: ConformanceBound<'_>,
            reverse: bool,
        ) -> Vec<(Vec<u8>, Vec<u8>)> {
            store
                .range(start, end, reverse)
                .collect()
                .await
                .expect("range scan must succeed")
                .into_iter()
                .map(|pair| (pair.key, pair.value))
                .collect()
        }

        /// Collect just the keys of a forward range.
        async fn keys<S: $crate::KvStore>(
            store: &S,
            start: ConformanceBound<'_>,
            end: ConformanceBound<'_>,
        ) -> Vec<Vec<u8>> {
            scan(store, start, end, false)
                .await
                .into_iter()
                .map(|(key, _)| key)
                .collect()
        }

        /// Collect every key in the store, in order.
        async fn all_keys<S: $crate::KvStore>(store: &S) -> Vec<Vec<u8>> {
            keys(
                store,
                $crate::__private::Bound::Unbounded,
                $crate::__private::Bound::Unbounded,
            )
            .await
        }

        fn owned(keys: &[&[u8]]) -> Vec<Vec<u8>> {
            keys.iter().map(|key| key.to_vec()).collect()
        }

        /// Owned `(key, value)` pairs, optionally reversed to match a descending
        /// scan. Written out explicitly where values no longer equal their keys.
        fn owned_pairs(pairs: &[(&[u8], &[u8])], reverse: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
            let mut owned: Vec<(Vec<u8>, Vec<u8>)> = pairs
                .iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect();
            if reverse {
                owned.reverse();
            }
            owned
        }

        #[test]
        fn kv_get_set_delete_exists_roundtrip() {
            let run = $block_on;
            run(async {
                let mut store = $store;

                assert_eq!(store.get(b"k1").await.expect("get absent"), None);
                assert!(!store.exists(b"k1").await.expect("exists absent"));

                store.set(b"k1", b"v1").await.expect("set");
                assert_eq!(
                    store.get(b"k1").await.expect("get present"),
                    Some(b"v1".to_vec())
                );
                assert!(store.exists(b"k1").await.expect("exists present"));

                store.delete(b"k1").await.expect("delete");
                assert_eq!(store.get(b"k1").await.expect("get deleted"), None);
                assert!(!store.exists(b"k1").await.expect("exists deleted"));
            });
        }

        #[test]
        fn kv_set_overwrites_and_delete_is_idempotent() {
            let run = $block_on;
            run(async {
                let mut store = $store;

                store.set(b"k", b"first").await.expect("set first");
                store.set(b"k", b"second").await.expect("set second");
                assert_eq!(
                    store.get(b"k").await.expect("get"),
                    Some(b"second".to_vec())
                );

                // Deleting an absent key is a no-op, not an error.
                store.delete(b"missing").await.expect("delete absent");
                store.delete(b"k").await.expect("delete present");
                store.delete(b"k").await.expect("delete again");
                assert_eq!(store.get(b"k").await.expect("get"), None);

                // Empty values are values, not absences.
                store.set(b"k", b"").await.expect("set empty value");
                assert_eq!(store.get(b"k").await.expect("get"), Some(Vec::new()));
                assert!(store.exists(b"k").await.expect("exists"));
            });
        }

        #[test]
        fn kv_orders_keys_lexicographically_with_prefix_shadowing() {
            let run = $block_on;
            run(async {
                let mut store = $store;

                // Inserted out of order; a shorter key must sort before every
                // longer key that extends it.
                seed(
                    &mut store,
                    &[
                        &[2][..],
                        &[1, 0, 0][..],
                        &[1][..],
                        &[1, 0][..],
                        &[0, 255][..],
                    ],
                )
                .await;

                assert_eq!(
                    all_keys(&store).await,
                    owned(&[
                        &[0, 255][..],
                        &[1][..],
                        &[1, 0][..],
                        &[1, 0, 0][..],
                        &[2][..]
                    ]),
                );

                // `[1]` and everything under it, without reaching `[2]`.
                assert_eq!(
                    keys(
                        &store,
                        $crate::__private::Bound::Included(&[1][..]),
                        $crate::__private::Bound::Excluded(&[2][..]),
                    )
                    .await,
                    owned(&[&[1][..], &[1, 0][..], &[1, 0, 0][..]]),
                );

                // Excluding `[1]` keeps its descendants: they are distinct keys.
                assert_eq!(
                    keys(
                        &store,
                        $crate::__private::Bound::Excluded(&[1][..]),
                        $crate::__private::Bound::Excluded(&[2][..]),
                    )
                    .await,
                    owned(&[&[1, 0][..], &[1, 0, 0][..]]),
                );
            });
        }

        #[test]
        fn kv_range_honours_every_bound_combination() {
            let run = $block_on;
            run(async {
                let mut store = $store;
                seed(&mut store, &[b"a", b"b", b"c", b"d", b"e"]).await;

                let cases: &[(ConformanceBound<'_>, ConformanceBound<'_>, &[&[u8]])] = &[
                    (
                        $crate::__private::Bound::Included(b"b"),
                        $crate::__private::Bound::Included(b"d"),
                        &[b"b", b"c", b"d"],
                    ),
                    (
                        $crate::__private::Bound::Included(b"b"),
                        $crate::__private::Bound::Excluded(b"d"),
                        &[b"b", b"c"],
                    ),
                    (
                        $crate::__private::Bound::Excluded(b"b"),
                        $crate::__private::Bound::Included(b"d"),
                        &[b"c", b"d"],
                    ),
                    (
                        $crate::__private::Bound::Excluded(b"b"),
                        $crate::__private::Bound::Excluded(b"d"),
                        &[b"c"],
                    ),
                    (
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Excluded(b"c"),
                        &[b"a", b"b"],
                    ),
                    (
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Included(b"c"),
                        &[b"a", b"b", b"c"],
                    ),
                    (
                        $crate::__private::Bound::Included(b"d"),
                        $crate::__private::Bound::Unbounded,
                        &[b"d", b"e"],
                    ),
                    (
                        $crate::__private::Bound::Excluded(b"d"),
                        $crate::__private::Bound::Unbounded,
                        &[b"e"],
                    ),
                    (
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Unbounded,
                        &[b"a", b"b", b"c", b"d", b"e"],
                    ),
                ];

                for (start, end, expected) in cases {
                    for reverse in [false, true] {
                        let got = scan(&store, *start, *end, reverse).await;
                        let mut expected = owned(expected);
                        if reverse {
                            expected.reverse();
                        }
                        assert_eq!(
                            got.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>(),
                            expected,
                            "bounds {start:?}..{end:?}, reverse={reverse}",
                        );
                        // Values must travel with their keys in either direction.
                        for (key, value) in got {
                            assert_eq!(key, value, "value must match its key");
                        }
                    }
                }
            });
        }

        #[test]
        fn kv_range_reflects_overwrites_and_deletes() {
            let run = $block_on;
            run(async {
                let mut store = $store;
                seed(&mut store, &[b"a", b"b", b"c", b"d", b"e"]).await;

                // Overwrite a key in the *middle* of the range. A backend that
                // merges a write buffer over a committed snapshot can emit `c`
                // twice — snapshot value then buffered value — or once with the
                // stale value. `get` sees neither failure.
                store
                    .set(b"c", b"c2")
                    .await
                    .expect("overwrite mid-range key");

                let expected: &[(&[u8], &[u8])] = &[
                    (b"a", b"a"),
                    (b"b", b"b"),
                    (b"c", b"c2"),
                    (b"d", b"d"),
                    (b"e", b"e"),
                ];
                for reverse in [false, true] {
                    assert_eq!(
                        scan(
                            &store,
                            $crate::__private::Bound::Unbounded,
                            $crate::__private::Bound::Unbounded,
                            reverse,
                        )
                        .await,
                        owned_pairs(expected, reverse),
                        "overwritten key must appear exactly once, with the new \
                         value, reverse={reverse}",
                    );
                }

                // Also through a scan bounded on both sides of the overwrite,
                // which is where a merge that mishandles the range start shows.
                let windowed: &[(&[u8], &[u8])] = &[(b"b", b"b"), (b"c", b"c2"), (b"d", b"d")];
                for reverse in [false, true] {
                    assert_eq!(
                        scan(
                            &store,
                            $crate::__private::Bound::Included(b"b"),
                            $crate::__private::Bound::Included(b"d"),
                            reverse,
                        )
                        .await,
                        owned_pairs(windowed, reverse),
                        "bounded scan over an overwrite, reverse={reverse}",
                    );
                }

                // A delete of a *non-first* key. A buffered tombstone has to be
                // filtered out mid-stream, not just at the head of the range.
                store.delete(b"d").await.expect("delete mid-range key");
                for reverse in [false, true] {
                    let mut expected = owned(&[b"a", b"b", b"c", b"e"]);
                    if reverse {
                        expected.reverse();
                    }
                    let got: Vec<Vec<u8>> = scan(
                        &store,
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Unbounded,
                        reverse,
                    )
                    .await
                    .into_iter()
                    .map(|(key, _)| key)
                    .collect();
                    assert_eq!(
                        got, expected,
                        "deleted key must not appear in a scan, reverse={reverse}",
                    );
                }

                // Including when it is the last key in the range.
                store.delete(b"e").await.expect("delete last key");
                assert_eq!(all_keys(&store).await, owned(&[b"a", b"b", b"c"]));

                // And re-inserting a deleted key must bring it back to the scan,
                // not leave the tombstone shadowing it.
                store.set(b"d", b"d3").await.expect("re-insert deleted key");
                assert_eq!(
                    scan(
                        &store,
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Unbounded,
                        false,
                    )
                    .await,
                    owned_pairs(
                        &[(b"a", b"a"), (b"b", b"b"), (b"c", b"c2"), (b"d", b"d3")],
                        false,
                    ),
                );
            });
        }

        #[test]
        fn kv_reverse_scans_yield_descending_order() {
            let run = $block_on;
            run(async {
                let mut store = $store;
                let all: &[&[u8]] = &[
                    &[0, 255][..],
                    &[1][..],
                    &[1, 0][..],
                    &[1, 0, 0][..],
                    &[2][..],
                ];
                seed(&mut store, all).await;

                let full = scan(
                    &store,
                    $crate::__private::Bound::Unbounded,
                    $crate::__private::Bound::Unbounded,
                    true,
                )
                .await;
                assert_eq!(
                    full.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
                    owned(&[
                        &[2][..],
                        &[1, 0, 0][..],
                        &[1, 0][..],
                        &[1][..],
                        &[0, 255][..],
                    ]),
                );

                // `reverse` flips the direction only; the bounds still mean the
                // same half-open interval. Excluding `[2]` retains the keys that
                // extend `[1]`; they are distinct keys.
                let bounded = scan(
                    &store,
                    $crate::__private::Bound::Included(&[1][..]),
                    $crate::__private::Bound::Excluded(&[2][..]),
                    true,
                )
                .await;
                assert_eq!(
                    bounded.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
                    owned(&[&[1, 0, 0][..], &[1, 0][..], &[1][..]]),
                );
            });
        }

        #[test]
        fn kv_empty_ranges_yield_nothing() {
            let run = $block_on;
            run(async {
                let mut store = $store;

                // Empty store, widest possible range.
                assert!(all_keys(&store).await.is_empty());

                seed(&mut store, &[b"a", b"z"]).await;

                // A gap with no keys in it.
                assert!(
                    keys(
                        &store,
                        $crate::__private::Bound::Included(b"m"),
                        $crate::__private::Bound::Excluded(b"n"),
                    )
                    .await
                    .is_empty()
                );

                // A degenerate `[k, k)` interval.
                assert!(
                    keys(
                        &store,
                        $crate::__private::Bound::Included(b"a"),
                        $crate::__private::Bound::Excluded(b"a"),
                    )
                    .await
                    .is_empty()
                );

                // `(k, k)` is also a valid empty interval in both directions.
                for reverse in [false, true] {
                    assert!(
                        scan(
                            &store,
                            $crate::__private::Bound::Excluded(b"a"),
                            $crate::__private::Bound::Excluded(b"a"),
                            reverse,
                        )
                        .await
                        .is_empty(),
                        "reverse={reverse}",
                    );

                    for (start, end) in [
                        (
                            $crate::__private::Bound::Included(b"z".as_slice()),
                            $crate::__private::Bound::Excluded(b"a".as_slice()),
                        ),
                        (
                            $crate::__private::Bound::Excluded(b"z".as_slice()),
                            $crate::__private::Bound::Included(b"a".as_slice()),
                        ),
                    ] {
                        assert!(
                            scan(&store, start, end, reverse).await.is_empty(),
                            "inverted bounds {start:?}..{end:?}, reverse={reverse}",
                        );
                    }
                }

                // Entirely below the first key.
                assert!(
                    keys(
                        &store,
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Excluded(b"a"),
                    )
                    .await
                    .is_empty()
                );

                // Entirely above the last key.
                assert!(
                    keys(
                        &store,
                        $crate::__private::Bound::Excluded(b"z"),
                        $crate::__private::Bound::Unbounded,
                    )
                    .await
                    .is_empty()
                );
            });
        }

        #[test]
        fn kv_clear_range_removes_exactly_the_range() {
            let run = $block_on;
            run(async {
                let all: &[&[u8]] = &[
                    &[0][..],
                    &[1][..],
                    &[1, 0][..],
                    &[1, 0, 0][..],
                    &[2][..],
                    &[3][..],
                ];
                let cases: &[(ConformanceBound<'_>, ConformanceBound<'_>, &[&[u8]])] = &[
                    (
                        $crate::__private::Bound::Included(&[1][..]),
                        $crate::__private::Bound::Included(&[2][..]),
                        &[&[0][..], &[3][..]],
                    ),
                    (
                        $crate::__private::Bound::Included(&[1][..]),
                        $crate::__private::Bound::Excluded(&[2][..]),
                        &[&[0][..], &[2][..], &[3][..]],
                    ),
                    (
                        $crate::__private::Bound::Excluded(&[1][..]),
                        $crate::__private::Bound::Included(&[2][..]),
                        &[&[0][..], &[1][..], &[3][..]],
                    ),
                    (
                        $crate::__private::Bound::Excluded(&[1][..]),
                        $crate::__private::Bound::Excluded(&[2][..]),
                        &[&[0][..], &[1][..], &[2][..], &[3][..]],
                    ),
                    (
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Included(&[2][..]),
                        &[&[3][..]],
                    ),
                    (
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Excluded(&[2][..]),
                        &[&[2][..], &[3][..]],
                    ),
                    (
                        $crate::__private::Bound::Included(&[1][..]),
                        $crate::__private::Bound::Unbounded,
                        &[&[0][..]],
                    ),
                    (
                        $crate::__private::Bound::Excluded(&[1][..]),
                        $crate::__private::Bound::Unbounded,
                        &[&[0][..], &[1][..]],
                    ),
                    (
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Unbounded,
                        &[],
                    ),
                    (
                        $crate::__private::Bound::Included(&[1][..]),
                        $crate::__private::Bound::Included(&[1][..]),
                        &[&[0][..], &[1, 0][..], &[1, 0, 0][..], &[2][..], &[3][..]],
                    ),
                    (
                        $crate::__private::Bound::Excluded(&[1][..]),
                        $crate::__private::Bound::Excluded(&[1][..]),
                        all,
                    ),
                    (
                        $crate::__private::Bound::Included(b"z"),
                        $crate::__private::Bound::Excluded(b"a"),
                        all,
                    ),
                    (
                        $crate::__private::Bound::Excluded(b"z"),
                        $crate::__private::Bound::Included(b"a"),
                        all,
                    ),
                ];

                for (start, end, expected) in cases {
                    let mut store = $store;
                    seed(&mut store, all).await;
                    store
                        .clear_range(*start, *end)
                        .await
                        .expect("clear range must succeed");
                    assert_eq!(
                        all_keys(&store).await,
                        owned(expected),
                        "bounds {start:?}..{end:?}",
                    );

                    for key in all {
                        let survives = expected.contains(key);
                        let expected_value = survives.then(|| key.to_vec());
                        assert_eq!(
                            store.get(key).await.expect("point get after clear"),
                            expected_value,
                            "get({key:02x?}) after bounds {start:?}..{end:?}",
                        );
                        assert_eq!(
                            store.exists(key).await.expect("point exists after clear"),
                            survives,
                            "exists({key:02x?}) after bounds {start:?}..{end:?}",
                        );
                    }
                }
            });
        }

        #[test]
        fn kv_mutations_apply_in_order_around_clear_range() {
            let run = $block_on;
            run(async {
                let mut store = $store;
                seed(&mut store, &[b"a", b"b", b"c", b"d"]).await;

                // Mutation order within a session is the contract: a
                // `clear_range` does not reach forward over the `set` that
                // follows it. A backend that accumulates range tombstones in a
                // separate list and applies them after its point writes at
                // commit silently drops `c`.
                store
                    .clear_range(
                        $crate::__private::Bound::Included(b"b"),
                        $crate::__private::Bound::Excluded(b"d"),
                    )
                    .await
                    .expect("clear range must succeed");
                store
                    .set(b"c", b"c2")
                    .await
                    .expect("re-insert into the cleared range");

                assert_eq!(
                    store.get(b"c").await.expect("get re-inserted key"),
                    Some(b"c2".to_vec()),
                );
                assert!(store.exists(b"c").await.expect("exists re-inserted key"));

                // Visible through a scan too, in both directions: a merge layer
                // can honour the write buffer for point reads and still let the
                // range tombstone mask it during a scan.
                let expected: &[(&[u8], &[u8])] = &[(b"a", b"a"), (b"c", b"c2"), (b"d", b"d")];
                for reverse in [false, true] {
                    assert_eq!(
                        scan(
                            &store,
                            $crate::__private::Bound::Unbounded,
                            $crate::__private::Bound::Unbounded,
                            reverse,
                        )
                        .await,
                        owned_pairs(expected, reverse),
                        "re-inserted key must survive the clear, reverse={reverse}",
                    );
                }

                // Including a scan bounded inside the cleared range itself.
                assert_eq!(
                    keys(
                        &store,
                        $crate::__private::Bound::Included(b"b"),
                        $crate::__private::Bound::Excluded(b"d"),
                    )
                    .await,
                    owned(&[b"c"]),
                );

                // The opposite order still removes it: `set` then a covering
                // `clear_range` leaves nothing behind.
                store
                    .clear_range(
                        $crate::__private::Bound::Included(b"c"),
                        $crate::__private::Bound::Included(b"c"),
                    )
                    .await
                    .expect("clear the re-inserted key");
                assert_eq!(store.get(b"c").await.expect("get cleared key"), None);
                assert_eq!(all_keys(&store).await, owned(&[b"a", b"d"]));
            });
        }

        #[test]
        fn kv_paged_traversal_is_complete_ordered_and_clearable() {
            let run = $block_on;
            run(async {
                const ROW_COUNT: u32 = 1_537;
                const WINDOW_START: u32 = 333;
                const WINDOW_END: u32 = 1_222;
                const TAKE_COUNT: usize = 1_025;

                let mut store = $store;
                let mut expected = Vec::with_capacity(ROW_COUNT as usize);

                for index in 0..ROW_COUNT {
                    let key = index.to_be_bytes();
                    let mut value = key.to_vec();
                    value.push(0xA5);
                    value.resize(5 + index as usize % 7, index as u8);
                    store
                        .set(&key, &value)
                        .await
                        .expect("paged seed write must succeed");
                    expected.push((key.to_vec(), value));
                }

                let forward = scan(
                    &store,
                    $crate::__private::Bound::Unbounded,
                    $crate::__private::Bound::Unbounded,
                    false,
                )
                .await;
                assert_eq!(forward.len(), ROW_COUNT as usize);
                assert_eq!(forward, expected, "full forward traversal");

                let reverse = scan(
                    &store,
                    $crate::__private::Bound::Unbounded,
                    $crate::__private::Bound::Unbounded,
                    true,
                )
                .await;
                let expected_reverse: Vec<_> = expected.iter().rev().cloned().collect();
                assert_eq!(reverse, expected_reverse, "full reverse traversal");

                let window_start = WINDOW_START.to_be_bytes();
                let window_end = WINDOW_END.to_be_bytes();
                let window = scan(
                    &store,
                    $crate::__private::Bound::Included(&window_start),
                    $crate::__private::Bound::Excluded(&window_end),
                    false,
                )
                .await;
                assert_eq!(
                    window,
                    expected[WINDOW_START as usize..WINDOW_END as usize],
                    "bounded traversal across interior page seams",
                );

                // This crosses the common 1,000-row FDB batch as well as
                // smaller SQLite cursor pages.
                let taken: Vec<_> = store
                    .range(
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Unbounded,
                        false,
                    )
                    .take(TAKE_COUNT)
                    .await
                    .expect("take across page boundary")
                    .into_iter()
                    .map(|pair| (pair.key, pair.value))
                    .collect();
                assert_eq!(taken, expected[..TAKE_COUNT]);

                // A *bounded* reverse scan long enough to cross a page
                // boundary: 1,204 rows from the top down to `window_start`.
                // This is where a backend that swaps the begin and end key
                // selectors when it flips direction — the classic FDB mistake —
                // starts a page from the wrong side and either repeats or drops
                // rows at the seam. The unbounded reverse scan above cannot see
                // it, because both of its selectors are the keyspace edges.
                let mut expected_reverse_window: Vec<_> =
                    expected[WINDOW_START as usize..].to_vec();
                expected_reverse_window.reverse();
                let reverse_window = scan(
                    &store,
                    $crate::__private::Bound::Included(&window_start),
                    $crate::__private::Bound::Unbounded,
                    true,
                )
                .await;
                assert_eq!(
                    reverse_window, expected_reverse_window,
                    "bounded reverse traversal across page boundaries",
                );

                let taken_reverse: Vec<_> = store
                    .range(
                        $crate::__private::Bound::Included(&window_start),
                        $crate::__private::Bound::Unbounded,
                        true,
                    )
                    .take(TAKE_COUNT)
                    .await
                    .expect("reverse take across page boundary")
                    .into_iter()
                    .map(|pair| (pair.key, pair.value))
                    .collect();
                assert_eq!(taken_reverse, expected_reverse_window[..TAKE_COUNT]);

                store
                    .clear_range(
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Unbounded,
                    )
                    .await
                    .expect("clear full paged store");
                assert!(all_keys(&store).await.is_empty());
            });
        }

        #[test]
        fn kv_handles_the_empty_key() {
            let run = $block_on;
            run(async {
                let mut store = $store;

                store.set(b"", b"root").await.expect("set empty key");
                assert_eq!(
                    store.get(b"").await.expect("get empty key"),
                    Some(b"root".to_vec())
                );
                assert!(store.exists(b"").await.expect("exists empty key"));

                seed(&mut store, &[&[0][..], b"a"]).await;

                // The empty key sorts before everything else.
                assert_eq!(all_keys(&store).await, owned(&[&[][..], &[0][..], b"a"]),);

                // It is reachable from an unbounded start and excludable.
                assert_eq!(
                    keys(
                        &store,
                        $crate::__private::Bound::Excluded(&[][..]),
                        $crate::__private::Bound::Unbounded,
                    )
                    .await,
                    owned(&[&[0][..], b"a"]),
                );

                store.delete(b"").await.expect("delete empty key");
                assert_eq!(all_keys(&store).await, owned(&[&[0][..], b"a"]));
            });
        }

        #[test]
        fn kv_handles_high_byte_keys() {
            let run = $block_on;
            run(async {
                let mut store = $store;

                // These keys are exactly where naive prefix-successor
                // arithmetic breaks: there is no byte string strictly between
                // `[0xFF, 0xFF]` and `[0xFF, 0xFF, 0xFF]` to use as a sentinel.
                let high: &[&[u8]] = &[
                    &[0xFE][..],
                    &[0xFF][..],
                    &[0xFF, 0x00][..],
                    &[0xFF, 0xFF][..],
                    &[0xFF, 0xFF, 0xFF][..],
                ];
                seed(&mut store, high).await;

                assert_eq!(all_keys(&store).await, owned(high));

                // An unbounded end must reach the very last key.
                assert_eq!(
                    keys(
                        &store,
                        $crate::__private::Bound::Included(&[0xFF, 0xFF][..]),
                        $crate::__private::Bound::Unbounded,
                    )
                    .await,
                    owned(&[&[0xFF, 0xFF][..], &[0xFF, 0xFF, 0xFF][..]]),
                );

                assert_eq!(
                    store.get(&[0xFF, 0xFF, 0xFF]).await.expect("get high key"),
                    Some(vec![0xFF, 0xFF, 0xFF]),
                );

                store
                    .clear_range(
                        $crate::__private::Bound::Included(&[0xFF][..]),
                        $crate::__private::Bound::Unbounded,
                    )
                    .await
                    .expect("clear high tail");
                assert_eq!(all_keys(&store).await, owned(&[&[0xFE][..]]));
            });
        }

        #[test]
        fn kv_stream_next_take_collect_for_each_agree() {
            let run = $block_on;
            run(async {
                let mut store = $store;
                let all: &[&[u8]] = &[b"a", b"b", b"c", b"d", b"e"];
                seed(&mut store, all).await;

                // `next` walks one pair at a time and hands back the rest.
                let mut cursor = store.range(
                    $crate::__private::Bound::Unbounded,
                    $crate::__private::Bound::Unbounded,
                    false,
                );
                let mut stepped = Vec::new();
                while let Some((pair, rest)) = cursor.next().await.expect("stream step") {
                    stepped.push(pair.key);
                    cursor = rest;
                }
                assert_eq!(stepped, owned(all));

                // `take` stops continuation polling at the requested count. The
                // suite checks results, not whether a backend prefetched more.
                let none = store
                    .range(
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Unbounded,
                        false,
                    )
                    .take(0)
                    .await
                    .expect("take 0");
                assert!(none.is_empty());

                let some = store
                    .range(
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Unbounded,
                        false,
                    )
                    .take(2)
                    .await
                    .expect("take 2");
                assert_eq!(
                    some.into_iter().map(|pair| pair.key).collect::<Vec<_>>(),
                    owned(&[b"a", b"b"]),
                );

                // Over-taking is capped by the range, not an error.
                let over = store
                    .range(
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Unbounded,
                        false,
                    )
                    .take(100)
                    .await
                    .expect("take 100");
                assert_eq!(over.len(), all.len());

                // `for_each` sees the same sequence `collect` produces.
                let mut visited = Vec::new();
                store
                    .range(
                        $crate::__private::Bound::Unbounded,
                        $crate::__private::Bound::Unbounded,
                        true,
                    )
                    .for_each(|pair| visited.push(pair.key))
                    .await
                    .expect("for_each");
                assert_eq!(visited, owned(&[b"e", b"d", b"c", b"b", b"a"]));

                // An exhausted stream yields `None`, not an error.
                let empty = store.range(
                    $crate::__private::Bound::Included(b"m"),
                    $crate::__private::Bound::Excluded(b"n"),
                    false,
                );
                assert!(empty.next().await.expect("empty stream").is_none());
            });
        }
    };
}
