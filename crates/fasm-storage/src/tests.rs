//! Pure-logic tests for `fasm-storage`: the `KvStream` failed/empty
//! constructors.
//!
//! The store-backed tests — the conformance suite, `ScopedKvStore` behaviour,
//! the fail-closed tests, and the keyspace/scoped property tests — live in the
//! backend crates, where a real store is in hand. This crate deliberately has
//! no dependency on any backend: a dev-dependency cycle (backends depend on
//! this one) would make `KvStore` compile as two distinct instances. So the
//! tests that stay here are written against local test doubles rather than a
//! backend's.

use core::error::Error;
use core::fmt;
use core::future::Future;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use core::ops::Bound;
use core::pin::pin;
use core::task::{Context, Poll, Waker};

// `proptest` reaches `wait-timeout`, which does not support browser wasm.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use proptest::prelude::*;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use crate::commit::Commit;
use crate::error::RetryableStorageError;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use crate::maybe_send::MaybeSend;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use crate::nav::KvDirNav;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use crate::store::KvStore;
use crate::stream::KvStream;

// =========================================================================
// Test support, local to this crate.
//
// The backend crates keep their own copies of these helpers; a separate crate
// cannot reach a `pub(crate)` item, so each crate carries its own copy of the
// executor and the key/value alphabets. This is deliberate test scaffolding,
// not surface API.
// =========================================================================

/// Minimal executor for this crate's own tests. The future this test drives is
/// a pure `KvStream` over a local error type: it has nothing to wait on and
/// must complete on the first poll, so `Pending` is a bug and we panic on it
/// rather than spin. A backend crate with real I/O supplies its own runtime
/// through the conformance macro's `block_on` parameter.
pub(crate) fn block_on<F: Future>(fut: F) -> F::Output {
    let fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    match fut.poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => {
            panic!("fasm-storage test future returned Pending; must never yield")
        }
    }
}

/// Arbitrary key bytes, drawn from a deliberately tiny alphabet.
///
/// Uniform random bytes would make every generated key distinct and every
/// prefix relationship impossible, which is exactly the structure the
/// `keyspace` properties are about. `0xFF` is over-represented because the
/// successor-less prefix is the interesting edge in `next_prefix`, and
/// `0x00`/`0x01` and `0xFE`/`0xFF` are adjacent so that one key can sit exactly
/// one step above another. The empty key is reachable because it is a legal key
/// that sorts first.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    let byte = prop_oneof![
        3 => Just(0xFFu8),
        3 => Just(0x00u8),
        2 => Just(0x01u8),
        1 => Just(0xFEu8),
        1 => any::<u8>(),
    ];
    prop::collection::vec(byte, 0..5)
}

/// Whether `key` lies inside `bounds`, by plain lexicographic byte comparison.
///
/// The reference answer the store's own range logic is checked against, and
/// the same reference the `keyspace` properties below are asserted against.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) fn bounds_contain(bounds: &(Bound<Vec<u8>>, Bound<Vec<u8>>), key: &[u8]) -> bool {
    let above_start = match &bounds.0 {
        Bound::Included(start) => key >= start.as_slice(),
        Bound::Excluded(start) => key > start.as_slice(),
        Bound::Unbounded => true,
    };
    let below_end = match &bounds.1 {
        Bound::Included(end) => key <= end.as_slice(),
        Bound::Excluded(end) => key < end.as_slice(),
        Bound::Unbounded => true,
    };
    above_start && below_end
}

// The test-double error type the stream test is written against. It exists
// only so this crate can exercise `KvStream` without a backend in the graph.
#[derive(Debug)]
struct TestErr;

impl fmt::Display for TestErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("test error")
    }
}

impl Error for TestErr {}

impl RetryableStorageError for TestErr {
    fn is_retryable(&self) -> bool {
        false
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
struct NativeStore;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl KvStore for NativeStore {
    type Error = TestErr;

    async fn get(&self, _dir: &[&[u8]], _key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(None)
    }

    async fn set(&mut self, _dir: &[&[u8]], _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn delete(&mut self, _dir: &[&[u8]], _key: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }

    fn range<'a>(
        &'a self,
        _dir: &[&[u8]],
        _start: Bound<&'a [u8]>,
        _end: Bound<&'a [u8]>,
        _reverse: bool,
    ) -> KvStream<'a, Self::Error> {
        KvStream::empty()
    }

    async fn clear_range(
        &mut self,
        _dir: &[&[u8]],
        _start: Bound<&[u8]>,
        _end: Bound<&[u8]>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl KvDirNav for NativeStore {
    async fn list_dirs(&self, _dir: &[&[u8]]) -> Result<Vec<Vec<u8>>, Self::Error> {
        Ok(Vec::new())
    }

    async fn dir_exists(&self, _dir: &[&[u8]]) -> Result<bool, Self::Error> {
        Ok(false)
    }

    async fn remove_dir(&mut self, _dir: &[&[u8]]) -> Result<bool, Self::Error> {
        Ok(false)
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
struct NativeCommit;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl Commit for NativeCommit {
    type Error = TestErr;

    async fn commit(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[test]
fn maybe_send_implies_send_on_native() {
    fn assert_send<T: Send>() {}

    fn via_marker<T: MaybeSend + 'static>() {
        assert_send::<T>();
    }

    via_marker::<TestErr>();
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[test]
fn native_storage_contract_remains_send_and_sync() {
    fn assert_send<T: Send>(_value: T) {}
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send(NativeCommit.commit());
    assert_send(KvStream::<'static, TestErr>::new(async { Ok(None) }));
    assert_send_sync::<NativeStore>();

    // Native `async fn` traits are not dyn-compatible, so the contract is
    // asserted on the concrete store rather than `Box<dyn KvStore>`.
}

#[test]
fn kv_stream_failed_defers_a_setup_error() {
    block_on(async {
        let stream: KvStream<'_, TestErr> = KvStream::failed(TestErr);
        stream.collect().await.expect_err("deferred error surfaces");

        let empty: KvStream<'_, TestErr> = KvStream::empty();
        assert!(empty.next().await.expect("empty stream").is_none());
    });
}

#[cfg(all(test, target_arch = "wasm32", target_os = "unknown"))]
mod browser_contract {
    use core::cell::Cell;
    use core::mem::drop;
    use core::ops::Bound;
    use std::rc::Rc;

    use super::TestErr;
    use crate::commit::Commit;
    use crate::nav::KvDirNav;
    use crate::scoped::ScopedKvStore;
    use crate::store::KvStore;
    use crate::stream::KvStream;

    struct BrowserStore {
        state: Rc<Cell<u8>>,
    }

    impl KvStore for BrowserStore {
        type Error = TestErr;

        async fn get(&self, _dir: &[&[u8]], _key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            self.state.set(self.state.get().wrapping_add(1));
            Ok(None)
        }

        async fn set(
            &mut self,
            _dir: &[&[u8]],
            _key: &[u8],
            _value: &[u8],
        ) -> Result<(), Self::Error> {
            self.state.set(self.state.get().wrapping_add(1));
            Ok(())
        }

        async fn delete(&mut self, _dir: &[&[u8]], _key: &[u8]) -> Result<(), Self::Error> {
            self.state.set(self.state.get().wrapping_add(1));
            Ok(())
        }

        fn range<'a>(
            &'a self,
            _dir: &[&[u8]],
            _start: Bound<&'a [u8]>,
            _end: Bound<&'a [u8]>,
            _reverse: bool,
        ) -> KvStream<'a, Self::Error> {
            let state = Rc::clone(&self.state);
            KvStream::new(async move {
                state.set(state.get().wrapping_add(1));
                Ok(None)
            })
        }

        async fn clear_range(
            &mut self,
            _dir: &[&[u8]],
            _start: Bound<&[u8]>,
            _end: Bound<&[u8]>,
        ) -> Result<(), Self::Error> {
            self.state.set(self.state.get().wrapping_add(1));
            Ok(())
        }
    }

    impl KvDirNav for BrowserStore {
        async fn list_dirs(&self, _dir: &[&[u8]]) -> Result<Vec<Vec<u8>>, Self::Error> {
            Ok(Vec::new())
        }

        async fn dir_exists(&self, _dir: &[&[u8]]) -> Result<bool, Self::Error> {
            Ok(false)
        }

        async fn remove_dir(&mut self, _dir: &[&[u8]]) -> Result<bool, Self::Error> {
            Ok(false)
        }
    }

    impl Commit for BrowserStore {
        type Error = TestErr;

        async fn commit(self) -> Result<(), Self::Error> {
            self.state.set(self.state.get().wrapping_add(1));
            Ok(())
        }
    }

    async fn drive_browser_contract() {
        let state = Rc::new(Cell::new(0));
        let store = BrowserStore {
            state: Rc::clone(&state),
        };
        let mut scoped = ScopedKvStore::new(store, vec![b"scope".to_vec()]);
        let _ = scoped.set(&[], b"key", b"value").await;
        let _ = scoped.dir_exists(&[]).await;

        let stream_state = Rc::clone(&state);
        let stream: KvStream<'static, TestErr> = KvStream::new(async move {
            stream_state.set(stream_state.get().wrapping_add(1));
            Ok(None)
        });
        let _ = stream.collect().await;

        let _ = scoped.commit().await;
    }

    #[test]
    fn thread_local_browser_storage_typechecks() {
        drop(drive_browser_contract());
    }
}
