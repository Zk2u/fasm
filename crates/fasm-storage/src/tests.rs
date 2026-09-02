//! Pure-logic tests for `fasm-storage`: the `KvStream` failed/empty
//! constructors.
//!
//! The store-backed tests — the conformance suite, `ScopedKvStore` behaviour,
//! the fail-closed tests, and the keyspace/scoped property tests — live in the
//! backend crates, where a real store is in hand. This crate deliberately has
//! no dependency on any backend: a dev-dependency cycle (backends depend on
//! this one) would make `KvStore` compile as two distinct instances. So the
//! one test that stays here is written against a local test-double error type
//! rather than a backend's.

use core::error::Error;
use core::fmt;
use core::future::Future;
use core::ops::Bound;
use core::pin::pin;
use core::task::{Context, Poll, Waker};

use proptest::prelude::*;

use crate::error::RetryableStorageError;
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

#[test]
fn kv_stream_failed_defers_a_setup_error() {
    block_on(async {
        let stream: KvStream<'_, TestErr> = KvStream::failed(TestErr);
        stream.collect().await.expect_err("deferred error surfaces");

        let empty: KvStream<'_, TestErr> = KvStream::empty();
        assert!(empty.next().await.expect("empty stream").is_none());
    });
}
