//! Lazy paged IndexedDB scans over one resolved directory prefix.

use std::{collections::VecDeque, ops::Bound};

use fasm_storage::{KvPair, KvStream, bound_as_slice, validate_dir};
use wasm_bindgen::JsValue;
use web_sys::{IdbCursorDirection, IdbRequest, IdbTransactionMode};

use crate::{
    IndexedDbError, IndexedDbReader, IndexedDbStore, IndexedDbTransaction,
    flat::{RawAsync, data_bounds, prefix_of},
    idb::{
        KV_STORE, KeyRange, PAGE_SIZE, TransactionOutcome, bytes_from_js, dom_error, key_range,
        read_cursor_page,
    },
    overlay::{WriteBuffer, merge_page, next_bounds, page_window},
    store::{Scope, readonly_result},
};

struct ScanState {
    prefix: Vec<u8>,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    reverse: bool,
    pending: VecDeque<KvPair>,
    exhausted: bool,
}

/// Start a directory-bounded scan over committed rows plus session writes.
pub(crate) fn transaction_scan<'a>(
    session: &'a IndexedDbTransaction,
    dir: &[&[u8]],
    start: Bound<&'a [u8]>,
    end: Bound<&'a [u8]>,
    reverse: bool,
) -> KvStream<'a, IndexedDbError> {
    scan_directory(
        session,
        &session.store,
        Some(&session.buffer),
        dir,
        start,
        end,
        reverse,
    )
}

/// Start a directory-bounded scan over committed rows only.
pub(crate) fn reader_scan<'a>(
    reader: &'a IndexedDbReader,
    dir: &[&[u8]],
    start: Bound<&'a [u8]>,
    end: Bound<&'a [u8]>,
    reverse: bool,
) -> KvStream<'a, IndexedDbError> {
    scan_directory(reader, &reader.store, None, dir, start, end, reverse)
}

fn scan_directory<'a, R: RawAsync + ?Sized + 'a>(
    raw: &'a R,
    store: &'a IndexedDbStore,
    buffer: Option<&'a WriteBuffer>,
    dir: &[&[u8]],
    start: Bound<&'a [u8]>,
    end: Bound<&'a [u8]>,
    reverse: bool,
) -> KvStream<'a, IndexedDbError> {
    if let Err(error) = validate_dir(dir).map_err(IndexedDbError::from) {
        return KvStream::failed(error);
    }
    if let Err(error) = store.database() {
        return KvStream::failed(error);
    }
    let directory = dir
        .iter()
        .map(|segment| segment.to_vec())
        .collect::<Vec<_>>();
    KvStream::new(async move {
        let segments = directory.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let Some(prefix) = prefix_of(raw, &segments).await? else {
            return Ok(None);
        };
        let Some((lower, upper)) = data_bounds(&prefix, start, end) else {
            return Ok(None);
        };
        stream_next_page(
            store,
            buffer,
            ScanState {
                prefix,
                lower,
                upper,
                reverse,
                pending: VecDeque::new(),
                exhausted: false,
            },
        )
        .next()
        .await
    })
}

fn stream_next_page<'a>(
    store: &'a IndexedDbStore,
    buffer: Option<&'a WriteBuffer>,
    state: ScanState,
) -> KvStream<'a, IndexedDbError> {
    KvStream::new(async move {
        let mut state = state;
        loop {
            if let Some(pair) = state.pending.pop_front() {
                return Ok(Some((pair, stream_next_page(store, buffer, state))));
            }
            if state.exhausted {
                return Ok(None);
            }

            let lower = bound_as_slice(&state.lower);
            let upper = bound_as_slice(&state.upper);
            let range = key_range(lower, upper)?;
            if matches!(range, KeyRange::Empty) {
                state.exhausted = true;
                continue;
            }

            let transaction = store.begin(IdbTransactionMode::Readonly, Scope::Kv)?;
            let object_store = transaction
                .object_store(KV_STORE)
                .map_err(|value| dom_error(&value))?;
            let request: IdbRequest = match (&range, state.reverse) {
                (KeyRange::All, false) => object_store.open_cursor(),
                (KeyRange::Bounded(range), false) => {
                    object_store.open_cursor_with_range(range.as_ref())
                }
                (KeyRange::All, true) => object_store.open_cursor_with_range_and_direction(
                    &JsValue::UNDEFINED,
                    IdbCursorDirection::Prev,
                ),
                (KeyRange::Bounded(range), true) => object_store
                    .open_cursor_with_range_and_direction(range.as_ref(), IdbCursorDirection::Prev),
                (KeyRange::Empty, _) => {
                    state.exhausted = true;
                    continue;
                }
            }
            .map_err(|value| dom_error(&value))?;
            let outcome = TransactionOutcome::new(transaction);
            let page = read_cursor_page(request, PAGE_SIZE).await;
            readonly_result(outcome.await)?;
            let page = page?;

            let committed = page
                .rows
                .into_iter()
                .map(|(key, value)| {
                    Ok((bytes_from_js(&key, "key")?, bytes_from_js(&value, "value")?))
                })
                .collect::<Result<Vec<_>, IndexedDbError>>()?;
            let last_committed = committed.last().map(|(key, _)| key.as_slice());
            let window = page_window(lower, upper, last_committed, page.exhausted, state.reverse);

            if !page.exhausted {
                let Some(last_committed) = last_committed else {
                    return Err(IndexedDbError::Corrupt {
                        detail: "non-exhausted cursor page contains no rows".to_owned(),
                    });
                };
                (state.lower, state.upper) =
                    next_bounds(lower, upper, last_committed, state.reverse);
            }
            state.exhausted = page.exhausted;

            let empty = WriteBuffer::new();
            let overlay = buffer.unwrap_or(&empty);
            let prefix = state.prefix.as_slice();
            state.pending = merge_page(
                overlay,
                committed,
                (bound_as_slice(&window.0), bound_as_slice(&window.1)),
                state.reverse,
            )
            .into_iter()
            .map(|(key, value)| {
                key.strip_prefix(prefix)
                    .map(|key| KvPair {
                        key: key.to_vec(),
                        value,
                    })
                    .ok_or_else(|| IndexedDbError::Corrupt {
                        detail: "directory cursor yielded a key outside its prefix".to_owned(),
                    })
            })
            .collect::<Result<_, _>>()?;
        }
    })
}
