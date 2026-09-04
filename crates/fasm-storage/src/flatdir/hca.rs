//! The High Contention Allocator, ported from the `foundationdb` crate's
//! `tuple_ext::hca::HighContentionAllocator` (pinned `foundationdb-0.11.0`).
//!
//! The control flow is the layer's exactly: windowed allocation, the
//! `count * 2 < window` advance check against the pre-increment count,
//! the clears of the abandoned window's counter and recent rows on
//! advance, the randomized candidate, and the recents row claim that
//! makes concurrent creations opportunistic.
//!
//! Documented deltas for the flat single-writer view:
//!
//! * FDB writes the recents row and a write-conflict range over the
//!   recents subspace in one transaction; here there is exactly one
//!   writer, so the claim degrades to the `claimed` check plus the row
//!   write (no conflict range, no version-vector coupling).
//! * FDB increments the counter with an atomic op and reads the
//!   pre-increment snapshot; here the increment is a read-modify-write of
//!   the counter row that happens after the window check, using the
//!   pre-increment count — the counter rows land at the same byte values
//!   FDB's atomic op leaves them at.
//! * The candidate range uses the caller's RNG: `thread_rng` in
//!   production, a seeded generator in tests.

use rand::RngExt;

use crate::flatdir::error::FlatError;
use crate::flatdir::layout::{COUNTERS_BASE, RECENT_BASE, counter_key, recent_key};
use crate::flatdir::tuple::unpack_i64_full;
use crate::rawkv::RawKv;

/// The window size for a window start (verbatim from the layer).
pub fn window_size(start: i64) -> i64 {
    match start {
        _ if start < 255 => 64,
        _ if start < 65535 => 1024,
        _ => 8192,
    }
}

/// Read the start of the latest window from the counters subspace.
///
/// The subspace holds at most one live counter row (abandoned rows are
/// cleared on advance). The scan is ascending; the last row is the
/// largest window start — the flat equivalent of FDB's reverse-scan
/// limit-1 read.
fn last_window_start<R: RawKv>(raw: &R) -> Result<i64, FlatError<R::Error>> {
    let mut start = COUNTERS_BASE.to_vec();
    start.push(0);
    let mut end = COUNTERS_BASE.to_vec();
    end.push(0xFF);
    end.push(0);
    let rows = raw.scan(
        core::ops::Bound::Included(start.as_slice()),
        core::ops::Bound::Excluded(end.as_slice()),
        true,
    )?;
    let Some(pair) = rows.last() else {
        return Ok(0);
    };
    let rel = &pair.key[COUNTERS_BASE.len()..];
    // The counter key is the base followed by a packed i64 window start.
    unpack_i64_full(rel).ok_or(FlatError::Corrupt)
}

/// Allocate a prefix value from the HCA.
///
/// Returns the allocated `i64` (pack it with
/// [`crate::flatdir::tuple::pack_i64`] for the raw form). This is the
/// layer's `allocate` control flow over a single `RawKv` view; see the
/// module docs for the documented deltas.
pub fn allocate<R: RawKv, RNG: RngExt>(
    raw: &mut R,
    rng: &mut RNG,
) -> Result<i64, FlatError<R::Error>> {
    'outer: loop {
        let mut start = last_window_start(raw)?;
        let mut window_advanced = false;
        let window = loop {
            if window_advanced {
                // Clear the abandoned window's counter and recent rows.
                // In FDB this happens in the same transaction as the
                // increment above, after the counter snapshot was taken;
                // the single-writer view keeps that order: the old rows
                // are read (last_window_start) before they are cleared,
                // and the net state is the same — the old counter row
                // gone, the new window's counter row at 1.
                let new_counter = counter_key(start);
                raw.clear_range(
                    core::ops::Bound::Included(COUNTERS_BASE),
                    core::ops::Bound::Excluded(new_counter.as_slice()),
                )?;
                let new_recent = recent_key(start);
                raw.clear_range(
                    core::ops::Bound::Included(RECENT_BASE),
                    core::ops::Bound::Excluded(new_recent.as_slice()),
                )?;
            }
            let key = counter_key(start);
            // Read the pre-increment count (FDB's atomic-op snapshot).
            let count: i64 = match raw.get(&key)? {
                Some(v) if v.len() == 8 => i64::from_le_bytes(v.as_slice().try_into().unwrap()),
                Some(_) => return Err(FlatError::Corrupt),
                None => 0,
            };
            let window = window_size(start);
            if count * 2 < window {
                // Increment after the window decision (see the module
                // docs); the row lands where FDB's atomic op left it.
                let mut v = [0u8; 8];
                v.copy_from_slice(&(count + 1).to_le_bytes());
                raw.insert(&key, &v)?;
                break window;
            }
            start += window;
            window_advanced = true;
        };

        loop {
            let candidate = rng.random_range(start..start + window);
            let claimed = raw.get(&recent_key(candidate))?;
            raw.insert(&recent_key(candidate), &[])?;
            // The window may have advanced (never, in a single-writer
            // view, between these reads — the re-read is kept verbatim).
            let latest = last_window_start(raw)?;
            if latest > start {
                continue 'outer;
            }
            if claimed.is_none() {
                // No conflict range: the single writer is the only
                // contender (see the module docs).
                return Ok(candidate);
            }
        }
    }
}

#[cfg(test)]
pub(crate) struct Lcg(u64);

#[cfg(test)]
impl Lcg {
    /// A deterministic test RNG with the given initial state.
    pub(crate) const fn new(state: u64) -> Self {
        Lcg(state)
    }

    fn step(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
}

#[cfg(test)]
impl rand::TryRng for Lcg {
    type Error = core::convert::Infallible;

    fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
        Ok((self.step() >> 32) as u32)
    }
    fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
        Ok(self.step())
    }
    fn try_fill_bytes(&mut self, dst: &mut [u8]) -> Result<(), Self::Error> {
        for chunk in dst.chunks_mut(8) {
            let v = self.try_next_u64()?;
            chunk.copy_from_slice(&v.to_le_bytes()[..chunk.len()]);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flatengine::Mem;

    fn last_window(raw: &Mem) -> i64 {
        last_window_start(raw).unwrap()
    }

    #[test]
    fn window_sizes_are_the_layer_thresholds() {
        assert_eq!(window_size(0), 64);
        assert_eq!(window_size(254), 64);
        assert_eq!(window_size(255), 1024);
        assert_eq!(window_size(65535 - 1), 1024);
        assert_eq!(window_size(65535), 8192);
        assert_eq!(window_size(100_000_000), 8192);
    }

    #[test]
    fn the_first_allocation_lands_in_the_first_window() {
        let mut raw = Mem::default();
        let mut rng = Lcg(1);
        let v = allocate(&mut raw, &mut rng).unwrap();
        assert!((0..64).contains(&v));
        // The counter row is 1 (LE) at the window start.
        assert_eq!(
            raw.get(&counter_key(0)).unwrap(),
            Some(vec![1, 0, 0, 0, 0, 0, 0, 0])
        );
        // The claimed recent row is empty.
        assert_eq!(raw.get(&recent_key(v)).unwrap(), Some(vec![]));
        assert_eq!(last_window(&raw), 0);
    }

    #[test]
    fn a_taken_candidate_is_skipped() {
        // The first candidate for seed 1 in window 0, from an empty
        // store:
        let mut raw = Mem::default();
        let mut probe = Lcg(1);
        let first = allocate(&mut raw, &mut probe).unwrap();

        // Pre-claim that candidate (a concurrent creation's leftover
        // recent row) and allocate again with the same seed: the
        // allocator must skip it and claim the next candidate instead.
        let mut raw = Mem::default();
        raw.insert(&recent_key(first), &[]).unwrap();
        let mut rng = Lcg(1);
        let v = allocate(&mut raw, &mut rng).unwrap();
        assert_ne!(v, first);
        assert!((0..64).contains(&v));
        // Both recent rows exist (the pre-claimed one and the new one).
        assert_eq!(raw.get(&recent_key(first)).unwrap(), Some(vec![]));
        assert_eq!(raw.get(&recent_key(v)).unwrap(), Some(vec![]));
    }

    #[test]
    fn the_window_advances_at_the_layer_threshold() {
        // Window 0 (size 64): the advance fires when count * 2 >= 64,
        // i.e. after 32 allocations. The 33rd allocation advances to
        // window start 64 and clears window 0's counter and recent rows.
        let mut raw = Mem::default();
        let mut rng = Lcg(7);
        let mut prefixes: Vec<i64> = Vec::new();
        for _ in 0..33 {
            prefixes.push(allocate(&mut raw, &mut rng).unwrap());
        }
        assert_eq!(
            prefixes.len(),
            prefixes
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            "all distinct"
        );
        assert_eq!(raw.get(&counter_key(0)).unwrap(), None);
        assert_eq!(
            raw.get(&counter_key(64)).unwrap(),
            Some(vec![1, 0, 0, 0, 0, 0, 0, 0])
        );
        assert_eq!(raw.get(&recent_key(0)).unwrap(), None);
        assert!((64..128).contains(&prefixes[32]));
    }

    #[test]
    fn a_corrupt_counter_row_is_corrupt_not_a_panic() {
        let mut raw = Mem::default();
        raw.insert(&counter_key(0), &[1, 2, 3]).unwrap(); // not 8 bytes LE
        let mut rng = Lcg(1);
        assert_eq!(
            allocate(&mut raw, &mut rng).unwrap_err(),
            FlatError::Corrupt
        );
    }

    #[test]
    fn a_malformed_window_key_is_corrupt() {
        let mut raw = Mem::default();
        // A row inside the counters subspace whose relative key is not a
        // well-formed packed i64.
        let mut key = COUNTERS_BASE.to_vec();
        key.extend_from_slice(&[0x99, 0x00]);
        raw.insert(&key, &[1, 0, 0, 0, 0, 0, 0, 0]).unwrap();
        let mut rng = Lcg(1);
        assert_eq!(
            allocate(&mut raw, &mut rng).unwrap_err(),
            FlatError::Corrupt
        );
    }
}
