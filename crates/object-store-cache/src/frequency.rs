/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! How often each block is read: a count-min sketch with saturating 4-bit
//! counters and periodic halving, as in `TinyLFU` (Einziger, Friedman and
//! Manes, 2017), the frequency filter of `W-TinyLFU`.
//!
//! - Four rows of counters; a block's counter in each row is picked by one of
//!   the four 32-bit words of its digest (a BLAKE3 prefix, so the words are
//!   independent hashes and nothing is hashed again).
//! - A read increments the block's four counters, each saturating at 15. The
//!   estimate is the smallest of the four.
//! - After `10 × width` increments every counter is halved, so the estimate
//!   follows recent reads and a block hot an hour ago decays. Halving walks
//!   every counter (up to 16 Mi of them), so [`FrequencySketch::record`] only
//!   reports that it is due and the caller runs [`FrequencySketch::halve`] on
//!   a blocking thread, never on the read that crossed the threshold.
//!
//! The sketch remembers blocks that are not resident: a block evicted and
//! read again keeps its history, so the slab it is written into is valued
//! accordingly at once.

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use crate::block_store::Digest;

const ROWS: usize = 4;
const MAX_COUNT: u8 = 15;
const MIN_WIDTH: usize = 1 << 10;
const MAX_WIDTH: usize = 1 << 22;
/// Increments between halvings, per counter of a row.
const SAMPLE_PER_COUNTER: u64 = 10;

pub(crate) struct FrequencySketch {
    /// `ROWS` rows of `mask + 1` counters, row after row.
    counters: Box<[AtomicU8]>,
    mask: usize,
    additions: AtomicU64,
    sample: u64,
}

/// The sketch's shape, not its counters.
impl std::fmt::Debug for FrequencySketch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrequencySketch")
            .field("width", &(self.mask + 1))
            .field("additions", &self.additions.load(Ordering::Relaxed))
            .field("sample", &self.sample)
            .finish_non_exhaustive()
    }
}

impl FrequencySketch {
    /// A sketch sized for about `blocks` distinct blocks (a power of two
    /// counters per row, between 1,024 and 4,194,304).
    pub(crate) fn new(blocks: u64) -> Self {
        let width = usize::try_from(blocks)
            .unwrap_or(MAX_WIDTH)
            .clamp(MIN_WIDTH, MAX_WIDTH)
            .next_power_of_two();
        Self {
            counters: (0..ROWS * width).map(|_| AtomicU8::new(0)).collect(),
            mask: width - 1,
            additions: AtomicU64::new(0),
            sample: SAMPLE_PER_COUNTER * width as u64,
        }
    }

    fn slots(&self, d: &Digest) -> [usize; ROWS] {
        let width = self.mask + 1;
        std::array::from_fn(|row| {
            let word = u32::from_le_bytes([
                d.0[4 * row],
                d.0[4 * row + 1],
                d.0[4 * row + 2],
                d.0[4 * row + 3],
            ]);
            row * width + (word as usize & self.mask)
        })
    }

    /// Counts one read of the block. Returns true when this read completed a
    /// sample period: the caller then runs [`FrequencySketch::halve`] off the
    /// read path.
    #[must_use]
    pub(crate) fn record(&self, d: &Digest) -> bool {
        for slot in self.slots(d) {
            let _ = self.counters[slot].try_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                (c < MAX_COUNT).then_some(c + 1)
            });
        }
        let n = self.additions.fetch_add(1, Ordering::Relaxed) + 1;
        // Exactly one reader crosses the threshold; increments racing the
        // halving land on either side of it, which only blurs an estimate.
        if n == self.sample {
            self.additions.fetch_sub(self.sample, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Halves every counter, so estimates follow recent reads.
    pub(crate) fn halve(&self) {
        for c in &self.counters {
            let _ = c.try_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v >> 1));
        }
    }

    /// The block's estimated recent reads, 0 to 15.
    pub(crate) fn estimate(&self, d: &Digest) -> u32 {
        self.slots(d)
            .into_iter()
            .map(|slot| u32::from(self.counters[slot].load(Ordering::Relaxed)))
            .min()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(i: u64) -> Digest {
        let h = blake3::hash(&i.to_le_bytes());
        let mut d = [0u8; 16];
        d.copy_from_slice(&h.as_bytes()[..16]);
        Digest(d)
    }

    #[test]
    fn estimates_saturate_and_halve() {
        let s = FrequencySketch::new(1);
        assert_eq!(s.mask + 1, MIN_WIDTH);
        let hot = digest(7);
        for _ in 0..20 {
            assert!(!s.record(&hot), "no halving due yet");
        }
        assert_eq!(s.estimate(&hot), 15, "saturates at 15");
        assert_eq!(s.estimate(&digest(8)), 0, "an unread block");
        // Fill up to the halving point with other blocks: exactly the last
        // of these reads reports the halving as due, and does not run it.
        let sample = s.sample;
        let due: Vec<u64> = (0..(sample - 20))
            .filter(|&i| s.record(&digest(1_000_000 + i)))
            .collect();
        assert_eq!(due, vec![sample - 21], "due once, on the last read");
        assert_eq!(s.estimate(&hot), 15, "the read path did not halve");
        assert_eq!(s.additions.load(Ordering::Relaxed), 0);
        // The hot block's counters, saturated at 15 (collisions cannot raise
        // them further), are halved to 7.
        s.halve();
        assert_eq!(s.estimate(&hot), 7, "after halving");
    }
}
