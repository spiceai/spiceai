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

//! Count-Min Sketch used by `TinyLFU` admission.
//!
//! Four rows of 4096 saturating `u8` counters (~16 KiB). When the addition
//! counter reaches the sample size, every cell is halved so the sketch tracks
//! recent frequency rather than lifetime counts.
//!
//! The row and column a key lands in are computed by
//! [`crate::layout::sketch_cell_index`], whose in-range result is proved rather
//! than sampled: this subscripts a flat array under the shard lock.

use crate::layout::{SKETCH_DEPTH as DEPTH, SKETCH_WIDTH as WIDTH, sketch_cell_index};

const SAMPLE_SIZE: u32 = 10_000;

const SEEDS: [u64; DEPTH] = [
    0x9E37_79B9_7F4A_7C15,
    0xC2B2_AE3D_27D4_EB4F,
    0x1656_67B1_9E37_79F9,
    0x85EB_CA77_C2B2_AE63,
];

/// Frequency sketch for `TinyLFU` admission decisions.
pub(crate) struct CountMinSketch {
    counters: Box<[u8]>,
    additions: u32,
}

impl CountMinSketch {
    pub(crate) fn new() -> Self {
        Self {
            counters: vec![0; DEPTH * WIDTH].into_boxed_slice(),
            additions: 0,
        }
    }

    pub(crate) fn increment(&mut self, key: u64) {
        self.additions = self.additions.saturating_add(1);
        if self.additions >= SAMPLE_SIZE {
            self.age();
        }
        for row in 0..DEPTH {
            let cell = &mut self.counters[sketch_cell_index(mix(key, row), row)];
            *cell = cell.saturating_add(1);
        }
    }

    pub(crate) fn estimate(&self, key: u64) -> u8 {
        let mut min = u8::MAX;
        for row in 0..DEPTH {
            min = min.min(self.counters[sketch_cell_index(mix(key, row), row)]);
        }
        min
    }

    fn age(&mut self) {
        self.additions = 0;
        for cell in &mut self.counters {
            *cell >>= 1;
        }
    }
}

/// Row-seeded mix of `key`. Only its low bits reach a column, so this is free
/// to change without affecting the bound `sketch_cell_index` carries.
fn mix(key: u64, row: usize) -> u64 {
    let mut mixed = key ^ SEEDS[row];
    mixed = mixed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    mixed ^= mixed >> 32;
    mixed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increment_raises_estimate() {
        let mut sketch = CountMinSketch::new();
        assert_eq!(sketch.estimate(7), 0);
        sketch.increment(7);
        sketch.increment(7);
        assert!(sketch.estimate(7) >= 2);
    }

    #[test]
    fn unreferenced_key_stays_near_zero() {
        let mut sketch = CountMinSketch::new();
        for _ in 0..32 {
            sketch.increment(1);
        }
        assert!(
            sketch.estimate(99) < sketch.estimate(1),
            "a never-seen key must not outrank a hot one"
        );
    }
}
