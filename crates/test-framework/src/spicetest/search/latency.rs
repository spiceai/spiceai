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

//! Tail-latency percentiles of search requests.

use std::time::Duration;

/// A percentile is only reported when at least this many samples are slower than it. With fewer,
/// the value is decided by a handful of requests and is mostly noise.
pub const MIN_SAMPLES_BEYOND_PERCENTILE: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LatencyPercentile {
    P50,
    P95,
    P99,
    P999,
}

impl LatencyPercentile {
    pub const ALL: [Self; 4] = [Self::P50, Self::P95, Self::P99, Self::P999];

    /// The share of samples at or below the percentile, in thousandths. Integer, so that the rank
    /// is exact (`0.999 * n` in floating point can round past it).
    #[must_use]
    pub fn per_mille(self) -> usize {
        match self {
            Self::P50 => 500,
            Self::P95 => 950,
            Self::P99 => 990,
            Self::P999 => 999,
        }
    }

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::P50 => "p50",
            Self::P95 => "p95",
            Self::P99 => "p99",
            Self::P999 => "p99.9",
        }
    }
}

/// The latency samples of a run (or of one query type), sorted once for percentile lookups.
#[derive(Debug, Clone)]
pub struct LatencySummary {
    sorted: Vec<Duration>,
}

impl LatencySummary {
    #[must_use]
    pub fn new(mut samples: Vec<Duration>) -> Self {
        samples.sort_unstable();
        Self { sorted: samples }
    }

    #[must_use]
    pub fn samples(&self) -> usize {
        self.sorted.len()
    }

    /// The nearest-rank percentile in milliseconds, or `None` when fewer than
    /// [`MIN_SAMPLES_BEYOND_PERCENTILE`] samples are slower than it.
    #[must_use]
    pub fn percentile_ms(&self, percentile: LatencyPercentile) -> Option<f64> {
        let n = self.sorted.len();
        // Nearest rank: the smallest sample with at least `per_mille * n / 1000` samples at or
        // below it.
        let rank = (percentile.per_mille() * n).div_ceil(1000).max(1);
        if n.checked_sub(rank)? < MIN_SAMPLES_BEYOND_PERCENTILE {
            return None;
        }
        Some(self.sorted[rank - 1].as_secs_f64() * 1000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{LatencyPercentile, LatencySummary};
    use std::time::Duration;

    fn millis(values: impl IntoIterator<Item = u64>) -> LatencySummary {
        LatencySummary::new(values.into_iter().map(Duration::from_millis).collect())
    }

    #[test]
    fn percentiles_use_nearest_rank_on_unsorted_input() {
        // 1..=1000 ms, reversed.
        let summary = millis((1..=1000).rev());
        assert_eq!(summary.samples(), 1000);
        assert_eq!(summary.percentile_ms(LatencyPercentile::P50), Some(500.0));
        assert_eq!(summary.percentile_ms(LatencyPercentile::P95), Some(950.0));
        assert_eq!(summary.percentile_ms(LatencyPercentile::P99), Some(990.0));
    }

    #[test]
    fn percentile_needs_ten_samples_beyond_it() {
        // P99.9 of 9,999 samples has 9 samples beyond it; of 10,000 samples, 10.
        assert_eq!(
            millis(1..=9_999).percentile_ms(LatencyPercentile::P999),
            None
        );
        assert_eq!(
            millis(1..=10_000).percentile_ms(LatencyPercentile::P999),
            Some(9_990.0)
        );
        // P99 of 999 samples has 9 samples beyond it.
        assert_eq!(millis(1..=999).percentile_ms(LatencyPercentile::P99), None);
    }

    #[test]
    fn empty_and_tiny_sets_report_nothing() {
        for percentile in LatencyPercentile::ALL {
            assert_eq!(millis([]).percentile_ms(percentile), None);
            assert_eq!(millis([5]).percentile_ms(percentile), None);
        }
    }

    #[test]
    fn sub_millisecond_latency_keeps_its_fraction() {
        let summary = LatencySummary::new(vec![Duration::from_micros(250); 40]);
        assert_eq!(summary.percentile_ms(LatencyPercentile::P50), Some(0.25));
    }
}
