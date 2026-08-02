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

//! Shared summary statistics for the reported metric series, so every table in a
//! run computes its percentiles the same way.

/// Nearest-rank percentile `q` (0.0–1.0) of a sorted slice, or `0.0` when empty.
#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub(crate) fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (((sorted.len() as f64) * q).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[idx]
}

/// Ascending copy of `values`, ready for [`percentile`]. Uses a total order, so
/// NaN sorts last rather than being treated as equal to everything.
pub(crate) fn sorted_ms(values: &[f64]) -> Vec<f64> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable_by(f64::total_cmp);
    sorted
}

#[cfg(test)]
mod tests {
    use super::{percentile, sorted_ms};

    /// An empty series must not panic — the nearest-rank index would underflow.
    #[test]
    fn percentile_of_an_empty_series_is_zero() {
        assert!((percentile(&[], 0.99) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn percentile_picks_the_nearest_rank() {
        let sorted = sorted_ms(&[5.0, 1.0, 4.0, 2.0, 3.0]);
        assert!((percentile(&sorted, 0.50) - 3.0).abs() < f64::EPSILON);
        assert!((percentile(&sorted, 1.00) - 5.0).abs() < f64::EPSILON);
        // Below the first rank still lands on the smallest sample.
        assert!((percentile(&sorted, 0.0) - 1.0).abs() < f64::EPSILON);
    }
}
