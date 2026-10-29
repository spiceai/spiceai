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

//! Warns while a refresh runs far longer than the dataset's recent refreshes did.
//!
//! A source that slows down (an overloaded replica, a saturated network path) shows up
//! first as refreshes that take longer. The warning fires once per refresh, while it is
//! still running, so a refresh that normally takes seconds is reported within about a
//! minute rather than when it finally finishes.

use std::collections::VecDeque;
use std::time::Duration;

use parking_lot::Mutex;
use runtime_component::dataset::acceleration::RefreshMode;
use tokio::task::AbortHandle;

/// How many of a dataset's recent successful refreshes, per mode, make its baseline.
const HISTORY: usize = 10;
/// The baseline needs at least this many refreshes before a refresh is compared with it.
const MIN_HISTORY: usize = 3;
/// A refresh is slow once it has run this many times the baseline...
const SLOWDOWN_FACTOR: u32 = 3;
/// ...and at least this much longer than it, so short refreshes that vary by a few
/// seconds are not reported.
const MIN_EXCESS: Duration = Duration::from_secs(30);

const DATA_REFRESH_DOCS: &str = "https://spiceai.org/docs/features/data-acceleration/data-refresh";

/// The durations of a dataset's recent successful refreshes, for full and append
/// refreshes separately: an append refresh reads only what is new, so the two are not
/// comparable.
#[derive(Debug, Default)]
pub(crate) struct RefreshDurations {
    full: Mutex<VecDeque<Duration>>,
    append: Mutex<VecDeque<Duration>>,
}

/// When a refresh becomes slow, and the baseline it is compared with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SlowAfter {
    /// How long the refresh may run before it is reported.
    pub elapsed: Duration,
    /// The median duration of the recent refreshes.
    pub median: Duration,
    /// How many recent refreshes the median is over.
    pub refreshes: usize,
}

impl RefreshDurations {
    fn history(&self, mode: RefreshMode) -> Option<&Mutex<VecDeque<Duration>>> {
        match mode {
            RefreshMode::Full => Some(&self.full),
            RefreshMode::Append => Some(&self.append),
            RefreshMode::Disabled
            | RefreshMode::Changes
            | RefreshMode::Caching
            | RefreshMode::Snapshot => None,
        }
    }

    /// Records the duration of a successful refresh in `mode`.
    pub(crate) fn record(&self, mode: RefreshMode, duration: Duration) {
        if let Some(history) = self.history(mode) {
            let mut history = history.lock();
            if history.len() == HISTORY {
                history.pop_front();
            }
            history.push_back(duration);
        }
    }

    /// When a refresh in `mode` starting now becomes slow, or `None` while there are too
    /// few recent refreshes to compare it with.
    pub(crate) fn slow_after(&self, mode: RefreshMode) -> Option<SlowAfter> {
        let history = self.history(mode)?.lock();
        if history.len() < MIN_HISTORY {
            return None;
        }
        let mut sorted: Vec<Duration> = history.iter().copied().collect();
        sorted.sort_unstable();
        let median = sorted[sorted.len() / 2];
        Some(SlowAfter {
            elapsed: (median * SLOWDOWN_FACTOR).max(median + MIN_EXCESS),
            median,
            refreshes: sorted.len(),
        })
    }
}

/// The warning for a refresh that has run past [`SlowAfter::elapsed`]. `component_type` is
/// `dataset` or `view`.
pub(crate) fn slow_refresh_warning(
    component_type: &str,
    name: &str,
    mode: RefreshMode,
    slow: SlowAfter,
) -> String {
    let mode = match mode {
        RefreshMode::Append => "append",
        _ => "full",
    };
    format!(
        "Refresh of {component_type} '{name}' has run for {elapsed}, more than {SLOWDOWN_FACTOR} times the {median} its last {refreshes} {mode} refreshes took at the median, so its accelerated data is falling further behind the source than usual. A slower source or network path is the usual cause: compare the dataset's `dataset_acceleration_refresh_bytes_written` rate with earlier refreshes, and check the load on the server its connections reached. See: {DATA_REFRESH_DOCS}",
        elapsed = format_duration(slow.elapsed),
        median = format_duration(slow.median),
        refreshes = slow.refreshes,
    )
}

/// Whole seconds from a second up, milliseconds below.
fn format_duration(duration: Duration) -> String {
    let rounded = if duration >= Duration::from_secs(1) {
        Duration::from_secs(duration.as_secs())
    } else {
        Duration::from_millis(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
    };
    humantime::format_duration(rounded).to_string()
}

/// Logs the warning if the refresh is still running after `slow.elapsed`. Dropping the
/// watch, when the refresh ends, cancels it.
pub(crate) struct SlowRefreshWatch(AbortHandle);

impl SlowRefreshWatch {
    pub(crate) fn start(warning: String, slow: SlowAfter) -> Self {
        let task = tokio::spawn(async move {
            tokio::time::sleep(slow.elapsed).await;
            tracing::warn!("{warning}");
        });
        Self(task.abort_handle())
    }
}

impl Drop for SlowRefreshWatch {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn durations(mode: RefreshMode, seconds: &[u64]) -> RefreshDurations {
        let durations = RefreshDurations::default();
        for s in seconds {
            durations.record(mode, Duration::from_secs(*s));
        }
        durations
    }

    #[test]
    fn needs_three_refreshes_before_comparing() {
        assert_eq!(
            durations(RefreshMode::Full, &[21, 22]).slow_after(RefreshMode::Full),
            None
        );
        assert_eq!(
            durations(RefreshMode::Full, &[21, 22, 20]).slow_after(RefreshMode::Full),
            Some(SlowAfter {
                elapsed: Duration::from_secs(63),
                median: Duration::from_secs(21),
                refreshes: 3,
            })
        );
    }

    #[test]
    fn short_refreshes_need_thirty_seconds_over_the_median() {
        let slow = durations(RefreshMode::Full, &[1, 2, 2, 3]).slow_after(RefreshMode::Full);
        assert_eq!(slow.map(|s| s.elapsed), Some(Duration::from_secs(32)));
    }

    #[test]
    fn the_median_ignores_one_outlier_and_keeps_the_last_ten() {
        let mut seconds = vec![600];
        seconds.extend([20; 10]);
        // The 600s refresh has left the window of the last ten.
        let slow = durations(RefreshMode::Full, &seconds).slow_after(RefreshMode::Full);
        assert_eq!(
            slow.map(|s| (s.median, s.refreshes)),
            Some((Duration::from_secs(20), 10))
        );

        let slow =
            durations(RefreshMode::Full, &[20, 21, 600, 22, 19]).slow_after(RefreshMode::Full);
        assert_eq!(slow.map(|s| s.median), Some(Duration::from_secs(21)));
    }

    #[test]
    fn full_and_append_refreshes_are_kept_apart() {
        let durations = durations(RefreshMode::Full, &[60, 60, 60]);
        durations.record(RefreshMode::Append, Duration::from_secs(1));
        assert_eq!(durations.slow_after(RefreshMode::Append), None);
        assert_eq!(durations.slow_after(RefreshMode::Caching), None);
        durations.record(RefreshMode::Caching, Duration::from_secs(1));
        assert_eq!(durations.slow_after(RefreshMode::Caching), None);
    }

    #[test]
    fn warning_names_the_dataset_the_baseline_and_what_to_check() {
        let slow = durations(RefreshMode::Full, &[21, 22, 20])
            .slow_after(RefreshMode::Full)
            .expect("three refreshes");
        assert_eq!(
            slow_refresh_warning("dataset", "orders", RefreshMode::Full, slow),
            "Refresh of dataset 'orders' has run for 1m 3s, more than 3 times the 21s its last 3 full refreshes took at the median, so its accelerated data is falling further behind the source than usual. A slower source or network path is the usual cause: compare the dataset's `dataset_acceleration_refresh_bytes_written` rate with earlier refreshes, and check the load on the server its connections reached. See: https://spiceai.org/docs/features/data-acceleration/data-refresh"
        );
    }

    #[test]
    fn sub_second_baselines_print_milliseconds() {
        let durations = RefreshDurations::default();
        for millis in [40, 50, 60] {
            durations.record(RefreshMode::Append, Duration::from_millis(millis));
        }
        let slow = durations
            .slow_after(RefreshMode::Append)
            .expect("three refreshes");
        assert!(
            slow_refresh_warning("view", "v", RefreshMode::Append, slow)
                .starts_with("Refresh of view 'v' has run for 30s, more than 3 times the 50ms its last 3 append refreshes took at the median")
        );
    }
}
