/*
Copyright 2024-2026 The Spice.ai OSS Authors
Copyright (c) 2016 Tibor Benke

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

use std::time::{Duration, Instant};

use crate::backoff::Backoff;

const INITIAL_INTERVAL: Duration = Duration::from_millis(500);
const RANDOMIZATION_FACTOR: f64 = 0.5;
const MULTIPLIER: f64 = 1.5;
const MAX_INTERVAL: Duration = Duration::from_mins(1);
const MAX_ELAPSED_TIME: Duration = Duration::from_mins(15);

/// Exponentially growing, randomized retry intervals.
///
/// Each interval is drawn uniformly from
/// `current_interval * [1 - randomization_factor, 1 + randomization_factor]`,
/// after which `current_interval` is multiplied by `multiplier`, up to
/// `max_interval`. Once `max_elapsed_time` has passed since `start_time`,
/// [`Backoff::next_backoff`] returns `None`.
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    /// The interval the next backoff is randomized around.
    pub current_interval: Duration,
    /// The interval [`Backoff::reset`] restores.
    pub initial_interval: Duration,
    /// How far, as a fraction of `current_interval`, an interval may deviate
    /// from it in either direction.
    pub randomization_factor: f64,
    /// The factor `current_interval` grows by after each attempt.
    pub multiplier: f64,
    /// The cap on `current_interval`.
    pub max_interval: Duration,
    /// When retrying started; set on creation and by [`Backoff::reset`].
    pub start_time: Instant,
    /// The total retry time after which retrying stops, or `None` to retry
    /// forever. An interval that would end past it is not returned.
    pub max_elapsed_time: Option<Duration>,
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            current_interval: INITIAL_INTERVAL,
            initial_interval: INITIAL_INTERVAL,
            randomization_factor: RANDOMIZATION_FACTOR,
            multiplier: MULTIPLIER,
            max_interval: MAX_INTERVAL,
            start_time: Instant::now(),
            max_elapsed_time: Some(MAX_ELAPSED_TIME),
        }
    }
}

impl ExponentialBackoff {
    /// Maps `random` in `[0, 1)` onto the randomized range around
    /// `current_interval`. The `+ 1.0` gives the upper bound the same chance as
    /// every other nanosecond in the range.
    fn randomized_interval(
        randomization_factor: f64,
        random: f64,
        current_interval: Duration,
    ) -> Duration {
        let current = duration_to_nanos(current_interval);
        let delta = randomization_factor * current;
        let min = current - delta;
        let max = current + delta;
        nanos_to_duration(min + random * (max - min + 1.0))
    }

    fn incremented_interval(&self) -> Duration {
        let current = duration_to_nanos(self.current_interval);
        let max = duration_to_nanos(self.max_interval);
        if current >= max / self.multiplier {
            self.max_interval
        } else {
            nanos_to_duration(current * self.multiplier)
        }
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "intervals are randomized in f64 nanoseconds; sub-nanosecond precision is irrelevant"
)]
fn duration_to_nanos(d: Duration) -> f64 {
    d.as_secs() as f64 * 1_000_000_000.0 + f64::from(d.subsec_nanos())
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "`as` saturates, so a negative interval (randomization_factor > 1) becomes zero"
)]
fn nanos_to_duration(nanos: f64) -> Duration {
    let secs = (nanos / 1_000_000_000.0) as u64;
    let subsec_nanos = (nanos as u64 % 1_000_000_000) as u32;
    Duration::new(secs, subsec_nanos)
}

impl Backoff for ExponentialBackoff {
    fn reset(&mut self) {
        self.current_interval = self.initial_interval;
        self.start_time = Instant::now();
    }

    fn next_backoff(&mut self) -> Option<Duration> {
        let elapsed = self.start_time.elapsed();
        if self.max_elapsed_time.is_some_and(|max| elapsed > max) {
            return None;
        }

        let interval = Self::randomized_interval(
            self.randomization_factor,
            rand::random::<f64>(),
            self.current_interval,
        );
        self.current_interval = self.incremented_interval();

        match self.max_elapsed_time {
            Some(max) if elapsed.saturating_add(interval) > max => None,
            _ => Some(interval),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn randomized_interval_spans_the_range_uniformly() {
        let f = ExponentialBackoff::randomized_interval;
        let two = Duration::from_nanos(2);
        // [1, 3] ns around 2 ns, each with a one-in-three chance.
        assert_eq!(Duration::from_nanos(1), f(0.5, 0.0, two));
        assert_eq!(Duration::from_nanos(1), f(0.5, 0.33, two));
        assert_eq!(Duration::from_nanos(2), f(0.5, 0.34, two));
        assert_eq!(Duration::from_nanos(2), f(0.5, 0.66, two));
        assert_eq!(Duration::from_nanos(3), f(0.5, 0.67, two));
        assert_eq!(Duration::from_nanos(3), f(0.5, 0.99, two));
    }

    #[test]
    fn randomized_interval_saturates_at_zero() {
        let f = ExponentialBackoff::randomized_interval;
        assert_eq!(Duration::ZERO, f(4.0, 0.0, Duration::from_secs(1)));
    }

    #[test]
    fn current_interval_grows_by_multiplier_up_to_max() {
        let mut backoff = ExponentialBackoff {
            initial_interval: Duration::from_millis(500),
            randomization_factor: 0.1,
            multiplier: 2.0,
            max_interval: Duration::from_secs(5),
            max_elapsed_time: Some(Duration::from_mins(16)),
            ..ExponentialBackoff::default()
        };
        backoff.reset();

        for millis in [500, 1000, 2000, 4000, 5000, 5000, 5000, 5000, 5000, 5000] {
            assert_eq!(Duration::from_millis(millis), backoff.current_interval);
            let interval = backoff
                .next_backoff()
                .expect("interval within max_elapsed_time");
            // ±10% around the current interval, give or take float rounding.
            let low =
                Duration::from_millis(millis * 9 / 10).saturating_sub(Duration::from_nanos(1));
            let high = Duration::from_millis(millis * 11 / 10) + Duration::from_nanos(1);
            assert!(
                (low..=high).contains(&interval),
                "{interval:?} outside {millis}ms ± 10%"
            );
        }
    }

    #[test]
    fn reset_restores_initial_interval() {
        let mut backoff = ExponentialBackoff::default();
        backoff.next_backoff();
        backoff.next_backoff();
        assert!(backoff.current_interval > backoff.initial_interval);

        backoff.reset();
        assert_eq!(backoff.current_interval, backoff.initial_interval);
    }

    #[test]
    fn stops_once_max_elapsed_time_has_passed() {
        let mut backoff = ExponentialBackoff {
            max_elapsed_time: Some(Duration::ZERO),
            ..ExponentialBackoff::default()
        };
        // The elapsed time itself is under test.
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(None, backoff.next_backoff());
    }

    #[test]
    fn withholds_an_interval_that_would_end_past_max_elapsed_time() {
        // Every interval around 4s is at least 2s, past the 1s budget.
        let mut backoff = ExponentialBackoff {
            max_elapsed_time: Some(Duration::from_secs(1)),
            current_interval: Duration::from_secs(4),
            ..ExponentialBackoff::default()
        };
        assert_eq!(None, backoff.next_backoff());
    }

    #[test]
    fn retries_forever_without_max_elapsed_time() {
        let mut backoff = ExponentialBackoff {
            max_elapsed_time: None,
            ..ExponentialBackoff::default()
        };
        for _ in 0..100 {
            assert!(backoff.next_backoff().is_some());
        }
        assert_eq!(backoff.current_interval, backoff.max_interval);
    }
}
