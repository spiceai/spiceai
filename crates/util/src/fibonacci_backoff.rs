/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::time::Duration;

use backoff::backoff::Backoff;

// Fibonacci-based backoff delay intervals capped at 5 mins
const BACKOFF_INTERVALS_MS: [u64; 14] = [
    1000, 1000, 2000, 3000, 5000, 8000, 13000, 21000, 34000, 55000, 89000, 144_000, 233_000,
    300_000,
];

#[derive(Debug)]
pub struct FibonacciBackoff {
    num_retries: usize,
    pub randomization_factor: f64,
    pub max_retries: Option<usize>,
}

impl Default for FibonacciBackoff {
    fn default() -> FibonacciBackoff {
        FibonacciBackoff {
            num_retries: 0,
            randomization_factor: 0.3,
            max_retries: None,
        }
    }
}

impl Backoff for FibonacciBackoff {
    fn reset(&mut self) {
        self.num_retries = 0;
    }

    fn next_backoff(&mut self) -> Option<Duration> {
        self.num_retries += 1;

        if let Some(max_retries) = self.max_retries {
            if self.num_retries > max_retries {
                return None;
            }
        }

        let interval = if self.num_retries >= BACKOFF_INTERVALS_MS.len() {
            Duration::from_millis(BACKOFF_INTERVALS_MS[BACKOFF_INTERVALS_MS.len() - 1])
        } else {
            Duration::from_millis(BACKOFF_INTERVALS_MS[self.num_retries])
        };

        let randomized_interval = get_random_value_from_interval(
            self.randomization_factor,
            rand::random::<f64>(),
            interval,
        );

        Some(randomized_interval)
    }
}

fn get_random_value_from_interval(
    randomization_factor: f64,
    random: f64,
    current_interval: Duration,
) -> Duration {
    let current_interval_nanos = duration_to_nanos(current_interval);

    let delta = randomization_factor * current_interval_nanos;
    let min_interval = current_interval_nanos - delta;
    let max_interval = current_interval_nanos + delta;
    // Get a random value from the range [minInterval, maxInterval].
    // The formula used below has a +1 because if the minInterval is 1 and the maxInterval is 3 then
    // we want a 33% chance for selecting either 1, 2 or 3.
    let diff = max_interval - min_interval;
    let nanos = min_interval + (random * (diff + 1.0));
    nanos_to_duration(nanos)
}

pub struct FibonacciBackoffBuilder {
    randomization_factor: f64,
    max_retries: Option<usize>,
}

impl FibonacciBackoffBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            randomization_factor: 0.3,
            max_retries: None,
        }
    }

    #[must_use]
    pub fn randomization_factor(mut self, value: f64) -> Self {
        self.randomization_factor = value;
        self
    }

    /// Set the maximum number of retries. None means no limit.
    #[must_use]
    pub fn max_retries(mut self, value: Option<usize>) -> Self {
        self.max_retries = value;
        self
    }

    #[must_use]
    pub fn build(self) -> FibonacciBackoff {
        FibonacciBackoff {
            randomization_factor: self.randomization_factor,
            max_retries: self.max_retries,
            ..FibonacciBackoff::default()
        }
    }
}

impl Default for FibonacciBackoffBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::cast_precision_loss)]
fn duration_to_nanos(d: Duration) -> f64 {
    d.as_secs() as f64 * 1_000_000_000.0 + f64::from(d.subsec_nanos())
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
fn nanos_to_duration(nanos: f64) -> Duration {
    let secs = nanos / 1_000_000_000.0;
    let nanos = nanos as u64 % 1_000_000_000;
    Duration::new(secs as u64, nanos as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_interval_within_range() {
        let tests = vec![
            (1000, 900, 1100),
            (13000, 11700, 14300),
            (300_000, 270_000, 330_000),
        ];

        for (duration_ms, expected_min_ms, expected_max_ms) in tests {
            let target_duration = Duration::from_millis(duration_ms);
            let expected_min = Duration::from_millis(expected_min_ms);
            let expected_max = Duration::from_millis(expected_max_ms);
            let result =
                get_random_value_from_interval(0.1, rand::random::<f64>(), target_duration);

            assert!(
                result >= expected_min && result <= expected_max,
                "Result out of range for target duration {target_duration:?}: got {result:?}, expected between {expected_min:?} and {expected_max:?}"
            );
        }
    }

    #[test]
    fn test_zero_randomization_factor() {
        let interval = Duration::from_millis(1000);
        let factor = 0.0;
        let random = 0.5;
        let result = get_random_value_from_interval(factor, random, interval);

        assert_eq!(
            result, interval,
            "Randomization factor of 0 should return the original interval"
        );
    }

    #[test]
    fn test_max_retries() {
        let mut backoff = FibonacciBackoffBuilder::new().max_retries(Some(5)).build();

        for _ in 0..5 {
            assert!(backoff.next_backoff().is_some());
        }

        assert!(backoff.next_backoff().is_none());
    }

    #[test]
    fn test_zero_max_retries() {
        let mut backoff = FibonacciBackoffBuilder::new().max_retries(Some(0)).build();

        assert!(backoff.next_backoff().is_none());
    }

    #[test]
    fn test_undefined_max_retries() {
        let mut backoff = FibonacciBackoffBuilder::new().max_retries(None).build();

        for _ in 0..100 {
            assert!(backoff.next_backoff().is_some());
        }
    }
}
