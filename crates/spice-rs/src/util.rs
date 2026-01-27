// Use Fibonacci backoff util from spiceai: https://github.com/spiceai/spiceai/tree/trunk/crates/util/src
pub use backoff::Error as RetryError;
pub use backoff::future::retry;

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
    pub max_duration: Option<Duration>,
}

impl Default for FibonacciBackoff {
    fn default() -> FibonacciBackoff {
        FibonacciBackoff {
            num_retries: 0,
            randomization_factor: 0.3,
            max_retries: None,
            max_duration: None,
        }
    }
}

impl Backoff for FibonacciBackoff {
    fn reset(&mut self) {
        self.num_retries = 0;
    }

    fn next_backoff(&mut self) -> Option<Duration> {
        self.num_retries += 1;

        if let Some(max_retries) = self.max_retries
            && self.num_retries > max_retries
        {
            return None;
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

        let final_interval = if let Some(max_duration) = self.max_duration {
            if randomized_interval > max_duration {
                max_duration
            } else {
                randomized_interval
            }
        } else {
            randomized_interval
        };

        Some(final_interval)
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
    max_duration: Option<Duration>,
}

impl FibonacciBackoffBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            randomization_factor: 0.3,
            max_retries: None,
            max_duration: None,
        }
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
            max_duration: self.max_duration,
            ..FibonacciBackoff::default()
        }
    }
}

impl Default for FibonacciBackoffBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[expect(clippy::cast_precision_loss)]
fn duration_to_nanos(d: Duration) -> f64 {
    d.as_secs() as f64 * 1_000_000_000.0 + f64::from(d.subsec_nanos())
}

#[expect(clippy::cast_possible_truncation)]
#[expect(clippy::cast_sign_loss)]
fn nanos_to_duration(nanos: f64) -> Duration {
    let secs = nanos / 1_000_000_000.0;
    let nanos = nanos as u64 % 1_000_000_000;
    Duration::new(secs as u64, nanos as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fibonacci_backoff_default() {
        let backoff = FibonacciBackoff::default();
        assert_eq!(backoff.num_retries, 0);
        assert!((backoff.randomization_factor - 0.3).abs() < f64::EPSILON);
        assert!(backoff.max_retries.is_none());
        assert!(backoff.max_duration.is_none());
    }

    #[test]
    fn test_fibonacci_backoff_reset() {
        let mut backoff = FibonacciBackoff::default();
        // Advance some retries
        backoff.next_backoff();
        backoff.next_backoff();
        assert_eq!(backoff.num_retries, 2);

        // Reset should set num_retries back to 0
        backoff.reset();
        assert_eq!(backoff.num_retries, 0);
    }

    #[test]
    fn test_fibonacci_backoff_next_returns_some() {
        let mut backoff = FibonacciBackoff::default();

        // First several calls should return Some
        for _ in 0..10 {
            let duration = backoff.next_backoff();
            assert!(duration.is_some());
        }
    }

    #[test]
    fn test_fibonacci_backoff_max_retries() {
        let mut backoff = FibonacciBackoff {
            max_retries: Some(3),
            ..FibonacciBackoff::default()
        };

        // First 3 should succeed
        assert!(backoff.next_backoff().is_some());
        assert!(backoff.next_backoff().is_some());
        assert!(backoff.next_backoff().is_some());

        // 4th should fail (exceeds max_retries)
        assert!(backoff.next_backoff().is_none());
    }

    #[test]
    fn test_fibonacci_backoff_max_duration() {
        let max_dur = Duration::from_millis(500);
        let mut backoff = FibonacciBackoff {
            max_duration: Some(max_dur),
            randomization_factor: 0.0, // No randomization for predictable testing
            ..FibonacciBackoff::default()
        };

        // All durations should be capped at max_duration
        for _ in 0..14 {
            let duration = backoff.next_backoff();
            assert!(duration.is_some());
            assert!(duration.expect("should have duration") <= max_dur);
        }
    }

    #[test]
    fn test_fibonacci_backoff_intervals_increase() {
        let mut backoff = FibonacciBackoff {
            randomization_factor: 0.0, // No randomization for predictable testing
            ..FibonacciBackoff::default()
        };

        let first = backoff.next_backoff().expect("should have duration");
        let second = backoff.next_backoff().expect("should have duration");

        // Intervals should match the Fibonacci sequence (1s, 1s, 2s, 3s, ...)
        assert_eq!(first, Duration::from_millis(1000));
        assert_eq!(second, Duration::from_millis(2000));
    }

    #[test]
    fn test_fibonacci_backoff_caps_at_max_interval() {
        let mut backoff = FibonacciBackoff {
            randomization_factor: 0.0,
            num_retries: 20, // Beyond the BACKOFF_INTERVALS_MS array
            ..FibonacciBackoff::default()
        };

        let duration = backoff.next_backoff().expect("should have duration");
        // Should be capped at the last value in BACKOFF_INTERVALS_MS (300_000 ms = 5 mins)
        assert_eq!(duration, Duration::from_millis(300_000));
    }

    #[test]
    fn test_fibonacci_backoff_builder_default() {
        let builder = FibonacciBackoffBuilder::default();
        let backoff = builder.build();

        assert_eq!(backoff.num_retries, 0);
        assert!((backoff.randomization_factor - 0.3).abs() < f64::EPSILON);
        assert!(backoff.max_retries.is_none());
    }

    #[test]
    fn test_fibonacci_backoff_builder_max_retries() {
        let backoff = FibonacciBackoffBuilder::new().max_retries(Some(5)).build();

        assert_eq!(backoff.max_retries, Some(5));
    }

    #[test]
    fn test_fibonacci_backoff_builder_chaining() {
        let backoff = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();

        assert_eq!(backoff.max_retries, Some(10));
    }

    #[test]
    fn test_duration_to_nanos() {
        let dur = Duration::new(1, 500_000_000); // 1.5 seconds
        let nanos = duration_to_nanos(dur);
        assert!((nanos - 1_500_000_000.0).abs() < 1.0);
    }

    #[test]
    fn test_nanos_to_duration() {
        let nanos = 1_500_000_000.0; // 1.5 seconds
        let dur = nanos_to_duration(nanos);
        assert_eq!(dur.as_secs(), 1);
        assert!(dur.subsec_nanos() >= 499_000_000 && dur.subsec_nanos() <= 501_000_000);
    }

    #[test]
    fn test_duration_roundtrip() {
        let original = Duration::new(2, 250_000_000);
        let nanos = duration_to_nanos(original);
        let restored = nanos_to_duration(nanos);

        // Should be very close (within a few nanoseconds due to floating point)
        let diff = original.abs_diff(restored);
        assert!(diff < Duration::from_nanos(1000));
    }

    #[test]
    fn test_get_random_value_from_interval_zero_factor() {
        let interval = Duration::from_secs(1);
        let result = get_random_value_from_interval(0.0, 0.5, interval);

        // With zero randomization, should return the original interval
        assert_eq!(result, interval);
    }

    #[test]
    fn test_get_random_value_from_interval_bounds() {
        let interval = Duration::from_secs(10);
        let factor = 0.5; // 50% randomization

        // With random = 0, should get min bound
        let min_result = get_random_value_from_interval(factor, 0.0, interval);
        // Min should be around 5 seconds (10 - 50%)
        assert!(min_result >= Duration::from_secs(4) && min_result <= Duration::from_secs(6));

        // With random = 1, should get max bound
        let max_result = get_random_value_from_interval(factor, 1.0, interval);
        // Max should be around 15 seconds (10 + 50%)
        assert!(max_result >= Duration::from_secs(14) && max_result <= Duration::from_secs(16));
    }

    // Edge case tests

    #[test]
    fn test_fibonacci_backoff_max_retries_zero() {
        let mut backoff = FibonacciBackoff {
            max_retries: Some(0),
            ..FibonacciBackoff::default()
        };

        // With max_retries = 0, first call should return None
        assert!(backoff.next_backoff().is_none());
    }

    #[test]
    fn test_fibonacci_backoff_max_retries_one() {
        let mut backoff = FibonacciBackoff {
            max_retries: Some(1),
            ..FibonacciBackoff::default()
        };

        // First should succeed
        assert!(backoff.next_backoff().is_some());
        // Second should fail
        assert!(backoff.next_backoff().is_none());
    }

    #[test]
    fn test_fibonacci_backoff_max_duration_zero() {
        let max_dur = Duration::ZERO;
        let mut backoff = FibonacciBackoff {
            max_duration: Some(max_dur),
            randomization_factor: 0.0,
            ..FibonacciBackoff::default()
        };

        let duration = backoff.next_backoff();
        assert!(matches!(duration, Some(d) if d == Duration::ZERO));
    }

    #[test]
    fn test_fibonacci_backoff_reset_after_max_retries() {
        let mut backoff = FibonacciBackoff {
            max_retries: Some(2),
            ..FibonacciBackoff::default()
        };

        assert!(backoff.next_backoff().is_some());
        assert!(backoff.next_backoff().is_some());
        assert!(backoff.next_backoff().is_none());

        // After reset, should work again
        backoff.reset();
        assert!(backoff.next_backoff().is_some());
    }

    #[test]
    fn test_duration_to_nanos_zero() {
        let dur = Duration::ZERO;
        let nanos = duration_to_nanos(dur);
        assert!((nanos - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_nanos_to_duration_zero() {
        let nanos = 0.0;
        let dur = nanos_to_duration(nanos);
        assert_eq!(dur, Duration::ZERO);
    }

    #[test]
    fn test_duration_to_nanos_large_value() {
        // Test with a very large duration (1 year in seconds)
        let dur = Duration::from_secs(365 * 24 * 60 * 60);
        let nanos = duration_to_nanos(dur);
        let expected = 365.0 * 24.0 * 60.0 * 60.0 * 1_000_000_000.0;
        assert!((nanos - expected).abs() < 1_000_000.0); // Allow small floating point error
    }

    #[test]
    fn test_get_random_value_from_interval_zero_interval() {
        let interval = Duration::ZERO;
        let result = get_random_value_from_interval(0.5, 0.5, interval);
        // With zero interval, result should be zero or very small
        assert!(result <= Duration::from_nanos(100));
    }

    #[test]
    fn test_fibonacci_backoff_builder_none_max_retries() {
        let backoff = FibonacciBackoffBuilder::new().max_retries(None).build();
        assert!(backoff.max_retries.is_none());
    }

    #[test]
    fn test_fibonacci_backoff_many_retries_beyond_array() {
        let mut backoff = FibonacciBackoff {
            randomization_factor: 0.0,
            ..FibonacciBackoff::default()
        };

        // Exhaust all the intervals and then some
        for _ in 0..20 {
            let _ = backoff.next_backoff();
        }

        // Should still return the max interval
        let duration = backoff.next_backoff();
        assert!(matches!(duration, Some(d) if d == Duration::from_millis(300_000)));
    }

    #[test]
    fn test_get_random_value_from_interval_full_randomization() {
        let interval = Duration::from_secs(10);
        let factor = 1.0; // 100% randomization

        // With random = 0, should get 0 (10 - 100%)
        let min_result = get_random_value_from_interval(factor, 0.0, interval);
        assert!(min_result <= Duration::from_secs(1));

        // With random = 1, should get 20 (10 + 100%)
        let max_result = get_random_value_from_interval(factor, 1.0, interval);
        assert!(max_result >= Duration::from_secs(19) && max_result <= Duration::from_secs(21));
    }
}
