// Use Fibonacci backoff util from spiceai: https://github.com/spiceai/spiceai/tree/trunk/crates/util/src
pub use backoff::future::retry;
pub use backoff::Error as RetryError;

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
