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

pub use backoff::backoff::Backoff;

/// Backoff strategy method
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffMethod {
    /// Fibonacci-based backoff intervals
    Fibonacci,
    /// Linear backoff with constant intervals
    Linear,
    /// Exponential backoff with doubling intervals
    Exponential,
}

impl std::str::FromStr for BackoffMethod {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "fibonacci" | "fib" => Ok(Self::Fibonacci),
            "linear" => Ok(Self::Linear),
            "exponential" | "exp" => Ok(Self::Exponential),
            _ => Err(format!(
                "Unknown backoff method: {s}. Valid options are: fibonacci, linear, exponential"
            )),
        }
    }
}

impl std::fmt::Display for BackoffMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fibonacci => write!(f, "fibonacci"),
            Self::Linear => write!(f, "linear"),
            Self::Exponential => write!(f, "exponential"),
        }
    }
}

/// Unified backoff strategy that supports multiple backoff methods
#[derive(Debug, Clone)]
pub struct RetryBackoff {
    pub method: BackoffMethod,
    num_retries: usize,
    pub randomization_factor: f64,
    pub max_retries: Option<usize>,
    pub max_duration: Option<Duration>,
    pub base_interval: Duration,
}

impl Default for RetryBackoff {
    fn default() -> Self {
        Self {
            method: BackoffMethod::Fibonacci,
            num_retries: 0,
            randomization_factor: 0.3,
            max_retries: None,
            max_duration: None,
            base_interval: Duration::from_millis(1000),
        }
    }
}

impl Backoff for RetryBackoff {
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

        let interval = match self.method {
            BackoffMethod::Fibonacci => self.fibonacci_interval(),
            BackoffMethod::Linear => self.linear_interval(),
            BackoffMethod::Exponential => self.exponential_interval(),
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

impl RetryBackoff {
    #[must_use]
    pub fn new(method: BackoffMethod) -> Self {
        Self {
            method,
            ..Default::default()
        }
    }

    pub fn next_duration(&mut self) -> Option<Duration> {
        self.next_backoff()
    }

    fn fibonacci_interval(&self) -> Duration {
        // Fibonacci-based backoff delay intervals capped at 5 mins
        const BACKOFF_INTERVALS_MS: [u64; 14] = [
            1000, 1000, 2000, 3000, 5000, 8000, 13000, 21000, 34000, 55000, 89000, 144_000,
            233_000, 300_000,
        ];

        if self.num_retries >= BACKOFF_INTERVALS_MS.len() {
            Duration::from_millis(BACKOFF_INTERVALS_MS[BACKOFF_INTERVALS_MS.len() - 1])
        } else {
            Duration::from_millis(BACKOFF_INTERVALS_MS[self.num_retries])
        }
    }

    fn linear_interval(&self) -> Duration {
        // Linear backoff: constant interval each time
        // Capped at 5 minutes
        #[allow(clippy::cast_possible_truncation)]
        let interval_ms = self.base_interval.as_millis() as u64;
        let max_interval_ms = 300_000; // 5 minutes
        Duration::from_millis(interval_ms.min(max_interval_ms))
    }

    fn exponential_interval(&self) -> Duration {
        // Exponential backoff: base_interval * 2^retry_count
        // Capped at 5 minutes
        #[allow(clippy::cast_possible_truncation)]
        let base_ms = self.base_interval.as_millis() as u64;
        let max_interval_ms = 300_000; // 5 minutes

        #[allow(clippy::cast_possible_truncation)]
        let multiplier = 2_u64.saturating_pow(self.num_retries.saturating_sub(1) as u32);
        let interval_ms = base_ms.saturating_mul(multiplier);

        Duration::from_millis(interval_ms.min(max_interval_ms))
    }
}

#[derive(Debug, Clone)]
pub struct RetryBackoffBuilder {
    method: BackoffMethod,
    randomization_factor: f64,
    max_retries: Option<usize>,
    max_duration: Option<Duration>,
    base_interval: Duration,
}

impl Default for RetryBackoffBuilder {
    fn default() -> Self {
        Self {
            method: BackoffMethod::Fibonacci,
            randomization_factor: 0.3,
            max_retries: None,
            max_duration: None,
            base_interval: Duration::from_millis(1000),
        }
    }
}

impl RetryBackoffBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn method(mut self, method: BackoffMethod) -> Self {
        self.method = method;
        self
    }

    #[must_use]
    pub fn randomization_factor(mut self, randomization_factor: f64) -> Self {
        self.randomization_factor = randomization_factor;
        self
    }

    #[must_use]
    pub fn max_retries(mut self, max_retries: Option<usize>) -> Self {
        self.max_retries = max_retries;
        self
    }

    #[must_use]
    pub fn max_duration(mut self, max_duration: Option<Duration>) -> Self {
        self.max_duration = max_duration;
        self
    }

    #[must_use]
    pub fn base_interval(mut self, base_interval: Duration) -> Self {
        self.base_interval = base_interval;
        self
    }

    #[must_use]
    pub fn build(self) -> RetryBackoff {
        RetryBackoff {
            method: self.method,
            num_retries: 0,
            randomization_factor: self.randomization_factor,
            max_retries: self.max_retries,
            max_duration: self.max_duration,
            base_interval: self.base_interval,
        }
    }
}

fn get_random_value_from_interval(
    randomization_factor: f64,
    random: f64,
    current_interval: Duration,
) -> Duration {
    // Avoid floating-point operations when there's no randomization
    if randomization_factor == 0.0 {
        return current_interval;
    }

    let current_interval_nanos = duration_to_nanos(current_interval);

    let delta = randomization_factor * current_interval_nanos;
    let min_interval = current_interval_nanos - delta;
    let max_interval = current_interval_nanos + delta;
    let diff = max_interval - min_interval;

    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    let nanos = min_interval + (random * diff);

    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    Duration::from_nanos(nanos as u64)
}

#[allow(clippy::cast_precision_loss)]
fn duration_to_nanos(duration: Duration) -> f64 {
    (duration.as_secs() as f64) * 1e9 + f64::from(duration.subsec_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_method_from_str() {
        assert_eq!(
            "fibonacci".parse::<BackoffMethod>().ok(),
            Some(BackoffMethod::Fibonacci)
        );
        assert_eq!(
            "fib".parse::<BackoffMethod>().ok(),
            Some(BackoffMethod::Fibonacci)
        );
        assert_eq!(
            "linear".parse::<BackoffMethod>().ok(),
            Some(BackoffMethod::Linear)
        );
        assert_eq!(
            "exponential".parse::<BackoffMethod>().ok(),
            Some(BackoffMethod::Exponential)
        );
        assert_eq!(
            "exp".parse::<BackoffMethod>().ok(),
            Some(BackoffMethod::Exponential)
        );
        assert!("invalid".parse::<BackoffMethod>().is_err());
    }

    #[test]
    fn test_fibonacci_backoff() {
        let mut backoff = RetryBackoffBuilder::new()
            .method(BackoffMethod::Fibonacci)
            .randomization_factor(0.0) // No randomization for predictable testing
            .max_retries(Some(5))
            .build();

        // With no randomization, intervals should be exact values from the Fibonacci sequence
        // The BACKOFF_INTERVALS_MS array is: [1000, 1000, 2000, 3000, 5000, 8000, ...]
        // num_retries starts at 0, increments before indexing, so:
        // - First call: num_retries becomes 1, returns BACKOFF_INTERVALS_MS[1] = 1000ms
        // - Second call: num_retries becomes 2, returns BACKOFF_INTERVALS_MS[2] = 2000ms
        // - Third call: num_retries becomes 3, returns BACKOFF_INTERVALS_MS[3] = 3000ms
        assert_eq!(
            backoff.next_backoff(),
            Some(Duration::from_millis(1000)),
            "First interval should be exactly 1000ms"
        );

        assert_eq!(
            backoff.next_backoff(),
            Some(Duration::from_millis(2000)),
            "Second interval should be exactly 2000ms"
        );

        assert_eq!(
            backoff.next_backoff(),
            Some(Duration::from_millis(3000)),
            "Third interval should be exactly 3000ms"
        );

        assert_eq!(
            backoff.next_backoff(),
            Some(Duration::from_millis(5000)),
            "Fourth interval should be exactly 5000ms"
        );

        assert_eq!(
            backoff.next_backoff(),
            Some(Duration::from_millis(8000)),
            "Fifth interval should be exactly 8000ms"
        );

        // After max_retries, should return None
        assert_eq!(
            backoff.next_backoff(),
            None,
            "Should return None after max retries"
        );
    }

    #[test]
    fn test_linear_backoff() {
        let mut backoff = RetryBackoffBuilder::new()
            .method(BackoffMethod::Linear)
            .randomization_factor(0.0)
            .base_interval(Duration::from_millis(2000))
            .max_retries(Some(5))
            .build();

        // All intervals should be around 2000ms
        for _ in 0..5 {
            let interval = backoff.next_backoff().expect("backoff should exist");
            assert!(
                interval >= Duration::from_millis(1900) && interval <= Duration::from_millis(2100)
            );
        }
    }

    #[test]
    fn test_exponential_backoff() {
        let mut backoff = RetryBackoffBuilder::new()
            .method(BackoffMethod::Exponential)
            .randomization_factor(0.0)
            .base_interval(Duration::from_millis(1000))
            .max_retries(Some(5))
            .build();

        // First: 1000ms
        let interval = backoff.next_backoff().expect("backoff should exist");
        assert!(interval >= Duration::from_millis(900) && interval <= Duration::from_millis(1100));

        // Second: 2000ms
        let interval = backoff.next_backoff().expect("backoff should exist");
        assert!(interval >= Duration::from_millis(1900) && interval <= Duration::from_millis(2100));

        // Third: 4000ms
        let interval = backoff.next_backoff().expect("backoff should exist");
        assert!(interval >= Duration::from_millis(3900) && interval <= Duration::from_millis(4100));

        // Fourth: 8000ms
        let interval = backoff.next_backoff().expect("backoff should exist");
        assert!(interval >= Duration::from_millis(7900) && interval <= Duration::from_millis(8100));
    }

    #[test]
    fn test_max_retries() {
        let mut backoff = RetryBackoffBuilder::new()
            .method(BackoffMethod::Linear)
            .max_retries(Some(3))
            .build();

        assert!(backoff.next_backoff().is_some());
        assert!(backoff.next_backoff().is_some());
        assert!(backoff.next_backoff().is_some());
        assert!(backoff.next_backoff().is_none());
    }
}
