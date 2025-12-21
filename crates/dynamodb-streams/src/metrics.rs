/*
Copyright 2025 The Spice.ai OSS Authors

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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

#[derive(Debug, Default)]
pub struct MetricsCollector {
    /// Number of currently active shards (gauge)
    pub active_shards_number: RwLock<usize>,
    /// Total number of records produced (counter)
    pub records: AtomicUsize,
    /// Total number of transient errors encountered (counter)
    pub transient_errors: AtomicUsize,
    /// Latest watermark from stream batches
    pub watermark: RwLock<Option<SystemTime>>,
}

#[derive(Debug)]
pub struct Metrics {
    metrics_collector: Arc<MetricsCollector>,
}

impl Metrics {
    pub fn new(metrics_collector: Arc<MetricsCollector>) -> Self {
        Self { metrics_collector }
    }

    /// Number of currently active shards (gauge)
    #[must_use]
    pub fn active_shards_number(&self) -> usize {
        self.metrics_collector
            .active_shards_number
            .read()
            .map_or_else(
                |poisoned| {
                    // Recover from poisoned lock - the data is still valid
                    tracing::warn!("RwLock was poisoned, recovering data");
                    *poisoned.into_inner()
                },
                |guard| *guard,
            )
    }

    /// Total number of records produced (counter)
    #[must_use]
    pub fn records(&self) -> usize {
        self.metrics_collector.records.load(Ordering::Relaxed)
    }

    /// Total lag in milliseconds (`now()` - stream watermark)
    /// Returns None if no watermark is available yet
    #[must_use]
    pub fn total_lag_ms(&self) -> Option<u64> {
        let watermark = self.metrics_collector.watermark.read().map_or_else(
            |poisoned| {
                // Recover from poisoned lock - the data is still valid
                tracing::warn!("RwLock was poisoned, recovering data");
                *poisoned.into_inner()
            },
            |guard| *guard,
        )?;
        SystemTime::now()
            .duration_since(watermark)
            .ok()
            .and_then(|d| u64::try_from(d.as_millis()).ok())
    }

    /// Total number of transient errors encountered (counter)
    #[must_use]
    pub fn transient_errors(&self) -> usize {
        self.metrics_collector
            .transient_errors
            .load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_metrics_collector_default() {
        let collector = MetricsCollector::default();

        assert_eq!(*collector.active_shards_number.read().expect("read"), 0);
        assert_eq!(collector.records.load(Ordering::Relaxed), 0);
        assert_eq!(collector.transient_errors.load(Ordering::Relaxed), 0);
        assert!(collector.watermark.read().expect("read").is_none());
    }

    #[test]
    fn test_metrics_active_shards() {
        let collector = Arc::new(MetricsCollector::default());

        {
            let mut guard = collector.active_shards_number.write().expect("write");
            *guard = 5;
        }

        let metrics = Metrics::new(Arc::clone(&collector));
        assert_eq!(metrics.active_shards_number(), 5);
    }

    #[test]
    fn test_metrics_records_counter() {
        let collector = Arc::new(MetricsCollector::default());

        collector.records.fetch_add(100, Ordering::Relaxed);
        collector.records.fetch_add(50, Ordering::Relaxed);

        let metrics = Metrics::new(Arc::clone(&collector));
        assert_eq!(metrics.records(), 150);
    }

    #[test]
    fn test_metrics_transient_errors_counter() {
        let collector = Arc::new(MetricsCollector::default());

        collector.transient_errors.fetch_add(3, Ordering::Relaxed);
        collector.transient_errors.fetch_add(2, Ordering::Relaxed);

        let metrics = Metrics::new(Arc::clone(&collector));
        assert_eq!(metrics.transient_errors(), 5);
    }

    #[test]
    fn test_metrics_lag_none_when_no_watermark() {
        let collector = Arc::new(MetricsCollector::default());
        let metrics = Metrics::new(Arc::clone(&collector));

        assert!(metrics.total_lag_ms().is_none());
    }

    #[test]
    fn test_metrics_lag_calculates_from_watermark() {
        let collector = Arc::new(MetricsCollector::default());

        // Set watermark to 1 second ago
        let one_second_ago = SystemTime::now() - Duration::from_secs(1);
        {
            let mut guard = collector.watermark.write().expect("write");
            *guard = Some(one_second_ago);
        }

        let metrics = Metrics::new(Arc::clone(&collector));
        let lag_ms = metrics.total_lag_ms().expect("should have lag");

        // Lag should be approximately 1000ms (give or take some margin for test execution)
        assert!(
            lag_ms >= 1000,
            "lag should be at least 1000ms, got {lag_ms}"
        );
        assert!(
            lag_ms < 2000,
            "lag should be less than 2000ms, got {lag_ms}"
        );
    }

    #[test]
    fn test_metrics_lag_zero_for_recent_watermark() {
        let collector = Arc::new(MetricsCollector::default());

        // Set watermark to now
        {
            let mut guard = collector.watermark.write().expect("write");
            *guard = Some(SystemTime::now());
        }

        let metrics = Metrics::new(Arc::clone(&collector));
        let lag_ms = metrics.total_lag_ms().expect("should have lag");

        // Lag should be very small (< 100ms for test execution)
        assert!(lag_ms < 100, "lag should be less than 100ms, got {lag_ms}");
    }

    #[test]
    fn test_metrics_shared_collector() {
        let collector = Arc::new(MetricsCollector::default());

        let metrics1 = Metrics::new(Arc::clone(&collector));
        let metrics2 = Metrics::new(Arc::clone(&collector));

        // Update through collector
        collector.records.fetch_add(42, Ordering::Relaxed);

        // Both metrics instances should see the update
        assert_eq!(metrics1.records(), 42);
        assert_eq!(metrics2.records(), 42);
    }

    #[test]
    fn test_metrics_collector_concurrent_updates() {
        let collector = Arc::new(MetricsCollector::default());
        let collector_clone = Arc::clone(&collector);

        // Simulate concurrent updates
        std::thread::scope(|s| {
            s.spawn(|| {
                for _ in 0..100 {
                    collector_clone.records.fetch_add(1, Ordering::Relaxed);
                }
            });

            for _ in 0..100 {
                collector.records.fetch_add(1, Ordering::Relaxed);
            }
        });

        let metrics = Metrics::new(Arc::clone(&collector));
        assert_eq!(metrics.records(), 200);
    }
}

/// Tests that verify the correct handling of poisoned `RwLock`s.
/// The implementation recovers from poisoned locks by using `into_inner()`.
#[cfg(test)]
mod poisoned_lock_tests {
    use super::*;
    use std::time::Duration;

    /// Verifies that `active_shards_number()` recovers data from a poisoned `RwLock`.
    ///
    /// When the `RwLock` is poisoned (which can happen if a thread panics while holding it),
    /// `active_shards_number()` should still return the correct value by recovering
    /// the data using `into_inner()`.
    #[test]
    fn test_active_shards_number_recovers_from_poisoned_lock() {
        let collector = Arc::new(MetricsCollector::default());

        // Set a non-zero value
        {
            let mut guard = collector.active_shards_number.write().expect("write");
            *guard = 42;
        }

        // Poison the lock by panicking in a thread while holding write lock
        let collector_clone = Arc::clone(&collector);
        let result = std::thread::spawn(move || {
            let _guard = collector_clone.active_shards_number.write().expect("write");
            panic!("Intentional panic to poison lock");
        })
        .join();

        // Thread should have panicked
        assert!(result.is_err(), "Thread should have panicked");

        let metrics = Metrics::new(Arc::clone(&collector));

        // The implementation recovers from poisoned locks using into_inner()
        // so we should still get the correct value (42)
        assert_eq!(
            metrics.active_shards_number(),
            42,
            "Should recover value from poisoned lock"
        );
    }

    /// Verifies that `total_lag_ms()` recovers data from a poisoned `RwLock`.
    ///
    /// When the `RwLock` is poisoned, `total_lag_ms()` should still return the
    /// correct lag value by recovering the watermark using `into_inner()`.
    #[test]
    fn test_total_lag_recovers_from_poisoned_lock() {
        let collector = Arc::new(MetricsCollector::default());

        // Set a watermark
        {
            let mut guard = collector.watermark.write().expect("write");
            *guard = Some(SystemTime::now() - Duration::from_secs(5));
        }

        // Poison the lock
        let collector_clone = Arc::clone(&collector);
        let result = std::thread::spawn(move || {
            let _guard = collector_clone.watermark.write().expect("write");
            panic!("Intentional panic to poison lock");
        })
        .join();

        assert!(result.is_err(), "Thread should have panicked");

        let metrics = Metrics::new(Arc::clone(&collector));

        // The implementation recovers from poisoned locks using into_inner()
        // so we should still get a valid lag value
        let lag = metrics.total_lag_ms();
        assert!(lag.is_some(), "Should recover watermark from poisoned lock");
        // Lag should be at least 5 seconds (5000ms)
        assert!(lag.unwrap_or(0) >= 5000, "Lag should be at least 5000ms");
    }
}
