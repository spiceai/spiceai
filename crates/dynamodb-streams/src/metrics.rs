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
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
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
    pub fn active_shards_number(&self) -> usize {
        self.metrics_collector
            .active_shards_number
            .read()
            .map(|guard| *guard)
            .unwrap_or(0)
    }

    /// Total number of records produced (counter)
    pub fn records(&self) -> usize {
        self.metrics_collector.records.load(Ordering::Relaxed)
    }

    /// Total lag in milliseconds (now() - stream watermark)
    /// Returns None if no watermark is available yet
    pub fn total_lag_ms(&self) -> Option<u64> {
        let watermark = self.metrics_collector.watermark.read().ok()?;
        watermark.and_then(|wm| {
            SystemTime::now()
                .duration_since(wm)
                .ok()
                .map(|d| d.as_millis() as u64)
        })
    }

    /// Total number of transient errors encountered (counter)
    pub fn transient_errors(&self) -> usize {
        self.metrics_collector.transient_errors.load(Ordering::Relaxed)
    }
}
