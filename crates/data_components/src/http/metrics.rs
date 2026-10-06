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

//! Occupancy counters for the HTTP connector's response cache.
//!
//! The cache is not one of the caches under `runtime.caching`, so it had no
//! instrumentation at all: whatever it held showed up only as process memory
//! with nothing to attribute it to, and an operator watching cache gauges would
//! have seen nothing however large it grew.
//!
//! Only the counters live here. Publishing them is the connector's job, because
//! that is where the dataset a cache belongs to is known — these are reported
//! per dataset, and one shared registry of gauges could not say which dataset a
//! figure came from.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Names of the metrics reported from [`HttpCacheMetrics`].
///
/// Kept beside the counters so a connector registering them cannot drift from
/// what this module actually tracks.
pub mod names {
    /// Bytes retained, response bodies and their request keys together.
    pub const RESPONSE_CACHE_SIZE_BYTES: &str = "response_cache_size_bytes";
    /// Responses currently held. Read beside the byte figure this says whether
    /// the cache holds a few large responses or very many small ones, which is
    /// what distinguishes a payload-bound cache from a cardinality-bound one.
    pub const RESPONSE_CACHE_ITEMS_COUNT: &str = "response_cache_items_count";
}

/// Live occupancy of one dataset's response cache.
///
/// Shared between the table provider that owns the cache and the metrics
/// provider that reports it, so reporting never has to reach into the cache
/// itself.
#[derive(Debug, Default)]
pub struct HttpCacheMetrics {
    retained_bytes: AtomicU64,
    items: AtomicU64,
}

impl HttpCacheMetrics {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records the cache's current occupancy.
    ///
    /// These are the cache implementation's own figures rather than a tally
    /// maintained here, so they can lag a write by one housekeeping cycle. They
    /// are also refreshed only when a request consults the cache, so on an idle
    /// dataset they report the last observed occupancy rather than a live one —
    /// which is the same thing, since nothing enters or leaves the cache except
    /// on a request.
    pub fn record(&self, retained_bytes: u64, items: u64) {
        self.retained_bytes.store(retained_bytes, Ordering::Relaxed);
        self.items.store(items, Ordering::Relaxed);
    }

    #[must_use]
    pub fn retained_bytes(&self) -> u64 {
        self.retained_bytes.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn items(&self) -> u64 {
        self.items.load(Ordering::Relaxed)
    }
}
