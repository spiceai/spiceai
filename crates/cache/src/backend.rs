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

use async_trait::async_trait;
use std::time::Duration;

pub mod moka;
pub mod pingora;

pub use self::moka::MokaBackend;
pub use self::pingora::PingoraBackend;

/// Trait abstraction for cache backend implementations (Moka, Pingora-LRU, etc.)
///
/// This trait allows runtime selection between different cache engines via configuration.
/// Implementations must be thread-safe and support TTL-based eviction.
#[async_trait]
pub trait CacheBackend<V>: Send + Sync {
    /// Insert a value into the cache with the given key and size
    async fn insert(&self, key: u64, value: V, size: usize);

    /// Get a value from the cache by key
    /// Returns None if key doesn't exist or value has expired
    async fn get(&self, key: &u64) -> Option<V>;

    /// Remove a value from the cache by key
    async fn remove(&self, key: &u64);

    /// Clear all entries from the cache
    async fn clear(&self);

    /// Iterate over all valid (non-expired) keys in the cache
    ///
    /// Note: For some backends (e.g., pingora-lru), this may have race conditions
    /// where a key is returned but the value has expired by the time it's accessed.
    async fn iter_keys(&self) -> Vec<u64>;

    /// Get the number of entries in the cache
    ///
    /// Note: This may include expired entries that haven't been evicted yet.
    async fn len(&self) -> usize;

    /// Check if the cache is empty
    async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

/// Builder for creating cache backend instances
pub struct CacheBackendBuilder {
    max_size: u64,
    ttl: Duration,
}

impl CacheBackendBuilder {
    #[must_use]
    pub fn new(max_size: u64, ttl: Duration) -> Self {
        Self { max_size, ttl }
    }

    #[must_use]
    pub const fn max_size(&self) -> u64 {
        self.max_size
    }

    #[must_use]
    pub const fn ttl(&self) -> Duration {
        self.ttl
    }
}
