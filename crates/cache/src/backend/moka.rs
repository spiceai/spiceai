/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Moka-based cache backend implementation.

use super::{CacheBackend, CacheBackendBuilder};
use crate::Sizeable;
use crate::key::PassthroughHashBuilder;
use async_trait::async_trait;
use moka::Expiry;
use moka::future::Cache;
use moka::ops::compute::Op;
use std::hash::BuildHasher;
use std::time::{Duration, Instant};

/// TTL policy that matches Moka's `time_to_live` on create and on a normal
/// overwrite, but keeps the remaining lifetime when the new value is an
/// in-place rewrite ([`Sizeable::keep_remaining_ttl`]).
#[derive(Clone, Copy)]
pub(crate) struct CacheTtl {
    pub ttl: Duration,
}

impl<K, V: Sizeable> Expiry<K, V> for CacheTtl {
    fn expire_after_create(&self, _key: &K, _value: &V, _created_at: Instant) -> Option<Duration> {
        Some(self.ttl)
    }

    fn expire_after_update(
        &self,
        _key: &K,
        value: &V,
        _updated_at: Instant,
        duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        if value.keep_remaining_ttl() {
            duration_until_expiry
        } else {
            Some(self.ttl)
        }
    }
}

/// Moka-based cache backend implementation
///
/// Provides:
/// - Built-in TTL management
/// - Automatic eviction
/// - Thread-safe operations
/// - No race conditions
pub struct MokaBackend<V, T>
where
    V: Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
{
    cache: Cache<u64, V, PassthroughHashBuilder<T>>,
}

impl<V, T> MokaBackend<V, T>
where
    V: Sizeable + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
    <T as BuildHasher>::Hasher: Send + Sync + 'static,
{
    /// Creates a new Moka backend with the given configuration.
    pub fn new(builder: &CacheBackendBuilder, hasher: T) -> Self {
        let cache: Cache<u64, V, PassthroughHashBuilder<T>> = Cache::builder()
            .expire_after(CacheTtl { ttl: builder.ttl() })
            .weigher(|_key, value: &V| -> u32 {
                let val: usize = value.get_memory_size();
                val.try_into().unwrap_or(u32::MAX)
            })
            .max_capacity(builder.max_capacity())
            .build_with_hasher(PassthroughHashBuilder::new(hasher));

        Self { cache }
    }

    /// Creates a Moka backend wrapping an existing Moka cache.
    ///
    /// This is useful when you have already configured a Moka cache with
    /// specific settings (eviction policy, listeners, etc.) and want to
    /// use it with the [`CacheBackend`] trait.
    #[must_use]
    pub(crate) fn from_cache(cache: Cache<u64, V, PassthroughHashBuilder<T>>) -> Self {
        Self { cache }
    }

    /// The moka cache this backend wraps.
    ///
    /// Moka's predicate-based invalidation (`invalidate_entries_if`) has no equivalent on
    /// the [`CacheBackend`] trait, so table invalidation reaches for the cache itself.
    pub(crate) fn cache(&self) -> &Cache<u64, V, PassthroughHashBuilder<T>> {
        &self.cache
    }
}

#[async_trait]
impl<V, T> CacheBackend<V> for MokaBackend<V, T>
where
    V: Sizeable + Clone + Send + Sync + 'static,
    T: BuildHasher + Clone + Send + Sync + 'static,
    <T as BuildHasher>::Hasher: Send + Sync + 'static,
{
    async fn insert(&self, key: u64, value: V) {
        // Moka handles sizing via the weigher
        self.cache.insert(key, value).await;
    }

    async fn replace_if(
        &self,
        key: u64,
        value: V,
        should_replace: &(dyn for<'v> Fn(&'v V) -> bool + Send + Sync),
    ) -> bool {
        let outcome = self
            .cache
            .entry(key)
            .and_compute_with(|current| {
                let replace = current
                    .as_ref()
                    .is_some_and(|entry| should_replace(entry.value()));
                std::future::ready(if replace { Op::Put(value) } else { Op::Nop })
            })
            .await;
        matches!(outcome, moka::ops::compute::CompResult::ReplacedWith(_))
    }

    async fn get(&self, key: &u64) -> Option<V> {
        self.cache.get(key).await
    }

    async fn remove(&self, key: &u64) -> Option<V> {
        self.cache.remove(key).await
    }

    async fn clear(&self) {
        self.cache.invalidate_all();
    }

    async fn iter_keys(&self) -> Vec<u64> {
        self.cache.run_pending_tasks().await;
        self.cache.iter().map(|(k, _)| *k).collect()
    }

    #[expect(clippy::cast_possible_truncation)]
    async fn len(&self) -> usize {
        self.cache.entry_count() as usize
    }

    async fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Sizeable;

    struct KeepTtl(bool);

    impl Sizeable for KeepTtl {
        fn get_memory_size(&self) -> usize {
            1
        }

        fn keep_remaining_ttl(&self) -> bool {
            self.0
        }
    }

    #[test]
    fn create_uses_the_configured_ttl() {
        let policy = CacheTtl {
            ttl: Duration::from_secs(10),
        };
        assert_eq!(
            policy.expire_after_create(&(), &KeepTtl(false), Instant::now()),
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn a_promotion_keeps_the_remaining_ttl() {
        let policy = CacheTtl {
            ttl: Duration::from_secs(10),
        };
        let remaining = Duration::from_secs(3);
        assert_eq!(
            policy.expire_after_update(&(), &KeepTtl(true), Instant::now(), Some(remaining)),
            Some(remaining)
        );
    }

    #[test]
    fn a_new_result_restarts_ttl() {
        let policy = CacheTtl {
            ttl: Duration::from_secs(10),
        };
        assert_eq!(
            policy.expire_after_update(
                &(),
                &KeepTtl(false),
                Instant::now(),
                Some(Duration::from_secs(3))
            ),
            Some(Duration::from_secs(10))
        );
    }
}
