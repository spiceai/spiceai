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

//! Differential correctness: same op sequences against Spice, Moka, and Pingora
//! must agree on returned/stored values (not just hit/miss).

#![cfg(all(test, feature = "pingora"))]

use super::{CacheBackend, CacheBackendBuilder, MokaBackend, PingoraBackend, SpiceBackend};
use crate::Sizeable;
use crate::metrics::{CacheMetrics, EvictionReason, InvalidationMode, StaleRejectionReason};
use sharded_cache::EvictionPolicy;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
struct DiffValue {
    data: String,
    size: usize,
}

impl DiffValue {
    fn new(data: &str, size: usize) -> Self {
        Self {
            data: data.to_string(),
            size,
        }
    }
}

impl Sizeable for DiffValue {
    fn get_memory_size(&self) -> usize {
        self.size
    }
}

impl CacheMetrics for DiffValue {
    fn record_hit() {}
    fn record_miss() {}
    fn record_request() {}
    fn record_item_count(_count: u64) {}
    fn record_size(_size: u64) {}
    fn record_max_size(_size: u64) {}
    fn record_eviction(_reason: EvictionReason) {}
    fn record_stale_rejection(_reason: StaleRejectionReason) {}
    fn record_table_invalidation(_mode: InvalidationMode) {}
    fn update_hit_ratio(_hits: u64, _total: u64) {}
    fn publish_counters_at_zero() {}
}

struct Engines {
    spice: Arc<SpiceBackend<DiffValue>>,
    moka: Arc<MokaBackend<DiffValue, std::hash::RandomState>>,
    pingora: Arc<PingoraBackend<DiffValue>>,
}

impl Engines {
    fn lru(max_capacity: u64, ttl: Duration) -> Self {
        let builder = CacheBackendBuilder::new(max_capacity, ttl);
        Self {
            spice: Arc::new(SpiceBackend::new(max_capacity, ttl, EvictionPolicy::Lru)),
            moka: Arc::new(MokaBackend::lru(&builder, std::hash::RandomState::new())),
            pingora: Arc::new(PingoraBackend::new(&builder)),
        }
    }
}

async fn assert_get_eq(engines: &Engines, key: u64, expected: Option<&str>) {
    let spice = engines.spice.get(&key).await.map(|v| v.data.clone());
    let moka = engines.moka.get(&key).await.map(|v| v.data.clone());
    let pingora = engines.pingora.get(&key).await.map(|v| v.data.clone());
    assert_eq!(spice.as_deref(), expected, "spice get({key})");
    assert_eq!(moka.as_deref(), expected, "moka get({key})");
    assert_eq!(pingora.as_deref(), expected, "pingora get({key})");
    assert_eq!(spice, moka, "spice vs moka get({key})");
    assert_eq!(spice, pingora, "spice vs pingora get({key})");
}

async fn insert_all(engines: &Engines, key: u64, data: &str, size: usize) {
    let v = DiffValue::new(data, size);
    engines.spice.insert(key, v.clone()).await;
    engines.moka.insert(key, v.clone()).await;
    engines.pingora.insert(key, v).await;
}

async fn remove_all(engines: &Engines, key: u64) -> [Option<String>; 3] {
    let spice = engines.spice.remove(&key).await.map(|v| v.data);
    let moka = engines.moka.remove(&key).await.map(|v| v.data);
    let pingora = engines.pingora.remove(&key).await.map(|v| v.data);
    assert_eq!(spice, moka, "spice vs moka remove({key})");
    assert_eq!(spice, pingora, "spice vs pingora remove({key})");
    [spice, moka, pingora]
}

#[tokio::test]
async fn differential_put_get_replace_remove_clear_agree() {
    let engines = Engines::lru(10_000, Duration::from_mins(1));

    insert_all(&engines, 1, "a", 10).await;
    assert_get_eq(&engines, 1, Some("a")).await;

    // replace
    insert_all(&engines, 1, "a2", 10).await;
    assert_get_eq(&engines, 1, Some("a2")).await;

    insert_all(&engines, 2, "b", 10).await;
    assert_get_eq(&engines, 2, Some("b")).await;

    let removed = remove_all(&engines, 1).await;
    assert_eq!(removed[0].as_deref(), Some("a2"));
    assert_get_eq(&engines, 1, None).await;
    assert_get_eq(&engines, 2, Some("b")).await;

    engines.spice.clear().await;
    engines.moka.clear().await;
    engines.pingora.clear().await;
    assert_get_eq(&engines, 2, None).await;
}

#[tokio::test]
async fn differential_ttl_expiry_agrees() {
    let engines = Engines::lru(10_000, Duration::from_millis(80));
    insert_all(&engines, 7, "ttl", 8).await;
    assert_get_eq(&engines, 7, Some("ttl")).await;
    tokio::time::sleep(Duration::from_millis(120)).await;
    // Pending maintenance so Moka observes expiry.
    engines.spice.run_pending_tasks().await;
    engines.moka.run_pending_tasks().await;
    engines.pingora.run_pending_tasks().await;
    assert_get_eq(&engines, 7, None).await;
}

#[tokio::test]
async fn differential_weight_lru_eviction_agrees_on_survivors() {
    // Under-capacity inserts must agree. Then force overflow and require that
    // for every key, engines that still hold it store the same value, and that
    // each engine fits the budget. Victim identity may differ across shard
    // layouts; value agreement is the contract.
    let engines = Engines::lru(100, Duration::from_mins(1));
    insert_all(&engines, 0, "old", 40).await;
    insert_all(&engines, 16, "mid", 40).await;
    assert_get_eq(&engines, 0, Some("old")).await;
    assert_get_eq(&engines, 16, Some("mid")).await;

    insert_all(&engines, 32, "new", 40).await;
    engines.spice.run_pending_tasks().await;
    engines.moka.run_pending_tasks().await;
    engines.pingora.run_pending_tasks().await;

    assert!(engines.spice.weighted_size().await <= 100);
    assert!(engines.moka.weighted_size().await <= 100);
    assert!(engines.pingora.weighted_size().await <= 100);

    for key in [0u64, 16, 32] {
        let spice = engines.spice.get(&key).await.map(|v| v.data.clone());
        let moka = engines.moka.get(&key).await.map(|v| v.data.clone());
        let pingora = engines.pingora.get(&key).await.map(|v| v.data.clone());
        // If two engines both hit, the stored value must match.
        if let (Some(s), Some(m)) = (&spice, &moka) {
            assert_eq!(s, m, "weight overflow get({key}) spice vs moka");
        }
        if let (Some(s), Some(p)) = (&spice, &pingora) {
            assert_eq!(s, p, "weight overflow get({key}) spice vs pingora");
        }
        if let (Some(m), Some(p)) = (&moka, &pingora) {
            assert_eq!(m, p, "weight overflow get({key}) moka vs pingora");
        }
    }
    // The just-inserted key should survive on Spice LRU (other-shard-first /
    // same-shard tail eviction leaves the MRU).
    assert_eq!(
        engines
            .spice
            .get(&32)
            .await
            .as_deref()
            .map(|v| v.data.as_str()),
        Some("new")
    );
}

#[tokio::test]
async fn differential_invalidate_matching_agrees() {
    let engines = Engines::lru(10_000, Duration::from_mins(1));
    insert_all(&engines, 1, "keep", 10).await;
    insert_all(&engines, 2, "drop-me", 10).await;
    insert_all(&engines, 3, "drop-me-too", 10).await;

    let pred = |v: &DiffValue| v.data.starts_with("drop");
    // Call through the trait so Spice/Pingora use the async CacheBackend path
    // rather than their inherent sync helpers.
    let s = CacheBackend::invalidate_matching(engines.spice.as_ref(), &pred).await;
    let m = CacheBackend::invalidate_matching(engines.moka.as_ref(), &pred).await;
    let p = CacheBackend::invalidate_matching(engines.pingora.as_ref(), &pred).await;
    assert_eq!(s, m, "invalidate count spice vs moka");
    assert_eq!(s, p, "invalidate count spice vs pingora");
    assert_eq!(s, 2);

    assert_get_eq(&engines, 1, Some("keep")).await;
    assert_get_eq(&engines, 2, None).await;
    assert_get_eq(&engines, 3, None).await;
}

#[tokio::test]
async fn differential_mixed_ops_sequence_agrees() {
    let engines = Engines::lru(5_000, Duration::from_mins(1));
    for i in 0..20u64 {
        insert_all(&engines, i, &format!("v{i}"), 20).await;
    }
    for i in 0..20u64 {
        if i % 2 == 0 {
            assert_get_eq(&engines, i, Some(&format!("v{i}"))).await;
        }
    }
    for i in (0..20u64).step_by(3) {
        let _ = remove_all(&engines, i).await;
    }
    for i in 0..20u64 {
        let expect = if i.is_multiple_of(3) {
            None
        } else {
            Some(format!("v{i}"))
        };
        let spice = engines.spice.get(&i).await.map(|v| v.data.clone());
        let moka = engines.moka.get(&i).await.map(|v| v.data.clone());
        let pingora = engines.pingora.get(&i).await.map(|v| v.data.clone());
        assert_eq!(spice, moka, "mixed get({i}) spice vs moka");
        assert_eq!(spice, pingora, "mixed get({i}) spice vs pingora");
        assert_eq!(spice, expect, "mixed get({i}) expected");
    }
}
