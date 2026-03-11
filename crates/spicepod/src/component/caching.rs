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

use std::fmt::Display;

use super::{default_true, is_default_or_none};
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CacheKeyType {
    #[default]
    Plan,
    Sql,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum HashingAlgorithm {
    #[serde(rename = "siphash")]
    Siphash,
    #[serde(rename = "ahash")]
    Ahash,
    #[serde(rename = "xxh3")]
    #[default]
    XXH3,
    #[serde(rename = "xxh32")]
    XXH32,
    #[serde(rename = "xxh64")]
    XXH64,
    #[serde(rename = "xxh128")]
    XXH128,
    #[serde(rename = "blake3")]
    Blake3,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CacheEngine {
    /// Moka cache engine (default) - stable, built-in TTL, no race conditions
    #[default]
    Moka,
    /// Pingora-LRU cache engine - 2-3x faster, sharded architecture, manual TTL handling with a rare race condition. Note: table-specific invalidation uses manual key iteration (O(n) operation).
    Pingora,
}

impl Display for CacheEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheEngine::Moka => write!(f, "Moka"),
            CacheEngine::Pingora => write!(f, "Pingora-LRU"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    #[default]
    None,
    Zstd,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CachingPolicy {
    /// Least Recently Used caching policy.
    /// Suitable for workloads with strong recency bias, such as streaming data processing.
    #[default]
    Lru,
    /// `TinyLFU` caching policy.
    /// Combines LRU eviction with LFU-based admission policy.
    /// Suitable for most workloads including database, search, and analytics.
    TinyLfu,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Caching {
    #[serde(skip_serializing_if = "is_default_or_none")]
    pub sql_results: Option<SQLResultsCacheConfig>,
    #[serde(skip_serializing_if = "is_default_or_none")]
    pub search_results: Option<CacheConfig>,
    #[serde(skip_serializing_if = "is_default_or_none")]
    pub embeddings: Option<CacheConfig>,
}

impl Default for Caching {
    fn default() -> Self {
        Self {
            sql_results: Some(SQLResultsCacheConfig::default()),
            search_results: Some(CacheConfig::default()),
            embeddings: Some(CacheConfig::default()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct CacheConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub max_size: Option<String>,
    pub item_ttl: Option<String>,
    #[serde(default, alias = "eviction_policy")]
    pub caching_policy: CachingPolicy,
    #[serde(default)]
    pub hashing_algorithm: HashingAlgorithm,
    #[serde(default)]
    pub engine: CacheEngine,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_size: None,
            item_ttl: None,
            caching_policy: CachingPolicy::default(),
            hashing_algorithm: HashingAlgorithm::default(),
            engine: CacheEngine::default(),
        }
    }
}

// https://serde.rs/attr-flatten.html
// > Note: flatten is not supported in combination with structs that use deny_unknown_fields. Neither the outer nor inner flattened struct should use that attribute.
// As a result, we cannot use flatten to get a nice unknown field experience
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct SQLResultsCacheConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub max_size: Option<String>,
    pub item_ttl: Option<String>,
    #[serde(default, alias = "eviction_policy")]
    pub caching_policy: CachingPolicy,
    #[serde(default)]
    pub hashing_algorithm: HashingAlgorithm,
    #[serde(default)]
    pub cache_key_type: CacheKeyType,
    #[serde(default)]
    pub engine: CacheEngine,
    /// Maximum age for serving stale cached results while revalidating in the background.
    /// When set, cached results past their TTL (but within this additional window) will be
    /// served immediately while a background refresh is triggered.
    /// Format: duration string (e.g., "30s", "5m"). This is a response directive.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_while_revalidate_ttl: Option<String>,
    /// Encoding algorithm for compressing cached results.
    #[serde(default)]
    pub encoding: Encoding,
}

// serde(default) only applies when deserializing, so to return enabled: true from ::default() calls
// we need to implement Default manually
impl Default for SQLResultsCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_size: None,
            item_ttl: None,
            caching_policy: CachingPolicy::default(),
            hashing_algorithm: HashingAlgorithm::default(),
            cache_key_type: CacheKeyType::default(),
            engine: CacheEngine::default(),
            stale_while_revalidate_ttl: None,
            encoding: Encoding::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct ResultsCache {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub cache_max_size: Option<String>,
    pub item_ttl: Option<String>,
    #[serde(default, alias = "eviction_policy")]
    pub caching_policy: CachingPolicy,
    #[serde(default)]
    pub cache_key_type: CacheKeyType,
    #[serde(default)]
    pub hashing_algorithm: HashingAlgorithm,
    #[serde(default)]
    pub engine: CacheEngine,
    /// Maximum stale-while-revalidate duration to add to the cache TTL.
    pub max_stale_while_revalidate: Option<String>,
}

impl Default for ResultsCache {
    fn default() -> Self {
        Self {
            enabled: true,
            cache_max_size: None,
            item_ttl: None,
            caching_policy: CachingPolicy::default(),
            cache_key_type: CacheKeyType::default(),
            hashing_algorithm: HashingAlgorithm::default(),
            engine: CacheEngine::default(),
            max_stale_while_revalidate: None,
        }
    }
}

impl From<ResultsCache> for SQLResultsCacheConfig {
    fn from(val: ResultsCache) -> Self {
        SQLResultsCacheConfig {
            enabled: val.enabled,
            max_size: val.cache_max_size,
            item_ttl: val.item_ttl,
            caching_policy: val.caching_policy,
            hashing_algorithm: val.hashing_algorithm,
            cache_key_type: val.cache_key_type,
            engine: val.engine,
            stale_while_revalidate_ttl: None,
            encoding: Encoding::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test your assumptions - caching default should be enabled
    #[test]
    fn test_caching_default() {
        let caching = Caching::default();
        assert!(caching.sql_results.is_some());
        assert!(caching.search_results.is_some());

        let sql_results = caching.sql_results.expect("Should have cache config");
        assert!(sql_results.enabled);
        assert_eq!(sql_results.hashing_algorithm, HashingAlgorithm::default());
        assert_eq!(sql_results.cache_key_type, CacheKeyType::default());
        assert!(sql_results.max_size.is_none());
        assert!(sql_results.item_ttl.is_none());
        assert_eq!(sql_results.caching_policy, CachingPolicy::Lru);
        assert_eq!(sql_results, SQLResultsCacheConfig::default());

        let search_results = caching.search_results.expect("Should have cache config");
        assert!(search_results.enabled);
        assert_eq!(
            search_results.hashing_algorithm,
            HashingAlgorithm::default()
        );
        assert!(search_results.max_size.is_none());
        assert!(search_results.item_ttl.is_none());
        assert_eq!(search_results.caching_policy, CachingPolicy::Lru);
        assert_eq!(search_results, CacheConfig::default());

        let embeddings = caching.embeddings.expect("Should have cache config");
        assert!(embeddings.enabled);
        assert_eq!(embeddings.hashing_algorithm, HashingAlgorithm::default());
        assert!(embeddings.max_size.is_none());
        assert!(embeddings.item_ttl.is_none());
        assert_eq!(embeddings.caching_policy, CachingPolicy::Lru);
        assert_eq!(embeddings, CacheConfig::default());
    }
}
