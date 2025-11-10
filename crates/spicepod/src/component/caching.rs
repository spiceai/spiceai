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
    #[default]
    Siphash,
    Ahash,
    #[serde(rename = "xxh3")]
    XXH3,
    #[serde(rename = "xxh32")]
    XXH32,
    #[serde(rename = "xxh64")]
    XXH64,
    #[serde(rename = "xxh128")]
    XXH128,
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
    pub eviction_policy: Option<String>,
    #[serde(default)]
    pub hashing_algorithm: HashingAlgorithm,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_size: None,
            item_ttl: None,
            eviction_policy: None,
            hashing_algorithm: HashingAlgorithm::default(),
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
    pub eviction_policy: Option<String>,
    #[serde(default)]
    pub hashing_algorithm: HashingAlgorithm,
    #[serde(default)]
    pub cache_key_type: CacheKeyType,
    /// Maximum stale-while-revalidate duration to add to the cache TTL.
    /// When stale-while-revalidate is used, cache entries need to live for
    /// `item_ttl + max_stale_while_revalidate` to allow serving stale data
    /// during the revalidation window.
    pub max_stale_while_revalidate: Option<String>,
}

// serde(default) only applies when deserializing, so to return enabled: true from ::default() calls
// we need to implement Default manually
impl Default for SQLResultsCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_size: None,
            item_ttl: None,
            eviction_policy: None,
            hashing_algorithm: HashingAlgorithm::default(),
            cache_key_type: CacheKeyType::default(),
            max_stale_while_revalidate: None,
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
    pub eviction_policy: Option<String>,
    #[serde(default)]
    pub cache_key_type: CacheKeyType,
    #[serde(default)]
    pub hashing_algorithm: HashingAlgorithm,
    /// Maximum stale-while-revalidate duration to add to the cache TTL.
    pub max_stale_while_revalidate: Option<String>,
}

impl Default for ResultsCache {
    fn default() -> Self {
        Self {
            enabled: true,
            cache_max_size: None,
            item_ttl: None,
            eviction_policy: None,
            cache_key_type: CacheKeyType::default(),
            hashing_algorithm: HashingAlgorithm::default(),
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
            eviction_policy: val.eviction_policy,
            hashing_algorithm: val.hashing_algorithm,
            cache_key_type: val.cache_key_type,
            max_stale_while_revalidate: val.max_stale_while_revalidate,
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
        assert!(sql_results.eviction_policy.is_none());
        assert_eq!(sql_results, SQLResultsCacheConfig::default());

        let search_results = caching.search_results.expect("Should have cache config");
        assert!(search_results.enabled);
        assert_eq!(
            search_results.hashing_algorithm,
            HashingAlgorithm::default()
        );
        assert!(search_results.max_size.is_none());
        assert!(search_results.item_ttl.is_none());
        assert!(search_results.eviction_policy.is_none());
        assert_eq!(search_results, CacheConfig::default());

        let embeddings = caching.embeddings.expect("Should have cache config");
        assert!(embeddings.enabled);
        assert_eq!(embeddings.hashing_algorithm, HashingAlgorithm::default());
        assert!(embeddings.max_size.is_none());
        assert!(embeddings.item_ttl.is_none());
        assert!(embeddings.eviction_policy.is_none());
        assert_eq!(embeddings, CacheConfig::default());
    }
}
