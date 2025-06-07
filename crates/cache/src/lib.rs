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

use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hasher;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use byte_unit::Byte;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::TableReference;
use fundu::ParseError;
use key::CacheKey;
use key::RawCacheKey;
use result::query::CachedQueryResult;
use result::search::CachedSearchResult;
use snafu::{ResultExt, Snafu};
use spicepod::component::caching::HashingAlgorithm;

pub mod lru_cache;
mod metrics;
mod simple_cache;
mod utils;

pub mod key;
pub mod result;

pub use lru_cache::LruCache;
pub use simple_cache::SimpleCache;
use spicepod::component::caching::SQLResultsCacheConfig;
pub use utils::get_logical_plan_input_tables;
pub use utils::to_cached_record_batch_stream;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to parse cache_max_size value: {source}"))]
    FailedToParseCacheMaxSize { source: byte_unit::ParseError },

    #[snafu(display("Failed to parse item_ttl value: {source}"))]
    FailedToParseItemTtl { source: ParseError },

    #[snafu(display("Cache invalidation for dataset {table_name} failed with error: {source}"))]
    FailedToInvalidateCache {
        source: moka::PredicateError,
        table_name: Arc<str>,
    },

    #[snafu(display(
        "Cache invalidation failed with error: {source}.\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToInvalidateCacheGeneric { source: moka::PredicateError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait Sizeable {
    fn get_memory_size(&self) -> usize;
}

pub trait HashProvider {
    fn hasher(&self) -> Box<dyn Hasher>;
}

/// Trait for types that can be converted to a set of table references.
pub trait AsTableRefs {
    fn as_table_refs(&self) -> Arc<HashSet<TableReference>>;
}

impl AsTableRefs for LogicalPlan {
    fn as_table_refs(&self) -> Arc<HashSet<TableReference>> {
        Arc::new(get_logical_plan_input_tables(self))
    }
}

#[async_trait]
pub trait CacheProvider<V: AsTableRefs + Clone + Send + Sync + 'static>:
    HashProvider + std::fmt::Debug
{
    async fn get_raw_key(&self, key: &u64) -> Option<V>;
    async fn put_raw_key(&self, key: &u64, value: V);
    fn invalidate_all(&self);

    /// Invalidates all cache entries for the specified table.
    ///
    /// # Errors
    ///
    /// If the cache invalidation fails.
    fn invalidate_for_table(&self, table_ref: TableReference) -> Result<()>;
    fn size_bytes(&self) -> u64;
    fn item_count(&self) -> u64;
    fn max_size(&self) -> usize;
    async fn checkpoint(&self);
}

#[derive(Default)]
pub struct Caching {
    pub results: Option<Arc<QueryResultsCacheProvider>>,
    pub plans: Option<Arc<dyn CacheProvider<LogicalPlan> + Send + Sync>>,
    pub search: Option<Arc<dyn CacheProvider<CachedSearchResult> + Send + Sync>>,
}

impl std::fmt::Debug for Caching {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Caching")
            .field("results", &self.results)
            .field("plans", &self.plans)
            .field("search", &self.search)
            .finish_non_exhaustive()
    }
}

impl Caching {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_results_cache(mut self, results: Arc<QueryResultsCacheProvider>) -> Self {
        self.results = Some(results);
        self
    }

    #[must_use]
    pub fn with_plans_cache(
        mut self,
        plans: Arc<dyn CacheProvider<LogicalPlan> + Send + Sync>,
    ) -> Self {
        self.plans = Some(plans);
        self
    }

    #[must_use]
    pub fn with_search_cache(
        mut self,
        search: Arc<dyn CacheProvider<CachedSearchResult> + Send + Sync>,
    ) -> Self {
        self.search = Some(search);
        self
    }

    /// Invalidates all configured caches for the specified table.
    ///
    /// This is purposely eager, as an invalidated cache is better than a stale one.
    ///
    /// # Errors
    ///
    /// If the cache invalidation fails for any of the caches.
    pub fn invalidate_for_table(&self, table_ref: TableReference) -> Result<()> {
        if let Some(results_cache) = &self.results {
            results_cache.invalidate_for_table(table_ref.clone())?;
        }
        if let Some(plans_cache) = &self.plans {
            plans_cache.invalidate_for_table(table_ref.clone())?;
        }
        if let Some(search_cache) = &self.search {
            search_cache.invalidate_for_table(table_ref)?;
        }
        Ok(())
    }
}

// TODO: sunset ``QueryResultsCacheProvider`` in favor of ``CacheProvider``?
pub struct QueryResultsCacheProvider {
    cache: Arc<dyn CacheProvider<CachedQueryResult> + Send + Sync>,
    cache_max_size: u64,
    ttl: std::time::Duration,

    ignore_schemas: Box<[Box<str>]>,
}

impl std::fmt::Debug for QueryResultsCacheProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResultsCacheProvider")
            .field("cache_max_size", &self.cache_max_size)
            .field("ttl", &self.ttl)
            .field("ignore_schemas", &self.ignore_schemas)
            .finish_non_exhaustive()
    }
}

impl QueryResultsCacheProvider {
    /// # Errors
    ///
    /// Will return `Err` if method fails to parse cache params or to create the cache
    pub fn try_new(
        config: &SQLResultsCacheConfig,
        ignore_schemas: Box<[Box<str>]>,
    ) -> Result<Self> {
        let cache_max_size: u64 = match &config.inner.max_size {
            Some(cache_max_size) => Byte::parse_str(cache_max_size, true)
                .context(FailedToParseCacheMaxSizeSnafu)?
                .as_u64(),
            None => 128 * 1024 * 1024, // 128 MiB
        };

        let ttl = match &config.inner.item_ttl {
            Some(item_ttl) => fundu::parse_duration(item_ttl).context(FailedToParseItemTtlSnafu)?,
            None => std::time::Duration::from_secs(1),
        };

        let cache_provider = QueryResultsCacheProvider {
            cache: match config.inner.hashing_algorithm {
                HashingAlgorithm::Ahash => Arc::new(LruCache::new(
                    cache_max_size,
                    ttl,
                    ahash::RandomState::default(),
                )),
                HashingAlgorithm::Siphash => Arc::new(LruCache::new(
                    cache_max_size,
                    ttl,
                    std::hash::RandomState::default(),
                )),
            },
            cache_max_size,
            ttl,
            ignore_schemas,
        };

        Ok(cache_provider)
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn get(&self, key: CacheKey<'_>) -> Result<Option<CachedQueryResult>> {
        let raw_key = key.as_raw_key(self.cache.hasher());
        self.get_raw_key(&raw_key).await
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn get_raw_key(&self, raw_key: &RawCacheKey) -> Result<Option<CachedQueryResult>> {
        match self.cache.get_raw_key(&raw_key.as_u64()).await {
            Some(cached_result) => Ok(Some(cached_result)),
            None => Ok(None),
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn put(&self, key: CacheKey<'_>, result: CachedQueryResult) -> Result<()> {
        let raw_key = key.as_raw_key(self.cache.hasher());
        self.put_raw_key(&raw_key, result).await
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn put_raw_key(
        &self,
        raw_key: &RawCacheKey,
        result: CachedQueryResult,
    ) -> Result<()> {
        let res = self.cache.put_raw_key(&raw_key.as_u64(), result).await;
        Ok(res)
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to invalidate cache for the table provided
    pub fn invalidate_for_table(&self, table_name: TableReference) -> Result<()> {
        self.cache.invalidate_for_table(table_name)
    }

    #[must_use]
    pub fn max_size(&self) -> u64 {
        self.cache_max_size
    }

    #[must_use]
    pub fn hasher(&self) -> Box<dyn Hasher> {
        self.cache.hasher()
    }

    #[must_use]
    pub fn size(&self) -> u64 {
        self.cache.size_bytes()
    }

    #[must_use]
    pub fn item_count(&self) -> u64 {
        self.cache.item_count()
    }

    #[must_use]
    pub fn cache_is_enabled_for_plan(&self, plan: &LogicalPlan) -> bool {
        let mut plan_stack = vec![plan];

        while let Some(current_plan) = plan_stack.pop() {
            match current_plan {
                LogicalPlan::TableScan(source, ..) => {
                    let schema_name = source.table_name.schema();
                    let Some(schema) = schema_name else {
                        continue;
                    };
                    for ignore_schema in &self.ignore_schemas {
                        if *schema == **ignore_schema {
                            return false;
                        }
                    }
                }
                LogicalPlan::Explain { .. }
                | LogicalPlan::Analyze { .. }
                | LogicalPlan::DescribeTable { .. }
                | LogicalPlan::Statement(..) => return false,
                _ => {}
            }

            plan_stack.extend(current_plan.inputs());
        }

        true
    }
}

impl Display for QueryResultsCacheProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "max size: {:.2}, item ttl: {:?}",
            Byte::from_u64(self.cache_max_size).get_adjusted_unit(byte_unit::Unit::MiB),
            self.ttl
        )
    }
}

pub(crate) fn current_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use utils::tests::parse_sql_to_logical_plan;

    use super::*;

    #[tokio::test]
    async fn test_cache_is_enabled_for_system_query_describe() {
        let sql = "describe customer";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let cache_provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        assert!(!cache_provider.cache_is_enabled_for_plan(&logical_plan));
    }

    #[tokio::test]
    async fn test_cache_is_enabled_for_show_tables() {
        let sql = "show tables";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let cache_provider = QueryResultsCacheProvider::try_new(
            &SQLResultsCacheConfig::default(),
            Box::new(["information_schema".into()]),
        )
        .expect("valid cache provider");

        assert!(!cache_provider.cache_is_enabled_for_plan(&logical_plan));
    }

    #[tokio::test]
    async fn test_cache_is_enabled_for_simple_select() {
        let sql = "SELECT * FROM customer";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let cache_provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        assert!(cache_provider.cache_is_enabled_for_plan(&logical_plan));
    }
}
