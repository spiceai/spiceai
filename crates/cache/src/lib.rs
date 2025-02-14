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
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use byte_unit::Byte;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::TableReference;
use fundu::ParseError;
use lru_cache::LruCache;
use snafu::{ResultExt, Snafu};
use spicepod::component::runtime::ResultsCache;

mod lru_cache;
mod metrics;
mod utils;

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct QueryResult {
    pub data: SendableRecordBatchStream,
    pub results_cache_status: QueryResultsCacheStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryResultsCacheStatus {
    // The request was not eligible for caching, and thus the results cache was not checked.
    CacheDisabled,
    // The request asked to bypass the cache, i.e. via `Cache-Control: no-cache`.
    CacheBypass,
    // The request was a cache hit.
    CacheHit,
    // The request was a cache miss.
    CacheMiss,
}

impl QueryResult {
    #[must_use]
    pub fn new(
        data: SendableRecordBatchStream,
        results_cache_status: QueryResultsCacheStatus,
    ) -> Self {
        QueryResult {
            data,
            results_cache_status,
        }
    }
}

#[derive(Clone)]
pub struct CachedQueryResult {
    pub records: Arc<Vec<RecordBatch>>,
    pub schema: Arc<Schema>,
    pub input_tables: Arc<HashSet<TableReference>>,
}

#[async_trait]
pub trait QueryResultCache {
    async fn get(&self, plan: &LogicalPlan) -> Result<Option<CachedQueryResult>>;
    async fn put(&self, plan: &LogicalPlan, result: CachedQueryResult) -> Result<()>;
    async fn put_key(&self, key: u64, result: CachedQueryResult) -> Result<()>;
    async fn invalidate_for_table(&self, table_name: TableReference) -> Result<()>;
    fn size_bytes(&self) -> u64;
    fn item_count(&self) -> u64;
}

pub struct QueryResultsCacheProvider {
    cache: Arc<dyn QueryResultCache + Send + Sync>,
    cache_max_size: u64,
    ttl: std::time::Duration,
    metrics_reported_last_time: AtomicU64,

    ignore_schemas: Box<[Box<str>]>,
}

impl QueryResultsCacheProvider {
    /// # Errors
    ///
    /// Will return `Err` if method fails to parse cache params or to create the cache
    pub fn try_new(config: &ResultsCache, ignore_schemas: Box<[Box<str>]>) -> Result<Self> {
        let cache_max_size: u64 = match &config.cache_max_size {
            Some(cache_max_size) => Byte::parse_str(cache_max_size, true)
                .context(FailedToParseCacheMaxSizeSnafu)?
                .as_u64(),
            None => 128 * 1024 * 1024, // 128 MiB
        };

        let ttl = match &config.item_ttl {
            Some(item_ttl) => fundu::parse_duration(item_ttl).context(FailedToParseItemTtlSnafu)?,
            None => std::time::Duration::from_secs(1),
        };

        let cache_provider = QueryResultsCacheProvider {
            cache: Arc::new(LruCache::new(cache_max_size, ttl)),
            cache_max_size,
            ttl,
            metrics_reported_last_time: AtomicU64::new(0),
            ignore_schemas,
        };

        metrics::MAX_SIZE_BYTES.record(cache_max_size, &[]);

        Ok(cache_provider)
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn get(&self, plan: &LogicalPlan) -> Result<Option<CachedQueryResult>> {
        metrics::REQUESTS.add(1, &[]);
        match self.cache.get(plan).await {
            Ok(Some(cached_result)) => {
                metrics::HITS.add(1, &[]);
                Ok(Some(cached_result))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn put(&self, plan: &LogicalPlan, result: CachedQueryResult) -> Result<()> {
        let res = self.cache.put(plan, result).await;
        self.report_size_metrics();
        res
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn put_key(&self, plan_key: u64, result: CachedQueryResult) -> Result<()> {
        let res = self.cache.put_key(plan_key, result).await;
        self.report_size_metrics();
        res
    }

    fn report_size_metrics(&self) {
        let now_seconds = current_time_secs();

        if now_seconds - self.metrics_reported_last_time.load(Ordering::Relaxed) >= 5 {
            self.metrics_reported_last_time
                .store(now_seconds, Ordering::Relaxed);
            metrics::SIZE_BYTES.record(self.size(), &[]);
            metrics::ITEMS.record(self.item_count(), &[]);
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to invalidate cache for the table provided
    pub async fn invalidate_for_table(&self, table_name: TableReference) -> Result<()> {
        self.cache.invalidate_for_table(table_name).await
    }

    #[must_use]
    pub fn max_size(&self) -> u64 {
        self.cache_max_size
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

fn current_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[must_use]
pub fn key_for_logical_plan(plan: &LogicalPlan) -> u64 {
    let mut hasher = DefaultHasher::new();
    plan.hash(&mut hasher);
    hasher.finish()
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
            QueryResultsCacheProvider::try_new(&ResultsCache::default(), Box::new([]))
                .expect("valid cache provider");

        assert!(!cache_provider.cache_is_enabled_for_plan(&logical_plan));
    }

    #[tokio::test]
    async fn test_cache_is_enabled_for_show_tables() {
        let sql = "show tables";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let cache_provider = QueryResultsCacheProvider::try_new(
            &ResultsCache::default(),
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
            QueryResultsCacheProvider::try_new(&ResultsCache::default(), Box::new([]))
                .expect("valid cache provider");

        assert!(cache_provider.cache_is_enabled_for_plan(&logical_plan));
    }
}
