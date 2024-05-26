/*
Copyright 2024 The Spice.ai OSS Authors

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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use byte_unit::Byte;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use fundu::ParseError;
use lru_cache::LruCache;
use metrics::atomics::AtomicU64;
use snafu::{ResultExt, Snafu};
use spicepod::component::runtime::ResultsCache;

mod lru_cache;
mod utils;

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
        table_name: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct QueryResult {
    pub data: SendableRecordBatchStream,
    pub from_cache: Option<bool>,
}

impl QueryResult {
    #[must_use]
    pub fn new(data: SendableRecordBatchStream, from_cache: Option<bool>) -> Self {
        QueryResult { data, from_cache }
    }
}

#[derive(Clone)]
pub struct CachedQueryResult {
    pub records: Arc<Vec<RecordBatch>>,
    pub schema: Arc<Schema>,
    pub input_tables: Arc<HashSet<String>>,
}

#[async_trait]
pub trait QueryResultCache {
    async fn get(&self, plan: &LogicalPlan) -> Result<Option<CachedQueryResult>>;
    async fn put(&self, plan: &LogicalPlan, result: CachedQueryResult) -> Result<()>;
    async fn invalidate_for_table(&self, table_name: &str) -> Result<()>;
    fn size_bytes(&self) -> u64;
    fn item_count(&self) -> u64;
}

pub struct QueryResultsCacheProvider {
    cache: Arc<dyn QueryResultCache + Send + Sync>,
    cache_max_size: u64,
    ttl: std::time::Duration,
    metrics_reported_last_time: AtomicU64,
}

impl QueryResultsCacheProvider {
    /// # Errors
    ///
    /// Will return `Err` if method fails to parse cache params or to create the cache
    pub fn new(config: &ResultsCache) -> Result<Self> {
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
        };

        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!("results_cache_max_size").set(cache_max_size as f64);

        Ok(cache_provider)
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn get(&self, plan: &LogicalPlan) -> Result<Option<CachedQueryResult>> {
        metrics::counter!("results_cache_request_count").increment(1);
        match self.cache.get(plan).await {
            Ok(Some(cached_result)) => {
                metrics::counter!("results_cache_hit_count").increment(1);
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

    fn report_size_metrics(&self) {
        let now_seconds = current_time_secs();

        if now_seconds - self.metrics_reported_last_time.load(Ordering::Relaxed) >= 5 {
            self.metrics_reported_last_time
                .store(now_seconds, Ordering::Relaxed);
            #[allow(clippy::cast_precision_loss)]
            metrics::gauge!("results_cache_size").set(self.size() as f64);
            #[allow(clippy::cast_precision_loss)]
            metrics::gauge!("results_cache_item_count").set(self.item_count() as f64);
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to invalidate cache for the table provided
    pub async fn invalidate_for_table(&self, table_name: &str) -> Result<()> {
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
