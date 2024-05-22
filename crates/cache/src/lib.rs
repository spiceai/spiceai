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

use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use byte_unit::Byte;
use datafusion::logical_expr::LogicalPlan;
use fundu::ParseError;
use lru_cache::LruCache;
use snafu::{ResultExt, Snafu};
use spicepod::component::runtime::ResultsCache;

mod lru_cache;
mod utils;

pub use utils::to_cached_record_batch_stream;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to parse cache_max_size value: {source}"))]
    FailedToParseCacheMaxSize { source: byte_unit::ParseError },

    #[snafu(display("Failed to parse item_expire value: {source}"))]
    FailedToParseItemExpire { source: ParseError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct CachedQueryResult {
    pub records: Arc<Vec<RecordBatch>>,
    pub schema: Arc<Schema>,
}

#[async_trait]
pub trait QueryResultCache {
    async fn get(&self, plan: &LogicalPlan) -> Result<Option<CachedQueryResult>>;
    async fn put(&self, plan: &LogicalPlan, result: CachedQueryResult) -> Result<()>;
    fn size(&self) -> u64;
    fn item_count(&self) -> u64;
}
#[derive(Clone)]
pub struct QueryResultCacheProvider {
    cache: Arc<dyn QueryResultCache + Send + Sync>,
    cache_max_size: u64,
    ttl: std::time::Duration,
}

impl QueryResultCacheProvider {
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

        let ttl = match &config.item_expire {
            Some(item_expire) => {
                fundu::parse_duration(item_expire).context(FailedToParseItemExpireSnafu)?
            }
            None => fundu::parse_duration("1s").context(FailedToParseItemExpireSnafu)?,
        };

        let cache_provider = QueryResultCacheProvider {
            cache: Arc::new(LruCache::new(cache_max_size, ttl)),
            cache_max_size,
            ttl,
        };

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

        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!("results_cache_max_size").set(self.max_size() as f64);
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!("results_cache_size").set(self.size() as f64);
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!("results_cache_item_count").set(self.item_count() as f64);

        res
    }

    #[must_use]
    pub fn max_size(&self) -> u64 {
        self.cache_max_size
    }

    #[must_use]
    pub fn size(&self) -> u64 {
        self.cache.size()
    }

    #[must_use]
    pub fn item_count(&self) -> u64 {
        self.cache.item_count()
    }
}

impl Display for QueryResultCacheProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "max size: {:.2}, item expire duration: {:?}",
            Byte::from_u64(self.cache_max_size).get_adjusted_unit(byte_unit::Unit::MiB),
            self.ttl
        )
    }
}
