use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use byte_unit::Byte;
use datafusion::logical_expr::LogicalPlan;
use lru_cache::RawLruCache;
use snafu::ResultExt;
use spicepod::component::runtime::ResultsCache;

use crate::CachedQueryResult;

mod lru_cache;

pub use lru_cache::key_for_sql;

pub struct QueryResultsCacheProviderRaw {
    cache: Arc<RawLruCache>,
    cache_max_size: u64,
    ttl: std::time::Duration,
    metrics_reported_last_time: AtomicU64,
}

impl QueryResultsCacheProviderRaw {
    /// # Errors
    ///
    /// Will return `Err` if method fails to parse cache params or to create the cache
    pub fn try_new(config: &ResultsCache) -> super::Result<Self> {
        let cache_max_size: u64 = match &config.cache_max_size {
            Some(cache_max_size) => Byte::parse_str(cache_max_size, true)
                .context(super::FailedToParseCacheMaxSizeSnafu)?
                .as_u64(),
            None => 128 * 1024 * 1024, // 128 MiB
        };

        let ttl = match &config.item_ttl {
            Some(item_ttl) => {
                fundu::parse_duration(item_ttl).context(super::FailedToParseItemTtlSnafu)?
            }
            None => std::time::Duration::from_secs(1),
        };

        let cache_provider = QueryResultsCacheProviderRaw {
            cache: Arc::new(RawLruCache::new(cache_max_size, ttl)),
            cache_max_size,
            ttl,
            metrics_reported_last_time: AtomicU64::new(0),
        };

        Ok(cache_provider)
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn get(&self, sql: &str) -> super::Result<Option<CachedQueryResult>> {
        match self.cache.get(sql).await {
            Ok(Some(cached_result)) => Ok(Some(cached_result)),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn put(&self, sql: &str, result: CachedQueryResult) -> super::Result<()> {
        let res = self.cache.put(sql, result).await;
        self.report_size_metrics();
        res
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn put_key(&self, plan_key: u64, result: CachedQueryResult) -> super::Result<()> {
        let res = self.cache.put_key(plan_key, result).await;
        self.report_size_metrics();
        res
    }

    fn report_size_metrics(&self) {
        let now_seconds = current_time_secs();

        if now_seconds - self.metrics_reported_last_time.load(Ordering::Relaxed) >= 5 {
            self.metrics_reported_last_time
                .store(now_seconds, Ordering::Relaxed);
        }
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

impl Display for QueryResultsCacheProviderRaw {
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
