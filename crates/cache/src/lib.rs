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

use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hasher;
use std::sync::Arc;

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

pub mod backend;
pub mod lru_cache;
pub mod metrics;
mod simple_cache;
mod utils;

pub mod encoding;
pub mod key;
pub mod result;

pub use backend::CacheBackend;
pub use backend::CacheBackendBuilder;
pub use backend::MokaBackend;

#[cfg(feature = "pingora")]
pub use backend::PingoraBackend;

pub use lru_cache::LruCache;
pub use metrics::CacheMetrics;
pub use simple_cache::SimpleCache;
use spicepod::component::caching::SQLResultsCacheConfig;
pub use utils::get_logical_plan_input_tables;
pub use utils::to_cached_record_batch_stream;

use crate::result::embeddings::CachedEmbeddingResult;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to parse cache_max_size value: {source}"))]
    FailedToParseCacheMaxSize { source: byte_unit::ParseError },

    #[snafu(display("Failed to parse {field} value: {source}"))]
    FailedToParseDuration { source: ParseError, field: String },

    #[snafu(display("Cache invalidation for dataset {table_name} failed with error: {source}"))]
    FailedToInvalidateCache {
        source: moka::PredicateError,
        table_name: Arc<str>,
    },

    #[snafu(display("Cache invalidation failed with error: {source}."))]
    FailedToInvalidateCacheGeneric { source: moka::PredicateError },

    #[snafu(display(
        "Invalid hashing algorithm. Please refer to the documentation for supported algorithms: https://spiceai.org/docs/features/caching#choosing-a-hashing_algorithm"
    ))]
    InvalidHashingAlgorithm,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait Sizeable {
    fn get_memory_size(&self) -> usize;
}

impl Sizeable for Vec<Vec<f32>> {
    fn get_memory_size(&self) -> usize {
        self.iter()
            .map(|vec| vec.len() * std::mem::size_of::<f32>())
            .sum()
    }
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
pub trait CacheProvider<V: Clone + Send + Sync + 'static>:
    HashProvider + std::fmt::Debug + std::fmt::Display
{
    async fn get_raw_key(&self, key: &u64) -> Option<V>;
    async fn put_raw_key(&self, key: &u64, value: V);
    async fn invalidate_all(&self);
    async fn size_bytes(&self) -> u64;
    async fn item_count(&self) -> u64;
    fn max_size(&self) -> usize;
    async fn checkpoint(&self);
}

/// A ``TabledCacheProvider`` represents a cache that can invalidate entries based on table references which their values reference.
pub trait TabledCacheProvider<V: AsTableRefs + Clone + Send + Sync + 'static>:
    CacheProvider<V>
{
    /// Invalidates all cache entries for the specified table.
    ///
    /// # Errors
    ///
    /// If the cache invalidation fails.
    fn invalidate_for_table(&self, table_ref: TableReference) -> Result<()>;
}

#[derive(Clone)]
pub enum HashBuilder {
    Ahash(ahash::RandomState),
    Siphash(std::hash::RandomState),
    Blake3,
    XxHash3(std::hash::BuildHasherDefault<twox_hash::XxHash3_64>),
    XxHash32(std::hash::BuildHasherDefault<twox_hash::XxHash32>),
    XxHash64(std::hash::BuildHasherDefault<twox_hash::XxHash64>),
    XxHash128,
}

impl std::hash::BuildHasher for HashBuilder {
    type Hasher = Box<dyn Hasher + Send + Sync + 'static>;

    fn build_hasher(&self) -> Self::Hasher {
        match self {
            HashBuilder::Ahash(builder) => Box::new(builder.build_hasher()),
            HashBuilder::Siphash(builder) => Box::new(builder.build_hasher()),
            HashBuilder::Blake3 => Box::new(blake3_compat::Blake3Wrapper::new()),
            HashBuilder::XxHash3(builder) => Box::new(builder.build_hasher()),
            HashBuilder::XxHash32(builder) => Box::new(builder.build_hasher()),
            HashBuilder::XxHash64(builder) => Box::new(builder.build_hasher()),
            HashBuilder::XxHash128 => Box::new(xxhash_compat::XxHash3_128Wrapper::new()),
        }
    }
}

/// Returns a hash builder for the specified algorithm.
///
/// # Errors
/// Return an error if the hashing algorithm is not supported.
pub fn get_hash_builder(hashing_algorithm: HashingAlgorithm) -> Result<HashBuilder, Error> {
    match hashing_algorithm {
        HashingAlgorithm::Siphash => Ok(HashBuilder::Siphash(std::hash::RandomState::default())),
        HashingAlgorithm::Ahash => Ok(HashBuilder::Ahash(ahash::RandomState::default())),
        HashingAlgorithm::Blake3 => Ok(HashBuilder::Blake3),
        HashingAlgorithm::XXH3 => Ok(HashBuilder::XxHash3(std::hash::BuildHasherDefault::<
            twox_hash::XxHash3_64,
        >::default())),
        HashingAlgorithm::XXH32 => Ok(HashBuilder::XxHash32(std::hash::BuildHasherDefault::<
            twox_hash::XxHash32,
        >::default())),
        HashingAlgorithm::XXH64 => Ok(HashBuilder::XxHash64(std::hash::BuildHasherDefault::<
            twox_hash::XxHash64,
        >::default())),
        HashingAlgorithm::XXH128 => Ok(HashBuilder::XxHash128),
    }
}

mod blake3_compat {
    use std::hash::Hasher;

    pub struct Blake3Wrapper {
        hasher: blake3::Hasher,
    }

    impl Blake3Wrapper {
        pub fn new() -> Self {
            Self {
                hasher: blake3::Hasher::new(),
            }
        }
    }

    impl Hasher for Blake3Wrapper {
        fn finish(&self) -> u64 {
            // blake3::Hasher::finalize_xof() doesn't consume self, so we must clone
            // to get the hash value while preserving the hasher state for potential reuse.
            // This is the intended design of blake3's incremental API.
            let mut xof = self.hasher.finalize_xof();
            let mut bytes = [0u8; 8];
            xof.fill(&mut bytes);
            u64::from_le_bytes(bytes)
        }

        fn write(&mut self, bytes: &[u8]) {
            self.hasher.update(bytes);
        }
    }
}

mod xxhash_compat {
    use std::hash::Hasher;

    pub struct XxHash3_128Wrapper {
        hasher: twox_hash::XxHash3_128,
    }

    impl XxHash3_128Wrapper {
        pub fn new() -> Self {
            Self {
                hasher: twox_hash::XxHash3_128::with_seed(0),
            }
        }
    }

    impl Hasher for XxHash3_128Wrapper {
        #[expect(clippy::cast_possible_truncation)]
        fn finish(&self) -> u64 {
            let hasher_copy = self.hasher.clone();
            let hash128 = hasher_copy.finish_128();

            let high = (hash128 >> 64) as u64;
            let low = hash128 as u64;
            high ^ low
        }

        fn write(&mut self, bytes: &[u8]) {
            self.hasher.write(bytes);
        }
    }
}

#[derive(Default)]
pub struct Caching {
    pub results: Option<Arc<QueryResultsCacheProvider>>,
    pub plans: Option<Arc<dyn TabledCacheProvider<LogicalPlan> + Send + Sync>>,
    pub search: Option<Arc<dyn TabledCacheProvider<CachedSearchResult> + Send + Sync>>,
    pub embeddings: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
}

impl std::fmt::Debug for Caching {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Caching")
            .field("results", &self.results)
            .field("plans", &self.plans)
            .field("search", &self.search)
            .field("embeddings", &self.embeddings)
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
        plans: Arc<dyn TabledCacheProvider<LogicalPlan> + Send + Sync>,
    ) -> Self {
        self.plans = Some(plans);
        self
    }

    #[must_use]
    pub fn with_search_cache(
        mut self,
        search: Arc<dyn TabledCacheProvider<CachedSearchResult> + Send + Sync>,
    ) -> Self {
        self.search = Some(search);
        self
    }

    #[must_use]
    pub fn with_embeddings_cache(
        mut self,
        embeddings: Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>,
    ) -> Self {
        self.embeddings = Some(embeddings);
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
    cache: Arc<dyn TabledCacheProvider<CachedQueryResult> + Send + Sync>,
    cache_max_size: u64,
    ttl: std::time::Duration,
    stale_while_revalidate_ttl: Option<std::time::Duration>,

    ignore_schemas: Box<[Box<str>]>,
    encoder: Option<Arc<dyn encoding::Encoder>>,
    encoding: spicepod::component::caching::Encoding,
    hashing_algorithm: spicepod::component::caching::HashingAlgorithm,
}

impl std::fmt::Debug for QueryResultsCacheProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResultsCacheProvider")
            .field("cache_max_size", &self.cache_max_size)
            .field("ttl", &self.ttl)
            .field(
                "stale_while_revalidate_ttl",
                &self.stale_while_revalidate_ttl,
            )
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
        let cache_max_size: u64 = match &config.max_size {
            Some(cache_max_size) => Byte::parse_str(cache_max_size, true)
                .context(FailedToParseCacheMaxSizeSnafu)?
                .as_u64(),
            None => 128 * 1024 * 1024, // 128 MiB
        };

        let ttl = match &config.item_ttl {
            Some(item_ttl) => {
                fundu::parse_duration(item_ttl).context(FailedToParseDurationSnafu {
                    field: "item_ttl".to_string(),
                })?
            }
            None => std::time::Duration::from_secs(1),
        };

        let stale_while_revalidate_ttl = match &config.stale_while_revalidate_ttl {
            Some(stale_ttl_str) => Some(fundu::parse_duration(stale_ttl_str).context(
                FailedToParseDurationSnafu {
                    field: "stale_while_revalidate_ttl".to_string(),
                },
            )?),
            None => None,
        };

        let hash_builder = get_hash_builder(config.hashing_algorithm)?;
        // Cache TTL should be the base TTL plus the stale-while-revalidate window
        // so entries aren't evicted before they can be served as stale
        let cache_ttl = ttl + stale_while_revalidate_ttl.unwrap_or_default();
        let cache = Arc::new(LruCache::new(
            cache_max_size,
            cache_ttl,
            hash_builder,
            config.caching_policy,
            config.engine,
        ));

        let encoder = encoding::get_encoder(config.encoding);

        let cache_provider = QueryResultsCacheProvider {
            cache,
            cache_max_size,
            ttl,
            stale_while_revalidate_ttl,
            ignore_schemas,
            encoder,
            encoding: config.encoding,
            hashing_algorithm: config.hashing_algorithm,
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
    pub async fn size(&self) -> u64 {
        self.cache.size_bytes().await
    }

    #[must_use]
    pub async fn item_count(&self) -> u64 {
        self.cache.item_count().await
    }

    /// Returns the base TTL for cache entries (used for staleness checks).
    #[must_use]
    pub fn ttl(&self) -> std::time::Duration {
        self.ttl
    }

    /// Returns the maximum stale-while-revalidate duration.
    #[must_use]
    pub fn max_stale_while_revalidate(&self) -> std::time::Duration {
        self.stale_while_revalidate_ttl.unwrap_or_default()
    }

    /// Returns the actual cache TTL (base TTL + stale-while-revalidate period).
    /// This is the duration after which entries are evicted from the cache.
    #[must_use]
    pub fn cache_ttl(&self) -> std::time::Duration {
        self.ttl + self.stale_while_revalidate_ttl.unwrap_or_default()
    }

    /// Runs pending cache maintenance tasks (e.g., eviction of expired entries).
    /// This is useful in tests to ensure eviction happens immediately.
    pub async fn run_pending_tasks(&self) {
        self.cache.checkpoint().await;
    }

    #[must_use]
    pub fn stale_while_revalidate_ttl(&self) -> Option<std::time::Duration> {
        self.stale_while_revalidate_ttl
    }

    #[must_use]
    pub fn encoder(&self) -> Option<Arc<dyn encoding::Encoder>> {
        self.encoder.as_ref().map(Arc::clone)
    }

    #[must_use]
    pub fn encoding_name(&self) -> &'static str {
        use spicepod::component::caching::Encoding;
        match self.encoding {
            Encoding::None => "none",
            Encoding::Zstd => "zstd",
        }
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
                | LogicalPlan::Ddl(..)
                | LogicalPlan::Dml(..)
                | LogicalPlan::Copy { .. }
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
            "max size: {:.2}, item ttl: {:?}, hashing algorithm: {:?}, encoding: {}",
            Byte::from_u64(self.cache_max_size).get_adjusted_unit(byte_unit::Unit::MiB),
            self.ttl,
            self.hashing_algorithm,
            self.encoding_name(),
        )
    }
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

        (!cache_provider.cache_is_enabled_for_plan(&logical_plan))
            .then_some(())
            .expect("cache should be disabled for SHOW TABLES");
    }

    #[tokio::test]
    async fn test_cache_is_enabled_for_simple_select() {
        let sql = "SELECT * FROM customer";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let cache_provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        cache_provider
            .cache_is_enabled_for_plan(&logical_plan)
            .then_some(())
            .expect("cache should be enabled for simple SELECT");
    }

    #[tokio::test]
    async fn test_cache_is_disabled_for_insert_into() {
        let sql = "INSERT INTO customer (id, first_name, last_name, state) VALUES (1, 'John', 'Doe', 'NY')";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let cache_provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        (!cache_provider.cache_is_enabled_for_plan(&logical_plan))
            .then_some(())
            .expect("cache should be disabled for INSERT INTO");
    }

    #[tokio::test]
    async fn test_cache_is_disabled_for_update() {
        let sql = "UPDATE customer SET first_name = 'Jane' WHERE id = 1";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let cache_provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        (!cache_provider.cache_is_enabled_for_plan(&logical_plan))
            .then_some(())
            .expect("cache should be disabled for UPDATE");
    }

    #[tokio::test]
    async fn test_cache_is_disabled_for_delete() {
        let sql = "DELETE FROM customer WHERE id = 1";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let cache_provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        (!cache_provider.cache_is_enabled_for_plan(&logical_plan))
            .then_some(())
            .expect("cache should be disabled for DELETE");
    }

    #[tokio::test]
    async fn test_cache_is_disabled_for_create_table() {
        let sql = "CREATE TABLE test_table (id INT, name VARCHAR(50))";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let cache_provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        (!cache_provider.cache_is_enabled_for_plan(&logical_plan))
            .then_some(())
            .expect("cache should be disabled for CREATE TABLE");
    }

    #[test]
    fn test_display_includes_encoding() {
        let config_none = SQLResultsCacheConfig {
            encoding: spicepod::component::caching::Encoding::None,
            ..SQLResultsCacheConfig::default()
        };
        let cache_none = QueryResultsCacheProvider::try_new(&config_none, Box::new([]))
            .expect("valid cache provider");
        let display_none = format!("{cache_none}");
        assert!(
            display_none.contains("encoding: none"),
            "Display should include encoding: none, got: {display_none}"
        );

        let config_zstd = SQLResultsCacheConfig {
            encoding: spicepod::component::caching::Encoding::Zstd,
            ..SQLResultsCacheConfig::default()
        };
        let cache_zstd = QueryResultsCacheProvider::try_new(&config_zstd, Box::new([]))
            .expect("valid cache provider");
        let display_zstd = format!("{cache_zstd}");
        assert!(
            display_zstd.contains("encoding: zstd"),
            "Display should include encoding: zstd, got: {display_zstd}"
        );
    }

    #[tokio::test]
    async fn test_cache_is_disabled_for_copy() {
        let sql = "COPY customer TO 'output.csv'";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let cache_provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        (!cache_provider.cache_is_enabled_for_plan(&logical_plan))
            .then_some(())
            .expect("cache should be disabled for COPY");
    }
}
