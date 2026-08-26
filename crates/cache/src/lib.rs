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
pub(crate) mod sizing;
pub mod utils;

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
pub use metrics::EvictionReason;
pub use simple_cache::SimpleCache;
use spicepod::component::caching::SQLResultsCacheConfig;
pub use utils::RESPONSE_STATUS_COLUMN;
pub use utils::batches_cacheable;
pub use utils::filter_transient_error_responses;
pub use utils::get_logical_plan_input_tables;
pub use utils::to_cached_record_batch_stream;

/// Stable [`datafusion::logical_expr::UserDefinedLogicalNodeCore::name`] values for
/// every Spice logical-plan extension node that performs (or dispatches) a write,
/// a schema mutation, or any other side-effect that must not be reachable via a
/// read-only SQL path and must not be served from or populated into the SQL
/// results cache.
///
/// Keep this list in sync with:
/// - `datafusion_ddl::DdlExtensionNode` → `"DdlExtension"`
/// - `datafusion_dml::DmlExtensionNode` → `"DmlExtension"`
pub const WRITE_CAPABLE_EXTENSION_NAMES: &[&str] = &["DdlExtension", "DmlExtension"];

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

    // Single line on purpose: callers interpolate this into a one-line `tracing` event, so an
    // embedded newline would split the event and break structured log ingestion.
    #[snafu(display(
        "Cache invalidation for dataset {table_name} did not finish: {source}. Cached results for it may be stale until the next invalidation."
    ))]
    InvalidationDidNotFinish {
        source: tokio::task::JoinError,
        table_name: Arc<str>,
    },

    #[snafu(display(
        "Invalid hashing algorithm. Please refer to the documentation for supported algorithms: https://spiceai.org/docs/features/caching#choosing-a-hashing_algorithm"
    ))]
    InvalidHashingAlgorithm,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The memory a cached value holds, as the byte budget sees it.
///
/// This is what `max_size` is enforced against, so an implementation must
/// account for everything the value reaches through a pointer — the weigher is
/// handed the value and nothing else — **and** add
/// `sizing::ENTRY_OVERHEAD_BYTES` for the store's own per-entry bookkeeping,
/// which no implementation can see. The `sizing` module holds the deep-size
/// helpers and states which imprecisions are deliberate.
///
/// Omitting any of it does not fail to compile; it silently makes `max_size`
/// unable to bound a stream of that value, which is the defect
/// <https://github.com/spiceai/spiceai/issues/12931> reported.
pub trait Sizeable {
    fn get_memory_size(&self) -> usize;
}

impl Sizeable for Vec<Vec<f32>> {
    fn get_memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + sizing::f32_vectors_heap_size(self)
            + sizing::ENTRY_OVERHEAD_BYTES
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

/// Default catalog and schema used to resolve table references before comparing
/// them during cache invalidation.
///
/// These mirror `runtime_datafusion::SPICE_DEFAULT_CATALOG` /
/// `SPICE_DEFAULT_SCHEMA`. They are duplicated here because the `cache` crate
/// cannot depend on `runtime-datafusion` without forming a dependency cycle
/// (`runtime-datafusion -> search -> datafusion-optimizer-rules -> cache`).
pub const SPICE_DEFAULT_CATALOG: &str = "spice";
pub const SPICE_DEFAULT_SCHEMA: &str = "public";

/// Returns `true` if `target` refers to the same physical table as any reference
/// in `stored`, after resolving both sides to fully-qualified
/// (`catalog.schema.table`) form.
///
/// Cache entries record table references exactly as written in the originating
/// SQL (e.g. bare `customer` or fully-qualified `spice.public.customer`), while
/// invalidators — accelerated-refresh completion, `INSERT INTO`, and other DML —
/// may pass a differently-qualified reference for the same table. Plain
/// `TableReference` equality therefore misses entries that name the same table
/// with a different qualification, leaving stale rows served as fresh cache hits
/// until TTL expiry. Resolving both sides first makes the comparison robust to
/// qualification differences.
#[must_use]
pub fn resolved_table_match<S: std::hash::BuildHasher>(
    stored: &HashSet<TableReference, S>,
    target: &TableReference,
) -> bool {
    // Resolve the target once into a local (&str, &str, &str) triple so the
    // O(cache_size) scan never clones TableReference or allocates via resolve().
    let target_catalog = target.catalog().unwrap_or(SPICE_DEFAULT_CATALOG);
    let target_schema = target.schema().unwrap_or(SPICE_DEFAULT_SCHEMA);
    let target_table = target.table();

    stored.iter().any(|stored_ref| {
        stored_ref.catalog().unwrap_or(SPICE_DEFAULT_CATALOG) == target_catalog
            && stored_ref.schema().unwrap_or(SPICE_DEFAULT_SCHEMA) == target_schema
            && stored_ref.table() == target_table
    })
}

/// The table name an invalidation error reports, shared by the `Snafu` contexts every
/// engine's invalidation path builds.
///
/// `TableReference::table()` yields a `&str`, so matching the variants is what lets the
/// name be an `Arc` clone rather than a fresh allocation per invalidation.
pub(crate) fn invalidated_table_name(table_ref: &TableReference) -> Arc<str> {
    match table_ref {
        TableReference::Bare { table }
        | TableReference::Partial { table, .. }
        | TableReference::Full { table, .. } => Arc::clone(table),
    }
}

#[async_trait]
pub trait CacheProvider<V: Clone + Send + Sync + 'static>:
    HashProvider + std::fmt::Debug + std::fmt::Display
{
    async fn get_raw_key(&self, key: &u64) -> Option<V>;
    /// Looks up `key`, treating a value that `is_valid` rejects as a miss —
    /// including for hit/miss metrics, so the hit ratio reflects results
    /// actually served rather than entries merely found.
    ///
    /// Deliberately has no default implementation: a default delegating to
    /// [`Self::get_raw_key`] would silently record a hit for a value the caller
    /// then discards, which is precisely the accounting error this exists to
    /// prevent.
    async fn get_raw_key_validated(
        &self,
        key: &u64,
        is_valid: &(dyn for<'v> Fn(&'v V) -> bool + Send + Sync),
    ) -> Option<V>;
    async fn put_raw_key(&self, key: &u64, value: V);
    async fn invalidate_all(&self);
    async fn size_bytes(&self) -> u64;
    async fn item_count(&self) -> u64;
    fn max_size(&self) -> usize;
    async fn checkpoint(&self);
}

/// A ``TabledCacheProvider`` represents a cache that can invalidate entries based on table references which their values reference.
#[async_trait]
pub trait TabledCacheProvider<V: AsTableRefs + Clone + Send + Sync + 'static>:
    CacheProvider<V>
{
    /// Invalidates all cache entries for the specified table.
    ///
    /// Awaiting this is what lets an implementation take its work off the calling runtime
    /// worker; the entries are invalidated by the time it returns, so a caller that awaits it
    /// before reporting a write complete keeps the ordering it had when this was synchronous.
    ///
    /// # Errors
    ///
    /// If the cache invalidation fails.
    async fn invalidate_for_table(&self, table_ref: TableReference) -> Result<()>;
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
    pub async fn invalidate_for_table(&self, table_ref: TableReference) -> Result<()> {
        if let Some(results_cache) = &self.results {
            results_cache
                .invalidate_for_table(table_ref.clone())
                .await?;
        }
        if let Some(plans_cache) = &self.plans {
            plans_cache.invalidate_for_table(table_ref.clone()).await?;
        }
        if let Some(search_cache) = &self.search {
            search_cache.invalidate_for_table(table_ref).await?;
        }
        Ok(())
    }

    /// Drives moka housekeeping on every configured cache. `moka::future::Cache`
    /// has no background maintenance thread, so invalidation predicates and
    /// expired entries on a cache with no `get`/`insert` traffic are only
    /// reclaimed when this runs.
    pub async fn run_pending_maintenance(&self) {
        if let Some(results) = &self.results {
            results.run_pending_tasks().await;
            // The size and item-count gauges are otherwise only refreshed when
            // something is *inserted*, so after a refresh invalidates a large
            // number of entries they keep reporting the pre-invalidation
            // values until the next write. Re-reporting here means they track
            // the drop instead of hiding it.
            results.report_size_metrics().await;
        }
        if let Some(plans) = &self.plans {
            plans.checkpoint().await;
        }
        if let Some(search) = &self.search {
            search.checkpoint().await;
        }
        if let Some(embeddings) = &self.embeddings {
            embeddings.checkpoint().await;
        }
    }
}

/// Records, per table, when that table was last invalidated.
///
/// Invalidating a cache can only remove entries that already exist. A query
/// that read a table *before* it was invalidated but finishes writing its
/// result *after* would otherwise repopulate the cache with pre-invalidation
/// data, and that entry survives — [`moka`] invalidation closures only match
/// entries last modified at or before the closure was registered.
///
/// An entry therefore records when its read began, and every cache *hit*
/// consults this clock: an entry whose tables were invalidated since it read
/// them is not served, no matter when it was stored. Checking on read rather
/// than on write is what makes this airtight — a check before storing leaves
/// the entry observable in the window between the check and the store, however
/// small. Reads are far more frequent than invalidations, so this is an
/// `RwLock` rather than a lock-free map, and lookups hash table names directly
/// rather than building a key string, keeping the hit path allocation-free.
///
/// Memory is bounded at [`MAX_TRACKED_TABLES`] regardless of how many distinct
/// tables are invalidated over a process lifetime. Table identities are not
/// bounded by configuration — where DDL is enabled a client can create and
/// write to arbitrarily many tables, and dataset names also churn across
/// hot-reloads — so the map cannot be allowed to grow with all-time history.
/// Reclamation must not open a hole for a still-running query, which rules out
/// discarding by age: with the default one-second `item_ttl`, a query easily
/// outlives any age-based cutoff. Instead, over-capacity collapses the map into
/// a single conservative `discarded_floor`, which rejects *every* write whose
/// read began before that point. That is sound (it can only over-reject) and
/// self-healing (later invalidations repopulate per-table entries), at the cost
/// of some lost cache entries in the moments after a collapse.
#[derive(Default)]
struct TableInvalidationClock {
    state: parking_lot::RwLock<TableInvalidationState>,
}

/// Upper bound on individually-tracked tables. Each entry is a `u64` hash of
/// the resolved table name plus an `Instant`, so the clock stays in the tens of
/// kilobytes while the bound sits far above the table count of any ordinary
/// deployment — a collapse should be reachable only under deliberate churn.
const MAX_TRACKED_TABLES: usize = 4096;

#[derive(Default)]
struct TableInvalidationState {
    invalidated_at: std::collections::HashMap<u64, std::time::Instant>,
    /// Stands in for every table dropped from `invalidated_at`. Holds the
    /// newest instant among the dropped entries, which is `>=` the true
    /// invalidation instant of each of them, so treating it as their stamp can
    /// only reject writes that a per-table entry would have allowed.
    discarded_floor: Option<std::time::Instant>,
}

impl TableInvalidationClock {
    /// Key a table by a hash of its fully-resolved `catalog.schema.table`
    /// form, so that differently-qualified references to the same physical
    /// table collide — matching [`resolved_table_match`].
    ///
    /// Hashing the components directly keeps the cache-hit path free of the
    /// string allocation a formatted key would need. Components are separated
    /// by a byte that cannot appear inside one, so `a.b`/`c` cannot be confused
    /// with `a`/`b.c`. A hash collision between two genuinely different tables
    /// would only ever *reject* a cacheable result, never serve a stale one.
    fn resolved_key(table_ref: &TableReference) -> u64 {
        use std::hash::{BuildHasher, Hasher};

        let mut hasher =
            std::hash::BuildHasherDefault::<twox_hash::XxHash3_64>::default().build_hasher();
        for component in [
            table_ref.catalog().unwrap_or(SPICE_DEFAULT_CATALOG),
            table_ref.schema().unwrap_or(SPICE_DEFAULT_SCHEMA),
            table_ref.table(),
        ] {
            hasher.write(component.as_bytes());
            hasher.write_u8(0xff);
        }
        hasher.finish()
    }

    fn mark_invalidated(&self, table_ref: &TableReference, at: std::time::Instant) {
        let key = Self::resolved_key(table_ref);
        let mut state = self.state.write();

        if state.invalidated_at.len() >= MAX_TRACKED_TABLES
            && !state.invalidated_at.contains_key(&key)
        {
            let newest = state.invalidated_at.values().copied().max();
            state.discarded_floor = state.discarded_floor.max(newest);
            state.invalidated_at.clear();
        }

        state.invalidated_at.insert(key, at);
    }

    /// Returns `true` if any of `tables` was invalidated at or after `since`.
    ///
    /// Ties count as invalidated: an invalidation recorded in the same instant
    /// as the read began must be assumed to have happened first, since serving
    /// stale data is worse than losing a cache entry.
    fn invalidated_since<S: std::hash::BuildHasher>(
        &self,
        tables: &HashSet<TableReference, S>,
        since: std::time::Instant,
    ) -> bool {
        if tables.is_empty() {
            return false;
        }
        let state = self.state.read();

        // Any table whose own entry was collapsed away is covered by the floor.
        if state.discarded_floor.is_some_and(|floor| floor >= since) {
            return true;
        }

        tables.iter().any(|table_ref| {
            state
                .invalidated_at
                .get(&Self::resolved_key(table_ref))
                .is_some_and(|at| *at >= since)
        })
    }

    #[cfg(test)]
    fn tracked_tables(&self) -> usize {
        self.state.read().invalidated_at.len()
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
    table_invalidations: TableInvalidationClock,
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
            table_invalidations: TableInvalidationClock::default(),
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
        // Validating here, on the read, is what makes stale results unservable
        // rather than merely short-lived. Removing an entry after storing it
        // would still leave it observable in between, and an entry stored
        // *after* an invalidation ran is invisible to that invalidation
        // entirely: `moka` predicates match only entries last modified at or
        // before the predicate was registered, and the Pingora scan has already
        // enumerated its keys. Both are covered by asking, at the moment of
        // use, whether anything this result read has changed since it read it.
        //
        // Going through `get_raw_key_validated` keeps the hit/miss accounting
        // honest: a rejected entry is counted as a miss, which is what the
        // caller experiences.
        // `Fn`, not `FnMut`, so the outcome comes back through a flag.
        let rejected_as_stale = std::sync::atomic::AtomicBool::new(false);
        // Bound to a local rather than passed as `&|…|`: the borrow has to
        // outlive the await, and an inline temporary leaves that to how the
        // async body happens to be lowered. `LruCache::get_raw_key` does the
        // same, where the temporary form does not compile at all.
        let is_valid = |cached_result: &CachedQueryResult| {
            if self.tables_invalidated_since(
                &cached_result.input_tables,
                cached_result.read_started_at,
            ) {
                rejected_as_stale.store(true, std::sync::atomic::Ordering::Relaxed);
                return false;
            }
            true
        };
        let result = self
            .cache
            .get_raw_key_validated(&raw_key.as_u64(), &is_valid)
            .await;

        if rejected_as_stale.load(std::sync::atomic::Ordering::Relaxed) {
            CachedQueryResult::record_stale_rejection();
        }

        Ok(result)
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
    pub async fn invalidate_for_table(&self, table_name: TableReference) -> Result<()> {
        // Record the invalidation before removing entries, never after. A
        // writer that started before this point must be rejected by
        // `tables_invalidated_since`, and stamping afterwards leaves exactly
        // the same gap one step earlier.
        self.table_invalidations
            .mark_invalidated(&table_name, std::time::Instant::now());
        // The invalidation itself is counted by the underlying cache, so that
        // every cache type is counted the same way rather than only this one.
        self.cache.invalidate_for_table(table_name).await
    }

    /// Returns `true` if any of `tables` has been invalidated at or after
    /// `read_started_at`, meaning a result read at that point may predate the
    /// invalidation and must therefore not be served from cache.
    ///
    /// Note this concerns *reusing* a result, never producing one: a query that
    /// read the committed state and returns it to its own caller is correct
    /// whatever happens to the table afterwards, and is never failed or
    /// retried. What this prevents is replaying that snapshot for someone else
    /// once the table has moved on.
    ///
    /// # Deliberately conservative
    ///
    /// Callers pass the instant their *read began*, which is earlier than the
    /// instant the data was actually scanned — `query_start` is captured before
    /// planning. So a query that began at `t1`, and scanned at `t3` *after* an
    /// invalidation at `t2`, read fresh data yet is still rejected, because
    /// `t1 < t2`. That false positive costs one cache entry; the alternative
    /// error costs a wrong answer, so the comparison is biased this way on
    /// purpose.
    ///
    /// Tightening it would mean threading the true snapshot-acquisition instant
    /// out of every `TableProvider` scan and back up to the cache write — a
    /// large amount of plumbing for a marginal hit-rate gain. The imprecision
    /// spans planning plus execution startup (typically milliseconds) against
    /// invalidations arriving on a refresh interval (typically minutes), so it
    /// should rarely fire at all.
    #[must_use]
    pub fn tables_invalidated_since<S: std::hash::BuildHasher>(
        &self,
        tables: &HashSet<TableReference, S>,
        read_started_at: std::time::Instant,
    ) -> bool {
        self.table_invalidations
            .invalidated_since(tables, read_started_at)
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

    /// Re-reports the size and item-count gauges from the cache's current
    /// state. Both accessors drive `moka` housekeeping first, so this reflects
    /// entries already dropped by invalidation or expiry.
    pub async fn report_size_metrics(&self) {
        CachedQueryResult::record_item_count(self.item_count().await);
        CachedQueryResult::record_size(self.size().await);
        CachedQueryResult::record_max_size(self.max_size());
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
                LogicalPlan::Extension(ext)
                    if WRITE_CAPABLE_EXTENSION_NAMES.contains(&ext.node.name()) =>
                {
                    return false;
                }
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
    use std::time::Instant;

    use utils::tests::parse_sql_to_logical_plan;

    use super::*;

    #[test]
    fn resolved_table_match_bare_vs_fully_qualified() {
        let stored: HashSet<TableReference> = HashSet::from([TableReference::bare("customer")]);
        let target = TableReference::full(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA, "customer");
        assert!(resolved_table_match(&stored, &target));

        // Reverse direction: stored fully-qualified, target bare.
        let stored: HashSet<TableReference> = HashSet::from([TableReference::full(
            SPICE_DEFAULT_CATALOG,
            SPICE_DEFAULT_SCHEMA,
            "customer",
        )]);
        let target = TableReference::bare("customer");
        assert!(resolved_table_match(&stored, &target));
    }

    #[test]
    fn resolved_table_match_bare_vs_partially_qualified() {
        let stored: HashSet<TableReference> = HashSet::from([TableReference::bare("customer")]);
        let target = TableReference::partial(SPICE_DEFAULT_SCHEMA, "customer");
        assert!(resolved_table_match(&stored, &target));

        // Different schema must not match a bare name (defaults to public).
        let target = TableReference::partial("other_schema", "customer");
        assert!(!resolved_table_match(&stored, &target));
    }

    #[test]
    fn resolved_table_match_partial_vs_fully_qualified() {
        let stored: HashSet<TableReference> =
            HashSet::from([TableReference::partial(SPICE_DEFAULT_SCHEMA, "customer")]);
        let target = TableReference::full(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA, "customer");
        assert!(resolved_table_match(&stored, &target));

        // Different catalog must not match.
        let target = TableReference::full("other_catalog", SPICE_DEFAULT_SCHEMA, "customer");
        assert!(!resolved_table_match(&stored, &target));
    }

    #[test]
    fn resolved_table_match_different_table_names() {
        let stored: HashSet<TableReference> = HashSet::from([TableReference::bare("customer")]);
        let target = TableReference::bare("orders");
        assert!(!resolved_table_match(&stored, &target));
    }

    #[test]
    fn resolved_table_match_empty_set() {
        let stored: HashSet<TableReference> = HashSet::new();
        let target = TableReference::bare("customer");
        assert!(!resolved_table_match(&stored, &target));
    }

    /// The clock is compared against the instant a read began, so an
    /// invalidation recorded at or after that instant must reject the write and
    /// one recorded strictly before it must not. Synthetic instants keep this
    /// deterministic instead of depending on clock granularity.
    #[test]
    fn table_invalidation_clock_orders_reads_against_invalidations() {
        let clock = TableInvalidationClock::default();
        let base = std::time::Instant::now();
        let tables: HashSet<TableReference> = HashSet::from([TableReference::bare("customer")]);

        // Nothing invalidated yet.
        assert!(!clock.invalidated_since(&tables, base));

        clock.mark_invalidated(&TableReference::bare("customer"), base);

        // A read that began at or after the invalidation is unaffected by it.
        assert!(!clock.invalidated_since(&tables, base + std::time::Duration::from_millis(1)));

        // A read that began before the invalidation must be discarded, and a
        // read beginning in the very same instant is treated the same way.
        assert!(clock.invalidated_since(&tables, base));
        assert!(
            clock.invalidated_since(
                &tables,
                base.checked_sub(std::time::Duration::from_millis(1))
                    .unwrap_or(base)
            )
        );
    }

    /// The clock must key tables the same way [`resolved_table_match`] compares
    /// them, so an invalidation written as `foo` still rejects a result that
    /// recorded `spice.public.foo`, and vice versa.
    #[test]
    fn table_invalidation_clock_resolves_qualification() {
        let base = std::time::Instant::now();
        let read_started_at = base + std::time::Duration::from_millis(1);

        let clock = TableInvalidationClock::default();
        clock.mark_invalidated(&TableReference::bare("customer"), read_started_at);
        let stored: HashSet<TableReference> = HashSet::from([TableReference::full(
            SPICE_DEFAULT_CATALOG,
            SPICE_DEFAULT_SCHEMA,
            "customer",
        )]);
        assert!(clock.invalidated_since(&stored, base));

        let clock = TableInvalidationClock::default();
        clock.mark_invalidated(
            &TableReference::full(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA, "customer"),
            read_started_at,
        );
        let stored: HashSet<TableReference> = HashSet::from([TableReference::bare("customer")]);
        assert!(clock.invalidated_since(&stored, base));

        // A different physical table must not reject the write.
        let other: HashSet<TableReference> = HashSet::from([TableReference::bare("orders")]);
        assert!(!clock.invalidated_since(&other, base));
        let other_schema: HashSet<TableReference> = HashSet::from([TableReference::full(
            SPICE_DEFAULT_CATALOG,
            "other",
            "customer",
        )]);
        assert!(!clock.invalidated_since(&other_schema, base));
    }

    /// The clock must stay bounded no matter how many distinct tables are
    /// invalidated — table identities are not bounded by configuration, since a
    /// client can create and write to new tables where DDL is enabled. Going
    /// over capacity must not open a hole for a query that is still running, so
    /// the collapsed entries are replaced by a conservative floor rather than
    /// simply forgotten.
    #[test]
    fn table_invalidation_clock_stays_bounded_under_table_churn() {
        let clock = TableInvalidationClock::default();
        let base = std::time::Instant::now();
        let read_started_at = base + std::time::Duration::from_millis(1);
        let churn = MAX_TRACKED_TABLES * 2 + 7;

        for i in 0..churn {
            clock.mark_invalidated(
                &TableReference::bare(format!("transient_table_{i}")),
                read_started_at,
            );
        }

        assert!(
            clock.tracked_tables() <= MAX_TRACKED_TABLES,
            "the clock must not grow with all-time table churn, tracked {} of {churn}",
            clock.tracked_tables()
        );

        // Soundness across the collapse: a read that began before those
        // invalidations is still rejected, even for a table whose individual
        // entry was discarded.
        let discarded: HashSet<TableReference> =
            HashSet::from([TableReference::bare("transient_table_0")]);
        assert!(
            clock.invalidated_since(&discarded, base),
            "a discarded table must still reject writes from reads that predate its invalidation"
        );

        // And the collapse is self-healing: a read that began afterwards is
        // unaffected, so caching resumes rather than being wedged off.
        let later = read_started_at + std::time::Duration::from_millis(1);
        assert!(
            !clock.invalidated_since(&discarded, later),
            "a read beginning after every recorded invalidation must still be cacheable"
        );
    }

    /// A table-less result (e.g. `SELECT 1`) records no input tables and must
    /// stay cacheable — an empty set is not "everything".
    #[test]
    fn table_invalidation_clock_ignores_empty_table_set() {
        let clock = TableInvalidationClock::default();
        let base = std::time::Instant::now();
        clock.mark_invalidated(&TableReference::bare("customer"), base);

        let empty: HashSet<TableReference> = HashSet::new();
        assert!(!clock.invalidated_since(&empty, base));
    }

    async fn cached_result_for(table: &str, read_started_at: Instant) -> CachedQueryResult {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        result::query::CachedQueryResult::from_batches(
            vec![arrow::array::RecordBatch::new_empty(Arc::clone(&schema))],
            schema,
            Arc::new(HashSet::from([TableReference::bare(table)])),
            Instant::now(),
            read_started_at,
            None,
        )
        .await
        .expect("valid cached result")
    }

    /// A result stored *after* its table was invalidated must never be served.
    ///
    /// This is the interleaving no write-side check can cover: the invalidation
    /// has already run, so it cannot remove an entry that does not exist yet,
    /// and any check the writer made has already passed. Validating on read
    /// catches it regardless of when the entry was stored.
    #[tokio::test]
    async fn get_raw_key_rejects_entry_whose_table_was_invalidated_after_its_read() {
        let provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        let read_started_at = std::time::Instant::now();
        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidation should succeed");

        let key = RawCacheKey::new(1);
        provider
            .put_raw_key(&key, cached_result_for("customer", read_started_at).await)
            .await
            .expect("cache access should succeed");

        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("cache access should succeed")
                .is_none(),
            "an entry stored after its table was invalidated must never be served"
        );
    }

    /// The other ordering: the entry is already stored when the invalidation
    /// runs. The invalidation removes it directly, and read-time validation
    /// would reject it even if the removal had not yet been applied.
    #[tokio::test]
    async fn invalidate_for_table_removes_an_entry_stored_before_it() {
        let provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        let key = RawCacheKey::new(2);
        provider
            .put_raw_key(
                &key,
                cached_result_for("customer", std::time::Instant::now()).await,
            )
            .await
            .expect("cache access should succeed");
        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("cache access should succeed")
                .is_some(),
            "nothing was invalidated, so the entry must be servable"
        );

        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidation should succeed");

        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("cache access should succeed")
                .is_none(),
            "an entry stored before the invalidation must be removed by it"
        );
    }

    /// The guard must not block ordinary caching: an invalidation of some other
    /// table leaves this entry servable.
    #[tokio::test]
    async fn get_raw_key_serves_entries_of_uninvalidated_tables() {
        let provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        let read_started_at = std::time::Instant::now();
        provider
            .invalidate_for_table(TableReference::bare("orders"))
            .await
            .expect("invalidation should succeed");

        let key = RawCacheKey::new(3);
        provider
            .put_raw_key(&key, cached_result_for("customer", read_started_at).await)
            .await
            .expect("cache access should succeed");

        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("cache access should succeed")
                .is_some(),
            "an entry for an uninvalidated table must remain cached"
        );
    }

    /// `invalidate_for_table` must stamp the clock, not just remove entries —
    /// this is the wiring the write-side guard depends on.
    #[tokio::test]
    async fn invalidate_for_table_records_the_invalidation() {
        let provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        let read_started_at = std::time::Instant::now();
        let tables: HashSet<TableReference> = HashSet::from([TableReference::bare("customer")]);
        assert!(!provider.tables_invalidated_since(&tables, read_started_at));

        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidation should succeed");

        assert!(
            provider.tables_invalidated_since(&tables, read_started_at),
            "a result whose read began before the invalidation must not be stored"
        );
    }

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
    async fn test_run_pending_maintenance_handles_untouched_caches() {
        use crate::result::search::CachedSearchResult;
        use spicepod::component::caching::CacheConfig;

        // A search cache that never receives get/insert traffic, plus a results
        // cache.
        let results = Arc::new(
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid results cache"),
        );
        let search = lru_cache::build_from_config::<CachedSearchResult>(&CacheConfig::default())
            .expect("valid search cache")
            .as_tabled_provider();

        let caching = Caching::new()
            .with_results_cache(results)
            .with_search_cache(search);

        let table = TableReference::bare("never_read");

        // Each call registers a moka invalidation predicate.
        for _ in 0..1_000 {
            caching
                .invalidate_for_table(table.clone())
                .await
                .expect("invalidation should succeed");
        }

        // Maintenance drains the predicates; the caches stay empty and functional.
        caching.run_pending_maintenance().await;

        let results = caching.results.as_ref().expect("results configured");
        assert_eq!(results.item_count().await, 0);
        let search = caching.search.as_ref().expect("search configured");
        assert_eq!(search.item_count().await, 0);

        // Another invalidate + maintenance cycle still works.
        caching
            .invalidate_for_table(table)
            .await
            .expect("invalidation should succeed");
        caching.run_pending_maintenance().await;
    }

    #[tokio::test]
    async fn test_run_pending_maintenance_on_empty_caching_is_noop() {
        // No configured caches: maintenance must be a harmless no-op.
        Caching::new().run_pending_maintenance().await;
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
