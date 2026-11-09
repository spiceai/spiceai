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
use std::hash::BuildHasher;
use std::hash::Hasher;
use std::sync::Arc;

use async_trait::async_trait;
use byte_unit::Byte;
use datafusion::common::TableReference;
use datafusion::logical_expr::LogicalPlan;
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
pub mod intern;
pub mod key;
mod namespace_key;
pub mod result;

pub use backend::CacheBackend;
pub use backend::CacheBackendBuilder;
pub use backend::MokaBackend;
pub use backend::SpiceBackend;

#[cfg(feature = "pingora")]
pub use backend::PingoraBackend;

pub use lru_cache::LruCache;
pub use metrics::CacheMetrics;
pub use metrics::EvictionReason;
pub use metrics::InvalidationMode;
pub use metrics::RevalidationOutcome;
pub use metrics::StaleRejectionReason;
pub use simple_cache::SimpleCache;
use spicepod::component::caching::SQLResultsCacheConfig;
pub use utils::RESPONSE_STATUS_COLUMN;
pub use utils::batches_boundable;
pub use utils::batches_cacheable;
pub use utils::filter_transient_error_responses;
pub use utils::get_logical_plan_input_tables;
pub use utils::plan_saw_transient_http_failure;
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

    #[snafu(display(
        "The query result ({size} bytes) is larger than the SQL results cache max_size ({max_size} bytes), so it was not cached"
    ))]
    ResultLargerThanMaxSize { size: u64, max_size: u64 },
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

    /// Whether replacing this value in the store should keep the entry's
    /// remaining TTL rather than starting a new one.
    ///
    /// Used when rewriting a resident results-cache entry in place (recording
    /// a decode hit, or promoting Encoded → Raw): the payload is the same
    /// result, so extending its life would make a hit reset `item_ttl`. New
    /// results (a miss store, a revalidation) leave this `false`.
    fn keep_remaining_ttl(&self) -> bool {
        false
    }
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
    async fn get_raw_key(&self, key: &u64) -> Option<std::sync::Arc<V>>;
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
    ) -> Option<std::sync::Arc<V>>;
    async fn put_raw_key(&self, key: &u64, value: V);
    /// Store `value`, which the caller has already sized: `weight` must be what
    /// [`Sizeable::get_memory_size`] returns for it. A cache that bills entries
    /// by size can then admit a small value on the calling task without walking
    /// it again; one that bounds entries by count ignores `weight`.
    async fn put_raw_key_with_weight(&self, key: &u64, value: V, weight: usize);
    /// Replace the value at `key` only when `should_replace` accepts the
    /// currently stored value. See [`crate::backend::CacheBackend::replace_if`].
    async fn replace_if(
        &self,
        key: &u64,
        value: V,
        should_replace: &(dyn for<'v> Fn(&'v V) -> bool + Send + Sync),
    ) -> bool;
    /// Insert `value`, or replace the resident, only when `admit` accepts the
    /// current value (`None` when the key is empty).
    ///
    /// Deliberately has no default. A default that always inserted would let a
    /// slower read overwrite a later one on any wrapper that forgot to forward
    /// the method. `weight` is what [`Sizeable::get_memory_size`] returns for
    /// `value`; a cache that bounds entries by count ignores it.
    async fn put_if(
        &self,
        key: &u64,
        value: V,
        weight: usize,
        admit: &(dyn for<'v> Fn(Option<&'v V>) -> bool + Send + Sync),
    ) -> bool;
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

    /// Returns `true` if any of `tables` was invalidated at or after `since`.
    ///
    /// Deliberately has no default. A provider without a table-change clock
    /// would inherit `false`, and `false` here is the answer that *serves* a
    /// result — `SearchEngine::search_with_cache` asks this both before
    /// serving a hit and before publishing a completed search, so a silent
    /// `false` is a stale result served rather than a missing optimisation.
    /// Requiring the method makes wiring such a provider into the search path
    /// a compile error instead.
    fn tables_changed_since(
        &self,
        tables: &HashSet<TableReference>,
        since: std::time::Instant,
    ) -> bool;
}

#[derive(Clone)]
pub enum HashBuilder {
    Ahash(ahash::RandomState),
    Siphash(std::hash::RandomState),
    Blake3,
    XxHash3(std::hash::BuildHasherDefault<twox_hash::XxHash3_64>),
    XxHash64(std::hash::BuildHasherDefault<twox_hash::XxHash64>),
    XxHash128,
}

impl std::hash::BuildHasher for HashBuilder {
    type Hasher = KeyHasher;

    fn build_hasher(&self) -> Self::Hasher {
        match self {
            HashBuilder::Ahash(builder) => KeyHasher::Ahash(builder.build_hasher()),
            HashBuilder::Siphash(builder) => KeyHasher::Siphash(builder.build_hasher()),
            HashBuilder::Blake3 => KeyHasher::Blake3(Box::new(blake3_compat::Blake3Wrapper::new())),
            HashBuilder::XxHash3(builder) => KeyHasher::XxHash3(builder.build_hasher()),
            HashBuilder::XxHash64(builder) => KeyHasher::XxHash64(builder.build_hasher()),
            HashBuilder::XxHash128 => {
                KeyHasher::XxHash128(xxhash_compat::XxHash3_128Wrapper::new())
            }
        }
    }
}

/// Concrete hasher for [`HashBuilder`].
///
/// `HashBuilder` is already an enum; boxing the hasher it builds was what
/// turned every plan-node write into a virtual call plus a heap allocation.
/// Match dispatch keeps each algorithm's write path — including `ahash`'s
/// integer folding — so a plan key is the same value as hashing the plan
/// write by write.
pub enum KeyHasher {
    Ahash(ahash::AHasher),
    Siphash(std::collections::hash_map::DefaultHasher),
    Blake3(Box<blake3_compat::Blake3Wrapper>),
    XxHash3(twox_hash::XxHash3_64),
    XxHash64(twox_hash::XxHash64),
    XxHash128(xxhash_compat::XxHash3_128Wrapper),
}

macro_rules! dispatch_key_hasher {
    ($self:expr, $method:ident $(, $arg:expr)* $(,)?) => {
        match $self {
            Self::Ahash(hasher) => hasher.$method($($arg),*),
            Self::Siphash(hasher) => hasher.$method($($arg),*),
            Self::Blake3(hasher) => hasher.$method($($arg),*),
            Self::XxHash3(hasher) => hasher.$method($($arg),*),
            Self::XxHash64(hasher) => hasher.$method($($arg),*),
            Self::XxHash128(hasher) => hasher.$method($($arg),*),
        }
    };
}

impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        dispatch_key_hasher!(self, finish)
    }

    fn write(&mut self, bytes: &[u8]) {
        dispatch_key_hasher!(self, write, bytes);
    }

    fn write_u8(&mut self, i: u8) {
        dispatch_key_hasher!(self, write_u8, i);
    }

    fn write_u16(&mut self, i: u16) {
        dispatch_key_hasher!(self, write_u16, i);
    }

    fn write_u32(&mut self, i: u32) {
        dispatch_key_hasher!(self, write_u32, i);
    }

    fn write_u64(&mut self, i: u64) {
        dispatch_key_hasher!(self, write_u64, i);
    }

    fn write_u128(&mut self, i: u128) {
        dispatch_key_hasher!(self, write_u128, i);
    }

    fn write_usize(&mut self, i: usize) {
        dispatch_key_hasher!(self, write_usize, i);
    }

    fn write_i8(&mut self, i: i8) {
        dispatch_key_hasher!(self, write_i8, i);
    }

    fn write_i16(&mut self, i: i16) {
        dispatch_key_hasher!(self, write_i16, i);
    }

    fn write_i32(&mut self, i: i32) {
        dispatch_key_hasher!(self, write_i32, i);
    }

    fn write_i64(&mut self, i: i64) {
        dispatch_key_hasher!(self, write_i64, i);
    }

    fn write_i128(&mut self, i: i128) {
        dispatch_key_hasher!(self, write_i128, i);
    }

    fn write_isize(&mut self, i: isize) {
        dispatch_key_hasher!(self, write_isize, i);
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
        // A cache key is the bare 64-bit hash, so a 32-bit hash would leave half of it
        // zero and let distinct queries share an entry; `xxh32` keys with XXH64.
        HashingAlgorithm::XXH32 | HashingAlgorithm::XXH64 => {
            Ok(HashBuilder::XxHash64(std::hash::BuildHasherDefault::<
                twox_hash::XxHash64,
            >::default()))
        }
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

/// Reports the first failure among invalidations that have all already run.
///
/// The caches invalidated for one table are independent of each other — each keeps
/// its own change clock — so a `?` between them would leave the later ones serving
/// the table that the earlier one was told had gone. Every arm is therefore awaited
/// before any error is returned, and the caller still sees a failure.
fn first_error<const N: usize>(outcomes: [Result<()>; N]) -> Result<()> {
    outcomes.into_iter().find(Result::is_err).unwrap_or(Ok(()))
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

    /// Hard-invalidates all configured caches for the specified table.
    ///
    /// # Errors
    ///
    /// If the cache invalidation fails for any of the caches.
    pub async fn invalidate_for_table(&self, table_ref: TableReference) -> Result<()> {
        let results = match &self.results {
            Some(results_cache) => results_cache.invalidate_for_table(table_ref.clone()).await,
            None => Ok(()),
        };
        first_error([results, self.invalidate_for_refresh(table_ref).await])
    }

    /// Invalidates plans and search results after a refresh.
    ///
    /// SQL results deliberately age out on `item_ttl` and
    /// `stale_while_revalidate_ttl`, so a refresh does not turn every dependent
    /// entry into a miss at the same instant.
    ///
    /// # Errors
    /// Returns an error if a plans or search cache invalidation fails.
    pub async fn invalidate_for_refresh(&self, table_ref: TableReference) -> Result<()> {
        let plans = match &self.plans {
            Some(plans_cache) => plans_cache.invalidate_for_table(table_ref.clone()).await,
            None => Ok(()),
        };
        let search = match &self.search {
            Some(search_cache) => search_cache.invalidate_for_table(table_ref).await,
            None => Ok(()),
        };
        first_error([plans, search])
    }

    /// Drives housekeeping on every configured cache. SQL results expire stale
    /// entries via `run_pending_tasks` (Spice shard walk on the blocking pool)
    /// and then refresh size gauges. Plans, search, and embeddings run
    /// `checkpoint`. Expired entries on a cache with no `get`/`insert` traffic
    /// are only reclaimed when this runs.
    pub async fn run_pending_maintenance(&self) {
        // The interner pools are reclaimed here rather than by the runtime,
        // because this crate is the only thing that populates them: every value
        // they share is held by a cache entry. A pool only reclaims a shard
        // that interning happens to touch, so a shard that goes quiet after a
        // burst of one-off query shapes would otherwise hold its dead rows and
        // their capacity for the process lifetime.
        crate::intern::sweep_all();

        if let Some(results) = &self.results {
            results.run_pending_tasks().await;
            // The size and item-count gauges are otherwise only refreshed when
            // something is *inserted*, so after client writes or expiry remove a large
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

/// Records, per table, when that table's contents last changed.
///
/// Client writes (DML and Flight `do_put`), dataset reload and unload stamp
/// this clock. Accelerated refresh, CDC and retention do not.
///
/// Eviction removes resident entries, but an in-flight query can publish after
/// eviction completes. Every hit therefore consults the clock: an entry whose
/// read began at or before a table's stamp is never served, regardless of when
/// it was stored. Checking only before storage would leave a gap between that
/// check and publication. Reads outnumber changes, so an `RwLock` keeps table
/// lookups allocation-free without a lock-free map.
///
/// Memory is bounded at [`MAX_TRACKED_TABLES`] regardless of how many distinct
/// tables change over a process lifetime. Table identities are not
/// bounded by configuration — where DDL is enabled a client can create and
/// write to arbitrarily many tables, and dataset names also churn across
/// hot-reloads — so the map cannot be allowed to grow with all-time history.
/// Reclamation must not open a hole for a still-running query, which rules out
/// discarding by age: with the default one-second `item_ttl`, a query easily
/// outlives any age-based cutoff. Instead, over-capacity collapses the map into
/// a single conservative `discarded_floor`, which rejects *every* write whose
/// read began before that point. That is sound (it can only over-reject) and
/// self-healing (later changes repopulate per-table entries), at the cost
/// of some lost cache entries in the moments after a collapse.
#[derive(Default)]
pub(crate) struct TableChangeClock {
    state: parking_lot::RwLock<TableChangeState>,
}

/// Upper bound on individually-tracked tables. Each entry is a `u64` hash of
/// the resolved table name plus an `Instant`, so the clock stays in the tens of
/// kilobytes while the bound sits far above the table count of any ordinary
/// deployment — a collapse should be reachable only under deliberate churn.
const MAX_TRACKED_TABLES: usize = 4096;

#[derive(Default)]
pub(crate) struct TableChangeState {
    changed_at: std::collections::HashMap<u64, std::time::Instant>,
    /// Stands in for every table dropped from `changed_at`. Holds the
    /// newest instant among the dropped entries, which is `>=` the true
    /// change instant of each of them, so treating it as their stamp can
    /// only reject writes that a per-table entry would have allowed.
    discarded_floor: Option<std::time::Instant>,
}

impl TableChangeClock {
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
        use std::hash::Hasher;

        // `XxHash64` keeps its state inline, where the streaming `XxHash3_64`
        // allocates it, and this runs for every table on every cache hit. The key
        // only has to agree with itself within this process.
        let mut hasher = twox_hash::XxHash64::with_seed(0);
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

    pub(crate) fn record_change(&self, table_ref: &TableReference, at: std::time::Instant) {
        let key = Self::resolved_key(table_ref);
        let mut state = self.state.write();

        if state.changed_at.len() >= MAX_TRACKED_TABLES && !state.changed_at.contains_key(&key) {
            let newest = state.changed_at.values().copied().max();
            state.discarded_floor = state.discarded_floor.max(newest);
            state.changed_at.clear();
        }

        // Callers sample `at` before this lock. An earlier invalidation can
        // therefore land last and must not rewind a newer mark — that would
        // let a read started between the two stamps pass `changed_since`.
        state
            .changed_at
            .entry(key)
            .and_modify(|recorded| *recorded = (*recorded).max(at))
            .or_insert(at);
    }

    /// Records a change to every table, so a result whose read began at or before
    /// `at` is never stored or served, whatever it read.
    pub(crate) fn record_change_everywhere(&self, at: std::time::Instant) {
        let mut state = self.state.write();
        state.discarded_floor = state.discarded_floor.max(Some(at));
    }

    /// Returns the newest instant at which any of `tables` changed, including a
    /// change recorded for every table, or `None` if there is none.
    ///
    /// The newest is what a result reading all of them is measured against: any
    /// one of its tables moving on is enough to leave the result behind, so the
    /// most recent such move is the one that matters.
    fn latest_change<S: std::hash::BuildHasher>(
        &self,
        tables: &HashSet<TableReference, S>,
    ) -> Option<std::time::Instant> {
        let state = self.state.read();

        // Any table whose own entry was collapsed away is covered by the floor,
        // which is `>=` the true instant of every entry it replaced. A result that
        // read no table depends only on definitions, which the floor also covers.
        let mut latest = state.discarded_floor;
        for table_ref in tables {
            latest = latest.max(
                state
                    .changed_at
                    .get(&Self::resolved_key(table_ref))
                    .copied(),
            );
        }
        latest
    }

    /// Returns `true` if any of `tables` changed at or after `since`.
    ///
    /// Ties count as changed: a change recorded in the same instant
    /// as the read began must be assumed to have happened first, since serving
    /// stale data is worse than losing a cache entry.
    pub(crate) fn changed_since<S: std::hash::BuildHasher>(
        &self,
        tables: &HashSet<TableReference, S>,
        since: std::time::Instant,
    ) -> bool {
        self.latest_change(tables).is_some_and(|at| at >= since)
    }

    #[cfg(test)]
    fn tracked_tables(&self) -> usize {
        self.state.read().changed_at.len()
    }
}

// TODO: sunset ``QueryResultsCacheProvider`` in favor of ``CacheProvider``?
pub struct QueryResultsCacheProvider {
    cache: Arc<dyn TabledCacheProvider<CachedQueryResult> + Send + Sync>,
    cache_max_size: u64,
    ttl: std::time::Duration,
    max_servable_age: std::time::Duration,
    stale_while_revalidate_ttl: Option<std::time::Duration>,

    ignore_schemas: Box<[Box<str>]>,
    encoder: Option<Arc<dyn encoding::Encoder>>,
    encoding: spicepod::component::caching::Encoding,
    hashing_algorithm: spicepod::component::caching::HashingAlgorithm,
    /// The builder the results-cache keys are hashed with. Kept beside the
    /// store so [`Self::hasher`] can return a [`KeyHasher`] without boxing
    /// through [`HashProvider`] — `ahash` / siphash keys are keyed from this
    /// instance, not a fresh [`get_hash_builder`] call.
    hash_builder: HashBuilder,
    table_changes: TableChangeClock,
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
        let max_servable_age = ttl.saturating_add(stale_while_revalidate_ttl.unwrap_or_default());
        let cache = Arc::new(LruCache::new(
            cache_max_size,
            max_servable_age,
            hash_builder.clone(),
            config.caching_policy,
            config.engine,
        ));

        let encoder = encoding::get_encoder(config.encoding);

        let cache_provider = QueryResultsCacheProvider {
            cache,
            cache_max_size,
            ttl,
            max_servable_age,
            stale_while_revalidate_ttl,
            ignore_schemas,
            encoder,
            encoding: config.encoding,
            hashing_algorithm: config.hashing_algorithm,
            hash_builder,
            table_changes: TableChangeClock::default(),
        };

        Ok(cache_provider)
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn get(&self, key: CacheKey<'_>) -> Result<Option<CachedQueryResult>> {
        let raw_key = key.as_raw_key(self.hasher());
        self.get_raw_key(&raw_key).await
    }

    /// Looks `raw_key` up, accepting any entry the cache may still serve: one no
    /// older than `item_ttl + stale_while_revalidate_ttl`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn get_raw_key(&self, raw_key: &RawCacheKey) -> Result<Option<CachedQueryResult>> {
        self.get_raw_key_within(raw_key, self.max_servable_age)
            .await
    }

    /// Looks `raw_key` up, accepting only an entry no older than `max_age`, capped
    /// at `item_ttl + stale_while_revalidate_ttl`.
    ///
    /// A request that accepts less than the cache serves (a `max-stale` shorter
    /// than `stale_while_revalidate_ttl`) passes its own bound here, so an entry it
    /// would turn away is rejected, and counted as a miss, by the lookup itself.
    ///
    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn get_raw_key_within(
        &self,
        raw_key: &RawCacheKey,
        max_age: std::time::Duration,
    ) -> Result<Option<CachedQueryResult>> {
        let max_age = max_age.min(self.max_servable_age);
        // Validate on lookup so a result published after an invalidation cannot be reused.
        // Both backends call the predicate at most once, preserving rejection accounting.
        let is_valid = |entry: &CachedQueryResult| {
            if self.tables_changed_since(&entry.input_tables, entry.read_started_at) {
                CachedQueryResult::record_stale_rejection(StaleRejectionReason::WriteDuringRead);
                return false;
            }
            let now = std::time::Instant::now();
            now.saturating_duration_since(entry.read_started_at) <= max_age
        };
        Ok(self
            .cache
            .get_raw_key_validated(&raw_key.as_u64(), &is_valid)
            .await
            .map(std::sync::Arc::unwrap_or_clone))
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn put(&self, key: CacheKey<'_>, result: CachedQueryResult) -> Result<()> {
        let raw_key = key.as_raw_key(self.hasher());
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

    /// Like [`Self::put_raw_key`], for a caller that has already sized
    /// `result`: `weight` must be `result.get_memory_size()`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn put_raw_key_with_weight(
        &self,
        raw_key: &RawCacheKey,
        result: CachedQueryResult,
        weight: usize,
    ) -> Result<()> {
        self.cache
            .put_raw_key_with_weight(&raw_key.as_u64(), result, weight)
            .await;
        Ok(())
    }

    /// Whether a result read at `read_started_at` could be served if stored
    /// now: the table clock must allow it and its age must not exceed the
    /// maximum servable age (TTL plus the stale-while-revalidate window).
    #[must_use]
    pub fn is_servable<S: std::hash::BuildHasher>(
        &self,
        tables: &HashSet<TableReference, S>,
        read_started_at: std::time::Instant,
    ) -> bool {
        let now = std::time::Instant::now();
        !self.tables_changed_since(tables, read_started_at)
            && now.saturating_duration_since(read_started_at) <= self.max_servable_age
    }

    /// Stores `result`, ruled the way a hit is. Returns `false` only when no
    /// lookup could serve it; keeping an entry that read later counts as stored.
    ///
    /// Results invalidated during their read or past the read-start TTL plus
    /// stale window are skipped. Timestamp-ordered insertion preserves a newer
    /// resident. `weight` is `result.get_memory_size()` when already computed.
    ///
    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache, or
    /// [`Error::ResultLargerThanMaxSize`] when `result` is heavier than `max_size`.
    pub async fn store_raw_key(
        &self,
        raw_key: &RawCacheKey,
        result: CachedQueryResult,
        weight: Option<usize>,
    ) -> Result<bool> {
        if !self.is_servable(&result.input_tables, result.read_started_at) {
            tracing::debug!(
                "Query result is invalidated or past its maximum servable age, skipping cache storage"
            );
            return Ok(false);
        }
        let weight = weight.unwrap_or_else(|| result.get_memory_size());
        // The cache refuses a result heavier than `max_size` before it consults
        // the resident. That refusal is neither storage nor invalidation, and the
        // key may stay empty, so it is an error the caller can tell apart.
        let weight_bytes = u64::try_from(weight).unwrap_or(u64::MAX);
        if weight_bytes > self.cache_max_size {
            return ResultLargerThanMaxSizeSnafu {
                size: weight_bytes,
                max_size: self.cache_max_size,
            }
            .fail();
        }
        let stored = self
            .put_raw_key_unless_older_read(raw_key, result, weight)
            .await?;
        if !stored {
            tracing::debug!(
                "A result from a query that began later is already cached under this key, keeping it"
            );
        }
        Ok(true)
    }

    /// Decode `result` for serving. The first successful decode of an encoded
    /// entry leaves it encoded, so a one-shot key does not inflate to Raw. The
    /// second successful decode replaces the stored value with
    /// [`result::query::CachedData::Raw`] so a later fetch of `raw_key` is an
    /// `Arc::clone` and the weigher bills the decoded size.
    ///
    /// Concurrent `records()` on one fetch share a decode (`OnceCell`). The
    /// stored cell is cleared after the first hit so the second fetch decodes
    /// again. The replace is skipped when a newer result already occupies the
    /// key, or when the decoded size would not fit `max_size`. That last case
    /// keeps the encoded bytes so the entry is not evicted by the promotion
    /// itself, and rewrites the stored value with a fresh empty decode cell so
    /// the decoded batches (still held by the returned `Arc`) are not retained
    /// off-budget.
    ///
    /// # Errors
    ///
    /// Returns an error if decoding fails.
    pub async fn records(
        &self,
        raw_key: &RawCacheKey,
        result: &CachedQueryResult,
    ) -> std::result::Result<crate::result::query::CachedBatches, encoding::Error> {
        let records = result.records().await?;
        self.after_encoded_decode(raw_key, result, &records).await;
        Ok(records)
    }

    async fn after_encoded_decode(
        &self,
        raw_key: &RawCacheKey,
        result: &CachedQueryResult,
        records: &crate::result::query::CachedBatches,
    ) {
        match result.encoded_decode_hits() {
            Some(0) => {
                let recorded = result.with_recorded_decode_hit();
                self.cache
                    .replace_if(
                        &raw_key.as_u64(),
                        recorded,
                        &|current: &CachedQueryResult| {
                            current.is_same_generation(result)
                                && current.encoded_decode_hits() == Some(0)
                        },
                    )
                    .await;
            }
            Some(1) => self.promote_encoded_to_raw(raw_key, result, records).await,
            _ => {}
        }
    }

    async fn promote_encoded_to_raw(
        &self,
        raw_key: &RawCacheKey,
        result: &CachedQueryResult,
        records: &crate::result::query::CachedBatches,
    ) {
        let promoted = result.to_promoted_raw(Arc::clone(records));
        let promoted_size = u64::try_from(promoted.get_memory_size()).unwrap_or(u64::MAX);
        if promoted_size > self.cache_max_size {
            tracing::debug!(
                promoted_size,
                cache_max_size = self.cache_max_size,
                "Skipping encoded-to-raw promotion because the decoded entry exceeds cache max size"
            );
            // Keep the encoded payload (promotion must not evict the only
            // copy) and the caller's served `Arc`, but drop the stored
            // decode cell so `memory_size()` is not under-billing resident
            // decoded batches.
            let reset = result.with_cleared_decode_cell();
            self.cache
                .replace_if(&raw_key.as_u64(), reset, &|current: &CachedQueryResult| {
                    current.is_same_generation(result) && current.encoded_decode_hits() == Some(1)
                })
                .await;
            return;
        }

        self.cache
            .replace_if(
                &raw_key.as_u64(),
                promoted,
                &|current: &CachedQueryResult| {
                    current.is_same_generation(result) && current.encoded_decode_hits() == Some(1)
                },
            )
            .await;
    }

    /// Drops every cached result.
    ///
    /// For a change no table change records: a result's key is the query, not
    /// the definitions of the functions, views or catalogs it read through, so when
    /// a Spicepod reload redefines or removes one, any entry may hold rows the old
    /// definition computed. A `cache_key_type: sql` hit is answered before
    /// planning, so discarding cached plans does not reach it; this does.
    pub async fn invalidate_all(&self) {
        // Stamp before evicting, so a query still running when the definitions
        // changed cannot store its result once the eviction has finished.
        self.table_changes
            .record_change_everywhere(std::time::Instant::now());
        self.cache.invalidate_all().await;
    }

    /// Stamps the table-change clock and evicts every result that read the table.
    /// Entries whose read began at or before the stamp are never served.
    ///
    /// # Errors
    /// Returns an error if cache invalidation fails.
    pub async fn invalidate_for_table(&self, table_name: TableReference) -> Result<()> {
        // Stamp before evicting: a writer whose read began before this point must
        // be rejected even if it publishes after the eviction has finished.
        self.table_changes
            .record_change(&table_name, std::time::Instant::now());
        CachedQueryResult::record_table_invalidation(InvalidationMode::Evict);
        // The entries each invalidation drops are counted by the underlying
        // cache, so that every cache type is counted the same way rather than
        // only this one.
        self.cache.invalidate_for_table(table_name).await
    }

    /// Returns `true` if any of `tables` changed at or after `read_started_at`,
    /// meaning a result read at that point may predate the change and so cannot
    /// be stored or served as a cache entry.
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
    /// large amount of plumbing. Accelerated refresh and CDC updates do not
    /// stamp this clock; their cached results expire through the TTL policy.
    #[must_use]
    pub fn tables_changed_since<S: std::hash::BuildHasher>(
        &self,
        tables: &HashSet<TableReference, S>,
        read_started_at: std::time::Instant,
    ) -> bool {
        self.table_changes.changed_since(tables, read_started_at)
    }

    /// Store `result` unless a resident of `raw_key` began reading at least as
    /// late as `result`.
    ///
    /// The comparison and the write share the shard lock, so a slower result
    /// cannot overwrite a result whose read began later. Lookup separately
    /// checks the table clock and maximum servable age.
    ///
    /// # Errors
    ///
    /// Will return `Err` if the cache write fails.
    pub async fn put_raw_key_unless_older_read(
        &self,
        raw_key: &RawCacheKey,
        result: CachedQueryResult,
        weight: usize,
    ) -> Result<bool> {
        let read_started_at = result.read_started_at;
        let stored = self
            .cache
            .put_if(&raw_key.as_u64(), result, weight, &|current| {
                current.is_none_or(|existing| existing.read_started_at < read_started_at)
            })
            .await;
        Ok(stored)
    }

    #[must_use]
    pub fn max_size(&self) -> u64 {
        self.cache_max_size
    }

    #[must_use]
    pub fn hasher(&self) -> KeyHasher {
        self.hash_builder.build_hasher()
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

    /// The oldest an entry may be and still be served, measured from when its
    /// query started: `item_ttl` plus `stale_while_revalidate_ttl`.
    #[must_use]
    pub fn max_servable_age(&self) -> std::time::Duration {
        self.max_servable_age
    }

    /// Runs pending cache maintenance tasks (e.g., eviction of expired entries).
    /// This is useful in tests to ensure eviction happens immediately.
    pub async fn run_pending_tasks(&self) {
        self.cache.checkpoint().await;
    }

    /// Re-reports the size and item-count gauges from the cache's current
    /// state. Size and item count read the Spice backend's live counters (no
    /// expiry scan); call `run_pending_tasks` / `checkpoint` first when the
    /// gauges should exclude unobserved TTL entries.
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

    /// Whether results of `plan` may be stored in / served from the SQL
    /// results cache.
    ///
    /// Caching policy for non-deterministic plans: only plans whose functions
    /// are all `Volatility::Immutable` are cacheable. A plan is never cached if
    /// any scalar, aggregate, or window function (including in subqueries) is
    /// `Volatile` (`random()`, `uuid()`, ...) or `Stable`. `Stable` covers the
    /// time functions (`now()`, `current_date`, `current_time`, ...; see
    /// [`NON_CACHEABLE_TIME_FUNCTIONS`]) and remote HTTP UDFs, which are capped
    /// at `Stable`. They are constant within one statement, but a cached result
    /// would replay stale output across statements. A plan that scans a
    /// `Temporary` table, anywhere including in a subquery, is never cached
    /// either: that is how a table function whose rows can change between
    /// executions is planned (remote and search table functions, and SQL and
    /// WASM table functions not declared `immutable`). Such queries report no
    /// results-cache status.
    #[must_use]
    pub fn cache_is_enabled_for_plan(&self, plan: &LogicalPlan) -> bool {
        use datafusion::common::tree_node::TreeNodeRecursion;

        // `apply_with_subqueries`, not an `inputs()` walk: a subquery in an
        // expression (`IN (SELECT ...)`, `EXISTS (...)`, a scalar subquery)
        // is held in the node's expressions, so `inputs()` never reaches the
        // scans inside it (see `utils::get_logical_plan_input_tables`).
        let mut cacheable = true;
        // The closure is infallible, so the returned Result cannot be an error.
        let _ = plan.apply_with_subqueries(|node| {
            if self.plan_node_is_cacheable(node) {
                Ok(TreeNodeRecursion::Continue)
            } else {
                cacheable = false;
                Ok(TreeNodeRecursion::Stop)
            }
        });

        cacheable && !plan_is_non_deterministic(plan)
    }

    /// Checks one plan node, not its inputs or subqueries.
    fn plan_node_is_cacheable(&self, node: &LogicalPlan) -> bool {
        match node {
            LogicalPlan::TableScan(source, ..) => {
                // Table-function providers whose rows can change between
                // executions (remote and search UDTFs, and SQL and WASM table
                // functions not declared `immutable`) are planned as
                // `Temporary` scans; never cache them. A recursive CTE's work
                // table is `Temporary` too, but it holds only the rows the
                // query itself derives, so it does not decide.
                if source.source.table_type() == datafusion::datasource::TableType::Temporary
                    && !is_cte_work_table(source)
                {
                    return false;
                }
                let Some(schema) = source.table_name.schema() else {
                    return true;
                };
                !self
                    .ignore_schemas
                    .iter()
                    .any(|ignore_schema| *schema == **ignore_schema)
            }
            LogicalPlan::Explain { .. }
            | LogicalPlan::Analyze { .. }
            | LogicalPlan::DescribeTable { .. }
            | LogicalPlan::Ddl(..)
            | LogicalPlan::Dml(..)
            | LogicalPlan::Copy { .. }
            | LogicalPlan::Statement(..) => false,
            LogicalPlan::Extension(ext) => {
                !WRITE_CAPABLE_EXTENSION_NAMES.contains(&ext.node.name())
            }
            _ => true,
        }
    }
}

/// Whether `scan` reads the work table of a recursive CTE, which `DataFusion`
/// plans as a `Temporary` scan of the rows the recursion has derived so far.
fn is_cte_work_table(scan: &datafusion::logical_expr::TableScan) -> bool {
    datafusion::datasource::source_as_provider(&scan.source).is_ok_and(|provider| {
        provider
            .downcast_ref::<datafusion::datasource::cte_worktable::CteWorkTable>()
            .is_some()
    })
}

/// Time functions that `DataFusion` marks `Volatility::Stable` (the value is
/// fixed for one statement but changes between statements). Caching their
/// results would serve a stale clock, so they are treated as uncacheable.
pub const NON_CACHEABLE_TIME_FUNCTIONS: &[&str] = &[
    "now",
    "current_timestamp",
    "current_date",
    "current_time",
    "localtime",
    "localtimestamp",
    "today",
];

/// Returns `true` when `plan` (including any subquery) contains a function
/// whose result can differ between two executions of the same plan: any
/// function that is not `Volatility::Immutable` (`random()`, `uuid()`,
/// `now()`, remote UDFs, ...) or one of [`NON_CACHEABLE_TIME_FUNCTIONS`]. Results of such plans must not be served
/// from the SQL results cache.
#[must_use]
pub fn plan_is_non_deterministic(plan: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::TreeNodeRecursion;

    let mut found = false;
    let _ = plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            if expr_is_non_deterministic(expr) {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
    });
    found
}

fn is_time_function_name(name: &str) -> bool {
    NON_CACHEABLE_TIME_FUNCTIONS
        .iter()
        .any(|f| f.eq_ignore_ascii_case(name))
}

/// Checks a single expression tree (not descending into subquery plans,
/// which [`plan_is_non_deterministic`] visits itself).
fn expr_is_non_deterministic(expr: &datafusion::logical_expr::Expr) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::{Expr, Volatility};

    let mut found = false;
    let _ = expr.apply(|e| {
        let hit = match e {
            Expr::ScalarFunction(f) => {
                f.func.signature().volatility != Volatility::Immutable
                    || is_time_function_name(f.func.name())
                    || f.func.aliases().iter().any(|a| is_time_function_name(a))
            }
            Expr::AggregateFunction(f) => f.func.signature().volatility != Volatility::Immutable,
            Expr::WindowFunction(w) => w.fun.signature().volatility != Volatility::Immutable,
            _ => false,
        };
        if hit {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    found
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
        let clock = TableChangeClock::default();
        let base = std::time::Instant::now();
        let tables: HashSet<TableReference> = HashSet::from([TableReference::bare("customer")]);

        // Nothing invalidated yet.
        assert!(!clock.changed_since(&tables, base));

        clock.record_change(&TableReference::bare("customer"), base);

        // A read that began at or after the invalidation is unaffected by it.
        assert!(!clock.changed_since(&tables, base + std::time::Duration::from_millis(1)));

        // A read that began before the invalidation must be discarded, and a
        // read beginning in the very same instant is treated the same way.
        assert!(clock.changed_since(&tables, base));
        assert!(
            clock.changed_since(
                &tables,
                base.checked_sub(std::time::Duration::from_millis(1))
                    .unwrap_or(base)
            )
        );
    }

    /// Both invalidation callers sample `Instant::now()` before the clock
    /// lock, so an earlier stamp can be written after a later one. The clock
    /// must keep the newest mark: a read started between those instants has
    /// to see the later invalidation.
    #[test]
    fn table_invalidation_clock_keeps_the_newest_mark() {
        let clock = TableChangeClock::default();
        let base = std::time::Instant::now();
        let earlier = base + std::time::Duration::from_millis(10);
        let between = base + std::time::Duration::from_millis(15);
        let later = base + std::time::Duration::from_millis(20);
        let tables: HashSet<TableReference> = HashSet::from([TableReference::bare("customer")]);

        clock.record_change(&TableReference::bare("customer"), later);
        clock.record_change(&TableReference::bare("customer"), earlier);

        assert!(
            clock.changed_since(&tables, between),
            "a later-arriving earlier stamp must not hide an invalidation a read already missed"
        );
        assert_eq!(clock.latest_change(&tables), Some(later));
    }

    /// The clock must key tables the same way [`resolved_table_match`] compares
    /// them, so an invalidation written as `foo` still rejects a result that
    /// recorded `spice.public.foo`, and vice versa.
    #[test]
    fn table_invalidation_clock_resolves_qualification() {
        let base = std::time::Instant::now();
        let read_started_at = base + std::time::Duration::from_millis(1);

        let clock = TableChangeClock::default();
        clock.record_change(&TableReference::bare("customer"), read_started_at);
        let stored: HashSet<TableReference> = HashSet::from([TableReference::full(
            SPICE_DEFAULT_CATALOG,
            SPICE_DEFAULT_SCHEMA,
            "customer",
        )]);
        assert!(clock.changed_since(&stored, base));

        let clock = TableChangeClock::default();
        clock.record_change(
            &TableReference::full(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA, "customer"),
            read_started_at,
        );
        let stored: HashSet<TableReference> = HashSet::from([TableReference::bare("customer")]);
        assert!(clock.changed_since(&stored, base));

        // A different physical table must not reject the write.
        let other: HashSet<TableReference> = HashSet::from([TableReference::bare("orders")]);
        assert!(!clock.changed_since(&other, base));
        let other_schema: HashSet<TableReference> = HashSet::from([TableReference::full(
            SPICE_DEFAULT_CATALOG,
            "other",
            "customer",
        )]);
        assert!(!clock.changed_since(&other_schema, base));
    }

    /// The clock must stay bounded no matter how many distinct tables are
    /// invalidated — table identities are not bounded by configuration, since a
    /// client can create and write to new tables where DDL is enabled. Going
    /// over capacity must not open a hole for a query that is still running, so
    /// the collapsed entries are replaced by a conservative floor rather than
    /// simply forgotten.
    #[test]
    fn table_invalidation_clock_stays_bounded_under_table_churn() {
        let clock = TableChangeClock::default();
        let base = std::time::Instant::now();
        let read_started_at = base + std::time::Duration::from_millis(1);
        let churn = MAX_TRACKED_TABLES * 2 + 7;

        for i in 0..churn {
            clock.record_change(
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
            clock.changed_since(&discarded, base),
            "a discarded table must still reject writes from reads that predate its invalidation"
        );

        // And the collapse is self-healing: a read that began afterwards is
        // unaffected, so caching resumes rather than being wedged off.
        let later = read_started_at + std::time::Duration::from_millis(1);
        assert!(
            !clock.changed_since(&discarded, later),
            "a read beginning after every recorded invalidation must still be cacheable"
        );
    }

    /// An entry is measured against the *latest* invalidation among the tables
    /// it read: any one of them moving on leaves the result behind, so the most
    /// recent such move is what its stale window is anchored to.
    #[test]
    fn table_invalidation_clock_reports_the_latest_mark_among_tables() {
        let clock = TableChangeClock::default();
        let base = std::time::Instant::now();
        let later = base + std::time::Duration::from_secs(1);

        clock.record_change(&TableReference::bare("orders"), base);
        clock.record_change(&TableReference::bare("customer"), later);

        let both: HashSet<TableReference> = HashSet::from([
            TableReference::bare("orders"),
            TableReference::bare("customer"),
        ]);
        assert_eq!(clock.latest_change(&both), Some(later));

        let only_orders: HashSet<TableReference> = HashSet::from([TableReference::bare("orders")]);
        assert_eq!(clock.latest_change(&only_orders), Some(base));

        let untouched: HashSet<TableReference> = HashSet::from([TableReference::bare("lineitem")]);
        assert_eq!(clock.latest_change(&untouched), None);

        let empty: HashSet<TableReference> = HashSet::new();
        assert_eq!(clock.latest_change(&empty), None);
    }

    /// A table-less result (e.g. `SELECT 1`) records no input tables, so a change
    /// to one table must leave it cacheable.
    #[test]
    fn table_invalidation_clock_ignores_empty_table_set() {
        let clock = TableChangeClock::default();
        let base = std::time::Instant::now();
        clock.record_change(&TableReference::bare("customer"), base);

        let empty: HashSet<TableReference> = HashSet::new();
        assert!(!clock.changed_since(&empty, base));
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

    /// Refresh invalidates derived caches while SQL results stay fresh.
    #[tokio::test]
    async fn refresh_leaves_results_fresh_and_invalidates_derived_caches() {
        let results = Arc::new(
            QueryResultsCacheProvider::try_new(&config_with_stale_window("5m"), Box::new([]))
                .expect("valid provider"),
        );
        let key = RawCacheKey::new(42);
        results
            .put_raw_key(&key, cached_result_for("customer", Instant::now()).await)
            .await
            .expect("store result");
        let config = SQLResultsCacheConfig::default();
        let plans = Arc::new(SimpleCache::new(
            1024 * 1024,
            std::time::Duration::from_secs(600),
            get_hash_builder(config.hashing_algorithm).expect("valid hasher"),
        ));
        let plan = parse_sql_to_logical_plan("SELECT * FROM customer").await;
        plans.put_raw_key(&42, plan.clone()).await;
        assert_eq!(*plans.get_raw_key(&42).await.expect("plan cached"), plan);
        let (search, search_calls) = RecordingCache::<CachedSearchResult>::new(false);
        let caching = Caching::new()
            .with_results_cache(Arc::clone(&results))
            .with_plans_cache(
                Arc::clone(&plans) as Arc<dyn TabledCacheProvider<LogicalPlan> + Send + Sync>
            )
            .with_search_cache(search);
        caching
            .invalidate_for_refresh(TableReference::bare("customer"))
            .await
            .expect("refresh invalidation");
        let entry = results
            .get_raw_key(&key)
            .await
            .expect("lookup result")
            .expect("refresh retains result");
        assert!(!entry.is_stale(results.ttl(), Instant::now()));
        assert!(
            plans.get_raw_key(&42).await.is_none(),
            "refresh evicts dependent plans"
        );
        assert_eq!(search_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// Read-start age prevents storing results dead on arrival.
    #[tokio::test]
    async fn dead_on_arrival_result_is_not_stored() {
        let provider = QueryResultsCacheProvider::try_new(
            &SQLResultsCacheConfig {
                item_ttl: Some("1s".to_string()),
                stale_while_revalidate_ttl: Some("1s".to_string()),
                ..Default::default()
            },
            Box::new([]),
        )
        .expect("valid provider");
        let read = Instant::now()
            .checked_sub(std::time::Duration::from_secs(3))
            .expect("monotonic clock exceeds three seconds");
        let result = cached_result_for("customer", read).await;
        assert!(!provider.is_servable(&result.input_tables, read));
        let key = RawCacheKey::new(43);
        assert!(
            !provider
                .store_raw_key(&key, result, None)
                .await
                .expect("store result")
        );
        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("lookup result")
                .is_none()
        );
    }

    /// Freshness counts from query start even when insertion is recent.
    #[tokio::test]
    async fn freshness_counts_from_read_start() {
        let now = Instant::now();
        let result = cached_result_for(
            "customer",
            now.checked_sub(std::time::Duration::from_millis(1001))
                .expect("monotonic clock exceeds the entry age"),
        )
        .await;
        assert!(result.is_stale(std::time::Duration::from_secs(1), now));
        assert!(!result.is_stale(std::time::Duration::from_secs(2), now));
        assert!(result.cached_at() >= now);
    }

    /// Two stores of the same key race, and the *older* result lands last.
    ///
    /// No cache engine orders concurrent writes to one key — the store that
    /// reaches the shard last wins, whichever query started first — so the
    /// resident entry can be the older of two results. Read-time validation is
    /// what makes that harmless: the entry carries its own `read_started_at`,
    /// and an entry whose table changed at or after that instant is never
    /// served, no matter which store deposited it.
    #[tokio::test]
    async fn get_raw_key_rejects_an_older_generation_that_overwrote_a_newer_one() {
        let provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        let key = RawCacheKey::new(42);

        // Query A starts, reading `customer`.
        let old_read_started_at = Instant::now();
        // `customer` changes while A is still running.
        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidation should succeed");
        // Query B starts after the change and stores first.
        let new_read_started_at = Instant::now();

        provider
            .put_raw_key(
                &key,
                cached_result_for("customer", new_read_started_at).await,
            )
            .await
            .expect("cache access should succeed");
        // A's older result lands last and overwrites B's.
        provider
            .put_raw_key(
                &key,
                cached_result_for("customer", old_read_started_at).await,
            )
            .await
            .expect("cache access should succeed");

        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("cache access should succeed")
                .is_none(),
            "an older generation that overwrote a newer one must not be servable once its table has changed"
        );
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

        let served = provider
            .get_raw_key(&key)
            .await
            .expect("cache access should succeed")
            .expect("an entry for an uninvalidated table must remain cached");
        // The served entry is the one stored, with the read start it was stored with:
        // the `orders` invalidation, later than that read start, must not touch it.
        assert_eq!(
            *served.input_tables,
            HashSet::from([TableReference::bare("customer")]),
            "the served entry must be the result stored for `customer`"
        );
        assert_eq!(
            served.read_started_at, read_started_at,
            "the served entry must carry the read start it was stored with"
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
        assert!(!provider.tables_changed_since(&tables, read_started_at));

        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidation should succeed");

        assert!(
            provider.tables_changed_since(&tables, read_started_at),
            "a result whose read began before the invalidation must not be stored"
        );
    }

    /// A redefinition (a view, catalog or user function) can change what any query
    /// returns, including one that read no table, so a read that began before
    /// `invalidate_all` must not be stored afterwards.
    #[tokio::test]
    async fn invalidate_all_records_the_invalidation_for_every_read() {
        let provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        let read_started_at = std::time::Instant::now();
        let customer: HashSet<TableReference> = HashSet::from([TableReference::bare("customer")]);
        let no_table: HashSet<TableReference> = HashSet::new();
        assert!(!provider.tables_changed_since(&customer, read_started_at));
        assert!(!provider.tables_changed_since(&no_table, read_started_at));

        provider.invalidate_all().await;

        assert!(
            provider.tables_changed_since(&customer, read_started_at),
            "a result whose read began before the invalidation must not be stored"
        );
        assert!(
            provider.tables_changed_since(&no_table, read_started_at),
            "a result that read no table must not be stored either"
        );
        assert!(
            !provider.tables_changed_since(&customer, std::time::Instant::now()),
            "a read that begins after the invalidation can be stored"
        );
    }

    pub(crate) fn config_with_stale_window(
        stale_while_revalidate_ttl: &str,
    ) -> SQLResultsCacheConfig {
        SQLResultsCacheConfig {
            // Long enough that nothing in these tests expires on the ordinary
            // TTL, so the only thing under test is the invalidation clock.
            item_ttl: Some("10m".to_string()),
            stale_while_revalidate_ttl: Some(stale_while_revalidate_ttl.to_string()),
            ..Default::default()
        }
    }

    /// Table changes reject older reads for any age and leave newer reads alone.
    #[tokio::test]
    async fn table_changes_reject_older_reads_and_preserve_newer_reads() {
        let provider =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("5m"), Box::new([]))
                .expect("valid cache provider");

        let read_started_at = std::time::Instant::now();
        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidation should succeed");
        // An upper bound on the mark the invalidation just stamped, so the
        // assertions below hold regardless of clock granularity.
        let after_mark = std::time::Instant::now();

        let tables: HashSet<TableReference> = HashSet::from([TableReference::bare("customer")]);
        assert!(
            provider.tables_changed_since(&tables, read_started_at),
            "an entry that read the table before the mutation is invalidated"
        );
        assert!(
            !provider
                .tables_changed_since(&tables, after_mark + std::time::Duration::from_millis(1)),
            "an entry whose read began after the mutation is unaffected by it"
        );
    }

    /// Read-start expiry is enforced even while the backend retains the entry.
    #[tokio::test]
    async fn get_raw_key_rejects_expired_resident_read() {
        let provider =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("5m"), Box::new([]))
                .expect("valid provider");
        let key = RawCacheKey::new(90);
        let read_started_at = Instant::now()
            .checked_sub(std::time::Duration::from_secs(901))
            .expect("monotonic clock exceeds 901 seconds");
        provider
            .put_raw_key(&key, cached_result_for("customer", read_started_at).await)
            .await
            .expect("insert old read directly into backend");
        let resident = provider
            .cache
            .get_raw_key(&key.as_u64())
            .await
            .expect("backend still retains the newly inserted entry");
        assert_eq!(resident.read_started_at, read_started_at);
        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("lookup result")
                .is_none(),
            "an expired read must be a miss even while resident"
        );
    }

    /// A stale window never makes a table invalidation servable.
    #[tokio::test]
    async fn invalidate_for_table_evicts_with_a_stale_window() {
        let provider =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("5m"), Box::new([]))
                .expect("valid provider");
        let key = RawCacheKey::new(4);
        provider
            .put_raw_key(&key, cached_result_for("customer", Instant::now()).await)
            .await
            .expect("store result");
        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidate table");
        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("lookup result")
                .is_none()
        );
    }

    /// A late publication after a mutation or unload is rejected at lookup.
    #[tokio::test]
    async fn a_read_started_before_an_unload_is_not_servable() {
        let provider =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("5m"), Box::new([]))
                .expect("valid cache provider");

        // The read begins before either invalidation, as a query already in flight would.
        let read_started_at = std::time::Instant::now();

        provider
            .invalidate_for_table(TableReference::bare("orders"))
            .await
            .expect("invalidation should succeed");
        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("eviction should succeed");

        let unloaded: HashSet<TableReference> = HashSet::from([TableReference::bare("customer")]);
        assert!(
            provider.tables_changed_since(&unloaded, read_started_at),
            "a result that read an unloaded dataset must not be served, however long the stale window is"
        );

        let refreshed: HashSet<TableReference> = HashSet::from([TableReference::bare("orders")]);
        assert!(
            provider.tables_changed_since(&refreshed, read_started_at),
            "a contents change also rejects the older read"
        );

        // A query joining both is ruled by the unloaded one: any single table going away
        // is enough to leave the whole result unservable.
        let both: HashSet<TableReference> = HashSet::from([
            TableReference::bare("customer"),
            TableReference::bare("orders"),
        ]);
        assert!(
            provider.tables_changed_since(&both, read_started_at),
            "one unloaded input is enough, even alongside a table that only changed"
        );

        // And a read that began *after* the unload is untouched by it — the watermark
        // must not invalidate everything forever.
        let after_unload = std::time::Instant::now();
        assert!(
            !provider.tables_changed_since(&unloaded, after_unload),
            "a read that began after the unload is unaffected by it"
        );
    }

    /// A table-change stamp equal to read start is invalidated immediately.
    #[tokio::test]
    async fn hard_invalidation_rejects_a_read_at_the_stamp() {
        let provider =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("5m"), Box::new([]))
                .expect("valid provider");
        let read = Instant::now();
        provider
            .table_changes
            .record_change(&TableReference::bare("customer"), read);
        let result = cached_result_for("customer", read).await;
        assert!(provider.tables_changed_since(&result.input_tables, read));
        assert!(
            !provider
                .store_raw_key(&RawCacheKey::new(8), result, None)
                .await
                .expect("store result")
        );
    }

    /// A zero-length window is a window nothing can be served from. Keeping
    /// entries resident for it would hold memory no lookup could use, so it
    /// reads as unset and the invalidation stays hard.
    #[tokio::test]
    async fn a_zero_stale_window_reads_as_no_stale_window() {
        let provider =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("0s"), Box::new([]))
                .expect("valid cache provider");

        let key = RawCacheKey::new(6);
        provider
            .put_raw_key(
                &key,
                cached_result_for("customer", std::time::Instant::now()).await,
            )
            .await
            .expect("cache access should succeed");

        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidation should succeed");
        provider.run_pending_tasks().await;

        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("cache access should succeed")
                .is_none(),
            "a zero window must not keep an invalidated entry servable"
        );
    }

    /// An entry that reads a table nobody invalidated is a plain hit, whichever
    /// lookup finds it — the window must not mark everything stale.
    #[tokio::test]
    async fn get_raw_key_leaves_uninvalidated_entries_alone() {
        let provider =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("5m"), Box::new([]))
                .expect("valid cache provider");

        let key = RawCacheKey::new(7);
        provider
            .put_raw_key(
                &key,
                cached_result_for("customer", std::time::Instant::now()).await,
            )
            .await
            .expect("cache access should succeed");
        provider
            .invalidate_for_table(TableReference::bare("orders"))
            .await
            .expect("invalidation should succeed");

        let entry = provider
            .get_raw_key(&key)
            .await
            .expect("cache access should succeed")
            .expect("an entry for an uninvalidated table must remain cached");
        assert!(!provider.tables_changed_since(&entry.input_tables, entry.read_started_at));
    }

    /// A table provider that reports a fixed `TableType`.
    #[derive(Debug)]
    struct TypedTable(
        datafusion::datasource::empty::EmptyTable,
        datafusion::datasource::TableType,
    );

    #[async_trait::async_trait]
    impl datafusion::catalog::TableProvider for TypedTable {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.0.schema()
        }
        fn table_type(&self) -> datafusion::datasource::TableType {
            self.1
        }
        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[datafusion::logical_expr::Expr],
            limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            self.0.scan(state, projection, filters, limit).await
        }
    }

    /// A table function whose provider reports the given `TableType`, as a
    /// table function whose rows can change between executions reports
    /// `Temporary`.
    #[derive(Debug)]
    struct TypedTableFunction(datafusion::datasource::TableType);

    impl datafusion::catalog::TableFunctionImpl for TypedTableFunction {
        fn call(
            &self,
            _args: &[datafusion::logical_expr::Expr],
        ) -> datafusion::error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
            use datafusion::arrow::datatypes::{DataType, Field, Schema};

            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
            Ok(Arc::new(TypedTable(
                datafusion::datasource::empty::EmptyTable::new(schema),
                self.0,
            )))
        }
    }

    /// A `Temporary` scan is never cached, wherever the plan holds it:
    /// directly, in a `FROM` subquery, or in an `IN`, `EXISTS`, or scalar
    /// subquery, which a node keeps in its expressions rather than its inputs.
    /// The same queries over a `Base` provider stay cacheable, so the scan's
    /// table type is what decides.
    #[tokio::test]
    async fn test_cache_is_disabled_for_temporary_table_function_scan() {
        use datafusion::datasource::TableType;

        let cache_provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        for sql in [
            "SELECT a FROM udtf()",
            "SELECT a FROM (SELECT a FROM udtf()) t",
            "SELECT * FROM customer WHERE id IN (SELECT a FROM udtf())",
            "SELECT * FROM customer WHERE EXISTS (SELECT 1 FROM udtf() WHERE a = customer.id)",
            "SELECT id, (SELECT max(a) FROM udtf()) AS m FROM customer",
        ] {
            for (table_type, cacheable) in [(TableType::Temporary, false), (TableType::Base, true)]
            {
                let ctx = utils::tests::create_session_context();
                ctx.register_udtf("udtf", Arc::new(TypedTableFunction(table_type)));
                let plan = ctx.state().create_logical_plan(sql).await.expect("plan");
                assert_eq!(
                    cache_provider.cache_is_enabled_for_plan(&plan),
                    cacheable,
                    "{table_type:?}: {sql}"
                );
            }
        }
    }

    /// A recursive CTE's work table is a `Temporary` scan of the rows the query
    /// derives itself, so it leaves a recursive query cacheable; a volatile
    /// function or a `Temporary` table function inside the recursion still
    /// keeps it out.
    #[tokio::test]
    async fn test_recursive_cte_work_table_does_not_disable_the_cache() {
        use datafusion::common::tree_node::TreeNode;
        use datafusion::datasource::TableType;

        let cache_provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        for (sql, cacheable) in [
            (
                "WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 3) SELECT n FROM nums",
                true,
            ),
            (
                "WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 3 AND random() < 2) SELECT n FROM nums",
                false,
            ),
            (
                "WITH RECURSIVE nums(n) AS (SELECT a FROM udtf() UNION ALL SELECT n + 1 FROM nums WHERE n < 3) SELECT n FROM nums",
                false,
            ),
        ] {
            let ctx = utils::tests::create_session_context();
            ctx.register_udtf("udtf", Arc::new(TypedTableFunction(TableType::Temporary)));
            let plan = ctx.state().create_logical_plan(sql).await.expect("plan");
            assert!(
                plan.exists(|node| Ok(matches!(node, LogicalPlan::RecursiveQuery(_))))
                    .expect("infallible"),
                "{sql} must plan as a recursive query, or this test checks nothing"
            );
            assert_eq!(
                cache_provider.cache_is_enabled_for_plan(&plan),
                cacheable,
                "{sql}"
            );
        }
    }

    /// A table in an ignored schema keeps a plan out of the cache when only a
    /// subquery reads it, as it does when the outer query reads it.
    #[test]
    fn test_cache_is_disabled_for_ignored_schema_in_subquery() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::TableReference;
        use datafusion::datasource::{empty::EmptyTable, provider_as_source};
        use datafusion::logical_expr::LogicalPlanBuilder;
        use datafusion::prelude::{col, in_subquery};

        let scan = |table: TableReference| {
            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
            LogicalPlanBuilder::scan(
                table,
                provider_as_source(Arc::new(EmptyTable::new(schema))),
                None,
            )
            .expect("scan")
        };
        let subquery = Arc::new(
            scan(TableReference::partial("runtime", "task_history"))
                .build()
                .expect("subquery plan"),
        );
        let plan = scan(TableReference::bare("t"))
            .filter(in_subquery(col("a"), subquery))
            .expect("filter")
            .build()
            .expect("plan");

        for (ignore_schemas, cacheable) in [(&["runtime"][..], false), (&[][..], true)] {
            let cache_provider = QueryResultsCacheProvider::try_new(
                &SQLResultsCacheConfig::default(),
                ignore_schemas
                    .iter()
                    .map(|s| Box::<str>::from(*s))
                    .collect(),
            )
            .expect("valid cache provider");
            assert_eq!(
                cache_provider.cache_is_enabled_for_plan(&plan),
                cacheable,
                "ignore_schemas={ignore_schemas:?}"
            );
        }
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
    async fn test_cache_is_disabled_for_non_deterministic_functions() {
        let cache_provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        for sql in [
            "SELECT uuid() u",
            "SELECT random() r",
            "SELECT uuid() u, random() r, now() t",
            "SELECT now()",
            "SELECT current_timestamp",
            "SELECT current_date",
            "SELECT current_time()",
            "SELECT id, random() FROM customer",
            "SELECT * FROM customer WHERE random() > 0.5",
            "SELECT * FROM customer WHERE now() > to_timestamp(0)",
            "SELECT * FROM customer WHERE id IN (SELECT id FROM customer WHERE random() > 0.5)",
            "SELECT count(*) FROM customer GROUP BY id ORDER BY random()",
        ] {
            let logical_plan = parse_sql_to_logical_plan(sql).await;
            assert!(
                !cache_provider.cache_is_enabled_for_plan(&logical_plan),
                "results cache must be disabled for non-deterministic query: {sql}"
            );
        }
    }

    /// `Stable` user functions (e.g. remote HTTP UDFs, which are capped at
    /// `Stable`) can change between statements, so only `Immutable`
    /// functions are cacheable.
    #[tokio::test]
    async fn test_cache_is_disabled_for_stable_udf() {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};

        let cache_provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        for (volatility, cacheable) in [(Volatility::Stable, false), (Volatility::Immutable, true)]
        {
            let ctx = utils::tests::create_session_context();
            ctx.register_udf(create_udf(
                "probe",
                vec![],
                DataType::Int64,
                volatility,
                std::sync::Arc::new(|_: &[ColumnarValue]| {
                    Ok(ColumnarValue::Scalar(
                        datafusion::scalar::ScalarValue::Int64(Some(1)),
                    ))
                }),
            ));
            let plan = ctx
                .state()
                .create_logical_plan("SELECT probe()")
                .await
                .expect("plan");
            assert_eq!(
                cache_provider.cache_is_enabled_for_plan(&plan),
                cacheable,
                "{volatility:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_cache_is_enabled_for_deterministic_functions() {
        let cache_provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid cache provider");

        for sql in [
            "SELECT abs(-1), upper('a')",
            "SELECT id, lower(first_name) FROM customer WHERE id > 1",
            "SELECT * FROM customer WHERE id IN (SELECT id FROM customer WHERE state = 'NY')",
        ] {
            let logical_plan = parse_sql_to_logical_plan(sql).await;
            assert!(
                cache_provider.cache_is_enabled_for_plan(&logical_plan),
                "results cache must stay enabled for deterministic query: {sql}"
            );
        }
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

    /// A cache that fails every table invalidation, and one that records whether
    /// it was asked — enough to tell "the later cache was skipped" from "the
    /// later cache ran and the earlier failure was still reported".
    struct RecordingCache<V> {
        fails: bool,
        calls: Arc<std::sync::atomic::AtomicUsize>,
        _marker: std::marker::PhantomData<fn() -> V>,
    }

    impl<V> RecordingCache<V> {
        fn new(fails: bool) -> (Arc<Self>, Arc<std::sync::atomic::AtomicUsize>) {
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            (
                Arc::new(Self {
                    fails,
                    calls: Arc::clone(&calls),
                    _marker: std::marker::PhantomData,
                }),
                calls,
            )
        }
    }

    impl<V> std::fmt::Debug for RecordingCache<V> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RecordingCache")
                .field("fails", &self.fails)
                .finish_non_exhaustive()
        }
    }

    impl<V> std::fmt::Display for RecordingCache<V> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "RecordingCache")
        }
    }

    impl<V> HashProvider for RecordingCache<V> {
        fn hasher(&self) -> Box<dyn Hasher> {
            Box::new(std::collections::hash_map::DefaultHasher::new())
        }
    }

    #[async_trait]
    impl<V: AsTableRefs + Clone + Send + Sync + 'static> CacheProvider<V> for RecordingCache<V> {
        async fn get_raw_key(&self, _key: &u64) -> Option<Arc<V>> {
            None
        }
        async fn get_raw_key_validated(
            &self,
            _key: &u64,
            _is_valid: &(dyn for<'v> Fn(&'v V) -> bool + Send + Sync),
        ) -> Option<Arc<V>> {
            None
        }
        async fn put_raw_key(&self, _key: &u64, _value: V) {}
        async fn put_raw_key_with_weight(&self, _key: &u64, _value: V, _weight: usize) {}
        async fn replace_if(
            &self,
            _key: &u64,
            _value: V,
            _should_replace: &(dyn for<'v> Fn(&'v V) -> bool + Send + Sync),
        ) -> bool {
            false
        }
        async fn put_if(
            &self,
            _key: &u64,
            _value: V,
            _weight: usize,
            _admit: &(dyn for<'v> Fn(Option<&'v V>) -> bool + Send + Sync),
        ) -> bool {
            false
        }
        async fn invalidate_all(&self) {}
        async fn size_bytes(&self) -> u64 {
            0
        }
        async fn item_count(&self) -> u64 {
            0
        }
        fn max_size(&self) -> usize {
            0
        }
        async fn checkpoint(&self) {}
    }

    #[async_trait]
    impl<V: AsTableRefs + Clone + Send + Sync + 'static> TabledCacheProvider<V> for RecordingCache<V> {
        async fn invalidate_for_table(&self, _table_ref: TableReference) -> Result<()> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fails {
                Err(Error::FailedToInvalidateCacheGeneric {
                    source: moka::PredicateError::InvalidationClosuresDisabled,
                })
            } else {
                Ok(())
            }
        }
        fn tables_changed_since(&self, _tables: &HashSet<TableReference>, _since: Instant) -> bool {
            false
        }
    }

    /// spiceai/spiceai#14251 review: the caches invalidated for one table each keep
    /// their own change clock, so a `?` between them left the later ones serving a
    /// table the earlier one had been told was gone. Every arm must run, and the
    /// failure must still reach the caller.
    ///
    /// Before the fix the search cache saw no call at all: the plans failure
    /// returned from `invalidate_for_refresh` first.
    #[tokio::test]
    async fn a_failing_cache_does_not_skip_the_ones_after_it() {
        let (plans, plans_calls) = RecordingCache::<LogicalPlan>::new(true);
        let (search, search_calls) = RecordingCache::<CachedSearchResult>::new(false);

        let caching = Caching::new()
            .with_plans_cache(plans)
            .with_search_cache(search);

        let outcome = caching
            .invalidate_for_table(TableReference::bare("unloaded"))
            .await;

        assert!(
            outcome.is_err(),
            "the plans failure must still be reported to the caller"
        );
        assert_eq!(
            plans_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the failing cache is the one that ran first"
        );
        assert_eq!(
            search_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the search cache must be invalidated even though the plans cache failed"
        );
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

    async fn encoded_counting_result(
        rows: usize,
    ) -> (
        CachedQueryResult,
        Arc<std::sync::atomic::AtomicUsize>,
        arrow::datatypes::SchemaRef,
    ) {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let values = vec![0i32; rows];
        let batch = arrow::array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values))],
        )
        .expect("batch");
        let (encoder, decodes) = encoding::CountingEncoder::zstd();
        let now = Instant::now();
        let result = CachedQueryResult::from_batches(
            vec![batch],
            Arc::clone(&schema),
            Arc::new(HashSet::new()),
            now,
            now,
            Some(encoder),
        )
        .await
        .expect("encoded result");
        (result, decodes, schema)
    }

    /// Store encoded → first fetch decodes and stays Encoded → second fetch
    /// decodes again and promotes → third fetch is Raw and does not decode.
    #[tokio::test]
    async fn third_fetch_of_an_encoded_key_is_raw_and_does_not_decode() {
        let provider = QueryResultsCacheProvider::try_new(
            &SQLResultsCacheConfig {
                item_ttl: Some("10m".to_string()),
                max_size: Some("8MiB".to_string()),
                encoding: spicepod::component::caching::Encoding::Zstd,
                ..SQLResultsCacheConfig::default()
            },
            Box::new([]),
        )
        .expect("valid cache provider");

        let key = RawCacheKey::new(42);
        let (result, decodes, _) = encoded_counting_result(5_000).await;
        assert!(result.is_encoded(), "store path must start encoded");
        let encoded_weight = result.memory_size();

        provider
            .put_raw_key(&key, result)
            .await
            .expect("put encoded");

        let first = provider.get_raw_key(&key).await.expect("get").expect("hit");
        assert!(first.is_encoded(), "first fetch is still the encoded store");
        let first_records = provider.records(&key, &first).await.expect("first decode");
        assert_eq!(first_records[0].num_rows(), 5_000);
        assert_eq!(
            decodes.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the first fetch must decode once"
        );

        provider.run_pending_tasks().await;
        let second = provider
            .get_raw_key(&key)
            .await
            .expect("get")
            .expect("hit after first decode");
        assert!(
            second.is_encoded(),
            "a one-shot must not inflate to Raw after the first decode"
        );
        assert_eq!(second.encoded_decode_hits(), Some(1));
        let second_records = provider
            .records(&key, &second)
            .await
            .expect("second decode+promote");
        assert_eq!(second_records[0].num_rows(), 5_000);
        assert_eq!(
            decodes.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the second fetch must decode again before promoting"
        );

        provider.run_pending_tasks().await;
        let third = provider
            .get_raw_key(&key)
            .await
            .expect("get")
            .expect("hit after promote");
        assert!(
            !third.is_encoded(),
            "the stored entry must be Raw after the second successful decode"
        );
        let third_records = provider.records(&key, &third).await.expect("raw path");
        assert!(
            Arc::ptr_eq(&second_records, &third_records),
            "the third fetch must Arc-share the promoted batches"
        );
        assert_eq!(
            decodes.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the third fetch must not decode"
        );
        assert!(
            third.memory_size() > encoded_weight,
            "the weigher must bill the raw size after promotion, got {} then {}",
            encoded_weight,
            third.memory_size()
        );
        assert!(
            provider.size().await >= third.memory_size(),
            "the store's weighted size must reflect the promoted raw entry, got {} vs {}",
            provider.size().await,
            third.memory_size()
        );
    }

    /// A one-shot key stays encoded after its only decode.
    #[tokio::test]
    async fn one_shot_fetch_leaves_the_entry_encoded() {
        let provider = QueryResultsCacheProvider::try_new(
            &SQLResultsCacheConfig {
                item_ttl: Some("10m".to_string()),
                max_size: Some("8MiB".to_string()),
                encoding: spicepod::component::caching::Encoding::Zstd,
                ..SQLResultsCacheConfig::default()
            },
            Box::new([]),
        )
        .expect("valid cache provider");

        let key = RawCacheKey::new(46);
        let (result, decodes, _) = encoded_counting_result(1_000).await;
        provider
            .put_raw_key(&key, result)
            .await
            .expect("put encoded");

        let first = provider.get_raw_key(&key).await.expect("get").expect("hit");
        let _ = provider.records(&key, &first).await.expect("decode");
        provider.run_pending_tasks().await;

        let stored = provider
            .get_raw_key(&key)
            .await
            .expect("get")
            .expect("still cached");
        assert!(
            stored.is_encoded(),
            "one successful decode must not promote to Raw"
        );
        assert_eq!(stored.encoded_decode_hits(), Some(1));
        assert!(
            !stored.encoded_has_resident_decode(),
            "the first hit must replace the stored decode cell so decoded batches are not retained"
        );
        assert_eq!(decodes.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// Concurrent first-wave `records()` on one fetch share a decode and must
    /// not promote: they are still one hit of a one-shot key.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_first_fetches_leave_the_entry_encoded() {
        let provider = Arc::new(
            QueryResultsCacheProvider::try_new(
                &SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    max_size: Some("8MiB".to_string()),
                    encoding: spicepod::component::caching::Encoding::Zstd,
                    ..SQLResultsCacheConfig::default()
                },
                Box::new([]),
            )
            .expect("valid cache provider"),
        );

        let key = RawCacheKey::new(47);
        let (result, decodes, _) = encoded_counting_result(2_000).await;
        provider
            .put_raw_key(&key, result)
            .await
            .expect("put encoded");

        let first = provider.get_raw_key(&key).await.expect("get").expect("hit");
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let provider = Arc::clone(&provider);
                let first = first.clone();
                tokio::spawn(async move { provider.records(&key, &first).await.expect("decode") })
            })
            .collect();

        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            results.push(task.await.expect("task joins"));
        }

        assert_eq!(
            decodes.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "concurrent first-wave records() must share one decode"
        );
        let head = &results[0];
        for (i, batches) in results.iter().enumerate() {
            assert!(
                Arc::ptr_eq(head, batches),
                "concurrent first fetch {i} must Arc-share the winning decode"
            );
        }

        provider.run_pending_tasks().await;
        let stored = provider
            .get_raw_key(&key)
            .await
            .expect("get")
            .expect("still cached");
        assert!(
            stored.is_encoded(),
            "a concurrent first wave must not promote to Raw"
        );
        assert_eq!(stored.encoded_decode_hits(), Some(1));
    }

    /// A promotion must not replace a newer result stored under the same key.
    #[tokio::test]
    async fn promote_does_not_overwrite_a_newer_generation() {
        let provider = QueryResultsCacheProvider::try_new(
            &SQLResultsCacheConfig {
                item_ttl: Some("10m".to_string()),
                max_size: Some("8MiB".to_string()),
                encoding: spicepod::component::caching::Encoding::Zstd,
                ..SQLResultsCacheConfig::default()
            },
            Box::new([]),
        )
        .expect("valid cache provider");

        let key = RawCacheKey::new(43);
        let (first, decodes, _) = encoded_counting_result(1_000).await;
        provider
            .put_raw_key(&key, first.clone())
            .await
            .expect("put first generation");

        let (newer, _, _) = encoded_counting_result(1_000).await;
        assert!(
            !first.is_same_generation(&newer),
            "the second store is a distinct generation"
        );
        provider
            .put_raw_key(&key, newer.clone())
            .await
            .expect("put newer generation");

        // First decode of the stale clone only records a hit, which must not
        // overwrite the newer generation. A second decode would try to promote.
        let _ = provider
            .records(&key, &first)
            .await
            .expect("stale clone still decodes");
        assert_eq!(decodes.load(std::sync::atomic::Ordering::SeqCst), 1);

        let stale_second = first.with_recorded_decode_hit();
        let _ = provider
            .records(&key, &stale_second)
            .await
            .expect("stale clone's second decode still serves");

        let stored = provider.get_raw_key(&key).await.expect("get").expect("hit");
        assert!(
            stored.is_encoded(),
            "the newer encoded generation must still be stored"
        );
        assert!(
            stored.is_same_generation(&newer),
            "promotion of the stale clone must not replace the newer result"
        );
    }

    /// When decoded size would not fit `max_size`, keep the encoded entry so
    /// promotion cannot evict the only copy of the result. The served `Arc` is
    /// kept; the stored decode cell is reset so decoded batches are not
    /// retained off-budget.
    #[tokio::test]
    async fn promote_is_skipped_when_raw_exceeds_max_size() {
        let provider = QueryResultsCacheProvider::try_new(
            &SQLResultsCacheConfig {
                item_ttl: Some("10m".to_string()),
                max_size: Some("4KiB".to_string()),
                encoding: spicepod::component::caching::Encoding::Zstd,
                ..SQLResultsCacheConfig::default()
            },
            Box::new([]),
        )
        .expect("valid cache provider");

        let key = RawCacheKey::new(44);
        let (result, decodes, schema) = encoded_counting_result(8_000).await;
        // A separate raw entry of the same shape, so the size check does not
        // fill this encoded value's decode cell.
        let now = Instant::now();
        let raw_probe = CachedQueryResult::new_raw(
            vec![
                arrow::array::RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(arrow::array::Int32Array::from(vec![0i32; 8_000]))],
                )
                .expect("batch"),
            ],
            schema,
            Arc::new(HashSet::new()),
            now,
            now,
        );
        assert!(
            raw_probe.memory_size() > 4 * 1024,
            "fixture must exceed a 4 KiB cache once raw, got {}",
            raw_probe.memory_size()
        );
        assert!(
            result.memory_size() <= 4 * 1024,
            "fixture must fit encoded, got {}",
            result.memory_size()
        );

        provider
            .put_raw_key(&key, result)
            .await
            .expect("put encoded that fits");

        let first = provider.get_raw_key(&key).await.expect("get").expect("hit");
        let _ = provider
            .records(&key, &first)
            .await
            .expect("first decode; still encoded");

        provider.run_pending_tasks().await;
        let second = provider
            .get_raw_key(&key)
            .await
            .expect("get")
            .expect("still cached after first hit");
        assert!(second.is_encoded(), "first hit must not promote");
        let _ = provider
            .records(&key, &second)
            .await
            .expect("second decode; promote must be skipped");

        provider.run_pending_tasks().await;
        let third = provider
            .get_raw_key(&key)
            .await
            .expect("get")
            .expect("still cached");
        assert!(
            third.is_encoded(),
            "an entry that cannot fit raw must stay encoded rather than be evicted"
        );
        assert!(
            !third.encoded_has_resident_decode(),
            "skipping promotion must drop the stored decode cell so decoded batches are not retained off-budget"
        );
        let third_records = provider.records(&key, &third).await.expect("third hit");
        assert_eq!(third_records[0].num_rows(), 8_000);
        assert_eq!(
            decodes.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "hit1 and hit2 each decode; skipping promotion must reset the stored decode cell, so a later fetch decodes again"
        );

        provider.run_pending_tasks().await;
        let fourth = provider
            .get_raw_key(&key)
            .await
            .expect("get")
            .expect("still cached after a post-skip decode");
        assert!(fourth.is_encoded(), "still encoded after another skip");
        assert!(
            !fourth.encoded_has_resident_decode(),
            "each skipped promotion must leave the stored decode cell empty"
        );
        assert!(
            fourth.memory_size() <= 4 * 1024,
            "the weigher must still bill compressed size after a skipped promotion, got {}",
            fourth.memory_size()
        );
    }

    /// A promotion is a reweigh of the same result, so it must not restart
    /// `item_ttl`. Time itself is under test here.
    #[tokio::test]
    async fn promote_does_not_restart_item_ttl() {
        let provider = QueryResultsCacheProvider::try_new(
            &SQLResultsCacheConfig {
                item_ttl: Some("250ms".to_string()),
                max_size: Some("8MiB".to_string()),
                encoding: spicepod::component::caching::Encoding::Zstd,
                ..SQLResultsCacheConfig::default()
            },
            Box::new([]),
        )
        .expect("valid cache provider");

        let key = RawCacheKey::new(45);
        let (result, _, _) = encoded_counting_result(32).await;
        provider
            .put_raw_key(&key, result)
            .await
            .expect("put encoded");

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let first = provider
            .get_raw_key(&key)
            .await
            .expect("get")
            .expect("still live before promote");
        let _ = provider.records(&key, &first).await.expect("first decode");
        provider.run_pending_tasks().await;

        let second = provider
            .get_raw_key(&key)
            .await
            .expect("get")
            .expect("still live for second decode");
        assert!(
            second.is_encoded(),
            "the first decode must leave the entry encoded"
        );
        let _ = provider
            .records(&key, &second)
            .await
            .expect("second decode+promote");
        provider.run_pending_tasks().await;

        let promoted = provider
            .get_raw_key(&key)
            .await
            .expect("get")
            .expect("promoted entry must still be live");
        assert!(
            !promoted.is_encoded(),
            "the entry must be Raw after the second decode"
        );

        // Original remaining TTL is ~150ms. If promotion restarted a 250ms
        // TTL, this wait would still leave the entry live.
        tokio::time::sleep(std::time::Duration::from_millis(180)).await;
        provider.run_pending_tasks().await;
        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("get after original ttl")
                .is_none(),
            "promotion must not restart item_ttl"
        );
    }

    /// Keeps two instants a test orders distinct: a tie counts as changed.
    pub(crate) async fn tick() {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    /// The resident entry's read start, bypassing the validity ruling.
    async fn resident_read_started_at(
        provider: &QueryResultsCacheProvider,
        key: &RawCacheKey,
    ) -> Option<Instant> {
        provider
            .cache
            .get_raw_key(&key.as_u64())
            .await
            .map(|entry| entry.read_started_at)
    }

    /// An untouched result is stored and served fresh, with or without a window.
    #[tokio::test]
    async fn store_raw_key_stores_a_result_no_change_overtook() {
        for config in [
            SQLResultsCacheConfig::default(),
            config_with_stale_window("5m"),
        ] {
            let provider =
                QueryResultsCacheProvider::try_new(&config, Box::new([])).expect("valid provider");
            let key = RawCacheKey::new(1);
            let read_started_at = Instant::now();

            assert!(
                provider
                    .store_raw_key(
                        &key,
                        cached_result_for("customer", read_started_at).await,
                        None
                    )
                    .await
                    .expect("cache access should succeed")
            );
            assert!(
                provider
                    .get_raw_key(&key)
                    .await
                    .expect("cache access should succeed")
                    .is_some(),
                "an untouched result must be served fresh"
            );
        }
    }

    /// A result heavier than `max_size` is refused before any resident is consulted.
    /// That is reported as its own error, not as storage or invalidation, and the key
    /// stays empty.
    #[tokio::test]
    async fn store_raw_key_reports_a_result_heavier_than_the_cache_as_an_error() {
        let provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid provider");
        let key = RawCacheKey::new(1);
        let max_size = provider.max_size();
        let too_heavy = usize::try_from(max_size).expect("max_size fits usize") + 1;

        let err = provider
            .store_raw_key(
                &key,
                cached_result_for("customer", Instant::now()).await,
                Some(too_heavy),
            )
            .await
            .expect_err("a result heavier than max_size is not stored");
        assert_eq!(
            err.to_string(),
            format!(
                "The query result ({too_heavy} bytes) is larger than the SQL results cache max_size ({max_size} bytes), so it was not cached"
            )
        );
        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("cache access should succeed")
                .is_none(),
            "the key stays empty"
        );
    }

    /// Without a stale window, an overtaken result is not stored.
    #[tokio::test]
    async fn store_raw_key_drops_a_result_overtaken_by_a_change_without_a_stale_window() {
        let provider =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid provider");
        let key = RawCacheKey::new(2);
        let read_started_at = Instant::now();
        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidation should succeed");

        assert!(
            !provider
                .store_raw_key(
                    &key,
                    cached_result_for("customer", read_started_at).await,
                    None
                )
                .await
                .expect("cache access should succeed")
        );
        assert!(
            resident_read_started_at(&provider, &key).await.is_none(),
            "an unservable result must not be resident"
        );
    }

    /// A table change rejects storage even with a stale window.
    #[tokio::test]
    async fn store_raw_key_rejects_an_overtaken_result_with_a_stale_window() {
        let provider =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("5m"), Box::new([]))
                .expect("valid provider");
        let key = RawCacheKey::new(3);
        let read = Instant::now();
        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidate table");
        assert!(
            !provider
                .store_raw_key(&key, cached_result_for("customer", read).await, None)
                .await
                .expect("store result")
        );
        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("lookup result")
                .is_none()
        );
    }

    /// A recent insertion cannot admit a result beyond the read-start window.
    #[tokio::test]
    async fn store_raw_key_drops_a_result_past_its_read_start_window() {
        let provider =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("20ms"), Box::new([]))
                .expect("valid provider");
        let key = RawCacheKey::new(4);
        let read_started_at = Instant::now()
            .checked_sub(std::time::Duration::from_secs(601))
            .expect("monotonic clock exceeds the entry age");

        assert!(!provider.is_servable(
            &HashSet::from([TableReference::bare("customer")]),
            read_started_at
        ));
        assert!(
            !provider
                .store_raw_key(
                    &key,
                    cached_result_for("customer", read_started_at).await,
                    None
                )
                .await
                .expect("cache access should succeed")
        );
        assert!(resident_read_started_at(&provider, &key).await.is_none());
    }

    /// An invalidated older result must not replace a newer one.
    #[tokio::test]
    async fn store_raw_key_keeps_a_newer_resident_over_an_invalidated_result() {
        let provider =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("5m"), Box::new([]))
                .expect("valid provider");
        let key = RawCacheKey::new(5);

        let older_read = Instant::now();
        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidation should succeed");
        tick().await;
        let newer_read = Instant::now();

        assert!(
            provider
                .store_raw_key(&key, cached_result_for("customer", newer_read).await, None)
                .await
                .expect("cache access should succeed")
        );
        assert!(
            !provider
                .store_raw_key(&key, cached_result_for("customer", older_read).await, None)
                .await
                .expect("cache access should succeed")
        );

        assert_eq!(
            resident_read_started_at(&provider, &key).await,
            Some(newer_read),
            "the newer result must stay resident"
        );
        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("cache access should succeed")
                .is_some(),
            "the newer result read after the change and is still served fresh"
        );
    }

    /// Both writes are still fresh (`Valid`) — no table change — so this is
    /// the admission that used an unconditional insert. The later resident
    /// must stay; overwriting it would move the cache backwards.
    #[tokio::test]
    async fn store_raw_key_keeps_a_newer_resident_over_an_older_fresh_result() {
        let provider =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("5m"), Box::new([]))
                .expect("valid provider");
        let key = RawCacheKey::new(7);
        let tables = HashSet::from([TableReference::bare("customer")]);

        let older_read = Instant::now();
        tick().await;
        let newer_read = Instant::now();

        assert!(
            !provider.tables_changed_since(&tables, older_read),
            "the older write is still a fresh admission"
        );
        assert!(
            !provider.tables_changed_since(&tables, newer_read),
            "the newer write is a fresh admission"
        );

        assert!(
            provider
                .store_raw_key(&key, cached_result_for("customer", newer_read).await, None)
                .await
                .expect("cache access should succeed")
        );
        assert!(
            provider
                .store_raw_key(&key, cached_result_for("customer", older_read).await, None)
                .await
                .expect("cache access should succeed")
        );

        assert_eq!(
            resident_read_started_at(&provider, &key).await,
            Some(newer_read),
            "a fresh admission must not replace a resident that began reading later"
        );
        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("cache access should succeed")
                .is_some(),
            "the newer result is still served fresh"
        );
    }

    /// Q1 begins before a table change, Q2 stores after it, then Q1 attempts
    /// storage. The newer resident must stay and Q1 must not be served.
    #[tokio::test]
    async fn store_raw_key_keeps_a_newer_resident_when_invalidation_lands_between_admission_and_write()
     {
        let provider =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("5m"), Box::new([]))
                .expect("valid provider");
        let key = RawCacheKey::new(8);
        let tables = HashSet::from([TableReference::bare("customer")]);

        let q1_started = Instant::now();
        assert!(
            !provider.tables_changed_since(&tables, q1_started),
            "Q1 is a fresh admission before the invalidation"
        );

        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidation should succeed");
        tick().await;
        let q2_started = Instant::now();

        assert!(
            provider
                .store_raw_key(&key, cached_result_for("customer", q2_started).await, None)
                .await
                .expect("cache access should succeed")
        );
        assert!(
            !provider
                .store_raw_key(&key, cached_result_for("customer", q1_started).await, None)
                .await
                .expect("cache access should succeed")
        );

        assert_eq!(
            resident_read_started_at(&provider, &key).await,
            Some(q2_started),
            "Q1 finishing after encoding must not replace Q2"
        );
        let entry = provider
            .get_raw_key(&key)
            .await
            .expect("cache access should succeed")
            .expect("Q2 must remain servable");
        assert_eq!(entry.read_started_at, q2_started);
        assert!(!provider.tables_changed_since(&entry.input_tables, entry.read_started_at));
        assert!(
            provider
                .get_raw_key(&key)
                .await
                .expect("cache access should succeed")
                .is_some(),
            "Q2 read after the change and must still be served fresh, not Q1 as stale"
        );
    }

    /// Continuous hard invalidations reject every overtaken result.
    #[tokio::test]
    async fn store_raw_key_rejects_results_under_continuous_changes() {
        let provider =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("5m"), Box::new([]))
                .expect("valid provider");
        let key = RawCacheKey::new(8);
        for _ in 0..3 {
            let read = Instant::now();
            provider
                .invalidate_for_table(TableReference::bare("customer"))
                .await
                .expect("invalidate table");
            assert!(
                !provider
                    .store_raw_key(&key, cached_result_for("customer", read).await, None)
                    .await
                    .expect("store result")
            );
            assert!(
                provider
                    .get_raw_key(&key)
                    .await
                    .expect("lookup result")
                    .is_none()
            );
        }
    }

    /// `is_servable` matches the hit-time ruling.
    #[tokio::test]
    async fn is_servable_matches_the_hit_time_ruling() {
        let customer = HashSet::from([TableReference::bare("customer")]);
        let orders = HashSet::from([TableReference::bare("orders")]);

        let hard =
            QueryResultsCacheProvider::try_new(&SQLResultsCacheConfig::default(), Box::new([]))
                .expect("valid provider");
        let windowed =
            QueryResultsCacheProvider::try_new(&config_with_stale_window("5m"), Box::new([]))
                .expect("valid provider");

        for provider in [&hard, &windowed] {
            let read_started_at = Instant::now();
            provider
                .invalidate_for_table(TableReference::bare("customer"))
                .await
                .expect("invalidation should succeed");
            assert!(
                provider.is_servable(&orders, read_started_at),
                "an unrelated table's change must not affect the result"
            );
            assert!(
                provider.is_servable(&HashSet::new(), read_started_at),
                "a recent table-less result is servable"
            );
            tick().await;
            assert!(
                provider.is_servable(&customer, Instant::now()),
                "a read that began after the change is servable"
            );
        }

        let read_started_at = Instant::now();
        for provider in [&hard, &windowed] {
            provider
                .invalidate_for_table(TableReference::bare("customer"))
                .await
                .expect("invalidation should succeed");
        }
        assert!(
            !hard.is_servable(&customer, read_started_at),
            "without a window, a result overtaken by a change is unservable"
        );
        assert!(
            !windowed.is_servable(&customer, read_started_at),
            "a stale window does not make an invalidated result servable"
        );
    }
}
