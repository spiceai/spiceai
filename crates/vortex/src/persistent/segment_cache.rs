// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::collections::HashSet;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, OnceLock, Weak};

use async_trait::async_trait;
use moka::future::Cache;
use object_store::path::Path;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Meter, ObservableCounter, ObservableGauge};
use parking_lot::Mutex;
use twox_hash::XxHash3_64;
use vortex::buffer::ByteBuffer;
use vortex::error::VortexResult;
use vortex::layout::segments::{SegmentCache, SegmentId};
use vortex_utils::aliases::dash_map::{DashMap, Entry};

/// Hasher for the segment cache key. XXH3 matches the
/// project-wide cache hashing default and is markedly faster than moka's default
/// `SipHash` on the per-segment hot path.
type SegmentCacheHasher = BuildHasherDefault<XxHash3_64>;
/// Per-path insertion state, sharded.
///
/// One cache now serves every table, so this registry is touched by every cached
/// file open and close in the process. A single mutex would serialize file
/// opening across all concurrent scans; `DashMap` shards it while still
/// serializing operations on the *same* path, which is what the retirement and
/// last-owner invariants rely on.
type PathStates = Arc<DashMap<Path, Weak<PathCacheState>>>;

/// Identity of the object store a [`Path`] is relative to, as a store URL
/// (`s3://bucket/`, `file:///`).
///
/// `object_store::path::Path` is **store-relative**, so it cannot identify a file
/// on its own. A per-table cache never noticed, because every path it saw came
/// from that table's store; one cache serving every table does, and two stores
/// holding the same relative path would otherwise return each other's bytes.
///
/// This is a registry URL, not an `ObjectStore` instance, so it assumes one
/// object-store registry per process — true of `spiced`. A second `Runtime` in
/// the same process shares the first one's cache anyway (see
/// [`install_process_segment_cache`]), and could in principle register a
/// different store under the same URL; supporting that properly means scoping
/// the cache to a `RuntimeEnv` rather than the process.
type StoreKey = Arc<str>;

/// Largest segment copied inline on the async put path.
///
/// At roughly 10 GB/s a 256 KiB copy is ~25µs, comfortably inside the ~100µs an
/// async task may run without yielding. Larger segments — up to the 16 MiB
/// coalescing ceiling — are trimmed on the blocking pool instead.
const INLINE_TRIM_MAX_BYTES: usize = 256 * 1024;

/// Cache key: the store, the file within it, and the segment within the file.
type SegmentKey = (StoreKey, Arc<Path>, SegmentId);

/// Metric label for the process-wide cache.
const SHARED_CACHE_LABEL: &str = "shared";

/// Every live cache, so one instrument set can report them all.
///
/// Weak, and pruned on read: a cache belongs to the format that built it, and a
/// dropped format must not be kept alive by the metrics that describe it.
static REGISTERED_CACHES: LazyLock<Mutex<Vec<Weak<SharedSegmentCache>>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Names a private cache. The shared one is [`SHARED_CACHE_LABEL`]; a format that
/// opts out and sizes its own gets `private-N`, which at least tells an operator
/// another cache exists and how full it is.
static PRIVATE_CACHE_SEQ: AtomicU64 = AtomicU64::new(0);

fn live_caches() -> Vec<Arc<SharedSegmentCache>> {
    let mut registered = REGISTERED_CACHES.lock();
    registered.retain(|cache| cache.strong_count() > 0);
    registered.iter().filter_map(Weak::upgrade).collect()
}

/// The process-wide segment cache decision, made once at startup by
/// [`install_process_segment_cache`].
///
/// Three states, and the distinction matters: unset means the process never made
/// a decision (an embedded host that skips the runtime builder), so a format may
/// still build a cache of its own; `Some` is the shared cache; `None` is caching
/// switched **off**, which must not be quietly re-enabled per table.
static PROCESS_CACHE: OnceLock<Option<Arc<SharedSegmentCache>>> = OnceLock::new();

/// Keeps the process cache's observable instruments alive. `OpenTelemetry`
/// retains observable callbacks for the meter provider's lifetime, so the
/// instruments are registered once and never rebuilt.
static PROCESS_METRICS: OnceLock<SegmentCacheMetrics> = OnceLock::new();

/// Install the process-wide segment cache with a total budget of
/// `max_capacity_bytes`, shared by every table.
///
/// One cache for the whole process rather than one per table: keys are qualified
/// by store and path (see [`SegmentKey`]) so tables cannot collide, and a single
/// budget means adding a table divides a fixed pool instead of reserving another
/// full-sized cache. Moka fixes capacity at build time, so per-table caches could
/// not be resized to share a budget without discarding their contents.
///
/// `max_capacity_bytes` of zero installs the **disabled** decision rather than a
/// cache: scans then run uncached, and no format may substitute one of its own.
///
/// Returns `false` when a decision was already made; the first one stands, so a
/// second runtime in the same process cannot resize or re-enable the cache.
///
/// Metrics are registered separately by [`register_segment_cache_metrics`]:
/// the cache has to exist before any table is registered, which is earlier than
/// the point where the real meter provider is in place.
pub fn install_process_segment_cache(max_capacity_bytes: u64) -> bool {
    let decision = (max_capacity_bytes > 0).then(|| {
        // Retirement tracking is always on here: Cayenne needs it to keep a late
        // put from repopulating a retired path, the per-put cost is one atomic
        // load, and read-only formats share this cache with mutable ones.
        SharedSegmentCache::new(max_capacity_bytes, true, SHARED_CACHE_LABEL)
    });
    PROCESS_CACHE.set(decision).is_ok()
}

/// Register the installed cache's observable instruments.
///
/// Call this **after** the process meter provider is final. Instruments bind to
/// whichever provider is current when they are built, so registering during
/// startup would attach them to the early noop provider and they would never
/// report — the same ordering constraint the other Cayenne operator gauges
/// document. No-op when no cache is installed or metrics are already registered.
pub fn register_segment_cache_metrics() {
    // `get_or_init`, not `set`: the instruments must be *built* once. Building
    // them registers OpenTelemetry callbacks that outlive the returned handles,
    // so constructing a second set and then discarding it would leave duplicate
    // callbacks emitting the same series.
    PROCESS_METRICS
        .get_or_init(|| SegmentCacheMetrics::register(&global::meter("cayenne_segment_cache")));
}

/// The installed process-wide cache, if one is installed.
pub(crate) fn process_segment_cache() -> Option<&'static Arc<SharedSegmentCache>> {
    PROCESS_CACHE.get()?.as_ref()
}

/// Byte capacity of the installed process cache, or `None` when caching is
/// disabled or no decision has been made.
///
/// Lets the runtime's memory accounting reserve against the cache that actually
/// exists rather than recomputing what it asked for.
#[must_use]
pub fn process_segment_cache_capacity_bytes() -> Option<u64> {
    process_segment_cache().map(|cache| cache.capacity_bytes())
}

/// Whether the process decided segment caching is off.
///
/// Distinct from "no cache installed": this says a decision was made and the
/// answer was no, so a format must not fall back to a private cache.
pub(crate) fn segment_caching_disabled() -> bool {
    matches!(PROCESS_CACHE.get(), Some(None))
}

/// Observable instruments over every registered segment cache.
///
/// Held for the process (or, in tests, for the harness) lifetime. The callbacks
/// hold no cache directly — they walk the weak registry — so a dropped cache is
/// neither kept alive nor reported after it goes.
struct SegmentCacheMetrics {
    _accesses: ObservableCounter<u64>,
    _hits: ObservableCounter<u64>,
    _weighted_bytes: ObservableGauge<u64>,
    _capacity_bytes: ObservableGauge<u64>,
    _entries: ObservableGauge<u64>,
}

impl SegmentCacheMetrics {
    /// Register one instrument set over *every* cache.
    ///
    /// Each series carries a `cache` label, so the shared cache and any private
    /// one a format sized for itself report side by side instead of silently
    /// summing into one number. The callbacks walk the weak registry, so a cache
    /// stops reporting when its format drops it.
    fn register(meter: &Meter) -> Self {
        let accesses = meter
            .u64_observable_counter("cayenne_segment_cache_accesses")
            .with_description("Cumulative Vortex segment cache get() calls, by cache.")
            .with_callback(|observer| {
                for cache in live_caches() {
                    cache.observe_accesses(|value| observer.observe(value, &cache.label));
                }
            })
            .build();
        let hits = meter
            .u64_observable_counter("cayenne_segment_cache_hits")
            .with_description("Cumulative Vortex segment cache hits, by cache.")
            .with_callback(|observer| {
                for cache in live_caches() {
                    cache.observe_hits(|value| observer.observe(value, &cache.label));
                }
            })
            .build();
        let weighted_bytes = meter
            .u64_observable_gauge("cayenne_segment_cache_weighted_bytes")
            .with_description("Approximate live Vortex segment cache size in bytes, by cache.")
            .with_unit("By")
            .with_callback(|observer| {
                for cache in live_caches() {
                    observer.observe(cache.cache.weighted_size(), &cache.label);
                }
            })
            .build();
        let capacity_bytes = meter
            .u64_observable_gauge("cayenne_segment_cache_capacity_bytes")
            .with_description("Configured Vortex segment cache capacity in bytes, by cache.")
            .with_unit("By")
            .with_callback(|observer| {
                for cache in live_caches() {
                    observer.observe(cache.capacity_bytes, &cache.label);
                }
            })
            .build();
        let entries = meter
            .u64_observable_gauge("cayenne_segment_cache_entries")
            .with_description("Approximate live Vortex segment cache entry count, by cache.")
            .with_callback(|observer| {
                for cache in live_caches() {
                    observer.observe(cache.cache.entry_count(), &cache.label);
                }
            })
            .build();

        Self {
            _accesses: accesses,
            _hits: hits,
            _weighted_bytes: weighted_bytes,
            _capacity_bytes: capacity_bytes,
            _entries: entries,
        }
    }
}

/// Segment cache keyed by file path and Vortex segment id.
///
/// Vortex segment ids are local to each file, so a key pairs the id with the
/// file's path *and* the store that path is relative to — see [`SegmentKey`].
/// Fully qualified, it lets one cache and one byte budget serve every table in the
/// process. Nothing here is per-table, so the metrics above carry no `dataset`
/// label: fill, capacity and hit rate describe the single shared resource.
#[derive(Debug)]
pub(crate) struct SharedSegmentCache {
    cache: Cache<SegmentKey, ByteBuffer, SegmentCacheHasher>,
    /// Weak per-path insertion states. An open file keeps its state alive; the
    /// last [`PathSegmentCache`] drop removes the weak registry entry, so paths
    /// retired over the process's lifetime do not accumulate as tombstones.
    path_states: Option<PathStates>,
    /// Configured byte capacity, reported next to the live fill.
    capacity_bytes: u64,
    /// Cumulative `get` calls. Read directly during collection, so the hot path
    /// neither allocates labels nor records synchronously.
    accesses: AtomicU64,
    /// `get` calls that returned a cached buffer (a hit).
    hits: AtomicU64,
    /// Value of the `cache` metric label: which cache this is.
    label: [KeyValue; 1],
    /// Access total published by the last collection. Observable callbacks can
    /// run independently and readers can collect concurrently; clamping hits to
    /// this bound keeps a hit total from being published against an access total
    /// no reader has seen yet, while leaving the read path lock-free.
    last_observed_accesses: Mutex<u64>,
}

impl SharedSegmentCache {
    /// A cache with a budget of its own. Callers outside the process cache use
    /// this — a standalone `VortexFormat` configured with
    /// A cache with a budget of its own, registered so its metrics report under
    /// `name`. Returns an `Arc` because the metrics registry holds a weak
    /// reference to it.
    ///
    /// Used for the process-wide cache, for a format that sizes its own through
    /// `segment_cache_size_bytes`, and by tests that need an isolated budget.
    pub(crate) fn new(
        max_capacity_bytes: u64,
        track_retirement: bool,
        name: impl AsRef<str>,
    ) -> Arc<Self> {
        let cache = Arc::new(Self::build(
            max_capacity_bytes,
            track_retirement,
            name.as_ref(),
        ));
        {
            // Prune here, not only in `live_caches`: that runs from the metric
            // callbacks, so with metrics disabled a process that repeatedly built
            // and dropped private formats would grow this vector without bound —
            // a leak, in the code meant to fix one.
            let mut registered = REGISTERED_CACHES.lock();
            registered.retain(|registered| registered.strong_count() > 0);
            registered.push(Arc::downgrade(&cache));
        }
        cache
    }

    /// A cache sized by a format that opted out of the shared one, reporting
    /// under `name` — the table it belongs to, where the caller knows it.
    pub(crate) fn new_private(max_capacity_bytes: u64, name: Option<Arc<str>>) -> Arc<Self> {
        let name = name.unwrap_or_else(|| {
            // No table name reached us. A sequence number at least tells an
            // operator that a second cache exists and how full it is.
            let seq = PRIVATE_CACHE_SEQ.fetch_add(1, Ordering::Relaxed) + 1;
            Arc::from(format!("unnamed-{seq}"))
        });
        Self::new(max_capacity_bytes, true, name)
    }

    fn build(max_capacity_bytes: u64, track_retirement: bool, name: &str) -> Self {
        Self {
            label: [KeyValue::new("cache", name.to_owned())],
            cache: Cache::builder()
                .name("vortex-datafusion-segment-cache")
                .max_capacity(max_capacity_bytes)
                .weigher(|_, buffer: &ByteBuffer| {
                    u32::try_from(buffer.len().min(u32::MAX as usize)).unwrap_or(u32::MAX)
                })
                .build_with_hasher(SegmentCacheHasher::default()),
            path_states: track_retirement.then(|| Arc::new(DashMap::default())),
            capacity_bytes: max_capacity_bytes,
            accesses: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            last_observed_accesses: Mutex::new(0),
        }
    }

    pub(crate) fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    fn observe_accesses(&self, observe: impl FnOnce(u64)) {
        let mut last_observed_accesses = self.last_observed_accesses.lock();
        let accesses = self.accesses.load(Ordering::Relaxed);
        observe(accesses);
        *last_observed_accesses = accesses;
    }

    fn observe_hits(&self, observe: impl FnOnce(u64)) {
        let last_observed_accesses = self.last_observed_accesses.lock();
        let hits = self.hits.load(Ordering::Relaxed);
        observe(hits.min(*last_observed_accesses));
    }

    /// A per-file view. `store` identifies the object store `path` is relative
    /// to; see [`StoreKey`].
    pub(crate) fn for_path(self: &Arc<Self>, store: StoreKey, path: Path) -> Arc<dyn SegmentCache> {
        let state = self.registered_state(&path);
        Arc::new(PathSegmentCache {
            shared: Arc::clone(self),
            store,
            path: Arc::new(path),
            state,
        })
    }

    /// The insertion state registered for `path`, registering one if the path
    /// has no live entry. `None` when this cache does not track retirement.
    ///
    /// Openers and retirement share this one path into the registry, so
    /// whichever arrives second for a path finds the state the first registered
    /// rather than minting a second one beside it.
    fn registered_state(&self, path: &Path) -> Option<Arc<PathCacheState>> {
        let path_states = self.path_states.as_ref()?;
        // Registered and live is the overwhelmingly common case on the open
        // path, and a read guard serves it without the key clone an `entry`
        // needs. It is not a weaker check: `PathSegmentCache::drop` unregisters
        // under this shard's *write* lock, so it cannot interleave here, and
        // once this upgrade succeeds its `strong_count == 1` predicate can no
        // longer pass. Release the guard before the fallback — taking `entry`
        // on a shard this thread already holds would deadlock.
        if let Some(live) = path_states.get(path).and_then(|state| state.upgrade()) {
            return Some(live);
        }
        // `entry` holds the shard lock across the upgrade-or-insert, so a
        // concurrent drop cannot unregister a state between the lookup and the
        // insert, and the upgrade is retried under it because a registration
        // can have landed since the read above.
        Some(match path_states.entry(path.clone()) {
            Entry::Occupied(mut occupied) => {
                if let Some(live) = occupied.get().upgrade() {
                    live
                } else {
                    // Registered but every opener has dropped: replace the
                    // expired weak rather than leaving a dead entry behind.
                    let state = Arc::new(PathCacheState::default());
                    occupied.insert(Arc::downgrade(&state));
                    state
                }
            }
            Entry::Vacant(vacant) => {
                let state = Arc::new(PathCacheState::default());
                vacant.insert(Arc::downgrade(&state));
                state
            }
        })
    }

    /// Retire every cached segment under `paths`.
    ///
    /// Matches on the path alone, so a path retired in one store also drops
    /// matching entries in *every* store — deliberate over-invalidation. The two
    /// errors are not symmetric: over-invalidating costs a re-read, while
    /// under-invalidating leaves retired segments resident, which is the leak
    /// this exists to prevent (#12936). Carrying the store through would mean
    /// callers rebuilding the key the opener derives from
    /// `FileScanConfig::object_store_url`, and any divergence between those two
    /// derivations would silently match nothing.
    ///
    /// The over-approximation is close to free in practice. It only bites when
    /// the same *relative* path exists in two stores, and the only caller whose
    /// paths reach this cache is Cayenne, whose files are
    /// `{uuid7}_p{shard}_{index}.vortex` beneath a uuid7 snapshot directory — so
    /// two stores colliding on one requires the same uuid7 twice.
    pub(crate) async fn invalidate_paths(&self, paths: HashSet<Path>) {
        if paths.is_empty() {
            return;
        }

        // Register a state for every retiring path — including paths nothing has
        // open — and hold it for the rest of this call, then mark them all
        // retired before enumerating keys. A put that started before the mark
        // incremented `active_puts`, so waiting for zero proves its insert is
        // visible to the enumeration; a put that starts after it observes
        // `retired` and skips insertion. An opener that lands between the two
        // loops takes the registered state while it is still unretired, and it
        // is that same mark-then-drain protocol that covers its put.
        //
        // Registering rather than snapshotting is what puts such an opener on a
        // state this call is tracking at all. A path with no live registry entry
        // — the ordinary case for a file no scan holds open, which is what
        // compaction retires — otherwise left `for_path` free to mint a fresh
        // unretired state and repopulate a path this call had already cleared
        // (#12963). The registrations are released below, so nothing accumulates
        // as a permanent tombstone; retirement therefore still tracks a path
        // only while it is retiring it, and making cache residency follow
        // logical reachability instead is spiceai/spiceai#12962.
        let states: Vec<_> = paths
            .iter()
            .filter_map(|path| self.registered_state(path))
            .collect();
        for state in &states {
            state.retired.store(true, Ordering::SeqCst);
        }
        for state in &states {
            while state.active_puts.load(Ordering::SeqCst) > 0 {
                // Back off instead of spinning on a Moka insert. This is not a
                // safety timeout: the path is not enumerated until every put
                // that registered before retirement has completed or canceled.
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }

        // Enumerate the exact keys and use Moka's direct async invalidation so
        // returning means the buffers are removed from the cache table;
        // predicate invalidation would defer physical eviction to bounded
        // maintenance passes.
        //
        // The walk is O(entries in the whole cache), not O(retiring paths), and
        // one cache now holds every table's segments — so a single table's
        // compaction would otherwise occupy a runtime worker scanning entries
        // belonging to every other table. Run it on the blocking pool.
        // A reverse index from path to resident keys would remove the scan
        // entirely; see spiceai/spiceai#12964.
        let scan_cache = self.cache.clone();
        let scan_paths = paths.clone();
        let keys: Vec<SegmentKey> = match tokio::task::spawn_blocking(move || {
            scan_cache
                .iter()
                .filter_map(|(key, _)| {
                    scan_paths
                        .contains(key.1.as_ref())
                        .then(|| key.as_ref().clone())
                })
                .collect()
        })
        .await
        {
            Ok(keys) => keys,
            Err(error) => {
                // The scan is the only way to find the retired keys, so a failed
                // join leaves them cached. Report it rather than pretending the
                // retirement completed.
                tracing::error!(
                    target: "vortex::segment_cache",
                    %error,
                    "Segment-cache invalidation scan failed to run; retired segments stay cached until capacity evicts them"
                );
                Vec::new()
            }
        };
        for key in keys {
            self.cache.invalidate(&key).await;
        }
        // Direct invalidation removes the hash-table entries immediately, but
        // Moka's queued policy-removal records still retain the removed values
        // until housekeeping drains them. Unlike predicate scanning, this pass
        // only has to consume the already-enqueued exact-key removals.
        self.run_pending_tasks().await;

        self.unregister_unheld(states, &paths);
    }

    /// Release this retirement's registrations and drop the registry entries for
    /// `paths` that nothing else holds.
    ///
    /// Taking `states` by value is what orders the two: the entries cannot be
    /// judged unheld while this call's own `Arc`s are still counted. Only the
    /// paths just retired, not a sweep of the whole registry. A surviving entry
    /// is one something still holds — a file some scan has open, one opened
    /// mid-retirement, or a path a concurrent retirement is also retiring — and
    /// each of those needs the state left in place.
    fn unregister_unheld(&self, states: Vec<Arc<PathCacheState>>, paths: &HashSet<Path>) {
        drop(states);
        let Some(path_states) = self.path_states.as_ref() else {
            return;
        };
        for path in paths {
            path_states.remove_if(path, |_, state| state.strong_count() == 0);
        }
    }

    pub(crate) async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }

    pub(crate) async fn entry_count(&self) -> u64 {
        self.run_pending_tasks().await;
        self.cache.entry_count()
    }
}

struct PathSegmentCache {
    shared: Arc<SharedSegmentCache>,
    /// The store `path` is relative to. Part of every key this view forms.
    store: StoreKey,
    // `Arc<Path>` so forming the `(path, segment)` cache key on every `get`/`put`
    // is a refcount bump, not a `Path` (string) clone — segment reads are hot.
    path: Arc<Path>,
    state: Option<Arc<PathCacheState>>,
}

#[derive(Debug, Default)]
struct PathCacheState {
    retired: AtomicBool,
    active_puts: AtomicUsize,
}

struct ActivePutGuard<'a>(&'a PathCacheState);

impl Drop for ActivePutGuard<'_> {
    fn drop(&mut self) {
        self.0.active_puts.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl SegmentCache for PathSegmentCache {
    async fn get(&self, id: SegmentId) -> VortexResult<Option<ByteBuffer>> {
        let result = self
            .shared
            .cache
            .get(&(Arc::clone(&self.store), Arc::clone(&self.path), id))
            .await;

        // Collection reads these atomics directly, so the hot path never
        // allocates labels or synchronously records metrics.
        self.shared.accesses.fetch_add(1, Ordering::Relaxed);
        if result.is_some() {
            self.shared.hits.fetch_add(1, Ordering::Relaxed);
        }

        Ok(result)
    }

    async fn put(&self, id: SegmentId, buffer: ByteBuffer) -> VortexResult<()> {
        // Two checks around the active-put registration close both races with
        // retirement: a put already registered makes invalidation wait; a put
        // that read `retired = false` just before the mark observes it on the
        // second check and never inserts.
        let _active_put = if let Some(state) = self.state.as_ref() {
            if state.retired.load(Ordering::SeqCst) {
                return Ok(());
            }
            state.active_puts.fetch_add(1, Ordering::SeqCst);
            let guard = ActivePutGuard(state);
            if state.retired.load(Ordering::SeqCst) {
                return Ok(());
            }
            Some(guard)
        } else {
            None
        };

        // Copy into an exact-sized allocation, after the retirement checks so a
        // compaction burst does not pay a memcpy per discarded put.
        //
        // The buffer handed to us is usually a view into a coalesced read block:
        // `ObjectStoreReadAt` merges requests within 1 MiB into reads of up to
        // 16 MiB, and the reader hands each segment `base.slice(..)` over that one
        // allocation. `ByteBuffer` is backed by `bytes::Bytes`, so a slice shares
        // the allocation and keeps all of it alive while the weigher counts only
        // the slice's length. Caching a view would therefore let one small
        // segment pin its whole block, and — worse — the overshoot would grow as
        // eviction proceeded, because dropping some slices frees nothing until
        // the last one goes. Copying makes the weight the true resident size, so
        // `max_capacity` bounds real memory.
        //
        // Only small copies run inline. A segment can reach the 16 MiB coalescing
        // ceiling, and copying that on a runtime worker would hold it far past the
        // ~100µs an async task may go without yielding, delaying unrelated
        // queries; anything above the threshold goes to the blocking pool.
        let buffer = if buffer.len() <= INLINE_TRIM_MAX_BYTES {
            ByteBuffer::copy_from_aligned(buffer.as_slice(), buffer.alignment())
        } else {
            let alignment = buffer.alignment();
            match tokio::task::spawn_blocking(move || {
                ByteBuffer::copy_from_aligned(buffer.as_slice(), alignment)
            })
            .await
            {
                Ok(trimmed) => trimmed,
                Err(error) => {
                    // Caching the untrimmed view would pin its whole read block
                    // while accounting for a fraction of it, so skip the insert.
                    tracing::warn!(
                        target: "vortex::segment_cache",
                        %error,
                        "Segment trim failed to run; not caching this segment"
                    );
                    return Ok(());
                }
            }
        };

        self.shared
            .cache
            .insert(
                (Arc::clone(&self.store), Arc::clone(&self.path), id),
                buffer,
            )
            .await;
        Ok(())
    }
}

impl Drop for PathSegmentCache {
    fn drop(&mut self) {
        let (Some(state), Some(path_states)) =
            (self.state.as_ref(), self.shared.path_states.as_ref())
        else {
            return;
        };
        // `remove_if` evaluates the predicate while holding this path's shard
        // lock, which `for_path` also takes before upgrading the registry's weak
        // entry. Checking ownership there — rather than before taking a lock —
        // is what stops an opener from upgrading between the last-owner check and
        // the removal, which would leave it on an unregistered state that a later
        // retirement could not mark.
        path_states.remove_if(self.path.as_ref(), |_, registered| {
            std::ptr::addr_eq(registered.as_ptr(), Arc::as_ptr(state))
                && Arc::strong_count(state) == 1
        });
    }
}
#[cfg(test)]
mod tests {
    //! Every cache built here takes a `name` unique to its test. `new`
    //! registers globally and `name` is the only label on the resulting series,
    //! so two concurrent tests sharing one feed a single series from
    //! independent counters — observed to make a delta reader read a stale
    //! value, and to panic inside the SDK subtracting a larger previous sample
    //! from a smaller current one.

    use std::collections::HashMap;
    use std::sync::Weak;
    use std::time::Duration;

    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::reader::MetricReader;
    use opentelemetry_sdk::metrics::{ManualReader, Pipeline, Temporality};
    use prometheus::proto::MetricType;

    use super::*;

    struct MetricsHarness {
        registry: prometheus::Registry,
        provider: SdkMeterProvider,
    }

    impl MetricsHarness {
        fn new() -> Self {
            let registry = prometheus::Registry::new();
            let exporter = opentelemetry_prometheus::exporter()
                .with_registry(registry.clone())
                .without_scope_info()
                .without_units()
                .without_counter_suffixes()
                .without_target_info()
                .build()
                .expect("build the Prometheus exporter");
            let provider = SdkMeterProvider::builder()
                .with_resource(Resource::builder_empty().build())
                .with_reader(exporter)
                .build();
            Self { registry, provider }
        }

        /// One cache with instruments registered against it. The returned
        /// `SegmentCacheMetrics` must be held for as long as the test collects.
        fn cache(
            &self,
            name: &str,
            capacity_bytes: u64,
        ) -> (Arc<SharedSegmentCache>, SegmentCacheMetrics) {
            let cache = SharedSegmentCache::new(capacity_bytes, false, name);
            let metrics =
                SegmentCacheMetrics::register(&self.provider.meter("cayenne_segment_cache_test"));
            (cache, metrics)
        }

        fn gather(&self) -> MetricSamples {
            MetricSamples::from_registry(&self.registry)
        }
    }

    #[derive(Clone, Debug)]
    struct SharedManualReader(Arc<ManualReader>);

    impl MetricReader for SharedManualReader {
        fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
            self.0.register_pipeline(pipeline);
        }

        fn collect(&self, metrics: &mut ResourceMetrics) -> OTelSdkResult {
            self.0.collect(metrics)
        }

        fn force_flush(&self) -> OTelSdkResult {
            self.0.force_flush()
        }

        fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
            self.0.shutdown_with_timeout(timeout)
        }

        fn temporality(&self, kind: opentelemetry_sdk::metrics::InstrumentKind) -> Temporality {
            self.0.temporality(kind)
        }
    }

    struct DeltaMetricsHarness {
        registry: prometheus::Registry,
        provider: SdkMeterProvider,
        reader: SharedManualReader,
    }

    impl DeltaMetricsHarness {
        fn new() -> Self {
            let registry = prometheus::Registry::new();
            let exporter = opentelemetry_prometheus::exporter()
                .with_registry(registry.clone())
                .without_scope_info()
                .without_units()
                .without_counter_suffixes()
                .without_target_info()
                .build()
                .expect("build the Prometheus exporter");
            let reader = SharedManualReader(Arc::new(
                ManualReader::builder()
                    .with_temporality(Temporality::Delta)
                    .build(),
            ));
            let provider = SdkMeterProvider::builder()
                .with_resource(Resource::builder_empty().build())
                .with_reader(exporter)
                .with_reader(reader.clone())
                .build();
            Self {
                registry,
                provider,
                reader,
            }
        }

        fn cache(
            &self,
            name: &str,
            capacity_bytes: u64,
        ) -> (Arc<SharedSegmentCache>, SegmentCacheMetrics) {
            let cache = SharedSegmentCache::new(capacity_bytes, false, name);
            let metrics =
                SegmentCacheMetrics::register(&self.provider.meter("segment_cache_delta_test"));
            (cache, metrics)
        }

        fn gather(&self) -> MetricSamples {
            MetricSamples::from_registry(&self.registry)
        }

        fn collect_accesses(&self, cache: &str) -> Option<u64> {
            let mut resource_metrics = ResourceMetrics::default();
            self.reader
                .collect(&mut resource_metrics)
                .expect("collect delta segment-cache metrics");

            for metric in resource_metrics
                .scope_metrics()
                .flat_map(|scope| scope.metrics())
                .filter(|metric| metric.name() == "cayenne_segment_cache_accesses")
            {
                let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
                    panic!("segment-cache accesses must be a u64 sum");
                };
                // The registry is process-wide, so pick this cache's series
                // rather than whichever one happens to come first.
                if let Some(point) = sum.data_points().find(|point| {
                    point.attributes().any(|attribute| {
                        attribute.key.as_str() == "cache" && attribute.value.as_str() == cache
                    })
                }) {
                    return Some(point.value());
                }
            }

            None
        }
    }

    struct MetricSamples {
        values: HashMap<(String, String), (MetricType, f64)>,
        series_counts: HashMap<(String, String), usize>,
    }

    impl MetricSamples {
        fn from_registry(registry: &prometheus::Registry) -> Self {
            let mut values = HashMap::new();
            let mut series_counts = HashMap::new();

            for family in registry
                .gather()
                .into_iter()
                .filter(|family| family.name().starts_with("cayenne_segment_cache_"))
            {
                let metric_type = family.get_field_type();
                for metric in family.get_metric() {
                    // Every cache reports under its own `cache` label, and the
                    // registry is process-wide, so a sibling test's cache can
                    // appear here too — key by label and assert per cache.
                    let cache = metric
                        .get_label()
                        .iter()
                        .find(|label| label.name() == "cache")
                        .map_or_else(String::new, |label| label.value().to_string());
                    let value = match metric_type {
                        MetricType::COUNTER => metric.get_counter().value(),
                        MetricType::GAUGE => metric.get_gauge().value(),
                        other => panic!("unexpected segment-cache metric type {other:?}"),
                    };
                    let key = (family.name().to_string(), cache);
                    *series_counts.entry(key.clone()).or_insert(0) += 1;
                    values.insert(key, (metric_type, value));
                }
            }

            Self {
                values,
                series_counts,
            }
        }

        fn assert_value(&self, metric: &str, cache: &str, metric_type: MetricType, expected: u32) {
            let key = (metric.to_string(), cache.to_string());
            let actual = self
                .values
                .get(&key)
                .unwrap_or_else(|| panic!("missing metric {metric} for cache {cache}"));
            assert_eq!(actual.0, metric_type, "wrong type for {metric}");
            assert!(
                (actual.1 - f64::from(expected)).abs() < f64::EPSILON,
                "wrong value for {metric}: expected {expected}, got {}",
                actual.1
            );
            assert_eq!(
                self.series_counts.get(&key),
                Some(&1),
                "metric {metric} must have exactly one series for cache {cache}"
            );
        }

        fn assert_absent(&self, cache: &str) {
            assert!(
                self.values.keys().all(|(_, label)| label != cache),
                "cache {cache} should report nothing, found {:?}",
                self.values.keys().collect::<Vec<_>>()
            );
        }
    }

    /// The store every single-store test keys against.
    fn test_store() -> StoreKey {
        Arc::from("memory:///")
    }

    async fn settle_cache_bookkeeping(cache: &SharedSegmentCache) {
        cache.cache.run_pending_tasks().await;
    }

    /// Regression test: one cache serves every table, and `Path` is
    /// store-relative, so the store has to be part of the key. Without it, two
    /// stores holding the same relative path return each other's bytes — wrong
    /// data, not just a wrong hit rate.
    #[tokio::test]
    async fn identical_paths_in_different_stores_do_not_collide() {
        let shared = SharedSegmentCache::new(1 << 20, false, "stores");
        let path = Path::from("narrow/019ff413/019ff41a/data.vortex");
        let id = SegmentId::from(1);

        let warm = shared.for_path(Arc::from("file:///"), path.clone());
        let cold = shared.for_path(Arc::from("s3://datalake/"), path.clone());

        warm.put(id, ByteBuffer::copy_from(vec![1_u8, 1, 1, 1]))
            .await
            .expect("cache the warm-tier segment");

        assert!(
            cold.get(id).await.expect("get should not error").is_none(),
            "the same relative path in another store must not hit"
        );

        cold.put(id, ByteBuffer::copy_from(vec![2_u8, 2, 2, 2]))
            .await
            .expect("cache the cold-tier segment");

        let from_warm = warm
            .get(id)
            .await
            .expect("get should not error")
            .expect("the warm entry is still cached");
        let from_cold = cold
            .get(id)
            .await
            .expect("get should not error")
            .expect("the cold entry is cached");
        assert_eq!(
            from_warm.as_slice(),
            &[1, 1, 1, 1],
            "each store must get its own bytes back"
        );
        assert_eq!(from_cold.as_slice(), &[2, 2, 2, 2]);

        shared.run_pending_tasks().await;
        assert_eq!(
            shared.cache.entry_count(),
            2,
            "the two stores occupy separate entries"
        );
    }

    #[tokio::test]
    async fn put_trims_a_coalesced_slice_so_the_weight_is_the_resident_size() {
        // The reader coalesces adjacent segment requests into one read of up to
        // 16 MiB and hands each segment a slice over that single allocation. A
        // cached slice would keep the whole block alive while weighing only its
        // own length, so `put` must copy.
        const BLOCK_BYTES: usize = 16 * 1024 * 1024;
        const SEGMENT_BYTES: usize = 64 * 1024;

        let block = ByteBuffer::copy_from(vec![7u8; BLOCK_BYTES]);
        let segment = block.slice(0..SEGMENT_BYTES);
        let segment_ptr = segment.as_slice().as_ptr();

        let shared = SharedSegmentCache::new(8 * 1024 * 1024, false, "trim");
        let cache = shared.for_path(test_store(), Path::from("coalesced.vortex"));
        let id = SegmentId::from(1);
        cache
            .put(id, segment.clone())
            .await
            .expect("put should not error");

        let cached = cache
            .get(id)
            .await
            .expect("get should not error")
            .expect("the segment should be cached");
        assert_eq!(cached.len(), SEGMENT_BYTES, "the segment round-trips whole");
        assert_eq!(
            cached.as_slice(),
            segment.as_slice(),
            "trimming must preserve the bytes"
        );
        assert!(
            !std::ptr::eq(cached.as_slice().as_ptr(), segment_ptr),
            "the cached buffer must own its allocation, not alias the 16 MiB block"
        );
        assert_eq!(
            cached.alignment(),
            segment.alignment(),
            "trimming must preserve alignment so decode stays zero-copy"
        );

        shared.run_pending_tasks().await;
        assert_eq!(
            shared.cache.weighted_size(),
            SEGMENT_BYTES as u64,
            "the accounted weight is the segment, and now so is the resident size"
        );
    }

    #[tokio::test]
    async fn get_put_roundtrip_and_path_isolation() {
        let shared = SharedSegmentCache::new(1 << 20, false, "roundtrip");
        let cache_a = shared.for_path(test_store(), Path::from("a.vortex"));
        let cache_b = shared.for_path(test_store(), Path::from("b.vortex"));
        let id = SegmentId::from(1);

        // Miss before insert.
        assert!(
            cache_a
                .get(id)
                .await
                .expect("get should not error")
                .is_none()
        );

        cache_a
            .put(id, ByteBuffer::from(vec![1u8, 2, 3, 4]))
            .await
            .expect("put should not error");

        // Hit on the same path + segment id.
        assert_eq!(
            cache_a
                .get(id)
                .await
                .expect("get should not error")
                .map(|b| b.len()),
            Some(4)
        );

        // The cache key is (path, segment id): a different path with the same
        // segment id must not collide — path isolation must survive the switch to
        // an `Arc<Path>` key.
        assert!(
            cache_b
                .get(id)
                .await
                .expect("get should not error")
                .is_none()
        );
    }

    #[test]
    fn metrics_report_initial_zero_state() {
        let harness = MetricsHarness::new();
        let (_cache, _metrics) = harness.cache("initial", 2_048);

        let samples = harness.gather();
        samples.assert_value(
            "cayenne_segment_cache_accesses",
            "initial",
            MetricType::COUNTER,
            0,
        );
        samples.assert_value(
            "cayenne_segment_cache_hits",
            "initial",
            MetricType::COUNTER,
            0,
        );
        samples.assert_value(
            "cayenne_segment_cache_weighted_bytes",
            "initial",
            MetricType::GAUGE,
            0,
        );
        samples.assert_value(
            "cayenne_segment_cache_capacity_bytes",
            "initial",
            MetricType::GAUGE,
            2_048,
        );
        samples.assert_value(
            "cayenne_segment_cache_entries",
            "initial",
            MetricType::GAUGE,
            0,
        );
    }

    #[test]
    fn counter_callbacks_cannot_publish_hits_ahead_of_accesses() {
        let counters = SharedSegmentCache::new(1_024, false, "counters");
        counters.accesses.store(1, Ordering::Relaxed);
        counters.hits.store(1, Ordering::Relaxed);

        let mut hits = u64::MAX;
        counters.observe_hits(|value| hits = value);
        assert_eq!(hits, 0, "hits wait for a completed access observation");

        let mut accesses = u64::MAX;
        counters.observe_accesses(|value| accesses = value);
        assert_eq!(accesses, 1);

        counters.accesses.store(2, Ordering::Relaxed);
        counters.hits.store(2, Ordering::Relaxed);
        counters.observe_hits(|value| hits = value);
        assert_eq!(hits, 1, "hits use the last completed access snapshot");

        counters.observe_accesses(|value| accesses = value);
        counters.observe_hits(|value| hits = value);
        assert_eq!((accesses, hits), (2, 2));
    }

    #[tokio::test]
    async fn metrics_are_visible_after_a_handful_of_accesses() {
        let harness = MetricsHarness::new();
        let (shared, _metrics) = harness.cache("active", 1_024);
        let cache = shared.for_path(test_store(), Path::from("active.vortex"));
        let id_one = SegmentId::from(1);
        let id_two = SegmentId::from(2);

        assert!(
            cache
                .get(id_one)
                .await
                .expect("initial cache miss")
                .is_none()
        );
        cache
            .put(id_one, ByteBuffer::from(vec![1_u8, 2, 3, 4]))
            .await
            .expect("insert first segment");
        settle_cache_bookkeeping(&shared).await;
        assert!(
            cache
                .get(id_one)
                .await
                .expect("read first cached segment")
                .is_some()
        );

        let first = harness.gather();
        first.assert_value(
            "cayenne_segment_cache_accesses",
            "active",
            MetricType::COUNTER,
            2,
        );
        first.assert_value(
            "cayenne_segment_cache_hits",
            "active",
            MetricType::COUNTER,
            1,
        );
        first.assert_value(
            "cayenne_segment_cache_weighted_bytes",
            "active",
            MetricType::GAUGE,
            4,
        );
        first.assert_value(
            "cayenne_segment_cache_entries",
            "active",
            MetricType::GAUGE,
            1,
        );

        // A second path wrapper over the same cache must not add a second series.
        let cache_clone = shared.for_path(test_store(), Path::from("active.vortex"));
        assert!(
            cache_clone
                .get(id_one)
                .await
                .expect("read through cloned path cache")
                .is_some()
        );
        assert!(
            cache
                .get(id_two)
                .await
                .expect("second cache miss")
                .is_none()
        );
        cache
            .put(id_two, ByteBuffer::from(vec![5_u8, 6, 7]))
            .await
            .expect("insert second segment");
        settle_cache_bookkeeping(&shared).await;

        let second = harness.gather();
        second.assert_value(
            "cayenne_segment_cache_accesses",
            "active",
            MetricType::COUNTER,
            4,
        );
        second.assert_value(
            "cayenne_segment_cache_hits",
            "active",
            MetricType::COUNTER,
            2,
        );
        second.assert_value(
            "cayenne_segment_cache_weighted_bytes",
            "active",
            MetricType::GAUGE,
            7,
        );
        second.assert_value(
            "cayenne_segment_cache_entries",
            "active",
            MetricType::GAUGE,
            2,
        );
    }

    #[tokio::test]
    async fn metrics_report_unsettled_moka_estimates_without_flushing() {
        let harness = MetricsHarness::new();
        let (shared, _metrics) = harness.cache("unsettled", 1_024);
        let cache = shared.for_path(test_store(), Path::from("unsettled.vortex"));
        cache
            .put(SegmentId::from(1), ByteBuffer::from(vec![1_u8; 9]))
            .await
            .expect("insert unsettled segment");

        let expected_weighted_bytes = u32::try_from(shared.cache.weighted_size())
            .expect("test cache weighted size fits in u32");
        let expected_entries =
            u32::try_from(shared.cache.entry_count()).expect("test cache entry count fits in u32");
        let samples = harness.gather();
        samples.assert_value(
            "cayenne_segment_cache_weighted_bytes",
            "unsettled",
            MetricType::GAUGE,
            expected_weighted_bytes,
        );
        samples.assert_value(
            "cayenne_segment_cache_entries",
            "unsettled",
            MetricType::GAUGE,
            expected_entries,
        );
    }

    #[test]
    fn metrics_stop_reporting_once_the_cache_is_dropped() {
        let harness = MetricsHarness::new();
        let (shared, _metrics) = harness.cache("retired", 1_024);
        let weak = Arc::downgrade(&shared);
        let cache = shared.for_path(test_store(), Path::from("retired.vortex"));

        harness.gather().assert_value(
            "cayenne_segment_cache_capacity_bytes",
            "retired",
            MetricType::GAUGE,
            1_024,
        );

        drop(cache);
        drop(shared);
        assert!(
            weak.upgrade().is_none(),
            "observable callbacks must not keep the cache alive"
        );

        // Every series stops, counters included. The process cache lives in a
        // `OnceLock` for the process lifetime, so this only happens to a private
        // cache — and reporting nothing for a cache that no longer exists beats
        // freezing its last value, which would read as a live-but-idle cache.
        let retired = harness.gather();
        retired.assert_absent("retired");
    }

    #[tokio::test]
    async fn a_delta_reader_sees_the_live_cache_and_nothing_after_it_drops() {
        let harness = DeltaMetricsHarness::new();
        let (shared, _metrics) = harness.cache("delta", 1_024);
        let cache = shared.for_path(test_store(), Path::from("delta.vortex"));
        let id = SegmentId::from(1);

        assert!(cache.get(id).await.expect("cache miss").is_none());
        cache
            .put(id, ByteBuffer::from(vec![1_u8; 4]))
            .await
            .expect("insert");
        assert!(cache.get(id).await.expect("cache hit").is_some());

        assert_eq!(
            harness.collect_accesses("delta"),
            Some(2),
            "a delta reader collects the accesses recorded so far"
        );

        drop(cache);
        drop(shared);
        // A callback run for one reader writes observations to every SDK
        // pipeline, so the other reader may still hold one buffered sample; its
        // first collection drains it.
        let _buffered = harness.gather();
        assert_eq!(
            harness.collect_accesses("delta"),
            None,
            "a dropped cache contributes no further observations"
        );
    }

    #[tokio::test]
    async fn invalidates_exact_paths_only() {
        let shared = SharedSegmentCache::new(1 << 20, true, "exact-paths");
        let path_a = Path::from("snapshot-a/a.vortex");
        let path_b = Path::from("snapshot-b/b.vortex");
        let cache_a = shared.for_path(test_store(), path_a.clone());
        let cache_b = shared.for_path(test_store(), path_b.clone());
        let id = SegmentId::from(1);

        cache_a
            .put(id, ByteBuffer::from(vec![1u8, 2, 3, 4]))
            .await
            .expect("put for retired path should not error");
        cache_b
            .put(id, ByteBuffer::from(vec![5u8, 6, 7, 8]))
            .await
            .expect("put for live path should not error");

        shared.invalidate_paths(HashSet::from([path_a])).await;

        assert!(
            cache_a
                .get(id)
                .await
                .expect("get for retired path should not error")
                .is_none(),
            "the retired path must be invalidated"
        );
        assert!(
            cache_b
                .get(id)
                .await
                .expect("get for live path should not error")
                .is_some(),
            "an unrelated live path must remain cached"
        );

        let late_id = SegmentId::from(2);
        cache_a
            .put(late_id, ByteBuffer::from(vec![9u8, 10, 11, 12]))
            .await
            .expect("a late put for a retired path should be ignored, not fail");
        assert!(
            cache_a
                .get(late_id)
                .await
                .expect("get after a late retired-path put should not error")
                .is_none(),
            "an already-open file cache must not repopulate a retired path"
        );
    }

    #[tokio::test]
    async fn invalidation_physically_evicts_entries_without_later_cache_activity() {
        let shared = SharedSegmentCache::new(1 << 20, true, "physical-eviction");
        let retired_path = Path::from("snapshot-a/retired.vortex");
        let live_path = Path::from("snapshot-b/live.vortex");
        let retired = shared.for_path(test_store(), retired_path.clone());
        let live = shared.for_path(test_store(), live_path);
        let id = SegmentId::from(1);

        retired
            .put(id, ByteBuffer::from(vec![1u8, 2, 3, 4]))
            .await
            .expect("put for retired path should not error");
        live.put(id, ByteBuffer::from(vec![5u8, 6, 7, 8]))
            .await
            .expect("put for live path should not error");
        for index in 0..256 {
            shared
                .for_path(
                    test_store(),
                    Path::from(format!("unrelated/{index}.vortex")),
                )
                .put(id, ByteBuffer::from(vec![9u8, 10, 11, 12]))
                .await
                .expect("put for unrelated path should not error");
        }
        shared.run_pending_tasks().await;
        assert_eq!(
            shared.cache.entry_count(),
            258,
            "retired, live, and more than one maintenance batch of unrelated paths should be resident"
        );

        shared.invalidate_paths(HashSet::from([retired_path])).await;

        assert_eq!(
            shared.cache.entry_count(),
            257,
            "invalidation must physically evict only the retired buffers before it returns"
        );
    }

    #[tokio::test]
    async fn path_state_stays_registered_until_the_last_open_file_drops() {
        let shared = SharedSegmentCache::new(1 << 20, true, "registration");
        let path = Path::from("snapshot/shared.vortex");
        let first = shared.for_path(test_store(), path.clone());
        let second = shared.for_path(test_store(), path.clone());
        let states = shared
            .path_states
            .as_ref()
            .expect("retirement tracking enabled");

        assert_eq!(
            states.get(&path).map_or(0, |state| state.strong_count()),
            2,
            "both file-cache handles must share one registered path state"
        );
        drop(first);
        assert_eq!(
            states.get(&path).map_or(0, |state| state.strong_count()),
            1,
            "dropping one opener must not unregister the state used by the other"
        );

        shared.invalidate_paths(HashSet::from([path.clone()])).await;
        second
            .put(SegmentId::from(1), ByteBuffer::from(vec![1u8, 2, 3, 4]))
            .await
            .expect("a late put on the still-registered retired state is ignored");
        assert!(
            second
                .get(SegmentId::from(1))
                .await
                .expect("get after retirement")
                .is_none(),
            "the surviving opener must observe retirement"
        );

        drop(second);
        assert!(
            !states.contains_key(&path),
            "the registry entry is removed when the last opener drops"
        );
    }

    /// A file opened while a retirement is in flight must not repopulate the
    /// path that retirement is clearing (#12963).
    ///
    /// The retirement is held mid-flight by an in-flight put on a second
    /// retiring path, which parks its drain loop on a `sleep` and makes a single
    /// poll land there deterministically. What the assertions rely on is only
    /// that no key has been invalidated yet, which the resident-segment check
    /// below establishes directly.
    ///
    /// The property asserted is the strong one: a put through a handle opened
    /// mid-retirement never lands. That is what makes the test independent of
    /// where in the retirement the put arrives — including the production window
    /// after the key scan, where nothing would remove it afterwards — and it is
    /// asserted *before* the retirement resumes so that the scan cannot be what
    /// makes it pass.
    #[tokio::test]
    async fn a_file_opened_during_retirement_cannot_repopulate_it() {
        let shared = SharedSegmentCache::new(1 << 20, true, "retirement");
        let retiring = Path::from("snapshot-a/retiring.vortex");
        let keeper = Path::from("snapshot-a/keeper.vortex");
        // Retired without ever being opened, before or during. Nothing else can
        // drop the state this retirement registers for it — the last-opener path
        // needs an opener — so it is the one that proves the registration is
        // temporary rather than a tombstone.
        let never_opened = Path::from("snapshot-a/never-opened.vortex");
        let states = shared
            .path_states
            .as_ref()
            .expect("retirement tracking enabled");
        let resident = SegmentId::from(1);
        let late = SegmentId::from(2);

        // Cache a segment under the retiring path, then drop the opener: the
        // registry keeps only a `Weak`, so nothing is left for the retirement to
        // find. This is the shape a compaction sees for any file no scan holds
        // open at the moment its snapshot is retired.
        {
            let opener = shared.for_path(test_store(), retiring.clone());
            opener
                .put(resident, ByteBuffer::from(vec![1u8, 2, 3, 4]))
                .await
                .expect("put for the retiring path should not error");
        }
        assert!(
            !states.contains_key(&retiring),
            "the dropped opener must leave no registered state for the retirement to snapshot"
        );

        // A second retiring path, held open with one put registered, parks the
        // retirement in its drain loop: it stands in for a put that started
        // before the mark, which is what that loop exists to wait for.
        let keeper_cache = shared.for_path(test_store(), keeper.clone());
        let keeper_state = states
            .get(&keeper)
            .and_then(|state| state.upgrade())
            .expect("the open keeper file registers a state");
        keeper_state.active_puts.fetch_add(1, Ordering::SeqCst);

        let mut retirement = std::pin::pin!(shared.invalidate_paths(HashSet::from([
            retiring.clone(),
            keeper.clone(),
            never_opened.clone(),
        ])));
        assert!(
            futures::poll!(retirement.as_mut()).is_pending(),
            "the in-flight put must park the retirement, leaving it mid-flight with every path marked"
        );

        // Exactly the opener the retirement snapshot could not have seen.
        let opened_mid_retirement = shared.for_path(test_store(), retiring.clone());
        assert!(
            opened_mid_retirement
                .get(resident)
                .await
                .expect("get for the resident segment should not error")
                .is_some(),
            "the retirement must still be short of invalidating any key, or it is not mid-flight at all"
        );
        opened_mid_retirement
            .put(late, ByteBuffer::from(vec![5u8, 6, 7, 8]))
            .await
            .expect("a put for a retiring path is ignored, not an error");
        assert!(
            opened_mid_retirement
                .get(late)
                .await
                .expect("get for the retiring path should not error")
                .is_none(),
            "a file opened mid-retirement must observe the retirement, not cache through it"
        );

        keeper_state.active_puts.fetch_sub(1, Ordering::SeqCst);
        // Last-opener removal counts strong references, so this test's own
        // upgrade has to go before the registry can be asserted on below.
        drop(keeper_state);
        retirement.await;

        assert_eq!(
            shared.entry_count().await,
            0,
            "neither the segment resident before the retirement nor the one a mid-retirement opener tried to add may survive it"
        );

        assert!(
            !states.contains_key(&never_opened),
            "a retirement that registers a state for a path nothing opens must not leave it behind"
        );

        drop(keeper_cache);
        drop(opened_mid_retirement);
        assert!(
            !states.contains_key(&retiring),
            "the state a mid-retirement opener took over must still go when that opener drops"
        );
    }
}
