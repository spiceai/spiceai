// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::collections::HashSet;
use std::fmt::Write as _;
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

/// How long retirement waits for the puts registered before the retirement mark
/// to finish, across the whole batch, before it enumerates keys.
///
/// A registered put has only a bounded copy (at most the 16 MiB coalescing
/// ceiling) and one Moka insert left to do, so reaching this deadline means the
/// host is starved rather than that the put is doing work. Waiting forever there
/// would hold every caller of [`SharedSegmentCache::invalidate_paths`] —
/// including the delete and overwrite paths, which await it inline — for as long
/// as the starvation lasts.
const ACTIVE_PUT_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Polling interval for the drain above. Short enough that the common case (no
/// registered put, or one about to finish) returns promptly.
const ACTIVE_PUT_DRAIN_POLL: std::time::Duration = std::time::Duration::from_millis(1);

/// How many paths a log line names before it summarizes the rest.
const DESCRIBED_PATHS_MAX: usize = 5;

/// How long retirement waits for its key scan to run on the blocking pool and
/// return.
///
/// A budget of its own rather than a share of [`ACTIVE_PUT_DRAIN_TIMEOUT`]: with
/// one deadline for both, a batch that spent it draining would then skip the
/// invalidation entirely, which is the worse of the two outcomes. The price is
/// that a wedged host can hold `invalidate_paths` in its two *waits* for the sum
/// of the two before it gives up on both.
///
/// The sum bounds the waits, not the call. What follows a scan that did return —
/// removing each found key, then `run_pending_tasks()` — carries no deadline. The
/// removals are proportional to the keys the scan found and are map operations
/// rather than I/O, but `run_pending_tasks()` drains the cache's maintenance
/// queue, whose depth is not a function of this batch. That phase is unmeasured
/// rather than known-small; spiceai/spiceai#13490 tracks measuring it and giving
/// it a deadline of its own if it needs one.
#[cfg(not(test))]
const INVALIDATION_SCAN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Shortened under test, because the one test that reaches this deadline needs a
/// real clock — Tokio will not auto-advance a paused one while a blocking task
/// is outstanding, which is that test's premise. Note the shortening applies to
/// this crate's unit tests only; an integration test linking the normal build
/// gets the value above.
#[cfg(test)]
const INVALIDATION_SCAN_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);

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
        let state = self.path_states.as_ref().map(|path_states| {
            // `entry` holds this shard's lock across the upgrade-or-insert, so a
            // concurrent drop for the same path cannot unregister a state between
            // the lookup and the insert.
            match path_states.entry(path.clone()) {
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
            }
        });
        Arc::new(PathSegmentCache {
            shared: Arc::clone(self),
            store,
            path: Arc::new(path),
            state,
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

        // Mark every path that is *currently registered* retired before
        // enumerating keys. A put that started before this mark incremented
        // `active_puts`, so draining to zero makes its insert visible to the
        // enumeration; a later put through one of these states observes `retired`
        // and skips insertion. The drain is bounded — see
        // [`ACTIVE_PUT_DRAIN_TIMEOUT`] for what a timeout costs and why waiting
        // forever costs more.
        //
        // That covers openers this snapshot can see, and no more. A path whose
        // registry entry is absent or whose weak has expired can be opened again
        // after the snapshot: `for_path` mints a fresh state with
        // `retired == false`, and a put through it can land after the key scan,
        // leaving the retired path repopulated. Closing that needs a retirement
        // marker installed under the same shard lock and kept until no stale
        // opener can appear — tracked in spiceai/spiceai#12963, since it changes
        // the no-permanent-tombstones property this design was built around.
        let mut retiring: Vec<&Path> = Vec::new();
        let mut states: Vec<Arc<PathCacheState>> = Vec::new();
        if let Some(path_states) = self.path_states.as_ref() {
            for path in &paths {
                if let Some(state) = path_states.get(path).and_then(|state| state.upgrade()) {
                    retiring.push(path);
                    states.push(state);
                }
            }
        }
        for state in &states {
            state.retired.store(true, Ordering::SeqCst);
        }
        if !drain_active_puts(&states, ACTIVE_PUT_DRAIN_TIMEOUT).await {
            // Proceeding is the lesser harm, and what it costs is bounded: a put
            // that inserts into a retired path removes its own entry once the
            // insert lands (see `put`), so a straggler leaves nothing behind
            // even when it finishes after the enumeration below. Reads are
            // unaffected either way — every caller has already deleted the file.
            let stalled: Vec<&Path> = retiring
                .iter()
                .zip(&states)
                .filter(|(_, state)| state.active_puts.load(Ordering::SeqCst) > 0)
                .map(|(path, _)| *path)
                .collect();
            // The recount can come back empty when the last straggler finished
            // inside the deadline's own shadow. Nothing was left behind, so
            // there is nothing to report.
            if !stalled.is_empty() {
                let stalled_paths = stalled.len();
                tracing::warn!(
                    target: "vortex::segment_cache",
                    stalled_paths,
                    "{}",
                    drain_gave_up_warning(&stalled)
                );
            }
        }

        // Enumerate the exact keys and use Moka's direct async invalidation so
        // returning means the buffers this enumeration reached are removed from
        // the cache table; predicate invalidation would defer physical eviction
        // to bounded maintenance passes. What the enumeration does not reach —
        // because the bound below gave up on it — stays cached until capacity
        // evicts it.
        //
        // The walk is O(entries in the whole cache), not O(retiring paths), and
        // one cache now holds every table's segments — so a single table's
        // compaction would otherwise occupy a runtime worker scanning entries
        // belonging to every other table. Run it on the blocking pool.
        // A reverse index from path to resident keys would remove the scan
        // entirely; see spiceai/spiceai#13294.
        //
        // Bound the join as well as the drain above. The pool that runs this
        // scan is the same one the large-segment copies use, so the starvation
        // the drain gives up on is precisely the condition that would leave this
        // await hanging — bounding only the drain would move the unbounded wait
        // two statements down rather than remove it.
        let scan_cache = self.cache.clone();
        let scan_paths = paths.clone();
        let mut scan = tokio::task::spawn_blocking(move || {
            scan_cache
                .iter()
                .filter_map(|(key, _)| {
                    scan_paths
                        .contains(key.1.as_ref())
                        .then(|| key.as_ref().clone())
                })
                .collect()
        });
        let keys: Vec<SegmentKey> = match tokio::time::timeout(INVALIDATION_SCAN_TIMEOUT, &mut scan)
            .await
        {
            Ok(Ok(keys)) => keys,
            Ok(Err(error)) => {
                // The scan is the only way to find the retired keys, so a
                // failed join leaves them cached. Report it rather than
                // pretending the retirement completed.
                tracing::error!(
                    target: "vortex::segment_cache",
                    %error,
                    "Failed to search cached data for the {count} file(s) just retired — {paths} — so their segments stay cached until capacity evicts them. Cause: {error}",
                    count = paths.len(),
                    paths = describe_paths(&paths.iter().collect::<Vec<_>>()),
                );
                Vec::new()
            }
            Err(_) => {
                // Give up the queue slot as well as the wait. A search that
                // has not started yet is dropped outright; one already
                // running is not interruptible, but it was the wait, not the
                // work, that was holding the caller.
                scan.abort();
                let timeout_secs = INVALIDATION_SCAN_TIMEOUT.as_secs();
                tracing::error!(
                    target: "vortex::segment_cache",
                    timeout_secs,
                    "Gave up after {timeout_secs}s searching cached data for the {count} file(s) just retired — {paths} — so their segments stay cached until capacity evicts them. Cause: the host cannot keep up with the work already queued on it.",
                    count = paths.len(),
                    paths = describe_paths(&paths.iter().collect::<Vec<_>>()),
                );
                Vec::new()
            }
        };
        // Both give-up arms above fall through here with nothing to invalidate
        // rather than returning: the registry cleanup below has to run whichever
        // way this ends. An opener that dropped while this call held its state
        // could not unregister it — `PathSegmentCache::drop` saw the extra
        // strong reference — so returning early would strand an expired entry
        // that nothing will ever revisit, the paths here being unique per
        // snapshot.
        if !keys.is_empty() {
            for key in &keys {
                self.cache.invalidate(key).await;
            }
            // Direct invalidation removes the hash-table entries immediately,
            // but Moka's queued policy-removal records still retain the removed
            // values until housekeeping drains them. Unlike predicate scanning,
            // this pass only has to consume the already-enqueued exact-key
            // removals.
            self.run_pending_tasks().await;
        }

        drop(states);
        if let Some(path_states) = self.path_states.as_ref() {
            // Only the paths just retired, not a sweep of the whole registry:
            // every other entry belongs to a file some other scan still has open.
            for path in &paths {
                path_states.remove_if(path, |_, state| state.strong_count() == 0);
            }
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

/// Wait for the puts registered before the retirement mark to finish, under a
/// single `timeout` for the whole batch.
///
/// Returns `true` when every one of them finished. One deadline for the batch
/// rather than one per path: a cleanup pass retires every file it swept, so a
/// per-path deadline would multiply by the batch size — and the cause a timeout
/// indicates is global, which is exactly when the batch is largest.
async fn drain_active_puts(states: &[Arc<PathCacheState>], timeout: std::time::Duration) -> bool {
    tokio::time::timeout(timeout, async {
        for state in states {
            while state.active_puts.load(Ordering::SeqCst) > 0 {
                // Back off instead of spinning on a Moka insert.
                tokio::time::sleep(ACTIVE_PUT_DRAIN_POLL).await;
            }
        }
    })
    .await
    .is_ok()
}

/// Render up to [`DESCRIBED_PATHS_MAX`] paths for a log line, then say how many
/// were left out. A retirement batch can hold every file a sweep sacrificed, and
/// a warning that lists them all becomes the incident.
fn describe_paths(paths: &[&Path]) -> String {
    let mut described: String = paths
        .iter()
        .take(DESCRIBED_PATHS_MAX)
        .map(|path| format!("'{path}'"))
        .collect::<Vec<_>>()
        .join(", ");
    if let Some(remaining) = paths.len().checked_sub(DESCRIBED_PATHS_MAX)
        && remaining > 0
    {
        let _ = write!(described, " and {remaining} more");
    }
    described
}

/// The warning [`SharedSegmentCache::invalidate_paths`] emits for the retiring paths whose
/// in-flight writes did not drain inside the deadline.
///
/// Built here rather than inline so its text is asserted in a unit test: it is the only account an
/// operator gets of why a retired file's segments are still occupying the shared cache, and it has
/// to distinguish the two outcomes past the deadline. A write that finishes clears its own entry
/// (`PathSegmentCache::put` re-checks `is_retired`), so that residency really is momentary; a write
/// cancelled after inserting has no such second chance and its entry stays until capacity eviction
/// — the case spiceai/spiceai#12963 tracks. Promising only the first would have an operator
/// dismiss sustained cache pressure as transient.
fn drain_gave_up_warning(stalled: &[&Path]) -> String {
    format!(
        "Gave up after {timeout_secs}s waiting for in-flight cache writes on {stalled_paths} \
         retiring file(s) — {paths} — so their segments stay cached past the retirement: a write \
         that finishes clears its own entry, but one cancelled after inserting leaves the segment \
         until capacity eviction. Queries are unaffected because those files are already deleted. \
         Writes this slow mean the host cannot keep up with the work already queued on it.",
        timeout_secs = ACTIVE_PUT_DRAIN_TIMEOUT.as_secs(),
        stalled_paths = stalled.len(),
        paths = describe_paths(stalled),
    )
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

impl PathSegmentCache {
    fn key(&self, id: SegmentId) -> SegmentKey {
        (Arc::clone(&self.store), Arc::clone(&self.path), id)
    }

    /// Whether this path has been retired since the file was opened. Always
    /// `false` for a cache built without retirement tracking, which has no state
    /// to mark.
    fn is_retired(&self) -> bool {
        self.state
            .as_ref()
            .is_some_and(|state| state.retired.load(Ordering::SeqCst))
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
                // Re-check retirement: this arm awaited, so a retirement marked
                // while the copy ran is covered by neither check above, and the
                // drain in `invalidate_paths` only waits for this put while its
                // own deadline holds. Skipping the insert is cheaper than
                // inserting and then undoing it below.
                Ok(_) if self.is_retired() => return Ok(()),
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

        self.shared.cache.insert(self.key(id), buffer).await;

        // The insert can land after a concurrent retirement enumerated this
        // path's keys — the checks above narrow that window but cannot close it,
        // and by then nothing else will remove this entry. Removing it here is
        // what makes a straggler self-correcting, and so what makes the bounded
        // drain in `invalidate_paths` safe: giving up on a put that goes on to
        // finish costs a moment of residency rather than a retired file's
        // segment staying cached until capacity evicts it.
        //
        // Reachable only once that drain has given up. While it is still
        // waiting, this put's guard holds it, so the enumeration follows the
        // insert and finds the key — including when the put is dropped
        // mid-flight, which releases the guard but leaves the entry for the
        // enumeration. Past the deadline both this line and a cancellation
        // between the insert and it leave the entry resident until capacity
        // evicts it; closing that needs the retirement tombstone tracked in
        // spiceai/spiceai#12963.
        if self.is_retired() {
            self.shared.cache.invalidate(&self.key(id)).await;
        }
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

    /// The give-up warning is the only account an operator gets of why a retired file's segments
    /// are still resident, so it has to name both outcomes past the deadline: momentary for a write
    /// that finishes, until capacity eviction for one cancelled after inserting. Asserted rather
    /// than eyeballed because a reword that drops the second half would read as a transient blip.
    #[test]
    fn the_give_up_warning_states_both_retention_outcomes() {
        let paths = [Path::from("a.vortex"), Path::from("b.vortex")];
        let stalled: Vec<&Path> = paths.iter().collect();
        let warning = drain_gave_up_warning(&stalled);

        assert!(
            warning.contains("capacity eviction"),
            "the warning must say a cancelled write's segment stays until capacity eviction: \
             {warning}"
        );
        assert!(
            warning.contains("cancelled after inserting"),
            "the warning must name which writes leave a lasting entry: {warning}"
        );
        assert!(
            !warning.contains("a moment longer"),
            "the warning must not promise only momentary residency: {warning}"
        );
        assert!(
            warning.contains("'a.vortex'") && warning.contains("'b.vortex'"),
            "the warning must name the retiring files it is about: {warning}"
        );
        assert!(
            warning.contains("2 retiring file(s)"),
            "the warning must count the files it is about: {warning}"
        );
        assert_eq!(
            warning.lines().count(),
            1,
            "a log line must not span more than one record: {warning}"
        );
    }

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

    /// The registered state for `path`, which an open file cache keeps alive.
    fn path_state(shared: &SharedSegmentCache, path: &Path) -> Arc<PathCacheState> {
        shared
            .path_states
            .as_ref()
            .expect("retirement tracking enabled")
            .get(path)
            .and_then(|state| state.upgrade())
            .expect("an open file registered a path state")
    }

    /// How long [`wait_for`] polls before it gives up. Long enough that a loaded
    /// machine does not fail a healthy test, short enough that a setup which
    /// never becomes true reports a failure rather than running to the harness's
    /// own kill.
    const READINESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    /// Poll `condition` until it holds, bounded.
    ///
    /// The condition these tests wait on is reached by another task or blocking
    /// thread, so it cannot be asserted directly. An unbounded poll turns a
    /// setup that never arrives — a pool that never schedules the occupier, a
    /// retirement that never marks the path — into a hang, and a hung test
    /// reports nothing at all: it is the timeout that turns it back into a
    /// failure naming what was being waited for.
    ///
    /// Call this only from a test on a real clock. Under `start_paused` the
    /// runtime auto-advances time whenever every task is idle, so the sleep
    /// below would burn the whole budget in a handful of iterations and the
    /// bound would fire before the condition ever could.
    async fn wait_for(awaited: &str, condition: impl Fn() -> bool) {
        let deadline = tokio::time::Instant::now() + READINESS_TIMEOUT;
        while !condition() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out after {}s waiting for {awaited}",
                READINESS_TIMEOUT.as_secs()
            );
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    /// Register a put against `path` and never finish it, the way a put stalled
    /// on a starved host looks to retirement.
    fn stick_a_put(shared: &SharedSegmentCache, path: &Path) {
        path_state(shared, path)
            .active_puts
            .fetch_add(1, Ordering::SeqCst);
    }

    /// Regression test: one cache serves every table, and `Path` is
    /// store-relative, so the store has to be part of the key. Without it, two
    /// stores holding the same relative path return each other's bytes — wrong
    /// data, not just a wrong hit rate.
    #[tokio::test]
    async fn identical_paths_in_different_stores_do_not_collide() {
        let shared = SharedSegmentCache::new(1 << 20, false, "test");
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

        let shared = SharedSegmentCache::new(8 * 1024 * 1024, false, "test");
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
        let shared = SharedSegmentCache::new(1 << 20, false, "test");
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
        let counters = SharedSegmentCache::new(1_024, false, "test");
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
        let shared = SharedSegmentCache::new(1 << 20, true, "test");
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
        let shared = SharedSegmentCache::new(1 << 20, true, "test");
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

    /// Retirement gives up on a put that never completes rather than waiting on
    /// it, because the delete and overwrite paths await `invalidate_paths`
    /// inline. Regression test for spiceai/spiceai#12964.
    #[tokio::test(start_paused = true)]
    async fn a_stuck_put_bounds_the_retirement_drain_instead_of_holding_it() {
        let shared = SharedSegmentCache::new(1 << 20, true, "test");
        let path = Path::from("snapshot-a/stuck.vortex");
        let file = shared.for_path(test_store(), path.clone());
        let id = SegmentId::from(1);
        file.put(id, ByteBuffer::from(vec![1u8, 2, 3, 4]))
            .await
            .expect("put should not error");

        stick_a_put(&shared, &path);

        let started = tokio::time::Instant::now();
        // Bounded from the outside too, so an unbounded drain fails this test
        // with a message instead of hanging the run.
        tokio::time::timeout(
            ACTIVE_PUT_DRAIN_TIMEOUT * 10,
            shared.invalidate_paths(HashSet::from([path])),
        )
        .await
        .expect("retirement must not wait on a stuck put indefinitely");
        let waited = started.elapsed();

        assert!(
            waited >= ACTIVE_PUT_DRAIN_TIMEOUT,
            "retirement must wait for the stuck put before giving up, waited {waited:?}"
        );
        assert!(
            waited < ACTIVE_PUT_DRAIN_TIMEOUT * 2,
            "retirement must give up near the deadline, not keep waiting: waited {waited:?}"
        );
        assert!(
            file.get(id)
                .await
                .expect("get after retirement should not error")
                .is_none(),
            "the already-cached segment must still be evicted once the drain gives up"
        );
    }

    /// The key scan's own `spawn_blocking` is bounded too: it queues behind the
    /// same starvation the drain gives up on, so leaving that join unbounded
    /// would move the unbounded wait two statements down rather than remove it.
    /// Regression test for spiceai/spiceai#12964.
    #[test]
    fn a_saturated_blocking_pool_cannot_hold_retirement_open() {
        // Real time, not a paused clock: Tokio does not auto-advance while a
        // blocking task is outstanding, and an outstanding blocking task is this
        // test's whole premise. `INVALIDATION_SCAN_TIMEOUT` is short under test
        // so the wait is a fraction of a second.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .max_blocking_threads(1)
            .build()
            .expect("build a runtime with a one-thread blocking pool");

        runtime.block_on(async {
            let shared = SharedSegmentCache::new(1 << 20, true, "test");
            let path = Path::from("snapshot-a/saturated.vortex");
            let file = shared.for_path(test_store(), path.clone());
            let id = SegmentId::from(1);
            file.put(id, ByteBuffer::from(vec![1u8, 2, 3, 4]))
                .await
                .expect("put should not error");

            // Occupy the only blocking thread, so the key scan can be queued but
            // never run. Poll the flag the thread sets rather than sleeping a
            // fixed amount, so saturation is a fact before retirement starts.
            let occupied = Arc::new(AtomicBool::new(false));
            let (release, released) = std::sync::mpsc::channel::<()>();
            let signal = Arc::clone(&occupied);
            let blocker = tokio::task::spawn_blocking(move || {
                signal.store(true, Ordering::SeqCst);
                let _ = released.recv();
            });
            wait_for("the blocking pool to be saturated", || {
                occupied.load(Ordering::SeqCst)
            })
            .await;

            tokio::time::timeout(
                INVALIDATION_SCAN_TIMEOUT * 3,
                shared.invalidate_paths(HashSet::from([path])),
            )
            .await
            .expect("retirement must not wait on a saturated blocking pool");

            // The scan never ran, so the entry is still resident — that is the
            // stated cost of giving up, and asserting it keeps the bound from
            // being confused with a successful retirement.
            assert!(
                file.get(id).await.expect("get should not error").is_some(),
                "a scan that never ran cannot have evicted anything"
            );

            drop(release);
            blocker.await.expect("the blocking task should not panic");
        });
    }

    /// Giving up on the key scan must not strand the path's registry entry. An
    /// opener that drops while retirement holds its state cannot unregister it —
    /// `PathSegmentCache::drop` sees retirement's own reference — so the
    /// give-up path has to do the cleanup the success path does.
    #[test]
    fn giving_up_on_the_scan_still_unregisters_the_retired_path() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .max_blocking_threads(1)
            .build()
            .expect("build a runtime with a one-thread blocking pool");

        runtime.block_on(async {
            let shared = SharedSegmentCache::new(1 << 20, true, "test");
            let path = Path::from("snapshot-a/stranded.vortex");
            let file = shared.for_path(test_store(), path.clone());
            file.put(SegmentId::from(1), ByteBuffer::from(vec![1u8, 2, 3, 4]))
                .await
                .expect("put should not error");

            let occupied = Arc::new(AtomicBool::new(false));
            let (release, released) = std::sync::mpsc::channel::<()>();
            let signal = Arc::clone(&occupied);
            let blocker = tokio::task::spawn_blocking(move || {
                signal.store(true, Ordering::SeqCst);
                let _ = released.recv();
            });
            wait_for("the blocking pool to be saturated", || {
                occupied.load(Ordering::SeqCst)
            })
            .await;

            // Retirement parks on the scan it will never get, holding the path
            // state; the opener then drops underneath it.
            let retiring = Arc::clone(&shared);
            let retired_path = path.clone();
            let invalidation = tokio::spawn(async move {
                retiring
                    .invalidate_paths(HashSet::from([retired_path]))
                    .await;
            });
            // Wait for the retirement mark — which current-thread scheduling
            // order alone would also reach, but only incidentally. The mark is
            // set while `invalidate_paths` holds its own strong reference to the
            // path state, so observing it is what makes the drop below land
            // *underneath* the retirement: the arrangement this test needs in
            // order to reach the give-up cleanup at all. If the drop went first,
            // `PathSegmentCache::drop` would unregister the path itself and the
            // assertion would pass without that cleanup ever running.
            //
            // Scoped so the handle cannot outlive the poll — the cleanup under
            // test only removes an entry whose `strong_count()` has reached zero,
            // so a strong reference held into the assertion would fail the test
            // for a reason that has nothing to do with the code it checks.
            {
                let state = path_state(&shared, &path);
                wait_for("retirement to mark the path retired", || {
                    state.retired.load(Ordering::SeqCst)
                })
                .await;
            }
            drop(file);

            tokio::time::timeout(INVALIDATION_SCAN_TIMEOUT * 3, invalidation)
                .await
                .expect("retirement must not wait on a saturated blocking pool")
                .expect("the retirement task should not panic");

            assert!(
                !shared
                    .path_states
                    .as_ref()
                    .expect("retirement tracking enabled")
                    .contains_key(&path),
                "the registry entry must be removed even when the scan is given up on"
            );

            drop(release);
            blocker.await.expect("the blocking task should not panic");
        });
    }

    /// The drain deadline covers the whole retiring batch. A cleanup pass
    /// retires every file it swept, so a per-path deadline would multiply by the
    /// batch size. Regression test for spiceai/spiceai#12964.
    #[tokio::test(start_paused = true)]
    async fn stuck_puts_on_many_paths_share_one_drain_deadline() {
        const STUCK_PATHS: u32 = 8;

        let shared = SharedSegmentCache::new(1 << 20, true, "test");
        let mut files = Vec::new();
        let mut paths = HashSet::new();
        for index in 0..STUCK_PATHS {
            let path = Path::from(format!("snapshot-a/stuck-{index}.vortex"));
            let file = shared.for_path(test_store(), path.clone());
            file.put(SegmentId::from(1), ByteBuffer::from(vec![1u8, 2, 3, 4]))
                .await
                .expect("put should not error");
            stick_a_put(&shared, &path);
            paths.insert(path);
            files.push(file);
        }

        let started = tokio::time::Instant::now();
        tokio::time::timeout(
            ACTIVE_PUT_DRAIN_TIMEOUT * (STUCK_PATHS + 2),
            shared.invalidate_paths(paths),
        )
        .await
        .expect("retirement must not wait on stuck puts indefinitely");
        let waited = started.elapsed();

        assert!(
            waited >= ACTIVE_PUT_DRAIN_TIMEOUT,
            "retirement must wait for the stuck puts before giving up, waited {waited:?}"
        );
        assert!(
            waited < ACTIVE_PUT_DRAIN_TIMEOUT * 2,
            "{STUCK_PATHS} stuck paths must share one deadline, not take one each: waited {waited:?}"
        );
        for (index, file) in files.iter().enumerate() {
            assert!(
                file.get(SegmentId::from(1))
                    .await
                    .expect("get after retirement should not error")
                    .is_none(),
                "path {index} must still be invalidated once the drain gives up"
            );
        }
    }

    /// A put whose trim was still running when its path retired leaves nothing
    /// resident — whether it skips the insert or undoes it. That is what makes
    /// bounding the drain safe. Regression test for spiceai/spiceai#12964.
    #[tokio::test]
    async fn a_put_trimming_when_the_path_retires_does_not_repopulate_it() {
        // Capacity well above the segment below, so that a segment which does
        // get inserted stays resident: sized under it, Moka would evict the
        // entry on weight alone and the assertion would hold with or without the
        // code it exists to check.
        let shared = SharedSegmentCache::new(64 << 20, true, "test");
        let path = Path::from("snapshot-a/trimming.vortex");
        let file = shared.for_path(test_store(), path.clone());
        let id = SegmentId::from(1);
        let state = path_state(&shared, &path);

        // Well above the inline threshold, so the trim goes to the blocking pool
        // and takes long enough that the put is reliably still parked on it when
        // this task looks. On the current-thread runtime the put can only make
        // progress while this task yields, so yielding is also how we let it
        // reach the trim in the first place.
        let buffer = ByteBuffer::from(vec![7u8; 8 << 20]);
        let put = tokio::spawn(async move { file.put(id, buffer).await });
        let mut in_flight = false;
        // Bounded, and it escapes the moment the put is done: a loop that only
        // watched `active_puts` would spin forever on the run where the trim
        // finished before this task looked.
        for _ in 0..10_000 {
            if state.active_puts.load(Ordering::SeqCst) > 0 {
                in_flight = true;
                break;
            }
            if put.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            in_flight,
            "the put has to still be trimming for this test to exercise anything"
        );

        // The put is past both of its pre-trim retirement checks. Retire the
        // path underneath it, the way a concurrent compaction would.
        state.retired.store(true, Ordering::SeqCst);
        put.await
            .expect("the put task should not panic")
            .expect("a put racing retirement is ignored, not an error");

        assert_eq!(
            shared.entry_count().await,
            0,
            "a put that finished trimming after retirement must leave nothing resident"
        );
    }

    #[tokio::test]
    async fn path_state_stays_registered_until_the_last_open_file_drops() {
        let shared = SharedSegmentCache::new(1 << 20, true, "test");
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
}
