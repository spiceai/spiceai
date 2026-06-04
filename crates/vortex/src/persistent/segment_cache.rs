// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::hash::BuildHasherDefault;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use moka::future::Cache;
use object_store::path::Path;
use opentelemetry::metrics::{Gauge, Meter};
use opentelemetry::{KeyValue, global};
use twox_hash::XxHash3_64;
use vortex::buffer::ByteBuffer;
use vortex::error::VortexResult;
use vortex::layout::segments::{SegmentCache, SegmentId};

/// Hasher for the segment cache key `(Path, SegmentId)`. XXH3 matches the
/// project-wide cache hashing default and is markedly faster than moka's default
/// `SipHash` on the per-segment hot path.
type SegmentCacheHasher = BuildHasherDefault<XxHash3_64>;

/// Publish a segment-cache stats sample once every this many `get` calls (per
/// table cache).
const SEGMENT_CACHE_STATS_SAMPLE_EVERY: u64 = 200_000;

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("cayenne_segment_cache"));

/// Cumulative `get` calls, by `dataset`.
static ACCESSES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("cayenne_segment_cache_accesses")
        .with_description("Cumulative Vortex segment cache get() calls.")
        .build()
});
/// Cumulative cache hits, by `dataset`. Miss count and hit rate derive from this
/// and `cayenne_segment_cache_accesses`.
static HITS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("cayenne_segment_cache_hits")
        .with_description("Cumulative Vortex segment cache hits.")
        .build()
});
/// Live cache fill in bytes, by `dataset`. Compare to
/// `cayenne_segment_cache_capacity_bytes` for the fill level — the key
/// right-sizing signal.
static WEIGHTED_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("cayenne_segment_cache_weighted_bytes")
        .with_description("Live Vortex segment cache size in bytes.")
        .with_unit("By")
        .build()
});
/// Configured cache capacity in bytes, by `dataset`.
static CAPACITY_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("cayenne_segment_cache_capacity_bytes")
        .with_description("Configured Vortex segment cache capacity in bytes.")
        .with_unit("By")
        .build()
});
/// Live cached entry count, by `dataset`.
static ENTRIES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("cayenne_segment_cache_entries")
        .with_description("Live Vortex segment cache entry count.")
        .build()
});

/// Shared segment cache keyed by file path and Vortex segment id.
///
/// Vortex segment ids are local to each file. This wrapper keeps a single
/// bounded cache across files while presenting each open file with the
/// `SegmentCache` interface it expects.
#[derive(Clone, Debug)]
pub(crate) struct SharedSegmentCache {
    cache: Cache<(Path, SegmentId), ByteBuffer, SegmentCacheHasher>,
    /// Total `get` calls; drives periodic stats sampling.
    accesses: Arc<AtomicU64>,
    /// `get` calls that returned a cached buffer (a hit).
    hits: Arc<AtomicU64>,
    /// Configured byte capacity, reported next to the live fill.
    capacity_bytes: u64,
    /// Dataset label applied to the emitted metrics (table name; `unknown` when
    /// the cache is built without one — e.g. the plain `DataFusion` format path).
    dataset: Arc<str>,
}

impl SharedSegmentCache {
    pub(crate) fn new(max_capacity_bytes: u64, dataset: Option<Arc<str>>) -> Self {
        Self {
            cache: Cache::builder()
                .name("vortex-datafusion-segment-cache")
                .max_capacity(max_capacity_bytes)
                .weigher(|_, buffer: &ByteBuffer| {
                    u32::try_from(buffer.len().min(u32::MAX as usize)).unwrap_or(u32::MAX)
                })
                .build_with_hasher(SegmentCacheHasher::default()),
            accesses: Arc::new(AtomicU64::new(0)),
            hits: Arc::new(AtomicU64::new(0)),
            capacity_bytes: max_capacity_bytes,
            dataset: dataset.unwrap_or_else(|| Arc::from("unknown")),
        }
    }

    pub(crate) fn for_path(&self, path: Path) -> Arc<dyn SegmentCache> {
        Arc::new(PathSegmentCache {
            shared: self.clone(),
            path,
        })
    }
}

struct PathSegmentCache {
    shared: SharedSegmentCache,
    path: Path,
}

#[async_trait]
impl SegmentCache for PathSegmentCache {
    async fn get(&self, id: SegmentId) -> VortexResult<Option<ByteBuffer>> {
        let result = self.shared.cache.get(&(self.path.clone(), id)).await;

        // Segment-cache right-sizing telemetry: count accesses/hits and, every
        // SEGMENT_CACHE_STATS_SAMPLE_EVERY calls, publish the cumulative hit rate
        // and live fill (weighted bytes vs capacity) as OpenTelemetry gauges
        // labelled by `dataset`. When no meter provider is installed (Prometheus
        // off) the records are cheap no-ops.
        if result.is_some() {
            self.shared.hits.fetch_add(1, Ordering::Relaxed);
        }
        let accesses = self.shared.accesses.fetch_add(1, Ordering::Relaxed) + 1;
        if accesses.is_multiple_of(SEGMENT_CACHE_STATS_SAMPLE_EVERY) {
            // Rare branch: flush moka bookkeeping so the reported fill is accurate.
            self.shared.cache.run_pending_tasks().await;
            let hits = self.shared.hits.load(Ordering::Relaxed);
            let labels = [KeyValue::new("dataset", self.shared.dataset.to_string())];
            ACCESSES.record(accesses, &labels);
            HITS.record(hits, &labels);
            WEIGHTED_BYTES.record(self.shared.cache.weighted_size(), &labels);
            CAPACITY_BYTES.record(self.shared.capacity_bytes, &labels);
            ENTRIES.record(self.shared.cache.entry_count(), &labels);
        }

        Ok(result)
    }

    async fn put(&self, id: SegmentId, buffer: ByteBuffer) -> VortexResult<()> {
        self.shared
            .cache
            .insert((self.path.clone(), id), buffer)
            .await;
        Ok(())
    }
}
