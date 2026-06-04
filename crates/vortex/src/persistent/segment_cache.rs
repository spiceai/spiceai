// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use moka::future::Cache;
use object_store::path::Path;
use vortex::buffer::ByteBuffer;
use vortex::error::VortexResult;
use vortex::layout::segments::{SegmentCache, SegmentId};

/// Emit a segment-cache stats sample once every this many `get` calls (per
/// table cache). Lightweight always-on telemetry for right-sizing
/// `cayenne_segment_cache_mb`: surfaces observed hit rate and fill level
/// (weighted bytes vs configured capacity) without per-access logging.
const SEGMENT_CACHE_STATS_SAMPLE_EVERY: u64 = 200_000;

/// Shared segment cache keyed by file path and Vortex segment id.
///
/// Vortex segment ids are local to each file. This wrapper keeps a single
/// bounded cache across files while presenting each open file with the
/// `SegmentCache` interface it expects.
#[derive(Clone, Debug)]
pub(crate) struct SharedSegmentCache {
    cache: Cache<(Path, SegmentId), ByteBuffer>,
    /// Total `get` calls; drives periodic stats sampling.
    accesses: Arc<AtomicU64>,
    /// `get` calls that returned a cached buffer (a hit).
    hits: Arc<AtomicU64>,
    /// Configured byte capacity, logged next to the live weighted size so the
    /// fill level (the key right-sizing signal) is visible in the sample.
    capacity_bytes: u64,
}

impl SharedSegmentCache {
    pub(crate) fn new(max_capacity_bytes: u64) -> Self {
        Self {
            cache: Cache::builder()
                .name("vortex-datafusion-segment-cache")
                .max_capacity(max_capacity_bytes)
                .weigher(|_, buffer: &ByteBuffer| {
                    u32::try_from(buffer.len().min(u32::MAX as usize)).unwrap_or(u32::MAX)
                })
                .build(),
            accesses: Arc::new(AtomicU64::new(0)),
            hits: Arc::new(AtomicU64::new(0)),
            capacity_bytes: max_capacity_bytes,
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

        // Segment-cache right-sizing telemetry: count every access, record hits,
        // and periodically log the cumulative hit rate plus the cache's live fill
        // (weighted bytes vs capacity). One cache exists per table provider, so a
        // sample reflects that table; the `path` field identifies which table.
        if result.is_some() {
            self.shared.hits.fetch_add(1, Ordering::Relaxed);
        }
        let accesses = self.shared.accesses.fetch_add(1, Ordering::Relaxed) + 1;
        if accesses % SEGMENT_CACHE_STATS_SAMPLE_EVERY == 0 {
            // Rare branch: flush moka's pending bookkeeping so the reported fill
            // is accurate rather than eventually-consistent.
            self.shared.cache.run_pending_tasks().await;
            let hits = self.shared.hits.load(Ordering::Relaxed);
            tracing::info!(
                target: "vortex::segment_cache",
                path = %self.path,
                accesses,
                hits,
                misses = accesses.saturating_sub(hits),
                entries = self.shared.cache.entry_count(),
                weighted_bytes = self.shared.cache.weighted_size(),
                capacity_bytes = self.shared.capacity_bytes,
                "segment cache stats sample",
            );
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
