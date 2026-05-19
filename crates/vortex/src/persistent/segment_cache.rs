// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use async_trait::async_trait;
use moka::future::Cache;
use object_store::path::Path;
use vortex::buffer::ByteBuffer;
use vortex::error::VortexResult;
use vortex::layout::segments::{SegmentCache, SegmentId};

/// Shared segment cache keyed by file path and Vortex segment id.
///
/// Vortex segment ids are local to each file. This wrapper keeps a single
/// bounded cache across files while presenting each open file with the
/// `SegmentCache` interface it expects.
#[derive(Clone, Debug)]
pub(crate) struct SharedSegmentCache {
    cache: Cache<(Path, SegmentId), ByteBuffer>,
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
        Ok(self.shared.cache.get(&(self.path.clone(), id)).await)
    }

    async fn put(&self, id: SegmentId, buffer: ByteBuffer) -> VortexResult<()> {
        self.shared
            .cache
            .insert((self.path.clone(), id), buffer)
            .await;
        Ok(())
    }
}