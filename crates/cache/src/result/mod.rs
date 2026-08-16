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

pub mod embeddings;
pub mod query;
pub mod search;

use arrow::array::RecordBatch;

/// Compacts batches that retain more memory than their own rows need, so a
/// cache entry holds — and is billed — what it stores.
///
/// Every store of raw batches funnels through here rather than through its
/// call sites: `LIMIT`/`OFFSET` and top-k plans emit zero-copy slices of the
/// scan batches they came from, and an entry built from one of those pins the
/// whole scan batch for the lifetime of the entry. Encoded entries need no
/// equivalent, because serialization already writes only the rows a batch
/// holds.
pub(crate) fn compact_for_storage(mut batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    for batch in &mut batches {
        *batch = arrow_tools::record_batch::compact_retained_buffers(batch);
    }
    batches
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheStatus {
    // The request was not eligible for caching, and thus the cache was not checked.
    CacheDisabled,
    // The request asked to bypass the cache, i.e. via `Cache-Control: no-cache`.
    CacheBypass,
    // The request was a cache hit.
    CacheHit,
    // The request was a cache miss.
    CacheMiss,
    // The request was a cache hit, but the entry is stale and being revalidated in the background.
    CacheStaleWhileRevalidate,
}

impl CacheStatus {
    #[must_use]
    pub fn to_header_string(self) -> Option<&'static str> {
        match self {
            CacheStatus::CacheDisabled => None,
            CacheStatus::CacheBypass => Some("BYPASS"),
            CacheStatus::CacheHit => Some("HIT"),
            CacheStatus::CacheMiss => Some("MISS"),
            CacheStatus::CacheStaleWhileRevalidate => Some("STALE"),
        }
    }
}
