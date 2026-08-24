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

/// Readies batches for long-term retention in a cache entry.
///
/// Every store of raw batches funnels through here rather than through its call
/// sites, so both of the things a retained batch needs happen exactly once:
///
/// * **Compaction.** `LIMIT`/`OFFSET` and top-k plans emit zero-copy slices of
///   the scan batches they came from, and an entry built from one of those pins
///   the whole scan batch for the lifetime of the entry.
/// * **Schema interning.** Every `RecordBatch` carries its own `SchemaRef`, and
///   nothing upstream shares them — batches do not even share with the stream
///   that carried them — so a collection of small batches re-holds one schema
///   per element.
///
/// Encoded entries need neither: serialization writes only the rows a batch
/// holds, and writes the schema once.
pub(crate) fn prepare_for_storage(mut batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    for batch in &mut batches {
        *batch = arrow_tools::record_batch::compact_retained_buffers(batch);
    }
    arrow_tools::schema_intern::intern_batch_schemas(&mut batches);
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
