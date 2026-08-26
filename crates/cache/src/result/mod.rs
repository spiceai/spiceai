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

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

/// What a cache entry should retain, and what it owes a memory budget for it.
pub(crate) struct Prepared {
    pub batches: Vec<RecordBatch>,
    /// The schema to store, shared with every other entry of this shape.
    pub schema: SchemaRef,
    /// Whether this entry is the one holding that schema alive — see
    /// [`arrow_tools::schema_intern::Interned::owned`]. The weigher charges the
    /// schema exactly when it is, so one charge is made per distinct schema
    /// rather than one per entry pointing at it.
    pub schema_owned: bool,
}

/// Readies batches for retention in a cache entry: compaction and schema
/// sharing, in one pass.
///
/// Every store of raw batches funnels through here rather than through its call
/// sites, because both things a retained batch needs happen here:
///
/// * **Compaction.** `LIMIT`/`OFFSET` and top-k plans emit zero-copy slices of
///   the scan batches they came from, and an entry built from one of those pins
///   the whole scan batch for the lifetime of the entry.
/// * **Schema sharing.** Nothing upstream shares a schema — a batch does not
///   even share with the stream that carried it — so N entries over one query
///   shape would hold N copies of that shape's schema.
///
/// The two are done together rather than in sequence: compaction already
/// rebuilds each batch, so the shared schema goes into that rebuild instead of
/// costing a second pass over the batches.
///
/// Encoded entries need neither — serialization writes only the rows a batch
/// holds, and writes the schema once — but they still take a shared schema for
/// the entry itself.
/// The schema half of [`prepare_for_storage`], for an entry that retains no raw
/// batches — an encoded one, whose bytes already carry the schema once.
pub(crate) fn prepare_schema(schema: SchemaRef) -> Prepared {
    let interned = arrow_tools::schema_intern::global().intern(schema);
    Prepared {
        batches: Vec::new(),
        schema: interned.schema,
        schema_owned: interned.owned,
    }
}

pub(crate) fn prepare_for_storage(batches: Vec<RecordBatch>, schema: SchemaRef) -> Prepared {
    let interned = arrow_tools::schema_intern::global().intern(schema);

    // `into_iter` rather than `iter`: the caller is handing the batches over, and
    // mapping in place reuses the vector's allocation instead of building a
    // second one alongside it.
    let batches = batches
        .into_iter()
        .map(|batch| {
            // Only a batch whose schema is *equal* may be re-labelled; one that
            // differs keeps its own, and is merely compacted.
            if batch.schema_ref().as_ref() == interned.schema.as_ref() {
                arrow_tools::record_batch::compact_retained_buffers_as(
                    &batch,
                    Arc::clone(&interned.schema),
                )
            } else {
                arrow_tools::record_batch::compact_retained_buffers(&batch)
            }
        })
        .collect();

    Prepared {
        batches,
        schema: interned.schema,
        schema_owned: interned.owned,
    }
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
