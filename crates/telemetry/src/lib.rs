/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

pub use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::metrics::{Gauge, UpDownCounter};
use std::{sync::OnceLock, time::Duration};

#[cfg(feature = "anonymous_telemetry")]
pub mod anonymous;
pub mod exporter;
pub mod hardware;
pub mod meter;
pub mod noop;
pub mod reader;
pub mod timing;

// As recommended by the OpenTelemetry Semantic Conventions:
// https://opentelemetry.io/docs/specs/semconv/database/database-metrics/#metric-dbclientresponsereturned_rows
// We added following buckets: 25000.0, 50000.0, 100000.0, 250000.0, 500000.0
pub const ROWS_RETURNED_HISTOGRAM_BUCKETS: [f64; 18] = [
    1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 25000.0,
    50000.0, 100_000.0, 250_000.0, 500_000.0,
];

// Extended default buckets for duration histogram: 25000.0, 50000.0, 100000.0, 250000.0, 500000.0
pub const DURATION_MS_HISTOGRAM_BUCKETS: [f64; 15] = [
    0.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0, 25000.0, 50000.0,
    100_000.0, 250_000.0, 500_000.0,
];

static QUERY_COUNT: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_query_count(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_COUNT
        .get_or_init(|| {
            m.u64_counter("query_executions")
                .with_description("Number of query executions.")
                .with_unit("queries")
                .build()
        })
        .add(1, dimensions);
}

/// Register the query counter instrument so it appears in the initial export
/// without recording a phantom count.
pub fn register_query_counter(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_COUNT
        .get_or_init(|| {
            m.u64_counter("query_executions")
                .with_description("Number of query executions.")
                .with_unit("queries")
                .build()
        })
        .add(0, dimensions);
}

static QUERY_ACTIVE_COUNT: OnceLock<UpDownCounter<i64>> = OnceLock::new();

pub fn inc_query_active_count(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_ACTIVE_COUNT
        .get_or_init(|| {
            m.i64_up_down_counter("query_active_count")
                .with_description(
                    "Number of concurrent top-level queries actively being processed in the runtime.",
                )
                .with_unit("queries")
                .build()
        })
        .add(1, dimensions);
}

pub fn dec_query_active_count(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_ACTIVE_COUNT
        .get_or_init(|| {
            m.i64_up_down_counter("query_active_count")
                .with_description(
                    "Number of concurrent top-level queries actively being processed in the runtime.",
                )
                .with_unit("queries")
                .build()
        })
        .add(-1, dimensions);
}

static BYTES_PROCESSED: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_bytes_processed(bytes: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    BYTES_PROCESSED
        .get_or_init(|| {
            m.u64_counter("query_processed_bytes")
                .with_description("Number of bytes processed by the runtime.")
                .with_unit("By")
                .build()
        })
        .add(bytes, dimensions);
}

static BYTES_RETURNED: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_bytes_returned(bytes: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    BYTES_RETURNED
        .get_or_init(|| {
            m.u64_counter("query_returned_bytes")
                .with_description("Number of bytes returned to query clients.")
                .with_unit("By")
                .build()
        })
        .add(bytes, dimensions);
}

static ROWS_RETURNED: OnceLock<Histogram<u64>> = OnceLock::new();

pub fn track_rows_returned(rows: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    ROWS_RETURNED
        .get_or_init(|| {
            m.u64_histogram("query_returned_rows")
                .with_description("Number of rows returned to query clients.")
                .with_boundaries(ROWS_RETURNED_HISTOGRAM_BUCKETS.to_vec())
                .with_unit("rows")
                .build()
        })
        .record(rows, dimensions);
}

static QUERY_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

pub fn track_query_duration(duration: Duration, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_DURATION_MS
        .get_or_init(|| {
            m.f64_histogram("query_duration_ms")
                .with_description(
                    "The total amount of time spent planning and executing queries in milliseconds.",
                )
                .with_unit("ms")
                .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
}

static QUERY_EXECUTION_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

pub fn track_query_execution_duration(duration: Duration, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_EXECUTION_DURATION_MS
        .get_or_init(|| {
            m.f64_histogram("query_execution_duration_ms")
                .with_description(
                    "The total amount of time spent only executing queries. This is 0 for cached queries.",
                )
                .with_unit("ms")
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
}

static AI_INFERENCES_WITH_SPICE_COUNT: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_ai_inferences_with_spice_count(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    AI_INFERENCES_WITH_SPICE_COUNT
        .get_or_init(|| {
            m.u64_counter("ai_inferences_with_spice_count")
                .with_description("AI Inferences with Spice count")
                .with_unit("inferences")
                .build()
        })
        .add(1, dimensions);
}

static TEXT_EMBEDDINGS: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_text_embedding(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    TEXT_EMBEDDINGS
        .get_or_init(|| {
            m.u64_counter("text_embeddings")
                .with_description("Number of text embeddings requests.")
                .with_unit("embedding")
                .build()
        })
        .add(1, dimensions);
}

static TEXT_SEARCHES: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_text_search(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    TEXT_SEARCHES
        .get_or_init(|| {
            m.u64_counter("text_searches")
                .with_description("Number of text search requests.")
                .with_unit("search")
                .build()
        })
        .add(1, dimensions);
}

static VECTOR_SEARCHES: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_vector_search(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    VECTOR_SEARCHES
        .get_or_init(|| {
            m.u64_counter("vector_searches")
                .with_description("Number of vector search requests.")
                .with_unit("search")
                .build()
        })
        .add(1, dimensions);
}

static QUERY_PRODUCED_SPILLS: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_produced_spills(value: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_PRODUCED_SPILLS
        .get_or_init(|| {
            m.u64_counter("query_produced_spills")
                .with_description("Number of spills produced by the query")
                .with_unit("spills")
                .build()
        })
        .add(value, dimensions);
}

static QUERY_SPILLED_BYTES: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_spilled_bytes(value: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_SPILLED_BYTES
        .get_or_init(|| {
            m.u64_counter("query_spilled_bytes")
                .with_description("Number of spilled bytes produced by the query")
                .with_unit("By")
                .build()
        })
        .add(value, dimensions);
}

static QUERY_SPILLED_ROWS: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_spilled_rows(value: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_SPILLED_ROWS
        .get_or_init(|| {
            m.u64_counter("query_spilled_rows")
                .with_description("Number of spilled rows produced by the query")
                .with_unit("rows")
                .build()
        })
        .add(value, dimensions);
}

// Hash Index Metrics

static HASH_INDEX_BUILDS: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_hash_index_build(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    HASH_INDEX_BUILDS
        .get_or_init(|| {
            m.u64_counter("hash_index_builds")
                .with_description("Number of hash index builds completed.")
                .with_unit("builds")
                .build()
        })
        .add(1, dimensions);
}

static HASH_INDEX_BUILD_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

pub fn track_hash_index_build_duration(duration: Duration, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    HASH_INDEX_BUILD_DURATION_MS
        .get_or_init(|| {
            m.f64_histogram("hash_index_build_duration_ms")
                .with_description("Time spent building hash indexes in milliseconds.")
                .with_unit("ms")
                .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
}

static HASH_INDEX_ENTRIES: OnceLock<Histogram<u64>> = OnceLock::new();

pub fn track_hash_index_entries(entries: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    HASH_INDEX_ENTRIES
        .get_or_init(|| {
            m.u64_histogram("hash_index_entries")
                .with_description("Number of entries in hash indexes.")
                .with_boundaries(ROWS_RETURNED_HISTOGRAM_BUCKETS.to_vec())
                .with_unit("entries")
                .build()
        })
        .record(entries, dimensions);
}

static HASH_INDEX_MEMORY_BYTES: OnceLock<Histogram<u64>> = OnceLock::new();

pub fn track_hash_index_memory_bytes(bytes: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    HASH_INDEX_MEMORY_BYTES
        .get_or_init(|| {
            m.u64_histogram("hash_index_memory_bytes")
                .with_description("Memory used by hash indexes in bytes.")
                .with_unit("By")
                .build()
        })
        .record(bytes, dimensions);
}

static HASH_INDEX_LOOKUPS: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_hash_index_lookups(count: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    HASH_INDEX_LOOKUPS
        .get_or_init(|| {
            m.u64_counter("hash_index_lookups")
                .with_description("Number of hash index point lookups performed.")
                .with_unit("lookups")
                .build()
        })
        .add(count, dimensions);
}

static HASH_INDEX_LOOKUP_ROWS: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_hash_index_lookup_rows(rows: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    HASH_INDEX_LOOKUP_ROWS
        .get_or_init(|| {
            m.u64_counter("hash_index_lookup_rows")
                .with_description("Number of rows returned from hash index lookups.")
                .with_unit("rows")
                .build()
        })
        .add(rows, dimensions);
}

/// Meter for Cayenne operational (operator-facing) metrics.
///
/// Unlike the anonymous-telemetry [`meter::METER`], these instruments bind to
/// the OpenTelemetry **global** provider that the runtime installs during
/// `init_metrics` with the operator's Prometheus `/metrics`, `spice.runtime.metrics`,
/// and OTLP readers. Cayenne write-path and scan timings are operator
/// observability, not product-usage telemetry, so they must NOT route to the
/// anonymous provider.
///
/// The global meter handle is intentionally **not** cached (no `OnceLock` /
/// `LazyLock`): caching binds permanently to whatever provider is global at first
/// access, so an early access could freeze it to the startup noop provider — the
/// same race [`meter::METER`] avoids by being set only after the provider is
/// installed. Fetching it fresh defers binding to each instrument's first record
/// (inside the `get_or_init` closures below), which on the scan/write paths
/// always runs after `init_metrics`.
fn cayenne_operational_meter() -> Meter {
    global::meter("cayenne")
}

static CAYENNE_SCAN_LISTING_TABLE_CACHE_ENTRIES: OnceLock<Gauge<u64>> = OnceLock::new();

pub fn track_cayenne_scan_listing_table_cache_entries(entries: u64, dimensions: &[KeyValue]) {
    CAYENNE_SCAN_LISTING_TABLE_CACHE_ENTRIES
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_gauge("cayenne_scan_listing_table_cache_entries")
                .with_description("Number of entries in the Cayenne scan ListingTable cache.")
                .with_unit("entries")
                .build()
        })
        .record(entries, dimensions);
}

static CAYENNE_LISTING_FENCE_WAIT_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

pub fn track_cayenne_listing_fence_wait_duration(duration: Duration, dimensions: &[KeyValue]) {
    CAYENNE_LISTING_FENCE_WAIT_DURATION_MS
        .get_or_init(|| {
            cayenne_operational_meter()
                .f64_histogram("cayenne_listing_fence_wait_duration_ms")
                .with_description(
                    "Time Cayenne scans spend waiting to acquire the listing fence read lock.",
                )
                .with_unit("ms")
                .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
}

static CAYENNE_LISTING_SCAN_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

pub fn track_cayenne_listing_scan_duration(duration: Duration, dimensions: &[KeyValue]) {
    CAYENNE_LISTING_SCAN_DURATION_MS
        .get_or_init(|| {
            cayenne_operational_meter()
                .f64_histogram("cayenne_listing_scan_duration_ms")
                .with_description(
                    "Time Cayenne scans spend building the main ListingTable execution plan while holding the listing fence.",
                )
                .with_unit("ms")
                .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
}

static CAYENNE_WRITE_PHASE_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

pub fn track_cayenne_write_phase_duration(duration: Duration, dimensions: &[KeyValue]) {
    CAYENNE_WRITE_PHASE_DURATION_MS
        .get_or_init(|| {
            cayenne_operational_meter()
                .f64_histogram("cayenne_write_phase_duration_ms")
                .with_description("Time spent in Cayenne write-path phases.")
                .with_unit("ms")
                .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
}

static CAYENNE_COMPACTION_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

/// Build-once accessor for the compaction-duration histogram. The first call
/// installs the instrument against whatever global meter is current, so the
/// binary forces this (via [`register_cayenne_compaction_metrics`]) only after
/// the Prometheus meter provider is installed — otherwise the instrument would
/// bind permanently to the early noop meter and never reach `/metrics`.
fn cayenne_compaction_duration_ms() -> &'static Histogram<f64> {
    CAYENNE_COMPACTION_DURATION_MS.get_or_init(|| {
        cayenne_operational_meter()
            .f64_histogram("cayenne_compaction_duration_ms")
            .with_description("Wall-clock time of Cayenne background compaction passes.")
            .with_unit("ms")
            .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
            .build()
    })
}

/// Records the wall-clock duration of a Cayenne background compaction pass.
/// `dimensions` should carry `table` and `result` (`"completed"` | `"failed"`).
/// The histogram's count doubles as the compaction-pass counter.
pub fn track_cayenne_compaction_duration(duration: Duration, dimensions: &[KeyValue]) {
    cayenne_compaction_duration_ms().record(duration.as_secs_f64() * 1000.0, dimensions);
}

static CAYENNE_COMPACTION_MEMORY_POOL_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

/// Build-once accessor for the carved compaction-pool-size gauge.
fn cayenne_compaction_memory_pool_bytes() -> &'static Gauge<u64> {
    CAYENNE_COMPACTION_MEMORY_POOL_BYTES.get_or_init(|| {
        cayenne_operational_meter()
            .u64_gauge("cayenne_compaction_memory_pool_bytes")
            .with_description(
                "Size of the dedicated compaction memory pool carved from the query memory limit.",
            )
            .with_unit("By")
            .build()
    })
}

/// Records the size in bytes of the dedicated compaction memory pool carved from
/// the query memory limit. Published once via [`register_cayenne_compaction_metrics`].
pub fn track_cayenne_compaction_memory_pool_bytes(bytes: u64, dimensions: &[KeyValue]) {
    cayenne_compaction_memory_pool_bytes().record(bytes, dimensions);
}

static CAYENNE_COMPACTION_MEMORY_EXHAUSTED: OnceLock<Counter<u64>> = OnceLock::new();

/// Build-once accessor for the compaction-pool-exhaustion counter.
fn cayenne_compaction_memory_exhausted() -> &'static Counter<u64> {
    CAYENNE_COMPACTION_MEMORY_EXHAUSTED.get_or_init(|| {
        cayenne_operational_meter()
            .u64_counter("cayenne_compaction_memory_exhausted_total")
            .with_description(
                "Compaction passes that hit ResourcesExhausted on the dedicated compaction memory pool.",
            )
            .build()
    })
}

/// Counts compaction passes that failed because the dedicated compaction memory
/// pool could not satisfy a reservation (`ResourcesExhausted`). A non-zero rate
/// means the carve fraction is too small for the rewrite working set.
pub fn track_cayenne_compaction_memory_exhausted(dimensions: &[KeyValue]) {
    cayenne_compaction_memory_exhausted().add(1, dimensions);
}

/// Register the Cayenne compaction instruments against the global meter so they
/// appear in Prometheus `/metrics` from startup — and so the one-shot pool-size
/// gauge binds to the real Prometheus meter rather than the early noop one.
///
/// The binary MUST call this once, AFTER `init_metrics` has installed the
/// Prometheus meter provider (the compaction runtime is set up earlier, before
/// metrics init, so emitting these at carve time would bind them to the noop
/// meter permanently). `compaction_pool_bytes` is the carved pool size to publish.
pub fn register_cayenne_compaction_metrics(compaction_pool_bytes: u64) {
    // Force the histogram + counter to build now (Prometheus-backed); they show
    // up at zero until the first compaction pass updates them.
    let _ = cayenne_compaction_duration_ms();
    let _ = cayenne_compaction_memory_exhausted();
    // Publish the carved pool size against the real meter.
    cayenne_compaction_memory_pool_bytes().record(compaction_pool_bytes, &[]);
}

static CAYENNE_INLINE_TOMBSTONE_WRITES: OnceLock<Counter<u64>> = OnceLock::new();
static CAYENNE_INLINE_TOMBSTONE_KEYS: OnceLock<Counter<u64>> = OnceLock::new();

/// Counts inline-tombstone writes on the on-conflict upsert path: one increment
/// per `add_inlined_tombstone` that actually writes a tombstone (the cheap,
/// O(deleted keys) path that hides the prior inline copy of an upserted PK),
/// plus a second counter for the number of keys hidden. Pair with
/// [`track_cayenne_inline_rewrite_fallback`] — the ratio of tombstone writes to
/// rewrite fallbacks shows how often the CDC stream takes the cheap path versus
/// the O(corpus) inline rewrite. `dimensions` should carry `table`.
pub fn track_cayenne_inline_tombstone_write(keys: u64, dimensions: &[KeyValue]) {
    CAYENNE_INLINE_TOMBSTONE_WRITES
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_counter("cayenne_inline_tombstone_writes_total")
                .with_description(
                    "On-conflict upserts that wrote an inline tombstone (the cheap O(deleted keys) path that hides the prior inline copy of an upserted PK).",
                )
                .build()
        })
        .add(1, dimensions);
    CAYENNE_INLINE_TOMBSTONE_KEYS
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_counter("cayenne_inline_tombstone_keys_total")
                .with_description(
                    "Total PK keys hidden by inline tombstones on the on-conflict upsert path.",
                )
                .with_unit("keys")
                .build()
        })
        .add(keys, dimensions);
}

static CAYENNE_INLINE_REWRITE_FALLBACKS: OnceLock<Counter<u64>> = OnceLock::new();

/// Counts the O(corpus) inline-data rewrite fallback
/// (`build_inlined_data_rewrite_for_pk_keys`): one increment per call that
/// actually removed inline rows (i.e. re-decoded and rewrote the inline corpus
/// to drop the superseded copies). This still fires on the inline-insert path
/// (`write_cdc_pipelined` inline fallback). A non-zero rate alongside
/// [`track_cayenne_inline_tombstone_write`] makes the tombstone-vs-rewrite ratio
/// observable. `dimensions` should carry `table`.
pub fn track_cayenne_inline_rewrite_fallback(dimensions: &[KeyValue]) {
    CAYENNE_INLINE_REWRITE_FALLBACKS
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_counter("cayenne_inline_rewrite_fallbacks_total")
                .with_description(
                    "Calls to the O(corpus) inline-data rewrite fallback that removed superseded inline rows (vs the cheap inline-tombstone path).",
                )
                .build()
        })
        .add(1, dimensions);
}

static CAYENNE_INLINE_CACHE_DELTA_POPULATES: OnceLock<Counter<u64>> = OnceLock::new();
static CAYENNE_INLINE_CACHE_FULL_REBUILDS: OnceLock<Counter<u64>> = OnceLock::new();

/// Counts how the inline-memtable read cache was materialized on a miss: one
/// increment to the delta counter when the incremental fast path was taken
/// (the rows committed since the last view were fetched + decoded AND/OR a
/// published tombstone's removal was applied to the reused base entries —
/// cycle-5 TASK 1), or to the full-rebuild counter when the whole
/// `cayenne_inlined_data` corpus had to be re-read and re-decoded (sentinel/first
/// touch, or a structural change — inline rewrite, checkpoint, overwrite,
/// recovery, or the over-cap tombstone-delta release). Under sustained CDC the
/// delta counter should dominate even on heavy-upsert tables: a published
/// tombstone is now a delta (removal-only), so it no longer forces a full
/// rebuild on every upsert batch. A high full-rebuild rate now means inline
/// rewrites (inline-vs-inline conflicts), frequent checkpoints, or the
/// tombstone-delta queue repeatedly hitting its cap. `dimensions` should carry
/// `table`.
pub fn track_cayenne_inline_cache_populate(delta: bool, dimensions: &[KeyValue]) {
    if delta {
        CAYENNE_INLINE_CACHE_DELTA_POPULATES
            .get_or_init(|| {
                cayenne_operational_meter()
                    .u64_counter("cayenne_inline_cache_delta_populates_total")
                    .with_description(
                        "Inline-memtable cache misses satisfied by the append-only delta path (only newly committed rows fetched + decoded), avoiding the O(corpus) re-read.",
                    )
                    .build()
            })
            .add(1, dimensions);
    } else {
        CAYENNE_INLINE_CACHE_FULL_REBUILDS
            .get_or_init(|| {
                cayenne_operational_meter()
                    .u64_counter("cayenne_inline_cache_full_rebuilds_total")
                    .with_description(
                        "Inline-memtable cache misses that required a full corpus re-read + re-decode (sentinel/first touch or a structural change: rewrite, tombstone, checkpoint, overwrite, recovery).",
                    )
                    .build()
            })
            .add(1, dimensions);
    }
}

static CAYENNE_LIST_FILES_CACHE_DELTA_APPLIES: OnceLock<Counter<u64>> = OnceLock::new();
static CAYENNE_LIST_FILES_CACHE_EVICTIONS: OnceLock<Counter<u64>> = OnceLock::new();

/// Counts how a current-snapshot publish updated DataFusion's list-files cache:
/// a delta-apply (the moved files were merged onto the cached directory listing,
/// avoiding a full re-LIST) or an eviction (the whole directory entry was
/// dropped, forcing the next scan to re-LIST — the fallback for compaction,
/// retention, a cold cache, or a standalone publish). Under sustained append CDC
/// the delta-apply counter should dominate; a high eviction rate means most
/// publishes lack recorded additions or the listing keeps getting evicted out
/// from under the writer. `dimensions` should carry `table`.
pub fn track_cayenne_list_files_cache_publish(delta: bool, dimensions: &[KeyValue]) {
    if delta {
        CAYENNE_LIST_FILES_CACHE_DELTA_APPLIES
            .get_or_init(|| {
                cayenne_operational_meter()
                    .u64_counter("cayenne_list_files_cache_delta_applies_total")
                    .with_description(
                        "Current-snapshot publishes that merged the moved files onto the cached directory listing (avoiding a full re-LIST).",
                    )
                    .build()
            })
            .add(1, dimensions);
    } else {
        CAYENNE_LIST_FILES_CACHE_EVICTIONS
            .get_or_init(|| {
                cayenne_operational_meter()
                    .u64_counter("cayenne_list_files_cache_evictions_total")
                    .with_description(
                        "Current-snapshot publishes that evicted the whole directory listing (forcing the next scan to re-LIST): compaction, retention, cold cache, or standalone publish.",
                    )
                    .build()
            })
            .add(1, dimensions);
    }
}

static SNAPSHOT_BOOTSTRAP_DURATION_MS: OnceLock<Counter<f64>> = OnceLock::new();
static SNAPSHOT_BOOTSTRAP_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

pub fn record_snapshot_bootstrap_metrics(duration_ms: f64, bytes: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    SNAPSHOT_BOOTSTRAP_DURATION_MS
        .get_or_init(|| {
            m.f64_counter("dataset_acceleration_snapshot_bootstrap_duration_ms")
                .with_description(
                    "Time in milliseconds taken to download the snapshot used to bootstrap acceleration.",
                )
                .build()
        })
        .add(duration_ms, dimensions);
    SNAPSHOT_BOOTSTRAP_BYTES
        .get_or_init(|| {
            m.u64_gauge("dataset_acceleration_snapshot_bootstrap_bytes")
                .with_description(
                    "Number of bytes downloaded when bootstrapping the acceleration from a snapshot.",
                )
                .build()
        })
        .record(bytes, dimensions);
}

static SNAPSHOT_FAILURE_COUNT: OnceLock<Counter<u64>> = OnceLock::new();

pub fn record_snapshot_failure(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    SNAPSHOT_FAILURE_COUNT
        .get_or_init(|| {
            m.u64_counter("dataset_acceleration_snapshot_failure_count")
                .with_description("Number of failures encountered while writing snapshots.")
                .build()
        })
        .add(1, dimensions);
}

static SNAPSHOT_WRITE_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();
static SNAPSHOT_WRITE_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

pub fn record_snapshot_write_metrics(duration_ms: f64, bytes: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    SNAPSHOT_WRITE_DURATION_MS
        .get_or_init(|| {
            m.f64_histogram("dataset_acceleration_snapshot_write_duration_ms")
                .with_description(
                    "Time in milliseconds taken to write the latest snapshot to object storage.",
                )
                .with_unit("ms")
                .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration_ms, dimensions);
    SNAPSHOT_WRITE_BYTES
        .get_or_init(|| {
            m.u64_gauge("dataset_acceleration_snapshot_write_bytes")
                .with_description("Number of bytes written for the most recent snapshot.")
                .build()
        })
        .record(bytes, dimensions);
}

static SNAPSHOT_SKIPPED_COUNT: OnceLock<Counter<u64>> = OnceLock::new();

pub fn record_snapshot_skipped(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    SNAPSHOT_SKIPPED_COUNT
        .get_or_init(|| {
            m.u64_counter("dataset_acceleration_snapshot_skipped_count")
                .with_description("Number of snapshot creations skipped due to no data updates.")
                .build()
        })
        .add(1, dimensions);
}
