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

// Buckets for byte-sized payload histograms (Cayenne CDC burst / WAL telemetry).
// Spans a single small inline write (~1 KiB) through a coalesced burst at the
// default 128 MiB coalesce budget up to a multi-hundred-MiB WAL backlog, so both
// the hot-path burst shape and a stalled-checkpoint WAL stay on-scale.
pub const BYTES_HISTOGRAM_BUCKETS: [f64; 16] = [
    1024.0,
    4096.0,
    16384.0,
    65536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
    134_217_728.0,
    268_435_456.0,
    536_870_912.0,
    1_073_741_824.0,
    2_147_483_648.0,
    4_294_967_296.0,
    8_589_934_592.0,
];

// Finer-grained millisecond buckets for sub-second contention timings (metastore
// writer wait/hold, WAL checkpoint, CDC linger). The shared
// `DURATION_MS_HISTOGRAM_BUCKETS` jumps straight from 0 to 100ms, which is too
// coarse for lock/checkpoint latencies that live in the 0.1–50ms band; this set
// resolves that band while still reaching into the multi-second tail that signals
// a stall.
pub const CONTENTION_MS_HISTOGRAM_BUCKETS: [f64; 17] = [
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
    10000.0, 30000.0,
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
            .with_description(
                "Wall-clock time of Cayenne compaction passes (kind=full current-snapshot rewrite | subset protected-snapshot merge).",
            )
            .with_unit("ms")
            .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
            .build()
    })
}

/// Records the wall-clock duration of a Cayenne compaction pass. `dimensions`
/// should carry `table`, `kind` (`"full"` current-snapshot rewrite | `"subset"`
/// protected-snapshot merge), and `result` (`"completed"` | `"failed"`). The
/// histogram's count doubles as the per-kind compaction-pass counter.
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
/// `dimensions` should carry `table` and `kind` (`"full"` | `"subset"`).
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

/// Counts how a current-snapshot publish updated `DataFusion`'s list-files cache:
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

// ---- Auto-tuning (cayenne::provider::tuning) ------------------------------

/// A table's auto-tuner state: the measured ingest/response signals plus the
/// live (possibly dynamically-tuned) knob values. Emitted as gauges on every
/// background tick — useful whether or not dynamic tuning is enabled, since the
/// accounting is always recorded (it makes "what did auto pick, and how is the
/// table behaving" observable in Prometheus).
#[derive(Debug, Clone, Copy)]
pub struct CayenneAutotuneState {
    /// Measured CDC ingest rate, rows/sec (EWMA).
    pub rows_per_sec: f64,
    /// Measured CDC ingest rate, bytes/sec (EWMA); `< 0` ⇒ unavailable (the
    /// gauge is then suppressed rather than emitting a misleading 0).
    pub bytes_per_sec: f64,
    /// Apply latency / offered-load interval; `> 1` ⇒ ingest falling behind.
    pub apply_vs_arrival: f64,
    /// Read amplification (runs a scan must merge); high ⇒ ingest slowing queries.
    pub read_amp: u64,
    /// cgroup-aware memory usage fraction of the budget; `< 0` ⇒ unknown.
    pub mem_pressure: f64,
    /// Per-batch apply wall time (EWMA, ms) — the latency the controller weighs
    /// against the offered-load interval.
    pub apply_ms: f64,
    /// Live inline-memtable flush byte budget.
    pub inline_flush_max_bytes: u64,
    /// Live background compaction interval (ms).
    pub compaction_interval_ms: u64,
    /// Live small-file compaction trigger (file count).
    pub compaction_trigger_files: u64,
    /// Configured target Vortex file size (MB) — the reference compacted files
    /// should trend toward (compare against `cayenne_compaction_merged_bytes`).
    pub target_file_size_mb: u64,
    /// Live write/encode concurrency (0 = session default).
    pub write_concurrency: u64,
}

static CAYENNE_AT_ROWS_PER_SEC: OnceLock<Gauge<f64>> = OnceLock::new();
static CAYENNE_AT_BYTES_PER_SEC: OnceLock<Gauge<f64>> = OnceLock::new();
static CAYENNE_AT_APPLY_VS_ARRIVAL: OnceLock<Gauge<f64>> = OnceLock::new();
static CAYENNE_AT_READ_AMP: OnceLock<Gauge<u64>> = OnceLock::new();
static CAYENNE_AT_MEM_PRESSURE: OnceLock<Gauge<f64>> = OnceLock::new();
static CAYENNE_AT_APPLY_MS: OnceLock<Gauge<f64>> = OnceLock::new();
static CAYENNE_AT_INLINE_FLUSH_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();
static CAYENNE_AT_COMPACTION_INTERVAL_MS: OnceLock<Gauge<u64>> = OnceLock::new();
static CAYENNE_AT_COMPACTION_TRIGGER_FILES: OnceLock<Gauge<u64>> = OnceLock::new();
static CAYENNE_AT_TARGET_FILE_SIZE_MB: OnceLock<Gauge<u64>> = OnceLock::new();
static CAYENNE_AT_WRITE_CONCURRENCY: OnceLock<Gauge<u64>> = OnceLock::new();

/// Emit the auto-tuner state gauges for one table. `dimensions` should carry
/// `table`. Called on each background tick.
pub fn track_cayenne_autotune_state(state: &CayenneAutotuneState, dimensions: &[KeyValue]) {
    CAYENNE_AT_ROWS_PER_SEC
        .get_or_init(|| {
            cayenne_operational_meter()
                .f64_gauge("cayenne_ingest_rows_per_sec")
                .with_description("Measured CDC ingest rate (rows/sec, EWMA).")
                .build()
        })
        .record(state.rows_per_sec, dimensions);
    // Suppress the bytes/sec gauge when byte accounting is unavailable
    // (`bytes_per_sec < 0`) rather than reporting a misleading 0 under load.
    if state.bytes_per_sec >= 0.0 {
        CAYENNE_AT_BYTES_PER_SEC
            .get_or_init(|| {
                cayenne_operational_meter()
                    .f64_gauge("cayenne_ingest_bytes_per_sec")
                    .with_description("Measured CDC ingest rate (bytes/sec, EWMA).")
                    .with_unit("By/s")
                    .build()
            })
            .record(state.bytes_per_sec, dimensions);
    }
    CAYENNE_AT_APPLY_VS_ARRIVAL
        .get_or_init(|| {
            cayenne_operational_meter()
                .f64_gauge("cayenne_ingest_apply_vs_arrival")
                .with_description(
                    "Apply latency / offered-load interval; > 1 means ingest is falling behind.",
                )
                .build()
        })
        .record(state.apply_vs_arrival, dimensions);
    CAYENNE_AT_READ_AMP
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_gauge("cayenne_ingest_read_amp")
                .with_description(
                    "Read amplification (protected-snapshot runs a scan must merge); high means ingest is slowing queries.",
                )
                .build()
        })
        .record(state.read_amp, dimensions);
    CAYENNE_AT_MEM_PRESSURE
        .get_or_init(|| {
            cayenne_operational_meter()
                .f64_gauge("cayenne_ingest_mem_pressure")
                .with_description(
                    "cgroup-aware memory usage as a fraction of the budget; negative means unknown.",
                )
                .build()
        })
        .record(state.mem_pressure, dimensions);
    CAYENNE_AT_INLINE_FLUSH_BYTES
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_gauge("cayenne_autotune_inline_flush_max_bytes")
                .with_description("Current (live) inline-memtable flush byte budget.")
                .with_unit("By")
                .build()
        })
        .record(state.inline_flush_max_bytes, dimensions);
    CAYENNE_AT_COMPACTION_INTERVAL_MS
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_gauge("cayenne_autotune_compaction_interval_ms")
                .with_description("Current (live) background compaction interval.")
                .with_unit("ms")
                .build()
        })
        .record(state.compaction_interval_ms, dimensions);
    CAYENNE_AT_APPLY_MS
        .get_or_init(|| {
            cayenne_operational_meter()
                .f64_gauge("cayenne_ingest_apply_ms")
                .with_description("Per-batch CDC apply wall time (EWMA).")
                .with_unit("ms")
                .build()
        })
        .record(state.apply_ms, dimensions);
    CAYENNE_AT_COMPACTION_TRIGGER_FILES
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_gauge("cayenne_autotune_compaction_trigger_files")
                .with_description("Current (live) small-file compaction trigger (file count).")
                .build()
        })
        .record(state.compaction_trigger_files, dimensions);
    CAYENNE_AT_TARGET_FILE_SIZE_MB
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_gauge("cayenne_autotune_target_file_size_mb")
                .with_description(
                    "Configured target Vortex file size — the reference compacted files should trend toward (compare cayenne_compaction_merged_bytes).",
                )
                .with_unit("MiB")
                .build()
        })
        .record(state.target_file_size_mb, dimensions);
    CAYENNE_AT_WRITE_CONCURRENCY
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_gauge("cayenne_autotune_write_concurrency")
                .with_description("Current (live) write/encode concurrency (0 = session default).")
                .build()
        })
        .record(state.write_concurrency, dimensions);
}

static CAYENNE_AT_ADJUSTMENTS: OnceLock<Counter<u64>> = OnceLock::new();

/// Counts dynamic auto-tune adjustments applied. `dimensions` should carry
/// `table` and `knob`. A non-zero rate means the closed loop is actively
/// adapting the table to its observed workload.
pub fn track_cayenne_autotune_adjustment(dimensions: &[KeyValue]) {
    CAYENNE_AT_ADJUSTMENTS
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_counter("cayenne_autotune_adjustments_total")
                .with_description("Dynamic auto-tune adjustments applied, by knob.")
                .build()
        })
        .add(1, dimensions);
}

static CAYENNE_COMPACTION_MERGED_BYTES: OnceLock<Histogram<u64>> = OnceLock::new();

/// Records the bytes a protected-snapshot subset compaction merged into one
/// output (≈ the resulting compacted file size). Compare its distribution
/// against `cayenne_autotune_target_file_size_mb` to see whether compaction is
/// trending to the target file size or stalling below it (a read-amplification
/// signal the adaptive tuner cares about). `dimensions` should carry `table`
/// and `kind` (currently always `"subset"` — the full current-snapshot rewrite
/// path does not yet emit this metric).
pub fn track_cayenne_compaction_merged_bytes(bytes: u64, dimensions: &[KeyValue]) {
    const MIB: f64 = 1024.0 * 1024.0;
    CAYENNE_COMPACTION_MERGED_BYTES
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_histogram("cayenne_compaction_merged_bytes")
                .with_description(
                    "Bytes merged into one output by a protected-snapshot subset compaction (≈ compacted file size); compare to the target file size.",
                )
                .with_unit("By")
                .with_boundaries(vec![
                    MIB,
                    4.0 * MIB,
                    16.0 * MIB,
                    32.0 * MIB,
                    64.0 * MIB,
                    128.0 * MIB,
                    256.0 * MIB,
                    512.0 * MIB,
                    1024.0 * MIB,
                ])
                .build()
        })
        .record(bytes, dimensions);
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

// ───────────────────────── Cayenne CDC observability (cycle-6) ─────────────────
//
// METRIC 1 — metastore writer wait/hold. The per-dataset metastore is a single
// SQLite DB with WAL-serialized writers, so a hot CDC table can queue behind its
// own Stage-A fold / sequence-reserve / publish-flip writes. These two histograms
// split that into (a) time spent waiting to acquire the write transaction and (b)
// time the write transaction (or a bare write statement) is held. A `txn` label
// names the stage where the call site can pass it cheaply; otherwise it is
// `"other"`. No `table` label: the metastore connection is shared across all
// tables in a dataset's catalog (the DB filename is always `cayenne.db`), so a
// table label is not cheaply available at this layer.

static CAYENNE_METASTORE_WRITER_WAIT_MS: OnceLock<Histogram<f64>> = OnceLock::new();

/// Records the time a metastore writer spent waiting to acquire the write
/// transaction (pool-slot acquire + `BEGIN IMMEDIATE`) or a bare write statement.
/// `dimensions` should carry a `txn` stage label (`stage_a_fold` / `seq_reserve`
/// / `flip` / `checkpoint` / `other`).
pub fn track_cayenne_metastore_writer_wait(duration: Duration, dimensions: &[KeyValue]) {
    CAYENNE_METASTORE_WRITER_WAIT_MS
        .get_or_init(|| {
            cayenne_operational_meter()
                .f64_histogram("cayenne_metastore_writer_wait_ms")
                .with_description(
                    "Time a Cayenne metastore writer spent waiting to acquire the write transaction (pool-slot acquire + BEGIN IMMEDIATE) or a bare write statement.",
                )
                .with_unit("ms")
                .with_boundaries(CONTENTION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
}

static CAYENNE_METASTORE_WRITER_HELD_MS: OnceLock<Histogram<f64>> = OnceLock::new();

/// Records how long a metastore write transaction (or a bare write statement) was
/// held, from acquisition through commit/rollback (or statement completion).
/// `dimensions` should carry a `txn` stage label (see
/// [`track_cayenne_metastore_writer_wait`]).
pub fn track_cayenne_metastore_writer_held(duration: Duration, dimensions: &[KeyValue]) {
    CAYENNE_METASTORE_WRITER_HELD_MS
        .get_or_init(|| {
            cayenne_operational_meter()
                .f64_histogram("cayenne_metastore_writer_held_ms")
                .with_description(
                    "Time a Cayenne metastore write transaction (or bare write statement) was held, from acquisition through commit/rollback.",
                )
                .with_unit("ms")
                .with_boundaries(CONTENTION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
}

// METRIC 2 — metastore WAL telemetry. The WAL gauge is sampled (cheap `stat()`)
// on each background maintenance checkpoint tick; the checkpoint histogram times
// the checkpoint itself with a `mode` label: `passive_background` (the default
// off-hot-path PASSIVE drain) or `truncate_background` (the size-triggered
// TRUNCATE escalation when the WAL exceeds its cap). With the inline
// auto-checkpoint disabled (cycle-8 TASK A2) these background modes are the sole
// WAL drain; an `inline_backstop` mode would only appear if a deployment
// re-enabled the inline auto-checkpoint via `wal_autocheckpoint_pages > 0`.

static CAYENNE_METASTORE_WAL_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

/// Records the current size in bytes of the metastore `-wal` file, sampled on the
/// background maintenance checkpoint tick (and after the inline backstop). A WAL
/// that keeps growing means the passive checkpoint cannot keep pace with the CDC
/// commit rate. `dimensions` may carry `table` (the maintenance tick that sampled
/// it) — the WAL file itself is shared across the catalog's tables.
pub fn track_cayenne_metastore_wal_bytes(bytes: u64, dimensions: &[KeyValue]) {
    CAYENNE_METASTORE_WAL_BYTES
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_gauge("cayenne_metastore_wal_bytes")
                .with_description(
                    "Current size in bytes of the Cayenne metastore SQLite -wal file, sampled at checkpoint time.",
                )
                .with_unit("By")
                .build()
        })
        .record(bytes, dimensions);
}

static CAYENNE_METASTORE_CHECKPOINT_MS: OnceLock<Histogram<f64>> = OnceLock::new();

/// Records the wall-clock duration of a metastore WAL checkpoint. `dimensions`
/// should carry a `mode` label: `passive_background` (the off-hot-path
/// maintenance-tick PASSIVE checkpoint, the common case), `truncate_background`
/// (the same tick escalated to TRUNCATE once the WAL exceeds its size cap), or
/// `inline_backstop` (only if a deployment re-enabled the inline auto-checkpoint
/// via `wal_autocheckpoint_pages > 0`).
pub fn track_cayenne_metastore_checkpoint(duration: Duration, dimensions: &[KeyValue]) {
    CAYENNE_METASTORE_CHECKPOINT_MS
        .get_or_init(|| {
            cayenne_operational_meter()
                .f64_histogram("cayenne_metastore_checkpoint_ms")
                .with_description("Wall-clock time of a Cayenne metastore WAL checkpoint.")
                .with_unit("ms")
                .with_boundaries(CONTENTION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
}

// METRIC 3 — inline admission flips. One increment each time a CDC batch that
// could have updated the inline memtable instead fell back to a Vortex staged
// write, labeled by `table` and the `reason` it could not inline:
// `rows_cap` / `bytes_cap` (the inline buffer overflowed its row or byte cap) or
// `blocking_config` (the table's shape — partition column or retention delete
// filters — bars inlining outright).

static CAYENNE_INLINE_FALLBACKS: OnceLock<Counter<u64>> = OnceLock::new();

/// Counts inline-admission fallbacks: a CDC batch that could not update the inline
/// memtable and fell back to a staged Vortex write. `dimensions` should carry
/// `table` and `reason` (`rows_cap` | `bytes_cap` | `blocking_config`).
pub fn track_cayenne_inline_fallback(dimensions: &[KeyValue]) {
    CAYENNE_INLINE_FALLBACKS
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_counter("cayenne_inline_fallback_total")
                .with_description(
                    "CDC batches that fell back from the inline memtable to a staged Vortex write, by reason (rows_cap | bytes_cap | blocking_config).",
                )
                .build()
        })
        .add(1, dimensions);
}

// METRIC 4 — CDC burst shape. Rows and Arrow in-memory bytes of each prepared CDC
// batch at the Cayenne staged/inlined write entry, per `table`. Pairs with the
// runtime-side coalesced-burst histograms to attribute size to a specific table.

static CAYENNE_CDC_BURST_ROWS: OnceLock<Histogram<u64>> = OnceLock::new();

/// Records the row count of a prepared CDC batch at the Cayenne write entry.
/// `dimensions` should carry `table`.
pub fn track_cayenne_cdc_burst_rows(rows: u64, dimensions: &[KeyValue]) {
    CAYENNE_CDC_BURST_ROWS
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_histogram("cayenne_cdc_burst_rows")
                .with_description(
                    "Row count of a prepared CDC batch at the Cayenne staged/inlined write entry. On the inline-overflow fallback path this is the BUFFERED row count — a lower bound, since the unbuffered stream remainder is not counted.",
                )
                .with_boundaries(ROWS_RETURNED_HISTOGRAM_BUCKETS.to_vec())
                .with_unit("rows")
                .build()
        })
        .record(rows, dimensions);
}

static CAYENNE_CDC_BURST_BYTES: OnceLock<Histogram<u64>> = OnceLock::new();

/// Records the Arrow in-memory byte size of a prepared CDC batch at the Cayenne
/// write entry. On the inline-overflow fallback path the value is the buffered
/// lower bound (the unbuffered stream remainder is not counted). `dimensions`
/// should carry `table`.
pub fn track_cayenne_cdc_burst_bytes(bytes: u64, dimensions: &[KeyValue]) {
    CAYENNE_CDC_BURST_BYTES
        .get_or_init(|| {
            cayenne_operational_meter()
                .u64_histogram("cayenne_cdc_burst_bytes")
                .with_description("Arrow in-memory byte size of a prepared CDC batch at the Cayenne staged/inlined write entry. On the inline-overflow fallback path this is the BUFFERED byte size — a lower bound, since the unbuffered stream remainder is not counted.")
                .with_boundaries(BYTES_HISTOGRAM_BUCKETS.to_vec())
                .with_unit("By")
                .build()
        })
        .record(bytes, dimensions);
}
