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
use opentelemetry::metrics::{Counter, Histogram};
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

static PROCESS_RESIDENT_MEMORY_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

/// Records the process's resident set size — the number the kernel's OOM
/// decision is made on. Budgets and pool gauges describe intent; this describes
/// fact, and the gap between them is the off-pool/unaccounted memory.
///
/// Resident memory is operator observability, not product-usage telemetry, so
/// this binds to the OpenTelemetry **global** provider that `init_metrics`
/// installs with the operator's Prometheus `/metrics` and OTLP readers — not to
/// the anonymous-telemetry [`meter::METER`], which would never reach an
/// operator's dashboard. The meter handle is fetched fresh rather than cached,
/// for the reason `cayenne::operational_meter` documents: caching binds
/// permanently to whatever provider is global at first access, so the `OnceLock`
/// holds only the built gauge, whose construction is deferred to the first
/// record — always after `init_metrics` on this 2s sampling path.
pub fn track_process_resident_memory_bytes(bytes: u64, dimensions: &[KeyValue]) {
    PROCESS_RESIDENT_MEMORY_BYTES
        .get_or_init(|| {
            global::meter("process")
                .u64_gauge("process_resident_memory_bytes")
                .with_description(
                    "Resident set size of the spiced process. Budgets and pool gauges describe intent; this describes fact, and the gap between them is the off-pool/unaccounted memory.",
                )
                .with_unit("By")
                .build()
        })
        .record(bytes, dimensions);
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

/// Registers per-tokio-runtime observable gauges so `/metrics` shows, per runtime,
/// whether its worker threads are idle or competing for cores. Pull-based: each
/// callback reads `tokio::runtime::Handle::metrics()` at Prometheus scrape time, so
/// there is no periodic sampler task and near-zero cost between scrapes.
///
/// `handles` is a list of `(label, handle)` — e.g. `("cpu", …)`, `("refresh", …)`,
/// `("cdc_apply", …)`, `("compaction", …)`, `("main", …)`; each becomes a
/// `runtime="<label>"` attribute on every gauge. Like [`cayenne::register_compaction_metrics`],
/// the binary MUST call this once AFTER `init_metrics` has installed the Prometheus meter.
///
/// The task/worker/queue gauges use stable `RuntimeMetrics` APIs and are always emitted.
/// The per-worker busy/park/steal gauges — the direct "are these threads doing work or
/// just parked" signal — require the `tokio_unstable` cfg at build time
/// (`RUSTFLAGS="--cfg tokio_unstable"`); without it only the stable gauges register, so
/// default and CI builds are unaffected.
/// Registers the CPU-sizing gauges, so a mis-sized deployment is greppable
/// across a fleet rather than diagnosed pod-by-pod.
///
/// Three quantities, deliberately separate: `spiced_cpu_budget_*` is the value
/// the runtime *uses*, while `spiced_cpu_limit_millicores` and
/// `spiced_cpu_request_millicores` are the cgroup *inputs* it was chosen
/// against. Comparing them is what distinguishes a pod sized for its request
/// from one sized for its whole node. `source` is `CpuSource::as_str` — the rung
/// of the detection ladder the budget came from.
///
/// `limit` and `request` are `None` when the cgroup expresses no such value; the
/// gauge then reports nothing rather than `0`, which would be indistinguishable
/// from a real zero.
///
/// Like [`register_tokio_runtime_metrics`], the binary MUST call this once
/// AFTER `init_metrics` has installed the Prometheus meter.
///
/// Takes plain scalars rather than the budget itself: `telemetry` is a
/// foundation crate and does not depend on `cpu-budget`.
pub fn register_cpu_budget_metrics(
    cores: u64,
    millicores: u64,
    source: &'static str,
    limit_millicores: Option<u64>,
    request_millicores: Option<u64>,
) {
    let meter = global::meter("cpu_budget");

    let _ = meter
        .u64_observable_gauge("spiced_cpu_budget_cores")
        .with_description("CPU cores the runtime sizes itself for, and where that value came from.")
        .with_unit("{cpu}")
        .with_callback(move |obs| obs.observe(cores, &[KeyValue::new("source", source)]))
        .build();

    let _ = meter
        .u64_observable_gauge("spiced_cpu_budget_millicores")
        .with_description(
            "CPU millicores the runtime sizes itself for, and where that value came from.",
        )
        .with_unit("{millicpu}")
        .with_callback(move |obs| obs.observe(millicores, &[KeyValue::new("source", source)]))
        .build();

    if let Some(limit) = limit_millicores {
        let _ = meter
            .u64_observable_gauge("spiced_cpu_limit_millicores")
            .with_description("CPU limit from the cgroup CPU quota (Kubernetes limits.cpu).")
            .with_unit("{millicpu}")
            .with_callback(move |obs| obs.observe(limit, &[]))
            .build();
    }

    if let Some(request) = request_millicores {
        let _ = meter
            .u64_observable_gauge("spiced_cpu_request_millicores")
            .with_description(
                "CPU request inferred from the cgroup CPU share (Kubernetes requests.cpu). Reported only; never used for sizing.",
            )
            .with_unit("{millicpu}")
            .with_callback(move |obs| obs.observe(request, &[]))
            .build();
    }
}

pub fn register_tokio_runtime_metrics(handles: Vec<(&'static str, tokio::runtime::Handle)>) {
    if handles.is_empty() {
        return;
    }
    let handles: std::sync::Arc<[(&'static str, tokio::runtime::Handle)]> = handles.into();
    let meter = global::meter("tokio_runtime");

    let h = std::sync::Arc::clone(&handles);
    let _ = meter
        .u64_observable_gauge("tokio_runtime_alive_tasks")
        .with_description("Alive (spawned, not-yet-completed) tasks per tokio runtime.")
        .with_unit("{task}")
        .with_callback(move |obs| {
            for (name, handle) in h.iter() {
                obs.observe(
                    handle.metrics().num_alive_tasks() as u64,
                    &[KeyValue::new("runtime", *name)],
                );
            }
        })
        .build();

    let h = std::sync::Arc::clone(&handles);
    let _ = meter
        .u64_observable_gauge("tokio_runtime_workers")
        .with_description("Worker threads per tokio runtime.")
        .with_unit("{thread}")
        .with_callback(move |obs| {
            for (name, handle) in h.iter() {
                obs.observe(
                    handle.metrics().num_workers() as u64,
                    &[KeyValue::new("runtime", *name)],
                );
            }
        })
        .build();

    let h = std::sync::Arc::clone(&handles);
    let _ = meter
        .u64_observable_gauge("tokio_runtime_global_queue_depth")
        .with_description(
            "Tasks waiting in the runtime's global (injection) queue per tokio runtime.",
        )
        .with_unit("{task}")
        .with_callback(move |obs| {
            for (name, handle) in h.iter() {
                obs.observe(
                    handle.metrics().global_queue_depth() as u64,
                    &[KeyValue::new("runtime", *name)],
                );
            }
        })
        .build();

    // Per-worker busy/park/steal — the direct "idle vs stealing cores" signal — are only
    // exposed under `--cfg tokio_unstable`. Gated so default/CI builds compile and emit
    // just the stable gauges above; build with the cfg to surface these in `/metrics`.
    #[cfg(tokio_unstable)]
    {
        let h = std::sync::Arc::clone(&handles);
        let _ = meter
            .f64_observable_gauge("tokio_runtime_worker_busy_seconds")
            .with_description(
                "Cumulative worker-busy time (summed across workers) per tokio runtime; rate()/workers = busy ratio.",
            )
            .with_unit("s")
            .with_callback(move |obs| {
                for (name, handle) in h.iter() {
                    let m = handle.metrics();
                    let busy: f64 = (0..m.num_workers())
                        .map(|w| m.worker_total_busy_duration(w).as_secs_f64())
                        .sum();
                    obs.observe(busy, &[KeyValue::new("runtime", *name)]);
                }
            })
            .build();

        let h = std::sync::Arc::clone(&handles);
        let _ = meter
            .u64_observable_gauge("tokio_runtime_worker_park_count")
            .with_description(
                "Cumulative worker park count (summed across workers) per tokio runtime.",
            )
            .with_unit("{park}")
            .with_callback(move |obs| {
                for (name, handle) in h.iter() {
                    let m = handle.metrics();
                    let parks: u64 = (0..m.num_workers()).map(|w| m.worker_park_count(w)).sum();
                    obs.observe(parks, &[KeyValue::new("runtime", *name)]);
                }
            })
            .build();

        let h = std::sync::Arc::clone(&handles);
        let _ = meter
            .u64_observable_gauge("tokio_runtime_worker_steal_count")
            .with_description(
                "Cumulative task-steal count (summed across workers) per tokio runtime.",
            )
            .with_unit("{steal}")
            .with_callback(move |obs| {
                for (name, handle) in h.iter() {
                    let m = handle.metrics();
                    let steals: u64 = (0..m.num_workers()).map(|w| m.worker_steal_count(w)).sum();
                    obs.observe(steals, &[KeyValue::new("runtime", *name)]);
                }
            })
            .build();
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
    /// Attribution for `mem_pressure`. That ratio's numerator is the best
    /// available cgroup-aware usage reading — cgroup v2 `memory.current`, else v1
    /// `memory.usage_in_bytes`, else this process's RSS — so what it includes
    /// depends on which one answered: the cgroup readings count reclaimable page
    /// cache (and are scoped to the cgroup, not the process), RSS does not.
    /// These fields exist to tell those cases apart. Negative means not sampled.
    pub mem_anon_bytes: f64,
    pub mem_working_set_bytes: f64,
    pub mem_active_file_bytes: f64,
    /// Memory PSI `some avg10`: percentage of wall clock stalled on reclaim.
    pub mem_psi_some_avg10: f64,
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
    /// Live in-memory CDC durability tier byte cap (`cdc_durability: memory`);
    /// `0` ⇒ no per-table cap (the process-global mem-tier budget still bounds RAM).
    pub mem_tier_max_bytes: u64,
    /// Measured fraction of ingested rows that are deletes (EWMA, `[0, 1]`).
    pub delete_fraction: f64,
    /// Arrival-interval coefficient of variation (burstiness); ~0 steady, `> 1` spiky.
    pub arrival_cv: f64,
    /// Measured end-to-end replication lag (seconds); `< 0` ⇒ unavailable (no
    /// upstream source timestamp seen).
    pub replication_lag_secs: f64,
    /// Replication-lag goal target (seconds); `< 0` ⇒ no goal configured.
    pub goal_replication_lag_secs: f64,
    /// Measured freshness — age of the newest applied data (seconds); `< 0` ⇒ unavailable.
    pub freshness_secs: f64,
    /// Freshness goal target (seconds); `< 0` ⇒ no goal configured.
    pub goal_freshness_secs: f64,
    /// Measured p99 query latency on this table (ms); `< 0` ⇒ no queries observed.
    pub query_latency_p99_ms: f64,
    /// Query-latency goal target (ms); `< 0` ⇒ no goal configured.
    pub goal_query_latency_ms: f64,
    /// Measured SYSTEM-WIDE query throughput (queries/hour); `< 0` ⇒ unavailable.
    /// Global, not per-table — a query spanning datasets is counted once, and the
    /// gauge is emitted without a per-table dimension.
    pub qph: f64,
    /// System-wide QPH goal target (queries/hour); `< 0` ⇒ no goal configured.
    pub goal_qph: f64,
    /// cgroup-aware CPU busy-fraction of available cores; `< 0` ⇒ unavailable
    /// (non-Linux or not yet sampled).
    pub cpu_pressure: f64,
    /// Per-batch object-store/disk write latency (EWMA, ms); `< 0` ⇒ no Vortex spill
    /// observed (pure-inline table).
    pub io_latency_ms: f64,
    /// Per-batch metastore publish-wall latency (EWMA, ms); `< 0` ⇒ no metastore
    /// publish observed (e.g. the writer-free pipelined path).
    pub publish_latency_ms: f64,
    /// Detected data-acceleration storage tier (`StorageClass::metric_code`: 0 local
    /// SSD, 1 EBS, 2 tmpfs, 3 unknown/object-store).
    pub data_storage_class: u64,
    /// Detected metastore storage tier (same code mapping as `data_storage_class`).
    pub metastore_storage_class: u64,
    /// Calibration-probe measured data-volume write throughput (MiB/s); `< 0` ⇒
    /// unprobed (remote / object-store / probe failed). Drives the continuous
    /// slow-tier bias.
    pub data_storage_write_mbps: f64,
    /// Calibration-probe measured metastore-volume write throughput (MiB/s); `< 0`
    /// ⇒ unprobed.
    pub metastore_storage_write_mbps: f64,
    /// `1` when the goal-driven controller has declared the SLO infeasible on this
    /// hardware (no further adjustment possible — actuator bounds or resource gating —
    /// and the goal still violated); `0` otherwise. Reflects current state — self-clears
    /// if the SLO becomes reachable again — so an operator can alert on a sustained `1`.
    pub goal_slo_infeasible: u64,
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

pub mod cayenne {
    use std::sync::OnceLock;
    use std::time::Duration;

    use opentelemetry::KeyValue;
    use opentelemetry::global;
    use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};

    use super::{
        BYTES_HISTOGRAM_BUCKETS, CONTENTION_MS_HISTOGRAM_BUCKETS, CayenneAutotuneState,
        DURATION_MS_HISTOGRAM_BUCKETS, ROWS_RETURNED_HISTOGRAM_BUCKETS,
    };

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
    fn operational_meter() -> Meter {
        global::meter("cayenne")
    }

    static SCAN_LISTING_TABLE_CACHE_ENTRIES: OnceLock<Gauge<u64>> = OnceLock::new();

    pub fn track_scan_listing_table_cache_entries(entries: u64, dimensions: &[KeyValue]) {
        SCAN_LISTING_TABLE_CACHE_ENTRIES
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_scan_listing_table_cache_entries")
                    .with_description("Number of entries in the Cayenne scan ListingTable cache.")
                    .with_unit("entries")
                    .build()
            })
            .record(entries, dimensions);
    }

    static LISTING_FENCE_WAIT_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

    pub fn track_listing_fence_wait_duration(duration: Duration, dimensions: &[KeyValue]) {
        LISTING_FENCE_WAIT_DURATION_MS
            .get_or_init(|| {
                operational_meter()
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

    static LISTING_SCAN_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

    pub fn track_listing_scan_duration(duration: Duration, dimensions: &[KeyValue]) {
        LISTING_SCAN_DURATION_MS
        .get_or_init(|| {
            operational_meter()
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

    static WRITE_PHASE_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

    pub fn track_write_phase_duration(duration: Duration, dimensions: &[KeyValue]) {
        WRITE_PHASE_DURATION_MS
            .get_or_init(|| {
                operational_meter()
                    .f64_histogram("cayenne_write_phase_duration_ms")
                    .with_description("Time spent in Cayenne write-path phases.")
                    .with_unit("ms")
                    .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
                    .build()
            })
            .record(duration.as_secs_f64() * 1000.0, dimensions);
    }

    static CDC_ABSORBED_DELETE_KEYS: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts CDC Delete-event keys absorbed by the in-memory CDC tier
    /// (`cdc_durability: memory`) as RAM tombstones instead of being routed onto
    /// the durable staged path. Each absorbed key defers its durability to the
    /// covering mem-tier checkpoint, exactly like in-memory upserts. `dimensions`
    /// should carry `table`.
    pub fn track_cdc_absorbed_delete_keys(keys: u64, dimensions: &[KeyValue]) {
        CDC_ABSORBED_DELETE_KEYS
        .get_or_init(|| {
            operational_meter()
                .u64_counter("cayenne_cdc_absorbed_delete_keys_total")
                .with_description(
                    "CDC Delete-event keys absorbed as in-memory CDC tier tombstones (durability deferred to the covering mem-tier checkpoint) instead of taking the durable staged path.",
                )
                .with_unit("keys")
                .build()
        })
        .add(keys, dimensions);
    }

    static MEM_TIER_CHECKPOINT_TICK: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts background mem-tier checkpoint TICK outcomes (`cdc_durability: memory`),
    /// so a stalled deferred source-slot ack is attributable to the trigger vs the
    /// checkpoint body. Outcomes (carried as the `outcome` dimension): `fired` (the
    /// tick ran a checkpoint), `skipped_empty` (whole tier empty), `skipped_gate`
    /// (size/age churn gate held it off), `no_advancer` / `not_memory_mode` (early
    /// return before the gate), `failed` (the checkpoint errored). A flat-zero `fired`
    /// under a growing WAL backlog localizes a slot-ack stall to the trigger path —
    /// the exact signal missing when the sharded (N>1) tier stopped draining.
    /// `dimensions` carries `table` + `outcome`.
    pub fn track_mem_tier_checkpoint_tick(dimensions: &[KeyValue]) {
        MEM_TIER_CHECKPOINT_TICK
        .get_or_init(|| {
            operational_meter()
                .u64_counter("cayenne_mem_tier_checkpoint_tick_total")
                .with_description(
                    "Background mem-tier checkpoint tick outcomes (fired / skipped_empty / skipped_gate / no_advancer / not_memory_mode / failed), labeled by table and outcome.",
                )
                .with_unit("ticks")
                .build()
        })
        .add(1, dimensions);
    }

    static MEM_TIER_APPLY_EPOCH: OnceLock<Gauge<u64>> = OnceLock::new();
    static MEM_TIER_DURABLE_EPOCH: OnceLock<Gauge<u64>> = OnceLock::new();

    /// The per-apply slot-ack epoch axis (`cdc_durability: memory`): `apply_epoch` is the
    /// latest allocated per-apply epoch counter; `durable_epoch` is the highest epoch the
    /// most recent mem-tier checkpoint reported durable (what `fire_slot_advancer` handed
    /// the runtime to advance the source slot). The GAP (`apply_epoch − durable_epoch`) is
    /// the un-acked source-slot backlog measured in apply epochs: a small/steady gap means
    /// the slot keeps pace; a gap that GROWS while checkpoints keep firing means the
    /// watermark is stuck — the exact signature of the N>1 WAL-drain stall, and the signal
    /// that pins it to the watermark computation vs the trigger. `dimensions` carries
    /// `table`. Emitted on each `fire_slot_advancer` (i.e. each completed checkpoint).
    pub fn track_mem_tier_epoch(apply_epoch: u64, durable_epoch: u64, dimensions: &[KeyValue]) {
        MEM_TIER_APPLY_EPOCH
            .get_or_init(|| {
                operational_meter()
                .u64_gauge("cayenne_mem_tier_apply_epoch")
                .with_description(
                    "Latest allocated per-apply slot-ack epoch counter (cdc_durability: memory).",
                )
                .build()
            })
            .record(apply_epoch, dimensions);
        MEM_TIER_DURABLE_EPOCH
        .get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_mem_tier_durable_epoch")
                .with_description(
                    "Highest per-apply epoch the last mem-tier checkpoint reported durable (handed to fire_slot_advancer to advance the source slot).",
                )
                .build()
        })
        .record(durable_epoch, dimensions);
    }

    static MEM_TIER_RESERVE_REFUSED: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts in-memory CDC tier reservation refusals (`cdc_durability: memory`): a
    /// `try_reserve_bytes` that could not fit an append into the process-global
    /// mem-tier byte budget, forcing the writer to wait for another table's
    /// checkpoint, spill its own tier, or fall back to the durable path. A non-zero,
    /// growing count is direct memory-mode ingest backpressure — the mem-tier budget
    /// is the bottleneck (pair with `cayenne_mem_tier_budget_used/total_bytes`
    /// occupancy). No labels: the budget is process-global.
    pub fn track_mem_tier_reserve_refused() {
        MEM_TIER_RESERVE_REFUSED
        .get_or_init(|| {
            operational_meter()
                .u64_counter("cayenne_mem_tier_reserve_refused_total")
                .with_description(
                    "In-memory CDC tier reservation refusals (budget full → wait / spill / durable fallback).",
                )
                .build()
        })
        .add(1, &[]);
    }

    static MEM_TIER_ACQUIRE_WAIT_MS: OnceLock<Histogram<f64>> = OnceLock::new();

    /// Records how long a write blocked waiting for in-memory CDC tier budget
    /// (`cdc_durability: memory`) before another table's checkpoint released bytes —
    /// i.e. `reserve_bytes_or_wait` on the process-global `MemTierBudget`. This makes
    /// the mem-tier budget observable as a *valve* (like the encode budget's
    /// `cayenne_encode_acquire_wait_ms`): a high wait means the global tier budget,
    /// not the encode path, is gating ingest. Uses the fine contention buckets (the
    /// wait is bounded by `BUDGET_WAIT`). `dimensions` should carry `table`.
    pub fn track_mem_tier_acquire_wait(duration: Duration, dimensions: &[KeyValue]) {
        MEM_TIER_ACQUIRE_WAIT_MS
        .get_or_init(|| {
            operational_meter()
                .f64_histogram("cayenne_mem_tier_acquire_wait_ms")
                .with_description(
                    "Time a write blocked waiting for in-memory CDC tier budget to free (reserve_bytes_or_wait), labeled by table.",
                )
                .with_unit("ms")
                .with_boundaries(CONTENTION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
    }

    static COMPACTION_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

    /// Build-once accessor for the compaction-duration histogram. The first call
    /// installs the instrument against whatever global meter is current, so the
    /// binary forces this (via [`register_compaction_metrics`]) only after
    /// the Prometheus meter provider is installed — otherwise the instrument would
    /// bind permanently to the early noop meter and never reach `/metrics`.
    fn compaction_duration_ms() -> &'static Histogram<f64> {
        COMPACTION_DURATION_MS.get_or_init(|| {
        operational_meter()
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
    pub fn track_compaction_duration(duration: Duration, dimensions: &[KeyValue]) {
        compaction_duration_ms().record(duration.as_secs_f64() * 1000.0, dimensions);
    }

    static COMPACTION_MEMORY_POOL_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Build-once accessor for the carved compaction-pool-size gauge.
    fn compaction_memory_pool_bytes() -> &'static Gauge<u64> {
        COMPACTION_MEMORY_POOL_BYTES.get_or_init(|| {
            operational_meter()
            .u64_gauge("cayenne_compaction_memory_pool_bytes")
            .with_description(
                "Size of the dedicated compaction memory pool carved from the query memory limit.",
            )
            .with_unit("By")
            .build()
        })
    }

    /// Records the size in bytes of the dedicated compaction memory pool carved from
    /// the query memory limit. Published once via [`register_compaction_metrics`].
    pub fn track_compaction_memory_pool_bytes(bytes: u64, dimensions: &[KeyValue]) {
        compaction_memory_pool_bytes().record(bytes, dimensions);
    }

    static QUERY_MEMORY_POOL_USED_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Build-once accessor for the live query-pool usage gauge.
    fn query_memory_pool_used_bytes() -> &'static Gauge<u64> {
        QUERY_MEMORY_POOL_USED_BYTES.get_or_init(|| {
            operational_meter()
                .u64_gauge("query_memory_pool_used_bytes")
                .with_description(
                    "Live bytes reserved in the query memory pool, excluding the in-memory CDC tier's mirror account.",
                )
                .with_unit("By")
                .build()
        })
    }

    /// Records live query-pool usage. Sampled by the mem-tier repartition loop,
    /// which already reads the pool on an interval; without this gauge the value
    /// is computed every two seconds and visible nowhere.
    pub fn track_query_memory_pool_used_bytes(bytes: u64, dimensions: &[KeyValue]) {
        query_memory_pool_used_bytes().record(bytes, dimensions);
    }

    static COMPACTION_MEMORY_POOL_USED_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Build-once accessor for the live compaction-pool usage gauge.
    fn compaction_memory_pool_used_bytes() -> &'static Gauge<u64> {
        COMPACTION_MEMORY_POOL_USED_BYTES.get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_compaction_memory_pool_used_bytes")
                .with_description("Live bytes reserved in the dedicated compaction memory pool.")
                .with_unit("By")
                .build()
        })
    }

    /// Records live compaction-pool usage, sampled alongside the query pool.
    pub fn track_compaction_memory_pool_used_bytes(bytes: u64, dimensions: &[KeyValue]) {
        compaction_memory_pool_used_bytes().record(bytes, dimensions);
    }

    static COMPACTION_MEMORY_EXHAUSTED: OnceLock<Counter<u64>> = OnceLock::new();

    /// Build-once accessor for the compaction-pool-exhaustion counter.
    fn compaction_memory_exhausted() -> &'static Counter<u64> {
        COMPACTION_MEMORY_EXHAUSTED.get_or_init(|| {
        operational_meter()
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
    pub fn track_compaction_memory_exhausted(dimensions: &[KeyValue]) {
        compaction_memory_exhausted().add(1, dimensions);
    }

    /// Register the Cayenne compaction instruments against the global meter so they
    /// appear in Prometheus `/metrics` from startup — and so the one-shot pool-size
    /// gauge binds to the real Prometheus meter rather than the early noop one.
    ///
    /// The binary MUST call this once, AFTER `init_metrics` has installed the
    /// Prometheus meter provider (the compaction runtime is set up earlier, before
    /// metrics init, so emitting these at carve time would bind them to the noop
    /// meter permanently). `compaction_pool_bytes` is the carved pool size to publish.
    pub fn register_compaction_metrics(compaction_pool_bytes: u64) {
        // Force the histogram + counter to build now (Prometheus-backed); they show
        // up at zero until the first compaction pass updates them.
        let _ = compaction_duration_ms();
        let _ = compaction_memory_exhausted();
        // Publish the carved pool size against the real meter.
        compaction_memory_pool_bytes().record(compaction_pool_bytes, &[]);
    }

    static ENCODE_ACQUIRE_WAIT_MS: OnceLock<Histogram<f64>> = OnceLock::new();

    /// Records how long a Cayenne write blocked acquiring encode-concurrency permits
    /// from the process-global budget (`cayenne::write_budget`). This is the direct
    /// CDC apply-path backpressure signal for the shared encode semaphore — the
    /// documented cause of the multi-second checkpoint stalls when a fleet of tables
    /// oversubscribes the encode budget: near-zero under headroom, seconds when
    /// saturated. `dimensions` should carry `class` (`delta` | `maintenance`). Uses
    /// the fine contention buckets — most acquisitions are sub-millisecond, but a
    /// starved one can stall into the multi-second tail.
    pub fn track_encode_acquire_wait(duration: Duration, dimensions: &[KeyValue]) {
        ENCODE_ACQUIRE_WAIT_MS
        .get_or_init(|| {
            operational_meter()
                .f64_histogram("cayenne_encode_acquire_wait_ms")
                .with_description(
                    "Time a Cayenne write blocked acquiring encode-concurrency permits from the process-global budget (labeled by write class).",
                )
                .with_unit("ms")
                .with_boundaries(CONTENTION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
    }

    static COMPACTION_ACQUIRE_WAIT_MS: OnceLock<Histogram<f64>> = OnceLock::new();

    /// Records how long a Cayenne background compaction pass blocked acquiring a
    /// permit from the fleet-wide compaction semaphore before it could run. A high
    /// wait means compaction is starved for its own concurrency slot (peer tables
    /// saturate the pool), letting the protected set and read-amp run away — which in
    /// turn slows the CDC write path. `dimensions` should carry `table`.
    pub fn track_compaction_acquire_wait(duration: Duration, dimensions: &[KeyValue]) {
        COMPACTION_ACQUIRE_WAIT_MS
        .get_or_init(|| {
            operational_meter()
                .f64_histogram("cayenne_compaction_acquire_wait_ms")
                .with_description(
                    "Time a Cayenne background compaction pass blocked acquiring a permit from the fleet-wide compaction semaphore (labeled by table).",
                )
                .with_unit("ms")
                .with_boundaries(CONTENTION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
    }
    static INLINE_TOMBSTONE_WRITES: OnceLock<Counter<u64>> = OnceLock::new();
    static INLINE_TOMBSTONE_KEYS: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts inline-tombstone writes on the on-conflict upsert path: one increment
    /// per `add_inlined_tombstone` that actually writes a tombstone (the cheap,
    /// O(deleted keys) path that hides the prior inline copy of an upserted PK),
    /// plus a second counter for the number of keys hidden. Pair with
    /// [`track_inline_rewrite_fallback`] — the ratio of tombstone writes to
    /// rewrite fallbacks shows how often the CDC stream takes the cheap path versus
    /// the O(corpus) inline rewrite. `dimensions` should carry `table`.
    pub fn track_inline_tombstone_write(keys: u64, dimensions: &[KeyValue]) {
        INLINE_TOMBSTONE_WRITES
        .get_or_init(|| {
            operational_meter()
                .u64_counter("cayenne_inline_tombstone_writes_total")
                .with_description(
                    "On-conflict upserts that wrote an inline tombstone (the cheap O(deleted keys) path that hides the prior inline copy of an upserted PK).",
                )
                .build()
        })
        .add(1, dimensions);
        INLINE_TOMBSTONE_KEYS
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_inline_tombstone_keys_total")
                    .with_description(
                        "Total PK keys hidden by inline tombstones on the on-conflict upsert path.",
                    )
                    .with_unit("keys")
                    .build()
            })
            .add(keys, dimensions);
    }

    static INLINE_REWRITE_FALLBACKS: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts the O(corpus) inline-data rewrite fallback
    /// (`build_inlined_data_rewrite_for_pk_keys`): one increment per call that
    /// actually removed inline rows (i.e. re-decoded and rewrote the inline corpus
    /// to drop the superseded copies). This still fires on the inline-insert path
    /// (`write_cdc_pipelined` inline fallback). A non-zero rate alongside
    /// [`track_inline_tombstone_write`] makes the tombstone-vs-rewrite ratio
    /// observable. `dimensions` should carry `table`.
    pub fn track_inline_rewrite_fallback(dimensions: &[KeyValue]) {
        INLINE_REWRITE_FALLBACKS
        .get_or_init(|| {
            operational_meter()
                .u64_counter("cayenne_inline_rewrite_fallbacks_total")
                .with_description(
                    "Calls to the O(corpus) inline-data rewrite fallback that removed superseded inline rows (vs the cheap inline-tombstone path).",
                )
                .build()
        })
        .add(1, dimensions);
    }

    static SCAN_FILES_LISTED: OnceLock<Counter<u64>> = OnceLock::new();
    static SCAN_FILES_PRUNED: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts Vortex data files considered ("listed") and skipped ("pruned") at
    /// scan listing time via the #11234 footer min/max statistics. The pruned/listed
    /// ratio is the read-amplification signal: sorted compaction tightens per-file
    /// ranges so listing-time pruning skips more files for an aligned filter.
    /// `dimensions` should carry `table`.
    pub fn track_scan_files(listed: u64, pruned: u64, dimensions: &[KeyValue]) {
        if listed > 0 {
            SCAN_FILES_LISTED
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_scan_files_listed_total")
                    .with_description(
                        "Vortex data files considered at scan listing time (before footer-statistics pruning).",
                    )
                    .build()
            })
            .add(listed, dimensions);
        }
        if pruned > 0 {
            SCAN_FILES_PRUNED
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_scan_files_pruned_total")
                    .with_description(
                        "Vortex data files skipped at scan listing time by footer min/max statistics.",
                    )
                    .build()
            })
            .add(pruned, dimensions);
        }
    }

    static INLINE_CACHE_DELTA_POPULATES: OnceLock<Counter<u64>> = OnceLock::new();
    static INLINE_CACHE_FULL_REBUILDS: OnceLock<Counter<u64>> = OnceLock::new();

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
    pub fn track_inline_cache_populate(delta: bool, dimensions: &[KeyValue]) {
        if delta {
            INLINE_CACHE_DELTA_POPULATES
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_inline_cache_delta_populates_total")
                    .with_description(
                        "Inline-memtable cache misses satisfied by the append-only delta path (only newly committed rows fetched + decoded), avoiding the O(corpus) re-read.",
                    )
                    .build()
            })
            .add(1, dimensions);
        } else {
            INLINE_CACHE_FULL_REBUILDS
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_inline_cache_full_rebuilds_total")
                    .with_description(
                        "Inline-memtable cache misses that required a full corpus re-read + re-decode (sentinel/first touch or a structural change: rewrite, tombstone, checkpoint, overwrite, recovery).",
                    )
                    .build()
            })
            .add(1, dimensions);
        }
    }

    static LIST_FILES_CACHE_DELTA_APPLIES: OnceLock<Counter<u64>> = OnceLock::new();
    static LIST_FILES_CACHE_EVICTIONS: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts how a current-snapshot publish updated `DataFusion`'s list-files cache:
    /// a delta-apply (the moved files were merged onto the cached directory listing,
    /// avoiding a full re-LIST) or an eviction (the whole directory entry was
    /// dropped, forcing the next scan to re-LIST — the fallback for compaction,
    /// retention, a cold cache, or a standalone publish). Under sustained append CDC
    /// the delta-apply counter should dominate; a high eviction rate means most
    /// publishes lack recorded additions or the listing keeps getting evicted out
    /// from under the writer. `dimensions` should carry `table`.
    pub fn track_list_files_cache_publish(delta: bool, dimensions: &[KeyValue]) {
        if delta {
            LIST_FILES_CACHE_DELTA_APPLIES
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_list_files_cache_delta_applies_total")
                    .with_description(
                        "Current-snapshot publishes that merged the moved files onto the cached directory listing (avoiding a full re-LIST).",
                    )
                    .build()
            })
            .add(1, dimensions);
        } else {
            LIST_FILES_CACHE_EVICTIONS
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_list_files_cache_evictions_total")
                    .with_description(
                        "Current-snapshot publishes that evicted the whole directory listing (forcing the next scan to re-LIST): compaction, retention, cold cache, or standalone publish.",
                    )
                    .build()
            })
            .add(1, dimensions);
        }
    }
    static AT_ROWS_PER_SEC: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_BYTES_PER_SEC: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_APPLY_VS_ARRIVAL: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_READ_AMP: OnceLock<Gauge<u64>> = OnceLock::new();
    static AT_MEM_PRESSURE: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_MEM_ANON_BYTES: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_MEM_WORKING_SET_BYTES: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_MEM_ACTIVE_FILE_BYTES: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_MEM_PSI_SOME_AVG10: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_APPLY_MS: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_INLINE_FLUSH_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();
    static AT_COMPACTION_INTERVAL_MS: OnceLock<Gauge<u64>> = OnceLock::new();
    static AT_COMPACTION_TRIGGER_FILES: OnceLock<Gauge<u64>> = OnceLock::new();
    static AT_TARGET_FILE_SIZE_MB: OnceLock<Gauge<u64>> = OnceLock::new();
    static AT_WRITE_CONCURRENCY: OnceLock<Gauge<u64>> = OnceLock::new();
    static AT_MEM_TIER_MAX_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();
    static AT_DELETE_FRACTION: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_ARRIVAL_CV: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_REPLICATION_LAG_SECS: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_GOAL_REPLICATION_LAG_SECS: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_FRESHNESS_SECS: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_GOAL_FRESHNESS_SECS: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_QUERY_LATENCY_P99_MS: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_GOAL_QUERY_LATENCY_MS: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_QPH: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_GOAL_QPH: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_CPU_PRESSURE: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_IO_LATENCY_MS: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_PUBLISH_LATENCY_MS: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_DATA_STORAGE_WRITE_MIBPS: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_METASTORE_STORAGE_WRITE_MIBPS: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_GOAL_SLO_INFEASIBLE: OnceLock<Gauge<u64>> = OnceLock::new();
    static AT_DATA_STORAGE_CLASS: OnceLock<Gauge<u64>> = OnceLock::new();
    static AT_METASTORE_STORAGE_CLASS: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Emit the auto-tuner state gauges for one table. `dimensions` should carry
    /// `table`. Called on each background tick.
    pub fn track_autotune_state(state: &CayenneAutotuneState, dimensions: &[KeyValue]) {
        AT_ROWS_PER_SEC
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_ingest_rows_per_sec")
                    .with_description("Measured CDC ingest rate (rows/sec, EWMA).")
                    .build()
            })
            .record(state.rows_per_sec, dimensions);
        // Suppress the bytes/sec gauge when byte accounting is unavailable
        // (`bytes_per_sec < 0`) rather than reporting a misleading 0 under load.
        if state.bytes_per_sec >= 0.0 {
            AT_BYTES_PER_SEC
                .get_or_init(|| {
                    operational_meter()
                        .f64_gauge("cayenne_ingest_bytes_per_sec")
                        .with_description("Measured CDC ingest rate (bytes/sec, EWMA).")
                        .with_unit("By/s")
                        .build()
                })
                .record(state.bytes_per_sec, dimensions);
        }
        AT_APPLY_VS_ARRIVAL
            .get_or_init(|| {
                operational_meter()
                .f64_gauge("cayenne_ingest_apply_vs_arrival")
                .with_description(
                    "Apply latency / offered-load interval; > 1 means ingest is falling behind.",
                )
                .build()
            })
            .record(state.apply_vs_arrival, dimensions);
        AT_READ_AMP
        .get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_ingest_read_amp")
                .with_description(
                    "Read amplification (protected-snapshot runs a scan must merge); high means ingest is slowing queries.",
                )
                .build()
        })
        .record(state.read_amp, dimensions);
        AT_MEM_PRESSURE
        .get_or_init(|| {
            operational_meter()
                .f64_gauge("cayenne_ingest_mem_pressure")
                .with_description(
                    "cgroup-aware memory usage as a fraction of the budget; negative means unknown.",
                )
                .build()
        })
        .record(state.mem_pressure, dimensions);
        AT_MEM_ANON_BYTES
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_ingest_mem_anon_bytes")
                    .with_description(
                        "Anonymous (unreclaimable) bytes at the last pressure sample; negative means unknown.",
                    )
                    .with_unit("By")
                    .build()
            })
            .record(state.mem_anon_bytes, dimensions);
        AT_MEM_WORKING_SET_BYTES
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_ingest_mem_working_set_bytes")
                    .with_description(
                        "Working set (memory.current minus inactive_file) at the last pressure sample; negative means unknown.",
                    )
                    .with_unit("By")
                    .build()
            })
            .record(state.mem_working_set_bytes, dimensions);
        AT_MEM_ACTIVE_FILE_BYTES
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_ingest_mem_active_file_bytes")
                    .with_description(
                        "Hot page cache, which a working set does NOT subtract; negative means unknown.",
                    )
                    .with_unit("By")
                    .build()
            })
            .record(state.mem_active_file_bytes, dimensions);
        AT_MEM_PSI_SOME_AVG10
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_ingest_mem_psi_some_avg10")
                    .with_description(
                        "Percentage of wall clock at least one task stalled on memory reclaim; negative means unknown.",
                    )
                    .build()
            })
            .record(state.mem_psi_some_avg10, dimensions);
        AT_INLINE_FLUSH_BYTES
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_autotune_inline_flush_max_bytes")
                    .with_description("Current (live) inline-memtable flush byte budget.")
                    .with_unit("By")
                    .build()
            })
            .record(state.inline_flush_max_bytes, dimensions);
        AT_COMPACTION_INTERVAL_MS
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_autotune_compaction_interval_ms")
                    .with_description("Current (live) background compaction interval.")
                    .with_unit("ms")
                    .build()
            })
            .record(state.compaction_interval_ms, dimensions);
        AT_APPLY_MS
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_ingest_apply_ms")
                    .with_description("Per-batch CDC apply wall time (EWMA).")
                    .with_unit("ms")
                    .build()
            })
            .record(state.apply_ms, dimensions);
        AT_COMPACTION_TRIGGER_FILES
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_autotune_compaction_trigger_files")
                    .with_description("Current (live) small-file compaction trigger (file count).")
                    .build()
            })
            .record(state.compaction_trigger_files, dimensions);
        AT_TARGET_FILE_SIZE_MB
        .get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_autotune_target_file_size_mb")
                .with_description(
                    "Configured target Vortex file size — the reference compacted files should trend toward (compare cayenne_compaction_merged_bytes).",
                )
                .with_unit("MiB")
                .build()
        })
        .record(state.target_file_size_mb, dimensions);
        AT_WRITE_CONCURRENCY
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_autotune_write_concurrency")
                    .with_description(
                        "Current (live) write/encode concurrency (0 = session default).",
                    )
                    .build()
            })
            .record(state.write_concurrency, dimensions);
        AT_MEM_TIER_MAX_BYTES
            .get_or_init(|| {
                operational_meter()
                .u64_gauge("cayenne_autotune_mem_tier_max_bytes")
                .with_description(
                    "Current (live) in-memory CDC durability tier byte cap (0 = no per-table cap).",
                )
                .with_unit("By")
                .build()
            })
            .record(state.mem_tier_max_bytes, dimensions);
        AT_DELETE_FRACTION
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_ingest_delete_fraction")
                    .with_description("Measured fraction of ingested rows that are deletes (EWMA).")
                    .build()
            })
            .record(state.delete_fraction, dimensions);
        AT_ARRIVAL_CV
            .get_or_init(|| {
                operational_meter()
                .f64_gauge("cayenne_ingest_arrival_cv")
                .with_description(
                    "Arrival-interval coefficient of variation (burstiness); ~0 steady, > 1 spiky.",
                )
                .build()
            })
            .record(state.arrival_cv, dimensions);

        // Goal signals: the measured high-level metric (always emitted when available)
        // and, when configured, its goal target. Each is suppressed (sentinel `< 0`)
        // when the metric is unavailable or the goal is unset, rather than emitting a
        // misleading 0. Comparing measured vs target shows convergence toward the SLO.
        if state.replication_lag_secs >= 0.0 {
            AT_REPLICATION_LAG_SECS
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_ingest_replication_lag_seconds")
                    .with_description(
                        "Measured end-to-end CDC replication lag (now − newest applied upstream commit timestamp).",
                    )
                    .with_unit("s")
                    .build()
            })
            .record(state.replication_lag_secs, dimensions);
        }
        if state.goal_replication_lag_secs >= 0.0 {
            AT_GOAL_REPLICATION_LAG_SECS
                .get_or_init(|| {
                    operational_meter()
                        .f64_gauge("cayenne_goal_replication_lag_seconds")
                        .with_description("Configured replication-lag goal target.")
                        .with_unit("s")
                        .build()
                })
                .record(state.goal_replication_lag_secs, dimensions);
        }
        if state.freshness_secs >= 0.0 {
            AT_FRESHNESS_SECS
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_ingest_freshness_seconds")
                    .with_description("Measured freshness — windowed-peak per-apply row freshness (worst PG-commit→queryable lag) over a ~60s window (the default goal-convergence window; fixed — it does not track per-dataset convergence-window overrides). Peak, not instantaneous: captures transient stalls and does not ramp on an idle table. Falls back to the instantaneous now−last_apply age on sources without a commit timestamp, or before the first timestamped apply.")
                    .with_unit("s")
                    .build()
            })
            .record(state.freshness_secs, dimensions);
        }
        if state.goal_freshness_secs >= 0.0 {
            AT_GOAL_FRESHNESS_SECS
                .get_or_init(|| {
                    operational_meter()
                        .f64_gauge("cayenne_goal_freshness_seconds")
                        .with_description("Configured freshness goal target.")
                        .with_unit("s")
                        .build()
                })
                .record(state.goal_freshness_secs, dimensions);
        }
        if state.query_latency_p99_ms >= 0.0 {
            AT_QUERY_LATENCY_P99_MS
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_query_latency_p99_ms")
                    .with_description("Measured p99 query latency on this table (pushed down from the query path).")
                    .with_unit("ms")
                    .build()
            })
            .record(state.query_latency_p99_ms, dimensions);
        }
        if state.goal_query_latency_ms >= 0.0 {
            AT_GOAL_QUERY_LATENCY_MS
                .get_or_init(|| {
                    operational_meter()
                        .f64_gauge("cayenne_goal_query_latency_ms")
                        .with_description("Configured query-latency (p99) goal target.")
                        .with_unit("ms")
                        .build()
                })
                .record(state.goal_query_latency_ms, dimensions);
        }
        // QPH is a system-wide metric (a query, e.g. a join, spans datasets and is
        // counted once), so these two gauges are GLOBAL — recorded with no per-table
        // dimension, they collapse to a single series even though every table's
        // controller tick reports the same value.
        if state.qph >= 0.0 {
            AT_QPH
                .get_or_init(|| {
                    operational_meter()
                        .f64_gauge("cayenne_query_throughput_qph")
                        .with_description("Measured system-wide query throughput (queries/hour).")
                        .build()
                })
                .record(state.qph, &[]);
        }
        if state.goal_qph >= 0.0 {
            AT_GOAL_QPH
                .get_or_init(|| {
                    operational_meter()
                        .f64_gauge("cayenne_goal_query_throughput_qph")
                        .with_description(
                            "Configured system-wide query-throughput (QPH) goal target.",
                        )
                        .build()
                })
                .record(state.goal_qph, &[]);
        }

        // Environment/data signals the closed loop reasons over (Part A). The three
        // pressure/latency gauges are suppressed (sentinel `< 0`) until sampled — CPU
        // is non-Linux/unsampled; I/O and publish latency stay unset until the table
        // spills to Vortex / takes the writer-bearing publish path. The storage-tier
        // codes are detected facts, always emitted (see `StorageClass::metric_code`).
        if state.cpu_pressure >= 0.0 {
            AT_CPU_PRESSURE
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_ingest_cpu_pressure")
                    .with_description(
                        "cgroup-aware CPU busy-fraction of available cores; gates CPU-stealing tuning moves.",
                    )
                    .build()
            })
            .record(state.cpu_pressure, dimensions);
        }
        if state.io_latency_ms >= 0.0 {
            AT_IO_LATENCY_MS
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_ingest_io_latency_ms")
                    .with_description(
                        "Per-batch object-store/disk write latency (EWMA); drives the I/O-bound tuning gate.",
                    )
                    .with_unit("ms")
                    .build()
            })
            .record(state.io_latency_ms, dimensions);
        }
        if state.publish_latency_ms >= 0.0 {
            AT_PUBLISH_LATENCY_MS
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_ingest_publish_latency_ms")
                    .with_description(
                        "Per-batch metastore publish-wall latency (EWMA); drives the publish-bound tuning gate.",
                    )
                    .with_unit("ms")
                    .build()
            })
            .record(state.publish_latency_ms, dimensions);
        }
        AT_DATA_STORAGE_CLASS
        .get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_data_storage_class")
                .with_description(
                    "Detected data-acceleration storage tier (0 local SSD, 1 EBS, 2 tmpfs, 3 unknown/object-store).",
                )
                .build()
        })
        .record(state.data_storage_class, dimensions);
        AT_METASTORE_STORAGE_CLASS
        .get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_metastore_storage_class")
                .with_description(
                    "Detected metastore storage tier (same code mapping as cayenne_data_storage_class).",
                )
                .build()
        })
        .record(state.metastore_storage_class, dimensions);
        if state.data_storage_write_mbps >= 0.0 {
            AT_DATA_STORAGE_WRITE_MIBPS
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_data_storage_write_mibps")
                    .with_description(
                        "Calibration-probe measured data-volume write throughput; drives the continuous slow-tier bias.",
                    )
                    .with_unit("MiB/s")
                    .build()
            })
            .record(state.data_storage_write_mbps, dimensions);
        }
        if state.metastore_storage_write_mbps >= 0.0 {
            AT_METASTORE_STORAGE_WRITE_MIBPS
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_metastore_storage_write_mibps")
                    .with_description(
                        "Calibration-probe measured metastore-volume write throughput; drives the continuous publish bias.",
                    )
                    .with_unit("MiB/s")
                    .build()
            })
            .record(state.metastore_storage_write_mbps, dimensions);
        }
        AT_GOAL_SLO_INFEASIBLE
        .get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_goal_slo_infeasible")
                .with_description(
                    "1 when the goal-driven tuner has declared the SLO infeasible on this hardware (no further adjustment possible due to bounds or gating, goal still violated); 0 otherwise.",
                )
                .build()
        })
        .record(state.goal_slo_infeasible, dimensions);
    }

    static AT_ADJUSTMENTS: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts dynamic auto-tune adjustments applied. `dimensions` should carry
    /// `table` and `actuator`. A non-zero rate means the closed loop is actively
    /// adapting the table to its observed workload.
    pub fn track_autotune_adjustment(dimensions: &[KeyValue]) {
        AT_ADJUSTMENTS
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_autotune_adjustments_total")
                    .with_description("Dynamic auto-tune adjustments applied, by actuator.")
                    .build()
            })
            .add(1, dimensions);
    }

    static COMPACTION_MERGED_BYTES: OnceLock<Histogram<u64>> = OnceLock::new();

    /// Records the bytes a protected-snapshot subset compaction merged into one
    /// output (≈ the resulting compacted file size). Compare its distribution
    /// against `cayenne_autotune_target_file_size_mb` to see whether compaction is
    /// trending to the target file size or stalling below it (a read-amplification
    /// signal the adaptive tuner cares about). `dimensions` should carry `table`
    /// and `kind` (currently always `"subset"` — the full current-snapshot rewrite
    /// path does not yet emit this metric).
    pub fn track_compaction_merged_bytes(bytes: u64, dimensions: &[KeyValue]) {
        const MIB: f64 = 1024.0 * 1024.0;
        COMPACTION_MERGED_BYTES
        .get_or_init(|| {
            operational_meter()
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

    static METASTORE_WRITER_WAIT_MS: OnceLock<Histogram<f64>> = OnceLock::new();

    /// Records the time a metastore writer spent waiting to acquire the write
    /// transaction (pool-slot acquire + `BEGIN IMMEDIATE`) or a bare write statement.
    /// `dimensions` should carry a `txn` stage label (`stage_a_fold` / `seq_reserve`
    /// / `flip` / `checkpoint` / `other`).
    pub fn track_metastore_writer_wait(duration: Duration, dimensions: &[KeyValue]) {
        METASTORE_WRITER_WAIT_MS
        .get_or_init(|| {
            operational_meter()
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

    static METASTORE_WRITER_HELD_MS: OnceLock<Histogram<f64>> = OnceLock::new();

    /// Records how long a metastore write transaction (or a bare write statement) was
    /// held, from acquisition through commit/rollback (or statement completion).
    /// `dimensions` should carry a `txn` stage label (see
    /// [`track_metastore_writer_wait`]).
    pub fn track_metastore_writer_held(duration: Duration, dimensions: &[KeyValue]) {
        METASTORE_WRITER_HELD_MS
        .get_or_init(|| {
            operational_meter()
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

    static METASTORE_WAL_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Records the current size in bytes of the metastore `-wal` file, sampled on the
    /// background maintenance checkpoint tick (and after the inline backstop). A WAL
    /// that keeps growing means the passive checkpoint cannot keep pace with the CDC
    /// commit rate. `dimensions` may carry `table` (the maintenance tick that sampled
    /// it) — the WAL file itself is shared across the catalog's tables.
    pub fn track_metastore_wal_bytes(bytes: u64, dimensions: &[KeyValue]) {
        METASTORE_WAL_BYTES
        .get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_metastore_wal_bytes")
                .with_description(
                    "Current size in bytes of the Cayenne metastore SQLite -wal file, sampled at checkpoint time.",
                )
                .with_unit("By")
                .build()
        })
        .record(bytes, dimensions);
    }

    static METASTORE_CHECKPOINT_MS: OnceLock<Histogram<f64>> = OnceLock::new();

    /// Records the wall-clock duration of a metastore WAL checkpoint. `dimensions`
    /// should carry a `mode` label: `passive_background` (the off-hot-path
    /// maintenance-tick PASSIVE checkpoint, the common case), `truncate_background`
    /// (the same tick escalated to TRUNCATE once the WAL exceeds its size cap), or
    /// `inline_backstop` (only if a deployment re-enabled the inline auto-checkpoint
    /// via `wal_autocheckpoint_pages > 0`).
    pub fn track_metastore_checkpoint(duration: Duration, dimensions: &[KeyValue]) {
        METASTORE_CHECKPOINT_MS
            .get_or_init(|| {
                operational_meter()
                    .f64_histogram("cayenne_metastore_checkpoint_ms")
                    .with_description("Wall-clock time of a Cayenne metastore WAL checkpoint.")
                    .with_unit("ms")
                    .with_boundaries(CONTENTION_MS_HISTOGRAM_BUCKETS.to_vec())
                    .build()
            })
            .record(duration.as_secs_f64() * 1000.0, dimensions);
    }

    static METASTORE_INCREMENTAL_VACUUM_MS: OnceLock<Histogram<f64>> = OnceLock::new();
    static METASTORE_INCREMENTAL_VACUUM_PAGES: OnceLock<Counter<u64>> = OnceLock::new();

    /// Records one off-hot-path incremental-vacuum pass over the metastore
    /// freelist: wall-clock duration of the pass (caller-supplied), and how many
    /// freelist pages it returned to the filesystem. Only emitted for a pass that
    /// reclaimed something, so the histogram describes real reclamation rather
    /// than being diluted by the no-op ticks of a database whose freelist is
    /// already drained.
    pub fn track_metastore_incremental_vacuum(duration: Duration, pages: u64) {
        METASTORE_INCREMENTAL_VACUUM_MS
            .get_or_init(|| {
                operational_meter()
                    .f64_histogram("cayenne_metastore_incremental_vacuum_ms")
                    .with_description(
                        "Wall-clock time of a Cayenne metastore incremental-vacuum pass.",
                    )
                    .with_unit("ms")
                    .with_boundaries(CONTENTION_MS_HISTOGRAM_BUCKETS.to_vec())
                    .build()
            })
            .record(duration.as_secs_f64() * 1000.0, &[]);
        METASTORE_INCREMENTAL_VACUUM_PAGES
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_metastore_incremental_vacuum_pages_total")
                    .with_description(
                        "Freelist pages reclaimed by Cayenne metastore incremental vacuum.",
                    )
                    .build()
            })
            .add(pages, &[]);
    }

    // METRIC 3 — inline admission flips. One increment each time a CDC batch that
    // could have updated the inline memtable instead fell back to a Vortex staged
    // write, labeled by `table` and the `reason` it could not inline:
    // `rows_cap` / `bytes_cap` (the inline buffer overflowed its row or byte cap) or
    // `blocking_config` (the table's shape — partition column or retention delete
    // filters — bars inlining outright).

    static INLINE_FALLBACKS: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts inline-admission fallbacks: a CDC batch that could not update the inline
    /// memtable and fell back to a staged Vortex write. `dimensions` should carry
    /// `table` and `reason` (`rows_cap` | `bytes_cap` | `blocking_config`).
    pub fn track_inline_fallback(dimensions: &[KeyValue]) {
        INLINE_FALLBACKS
        .get_or_init(|| {
            operational_meter()
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

    static CDC_BURST_ROWS: OnceLock<Histogram<u64>> = OnceLock::new();

    /// Records the row count of a prepared CDC batch at the Cayenne write entry.
    /// `dimensions` should carry `table`.
    pub fn track_cdc_burst_rows(rows: u64, dimensions: &[KeyValue]) {
        CDC_BURST_ROWS
        .get_or_init(|| {
            operational_meter()
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

    static CDC_BURST_BYTES: OnceLock<Histogram<u64>> = OnceLock::new();

    /// Records the Arrow in-memory byte size of a prepared CDC batch at the Cayenne
    /// write entry. On the inline-overflow fallback path the value is the buffered
    /// lower bound (the unbuffered stream remainder is not counted). `dimensions`
    /// should carry `table`.
    pub fn track_cdc_burst_bytes(bytes: u64, dimensions: &[KeyValue]) {
        CDC_BURST_BYTES
        .get_or_init(|| {
            operational_meter()
                .u64_histogram("cayenne_cdc_burst_bytes")
                .with_description("Arrow in-memory byte size of a prepared CDC batch at the Cayenne staged/inlined write entry. On the inline-overflow fallback path this is the BUFFERED byte size — a lower bound, since the unbuffered stream remainder is not counted.")
                .with_boundaries(BYTES_HISTOGRAM_BUCKETS.to_vec())
                .with_unit("By")
                .build()
        })
        .record(bytes, dimensions);
    }
}
