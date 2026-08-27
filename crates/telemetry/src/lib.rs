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
use std::sync::atomic::{AtomicBool, Ordering};
use std::{sync::OnceLock, time::Duration};

#[cfg(feature = "anonymous_telemetry")]
pub mod anonymous;
pub mod exporter;
pub mod hardware;
pub mod meter;
pub mod metrics_reader;
pub mod noop;
pub mod reader;
pub mod timing;
pub mod tracers;

// As recommended by the OpenTelemetry Semantic Conventions:
// https://opentelemetry.io/docs/specs/semconv/database/database-metrics/#metric-dbclientresponsereturned_rows
// We added following buckets: 25000.0, 50000.0, 100000.0, 250000.0, 500000.0
pub const ROWS_RETURNED_HISTOGRAM_BUCKETS: [f64; 18] = [
    1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 25000.0,
    50000.0, 100_000.0, 250_000.0, 500_000.0,
];

// Boundaries for every millisecond-scale duration histogram. The sub-100ms head resolves the band
// most requests finish in: without it a 0.1ms lookup and a 99ms one share a bucket, and the
// quantile interpolated inside it tracks the requested percentile rather than any latency.
pub const DURATION_MS_HISTOGRAM_BUCKETS: [f64; 24] = [
    0.0, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 750.0, 1000.0,
    2500.0, 5000.0, 7500.0, 10000.0, 25000.0, 50000.0, 100_000.0, 250_000.0, 500_000.0,
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

// Finer-grained millisecond buckets for sub-second contention timings (metastore writer
// wait/hold, WAL checkpoint, CDC linger). Shares its head with `DURATION_MS_HISTOGRAM_BUCKETS`
// and stops at 30s: a lock or checkpoint wait that long is already a stall.
pub const CONTENTION_MS_HISTOGRAM_BUCKETS: [f64; 17] = [
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
    10000.0, 30000.0,
];

/// Whether the process has finished choosing the `OpenTelemetry` global meter
/// provider that operator-facing instruments bind to.
static OPERATOR_METER_PROVIDER_SEALED: AtomicBool = AtomicBool::new(false);

/// Declares that the global meter provider will not be replaced again, so an
/// instrument built from it may be cached for the life of the process.
///
/// `spiced` installs a noop provider at startup and replaces it during
/// `init_metrics` with the one carrying the operator's Prometheus `/metrics`
/// and OTLP readers. Call this once the choice is final — after `init_metrics`
/// when metrics are configured, and also when they are not, so a deployment
/// that exports nothing still caches (against noop) rather than rebuilding an
/// instrument on every record.
pub fn seal_operator_meter_provider() {
    OPERATOR_METER_PROVIDER_SEALED.store(true, Ordering::Release);
}

/// Records through a lazily built, cached instrument — except before
/// [`seal_operator_meter_provider`], where the instrument is built, used and
/// dropped instead of being cached.
///
/// An instrument binds permanently to the provider that was global when it was
/// built, so caching one built during startup freezes it to the noop provider
/// and it never reaches `/metrics`. That is not hypothetical: the memory
/// sampler's first `tokio::time::interval` tick fires immediately, ahead of
/// the `init_metrics` that installs the real provider, which is what kept
/// `query_memory_pool_used_bytes` and
/// `cayenne_compaction_memory_pool_used_bytes` off `/metrics` entirely
/// (#12667).
///
/// Deferring construction to the first record is not enough on its own,
/// because "the first record" is exactly what a startup-time sampler makes
/// early. Gating the *cache* rather than the record keeps every early sample
/// exported through whatever provider is current, and costs a single
/// [`OnceLock::get`] once the seal is in place — the same steady-state path as
/// caching unconditionally.
fn record_via_cached<I>(cell: &OnceLock<I>, build: impl FnOnce() -> I, record: impl FnOnce(&I)) {
    if let Some(instrument) = cell.get() {
        record(instrument);
    } else if OPERATOR_METER_PROVIDER_SEALED.load(Ordering::Acquire) {
        record(cell.get_or_init(build));
    } else {
        record(&build());
    }
}

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
/// permanently to whatever provider is global at first access.
///
/// The built gauge goes through `record_via_cached`, which withholds the
/// cache until the provider is sealed. Deferring construction to the first
/// record is not sufficient on its own: the sampler that records this gauge
/// takes its first sample before `init_metrics` runs.
pub fn track_process_resident_memory_bytes(bytes: u64, dimensions: &[KeyValue]) {
    record_via_cached(
        &PROCESS_RESIDENT_MEMORY_BYTES,
        || {
            global::meter("process")
                .u64_gauge("process_resident_memory_bytes")
                .with_description(
                    "Resident set size of the spiced process. Budgets and pool gauges describe intent; this describes fact, and the gap between them is the off-pool/unaccounted memory.",
                )
                .with_unit("By")
                .build()
        },
        |gauge| gauge.record(bytes, dimensions),
    );
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
/// `spiced_cpu_budget_*` is the value the runtime *uses*; the rest are the
/// inputs it was chosen against. Comparing them is what distinguishes a pod
/// sized for its request from one sized for its whole node. `source` is
/// `CpuSource::as_str` — the rung of the detection ladder the budget came from,
/// and the only authority on which input actually won: an exported input may
/// have been outranked by an explicit setting or by a CPU limit.
///
/// `spiced_cpu_request_millicores` is the pod's own `requests.cpu`, exact, as
/// declared by whatever wrote the pod spec. The cgroup CPU *share* is
/// deliberately not exported: every cgroup has one whether or not a request was
/// expressed — cgroup v2 defaults `cpu.weight: 100`, which inverts to ~2536m in
/// a plain `docker run` — so a gauge for it would report a request-shaped number
/// on hosts where nothing requested anything. It stays in the startup log, where
/// it is read next to the source that was actually used.
///
/// Each optional input is `None` when no such value exists; the gauge then
/// reports nothing rather than `0`, which would be indistinguishable from a real
/// zero.
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
    declared_request_millicores: Option<u64>,
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

    if let Some(declared) = declared_request_millicores {
        let _ = meter
            .u64_observable_gauge("spiced_cpu_request_millicores")
            .with_description(
                "The pod's own CPU request (Kubernetes requests.cpu), as declared by the surface that wrote the pod spec. It drives sizing only when nothing outranks it: no CPU limit, and no explicit runtime.cpu.cores (a quantity or all). spiced_cpu_budget_cores{source} reports whether it did, as source=request_burst.",
            )
            .with_unit("{millicpu}")
            .with_callback(move |obs| obs.observe(declared, &[]))
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
    /// Per-batch apply wall time (EWMA, ms) — the latency the controller weighs
    /// against the offered-load interval.
    pub apply_ms: f64,
    /// Live inline-memtable flush byte budget.
    pub inline_flush_max_bytes: u64,
    /// Live background compaction interval (ms).
    pub compaction_interval_ms: u64,
    /// Live small-file compaction trigger (file count).
    pub compaction_trigger_files: u64,
    /// Live seq-prefix bake trigger — the deletion-index size the bake fires at.
    /// Read from the same accessor the gate reads, so an experiment that pins
    /// the value cannot report the controller's value instead of the one in
    /// force.
    pub bake_deletion_index_trigger: u64,
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
    ///
    /// An instrument whose first record can happen at *startup* needs more than
    /// that, because the deferred build then still lands on the noop provider.
    /// Those cache through `record_via_cached`, which withholds the cache until
    /// `seal_operator_meter_provider`.
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

    /// Builds the live query-pool usage gauge against the current global provider.
    fn build_query_memory_pool_used_bytes() -> Gauge<u64> {
        operational_meter()
            .u64_gauge("query_memory_pool_used_bytes")
            .with_description(
                "Live bytes reserved in the query memory pool, excluding the in-memory CDC tier's mirror account.",
            )
            .with_unit("By")
            .build()
    }

    /// Records live query-pool usage. Sampled by the mem-tier repartition loop,
    /// which already reads the pool on an interval; without this gauge the value
    /// is computed every two seconds and visible nowhere.
    ///
    /// That loop takes its first sample before `init_metrics` installs the
    /// operator's provider, so the gauge is cached through `record_via_cached`
    /// rather than a bare `get_or_init` — otherwise the cache freezes it to the
    /// noop provider and it never reaches `/metrics` (#12667).
    pub fn track_query_memory_pool_used_bytes(bytes: u64, dimensions: &[KeyValue]) {
        super::record_via_cached(
            &QUERY_MEMORY_POOL_USED_BYTES,
            build_query_memory_pool_used_bytes,
            |gauge| gauge.record(bytes, dimensions),
        );
    }

    static COMPACTION_MEMORY_POOL_USED_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Builds the live compaction-pool usage gauge against the current global provider.
    fn build_compaction_memory_pool_used_bytes() -> Gauge<u64> {
        operational_meter()
            .u64_gauge("cayenne_compaction_memory_pool_used_bytes")
            .with_description("Live bytes reserved in the dedicated compaction memory pool.")
            .with_unit("By")
            .build()
    }

    /// Records live compaction-pool usage, sampled alongside the query pool —
    /// and cached under the same seal, for the same reason.
    pub fn track_compaction_memory_pool_used_bytes(bytes: u64, dimensions: &[KeyValue]) {
        super::record_via_cached(
            &COMPACTION_MEMORY_POOL_USED_BYTES,
            build_compaction_memory_pool_used_bytes,
            |gauge| gauge.record(bytes, dimensions),
        );
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
    static AT_APPLY_MS: OnceLock<Gauge<f64>> = OnceLock::new();
    static AT_INLINE_FLUSH_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();
    static AT_COMPACTION_INTERVAL_MS: OnceLock<Gauge<u64>> = OnceLock::new();
    static AT_COMPACTION_TRIGGER_FILES: OnceLock<Gauge<u64>> = OnceLock::new();
    static AT_BAKE_DELETION_INDEX_TRIGGER: OnceLock<Gauge<u64>> = OnceLock::new();
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

        AT_BAKE_DELETION_INDEX_TRIGGER
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_autotune_bake_deletion_index_trigger")
                    .with_description(
                        "Live seq-prefix bake trigger: the deletion-index size (`cayenne_deletion_index_len`) at which the bake fires.",
                    )
                    .with_unit("keys")
                    .build()
            })
            .record(state.bake_deletion_index_trigger, dimensions);
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
    /// commit rate. `dimensions` carries `catalog` (the metastore path): the WAL file
    /// is shared across the catalog's tables, so without it every dataset's metastore
    /// would overwrite one another's sample on a single series.
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

    // METRIC 3 — inline admission flips. One increment each time a write that
    // could have updated the inline memtable instead fell back to a Vortex staged
    // write, labeled by `table` and the `reason` it could not inline:
    // `rows_cap` / `bytes_cap` (the inline buffer overflowed its row or byte cap),
    // `blocking_config` (the table's shape — partition column or retention delete
    // filters — bars inlining outright), or, on the whole-table-replace path,
    // `ipc_bytes_cap` (the serialized payload exceeded the entry cap even though the
    // in-memory buffer fit) / `admission_busy` (a sibling partition holds the
    // context's single inline-admission slot).

    static INLINE_FALLBACKS: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts inline-admission fallbacks: a write that could not use the inline
    /// memtable and fell back to a staged Vortex write. `dimensions` should carry
    /// `table` and `reason` (`rows_cap` | `bytes_cap` | `blocking_config` |
    /// `ipc_bytes_cap` | `admission_busy`).
    pub fn track_inline_fallback(dimensions: &[KeyValue]) {
        INLINE_FALLBACKS
        .get_or_init(|| {
            operational_meter()
                .u64_counter("cayenne_inline_fallback_total")
                .with_description(
                    "Writes that fell back from the inline memtable to a staged Vortex write, by reason (rows_cap | bytes_cap | blocking_config | ipc_bytes_cap | admission_busy).",
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

    // ────────────── Cayenne maintenance + footprint observability ──────────────
    //
    // Two questions these answer that nothing else could: "is each maintenance
    // operation running, and if not, why not", and "what is this dataset's disk
    // and metastore footprint made of". Both were previously answerable only from
    // debug logs and a hand-opened metastore.
    //
    // `kind` reuses the vocabulary already on `cayenne_compaction_duration_ms`
    // (`full`, `subset_current`, `subset`, `datalake`) plus `bake`, so a single
    // label joins an attempt to its duration and merged bytes. `outcome` is
    // `committed` / `no_op` / `failed`, or a `declined_*` reason — the prefix
    // makes "why is nothing being reclaimed" one PromQL selector.

    static COMPACTION_OUTCOME: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts one compaction-family attempt and how it ended. `dimensions`
    /// carries `table`, `kind` (the same `full` / `subset_current` / `subset` /
    /// `datalake` vocabulary as `cayenne_compaction_duration_ms`, plus `bake`),
    /// and `outcome`.
    ///
    /// Every early return records exactly one outcome, so `sum by (outcome)`
    /// over a `kind` is that pass's complete decision history. The distinction
    /// that only this metric can draw is `declined_*` versus `no_op`: "the pass
    /// refused to run" and "the pass ran and found nothing to do" are different
    /// diagnoses with different fixes, and a duration histogram cannot tell them
    /// apart because neither records a duration.
    pub fn track_compaction_outcome(dimensions: &[KeyValue]) {
        COMPACTION_OUTCOME
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_compaction_outcome_total")
                    .with_description(
                        "Cayenne compaction passes by kind and outcome. `outcome` is `committed`, `no_op`, `failed`, or a `declined_<reason>` naming why the pass did not run.",
                    )
                    .with_unit("passes")
                    .build()
            })
            .add(1, dimensions);
    }

    static MAINTENANCE_OUTCOME: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts one non-compaction maintenance attempt and how it ended.
    /// `dimensions` carries `table`, `op` (`orphan_dv_sweep` / `retention` /
    /// `retired_dir_sweep`), and the same `outcome` grammar as
    /// [`track_compaction_outcome`].
    ///
    /// Split from the compaction family so neither carries a label the other
    /// cannot fill: these operations have no `kind`, and a compaction has no
    /// `op` beyond its kind.
    pub fn track_maintenance_outcome(dimensions: &[KeyValue]) {
        MAINTENANCE_OUTCOME
            .get_or_init(build_maintenance_outcome)
            .add(1, dimensions);
    }

    fn build_maintenance_outcome() -> Counter<u64> {
        operational_meter()
            .u64_counter("cayenne_maintenance_outcome_total")
            .with_description(
                "Cayenne non-compaction maintenance passes (deletion-vector sweep, retention, retired-directory sweep) by operation and outcome.",
            )
            .with_unit("passes")
            .build()
    }

    /// Registers a maintenance operation's counters at zero, so they appear in
    /// the export before anything has happened. `dimensions` carries `table`
    /// and `op`; the outcome series is registered as `reclaimed`.
    ///
    /// A counter with no samples and a counter that is not implemented look
    /// identical to a consumer — "the deletion-vector sweep has never run" and
    /// "the deletion-vector sweep is not instrumented" are very different
    /// conclusions to draw from an empty query, and the first is the more
    /// alarming one. Publishing zero makes the absence of reclamation an
    /// observation instead of a gap. (Same technique as
    /// [`register_query_counter`], for the same reason.)
    pub fn register_maintenance_counters(dimensions: &[KeyValue]) {
        let mut with_outcome = Vec::with_capacity(dimensions.len() + 1);
        with_outcome.extend_from_slice(dimensions);
        with_outcome.push(KeyValue::new("outcome", "reclaimed"));
        MAINTENANCE_OUTCOME
            .get_or_init(build_maintenance_outcome)
            .add(0, &with_outcome);
        track_maintenance_reclaimed(0, 0, 0, dimensions);
    }

    static COMPACTION_TRIGGER: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts what caused a compaction pass to be attempted. `dimensions`
    /// carries `table`, `kind`, and `trigger` (`small_file_count`,
    /// `protected_snapshot_count`, `protected_snapshot_age`, `deletion_index`,
    /// `deletion_index_memory_ceiling`).
    ///
    /// Pairs with [`track_compaction_outcome`]: the outcome says whether work
    /// happened, this says which threshold asked for it — together they separate
    /// "the trigger never fired" from "it fired and the pass was declined".
    pub fn track_compaction_trigger(dimensions: &[KeyValue]) {
        COMPACTION_TRIGGER
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_compaction_trigger_total")
                    .with_description(
                        "Cayenne compaction passes attempted, by the threshold that asked for the pass.",
                    )
                    .with_unit("passes")
                    .build()
            })
            .add(1, dimensions);
    }

    static MAINTENANCE_RECLAIMED_FILES: OnceLock<Counter<u64>> = OnceLock::new();
    static MAINTENANCE_RECLAIMED_BYTES: OnceLock<Counter<u64>> = OnceLock::new();
    static MAINTENANCE_RECLAIMED_ROWS: OnceLock<Counter<u64>> = OnceLock::new();

    /// Records what one maintenance pass physically reclaimed: files unlinked,
    /// bytes those files occupied, and rows (tombstones, or deleted rows for
    /// retention) dropped. `dimensions` carries `table` and `op`.
    ///
    /// This is the counterpart to the footprint gauges: the gauges say how big
    /// the dataset is, these say how much each operation is actually giving back.
    /// A growing gauge with a flat reclaim counter is the signature of a
    /// reclamation path that is scheduled but never doing work.
    pub fn track_maintenance_reclaimed(files: u64, bytes: u64, rows: u64, dimensions: &[KeyValue]) {
        MAINTENANCE_RECLAIMED_FILES
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_maintenance_reclaimed_files_total")
                    .with_description("Files physically unlinked by Cayenne maintenance passes.")
                    .with_unit("files")
                    .build()
            })
            .add(files, dimensions);
        MAINTENANCE_RECLAIMED_BYTES
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_maintenance_reclaimed_bytes_total")
                    .with_description("Bytes reclaimed by Cayenne maintenance passes (the on-disk size of the files it unlinked).")
                    .with_unit("By")
                    .build()
            })
            .add(bytes, dimensions);
        MAINTENANCE_RECLAIMED_ROWS
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_maintenance_reclaimed_rows_total")
                    .with_description("Rows dropped by Cayenne maintenance passes — tombstones for a deletion-vector sweep, deleted rows for a retention pass.")
                    .with_unit("rows")
                    .build()
            })
            .add(rows, dimensions);
    }

    /// A table's in-memory deletion index, sampled where the seq-prefix bake
    /// reads it. This is the *input* to the bake trigger, so without it a
    /// `declined_below_trigger` outcome cannot be interpreted.
    #[derive(Debug, Clone, Copy)]
    pub struct CayenneDeletionIndexState {
        /// Live tombstones (`delete_len`) — the value compared against the bake
        /// trigger.
        pub len: u64,
        /// Re-insert records (`insert_len`). In an upsert workload most
        /// tombstones are superseded by a re-insert, so the ratio to `keys` says
        /// how much of the index is dead weight.
        pub reinserts: u64,
        /// Approximate resident bytes — the quantity the OOM backstop measures
        /// against the query memory pool.
        pub resident_bytes: u64,
    }

    static DELETION_INDEX_LEN: OnceLock<Gauge<u64>> = OnceLock::new();
    static DELETION_INDEX_REINSERTS: OnceLock<Gauge<u64>> = OnceLock::new();
    static DELETION_INDEX_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Publishes the deletion-index gauges for one table. `dimensions` carries
    /// `table`.
    pub fn track_deletion_index(state: CayenneDeletionIndexState, dimensions: &[KeyValue]) {
        DELETION_INDEX_LEN
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_deletion_index_len")
                    .with_description(
                        "Live tombstones in a Cayenne table's in-memory deletion index — the input to the seq-prefix bake trigger.",
                    )
                    .with_unit("keys")
                    .build()
            })
            .record(state.len, dimensions);
        DELETION_INDEX_REINSERTS
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_deletion_index_reinserts")
                    .with_description(
                        "Re-insert records in a Cayenne table's in-memory deletion index; the fraction of `cayenne_deletion_index_len` they cover is the superseded (dead) share.",
                    )
                    .with_unit("keys")
                    .build()
            })
            .record(state.reinserts, dimensions);
        DELETION_INDEX_BYTES
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_deletion_index_bytes")
                    .with_description(
                        "Approximate resident bytes of a Cayenne table's in-memory deletion index — the quantity the bake's OOM backstop measures against the query memory pool.",
                    )
                    .with_unit("By")
                    .build()
            })
            .record(state.resident_bytes, dimensions);
    }

    /// One Cayenne table's on-disk and metastore footprint, sampled on the
    /// background maintenance tick.
    ///
    /// Counts and bytes come from the authoritative `cayenne_snapshot_file`
    /// manifest and the `cayenne_delete_file` / `cayenne_cold_tier_file` /
    /// inlined tables, not from a directory walk — a single aggregate query per
    /// tick rather than a LIST per snapshot.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct CayenneTableStorage {
        /// Data files in the current snapshot.
        pub current_files: u64,
        /// On-disk bytes of the current snapshot's data files.
        pub current_bytes: u64,
        /// Rows in the current snapshot's data files (before deletions apply).
        pub current_rows: u64,
        /// Data files across the protected snapshots (the merge-on-read runs).
        pub protected_files: u64,
        /// On-disk bytes of the protected snapshots' data files.
        pub protected_bytes: u64,
        /// Rows in the protected snapshots' data files (before deletions apply).
        pub protected_rows: u64,
        /// Files promoted to the cold object-store tier.
        pub cold_files: u64,
        /// Bytes of the cold-tier files.
        pub cold_bytes: u64,
        /// Rows in the cold-tier files.
        pub cold_rows: u64,
        /// Live deletion-vector files.
        pub delete_files: u64,
        /// On-disk bytes of the deletion-vector files. A value approaching or
        /// exceeding `current_bytes + protected_bytes` means the deletion set
        /// now costs more than the data it shadows.
        pub delete_file_bytes: u64,
        /// Tombstones recorded across those deletion-vector files.
        pub delete_file_tombstones: u64,
        /// Manifest rows reachable from the current snapshot or a registered
        /// snapshot sequence.
        pub manifest_rows_reachable: u64,
        /// Manifest rows pointing at snapshots that are no longer live — dead
        /// weight in the metastore until a compaction prunes them.
        pub manifest_rows_unreachable: u64,
        /// Distinct files the reachable manifest rows describe.
        ///
        /// A manifest row is a `(snapshot, file)` pair, so this is `<=`
        /// `manifest_rows_reachable` and usually strictly less. Without it the
        /// row count reads as a file count and overstates real state — by an
        /// order of magnitude on a table with a deep snapshot chain, which is
        /// alarming for entirely the wrong reason.
        pub manifest_live_files: u64,
        /// Registered snapshot sequences (the durable protected-snapshot set).
        pub snapshot_sequences: u64,
        /// Per-file pruning-statistics rows (`cayenne_snapshot_file_statistics`).
        pub file_statistics_rows: u64,
        /// Re-insert records held in the metastore.
        pub insert_records: u64,
        /// Inline (level-0) data entries not yet checkpointed to Vortex files.
        pub inlined_entries: u64,
        /// Rows held in those inline entries.
        pub inlined_rows: u64,
        /// Serialized Arrow IPC bytes held inline.
        pub inlined_bytes: u64,
        /// Inline tombstone entries not yet flushed to deletion vectors.
        pub inlined_delete_entries: u64,
        /// Tombstones held in those inline entries.
        pub inlined_delete_rows: u64,
    }

    static STORAGE_FILES: OnceLock<Gauge<u64>> = OnceLock::new();
    static STORAGE_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();
    static STORAGE_ROWS: OnceLock<Gauge<u64>> = OnceLock::new();
    static STORAGE_MANIFEST_ROWS: OnceLock<Gauge<u64>> = OnceLock::new();
    static STORAGE_MANIFEST_FILES: OnceLock<Gauge<u64>> = OnceLock::new();
    static STORAGE_METASTORE_ROWS: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Publishes one table's footprint gauges. `dimensions` carries `table`; a
    /// `tier` label (`current` / `protected` / `cold` / `delete_vector` /
    /// `inline`) splits files, bytes, and rows so the growth can be attributed
    /// to the layer producing it.
    #[expect(
        clippy::too_many_lines,
        reason = "one gauge family emitted per tier; splitting it would separate the label vocabulary from the values it labels"
    )]
    pub fn track_table_storage(storage: &CayenneTableStorage, dimensions: &[KeyValue]) {
        let files = STORAGE_FILES.get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_storage_files")
                .with_description(
                    "Files a Cayenne table holds, by storage tier (`current`, `protected`, `cold`, `delete_vector`, `inline`).",
                )
                .with_unit("files")
                .build()
        });
        let bytes = STORAGE_BYTES.get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_storage_bytes")
                .with_description(
                    "On-disk bytes a Cayenne table holds, by storage tier. The `delete_vector` tier is the deletion set's own footprint — compare it against `current` + `protected` to see a deletion set outgrowing the data it shadows.",
                )
                .with_unit("By")
                .build()
        });
        let rows = STORAGE_ROWS.get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_storage_rows")
                .with_description(
                    "Rows a Cayenne table holds, by storage tier, before deletions are applied. The `delete_vector` tier is the tombstone count.",
                )
                .with_unit("rows")
                .build()
        });

        let with_tier = |tier: &'static str| {
            let mut d = Vec::with_capacity(dimensions.len() + 1);
            d.extend_from_slice(dimensions);
            d.push(KeyValue::new("tier", tier));
            d
        };

        for (tier, f, b, r) in [
            (
                "current",
                storage.current_files,
                storage.current_bytes,
                storage.current_rows,
            ),
            (
                "protected",
                storage.protected_files,
                storage.protected_bytes,
                storage.protected_rows,
            ),
            (
                "cold",
                storage.cold_files,
                storage.cold_bytes,
                storage.cold_rows,
            ),
            (
                "delete_vector",
                storage.delete_files,
                storage.delete_file_bytes,
                storage.delete_file_tombstones,
            ),
            (
                "inline",
                storage.inlined_entries,
                storage.inlined_bytes,
                storage.inlined_rows,
            ),
        ] {
            let d = with_tier(tier);
            files.record(f, &d);
            bytes.record(b, &d);
            rows.record(r, &d);
        }

        let manifest_rows = STORAGE_MANIFEST_ROWS.get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_snapshot_manifest_rows")
                .with_description(
                    "Rows a Cayenne table holds in the `cayenne_snapshot_file` manifest, split by whether the snapshot they name is still live. A row is a (snapshot, file) pair, NOT a file — read the reachable count against `cayenne_snapshot_manifest_files`. Unreachable rows are metastore weight no query can use.",
                )
                .with_unit("rows")
                .build()
        });
        for (reachable, value) in [
            ("true", storage.manifest_rows_reachable),
            ("false", storage.manifest_rows_unreachable),
        ] {
            let mut d = Vec::with_capacity(dimensions.len() + 1);
            d.extend_from_slice(dimensions);
            d.push(KeyValue::new("reachable", reachable));
            manifest_rows.record(value, &d);
        }

        STORAGE_MANIFEST_FILES
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_snapshot_manifest_files")
                    .with_description(
                        "Distinct files the live Cayenne snapshot manifest describes. Always at or below `cayenne_snapshot_manifest_rows{reachable=\"true\"}`, because compaction references an un-baked file from a new snapshot in place rather than copying it, so one file earns a row under every live snapshot referencing it. This is the file count; the row count is not.",
                    )
                    .with_unit("files")
                    .build()
            })
            .record(storage.manifest_live_files, dimensions);

        let metastore_rows = STORAGE_METASTORE_ROWS.get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_metastore_table_rows")
                .with_description(
                    "Metastore rows attributable to one Cayenne table, by metastore table — the per-table breakdown of metastore growth. These are ROW counts, not state: `cayenne_snapshot_file` counts (snapshot, file) pairs including dead snapshots' — see `cayenne_snapshot_manifest_files` and `cayenne_snapshot_manifest_rows{reachable}` before reading a large value as a large table.",
                )
                .with_unit("rows")
                .build()
        });
        for (metastore_table, value) in [
            (
                "cayenne_snapshot_file",
                storage.manifest_rows_reachable + storage.manifest_rows_unreachable,
            ),
            (
                "cayenne_snapshot_file_statistics",
                storage.file_statistics_rows,
            ),
            ("cayenne_snapshot_sequence", storage.snapshot_sequences),
            ("cayenne_delete_file", storage.delete_files),
            ("cayenne_insert_record", storage.insert_records),
            ("cayenne_inlined_data", storage.inlined_entries),
            ("cayenne_inlined_delete", storage.inlined_delete_entries),
            ("cayenne_cold_tier_file", storage.cold_files),
        ] {
            let mut d = Vec::with_capacity(dimensions.len() + 1);
            d.extend_from_slice(dimensions);
            d.push(KeyValue::new("metastore_table", metastore_table));
            metastore_rows.record(value, &d);
        }
    }

    static METASTORE_DB_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Records the size in bytes of the metastore `SQLite` database file itself,
    /// sampled (a cheap `stat()`) alongside [`track_metastore_wal_bytes`].
    /// `dimensions` carries `catalog` (the metastore path).
    ///
    /// The database file plus the `-wal` file is the whole metadata footprint;
    /// without this only the WAL half was observable.
    pub fn track_metastore_db_bytes(bytes: u64, dimensions: &[KeyValue]) {
        METASTORE_DB_BYTES
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_metastore_db_bytes")
                    .with_description(
                        "Current size in bytes of the Cayenne metastore SQLite database file.",
                    )
                    .with_unit("By")
                    .build()
            })
            .record(bytes, dimensions);
    }

    static METASTORE_FREELIST_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Records the bytes held on the metastore's `SQLite` freelist — pages that
    /// are free inside the database file but, under the default
    /// `auto_vacuum: none`, are never returned to the OS. `dimensions` carries
    /// `catalog`.
    ///
    /// This is the part of `cayenne_metastore_db_bytes` that churn has already
    /// released; a large freelist against a flat live row count is what
    /// `auto_vacuum: incremental` would give back.
    pub fn track_metastore_freelist_bytes(bytes: u64, dimensions: &[KeyValue]) {
        METASTORE_FREELIST_BYTES
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_metastore_freelist_bytes")
                    .with_description(
                        "Bytes on the Cayenne metastore SQLite freelist — free inside the database file, and under the default `auto_vacuum: none` never returned to the OS.",
                    )
                    .with_unit("By")
                    .build()
            })
            .record(bytes, dimensions);
    }

    // ───────────────── Cayenne primary-key index observability ─────────────────
    //
    // The PK existence index is the structure an upsert-heavy CDC apply leans on
    // hardest, and the one whose failures are silent: a discarded index is
    // rebuilt from the table, which is correct but costs a full keyset scan and
    // (for a bloom) re-sizes the filter. Nothing in the write-path timings
    // separates "the index was reused" from "it was thrown away and rebuilt".
    //
    // The `site` label is the load-bearing one. It names WHICH store path
    // discarded the index, and a discard concentrated at one site is what
    // distinguishes a genuine invalidation from a checkout-time guard firing on
    // indexes that needed no invalidating. If these are ever trimmed for
    // cardinality, keep `site`.

    static METASTORE_TABLE_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Records the bytes one metastore table (plus its indexes) occupies inside
    /// the database file. `dimensions` carries `catalog` and `metastore_table`.
    ///
    /// This is the attribution `cayenne_metastore_db_bytes` cannot give: the file
    /// total says the metastore is growing, this says which table is growing it.
    /// Divided by that table's total row count and multiplied by one dataset
    /// table's `cayenne_metastore_table_rows`, it also estimates a single
    /// dataset's share — an estimate, because pages are shared between the rows
    /// of every table in the catalog and cannot be attributed exactly.
    pub fn track_metastore_table_bytes(bytes: u64, dimensions: &[KeyValue]) {
        METASTORE_TABLE_BYTES
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_metastore_table_bytes")
                    .with_description(
                        "Bytes one Cayenne metastore table and its indexes occupy inside the database file — the per-table attribution of `cayenne_metastore_db_bytes`.",
                    )
                    .with_unit("By")
                    .build()
            })
            .record(bytes, dimensions);
    }

    /// What one Cayenne table's data directory actually holds on disk, by file
    /// role.
    ///
    /// Measured by walking the directory, not read from the manifest — the
    /// difference between the two is the point. The manifest counts the files a
    /// scan will read; the directory also holds retired snapshot dirs awaiting
    /// their sweep, deletion vectors nothing reclaimed, and staging left by an
    /// interrupted write. A directory materially larger than
    /// `cayenne_storage_bytes` is space no query can use.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct CayenneDataDirUsage {
        /// `.vortex` data files anywhere under the table directory, including
        /// snapshot directories the manifest no longer references.
        pub data_files: u64,
        /// Bytes of those data files.
        pub data_bytes: u64,
        /// Deletion-vector files (under `deletions/`).
        pub deletion_vector_files: u64,
        /// Bytes of the deletion-vector files.
        pub deletion_vector_bytes: u64,
        /// Files under a `_staging/` directory — an interrupted write's residue
        /// when they outlive the write that made them.
        pub staging_files: u64,
        /// Bytes under `_staging/`.
        pub staging_bytes: u64,
        /// Everything else (write-ahead logs, temporary files).
        pub other_files: u64,
        /// Bytes of everything else.
        pub other_bytes: u64,
        /// Snapshot directories present on disk. Compare against
        /// `cayenne_snapshot_manifest_rows` and the protected-snapshot count: a
        /// directory count far above the live snapshot count is retired
        /// directories the sweep has not reclaimed.
        pub snapshot_dirs: u64,
    }

    static DATA_DIR_FILES: OnceLock<Gauge<u64>> = OnceLock::new();
    static DATA_DIR_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();
    static DATA_DIR_SNAPSHOT_DIRS: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Publishes one table's measured data-directory usage. `dimensions` carries
    /// `table`; a `kind` label (`data` / `deletion_vector` / `staging` /
    /// `other`) splits files and bytes by file role.
    pub fn track_data_dir_usage(usage: &CayenneDataDirUsage, dimensions: &[KeyValue]) {
        let files = DATA_DIR_FILES.get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_data_dir_files")
                .with_description(
                    "Files present in a Cayenne table's data directory by role (`data`, `deletion_vector`, `staging`, `other`), measured by walking the directory rather than reading the manifest.",
                )
                .with_unit("files")
                .build()
        });
        let bytes = DATA_DIR_BYTES.get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_data_dir_bytes")
                .with_description(
                    "Bytes present in a Cayenne table's data directory by role. Compare against `cayenne_storage_bytes`: what the directory holds beyond what the manifest tracks is space no query can use.",
                )
                .with_unit("By")
                .build()
        });

        for (kind, f, b) in [
            ("data", usage.data_files, usage.data_bytes),
            (
                "deletion_vector",
                usage.deletion_vector_files,
                usage.deletion_vector_bytes,
            ),
            ("staging", usage.staging_files, usage.staging_bytes),
            ("other", usage.other_files, usage.other_bytes),
        ] {
            let mut d = Vec::with_capacity(dimensions.len() + 1);
            d.extend_from_slice(dimensions);
            d.push(KeyValue::new("kind", kind));
            files.record(f, &d);
            bytes.record(b, &d);
        }

        DATA_DIR_SNAPSHOT_DIRS
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_data_dir_snapshot_dirs")
                    .with_description(
                        "Snapshot directories present on disk for a Cayenne table. A count far above its live snapshot count is retired directories the sweep has not reclaimed.",
                    )
                    .with_unit("directories")
                    .build()
            })
            .record(usage.snapshot_dirs, dimensions);
    }

    static PK_INDEX_DISCARD: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts a checked-out primary-key index that was thrown away instead of
    /// being cached back. `dimensions` carries `table`, `site` (`table_keyset` /
    /// `sharded_keyset`), `kind` (`exact` / `bloom`), and `reason`
    /// (`overflowed` — the pending-key log hit its byte cap; `invalidated` — the
    /// cache was invalidated while the index was out; `replay_over_budget` — the
    /// replay pushed an exact keyset past its budget on a table that cannot
    /// degrade to a bloom).
    pub fn track_pk_index_discard(dimensions: &[KeyValue]) {
        PK_INDEX_DISCARD
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_pk_index_discard_total")
                    .with_description(
                        "Cayenne primary-key indexes discarded rather than cached back, by the store site and the reason the index could no longer describe the table's live keys.",
                    )
                    .with_unit("indexes")
                    .build()
            })
            .add(1, dimensions);
    }

    static PK_INDEX_PRESERVED: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts a checked-out primary-key index that was cached back for reuse —
    /// the positive control for [`track_pk_index_discard`]. `dimensions` carries
    /// `table`, `site`, and `kind`.
    ///
    /// A discard rate is uninterpretable without this: a low discard count can
    /// mean the preserve path is healthy, or that nothing ever checked an index
    /// out at all.
    pub fn track_pk_index_preserved(dimensions: &[KeyValue]) {
        PK_INDEX_PRESERVED
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_pk_index_preserved_total")
                    .with_description(
                        "Cayenne primary-key indexes cached back for reuse after a checkout — the positive control for `cayenne_pk_index_discard_total`.",
                    )
                    .with_unit("indexes")
                    .build()
            })
            .add(1, dimensions);
    }

    /// Which representation a primary-key existence index currently holds.
    ///
    /// Reported as a numeric gauge code rather than a label, matching
    /// `cayenne_data_storage_class`: the value is the thing that changes over
    /// time, and a label would spread one index across three series with two of
    /// them stale.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CayennePkIndexFormat {
        /// No index is cached. Every conflict-validated batch rebuilds one from
        /// the table — correct, but an O(live rows) scan inside the apply.
        Absent = 0,
        /// An exact keyset: every live primary key, with its row location and
        /// (for the table-wide index) its per-key sequence stamp. Answers
        /// "absent" with certainty, and is required by `on_conflict: do_nothing`.
        /// Its bytes grow with the live key count until the budget is reached.
        Exact = 1,
        /// A bounded bloom filter: fixed bytes, no false negatives, some false
        /// positives (which cost a validation, never correctness). What an
        /// upsert table degrades to when the exact keyset exceeds its budget.
        Bloom = 2,
    }

    impl CayennePkIndexFormat {
        /// The gauge value for this format.
        #[must_use]
        pub const fn metric_code(self) -> u64 {
            self as u64
        }
    }

    static MEMORY_ACCOUNT_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();
    static MEMORY_ACCOUNT_RESERVED_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Records the memory Cayenne accounts for one table against the `DataFusion`
    /// query pool: the three computed components, and the reservation those
    /// components actually resized on the pool.
    ///
    /// `dimensions` carries `table`; the components are split by a `kind` label
    /// (`keyset` / `deletion_index` / `cold_existence`).
    ///
    /// **Publishing both halves is the point.** `process_resident_memory_bytes`
    /// describes fact and the pool gauges describe intent, and closing the gap
    /// between them needs to know which side is wrong. If the components sum to
    /// the reservation, the accounting is landing and any remaining resident
    /// memory is off-pool structure. If they exceed it, the accounting itself is
    /// not reaching the pool. One gauge cannot distinguish those, which is
    /// exactly why a large resident figure next to a small
    /// `query_memory_pool_used_bytes` was previously uninterpretable.
    pub fn track_memory_account(
        keyset_bytes: u64,
        deletion_bytes: u64,
        cold_existence_bytes: u64,
        reserved_bytes: u64,
        dimensions: &[KeyValue],
    ) {
        let components = MEMORY_ACCOUNT_BYTES.get_or_init(|| {
            operational_meter()
                .u64_gauge("cayenne_memory_account_bytes")
                .with_description(
                    "Memory Cayenne has COMPUTED for one table and registered against the DataFusion query pool, by kind (`keyset`, `deletion_index`, `cold_existence`). Compare the sum against `cayenne_memory_account_reserved_bytes`.",
                )
                .with_unit("By")
                .build()
        });
        for (kind, bytes) in [
            ("keyset", keyset_bytes),
            ("deletion_index", deletion_bytes),
            ("cold_existence", cold_existence_bytes),
        ] {
            let mut d = Vec::with_capacity(dimensions.len() + 1);
            d.extend_from_slice(dimensions);
            d.push(KeyValue::new("kind", kind));
            components.record(bytes, &d);
        }

        MEMORY_ACCOUNT_RESERVED_BYTES
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_memory_account_reserved_bytes")
                    .with_description(
                        "Bytes one Cayenne table's memory reservation currently holds on the DataFusion query pool — what actually reached the pool, against the `cayenne_memory_account_bytes` components that were computed for it.",
                    )
                    .with_unit("By")
                    .build()
            })
            .record(reserved_bytes, dimensions);
    }

    static INLINE_CACHE_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();
    static INLINE_CACHE_BATCHES: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Records the resident Arrow bytes and batch count of a table's decoded
    /// inline (level-0) view cache. `dimensions` carries `table`.
    ///
    /// This is an off-pool derived cache: it is not registered against the query
    /// pool and does not appear in any budget, so before this it was resident
    /// memory with no gauge at all. Its bytes are the *decoded* Arrow size, so
    /// they legitimately exceed the serialized `cayenne_storage_bytes{tier="inline"}`
    /// the same rows occupy in the metastore.
    pub fn track_inline_cache(bytes: u64, batches: u64, dimensions: &[KeyValue]) {
        INLINE_CACHE_BYTES
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_inline_cache_bytes")
                    .with_description(
                        "Resident Arrow bytes of a Cayenne table's decoded inline (level-0) view cache — an off-pool derived cache that no budget bounds.",
                    )
                    .with_unit("By")
                    .build()
            })
            .record(bytes, dimensions);
        INLINE_CACHE_BATCHES
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_inline_cache_batches")
                    .with_description(
                        "Record batches held in a Cayenne table's decoded inline view cache.",
                    )
                    .with_unit("batches")
                    .build()
            })
            .record(batches, dimensions);
    }

    static FLEET_BUDGET_USED: OnceLock<Gauge<u64>> = OnceLock::new();
    static FLEET_BUDGET_LIMIT: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Records one process-global Cayenne memory budget: how much of it is in use
    /// and what its ceiling is. `dimensions` carries `budget`
    /// (`pk_keyset` / `mem_tier`); there is no `table` label because these
    /// ceilings are shared across every table in the process.
    ///
    /// The per-table gauges cannot answer "is the fleet at its ceiling", which is
    /// the question behind a table whose index refuses to grow: a keyset that
    /// stays small because the fleet budget is exhausted looks identical to one
    /// that is small because the table is.
    pub fn track_fleet_budget(used_bytes: u64, limit_bytes: u64, dimensions: &[KeyValue]) {
        FLEET_BUDGET_USED
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_fleet_budget_used_bytes")
                    .with_description(
                        "Bytes in use against a process-global Cayenne memory budget (`pk_keyset`, `mem_tier`).",
                    )
                    .with_unit("By")
                    .build()
            })
            .record(used_bytes, dimensions);
        FLEET_BUDGET_LIMIT
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_fleet_budget_limit_bytes")
                    .with_description(
                        "Ceiling of a process-global Cayenne memory budget. A used figure at the ceiling is why a table's index refuses to grow.",
                    )
                    .with_unit("By")
                    .build()
            })
            .record(limit_bytes, dimensions);
    }

    static PK_INDEX_FORMAT: OnceLock<Gauge<u64>> = OnceLock::new();
    static PK_INDEX_KEYS: OnceLock<Gauge<u64>> = OnceLock::new();
    static PK_INDEX_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();
    static PK_INDEX_BUDGET_BYTES: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Publishes one primary-key cache's resident bytes and the budget that
    /// decides when it changes shape. `dimensions` carries `table` and `site`
    /// (`table_keyset` / `sharded_keyset`).
    ///
    /// **Per cache, not per table.** A sharded (N>1) table keeps two of these at
    /// once and gives each HALF the configured budget, and they transition
    /// independently — one can be an exact keyset still growing while the other
    /// has already degraded to a bloom. A per-table sum against a single budget
    /// cannot express that, and the cache nearing its transition is the one worth
    /// knowing about. Sum across `site` for a table total, or read
    /// `cayenne_memory_account_bytes{kind="keyset"}`, which is that sum as the
    /// memory account registers it against the query pool.
    ///
    /// Split from [`track_pk_index_shape`] because the two are available under
    /// different conditions: these come from lock-free accounting counters and
    /// can always be published, while the shape needs the cache itself.
    ///
    /// Only interpretable together with the format. Bytes alone cannot
    /// distinguish an exact keyset growing toward its budget (which will degrade
    /// to a bloom and give most of them back) from a bloom already at its fixed
    /// size (which will not shrink); `bytes / budget_bytes` at
    /// `cayenne_pk_index_format = 1` is the countdown to that transition.
    pub fn track_pk_index_size(resident_bytes: u64, budget_bytes: u64, dimensions: &[KeyValue]) {
        PK_INDEX_BYTES
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_pk_index_bytes")
                    .with_description(
                        "Approximate resident bytes of one Cayenne primary-key existence cache (`site`), whichever representation it holds. A sharded table keeps two, each with its own budget; sum across `site` for a table total.",
                    )
                    .with_unit("By")
                    .build()
            })
            .record(resident_bytes, dimensions);
        PK_INDEX_BUDGET_BYTES
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_pk_index_budget_bytes")
                    .with_description(
                        "Effective byte budget THIS primary-key cache is bounded by — half the per-table figure on a sharded table, and already clamped by whatever the fleet has left. An exact keyset crossing it degrades to a bloom (upsert) or is dropped (exact-answer tables). The process-global ceiling is `cayenne_fleet_budget_limit_bytes{budget=\"pk_keyset\"}`. 0 when unbounded.",
                    )
                    .with_unit("By")
                    .build()
            })
            .record(budget_bytes, dimensions);
    }

    /// Publishes which representation one primary-key cache holds and how many
    /// keys it covers. `dimensions` carries `table` and `site`.
    ///
    /// Reported per cache because the two caches transition independently, and
    /// because a divergence in their key counts is itself a signal: they are
    /// meant to cover the same key set in different layouts, so a gap between
    /// them means one is stale. A per-table maximum would hide exactly that.
    /// Do NOT sum `keys` across `site` — it double-counts every key. (Bytes do
    /// sum; keys do not.)
    pub fn track_pk_index_shape(format: CayennePkIndexFormat, keys: u64, dimensions: &[KeyValue]) {
        PK_INDEX_FORMAT
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_pk_index_format")
                    .with_description(
                        "Representation of a Cayenne primary-key existence index: 0 absent (every validated batch rebuilds it), 1 exact keyset (grows with the live key count), 2 bounded bloom (fixed bytes, no false negatives).",
                    )
                    .build()
            })
            .record(format.metric_code(), dimensions);
        PK_INDEX_KEYS
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_pk_index_keys")
                    .with_description(
                        "Keys one Cayenne primary-key existence cache covers — exact entries, or a bloom's inserted-key count. Must NOT be summed across `site`: the two caches hold the same key set in different layouts, so a divergence between them means one is stale.",
                    )
                    .with_unit("keys")
                    .build()
            })
            .record(keys, dimensions);
    }

    /// One primary-key cache's bloom density, sampled on the background
    /// maintenance tick.
    ///
    /// No key count of its own: it is the same set `cayenne_pk_index_keys`
    /// reports for this `site`, and a second series for it would invite the two
    /// to be summed.
    #[derive(Debug, Clone, Copy)]
    pub struct CayennePkBloomState {
        /// Bits allocated by this cache's filter, or summed across its per-shard
        /// filters — every one of them is resident.
        pub bits: u64,
        /// Allocated bits per key covered by this cache. The configured target is
        /// single-digit; an order of magnitude above it is over-allocation, and
        /// on a large table that gap is the difference between a filter that
        /// fits in memory and one that does not.
        pub bits_per_key: f64,
    }

    static PK_BLOOM_BITS_PER_KEY: OnceLock<Gauge<f64>> = OnceLock::new();
    static PK_BLOOM_BITS: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Publishes one cache's bloom density gauges. `dimensions` carries `table`
    /// and `site`. Emit on every sample, ZEROED when that cache holds no bloom:
    /// a skipped gauge keeps its last value, so a cache that rebuilt an exact
    /// index would go on reporting a filter that no longer exists.
    pub fn track_pk_bloom(state: CayennePkBloomState, dimensions: &[KeyValue]) {
        PK_BLOOM_BITS_PER_KEY
            .get_or_init(|| {
                operational_meter()
                    .f64_gauge("cayenne_pk_bloom_bits_per_key")
                    .with_description(
                        "Bits one Cayenne primary-key cache's bloom filters allocate per key it covers. Compare against the configured target — far above it is over-allocation, and resident bytes scale with the gap.",
                    )
                    .with_unit("bits")
                    .build()
            })
            .record(state.bits_per_key, dimensions);
        PK_BLOOM_BITS
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_pk_bloom_bits")
                    .with_description(
                        "Bits one Cayenne primary-key cache's bloom filters allocate (summed across its per-shard filters).",
                    )
                    .with_unit("bits")
                    .build()
            })
            .record(state.bits, dimensions);
    }

    static PK_BLOOM_SPLIT_ROWS: OnceLock<Counter<u64>> = OnceLock::new();

    /// Counts rows the primary-key bloom split on the CDC apply: `result="miss"`
    /// rows the filter proved absent, which skip on-conflict validation
    /// entirely, and `result="hit"` rows that had to be validated.
    /// `dimensions` carries `table` and `result`.
    ///
    /// This is the filter's return on investment stated directly — the fraction
    /// of apply rows it takes off the validation path.
    pub fn track_pk_bloom_split_rows(rows: u64, dimensions: &[KeyValue]) {
        PK_BLOOM_SPLIT_ROWS
            .get_or_init(|| {
                operational_meter()
                    .u64_counter("cayenne_pk_bloom_split_rows_total")
                    .with_description(
                        "CDC apply rows split by the Cayenne primary-key bloom: `miss` rows skip on-conflict validation entirely, `hit` rows are validated.",
                    )
                    .with_unit("rows")
                    .build()
            })
            .add(rows, dimensions);
    }

    static WRITE_SHAPE_SHARDS: OnceLock<Gauge<u64>> = OnceLock::new();

    /// Records the encode fan-out one write resolved to, together with the
    /// branch that chose it. `dimensions` carries `table` and `decision`
    /// (`serial_sort_columns` / `serial_zorder` / `size_bounded` /
    /// `concurrency_bounded`).
    ///
    /// The shard count alone cannot be acted on: a fan-out of 1 caused by a
    /// configured write concurrency is a knob to raise, while one caused by a
    /// sort order is a structural property of the write that no knob reaches.
    /// The `decision` label is what separates them.
    pub fn track_write_shape_shards(shards: u64, dimensions: &[KeyValue]) {
        WRITE_SHAPE_SHARDS
            .get_or_init(|| {
                operational_meter()
                    .u64_gauge("cayenne_write_shape_shards")
                    .with_description(
                        "Encode fan-out (shard count) a Cayenne snapshot write resolved to, labelled with the branch that chose it.",
                    )
                    .with_unit("shards")
                    .build()
            })
            .record(shards, dimensions);
    }
}

#[cfg(test)]
mod tests {
    use super::{CONTENTION_MS_HISTOGRAM_BUCKETS, DURATION_MS_HISTOGRAM_BUCKETS};

    /// The first boundary above zero is the floor of every quantile drawn from these buckets, and
    /// requests answered in a fraction of a millisecond are ordinary.
    ///
    /// Regression test for #12693.
    #[test]
    fn the_duration_buckets_resolve_below_a_millisecond() {
        let floor = DURATION_MS_HISTOGRAM_BUCKETS
            .iter()
            .copied()
            .find(|&bound| bound > 0.0)
            .expect("the duration buckets should have a boundary above zero");

        assert!(
            floor <= 1.0,
            "the first duration boundary above zero is the floor of every quantile drawn from \
             these buckets, and {floor}ms is above a millisecond"
        );
    }

    #[test]
    fn histogram_boundaries_are_strictly_increasing() {
        for (name, bounds) in [
            ("duration", DURATION_MS_HISTOGRAM_BUCKETS.as_slice()),
            ("contention", CONTENTION_MS_HISTOGRAM_BUCKETS.as_slice()),
        ] {
            assert!(
                bounds.windows(2).all(|pair| pair[0] < pair[1]),
                "the {name} boundaries must be strictly increasing"
            );
        }
    }
}
