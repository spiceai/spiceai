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

use opentelemetry::{
    global,
    metrics::{Counter, Gauge, Histogram, Meter},
};
use std::sync::LazyLock;
use telemetry::{CONTENTION_MS_HISTOGRAM_BUCKETS, DURATION_MS_HISTOGRAM_BUCKETS};

pub const METRIC_MAX_TIMESTAMP_BEFORE_REFRESH_MS: &str =
    "dataset_acceleration_max_timestamp_before_refresh_ms";
pub const METRIC_MAX_TIMESTAMP_AFTER_REFRESH_MS: &str =
    "dataset_acceleration_max_timestamp_after_refresh_ms";
pub const METRIC_REFRESH_LAG_MS: &str = "dataset_acceleration_refresh_lag_ms";
pub const METRIC_INGESTION_LAG_MS: &str = "dataset_acceleration_ingestion_lag_ms";
pub const METRIC_REFRESH_WORKER_PANICS: &str = "dataset_acceleration_refresh_worker_panics";

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("dataset_acceleration"));

pub static REFRESH_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("dataset_acceleration_refresh_errors")
        .with_description("Number of errors refreshing the dataset.")
        .build()
});

pub static REFRESH_DATA_FETCHES_SKIPPED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("dataset_acceleration_refresh_data_fetches_skipped")
        .with_description("Number of refresh data fetches skipped due to unchanged file metadata.")
        .build()
});

pub static REFRESH_PROCESSED_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("dataset_acceleration_refresh_processed_rows")
        .with_description("Number of rows processed during dataset refresh.")
        .build()
});

pub static REFRESH_PROCESSED_BYTES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("dataset_acceleration_refresh_processed_bytes")
        .with_description("Number of bytes processed during dataset refresh.")
        .build()
});

pub static LAST_REFRESH_TIME_MS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("dataset_acceleration_last_refresh_unix_time_ms")
        .with_description("Unix timestamp in milliseconds when the last refresh completed.")
        .with_unit("ms")
        .build()
});

pub static REFRESH_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("dataset_acceleration_refresh_duration_ms")
        .with_description("Duration in milliseconds to load a full or appended refresh data.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

pub static REFRESH_WORKER_PANICS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter(METRIC_REFRESH_WORKER_PANICS)
        .with_description("Number of times a refresh worker panicked while refreshing a dataset.")
        .build()
});

pub static READY_STATE_FALLBACK: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("accelerated_ready_state_federated_fallback")
        .with_description("Number of times the federated table was queried due to the accelerated table loading the initial data.")
        .build()
});

pub static MAX_TIMESTAMP_BEFORE_REFRESH_MS: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    METER
        .i64_gauge(METRIC_MAX_TIMESTAMP_BEFORE_REFRESH_MS)
        .with_description("Maximum value of the dataset's time_column before the refresh operation, in milliseconds.")
        .build()
});

pub static MAX_TIMESTAMP_AFTER_REFRESH_MS: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    METER
        .i64_gauge(METRIC_MAX_TIMESTAMP_AFTER_REFRESH_MS)
        .with_description("Maximum value of the dataset's time_column after the refresh operation, in milliseconds.")
        .build()
});

pub static REFRESH_LAG_MS: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    METER
        .i64_gauge(METRIC_REFRESH_LAG_MS)
        .with_description("Difference between the maximum time_column value after and before the refresh operation, in milliseconds.")
        .build()
});

pub static INGESTION_LAG_MS: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    METER
        .i64_gauge(METRIC_INGESTION_LAG_MS)
        .with_description("Lag between the current wall-clock time and the maximum time_column value after the refresh operation, in milliseconds.")
        .build()
});

pub static WRITE_BACK_PENDING_KEYS: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    METER
        .i64_gauge("dataset_acceleration_write_back_pending_keys")
        .with_description(
            "Undelivered durable write-back markers: primary keys marked by a committed transaction whose values have not yet reached the federated source.",
        )
        .with_unit("keys")
        .build()
});

pub static CDC_REPLICATION_LAG_MS: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    METER
        .i64_gauge("dataset_acceleration_cdc_replication_lag_ms")
        .with_description(
            "CDC replication lag in milliseconds: wall-clock now minus the upstream commit timestamp of the latest applied change batch. For multi-shard sources (e.g. DynamoDB) this tracks the slowest shard. Low/zero = caught up; growing = falling behind.",
        )
        .with_unit("ms")
        .build()
});

pub static CDC_APPLIED_COMMIT_UNIX_TIME_MS: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    METER
        .i64_gauge("dataset_acceleration_cdc_applied_commit_unix_time_ms")
        .with_description(
            "Upstream commit timestamp (Unix epoch ms) of the latest applied CDC change batch — the source position the accelerator has caught up to. For multi-shard sources (e.g. DynamoDB) this is the slowest shard. Pair with wall-clock now to compute replication lag on your own clock.",
        )
        .with_unit("ms")
        .build()
});

/// Upstream commit timestamp (Unix epoch ms) of the latest RECEIVED CDC envelope
/// (at ingress, before coalesce/apply). Paired with
/// `cdc_applied_commit_unix_time_ms` (egress), the advance RATE of each vs wall
/// clock gives a "progress ×realtime" ladder:
///   received-rate = `d(received_commit_ts)/d(wall)`  — how fast we pull source-time IN
///   applied-rate  = `d(applied_commit_ts)/d(wall)`   — how fast we make it queryable
/// received-rate < 1 ⇒ ingress can't keep up with the source (delivery/source-bound;
/// split further by reader input-wait vs decode). received-rate ≈ 1 but
/// applied-rate < 1 ⇒ the slowdown is INSIDE our apply/write path. An independent
/// corroborator for the lag-slope / arrival-lag / classifier signals.
pub static CDC_RECEIVED_COMMIT_UNIX_TIME_MS: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    METER
        .i64_gauge("dataset_acceleration_cdc_received_commit_unix_time_ms")
        .with_description(
            "Upstream commit timestamp (Unix epoch ms) of the latest received CDC envelope (ingress). Its advance rate vs wall clock = how fast the reader pulls source-time in; compare to cdc_applied_commit_unix_time_ms (egress) to localize slowdowns to delivery vs apply.",
        )
        .with_unit("ms")
        .build()
});

pub static SIZE_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("dataset_acceleration_size_bytes")
        .with_description("Size of the accelerated table storage in bytes.")
        .with_unit("By")
        .build()
});

pub static REFRESH_ROWS_WRITTEN: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("dataset_acceleration_refresh_rows_written")
        .with_description(
            "Cumulative number of rows read from the federated source and written into the accelerated table.",
        )
        .with_unit("rows")
        .build()
});

pub static REFRESH_BYTES_WRITTEN: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("dataset_acceleration_refresh_bytes_written")
        .with_description(
            "Cumulative number of bytes (Arrow in-memory size) read from the federated source and written into the accelerated table.",
        )
        .with_unit("By")
        .build()
});

pub static CDC_APPLY_BURST_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("dataset_acceleration_cdc_apply_burst_duration_ms")
        .with_description("Duration in milliseconds to apply one coalesced CDC burst.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

pub static CDC_APPLY_BURST_BYTES: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("dataset_acceleration_cdc_apply_burst_bytes")
        .with_description(
            "Estimated size of one coalesced CDC apply burst (coalescing byte-budget proxy): \
             a schema-aware wire-size estimate for deferred (e.g. PostgreSQL) envelopes, \
             actual Arrow in-memory size for eager ones. Not an exact Arrow memory measurement.",
        )
        .with_unit("By")
        .build()
});

pub static CDC_APPLY_BURST_ENVELOPES: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("dataset_acceleration_cdc_apply_burst_envelopes")
        .with_description("Number of source envelopes in one coalesced CDC apply burst.")
        .with_unit("envelopes")
        .build()
});

pub static CDC_APPLY_BURST_ROWS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("dataset_acceleration_cdc_apply_burst_rows_total")
        .with_description("Number of rows in one coalesced CDC apply burst.")
        .with_unit("rows")
        .build()
});

pub static CDC_APPLY_FIXED_COST_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("dataset_acceleration_cdc_apply_fixed_cost_ms")
        .with_description("Duration in milliseconds for fixed-cost phases of CDC apply.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

// TODO(cdc-metrics): a `cdc_apply_unaccounted_ms = burst_ms − Σ(in-burst write phases)`
// histogram is a few lines given the burst brackets + fixed-cost phases are both here,
// and would surface an instrumentation blind spot (a delete-heavy table measured ~0.8%
// phase coverage of burst wall clock) at record time. The waterfall computes the same
// ratio from the exported sums today (two-level coverage), so this is a convenience /
// CI-gate follow-up.

/// Which apply path each CDC sub-batch took, labeled by `path`
/// (`inmem_append` | `inmem_delete` | `durable_append` | `durable_delete`). The
/// `durable_*` paths take the synchronous whole-burst commit + maintenance and are
/// far more expensive; a table pinned to them (e.g. delete-bearing bursts that clear
/// the slot-advancer) explains a large apply time that the write-phase breakdown
/// alone leaves unattributed.
pub static CDC_APPLY_PATH_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("dataset_acceleration_cdc_apply_path_total")
        .with_description(
            "Count of CDC apply sub-batches by path (inmem_append/inmem_delete/durable_append/durable_delete).",
        )
        .build()
});

/// Bucket boundaries for the per-delete-burst key-count histogram. Resolves the
/// sub-cap band (`< 2048`, where a burst runs as a single durable plan) from the
/// multi-cap tail (`> 2048`, where chunking splits the burst) so the fleet can
/// see how often bursts exceed `cdc_delete_subbatch_max`.
const DELETE_BURST_KEYS_HISTOGRAM_BUCKETS: [f64; 11] = [
    1.0, 8.0, 64.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 32768.0,
];

/// Number of primary keys in one CDC Delete sub-batch ("delete burst"), recorded
/// once per delete sub-batch in the CDC apply path regardless of whether the
/// burst was absorbed in memory or applied durably. The distribution tells us
/// how large delete bursts actually get — i.e. whether the
/// `cdc_delete_subbatch_max` cap ever binds in practice.
pub static CDC_KEYS_PER_DELETE_BURST: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("dataset_acceleration_cdc_keys_per_delete_burst")
        .with_description(
            "Number of primary keys in one CDC delete sub-batch (delete burst), before any per-plan chunking.",
        )
        .with_unit("keys")
        .with_boundaries(DELETE_BURST_KEYS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Count of CDC Delete sub-batches that could NOT be absorbed as in-memory
/// tombstones (`cdc_durability: memory`, key-mode Cayenne) and fell through to
/// the durable delete path (keyed rows via `delete_from`, keyless rows via
/// row-matching), broken down by the `reason` attribute:
/// `no_capability` (no Cayenne mem-tier delete support — the common non-Cayenne
/// / non-key-mode case), `no_advancer` (capable but the slot advancer is not
/// armed), `inextractable_keys` (a delete row carried no primary key), or
/// `budget` (the mem-tier byte budget refused the write after spill). This is
/// the discriminator for whether the eventual composite-key absorb fix targets
/// the right reason.
pub static CDC_DELETE_ABSORB_FALLTHROUGH: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("dataset_acceleration_cdc_delete_absorb_fallthrough_total")
        .with_description(
            "Count of CDC delete sub-batches that fell through the in-memory absorb path to a durable delete, by reason (no_capability|no_advancer|inextractable_keys|budget).",
        )
        .with_unit("bursts")
        .build()
});

/// Time the CDC apply loop spent blocked waiting to receive the next batch from
/// the source-reader channel (i.e. waiting on the replication-slot read + WAL
/// decode that the reader task performs). This is the discriminator for the
/// "unaccounted per-batch overhead" gap: a high recv-wait means the apply loop
/// is *source-bound* (the reader cannot decode/deliver batches fast enough),
/// while a near-zero recv-wait means the loop is *apply-bound* (the bottleneck
/// is the accelerator write, e.g. Cayenne's synchronous on-conflict path). Pair
/// it with `cdc_apply_burst_duration_ms` for full per-batch attribution
/// (wall-clock ≈ recv-wait + apply-burst).
pub static CDC_SOURCE_RECV_WAIT_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("dataset_acceleration_cdc_source_recv_wait_ms")
        .with_description(
            "Duration in milliseconds the CDC apply loop waited to receive the next batch from the source-reader channel. High = source-bound (slot read / WAL decode can't keep up); near-zero = apply-bound.",
        )
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

pub static CDC_LINGER_WAIT_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("dataset_acceleration_cdc_linger_wait_ms")
        .with_description(
            "Duration in milliseconds the CDC apply loop spent in the Phase-2 linger window accumulating envelopes before applying the coalesced burst (cdc_max_coalesce_age_ms).",
        )
        .with_unit("ms")
        .with_boundaries(CONTENTION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Occupancy of the bounded prefetch channel between the CDC source-reader task
/// and the apply loop, sampled each time the apply loop wakes with a new batch.
/// Pinned at (or near) `dataset_acceleration_cdc_prefetch_buffer_capacity` means
/// the reader is producing faster than the apply loop drains — the definitive
/// **apply-bound** signal (the accelerator write, e.g. Cayenne, is the
/// bottleneck). Near-zero means the loop keeps up / is **source-bound** (pair
/// with `cdc_source_recv_wait_ms`). Labeled by `dataset`.
pub static CDC_PREFETCH_BUFFER_OCCUPANCY: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("dataset_acceleration_cdc_prefetch_buffer_occupancy")
        .with_description(
            "Buffered items in the CDC source-reader→apply prefetch channel when the apply loop last woke. Near capacity = apply-bound (accelerator write is the bottleneck); near zero = source-bound.",
        )
        .with_unit("{envelope}")
        .build()
});

/// Capacity (buffer size) of the CDC prefetch channel — the `cdc_prefetch_buffer`
/// config. Emitted alongside the occupancy so a dashboard can compute the
/// fill ratio without hard-coding the default. Labeled by `dataset`.
pub static CDC_PREFETCH_BUFFER_CAPACITY: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("dataset_acceleration_cdc_prefetch_buffer_capacity")
        .with_description(
            "Capacity of the CDC source-reader→apply prefetch channel (the cdc_prefetch_buffer config).",
        )
        .with_unit("{envelope}")
        .build()
});

/// Estimated **size** of the CDC prefetch backlog, as distinct from the envelope
/// count next to it. The channel is bounded by count, not size, and an envelope
/// carries a batch whose width the bound says nothing about — so occupancy in
/// envelopes can sit mid-range while the backlog behind it is large enough to
/// matter to the process.
///
/// Nothing else sizes this. The query and compaction pools account for their own
/// reservations, the mem-tier budget accounts for applied rows, and the
/// coalescing path is bounded by `cdc_max_coalesced_bytes` — but what *waits* in
/// this channel, ahead of apply, appears in none of them. Reading it against a
/// process total is what tells an operator whether this path is worth
/// investigating at all: this measures, it does not bound.
///
/// **This is an estimate, not measured resident bytes.** It sums each envelope's
/// `ChangeRows::encoded_len()`, the same decode-free figure
/// `cdc_max_coalesced_bytes` budgets against, chosen so that sampling never
/// forces a deferred envelope to build. What that figure means depends on the
/// envelope: for one already built it is the Arrow footprint
/// (`get_array_memory_size`); for one still deferred in wire form it is the
/// source's proxy for the Arrow memory the envelope *will* occupy — `PostgreSQL`
/// uses `max(wire_bytes, rows × fixed-width footprint)` — which can exceed what
/// the queued object holds today. Read it as the backlog's size on the same
/// scale the coalescing budget uses, not as this path's exact contribution to
/// RSS. Labeled by `dataset`.
pub static CDC_PREFETCH_BUFFER_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("dataset_acceleration_cdc_prefetch_buffer_bytes")
        .with_description(
            "Estimated encoded size of the CDC source-reader→apply prefetch backlog, summed from the same decode-free per-envelope estimate the coalescing byte budget uses (not measured resident bytes). The channel is bounded by envelope count, not size, and this backlog is counted by no other budget.",
        )
        .with_unit("By")
        .build()
});

/// Counts applied CDC bursts by what ended coalescing (the flush `reason`):
/// `deadline` (the `cdc_max_coalesce_age_ms` linger timer fired — the batch was
/// held for freshness-cost time waiting for more rows), `envelope_cap`
/// (`cdc_max_coalesced_envelopes` reached), `byte_cap` (`cdc_max_coalesced_bytes`
/// reached), `buffer_drained` (linger disabled or nothing left to coalesce),
/// `channel_closed`, or `shutdown`. A high `deadline` share means coalescing is
/// timer-bound (low source volume) and the linger is adding latency without
/// filling batches; a high `envelope_cap`/`byte_cap` share means batches fill
/// before the deadline (the write path, not the timer, paces the apply loop).
/// Labeled by `dataset` + `reason`.
pub static CDC_COALESCE_FLUSH_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("dataset_acceleration_cdc_coalesce_flush_total")
        .with_description(
            "CDC coalesced bursts applied, labeled by dataset and flush reason (deadline / envelope_cap / byte_cap / buffer_drained / channel_closed / shutdown).",
        )
        .with_unit("{burst}")
        .build()
});

/// Time from receiving the FIRST envelope of a coalesced burst until that burst
/// is flushed (the accelerator write begins) — i.e. how long the head-of-batch
/// change sat being coalesced (Phase-1 drain + Phase-2 linger). This is the
/// per-batch queued/coalescing latency the linger policy trades for larger
/// writes; pair with `cdc_replication_lag_ms` to attribute lag to coalescing vs
/// the write path. Uses the fine contention buckets since a batch can flush in
/// sub-ms (cap hit) or wait out the multi-second `cdc_max_coalesce_age_ms`.
/// Labeled by `dataset`.
pub static CDC_COALESCE_BATCH_AGE_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("dataset_acceleration_cdc_coalesce_batch_age_ms")
        .with_description(
            "Time from receiving the first envelope of a coalesced CDC burst until it is flushed to the accelerator (Phase-1 drain + Phase-2 linger) — the per-batch queued/coalescing latency.",
        )
        .with_unit("ms")
        .with_boundaries(CONTENTION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Staleness of the first envelope of a burst AT THE MOMENT IT IS RECEIVED by the
/// apply loop: wall-clock now − its upstream source-commit timestamp. This is lag
/// that is ALREADY present before the accelerator does anything — PG WAL flush +
/// network + logical-decode + reader delivery — so it cleanly separates
/// *source-side* lag from the lag the apply path ADDS (queue + write). Pair with
/// `cdc_coalesce_batch_age_ms` (queue) and `cdc_apply_burst_duration_ms` (write)
/// for an additive decomposition of `cdc_replication_lag_ms`. High arrival lag +
/// low `recv_wait` ⇒ source can't keep up (not idle); near-zero arrival lag ⇒ any
/// lag is added downstream. Uses the coarse duration buckets since a backlog can
/// reach many seconds/minutes (the histogram tail is the point). Labeled by
/// `dataset`.
pub static CDC_SOURCE_ARRIVAL_LAG_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("dataset_acceleration_cdc_source_arrival_lag_ms")
        .with_description(
            "Staleness of a burst's first envelope when received (now − source commit ts) — source-side lag present before the accelerator acts, separating it from lag the apply path adds.",
        )
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Time the CDC source-reader task blocked on `send` into the prefetch channel.
/// Non-zero means the channel was full — the apply loop (accelerator write) is not
/// draining fast enough, i.e. **apply-bound / downstream backpressure** on the
/// reader. Near-zero with a rising lag means the reader itself (source socket or
/// decode) is the limiter, not the apply path. Labeled by `dataset`.
pub static CDC_READER_SEND_WAIT_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("dataset_acceleration_cdc_reader_send_wait_ms")
        .with_description(
            "Time the CDC source-reader blocked sending into the prefetch channel (channel full => apply-bound / downstream backpressure).",
        )
        .with_unit("ms")
        .with_boundaries(CONTENTION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Wall-clock period between successive CDC burst applies (one iteration's
/// recv-start to the next). This is the ground-truth apply CADENCE that anchors
/// the per-stage attribution: the sum of the per-stage means overstates the real
/// cycle where phases overlap (pipelined commit/finalize), so comparing that sum
/// to this cadence exposes the overlap. A cadence pinned near
/// `cdc_max_coalesce_age_ms` means the linger timer paces the loop. Labeled by
/// `dataset`.
pub static CDC_APPLY_CYCLE_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("dataset_acceleration_cdc_apply_cycle_ms")
        .with_description(
            "Wall-clock period between successive CDC burst applies (recv-start to recv-start) — the apply cadence that ground-truths the per-stage attribution.",
        )
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});
