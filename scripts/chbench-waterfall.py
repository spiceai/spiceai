#!/usr/bin/env python3
#
# Copyright 2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# CDC backpressure waterfall for CH-BenCHmark runs.
#
# Consumes the JSON metrics dump written by `testoperator run htap --metrics-dump`
# (the full scraped /metrics time-series plus run metadata) and attributes
# end-to-end CDC lag/backpressure across each stage of the pipeline, from
# transactions committed on the source Postgres through every layer inside
# Cayenne:
#
#   0. Postgres -> WAL backlog        replication_lag_bytes / _ms (source side)
#   1. Source decode/deliver          cdc_source_recv_wait_ms  (high => source-bound)
#   2. Prefetch channel               cdc_prefetch_buffer_occupancy / _capacity
#   3. Coalesce / linger              cdc_apply_fixed_cost_ms{phase=coalesce}, cdc_linger_wait_ms
#   4a. Encode-permit wait            cayenne_encode_acquire_wait_ms, write_phase{encode_permit_wait}
#   4b. Encode                        cayenne_write_phase_duration_ms{phase=vortex_write}
#   4c. Mem-tier                      cayenne_mem_tier_budget_used/total_bytes
#   5. Commit / finalize              cdc_apply_fixed_cost_ms{phase=commit_wait|finalize_wait}
#   6. Compaction (async)             cayenne_compaction_acquire_wait_ms, read-amp
#
# The per-burst stage means (recv_wait + coalesce + linger + write + commit +
# finalize) sum to ~= the apply-burst wall clock, so the biggest term is the
# dominant lag contributor. Occupancy gauges (WAL backlog, channel fill,
# permits-available minima, acquire-wait percentiles) show which valve is the
# bottleneck.
#
# Usage:
#   scripts/chbench-waterfall.py <metrics.json>
#
# Only the Python standard library is used, so it runs anywhere (locally and on
# CI runners) with no dependencies.

import argparse
import json
import math
import sys


# ---- metric names ---------------------------------------------------------

DA = "dataset_acceleration_"          # runtime apply-loop meter
PG = "dataset_postgres_replication_"  # postgres connector meter
CY = "cayenne_"                       # cayenne operational meter

# ---------------------------------------------------------------------------
# Metric contract (a.k.a. "what to export").
#
# The analysis depends ONLY on the standard spiced metric series below — all
# exposed at spiced's Prometheus `/metrics` and via OTLP — plus optional run
# metadata (commit/config) that is display-only. There is NO dependency on
# anything specific to the testoperator dump, so a future source adapter
# (offline Prometheus scrape, live PromQL/Grafana, OTLP export — see the
# "planned improvement" in the plan) can feed the same analyzer unchanged, as
# long as it provides these series normalized to {name, labels, value, type}.
#
# Fetch requirements for a non-dump source:
#   * gauge   -> a TIME SERIES over the window (percentiles/min/max) — i.e. a
#                Prometheus range query or periodic scrapes, NOT one instant read.
#   * hist/ctr-> only the FINAL cumulative value (quantiles over final buckets /
#                counter totals) — a single instant query or scrape suffices.
#   * A configurable `metric_prefix` in spiced would prefix every name; a future
#     adapter should accept a prefix override. This tool assumes no prefix.
#   * JOIN KEY: the replication (`dataset_postgres_replication_*`) series label the
#     dataset `name`, while the acceleration (`dataset_acceleration_cdc_*`) series
#     label it `dataset` — same value, different key. A consumer joining the two
#     rungs must treat `name == dataset` (this tool special-cases it per lookup).
#
# (base_name, kind, key_labels)
REQUIRED_METRICS = [
    # gauges (need a time series)
    ("dataset_acceleration_cdc_replication_lag_ms", "gauge", "dataset"),
    ("dataset_acceleration_cdc_received_commit_unix_time_ms", "gauge", "dataset"),
    ("dataset_acceleration_cdc_applied_commit_unix_time_ms", "gauge", "dataset"),
    ("dataset_acceleration_cdc_prefetch_buffer_occupancy", "gauge", "dataset"),
    ("dataset_acceleration_cdc_prefetch_buffer_capacity", "gauge", "dataset"),
    ("dataset_postgres_replication_lag_bytes", "gauge", "name"),
    ("dataset_postgres_replication_confirmed_flush_lsn", "gauge", "name"),
    ("dataset_postgres_replication_server_wal_end_lsn", "gauge", "name"),
    ("cayenne_encode_permits_available", "gauge", "(none)"),
    ("cayenne_encode_permits_total", "gauge", "(none)"),
    ("cayenne_compaction_permits_available", "gauge", "(none)"),
    ("cayenne_compaction_permits_total", "gauge", "(none)"),
    ("cayenne_mem_tier_budget_used_bytes", "gauge", "(none)"),
    ("cayenne_mem_tier_budget_total_bytes", "gauge", "(none)"),
    ("cayenne_ingest_read_amp", "gauge", "table"),
    ("cayenne_mem_tier_apply_epoch", "gauge", "table (memory mode)"),
    ("cayenne_mem_tier_durable_epoch", "gauge", "table (memory mode)"),
    # histograms (final cumulative _bucket/_sum/_count)
    ("dataset_acceleration_cdc_source_recv_wait_ms", "histogram", "dataset"),
    ("dataset_acceleration_cdc_source_arrival_lag_ms", "histogram", "dataset"),
    ("dataset_acceleration_cdc_apply_fixed_cost_ms", "histogram", "dataset,phase"),
    ("dataset_acceleration_cdc_linger_wait_ms", "histogram", "dataset"),
    ("dataset_acceleration_cdc_apply_burst_duration_ms", "histogram", "dataset"),
    ("dataset_acceleration_cdc_apply_cycle_ms", "histogram", "dataset"),
    ("dataset_acceleration_cdc_coalesce_batch_age_ms", "histogram", "dataset"),
    ("cayenne_write_phase_duration_ms", "histogram", "table,phase"),
    ("cayenne_encode_acquire_wait_ms", "histogram", "class"),
    ("cayenne_compaction_acquire_wait_ms", "histogram", "table"),
    ("cayenne_mem_tier_acquire_wait_ms", "histogram", "table (memory mode)"),
    ("dataset_acceleration_cdc_reader_send_wait_ms", "histogram", "dataset"),
    # counters (final cumulative)
    ("dataset_acceleration_cdc_coalesce_flush_total", "counter", "dataset,reason"),
    ("dataset_acceleration_cdc_apply_path_total", "counter", "dataset,path"),
    ("dataset_postgres_replication_reader_input_wait_micros_total", "counter", "name"),
    ("dataset_postgres_replication_reader_processing_micros_total", "counter", "name"),
    ("dataset_postgres_replication_member_send_wait_micros_total", "counter", "name"),
    ("dataset_postgres_replication_reconnects_total", "counter", "name"),
    ("dataset_postgres_replication_disconnected_ms_total", "counter", "name"),
    ("dataset_postgres_replication_member_attached", "gauge", "name,slot"),
    ("cayenne_mem_tier_reserve_refused_total", "counter", "(none) (memory mode)"),
    ("cayenne_mem_tier_checkpoint_tick_total", "counter", "table,outcome (memory mode)"),
]


# Per-burst apply-loop stage timings (histograms; mean = _sum/_count).
STAGE_HISTS = [
    ("recv_wait", DA + "cdc_source_recv_wait_ms", {}),
    ("decode", DA + "cdc_apply_fixed_cost_ms", {"phase": "decode"}),
    ("coalesce", DA + "cdc_apply_fixed_cost_ms", {"phase": "coalesce"}),
    ("linger", DA + "cdc_linger_wait_ms", {}),
    ("write", DA + "cdc_apply_fixed_cost_ms", {"phase": "write"}),
    ("commit_wait", DA + "cdc_apply_fixed_cost_ms", {"phase": "commit_wait"}),
    ("finalize_wait", DA + "cdc_apply_fixed_cost_ms", {"phase": "finalize_wait"}),
]


# ---- sample helpers -------------------------------------------------------


def load(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def samples(data, name):
    return data.get("samples", {}).get(name) or []


def matches(sample, want):
    labels = sample.get("labels", {})
    return all(labels.get(k) == v for k, v in want.items())


def label_key(labels, exclude=()):
    return tuple(sorted((k, v) for k, v in labels.items() if k not in exclude))


def gauge_values(data, name, want=None):
    """Ordered non-NaN values of a gauge series matching `want`."""
    want = want or {}
    out = []
    for s in samples(data, name):
        if not matches(s, want):
            continue
        v = s.get("value")
        if v is None or (isinstance(v, float) and math.isnan(v)):
            continue
        out.append(v)
    return out


# Window span (ms) across the dump's scrapes, set by require_window().
_STATE = {"window_ms": 0}


def require_window(data):
    """Compute the window span and BAIL if the dump lacks the data the windowed
    analysis needs: per-scrape timestamps on ≥2 cumulative snapshots. We do not
    support lifetime-only / single-snapshot dumps — a current testoperator
    `--metrics-dump` always provides them."""
    tmin = tmax = None
    windowed = False
    for series in data.get("samples", {}).values():
        mtype = series[0].get("metric_type") if series else None
        cumulative = mtype in ("Counter", "Histogram", "Summary")
        tss = {s["ts_ms"] for s in series if s.get("ts_ms", 0)}
        if tss:
            lo, hi = min(tss), max(tss)
            tmin = lo if tmin is None else min(tmin, lo)
            tmax = hi if tmax is None else max(tmax, hi)
        if cumulative and len(tss) >= 2:
            windowed = True
    if not windowed:
        raise SystemExit(
            "error: dump lacks per-scrape timestamps / has a single cumulative "
            "snapshot. The windowed analysis needs a current testoperator "
            "--metrics-dump (≥ 2 timestamped snapshots)."
        )
    _STATE["window_ms"] = (tmax - tmin) if (tmin is not None and tmax is not None) else 0
    _STATE["window_start_ms"] = tmin
    _STATE["window_end_ms"] = tmax
    return _STATE["window_ms"]


def _pairs(data, name, want=None):
    """Per distinct label-set matching `want`, the (first, last) sample by ts.
    Cumulative dumps carry exactly first+last; a full series collapses to its
    time-extremes. Returns {label_key: (first, last)}."""
    want = want or {}
    out = {}
    for s in samples(data, name):
        if not matches(s, want):
            continue
        k = label_key(s.get("labels", {}))
        ts = s.get("ts_ms", 0) or 0
        cur = out.get(k)
        if cur is None:
            out[k] = [s, s]
        else:
            if ts <= (cur[0].get("ts_ms", 0) or 0):
                cur[0] = s
            if ts >= (cur[1].get("ts_ms", 0) or 0):
                cur[1] = s
    return out


def _delta(first, last):
    """Windowed Δ (last − first) across the snapshot pair."""
    return last.get("value", 0.0) - first.get("value", 0.0)


def hist_mean(data, base, want=None):
    """Windowed average per observation (ms): ΔΣ_sum / ΔΣ_count over the window."""
    ds = sum(_delta(f, l) for f, l in _pairs(data, base + "_sum", want).values())
    dc = sum(_delta(f, l) for f, l in _pairs(data, base + "_count", want).values())
    return (ds / dc) if dc > 0 else 0.0


def hist_count(data, base, want=None):
    return sum(_delta(f, l) for f, l in _pairs(data, base + "_count", want).values())


def hist_top_finite_le(data, base, want=None):
    """Largest finite bucket edge (`le`) of a histogram — the ceiling a quantile can
    report. A quantile landing at/above this is a bucket-edge artifact (the true value
    is in the +inf bucket), not a measurement, so callers render it as `≥ceiling`."""
    top = None
    for f, _ in _pairs(data, base + "_bucket", want).values():
        le = f.get("labels", {}).get("le")
        if le is None or le.lower() in ("+inf", "inf"):
            continue
        v = _parse_float(le)
        if v is not None and (top is None or v > top):
            top = v
    return top


def hist_quantile(data, base, q, want=None):
    """Windowed histogram_quantile over Δ`_bucket` (Δ per le, summed across series)."""
    per_le = {}
    for f, l in _pairs(data, base + "_bucket", want).values():
        le = f.get("labels", {}).get("le")
        if le is None:
            continue
        per_le[le] = per_le.get(le, 0.0) + _delta(f, l)
    if not per_le:
        return 0.0

    bounds = []
    for le, cum in per_le.items():
        b = math.inf if le.lower() in ("+inf", "inf") else _parse_float(le)
        if b is None:
            continue
        bounds.append((b, cum))
    bounds.sort(key=lambda x: x[0])
    if not bounds or bounds[-1][1] <= 0:
        return 0.0

    total = bounds[-1][1]
    rank = q * total
    lower_le, lower_cum = 0.0, 0.0
    for le, cum in bounds:
        if cum >= rank:
            if math.isinf(le):
                return lower_le
            if cum > lower_cum:
                return lower_le + (le - lower_le) * (rank - lower_cum) / (cum - lower_cum)
            return le
        lower_le, lower_cum = le, cum
    return bounds[-1][0]


def _parse_float(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def percentile(values, q):
    if not values:
        return 0.0
    s = sorted(values)
    idx = min(len(s) - 1, max(0, math.ceil(len(s) * q) - 1))
    return s[idx]


def counter_totals_by(data, name, group_label, want=None):
    """Windowed Δ counter per `group_label` value, matching `want`.
    Returns {group_value: delta}."""
    per = {}
    for f, l in _pairs(data, name, want).values():
        g = f.get("labels", {}).get(group_label, "-")
        per[g] = per.get(g, 0.0) + _delta(f, l)
    return per


def gauge_rate(data, name, want=None):
    """Advance rate of a monotonic timestamp gauge vs wall clock, over the window:
    Δ(value)/Δ(ts_ms). For the commit-ts frontiers this is the '×realtime' progress
    (1.0 = keeping up, <1 = falling behind). None if <2 timestamped samples."""
    want = want or {}
    pts = sorted((x["ts_ms"], x["value"]) for x in samples(data, name)
                 if matches(x, want) and x.get("ts_ms", 0))
    if len(pts) >= 2 and pts[-1][0] > pts[0][0]:
        return (pts[-1][1] - pts[0][1]) / (pts[-1][0] - pts[0][0])
    return None


def gauge_sample_count(data, name, want=None):
    """Number of gauge samples in the window (for low-confidence marking)."""
    want = want or {}
    return sum(1 for s in samples(data, name) if matches(s, want))


LOW_CONF_MIN_SAMPLES = 10


def label_values(data, name, label):
    """Distinct values a label takes across a metric's samples (order-preserving)."""
    seen = []
    for s in samples(data, name):
        v = s.get("labels", {}).get(label)
        if v is not None and v not in seen:
            seen.append(v)
    return seen


# ---- rendering ------------------------------------------------------------


def bar(fraction, width=28):
    fraction = 0.0 if fraction < 0 else (1.0 if fraction > 1 else fraction)
    filled = int(round(fraction * width))
    return "#" * filled + "." * (width - filled)


def fmt_bytes(n):
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024.0 or unit == "TiB":
            return f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}TiB"


WAL_ACCUMULATING_BYTES_PER_S = 64 * 1024  # server-side byte backlog growth => source can't ship


def classify_dataset(lag_p99, fill, arrival_p99, wal_slope, wal_max, top_stage,
                     memory_mode, residual_p99, apply_ratio, lag_slope_s,
                     input_share, send_wait_p99, received_ratio=None, reconnects=0.0,
                     member_attached=1.0):
    """Decision tree over the measured evidence (thresholds heuristic; the deciding
    inputs are printed so the label is auditable).

    `apply_ratio` = applied source-time advance ÷ wall time (<1 = diverging).
    `lag_slope_s` = lag growth in s per wall-second. `input_share` = reader time
    blocked on the source socket ÷ (socket-wait + decode/build); high ⇒ the source
    can't deliver (source-bound), low ⇒ our decode/build is the limiter.
    `send_wait_p99` = reader blocked pushing into the prefetch channel (apply-bound).
    `reconnects` = replication reconnects observed in the window (stream health).

    Ordering matters: stream instability and a saturated prefetch buffer are checked
    FIRST, because both corrupt the downstream source-vs-apply signals — a replaying
    connection inflates apply and resets the rate frontiers, and a pinned buffer means
    delivery already outran apply no matter what the WAL backlog upstream looks like.
    """
    diverging = (apply_ratio is not None and apply_ratio < 0.9) or lag_slope_s > 0.1
    ev = (f"lag_p99={lag_p99:.0f}ms lag_slope={lag_slope_s:+.2f}s/s "
          f"apply_rate={'?' if apply_ratio is None else f'{apply_ratio:.2f}x'} "
          f"fill={fill * 100:.0f}% arrival_p99={arrival_p99:.0f}ms "
          f"wal_slope={fmt_bytes(wal_slope)}/s wal_max={fmt_bytes(wal_max)} "
          f"dominant_stage={top_stage} residual_p99={residual_p99:.0f}ms "
          f"reader_input_share={'?' if input_share is None else f'{input_share * 100:.0f}%'} "
          f"reader_send_wait_p99={send_wait_p99:.0f}ms "
          f"received_rate={'?' if received_ratio is None else f'{received_ratio:.2f}x'} "
          f"reconnects={reconnects:.0f} member_attached={member_attached:.0f} memory_mode={memory_mode}")
    # (0) Liveness FIRST: a member that detached mid-window has a frozen frontier and a
    # meaningless rate ladder, and it pins the shared slot's WAL retention. Classifying
    # it by its (stale) rates would be actively misleading — surface the death instead.
    if member_attached == 0:
        return ("STREAM-DEAD (member detached mid-window; frontier frozen + slot WAL "
                "retention pinned — rate ladder below is stale, do not trust it)", ev)
    # (1) Stream instability next: a dropping/replaying connection replays already-
    # applied rows (inflating apply) and resets the received/applied frontiers, so
    # every source-vs-apply signal below is unreliable until this is fixed.
    if reconnects >= 1:
        return (f"STREAM-UNSTABLE ({reconnects:.0f} reconnect(s) in window; connection "
                "dropping + replaying — fix before trusting the source/apply split)", ev)
    if not diverging and lag_p99 < 1000:
        return "healthy / fresh (keeping up)", ev
    # (2) Prefetch buffer saturated => we already delivered more than the accelerator
    # can drain: apply-bound regardless of the upstream WAL backlog.
    if fill >= 0.95:
        return ("APPLY-bound (prefetch buffer saturated; accelerator can't drain "
                "what the reader already delivered)", ev)
    # Server-side WAL bytes accumulating => the source genuinely can't ship.
    # CAVEAT: wal_slope is the CLIENT-view backlog (server_wal_end − confirmed_flush).
    # When the walsender is WriteData-blocked (client not draining) its keepalives stop
    # arriving, so the client's wal_end — and this slope — go FLAT exactly when the
    # source pipe is the story. Corroborate with the source-side authoritative retained
    # WAL + walsender WriteData% in the pg_stats section before trusting a quiet slope.
    if wal_slope > WAL_ACCUMULATING_BYTES_PER_S:
        return "SOURCE-throughput-bound (WAL bytes accumulating server-side)", ev
    # Independent rate signal: ingress kept up (~realtime) but apply didn't => the
    # slowdown is our apply/write, regardless of buffer fill.
    if received_ratio is not None and received_ratio >= 0.9 \
            and apply_ratio is not None and apply_ratio < 0.9:
        return "APPLY-bound (ingress ~realtime by rate ladder, but apply lags)", ev
    # Reader blocked pushing downstream => the apply/write path can't drain.
    if send_wait_p99 > 100 or (fill > 0.5 and top_stage == "write"):
        return "APPLY-bound (accelerator write is the bottleneck; reader send-blocked)", ev
    if memory_mode and fill <= 0.5 and residual_p99 > 0.3 * max(lag_p99, 1.0):
        return "DURABILITY/checkpoint-bound (mem-tier ack gating WAL recycle)", ev
    # Diverging, buffer starved, accelerator idle, WAL bytes flat => the reader's
    # delivery rate is the limiter. Split source-socket-wait from our-decode via
    # the reader input-share.
    if diverging and fill <= 0.5:
        if input_share is not None and input_share >= 0.6:
            return ("SOURCE-send-bound (reader mostly BLOCKED on the PG socket; "
                    "source/network/PG can't deliver fast enough)", ev)
        if input_share is not None and input_share <= 0.4:
            return ("READER-decode-bound (socket has data; OUR decode/build is the "
                    "limiter — the reader is CPU/scheduling constrained). NOTE: on the "
                    "SHARED-slot path the 'decode/build' bucket also includes time BLOCKED "
                    "delivering to a slow member's channel (downstream back-pressure, not "
                    "decode) — cross-check the source-side walsender WriteData% before "
                    "trusting this for multi-member sources", ev)
        return ("READER/delivery-bound (mixed socket-wait vs decode; see "
                "reader_input_share)", ev)
    if wal_max < 1_048_576 and arrival_p99 < 1000:
        return "IDLE (lag without backlog — stale watermark / low volume / clock skew)", ev
    return "UNCLEAR (see evidence; check the checkpoint stage if memory mode)", ev


def render(data):
    out = []
    p = out.append

    run = data.get("run", {}) or {}
    p("CH-BenCHmark CDC backpressure waterfall")
    p("=" * 60)
    if run:
        p(f"spiced commit : {run.get('spiced_commit_sha', '?')}")
        p(f"branch        : {run.get('branch_name', '?')}")
        p(f"spicepod      : {run.get('spicepod_path', '?')}")
        p(
            f"config        : SF={run.get('scale_factor', '?')} "
            f"terminals={run.get('terminals', '?')} "
            f"duration={run.get('duration_secs', '?')}s "
            f"concurrency={run.get('concurrency', '?')} "
            f"oltp_rate={run.get('target_oltp_rate', '?')}"
        )

    window_ms = require_window(data)
    p(f"window        : {window_ms / 1000:.0f}s (cumulative stats are Δ over this window; "
      "excludes bootstrap baked into the first snapshot)")
    # `*_lag_ms` = local now − upstream commit ts across hosts, so any source↔spiced
    # clock skew (local − server) biases arrival/replication lag. When the artifact
    # carries an estimate (testoperator probes clock_timestamp()), subtract it from the
    # displayed lag/arrival (rates/slopes are differences, so skew cancels — left as-is).
    skew_ms = run.get("clock_skew_ms_estimate")
    if isinstance(skew_ms, (int, float)):
        skew_ms = float(skew_ms)
        p(f"note          : lag/arrival corrected for source↔spiced clock skew "
          f"({skew_ms:+.0f}ms, subtracted; clamped ≥0).")
    else:
        skew_ms = 0.0
        p("note          : lag metrics carry cross-host clock skew (no estimate in artifact; uncorrected).")
    # Subtract skew from a displayed lag/arrival value, clamped at 0.
    def _skew_corr(v):
        return max(0.0, v - skew_ms)
    p("")

    # Memory-durability mode? (mem-tier active or checkpoint metrics present.)
    memory_mode = bool(
        gauge_values(data, CY + "mem_tier_budget_used_bytes")
        and max(gauge_values(data, CY + "mem_tier_budget_used_bytes")) > 0
    ) or bool(samples(data, CY + "mem_tier_checkpoint_tick_total"))

    # ---- Per-dataset stage waterfall (mean ms per applied burst) ----
    datasets = label_values(data, DA + "cdc_source_recv_wait_ms_count", "dataset")
    if not datasets:
        datasets = label_values(data, DA + "cdc_replication_lag_ms", "dataset")

    if datasets:
        p("Per-dataset lag decomposition + apply-path breakdown")
        p("-" * 60)
    for ds in datasets:
        want = {"dataset": ds}
        stage_ms = [(label, hist_mean(data, base, {**want, **extra}))
                    for (label, base, extra) in STAGE_HISTS]
        burst = hist_mean(data, DA + "cdc_apply_burst_duration_ms", want)
        bursts = hist_count(data, DA + "cdc_source_recv_wait_ms", want)

        # Ground-truth pieces (each an independent, directly-measured metric).
        # Arrival + replication lag are absolute host-clock deltas, so skew-correct them.
        arrival_p50 = _skew_corr(hist_quantile(data, DA + "cdc_source_arrival_lag_ms", 0.50, want))
        arrival_p99 = _skew_corr(hist_quantile(data, DA + "cdc_source_arrival_lag_ms", 0.99, want))
        # Top finite arrival bucket: a quantile at/above it is a bucket-edge artifact
        # (true value is in the +inf overflow), rendered `≥ceiling` so a round number
        # like 500000 isn't mistaken for a measurement.
        arr_top = hist_top_finite_le(data, DA + "cdc_source_arrival_lag_ms", want)

        def _fmt_arr(v):
            return f"≥{v:>8.0f}" if (arr_top is not None and v >= arr_top) else f"{v:>9.0f}"
        age_p50 = hist_quantile(data, DA + "cdc_coalesce_batch_age_ms", 0.50, want)
        age_p99 = hist_quantile(data, DA + "cdc_coalesce_batch_age_ms", 0.99, want)
        cycle_p50 = hist_quantile(data, DA + "cdc_apply_cycle_ms", 0.50, want)

        # End-to-end outcome + source backlog.
        lag_series = [_skew_corr(v) for v in gauge_values(data, DA + "cdc_replication_lag_ms", want)]
        lag_max = max(lag_series) if lag_series else 0.0
        lag_last = lag_series[-1] if lag_series else 0.0
        # Lag trend (s of lag per wall-second) and applied-watermark throughput
        # (source-time advanced ÷ wall time; <1 = diverging). Both from timestamped
        # gauge series, so we can see whether lag is draining, steady, or diverging.
        lag_slope_s = 0.0
        lag_pairs = [(x["ts_ms"], x["value"]) for x in samples(data, DA + "cdc_replication_lag_ms")
                     if matches(x, want) and x.get("ts_ms", 0)]
        lag_pairs.sort()
        if len(lag_pairs) >= 2 and lag_pairs[-1][0] > lag_pairs[0][0]:
            lag_slope_s = (lag_pairs[-1][1] - lag_pairs[0][1]) / (lag_pairs[-1][0] - lag_pairs[0][0])
        # Progress ×realtime ladder (independent signal): how fast the received
        # (ingress) and applied (egress) source-time frontiers advance vs wall.
        received_ratio = gauge_rate(data, DA + "cdc_received_commit_unix_time_ms", want)
        apply_ratio = gauge_rate(data, DA + "cdc_applied_commit_unix_time_ms", want)
        wal_bytes = gauge_values(data, PG + "lag_bytes", {"name": ds})
        wal_max = max(wal_bytes) if wal_bytes else 0.0
        occ = gauge_values(data, DA + "cdc_prefetch_buffer_occupancy", want)
        cap = gauge_values(data, DA + "cdc_prefetch_buffer_capacity", want)
        cap_last = cap[-1] if cap else 0.0

        # Liveness (member_attached) + shared-slot grouping (its `slot` label). A member
        # that detached mid-window has a stale rate ladder and pins the slot's WAL — the
        # classifier treats this FIRST. Absent on pre-#member_attached dumps ⇒ assume live.
        att = gauge_values(data, PG + "member_attached", {"name": ds})
        member_attached_min = min(att) if att else 1.0
        slot_of_ds = next((x["labels"].get("slot") for x in samples(data, PG + "member_attached")
                           if x.get("labels", {}).get("name") == ds and x.get("labels", {}).get("slot")), None)
        # AUTHORITATIVE per-slot retained WAL from pg_stats (source view; does not stall
        # when the walsender is WriteData-blocked, unlike the client-view lag_bytes).
        # Also derive the authoritative accumulation slope (bytes/s) for the classifier's
        # SOURCE-throughput branch — the client-view slope flattens exactly when blocked.
        auth_wal = 0.0
        auth_slope = None
        if slot_of_ds:
            pts = sorted(
                (s["ts_ms"], (s.get("slot_retained_bytes") or {}).get(slot_of_ds))
                for s in (data.get("pg_stats") or [])
                if s.get("ts_ms") and (s.get("slot_retained_bytes") or {}).get(slot_of_ds) is not None
            )
            for _, b in pts:
                auth_wal = max(auth_wal, b)
            if len(pts) >= 2 and pts[-1][0] > pts[0][0]:
                auth_slope = (pts[-1][1] - pts[0][1]) / ((pts[-1][0] - pts[0][0]) / 1000.0)

        slot_hdr = f" slot={slot_of_ds}" if slot_of_ds else ""
        dead_hdr = "  [STREAM-DEAD: detached mid-window]" if member_attached_min == 0 else ""
        p(f"\n  dataset: {ds}   ({int(bursts)} bursts, apply cadence p50={cycle_p50:.0f}ms){slot_hdr}{dead_hdr}")

        # Ground-truth additive decomposition of end-to-end lag: what arrived
        # already-stale (source) + how long it queued (coalesce) + the write.
        p("    lag decomposition (ground truth, p50 | p99 ms):")
        p(f"      source arrival    {_fmt_arr(arrival_p50)} | {_fmt_arr(arrival_p99)}   "
          "(already stale on receipt: WAL flush + network + decode)")
        p(f"      queued/coalesce   {age_p50:>9.0f} | {age_p99:>9.0f}   "
          "(batch age: first envelope -> flush)")
        apply_p99 = hist_quantile(data, DA + "cdc_apply_burst_duration_ms", 0.99, want)
        lag_p99 = percentile(lag_series, 0.99)
        p(f"      apply/write       {burst:>9.0f} | {apply_p99:>9.0f}   "
          "(apply_burst_duration mean | p99)")
        # Prefer the AUTHORITATIVE source-side backlog (per-slot, doesn't stall when the
        # walsender is WriteData-blocked); fall back to the client-view when absent.
        backlog = auth_wal if auth_wal > 0 else wal_max
        auth_note = ""
        if auth_wal > 0:
            src = f"authoritative {fmt_bytes(auth_wal)}"
            if wal_max > 0 and auth_wal > wal_max * 3:
                src += (f" — client-view {fmt_bytes(wal_max)} understates {auth_wal / wal_max:.0f}x "
                        "⇒ walsender blocked on us")
            auth_note = f"   WAL backlog: {src}"
        else:
            auth_note = f"   WAL backlog max={fmt_bytes(wal_max)} (client-view)"
        p(f"      => replication lag  p99={lag_p99:.0f}ms max={lag_max:.0f}ms last={lag_last:.0f}ms"
          f"{auth_note}")
        trend = ("DIVERGING" if lag_slope_s > 0.1 else
                 "draining" if lag_slope_s < -0.1 else "steady")
        # Progress ladder (2nd, independent approach): source-time advanced ÷ wall at
        # ingress (received) and egress (applied). internal = applied/received =
        # fraction of received progress that makes it through our apply path.
        rx = lambda r: "?" if r is None else f"{r:.2f}x"
        internal = (apply_ratio / received_ratio) if (received_ratio and apply_ratio is not None
                                                      and received_ratio > 0) else None
        p(f"      progress ×realtime: received(ingress)={rx(received_ratio)} "
          f"applied(egress)={rx(apply_ratio)}"
          + (f" internal(applied/received)={internal:.2f}" if internal is not None else "")
          + f"; lag trend {lag_slope_s:+.2f}s/s ({trend})  [<1x / rising ⇒ falling behind]")
        # Independent rate-ladder read of where the slowdown is.
        if received_ratio is None or apply_ratio is None:
            ladder = "unknown (need ≥2 commit-ts samples)"
        elif received_ratio >= 0.9 and apply_ratio >= 0.9:
            ladder = "keeping up (~realtime end to end)"
        elif received_ratio >= 0.9 and apply_ratio < 0.9:
            ladder = "APPLY-limited (ingress ~realtime, but our apply/write is slower)"
        elif received_ratio < 0.9 and (internal is None or internal > 0.8):
            ladder = "INGRESS-limited (receiving < realtime; apply keeps up with what arrives)"
        else:
            ladder = "BOTH ingress- and apply-limited"
        p(f"      rate-ladder read: {ladder}")
        # Residual = end-to-end lag NOT explained by arrival+queue+write. A large
        # positive residual means a stage the waterfall doesn't attribute — most
        # often downstream commit/finalize/visibility, or (memory mode) checkpoint
        # gating. This is the signal that a missing stage exists. Directional
        # (gauge lag vs histogram spans), so it is labeled approximate.
        explained_p99 = arrival_p99 + age_p99 + apply_p99
        residual_p99 = lag_p99 - explained_p99
        if lag_p99 > 0:
            # Only a POSITIVE residual is interesting (a stage the waterfall doesn't
            # attribute). A ≤0 residual just means the pieces (each an independent
            # percentile) over-cover the lag — tail-percentile misalignment, not a
            # finding — so collapse it to one line instead of drawing attention.
            if residual_p99 > 0.15 * lag_p99:
                p(f"      residual (p99, approx): +{residual_p99:.0f}ms "
                  f"(+{residual_p99 / lag_p99 * 100:.0f}% of lag) — UNATTRIBUTED "
                  "(a stage not covered: downstream commit/finalize/visibility; "
                  "memory mode: see checkpoint stage)")
            else:
                p("      residual (p99): ≤0 — decomposition covers the lag")

        # Apply-loop component breakdown. NON-additive (mixes idle-waits and work,
        # and commit/finalize overlap the next burst) — see docs. Diagnostic for
        # *which apply-loop phase* dominates, not an exact latency partition.
        p("    apply-path stage means (ms/burst; NON-additive — see docs):")
        denom = max(sum(v for _, v in stage_ms), 1e-9)
        top_label, top_val = None, -1.0
        for label, v in stage_ms:
            share = v / denom
            if v > top_val:
                top_label, top_val = label, v
            p(f"      {label:<14} {v:>9.2f}  {share*100:>5.1f}%  {bar(share)}")
        p(f"      -> dominant apply-loop stage: {top_label}")

        # Evidence-printing classifier (replaces the binary verdict).
        occ_p50 = percentile(occ, 0.50)
        occ_p99 = percentile(occ, 0.99)
        fill = (occ_p99 / cap_last) if cap_last else 0.0
        wal_slope = 0.0
        if len(wal_bytes) >= 2 and window_ms > 0:
            wal_slope = (wal_bytes[-1] - wal_bytes[0]) / (window_ms / 1000.0)
        # Reader split: time blocked on the source socket (input-wait) vs decode/build
        # (processing) — the definitive source-send vs our-decode discriminator. On the
        # shared pump, member-send-wait (time blocked delivering into a slow member's
        # channel — downstream apply back-pressure) is already SUBTRACTED from
        # `reader_processing_micros_total` at the source, so `input_share` is honest:
        # a slow member no longer deflates the input-share and mislabels the slot
        # READER-decode-bound. It is exported separately as
        # `member_send_wait_micros_total` and surfaced below as its own bucket.
        in_us = sum(_delta(f, l) for f, l in
                    _pairs(data, PG + "reader_input_wait_micros_total", {"name": ds}).values())
        proc_us = sum(_delta(f, l) for f, l in
                      _pairs(data, PG + "reader_processing_micros_total", {"name": ds}).values())
        member_send_wait_us = sum(_delta(f, l) for f, l in
                      _pairs(data, PG + "member_send_wait_micros_total", {"name": ds}).values())
        reader_total = in_us + proc_us
        input_share = (in_us / reader_total) if reader_total > 0 else None
        send_wait_p99 = hist_quantile(data, DA + "cdc_reader_send_wait_ms", 0.99, want)
        if input_share is not None:
            p(f"    reader split: socket-wait {input_share * 100:.0f}% vs decode/build "
              f"{(1 - input_share) * 100:.0f}% (decode/build excludes member-send-wait "
              f"{member_send_wait_us / 1e6:.1f}s of shared-slot apply back-pressure); "
              f"reader send-wait p99={send_wait_p99:.0f}ms "
              "(socket-wait high ⇒ source; decode high ⇒ our reader; send-wait high ⇒ apply-bound)")
        # Stream health: reconnects in the window mean the connection dropped and
        # replayed (Δ of the cumulative counter over the window, summed across the
        # group's members that carry this dataset's `name` label).
        reconnects_delta = sum(
            _delta(f, l) for f, l in
            _pairs(data, PG + "reconnects_total", {"name": ds}).values()
        )
        disconnected_ms_delta = sum(
            _delta(f, l) for f, l in
            _pairs(data, PG + "disconnected_ms_total", {"name": ds}).values()
        )
        if reconnects_delta >= 1:
            downtime_pct = (disconnected_ms_delta / window_ms * 100) if window_ms else 0.0
            p(f"    stream health: {int(reconnects_delta)} reconnect(s), "
              f"disconnected {disconnected_ms_delta / 1000:.1f}s of the {window_ms / 1000:.0f}s window "
              f"({downtime_pct:.0f}% down) — replay on each resume inflates apply + resets frontiers")
        # Prefer authoritative source-side backlog + slope for the classifier (the
        # client-view flattens when the walsender is WriteData-blocked); fall back to
        # client-view when the pg_stats slot join is unavailable (older dumps).
        cls_wal_max = backlog
        cls_wal_slope = auth_slope if auth_slope is not None else wal_slope
        cls, evidence = classify_dataset(
            lag_p99, fill, arrival_p99, cls_wal_slope, cls_wal_max, top_label, memory_mode,
            residual_p99, apply_ratio, lag_slope_s, input_share, send_wait_p99,
            received_ratio, reconnects_delta, member_attached_min
        )
        low_conf = (f"  [LOW-CONFIDENCE: {len(occ)} occupancy samples < "
                    f"{LOW_CONF_MIN_SAMPLES}]" if 0 < len(occ) < LOW_CONF_MIN_SAMPLES else "")
        p(f"    classification: {cls}{low_conf}")
        p(f"      evidence: {evidence}")
        if cap_last > 0:
            p(f"      prefetch fill p50={occ_p50:.0f} p99={occ_p99:.0f} / cap={cap_last:.0f}")
        # Correlate the two independent approaches (lag/arrival/reader-split classifier
        # vs the rate ladder). Agreement raises confidence; disagreement flags a gap.
        cls_dir = ("dead" if "STREAM-DEAD" in cls else
                   "stream" if "STREAM-UNSTABLE" in cls else
                   "apply" if "APPLY" in cls else
                   "source" if ("SOURCE" in cls or "READER" in cls or "IDLE" in cls) else
                   "durability" if "DURABILITY" in cls else "?")
        ladder_dir = ("apply" if ladder.startswith("APPLY") else
                      "source" if ladder.startswith("INGRESS") else
                      "both" if ladder.startswith("BOTH") else
                      "healthy" if ladder.startswith("keeping up") else "?")
        if cls_dir in ("stream", "dead"):
            # The rate ladder is unreliable under reconnect/replay or after detach.
            agree = "n/a (stream not healthy — ladder unreliable)"
        else:
            agree = ("AGREE" if cls_dir == ladder_dir
                     or (cls_dir == "source" and ladder_dir in ("source", "healthy"))
                     else "DIFFER — investigate" if "?" not in (cls_dir, ladder_dir) else "n/a")
        p(f"      correlation: classifier→{cls_dir} vs rate-ladder→{ladder_dir}  [{agree}]")

        # Why bursts flushed (timer-bound vs cap-bound), disambiguated by occupancy:
        # the deadline is anchored at the previous apply's start, so it can fire
        # under saturation too — cross it with buffer fill.
        reasons = counter_totals_by(data, DA + "cdc_coalesce_flush_total", "reason", want)
        if reasons:
            total_flush = sum(reasons.values()) or 1.0
            reason_str = " ".join(
                f"{r}={int(c)}({c / total_flush * 100:.0f}%)"
                for r, c in sorted(reasons.items(), key=lambda kv: -kv[1])
            )
            # Mechanism text from the dominant flush reason (§C): the SAME "ingress-
            # limited" outcome has different causes, and the flush-reason shares tell
            # them apart. deadline-dominated = batches time out (linger); byte/envelope
            # cap-dominated = batches fill fast (steady/backlog volume), NOT starved.
            deadline_share = reasons.get("deadline", 0.0) / total_flush
            byte_cap_share = reasons.get("byte_cap", 0.0) / total_flush
            env_cap_share = reasons.get("envelope_cap", 0.0) / total_flush
            drained_share = reasons.get("buffer_drained", 0.0) / total_flush
            if deadline_share >= 0.5:
                note = ("  -> waits out the linger deadline under saturation (buffer full)"
                        if fill > 0.5
                        else "  -> waits out the linger deadline (batch fills slowly under a trickle)")
            elif byte_cap_share >= 0.5:
                note = "  -> fills batches to the byte cap (steady volume; not deadline-starved)"
            elif env_cap_share >= 0.5:
                note = "  -> burst-capped on envelope count (large backlog draining in max batches)"
            elif drained_share >= 0.5:
                note = "  -> flushing on buffer-drained (reader outrunning volume; low pressure)"
            else:
                note = "  -> mixed flush reasons (no single dominant trigger)"
            p(f"    flush reasons: {reason_str}{note}")

        # Apply-path mix: which path the sub-batches took. durable_* = the expensive
        # synchronous whole-burst commit+maintenance (e.g. delete bursts that clear
        # the slot-advancer), the usual reason a table's apply time balloons.
        paths = counter_totals_by(data, DA + "cdc_apply_path_total", "path", want)
        if paths:
            tot = sum(paths.values()) or 1.0
            p("    apply-path mix: " + " ".join(
                f"{k}={int(v)}({v / tot * 100:.0f}%)"
                for k, v in sorted(paths.items(), key=lambda kv: -kv[1])))
        # Durable-delete decomposition (present only when a dataset hit the durable
        # delete fallback): where that path's time went — lock contention, the delete
        # itself, or the synchronous post-write maintenance/compaction trigger.
        dd = [(lbl, hist_mean(data, DA + "cdc_apply_fixed_cost_ms", {**want, "phase": ph}))
              for lbl, ph in (("lock_wait", "durable_delete_lock_wait"),
                              ("apply", "durable_delete_apply"),
                              ("maintenance", "durable_delete_maintenance"))]
        if any(v > 0 for _, v in dd):
            p("    durable-delete breakdown (mean ms/invocation): "
              + " ".join(f"{lbl}={v:.0f}" for lbl, v in dd))

    # ---- Cayenne write-phase breakdown (per table, mean ms) ----
    tables = label_values(data, CY + "write_phase_duration_ms_count", "table")
    phases_seen = label_values(data, CY + "write_phase_duration_ms_count", "phase")
    if tables and phases_seen:
        p("\nCayenne write-phase breakdown (mean ms per phase invocation)")
        p("-" * 60)
        # Stable, meaningful phase ordering; unknown phases appended.
        preferred = ["encode_permit_wait", "vortex_write", "inmemory_spill",
                     "inmemory_budget_wait", "publish"]
        ordered = [ph for ph in preferred if ph in phases_seen] + \
                  [ph for ph in phases_seen if ph not in preferred]
        header = "    {:<20} ".format("table") + " ".join(f"{ph[:14]:>15}" for ph in ordered)
        p(header)
        for t in tables:
            row = [f"    {t:<20} "]
            for ph in ordered:
                m = hist_mean(data, CY + "write_phase_duration_ms",
                              {"table": t, "phase": ph})
                row.append(f"{m:>15.2f}")
            p(" ".join(row))

        # Phase coverage — two levels localize a blind spot (<85% ⇒ gap):
        #   write/burst   = apply-loop 'write' stage ÷ apply-burst wall
        #                   (low ⇒ gap is apply-loop framing: coalesce/linger/commit)
        #   cayenne/write = Σ(BURST-scoped outer write-path phases) ÷ 'write' stage
        #                   (low ⇒ an un-instrumented span INSIDE the write call)
        # CRITICAL scoping rule: the numerator sums ONLY the mutually-exclusive OUTER
        # burst-path brackets — every `cdc_path_*` phase (inmemory / inmemory_sharded /
        # inlined / staged / fallback), exactly one of which brackets each burst's whole
        # cayenne write. It must NOT sum every write_phase: the inner sub-phases NEST
        # inside the outer bracket (double-count) and BACKGROUND-scoped phases
        # (mem_tier_checkpoint*, off-burst fence work, deferred tombstone flips during
        # checkpoint) are not burst work at all — summing all of them pushed the ratio to
        # 270–460% and false-flagged every table. Prefix-matching `cdc_path_` auto-covers
        # any new outer variant; a ratio >100% then means a genuine scoping error.
        outer_phases = [ph for ph in phases_seen if ph.startswith("cdc_path_")]
        p("")
        p("    phase coverage — two levels localize any blind spot (<85% ⇒ gap):")
        p("      write/burst   = apply-loop 'write' stage ÷ apply-burst wall")
        p("      cayenne/write = Σ outer cdc_path_* phase ÷ 'write' stage")
        for t in tables:
            outer_sum = sum(
                hist_mean(data, CY + "write_phase_duration_ms", {"table": t, "phase": ph})
                * hist_count(data, CY + "write_phase_duration_ms", {"table": t, "phase": ph})
                for ph in outer_phases
            )
            burst_sum = (hist_mean(data, DA + "cdc_apply_burst_duration_ms", {"dataset": t})
                         * hist_count(data, DA + "cdc_apply_burst_duration_ms", {"dataset": t}))
            if burst_sum <= 0:
                continue  # static/full-refresh table: no CDC apply bursts
            # 'write' apply-loop stage total (the cayenne write call wall time).
            write_sum = (hist_mean(data, DA + "cdc_apply_fixed_cost_ms", {"dataset": t, "phase": "write"})
                         * hist_count(data, DA + "cdc_apply_fixed_cost_ms", {"dataset": t, "phase": "write"}))
            write_burst = write_sum / burst_sum if burst_sum else 0.0
            cay_write = (outer_sum / write_sum) if write_sum > 0 else 0.0
            if cay_write > 1.05:
                tag = "  <<< SCOPING ERROR (>100%: an outer write-path phase is unlisted)"
            elif write_burst < 0.85:
                tag = "  <<< gap in apply-loop framing (coalesce/linger/commit)"
            elif cay_write < 0.85:
                tag = "  <<< BLIND SPOT: un-instrumented span inside the write call"
            else:
                tag = ""
            p(f"      {t:<20} write/burst={write_burst * 100:>5.1f}%  "
              f"cayenne/write={cay_write * 100:>5.1f}%  (burst {burst_sum / 1000:.0f}s){tag}")

    # ---- Process-global backpressure valves ----
    p("\nProcess-global backpressure valves (SHARED across ALL datasets)")
    p("  note: these are process-wide; a dataset's attributed wait is an UPPER BOUND —")
    p("  a noisy neighbor's contention lands here and in that dataset's write phase.")
    p("-" * 60)

    enc_avail = gauge_values(data, CY + "encode_permits_available")
    enc_total = gauge_values(data, CY + "encode_permits_total")
    if enc_avail or enc_total:
        total = enc_total[-1] if enc_total else 0.0
        p(f"  Encode budget: permits available min={min(enc_avail) if enc_avail else 0:.0f} "
          f"p50={percentile(enc_avail, 0.5):.0f} of total={total:.0f}  "
          f"(0 => encode-semaphore stall)")
    if samples(data, CY + "encode_acquire_wait_ms_bucket"):
        p(f"  Encode acquire wait: p90={hist_quantile(data, CY + 'encode_acquire_wait_ms', 0.9):.1f}ms "
          f"p99={hist_quantile(data, CY + 'encode_acquire_wait_ms', 0.99):.1f}ms "
          f"(delta p99={hist_quantile(data, CY + 'encode_acquire_wait_ms', 0.99, {'class': 'delta'}):.1f}ms)")

    comp_avail = gauge_values(data, CY + "compaction_permits_available")
    comp_total = gauge_values(data, CY + "compaction_permits_total")
    if comp_avail or comp_total:
        total = comp_total[-1] if comp_total else 0.0
        p(f"  Compaction semaphore: permits available min={min(comp_avail) if comp_avail else 0:.0f} "
          f"of total={total:.0f}")
    if samples(data, CY + "compaction_acquire_wait_ms_bucket"):
        p(f"  Compaction acquire wait: p90={hist_quantile(data, CY + 'compaction_acquire_wait_ms', 0.9):.1f}ms "
          f"p99={hist_quantile(data, CY + 'compaction_acquire_wait_ms', 0.99):.1f}ms")

    mt_used = gauge_values(data, CY + "mem_tier_budget_used_bytes")
    mt_total = gauge_values(data, CY + "mem_tier_budget_total_bytes")
    if mt_used or mt_total:
        used_max = max(mt_used) if mt_used else 0.0
        total = mt_total[-1] if mt_total else 0.0
        frac = used_max / total if total else 0.0
        refused = sum(counter_totals_by(data, CY + "mem_tier_reserve_refused_total", "-").values())
        p(f"  Mem-tier byte budget: used max={fmt_bytes(used_max)} of {fmt_bytes(total)} "
          f"({frac*100:.0f}%); reserve refusals (spill/fallback)={int(refused)}")
    if samples(data, CY + "mem_tier_acquire_wait_ms_bucket"):
        p(f"  Mem-tier acquire wait: p90={hist_quantile(data, CY + 'mem_tier_acquire_wait_ms', 0.9):.1f}ms "
          f"p99={hist_quantile(data, CY + 'mem_tier_acquire_wait_ms', 0.99):.1f}ms")

    # ---- Durability / checkpoint stage (memory mode only) ----
    # In memory mode the source-slot ack (hence WAL recycle) is gated by mem-tier
    # checkpoint cadence, which the apply-path stages don't cover — a large
    # positive residual (above) usually lands here.
    if memory_mode:
        p("\nDurability / checkpoint stage (cdc_durability: memory)")
        p("-" * 60)
        have_ckpt = bool(samples(data, CY + "mem_tier_checkpoint_tick_total")) or \
            bool(samples(data, CY + "mem_tier_apply_epoch"))
        if not have_ckpt:
            p("  INCOMPLETE CONTRACT: memory mode detected but checkpoint metrics "
              "(mem_tier_checkpoint_tick_total / mem_tier_apply_epoch) are absent —")
            p("  NOT emitting a durability verdict. A stalled checkpoint would "
              "otherwise misread as source-bound. Export the checkpoint series.")
        else:
            ticks = counter_totals_by(data, CY + "mem_tier_checkpoint_tick_total", "outcome")
            if ticks:
                tick_str = " ".join(f"{o}={int(c)}" for o, c in
                                    sorted(ticks.items(), key=lambda kv: -kv[1]))
                p(f"  checkpoint ticks (Δ over window): {tick_str}")
            # Slot-advance lag = apply_epoch − durable_epoch (per table); a growing
            # gap = the watermark is stuck (WAL can't recycle).
            for t in label_values(data, CY + "mem_tier_apply_epoch", "table"):
                ae = gauge_values(data, CY + "mem_tier_apply_epoch", {"table": t})
                de = gauge_values(data, CY + "mem_tier_durable_epoch", {"table": t})
                if ae and de:
                    gap = (ae[-1] or 0) - (de[-1] or 0)
                    p(f"    {t:<20} slot-advance gap (apply−durable epoch)={gap:.0f}")

    # ---- Source-side Postgres (pg_stats scraper) — the "produced" rung ----
    # Independent, source-authored view: WAL-production/commit rate, walsender wait
    # mix (idle-waiting-for-WAL vs busy vs client-write), and OLTP lock contention.
    pg = sorted((data.get("pg_stats") or []), key=lambda x: x.get("ts_ms", 0))
    if len(pg) >= 2 and (pg[-1].get("ts_ms", 0) - pg[0].get("ts_ms", 0)) > 0:
        import collections
        p("\nSource-side Postgres (pg_stats)")
        p("-" * 60)
        dt = (pg[-1]["ts_ms"] - pg[0]["ts_ms"]) / 1000.0
        # Cumulative counters; clamp negative deltas to 0 (a pg_stat_reset() mid-run
        # would otherwise yield a nonsense negative rate).
        rate = lambda f: max(0.0, (pg[-1].get(f, 0) - pg[0].get(f, 0))) / dt
        wal_rate, rec_rate, xact_rate = rate("wal_bytes"), rate("wal_records"), rate("xact_commit")
        p(f"  source production: {fmt_bytes(wal_rate)}/s WAL, {rec_rate:.0f} records/s, "
          f"{xact_rate:.0f} txn/s  (independent 'produced' rate)")
        # AUTHORITATIVE retained WAL from pg_replication_slots (source view). Unlike the
        # client-view lag_bytes (server_wal_end − confirmed_flush, which freezes when the
        # walsender is WriteData-blocked), this keeps advancing with the source WAL head —
        # it is the truth for drain/caught-up. A large gap between the two = the sender is
        # blocked on us (client-view badly understates the real backlog).
        retained_max = {}
        for s in pg:
            for slot, b in (s.get("slot_retained_bytes") or {}).items():
                retained_max[slot] = max(retained_max.get(slot, 0), b)
        if retained_max:
            auth_total = sum(retained_max.values())
            client_total = sum(
                max(gauge_values(data, PG + "lag_bytes", {"name": ds}) or [0])
                for ds in datasets
            )
            p("  authoritative retained WAL (pg_replication_slots, max/window): "
              + " ".join(f"{sl}={fmt_bytes(b)}" for sl, b in sorted(retained_max.items())))
            if client_total > 0 and auth_total > client_total * 3:
                p(f"  -> AUTHORITATIVE total {fmt_bytes(auth_total)} vs client-view "
                  f"{fmt_bytes(client_total)} ({auth_total / client_total:.0f}x): the walsender is "
                  "blocked writing to us — client-view lag_bytes badly understates the real backlog.")
        ws, ab = collections.Counter(), collections.Counter()
        nws = []
        for s in pg:
            for k, v in (s.get("walsender_waits") or {}).items():
                ws[k] += v
            for k, v in (s.get("active_backend_waits") or {}).items():
                ab[k] += v
            nws.append(s.get("walsenders", 0))
        tot = sum(ws.values()) or 1
        p(f"  walsenders (max {int(max(nws)) if nws else 0}) wait mix: "
          + " ".join(f"{k}={v}({v * 100 // tot}%)" for k, v in ws.most_common()))
        if ab:
            p("  active-backend waits (top): "
              + " ".join(f"{k}={v}" for k, v in ab.most_common(6)))
        idle = ws.get("WalSenderWaitForWAL", 0) / tot
        # `WalSenderWriteData` (PG16+) / `ClientWrite` = the walsender is blocked WRITING
        # to us ⇒ WE are the constraint (not draining fast enough), independent of the
        # reader-side attribution (which the shared path muddies — see the reader-split
        # caveat). This is direct source-side evidence of a client bottleneck.
        write_blocked = (ws.get("WalSenderWriteData", 0) + ws.get("ClientWrite", 0)) / tot
        if idle > 0.5:
            p("  -> walsenders mostly idle-waiting-for-WAL: source WAL production is the "
              "limit (see active-backend lock waits / box load), NOT our reader/decode.")
        elif write_blocked > 0.3:
            p(f"  -> walsenders {write_blocked * 100:.0f}% blocked writing to us "
              "(WalSenderWriteData/ClientWrite): WE aren't draining fast enough — the CLIENT "
              "is the constraint (source-side proof, independent of the reader split).")
        else:
            p("  -> walsenders busy (decode/send): source is producing and shipping "
              "(compare with received/applied rate ladder).")

    # ---- Query-side outcome: read amplification (SEPARATE from ingest lag) ----
    # Compaction starvation shows up as read-amp (query staleness), which is a
    # distinct outcome from CDC ingest lag — keep them separate.
    if samples(data, CY + "ingest_read_amp"):
        p("\nQuery-side read amplification (compaction health; distinct from ingest lag)")
        p("-" * 60)
        for t in label_values(data, CY + "ingest_read_amp", "table") or \
                 label_values(data, CY + "ingest_read_amp", "name"):
            vals = gauge_values(data, CY + "ingest_read_amp", {"table": t}) or \
                   gauge_values(data, CY + "ingest_read_amp", {"name": t})
            if vals:
                p(f"    {t:<20} p99={percentile(vals, 0.99):.0f}  max={max(vals):.0f}")

    # ---- Estimator provenance (§D.3) ----
    # One machine-readable dict recording the window bounds, how slopes were fit, and
    # which estimator produced each percentile/rate — so a reviewer can reconcile two
    # reports without re-deriving the windowing/estimator choices from the numbers.
    p("\nEstimator provenance (windows + estimators; for reconciling reruns)")
    p("-" * 60)
    provenance = {
        "window": {
            "start_ts_ms": _STATE.get("window_start_ms"),
            "end_ts_ms": _STATE.get("window_end_ms"),
            "window_ms": _STATE.get("window_ms"),
            "basis": "min/max per-scrape ts over all timestamped series; "
                     "cumulative stats are Δ(last−first) over this window (bootstrap excluded)",
        },
        "lag_slope": "2-point secant of the cdc_replication_lag_ms gauge "
                     "(last−first sample)/(Δt) over the window; s per wall-second",
        "percentiles": {
            "arrival / coalesce_age / apply_burst": "histogram-bucket quantile "
                "(hist_quantile) over the FINAL cumulative buckets",
            "replication_lag p99 / max": "empirical percentile over the "
                "cdc_replication_lag_ms gauge sample series in-window",
            "received_rate / applied_rate": "2-point gauge secant of the commit-ts "
                "frontier (last−first) ÷ wall time",
        },
        "clock_skew_ms_estimate": run.get("clock_skew_ms_estimate"),
        "clock_skew_applied": run.get("clock_skew_ms_estimate") is not None,
    }
    p("  " + json.dumps(provenance, separators=(",", ": ")))

    p("")
    return "\n".join(out)


def load_source(path):
    """Load a normalized {run, samples} document. Currently only the testoperator
    `--metrics-dump` JSON; a future `--source` flag will add prom-text / PromQL /
    OTLP adapters that produce the same shape (see REQUIRED_METRICS)."""
    return load(path)


def print_required_metrics():
    """Print the exact series the analysis needs — the 'what to export' list for
    running this against a Prometheus/Grafana/OTLP source."""
    print("Metrics required by the CDC backpressure waterfall")
    print("(gauge => needs a time series/range; histogram/counter => final cumulative is enough)\n")
    width = max(len(m[0]) for m in REQUIRED_METRICS)
    for name, kind, labels in REQUIRED_METRICS:
        print(f"  {name:<{width}}  {kind:<9}  labels: {labels}")


def main(argv):
    ap = argparse.ArgumentParser(description="CDC backpressure waterfall for CH-BenCHmark runs.")
    ap.add_argument("metrics_json", nargs="?",
                    help="Path to the testoperator --metrics-dump JSON file.")
    ap.add_argument("--list-metrics", action="store_true",
                    help="Print the metric series the analysis requires, then exit.")
    args = ap.parse_args(argv)

    if args.list_metrics:
        print_required_metrics()
        return 0
    if not args.metrics_json:
        ap.error("metrics_json is required (or pass --list-metrics)")

    try:
        data = load_source(args.metrics_json)
    except (OSError, json.JSONDecodeError) as e:
        print(f"error: could not read metrics dump {args.metrics_json}: {e}", file=sys.stderr)
        return 2

    print(render(data))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
