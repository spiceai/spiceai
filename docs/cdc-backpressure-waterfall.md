# CDC Backpressure Waterfall — Methodology & Metric Attribution

This document explains **how `scripts/chbench-waterfall.py` attributes CDC replication
lag/backpressure to each stage of the pipeline, and exactly why each metric means what the
report claims it means.** It exists so the report can be *audited* — every number below is
traceable to a specific instrument and a specific line of code, and every attribution rule
states what is exact vs. approximate.

The analysis depends only on standard spiced metric series (all exposed at Prometheus
`/metrics` and via OTLP). Run `chbench-waterfall.py --list-metrics` for the exact contract.

---

## 1. The pipeline being measured

A committed transaction on the source Postgres becomes queryable in the Cayenne accelerator
by flowing through these stages:

```
[Postgres commit]
   │  WAL                                             dataset_postgres_replication_lag_bytes  (gauge)
   ▼
[Source reader task] decode WAL → ChangeEnvelope
   │  bounded prefetch channel (cdc_prefetch_buffer)  dataset_acceleration_cdc_prefetch_buffer_occupancy/_capacity (gauge)
   ▼                                                  dataset_acceleration_cdc_source_recv_wait_ms  (hist)
[CDC apply loop]  (refresh_task/changes.rs)
   ├─ Phase 1 coalesce (non-blocking drain)           cdc_apply_fixed_cost_ms{phase=coalesce}  (hist)
   ├─ Phase 2 linger  (cdc_max_coalesce_age_ms)       cdc_linger_wait_ms  (hist)
   │                                                   cdc_coalesce_batch_age_ms / cdc_coalesce_flush_total{reason}
   ├─ apply burst → CayenneCdcWrite                    cdc_apply_burst_duration_ms  (hist)
   │    ├─ encode-permit wait                          cayenne_write_phase_duration_ms{phase=encode_permit_wait} (hist)
   │    │                                              cayenne_encode_acquire_wait_ms{class}, cayenne_encode_permits_available
   │    ├─ encode (vortex_write)                       cayenne_write_phase_duration_ms{phase=vortex_write}
   │    └─ mem-tier reserve (memory durability)        cayenne_mem_tier_budget_used_bytes / _total_bytes
   └─ ordered source-offset commit / finalize         cdc_apply_fixed_cost_ms{phase=commit_wait|finalize_wait}
[background compaction] (async, off the apply path)    cayenne_compaction_acquire_wait_ms, cayenne_compaction_permits_available, cayenne_ingest_read_amp
   ▼
[queryable]                                            dataset_acceleration_cdc_replication_lag_ms  (gauge, end-to-end outcome)
```

`cdc_replication_lag_ms` is the **outcome** (wall-clock now − upstream commit ts of the latest
applied burst). The rest of the report explains *where that lag comes from*.

---

## 2. How each number is computed

The report reduces raw series with three operations. Understanding them is required to audit
the output:

- **Histogram mean** = `Σ(_sum) / Σ(_count)` over the final cumulative samples (all matching
  label sets). This is the *average per-observation* value (e.g. mean ms per burst). Used for
  the per-stage "mean_ms" columns.
- **Histogram quantile** (`p90`/`p99`) = standard Prometheus `histogram_quantile` over the
  final cumulative `_bucket` series (buckets summed across matching label sets). Used for
  acquire-wait and batch-age percentiles.
- **Gauge percentile / min / max / last** = computed over the *time series* of scraped gauge
  samples. Used for occupancy, permits-available, mem-tier, and lag.

**Windowing (important).** Cumulative series are reduced in the dump to their **first and last**
snapshot (each carrying `ts_ms`), and every histogram/counter stat is a **Δ across that window**
(`ΔΣ_sum / ΔΣ_count`, quantiles over `Δ_bucket`, counter rates `Δ_count / Δt`) — *not* a
lifetime total. This keeps histograms on the same time base as gauges and, critically, makes the
whole-run window **exclude the bootstrap/initial-snapshot writes** baked into the first snapshot
(a 30 s stall in an hour-long run is invisible in a lifetime p99). Gauges keep their full series
over the window. The tool **requires** ≥2 timestamped cumulative snapshots and **bails** if they
are absent (it does not support lifetime-only / single-snapshot dumps).

Because a single instant read can only ever yield a lifetime histogram, a non-dump source (§7)
must provide a range (two snapshots) for cumulative series.

---

## 3. Stage-by-stage attribution

For each stage: the metric, its precise definition, the emit site, and what a high/low value
means. Instrument definitions live in
[`crates/runtime/src/accelerated_table/metrics.rs`](../crates/runtime/src/accelerated_table/metrics.rs)
(runtime `dataset_acceleration` meter) and
[`crates/telemetry/src/lib.rs`](../crates/telemetry/src/lib.rs) (cayenne `cayenne` meter);
emit sites in [`refresh_task/changes.rs`](../crates/runtime/src/accelerated_table/refresh_task/changes.rs)
and [`crates/cayenne/src/provider/`](../crates/cayenne/src/provider/).

### Stage 0 — Postgres → WAL backlog
- **`dataset_postgres_replication_lag_bytes`** (gauge, label `name`) = `server_wal_end_lsn −
  confirmed_flush_lsn`: WAL the source has produced but the accelerator has not yet
  acknowledged. **Meaning:** the un-drained backlog at the source. Growing = the whole
  downstream can't keep up; small = downstream is keeping pace (any lag is elsewhere).

### Stage 1 — Source decode/deliver
- **`cdc_source_recv_wait_ms`** (hist) = time the apply loop blocked on `rx.recv()` waiting
  for the reader to deliver the next envelope. **Meaning:** the discriminator between
  *source-bound* and *apply-bound*. High recv_wait ⇒ the reader (WAL decode / network)
  can't deliver fast enough (or the source is idle); near-zero ⇒ the apply side is the
  bottleneck. It is an **idle-wait**, not work.
- **`cdc_source_arrival_lag_ms`** (hist) = `now − source_commit_ts` measured **when the first
  envelope of a burst is received**. **Meaning:** lag that is *already present before the
  accelerator does anything* — PG WAL flush + network + logical decode + reader delivery.
  This is the metric that makes "source-bound vs apply-bound" *quantitative* rather than
  inferred: high arrival lag ⇒ the source genuinely can't keep up (distinct from an idle
  source, where recv_wait is high but arrival lag is ~0). It is the first, additive term of
  the lag decomposition (see §4).
- **Reader split — the source-send vs our-decode discriminator.** When the pipeline is
  reader-limited (buffer starved, accelerator idle, WAL *bytes* flat but lag rising in
  *time*), these tell you *why the reader is slow*:
  - `replication_reader_input_wait_micros_total` (counter) — reader time BLOCKED awaiting the
    next event from the PG socket. High share ⇒ **source/network/PG can't deliver** (source-bound).
  - `replication_reader_processing_micros_total` (counter) — reader time decoding + building
    batches. High share ⇒ **our decode/build is the limiter** (reader CPU/scheduling bound).
  - `cdc_reader_send_wait_ms` (hist) — reader blocked pushing into the prefetch channel ⇒
    the channel is full and the **apply/write path** can't drain (apply-bound).
  Together with `confirmed_flush_lsn` / `server_wal_end_lsn` (now exported) these turn the
  formerly-ambiguous "source-bound" verdict into `SOURCE-send-bound` vs `READER-decode-bound`
  vs `APPLY-bound` — see the classifier in §4.

### Stage 2 — Prefetch channel occupancy
- **`cdc_prefetch_buffer_occupancy` / `_capacity`** (gauge) = buffered items in the
  reader→apply channel when the apply loop last woke (`max_capacity − capacity`), sampled at
  [changes.rs](../crates/runtime/src/accelerated_table/refresh_task/changes.rs) right after
  the recv. **Meaning:** the definitive apply-bound signal. Pinned near capacity ⇒ the reader
  is outrunning the apply loop (apply-bound; Cayenne write is the bottleneck). Near zero ⇒ the
  loop drains faster than the source fills (source-bound / keeping up). The report's
  "APPLY-bound vs source-bound" verdict uses `p99(occupancy) / capacity > 0.5`.

### Stage 3 — Coalesce / linger  ← the freshness-vs-efficiency trade
- **`cdc_apply_fixed_cost_ms{phase=coalesce}`** (hist) = Phase-1 non-blocking drain time
  (usually tiny).
- **`cdc_linger_wait_ms`** (hist) = time in the Phase-2 linger window, where the loop
  *deliberately* awaits more envelopes until the envelope cap (`cdc_max_coalesced_envelopes`),
  byte cap (`cdc_max_coalesced_bytes`), or the time deadline (`cdc_max_coalesce_age_ms`,
  anchored at the *previous* apply's start) is hit. **Meaning:** intentional batching latency,
  traded for fewer/larger writes. An **idle-wait**. Recorded only when linger is enabled.
- **`cdc_coalesce_batch_age_ms`** (hist) — **the authoritative queued-time metric.** Time from
  receiving the *first* envelope of a burst until the burst is flushed (Phase-1 + Phase-2).
  This is the cleanest single measure of "how long the head-of-batch change sat queued before
  the write began," and should be preferred over summing recv_wait+linger. Pair with
  `cdc_replication_lag_ms` to attribute lag to coalescing.
- **`cdc_coalesce_flush_total{reason}`** (counter) — bursts by what ended coalescing:
  `deadline` (timer fired — batch not filled), `envelope_cap` / `byte_cap` (batch filled
  early), `buffer_drained` (linger disabled / nothing to wait for), `channel_closed`,
  `shutdown`. **Meaning:** a high `deadline` share ⇒ coalescing is *timer-bound* (low source
  volume; the linger is adding latency without filling batches). A high cap share ⇒ the write
  path (not the timer) paces the loop, so the batching is doing real work.

### Stage 4 — Cayenne write
- **`cdc_apply_burst_duration_ms`** (hist) = wall-clock to apply one coalesced burst (the
  actual write work). This is the ground-truth "apply cost" and is printed for cross-check.
- Broken down inside Cayenne via **`cayenne_write_phase_duration_ms{table,phase}`**:
  - `phase=encode_permit_wait` — time blocked acquiring encode-concurrency permits
    (backpressure from the process-global encode budget).
  - `phase=vortex_write` — the actual encode.
  - other phases (`publish`, `cdc_path_*`, `stage_*`, `inmemory_*`) — the remaining write
    steps.
- Also **`cayenne_encode_acquire_wait_ms{class}`** (hist) and **`cayenne_encode_permits_available/total`**
  (gauge) — see §5.

### Stage 5 — Commit / finalize
- **`cdc_apply_fixed_cost_ms{phase=commit_wait|finalize_wait}`** (hist) = draining the
  previous burst's ordered source-offset commit / deferred finalize. **Meaning:** source-side
  ack round-trips (PG standby status update) and metastore finalize. **These overlap the next
  burst's apply** (they are pipelined), so they do *not* add to serial per-burst latency —
  see the caveat in §4.

### Stage 6 — Background compaction (async)
- **`cayenne_compaction_acquire_wait_ms{table}`** (hist) + **`cayenne_compaction_permits_available/total`**
  (gauge) — waiting for a slot in the fleet-wide compaction semaphore. **Meaning:** high wait ⇒
  compaction is starved (peers saturate the pool), letting the protected set and read-amp grow.
- **`cayenne_ingest_read_amp{table}`** (gauge) — files a scan must merge; the query-side
  symptom of compaction falling behind. Compaction runs *off* the apply path, so it affects
  freshness only indirectly (by contending for the shared encode budget / CPU).

---

### Ground-truth decomposition, residual, and the classifier

The report leads with an **additive, ground-truth** lag decomposition — each term an
independently-measured metric, not a derived sum:

```
replication lag  ≈  source arrival (cdc_source_arrival_lag_ms)      [source-side: WAL flush+network+decode]
                 +  queued/coalesce (cdc_coalesce_batch_age_ms)     [first envelope -> flush]
                 +  apply/write (cdc_apply_burst_duration_ms)       [the accelerator write]
                 +  residual                                        [everything else]
```

- **Residual** = `lag_p99 − (arrival_p99 + batch_age_p99 + apply_burst_p99)`. It is a first-class
  output, not a disclaimer: a large positive residual means a stage the waterfall does **not**
  attribute — most often downstream commit/finalize/visibility, or (memory mode) mem-tier
  **checkpoint gating**. It is directional (a gauge lag vs. histogram spans), so it is labeled
  approximate. This is the signal that surfaces the checkpoint stage on a memory-mode run.
- **`cdc_apply_cycle_ms`** (the apply cadence) ground-truths the *non-additive* stage-means table:
  where the stage sum exceeds the cadence, phases are overlapping (pipelined commit/finalize).
- **Evidence-printing classifier** replaces the old binary verdict. It buckets each dataset —
  `healthy`, `APPLY-bound`, `SOURCE-bound`, `DURABILITY/checkpoint-bound`, `IDLE`, or `UNCLEAR` —
  and **prints the deciding inputs** (`lag_p99`, buffer fill, `arrival_p99`, WAL-backlog slope,
  dominant stage, residual, memory-mode flag) so the label is auditable, not a black box.

## 4. How to read the report (interpretation rules & honesty about limits)

1. **Wait vs. work.** `recv_wait` and `linger` are *idle waits*; `coalesce`/`write`/
   `commit_wait`/`finalize_wait` and `apply_burst_duration` are *measured work spans*. The
   per-dataset "share" mixes them, so a large `linger`/`recv_wait` share means the loop is
   *waiting*, not that Cayenne is slow.
2. **The apply-path stage-means table is NON-additive** — do not sum it. It mixes idle-waits
   with work, and `commit_wait`/`finalize_wait` are **pipelined** (overlap the next burst). Use
   it only to see *which apply-loop phase dominates*. For real latency use the **ground-truth
   decomposition** (arrival + batch_age + apply_burst) and the **residual**; use
   **`cdc_apply_cycle_ms`** as the true cadence to detect where the stage sum overstates the
   cycle (that gap = overlap).
3. **apply-bound vs source-bound** is decided by *prefetch occupancy* + *recv_wait*, not by
   the lag number: near-full buffer + low recv_wait ⇒ apply-bound; near-empty buffer + high
   recv_wait ⇒ source-bound.
4. **Lag with an idle apply loop ⇒ look upstream or at linger.** If lag is high but the apply
   loop is dominated by waits and the buffer is near-empty, the constraint is source delivery
   and/or the linger policy — not the Cayenne write valves.

---

## 5. Process-global backpressure valves

Some valves are process-global singletons and therefore **cannot** carry a per-dataset label
on their occupancy gauges (one number for the whole process):

- **Encode budget** — `cayenne_encode_permits_available/total`. **Subtlety worth auditing:**
  a write acquires `min(shards, cap)` permits *atomically*, so a multi-shard write can block
  even when some permits are free. Therefore `permits_available` rarely reaching 0 does **not**
  mean "no contention" — read `cayenne_encode_acquire_wait_ms` (and the `encode_permit_wait`
  write phase) for the real wait. (In the SF-100 local run, permits-available stayed ≥5/14 yet
  acquire-wait p99 was multiple seconds — exactly this effect.)
- **Mem-tier byte budget** — `cayenne_mem_tier_budget_used/total_bytes` (occupancy), plus
  **`cayenne_mem_tier_acquire_wait_ms{table}`** (the wait for budget to free — the valve-wait,
  analogous to encode acquire-wait) and **`cayenne_mem_tier_reserve_refused_total`** (refusals
  that forced a spill/durable-fallback). Only meaningful under `cdc_durability: memory`; in
  `file` mode occupancy stays 0 (expected).
- **Compaction semaphore** — per-accelerator; published from the real table-registration path.

**These valves are SHARED across all datasets.** A dataset's attributed wait (e.g. its
`encode_permit_wait` write phase) is therefore an **upper bound** — a noisy neighbor's contention
lands in the shared valve *and* in the studied dataset's write phase. The report labels the valve
section accordingly.

## 5a. Durability / checkpoint stage (memory mode)

Under `cdc_durability: memory`, the source-slot ack — hence WAL recycle — is gated by mem-tier
**checkpoint cadence**, a stage the apply-path metrics don't cover (it's where the residual in §4
usually lands). The report consumes the existing checkpoint signals:
- **`cayenne_mem_tier_checkpoint_tick_total{outcome}`** — checkpoint ticks by outcome (`fired` /
  `skipped_*` / `failed`). A flat-zero `fired` under a rising backlog localizes a slot-ack stall.
- **`cayenne_mem_tier_apply_epoch` − `cayenne_mem_tier_durable_epoch`** — the slot-advance gap; a
  growing gap = the durable watermark is stuck (WAL can't recycle).

**Guardrail:** if the dump is memory mode but these checkpoint series are absent, the tool
**refuses to emit a durability verdict** and says the contract is incomplete — a stalled
checkpoint must not read out as "source-bound."

## 5b. Guardrails that keep individual numbers honest
- **Tail stats for work phases.** `apply/write` shows mean **and p99**; lag is tail-driven, so a
  means-only view hides the stall that caused it. Idle waits stay mean-only.
- **Gauge low-confidence.** Occupancy is event-sampled at each `recv`, so it *undersamples
  exactly when the apply loop is slow* — the interesting case. If a gauge has fewer than
  `LOW_CONF_MIN_SAMPLES` in the window, the line is flagged low-confidence.
- **Deadline-flush disambiguation.** Because the linger deadline is anchored at the previous
  apply's start, `reason=deadline` fires under saturation too; the report crosses the deadline
  share with buffer fill to print the resolved reading (timer-bound/low-volume vs. saturated).
- **Clock skew.** `*_lag_ms = local_now − upstream_commit_ts` across hosts, so skew biases
  arrival/replication lag; the report prints an uncorrected-skew caveat (estimation is a
  follow-up — see the plan).

---

## 6. Computational efficiency

The analysis is cheap. Per run it is a handful of linear passes over the (reduced) sample set:
histogram quantiles are `O(buckets)` per series (~17 buckets × ~150 series), gauge stats are
`O(window samples)` with a sort. Total work is milliseconds; the only historically large cost
was `json.load` of the *full* dump (hundreds of MB), which is why `write_metrics_dump` now
collapses cumulative series to their final sample and keeps only gauges as full series (a
~20–50× size reduction). Nothing in the computation is superlinear or stateful.

This matters for §7: the *compute* is trivial to run anywhere (including inside spiced); the
only non-trivial requirement is **retaining a window of gauge samples**, which spiced does not
do today (observable gauges are computed at scrape time and not stored).

---

## 7. Portability & the planned "diagnostic tool"

The analyzer reads only the series in `--list-metrics` plus optional, display-only run
metadata. It has **no dependency on anything specific to the testoperator dump**, so the same
analyzer can later be fed by other sources (all normalized to `{name, labels, value, type}`):

- **offline Prometheus scrape** (`curl /metrics`, one or more snapshots),
- **live PromQL** (Prometheus/Mimir, or Grafana via its datasource proxy),
- **OTLP JSON export**.

Two portability rules a future adapter must honor: gauges need a **range** (time series), not a
single instant read; histograms/counters need only the **final cumulative**. A configurable
spiced `metric_prefix` would prefix every name, so an adapter should accept a prefix override.
(These adapters are a planned improvement; not yet implemented.)
