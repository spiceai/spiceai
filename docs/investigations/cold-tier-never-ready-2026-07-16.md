# Investigation: Cayenne cold-tier chbench "spiced never becomes ready"

**Status:** root cause LOCALIZED, not yet 100% classified (stuck-vs-slow). Fix NOT applied.
**Started:** 2026-07-16 · **Owner:** sergei · **Branch:** `sgrebnov/0716-cold-stall-diagnostics`

## Symptom

The SF1000 CH-benCHmark with the cold-tier (datalake) Cayenne config **sometimes** never
reports ready: `testoperator` fails with `Spiced instance not ready within <N>s`. Intermittent
("sometimes locks and does not progress").

- Spicepod: `test/spicepods/chbench/accelerated/postgres-cayenne[file]-adaptive-cold-sf1000.yaml`
- Config of note: `cayenne_tuning: adaptive`, cold tier on shared MinIO
  (`s3://benchmarks/cayenne-cold-sf1000/`), `cayenne_datalake_promotion_interval_ms` (promoter
  tick; default `cold_tier_background_interval_ms = 60_000`), `cayenne_datalake_warm_max_bytes`
  (warm→cold trigger).

## Confirmed mechanism (localized)

A **cold-tier promotion runs during a large table's initial-snapshot bootstrap** and gets stuck in
its `write-cold-store` phase (scan → Z-order sort → encode → upload) while holding that table's
`write_lock`. Because ingest (the bootstrap itself) needs `write_lock`, the initial snapshot can't
complete → the runtime never reports ready.

Why it fires mid-bootstrap: the promoter is spawned at dataset registration
(`crates/runtime/src/dataaccelerator/cayenne/mod.rs:2232`) and its first tick fires after
`cold_tier_background_interval_ms` (default 60s). When a large table's bootstrap outlasts that
interval (always true for `order_line` at SF1000 — hundreds of millions of rows), the tick lands
mid-bootstrap and the warm tier has already crossed the promotion threshold.

## Key evidence

### CI run 29550226067 (SF1000, spiced@`40d6c23`, `--ready-wait 2000`) — the definitive freeze
https://github.com/spiceai/spiceai/actions/runs/29550226067/job/87798490639

- 03:39:34 runtime starts.
- Several promotions **complete fine**: `customer` 21s then 65s; `stock` 28s then 60s. `stock`'s 2nd
  promotion reports `datalake_rewrite_selectivity=28.6% (2/7 files), carried=5` → **carry-forward
  works** (does NOT rewrite the whole cold set).
- 03:42:09 `order_line` promotion starts (warm 5.6 GB, 41 files) → enters `write-cold-store` and
  **never leaves it**: watchdog logs it stuck from `in_phase_s=114` to **`1794` (~30 min)**.
- ~03:44:40 a `stock` promotion **also** freezes in `write-cold-store` → **`in_phase_s=1617` (~27
  min)**. Both hold `write_lock` concurrently.
- During the stall: `mem_tier_used` 1.07 GB → **0** (mem-tier drained before the sort);
  `encode_permits = 48/48 free` the entire time (note: the cold write may be an exempt/coupled
  writer, so this is weak evidence).
- 04:12:26 `Error: Spiced instance not ready within 2000s` → fail.

**Shape:** a *single* order_line promotion (plus one concurrent stock) freezes for ~30 min; *other*
promotions of the same tables complete in 20–60s in the same run. **Racy freeze**, not uniform
slowness.

### Original failing run 29509572869 (trunk `1cc8f53`)
- `order_line` initial-snapshot bootstrap never completed; ~38 min of total silence before the
  ready-wait timeout. (Pre-diagnostics build; no phase visibility — motivated the watchdog.)

### Local SF100 (this machine: 64 GB, compaction pool ~7 GB; DB = 100 warehouses / 30M order_line)
Binary: `make install` at `3ec6368` (full telemetry). Harness: `scratchpad/cold_repro/`.
- **Default config → READY in ~26s.** Bootstraps finish (order_line 30M in ~19s) *before* the 60s
  promoter tick, so promotions run post-ready and never block readiness. Negative control.
- **Forced mid-bootstrap (promoter interval 2s):** order_line promoted 9× during bootstrap, each
  rewriting a growing cold set (65 MB→959 MB, 1.6s→17s), pushing bootstrap 42s→101s (~4× slower).
  This is an **artifact of the tiny interval**, NOT the CI shape.
- **Forced concurrent heavy spill (1 GiB warm trigger, 15s interval):** order_line + stock sorts
  spilled ~3.2–3.7 GB **concurrently** on the 7 GB pool → **all completed in 6–18s** (`first_batch_ms`
  5.8–13.9s), ready reached. **SF100 cannot reproduce the freeze even under heavy concurrent spill.**

Measured: a Z-order sort's memory footprint is **~3.5× warm bytes** (SF100: 1 GB warm → 3.5 GB
spilled). At CI, order_line(5.6 GB)+stock(5 GB) warm → ~37 GB combined footprint on a 30 GB pool
(exceeds it) — but SF100 shows even exceeding the pool completes quickly, so pool-exceed alone is
not the freeze.

## What this corrects

- **NOT a "quadratic repeated full-rewrite."** That only appeared in the SF100 2s-interval artifact.
  Carry-forward works at CI (stock 28.6% selectivity).
- The CI freeze is a **single promotion that never completes its first write-cold pass**, racy, and
  **not reproduced locally even under heavy concurrent spilling** → points toward a scale-dependent
  or rare-race stall in the scan/sort pipeline rather than deterministic slowness.

## Open question (blocks the fix choice)

Is the 30-min freeze a **hard stall (lost-wake / deadlock)** or a **pathologically slow
spilling sort**? The fix differs fundamentally:
- Hard stall → fix the stall in the scan/sort/vortex-write pipeline (deferring promotion would only
  mask the readiness symptom; it could still wedge under steady-state CDC and block ingest).
- Slow sort → serialize promotions / bound concurrency / give the sort memory / defer during load.

**Discriminator:** the `3ec6368` telemetry on a fresh SF1000 run:
- `progress` / `progress_delta_tick` in the watchdog line — frozen-at-N = stuck; climbing = slow.
- Bounded-sort run logs (`Bounded sort run starting/first batch/complete`) — does `run_idx=0` ever
  emit a first batch, or hang before it?
- `spill_count` / `spilled_bytes` per run; `compaction_pool_used`/`total` in the watchdog.

Local SF100 can't produce the freeze, so this must come from CI SF1000.

## Diagnostics added (branch `sgrebnov/0716-cold-stall-diagnostics`)

- `40d6c23` — `crates/cayenne/src/provider/stall_watchdog.rs`: dedicated OS-thread watchdog (immune
  to tokio starvation) reading a global registry of in-flight ops; WARNs on any op whose phase hasn't
  advanced past a threshold. RAII `StallOp` with `.phase()`. Promotion phase tracing in
  `promote_warm_to_cold_inner`; ingest write-lock instrumentation in `sink.rs`. Env:
  `CAYENNE_STALL_WATCHDOG_SECS` (default 30, 0 disables), `CAYENNE_STALL_WATCHDOG_WARN_SECS` (90).
- `f9b703a` — cold-write **progress counter** (rows delivered to the sink) + per-chunk INFO logs.
- `ded5c15` — CI workflow `testoperator_run_htap.yml` sets `SPICED_LOG` (cayenne=info): **cayenne is
  NOT in `bin/spiced/src/tracing.rs` `INTERNAL_COMPONENTS`, so its INFO logs are suppressed by
  default** — only WARN shows without SPICED_LOG.
- `3ec6368` — bounded-sort run logs (starting/first-batch/complete) DEBUG→INFO with `total_ms`;
  per-run `SortExec` **spill metrics** via new `util::stream_utils::sort_stream_with_plan`; watchdog
  dumps the **compaction memory pool** reserved/limit.

Diagnostics only — no locking/control-flow change.

## Key code references

- Promotion entry / phases: `crates/cayenne/src/provider/table.rs` `promote_warm_to_cold_inner`
  (~14672) — holds `compaction_lock` then `write_lock` across the whole graduation.
- Cold write: `write_stream_to_cold` (~14205) → `insert_stream_into_cold_dir` (~14174) →
  `collect(plan)` drives scan → sort → vortex sink.
- Z-order sort: `crates/cayenne/src/provider/streaming.rs` `bounded_sort_stream` (~385) →
  `util::stream_utils::sort_stream` → DataFusion `SortExec` (spilling, pipeline-breaker).
- Promoter spawn: `crates/runtime/src/dataaccelerator/cayenne/mod.rs:2232`
  (`spawn_background_cold_tier_promotion`), per-table, no cross-table serialization.
- Interval: `cold_tier_background_interval_ms` default `60_000` (`crates/cayenne/src/metadata.rs`),
  set from spicepod `cayenne_datalake_promotion_interval_ms`
  (`crates/runtime/src/dataaccelerator/cayenne/mod.rs:1409`).
- Initial-load-complete signal (fix target): `AcceleratedTable` refresher owns
  `initial_load_completed: Arc<AtomicBool>` (`crates/runtime/src/accelerated_table/refresh.rs:685`,
  flipped at Ready `refresh_task.rs:2194`).

## Candidate fixes (pending classification)

1. **Defer cold promotion until `initial_load_completed`** — inject the refresher flag into the
   Cayenne provider; `promote_warm_to_cold_inner` early-returns while false. Prevents the mid-bootstrap
   contention (fixes readiness). Downside: masks a genuine stall if one exists post-load.
2. **Serialize / bound cold-promotion concurrency** globally (one big sort at a time) — if concurrency
   contention is implicated.
3. **Fix the stall** in the scan/sort/vortex-write pipeline — if `3ec6368` telemetry shows a hard
   freeze (lost-wake/deadlock).

## Runs & links

- Original fail (trunk): https://github.com/spiceai/spiceai/actions/runs/29509572869
- Definitive freeze (spiced@40d6c23): https://github.com/spiceai/spiceai/actions/runs/29550226067/job/87798490639
- `3ec6368` build_and_release (telemetry binary): https://github.com/spiceai/spiceai/actions/runs/29553099217

## CONFIRMED root cause (2026-07-17, run 29558170355, spiced 3ec6368, SF1000, default terminals, ready-wait 2400)

Reproduced "not ready within 2400s" (40 min) WITH full traces. The decisive evidence — **a hard stall (lost-wake/deadlock) in a cold-promotion bounded-sort run, NOT slowness:**

`stock`'s 2nd promotion (05:54:06, warm 4.3 GB):
```
Bounded sort run starting stock run_idx=0
run_idx=0 first_batch 22.4s → COMPLETE 15,175,683 rows spill=3/5.7GB (31.7s)
Bounded sort run starting stock run_idx=1        ← starts, then NOTHING
  (no "first batch", no "complete" for run_idx=1, ever)
stall @30s cadence for 36 min: progress FROZEN at 15,175,683 (== run_idx=0 output),
  progress_delta_tick=0 every tick, compaction_pool_used 21GB→11GB→7.8GB (DECREASING,
  room free), mem_tier_used=0, encode 48/48 free, in_phase_s 111→2181
→ holds stock write_lock 36 min → stock initial snapshot never completes → never ready.
```

Discriminators (all from traces):
- `progress_delta_tick=0` with `run_idx=1` started-but-no-first-batch ⇒ SortExec/scan for the 2nd run is **parked** (produces zero rows), not slow.
- compaction pool has room and is **shrinking** ⇒ NOT memory-pool exhaustion.
- `order_line` in the SAME run is the red herring: its promotions **completed** (203s, 346s; progress climbs), bootstrap finished 06:09:57. It's `stock`'s `run_idx=1` freeze that blocks readiness.
- Racy: which table/run hits the freeze varies (order_line froze in run 29550226067; stock froze here) ⇒ a lost-wakeup in the **multi-run** `bounded_sort_stream` transition (run_idx≥1), i.e. the `ChunkedSource.next_chunk` → `RunInputStream` → `SortExec.execute` path for a subsequent run. Matches the original kanal/lost-wake history ("wedge in the scan/sort drain").

**Corrects earlier readings:** not "quadratic rewrite" (SF100 2s-interval artifact) and not merely "slow spilling sort" (that was order_line, which recovers). The never-ready is a genuine **deadlock in the promotion sort's 2nd+ run**.

Fix target therefore shifts: the deferral-until-initial-load gate would hide the readiness symptom but the lost-wake could still wedge post-load under CDC (holding write_lock, stalling ingest). The real fix must address the stall in `bounded_sort_stream`'s multi-run path (candidate: the `RunInputStream`/`ChunkedSource` handoff or the per-run `SortExec.execute` on the compaction runtime). Investigate before implementing.

## Next step

Wait for the `3ec6368` build → dispatch one SF1000 `testoperator_run_htap.yml` run on the branch →
read `progress`/sort-run/spill/compaction-pool telemetry → classify stuck-vs-slow → apply the
matching fix and verify (re-run until ready 10×).
