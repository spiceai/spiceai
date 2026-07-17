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

## Iteration 3 diagnostics (commit 12e96e7b, to pin the exact deadlock frame)

Static analysis narrowed but did not pin it: NOT kanal (gone from trunk vortex), NOT moka
load-coalescing (segment_cache uses plain `get`/`insert`), NOT DataFusion memory-pool blocking
(pool `try_grow` errors, never blocks — and the CI pool had room + was shrinking). The scan is a
standard `TableProvider::scan`→`execute_stream` driven single-task (one `collect` waker), so no
waker-identity change at the cayenne layer. Remaining candidates: (a) scan/vortex-read park, (b)
`SortExec` spill I/O, (c) compaction-runtime worker/blocking-thread starvation under concurrent
promotions.

Shipped two signals to disambiguate on the next SF1000 run (default terminals, ready-wait 2400,
`collect_diagnostics=true`, spiced 12e96e7b):
1. **Scan-vs-sort input counter**: `bounded_sort_stream` threads a rows-fed-INTO-sort counter,
   surfaced by the watchdog as `input_rows`/`input_delta_tick` next to sink `progress`.
   `input_delta=0` ⇒ scan/upstream parked; `input` advancing while `progress` frozen ⇒ sort/sink
   parked. Also bumped "Bounded sort run input consumed" DEBUG→INFO.
2. **Native thread-dump probe** (`scripts/htap-threaddump-probe.sh`, wired into
   `collect_diagnostics`): periodic all-thread backtraces of stalled spiced via eu-stack/gdb +
   `/proc/<pid>/task/*/{comm,wchan}` fallback — captures the exact parked frame / thread-pool
   starvation the async watchdog can't see. This is the artifact that yields "specific mechanism".

Once the thread-dump + input counter identify the exact frame, write a targeted unit test
(location depends on the frame: `bounded_sort_stream` multi-run harness in `streaming.rs`, or a
vortex read/spill test).

## Local repro attempt result (2026-07-17) — sort machinery EXONERATED

Added `bounded_sort_concurrent_multi_run_spilling_does_not_deadlock` (streaming.rs tests): two
concurrent `bounded_sort_stream`s sharing one 32 MiB `GreedyMemoryPool`, ~64 MiB/stream (forces
real disk spilling), multi-run, over an async input that returns `Pending` before every batch
(exercises task parking/waking across run boundaries). **Result: both sorted 8,192,000 rows,
errored=false, completed in 2.63s — NO deadlock.**

⇒ The deadlock is NOT in `bounded_sort_stream` / `SortExec` / memory pool / spilling / concurrency /
generic async parking. Combined with earlier eliminations (kanal gone, moka plain get/insert, pool
try_grow errors-not-blocks), the hang is in the **real vortex scan read path** (warm vortex files
through the vortex reader at scale) feeding run-1 — which a synthetic in-memory Int64 stream can't
reproduce. The test is kept as a regression guard proving the sort path is clean, but it is NOT the
repro of the actual bug.

Next: the CI thread-dump (`scripts/htap-threaddump-probe.sh`, on the pending 12e96e7b repro run)
will show the exact parked frame in the vortex read path → then write the targeted unit test there
(a vortex read test / the scan primitive), which is where the reproducing test belongs.

## DEFINITIVE: the freeze is a lost-wakeup in the SCAN (input to SortExec), not the SortExec (run 29599553490, 12e96e7b, 2026-07-17)

Reproduced "not ready within 2400s" with the `input_rows` counter. Frozen table `order_line`:
- Bounded-sort `run_idx=0,1,2` **completed** (~37M rows each; run 1 spilled 4.4 GB).
- `run_idx=3` logged **"starting" but never "input consumed"** → its input chunk never ended.
- Watchdog, flat for ~30 min (in_phase 1339→2209s): `input_rows=124,395,520 (+0)`,
  `progress=111,311,295 (+0)`. `input − progress = 13.1M` = rows run 3 consumed before the scan
  stopped (111.3M = runs 0-2 output). Compaction pool constant 6.4 GB (NOT exhausted).
- Thread-dump (ptrace blocked on runner even with sudo — no user backtraces): ALL `tokio-rt-worker`
  threads in `wchan=futex_wait_queue` (no runnable task) = **lost-wakeup signature** (not CPU-spin,
  not lock contention, not memory).

⇒ **It is the INPUT to the SortExec that parks — the visible cross-tier vortex read scan feeding
`bounded_sort_stream` stops producing (~13M rows into run 3) and never wakes / signals EOF.** run 3's
`SortExec` waits forever for input; the promotion holds `order_line`'s `write_lock`; the initial
snapshot can't complete → spiced never ready. Racy (~50%; 3ec6368 also became ready once, so the
input_rows atomic is NOT a Heisenbug — just a timing-sensitive race).

Scope narrowed to: the scan stream `visible_file_stream_for_rewrite` → `TableProvider::scan` →
vortex read (`crates/vortex/src/persistent`: opener/source/format `buffer_unordered` meta-fetch /
segment read) parking mid-stream on a later run's data. Matches the historical "wedge in the
scan/sort drain."

## Next step

Exact vortex frame needs finer scan-read tracing (ptrace backtraces are unavailable on the runner):
add a rebuild that logs the scan's last in-flight read op / a per-poll heartbeat so the last line
before silence pinpoints the vortex read primitive; then write a unit test reproducing that read
parking. (The `bounded_sort` machinery is exonerated — runs 0-2 completed; the repro test must be at
the vortex read layer.)

Wait for the `3ec6368` build → dispatch one SF1000 `testoperator_run_htap.yml` run on the branch →
read `progress`/sort-run/spill/compaction-pool telemetry → classify stuck-vs-slow → apply the
matching fix and verify (re-run until ready 10×).
