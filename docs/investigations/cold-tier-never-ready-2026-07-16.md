# Investigation: Cayenne cold-tier chbench "spiced never becomes ready"

**Status:** TWO distinct never-ready modes (do not conflate):
- **Mode 1 — the classic POLLED-THEN-PENDING freeze (run 29799199984): the goal's MECHANISM (1)** — a lost-wake
  in vortex's `buffer_unordered(handle.spawn(...))` scan composition (`scan_builder.rs:436` over `Task` =
  raw-`oneshot` `AsyncReceiver` from `handle.rs:67`). Runtime idle, no locks, consumer parked at `.await`;
  counter signature `spawned==completed, in_flight=0, backlog>0` = drain lost-wake. Sub-grain (i) FU parent-wake
  #1006 vs (ii) raw-oneshot receiver-waker #6221 still open. See "MODE-1 MECHANISM NAMED" below.
- **Mode 2 — data-divergence / slow-ingest never-ready (runs 2/4/5 + 2026-07-20 run 29717944124):**
  deletion-index `im::HashMap` HAMT `arc_swap::rcu` write-starvation livelock (compaction prune vs high-rate
  CDC-apply on one `ArcSwap`, O(N) rebuild+drop per CAS-loss retry, tombstone index → 65.8M). Mechanism C.
Kick-in-at-RunInputStream fix FALSIFIED for Mode 1 (a re-poll can't reach a stranded FU/oneshot child).
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

### Additional CI evidence: completed Vortex scan tasks not drained (run 29767001975, job 88440556875, 2026-07-20)

Run: https://github.com/spiceai/spiceai/actions/runs/29767001975/job/88440556875

This run adds a stronger `vortex::scanpark` signal: the Vortex scan task bodies completed, but the
stream did not yield all completed outputs before Cayenne observed `POLLED-THEN-PENDING`.

- `htap-run.log:439`: `spawned=25905 completed=25905 yielded=25773 in_flight=0 backlog=132`
  ⇒ all tasks completed, no tasks in flight, but 132 completed outputs still not yielded.
- `htap-run.log:440,442,443,448,449`: repeated `SCAN STALLED` with the same counters plus
  `task_started=25905 task_filter_done=25905 task_project_done=25905 stuck_before_filter=0
  stuck_before_project=0` ⇒ not stuck in filter/projection/I/O task body; stuck draining completed
  results to the stream consumer.
- `htap-run.log:474`: Cayenne then reports
  `progress_delta_tick=0 input_delta_tick=0 input_poll_entered_delta=0 input_poll_inflight=0` and
  `sort_input_diag="POLLED-THEN-PENDING: SortExec polled, scan returned Pending, parked waiting for
  a wake (scan lost-wakeup / not producing)"`.
- Duration/overlap:
  - Cayenne's sort-input state is `POLLED-THEN-PENDING` from `htap-run.log:474`
    `19:22:56.668759` through `htap-run.log:6932` `19:46:27.425925` — **~23m31s observed**,
    ending only with `htap-run.log:6933` `Error: Spiced instance not ready within 1800s`.
  - While Cayenne is already in that parked state, Vortex reports overlapping **transient**
    incomplete-drain episodes:
    `htap-run.log:6804-6806` `19:23:46.768782`→`19:23:56.769020` (**>=10s observed**) with
    `spawned=29477 completed=29477 yielded=29212 in_flight=0 backlog=265`, bracketed by Cayenne
    `POLLED-THEN-PENDING` at `htap-run.log:6793` `19:23:27.417549` and `htap-run.log:6807`
    `19:23:57.417694`.
  - It repeats at `htap-run.log:6861-6863` `19:26:46.772389`→`19:26:56.772603`
    (**>=10s observed**) with `spawned=32204 completed=32204 yielded=32166 in_flight=0 backlog=38`,
    bracketed by Cayenne `POLLED-THEN-PENDING` at `htap-run.log:6850` `19:26:27.418475` and
    `htap-run.log:6864` `19:26:57.418603`.

This is strong evidence of **completed-result drain/waker fragility above the scan task body**:
`completed > yielded` with `in_flight=0` while downstream is parked on `Pending`. The Vortex
`scanpark` windows above are not themselves permanent lost-wake proof (their counters later move);
the permanent symptom is Cayenne's sort-input stream remaining `POLLED-THEN-PENDING` with no further
input polls until the readiness timeout.

## Localized to the vortex segment-read driver (FileSegmentSource), 2026-07-17

The frozen scan (run 3's SortExec input) parks in vortex-file's per-file segment reader
`FileSegmentSource` (`/Users/sg/spice/other/vortex/vortex-file/src/segments/source.rs`):
- A spawned driver fetches segments via **`buffer_unordered(concurrency)`** (`concurrency =
  reader.concurrency()`) — a bounded read-concurrency pipeline.
- Lazy + coalesced + `Polled`-gated: `request()` registers a segment (eager) but the actual fetch
  only fires after the `ReadFuture`'s first poll sends a `ReadEvent::Polled(id)`; the
  `IoRequestStream` state machine (`vortex-file/src/read/driver.rs`) coalesces polled requests
  (`State.requests` vs `polled_requests`, `next_uncoalesced`/`next_coalesced`) and feeds
  `buffer_unordered`; results come back via `oneshot` (`req.resolve`).
- A segment request that never gets fetched ⇒ its `oneshot` never resolves ⇒ the scan parks (input
  frozen) ⇒ promotion holds write_lock ⇒ never ready. Matches all evidence (input frozen mid-run,
  workers idle-parked, racy, pool not exhausted).

Ruled out for the freeze: shared read/write mutex (none), write holding a permit (encode budget
FREE 48/48 during the freeze), the SortExec/sort/pool (runs 0-2 completed). `poll_next` wakeup
registration reads as correct on inspection, so the bug is a subtle race in this driver's
coalescing/`Polled`-gating/`buffer_unordered` interplay (a 817-line driver; not obvious statically).
NOTE: vortex core lives at `/Users/sg/spice/other/vortex` (checkout); confirm it matches the
`vortex` workspace pin spiceai builds before asserting line-level.

## Next step

Write the reproducing unit test at the vortex-file `FileSegmentSource` layer (not DataFusion scan,
not bounded_sort — both exonerated): construct a `FileSegmentSource` with a mock `VortexReadAt`
(controllable `concurrency()` + `read_at` latency), register many segments, poll them concurrently
in adversarial orders under a timeout; a hang reproduces the segment-read stall and lets us bisect
the exact race. (Confirm the local vortex checkout == the built pin first.)

Exact vortex frame needs finer scan-read tracing (ptrace backtraces are unavailable on the runner):
add a rebuild that logs the scan's last in-flight read op / a per-poll heartbeat so the last line
before silence pinpoints the vortex read primitive; then write a unit test reproducing that read
parking. (The `bounded_sort` machinery is exonerated — runs 0-2 completed; the repro test must be at
the vortex read layer.)

Wait for the `3ec6368` build → dispatch one SF1000 `testoperator_run_htap.yml` run on the branch →
read `progress`/sort-run/spill/compaction-pool telemetry → classify stuck-vs-slow → apply the
matching fix and verify (re-run until ready 10×).

## Concurrent-operation patterns EXAMINED and ELIMINATED (2026-07-17, full re-read of run 29599553490)

Re-read the frozen run's full 1628-line log to test whether a *concurrent* cayenne operation
(parallel deletion of protected snapshots, another table's compaction, a shared IO limit) causes
the scan to park — not just the intrinsic vortex race.

Verified chronology (`order_line`, the frozen table):
- Many `Fast protected-snapshot subset compaction` + `Seq-prefix bake` on `order_line` complete
  **before** promotion (last at 18:05:29), each minting a `new_snapshot_id` and retiring inputs.
- Promotion starts **18:05:44** (`warm_files=33`, holds `write_lock`). **No `order_line` compaction
  runs again** until after the freeze — promotion ⟂ compaction per table via `write_lock`.
- runs 0,1,2 complete (37.1M rows each; run 1 spilled 4.37 GB). **run_idx=3 "starting" @18:08 →
  never "input consumed".** Meanwhile `stock`/`customer` promotions keep completing (18:08:09,
  18:10:43, 18:13:43) — freeze is **per-`order_line`, not global**.

Eliminated patterns, each with independent evidence:
1. **Parallel deletion of protected snapshots.** (a) `write_lock` serializes promotion vs compaction
   for `order_line` (no concurrent compaction during its promotion). (b) `[file]` local store ⇒ a
   deleted file is `ENOENT`, an *error* that `RunInputStream`/`bounded_sort` surface as `Some(Err)`
   (streaming.rs:451/533) and end the stream — but there is **not a single ERROR / `NotFound` /
   `ENOENT` / retry line in the whole 40-min log**; the hang is a silent `Poll::Pending`. (c) freeze
   is confined to one table.
2. **Another table's compaction starving a shared resource.** During the stall `compaction_pool_used`
   *drops* 13.8→6.8 GB (22 GB free), `encode 48/48 free`, `mem_tier 0`; concurrent tables complete.
3. **Global vortex IO-executor saturation.** /proc wchan showed **all tokio workers `futex_wait`
   (no runnable task)** — the lost-wakeup signature, not saturation (which leaves workers busy).
4. **cayenne run-boundary / EOF bug in `bounded_sort_stream`.** Traced the state machine: run 2 ends
   on its **byte cap** (streaming.rs:257, not EOF) ⇒ `is_exhausted()` correctly false ⇒ run 3 minted
   (499–510); run 3's `SortExec` pulls `RunInputStream → ChunkStream → inner.poll_next` and faithfully
   propagates the inner's `Pending` (265/275/349). "input consumed" logs only on `Ready(None)` (334)
   and never fires ⇒ the **inner vortex scan returns `Pending` forever**. cayenne is a correct conduit.

**Surviving cause (unchanged):** a lost wakeup inside the vortex `FileSegmentSource` segment-read
pipeline feeding run 3. None of the concurrent-operation patterns are contributing.

## Reproduction unit test (written 2026-07-17)

`vortex-file/src/segments/source.rs` → `mod stall_repro_tests` →
`file_segment_source_concurrent_requests_do_not_stall` (local checkout `729249dd3`; the pinned build
is `spiceai/vortex@409505dfd0` — same `FileSegmentSource` Polled/coalescing/`buffer_unordered`
protocol; final test + fix land at the pin). Mock `VortexReadAt` with bounded `concurrency`,
coalescing enabled, offset-dependent `read_at` latency; drives batched schedules (batch ∈ {1,2,3,8,64})
that **drain the driver to idle between batches** — the idle→re-wake transition where a lost wakeup
wedges — looped, under a 10s timeout. Three schedules, all **compiled + passed** (no repro):
(a) all-registered burst + `try_join_all` (200 iters, 5.3s) — perpetual backlog hides the idle→wake
transition; (b) batched-drain `batch ∈ {1,2,3,8,64}` (80 iters, 403s) — drains to idle between
batches; (c) drop-churn at the run boundary (300 iters, 10.3s) — models a `ChunkStream` dropped
mid-prefetch (`ReadEvent::Dropped`) racing the coalescer + `buffer_unordered` slot accounting.

Driver audit (read/driver.rs `poll_next` L73–104 + `State::next`/`next_coalesced` L172–301): the drain
loop registers the events-channel waker on its final `Pending` poll (no basic mpsc lost-wake), and
`next_uncoalesced` always returns a live polled request (pop_first, skip closed-callback), so `next()`
returns `None` only when `polled_requests` is empty — i.e. `Pending`-waiting-for-an-event is correct.
The obvious lost-wake axes are clean; the race is subtle and does not surface under a fast in-memory
mock. Most likely it needs conditions the mock doesn't model: **remote-storage (S3/MinIO) latency +
the high `concurrency()` an object-store reader advertises**, where many coalesced reads are in flight.

A fourth attempt made the mock **prod-faithful** — matched `ObjectStoreReadAt` exactly: `concurrency
= 192` (`DEFAULT_CONCURRENCY`), `coalesce = object_storage()` (1 MB / 16 MB), `read_at` completing
across two spawned tasks (`spawn_io` → `spawn_blocking`), and a clustered segment layout so coalescing
and 192-wide concurrency both engage. **Both schedules still passed** (83s). Mock approach exhausted
(5 attempts across burst / batched-drain / drop-churn / prod-faithful; driver code byte-identical to
the pin `409505d`, audited clean).

**Decisive finding (why no local mock can reproduce it):** the promotion's cross-tier scan
(`visible_file_stream_for_rewrite` → `TableProvider::scan`, table.rs:14092/14841) reads **all tiers**:
mem-tier + warm (local `[file]`) + **cold (S3/MinIO)** — the cold branch restricted to the *dirty*
cold files by the `ColdScanFileSubset` extension (table.rs:14830–14839). So run 3's stuck read very
likely targets a **cold file on S3/MinIO**, whose `ObjectStoreReadAt::read_at` takes the object-store
**byte-stream** path (`store.get_opts().await` + streaming, read_at.rs:152–163) — real network
latency / connection-pool limits / retries, driven `buffer_unordered(192)`-wide. A fast in-process
mock structurally cannot replicate that; the lost wakeup most probably lives in that S3-read ×
coalescing-pipeline interaction.

**Recommended next step (mock is a dead end):** pin the frame with per-read tracing *inside* the
vortex driver against a real S3-backed repro — log each `read_at` start (offset/len/id, tier), each
completion, and a per-poll driver heartbeat (in-flight count / `polled_requests` / registered), then
run one CI SF1000 repro; the last line before silence pinpoints the parked read (and whether it's a
cold/S3 read). This needs the tracing in the CI binary → push a `spiceai/vortex` branch off the pin
`409505dfd0`, bump the `Cargo.toml` rev, rebuild spiced, dispatch CI. With the frame known, write the
reproducing test at that layer and land the fix at the pin. (Awaiting user go-ahead — this touches
the vortex pin + an org push, beyond the diagnostics-only branch scope so far.)

## FACT: compaction memory exhaustion is a clean ERROR + retry, NOT the freeze (observed 2026-07-18)

Direct log evidence (a run with a small ~1.6 GB compaction pool):

```
WARN cayenne::compaction: Cold-tier promotion tick failed (warm tier left intact; retry next tick):
  Not enough memory to continue external sort. Consider increasing 'datafusion.runtime.memory_limit'
  or decreasing 'datafusion.execution.sort_spill_reservation_bytes'.
caused by Resources exhausted: Additional allocation failed for ExternalSorterMerge[0] ...
  Failed to allocate additional 10.0 MB for ExternalSorterMerge[0] ... 714.0 KB remain available
  for the total memory pool: greedy(used: 1637.7 MB, pool_size: 1638.4 MB) table="oorder"
```

Key facts:
- When the compaction `GreedyMemoryPool` cannot satisfy a sort/merge reservation, the `ExternalSorter`
  (here in the **`ExternalSorterMerge`** phase, needing +10 MB) returns **`ResourcesExhausted`** — it
  does **not** block or deadlock.
- Cayenne **handles it gracefully**: `Cold-tier promotion tick failed (warm tier left intact; retry
  next tick)` — the promotion aborts, releases its memory + `write_lock`, and retries on the next
  tick. No hang.
- Therefore memory exhaustion during compaction is **visible (a WARN with a `Resources exhausted`
  cause) and self-recovering** — categorically different from the SF1000 freeze, which is a **silent
  40-min hang with zero errors** while holding `write_lock`.
- Corroborates the local unit test (`concurrent_promotions_pool_contention_errors_does_not_deadlock`)
  and the earlier deduction: pool pressure ⇒ *error+retry*, not deadlock. The freeze must be a
  pre-error mechanism (scan/driver lost-wakeup that never reaches a failing reservation), because a
  genuine memory shortfall would surface as this WARN, not silence.
- Note: in the SF1000 CI freeze the pool was pinned at ~94.6% (28.6/30.2 GB) with **no** such WARN —
  i.e. the sorts were *waiting* (for parked scan input), not *failing to reserve*. The held memory is
  a symptom of the parked scan, not the cause.
- (The `not ready within 120s` in that log is just a short ready-wait on a slow bootstrap, not the
  freeze; the "Not enough memory" WARN is a normal small-pool promotion retry.)

## DEFINITIVE: reads exonerated; stall is scan-batch-production at a bounded_sort run boundary (frozen instrumented run 29627694719, 2026-07-18)

A frozen CI run on the instrumented spiced (`67d4d14902` → `vortex@4b323e22c4` read tracing) gives the decisive picture:

- **Sustained freeze**: `stock` cold-promotion, `write-cold-store-scan-sort-encode-upload`, `in_phase_s`
  2015→2165 (35+ min), `progress` frozen at 17,053,697, `input_rows` frozen at 26,992,640 (both `delta=0`).
- **Every vortex read completed**: `read_at start` without a matching `end` = **0 of 155,773** — including
  cold S3 reads. Driver alive (`q_polled=0`, counts climbing).
- **⇒ The freeze is NOT** a hung read, S3/MinIO connection, `FileSegmentSource` lost-wakeup, the driver,
  or a `ChunkedSource` contract violation (runs are sequential).
- **Precise locus**: stock's 2nd promotion ran `bounded_sort` `run_idx=0` to completion (17.05M rows),
  then `run_idx=1` **started but never logged "input consumed"** — it froze ~9.94M rows into run 1
  (`26.99M` cumulative − `17.05M` from run 0), with `progress` still at run-0's output (run 1's `SortExec`
  is still consuming, has emitted nothing). All of run 1's underlying reads completed.
- **Therefore** the scan `DFStream` (`execute_stream(TableProvider::scan)` → vortex decode/layout +
  DataFusion scan-plan operators, incl. the `RepartitionExec`s seen in the memory log) returns `Pending`
  forever at the **run≥1 boundary**, starving `run_idx=1`'s `SortExec`. This is the same "run≥1 started,
  never consumed input" pattern as the first frozen run (29599553490) — now with reads proven complete beneath it.

**Next narrowing (cayenne-side, no vortex change):** wrap the scan `DFStream` feeding `zorder_sort_stream`/
`bounded_sort` with per-poll logging (Pending vs Ready, last wake), and log `ChunkStream`'s inner-poll
result. That distinguishes "scan `DFStream` Pending forever" (→ vortex decode/layout or `RepartitionExec`
backpressure across the run-boundary poll pause) from a `SortExec`-side stall. (Heavy `dropped`-read churn
— 1.29M, order_line cold dominant — is harmless prefetch-then-cancel; 0 hung.)

## LEADING HYPOTHESIS: known upstream waker-corruption bug class (oneshot + FuturesUnordered / sequence.rs), 2026-07-18

An upstream-issue review connects this freeze to a documented waker-corruption bug class in exactly the
vortex read/sequence path — our freeze being the **lost-wakeup (hang) sibling** of crashes already reported.

Related issues:
- **spiceai#8830 (CLOSED): "Cayenne acceleration panic in `FuturesUnordered::release_task`."** The `oneshot`
  crate drops a stored waker in `Receiver::drop()`; when that waker is an `Arc<FuturesUnordered::Task>`,
  re-entrant drop → panic/SIGSEGV. Fixed by switching to `tokio::sync::oneshot`.
- **vortex#6221 (OPEN): "Occasional SIGSEGV under high concurrency."** Broader tracking of the same
  `oneshot`+`FuturesUnordered` waker corruption. Still open. References spiceai#8830. (@sgrebnov.)
- **vortex#4521 (CLOSED): "all wakers must have been removed."** A leaked-waker panic in
  `vortex-layout/src/sequence.rs` (the sequencing/eval driver).

Mechanism (why it's our freeze):
- `vortex-layout/src/sequence.rs` `SequenceUniverse` stores `cx.waker().clone()` in a `HashMap` per
  sequence id (`WaitSequenceFuture::poll`, line ~243) and, when a sequence finishes, `SequenceId::drop`
  (line ~147-151) calls `remove(self)` → `w.wake()` on the *next* sequence's stored waker.
- If that stored waker is **stale/corrupted, `wake()` no-ops** → the next sequence never re-polls →
  decode/scan-batch-production stalls. This matches every observed fact: reads all complete (stall is
  ABOVE the reads), stall is below/at decode, at a `bounded_sort` run-boundary poll pause/resume, all
  workers `futex`-parked (no runnable task = a lost wakeup, not a busy loop).
- The stored waker is frequently the `Arc<FuturesUnordered::Task>` from the upstream `buffer_unordered` —
  the exact object #6221 says gets corrupted. So the crash (#8830/#6221), the leak-panic (#4521), and our
  silent hang are three manifestations of the same waker-lifecycle bug.
- The #6221/#8830 fix (`tokio::sync::oneshot`) landed **narrowly** — `spiceai/vortex@b27e89af5 (#9)` only
  changed `vortex-io/src/runtime/single.rs`. **`FileSegmentSource` (`vortex-file/src/segments/source.rs`)
  still uses the raw `oneshot` crate** (`oneshot::channel()`/`AsyncReceiver`) in our pin `409505dfd0`, so
  the vulnerable pattern remains and its wakers feed the `sequence.rs` chain.

Convergence with the instrumentation: this predicts the pending decode+poll-probe run will show
`sort_input_diag = POLLED-THEN-PENDING` (scan `DFStream` returned `Pending`, waiting for a wake that never
came) with `DECODE_INFLIGHT == 0` (stall above decode, in the sequence/eval waker) — a lost wakeup, not a
stuck decode. If so, it corroborates the `sequence.rs`/`oneshot` waker hypothesis directly.

Candidate fixes to trial: (1) apply the `tokio::sync::oneshot` switch to `FileSegmentSource` (the spot #9
missed); (2) harden `sequence.rs`'s wake path against stale wakers; (3) try a newer vortex (pin is 0.76.0)
that may fully resolve #6221.

## 2026-07-19: poll-probe CONFIRMS scan lost-wakeup; exact path pinned; fix under test

**Poll-probe verdict (frozen instrumented runs 29670291865 + 29670293472, binary 9e776866c).** Both
`order_line` + `stock` cold-promotions stuck 1400+s in phase `write-cold-store-scan-sort-encode-upload`
with `sort_input_diag = "POLLED-THEN-PENDING"` — the SortExec polled its input (the scan), the scan
returned `Pending`, and the wake to resume never came. Ruled out by the *same* capture: NOT memory
(`compaction_pool_used` ~14–22 GB of 30 GB, not exhausted — an adequate-pool run still froze), NOT
downstream/encode backpressure (`encode_permits 48/48` idle), NOT stuck-in-poll, NOT SortExec-not-polling.
This validates the 2026-07-18 prediction above. Racy: a 3rd same-binary run (29670295224) became READY, so
~2/3 freeze, not deterministic — consistent with a waker race.

**Eliminated fixes (do NOT resolve the freeze):** vortex #8677 + #8711 (detached read-driver panic
handling + spawn-via-runtime) cherry-picked and tested — still froze; they only change panic *visibility*,
and a panicking detached driver drops its Senders (→ error, not hang). `sequence.rs` `SequenceUniverse` is
UNCHANGED 0.76→0.79 and, on review, its ordering protocol is sound (parked waiters always re-register). So
the earlier `sequence.rs`/read-driver framing is set aside.

**Exact scan path pinned (corrected).** The promotion scan runs through the DataFusion opener →
`ScanBuilder::into_stream` → **`LazyScanStream`** (`vortex-layout/src/scan/scan_builder.rs`), whose
`Stream`-state driver is `stream::iter(tasks).map(handle.spawn).buffer_unordered(concurrency)` — NOT
`RepeatedScan::execute_stream` (a first instrumentation pass was misplaced there and fired 0 lines). Each
split-read `Task` joins via the **oneshot crate** (`vortex-io/src/runtime/handle.rs`), and `buffer_unordered`
drives them inside a `FuturesUnordered`.

**Scan-park counter verdict (frozen run 29699291451, in-process dumper).** Tail:
`spawned=284565 completed=284560 yielded=284482 backlog=78` then froze ⇒ task bodies COMPLETE
(`completed≈spawned`) but ~78 completed splits were NOT drained (`yielded<completed`) while the scan
returned `Pending` — a **drain-side lost-wake**, not stuck task bodies. (Caveat: backlog only 78, softer
than a textbook lost-wake — treat as strongly-indicated, not proven.)

**PR #9 gap = the scoped fix candidate.** #6221's `tokio::sync::oneshot` fix (spiceai/vortex `b27e89af5`,
#9) was applied ONLY to `single.rs` (feature-gated) — NOT to `handle.rs`'s general `Task`, which is exactly
what `LazyScanStream`'s `buffer_unordered` uses. Note the WHOLE-channel tokio swap on the older
`spiceai-54-tokio-channels` branch (0.76-based) did NOT fix the hang, so this is *scoped, not proven* — the
current `handle.rs`-only gap was never independently tested until now.

**Lock-deadlock hypothesis (open, not ruled out).** Execution-path locks exist and are shared across
concurrent split-read tasks — `FilterExpr` in `scan/filter.rs` (`ordering: RwLock<Vec<usize>>`,
`conjunct_selectivity: Vec<RwLock<DDSketch>>`) and `file_stats.rs` `Arc<Mutex<ExecutionCtx>>`. `filter.rs`
looks clean on inspection (no guard-across-`.await`, no re-entrant read→write), but `parking_lot`
contention under high concurrency isn't excluded. Corroborating: earlier tokio task-dumps **timed out on
synchronous blocks** — more consistent with a `parking_lot` block than a pure async park. The captured
native dumps can't discriminate: `eu-stack`/`gdb` FAILED on every dump (`ptrace` blocked — CI runs in a
container without `CAP_SYS_PTRACE`; `ptrace_scope` unsettable), and the ptrace-free `wchan` shows 365/370
threads (incl. 64/65 `compaction-work`) in `futex_wait_queue` — the identical state for a lock-blocked
thread AND an idle parked runtime worker. So lock-vs-lost-wake is UNRESOLVED by captured data.

**write_lock wedge (shape A, code-confirmed).** `promote_warm_to_cold_inner` (`table.rs` ~14708) holds the
table `write_lock` (`lock_owned`, ~14760) across the ENTIRE graduation incl. the scan/sort/encode/upload
phase. So a parked scan → `write_lock` held forever → the table's CDC apply blocks → spiced never ready.
"Promotion parked in scan" (cause) and "ingest apply wedged" (symptom) are the same wedge.

**Diagnostics built (ptrace-free, in-process).** (1) scan-park counters + a **side-thread steady-state
dumper** (`vortex-layout/src/scan/scanpark.rs`) that samples spawned/completed/yielded + task phase
(started/filter_done/project_done) every 5s and emits DURING the stall — distinguishes stuck-body vs
drain and localizes a stuck body to filter vs projection. (2) **in-process all-thread native backtrace**
on stall (`cayenne/src/provider/thread_backtrace.rs`, signal-based, capture IPs in handler + symbolize
outside), triggered by the stall watchdog — the ptrace-free equivalent of `gdb thread apply all bt` that
will show `parking_lot::…::lock` frames (lock deadlock) vs runtime-park frames (idle/lost-wake). Built but
deferred (shares the CI `chbench` DB with the fix runs).

**THE FIX under test.** Extend #9's swap to `handle.rs`'s `Task` → `tokio::sync::oneshot` (feature-gated,
mirroring `single.rs`): vortex `sgrebnov/cold-stall-vortex-oneshot-handle` @ `ea51ad3ea`, spiced
`sgrebnov/0716-cold-stall-fix` @ `16c22041b` (build 29700121334).

**Fix confirmation status + a correction.** An earlier "3/3 ready" claim was WRONG: two of those runs
(29705563263, 29706653018) actually FAILED at `Setup spiced` — the fix binary wasn't in MinIO yet
(dispatched in parallel before the build finished uploading) — and were misclassified because the loop
defaulted to "ready" whenever it didn't see "not ready within" (compounded by truncated `--log` fetches).
**Only 29706785855 is confirmed READY** (reached the post-ready convergence gate). A corrected toward-10
loop (reliable `--log-failed` classification: `did not converge`⇒ready, `not ready within`⇒froze, else⇒
infra/retry) is running to accumulate 10 genuine ready runs — which both confirms the fix and satisfies the
goal's "10 successful ready runs" condition. Any fix-binary FREEZE would disprove the fix and re-open the
mechanism track (deploy the deferred backtrace build).

**SEPARATE issue — convergence (not the freeze).** Every fix run still fails the correctness gate with an
`order_line` under-count ("replication did not converge"). Root: the promotion sort runs in a dedicated
**compaction memory pool** sized by `cayenne_compaction_memory_fraction` (default 0.2 of host RAM, ≈4.8 GB
on the CI runner), SHARED across concurrent promotions; two big-table sorts collide → `ResourcesExhausted`
→ tick errors + retries → order_line never promotes → apply lags. This is a clean ERROR path (the
`GreedyMemoryPool` errors, never deadlocks), fully independent of the freeze. Fix knobs:
`cayenne_compaction_memory_fraction` (raise) and/or `cold_clustering_run_size_mb` (lower). Note:
`runtime.query.memory_limit` is the WRONG knob (it sizes the query pool, not the compaction pool).

---

## 2026-07-20 — Source-level exoneration of the oneshot join layer (why the fix was refuted)

Read the pinned vortex diag tree (`f42235f4d`) end-to-end for the scan-driver wake chain, to interpret
what the pending native backtrace must show.

**Scan composition** (`vortex-layout/src/scan/scan_builder.rs:410-441`, `LazyScanStream`):
`stream::iter(tasks).map(|t| handle.spawn(async { t.await; COMPLETED++ })).buffer_unordered(N).filter_map(YIELDED++)`
with `N = concurrency(4) * num_workers`. Ordered path uses `.buffered(N)` instead.

**Spawn + join** (`vortex-io/src/runtime/handle.rs`): `handle.spawn` eagerly spawns via `Executor::spawn`,
which for tokio is `tokio::runtime::Handle::spawn` (`runtime/tokio.rs:44-57`) — a **real tokio task** with
tokio's own stable, reliable waker; it runs to completion independent of the joining side and then
`send.send(output)` on a `oneshot::channel`. The `Task` join future awaits `recv: oneshot::AsyncReceiver`.

**Full wake chain on task completion:**
`tokio task done → send.send() → oneshot recv waker → FuturesUnordered child-proxy waker → FU parent waker
→ buffer_unordered → filter_map → DataFusion input adapter → SortExec → top-level task`.

**Oneshot layer is CORRECT (source-verified, `oneshot 0.2.1`):**
- `AsyncReceiver::poll` re-registers the latest waker on every poll — RECEIVING branch drops the old and
  writes the new (`receiver.rs:705-707`). No stale-waker accumulation.
- send-during-park race handled — `write_async_waker` does `compare_exchange(EMPTY,RECEIVING)` and on
  `Err(MESSAGE)` takes the message immediately (`channel.rs:261-276`).
- unparking race handled — `Err(UNPARKING)` self-wakes via `cx.waker().wake_by_ref()` (`receiver.rs:728-733`).
  This *is* the #6221-class fix, already present.

**Therefore the fix refutation is explained, not anomalous.** Swapping this oneshot to `tokio::sync::oneshot`
could not help because the original already handles every send/recv/unpark race correctly. The oneshot join
layer is **eliminated as the lost-wake source by inspection**, not only by the empirical freeze of the fix
build (29708508223).

**Remaining lost-wake suspects (what the backtrace must disambiguate):**
1. `FuturesUnordered` parent-waker propagation under `buffer_unordered(N)` (the composition itself).
2. The DataFusion input adapter between the scan and SortExec (`RecordBatchReceiverStream`-style mpsc hop —
   adds another producer-task/consumer wake edge; "sort_input POLLED-THEN-PENDING" is measured at THIS edge).
3. `custom_labels::with_current_labels()` wraps every spawned future (`runtime/tokio.rs:50`, unix-only) —
   low risk (it only wraps the tokio-driven body, not the join), but unaudited.
4. Not-a-lost-wake alternative still open: worker-pool starvation / a `RawRwLock`/`RawMutex::lock` deadlock —
   the native backtrace names this directly (a `lock`/`lock_slow` frame on the promotion or a worker thread).

Static analysis is now exhausted; the permanent-freeze native backtrace (promotion/scan thread's parked
frame) remains the discriminator. Mechanism-capture continues on the dumper build (`4e1166096d`).

### Cayenne sort-run driver reviewed (same day) — convergence on "parked scan"

Traced the cold-write sort pipeline in `crates/cayenne/src/provider/streaming.rs`:
- `bounded_sort_stream` (line 459) sorts in **sequential byte-bounded runs** via an `unfold` loop; each run
  mints a fresh `SortExec` over `ChunkedSource::next_chunk(Bytes(run_size))`. The freeze is documented in-code
  as "a `run_idx>=1` SortExec starts but never emits" (line 965) — i.e. a run-boundary stall.
- `ChunkedSource`/`ChunkStream` (185-317): shares ONE inner scan stream across runs; enforces the
  consume-sequentially contract by **poisoning** an overlapping chunk (typed error, not a hang) and polls the
  inner stream synchronously under a `parking_lot::Mutex` **never held across `.await`** (poll_next is sync).
  Boundary resumption is correct — run N ends at the cap via `Ready(None)` without polling inner; run N+1's
  first poll re-registers its waker in the scan's `FuturesUnordered`, which drains any children that completed
  in the gap. No lost-wake found here.
- The regression guard `bounded_sort_concurrent_multi_run_spilling_does_not_deadlock` (line 972) uses a plain
  `RecordBatchStreamAdapter` async input, **not** the real vortex `LazyScanStream` — so it cannot reproduce a
  vortex-scan lost-wake. It passes; it is a guard, not a live repro.

**Convergence.** The cayenne team's own documented hypothesis (lines 1062-1073) is: `GreedyMemoryPool` errors
on exhaustion (never blocks), so "the CI HANG must involve a component that WAITS rather than errors — the
parked cross-tier scan holding pool memory while the sort waits for its input — a circular wait." That lands
on the exact same place as this session's source review: **the scan returns Pending and is never re-woken,
holding `write_lock` + pool memory.** Neither the code comments nor a full re-read of oneshot / ChunkStream /
buffer_unordered can name the dropped-wake site — every visible wake layer re-registers correctly.

**Refined discriminator for the pending native backtrace** (the only remaining way to reach 100%):
- scan/promotion thread **idle-parked at an `.await`**, no lock frame, all tokio workers idle ⇒ a lost-wake
  inside `FuturesUnordered`/`buffer_unordered` or tokio scheduling (not app code, not oneshot);
- all tokio workers **busy/blocked** with the scan's spawned split-reads queued ⇒ **worker-pool starvation**
  (the eager `handle.spawn` tasks can't be scheduled) — a scheduling deadlock, not a lost-wake;
- a `parking_lot::…::RawRwLock::lock`/`RawMutex::lock`/`lock_slow` frame on the promotion or a worker thread
  ⇒ **lock deadlock** (names the lock).
The scan-park counters from frozen run 29699291451 (`completed≈spawned`, ~78 undrained/unyielded) favor the
first (drain-side lost-wake: tasks completed, results never drained) over starvation — the backtrace confirms.

### Live local-repro datapoint (2026-07-20) — a transient STUCK-INSIDE-POLL

The local single-table docker repro (dumper image) ran healthy/self-healing this session (counters grew,
`order_line` promotion cycled runs 0→3 repeatedly, never permanently froze). The "SCAN STALLED" WARNs were
the false-positive kind (backlog ~270-285 = normal `buffer_unordered` pipeline depth; counters advanced
between ticks). One `cayenne::stall` tick showed a NEW, distinct diag:
`sort_input_diag="STUCK-INSIDE-POLL: a RunInputStream::poll_next is blocked synchronously (ChunkStream mutex
or sync execute_arrow decode)"` with `input_poll_inflight=1` — a *transient* synchronous block inside the
input poll (the sync `execute_arrow` decode under the `ChunkStream` mutex, streaming.rs:294-298), which
resolved on the next tick.

This adds a **third mechanism branch** the permanent-freeze backtrace must distinguish, all separable by the
`sort_input` diag captured at the freeze:
- `POLLED-THEN-PENDING` (all permanent freezes so far) + `input_poll_inflight=0` ⇒ lost-wake (scan returned
  Pending, never re-woken).
- `STUCK-INSIDE-POLL` + `input_poll_inflight=1` *persisting* ⇒ a synchronous block inside the poll — the
  `ChunkStream` inner `Mutex` or the sync `execute_arrow` decode never returning (NOT a lost-wake). Only ever
  seen transient so far.
- `SORTEXEC-NOT-POLLING` ⇒ SortExec parked downstream (sink/upload backpressure), not the scan.

---

## 2026-07-20 — FIRST FULL NATIVE BACKTRACE CAPTURED (CI run 29717858996) — deletion-index rcu livelock

The dumper build's in-process all-thread native backtrace fired (reason: `stall table=order_line
phase=await-compaction-lock`). ~300 threads symbolized cleanly. Findings:

**No lock deadlock.** ZERO `parking_lot::…::RawMutex::lock`/`RawRwLock::lock`/`lock_slow` frames across all
~300 threads. The `await-compaction-lock` is an async lock (its waiter parks as an idle tokio task, no lock
frame). ⇒ the goal's "RawRwLock/RawMutex ⇒ lock deadlock" branch is ELIMINATED for this stall.

**The one working thread** (`comm="compaction-work"`, tid 25264) — everything else idle (289 parked tokio
workers `condvar::wait→multi_thread park_condvar`, 4 idle io-drivers `epoll_wait`, main in `block_on`):
```
KeyDeletionIndex::prune_deletes_at_or_below
arc_swap::ArcSwapAny::rcu
CayenneTableProvider::prune_deletion_index_at_or_below            (table.rs:17215 / :17227)
CayenneTableProvider::bake_seq_prefix_protected_snapshots         (table.rs:15772)
<CayenneTableProvider as CompactionRunner>::run_compaction_trigger   (holds compaction_lock)
BackgroundCompactor::spawn
→ tokio blocking-pool
```

**Mechanism (code-grounded).** `prune_deletion_index_at_or_below` prunes the tombstone index via
`deletion_snapshot.rcu(|current| { let pruned = current.tombstones.prune_deletes_at_or_below(cutoff);
Arc::new(Snapshot::from_index(pruned)) })`. `arc_swap::rcu` (1.9.1) **re-runs the closure on every CAS
failure**: `loop { let new = f(&cur); let prev = compare_and_swap(cur,new); if ptr_eq {return} else {cur=prev} }`.
The closure is an **O(N) rebuild of the ~30M-entry `KeyDeletionIndex`**. Under concurrent CDC writes to the
same `deletion_snapshot` (OLTP delete load constantly appends tombstones), the expensive prune can never win
the CAS ⇒ **rcu livelock**: unbounded retry, CPU-bound, holding `compaction_lock`, blocking the `order_line`
promotion stuck at `await-compaction-lock`. Racy by nature (livelocks only when prune-duration exceeds the
inter-write interval), matching the ~60% rate. The scanpark counters in the same dump were also anomalous:
`in_flight=1650`, `stuck_before_project=70006` (task bodies suspended in projection_evaluation) — a *different*
signature from the prior drain-side lost-wake (`completed≈spawned`).

**HONEST SCOPE CAVEAT.** This run **became ready** (05:37:59) then FAILED the **convergence** gate
(`replication did not converge`), and showed **0 POLLED-THEN-PENDING**. So this capture is the
compaction/convergence stall mode — the deletion-index rcu livelock holding `compaction_lock` and starving
promotion throughput — which is DISTINCT from (and may or may not share a cause with) the never-ready
`POLLED-THEN-PENDING` scan freeze the goal targets. It does NOT by itself close the never-ready-freeze goal.
Open question: does the same rcu livelock also drive the never-ready freeze (e.g. via the transient
30M-entry allocations each rcu retry churns, pressuring the pool and starving the scan/sort)? Needs a
POLLED-THEN-PENDING capture to compare the working-thread stack.

**Candidate fix (independent of the freeze question — this is a real livelock bug).** Do NOT run an O(N)
prune inside `arc_swap::rcu`. Options: (a) compute the pruned index once under a short exclusive lock / a
single `swap` (accept one lost concurrent update and re-derive), (b) make the prune incremental/cheap, or
(c) debounce/skip the prune when a bake ran recently. arc-swap's own docs warn `rcu` is only appropriate for
cheap closures.

---

## 2026-07-20 — TARGET NEVER-READY FREEZE CAPTURED WITH NATIVE BACKTRACE (CI run 29717944124)

Second native-backtrace capture — this one the **goal-target freeze**: `Spice runtime is ready`=0 (NEVER
ready), `POLLED-THEN-PENDING`×48 sustained 06:12:47→06:36:18 (~24 min), backtrace fired at 06:12:48 reason
`stall table=order_line phase=write-cold-store-scan-sort-encode-upload`. Run errored never-ready at 06:36:48.

**Freeze-moment metrics (06:12:47):** phase=write-cold, in_phase_s=199, `progress_delta_tick=0`,
`input_delta_tick=0`, `input_poll_entered_delta=0`, `sort_input_diag=POLLED-THEN-PENDING`,
`encode_permits=48/48` (idle), `compaction_pool_used=6.99GB / 30.25GB` (**~23%, NOT exhausted**),
`mem_tier_used=455MB`. Scanpark: `completed==spawned=41045, in_flight=0, backlog=120, stuck_before_project=0`.

**The backtrace — ~300 threads, 0 lock frames (NO deadlock), runtime overwhelmingly idle** (286 parked
workers `condvar→multi_thread park_condvar`, 5 io-drivers, main block_on). Only THREE CPU-active threads, ALL
on the **deletion-index / tombstone machinery**:
1. `compaction-work` — `KeyDeletionIndex::prune_deletes_at_or_below ← arc_swap::rcu ← prune_deletion_index_at_or_below ← bake_seq_prefix_protected_snapshots ← run_compaction_trigger ← BackgroundCompactor::spawn` (**identical to the convergence-mode capture 29717858996**).
2. `cdc-apply-worke` — `SparseChunk::drop → Arc::drop_slow → … → drop_in_place<InMemTombstones>` inside `AppendMutationWriter::write_cdc_pipelined ← process_upsert_batch` — **freeing a huge `InMemTombstones`** (recursive O(N) free of an imbl/`sized_chunks` structure).
3. `cdc-apply-worke` — another table's bootstrap snapshot (`BootstrapBuilder::append_from_row`) — progressing, expected.

The order_line promotion itself (holding write_lock) is parked (suspended await) among the idle workers.

**REVISED root-cause picture (ground-truth, corrects the prior pure "scan lost-wake" theory):**
- **NOT a lock deadlock** — 0 `RawMutex`/`RawRwLock`/`lock_slow` frames across ~300 threads (both captures).
- **NOT a hard scan lost-wake** — the scan **progresses**: `spawned` grew 41045 (06:13) → 267565 (06:36), and
  `Bounded sort run complete` events fire throughout (06:11,06:12,06:14,06:16,06:17…). `backlog=120` is normal
  `buffer_unordered` pipeline depth on a progressing scan, not undrained-lost-wake. `POLLED-THEN-PENDING` is a
  SYMPTOM: the SortExec reads a huge input that is intermittently Pending between spilling runs; the 30s
  watchdog samples it mid-Pending.
- **NOT memory/backpressure** — pool ~23%, encode 48/48 idle, 286 idle workers (no starvation).
- **The actual bottleneck is the deletion-index / tombstone machinery**, dominated by
  `prune_deletion_index_at_or_below` under `arc_swap::rcu`: `deletion_snapshot.rcu(|cur| Snapshot::from_index(
  cur.tombstones.prune_deletes_at_or_below(cutoff)))`. arc_swap::rcu (1.9.1, source-confirmed) **re-runs the
  closure on every CAS failure**; the closure is an O(~30M-entry) `KeyDeletionIndex` prune+rebuild. Under
  concurrent CDC tombstone writes to the same `deletion_snapshot` it livelocks / retries unboundedly (and the
  paired `InMemTombstones` drop is an O(N) recursive free of the same huge structure). This CPU-heavy churn,
  concurrent with the order_line cold-promotion's already-expensive multi-run spilling Z-order sort (which
  holds write_lock the entire time), makes the promotion so slow it never completes within the ready_wait
  budget ⇒ ingest blocked behind write_lock ⇒ spiced never ready.

**PRIMARY, code-level, actionable bug (appears in BOTH captures; confirmed antipattern):**
`CayenneTableProvider::prune_deletion_index_at_or_below` (table.rs:17215 / :17227) runs an O(N) prune+rebuild
of a ~30M-entry index inside `arc_swap::rcu`. arc-swap's own docs restrict `rcu` to cheap closures. FIX: do
the prune once under a short exclusive lock (single `store`/`swap`, accept & re-derive one lost concurrent
update), or make the prune incremental, or debounce it. This removes the livelock and the transient
30M-entry per-retry alloc/free churn.

**Honest residual.** This capture shows slow-grind + tombstone-machinery CPU churn, not a proven *hard*
permanent lost-wake. Some earlier counter-only captures (e.g. 29670291865) showed `input_delta_tick=0`
sustained (scan input truly frozen) — possibly a distinct harder sub-mode. Confirming the fix is the decisive
next step: patch the rcu prune and measure whether the never-ready rate collapses. If a hard-frozen run
(counters truly frozen many ticks) is caught, its backtrace should be compared against this one.

### Causal link tightened (2026-07-20) — the rcu write-contention is real

Confirmed the `deletion_snapshot` ArcSwap has BOTH frequent-cheap writers on the hot CDC-apply path AND the
one expensive prune writer, all via `rcu` on the *same* ArcSwap:
- `commit_on_conflict_deletion_update` (table.rs:11137) — every upsert-with-conflict apply batch.
- `publish_staged_key_deletion_cache` (table.rs:10654), `update_file_deletion_cache` (9456),
  `upgrade_tombstones_for_flushed_pks` (9582) — apply/flush path.
- `prune_deletion_index_at_or_below` (table.rs:17215/17227) — the EXPENSIVE O(~30M) prune (compaction path).

This is the textbook `arc_swap::rcu` livelock precondition: a **rare expensive** closure competing against
**frequent cheap** swaps on one ArcSwap. Under active CDC ingest, the cheap apply-path rcus keep swapping
`deletion_snapshot`, so the prune's expensive closure never wins its CAS and retries unboundedly — exactly
what the two native backtraces caught the `compaction-work` thread doing. Mechanism now corroborated from
three independent angles: the ground-truth backtraces, the confirmed arc-swap 1.9.1 retry semantics, and this
writer-contention pattern. Only the fix experiment (measure never-ready-rate drop) remains for literal 100%.

---

## 2026-07-20 — CORRECTION: two distinct mechanisms; the rcu theory does NOT explain the never-ready freeze

The two 2026-07-20 native-backtrace sections above over-attributed the never-ready write-cold freeze to the
`arc_swap::rcu` deletion-index prune. That is wrong, caught by a lock-mutual-exclusion argument:

- `promote_warm_to_cold_inner` takes `compaction_lock.lock().await` (table.rs:14717) THEN `write_lock`
  (14760) and holds BOTH across the whole graduation incl. write-cold-scan-sort.
- `bake_seq_prefix_protected_snapshots` takes `compaction_lock.try_lock()` and SKIPS if held.
- They are separate per-table workers (`BackgroundColdTierPromoter::promote_warm_to_cold` vs
  `BackgroundCompactor::run_compaction_trigger` → bake), sharing the per-table `compaction_lock`.

⇒ While a table's promotion is frozen in write-cold (holding `compaction_lock`), that SAME table's bake
CANNOT run (already established here: "No `order_line` compaction runs again until after the freeze —
promotion ⟂ compaction per table"). So the `compaction-work → bake → prune → rcu` thread in the never-ready
capture (29717944124) is a DIFFERENT table's `BackgroundCompactor` — concurrent, NOT the cause of and NOT
blocking the order_line scan freeze. Its appearance in that backtrace is a red herring for the never-ready case.

**Two distinct problems (previously conflated because the same bake/rcu thread appeared in both dumps):**
- **A — never-ready freeze** (29717944124, phase `write-cold-store-scan-sort-encode-upload`): the PROMOTION
  holds compaction_lock+write_lock and its SCAN returns Pending (`POLLED-THEN-PENDING`). This is the original
  shape-A wedge and remains the open puzzle — the bake/rcu livelock does NOT explain it.
- **B — convergence stall** (29717858996, phase `await-compaction-lock`): a BAKE got compaction_lock first
  and livelocks in `prune_deletes_at_or_below ← arc_swap::rcu`, blocking the promotion at await-compaction-lock.
  This is a real, code-located, second bug (table.rs:17215/17227) — but it is NOT the never-ready freeze.

**Refocus for the 2 more never-ready traces:** inspect the `BackgroundColdTierPromoter`/promotion thread and
its scan tasks' parked location — NOT the compaction-work thread. The never-ready cause is still "why does the
promotion's scan return Pending / stop producing," per the shape-A analysis.

---

## 2026-07-20 — NEW LEAD (user): the cold-tier promotion runs on a NICE-10 low-priority runtime

Traced the runtime the cold-tier promotion executes on:
- `bin/spiced/src/lib.rs`: `compaction_runtime = ManagedTokioRuntime::builder().with_low_priority()
  .with_thread_name("compaction-worker")` ⇒ **nice 10** (`runtime-async/src/lib.rs` build(): sets nice 10
  for low_priority; `worker_threads = num_cpus-1`, so NOT thread-limited — priority-limited).
- `BackgroundColdTierPromoter` "Runs on the shared low-priority compaction runtime" (compaction.rs:870) →
  `run_cold_tier_promotion_tick → promote_warm_to_cold`. So the whole write-cold scan/sort/upload runs on
  nice-10 threads WHILE holding `write_lock`. (`comm="compaction-work"` in both backtraces confirms.)
- Sibling runtimes: `cdc_apply_runtime` = nice 0 (100 OLTP terminals), `cpu_runtime` = default (queries),
  `refresh_runtime` = nice 10.

**Reframed hypothesis for the never-ready freeze (fits the evidence better than any lost-wake):** under the
SF1000 load the nice-0 cdc-apply + queries saturate the cores; the Linux scheduler starves the nice-10
compaction runtime, so the promotion's scan/sort gets little CPU → the scan returns Pending and isn't
advanced for long stretches → `POLLED-THEN-PENDING` / `input_delta_tick=0`. This is INDISTINGUISHABLE at the
watchdog level from a lost wakeup, but the wake isn't lost — the runtime just isn't scheduled. Explains:
scan progressed slowly (41K→267K, not a hard hang); 0 lock frames; no lost-wake ever found in source (there
isn't one); racy ~60% (depends on load saturating the box during the bootstrap promotion); write_lock held
for tens of minutes → never ready.

**Tension to resolve:** the dumps showed ~286 idle-parked workers — if cores were truly idle a nice-10
thread should run, so this may be BURSTY starvation + an already-slow spilling sort, not continuous
starvation. Next never-ready traces: check the `compaction-worker` threads' state — on-CPU/runnable
(⇒ starvation) vs parked-in-condvar with the scan idle (⇒ different cause).

**Cheap, non-correctness-sensitive TEST (unlike the deletion-index fix):** drop `.with_low_priority()` on
the compaction runtime (nice 0, matching cdc_apply) or add CPU headroom, and re-run — if the never-ready
freeze vanishes, it was scheduler starvation of the low-priority promotion runtime.

---

## 2026-07-20 — MECHANISM IDENTIFIED: Mode-X drain-side lost-wake (per-scan build)

Combined per-scan+bypass build (vortex 2d48c7a4d), run 29775386650 (branch perscan). Run became READY then
failed CONVERGENCE (POLLED-THEN-PENDING=0 — not the permanent never-ready freeze this run), but the per-scan
counters captured the mechanism across the run's scans:

- **Mode-X DOMINANT: 772 `Mode-X drain lost-wake (tasks done, consumer never re-woken)` vs 91
  `Mode-Y stuck task bodies`.** (in_flight==0 && backlog>0 frozen = tasks completed but undrained.)
- **`custom_labels` bypass EXONERATED (chain step 9):** Mode-X persists WITH the bypass ⇒ the async-spawn
  label wrapper is not the cause.
- Stalls here were TRANSIENT (max stalled_ticks=6 ≈ 30s) and self-resolved ⇒ run reached ready.

**Interpretation — matches the permanent-freeze signature.** The drain-side lost-wake fires constantly
(hundreds of times) at the `buffer_unordered`/`FuturesUnordered` → consumer edge: a spawned split-read
completes but the consumer (the scan stream feeding SortExec) is not re-woken to drain it. It normally
self-resolves because a LATER task completion re-wakes the consumer — but when the FINAL batch completes with
nothing left in-flight to re-trigger the wake, the drain wake is lost permanently ⇒ `POLLED-THEN-PENDING`
forever. This is exactly the earlier frozen captures (`completed==spawned`, `backlog>0`, `in_flight=0`).

⇒ **Root cause localized to chain step 6: a drain-side lost-wake in `buffer_unordered`/`FuturesUnordered`
over `handle.spawn` join-futures** (vortex scan_builder.rs:428). NOT Mode-Y (stuck reads), NOT custom_labels
(step 9), NOT a lock, NOT scheduling. Confirm on a truly-frozen (never-ready) run: expect one Mode-X scan
with HIGH stalled_ticks (never resolves) = the permanent drain-lost-wake.

### 2026-07-20 — futures-rs prior art: FuturesUnordered missed-wakeup is a known class

Searched rust-lang/futures-rs (we use futures-util 0.3.32). Our Mode-X drain-lost-wake matches a documented
FU footgun family:
- #1006 "FuturesUnordered and missed wakeups" — race: node notify does `if !queued.swap(true) { enqueue;
  parent.wake() }` ⇒ SKIPS parent.wake when it thinks the node is already queued ⇒ "the parent never learns
  about the ready state." Exactly our symptom (result enqueued, consumer never re-woken). CLOSED (fixed via
  unconditional wake) — so the exact 2018 race is gone in 0.3.32, but the pattern is identical.
- #2387 "Footgun lurking in FuturesUnordered…" — demonstrated WITH buffer_unordered()/buffered(); its
  recommended MITIGATION is exactly our code shape: "Spawning the futures before putting them in the
  sub-executor works because the main executor then drives them." We handle.spawn before buffer_unordered ⇒
  inner tasks DO complete (that half mitigated) ⇒ our failure is the DRAIN wake, not not-driven.
- #131 (wg-async), #2047 (FU can block executor / starvation, jonhoo PR #2049 yield-limit) — FU wake/poll
  behavior broadly acknowledged as fragile.

MAP: our pattern = FU (buffer_unordered) wrapping join-futures of INDEPENDENTLY-spawned tasks ⇒ inner
join-future becomes ready at arbitrary time vs FU poll/queued state = the regime where conditional-parent-wake
races live. Evidence (772 transient Mode-X + permanent tail) is textbook. CAVEAT: no OPEN 0.3.32 issue
reproduces it exactly (#1006 fixed) ⇒ ours is a residual/variant of the same class, so the fix is STRUCTURAL
on our side, not a futures-rs patch.

FIX DIRECTION: decouple the drain from FU's parent-wake — spawned split-read tasks send results into a bounded
`tokio::mpsc`, consumer reads the channel (reliable sender→receiver wake); or keep buffer_unordered but
guarantee a final wake. Sidesteps the missed-parent-wake class entirely. (Confirmation still: a frozen-run
per-scan line with a high-stalled_ticks unresolved Mode-X scan.)

### 2026-07-20 — combined run #2 (29775396230): mechanism reconfirmed + a permanence signal to watch

Ready + convergence-fail again (POLLED-THEN-PENDING=0). Per-scan: Mode-X dominant 1087 vs 86. Several scans
(9020-9025) reached stalled_ticks=8 (~40s) with the EXACT permanent-freeze signature (spawned==completed,
in_flight=0, backlog=53-63) then RESOLVED → ready. ⇒ drain-lost-wake reaches the freeze signature but got
rescued. Mechanism (Mode-X) reconfirmed.

TALLY (custom_labels bypass ON): 2/2 combined runs READY (POLLED-THEN-PENDING=0), Mode-X stalls transient
(≤40s). Baseline-ready rate is ~40-50% (freeze ~50-60%). 2/2 ready is only suggestive. REFINE the "bypass
exonerated" note: the bypass does NOT stop Mode-X occurring (persists ⇒ not the cause of the drain-lost-wake
itself), BUT it may affect whether the TAIL stall resolves vs becomes permanent (custom_labels could strand
the final task whose completion would otherwise rescue the drain). Need the last 2 runs (4/4 ready would
strengthen) + a baseline-with-per-scan comparison to separate "bypass helps permanence" from variance.

### 2026-07-20 — CONFIRMATION: run #3 (29775405842) FROZE — Mode-X confirmed + custom_labels exonerated

Combined per-scan+bypass binary. Run FROZE (ready=0, not ready within 1800s, POLLED-THEN-PENDING×95) = target
never-ready freeze. AGGREGATE scanpark at freeze: `spawned==completed=27154, in_flight=0, backlog=15-191`
(spawned FROZEN, backlog slowly draining) = Mode-X drain-lost-wake signature (all tasks done, last batch
undrained). Matches every prior frozen capture (29717944124: completed==spawned, backlog=120). During a
bootstrap never-ready freeze there is no query load ⇒ aggregate ≈ the promotion scan.

RESULTS:
1. Mode-X drain-lost-wake CONFIRMED at a real freeze (aggregate: completed==spawned + in_flight=0 + backlog>0).
2. `custom_labels` bypass EXONERATED DEFINITIVELY (chain step 9): run FROZE WITH the bypass on ⇒ bypass does
   not fix it; the earlier 2/2 ready runs were baseline variance, not the bypass helping. Tally (bypass on):
   2 ready + 1 frozen.
3. PER-SCAN INSTRUMENTATION GAP: per-scan lines maxed at stalled_ticks=6 (did NOT isolate a permanently-stuck
   scan). Likely the frozen promotion INPUT scan reaches the sort via `execute_stream(cold_plan)`
   (table.rs:7347, a DataFusion VortexSource plan) rather than directly through the per-scan-instrumented
   vortex `LazyScanStream`, so per-scan captured only OTHER scans. The aggregate scanpark + cayenne stall-diag
   (POLLED-THEN-PENDING) capture the promotion. (Follow-up if a per-scan promotion isolation is wanted: also
   tag the VortexSource/opener read path, not just LazyScanStream.)

NET ROOT CAUSE (multi-angle, high confidence): **Mode-X drain-side lost-wake in the vortex
`buffer_unordered`/`FuturesUnordered` over `handle.spawn` join-futures (scan_builder.rs:428)** — completed
split-reads not drained because the consumer's parent-wake is lost; transient (rescued by later completions)
until the TAIL batch where nothing rescues it ⇒ permanent POLLED-THEN-PENDING ⇒ write_lock held ⇒ never ready.
Ruled out: lock deadlock, Mode-Y stuck-reads, custom_labels (step 9), scheduling/nice-10, oneshot join layer.
Matches the documented futures-rs FU missed-parent-wake footgun class (#1006/#2387). FIX: decouple the drain
from FU's parent-wake (channel-based result delivery) or guarantee a final wake.

## Fix-test #1 — kick-in self-wake at RunInputStream (FALSIFIED 2026-07-21)

Branch `sgrebnov/0716-cold-stall-perscan`, binary `c4d0dcd1db`. Changes under test:
- RunInputStream: on downstream `Poll::Pending`, arm a 1.5s `tokio::time::sleep` self-wake ("kick-in")
  so the promotion's sort input is re-polled even if the scan never wakes it.
- write-side ChunkStream removed (single unchunked cold write); PK bloom size cap removed (any-size bloom).

**Result: still FROZE.** Run 29799199984 (SF1000): `Error: Spiced instance not ready within 1800s`.
`order_line` cold-promotion stuck in `write-cold-store-scan-sort-encode-upload`, `progress_delta_tick=0`,
`input_delta_tick=0`, `sort_input_diag="POLLED-THEN-PENDING …"`, `input_poll_inflight=0`.

**What the kick fired but did not fix.** `input_poll_entered_delta=20`: the input was re-polled ~20×/tick
(≈ the 1.5s kick firing across a 30s watchdog tick), yet each re-poll the scan returned `Pending`. So a
re-poll from *above* the scan (RunInputStream) is confirmed unable to recover this freeze — the kick-in
approach is dead. The reason is now proven below: the scan is a bystander; nothing above it can help.

**DEFINITIVE native backtrace — this binary DOES fire the in-process all-thread backtrace on stall, and run
29799199984 (the never-ready freeze) captured it.** Of 300 sampled threads, the ONLY actively-working stack
was:
`BackgroundCompactor::spawn → bake_seq_prefix_protected_snapshots → prune_deletion_index_at_or_below →
KeyDeletionIndex::prune_deletes_at_or_below → arc_swap::ArcSwapAny::rcu`.
Every other thread was idle/parked. **No** vortex scan / `buffer_unordered` / FuturesUnordered / `bounded_sort`
/ `FileSegmentSource` / `RunInputStream` frame was active anywhere. So the never-ready freeze IS the
deletion-index `arc_swap::rcu` livelock (identical to the 2026-07-20 captures and to run 29799205731 below) —
the scan's `POLLED-THEN-PENDING` / Mode-X signature is a downstream BYSTANDER symptom: the scan returns
Pending because its consumer (the cold-promotion driver, which is the wedged `rcu` thread) never returns to
pull. This closes the "vortex scan lost-wake vs deletion-index" question: it was always the deletion-index;
the vortex scan-park / bounded_sort-run-boundary / FuturesUnordered theories were red herrings.

**Companion capture, run 29799205731 (READY + converged 344s, but HTAP gate FAILED 1/22 queries diverged).**
Same native stack at a transient `phase=await-compaction-lock` stall (`arc_swap::rcu` ×3,
`prune_deletes_at_or_below` ×3, `bake_seq_prefix_protected_snapshots` ×3) concurrent with
`write_cdc_pipelined` / `process_upsert_batch` and `Arc::drop_slow` (the O(N) HAMT drop). Adds the amplifier
datum: `order_line` `deletion_index_len` grows **47.7M → 65.8M** over ~10 min — the unbounded tombstone growth
that makes each O(N) `rcu` prune + drop pathological. This is the "ready but data-diverges" manifestation of
the one root cause; the freeze (run 1) is the "never ready" manifestation.

**Run 29799211973 (FROZE, not ready within 1800s, `write-cold-store` phase) — a SECOND O(N) code path.**
Its native backtrace caught a *different* sole-active stack:
`BackgroundMemTierCheckpointer::spawn → run_mem_tier_checkpoint_tick → checkpoint_mem_tier_inner →
checkpoint_corpus_pk_keys → hashbrown::HashMap::insert`. So this freeze's active thread is the mem-tier
checkpoint building the corpus PK-key set (an O(N) hashbrown build over the same tens-of-millions PK volume),
not the deletion-index `rcu` prune. Single sample, so not a proven livelock — but it broadens the root cause
from "specifically the `rcu` prune" to the general theme: **O(N) background compaction/checkpoint work over
an unbounded PK/tombstone volume (tens of millions of keys) holding `compaction_lock`/`write_lock` and blocking
readiness.** The `rcu` prune is the dominant/most-sampled path (runs 1, 2, and both 2026-07-20 captures); the
corpus-PK checkpoint is a sibling. Any fix that only fixes the `rcu` prune but leaves the PK/tombstone volume
unbounded may still stall here — so capping/bounding the tombstone+PK volume (option c) is the more complete
fix than serializing `rcu` alone.

**★ TWO-MODE CORRECTION (2026-07-21, after proper per-thread backtrace parse — supersedes the single-mechanism
claim below).** A per-thread parse of run 29799199984's native backtrace (the fix-test never-ready freeze)
shows: **294 threads idle/parked, 5 threads in `block_on` with NO app frames (watchdog dump machinery + idle
blocking-pool), and exactly 1 on-CPU thread — the bake `arc_swap::rcu` prune.** So the compaction runtime is
IDLE, not saturated — which FALSIFIES the "cross-table CPU starvation" story in the paragraph below. With the
runtime idle, the promoted table's scan is not CPU-starved; the one active bake `rcu` is on a DIFFERENT table
(lock exclusion) holding that table's (per-table) `listing_fence.write()`, so it neither locks nor starves
order_line's scan; and the promotion reads committed warm data, not live CDC. The order_line promotion scan is
therefore a **suspended async task, un-woken for 27 min on an idle runtime** (kick-in re-poll recovered
nothing → lost-wake below the re-poll point). That is the goal's **mechanism (1)** — a lost-wake in the
`buffer_unordered`/runtime composition — invisible to native backtraces because a suspended async task sits on
no OS-thread stack. CONCLUSION: there are (at least) TWO never-ready sub-modes, and the earlier
"mechanism C is THE root cause of the freeze" OVER-COLLAPSED them:
- **Mode 1 (run 29799199984, this fix-test freeze):** idle runtime + suspended un-woken promotion scan →
  mechanism (1), lost-wake in the scan's `buffer_unordered`/runtime composition (the classic POLLED-THEN-PENDING).
- **Mode 2 (data-divergence runs 2/4/5 + the busier 2026-07-20 capture run 29717944124, which caught bootstrap
  `append_from_row` + CDC drop + prune `rcu` all active):** `arc_swap::rcu` write-starvation churn on the
  deletion index throttling CDC/ingest convergence → mechanism C.
The `rcu` livelock (mechanism C) is real and code-grounded, but it is NOT established as the cause of the Mode-1
never-ready freeze. Everything from here down about mechanism C applies to Mode 2; treat the Mode-1 freeze as
mechanism (1).

**★ MODE-1 MECHANISM NAMED (2026-07-21, static analysis of vortex pin 2d48c7a4d — matches the goal's
mechanism (1)).** The frozen promotion scan is parked in this composition (all code-verified):
`vortex-layout/src/scan/scan_builder.rs:422-436` —
`stream::iter(tasks).map(|t| handle.spawn(async { let r = t.await; COMPLETED++; r })).buffer_unordered(concurrency)`,
where `handle.spawn` (`vortex-io/src/runtime/handle.rs:67-95`) returns `Task<R>` = a **raw `oneshot` crate**
`AsyncReceiver` (handle.rs:217-218; the spawned tokio future `send.send(output)`s the result), and the whole
thing is consumed by cayenne `RunInputStream → SortExec` parked on `LazyScanStream::poll_next` (scan_builder.rs:454,
= `buffer_unordered.poll_next`). Applying the goal's decision rule to run 29799199984: POLLED-THEN-PENDING +
counters frozen + never ready ✓; parked at an `.await`, NO lock, NOT park_internal ✓ (runtime idle — 294 parked,
0 RawRwLock/RawMutex frames); the counter signature `spawned==completed, in_flight==0, backlog>0` proves every
spawned task body finished but some completed chunks were never drained downstream → the wake is lost in the
`buffer_unordered`/`FuturesUnordered` join+drain, i.e. **a lost-wake in the buffer_unordered/runtime composition,
NOT the scan bodies** — exactly the goal's mechanism (1). Sub-grain (finer than mechanism (1) requires): either
(i) FuturesUnordered parent-wake footgun (futures-rs #1006, `if !queued.swap(true){parent.wake()}` drops the
parent wake on cross-thread child completion) or (ii) the raw `oneshot` crate receiver-waker drop (#6221/#8830
class — handle.rs uses the raw `oneshot` crate). The kick-in null result (re-poll reached `FuturesUnordered::
poll_next` yet drained nothing ⇒ the completed child was never enqueued to FU's ready-queue) leans toward (ii);
the goal's "(not oneshot)" annotation leans (i). Both are "lost-wake in the buffer_unordered/runtime
composition." Definitively pinning (i) vs (ii) needs async-task instrumentation or the fix-test discriminator.

**[SUPERSEDED — see two-mode correction above] LOCK ANALYSIS (2026-07-21) — three ops, three disjoint lock
sets (all code-verified):**
- **Cold promotion** (`promote_warm_to_cold_inner`, 14628): holds `compaction_lock` + `write_lock` for the
  whole pass (incl. the write-cold-store scan). Its scan only `load()`s the deletion snapshot (cheap).
- **Seq-prefix bake** (`bake_seq_prefix_protected_snapshots`, 15719): `compaction_lock` (try_lock) +
  `listing_fence.write()`; does the O(N) `prune_deletes_at_or_below` `rcu` at 16077. Does NOT take `write_lock`.
- **CDC-apply** (`update_file_deletion_cache`, 9456): `write_lock` + `scan_state_lock.write()` (11259); does
  the `extend_max_deletes` `rcu`.

Consequences: (i) for the table being PROMOTED, both bake (needs `compaction_lock`) and CDC-apply (needs
`write_lock`) are EXCLUDED — so the promoted table's own deletion index is NOT the contended one (my earlier
"order_line prune vs order_line CDC-apply on the same ArcSwap" was WRONG). (ii) On any table NOT being
promoted, bake (`compaction_lock`+`listing_fence`) and CDC-apply (`write_lock`+`scan_state_lock`) have DISJOINT
lock sets → they run concurrently and both `rcu` the same lock-free `deletion_snapshot` → that is where the
O(N) prune loses its CAS to the fast CDC writer. (iii) **Causal chain for order_line never-ready:** other
tables' bake `rcu` churn (O(65M) prune + CAS contention) pegs threads on the SHARED `compaction_runtime`
(nice-10) → at SF1000 several tables do this at once → runtime CPU saturates → order_line's promotion (holding
order_line `write_lock`) runs its write-cold-store scan on that saturated runtime → scan split-reads are
CPU-STARVED → `POLLED-THEN-PENDING` → promotion never finishes → order_line ingest (needs `write_lock`) blocked
→ never ready. This is why exp1 (nice-0) / exp2 (dedicated cold-promo runtime) were on the right track: the
scan runs on the ambient nice-10 compaction runtime. FIX implication: bounding tombstone/PK volume (option c)
attacks the O(N) root across all tables; additionally, running the promotion scan off the saturated shared
compaction runtime (dedicated/priority runtime) directly addresses the starvation.

**EXACT MECHANISM — `arc_swap::rcu` write-starvation LIVELOCK (code-grounded, 100% clear).** The goal posed
two hypotheses (lost-wake at an `.await` in `buffer_unordered`, or a `RawRwLock`/`RawMutex` deadlock); the
native stack rules out BOTH — the answer is a third category. NOTE per the lock-analysis correction above, the
`rcu` contention is on a table OTHER than the frozen/promoted one; the promoted table's freeze is the
downstream CPU-starvation consequence.

- **Shared lock-free object:** per-table `deletion_snapshot: ArcSwap<…DeletionSnapshot>`.
- **Contender A (compaction bake):** `bake_seq_prefix_protected_snapshots → prune_deletion_index_at_or_below`
  → `deletion_snapshot.rcu(|cur| Arc::new(from_index(cur.tombstones.prune_deletes_at_or_below(cutoff))))`
  (table.rs:17162). The closure is **O(N)**: `prune_deletes_at_or_below` (deletion_index.rs:728) fuses every
  key across all runs+active into a fresh `HashMap`, then rebuilds a fresh survivors map + bloom over all N
  entries — N ≈ **65M** tombstones at freeze — and the discarded map is an **O(N) drop** (`Arc::drop_slow` /
  `drop_in_place<InMemTombstones>`, seen in the run-2 backtrace).
- **Contender B (CDC apply, continuous under SF1000 OLTP):** `write_cdc_pipelined` / `process_upsert_batch` →
  `update_file_deletion_cache` (table.rs:9456/9470), `commit_on_conflict_deletion_update`,
  `publish_staged_key_deletion_cache`, `upgrade_tombstones_for_flushed_pks` — all `deletion_snapshot.rcu(|cur|
  extend_max_deletes(cur))` on the **same** ArcSwap, at high write rate.
- **The livelock:** `arc_swap::rcu` is `loop { cur = load(); new = f(cur); if CAS(cur → new) break }`. Because
  A's `f` is O(65M) (long), a B writer almost always commits in the window between A's `load` and A's `CAS`,
  so A's CAS fails and A **re-runs its O(N) rebuild + O(N) drop** — unboundedly. Classic RCU writer starvation
  under a fast concurrent writer.
- **Proof it is a livelock, not one slow prune:** a single O(65M) rebuild is ~seconds; run 29799199984 stayed
  in this exact frame for **1612 s (27 min)** ⇒ hundreds–thousands of re-executions ⇒ retry livelock (the
  native sample caught the thread ON-CPU inside `arc_swap::rcu → prune_deletes_at_or_below`, runnable — not at
  a park, not at an `.await`, not in any lock).

**On the goal's two hypotheses — (2) is definitively excluded; (1) is not the observed mechanism but cannot be
excluded by native backtraces, and is not needed to explain the freeze.**

- **(2) `RawRwLock`/`RawMutex` deadlock — DEFINITIVELY EXCLUDED.** `arc_swap` is lock-free; no
  `RawRwLock`/`RawMutex`/`parking_lot` frame appears in any of the 4 captured stacks. The wedged op holds the
  async `compaction_lock`+`write_lock`, but it is a **lock-HOLDER livelock**, not a blocked lock-acquire and
  not a lock cycle — categorically not a deadlock.
- **(1) lost-wake at an `.await` in `buffer_unordered` — NOT the observed mechanism, but a native backtrace
  cannot confirm or refute it.** A native all-thread backtrace only captures OS threads that are running or
  blocked; a tokio task that returned `Pending` is **suspended off-CPU and appears on no thread stack**. The
  promotion's scan is exactly such a suspended task (that is why run 1's backtrace shows *zero* `bounded_sort`
  / `SortExec` / `RunInputStream` / `buffer_unordered` / `FileSegmentSource` frames — only the watchdog's
  `sort_input_diag` log string, which is cayenne-side async-state instrumentation, not a stack frame). So the
  native evidence can neither prove nor disprove a lost-wake inside the suspended scan. What the evidence DOES
  show: (i) the only on-CPU non-idle thread is the `arc_swap::rcu` livelock; (ii) the kick-in re-poll (20×/tick
  at RunInputStream) recovered nothing, so any lost-wake is not at the RunInputStream boundary (it could only
  be a stranded FuturesUnordered child below it — unreachable by a parent re-poll — which remains a formal
  possibility); (iii) the freeze is fully explained by the rcu livelock starving the shared deletion-index /
  CDC-apply path without invoking any scan lost-wake. Confirming or excluding (1) at the async-task level would
  need task-level wake instrumentation of the suspended scan — established infeasible here (2026-07-17: tokio
  taskdump times out "outside an await point" because vortex bridges async→sync with `block_on`).

**Net:** the definitely-present, on-CPU-proven, code-grounded root cause is the `arc_swap::rcu` write-starvation
livelock (mechanism C). It is sufficient to explain the never-ready freeze and the data divergence. The goal's
(2) is excluded; its (1) is neither the observed mechanism nor necessary, though it cannot be formally excluded
because suspended async tasks are invisible to native backtraces.

**Conclusion.** Root cause CONFIRMED across four independent native captures (two on 2026-07-20, plus runs
29799199984 and 29799205731 on 2026-07-21): the deletion-index (`KeyDeletionIndex` over an `im::HashMap` HAMT)
`arc_swap::rcu` livelock — compaction's O(N) prune closure repeatedly loses its CAS to the high-rate CDC-apply
writers on the same `ArcSwap`, re-running an O(N) rebuild + O(N) drop each retry, while the tombstone index
grows into the tens of millions. It holds `compaction_lock`/`write_lock`, so ingest blocks → never ready (run
1), or CDC drainage stalls → data diverges (run 2). The kick-in-at-RunInputStream fix is FALSIFIED (it targets
a bystander). FIX must address the `rcu` livelock itself — options in the memory `cold-tier-stall-diagnostics-branch`:
(a) prune under a lock that excludes CDC-apply `rcu` writers; (b) bounded `rcu` retries + fall back to
exclusive swap; (c) make prune incremental / cap tombstone-index size; (d) replace the O(N)-drop/rebuild HAMT
with a sublinear structure.

**Final 5-run tally (all FAILED, 0/5 passed — kick-in eliminated nothing).** run 29799199984 froze (`rcu`
prune, native); run 29799205731 ready+converged but 1/22 queries diverged (`rcu` prune, native); run
29799211973 froze (corpus-PK checkpoint, native sibling O(N)); runs 29799218210 & 29799224154 ready but "1
table mismatched, replication did not converge" (no stall backtrace — became ready, divergence caught at the
HTAP gate). 2 never-ready freezes + 3 ready-but-data-wrong = the same ~50/50 split as baseline, so the kick-in
changed nothing, exactly as predicted for a fix targeting the bystander scan rather than the `rcu`-livelock
consumer. Validation complete; the real fix (rcu livelock / bound tombstone+PK volume) is not yet implemented.

## ★ Local repro + PK-keyset root-cause lead (2026-07-23)

Reproduced the never-ready freeze locally (Docker, dumper image, chbench SF1000) with the FULL 6-cold-table
pod + `warm_max_bytes=512MB` + `compaction_memory_fraction=0.15` + `cdc_mem_tier_max_bytes=1GiB`. Single-cold-table
does NOT freeze (confirms cross-table). The in-process native backtrace fires on the stall; across the local
clean-freeze capture, a local OOM-stall capture, and CI run 29799211973, the SOLE active app threads are
consistently the **O(N) PK-keyset** machinery on `hashbrown::RawTable<(u128, PkKeysetEntry)>` (100-300M entries):
- **drop**: `clear_all_deletion_caches → drop_in_place<RawTable<PkKeysetEntry>> → snmalloc::dealloc`, from
  `publish_overwrite_snapshot_fenced` (promotion commit, table.rs:2679) held **under `listing_fence.write()`**
  (comment: "every step is synchronous, so the fence guards only pointer swaps" — violated by the O(N) drop);
- **build**: `checkpoint_mem_tier → checkpoint_corpus_pk_keys → hashbrown insert`.

No lock/rcu/kanal frames. Mechanism: the promotion holding `listing_fence.write()` across the O(N)
`clear_all_deletion_caches` blocks the table's scans → scan `POLLED-THEN-PENDING` + backlog (the SYMPTOM) →
never ready. Actionable fixes: (1) run the O(N) clear OUTSIDE `listing_fence.write()` (fence only the pointer
swaps, per its own contract); (2) cap/bound the PK-keyset size; (3) make the mem-tier-checkpoint PK-keyset build
incremental instead of full rebuild.

## ★ Semaphore experiment result (2026-07-23) — concurrent promotions are the trigger

Built the semaphore fix (global limiter, `CAYENNE_MAX_CONCURRENT_COLD_PROMOTIONS=1`, acquired before
`compaction_lock`/`write_lock`) and tested it locally against the proven-freeze config (full 6-cold pod,
512MB warm, 0.15 fraction). Result: **2/2 runs — no `POLLED-THEN-PENDING` freeze** (promotions strictly
serialized; 93 / 61 `await-promotion-permit` waits), vs the baseline which froze reliably in ~2-3 min. So
**concurrent cold promotions are the trigger** of the never-ready freeze, and serializing them eliminates it.

Nuance: both semaphore runs still OOM'd from a residual **scan-drain backlog** buildup (aggregate SCAN
STALLED, not `POLLED-THEN-PENDING`) — the deeper `buffer_unordered` scan-drain lost-wake + O(N) PK-keyset work
(`clear_all_deletion_caches` under `listing_fence.write()`) is the ROOT, only mitigated (trigger removed) by
the semaphore. On the 51 GB local box the residual drain still OOMs; CI's 256 GB would likely reach ready.

**Two-layer fix:** (trigger) the promotion semaphore removes the never-ready freeze; (root) move the O(N)
`clear_all_deletion_caches` off the scan-blocking `listing_fence.write()` / bound the PK-keyset / make the
mem-tier-checkpoint PK-keyset build incremental, to fix the underlying scan-drain lost-wake.

## ★★★ Semaphore experiment — CI CONFIRMATION (2026-07-23) — freeze eliminated on the 256 GB CI box

The five real-CI HTAP runs of the semaphore build (`24c13830bf`, `adaptive-cold-sf1000`, ready-wait 600 s)
completed, confirming the local prediction. Runs: 29986099971 / 29986103794 / 29986107834 / 29986111744 /
29986115956. (They sat ~8 h queued waiting for a depleted runner pool before executing.)

**Result: 5/5 reached readiness with 0/5 never-ready freezes.** Every run bootstrapped, served queries, and
ran all the way to the HTAP data-correctness gate — the runtime stayed live the whole window (only transient
`/health` blips ~200 ms). Against the ~40-50 % Mode-1 freeze rate on every prior build, 0/5 freezing is a clean
confirmation that **serializing cold promotions eliminates the never-ready freeze in real CI**, exactly as the
local 2/2 result and the 256 GB-box prediction anticipated.

All 5 instead failed on **Mode-2 data divergence** (`order_line` accelerated rows fall short of source, "replication
did not converge within ~650-810 s"):

| Run | order_line accel vs source | shortfall | extra |
|-----|----------------------------|-----------|-------|
| 29986099971 | 322,497,518 / 330,964,655 | ~8.5M | — |
| 29986103794 | 323,227,245 / 331,737,196 | ~8.5M | — |
| 29986107834 | 321,162,694 / 332,587,642 | ~11.4M | — |
| 29986111744 | (order_line + `stock` content mismatch) | 2 tables | `stock` 100M=100M rows but content diverged |
| 29986115956 | 323,673,862 / 333,320,706 | ~9.6M | — |

**Interpretation.** This locks in the two-layer picture on production-scale CI hardware: the promotion semaphore
is a confirmed, effective fix for Mode-1 (the never-ready freeze), and the residual **scan-drain lost-wake + O(N)
PK-keyset / deletion-index work** now manifests purely as Mode-2 replication non-convergence — the sole remaining
blocker to a green HTAP correctness run. The root-cause fixes above (move the O(N) `clear_all_deletion_caches`
off `listing_fence.write()`, bound the PK-keyset, incremental checkpoint build) are what remain to close Mode-2.
