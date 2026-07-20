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

### Local capture 2026-07-20 (table=customer) — supports load-dependent starvation

Local docker (full pod) fired a native backtrace on the write-cold phase-duration threshold, but it was a
SOFT trigger, not the target freeze: `sort_input_diag="SORTEXEC-NOT-POLLING"` with `progress_delta_tick` > 0
(cold rows written grew 5M→12M→16M across ticks) — the promotion was SLOWLY PROGRESSING, not hung. The
155-thread dump: ~136 parked (futex_wait) + 4 io-drivers + **~10 threads actively on-CPU in the cold-write
ENCODE path** — `vortex_fsst::compress` (×4), `ShardedPkIndex::record_keys_in_shard`, `arrow_select::interleave`,
arrow→vortex conversion. 0 lock frames. So locally the promotion *gets* CPU and grinds forward (heavy FSST
encode + spilling sort of ~19M rows) → self-heals; it does NOT hit the hard never-ready. This is consistent
with the never-ready being **load-dependent scheduler starvation**: CI's 100-terminal OLTP (nice-0) saturates
cores and starves the nice-10 promotion; a lighter local load lets it progress. (Distinct sub-phase from the
CI never-ready POLLED-THEN-PENDING: local caught the sort-drain/encode stage, CI caught the scan stage — both
"slow promotion holding write_lock," different sub-stages.)
