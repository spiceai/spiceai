# MySQL OLTP Generator Performance

**Status:** implemented on `sgrebnov/0725-mysql-oltp-tpmc`; validated locally and in a full CI HTAP run (see results)
**Scope:** `tools/chbench-driver` (MySQL path only)
**Goal:** raise the tpmC the OLTP generator can push into a MySQL source, so the HTAP benchmark's CDC input rate is limited by the server — not the driver. The Postgres path already generates enough.

---

## Problem

The MySQL transactions issued every statement as its own client-server round
trip: ~37 per 10-line `new_order`, ~10 per `payment`. At 100 terminals each
transaction spent most of its wall-clock time waiting on the wire while
`mysqld` sat at ~30% CPU (measured under full load, 16-core Docker VM).
Round-trip count — not server capacity — capped generated tpmC.

## Benchmark harness

`tests/mysql_oltp_bench.rs` — self-contained measurement, not an assertion:
starts a `mysql:8.0` container with the same server flags as
`.github/actions/setup-chbench-mysql` (host port 33306), seeds SF10 (~20s via
`LOAD DATA`), runs 100 terminals at unlimited rate for 30s, prints the tpmC
report, and leaves the container running for fast re-runs.

```shell
CHBENCH_MYSQL_BENCH=1 cargo test -p chbench-driver --test mysql_oltp_bench -- --nocapture
```

Knobs: `CHBENCH_WAREHOUSES`, `CHBENCH_TERMINALS`, `CHBENCH_RATE`,
`CHBENCH_BENCH_SECS`, `CHBENCH_SKIP_PREPARE=1` (reuse seed),
`CHBENCH_MYSQL_BENCH_PORT`.

### Measurement rules (learned the hard way)

- **Same-sitting A/B pairs only.** Host throughput drifts ~15% day to day; a
  cross-day comparison mislabeled a neutral change as a regression.
- **Fresh seed per measured run.** Reused datasets accumulate `new_order`
  backlog and InnoDB purge/flush debt; observed swings up to 2x.
- Always report abort rate next to tpmC — a change that gains tpmC by
  aborting is not a win. All changes below measured **0 aborts**.

## Changes (all in `src/txn/mysql.rs` unless noted)

| Change | Round trips | Same-day effect |
|---|---|---|
| `new_order`: items via one `IN`-list SELECT; stock via one `(s_w_id, s_i_id) IN (…) FOR UPDATE` (PK range accesses, locks in PK scan order); stock UPDATEs as one multi-statement batch | ~37 → ~10 | **+47%** |
| `payment`: warehouse+district YTD UPDATEs share one round trip; display SELECTs joined into one read | ~10 → ~8 | **+6%** |
| `_bench_ts`: 16 `BEFORE INSERT/UPDATE` triggers replaced with native `DEFAULT / ON UPDATE CURRENT_TIMESTAMP(3)` (`src/schema_mysql.rs`, `src/lib.rs`); `verify_prepared` reconciles trigger-era templates | — | neutral locally; removes per-row trigger dispatch server-side |
| `mysql_async` `stmt_cache_size` 32 → 256 (`src/config.rs`) — workload has 40+ distinct statements/connection | — | neutral locally; removes re-`PREPARE`s, matters more at higher RTT |

**Local headline (same-sitting, fresh-seed, SF10 / 100 terminals / 30s, Docker
on macOS): 79.3k → 131.2k tpmC (+65%), zero aborts.**

## CI results (full HTAP runs, in-cluster MySQL pod, live CDC)

Both runs: `htap benchmark tests`, spicepod
`accelerated/mysql-cayenne[file]-adaptive.yaml`, SF1000, 100 terminals, spiced
consuming the binlog concurrently.

| | trunk ([30158866659](https://github.com/spiceai/spiceai/actions/runs/30158866659/job/89698650797)) | this branch ([30170010535](https://github.com/spiceai/spiceai/actions/runs/30170010535/job/89709796991)) |
|---|---|---|
| rate / duration | capped 9,250 txn/s / 900s | **unlimited** / 600s |
| tpmC | 233,710 | 237,903 |
| actual txn/s | 8,660 — fell 6.4% short of its cap | 8,819 — natural ceiling |
| aborts | 0 | 0 |

**Interpretation: the pod, not the driver, is the CI bottleneck.** Both
drivers converge on ~8.7–8.8k txn/s (~235k tpmC). Trunk could not sustain its
requested 9,250 txn/s; the branch uncapped found the same ceiling (+1.8%,
within cross-run noise — different durations, template hit vs miss). If the
pod were latency-bound like the local setup, the branch's ~4x round-trip
reduction would have moved throughput the way it did locally (+65%); it
didn't, so the pod is saturated server-side under OLTP + binlog + CDC load.

Consequences:
- The generator now has ample headroom; raising CI tpmC further is a
  **server-side** problem (pod CPU/IO/config), not a driver problem.
- The per-improvement attribution ladder is meaningless against the pod
  (server-bound flattens driver deltas) — run it on standard runners with the
  Docker container, where RTT dominates.
- The `order_line` index experiment's *cost* side is best measured on the pod
  precisely **because** it is server-bound: added index maintenance there
  translates directly into lost tpmC.

## Server-tuning A/B result (run 30179041669, 2026-07-26)

First run after the pod tuning batch + CPU rebalance (helm revision 13:
`purge_threads=8`, AHI OFF, `performance_schema=OFF`, `io_capacity`
10000/20000, `page_cleaners=16`, `binlog_cache_size=1M`, `MYSQLD_CPUSET`
38-63,102-127 = 26 cores; workflow pins testoperator to 32-37,96-101).
Same shape as the baseline arm: SF1000, 100 terminals, unlimited rate, 600s,
fresh seed, driver commit `ab7321526b` (pre-watermark).

| | baseline (30170010535) | tuned (30179041669) |
|---|---|---|
| tpmC | 237,903 | **244,794 (+2.9%)** |
| txn/s | 8,819 | 9,073 |
| aborts | 0 | 0 |

Profiler facts (10s ticks, 55 OLTP ticks; commit-counter basis ≈ 92% of
driver txn/s in both runs since read-only transactions issue no COMMIT):

- Rate shape: ~14k commits/s burst in the first seconds (empty delivery
  queues, no undo backlog), sag to ~7.5k by minute 2-3, recovery to a stable
  ~8.7k from minute 6 to the end.
- mysqld: avg 74% / peak 82% of its 52 threads — ~19 physical cores of work,
  the same absolute CPU as the pre-tuning run (77-81% of 48 threads).
- Every tuned signal fixed: checkpoint age stayed **< 1 GB** during OLTP
  (peak 8.82 GB across the whole window, incl. drain) vs riding 12.8 GB of
  16 GB before; binlog cache spills **0** vs ~33/s; purge history list
  **oscillated 70k-924k** (purge keeps catching up) vs a monotonic climb to
  1.18M.
- Unchanged: row-lock wait time ~60-77k lock-ms per 11s tick (~6.3s of
  blocked time per second across 100 terminals, ~66ms per wait) — the
  dominant remaining wait in both runs.
- testoperator: avg ~40% / peak 68% of its reduced 12 threads — the 2-core
  donation did not throttle generation, and there is no further slack.

**Reading: the resource-side levers are exhausted at ~245k tpmC for this
workload shape** — mysqld is not CPU-pegged, flushing/purge/binlog are clean,
and the remaining wait is row-lock contention.

## The bottleneck moved to the consumer (CDC apply)

Facts from the same run's log and freshness report:

- `order_line` initial snapshot completed 00:11:38 (300,016,966 rows, ~8 min
  after registration); the OLTP workload started 00:11:41 — readiness gating
  worked (`/v1/ready` waited for the snapshot).
- Drain probe at +63s after OLTP: `rows src=324,516,463 spice=300,016,966` —
  Spice's `order_line` count was still exactly the snapshot count, i.e.
  **zero of the ~24.5M workload-written `order_line` rows were applied during
  the 600s run**; `max_bench_ts` lag 1,446,826 ms and growing at wall speed.
- spiced averaged 85-88% of its 64 threads during OLTP — the hottest
  component on the node.
- The freshness table shows the symptom: 2-4 samples per table for the run
  (each probe cycle serialized behind multi-minute source `MAX` scans on this
  pre-watermark build), `order_line` p99 = max = 1,050,001 ms from a single
  retained sample — the gap exceeds the run duration because Spice's newest
  `order_line` stamp still predated OLTP start (last seed row, ~10 min before
  the workload began).
- cayenne's adaptive tuner reacted (`compaction_interval_ms` adjustments
  "replication-lag goal" at 00:07:35 and 00:22:58) but apply did not keep up.

**Reading: the generator and source now outrun the consumer.** At ~9k txn/s
of input, cayenne's CDC apply for the dominant table made no visible progress
until the drain. Raising end-to-end HTAP throughput further is a
spiced/cayenne apply-path investigation, not a source-side one.

## Invariants preserved

Rows read/locked/written, values, transaction boundaries, RNG draw order (a
seed produces the same order stream), and binlog ROW events are unchanged —
CDC sees an identical change stream. Locking got safer: batched
`FOR UPDATE` acquires locks in PK scan order versus the old random per-line
order. `ON UPDATE` stamps only value-changing updates; every TPC-C update
changes data columns, so stamping is equivalent to the triggers.

**Safety rule for multi-statement batches:** literal SQL may carry only
integers and exact `Decimal`s — statements with string parameters stay
prepared (`?`). This is the injection/quoting line; hold it in review.

## Considered and rejected

- **Per-warehouse grouped stock reads** — measured identical to the
  row-constructor form; the uniform single statement wins on simplicity.
- **`INSERT … ON DUPLICATE KEY UPDATE` for stock writes** — one prepared
  statement, but a bookkeeping bug could silently insert a phantom stock row
  instead of erroring.
- **Batching `delivery`** (~70 RTs but 4% of the mix, heavily
  read-after-write sequential) — worst effort/reward; revisit only if needed.
- **TCP_NODELAY** — `mysql_async` already enables it.

## In-memory `_bench_ts` watermarks (implemented on this branch)

After the `order_line` index A/B measured a 7.7% tpmC cost, the driver-side
watermark design (`docs/bench-ts-watermark.md`) was ported onto this branch
for the MySQL path, with one deliberate deviation: **no `_bench_ts` index
anywhere** — the delete-bearing `new_order` is answered by a plain scan
(bounded at ~9k rows/warehouse ⇒ ~1s at SF1000), since index maintenance on
hot write paths is the measured-expensive option. Mechanics: every mutating
statement binds a driver-generated stamp pre-truncated to `DATETIME(3)`
precision (`watermark::BenchTs`); each transaction folds it into per-table
atomics (`watermark::Watermarks`) only after COMMIT; the probe's
`max_bench_ts` is an atomic load for 7 of 8 tables. Seed rows get a constant
column default (the load timestamp) which is dropped post-load so an unbound
statement fails loudly on `NOT NULL`; `verify_prepared` strips any
server-side stamping a restored template carries (`ON UPDATE`, defaults,
triggers) and seeds the watermarks with one `MAX` scan per table, timed and
logged. The drain gate gained a once-per-run three-way audit
(driver watermark vs source `MAX` vs Spice) so a driver bookkeeping bug
fails the gate instead of passing it. Verified by seven live-MySQL e2e tests
(`tests/mysql_oltp.rs`), including watermark==source equality after a real
workload and the drained-`new_order` case; fresh-seed SF10/100 throughput is
unregressed. The Postgres path still scans (unchanged behavior).

## Planned experiments

**1. `_bench_ts` index on `order_line`.** `SELECT MAX(_bench_ts)` on
`order_line` is a full clustered scan (~2 min at SF1000 in CI), throttling the
staleness probe; an index fixes the read but adds secondary-index maintenance
to the hottest write table (monotonic inserts hit the rightmost page; delivery
re-stamps move entries). **Implemented**: `CHBENCH_EXPERIMENT_BENCH_TS_INDEX=1`
gates `idx_ol_bench_ts` in **two** sites — `create_indexes` (fresh-seed path)
*and* `verify_prepared` (checked via `information_schema.STATISTICS`; required
because the template restore is a cold data-dir copy that wipes indexes, and
`--skip-prepare` never runs `create_indexes`). The shared template stays
pristine — the index is built per run, before terminals start, with the build
duration logged; with it, `EXPLAIN SELECT MAX(_bench_ts) FROM order_line`
reports `Select tables optimized away`. Compare A/B
same-runner: tpmC + abort rate (cost) vs probe latency / staleness sample
count (benefit). Keep the index only if the tpmC cost is ≲3%; note B's
buffer-pool warmup bias from the build scan when reading results.

**Result (2026-07-25): the index costs 7.7% tpmC — do not adopt permanently.**
Same node, same day, both arms freshly seeded at SF1000, 100 terminals,
unlimited rate, 600s, serialized:

| | A — no index ([30170010535](https://github.com/spiceai/spiceai/actions/runs/30170010535)) | B — index ([30174992679](https://github.com/spiceai/spiceai/actions/runs/30174992679)) |
|---|---|---|
| tpmC | 237,903 | **219,649 (−7.7%)** |
| txn/s | 8,819 | 8,144 |
| aborts | 0 | 0 |

The cost mechanism matches the prediction: mysqld ran at 77–81% of its cpuset
during OLTP, so the index maintenance (rightmost-page appends per `order_line`
insert; entry delete+insert per delivery re-stamp) directly displaced
transaction work on a CPU-saturated server. The gate stays in the tree as a
measurement tool; the `_bench_ts` watermark design remains the proper fix for
probe freshness (same benefit, ~zero write cost).

## Pod profiling findings (run 30174992679, OLTP window)

Captured with `scripts/profile-mysql-pod.sh` (10s ticks, 55 OLTP ticks).
Node `spice-dev-large-1`, 64 physical cores: spiced pinned to 32
(`0-31,64-95`), testoperator 8 (`32-39,96-103`), mysqld 24
(`MYSQLD_CPUSET=40-63,104-127`).

| Signal | Reading | Verdict |
|---|---|---|
| mysqld CPU | avg 77%, peak 81% of its 48 threads | effectively CPU-saturated — CPU allocation is a live lever |
| testoperator group | avg 30%, peak 36% of 16 threads | can donate 2 physical cores safely; 4 is borderline |
| spiced group | avg 85% of 64 threads | hottest component on the node; MySQL gains will push spiced toward its own ceiling |
| purge history list | peaked at 1.18M undo records | **purge is drowning** at ~138k rows/s — strongest new signal; raise `innodb_purge_threads` 4→8 |
| bp wait-free / redo log waits | 0 / 0; checkpoint age rode 12.8 GB of 16 GB | no foreground flush stalls — `io_capacity=200` is hygiene, not the first move (idle/seed phases did show heavy checkpoint debt) |
| row-lock waits | ~34k waits, ~3,900s total (~7 of 100 terminals blocked at any instant) | real but secondary |
| binlog cache spills | 0.45% of transactions | bump `binlog_cache_size` to 1M; sub-1% effect |

**Next server-side batch (flags reversible, one A/B vs 237.9k):**
`innodb_purge_threads=8`; `MYSQLD_CPUSET` +2 cores from testoperator
(testoperator `32-37,96-101`, mysql `38-63,102-127` — coordinated with the
workflow's `TESTOP_PREFIX`); `innodb_adaptive_hash_index=OFF`;
`performance_schema=OFF`; `innodb_io_capacity=10000` /
`io_capacity_max=20000` / `page_cleaners=16`; `binlog_cache_size=1M`.
Rejected for now: MySQL 9.x (no OLTP gain expected, `mysql_native_password`
removed, innovation-track churn); 8.4 LTS later — its modernized InnoDB
defaults are effectively this flag batch, so the batch de-risks the upgrade.

**2. Per-improvement tpmC attribution.** A stacked ladder of refs (no runtime
flags): trunk+test → +stmt cache → +new_order batching → +payment merging →
+ON UPDATE (= this branch). Run all five sequentially in one job on the same
runner (driver-only: container + benchmark test, ~10 min/ref), 3 repeats,
medians; full HTAP runs only for the two endpoints. Statement-cache first
(interacts with statement shapes), trigger replacement last (most
CPU-environment-sensitive).

## Follow-ups

- `.github/actions/setup-chbench-mysql`: `--log-bin-trust-function-creators`
  is no longer needed once this lands (harmless meanwhile).
- Optional: a Postgres twin of the benchmark test for same-host comparisons.
