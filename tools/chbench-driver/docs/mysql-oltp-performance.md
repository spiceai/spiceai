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

## Interaction with the `_bench_ts` watermark design

The driver-side watermark proposal (`docs/bench-ts-watermark.md` on
`sgrebnov/0725-chbench-bench-ts-watermark`) also deletes the triggers but
replaces engine stamping with driver-bound stamps and requires the column to
carry **no** default and no `ON UPDATE`. If that lands, its reconcile step
must additionally strip `ON UPDATE` from templates created by this build
(one `ALTER … MODIFY` alongside its existing trigger cleanup).

## Planned experiments

**1. `_bench_ts` index on `order_line`.** `SELECT MAX(_bench_ts)` on
`order_line` is a full clustered scan (~2 min at SF1000 in CI), throttling the
staleness probe; an index fixes the read but adds secondary-index maintenance
to the hottest write table (monotonic inserts hit the rightmost page; delivery
re-stamps move entries). Plan: env-gate the index behind
`CHBENCH_EXPERIMENT_BENCH_TS_INDEX=1` in **two** sites — `create_indexes`
(fresh-seed path) *and* `verify_prepared` (checked via
`information_schema.STATISTICS`; required because the template restore is a
cold data-dir copy that wipes indexes, and `--skip-prepare` never runs
`create_indexes`). The shared template stays pristine — the index is built
per run, before terminals start, with the build duration logged. Compare A/B
same-runner: tpmC + abort rate (cost) vs probe latency / staleness sample
count (benefit). Keep the index only if the tpmC cost is ≲3%; note B's
buffer-pool warmup bias from the build scan when reading results.

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
