# MySQL OLTP Generator Performance

**Status:** implemented on `sgrebnov/0725-mysql-oltp-tpmc`; local results below, CI (HTAP dispatch) validation pending
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

**Headline (same-sitting, fresh-seed, SF10 / 100 terminals / 30s, Docker on macOS):
79.3k → 131.2k tpmC (+65%), zero aborts.**

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

## Next steps

- Validate on CI via the HTAP dispatch with a `mysql*` spicepod and empty
  `rate` (unlimited); driver builds from the branch, spiced from trunk.
- Watch `reconcile_bench_ts` on the template-restore (`--skip-prepare`) path —
  first exercise outside local runs.
- `.github/actions/setup-chbench-mysql`: `--log-bin-trust-function-creators`
  is no longer needed once this lands (harmless meanwhile).
- Optional: a Postgres twin of the benchmark test for same-host comparisons.
