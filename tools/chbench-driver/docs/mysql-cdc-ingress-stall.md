# MySQL CDC Ingress Stall — Observations for Investigation

**Status:** evidence file; consumer-side (spiced/cayenne) investigation not started
**Context:** CH-benCH HTAP runs at SF1000, 100 terminals, unlimited rate, 600s, tuned
`chbench-mysql` pod (26 cores), spiced from trunk, spicepod
`accelerated/mysql-cayenne[file]-adaptive.yaml`. Source-side work is complete
(237.9k → 248.5k tpmC, see `mysql-oltp-performance.md`); the pipeline's limiting
stage is now CDC ingress on the spiced side. All facts below are from runs
[30179041669](https://github.com/spiceai/spiceai/actions/runs/30179041669) and
[30181168207](https://github.com/spiceai/spiceai/actions/runs/30181168207).

## Observation 1: multiple binlog dump threads, not one shared stream

`SHOW PROCESSLIST` on the source during the OLTP phase (5 samples, ~8s apart):
**5–6 concurrent `Binlog Dump` threads**, e.g. one sample:

```
Binlog Dump   Sending to client   531
Binlog Dump   Sending to client   326
Binlog Dump   Sending to client   494
Binlog Dump   Sending to client   432
Binlog Dump   Sending to client   528
Binlog Dump   Sending to client   540
```

Each dump connection streams the **entire** binlog (every table, FULL row
images), so the change stream leaves mysqld N times over, and each spiced-side
reader must decode the full stream to filter its own tables. spiced's log says
datasets share a group (`dataset joined shared mysql binlog group … members=1`),
but 5–6 server-side connections means sharing is partial at best.

The `TIME` spread (46s vs 540s+ in the same sample) shows one connection was
established mid-run — yet per-dataset metrics report `reconnects=0`.

## Observation 2: the source is never idle — clients absorb slower than it sends

Every dump thread stayed in state `Sending to client` across all samples;
none ever showed `Has sent all binlog to replica; waiting for more updates`.
mysqld ran at 74% of its cpuset (headroom); spiced ran at **85–88% of its 64
threads** — the hottest component on the node. The constraint is the wire +
decode side, not the source's ability to produce.

## Observation 3: `order_line` received zero events for an entire 600s run

Three independent measurements agree (run 30179041669):

- Row counts: snapshot completed with `rows=300,016,966`; drain probe 63s after
  OLTP ended: `rows src=324,516,463 spice=300,016,966` — **none of the ~24.5M
  workload-written rows applied**; `order_line` only receives INSERTs, so the
  count is monotone in applied events.
- spiced's own counters (waterfall from `htap-metrics.json`): `0 bursts`,
  `received(ingress)=0.00x`, prefetch fill `0/128` all run — **nothing was even
  delivered** to its apply stage (not a full-buffer stall; an empty pipeline).
- Freshness probe: Spice's `MAX(_bench_ts)` frozen at a pre-OLTP stamp, gap
  growing at wall speed (1,050s mid-run → 1,447s at drain). In the watermark run
  (30181168207, 53–60 samples/table) **59 of 59 `order_line` samples exceeded
  the 1,200s discard cap**.

`order_line` was the last dataset to finish its initial snapshot (00:11:38,
~8 min, 3s before OLTP start) — a possible link to its subscription/routing
never delivering.

## Observation 4: the other datasets are ingress-limited, not apply-limited

Waterfall decomposition (run 30179041669), e.g. `customer`:

- `source arrival p50=269,541ms p99=495,391ms` — events already ~4.5 min stale
  **on receipt**;
- apply itself healthy: `write 27ms/burst`, applied/received ratio 1.10×;
- `received(ingress)=0.27x realtime`, lag slope +0.70s/s (diverging);
- rate-ladder classification: **INGRESS-limited**; stage classifier says
  APPLY-bound (prefetch fill 128/128) — the tool flags the disagreement
  (`[DIFFER — investigate]`), itself worth understanding;
- `stock` (second-biggest stream) crossed the 1,200s cap for a third of the
  watermark run (21 of 59 samples discarded, retained p99 534s).

## Observation 5: staging-WAL recovery churn during snapshot ingest (build-correlated)

Run [30174992679](https://github.com/spiceai/spiceai/actions/runs/30174992679)
(spiced `5b4f2086ff`) logged **2,439** `cayenne::provider::staging_wal` warnings:

```
WARN cayenne::provider::staging_wal: Incomplete staged append detected — attempting automated recovery
     table="order_line" wal_location=/tmp/…/order_line/…/_staging/…/_wal.json target_snapshot=… staged_files=1
INFO cayenne::provider::staging_wal: Automated recovery from incomplete write succeeded; table is now writable
```

Distribution facts (from the run's `htap-run.log` artifact):

- Every occurrence falls inside the **initial-snapshot ingest phase**
  (22:01:06–22:09:21Z, ~8 min); zero during OLTP or drain.
- Per table: customer 881, stock 768, order_line 541, oorder 162,
  new_order 85, district 2 — roughly proportional to snapshot volume.
- Front-loaded: 1,263 in the first minute, decaying to ~50/min.
- Every recovery succeeded (`staged_files=1` each; table returned writable).

**Build correlation:** the two later runs on spiced `f4cb86bd1e`
([30179041669](https://github.com/spiceai/spiceai/actions/runs/30179041669),
[30181168207](https://github.com/spiceai/spiceai/actions/runs/30181168207))
logged **zero** such warnings under the same workload shape. So the churn is
tied to the older spiced build (fixed on trunk in between, or
timing-sensitive), and — importantly — it is **not the cause of the
`order_line` ingress stall**, which occurred in the warning-free runs.

Open sub-questions: what interrupts thousands of staged appends mid-write
during snapshot ingest on `5b4f2086ff` (concurrent reopen? compaction racing
the append path?); whether recovery discards and re-stages work (wasted
ingest throughput during the churn window); and which trunk change between
the two spiced commits made it disappear.

## Open questions

1. Why 5–6 binlog connections when datasets nominally share a group? What maps
   datasets → groups (`data_components/mysql_replication/shared.rs`)?
2. Why did `order_line` — a group member with `member_attached=1` — receive
   zero envelopes while other datasets received theirs late? Is the shared
   reader's routing skipping it, or is its group's reader the mid-run
   connection seen at `TIME=46`?
3. Is per-connection decode single-threaded, and what fraction of spiced's
   85–88% CPU is redundant full-stream decode across the N connections?
4. Why do the waterfall's stage classifier and rate-ladder disagree for
   `customer` (fill=100% vs received=0.27x)? A full prefetch buffer with
   ingress-limited arrival suggests the fill metric or cap semantics mislead.
5. The `order_line` misclassification: zero events renders as
   `healthy / fresh (keeping up)` — silence looks like freshness.

## How to measure (existing tooling)

- **Dump threads (source side)**, live during a run:
  `kubectl -n dataplatform exec chbench-mysql-0 -- mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -N -e "SELECT COMMAND, STATE, TIME FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog%'"`
- **Pod resource profile**: `tools/chbench-driver/scripts/profile-mysql-pod.sh`
  (per-cpuset-group CPU incl. the spiced group, from inside the mysql pod).
- **Per-dataset ingress/apply decomposition**: `htap-metrics-<sf>-<run>` artifact
  → `python3 scripts/chbench-waterfall.py htap-metrics.json`.
- **Freshness cadence**: the driver's in-memory watermarks give ~60
  samples/table/run; `order_line`'s stall appears as all-samples-discarded
  (a bootstrap/steady-state split with an explicit `never caught up` line is
  proposed but not yet implemented).
