# CH-benCH direct-write-to-Spice local variant

A local CH-benCH variant that drives **writes directly into Spice** (gated
`BEGIN … COMMIT` transactions over HTTP `/v1/sql`) instead of writing to Postgres
and ingesting read-only via CDC. It exists to validate, end-to-end on a laptop,
the per-key optimistic-concurrency and fused-transaction IVM fixes in the Cayenne
serializable-transactions surface (#11870).

- `spicepod.yaml` — the mutated TPC-C tables configured for durable **write-back**
  (`access: read_write` + `acceleration.write_mode: write_back` +
  `refresh_mode: changes` + `on_conflict: upsert` + dataset
  `replication.enabled: true`), with a maintained `SUM(s_quantity)` aggregate on
  `stock` to exercise the IVM path.
- `direct_write_bench.py` — the driver: concurrent gated stock-decrement
  transactions to `/v1/sql` and three
  self-checked invariants (no-lost-updates / IVM-fresh / write-back-converge).
  Stdlib only (needs `python3`, `psql`, and a running spiced).

This is a **correctness** variant, not a perf run — a laptop cannot sustain
chbench CDC at scale (see the `accelerated/*-cdc-tuned-*` pods for m7a perf runs).

> **This fixture does not load as written.** Its write-back datasets (`district`,
> `stock`, `oorder`) declare composite primary keys, and durable write-back keys each
> delivery on a single column, so the runtime refuses them at registration. They never
> delivered either — the worker saw the composite key, logged it, and exited, leaving
> the markers to accumulate. Reviving this benchmark needs write-back datasets keyed on
> one column, or composite-key delivery support.

## What it proves

| Invariant | Fix it guards | Oracle |
|-----------|---------------|--------|
| No lost updates under concurrent disjoint-key transactions | P0-2 (dropped OCC stamps) | observed drop in `SUM(s_quantity)` == sum of committed deltas |
| Maintained aggregate reflects committed transactions | P1 (fused-txn IVM staleness) | maintained `SUM … GROUP BY` == base-scan `SUM` after each round |


The **write-back convergence** check (Spice `SUM` == Postgres `SUM` after the
delivery worker drains) is reported but does **not** gate the exit code: a
non-self-healing divergence there would be the *separate, deferred* write-back
echo-loss P0 (its own issue/PR), not an OCC/IVM regression.

## Prerequisites

- Docker (for the Postgres source), `psql`, `python3`.
- A `spiced` **built from this branch** (the fixes must be in the binary) and
  `testoperator` (used only to seed the chbench schema + data into Postgres):

  ```bash
  # one cargo invocation; ~build time on a warm target dir
  env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ \
    cargo build --release -p spiced -p testoperator
  ```

## Run

```bash
cd test/spicepods/chbench/local-writeback

# 1. Postgres with logical replication (write-back + CDC need wal_level=logical).
docker run -d --name chbench-wb-pg \
  -e POSTGRES_USER=bench -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=chbench \
  -p 5432:5432 postgres:16 \
  -c wal_level=logical -c max_replication_slots=16 -c max_wal_senders=16
# wait for it to accept connections
until psql "postgresql://bench:bench@localhost:5432/chbench" -c 'SELECT 1' >/dev/null 2>&1; do sleep 1; done

# 2. Seed the chbench schema + SF1 data into Postgres (schema/seed only, no run).
CHBENCH_PG_HOST=localhost \
  ../../../../target/release/testoperator run htap \
    --spicepod ../accelerated/'postgres-cayenne[file].yaml' \
    --query-set chbench --scale-factor 1 --prepare-only

# 3. Start spiced with the write-back variant (CDC bootstraps the tables).
#    spiced takes the spicepod as a positional path (dir or file).
CHBENCH_PG_HOST=localhost \
  ../../../../target/release/spiced . &
SPICED_PID=$!

# 4. Drive direct writes to Spice + validate.
python3 direct_write_bench.py \
  --spice-url http://localhost:8090 \
  --pg-dsn postgresql://bench:bench@localhost:5432/chbench \
  --workers 8 --rounds 50
echo "exit=$?"   # 0 = OCC + IVM invariants held

# 5. Teardown.
kill "$SPICED_PID"; docker rm -f chbench-wb-pg
```

`--skip-writeback-check` skips the driver's final Postgres-convergence probe (the
write-back oracle inside `direct_write_bench.py`, not the teardown step above) —
useful while the write-back echo-loss P0 is still open, to keep the run focused on
the OCC/IVM invariants.

## Notes

- The gate is a scalar-subquery `assert((SELECT s_quantity …) >= delta)` — the
  NULL-safe form (an empty subquery aborts), matching how the transaction tests
  write gates. A per-row projection `assert` over a filtered scan would pass
  vacuously on zero rows; do not use that form for gates.
- Disjoint-key workers should show ~0 conflicts (per-key OCC admits them); the
  two overlap-band workers drive the conflict/retry path. A flood of conflicts on
  the disjoint workers would indicate the degraded flag is stuck on — investigate.
