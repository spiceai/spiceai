# Cayenne vs DuckDB performance matrix

This directory pairs every Cayenne spicepod with its DuckDB counterpart so the
two accelerators can be compared head-to-head across query, throughput, load,
ingest, write-heavy workloads, and mixed append+query workloads.

It is **not** a new set of spicepods — every yaml referenced from
`pairs.yaml` already lives under `test/spicepods/`. The manifest is the single
source of truth for "which Cayenne pod should I compare against which DuckDB
pod, on which workload, at which scale."

## Metastore variants (SQLite vs Turso)

Cayenne supports two metastore backends: **SQLite** (default) and **Turso**
(libSQL). Each Cayenne entry in `pairs.yaml` exists in two forms:

- The default entry (e.g. `bench-tpch-sf1-file`) points at a pod with no
  `cayenne_metastore` param, which falls back to SQLite.
- The `*-turso` entry (e.g. `bench-tpch-sf1-file-turso`) points at a sibling
  pod with `cayenne_metastore: turso` set under `acceleration.params`. The
  sibling is otherwise byte-identical to the SQLite pod.

The DuckDB side is shared by both — the only thing changing across a SQLite/
Turso pair is the metastore. So the SQLite-vs-Turso comparison (running the
two `cayenne` pods side-by-side and ignoring the DuckDB column) isolates the
metastore's contribution to Cayenne's overall numbers.

This pairing is most informative on **write-heavy and mixed workloads**
(`append-*`, `mixed-*`) where the metastore commit path is on the critical
path of every burst. On pure-read benchmarks the two metastores should be
indistinguishable.

## Running a single pair locally

```sh
# Bench (single query stream, all 22 TPC-H queries).
# Spicepod paths are quoted because they contain `[` and `]`, which
# zsh and some other shells interpret as glob characters.
testoperator run bench \
  -p 'test/spicepods/tpch/sf1/accelerated/file[parquet]-cayenne[file].yaml' \
  -s spiced -d ./.data --query-set tpch --validate

testoperator run bench \
  -p 'test/spicepods/tpch/sf1/accelerated/file[parquet]-duckdb[file].yaml' \
  -s spiced -d ./.data --query-set tpch --validate
```

Run both and diff the resulting query durations. A first-class
`testoperator compare` subcommand that does this in one shot is planned —
this manifest is its input format.

## Running the mixed append+query pair locally

The `mixed-*` entries model real-world interference: analytical query workers
loop through the query set while append loads are generated in the background.
Run both sides with the same duration, append cadence, load count, and query
concurrency.

```sh
# Cayenne side
testoperator run append \
  -p 'test/spicepods/tpch/sf1/accelerated/append/file[parquet]-cayenne[file]-append.yaml' \
  -s spiced -d ./.data --query-set tpch --validate \
  --duration 720 --concurrency 4 --load-interval 30 --load-steps 20

# DuckDB side
testoperator run append \
  -p 'test/spicepods/tpch/sf1/accelerated/append/file[parquet]-duckdb[file]-append.yaml' \
  -s spiced -d ./.data --query-set tpch --query-overrides duckdb --validate \
  --duration 720 --concurrency 4 --load-interval 30 --load-steps 20
```

This is the Cayenne-vs-DuckDB analogue of a CH-benCH-style benchmark: query
latency, append progress, correctness, memory, and health are measured while
reads and writes contend for the same accelerator.

## Adding a new pair

1. Confirm both yamls exist under `test/spicepods/`.
2. Open both and confirm they differ **only** in:
   - `engine: cayenne` vs `engine: duckdb`
   - Accelerator-tuning fields (`vortex_config`, DuckDB `params`, etc.)
   Everything else (source, schema, primary key, partition column, refresh
   policy, retention policy, on_conflict behavior) must match.
3. If you must change something else (e.g. Cayenne supports a feature DuckDB
   doesn't), document it in the entry's `notes:` field and set
   `must_beat: false`.
4. Append the entry to `pairs.yaml`. Keep entries grouped by workload, then
   `query_set`, then `scale_factor`.
5. **Add the matching `*-turso` entry directly after the SQLite default**.
   The Turso variant points at a sibling pod that differs only by an added
   `cayenne_metastore: turso` under `acceleration.params`. If the Turso pod
   doesn't exist yet, create it next to the SQLite pod with the `[file]turso`
   naming convention (e.g. `file[parquet]-cayenne[file]turso.yaml`).

## Fair-comparison rules

These rules keep the matrix honest and reproducible.

### Allowed to differ
- Accelerator engine and its tuning (`vortex_config`, file size targets,
  DuckDB `params.memory_limit`, etc.). Defaults vs. tuned should be tracked
  in separate entries (`-2gib`, `-4gib` variants already follow this pattern).
- Mode-specific tuning, *only when both engines support that mode at the
  configured target* (e.g. partitioning).

### Must be identical
- Source connector (`from:`), source data, schema, refresh policy.
- Primary key and `on_conflict` semantics.
- Partition column. (If only one engine supports a partition scheme, do
  not include the asymmetric pair without `must_beat: false`.)
- Retention period or retention SQL.
- Refresh check interval.

### Refused asymmetries
- `engine: cayenne` only supports `mode: file`. A pair that pits
  `cayenne[file]` against `duckdb[memory]` is **not** fair and will be
  rejected by `testoperator compare` unless `--allow-asymmetric` is set.
- A pair that uses different query overrides (`--query-overrides`) on each
  side is asymmetric. Use the same overrides (or none) on both runs.

### What counts as "winning"
For a `must_beat: true` pair, Cayenne is expected to be **strictly faster**
on at least the configured `success_metric` (default: median query
duration). A regression on any individual query is reported but does not
fail the run unless it's the success_metric.

## Why a manifest instead of paired yamls?

The existing `test/spicepods/` tree is already exhaustive. Duplicating it
would double maintenance: every time someone tunes the Cayenne pod for
SF10 they'd have to remember to update a mirror under
`perf-cayenne-vs-duckdb/`. The manifest references the originals so the
comparison always uses the same configuration that runs in dedicated
benchmark workflows.
