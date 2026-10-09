# BigQuery federation corpus

Run with `cargo test -p connector-adbc --lib bigquery_federation_corpus`.
The test also runs in the normal workspace Rust test suite. It needs no driver,
credentials, service or network connection.

The E2E Test CI workflow's `run_all_tests` release gate executes every corpus
statement through real BigQuery using `test/scripts/bigquery_corpus.py`. It creates
isolated tables from `schemas.json`, checks federation plans and successful
execution, and requires 269 successful data-query jobs — one per remote subtree,
so the three queries a rounding cast splits contribute nine between them.
The eight table-free
statements execute locally. Queries 168 and 169 divide by cohort counts, so they
execute last against synthetic cohorts with nonzero denominators and exact
expected results. The other queries execute against empty tables, which does not
establish result correctness. The gate also runs the nonempty federation,
pushdown and JSON harnesses in `test/scripts/` with explicit result oracles.

These tests reuse the Linux `spiced` artifact from the same E2E workflow run and
use the pinned ADBC driver.
They require the `BIGQUERY_SERVICE_ACCOUNT_JSON` repository secret; missing
credentials fail the gate. The service account needs query-job and dataset/table creation/deletion
permissions in its own project. Each harness deletes its datasets on exit, and
fixture tables expire after one day if a run is interrupted. Results, plans and
job evidence are uploaded as `bigquery-regression-evidence`.

`queries.sql` contains numbered SQL statements with synthetic identifiers and
labels. `schemas.json` contains Arrow schemas derived from a historical replay's
saved BigQuery table metadata. These fixtures include inferred schemas; they do
not establish compatibility with any particular production schema or data.
Source JSON versus STRING, timestamp timezone, nullability, shared table aliases,
and project/dataset boundaries are explicit.

The test uses the production runtime session, UDFs, optimizers, ADBC table factory,
connector federation policy and BigQuery dialect. Only the ADBC metadata and
execution boundary is replaced: schemas come from these files and statements
fail. No fixture reports exact empty-table statistics.

A fully federated statement must have exactly one `VirtualExecutionPlan`, with no
local semantic work above it. Only schema adaptation with an identical schema,
cooperative scheduling, byte accounting and partition coalescing without a limit
are permitted. Query 241 is the explicit partial-federation case: its median and
approximate-percentile windows and dependent projections/sorts stay local; its
source joins, JSON extraction and aggregation must remain one remote subtree.

Queries 033, 124 and 147 are the rounding-cast partial cases. Each computes a
cast from a fractional value into an integer, which BigQuery rounds where
DataFusion truncates, so the connector policy declines to push it down (issue
#14482) and the plan splits into 3, 5 and 1 remote subtrees respectively. Both
gates pin those counts and require the local integer cast to still be there:
de-federating further, or restoring the pushdown, fails and asks for the entry
to be re-decided. Issue #14607 tracks restoring full federation by rendering the
cast as a truncating one instead of declining it.

Queries 085, 086, 087 and 226 are the ordered-aggregate partial cases. Each
calls `ARRAY_AGG` or `STRING_AGG` with an `ORDER BY` inside the call, which the
unparser drops and the BigQuery dialect does not render, so a federated call
came back in BigQuery's order. Every connector policy keeps such an aggregate
local, and the plans split into 3, 6, 3 and 1 remote subtrees respectively.
Against the gate's empty fixtures each runs one of them: an empty join build side
never polls the rest. Both gates pin those counts and require the local ordered
aggregate to still be there. Rendering the ordering inside the call, as the
DuckDB dialect does, would restore full federation.

Table-free statements are individually identified in the test and must contain
no table scan or remote node. Final SQL rendering errors also fail the test.

The offline corpus is a planning regression guard. In particular,
the interval-to-text parsing in query 124 requires nonempty, date-sensitive
execution fixtures to validate its results. Full federation alone does not prove
that a remote engine accepts every expression or returns correct values.
