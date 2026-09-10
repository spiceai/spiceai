# BigQuery federation corpus

Run with `cargo test -p connector-adbc --lib bigquery_federation_corpus`.
The test also runs in the normal workspace Rust test suite. It needs no driver,
credentials, service or network connection.

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
Table-free statements are individually identified in the test and must contain
no table scan or remote node. Final SQL rendering errors also fail the test.

This is a planning regression guard, not a result-equivalence test. In particular,
the interval-to-text parsing in query 124 requires nonempty, date-sensitive
execution fixtures to validate its results. Full federation alone does not prove
that a remote engine accepts every expression or returns correct values.
