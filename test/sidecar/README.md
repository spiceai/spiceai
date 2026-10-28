# Sidecar qualification

Enhancement: [#15707](https://github.com/spiceai/spiceai/issues/15707).
Configuration and operating behavior: [application sidecars](../../docs/features/cluster-sidecar.md).

The native drivers start real `spiced` processes. SQLite supplies independent
answers for the relational corpus; literal Python values, `Decimal`, timestamps
and Arrow schemas supply the typed oracle. These tests do not share Spice's
Substrait encoder or consumer. The cache driver constructs Flight SQL command
envelopes independently and uses Python DataFusion only to generate the input
plan. The mTLS server identifies each client from its actual peer certificate.

The multi-process drivers are necessary for upstream outages, process suspension,
certificate-specific Flight responses and comparisons between different runtime
binaries. The source-free local-serving checks also run as the repository's
`test/spicescript/sidecar-local.yaml` scenario.

## Reproduction commands

Use a fresh output directory for each invocation. `--source-commit` identifies
the source of a prebuilt binary; it is distinct from the harness commit and
SHA-256, which each driver also records. All commands below run from the
repository root. The standalone benchmark is the primary performance topology;
distributed execution is a separate functional check.

For upgrade and rollback qualification, add
`--legacy-spiced /path/to/older/spiced --legacy-source-commit OLDER_COMMIT` to the
main driver. It requires SQL federation to return exact rows against that
upstream and Substrait federation to return the actionable upgrade/rollback
diagnostic. Use an upstream built before the Spice Substrait extensions.

```bash
python3 test/scripts/sidecar_semantics.py \
  --spiced /path/to/spiced --source-commit COMMIT --out /tmp/sidecar-semantics

python3 test/scripts/sidecar.py \
  --spiced /path/to/spiced --source-commit COMMIT --rows 1000 \
  --out /tmp/sidecar-correctness

uv run --python 3.12 --with pyarrow==26.0.0 python test/scripts/sidecar_types.py \
  --spiced /path/to/spiced --source-commit COMMIT --out /tmp/sidecar-types

python3 test/scripts/sidecar_schema.py \
  --spiced /path/to/spiced --source-commit COMMIT --out /tmp/sidecar-schema

python3 test/scripts/sidecar_trace.py \
  --spiced /path/to/spiced --source-commit COMMIT --out /tmp/sidecar-trace

uv run --python 3.12 --with datafusion==55.0.0 --with pyarrow==26.0.0 \
  python test/scripts/substrait_cache.py \
  --spiced /path/to/spiced --source-commit COMMIT --out /tmp/sidecar-cache

uv run --python 3.12 --with datafusion==55.0.0 --with pyarrow==26.0.0 \
  python test/scripts/sidecar_scope.py \
  --spiced /path/to/spiced --source-commit COMMIT --out /tmp/sidecar-mtls

python3 test/scripts/sidecar.py \
  --spiced /path/to/spiced --source-commit COMMIT --rows 1000 --executors 2 \
  --out /tmp/sidecar-cluster-correctness

python3 test/scripts/sidecar.py \
  --spiced /path/to/spiced --source-commit COMMIT \
  --testoperator /path/to/testoperator --rows 100000 \
  --concurrency 2 8 32 128 --duration 30 --cache-states miss \
  --out /tmp/sidecar-uplink

python3 test/scripts/sidecar_report.py /tmp/sidecar-uplink \
  --output /tmp/sidecar-uplink-summary.json

python3 test/scripts/sidecar.py \
  --spiced /path/to/spiced --source-commit COMMIT \
  --testoperator /path/to/testoperator --rows 100000 \
  --concurrency 32 --duration 30 --cache-states hit \
  --out /tmp/sidecar-cache-hits
```

## Candidate qualification

The initial qualification binary was built from
`944b5f351ef64c8713bfefd9d9cb8ce4cc134606` on 2026-10-28 UTC with:

```bash
env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ \
  CARGO_TARGET_DIR=/Users/Code/dev/spiceai/target \
  cargo build --release -p spiced -p testoperator -p spicescript \
  --no-default-features --features release
```

The build exited 0 after 14m51s. `spiced --version` reported
`v2.5.0-unstable`; its SHA-256 is
`823f19ba1718df146e8ec7e67627c0c48a65cca0c3e4b6fc6cb68d9ae71cbed9`.
This uses the core release feature set; the complete workspace sign-off remains
a separate requirement. The trace correction described below changes the
production Rust tree and needs a new binary and qualification run.

The [candidate artifacts](evidence/candidate-944b5f3) retain returned rows,
schemas, plans, responses, source identities and commands. The native commands
above used `/tmp/spice-sidecar-artifacts/candidate-944b5f3/spiced`,
`--source-commit 944b5f351ef64c8713bfefd9d9cb8ce4cc134606`, and fresh output
directories named after each driver under `/tmp/spice-sidecar-artifacts`.

| Run | Actual result |
| --- | --- |
| Main standalone driver | 47 SQLite comparisons per mode; local cache and Cayenne reads during upstream outage; five HTTP 504 deadline responses; HTTP 499 cancellation; reload and explicit partition checks |
| Legacy upstream | SQL federation returned exact rows; sidecar returned the upgrade/server-mode diagnostic |
| Focused semantics | All five recorded development regressions returned their expected answers |
| Typed driver | Exact decimal, nested, timestamp, binary and view results; valid BIGINT boundaries; 320 comparisons per mode with 32 threads and zero mismatches; cast/division errors propagated |
| Schema changes | Reordered same-type fields preserved bindings; incompatible field type returned HTTP 400; restored schema recovered without sidecar restart |
| Plan cache | Exact-wire and authorization separation: `MISS, HIT, MISS, HIT, MISS, HIT`; SQL-key mode bypassed all six requests |
| Cache freshness | Refresh preserved eligible cached count `1000`; TTL expiry returned updated count `1001` |
| mTLS identities | Returned `(aid=1, bid=2)` through two identity-scoped fragments |
| Source-free spicescript | `16 checks passed, 0 failed` in server/sidecar matrix |
| Scheduler topology | Two of two executors registered; 47 exact comparisons per mode. Captured synchronous plan executed locally, so this does not establish distributed task execution |
| Flight/Substrait integration | `6 tests passed, 552 skipped` under the focused protocol filter |

The scoped release unit command was:

```bash
env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ \
  CARGO_TARGET_DIR=/Users/Code/dev/spiceai/target \
  make nextest-packages \
  PACKAGES='spiced testoperator spicescript runtime spicepod flight_client data_components cache runtime-request-context cpu-budget' \
  FEATURES=release RUST_PROFILE=release \
  _FEATURES_FLAGS='--no-default-features --features release'
```

It reported `4282 tests run: 4282 passed (2 after retry), 2 skipped`.
The two corrected release-profile test fixtures passed on their first attempts.
Two other first attempts remain unresolved: the idle-executor test failed its
registration assertion, and the shared Databricks semaphore test timed out at
360 seconds with threads parked in Tokio. Both succeeded on the repository's
permitted retry. The [first-attempt diagnostics](evidence/candidate-944b5f3/unit.log)
and [captured stack](evidence/candidate-944b5f3/databricks-semaphore-stack.txt)
are retained; retries do not establish a fix or a cause for either failure.

## Before-change observations

The committed JSON preserves the actual rows, errors, plans and source/date
identities. These observations describe development candidates, not released
Spice behavior, except for the explicitly identified cache-contract baseline.

`sidecar_semantics.py` ran unchanged on 2026-10-28 UTC against
`0b9ac905ee97165cdc39fd33fc955bc0e109491a` and
`acdfbe0586b178ef580598039823442534092a3b`. Commands used the corresponding
`candidate-initial/spiced` and `candidate-acdfbe0/spiced` binaries under
`/tmp/spice-sidecar-artifacts`, with output directories `semantics-0b9ac90` and
`semantics-acdfbe0`. Both exited 1 and saved every observation before asserting.

| Case | Initial candidate | Candidate acdfbe0 |
| --- | --- | --- |
| `value NOT IN (...)` with NULLs | Expected `[]`; returned IDs `0, 7, 14` | `[]` |
| SQL `id + 1` with upstream `add(a,b) = a - b` | Expected `1..5`; returned `-1..3` | `1..5` |
| `isnan(CAST(id AS DOUBLE))` with upstream `is_nan(x) = x > -100` | Ten `true` values instead of `false` | Same wrong values |
| Two-argument `log(2, id)` at ID 8 | `3.0` | `3.0` |
| `Utf8View` output over Flight | HTTP 400, wire `LargeUtf8` rejected | Correct strings |

Read the full observations in [initial results](evidence/semantics-0b9ac90/results.json)
and [acdfbe0 results](evidence/semantics-acdfbe0/results.json).

The separate certificate-scope reproduction on `0b9ac90` expected `(1, 2)` from
two different certificate identities but received `(1, 1)`. Its actual
[rows](evidence/scope-before-distinct/result.json) and
[plan](evidence/scope-before-distinct/explain.json) show a single incorrectly
combined remote fragment. Certificate and private-key files are excluded.

The deadline run on `ee64378e0a842f9e85362ccfd72dd8452deaa1e4` suspended the
upstream with a 250 ms query budget. It observed HTTP statuses
`504, 504, 504, 504, 400`; the last response was
`External error: Query execution failed. Timeout expired` at 256.28825 ms.
The [five responses](evidence/deadline-ee64378/timeouts.json) retain the bodies
and elapsed times. This exposed the transport deadline/timer scheduling race.

The cache-contract baseline is source
`34825bb456d62d616f693187b0baf0fa1e8cfb14`. Two different wire encodings of
`SELECT 11 AS value` returned the same correct row but shared a decoded-plan
cache entry, and SQL cache mode also cached wire plans. The
[observations](evidence/cache-baseline-direct/observations.json) record these
statuses. The requested contract instead distinguishes exact wire bytes in
plan mode and bypasses SQL mode. This baseline is a contract change, not a
wrong-result observation.

Additional boundary testing exposed inherited BIGINT overflow behavior. On both
the original baseline and candidate acdfbe0, the source-free
[`int64-overflow.yaml`](../spicescript/known-failing/datafusion/int64-overflow.yaml)
scenario received HTTP 200 where it expected a structured error. A separate
Flight query on candidate acdfbe0 returned `-9223372036854775798` for
`11 + 9223372036854775807`, and `9223372036854775801` for
`-7 + (-9223372036854775808)`. DataFusion's physical binary expression dispatches
addition to Arrow's `add_wrapping` kernel. This behavior is outside the new
transport; it remains a recorded known failure, rather than being accepted as a
correct overflow result. The [command, sources, output and rows](evidence/integer-overflow-baseline)
are retained. The sidecar typed corpus separately checks valid signed-64-bit
boundaries and structured cast/division errors.

The refresh experiment also corrected an initial test expectation. Source refresh
deliberately preserves eligible SQL results until TTL expiry
(`Caching::invalidate_for_refresh`), so expecting an immediate changed answer
was wrong. On the same acdfbe0 engine, the corrected external check observed
count `1000` twice after refresh while the uncached upstream count was `1001`,
then observed `1001` after the configured ten-second TTL expired. The
[observations](evidence/cache-refresh-policy-acdfbe0/refresh-results.json) retain
both phases. This was a test-policy error, not an engine invalidation defect;
the complete acdfbe0 run still stopped later on the recorded scalar-alias defect.

A Flight network integration test also exercises repeated authorization metadata
through the public `FlightClient` API. Before the correction, the server observed
`Bearer bob` and `Bearer alice` while the clients reported equal federation
contexts. Stable sorting by header name retains repeated-value overwrite order;
the same test then observed those two values with distinct contexts. The
[before/after logs and command](evidence/metadata-order) include the four-test
Flight client suite result. This state is tested directly through `MetadataMap`,
rather than claimed as a Spicepod-only reproduction.

## Benchmark method and limits

Each concurrency level uses SQL, Substrait, Substrait, SQL arms. Every arm starts
a fresh standalone upstream and sidecar, with two CPU cores of runtime sizing,
one target partition, 512 MiB query memory, admission limit 16 and identical
datasets and queries. The upstream stores 100,000 seeded rows in Cayenne memory;
CSV ingestion completes before measurement. Remote-miss arms disable results
caches at both hops. Separate hit arms enable only the sidecar results cache.

`testoperator run load --validate` performs its validation and warmup, then its
60-second baseline, then a 30-second measured load phase. The report retains
individual per-query P99 values and complete repetitions; it does not average
quantiles. The current load table reports integer milliseconds and does not
provide P99.9. Validation during warmup is supplemented by the separate typed
test's 32 concurrent clients and 320 exact comparisons per mode.

The client uses Flight SQL to the sidecar for both arms. Uplink metrics identify
SQL `GetFlightInfo` plus `DoGet`, or the typed Substrait `DoGet`. Plans, metrics,
query outputs, commands, process IDs, resource samples and full testoperator logs
remain in each run directory. RPC counters and RSS/health samples cover the
whole arm; client throughput and latency cover only the measured load phase.

Measurements use loopback on one macOS host. They measure full client-to-sidecar
request latency and process both hops, but do not establish performance across a
remote network or at a different RTT. An early run reused its upstream and
showed changing SQL bracket timings; it was stopped and is excluded from
performance acceptance. Subsequent arms use fresh upstreams.

The qualification host reports Apple M4 Pro, model `Mac16,11`, 14 logical CPUs
and 68,719,476,736 bytes of memory. Runtime sizing is explicitly two cores per
instance; this is not an OS CPU quota on macOS. Provenance records the CPU model,
host CPU count, memory and starting load average. The integer-millisecond load
table cannot establish submillisecond differences.

## Standalone uplink measurements

Source `944b5f351ef64c8713bfefd9d9cb8ce4cc134606`, 2026-10-28 UTC, on the
host described above. The command was:

```bash
python3 test/scripts/sidecar.py \
  --spiced /tmp/spice-sidecar-artifacts/candidate-944b5f3/spiced \
  --source-commit 944b5f351ef64c8713bfefd9d9cb8ce4cc134606 \
  --testoperator /tmp/spice-sidecar-artifacts/candidate-944b5f3/testoperator \
  --rows 100000 --concurrency 2 8 32 128 --duration 30 --cache-states miss \
  --out /tmp/spice-sidecar-artifacts/bench-944b5f3-miss
python3 test/scripts/sidecar_report.py \
  /tmp/spice-sidecar-artifacts/bench-944b5f3-miss \
  --output /tmp/spice-sidecar-artifacts/bench-944b5f3-miss-summary.json
```

Both commands exited 0. The report recorded `complete: true`, 16 of 16 arms,
and zero resource-observer errors. Each arm's five measured query statuses were
`passed`. The [summary and raw artifacts](evidence/bench-944b5f3-miss) retain
every repetition. Raw testoperator logs, resource traces and metric snapshots
are gzip-compressed without changing their contents; `manifest.json` records
their uncompressed SHA-256 values. Decompress with `gzip -dc FILE.gz`.

| Clients | Protocol | Repetition | Queries/s | P99 ms: aggregate / join / nulls / point / topk | Upstream peak RSS MiB |
| --- | --- | --- | ---: | --- | ---: |
| 2 | SQL | 0 | 650.0 | 5 / 5 / 5 / 4 / 5 | 180.6 |
| 2 | Substrait | 1 | 630.4 | 6 / 8 / 5 / 6 / 8 | 831.2 |
| 2 | Substrait | 2 | 632.1 | 6 / 6 / 5 / 5 / 10 | 827.2 |
| 2 | SQL | 3 | 641.1 | 5 / 5 / 4 / 5 / 5 | 180.5 |
| 8 | SQL | 0 | 836.6 | 22 / 22 / 21 / 21 / 26 | 191.4 |
| 8 | Substrait | 1 | 829.5 | 23 / 22 / 22 / 21 / 25 | 379.3 |
| 8 | Substrait | 2 | 831.3 | 22 / 22 / 21 / 21 / 25 | 378.1 |
| 8 | SQL | 3 | 835.8 | 22 / 21 / 21 / 21 / 25 | 190.7 |
| 32 | SQL | 0 | 849.4 | 64 / 65 / 63 / 64 / 67 | 193.9 |
| 32 | Substrait | 1 | 851.6 | 63 / 64 / 62 / 62 / 66 | 318.5 |
| 32 | Substrait | 2 | 845.2 | 63 / 65 / 62 / 63 / 66 | 318.1 |
| 32 | SQL | 3 | 851.2 | 63 / 64 / 63 / 63 / 66 | 194.4 |
| 128 | SQL | 0 | 852.0 | 186 / 188 / 184 / 187 / 188 | 193.7 |
| 128 | Substrait | 1 | 848.1 | 198 / 197 / 198 / 200 / 201 | 312.6 |
| 128 | Substrait | 2 | 857.5 | 186 / 190 / 187 / 187 / 190 | 309.6 |
| 128 | SQL | 3 | 853.1 | 193 / 193 / 192 / 190 / 193 | 195.2 |

These runs establish no consistent loopback throughput or P99 improvement.
At concurrency two, Substrait had lower throughput and higher tails; at higher
concurrency, the throughput ranges mostly overlap. Upstream counters recorded
one `do_get/statement_substrait_plan` per Substrait fragment versus one
`get_flight_info/sql_query` plus one `do_get/sql_query` for SQL. This removes an
uplink round trip, but the local measurements do not quantify its benefit at a
remote network RTT.

The Substrait arms also had higher upstream peak RSS, especially at concurrency
two. Sidecar RSS stayed similar between protocols. These are whole-arm process
peaks, not live query-pool reservations: the upstream query-pool gauge was zero
at the final scrape. Follow-up identified trace reconciliation, as described
below. These numbers precede that correction. Full workspace sign-off is still
pending.

The separate cache-hit command used the same binary and testoperator, with
`--rows 100000 --concurrency 32 --duration 30 --cache-states hit` and output
`/tmp/spice-sidecar-artifacts/bench-944b5f3-hit`. It exited 0; the
[report and complete raw evidence](evidence/bench-944b5f3-hit) contain four of
four arms with every measured query status `passed` and zero observer errors:

| Protocol | Repetition | Queries/s | Per-query P99 ms |
| --- | --- | ---: | --- |
| SQL | 0 | 13083.6 | 3 for all five queries |
| Substrait | 1 | 13643.3 | 3 for all five queries |
| Substrait | 2 | 13691.4 | 3 for all five queries |
| SQL | 3 | 13059.3 | 3 for all five queries |

Upstream query RPC deltas were zero in every hit arm after warmup. Whole-arm
sidecar peak RSS was 699.5–729.1 MiB, with task history enabled; upstream peak
RSS was 138.7–140.1 MiB. HTTP health P99 was 3.30–3.42 ms during these arms.
The higher cache-hit throughput does not measure delegation: both modes served
their local cache without an uplink query.

## Trace reconciliation follow-up

The matched-count [memory probe](evidence/memory-944b5f3) used 20,000 requests,
two clients and a 30-second idle period per protocol/history combination. It
retains its exact source, command, every returned row, RSS trace, metrics, and
macOS `vmmap`/`heap` captures. With history enabled, upstream peak RSS was
156.3 MiB for SQL and 845.6 MiB for Substrait. With history disabled it was
137.3 MiB and 133.8 MiB respectively. The enabled Substrait run recorded 2,446
internal reads of `runtime.task_history` during load; SQL recorded none.

The sidecar sent its runtime-generated ID as a `spice-trace-id` client pin.
`TaskSpan::write` reconciles such overrides by scanning and rewriting rows under
the original trace ID. Generated IDs instead belong in the existing opaque
Flight ticket envelope, which joins the trace before execution. The correction
shares that encoder with the Flight client, preserves explicit trace-header
precedence and matching W3C parents, and skips identity rewrites when the SDK
span already has the requested ID.

The maintained `sidecar_trace.py` scenario reads generated HTTP response IDs,
queries the upstream history and compares internal query counters. These dynamic
cross-runtime values require the native driver; spicescript HTTP assertions do
not bind response values for later SQL. On binary `944b5f3`, the same scenario
recorded `10, 1, 1, 1` reconciliation reads for generated IDs, explicit pins,
W3C parents and conflicting headers respectively, while every ID and parent
remained correlated. All four cases failed the zero-reconciliation guard;
the [actual failure and observations](evidence/trace-before-944b5f3) are retained.
The corrected binary, repeated memory probe and new uplink measurements remain
to be run.

The real-SDK nested-span guard also caught a root-claim ordering error in
candidate `00971c2`: a root with no incoming propagated ID could publish its ID
without consuming the root claim, letting its child reparent itself. The
[failure](evidence/trace-sdk-00971c2) retains all four unsuccessful attempts.
Candidate `f4f0d5d` consumes that claim after resolution. The exact same focused
command then reported [16 passed, 3625 skipped](evidence/trace-sdk-f4f0d5d) in
0.383s, including matching pins, conflicting IDs, missing propagation, generated
IDs and nested parent checks. These SDK tests supplement the pending native
after-run; they do not replace it.
