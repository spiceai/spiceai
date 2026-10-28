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
