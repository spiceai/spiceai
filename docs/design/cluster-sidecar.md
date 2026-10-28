# Cluster sidecar mode Enhancement draft

Status: surface approved by the requester on 2026-10-27; implementation in progress.
Enhancement: [#15707](https://github.com/spiceai/spiceai/issues/15707).
Source baseline: `34825bb456d62d616f693187b0baf0fa1e8cfb14`, inspected 2026-10-27.
The specification below is the approved implementation contract.

## Goal-State/What/Result

An application queries its local Spice sidecar. The sidecar serves eligible
results-cache hits and queries over configured local acceleration without a
cluster round trip. For reads from another Spice runtime, the sidecar plans the
query and sends eligible remote logical-plan fragments as Substrait. The cluster
creates physical and distributed execution plans and streams Arrow results back.

One optional Spicepod setting selects this behavior:

```yaml
runtime:
  mode: sidecar
```

Omitting `runtime.mode`, or setting it to `server`, preserves current behavior
for standalone servers and cluster nodes. The new setting describes how an
instance serves its application; existing scheduler and executor roles continue
to determine cluster membership.

This develops the application-local serving tier described in the
[cluster-sidecar architecture](https://spice.ai/blog/cluster-sidecar-architecture).
Success means correct answers under concurrent load, fewer application reads
reaching the cluster, and measured improvements in tail latency and throughput.

## Why/Purpose

Make the deployment intent explicit so Spice can select defaults for concurrent,
small queries and keep planning close to the application. An application should
need one endpoint and should not select a wire protocol or implement cache,
acceleration, and cluster routing itself.

This is a product proposal, not a reproduced bug or a measured performance result.
The implementation map below is based on code inspection. Implementation and
performance acceptance require the engine runs in the QA plan.

## By When

**Issue/Spec written and reviewed:** 2026-10-27; requester approved implementation.
**Done-Done:** schedule after sign-off and baseline benchmark collection.

## Done-Done

- [ ] Principles Driven
- [x] The Algorithm
- [x] PM/Design Review
- [x] DX/UX Review
- [ ] Release Notes / PRFAQ
- [ ] Threat Model / Security Review
- [ ] Tests
- [ ] Telemetry / Metrics / Task History
- [ ] Performance / Benchmarks
- [ ] Documentation
- [ ] Cookbook Recipes/Tutorials

The requester approved proceeding with implementation after reviewing this draft
on 2026-10-27: "Just do the implementation, production-grade, verified, tested,
benched, and ready to merge to trunj."

## The Algorithm

- [x] Question requirements: sidecar is a deployment profile, not another engine
  or a new cluster role. SQL text planning belongs in the sidecar; physical
  planning for remote execution belongs in the cluster.
- [x] Delete scope: no new service, protocol-selection parameter, routing DSL,
  accelerator, cache backend, CLI flag, environment variable, or scheduler role.
- [x] Simplify: one enum on the existing runtime object; reuse connector endpoints,
  authentication, cache controls, acceleration policies, and resource limits.
- [x] Break down: transport, federation, runtime integration, and qualification
  are separate implementation steps below.
- [x] Automate: choose the remote plan representation from runtime intent and
  source capability; reuse pooled Flight connections and existing cache probes.

The decisions removed are per-dataset protocol choice, application-side query
forwarding, and manual tuning of local query fan-out for the common sidecar case.
No cloud credentials, object store, or extra metadata service are required for a
self-hosted sidecar and cluster.

## Specification

### Configuration

`runtime.mode` accepts exactly `server` and `sidecar`, case-sensitive. The default
is `server`; omit it when serializing a default configuration. `cluster` is not a
third spelling: a server may be standalone, a scheduler, or an executor.

The complete minimal example for a self-hosted upstream is:

```yaml
version: v1
kind: Spicepod
name: application-sidecar
runtime:
  mode: sidecar
datasets:
  - from: spice.ai:orders
    name: orders
    params:
      spiceai_endpoint: http://spice-cluster:50051
```

`spiceai:orders` has identical behavior to `spice.ai:orders`. The same mode applies
to Spice Cloud datasets using their existing region, credentials, organization,
and application configuration. A self-hosted endpoint needs no cloud region.

Local working sets use existing acceleration configuration, for example:

```yaml
version: v1
kind: Spicepod
name: application-sidecar
runtime:
  mode: sidecar
datasets:
  - from: spiceai:orders
    name: orders
    params:
      spiceai_endpoint: http://spice-cluster:50051
    acceleration:
      enabled: true
      engine: cayenne
      mode: memory
```

The mode does not create accelerations or start additional source ingestion.
Memory and persistent local Cayenne storage use the same existing dataset
configuration. Existing accelerators remain usable during their migration to
Cayenne; this change does not retire one.

| Behavior when its setting is omitted | `server` | `sidecar` |
| --- | --- | --- |
| SQL results caching | Existing defaults | Existing defaults, including TTL, size, key type, and eviction policy |
| Local query target partitions | Existing CPU-budget default | `1` |
| Query execution admission limit | Existing CPU-budget default | Existing CPU-budget default |
| Memory, CPU entitlement, timeouts, spill | Existing defaults | Existing defaults |
| Reads through the Spice connector | Existing SQL federation | Substrait federation |
| Other connectors and accelerators | Existing behavior | Existing behavior |
| Bind addresses, ports, authentication, readiness | Existing behavior | Existing behavior |

Explicit `runtime.query.target_partitions` overrides the sidecar partition
default. All explicit resource and cache settings retain precedence, including
disabling the results cache. Derive defaults centrally through `cpu-budget`;
do not infer CPU size or duplicate CPU arithmetic at connector call sites.
The `1` partition default applies to execution in the sidecar, not to remote
cluster execution. Keep bounded admission rather than selecting unbounded
concurrency to improve a benchmark.

Mode is selected at startup. A Spicepod reload cannot switch the effective mode;
restart `spiced` to apply a change. The active mode must remain consistent across
resource defaults, existing providers, and providers created by later reloads.
Reject `sidecar` with `--role scheduler`, `--role executor`, or an implied
executor role from `--scheduler-address`, before components start.

### Query planning and execution

1. Authenticate the application request and establish its session and cache
   namespace before looking up a result. Reuse the existing cache probe before
   execution admission and expensive planning wherever that probe is valid.
   A servable hit returns without querying the cluster.
2. Plan cache misses against the sidecar's declared catalog. Use configured local
   accelerations according to existing readiness and fallback policies. A valid
   empty result remains empty unless the dataset explicitly requests existing
   `on_zero_results: use_source` behavior. A partly populated working set cannot
   be inferred complete from an empty or nonempty result.
3. Identify maximal eligible remote subplans. Push filters, projections, joins,
   aggregations, and limits only when their semantics are preserved. Combine
   tables only when endpoint, credentials, organization/application, session
   semantics, and authorization scope are compatible. Rewrite each remote table
   reference from its provider mapping, preserving catalog/schema/name parts,
   quoted identifiers, aliases, and column bindings.
4. Serialize those subplans directly from the sidecar's logical plans to
   Substrait. Do not unparse to SQL and reparse before serialization. Operators
   that cannot be represented or executed remotely stay local above supported
   remote fragments when that yields an equivalent plan. Reject a query when no
   equivalent supported decomposition exists.
5. Send each fragment over the existing Flight connection using
   `CommandStatementSubstraitPlan` directly as a `DoGet` ticket, which the
   existing Spice endpoint supports. The sidecar already knows the result schema,
   so a `GetFlightInfo` round trip is unnecessary. Substrait is the plan
   representation; Arrow Flight SQL remains the transport. The cluster resolves its tables, validates the plan,
   enforces authorization and resource limits, and creates physical/distributed
   plans. It need not parse the application's SQL or know sidecar-only names.
6. Stream Arrow batches, apply any remaining local operators, and populate the
   sidecar's existing results cache only after successful completion under its
   existing eligibility and freshness rules. A partial or failed stream is not
   a complete cache entry. Preserve schemas and error propagation, including
   MAP normalization and dictionary handling on the existing Flight path.

Fallback means using the configured federated source when existing acceleration
policy selects it. It does not mean retrying every local execution error on the
cluster. Local and remote tables may participate in one query, with the sidecar
composing their results; do not transmit local data to the cluster implicitly.
Do not add SQL text fallback for an unsupported upstream Substrait capability.
Such a fallback would obscure which runtime owns planning and make behavior
depend on server version.

The sidecar owns SQL parsing, analysis, logical optimization, and fragment
placement. The cluster may validate and optimize the received fragment and must
own physical optimization, partitioning, and distributed scheduling. Substrait
does not send the sidecar's physical plan or remove all planning at the cluster.

### Freshness and failures

Preserve existing TTL, invalidation, stale-while-revalidate, `Cache-Control`,
and `Spice-Cache-Key` semantics. The mode alone does not promise transactionally
fresh reads, cross-instance invalidation, or a snapshot spanning local
accelerations and remote tables. Serving through the existing results cache
must not silently expand an entry's permitted age. Do not enable stale serving
or cache warmup implicitly.

Preserve TLS, mTLS, token handling, organization/application headers, tracing,
and request context on Flight calls. Propagate cancellation and the
remaining request deadline to remote execution; apply backpressure through the
result stream. Do not restart a fragment after delivering rows or hide a stream
error as end-of-stream. Schema mismatch is an error, not permission to coerce
values or drop columns. Replanning after schema rediscovery must not replay a
partly delivered query.

Older upstreams that do not support the submitted Substrait plan return an
actionable error; the operator upgrades the upstream or uses `server` mode
temporarily. Check capability before serving remote queries where the existing
Flight SQL capability API supports it, and retain a clear error for a capability
rejected at execution. Do not cache authentication, authorization, or execution
failures as query results.

### Errors and logs

The following new text is part of the proposed surface. Use existing error
wrappers and protocol status mappings; do not add a new response envelope.

| Trigger | Message |
| --- | --- |
| Invalid mode | `Invalid runtime.mode '{value}'. Use 'server' (default) or 'sidecar'.` |
| Sidecar with a cluster node role | `runtime.mode: sidecar cannot run as a cluster scheduler or executor. Remove --role and --scheduler-address, or set runtime.mode: server.` |
| Unsupported upstream Substrait operation | `The upstream Spice runtime for dataset '{dataset}' does not support the required Substrait plan: {source}. Upgrade the upstream runtime, or set runtime.mode: server to use SQL federation.` |

On sidecar startup, emit one `info` line:
`Runtime mode: sidecar. Remote Spice reads use Substrait plans created locally.`
On a mode change during reload, retain the running mode and emit one `warn` line:
`runtime.mode changed from '{active}' to '{requested}'. Restart spiced to apply the new mode.`
Other remote failures retain their existing diagnostic and status behavior.
Do not add a log per cache hit, query, or remote fragment. Include the new mode
in the generated Spicepod schema through the Rust types, not a hand edit.

### Observability

Reuse existing results-cache instruments, query/task history, and Flight request
metrics. Retain the existing `results_cache_hit` task-history label and the
Flight method label `statement_substrait_plan` on the cluster. Use `EXPLAIN` and
captured Flight commands to establish placement and transport in qualification.
Do not add a mode metric, endpoint/SQL-valued metric label, or new system-table
column. Any new instrument needed after baseline collection requires an exact
name, unit, label set, and review before implementation.

### Upstream results cache

For Substrait requests, the upstream uses a SHA-256 digest of the serialized
plan under the existing `runtime.caching.sql_results.cache_key_type: plan`.
Namespace and domain-separate this key from SQL and decoded logical-plan keys.
Retain the decoded plan for cache eligibility, table invalidation, and stale
revalidation. Deterministically order sidecar-produced extension declarations.
Substrait requests bypass `sql` and client-supplied key modes; a wire plan has
no SQL text. Native SQL requests retain their configured cache-key behavior.
This refines the existing endpoint's cache behavior as requested on 2026-10-27.

### Security Review

Treat Substrait as untrusted input at the cluster, with the same authentication,
authorization, read-only restrictions, limits, and function permissions as SQL.
Validate both ticket creation and execution. A sidecar's catalog limits what it
can plan, but the cluster must enforce its own permissions independently.

Never combine tenants or credentials in a federation context or a cache key.
An application token must not be replaced by a broader upstream identity without
the existing authorization contract. Keep direct access to undeclared upstream
tables out of the application's catalog. Mode is not a firewall: existing
bindings, supported APIs, connector configuration, and credentials still govern
access. Do not claim it makes arbitrary deployments a security sandbox.

## Manual PM and UX review

**PM/Design assessment: recommend approval of the scoped contract.** It directly
supports the application-local tier while preserving server deployments. One
deployment-intent setting removes protocol selection and common fan-out tuning.
Retain configured working sets rather than adding automatic replication in this
effort. Performance acceptance includes mixed local/remote workloads, preventing
a cache-only demonstration from defining success. Approved by the requester.

**DX/UX assessment: recommend approval.** The golden path adds one YAML line to
an existing Spice connector configuration. `server` is the canonical default;
cluster membership uses familiar existing roles. No new connector protocol knob
or mandatory service is introduced. The error names the upgrade or mode change
needed for an older upstream. Explicit cache freshness and resource settings
retain their meaning. Approved by the requester.

The experience uses standard Substrait over Flight SQL. The intended improvement
over requiring users to compose those protocols themselves is automatic plan
placement from an existing Spicepod, with one application endpoint. Adaptive
working-set selection and distributed cache coherence are outside this scope;
their engineering cost and freshness contracts require separate review.

## How/Implementation Plan

1. Add an internal Substrait execution method to
   [flight_client](../../crates/flight_client/src/lib.rs), using Arrow's existing
   command types and shared authentication, metadata, TLS, cookies, streaming,
   and errors. A real Flight service round trip must inspect the command and
   return known Arrow rows. No new wire protocol or DataFusion dependency is
   needed in the transport crate.
2. Add plan-based federation for the Spice connector rather than using
   [the SQL executor](../../crates/data_components/src/flight/federation.rs).
   Keep generic Flight/Flight SQL sources on their existing paths. Preserve the
   fallback scan, write, subscription, and acceleration-refresh contracts while
   using Substrait for sidecar Spice source reads. Audit all provider wrappers
   and federation contexts; an unimplemented wrapper method must not silently
   select SQL or disable pushdown. Use the existing federation extension points
   or extend that dependency's plan interface without unparsing to SQL.
3. After sign-off in the Enhancement issue, add the mode to both `Runtime` and
   `RuntimeDeserializer` in
   [Spicepod runtime](../../crates/spicepod/src/component/runtime.rs). Resolve and
   freeze the effective mode once, thread it through application/connector
   context, validate cluster role conflicts, and add the CPU-budget profile.
   Select the new provider only for sidecar Spice reads. Preserve
   organization/application metadata in
   [SpiceAI](../../crates/runtime/src/dataconnector/spiceai.rs).
4. Reuse the cluster's
   [Substrait handler](../../crates/runtime/src/flight/flightsql/statement_substrait_plan.rs)
   and distributed execution path. Extend producer/consumer coverage only where
   the sidecar's actual emitted plans require it. Do not claim all Substrait
   operations or cross-version combinations are supported from TPC-H alone.
5. Land qualification scenarios, collect benchmarks, regenerate/review schemas,
   and document rollout. The default changes only for explicitly opted-in
   sidecars. Roll back by selecting `server` and restarting; cache entries may
   be discarded and configured acceleration files remain intact.

Keep capability libraries below `runtime` in the crate layering. New optional
capabilities belong in crates selected by `spiced`, not new features on shared
library crates. Fix the package feature set before compilation and use one cargo
invocation at a time.

## QA Plan

The primary protocol comparison measures client → sidecar → standalone upstream
for Substrait and for SQL federation, with identical CPU, memory, admission, local
partition count, data, and queries. Interleave A/B/B/A runs. Disable both caches
for remote-miss measurements; measure sidecar-cache hits separately. Retain
upstream plans and metrics. The requester selected a standalone upstream to
isolate the uplink cost; distributed-cluster qualification is a separate run.

For the cluster results cache, the caller additionally specified that Substrait
requests use the serialized plan's SHA-256 digest under `cache_key_type: plan`.
Include the caller's cache namespace and a serialized-plan domain separator in
the key. Retain the decoded plan for eligibility, invalidation, and background
revalidation. Substrait requests bypass SQL-key and client-supplied-key caching;
their task-history description is not SQL. Sidecar-produced extension
declarations are ordered by kind and anchor for deterministic serialization.

Extend existing Spice connector and Substrait suites, and add a spicescript
scenario for the sidecar/cluster topology. Start an upstream `spiced` as a source
using the harness's Docker source or extend source lifecycle support if needed;
do not pretend independent variants form a connected sidecar and cluster.
Repeat against a scheduler plus executors as well as a standalone upstream.

| Path | Required external evidence |
| --- | --- |
| Default compatibility | Omitted mode and explicit `server` return the same schemas and rows and retain SQL federation |
| Both connector spellings | Capture `CommandStatementSubstraitPlan`, prove no SQL command is sent for sidecar Spice reads, including a non-federated fallback scan |
| Local cache | Warm a result, make the upstream unavailable, repeat within TTL, check exact rows and cache-hit metadata with no remote execution |
| Local acceleration | Populate Cayenne, expire/bypass the result cache, make the upstream unavailable, and check exact rows plus a plan reading Cayenne |
| Source fallback | Exercise configured not-ready and zero-result policies; verify exact remote rows, Substrait commands, and genuine empty results |
| Placement | `EXPLAIN ANALYZE` plus wire capture for remote joins/aggregations/limits, mixed local/remote joins, and incompatible endpoint/auth contexts |
| Correctness | Diff schemas and row multisets against an independent DuckDB/PostgreSQL oracle over seeded tables and a query corpus |
| Boundaries | NULLs, empty sets, decimals, overflow, timestamps/time zones, Unicode, quoted names, schema drift, dictionaries, and nested MAP values |
| Failures | Unsupported operation/version, permission denial, deadline, cancellation, upstream restart, mid-stream loss, and partial cache population |
| Isolation | Two sessions/identities with the same SQL cannot reuse unauthorized results or fuse remote fragments |
| Configuration | Invalid values, explicit overrides, incompatible cluster roles, restart-only mode changes, and generated schema |

Watch each regression scenario fail with the guarded behavior disabled and then
succeed on the implementation. Record commands, exact output, date, and commit
SHAs for both runs. Report an unexercised path as unverified. Add targeted
integration tests for states the external harness cannot reach, with the reason
recorded beside the test.

Benchmark the same binary/rig with `server` and `sidecar` profiles, then compare
the baseline and candidate commits with interleaved A/B/B/A runs. Use
`testoperator` for query load and retain per-query timings, returned rows,
validation output, metrics, and run directories. Exercise concurrency
1/8/32/128 at 1/2/4 CPU budgets: warm results cache, cold remote misses,
local point lookups, aggregations, mixed joins, unique-query bursts, and slow
consumers. Separate SQL/plan/serialization time, remote request count, and
execution time in the artifacts. Report P99 (P99.9 with sufficient samples),
throughput, errors, and RSS under a fixed memory limit. Include expiry bursts,
cache-disabled runs, sustained execution, and `/health` under load.

Ship only with equal results, bounded memory and admission, and no per-workload
performance regression beyond the measured noise floor. Report improvements
only where those runs show them; Substrait alone is not a latency measurement.
The current documentation-only draft cannot reach a shipped binary, so it has
no runtime performance numbers.

## Release Notes

Proposed release copy, to publish only after qualification:

**Application sidecars:** Set `runtime.mode: sidecar` to plan queries next to your
application, serve eligible cached and accelerated results locally, and send
remote Spice reads as Substrait plans for execution by your Spice cluster.
Existing deployments retain `server` behavior by default. Sidecar mode reuses
your current cache, acceleration, freshness, and resource settings.

Document the mode reference, self-hosted and cloud examples, local Cayenne
working sets, upstream compatibility, freshness, restart behavior, and rollback
in the same release. Include a cookbook deploying an application and its sidecar
against a shared Spice cluster with existing authentication.
