# Experimental query coalescing

Query coalescing combines concurrent point lookups into a shared scan with `IN`
predicates. Each original query receives at most one row for its own exact key.
It applies to synchronous, anonymous HTTP SQL requests over accelerated tables
that return an empty result when a key is absent.

## Enable an experiment

Set these environment variables before starting `spiced`:

| Variable | Default | Behavior |
| --- | --- | --- |
| `SPICE_QUERY_COALESCE_MAX_BATCH_SIZE` | `1` | Maximum requests per batch; `1` disables coalescing. Values are clamped to 1–65,536. |
| `SPICE_QUERY_COALESCE_MODE` | `queued` | `queued` collects when query admission is saturated or a shared producer of the same family exists; `window` always collects eligible requests. |
| `SPICE_QUERY_COALESCE_WINDOW_MS` | `1` | Minimum collection time in `window` mode, clamped to 1–1,000 ms. |

For example:

```sh
SPICE_QUERY_COALESCE_MAX_BATCH_SIZE=128 SPICE_QUERY_COALESCE_MODE=queued spiced
```

A missing or nonnumeric batch size disables coalescing. A missing or nonnumeric
window uses 1 ms. An unknown mode logs a warning and uses `queued`.
`runtime.query.max_concurrent_queries: 0` disables query admission and bypasses
coalescing as well.

When disabled, the runtime skips query recognition and batching. In `queued`
mode, an eligible request takes an available execution permit immediately when
its family has no shared producer; it pays no collection timer.

A batch stays open until it obtains execution admission or reaches its size cap.
The collection window is a minimum time, not a deadline for sealing or responding.
Reaching the size cap can end collection before that time. A large cap supports
time-based experiments, but neither the cap nor the window bounds total latency.

## Eligible SQL

```sql
SELECT id, name FROM items WHERE account_id = 'account-a' AND item_id = 'item-b' LIMIT 1
```

The plan must be `LIMIT 1`, an optional projection of plain columns or aliases,
an equality filter, and one table scan. It can contain up to eight distinct key
columns. String, integer, and boolean keys are accepted; integer conversions
must round-trip exactly. NULL literals, cross-type comparisons, expressions,
ordering, offsets, joins, aggregates, and other limits use ordinary execution.

Authenticated requests, client-selected sessions, transactions, caching
acceleration, tables whose reads use the federated source, and source fallback
on empty results bypass coalescing. Their
request-specific read semantics require independent execution.

## Execution and isolation

A family identifies the provider instance, session, cache namespace, key columns,
output projection, and output schema. Families retain state only while shared
producers exist. One shared producer per family executes at a time. Individual
queries that already obtained admission may still overlap that producer.

The shared logical plan projects the union of key and output columns and filters
with one `IN` list per key column. For composite keys, those independent lists
are a prefilter: exact tuple matching rejects cross-pairs. Duplicate requests
share a matching row. Each key takes its first matching row; without `ORDER BY`,
SQL does not define which matching row wins. Missing keys receive an empty batch
after the stream ends. The shared scan has no global `LIMIT`.

Cancellation removes a request's interest without cancelling other members.
When all callers cancel, collection, admission, physical planning, and scan
execution stop. Shutdown cancels and drains shared producers before removing
accelerated tables. The producer retains cache scope while using independent
cancellation state.

## Metrics

Instruments use the `query_coalescing` meter and have no key or SQL labels:

- `query_coalescing_requests`: requests joining shared work.
- `query_coalescing_batches`: admitted batches with live callers.
- `query_coalescing_flushes{reason="size"|"admission"}`: why collection ended.
- `query_coalescing_unique_keys`: distinct tuples in admitted batches.
- `query_coalescing_candidate_rows`: rows consumed from shared scans.
- `query_coalescing_batch_size`: live requests per admitted batch.
- `query_coalescing_collection_ms`: first arrival to admission.
- `query_coalescing_admission_ms`: permit wait after the collection window; this
  overlaps collection time and must not be added to it.
- `query_coalescing_execution_ms`: planning and shared execution duration.
- `query_coalescing_planning_ms`: physical plan creation duration.
- `query_coalescing_first_batch_ms`: planning start to the first Arrow batch,
  including an empty first batch.

A logical request still records its own latency and result. Its physical plan is
shared, so per-request physical scan statistics can refer to the same work and
must not be summed to estimate total I/O. Coalescing metrics describe the shared
producer directly. Instruments are initialized when first used after runtime
telemetry startup.
