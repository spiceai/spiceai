# Query Trace ID

Every query Spice runs carries a **trace id**: a 32-character hexadecimal (16-byte)
correlation id, the same value the `trace_id` column of `runtime.task_history`
records. A client may pin it, and Spice generates one when the client does not.

The id is emitted on the log records a query produces — its own and those of
everything the query reaches — so a failure and the records explaining it can be
found together **whether or not `runtime.task_history` is recording**.

```
2026-08-05T09:14:22.118431Z  WARN query{trace_id=4bf92f3577b34da6a3ce929d0e0e4736}: data_components::postgres: connection reset by peer
2026-08-05T09:14:22.118502Z  WARN query{trace_id=4bf92f3577b34da6a3ce929d0e0e4736}: runtime::datafusion::query::tracker: Query refused, out of memory (ResourcesExhausted): Resources exhausted: …
```

`query{…}` is the span the id travels on — standard `tracing` rendering for an entered span, so
`grep trace_id=4bf92f35` finds every record of that query.

A query refused for want of memory is logged at `WARN`, so it is visible at the default verbosity
— that refusal is an outage an operator cannot see any other way, since `/health` is served by a
separate runtime and stays green throughout. Every other failure is `DEBUG` (`Query failed (…): …`)
and needs `spice run -v` or `SPICED_LOG`: a rejected query is usually the caller's problem, and
promoting every one would bury the condition above. Records the query *provokes* keep the level
they were emitted at.

Both are written while the query's span is still entered, which is what puts the id on them, and
both are protocol-agnostic — a Flight query's failure is named the same way as an HTTP one.

## Pinning an id from the client

Send either header. Both work on HTTP and on Flight / Flight SQL, where gRPC
metadata *is* HTTP/2 headers.

| Header             | Value                                      | Use when                                        |
| ------------------ | ------------------------------------------ | ----------------------------------------------- |
| `spice-trace-id` | 32 hexadecimal characters, not all zero    | The caller just wants an id it chose             |
| `traceparent`      | W3C trace context, e.g. `00-{trace_id}-{span_id}-01` | The caller is part of a distributed trace |

```bash
curl -X POST http://localhost:8090/v1/sql \
  -H 'spice-trace-id: 4bf92f3577b34da6a3ce929d0e0e4736' \
  -d 'SELECT * FROM taxi_trips LIMIT 10'
```

A request carrying both uses `spice-trace-id`: a caller sets it knowing what it
wants correlated, whereas a `traceparent` is routinely injected by a proxy or APM
agent that knows nothing about the request.

The `traceparent` span is then *not* recorded as the task's parent. A span id is only
meaningful inside its own trace, so recording one from the losing trace would put an edge in
the `task_history` tree that exists in no caller's graph — and anything joining on
`(trace_id, parent_span_id)` would follow it. When the two headers agree, or when only a
`traceparent` is sent, the parent span is recorded as usual.

A malformed value is not silently recorded — Spice warns, then numbers the task
itself, so a bad header costs correlation rather than the request.

Pinning is also what makes one id span a distributed query: the id travels with
the query to cluster executors, so their task-history rows and log records name
the id the caller chose rather than one of their own.

## When the client pins nothing

Spice resolves an id anyway:

- **Task history recording** — the id is the task-history span's own
  OpenTelemetry trace id, so a log record and the `runtime.task_history` row it
  belongs to always name the same id, and

  ```sql
  SELECT * FROM runtime.task_history WHERE trace_id = '4bf92f35…';
  ```

  finds the row for a record seen in the log.

- **`runtime.task_history.enabled: false`** — the id is generated per query.
  Nothing is written to the table, so there is nothing to agree with; the id
  exists to tie the query's own records together.

Each query gets its own id. A client that wants several queries under one id
pins it with a header.

## Scope

The id covers a query's whole lifetime — planning, execution, and result
streaming — so a failure raised at any point in that window is attributable,
including one raised mid-stream after the response has begun.

The span is entered around SQL query execution. A chat completion, a search, or an
embedding call is a task-history task in its own right and gets a `trace_id` column,
but its log records carry the id only for the queries it runs — under a pinned id
those queries share the caller's id, and with task history recording they inherit
the parent task's trace id.

## Reading the id back

Pinning presumes a caller can set a header per query. Plenty cannot: with HikariCP
the JDBC URL — and so every gRPC header the Arrow Flight SQL driver sends — is fixed
when the pool is built, and an MCP client does not choose the headers of a tool call
at all. So Spice *returns* the id as well, on every response it has settled one for,
with nothing to configure.

| Protocol            | Where                                                                  |
| ------------------- | ---------------------------------------------------------------------- |
| HTTP, MCP           | `spice-trace-id` response header                                        |
| Flight, Flight SQL  | `spice-trace-id` response metadata — gRPC metadata *is* HTTP/2 headers  |
| Flight SQL          | also `FlightInfo.app_metadata`, the only surface the JDBC driver exposes |

Each protocol writes it in the layer that wraps every request, below the point a
handler's error becomes a response, so **a failed query carries the id too** — which
is the response most worth correlating. A request that ran no query (a health check, a
handshake) carries no header.

`app_metadata` is JSON so it can carry a second field later:

```json
{ "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736" }
```

From the Arrow Flight SQL JDBC driver, unwrap the result set — this works through a
HikariCP proxy, which delegates `unwrap`:

```java
try (ResultSet rs = statement.executeQuery(sql)) {
    byte[] metadata = rs.unwrap(ArrowFlightJdbcFlightStreamResultSet.class).getAppMetadata();
    // {"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"}
}
```

Log that id next to the application's own request id and the two sides join:

```sql
SELECT * FROM runtime.task_history WHERE trace_id = '4bf92f35…';
```

### Why the id survives the two RPCs

A Flight SQL query is two requests — `GetFlightInfo` plans it, `DoGet` runs it — each
with its own headers and its own request context. `GetFlightInfo` is the one that can
answer the client, but `DoGet` is the one that executes and logs a failure, so an id
returned by the first and unused by the second would name the planning call and
correlate nothing.

`GetFlightInfo` therefore resolves the id, returns it, and wraps the ticket it hands
out with it; `DoGet` unwraps it and joins that trace before the query starts. Tickets
are opaque to clients, which echo them back unread, and a ticket without an id — from
an older runtime, or a client that built its own — still works: the query numbers
itself as before.

Joining the trace, rather than declaring the id as an override the way a pinned one
is, is what keeps this free: an override is reconciled afterwards by scanning
`runtime.task_history` for the rows to rewrite, which on this path would be a scan per
query.

Only Flight SQL needs this. HTTP and MCP answer from the same request that ran the
query, so the id is simply read off the request once a task has resolved one.
