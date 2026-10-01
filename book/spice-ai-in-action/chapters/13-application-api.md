# 13. Building a bounded SQL application API

Northstar's browser should ask for its sales summary, not receive a database credential and a text box for arbitrary SQL. The application service authenticates a user, derives the tenant, binds values into a fixed query, and returns a small typed response. Spice supplies the data execution boundary behind that service.

## 13.1 Bind values, control structure

The local runtime supports parameterized SQL over HTTP using a JSON body. The companion verification executed this pattern and returned 22,200 cents for `north`:

```bash
curl --fail-with-body -sS http://127.0.0.1:8090/v1/sql \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  --data '{"sql":"SELECT SUM(total_cents) AS gross_cents FROM paid_orders WHERE tenant_id = $1","parameters":["north"]}'
```

Bind user-derived values. Choose table names, column names, sort expressions, and SQL structure from server-controlled code or a strict allowlist. A placeholder for a value is not a general mechanism for substituting SQL identifiers.

Most importantly, derive `tenant_id` from the authenticated session or verified service identity. A well-parameterized request can still read the wrong tenant if the caller is allowed to choose that tenant without authorization.

## 13.2 A small Python client

The companion `app_client.py` uses the standard library, an explicit timeout, and parameter binding. Its core operation is:

```python
payload = {
    "sql": """
      SELECT tenant_id, COUNT(*) AS paid_orders,
             SUM(total_cents) AS gross_cents
      FROM paid_orders
      WHERE tenant_id = $1
      GROUP BY tenant_id
    """,
    "parameters": [tenant_id],
}
request = urllib.request.Request(
    base_url + "/v1/sql",
    data=json.dumps(payload).encode("utf-8"),
    headers={
        "Content-Type": "application/json",
        "Accept": "application/json",
    },
)
with urllib.request.urlopen(request, timeout=10) as response:
    rows = json.load(response)
```

The listing shows the query boundary. The complete companion script validates its tenant argument for the local demonstration and checks the response shape. A production service must obtain tenant identity from its own authentication layer; a CLI allowlist is not that layer.

Preserve the distinction between no matching rows and a request failure. An HTTP error must not become `gross_cents = 0`. That would turn an unavailable dataset into a false sales report. Return an application error with a request identifier and retain the detailed upstream cause in controlled diagnostics.

## 13.3 Choose the result transport

JSON is convenient for small application responses. It carries a serialization cost and a less expressive type surface than Arrow. For large analytical results or columnar clients, evaluate Arrow Flight SQL and ADBC.

The cookbook's ADBC pattern connects using `grpc://127.0.0.1:50051` and binds parameters through the cursor API:

```python
from adbc_driver_flightsql.dbapi import connect

with connect("grpc://127.0.0.1:50051", autocommit=True) as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT order_id, total_cents FROM paid_orders "
            "WHERE tenant_id = $1 ORDER BY order_id",
            parameters=("north",),
        )
        result = cur.fetch_arrow_table()
```

This is an integration example requiring the ADBC Flight SQL driver. `fetch_arrow_table()` materializes the returned table in the client. For large results, use the driver's supported batch or reader interface and process incrementally. The fact that the transport is Arrow does not make an unbounded client collection safe.

Authenticate and encrypt Flight separately according to the deployed interface's settings. An authenticated HTTP client does not configure a different client's gRPC channel by implication.

## 13.4 Put limits at the application boundary

Define maximum rows, maximum date span, allowed groupings, and a request deadline. A SQL `LIMIT` bounds returned rows, but a query may still scan or sort a large relation before returning them. Bound the query's input domain where the use case permits it.

For an order list, use keyset pagination with a stable ordering. A continuation token can contain the last timestamp and order key, signed or otherwise protected by the application. Reapply tenant scope on every page. Decide whether pages reflect a fixed snapshot or a changing live dataset and communicate that behavior.

For a report export, use an asynchronous job if the deployment supports the necessary workflow. Keep job ownership and result access tied to the submitting identity. A long-running result object is another protected resource, not merely a URL anyone may fetch.

## 13.5 Retry the operation you actually have

Read-only requests can often be retried after a transient failure, but a retry consumes capacity and may read a newer state. Use a bounded retry policy with jitter and a total deadline. Do not retry every error: a bad query, missing permission, or incompatible type needs correction.

A timed-out write has an uncertain outcome until the caller determines whether it committed. Use a supported idempotency or transaction protocol rather than replaying it as though no response means no effect. The book's application client is intentionally read-oriented.

Cancellation is also a protocol behavior. Verify whether disconnecting the client cancels remote work or whether an explicit query cancellation interface is needed. A browser navigating away does not automatically prove that the database stopped executing.

## 13.6 Version the response meaning

A response can include the tenant, metric definition version, currency, covered interval, and freshness state. Do not expose raw internal table names or stack traces to a customer unless they help that customer act. Give the application a stable error vocabulary while preserving enough diagnostic context for operators.

The sales endpoint's definition should state whether it includes pending orders, whether refunds are attributed to purchase or return date, and whether zero-value paid orders count. These are API compatibility concerns. Changing them can be a breaking change even when the JSON schema stays the same.

**Exercise.** Implement a `sales_summary` function that derives its tenant from a supplied trusted context object, never a request body. Test an empty tenant result, an unavailable runtime, and an attempted SQL-like tenant string. Explain why parameterization solves only one of those cases.

**Further reading.** See the [HTTP API reference](https://spiceai.org/docs/api), cookbook `clients/adbc/`, and the request parsing in `crates/runtime/src/http/v1/query.rs`. The local parameter result is recorded in `evidence/stable-sql.json`.
