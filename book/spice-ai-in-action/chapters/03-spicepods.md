# 3. Spicepods as executable data contracts

A configuration file can look harmless while deciding the meaning of an entire application. Changing a dataset source can alter timestamp precision. Changing refresh behavior can alter when a refund appears. Adding a catalog can expose tables that nobody intended the assistant to query. Treat the Spicepod as a reviewed data contract, with the same care you give an API schema.

## 3.1 Separate identity from location

A dataset has a source locator and a SQL name. Northstar's `orders` name is a logical interface; `file://data/orders.csv` is a physical source. Replacing that source with `postgres:public.orders` should be accompanied by explicit checks of column names, types, NULL behavior, and key uniqueness.

A useful contract records more than a schema. For `orders`, define one row per order, an order key unique within its declared scope, a tenant identifier required for every row, UTC event time, and an integer amount denominated in a stated currency. If order identifiers are only tenant-unique in production, then `(tenant_id, order_id)` is the key. The fixture's globally unique identifiers must not quietly become a production assumption.

Descriptions and column metadata help readers and model tools discover intended meaning, but prose is not an access-control mechanism. A description saying “internal only” does not prevent a query. Enforce exposure through credentials, network boundaries, dataset selection, and the application interface.

## 3.2 Know where a parameter belongs

The top-level `runtime` block controls the process and common services. Dataset `params` configure a connector. The `acceleration` block configures an accelerator and refresh behavior. An accelerator's own `params` configure that storage engine. Model parameters belong under their model. These namespaces may contain similar words with different meanings.

```yaml
# Dataset fragment: connector settings and accelerator settings
- from: postgres:public.orders
  name: orders
  params:
    pg_host: ${ env:PG_HOST }
    pg_db: northstar
    pg_user: spice_reader
    pg_pass: ${ env:PG_PASS }
    pg_sslmode: verify-full
  acceleration:
    enabled: true
    engine: duckdb
    mode: file
    refresh_mode: full
    refresh_check_interval: 1m
    params:
      duckdb_file: .spice/duckdb/orders.db
```

This is an integration fragment, not a claim that the database exists. Its password comes from the environment, its source connection uses the PostgreSQL connector namespace, and its storage path uses the DuckDB accelerator namespace. Create the parent directory before starting the file-backed lab.

Unknown-field checks catch some mistakes, but connector parameter maps may accept names that are validated later. Schema validation therefore supplements runtime startup; it cannot replace it. A syntactically valid Spicepod can still name an unavailable connector, missing secret, or unreadable file.

## 3.3 Make environments explicit

Use separate complete configurations for a few small environments, or generate them from a controlled template when repetition becomes significant. In either case, save the fully resolved nonsecret configuration used in each test. An environment variable that changes a source host is part of the deployment state, even though the hostname is absent from the committed file.

Keep secrets out of generated output. Refer to a named secret store or environment source, inject credentials at runtime, and record only the secret's identity or version in a deployment manifest. A diagnostic bundle should explain which credential was selected without containing its value.

Avoid making one large file depend on undocumented shell state. A new engineer should be able to identify required variables from an example environment file containing names and nonsecret defaults. The Northstar production configuration might require `PG_HOST`, `PG_PASS`, `BOOK_API_KEY`, and a model-provider credential; the local starter requires none.

## 3.4 Views stabilize business meaning

A view can normalize types, select columns, and name a business definition. Northstar uses `paid_orders` so that every dashboard does not reinvent the paid-status predicate. A view should have a named owner and a versioned definition when downstream callers depend on its semantics.

For a longer query, use the supported `sql_ref` field to refer to a SQL file instead of embedding a large block in YAML. Keep that file with the Spicepod. Review changes to the SQL and configuration together. Do not set both an inline query and a file reference unless the schema explicitly defines the intended precedence.

Views can depend on other views. A dependency chain creates startup ordering and availability implications. Keep it understandable: raw datasets, a small normalization layer, and application views are easier to operate than a long chain whose final error obscures the unavailable source.

A view that filters `tenant_id = 'north'` is a useful relation, but it is not a complete tenant isolation policy while callers can query the underlying table. The accessible namespace and the caller's available operations are part of the same design.

## 3.5 Schema evolution is an application change

Suppose the source adds `currency_code`. A consumer summing all `total_cents` without grouping by currency is now at risk of assigning an invalid meaning to a perfectly computed sum. A column-addition policy can keep ingestion available, but it cannot decide whether the business metric remains valid.

Classify schema changes into additive fields, type widening, incompatible types, removals, and semantic changes that leave types intact. The last category is often overlooked. Changing a timestamp from “order created” to “payment settled” can invalidate a daily-sales chart without changing a single SQL type.

The inspected source includes `on_schema_change` behavior with modes such as `block`, `fail`, `append_new_columns`, and `sync_all_columns`; availability and engine support depend on the release. Read the matching dataset reference and exercise the chosen connector–accelerator pair. Never assume that accepting a configuration value proves every engine can perform that migration.

For production, create a migration rehearsal: start with the old schema and data, apply the proposed change, restart if the documented procedure requires it, and compare row counts, key sets, and representative queries. Preserve old storage until the new representation is accepted. A destructive rebuild is a recovery plan only if the source can reproduce the required history.

## 3.6 Review a Spicepod like code

A useful review asks which source will receive traffic, what credentials it needs, how much data may be loaded, when data becomes ready, where state is stored, and what downstream meaning changes. Require a runtime startup check and a small query contract for material changes. For deployment changes, add a restart check; for replication changes, add recovery checks.

**Exercise.** Write a contract for `articles`: key scope, tenant scope, text encoding, deletion behavior, and the meaning of “current policy.” Then decide which fields the assistant may receive. A search index should be derived from that contract rather than becoming the place where it is first invented.

**Further reading.** Consult the [Spicepod reference](https://spiceai.org/docs/reference/spicepod), source definitions under `crates/spicepod/src`, and cookbook recipes `acceleration/data-refresh/` and `api_key/`.
