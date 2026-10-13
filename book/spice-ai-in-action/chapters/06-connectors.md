# 6. Databases, files, APIs, and catalogs

The right connector is the one whose operational and semantic behavior fits your source. A list of supported systems is only the beginning. Northstar needs to know how types map, which privileges are required, what work can be pushed down, and what happens when a source changes or becomes unavailable.

## 6.1 A connector acceptance card

Before registering a production dataset, create a short acceptance card. Record the source system and version, dataset locator, key, expected schema, authentication method, connection budget, read or write capability, and refresh mechanism. Add the smallest query that proves access and the smallest query that proves the business contract.

For a PostgreSQL table, `SELECT 1` checks a connection but not table permissions. A query against the actual table checks access but not timestamp or decimal fidelity. Include representative values, a NULL, and an identifier requiring quoting if the real schema uses one. Validate data types at the Spice boundary.

Treat connector maturity and feature support as release-specific. A connector can support federation without supporting every refresh mode or every data type. A source that permits writes does not imply that its connector or selected acceleration path exposes a general transaction interface.

## 6.2 PostgreSQL federation

**Integration lab.** Create a disposable `northstar` database and load the fixture with a typed table. A representative source schema is:

```sql
CREATE TABLE public.orders (
  order_id BIGINT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  customer_id BIGINT,
  ordered_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL,
  total_cents BIGINT NOT NULL
);
```

Use a reader role with access to the database, schema, and table. Supply the password through the environment and configure the dataset:

```yaml
# Dataset fragment
- from: postgres:public.orders
  name: orders
  params:
    pg_host: ${ env:PG_HOST }
    pg_port: '5432'
    pg_db: northstar
    pg_user: spice_reader
    pg_pass: ${ env:PG_PASS }
    pg_sslmode: verify-full
```

Configure the root certificate parameter where the source certificate chain requires it. Hostname verification must match the address used. A local disposable server with no TLS may use `disable` for that lab; carry a properly verified connection into deployed environments.

Run the chapter 2 query and reconcile 22,200 and 24,900 cents. Inspect the schema. Compare an `EXPLAIN` plan with the file-backed plan. Finally, revoke the reader's table permission in the disposable database and observe the error path. Restore the permission and verify recovery. This checks the integration's failure behavior without mistaking a connection success for complete acceptance.

## 6.3 File and object-storage sources

File connectors are useful for controlled fixtures, batch exports, and data-lake inputs. Their simplicity hides a publication problem: when is a set of files complete? If a producer writes into the same directory that readers scan, readers may observe an incomplete export unless the publication protocol prevents it.

Prefer writing a new immutable version and publishing a manifest or pointer after it is complete. Use supported table formats when snapshot semantics are needed. A directory name alone is not a transaction boundary.

For S3, a dataset declaration might use:

```yaml
# Dataset fragment; replace the bucket and prefix
- from: s3://northstar-example/orders/
  name: historical_orders
  params:
    file_format: parquet
```

The bucket is illustrative and is not an available public dataset. Configure credentials using the documented provider chain or connector parameters for the selected environment. Check permissions for both listing and reading as required by the access pattern. A principal able to read a known object may still be unable to discover objects under a prefix.

Object storage adds request cost, network behavior, and potentially many small objects. Capture object counts and observed transfer volume before choosing a file-size strategy. Partitioning and columnar formats are useful only when the query and engine can exploit their layout.

## 6.4 API and document connectors

A SaaS or HTTP connector may have pagination, rate limits, eventual consistency, and fields that appear only for some records. The provider's API often has a different data model from a relational table. Verify whether a scan represents a complete snapshot, a time-windowed view, or only the objects discoverable with the supplied credentials.

For document sources, distinguish identity from content. A document path, source ID, version, and last-modified value serve different purposes. Use a stable identity for updates and deletions; retain provenance for citations. A renamed path should not accidentally leave an old policy permanently searchable.

Do not send an entire document collection to an external embedding service before defining which fields and tenants are allowed to leave the environment. This is a data-flow choice made at ingestion, not only when a user asks a question.

## 6.5 Catalogs trade convenience for exposure

A catalog connector discovers tables from a source namespace. It is useful when explicitly declaring hundreds of datasets would be unmanageable. Discovery also changes the review surface: a newly created source table can become discoverable through a policy that was approved months earlier.

Define inclusion and exclusion rules using the exact semantics documented for the catalog connector. Test them with representative schema and table names, including names containing punctuation. Record which tables were registered after startup. A catalog that returns a subset because of source permissions can look healthy while omitting data the application expects.

Catalog-wide acceleration is a capacity decision. A rule that begins with five small tables can later include a very large table. Put ownership and resource limits around broad discovery, and retain an inventory of expected tables as part of readiness checks.

## 6.6 Operational ownership

Each connector adds an external dependency. Give it an owner, a credential-rotation procedure, a connection budget, and a recovery playbook. A source migration should preserve the dataset contract or deliberately version it. Never use a silent coercion or dropped field to make a migration appear successful.

**Exercise.** Build an acceptance card for one database source and one document source. Include an outage test and a deletion test. Specify which evidence would make you refuse the integration even if ordinary queries succeed.

**Further reading.** Use the [connector reference](https://spiceai.org/docs/components/data-connectors) for exact source parameters. Relevant cookbook directories include `postgres/connector/`, `s3/`, `github/` where present, `snowflake/`, and `databricks/`. Verify the recipe's stated version against your deployment.
