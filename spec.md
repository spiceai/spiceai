<!--
_REMEMBER, BE **SMART**!_

_S: Specific_
_M: Measurable_
_A: Achievable_
_R: Relevant_
_T: Time-Bound_
-->

## Goal-State/What/Result

Tables from federated Iceberg catalog sources can be accelerated via DDL `CREATE TABLE ... WITH ("acceleration.*")` statements. Accelerated tables support data ingestion (via `INSERT INTO` / Arrow Flight `DoPut`) that writes through to the underlying federated catalog source, making the acceleration a write-through cache with the federated source as the durable source of truth.

## Why/Purpose

Today, data written into accelerated tables does not propagate through to the federated data source. The acceleration layer becomes the sole storage for ingested data, tying durability to executor availability. Write-through ensures the federated source remains the canonical store - surviving executor loss and allowing the acceleration to be rebuilt from the source on refresh.

## By When

**Issue/Spec written and reviewed:** April 2nd, 2026
**Done-Done:** April 3rd, 2026

## Done-Done

- [x] [Principles Driven](https://github.com/spiceai/spiceai/blob/trunk/docs/PRINCIPLES.md)
- [x] The Algorithm
- [x] PM/Design Review
- [x] DX/UX Review
- [x] Release Notes / PRFAQ
- [ ] Threat Model / Security Review
- [ ] Tests
- [ ] Telemetry / Metrics / Task History
- [ ] Performance / Benchmarks
- [ ] Documentation
- [ ] Cookbook Recipes/Tutorials

## The Algorithm

- [x] Every requirement questioned?
- [x] Delete (Scope) any part you can.
- [x] Simplify.
- [x] Break down into smaller iterations/milestones.
- [x] Opportunities for automation.

## Specification

### Federated Catalog Sources in Spicepod

The spicepod defines federated catalog sources. Acceleration is **not** defined in the spicepod - it is defined via DDL at query time.

Catalogs are defined like usual in the spicepod, with DDL-enabled catalogs supported where `access_mode: read_write_create`. For example, **Iceberg catalog**:

```yaml
catalogs:
  - name: iceberg_catalog
    from: iceberg:my_iceberg_catalog
    access_mode: read_write_create
    params:
      .. Iceberg connection params ..
```

### DDL Acceleration with Write-Through

Users create accelerated tables over federated catalog tables using DDL. When an acceleration is added to a catalog table, the acceleration engine is configured as a write-through cache for the federated source. Data ingested is automatically written through to the source, and the write is not acknowledged until the write-through is successful.

```sql
-- Accelerate an Iceberg catalog table with write-through enabled
CREATE TABLE iceberg_catalog.my_schema.events (
    id BIGINT,
    region TEXT,
    payload TEXT,
    created_at TIMESTAMP
) WITH (
    "acceleration.engine" = 'duckdb',
    "acceleration.mode" = 'file',
    "time_column" = 'created_at',
    "time_format" = 'timestamp'
)
PARTITION BY (region);
```

Data ingested into the accelerated table (via `INSERT INTO` or Arrow Flight `DoPut`) is written to both the acceleration engine and the federated source concurrently. The accelerator write is held in a pending transaction - data is not queryable until the Iceberg commit succeeds. Once the Iceberg write commits, the accelerator transaction is committed, making the data visible. The write is not acknowledged to the client until both writes succeed.

If the Iceberg write fails, the accelerator transaction is rolled back and the write fails - the acceleration never contains data that is not in the source. If the accelerator write fails after a successful Iceberg commit, the write fails; the data is safely in Iceberg and will be reconciled into the accelerator on the next refresh cycle.

### ACID and Transaction Semantics

The Runtime provides per-system ACID guarantees and fail-safe end-to-end write semantics:

- **Atomicity**: Each individual write to Iceberg and to the accelerator is atomic within that system.
- **Consistency**: Schema and constraints are validated before acknowledgement.
- **Isolation**: Concurrent writes follow the isolation guarantees of the underlying engines (e.g. Iceberg snapshot isolation).
- **Durability**: A write is never acknowledged unless the Iceberg write commits durably. Accelerations are a cache layer on top of a durable source of truth.

The Runtime does not provide a single distributed ACID transaction across Iceberg and the accelerator. Instead, writes to the accelerator and Iceberg proceed concurrently, with the accelerator transaction held pending until the Iceberg commit succeeds. The accelerator commit is synchronized to the Iceberg commit - data is not visible in the accelerator until Iceberg confirms durability. The write is acknowledged only after both commits succeed. If the Iceberg write fails, the accelerator transaction is rolled back, keeping Iceberg authoritative. If the accelerator write fails, the request fails; the data is safely in Iceberg and will be reconciled into the accelerator on the next refresh cycle.

### Write-Through Cache Semantics

Accelerated catalog tables use write-through cache semantics:

- **Iceberg is the durable source of truth.** The accelerator is a cache that can be rebuilt from Iceberg at any time.
- **Writes are concurrent, commits are synchronized.** On ingestion, data is written to the accelerator and Iceberg in parallel. The accelerator transaction is held pending until the Iceberg commit succeeds, then committed. Data is not queryable from the accelerator until both writes commit.
- **Success requires both writes.** A write is acknowledged only after both the Iceberg commit and the accelerator commit complete.
- **Failure is safe.** If the Iceberg write fails, the accelerator transaction is rolled back - the acceleration never contains data not in the source. If the accelerator write fails, the data is safely in Iceberg and will be reconciled on the next refresh cycle.
- **No write-behind.** There is no background queue or eventual consistency path for durable writes. Both writes happen in the request path. Every acknowledged write is durable in Iceberg.
- **Cache is reconstructible.** The accelerator must never become an independent source of truth. It can be dropped and rebuilt from Iceberg via refresh.

### Supported Catalog Sources (First Iteration)

| Catalog Source | `from:` Pattern |
|---|---|
| Iceberg | `iceberg:<catalog_id>` |

### Runtime Iceberg REST Catalog API

DDL-created tables with acceleration write-through will appear in the Runtime Iceberg REST Catalog endpoints, like any other dataset or catalog table would. External Iceberg clients will see the DDL-created tables via the REST catalog.

### Out of Scope

- **`on_conflict` / upsert semantics**: Primary key and `on_conflict` configuration for write-through accelerated tables is not supported in this iteration. Duplicate inserts follow the default append behavior of the underlying acceleration engines or federated sources.
- **Primary key enforcement**: No primary key constraints are enforced across the acceleration and federated source in this iteration.

### Security

- Federated source credentials are managed via the Spice secret store and must never be logged.
- Write-through data transfer uses the security mechanisms of the configured federated source. For example, writes to an Iceberg REST catalog use TLS to an S3/Glue endpoint.
- Distributed scheduler-to-executor forwarding continues to use mTLS.
- DDL operations are restricted to users with a read-write API key configured in the spicepod.

## How/Implementation Plan

1. **DDL catalog table detection**: When a `CREATE TABLE ... WITH ("acceleration.*")` targets a catalog-backed schema (e.g. Iceberg), the DDL analyzer rule detects this and enables write-through automatically. No explicit option is needed - acceleration on a DDL-based catalog table enables write-through.

2. **Distributed catalog configuration propagation**: In distributed mode, the scheduler forwards `DoPut` writes to executors. Each executor must have the catalog configuration (connection params, credentials) available to perform the write-through. The scheduler must propagate catalog configuration to executors as part of table registration so that executors can independently write through to the federated source. This expands on the DDL state storage defined in https://github.com/spiceai/spiceai/issues/9949 to define DDL-created tables as federated vs accelerated with configuration parameters.

3. **Concurrent write-through in `AcceleratedTable::insert_into`**: On ingestion, write to the acceleration engine and the federated source concurrently, with the accelerator commit synchronized to the Iceberg commit. The write-through path:
   - Schema-casts the ingested data to match the federated source schema.
   - Begins a transaction on the accelerator and spawns both `accelerator.insert_into` (within the transaction) and `federated_table.insert_into` concurrently (e.g. `tokio::try_join!`).
   - The accelerator write is held in a pending transaction - data is not queryable until the Iceberg commit succeeds. This is similar to the existing partition write synchronization where commits to all DuckDB partitions are coordinated.
   - Once the Iceberg write commits, the accelerator transaction is committed, making data visible.
   - The write is not acknowledged until both commits succeed.
   - If the Iceberg write fails, the accelerator transaction is rolled back. The client receives an error.
   - If the accelerator write fails, the request fails; the data is safely in Iceberg and will be reconciled into the accelerator on the next refresh cycle.
   No watermark or async tracking is needed. A write is acknowledged only after end-to-end success; if any step fails, the request fails and is reconciled via retry and refresh, with Iceberg remaining authoritative.

4. **Retry and error handling**: On transient write-through failures (network timeouts, throttling), retry with `FibonacciBackoffBuilder` before returning an error. Surface persistent failures via `tracing::error!` and `RuntimeStatus`.

5. **Validation**: At DDL execution time, reject acceleration on catalog tables whose federated source does not support writes, with a clear error message.

## QA Plan

### Unit Tests

- Schema-cast alignment between acceleration and federated source schemas.
- Reject acceleration on a catalog table when the federated source is read-only.
- Retry logic: verify exponential backoff on transient failures and error surfacing on persistent failures.

### Integration Tests

1. **Iceberg write-through** - Create a DuckDB-accelerated Iceberg table via DDL. Insert data. Verify data appears in both DuckDB and the Iceberg source.
2. **Write-through failure propagation** - Simulate a federated source write failure. Verify the `INSERT` returns an error to the client and no partial state is left.
3. **Duplicate insert handling** - Insert duplicate rows. Verify behavior is consistent between the accelerator and the Iceberg source.
4. **Distributed write-through** - In a scheduler/executor topology, insert data via the scheduler. Verify each executor writes through to the Iceberg source and the combined result is correct.

### Data Correctness Validation

- Row-level data matches between acceleration engine and Iceberg source after ingestion.
- No data loss or duplication after successful writes.
- Consistent state between acceleration engine and Iceberg source after any failure scenario.

## Release Notes

**Acceleration Write-Through for Iceberg Catalog Tables**: Accelerated tables created via DDL over Iceberg catalog sources now automatically write ingested data through to the underlying Iceberg source. Data inserted into the acceleration (via `INSERT INTO` or Arrow Flight `DoPut`) is synchronously written to both the acceleration engine and the Iceberg source before the write is acknowledged, making the acceleration a write-through cache.

Example:

```sql
-- Create an accelerated table over an Iceberg catalog
CREATE TABLE iceberg_catalog.my_schema.events (
    id BIGINT,
    region TEXT,
    payload TEXT,
    created_at TIMESTAMP
) WITH (
    "acceleration.engine" = 'cayenne',
    "acceleration.mode" = 'file',
    "dataset.time_column" = 'created_at'
);

-- Insert data - written to Cayenne acceleration and Iceberg source
INSERT INTO iceberg_catalog.my_schema.events VALUES (1, 'us-east-1', 'hello', NOW());
```
