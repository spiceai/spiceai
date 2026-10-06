# ScyllaDB Data Connector

The ScyllaDB data connector enables Spice to query data from [ScyllaDB](https://www.scylladb.com/) and [Apache Cassandra](https://cassandra.apache.org/) clusters using CQL (Cassandra Query Language).

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Type Mappings](#type-mappings)
- [Query Execution Model](#query-execution-model)
- [Filter Pushdown](#filter-pushdown)
- [CQL Dialect](#cql-dialect)
- [Connection Pooling](#connection-pooling)
- [Error Handling](#error-handling)
- [Performance Considerations](#performance-considerations)
- [Examples](#examples)
- [Limitations](#limitations)

## Overview

The ScyllaDB connector provides federated SQL access to ScyllaDB/Cassandra data through DataFusion. Due to the fundamental differences between CQL and SQL, the connector implements a **partition key filter pushdown strategy**: filters on primary key columns are pushed down to CQL for efficient key-based lookups, while other SQL operations (complex filters, joins, aggregations) are performed locally by DataFusion.

### Key Design Decisions

1. **Partition key filter pushdown**: Equality filters on partition keys are pushed to CQL for efficient queries
2. **Clustering key filter pushdown**: Comparison filters on clustering keys are pushed when partition key is present
3. **Complex filter local processing**: Non-key filters are handled locally by DataFusion
4. **Streaming results**: Results are streamed in batches of 8192 rows to minimize memory usage
5. **CQL-compatible dialect**: A custom SQL dialect ensures proper identifier quoting and type casting

## Architecture

The connector is implemented across three crates:

```
┌─────────────────────────────────────────────────────────────────────┐
│                       runtime/src/dataconnector/                     │
│                           scylladb.rs                               │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ ScyllaDbFactory: DataConnectorFactory                          │ │
│  │ - Creates ScyllaDb connector instances                         │ │
│  │ - Handles connection parameters (host, port, keyspace, etc.)   │ │
│  │ - Builds ScyllaDB sessions with authentication                 │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       data_components/src/scylladb/                  │
│                              mod.rs                                  │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ ScyllaDbTableFactory: Read trait                               │ │
│  │ - Creates TableProvider instances for each table               │ │
│  │ - Fetches table schema (partition/clustering keys)             │ │
│  │ - Wraps SqlTable with CqlDialect                               │ │
│  └────────────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ ScyllaDbTable: TableProvider                                   │ │
│  │ - Enables partition key filter pushdown (returns Exact)        │ │
│  │ - Marks clustering key filters as Inexact                      │ │
│  │ - Delegates schema/scan to base SqlTable                       │ │
│  └────────────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ ScyllaDBTableSchema (table_schema.rs)                          │ │
│  │ - Queries system_schema.columns for key information            │ │
│  │ - Determines which filters can be pushed down                  │ │
│  │ - Separates key filters from regular filters                   │ │
│  └────────────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ CqlDialect: Dialect (cql_dialect.rs)                           │ │
│  │ - Double-quote identifier quoting                              │ │
│  │ - CQL-compatible type casting                                  │ │
│  │ - Disables unsupported SQL features                            │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    db_connection_pool/src/                          │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ ScyllaDbConnectionPool (scylladbpool.rs)                       │ │
│  │ - Wraps scylla::Session (internally pooled)                    │ │
│  │ - Implements DbConnectionPool trait                            │ │
│  │ - Manages keyspace and join push-down context                  │ │
│  └────────────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ ScyllaDbConnection (dbconnection/scylladbconn.rs)              │ │
│  │ - Implements AsyncDbConnection trait                           │ │
│  │ - Executes CQL queries and streams Arrow RecordBatches         │ │
│  │ - Handles CQL-to-Arrow type conversions                        │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

## Configuration

### Connection Parameters

| Parameter             | Description                                                        | Required | Default       |
| --------------------- | ------------------------------------------------------------------ | -------- | ------------- |
| `scylladb_host`       | Hostname(s) of ScyllaDB nodes. Comma-separated for multiple nodes. | Yes      | -             |
| `scylladb_hosts`      | Alternative to `scylladb_host`. Comma-separated list of hostnames. | No       | -             |
| `scylladb_port`       | ScyllaDB CQL native transport port.                                | No       | `9042`        |
| `scylladb_keyspace`   | The keyspace to use for queries.                                   | Yes      | -             |
| `scylladb_user`       | Username for authentication.                                       | No       | -             |
| `scylladb_pass`       | Password for authentication (marked as secret).                    | No       | -             |
| `scylladb_datacenter` | Preferred datacenter for connection routing.                       | No       | -             |
| `scylladb_ssl`        | Enable SSL/TLS for connections.                                    | No       | `false`       |
| `connection_timeout`  | Connection timeout in milliseconds.                                | No       | `10000` (10s) |

### Environment Variables

The connector supports environment variable substitution in Spicepod configuration:

```yaml
params:
  scylladb_host: ${ env:SCYLLADB_HOST }
  scylladb_port: ${ env:SCYLLADB_PORT }
  scylladb_keyspace: ${ env:SCYLLADB_KEYSPACE }
```

## Type Mappings

### CQL to Arrow Type Mappings

| CQL Type                        | Arrow Type                     | Notes                                 |
| ------------------------------- | ------------------------------ | ------------------------------------- |
| `boolean`                       | `Boolean`                      |                                       |
| `tinyint`                       | `Int8`                         |                                       |
| `smallint`                      | `Int16`                        |                                       |
| `int`                           | `Int32`                        |                                       |
| `bigint`                        | `Int64`                        |                                       |
| `counter`                       | `Int64`                        | Cassandra counter type                |
| `float`                         | `Float32`                      |                                       |
| `double`                        | `Float64`                      |                                       |
| `decimal`                       | `Decimal128(38, 2)`            | Arbitrary precision → fixed precision |
| `blob`                          | `Binary`                       |                                       |
| `date`                          | `Date32`                       | Days since epoch                      |
| `timestamp`                     | `Timestamp(Millisecond, None)` | Milliseconds since epoch              |
| `time`                          | `Timestamp(Microsecond, None)` | Time of day                           |
| `text`, `varchar`, `ascii`      | `Utf8`                         |                                       |
| `uuid`, `timeuuid`              | `Utf8`                         | String representation                 |
| `inet`                          | `Utf8`                         | IP address as string                  |
| `varint`                        | `Utf8`                         | Arbitrary precision integer as string |
| `duration`                      | `Utf8`                         | ISO 8601 duration string              |
| `list<T>`, `set<T>`, `map<K,V>` | `Utf8`                         | JSON string representation            |
| `tuple<...>`                    | `Utf8`                         | String representation                 |
| `frozen<T>`                     | `Utf8`                         | Same as underlying collection         |
| User-Defined Types              | `Utf8`                         | String representation                 |

### Decimal Handling

CQL `decimal` is an arbitrary-precision type, while Arrow `Decimal128` has a maximum precision of 38 digits. The connector uses:

- **Precision**: 38 (maximum for Decimal128)
- **Scale**: 2 (suitable for monetary/financial data like TPC-H)

For decimals that exceed this precision, values may be truncated or rounded.

### Date/Time Handling

- **CQL `date`**: Stored as days since epoch with a 2^31 offset. Converted to Arrow `Date32` (signed days since 1970-01-01).
- **CQL `timestamp`**: Milliseconds since Unix epoch. Directly mapped to Arrow `Timestamp(Millisecond)`.
- **CQL `time`**: Nanoseconds since midnight. Converted to Arrow `Timestamp(Microsecond)` with nanosecond truncation.

## Query Execution Model

### Filter Pushdown Strategy

The connector implements intelligent filter pushdown based on ScyllaDB's primary key structure:

| Filter Type                              | Pushdown Support | Notes                          |
| ---------------------------------------- | ---------------- | ------------------------------ |
| Partition key `=`                        | ✅ Exact          | Required for efficient queries |
| Partition key `IN`                       | ❌ Unsupported    | Not currently implemented      |
| Clustering key `=`, `<`, `<=`, `>`, `>=` | ✅ Inexact        | Only with partition key        |
| Regular column filters                   | ❌ Unsupported    | Would require ALLOW FILTERING  |
| OR conditions                            | ❌ Unsupported    | CQL doesn't support            |
| CAST, BETWEEN, LIKE                      | ❌ Unsupported    | CQL doesn't support            |

### CQL vs SQL Differences

CQL is fundamentally different from SQL and lacks many constructs:

| Feature                 | SQL | CQL                               |
| ----------------------- | --- | --------------------------------- |
| CASE WHEN               | ✅   | ❌                                 |
| Subqueries              | ✅   | ❌                                 |
| Complex JOINs           | ✅   | ❌ (partition key only in C* 4.0+) |
| CAST expressions        | ✅   | ❌                                 |
| INTERVAL                | ✅   | ❌                                 |
| Window functions        | ✅   | ❌                                 |
| NULLS FIRST/LAST        | ✅   | ❌                                 |
| COUNT(DISTINCT)         | ✅   | ❌                                 |
| Arbitrary WHERE clauses | ✅   | ❌ (must include partition key)    |

Because of these limitations, the connector:

1. **Enables partition key filter pushdown** - Equality on partition key enables efficient key-based queries
2. **Supports clustering key pushdown** - Range filters on clustering keys when partition key is present
3. **Supports projection pushdown** - Only requested columns are transferred
4. **Supports limit pushdown** - CQL `LIMIT` clause is used when specified
5. **Handles complex filters locally** - DataFusion evaluates non-pushable filters after data retrieval

## Filter Pushdown

The connector uses a common key filter extraction module (`key_filter`) shared with the DynamoDB connector to identify filters that can be safely pushed to CQL.

### How It Works

1. **Table Schema Discovery**: On table creation, the connector queries `system_schema.columns` to discover partition and clustering key columns
2. **Filter Analysis**: Each filter expression is analyzed to determine if it matches a key column
3. **Filter Separation**: Filters are separated into pushable (key) and non-pushable (regular) groups
4. **Query Generation**: Pushable filters are included in the CQL WHERE clause

### Example

Given a table with:
- Partition key: `user_id`
- Clustering key: `timestamp`

```sql
-- This SQL query:
SELECT * FROM events 
WHERE user_id = 'user123' 
  AND timestamp > '2024-01-01'
  AND status = 'active';

-- Becomes this CQL query:
SELECT * FROM events 
WHERE "user_id" = 'user123' 
  AND "timestamp" > '2024-01-01';

-- With DataFusion applying locally:
-- WHERE status = 'active'
```

### Streaming Execution

Query results are streamed using the scylla driver's paging mechanism:

```
ScyllaDB Cluster
       │
       ▼ (CQL paged query)
┌──────────────┐
│ scylla::     │
│ query_iter() │
└──────────────┘
       │
       ▼ (Row iterator)
┌──────────────────────┐
│ Batch accumulator    │
│ (8192 rows default)  │
└──────────────────────┘
       │
       ▼ (Arrow RecordBatch)
┌──────────────────────┐
│ DataFusion           │
│ RecordBatchStream    │
└──────────────────────┘
       │
       ▼ (Query results)
   Client
```

## CQL Dialect

The `CqlDialect` configures DataFusion's SQL unparser for CQL compatibility:

```rust
impl Dialect for CqlDialect {
    // CQL uses double quotes for identifiers
    fn identifier_quote_style(&self, _: &str) -> Option<char> { Some('"') }
    
    // CQL doesn't support NULLS FIRST/LAST
    fn supports_nulls_first_in_sort(&self) -> bool { false }
    
    // CQL uses TEXT for strings
    fn utf8_cast_dtype(&self) -> ast::DataType { ast::DataType::Text }
    
    // CQL uses BIGINT for 64-bit integers
    fn int64_cast_dtype(&self) -> ast::DataType { ast::DataType::BigInt(None) }
    
    // CQL doesn't support window functions
    fn window_func_support_window_frame(&self, ..) -> bool { false }
}
```

### Reserved Keywords

The connector recognizes CQL reserved keywords that require quoting:

```
ADD, ALLOW, ALTER, AND, ANY, APPLY, ASC, AUTHORIZE, BATCH, BEGIN, BY,
COLUMNFAMILY, CREATE, DELETE, DESC, DROP, FROM, GRANT, IF, IN, INDEX,
INSERT, INTO, KEYSPACE, LIMIT, MODIFY, NOT, OF, ON, ORDER, PASSWORD,
PRIMARY, RENAME, REVOKE, SCHEMA, SELECT, SET, TABLE, TOKEN, TRUNCATE,
TTL, UPDATE, USE, USING, WHERE, WITH, WRITETIME, ...
```

## Connection Pooling

The ScyllaDB driver (`scylla` crate) internally manages connection pooling:

```rust
pub struct ScyllaDbConnectionPool {
    session: Arc<Session>,      // Shared session (internally pooled)
    keyspace: Arc<str>,         // Target keyspace
    join_push_down: JoinPushDown,
}
```

Key characteristics:

- **Single Session**: One `scylla::Session` is shared across all connections
- **Internal Pooling**: The scylla driver manages connections to cluster nodes
- **Load Balancing**: Configurable via datacenter preference
- **Automatic Reconnection**: Handled by the scylla driver

## Error Handling

The connector uses SNAFU for structured error handling:

### Connection Errors

```rust
Error::UnableToCreateSession { source }     // Session creation failed
Error::InvalidHostOrPortError { host, port } // Cannot reach host
Error::AuthenticationError                   // Invalid credentials
Error::MissingRequiredParameter { name }     // Required param not provided
```

### Query Errors

```rust
Error::QueryError { source }        // CQL query execution failed
Error::ExecuteError { source }      // Statement execution failed
Error::ConversionError { message }  // Arrow conversion failed
Error::DeserializeError { source }  // Row deserialization failed
```

### User-Facing Error Messages

Errors follow the project's error message format:

```
Failed to connect to ScyllaDB: {detailed_reason}
Unable to connect to ScyllaDB on {host}:{port}. Ensure that the host 
and port are correctly configured, and that the host is reachable.
```

## Performance Considerations

### Data Transfer

Partition key equality filters are pushed down to ScyllaDB, so queries that constrain the partition key transfer only the matching partitions instead of the entire table. Other filters are evaluated in Spice, which may still require scanning more data on the connector side. Consider:

1. **Use acceleration**: Enable Spice acceleration to cache data locally
2. **Partition wisely**: Design keyspaces and partition keys to align with common query filters
3. **Limit result sets**: Use LIMIT clauses when possible

### Memory Usage

The connector streams results in batches to minimize memory:

```rust
const BATCH_SIZE: usize = 8192;  // Rows per batch
```

For large tables, memory usage is bounded by:
- Current batch being processed
- DataFusion's working memory for filtering/aggregation

### Recommendations

1. **Enable acceleration** for frequently queried data:
   ```yaml
   acceleration:
     enabled: true
     engine: duckdb
   ```

2. **Use appropriate batch sizes** for your workload

3. **Consider datacenter locality**:
   ```yaml
   params:
     scylladb_datacenter: us-east-1
   ```

4. **Set connection timeouts** appropriately for your network:
   ```yaml
   params:
     connection_timeout: 30000  # 30 seconds
   ```

## Examples

### Basic Federated Query

```yaml
# spicepod.yaml
version: v1
kind: Spicepod
name: scylladb-example

datasets:
  - from: scylladb:users
    name: users
    params:
      scylladb_host: localhost
      scylladb_port: 9042
      scylladb_keyspace: my_app
```

### With Authentication

```yaml
datasets:
  - from: scylladb:orders
    name: orders
    params:
      scylladb_host: scylla-cluster.example.com
      scylladb_keyspace: ecommerce
      scylladb_user: app_user
      scylladb_pass: ${ secrets:SCYLLA_PASSWORD }
```

### Multi-Node Cluster

```yaml
datasets:
  - from: scylladb:events
    name: events
    params:
      scylladb_hosts: node1.scylla.local,node2.scylla.local,node3.scylla.local
      scylladb_keyspace: analytics
      scylladb_datacenter: us-west-2
```

### With Acceleration (Recommended for Large Tables)

```yaml
datasets:
  - from: scylladb:products
    name: products
    params:
      scylladb_host: ${ env:SCYLLADB_HOST }
      scylladb_keyspace: catalog
    acceleration:
      enabled: true
      engine: duckdb
      refresh_interval: 1h
```

### TPC-H Benchmark Configuration

```yaml
# test/spicepods/tpch/sf1/federated/scylladb.yaml
version: v1
kind: Spicepod
name: scylladb-federated

definitions:
  - &scylladb_params
    scylladb_host: ${ env:SCYLLADB_HOST }
    scylladb_port: ${ env:SCYLLADB_PORT }
    scylladb_keyspace: ${ env:SCYLLADB_KEYSPACE }

datasets:
  - from: scylladb:customer
    name: customer
    params: *scylladb_params

  - from: scylladb:lineitem
    name: lineitem
    params: *scylladb_params

  - from: scylladb:orders
    name: orders
    params: *scylladb_params
```

## Limitations

### CQL Limitations (Cannot Be Pushed Down)

- **No JOINs**: All joins are performed locally by DataFusion
- **No aggregations**: COUNT, SUM, AVG, etc. are computed locally
- **No subqueries**: Nested queries are not supported
- **No window functions**: RANK, ROW_NUMBER, etc. not supported
- **Limited WHERE clauses**: Partition key equality filters are pushed down to CQL; other filters are evaluated locally and may require fetching more data
- **No ORDER BY pushdown**: Sorting is done locally

### Connector Limitations

- **SSL not fully implemented**: The `scylladb_ssl` parameter is defined but SSL configuration is not fully wired
- **No write support**: The connector is read-only; INSERT/UPDATE/DELETE not supported
- **Decimal precision**: Fixed at precision=38, scale=2; may not suit all use cases
- **Collection types**: Lists, sets, and maps are converted to string representation
- **Large tables**: Without acceleration, large tables cause significant data transfer

### Data Type Limitations

- **varint**: Arbitrary-precision integers are converted to strings
- **duration**: CQL durations are converted to string representation
- **UDTs**: User-defined types are converted to string representation
- **Nested collections**: Deeply nested collections become complex JSON strings

## Testing

The connector includes comprehensive unit tests for:

- Type mapping (`map_scylladb_type_to_arrow`, `map_cql_type_to_arrow`)
- Arrow conversion (`convert_cqlvalue_rows_to_record_batch`)
- Edge cases (NULL handling, boundary values, Unicode, special floats)
- Connection pool behavior
- CQL dialect configuration

Integration tests require a running ScyllaDB instance:

```bash
# Set environment variables
export SCYLLADB_HOST=localhost
export SCYLLADB_PORT=9042
export SCYLLADB_KEYSPACE=test

# Load TPC-H data
./scripts/load_tpch_scylladb.sh

# Run integration tests
make test-integration
```

## See Also

- [ScyllaDB Documentation](https://docs.scylladb.com/)
- [Apache Cassandra CQL Reference](https://cassandra.apache.org/doc/latest/cql/)
- [Spice Data Connectors](https://spiceai.org/docs/components/data-connectors)
- [Data Acceleration](https://spiceai.org/docs/features/acceleration)

