# ADBC Data Connector

This directory contains the implementation of the ADBC (Arrow Database Connectivity) data connector for Spice.

## Implementation

The ADBC connector is implemented in `../adbc.rs` with the following features:

- Full DataConnector trait implementation
- Support for read-only and read-write table providers
- Connection pooling via `ADBCPool`
- Dynamic driver loading (DuckDB, SQLite, Postgres, etc.)
- Configurable connection parameters

## Configuration

The connector can be used with the following spicepod configuration:

```yaml
datasets:
  - from: adbc:table_name
    name: my_table
    params:
      adbc_driver: duckdb # or sqlite, postgres, etc.
      adbc_uri: path/to/database.db
      # Optional connection pool settings
      connection_pool_size: 5
      connection_pool_min_idle: 1
```

### Parameters

- `adbc_driver` (required): The ADBC driver name (e.g., 'duckdb', 'sqlite', 'postgres')
- `adbc_driver_path` (optional): Path to the ADBC driver library
- `adbc_uri` (required): Database URI/connection string. Note: in-memory URIs (e.g., `:memory:`) are not supported.
- `connection_pool_size` (optional, default: 5): Maximum connections in the pool.
- `connection_pool_min_idle` (optional, default: 1): Minimum idle connections

## Architecture

The ADBC connector follows the standard Spice connector pattern:

```
AdbcFactory (DataConnectorFactory)
    ↓ creates
Adbc (DataConnector)
    ↓ uses
AdbcTableFactory (from datafusion-table-providers)
    ↓ provides
TableProvider (read or read/write)
```

The connector leverages the ADBC table providers from `datafusion-table-providers` which provide:

- Connection pooling via `r2d2` and `ADBCPool`
- Query pushdown and optimization
- Support for multiple ADBC-compatible database drivers
- Arrow-native data transfer

## References

- [ADBC Specification](https://arrow.apache.org/adbc/)
- [datafusion-table-providers ADBC PR](https://github.com/datafusion-contrib/datafusion-table-providers/pull/481)
- [Spice Architecture](https://docs.spice.ai/architecture)
