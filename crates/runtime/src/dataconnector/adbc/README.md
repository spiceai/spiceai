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
      # Optional authentication
      adbc_username: ${secrets:adbc_username}
      adbc_password: ${secrets:adbc_password}
      # Optional driver-specific database options (semicolon-delimited key=value pairs)
      adbc_driver_options: snowflake.sql.db=MY_DB; snowflake.sql.schema=MY_SCHEMA
      # Optional connection defaults
      adbc_catalog: my_catalog
      adbc_schema: my_schema
      # Optional connection pool settings
      connection_pool_size: 5
      connection_pool_min_idle: 1
```

### Snowflake Example

```yaml
datasets:
  - from: adbc:MY_TABLE
    name: my_table
    params:
      adbc_driver: snowflake
      adbc_uri: "user@account/database/schema"
      adbc_password: ${secrets:snowflake_password}
      adbc_driver_options: >-
        snowflake.sql.warehouse=MY_WH;
        snowflake.sql.role=MY_ROLE;
        snowflake.sql.auth_type=auth_snowflake
```

See the [Snowflake ADBC driver docs](https://arrow.apache.org/adbc/current/driver/snowflake.html#client-options) for all available client options.

### Parameters

- `adbc_driver` (required): The ADBC driver name (e.g., 'duckdb', 'sqlite', 'postgres', 'snowflake')
- `adbc_driver_path` (optional): Path to the ADBC driver library
- `adbc_uri` (required): Database URI/connection string. Note: in-memory URIs (e.g., `:memory:`) are not supported.
- `adbc_username` (optional, secret): Username for database authentication
- `adbc_password` (optional, secret): Password for database authentication
- `adbc_driver_options` (optional): Semicolon-delimited driver-specific database options (e.g., `key1=value1; key2=value2`). Keys are automatically prefixed with `adbc.` if not already present (e.g., `snowflake.sql.db` becomes `adbc.snowflake.sql.db`). See driver-specific documentation for available options.
- `adbc_catalog` (optional): The catalog for the connection
- `adbc_schema` (optional): The schema for the connection
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
- [ADBC Driver Status & Options](https://arrow.apache.org/adbc/current/driver/status.html)
- [Snowflake ADBC Driver](https://arrow.apache.org/adbc/current/driver/snowflake.html)
- [datafusion-table-providers ADBC PR](https://github.com/datafusion-contrib/datafusion-table-providers/pull/481)
- [Spice Architecture](https://docs.spice.ai/architecture)
