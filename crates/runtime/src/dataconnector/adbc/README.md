# ADBC Data Connector

This directory contains the implementation of the ADBC (Arrow Database Connectivity) data connector for Spice.

## Status

🚧 **Work in Progress** - The connector is implemented but currently commented out pending an upstream dependency update.

## Implementation

The ADBC connector has been fully implemented in `/Users/lukim/dev/spice2/crates/runtime/src/dataconnector/adbc.rs` with the following features:

- Full DataConnector trait implementation
- Support for read-only and read-write table providers
- Connection pooling via `ADBCPool`
- Dynamic driver loading (DuckDB, SQLite, Postgres, etc.)
- Configurable connection parameters

## Configuration

Once enabled, the connector can be used with the following spicepod configuration:

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
      # Additional driver-specific parameters can be passed
```

### Parameters

- `adbc_driver` (required): The ADBC driver name (e.g., 'duckdb', 'sqlite', 'postgres')
- `adbc_driver_path` (optional): Path to the ADBC driver library
- `adbc_uri` (optional): Database URI/connection string
- `connection_pool_size` (optional, default: 5): Maximum connections in the pool
- `connection_pool_min_idle` (optional, default: 1): Minimum idle connections
- Any other parameters are passed through to the database as options

## Pending Work

The connector is currently disabled with `// TODO` comments because:

1. The `datafusion-table-providers` crate revision currently used by Spice (`9fa10f280175667a54ca18b55aa1320e82737840`) does not include the ADBC feature
2. The ADBC feature was merged in [PR #481](https://github.com/datafusion-contrib/datafusion-table-providers/pull/481) but hasn't been included in the Spice workspace revision yet

### To Enable the Connector

1. Update the `datafusion-table-providers` revision in `/Users/lukim/dev/spice2/Cargo.toml` to a version that includes the ADBC feature (post-PR #481)
2. Uncomment the feature flags in:
   - `/Users/lukim/dev/spice2/crates/data_components/Cargo.toml`
   - `/Users/lukim/dev/spice2/crates/runtime/Cargo.toml`
   - `/Users/lukim/dev/spice2/bin/spiced/Cargo.toml`
3. Uncomment the module declarations in `/Users/lukim/dev/spice2/crates/runtime/src/dataconnector/mod.rs`
4. Uncomment the Makefile check in `/Users/lukim/dev/spice2/Makefile`
5. Run `cargo check --features adbc` to verify compilation

## Files Modified

- ✅ `/Users/lukim/dev/spice2/crates/runtime/src/dataconnector/adbc.rs` - Main connector implementation
- ✅ `/Users/lukim/dev/spice2/crates/runtime/src/dataconnector/mod.rs` - Module registration (commented out)
- ✅ `/Users/lukim/dev/spice2/crates/data_components/Cargo.toml` - Feature flag (commented out)
- ✅ `/Users/lukim/dev/spice2/crates/runtime/Cargo.toml` - Feature flag (commented out)
- ✅ `/Users/lukim/dev/spice2/bin/spiced/Cargo.toml` - Feature flag (commented out)
- ✅ `/Users/lukim/dev/spice2/Makefile` - Build check (commented out)

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
