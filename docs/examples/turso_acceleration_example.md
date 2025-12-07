# Example: Using Turso as Data Accelerator

This example demonstrates how to configure Turso as a data acceleration engine in Spice.ai.

## Memory Mode

```yaml
version: v1beta1
kind: Spicepod
name: turso_file_default

datasets:
  - from: postgres:transactions
    name: transactions
    acceleration:
      enabled: true
      engine: turso
```

## File Mode with Default Path

```yaml
version: v1beta1
kind: Spicepod
name: turso_file_default

datasets:
  - from: postgres:transactions
    name: transactions
    acceleration:
      enabled: true
      engine: turso
      mode: file
      # Default path: .spice/data/transactions.turso
```

## File Mode with Custom Path

```yaml
version: v1beta1
kind: Spicepod
name: turso_file_custom

datasets:
  - from: s3://bucket/data.parquet
    name: sales_data
    acceleration:
      enabled: true
      engine: turso
      mode: file
      params:
        turso_file: /var/data/spice/sales.turso
```

## With Refresh Configuration

```yaml
version: v1beta1
kind: Spicepod
name: turso_with_refresh

datasets:
  - from: mysql:orders
    name: orders
    acceleration:
      enabled: true
      engine: turso
      mode: file
      refresh_mode: full
      refresh_check_interval: 10m
      params:
        turso_file: ./orders.turso
```

## Building Spice with Turso Support

```bash
# Build spiced with Turso support
cargo build --release --features turso

# Or for development
cargo build --features turso
```

## Comparison with SQLite

Turso is built on SQLite and offers:

- SQLite-compatible API
- Native async support using the libSQL library
- Better performance for modern workloads
- File-based persistence (memory mode not supported)
- Potential for cloud integration (future)

Key differences from SQLite:

- **Turso requires file mode** - memory mode is not available
- Uses libSQL client library instead of rusqlite
- Optimized for async operations

Configuration example:

```yaml
# SQLite configuration (supports both modes)
acceleration:
  engine: sqlite
  mode: file  # or memory
  params:
    sqlite_file: ./data.sqlite

# Turso configuration (file mode only)
acceleration:
  engine: turso
  mode: file  # required - memory mode not supported
  params:
    turso_file: ./data.turso
```

## Remote Turso Databases - Not Supported

**Remote Turso databases are not supported when using Turso as a file accelerator.** The `turso_url` and `turso_auth_token` parameters will be rejected with an error.

Remote Turso database support will be available in the future when Turso is implemented as a **data connector** (not an accelerator).

If you attempt to use remote parameters, you will receive an error:

```text
Remote Turso databases are not supported when using Turso as a file accelerator. Remote database support (turso_url, turso_auth_token) will be available when Turso is used as a data connector.
```

**Example of what will NOT work:**

```yaml
datasets:
  - name: remote_data
    from: ...
    acceleration:
      enabled: true
      engine: turso
      params:
        turso_url: libsql://[database].turso.io # ❌ Will be rejected
        turso_auth_token: ${secrets:turso_token} # ❌ Will be rejected
```

## Notes

- **File Mode Only**: Turso only supports file mode acceleration. Memory mode is not available.
- **Local Files Only**: Remote Turso databases (turso_url, turso_auth_token) are not supported as accelerators. This will be available when Turso is added as a data connector.
- **Current Status**: Alpha - Custom TableProvider implementation using libSQL
- **Testing**: Basic operations should work, comprehensive testing needed
- **Performance**: Benchmarking not yet done
- **Production**: Not recommended for production use until fully tested

## Next Steps for Implementation

1. Add comprehensive tests
2. Performance benchmarking vs SQLite
3. Implement write operations (INSERT, UPDATE, DELETE)
4. Add federation support (database attachment)
5. Implement Turso as a data connector (for remote database support)
