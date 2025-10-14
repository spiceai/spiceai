# DuckDB Accelerator Configuration

The DuckDB accelerator supports configuration parameters to control index scan behavior and checkpoint settings.

## Automatic Configuration

### Checkpoint on Shutdown

The DuckDB accelerator automatically enables `PRAGMA enable_checkpoint_on_shutdown` for all connections. This ensures that any pending changes in the Write-Ahead Log (WAL) are checkpointed when the database is shut down, maintaining data consistency.

## Index Scan Parameters

These parameters control when ART (Adaptive Radix Tree) index scans are used for query execution.

### `duckdb_index_scan_percentage`

The index scan percentage sets a threshold for index scans. An index scan is performed instead of a table scan when the number of matching rows is less than the maximum of `index_scan_max_count` and `index_scan_percentage × total_row_count`.

**Type:** DOUBLE (0.0 to 1.0, representing 0% to 100%)  
**Default:** 0.001 (0.1%)  
**Scope:** Global

**Example:**

```yaml
datasets:
  - from: postgres:my_table
    name: my_table
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      params:
        duckdb_index_scan_percentage: '0.10' # Use index scan if < 10% of rows qualify
```

### `duckdb_index_scan_max_count`

The maximum index scan count sets a threshold for index scans. An index scan is performed instead of a table scan when the number of matching rows is less than the maximum of `index_scan_max_count` and `index_scan_percentage × total_row_count`.

**Type:** UBIGINT (non-negative integer)  
**Default:** 2048  
**Scope:** Global

**Example:**

```yaml
datasets:
  - from: postgres:my_table
    name: my_table
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      params:
        duckdb_index_scan_max_count: '1000' # Use index scan if < 1000 rows qualify
```

## Combined Usage

Both parameters can be used together. DuckDB will use an index scan when the number of qualifying rows is less than the maximum of these two thresholds:

```yaml
datasets:
  - from: postgres:my_table
    name: my_table
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      params:
        duckdb_index_scan_percentage: '0.10' # 10% as decimal
        duckdb_index_scan_max_count: '1000'
```

## References

For more information about DuckDB ART index scans, see:
<https://duckdb.org/docs/stable/guides/performance/indexing#art-index-scans>
