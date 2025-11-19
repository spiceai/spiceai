# SWR (Stale-While-Revalidate) Acceleration Mode - Implementation Status

## Overview

SWR mode is a new acceleration mode that uses a stale-while-revalidate pattern. Unlike `full`, `changes`, and `append` modes which do periodic refreshes, SWR:

- Does NOT perform an initial full refresh
- Fetches data from source on-demand based on queries
- Stores fetched data with a `refreshed_at` metadata column
- Returns stale data if TTL expired, but triggers background refresh
- Returns data from source if no cached data exists

## Completed Implementation

### 1. Configuration & Enums ✅

- Added `Swr` variant to `RefreshMode` enum in both `spicepod` and `runtime` crates
- Added `refresh_swr_ttl` configuration field to `Acceleration` struct
- Updated enum conversions and Display implementations
- Updated tracing to show "swr" mode in logs

### 2. Core Architecture ✅

- Added `AccelerationRefreshMode::Swr` variant
- Modified `AcceleratedTable::Builder` to support `swr_ttl` parameter
- Updated accelerated table scan to detect SWR mode
- SWR mode skips initial refresh and marks table as immediately ready

### 3. Metadata Column Handling ✅

Created `crates/runtime/src/accelerated_table/swr.rs` with:

- `SWR_REFRESHED_AT_COLUMN` constant for metadata column name
- `add_swr_metadata_column()` - adds `__spice_swr_refreshed_at` timestamp column to schema
- `is_data_stale()` - checks if data is stale based on TTL
- `add_refreshed_at_column()` - adds current timestamp to record batches
- `remove_swr_metadata_column()` - strips metadata before returning to user
- `SwrScanExec` - execution plan wrapper that removes SWR metadata from results

### 4. Integration Points ✅

- Updated `datafusion/mod.rs` to pass `swr_ttl` when building accelerated tables
- Modified scan method to wrap results with `SwrScanExec` when in SWR mode
- SWR mode bypasses "not ready" checks since no initial load is required

## Remaining Work

### 5. Query-Driven Fetch Logic ❌

**Not yet implemented:**

- Logic to determine if queried data exists in acceleration (based on filter predicates)
- Synchronous fetch from source when data doesn't exist in cache
- Store fetched data with `refreshed_at` timestamp

**Complexity:** This requires:

- Filter predicate analysis to determine what data is being requested
- Checking if that specific data (with those predicates) exists in acceleration
- Merging query-driven updates with existing data

### 6. Background Refresh Triggering ❌

**Not yet implemented:**

- Detecting when returned data is stale (TTL exceeded)
- Triggering background refresh task to update stale data
- Coordinating refresh to avoid duplicate fetches

**Approach needed:**

- Check `is_data_stale()` on query results
- Spawn background task using `io_runtime` to refresh data
- Use refresh task runner infrastructure (similar to `Full` mode)

### 7. Accelerator Schema Modifications ❌

**Not yet implemented:**

- Modify Arrow accelerator to include `refreshed_at` column
- Modify DuckDB accelerator to include `refreshed_at` column
- Modify SQLite accelerator to include `refreshed_at` column
- Modify PostgreSQL accelerator to include `refreshed_at` column
- Ensure inserts/updates include timestamp

**Files to modify:**

- `crates/runtime-acceleration/src/arrow.rs`
- `crates/runtime-acceleration/src/duckdb.rs`
- `crates/runtime-acceleration/src/sqlite.rs`
- `crates/runtime-acceleration/src/postgres.rs`

### 8. Testing ❌

**Not yet implemented:**

- Integration tests for SWR mode
- Test data fetch on cache miss
- Test stale data return + background refresh
- Test TTL configuration
- Test spicepod with SWR mode

## Architecture Decisions

### Why SWR is Different

Traditional modes (`full`, `append`, `changes`) are **time-driven**:

- Scheduled periodic refreshes
- Full or incremental data synchronization
- Query execution always hits acceleration

SWR mode is **query-driven**:

- No scheduled refreshes
- Data fetched only for specific queries
- Acts as a query result cache with staleness handling

### Schema Handling

- Accelerators must store `__spice_swr_refreshed_at` column internally
- Column is hidden from user queries (stripped by `SwrScanExec`)
- Timestamp is Unix epoch in seconds (TimestampSecondArray)

### Staleness Check

- Performed during query execution, not as background task
- If stale: return data immediately, refresh in background
- If missing: fetch synchronously (blocks query)

## Next Steps

1. **Implement query-driven fetch**: Modify scan logic to fetch from source on cache miss
2. **Add background refresh**: Trigger refresh task when stale data is detected
3. **Update accelerators**: Add `refreshed_at` column to all acceleration engines
4. **Add tests**: Create integration tests covering all SWR scenarios
5. **Documentation**: Update docs to explain SWR mode configuration and behavior

## Files Modified

### Configuration

- `crates/spicepod/src/acceleration/mod.rs`
- `crates/runtime/src/component/dataset/acceleration.rs`

### Core Runtime

- `crates/runtime/src/accelerated_table/mod.rs`
- `crates/runtime/src/accelerated_table/refresh.rs`
- `crates/runtime/src/accelerated_table/swr.rs` (new)
- `crates/runtime/src/datafusion/mod.rs`
- `crates/runtime/src/tracing_util.rs`

## Usage Example (when fully implemented)

```yaml
datasets:
  - from: postgres:public.orders
    name: orders
    acceleration:
      enabled: true
      refresh_mode: swr
      refresh_swr_ttl: 5m # Data older than 5 minutes triggers background refresh
```

This will:

1. Not perform initial data load
2. On first query: fetch from source, cache with timestamp
3. On subsequent queries within 5min: return cached data
4. On queries after 5min: return stale cache, refresh in background
5. On next query: get updated data
