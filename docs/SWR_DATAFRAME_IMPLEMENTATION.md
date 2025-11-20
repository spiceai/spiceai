# SWR DataFrame API Implementation Summary

## Overview

The SWR (Stale-While-Revalidate) acceleration mode has been updated to use DataFusion's DataFrame API instead of manual ExecutionPlan iteration. This provides cleaner, more maintainable code that leverages DataFusion's query optimization.

## Key Changes

### 1. DataFrame API Integration

**Before**: Manual iteration through ExecutionPlan and RecordBatchStream
**After**: Using `SessionContext`, `ctx.table()`, `df.collect()`, and SQL queries

```rust
// Cache miss detection
let ctx = SessionContext::new();
ctx.register_table("__swr_check", accelerator)?;
let df = ctx.sql("SELECT COUNT(*) FROM __swr_check LIMIT 1").await?;
let has_data = df.collect().await?;

// Query source with filters
ctx.register_table("__swr_source", federated)?;
let mut df = ctx.table("__swr_source").await?;
for filter in filters {
    df = df.filter(filter.clone())?;
}
df = df.limit(0, Some(limit))?;
let batches = df.collect().await?;
```

### 2. Filter/Projection/Limit Support

`SwrScanExec` now captures and uses the query parameters:

```rust
pub struct SwrScanExec {
    // ... existing fields ...
    filters: Vec<Expr>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
}
```

This enables:

- **Cache miss queries** to apply the same filters/limits as the user's query
- **Background refresh** to potentially optimize which data to fetch
- **Query-specific caching** in future enhancements

### 3. Cache Miss Handling

New method `handle_cache_miss()`:

```rust
async fn handle_cache_miss(
    federated: Arc<dyn TableProvider>,
    accelerator: Arc<dyn TableProvider>,
    dataset_name: &str,
    filters: &[Expr],
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
) -> DataFusionResult<Vec<RecordBatch>>
```

Flow:

1. Detects when accelerator is empty using COUNT query
2. Fetches from source with user's filters/projection/limit
3. Adds `__spice_swr_refreshed_at` timestamp metadata
4. Prepares data for insertion into accelerator (TODO)
5. Returns fetched batches directly to user

### 4. Background Refresh Updates

The `refresh_data()` method now:

```rust
async fn refresh_data(
    federated: Arc<dyn TableProvider>,
    accelerator: Arc<dyn TableProvider>,
    dataset_name: &str,
) -> DataFusionResult<()> {
    let ctx = SessionContext::new();
    ctx.register_table("__swr_source", federated)?;

    let df = ctx.table("__swr_source").await?;
    let batches = df.collect().await?;

    // Add refreshed_at timestamps
    let timestamped_batches = add_refreshed_at_column(batches)?;

    // TODO: INSERT into accelerator
    Ok(())
}
```

## Benefits of DataFrame API

1. **Cleaner Code**: Higher-level abstractions vs. low-level execution plans
2. **Query Optimization**: DataFusion automatically optimizes DataFrame operations
3. **Type Safety**: Compile-time checking of operations
4. **Composability**: Easy to chain filters, projections, aggregations
5. **Maintainability**: More readable and easier to debug
6. **Flexibility**: Simple to add new query patterns

## Remaining Work

### 1. Accelerator INSERT Capability

Currently, the background refresh fetches data but doesn't insert it into the accelerator. Need to:

- Add an INSERT/UPSERT trait method to accelerator providers
- Or use DataFusion's INSERT INTO with registered table
- Handle schema with `__spice_swr_refreshed_at` column
- Implement for DuckDB, SQLite, PostgreSQL, Arrow accelerators

### 2. Accelerator Schema Management

Accelerators need to:

- Store the `__spice_swr_refreshed_at` column (TimestampSecond)
- Create appropriate indexes on timestamp for staleness queries
- Handle schema evolution when adding metadata column

### 3. Concurrent Refresh Management

Need to prevent multiple simultaneous refreshes:

- Use `tokio::sync::Mutex` or `DashMap` to track in-flight refreshes
- Debounce refresh triggers within a time window
- Cancel stale refresh tasks if newer one starts

## Testing Plan

1. **Unit Tests**:
   - Test cache miss detection
   - Test staleness checking
   - Test DataFrame query construction
   - Test timestamp addition/removal

2. **Integration Tests**:
   - Create dataset with `refresh_mode: swr`
   - Query empty accelerator (cache miss)
   - Wait for data to become stale
   - Verify background refresh triggers
   - Confirm stale data served during refresh

3. **Error Cases**:
   - Source unavailable during cache miss
   - Source unavailable during background refresh
   - Invalid TTL configuration
   - TTL set without SWR mode

## Performance Considerations

1. **Cache Miss Latency**: First query blocks on source fetch
   - Could add timeout with fallback to error
   - Could pre-warm cache on startup for critical queries

2. **Background Refresh Overhead**: Each stale query spawns refresh task
   - Need debouncing to prevent refresh storms
   - Could use refresh_in_progress flag

3. **Metadata Column Overhead**: Extra column in every batch
   - Minimal (single i64 per row)
   - Removed before user sees data

## Files Modified

- `crates/runtime/src/accelerated_table/swr.rs`:
  - Added DataFrame API imports
  - Implemented `accelerator_has_data()` using COUNT query
  - Implemented `handle_cache_miss()` with filter/limit support
  - Updated `refresh_data()` to use DataFrame API
  - Added `filters`, `projection`, `limit` fields to `SwrScanExec`

- `crates/runtime/src/accelerated_table/mod.rs`:
  - Updated `SwrScanExec::new()` call to pass filters/projection/limit

## Next Steps

1. Implement accelerator INSERT capability
2. Add concurrent refresh protection
3. Write integration tests
4. Add metrics/observability
5. Document in main README
6. Consider query-specific caching (per filter set)
