# Runtime Performance Tests

This document describes the performance tests available in the runtime crate.

## Data Accelerator Performance Test

A shared performance test that compares the performance of data accelerators (SQLite, Turso, DuckDB, and Arrow) using the TableProvider interface with the DataFrame API.

> **Note**: This is implemented as an ignored test rather than a traditional cargo benchmark because the project uses stable Rust (not nightly). This approach provides detailed performance metrics without requiring external dependencies.

### Test Schema

The performance test uses a comprehensive schema that covers most major Arrow data types:

- **Integers**: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64
- **Floats**: Float32, Float64
- **Boolean**: Boolean
- **Strings**: Utf8
- **Binary**: Binary
- **Temporal**: Date32, Date64, Time32, Time64, Timestamp, Duration, Interval
- **Complex**: List (of Int32)
- **Decimal**: Decimal128

This ensures the accelerators are tested with real-world data type coverage.

### What it Tests

The benchmark measures round-trip performance for:

- **Inserts**: Writing batches of records to the database (with all data types)
- **Queries**: Reading all data back from the database (with type conversion)

### Running the Performance Test

The test is marked with `#[ignore]` to prevent it from running during normal test runs. To execute it:

#### Run for all accelerators (SQLite, Turso, DuckDB, Arrow)

```bash
cargo test --package runtime --lib --features sqlite,turso,duckdb -- --ignored --nocapture benchmark_roundtrip
```

#### Run for specific accelerators

```bash
# SQLite only
cargo test --package runtime --lib --features sqlite -- --ignored --nocapture benchmark_roundtrip

# Turso only
cargo test --package runtime --lib --features turso -- --ignored --nocapture benchmark_roundtrip

# DuckDB only
cargo test --package runtime --lib --features duckdb -- --ignored --nocapture benchmark_roundtrip

# Arrow only (no feature flag needed, always available)
cargo test --package runtime --lib -- --ignored --nocapture benchmark_roundtrip

# SQLite + DuckDB + Arrow
cargo test --package runtime --lib --features sqlite,duckdb -- --ignored --nocapture benchmark_roundtrip
```

### Test Parameters

The performance test runs each database engine in both **memory** and **file** modes with different dataset sizes:

| Engine | Mode   | Records/Iteration | Iterations | Total Records |
| ------ | ------ | ----------------- | ---------- | ------------- |
| Turso  | Memory | 1,000             | 10         | 10,000        |
| Turso  | File   | 100,000           | 10         | 1,000,000     |
| SQLite | Memory | 100,000           | 10         | 1,000,000     |
| SQLite | File   | 1,000,000         | 10         | 10,000,000    |
| DuckDB | Memory | 100,000           | 10         | 1,000,000     |
| DuckDB | File   | 1,000,000         | 10         | 10,000,000    |
| Arrow  | Memory | 100,000           | 10         | 1,000,000     |

**Notes**:

- Turso's memory mode has tight page cache limitations
- File mode allows for much larger datasets as data is persisted to disk
- The test accumulates data across iterations to test performance with growing datasets
- These parameters can be adjusted by modifying the match statement in the test code

### Output

The performance test provides detailed statistics including:

- **Insert Performance**: Min, P75, P90, P95, P99, Max times, and P50 (median) records/second
- **Query Performance**: Min, P75, P90, P95, P99, Max times, and P50 (median) records/second
- **Round-trip Time**: Min, P75, P90, P95, P99, Max for combined insert + query

Example output (comparing all engines):

```text
Testing with engine: Sqlite
=== Benchmarking Sqlite ===
Records per iteration: 10000
Number of iterations: 10
  ...
--- Results for Sqlite ---
Insert Performance:
  Min: 12.4ms
  P75: 13.0ms
  P90: 13.2ms
  P95: 13.4ms
  P99: 13.7ms
  Max: 13.9ms
  P50 records/sec: 769,230.77

Query Performance:
  Min: 5.9ms
  P75: 403.4ms
  P90: 518.2ms
  P95: 544.8ms
  P99: 569.3ms
  Max: 573.5ms
  P50 records/sec: 3,846,153.85

Round-trip (Insert + Query):
  Min: 18.3ms
  P75: 416.4ms
  P90: 531.4ms
  P95: 558.2ms
  P99: 583.0ms
  Max: 587.4ms
========================

Testing with engine: DuckDB
=== Benchmarking DuckDB ===
--- Results for DuckDB ---
Insert Performance:
  Min: 11.5ms
  P75: 12.8ms
  P90: 13.0ms
  P95: 13.2ms
  P99: 13.5ms
  Max: 13.7ms
  P50 records/sec: 793,650.79

Query Performance:
  Min: 2.9ms
  P75: 61.7ms
  P90: 76.2ms
  P95: 80.1ms
  P99: 84.3ms
  Max: 85.6ms
  P50 records/sec: 25,000,000.00

Round-trip (Insert + Query):
  Min: 14.4ms
  P75: 74.5ms
  P90: 89.2ms
  P95: 93.3ms
  P99: 97.8ms
  Max: 99.3ms
========================

Testing with engine: Arrow
=== Benchmarking Arrow ===
--- Results for Arrow ---
Insert Performance:
  Min: 77.7µs
  P75: 79.1µs
  P90: 80.5µs
  P95: 82.3µs
  P99: 89.7µs
  Max: 187.3µs
  P50 records/sec: 125,786,163.52

Query Performance:
  Min: 64.3µs
  P75: 186.2µs
  P90: 215.8µs
  P95: 223.4µs
  P99: 231.6µs
  Max: 235.9µs
  P50 records/sec: 7,142,857,142.86

Round-trip (Insert + Query):
  Min: 142.0µs
  P75: 265.3µs
  P90: 296.3µs
  P95: 305.7µs
  P99: 321.3µs
  Max: 423.2µs
========================
```

**Performance Summary** (P95 round-trip latency, fastest to slowest):

1. **Arrow**: 305.7µs (in-memory, no persistence)
2. **DuckDB**: 93.3ms (embedded analytical database)
3. **SQLite**: 558.2ms (embedded transactional database)

Note: Turso results omitted from summary as it requires remote sync configuration.

### Implementation Details

- Uses the `TableProvider` interface to ensure both accelerators are tested through the same API
- Leverages the DataFrame API for data operations
- Tests real data integrity by verifying row counts after each operation
- Accumulates data across iterations to test performance with growing datasets
- Both accelerators are tested in the same test run for direct comparison

### Location

The performance test is located in:

```text
crates/runtime/src/dataaccelerator/mod.rs
```

In the `accelerator_compat_tests` module under the test `benchmark_roundtrip()`.
