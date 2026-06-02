# Runtime Performance Tests

This document describes the performance tests available in the runtime crate.

## Data Accelerator Performance Test

A shared performance test that compares the performance of data accelerators (SQLite, Turso, DuckDB, Arrow, and Vortex) using the TableProvider interface with the DataFrame API.

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

#### Run for all accelerators (SQLite, Turso, DuckDB, Arrow, Vortex)

```bash
cargo test --package runtime --lib --features sqlite,turso,duckdb,vortex -- --ignored --nocapture benchmark_roundtrip
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

# Vortex only
cargo test --package runtime --lib --features vortex -- --ignored --nocapture benchmark_roundtrip

# SQLite + DuckDB + Arrow + Vortex
cargo test --package runtime --lib --features sqlite,duckdb,vortex -- --ignored --nocapture benchmark_roundtrip
```

### Test Parameters

The performance test runs each database engine in both **memory** and **file** modes with different dataset sizes:

| Engine | Mode   | Records/Iteration | Iterations | Total Records |
| ------ | ------ | ----------------- | ---------- | ------------- |
| Turso  | Memory | 100               | 3          | 300           |
| Turso  | File   | 1,000             | 10         | 10,000        |
| SQLite | Memory | 100,000           | 10         | 1,000,000     |
| SQLite | File   | 1,000,000         | 10         | 10,000,000    |
| DuckDB | Memory | 100,000           | 10         | 1,000,000     |
| DuckDB | File   | 1,000,000         | 10         | 10,000,000    |
| Arrow  | Memory | 100,000           | 10         | 1,000,000     |
| Vortex | File   | 1,000,000         | 10         | 10,000,000    |

**Notes**:

- Turso's memory mode has tight page cache limitations
- Vortex only supports file mode (not memory mode)
- File mode allows for much larger datasets as data is persisted to disk
- The test accumulates data across iterations to test performance with growing datasets
- These parameters can be adjusted by modifying the match statement in the test code

### Output

The performance test provides detailed statistics including:

- **Insert Performance**: Min, P90, P95, P99, P99.9, Max times, and P95 records/second
- **Query Performance**: Min, P90, P95, P99, P99.9, Max times, and P95 records/second
- **Round-trip Time**: Min, P90, P95, P99, P99.9, Max for combined insert + query

The benchmark generates a comprehensive comparison table at the end showing all metrics side-by-side across accelerators.

> **Note**: The numbers shown below are example output from a previous run. Actual performance will vary based on hardware, system load, and other factors. Run the benchmark yourself to get accurate numbers for your system.

Example output format (run the benchmark to get current numbers):

```text
Testing with engine: Sqlite
=== Benchmarking Sqlite ===
Records per iteration: 100000
Number of iterations: 10
  ...
--- Results for Sqlite ---
Insert Performance:
  Min: 12.4ms
  P90: 13.2ms
  P95: 13.4ms
  P99: 13.7ms
  P99.9: 13.8ms
  Max: 13.9ms
  P95 records/sec: 746,268.66

Query Performance:
  Min: 5.9ms
  P90: 518.2ms
  P95: 544.8ms
  P99: 569.3ms
  P99.9: 572.5ms
  Max: 573.5ms
  P95 records/sec: 1,834,862.39

Round-trip (Insert + Query):
  Min: 18.3ms
  P90: 531.4ms
  P95: 558.2ms
  P99: 583.0ms
  P99.9: 586.3ms
  Max: 587.4ms
========================

Testing with engine: DuckDB
=== Benchmarking DuckDB ===
--- Results for DuckDB ---
Insert Performance:
  Min: 11.5ms
  P90: 13.0ms
  P95: 13.2ms
  P99: 13.5ms
  P99.9: 13.6ms
  Max: 13.7ms
  P95 records/sec: 757,575.76

Query Performance:
  Min: 2.9ms
  P90: 76.2ms
  P95: 80.1ms
  P99: 84.3ms
  P99.9: 85.0ms
  Max: 85.6ms
  P95 records/sec: 12,484,394.51

Round-trip (Insert + Query):
  Min: 14.4ms
  P90: 89.2ms
  P95: 93.3ms
  P99: 97.8ms
  P99.9: 98.6ms
  Max: 99.3ms
========================

Testing with engine: Arrow
=== Benchmarking Arrow ===
--- Results for Arrow ---
Insert Performance:
  Min: 77.7µs
  P90: 80.5µs
  P95: 82.3µs
  P99: 89.7µs
  P99.9: 120.2µs
  Max: 187.3µs
  P95 records/sec: 1,215,066,828.68

Query Performance:
  Min: 64.3µs
  P90: 215.8µs
  P95: 223.4µs
  P99: 231.6µs
  P99.9: 233.8µs
  Max: 235.9µs
  P95 records/sec: 4,477,611,940.30

Round-trip (Insert + Query):
  Min: 142.0µs
  P90: 296.3µs
  P95: 305.7µs
  P99: 321.3µs
  P99.9: 354.0µs
  Max: 423.2µs
========================

Testing with engine: Vortex
=== Benchmarking Vortex ===
--- Results for Vortex ---
(Run benchmark to see Vortex results. Vortex is currently in ALPHA stage.)
========================
```

**Performance Summary** (Based on example P95 round-trip latency):

1. **Arrow**: ~300µs (in-memory, no persistence)
2. **DuckDB**: ~93ms (file-based, embedded analytical database)
3. **SQLite**: ~558ms (file-based, embedded transactional database)
4. **Vortex**: Run benchmarks to see results (ALPHA - file-based, columnar format optimized for compression and query performance)

**Important Notes**:

- Performance numbers shown above are examples from a previous run and will vary based on your hardware
- Turso results omitted from example as it requires remote sync configuration
- Vortex is in ALPHA stage and should not be used in production
- **To get accurate, up-to-date benchmark results for your system, run the benchmark test with the appropriate feature flags**

### Implementation Details

- Uses the `TableProvider` interface to ensure all accelerators are tested through the same API
- Leverages the DataFrame API for data operations
- Tests real data integrity by verifying row counts after each operation
- Accumulates data across iterations to test performance with growing datasets
- All accelerators are tested in the same test run for direct comparison
- Generates a comprehensive comparison table showing all metrics side-by-side

**Vortex-specific notes**:

- Vortex is currently in ALPHA stage and should not be used in production
- Only supports file mode (append-only operations)
- Uses a columnar format optimized for compression and query performance
- Supports most Arrow data types with automatic type conversion for timestamps and Float16

### Location

The performance test is located in:

```text
crates/runtime/src/dataaccelerator/mod.rs
```

In the `accelerator_compat_tests` module under the test `benchmark_roundtrip()`.
