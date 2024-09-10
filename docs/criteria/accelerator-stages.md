# Spice.ai OSS Data Accelerators - Stages of Release

## Description

Current supported data accelerators (DuckDB, Postgres, SQlite, Arrow) release stages align with the runtime release stages. Introducing a new accelerator will follow a certification process based on these milestone criteria.

## Beta milestone criteria

### Feature set complete

- [ ] Streaming when accelerating/loading
- [ ] Streaming when querying
- [ ] Supports primary keys and indexes
- [ ] Supports federation in a single dataset
- [ ] Supports push-down federation across multiple datasets within the same accelerator
- [ ] On conflict behaviors - Drop/Upsert
- [ ] File mode for supported accelerator engines, i.e. SQLite, DuckDB, etc.
- [ ] Common/core arrow data type supports: Null, all Int types, all Float types, Time32, Time64, Timestamp (with/without TZ), Date32, Date64, Duration, Interval, Binary/LargeBinary/FixedSizeBinary, Utf8/LargeUtf8, List/FixedSizeList/LargeList, Struct, Decimal128/Decimal256
- [ ] No known major bugs/issues

### Test Coverage

- [ ] End-to-end test to cover accelerating TPCH-SF1 data from S3 and benchmarking TPCH queries (official and simple).
  - [ ] File mode
  - [ ] Embedded memory mode
- [ ] Integration tests to cover accelerating data from S3 parquet, MySQL, Postgres with arrow types: Null, all Int types, all Float types, Time32, Time64, Timestamp (with/without TZ), Date32, Date64, Duration, Interval, Binary/LargeBinary/FixedSizeBinary, Utf8/LargeUtf8, List/FixedSizeList/LargeList, Struct, Decimal128/Decimal256
- [ ] Integration tests to cover "On Conflict" behaviors.
- [ ] A reproducible test script that can load a variety of datasets into accelerators in both file/embedded memory mode.
  - Loads and runs these benchmarks with no errors:
    - [ ] TPCH-SF10 (embedded memory/file)
    - [ ] TPCH-SF100 (file)
    - [ ] TPCDS-SF10 (embedded memory/file)
    - [ ] TPCDS-SF100 (file)
    - [ ] Clickbench

### Docs

- [ ] Doc/Specification include all required information from a user to set up the acceleration.
- [ ] Include all known issues/limitations
- [ ] Easy to follow quickstart

### Data correctness

- [ ] TPCH SF10 loaded into memory, returned results are identical across source/federated/accelerated queries for all 21 TPCH queries and TPC simple queries.
- [ ] TPCH SF100 loaded into file, returned results are identical across source/federated/accelerated queries for all 21 TPCH queries and TPC simple queries.

## Stable

WIP
