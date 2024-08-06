# Spice.ai OSS Data Accelerators - Stages of Release

## Description

Current supported data accelerators (DuckDB, Postgres, SQlite, Arrow) release stages align with the runtime release stages. Introducing a new accelerator will follow a certification process based on these milestone criteria.

## Beta milestone criteria

### Feature set complete

- [ ] Streaming when accelerating/loading
- [ ] Index
- [ ] On conflict behaviors - Drop/Upsert
- [ ] File mode for SQLite and DuckDB
- [ ] Common/core arrow data type supports: Null, all Int types, all Float types, Time32, Time64, Timestamp (with/without TZ), Date32, Date64, Duration, Interval, Binary/LargeBinary/FixedSizeBinary, Utf8/LargeUtf8, List/FixedSizeList/LargeList, Struct, Decimal128/Decimal256
- [ ] No known major bugs/issues

### Test Coverage

- [ ] End-to-end test to cover accelerating TPCH-SF1 data from S3 and benchmarking TPCH queries (official and simple).
  - file mode if applicable
  - memory mode if applicable
- [ ] Integration tests to cover accelerating data from S3 parquet, MySQL, Postgres with arrow types: Null, all Int types, all Float types, Time32, Time64, Timestamp (with/without TZ), Date32, Date64, Duration, Interval, Binary/LargeBinary/FixedSizeBinary, Utf8/LargeUtf8, List/FixedSizeList/LargeList, Struct, Decimal128/Decimal256
- [ ] Integration tests to cover "On Conflict" behaviors.
- [ ] A reproducible test script that can load a variety of datasets into accelerators in both file/memory mode.
  - Can load below datasets
    - [ ] TPCH-SF10
    - [ ] TPCH-SF100
    - [ ] TPCDS-SF10
    - [ ] TPCDS-SF100
    - [ ] db-benchmark join-datagen 50G (from datafusion benchmark)
    - [ ] clickbench_1 (from datafusion benchmark)
    - [ ] Semi-structure/Blob based data with row size 1M, 10M, 100M
  - Can run smoke test on accelerating
    - [ ] Load SF10 into memory
    - [ ] Load SF100 into file
  - Can run smoke test on querying on accelerated data
    - [ ] Query on SF10 dataset with memory mode
    - [ ] Query on SF100 dataset with file mode

### Docs

- [ ] Doc/Specification include all required information from a user to set up the acceleration.
- [ ] Include all known issues/limitations
- [ ] Easy to follow quickstart

### Data loading robustness

- [ ] TPCH SF10 loaded into memory, returned results are identical across source/federated/accelerated queries for all 21 TPCH queries and TPC simple queries.
- [ ] TPCH SF100 loaded into file, returned results are identical across source/federated/accelerated queries for all 21 TPCH queries and TPC simple queries.

### Functional queries

- [ ] TPCH Queries can run on SF10 data in memory mode
- [ ] TPCH Queries can run on SF100 data in file mode

## Stable

WIP
