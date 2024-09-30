# Spice.ai OSS Data Accelerators - Beta Release Criteria

## Description

This doucment defines the set of criteria that is required before a data accelerator is considered to be of Beta quality.

All criteria must be met for the accelerator to be considered Beta, with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular accelerator).

## Beta Quality Accelerators

| Accelerator | Beta Quality | DRI Sign-off |
| - | - | - |
| Arrow | ❌ |  |
| DuckDB | ❌ |  |
| SQLite | ❌ |  |
| PostgreSQL | ❌ |  |

## Beta Release Criteria

### Bug levels

#### Major Bug

A major bug is classified as a bug that:

- Renders the accelerator completely inoperable, or;
- Causes data inconsistency errors on data stored within the accelerator, or;
  - Excludes data inconsistency errors as a result of queries
- A bug that occurs in more than one accelerator, or;
- At the discretion of the DRI, is a bug that is likely to be experienced in common use cases.

#### Minor Bug

A minor bug is any bug that cannot be classified as a major bug.

### Feature complete

- [ ] Data is streamed when accelerating from source into this accelerator
- [ ] Data is streamed when reading/performing queries from this accelerator
- [ ] The accelerator supports primary keys and indexes
- [ ] The accelerator supports full federation within a single dataset (e.g. `select * from my_dataset`)
- [ ] The accelerator supports federation push down across multiple datasets within the same accelerator (e.g. `select * from first_dataset, second_dataset`)
- [ ] The accelerator supports resolving on conflict behaviors (e.g. Drop/Upsert)
- [ ] Embdedded accelerators support file-mode storage (e.g. SQLite, DuckDB)
- [ ] Core Arrow data types are supported:
  - Null
  - Int/Float/Decimal
  - Time32/64
  - Timestamp/TimestampTZ
  - Date32/64
  - Duration
  - Interval
  - Binary/LargeBinary/FixedSizeBinary
  - Utf8/LargeUtf8
  - List/FixedSizeList/LargeList
  - Struct
  - Decimal128/Decimal256
- [ ] All known [major bugs](#bug-levels) are resolved

### Test Coverage

Indexes are not required for test coverage, but can be introduced if required for tests to pass (e.g. due to performance characteristics, etc).

- Clickhouse: Primary key constraint & index: <https://github.com/ClickHouse/ClickBench/#indexing>
- TPCH: Primary/foreign key constraints & index
- TPCDS: Primary/foreign key constraints & index

- [ ] End-to-end test to cover accelerating TPCH-SF1 data from S3 and benchmarking TPCH queries (official and simple).
  - [ ] File mode
  - [ ] In-Memory mode
- [ ] End-to-end test to cover accelerating TPCDS-SF1 data from S3 and benchmarking TPCDS queries (official and simple).
  - [ ] File mode
  - [ ] In-Memory mode
- [ ] Integration tests to cover accelerating data from S3 parquet, MySQL, Postgres with arrow types:
  - [ ] Null
  - [ ] All Int types
  - [ ] All Float types
  - [ ] Time32
  - [ ] Time64
  - [ ] Timestamp (with/without TZ)
  - [ ] Date32
  - [ ] Date64
  - [ ] Duration
  - [ ] Interval
  - [ ] Binary/LargeBinary/FixedSizeBinary
  - [ ] Utf8/LargeUtf8
  - [ ] List/FixedSizeList/LargeList
  - [ ] Struct
  - [ ] Decimal128/Decimal256
- [ ] Integration tests to cover "On Conflict" behaviors.
- [ ] A reproducible test script that can load a variety of datasets into accelerators in both file/embedded memory mode.
  - Loads and runs these benchmarks:
    - [ ] TPCH-SF10 (embedded memory/file)
    - [ ] TPCH-SF100 (file)
    - [ ] TPCDS-SF10 (embedded memory/file)
    - [ ] TPCDS-SF100 (file)
    - [ ] ClickBench (memory/file)

### Docs

- [ ] Doc/Specification include all required information for a user to set up the accelerator.
- [ ] Include all known issues/limitations.
- [ ] Include any exceptions made to allow this accelerator to reach Beta quality (e.g. if a particular data type cannot be supported by the accelerator).
- [ ] Easy to follow quickstart.
- [ ] All remaining [minor](#bug-levels) TPCDS, TPCH and ClickBench bugs are raised as issues.

### Data correctness

- [ ] TPCH SF10 loaded into memory, returned results are identical across source/federated/accelerated queries for all 21 TPCH queries and TPC simple queries.
- [ ] TPCH SF100 loaded into file, returned results are identical across source/federated/accelerated queries for all 21 TPCH queries and TPC simple queries.
