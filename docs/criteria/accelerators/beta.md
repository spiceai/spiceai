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

- Renders the accelerator completely inoperable (i.e. all queries on the accelerator fail, loading fails, etc), or;
- Causes data inconsistency errors, or;
- A bug that occurs in more than one accelerator, or;
- A bug that is high impact or likely to be experienced in common use cases, and there is no viable workaround.

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

Beta quality accelerators should be able to run test packages derived from the following:

- [TPC-H](https://www.tpc.org/tpch/)
- [TPC-DS](https://www.tpc.org/tpcds/)
- [ClickBench](https://github.com/ClickHouse/ClickBench)

Indexes are not required for test coverage, but can be introduced if required for tests to pass (e.g. due to performance characteristics, etc).

When referring to accelerator access modes, "all supported modes" identifies every possible way to use that accelerator. For example, for DuckDB this would be file and memory modes. For PostgreSQL, this would only be the direct database access mode.

#### General

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

#### TPCH

- [ ] End-to-end test to cover accelerating TPCH-SF1 data from S3 and benchmarking TPCH queries (official and simple).
  - [ ] All supported modes should run all queries with no [major bugs](#bug-levels).
- [ ] A test script exists that can load TPCH-SF10 and TPCH-SF100 data into this accelerator in all supported modes.
- [ ] The accelerator can load TPCH-SF10 in all supported modes, and can run all queries with no [major bugs](#bug-levels).
- [ ] The accelerator can load TPCH-SF100 in either file or direct database mode, and can run all queries with no [major bugs](#bug-levels).

#### TPCDS

- [ ] End-to-end test to cover accelerating TPCDS-SF1 data from S3 and benchmarking TPCDS queries (official and simple).
  - [ ] All supported modes should run all queries with no [major bugs](#bug-levels).
- [ ] A test script exists that can load TPCDS-SF10 and TPCDS-SF100 data into this accelerator in all supported modes.
- [ ] The accelerator can load TPCDS-SF10 in all supported modes, and can run all queries with no [major bugs](#bug-levels).
- [ ] The accelerator can load TPCDS-SF100 in either file or direct database mode, and can run all queries with no [major bugs](#bug-levels).

#### ClickBench

- [ ] A test script exists that can load ClickBench data into this accelerator in either file or direct database mode.
- [ ] The accelerator can load ClickBench in either file or direct database mode, and all queries are attempted.
  - [ ] All query failures should be logged as issues. No bug fixes are required for ClickBench

#### Data correctness

- [ ] TPCH-SF10 loaded into memory, returned results are identical across source/federated/accelerated queries for all TPCH queries and TPCH simple queries.
- [ ] TPCH-SF100 loaded into file, returned results are identical across source/federated/accelerated queries for all TPCH queries and TPCH simple queries.

- [ ] TPCDS-SF10 loaded into memory, returned results are identical across source/federated/accelerated queries for all TPCDS queries and TPCDS simple queries.
- [ ] TPCDS-F100 loaded into file, returned results are identical across source/federated/accelerated queries for all TPCDS queries and TPCDS simple queries.

### Docs

- [ ] Documentation includes all information and steps for a user to set up the accelerator.
- [ ] Documentation includes all known issues/limitations for the accelerator.
- [ ] Documentation includes any exceptions made to allow this accelerator to reach Beta quality (e.g. if a particular data type cannot be supported by the accelerator).
- [ ] The accelerator has an easy to follow quickstart.
- [ ] All [minor](#bug-levels) TPCDS and TPCH bugs are raised as issues.
- [ ] All ClickBench bugs are raised as issues.
