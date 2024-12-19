# Spice.ai OSS Data Accelerators - Beta Release Criteria

This document defines the set of criteria that is required before a data accelerator is considered to be of Beta quality.

All criteria must be met for the accelerator to be considered Beta, with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular accelerator).

## Beta Quality Accelerators

| Accelerator | Beta Quality | DRI Sign-off |
| ----------- | ------------ | ------------ |
| Arrow       | ✅           | @Sevenannn   |
| DuckDB      | ✅           | @peasee      |
| SQLite      | ✅           | @sgrebnov    |
| PostgreSQL  | ✅           | @sgrebnov    |

## Beta Release Criteria

The Beta release criteria expand on and require that all [Alpha release criteria](./alpha.md) continue to pass for the accelerator.

- [ ] All [Alpha release criteria](./alpha.md) pass for this accelerator.

### Feature complete

- [ ] Data is streamed when accelerating from source into this accelerator
- [ ] Data is streamed when reading/performing queries from this accelerator
- [ ] The accelerator supports primary keys and indexes
- [ ] The accelerator supports full federation within a single dataset (e.g. `select * from my_dataset`)
- [ ] The accelerator supports federation push down across multiple datasets within the same accelerator (e.g. `select * from first_dataset, second_dataset`)
- [ ] The accelerator supports resolving on conflict behaviors (e.g. Drop/Upsert)
- [ ] Embdedded accelerators support file-mode storage (e.g. SQLite, DuckDB)
- [ ] [Core Arrow Data Types](../definitions.md) are supported
- [ ] All known [Major Bugs](../definitions.md) are resolved

### Test Coverage

Beta quality accelerators should be able to run test packages derived from the following:

- [TPC-H](https://www.tpc.org/TPC-H/)
- [TPC-DS](https://www.tpc.org/TPC-DS/)
- [ClickBench](https://github.com/ClickHouse/ClickBench)

Indexes are not required for test coverage, but can be introduced if required for tests to pass (e.g. due to performance characteristics, etc).

#### General

- [ ] Integration tests to cover accelerating data from S3 parquet, MySQL, Postgres with the [Core Arrow Data Types](../definitions.md)
- [ ] Integration tests to cover "On Conflict" behaviors.

#### TPC-H

- [ ] End-to-end test to cover accelerating TPC-H SF1 data from S3 and benchmarking TPC-H queries (official and simple).
  - [ ] All [Access Modes](../definitions.md) should run all queries with no [Major Bugs](../definitions.md).
- [ ] A test script exists that can load TPC-H SF10 and TPC-H SF100 data into this accelerator in all [Access Modes](../definitions.md).
- [ ] The accelerator can load TPC-H SF10 in all [Access Modes](../definitions.md), and can run all queries with no [Major Bugs](../definitions.md).
- [ ] The accelerator can load TPC-H SF100 in either [file or database mode](../definitions.md), and can run all queries with no [Major Bugs](../definitions.md).

#### TPC-DS

- [ ] End-to-end test to cover accelerating TPC-DS SF1 data from S3 and benchmarking TPC-DS queries (official and simple).
  - [ ] All [Access Modes](../definitions.md) should run all queries with no [Major Bugs](../definitions.md).
- [ ] A test script exists that can load TPC-DS SF10 and TPC-DS SF100 data into this accelerator in all [Access Modes](../definitions.md).
- [ ] The accelerator can load TPC-DS SF10 in all [Access Modes](../definitions.md), and can run all queries with no [Major Bugs](../definitions.md).
- [ ] The accelerator can load TPC-DS SF100 in either [file or database mode](../definitions.md), and can run all queries with no [Major Bugs](../definitions.md).

#### ClickBench

- [ ] A test script exists that can load ClickBench data into this accelerator in either [file or database mode](../definitions.md).
- [ ] The accelerator can load ClickBench in either [file or database mode](../definitions.md), and all queries are attempted.
  - [ ] All query failures should be logged as issues. No bug fixes are required for ClickBench

#### Data correctness

- [ ] TPC-H SF10 loaded into memory, returned results are identical across source and accelerated queries for all TPC-H queries and TPC-H simple queries.
- [ ] TPC-H SF100 loaded into [file or database mode](../definitions.md), returned results are identical across source and accelerated queries for all TPC-H queries and TPC-H simple queries.
- [ ] TPC-DS SF10 loaded into memory, returned results are identical across source and accelerated queries for all TPC-DS queries and TPC-DS simple queries.
- [ ] TPC-DS SF100 loaded into [file or database mode](../definitions.md), returned results are identical across source and accelerated queries for all TPC-DS queries and TPC-DS simple queries.

### Documentation

- [ ] Documentation includes all information and steps for a user to set up the accelerator.
- [ ] Documentation includes all known issues/limitations for the accelerator.
- [ ] Documentation includes any exceptions made to allow this accelerator to reach Beta quality (e.g. if a particular data type cannot be supported by the accelerator).
- [ ] The accelerator has an easy to follow cookbook recipe.
- [ ] All [Minor Bugs](../definitions.md) for TPC-DS and TPC-H are raised as issues.
- [ ] All ClickBench bugs are raised as issues.
- [ ] The accelerator status is updated in the table of accelerators in [spiceai/docs](https://github.com/spiceai/docs).
