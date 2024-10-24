# Spice.ai OSS Data Accelerators - RC Criteria

This document defines the set of criteria that is required before a data accelerator is considered to be of [RC](../definitions.md) quality.

All criteria must be met for the accelerator to be considered [RC](../definitions.md), with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular accelerator).

## RC Quality Accelerators

| Accelerator | RC Quality | DRI Sign-off |
| - | - | - |
| Arrow      | ❌ |  |
| DuckDB     | ❌ |  |
| SQLite     | ❌ |  |
| PostgreSQL | ❌ |  |

## RC Release Criteria

The RC release criteria expand on and require that all [Beta release criteria](./beta.md) continue to pass for the accelerator.

- [ ] All [Beta release criteria](./beta.md) pass for this accelerator.

### Test Coverage

RC quality accelerators should be able to run test packages derived from the following:

- [TPC-H](https://www.tpc.org/TPC-H/)
- [TPC-DS](https://www.tpc.org/TPC-DS/)
- [ClickBench](https://github.com/ClickHouse/ClickBench)

Indexes are not required for test coverage, but can be introduced if required for tests to pass (e.g. due to performance characteristics, etc).

When referring to accelerator access modes, "all supported modes" identifies every possible way to use that accelerator. For example, for DuckDB this would be file and memory modes. For PostgreSQL, this would only be the direct database access mode.

#### TPC-H

- [ ] The accelerator can load TPC-H SF10 in all supported modes, and can run all queries with no [Major or Minor Bugs](../definitions.md).
- [ ] The accelerator can load TPC-H SF100 in either file or direct database mode, and can run all queries with no [Major or Minor Bugs](../definitions.md).

#### TPC-DS

- [ ] The accelerator can load TPC-DS SF10 in all supported modes, and can run all queries with no [Major or Minor Bugs](../definitions.md).
- [ ] The accelerator can load TPC-DS SF100 in either file or direct database mode, and can run all queries with no [Major or Minor Bugs](../definitions.md).

#### ClickBench

- [ ] A test script exists that can load ClickBench data into this accelerator in either file or direct database mode.
- [ ] The accelerator can load ClickBench in either file or direct database mode, and all queries run with no [Major Bugs](../definitions.md).

#### Data correctness

- [ ] TPC-H SF10 loaded into memory, returned results are identical across source and accelerated queries for all TPC-H queries and TPC-H simple queries.
- [ ] ClickBench loaded into file or direct database mode, returned results are identical across source and accelerated queries for all ClickBench queries.

### Documentation

- [ ] Documentation includes all known issues/limitations for the accelerator.
- [ ] Documentation includes any exceptions made to allow this accelerator to reach RC quality (e.g. if a particular data type cannot be supported by the accelerator).
- [ ] The accelerator status is updated in the table of accelerators in [spiceai/docs](https://github.com/spiceai/docs).
