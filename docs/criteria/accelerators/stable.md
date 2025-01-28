# Spice.ai OSS Data Accelerators - Stable Criteria

This document defines the set of criteria that is required before a data accelerator is considered to be of Stable quality.

All criteria must be met for the accelerator to be considered Stable, with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular accelerator).

## Stable Quality Accelerators

| Accelerator | Stable Quality | DRI Sign-off |
| ----------- | -------------- | ------------ |
| Arrow       | ✅            | @peasee      |
| DuckDB      | ✅            | @peasee      |
| SQLite      | ❌            |              |
| PostgreSQL  | ❌            |              |

## Stable Release Criteria

The Stable release criteria expand on and require that all [RC release criteria](./rc.md) continue to pass for the accelerator.

- [ ] All [RC release criteria](./rc.md) pass for this accelerator.

### Test Coverage

Stable quality accelerators should be able to run test packages derived from the following:

- [TPC-H](https://www.tpc.org/TPC-H/)
- [TPC-DS](https://www.tpc.org/TPC-DS/)
- [ClickBench](https://github.com/ClickHouse/ClickBench)

Indexes are not required for test coverage, but can be introduced if required for tests to pass (e.g. due to performance characteristics, etc).

#### TPC-H

- [ ] An end-to-end test is created simulating an append mode load of the TPC-H dataset at scale factor 10 in all [Access Modes](../definitions.md). The data load should complete to the source within 30 minutes, using 5 minute refresh intervals on the accelerator.
  - An accelerator that takes longer than 1 hour to load the dataset is considered a test fail.
- [ ] An end-to-end test is created using a refresh SQL that excludes results from the accelerator for the TPC-H dataset at scale factor 1 in all [Access Modes](../definitions.md). TPC-H queries should induce a zero results action, with `on_zero_results: use_source` behavior.
  - The test validates that for queries that would return results from the source that are excluded in the accelerator, that they correctly return results through the use of `on_zero_results`.
  - A connector that is currently Release Candidate quality or higher must be used as the acceleration source, to reduce the impact of connector-related issues.
- [ ] An end-to-end [Throughput Test](../definitions.md) is performed on the accelerator using the TPC-H dataset at scale factor 1 in all [Access Modes](../definitions.md).
  - [ ] End-to-end tests should perform [Throughput Tests](../definitions.md) at the required [parallel query count](../definitions.md)
  - [ ] [Throughput Metric](../definitions.md) is calculated and reported as a metric with a parallel query count of 1 to serve as a baseline metric.
  - [ ] [Throughput Metric](../definitions.md) is calculated and reported as a metric at the required [parallel query count](../definitions.md).
  - [ ] Memory usage is collected at the end of the end-to-end test and reported as a metric on the overall connector.

#### TPC-DS

- [ ] An end-to-end test is created simulating an append mode load of the TPC-DS dataset at scale factor 10 in all [Access Modes](../definitions.md). The data load should complete to the source within 30 minutes, using 5 minute refresh intervals on the accelerator.
  - An accelerator that takes longer than 1 hour to load the dataset is considered a test fail.
- [ ] An end-to-end test is created using a refresh SQL that excludes results from the accelerator for the TPC-DS dataset at scale factor 1 in all [Access Modes](../definitions.md). TPC-DS queries should induce a zero results action, with `on_zero_results: use_source` behavior.
  - The test validates that for queries that would return results from the source that are excluded in the accelerator, that they correctly return results through the use of `on_zero_results`.
  - A connector that is currently Release Candidate quality or higher must be used as the acceleration source, to reduce the impact of connector-related issues.
- [ ] An end-to-end [Throughput Test](../definitions.md) is performed on the accelerator using the TPC-DS dataset at scale factor 1 in all [Access Modes](../definitions.md).
  - [ ] End-to-end tests should perform [Throughput Tests](../definitions.md) at the required [parallel query count](../definitions.md)
  - [ ] [Throughput Metric](../definitions.md) is calculated and reported as a metric with a parallel query count of 1 to serve as a baseline metric.
  - [ ] [Throughput Metric](../definitions.md) is calculated and reported as a metric at the required [parallel query count](../definitions.md).
  - [ ] A [Load Test](../definitions.md) runs for a minimum of 8 hours at scale factor 1 as part of the end-to-end test. The 99th percentile of load test query [timing measurements](../definitions.md) must be compared against than the 99th percentile of the baseline throughput test timing measurements.
    - Three or more [Yellow percentile measurements](../definitions.md#stop-light-percentile-measurements) are considered a test failure.
    - One or more [Red percentile measurements](../definitions.md#stop-light-percentile-measurements) are considered a test failure.
    - The service must not become unavailable for the entire duration of the test. A connection failure is considered a test failure.
    - Queries that have a 99th percentile execution time faster than 1000ms are excluded from this check, as they complete so fast that this check is not meaningful.
  - [ ] Memory usage is collected at the end of the end-to-end test and reported as a metric on the overall connector.

#### ClickBench

- [ ] The accelerator can load ClickBench in all [Access Modes](../definitions.md), and all queries run with no [Major or Minor Bugs](../definitions.md).
- [ ] An end-to-end test is created simulating an append mode load of the ClickBench dataset in all [Access Modes](../definitions.md). The data load should complete to the source within 30 minutes, using 5 minute refresh intervals on the accelerator.
  - An accelerator that takes longer than 1 hour to load the dataset is considered a test fail.
- [ ] An end-to-end test is created using a refresh SQL that excludes results from the accelerator for the ClickBench dataset in all [Access Modes](../definitions.md). ClickBench queries should induce a zero results action, with `on_zero_results: use_source` behavior.
  - The test validates that for queries that would return results from the source that are excluded in the accelerator, that they correctly return results through the use of `on_zero_results`.
  - A connector that is currently Release Candidate quality or higher must be used as the acceleration source, to reduce the impact of connector-related issues.

### AI Workloads Support

- [ ] Accelerator supports accelerating datasets with embeddings (including chunking)
- [ ] Accelerator supports vector-similarity search. Search benchmark includes corresponding configuration.

### Documentation

- [ ] Documentation includes all known issues/limitations for the accelerator.
- [ ] Documentation includes any exceptions made to allow this accelerator to reach Stable quality (e.g. if a particular data type cannot be supported by the accelerator).
- [ ] The accelerator status is updated in the table of accelerators in [spiceai/docs](https://github.com/spiceai/docs).
