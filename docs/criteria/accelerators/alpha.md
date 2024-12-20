# Spice.ai OSS Data Accelerators - Alpha Release Criteria

This document defines the set of criteria that is required before a Data Accelerator is considered to be of Alpha quality.

All criteria must be met for the Accelerator to be considered Alpha. As Alpha signifies the lowest release quality, criteria exceptions are not permitted.

## Alpha Quality Accelerators

| Accelerator | Alpha Quality | DRI Sign-off |
| ----------- | ------------- | ------------ |
| Arrow       | ✅            | @Sevenannn   |
| DuckDB      | ✅            | @peasee      |
| SQLite      | ✅            | @sgrebnov    |
| PostgreSQL  | ✅            | @sgrebnov    |

## Alpha Release Criteria

The Alpha Release Criteria for accelerators is set at a level that ensures the Accelerator operates in common conditions with a low error rate.

Accelerators are intended to be more stable than Connectors, and as such the Alpha criteria is set higher than that of Connectors.

The Alpha Release Criteria is not intended to cover edge cases or advanced functionality, such as federation/streaming. However, testing is required to cover TPC-H derived queries.

### All Accelerators

- [ ] The accelerator implements the querying functionality of the native accelerator source in all [Access Modes](../definitions.md)
- [ ] The accelerator supports only String, Number, Date and Binary [Core Connector Data Types](../definitions.md).
- [ ] The accelerator executes common use case queries (e.g. `SELECT * FROM tbl WHERE col = 'blah'`).
  - Advanced queries (e.g. nested subqueries, derived tables, window frame functions and rank) should not make up common use cases, even if included in TPC-H.
- [ ] Known [Minor and Major](../definitions.md) bugs are logged, but not required to be fixed unless needed to achieve the common use cases and/or TPC-H success.

#### Testing

- [ ] End-to-end test to cover accelerating only String, Number, Date and Binary [Core Connector Data Types](../definitions.md).
- [ ] End-to-end test to cover accelerating TPC-H scale factor 1 data from S3 and benchmarking TPC-H queries (official and simple).
- [ ] The accelerator in all [Access Modes](../definitions.md) successfully loads TPC-H derived data at scale factor 1.
- [ ] The accelerator in all [Access Modes](../definitions.md) successfully executes at least 75% of TPC-H derived queries at scale factor 1.

### Documentation

- [ ] Documentation includes all information and steps for a user to set up the Accelerator.
- [ ] Documentation includes all known issues/limitations for the Accelerator.
- [ ] The Accelerator has an easy to follow cookbook recipe.
- [ ] The Accelerator is added to the table of Accelerators in [spiceai/docs](https://github.com/spiceai/docs).
