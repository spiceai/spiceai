# Spice.ai OSS Data Accelerators - Alpha Release Criteria

This document defines the set of criteria that is required before a data Accelerator is considered to be of Alpha quality.

All criteria must be met for the Accelerator to be considered Alpha. As Alpha signifies the lowest release quality, criteria exceptions are not permitted.

## Alpha Quality Accelerators

| Accelerator | Alpha Quality | DRI Sign-off |
| - | - | - |
| Arrow      | ❌ |  |
| DuckDB     | ❌ |  |
| SQLite     | ❌ |  |
| PostgreSQL | ❌ |  |

## Alpha Release Criteria

The Alpha Release Criteria for accelerators is set at a level that ensures the Accelerator operates in common conditions with a low error rate.

Accelerators are intended to be more stable than Connectors, and as such the Alpha criteria is set slightly higher than that of Connectors.

The Alpha Release Criteria is not intended to cover any edge cases or complex functionality, so federation, streaming. However, some testing is required to cover TPC-H derived queries.

### All Accelerators

- [ ] The connector implements the basic functionality of the native accelerator source.
  - Basic functionality is determined at the discretion of the connector DRI.
  - For example, for DuckDB basic functionality is querying tables from a database either in-memory or on-disk.
- [ ] The accelerator executes common use cases with a low error rate.
  - A common use case is determined at the discretion of the connector DRI.
- [ ] Known [Minor and Major](../definitions.md) bugs are logged, but not required to be fixed unless needed to achieve a low error rate or TPC-H data loading.
  - A "low error rate" indicates that more than 90% of the time, the common use case succeeds.

#### Testing

- [ ] The accelerator in all [Access Modes](../definitions.md) successfully loads TPC-H derived data at scale factor 1.
- [ ] The accelerator in all [Access Modes](../definitions.md) successfully executes at least 75% of TPC-H derived queries at scale factor 1.

### Documentation

- [ ] Documentation includes all information and steps for a user to set up the Accelerator
- [ ] Documentation includes all known issues/limitations for the Accelerator.
- [ ] The Accelerator has an easy to follow quickstart.
- [ ] The Accelerator is added to the table of Accelerators in [spiceai/docs](https://github.com/spiceai/docs).
