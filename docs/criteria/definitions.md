# Criteria Definitions

## RC

Acronym for "Release Candidate". Identifies a version that is eligible for general/stable release.

## Major Bug

A major bug is classified as a bug that:

- Renders the component completely inoperable (i.e. all queries on an accelerator fail, accelerator loading fails, all connector queries fail, etc), or;
- Causes data inconsistency errors, or;
- A bug that occurs in more than one instance of the component (i.e. more than one accelerator, more than one connector), or;
- A bug that is high impact or likely to be experienced in common use cases, and there is no viable workaround.

## Minor Bug

A minor bug is any bug that cannot be classified as a major bug.

## Core Arrow Data Types

Core Arrow Data Types consist of the following data types:

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

## Core Connector Data Types

Core Connector Data Types depend on the specific connector, but in general can be abstracted as (non-exhaustive) types like:

- String: VARCHAR, CHAR, TEXT
- Number: INTEGER, BIGINT, TINYINT, DECIMAL, FLOAT, DOUBLE
- Date: DATETIME, TIMESTAMP, TIME, DATE
- Binary: BLOB, BINARY, CLOB
- Structures: SET, ENUM

## Access Mode

Defines the supported access modes for a particular accelerator:

- Arrow: in-memory only.
- DuckDB: in-memory or file based.
- SQLite: in-memory or file based.
- PostgreSQL: database only.

## Throughput Test

A throughput test is derived from the throughput test requirements of the TPC-H benchmark definitions. In Spice, a throughput test refers to executing multiple parallel executions of the same query.

Refer to the specific benchmark test definition for the required number of parallel queries.

A timing measurement is calculated for each parallel query completion, measured as `Ts`. The measurement is calculated in seconds, begins when the query is sent to Spice, and ends when the last data row is retrieved from Spice. `Ts` is rounded up to the next 0.01 second.

## Throughput Metric

A throughput metric is calculated from the cumulative sum of `Ts` from every parallel query execution. This cumulative timing measurement is measured as `Cs`.

A metric of throughput, measured as Queries Per Hour * Scale Factor, is calculated as: `(Parallel Query Count * Test Suite Query Count * 3600) / Cs * Scale`.

## TPC-H Throughput Test

### Test Suite Query Count

There is a baseline number of 22 queries in the TPC-H test. Some connectors may run less queries. For these queries, the Test Suite Query Count is reduced by the number of skipped queries.

### Parallel Queries

The following table defines how many parallel queries are required at a given scale factor for Spice throughput tests:

| Scale Factor | Parallel Queries |
| ------------ | ---------------- |
| 1            | 8                |
| 10           | 16               |
| 100          | 32               |
| 1000         | 64               |

## TPC-DS Throughput Test

### Test Suite Query Count

There is a baseline number of 99 queries in the TPC-DS test. Some connectors may run less queries. For these queries, the Test Suite Query Count is reduced by the number of skipped queries.

### Parallel Queries

The following table defines how many parallel queries are required at a given scale factor for Spice throughput tests:

| Scale Factor | Parallel Queries |
| ------------ | ---------------- |
| 1            | 4                |
| 10           | 8                |
| 100          | 16               |
| 1000         | 32               |

## Load Test

A load test refers to an extended duration throughput test. For a given throughput test, a load test is where the throughput test is repeated for a set number of hours.

The system is provided with no delays or pauses between throughput test repetitions, resulting in a sustained high-load test.

### Stop-light percentile measurements

The load test uses a stop-light system to determine severity of timing measurements compared against the baseline:

- 🟢: Green - an increase of the 99th percentile of less than 10% compared to the baseline
- 🟡: Yellow - an increase of the 99th percentile between 10-20% compared to the baseline
- 🔴: Red - an increase of the 99th percentile of more then 20% compared to the baseline
