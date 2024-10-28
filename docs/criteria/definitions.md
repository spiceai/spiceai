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
