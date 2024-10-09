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

## Structured Connector

A connector that could be considered a relational database. Usually has some concept of rows, and natively supports some variant of SQL.

## Unstructured Connector

A connector that could not be considered a relational database, and usually does not have a concept of rows. Does not natively support SQL execution, and instead is accessed through APIs, file downloads, etc.
