# Session Context

## User Prompts

### Prompt 1

At `crates/runtime/src/dataaccelerator/partitioned_arrow.rs:L128`

The _source parameter in create_external_table is intentionally ignored, but the PARAMETERS declaration lists hash_index and sort_columns as supported parameters. In ArrowAccelerator, these parameters are read from source and inserted into cmd.options before creating the table. Since PartitionedArrowAccelerator ignores _source, these parameters are silently dropped—the options are never inserted into cmd and are never passed to...

### Prompt 2

[Request interrupted by user for tool use]

### Prompt 3

Make integration tests like `/Users/jeadie/Github/spiceai/crates/runtime/tests/acceleration/partition_by_cayenne.rs` for the new `PartitionedArrowAccelerator`.

### Prompt 4

How do i specifically only run that test?

