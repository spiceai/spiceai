# Spice.ai Extensibility

This document is an overview of all the interfaces and extension points in Spice.ai.

## DataConnector

A `DataConnector` is a component that represents the source of data to the Spice.ai runtime. Datasets specify which `DataConnector` to use via the `from` field in the dataset configuration. A `DataConnector` knows how to retrieve data, stream data updates, and writing data back to the source.

Defined in [dataconnector.rs](../crates/runtime/src/dataconnector.rs)

## DataBackend

A `DataBackend` is a component the runtime uses to store accelerated data locally. The `acceleration` object in the dataset configuration specifies which `DataBackend` to use via the `engine` & `mode` fields.

i.e.
```yaml
acceleration:
  engine: duckdb # arrow
  mode: memory # file
```

Defined in [databackend.rs](../crates/runtime/src/databackend.rs)

