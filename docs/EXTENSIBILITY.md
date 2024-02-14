# Spice.ai Extensibility

This document is an overview of all the interfaces and extension points in Spice.ai.

| Component     | Description                                                                                                                        | Definition Link                                      |
|---------------|------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| DataConnector | Represents the source of data to the Spice.ai runtime. Specifies how to retrieve data, stream data updates, and write data back. | [dataconnector.rs](../crates/runtime/src/dataconnector.rs) |
| DataBackend   | Used by the runtime to store accelerated data locally. Specifies which data backend to use via `engine` & `mode` fields.          | [databackend.rs](../crates/runtime/src/databackend.rs)     |
