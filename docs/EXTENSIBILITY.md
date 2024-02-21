# Spice.ai Extensibility

This document is an overview of all the interfaces and extension points in Spice.ai.

| Component       | Description                                                                                                                                | Definition Link                                            |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------       | ------------------------------------------------------     |
| DataConnector   | Represents the source of data to the Spice.ai runtime. Specifies how to retrieve data, stream data updates, and write data back.           | [dataconnector.rs](../crates/runtime/src/dataconnector.rs) |
| DataBackend     | Used by the runtime to store accelerated data locally. Specifies which data backend to use via `engine` & `mode` fields.                   | [databackend.rs](../crates/runtime/src/databackend.rs)     |
| DataPublisher   | An interface that specifies how to publish data updates. All DataBackends implement it, and DataConnectors that support writing data back. | [datapublisher.rs](../crates/runtime/src/datapublisher.rs) |
| ModelFormat     | Specifies the format of model artifacts.                                                                                                   | [modelformat.rs](../crates/runtime/src/modelformat.rs)     |
| ModelRuntime    | Provides the runtime-specific information needed to run a model. Specifies the runtime and its configuration.                              | [modelruntime.rs](../crates/runtime/src/modelruntime.rs)   |
| ModelSource     | Specifies the source of the model.                                                                                                         | [modelsource.rs](../crates/runtime/src/modelsource.rs)     |