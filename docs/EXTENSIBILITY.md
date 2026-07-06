# Spice.ai Extensibility

This document is an overview of all the interfaces and extension points in Spice.ai.

| Component           | Description                                                                                                                                                                                  | Definition Link                                            |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| [Data Connector]    | Represents the source of data to the Spice.ai runtime. Specifies how to retrieve data, stream data updates, and write data back.                                                             | [dataconnector/mod.rs](../crates/runtime/src/dataconnector/mod.rs) |
| [Data Accelerator]  | Used by the runtime to store accelerated data locally. Specify which data accelerator to use via `engine` & `mode` fields.                                                                   | [dataaccelerator/mod.rs](../crates/runtime/src/dataaccelerator/mod.rs) |
| [Catalog Connector] | Catalog Connectors connect to external catalog providers and make their tables available for federated SQL query in Spice. Implemented via the `CatalogConnector` trait.                    | [catalogconnector/mod.rs](../crates/runtime/src/catalogconnector/mod.rs) |
| [Secret Stores]     | A Secret Store is a location where secrets are stored and can be used to store sensitive data, like passwords, tokens, and secret keys.                                                      | [runtime-secrets](../crates/runtime-secrets/src/lib.rs)    |
| [Models]            | A machine-learning (ML) or language model (LLM) to load for inferencing.                                                                                                                     | [model.rs](../crates/model_components/src/model.rs)        |
| Embeddings          | Embeddings map high-dimensional data to a lower-dimensional vector space.                                                                                                                    | [embeddings.rs](../crates/llms/src/embeddings/mod.rs)      |

[Data Connector]: https://spiceai.org/docs/components/data-connectors
[Data Accelerator]: https://spiceai.org/docs/components/data-accelerators
[Catalog Connector]: https://spiceai.org/docs/components/catalogs
[Secret Stores]: https://spiceai.org/docs/components/secret-stores
[Models]: https://spiceai.org/docs/components/models
