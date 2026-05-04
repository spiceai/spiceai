/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Azure Cosmos DB (`NoSQL` / Core SQL API) data connector components.
//!
//! Exposes a [`provider::CosmosDBTableProvider`] built on top of the
//! `azure_data_cosmos` crate. Documents are fetched via a Cosmos SQL query
//! (`SELECT * FROM c` by default) and projected into Arrow `RecordBatch`es.
//!
//! The current connector release targets *RC* quality: read-only,
//! cross-partition scan, schema inference from a sample of documents, and no
//! filter push-down yet. See `docs/criteria/connectors/rc.md` for the full
//! Cosmos DB row and `docs/dev/cosmosdb.md` for the type map and limitations.

pub mod client;
pub mod provider;
pub mod resilience;
pub mod schema;

use snafu::Snafu;

pub use client::{CosmosDBCredential, build_container_client};
pub use provider::CosmosDBTableProvider;
pub use resilience::{
    BackoffMethod, CosmosResilienceConfig, DEFAULT_MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_RETRIES,
    ResilienceError,
};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Default SQL query used when no custom `query` is provided. Selects every
/// property from the root alias `c`, which is the canonical cross-partition
/// scan in Cosmos DB `NoSQL`.
pub const DEFAULT_QUERY: &str = "SELECT * FROM c";

/// Default sample size used for schema inference when no explicit value is
/// provided. Kept intentionally small to minimize Request Unit (RU) usage on
/// initial dataset registration.
pub const DEFAULT_SCHEMA_INFER_MAX_RECORDS: usize = 100;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display(
        "Failed to build the Azure Cosmos DB client for account {endpoint}: {source}"
    ))]
    BuildClient {
        endpoint: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Invalid Azure Cosmos DB connection string. Ensure the connection string was copied directly from the Azure portal: {source}"
    ))]
    InvalidConnectionString {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Azure Cosmos DB requires either 'connection_string' or both 'account_endpoint' and 'account_key' to be set."
    ))]
    MissingCredentials,

    #[snafu(display(
        "Failed to query Azure Cosmos DB container '{container}' in database '{database}': {source}"
    ))]
    QueryFailed {
        database: String,
        container: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Azure Cosmos DB container '{container}' in database '{database}' returned no documents to infer schema from. \
        Ensure the container is populated, or pin a schema explicitly via the dataset `columns` configuration."
    ))]
    EmptyContainer { database: String, container: String },

    #[snafu(display("Failed to infer Arrow schema from Cosmos DB documents: {source}"))]
    SchemaInference { source: arrow::error::ArrowError },

    #[snafu(display("Failed to decode Cosmos DB JSON document into Arrow: {source}"))]
    JsonDecode { source: arrow::error::ArrowError },

    #[snafu(display(
        "Invalid dataset path '{path}'. Azure Cosmos DB dataset paths must be of the form 'database.container' or 'database/container'."
    ))]
    InvalidDatasetPath { path: String },

    #[snafu(display(
        "The Azure Cosmos DB connector at '{endpoint}' is disabled after a permanent error (401/403/404). Fix the credentials or grants, then restart Spice."
    ))]
    ConnectorDisabled { endpoint: String },

    #[snafu(display(
        "Column '{column}' in Azure Cosmos DB dataset '{database}.{container}' has an unsupported Arrow data type ({data_type}). Set the dataset's `unsupported_type_action` parameter to `warn`, `ignore`, or `string` to proceed."
    ))]
    UnsupportedColumn {
        database: String,
        container: String,
        column: String,
        data_type: String,
    },
}
