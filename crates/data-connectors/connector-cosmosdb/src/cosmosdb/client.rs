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

//! Build a [`ContainerClient`] for a specific `(database, container)` from a
//! user-supplied credential. Each [`CosmosDBTableProvider`] is pinned to one
//! container, so we construct the `ContainerClient` once at connector setup
//! and reuse it for schema inference and every subsequent scan.
//!
//! [`CosmosDBTableProvider`]: super::provider::CosmosDBTableProvider

use std::sync::Arc;

use azure_core::credentials::Secret;
use azure_data_cosmos::{ConnectionString, CosmosClient, clients::ContainerClient};
use snafu::ResultExt;

use super::{BuildClientSnafu, Error, InvalidConnectionStringSnafu};

/// Credential used to build a Cosmos client.
///
/// Carries account keys / full connection strings; the manual `Debug` below
/// redacts both so tracing / panic dumps never surface them.
#[derive(Clone)]
pub enum CosmosDBCredential {
    /// An `AccountEndpoint=https://...;AccountKey=...;` connection string.
    ConnectionString(String),
    /// Explicit account endpoint URL plus primary/secondary key.
    Key { endpoint: String, key: String },
}

impl std::fmt::Debug for CosmosDBCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionString(_) => f
                .debug_tuple("ConnectionString")
                .field(&"<redacted>")
                .finish(),
            Self::Key { endpoint, .. } => f
                .debug_struct("Key")
                .field("endpoint", endpoint)
                .field("key", &"<redacted>")
                .finish(),
        }
    }
}

/// Build a [`ContainerClient`] for the given `(database, container)` pair,
/// returning the account endpoint alongside it (needed for resilience keying
/// and error messages).
///
/// # Errors
/// Returns an error if the credential is malformed or the underlying Azure
/// SDK client cannot be constructed.
pub fn build_container_client(
    credential: CosmosDBCredential,
    database: &str,
    container: &str,
) -> Result<(ContainerClient, Arc<str>), Error> {
    let (client, endpoint) = match credential {
        CosmosDBCredential::ConnectionString(conn_str) => {
            let parsed: ConnectionString = conn_str
                .parse()
                .map_err(boxed_err)
                .context(InvalidConnectionStringSnafu)?;
            let endpoint = parsed.account_endpoint;

            let client = CosmosClient::with_connection_string(Secret::from(conn_str), None)
                .map_err(boxed_err)
                .context(BuildClientSnafu {
                    endpoint: endpoint.clone(),
                })?;

            (client, endpoint)
        }
        CosmosDBCredential::Key { endpoint, key } => {
            let client = CosmosClient::with_key(&endpoint, Secret::from(key), None)
                .map_err(boxed_err)
                .context(BuildClientSnafu {
                    endpoint: endpoint.clone(),
                })?;

            (client, endpoint)
        }
    };

    let container_client = client.database_client(database).container_client(container);

    Ok((container_client, Arc::from(normalize_endpoint(&endpoint))))
}

/// Normalize a Cosmos DB account endpoint so benign URL-formatting differences
/// (trailing slash, casing) don't split the shared per-account concurrency
/// budget across datasets that target the same account.
fn normalize_endpoint(endpoint: &str) -> String {
    endpoint.trim().trim_end_matches('/').to_ascii_lowercase()
}

fn boxed_err<E>(e: E) -> Box<dyn std::error::Error + Send + Sync>
where
    E: std::error::Error + Send + Sync + 'static,
{
    Box::new(e)
}

#[cfg(test)]
mod tests {
    use super::normalize_endpoint;

    #[test]
    fn normalize_endpoint_collapses_benign_variants() {
        let canonical = "https://myaccount.documents.azure.com:443";
        for variant in [
            "https://myaccount.documents.azure.com:443",
            "https://myaccount.documents.azure.com:443/",
            "https://MYACCOUNT.documents.azure.com:443/",
            "  https://myaccount.documents.azure.com:443/  ",
        ] {
            assert_eq!(normalize_endpoint(variant), canonical, "input: {variant:?}");
        }
    }
}
