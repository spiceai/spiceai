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

//! Thin wrapper around [`azure_data_cosmos::CosmosClient`] that holds the
//! user-supplied credentials and exposes the small set of operations the
//! table provider needs.

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

/// Owns a [`CosmosClient`] and hands out container clients.
#[derive(Clone)]
pub struct CosmosDBClient {
    inner: Arc<CosmosClient>,
    endpoint: String,
}

impl std::fmt::Debug for CosmosDBClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CosmosDBClient")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl CosmosDBClient {
    /// Build a new Cosmos DB client from the supplied credential.
    ///
    /// # Errors
    /// Returns an error if the credential is malformed or the underlying
    /// Azure SDK client cannot be constructed.
    pub fn new(credential: CosmosDBCredential) -> Result<Self, Error> {
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

        Ok(Self {
            inner: Arc::new(client),
            endpoint,
        })
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn container_client(&self, database: &str, container: &str) -> ContainerClient {
        self.inner
            .database_client(database)
            .container_client(container)
    }
}

fn boxed_err<E>(e: E) -> Box<dyn std::error::Error + Send + Sync>
where
    E: std::error::Error + Send + Sync + 'static,
{
    Box::new(e)
}
