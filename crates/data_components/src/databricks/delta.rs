/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::Read;
use crate::unity_catalog::credential_vending::vended_delta_table;
use crate::unity_catalog::{UCTable, UnityCatalog};
use crate::{delta_lake::DeltaTable, unity_catalog::Endpoint};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};
use token_provider::TokenProvider;
use tokio::runtime::Handle;

#[derive(Clone)]
pub struct DatabricksDelta {
    endpoint: Endpoint,
    token_provider: Arc<dyn TokenProvider>,
    storage_options: HashMap<String, SecretString>,
    io_runtime: Handle,
    credential_vending_client: Option<Arc<UnityCatalog>>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "A storage location for the Databricks table '{table_reference}' must be provided. Specify a storage location, and try again."
    ))]
    TableDoesNotHaveStorageLocation { table_reference: TableReference },
    #[snafu(display(
        "Failed to find the Databricks table '{table_reference}'. Verify the table exists, and try again."
    ))]
    TableDoesNotExist { table_reference: TableReference },
}

impl DatabricksDelta {
    pub fn new(
        endpoint: Endpoint,
        storage_options: HashMap<String, SecretString>,
        token_provider: Arc<dyn TokenProvider>,
        io_runtime: Handle,
    ) -> Self {
        Self {
            endpoint,
            token_provider,
            storage_options,
            io_runtime,
            credential_vending_client: None,
        }
    }

    /// Enables Unity Catalog credential vending: each table's storage
    /// credentials are vended through `client` instead of taken from the
    /// configured storage options.
    #[must_use]
    pub fn with_credential_vending(mut self, client: Arc<UnityCatalog>) -> Self {
        self.credential_vending_client = Some(client);
        self
    }

    /// Returns `true` when Unity Catalog credential vending is enabled.
    #[must_use]
    pub fn credential_vending_enabled(&self) -> bool {
        self.credential_vending_client.is_some()
    }

    async fn get_delta_table(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let table = self.get_uc_table(&table_reference).await?;
        let Some(table_uri) = table.storage_location.clone() else {
            return Err(Error::TableDoesNotHaveStorageLocation { table_reference }.into());
        };

        if let Some(client) = &self.credential_vending_client {
            let aws_region = self
                .storage_options
                .get("aws_region")
                .map(|s| s.expose_secret().to_string());
            match vended_delta_table(Arc::clone(client), &table, &table_uri, aws_region).await {
                Ok(delta) => return Ok(Arc::new(delta) as Arc<dyn TableProvider>),
                Err(err) => {
                    tracing::warn!(
                        table = %table.full_name(),
                        "Unable to use Unity Catalog credential vending for table; falling back to configured storage credentials: {err}"
                    );
                }
            }
        }

        let mut storage_options = HashMap::new();
        for (key, value) in &self.storage_options {
            match key.as_ref() {
                "token" | "endpoint" => {}
                "client_timeout" => {
                    storage_options.insert("timeout".into(), value.clone());
                }
                _ => {
                    storage_options.insert(key.clone(), value.clone());
                }
            }
        }

        let delta_table = DeltaTable::from(table_uri, storage_options, &self.io_runtime).boxed()?;

        Ok(Arc::new(delta_table) as Arc<dyn TableProvider>)
    }

    async fn get_uc_table(
        &self,
        table_reference: &TableReference,
    ) -> Result<UCTable, Box<dyn std::error::Error + Send + Sync>> {
        // Reuse the vending client when present: it carries the connector's
        // rate-control configuration and avoids rebuilding an HTTP client for
        // every table lookup.
        let table_opt = match &self.credential_vending_client {
            Some(client) => client.get_table(table_reference).await.boxed()?,
            None => {
                let uc_client = UnityCatalog::new(
                    self.endpoint.clone(),
                    Some(Arc::clone(&self.token_provider)),
                    None,
                )
                .boxed()?;
                uc_client.get_table(table_reference).await.boxed()?
            }
        };

        table_opt.ok_or_else(|| {
            Error::TableDoesNotExist {
                table_reference: table_reference.clone(),
            }
            .into()
        })
    }

    pub async fn resolve_table_uri(
        &self,
        table_reference: TableReference,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let table = self.get_uc_table(&table_reference).await?;
        table
            .storage_location
            .ok_or_else(|| Error::TableDoesNotHaveStorageLocation { table_reference }.into())
    }
}

#[async_trait]
impl Read for DatabricksDelta {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        self.get_delta_table(table_reference).await
    }
}
