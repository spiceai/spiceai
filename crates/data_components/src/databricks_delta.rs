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

use crate::unity_catalog::UnityCatalog;
use crate::Read;
use crate::{delta_lake::DeltaTable, unity_catalog::Endpoint};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use secrecy::SecretString;
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct DatabricksDelta {
    endpoint: Endpoint,
    token: SecretString,
    storage_options: HashMap<String, SecretString>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("A storage location for the Databricks table '{table_reference}' must be provided.\nSpecify a storage location, and try again."))]
    TableDoesNotHaveStorageLocation { table_reference: TableReference },
    #[snafu(display("Failed to find the Databricks table '{table_reference}'.\nVerify the table exists, and try again."))]
    TableDoesNotExist { table_reference: TableReference },
}

impl DatabricksDelta {
    #[must_use]
    pub fn new(
        endpoint: Endpoint,
        token: SecretString,
        storage_options: HashMap<String, SecretString>,
    ) -> Self {
        Self {
            endpoint,
            token,
            storage_options,
        }
    }

    async fn get_delta_table(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let table_uri = self.resolve_table_uri(table_reference).await?;

        let mut storage_options = HashMap::new();
        for (key, value) in &self.storage_options {
            match key.as_ref() {
                "token" | "endpoint" => {
                    continue;
                }
                "client_timeout" => {
                    storage_options.insert("timeout".into(), value.clone());
                }
                _ => {
                    storage_options.insert(key.to_string(), value.clone());
                }
            }
        }

        let delta_table = DeltaTable::from(table_uri, storage_options)?;

        Ok(Arc::new(delta_table) as Arc<dyn TableProvider>)
    }

    #[allow(clippy::implicit_hasher)]
    pub async fn resolve_table_uri(
        &self,
        table_reference: TableReference,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let uc_client = UnityCatalog::new(self.endpoint.clone(), Some(self.token.clone()));

        let table_opt = uc_client.get_table(&table_reference).await.boxed()?;

        if let Some(table) = table_opt {
            if let Some(storage_location) = table.storage_location {
                Ok(storage_location)
            } else {
                Err(Error::TableDoesNotHaveStorageLocation { table_reference }.into())
            }
        } else {
            Err(Error::TableDoesNotExist { table_reference }.into())
        }
    }
}

#[async_trait]
impl Read for DatabricksDelta {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        _schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        self.get_delta_table(table_reference).await
    }
}
