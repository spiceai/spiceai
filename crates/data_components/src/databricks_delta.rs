/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::delta_lake::DeltaTable;
use crate::unity_catalog::UnityCatalog;
use crate::Read;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use secrecy::SecretString;
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct DatabricksDelta {
    pub params: Arc<HashMap<String, SecretString>>,
}

impl DatabricksDelta {
    #[must_use]
    pub fn new(params: Arc<HashMap<String, SecretString>>) -> Self {
        Self { params }
    }
}

#[async_trait]
impl Read for DatabricksDelta {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        _schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        get_delta_table(table_reference, Arc::clone(&self.params)).await
    }
}

async fn get_delta_table(
    table_reference: TableReference,
    params: Arc<HashMap<String, SecretString>>,
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
    let table_uri = resolve_table_uri(table_reference, Arc::clone(&params)).await?;

    let mut storage_options = HashMap::new();
    for (key, value) in params.iter() {
        if key == "token" || key == "endpoint" {
            continue;
        }
        storage_options.insert(key.to_string(), value.clone());
    }

    let delta_table = DeltaTable::from(table_uri, storage_options)?;

    Ok(Arc::new(delta_table) as Arc<dyn TableProvider>)
}

#[allow(clippy::implicit_hasher)]
pub async fn resolve_table_uri(
    table_reference: TableReference,
    params: Arc<HashMap<String, SecretString>>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let uc_client = UnityCatalog::from_params(&params).boxed()?;

    let table_opt = uc_client.get_table(&table_reference).await.boxed()?;

    if let Some(table) = table_opt {
        if let Some(storage_location) = table.storage_location {
            Ok(storage_location)
        } else {
            Err(
                format!("Databricks table {table_reference} does not have a storage location")
                    .into(),
            )
        }
    } else {
        Err(format!("Databricks table {table_reference} does not exist").into())
    }
}
