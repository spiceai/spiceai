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

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use secrecy::{ExposeSecret, Secret, SecretString};
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc};

use crate::delta::DeltaTable;
use crate::Read;

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

#[derive(Deserialize)]
struct DatabricksTablesApiResponse {
    storage_location: String,
}

#[allow(clippy::implicit_hasher)]
pub async fn resolve_table_uri(
    table_reference: TableReference,
    params: Arc<HashMap<String, SecretString>>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let Some(endpoint) = params.get("endpoint").map(Secret::expose_secret) else {
        return Err("Endpoint not found in dataset params".into());
    };

    let table_name = table_reference.to_string();

    let mut token = "Token not found in auth provider";
    if let Some(token_secret_val) = params.get("token").map(Secret::expose_secret) {
        token = token_secret_val;
    };

    let url = format!(
        "{}/api/2.1/unity-catalog/tables/{}",
        endpoint.trim_end_matches('/'),
        table_name
    );

    let client = reqwest::Client::new();
    let response = client.get(&url).bearer_auth(token).send().await?;

    if response.status().is_success() {
        let api_response: DatabricksTablesApiResponse = response.json().await?;
        tracing::debug!("Databricks table URI: {}", api_response.storage_location);
        Ok(format!("{}/", api_response.storage_location))
    } else {
        Err(format!(
            "Failed to retrieve databricks table URI. Status: {}",
            response.status()
        )
        .into())
    }
}
