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
use deltalake::aws::storage::s3_constants::AWS_S3_ALLOW_UNSAFE_RENAME;
use deltalake::open_table_with_storage_options;
use secrets::{ExposeSecret, Secret};
use serde::Deserialize;
use std::{collections::HashMap, error::Error, sync::Arc};

use crate::{Read, ReadWrite};

use crate::deltatable::write::DeltaTableWriter;

#[derive(Clone)]
pub struct DatabricksDelta {
    pub secret: Arc<Option<Secret>>,
    pub params: Arc<HashMap<String, String>>,
}

impl DatabricksDelta {
    #[must_use]
    pub fn new(secret: Arc<Option<Secret>>, params: Arc<HashMap<String, String>>) -> Self {
        Self { secret, params }
    }
}

#[async_trait]
impl Read for DatabricksDelta {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        get_delta_table(
            Arc::clone(&self.secret),
            table_reference,
            Arc::clone(&self.params),
        )
        .await
    }
}

#[async_trait]
impl ReadWrite for DatabricksDelta {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        let delta_table = get_delta_table(
            Arc::clone(&self.secret),
            table_reference,
            Arc::clone(&self.params),
        )
        .await?;

        DeltaTableWriter::create(delta_table).map_err(Into::into)
    }
}

async fn get_delta_table(
    secret: Arc<Option<Secret>>,
    table_reference: TableReference,
    params: Arc<HashMap<String, String>>,
) -> Result<Arc<dyn TableProvider>, Box<dyn Error + Send + Sync>> {
    // Needed to be able to load the s3:// scheme
    deltalake::aws::register_handlers(None);
    deltalake::azure::register_handlers(None);
    let table_uri = resolve_table_uri(table_reference, &secret, params).await?;

    let mut storage_options = HashMap::new();
    if let Some(secret) = secret.as_ref() {
        for (key, value) in secret.iter() {
            if key == "token" {
                continue;
            }
            storage_options.insert(key.to_string(), value.expose_secret().clone());
        }
    };
    storage_options.insert(AWS_S3_ALLOW_UNSAFE_RENAME.to_string(), "true".to_string());

    let delta_table = open_table_with_storage_options(table_uri, storage_options).await?;

    Ok(Arc::new(delta_table) as Arc<dyn TableProvider>)
}

#[derive(Deserialize)]
struct DatabricksTablesApiResponse {
    storage_location: String,
}

#[allow(clippy::implicit_hasher)]
pub async fn resolve_table_uri(
    table_reference: TableReference,
    secret: &Arc<Option<Secret>>,
    params: Arc<HashMap<String, String>>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let Some(endpoint) = params.get("endpoint") else {
        return Err("Endpoint not found in dataset params".into());
    };

    let table_name = table_reference.table();

    let mut token = "Token not found in auth provider";
    if let Some(secret) = secret.as_ref() {
        if let Some(token_secret_val) = secret.get("token") {
            token = token_secret_val;
        };
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
        Ok(api_response.storage_location)
    } else {
        Err(format!(
            "Failed to retrieve databricks table URI. Status: {}",
            response.status()
        )
        .into())
    }
}
