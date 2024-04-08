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
use datafusion::execution::context::SessionContext;
use deltalake::aws::storage::s3_constants::AWS_S3_ALLOW_UNSAFE_RENAME;
use deltalake::protocol::SaveMode;
use deltalake::{open_table_with_storage_options, DeltaOps};
use ns_lookup::verify_endpoint_connection;
use secrecy::ExposeSecret;
use secrets::Secret;
use serde::Deserialize;
use snafu::prelude::*;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use spicepod::component::dataset::Dataset;

use crate::datapublisher::{AddDataResult, DataPublisher};
use crate::dataupdate::DataUpdate;

use super::{DataConnector, DataConnectorFactory};

#[derive(Clone)]
pub struct Databricks {
    secret: Arc<Option<Secret>>,
    params: Arc<Option<HashMap<String, String>>>,
}

impl DataConnectorFactory for Databricks {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        // Needed to be able to load the s3:// scheme
        deltalake::aws::register_handlers(None);
        deltalake::azure::register_handlers(None);

        let databricks = Self {
            secret: Arc::new(secret),
            params: params.clone(),
        };

        Box::pin(async move {
            let url: String = params
                .as_ref() // &Option<HashMap<String, String>>
                .as_ref() // Option<&HashMap<String, String>>
                .and_then(|params| params.get("endpoint").cloned())
                .ok_or_else(|| super::Error::UnableToCreateDataConnector {
                    source: "Missing required parameter: endpoint".into(),
                })?;

            verify_endpoint_connection(&url)
                .await
                .map_err(|e| super::Error::UnableToCreateDataConnector { source: e.into() })?;

            Ok(Box::new(databricks) as Box<dyn DataConnector>)
        })
    }
}

#[async_trait]
impl DataConnector for Databricks {
    fn get_all_data(
        &self,
        dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<arrow::record_batch::RecordBatch>> + Send>> {
        let dataset = dataset.clone();
        let secret = Arc::clone(&self.secret);
        let params = Arc::clone(&self.params);
        Box::pin(async move {
            let ctx = SessionContext::new();

            let table_provider = match get_table_provider(&secret, &params, &dataset).await {
                Ok(provider) => provider,
                Err(e) => {
                    tracing::error!("Failed to get table provider: {}", e);
                    return vec![];
                }
            };

            let _ = ctx.register_table("temp_table", table_provider);

            let sql = "SELECT * FROM temp_table;";

            let df = match ctx.sql(sql).await {
                Ok(df) => df,
                Err(e) => {
                    tracing::error!("Failed to execute query: {}", e);
                    return vec![];
                }
            };

            df.collect().await.unwrap_or_else(|e| {
                tracing::error!("Failed to collect results: {}", e);
                vec![]
            })
        })
    }

    fn has_table_provider(&self) -> bool {
        true
    }

    async fn get_table_provider(
        &self,
        dataset: &Dataset,
    ) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
        get_table_provider(&self.secret, &self.params, dataset).await
    }

    fn get_data_publisher(&self) -> Option<Box<dyn DataPublisher>> {
        Some(Box::new(self.clone()))
    }
}

impl DataPublisher for Databricks {
    fn add_data(&self, dataset: Arc<Dataset>, data_update: DataUpdate) -> AddDataResult {
        Box::pin(async move {
            let delta_table = get_delta_table(&self.secret, &self.params, &dataset).await?;

            let _ = DeltaOps(delta_table)
                .write(data_update.data)
                .with_save_mode(SaveMode::Append)
                .await?;

            Ok(())
        })
    }

    fn name(&self) -> &str {
        "Databricks"
    }
}

async fn get_table_provider(
    secret: &Arc<Option<Secret>>,
    params: &Arc<Option<HashMap<String, String>>>,
    dataset: &Dataset,
) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
    let delta_table: deltalake::DeltaTable = get_delta_table(secret, params, dataset).await?;

    Ok(Arc::new(delta_table))
}

const DEFAULT_TIMEOUT: &str = "300s";

async fn get_delta_table(
    secret: &Arc<Option<Secret>>,
    params: &Arc<Option<HashMap<String, String>>>,
    dataset: &Dataset,
) -> std::result::Result<deltalake::DeltaTable, super::Error> {
    let table_uri = resolve_table_uri(dataset, secret)
        .await
        .context(super::UnableToGetTableProviderSnafu)?;

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

    let timeout: Option<String> = params
        .as_ref() // &Option<HashMap<String, String>>
        .as_ref() // Option<&HashMap<String, String>>
        .and_then(|params| params.get("timeout").cloned());

    if let Some(timeout) = timeout {
        storage_options.insert("timeout".to_string(), timeout);
    } else {
        storage_options.insert("timeout".to_string(), DEFAULT_TIMEOUT.to_string());
    }

    let delta_table = open_table_with_storage_options(table_uri, storage_options)
        .await
        .boxed()
        .context(super::UnableToGetTableProviderSnafu)?;

    Ok(delta_table)
}

#[derive(Deserialize)]
struct DatabricksTablesApiResponse {
    storage_location: String,
}

pub async fn resolve_table_uri(
    dataset: &Dataset,
    secret: &Arc<Option<Secret>>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = match &dataset.params {
        None => return Err("Dataset params not found".into()),
        Some(params) => match params.get("endpoint") {
            Some(val) => val,
            None => return Err("Endpoint not specified in dataset params".into()),
        },
    };

    let table_name = dataset.path();

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
