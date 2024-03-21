use async_trait::async_trait;
use datafusion::{common::OwnedTableReference, datasource::TableProvider};
use deltalake::aws::storage::s3_constants::AWS_S3_ALLOW_UNSAFE_RENAME;
use deltalake::open_table_with_storage_options;
use secrets::{ExposeSecret, Secret};
use serde::Deserialize;
use std::{collections::HashMap, error::Error, sync::Arc};

use crate::Read;

mod write;

#[derive(Clone)]
pub struct Databricks {
    pub secret: Arc<Option<Secret>>,
    pub params: Option<HashMap<String, String>>,
}

impl Databricks {
    #[must_use]
    pub fn new(secret: Option<Secret>, params: Option<HashMap<String, String>>) -> Self {
        Self {
            secret: Arc::new(secret),
            params,
        }
    }
}

#[async_trait]
impl Read for Databricks {
    async fn table_provider(
        &self,
        table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        get_delta_table(
            Arc::clone(&self.secret),
            table_reference,
            self.params.clone(),
        )
        .await
    }
}

async fn get_delta_table(
    secret: Arc<Option<Secret>>,
    table_reference: OwnedTableReference,
    params: Option<HashMap<String, String>>,
) -> Result<Arc<dyn TableProvider>, Box<dyn Error + Send + Sync>> {
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
    table_reference: OwnedTableReference,
    secret: &Arc<Option<Secret>>,
    params: Option<HashMap<String, String>>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let params = match params {
        None => return Err("Dataset params not found".into()),
        Some(params) => params,
    };

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
