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

//! Azure Cosmos DB (NoSQL / Core SQL API) data connector.
//!
//! Alpha-quality: read-only, cross-partition scan, schema inference from a
//! sample of documents. Full RC criteria (federation push-down, streaming
//! changes, write support, benchmarks) will be layered on incrementally.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use data_components::cosmosdb::{
    CosmosDBClient, CosmosDBCredential, CosmosDBTableProvider, DEFAULT_QUERY,
    DEFAULT_SCHEMA_INFER_MAX_RECORDS, provider::CosmosDBTableProviderConfig,
};
use datafusion::datasource::TableProvider;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec, Parameters,
};
use crate::component::dataset::Dataset;

const CONNECTOR_NAME: &str = "cosmosdb";

#[derive(Debug)]
pub struct CosmosDB {
    params: Parameters,
}

#[derive(Default, Debug, Copy, Clone)]
pub struct CosmosDBFactory {}

impl CosmosDBFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("account_endpoint")
        .description("The Azure Cosmos DB account endpoint URL, e.g. 'https://my-account.documents.azure.com:443/'.")
        .secret(),
    ParameterSpec::component("account_key")
        .description("The Azure Cosmos DB account primary or secondary key.")
        .secret(),
    ParameterSpec::component("connection_string")
        .description("An Azure Cosmos DB connection string (AccountEndpoint=...;AccountKey=...). Takes precedence over account_endpoint/account_key if set.")
        .secret(),
    ParameterSpec::component("database")
        .description("The Cosmos DB database name. Defaults to the first segment of the dataset `from:` path ('database.container').")
        .required(),
    ParameterSpec::runtime("query")
        .description("Cosmos SQL query used to scan the container. Defaults to 'SELECT * FROM c'.")
        .default(DEFAULT_QUERY),
    ParameterSpec::runtime("schema_infer_max_records")
        .description("Number of documents sampled during schema inference. Larger samples produce a more precise schema at the cost of additional RU consumption on dataset registration.")
        .default("100"),
];

impl DataConnectorFactory for CosmosDBFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let conn = CosmosDB {
                params: params.parameters,
            };
            Ok(Arc::new(conn) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        CONNECTOR_NAME
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

impl CosmosDB {
    fn build_credential(
        &self,
        dataset: &Dataset,
    ) -> Result<CosmosDBCredential, DataConnectorError> {
        if let Some(conn_str) = self.params.get("connection_string").expose().ok() {
            return Ok(CosmosDBCredential::ConnectionString(conn_str.to_string()));
        }

        let endpoint = self.params.get("account_endpoint").expose().ok();
        let key = self.params.get("account_key").expose().ok();

        match (endpoint, key) {
            (Some(endpoint), Some(key)) => Ok(CosmosDBCredential::Key {
                endpoint: endpoint.to_string(),
                key: key.to_string(),
            }),
            _ => Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: "Azure Cosmos DB requires either 'connection_string' or both 'account_endpoint' and 'account_key'.".to_string(),
            }),
        }
    }
}

/// Parse `database.container` / `database/container` from the dataset path.
/// If the configured `database` parameter is set, it overrides the database
/// segment and the path is treated as just the container name.
fn resolve_database_and_container(
    dataset: &Dataset,
    database_param: Option<&str>,
) -> Result<(String, String), DataConnectorError> {
    let path = dataset.path();
    let path_str: &str = &path;

    // Accept either `database.container` or `database/container`, or just the
    // container when `database` is explicitly set.
    let (db_from_path, container) = if let Some((db, container)) = path_str.split_once('.') {
        (Some(db.to_string()), container.to_string())
    } else if let Some((db, container)) = path_str.split_once('/') {
        (Some(db.to_string()), container.to_string())
    } else {
        (None, path_str.to_string())
    };

    let database = match (database_param, db_from_path) {
        (Some(d), _) => d.to_string(),
        (None, Some(d)) => d,
        (None, None) => {
            return Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: format!(
                    "Could not determine Cosmos DB database from dataset path '{path_str}'. Expected 'database.container' or set the 'database' parameter."
                ),
            });
        }
    };

    if container.is_empty() {
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: CONNECTOR_NAME.to_string(),
            connector_component: ConnectorComponent::from(dataset),
            message: format!(
                "Could not determine Cosmos DB container from dataset path '{path_str}'."
            ),
        });
    }

    Ok((database, container))
}

#[async_trait]
impl DataConnector for CosmosDB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> Result<Arc<dyn TableProvider>, DataConnectorError> {
        let credential = self.build_credential(dataset)?;

        let client = CosmosDBClient::new(credential).map_err(|e| {
            DataConnectorError::UnableToGetReadProvider {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            }
        })?;

        let database_param = self.params.get("database").expose().ok();
        let (database, container) = resolve_database_and_container(dataset, database_param)?;

        let query = self
            .params
            .get("query")
            .expose()
            .ok()
            .unwrap_or(DEFAULT_QUERY)
            .to_string();

        let schema_infer_max_records = self
            .params
            .get("schema_infer_max_records")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_SCHEMA_INFER_MAX_RECORDS);

        let config = CosmosDBTableProviderConfig::new(database, container, query)
            .with_schema_infer_max_records(schema_infer_max_records);

        let provider = CosmosDBTableProvider::try_new(client, config)
            .await
            .map_err(|e| DataConnectorError::UnableToGetReadProvider {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            })?;

        Ok(Arc::new(provider))
    }
}

register_data_connector!("cosmosdb", CosmosDBFactory);
