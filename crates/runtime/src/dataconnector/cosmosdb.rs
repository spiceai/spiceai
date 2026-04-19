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
        .description("The Cosmos DB database name. Defaults to the first segment of the dataset `from:` path ('database.container')."),
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
                message: "Azure Cosmos DB requires either 'cosmosdb_connection_string' or both 'cosmosdb_account_endpoint' and 'cosmosdb_account_key'.".to_string(),
            }),
        }
    }
}

/// Pure parsing helper for [`resolve_database_and_container`]. Split out so
/// it can be exercised in unit tests without constructing a full [`Dataset`].
fn parse_database_and_container(
    path: &str,
    database_param: Option<&str>,
) -> Result<(String, String), String> {
    // Accept either `database.container` or `database/container`, or just the
    // container when `database` is explicitly set.
    let (db_from_path, container) = if let Some((db, container)) = path.split_once('.') {
        (Some(db.to_string()), container.to_string())
    } else if let Some((db, container)) = path.split_once('/') {
        (Some(db.to_string()), container.to_string())
    } else {
        (None, path.to_string())
    };

    let database = match (database_param, db_from_path) {
        (Some(d), _) => d.to_string(),
        (None, Some(d)) => d,
        (None, None) => {
            return Err(format!(
                "Could not determine Cosmos DB database from dataset path '{path}'. Expected 'database.container' or set the 'cosmosdb_database' parameter."
            ));
        }
    };

    if database.is_empty() {
        return Err(format!(
            "Could not determine Cosmos DB database from dataset path '{path}'. Expected 'database.container' or set the 'cosmosdb_database' parameter."
        ));
    }

    if container.is_empty() {
        return Err(format!(
            "Could not determine Cosmos DB container from dataset path '{path}'."
        ));
    }

    Ok((database, container))
}

/// Parse `database.container` / `database/container` from the dataset path.
/// If the configured `database` parameter is set, it overrides the database
/// segment and the path is treated as just the container name.
fn resolve_database_and_container(
    dataset: &Dataset,
    database_param: Option<&str>,
) -> Result<(String, String), DataConnectorError> {
    parse_database_and_container(dataset.path(), database_param).map_err(|message| {
        DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: CONNECTOR_NAME.to_string(),
            connector_component: ConnectorComponent::from(dataset),
            message,
        }
    })
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

        let schema_infer_max_records = match self
            .params
            .get("schema_infer_max_records")
            .expose()
            .ok()
        {
            Some(value) => match value.parse::<usize>() {
                Ok(0) => {
                    tracing::warn!(
                        "Ignoring invalid schema_infer_max_records value '0' for dataset {}; using default value {}.",
                        dataset.name,
                        DEFAULT_SCHEMA_INFER_MAX_RECORDS
                    );
                    DEFAULT_SCHEMA_INFER_MAX_RECORDS
                }
                Ok(v) => v,
                Err(_) => {
                    tracing::warn!(
                        "Ignoring invalid schema_infer_max_records value '{}' for dataset {}; expected a positive integer, using default value {}.",
                        value,
                        dataset.name,
                        DEFAULT_SCHEMA_INFER_MAX_RECORDS
                    );
                    DEFAULT_SCHEMA_INFER_MAX_RECORDS
                }
            },
            None => DEFAULT_SCHEMA_INFER_MAX_RECORDS,
        };

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

#[cfg(test)]
mod tests {
    use super::parse_database_and_container;

    #[test]
    fn parses_dot_delimited_path() {
        let (db, container) = parse_database_and_container("mydb.mycontainer", None).unwrap();
        assert_eq!(db, "mydb");
        assert_eq!(container, "mycontainer");
    }

    #[test]
    fn parses_slash_delimited_path() {
        let (db, container) = parse_database_and_container("mydb/mycontainer", None).unwrap();
        assert_eq!(db, "mydb");
        assert_eq!(container, "mycontainer");
    }

    #[test]
    fn uses_database_param_when_path_is_container_only() {
        let (db, container) =
            parse_database_and_container("mycontainer", Some("explicit_db")).unwrap();
        assert_eq!(db, "explicit_db");
        assert_eq!(container, "mycontainer");
    }

    #[test]
    fn database_param_overrides_path_segment() {
        let (db, container) =
            parse_database_and_container("path_db.mycontainer", Some("override_db")).unwrap();
        assert_eq!(db, "override_db");
        assert_eq!(container, "mycontainer");
    }

    #[test]
    fn errors_when_no_database_can_be_determined() {
        let err = parse_database_and_container("just_container", None).unwrap_err();
        assert!(err.contains("Could not determine Cosmos DB database"));
    }

    #[test]
    fn errors_on_empty_container_segment() {
        let err = parse_database_and_container("mydb.", None).unwrap_err();
        assert!(err.contains("Could not determine Cosmos DB container"));

        let err = parse_database_and_container("mydb/", None).unwrap_err();
        assert!(err.contains("Could not determine Cosmos DB container"));
    }

    #[test]
    fn errors_on_empty_database_segment() {
        let err = parse_database_and_container(".mycontainer", None).unwrap_err();
        assert!(err.contains("Could not determine Cosmos DB database"));

        let err = parse_database_and_container("/mycontainer", None).unwrap_err();
        assert!(err.contains("Could not determine Cosmos DB database"));
    }

    #[test]
    fn dot_takes_precedence_over_slash() {
        // Documents current behavior: the first `.` wins even when a `/` is
        // also present. Cosmos DB names do not legally contain `.`, so this
        // mainly matters for malformed input.
        let (db, container) = parse_database_and_container("a/b.c", None).unwrap();
        assert_eq!(db, "a/b");
        assert_eq!(container, "c");
    }

    #[test]
    fn multiple_dots_split_at_first() {
        let (db, container) = parse_database_and_container("a.b.c", None).unwrap();
        assert_eq!(db, "a");
        assert_eq!(container, "b.c");
    }
}
