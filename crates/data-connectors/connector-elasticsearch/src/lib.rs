/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Elasticsearch data connector for Spice.ai runtime.
//!
//! This connector allows Spice.ai to connect to Elasticsearch clusters as data
//! sources, supporting:
//! - Querying Elasticsearch indexes as SQL tables
//! - kNN vector similarity search (via `vector_search` UDTF)
//! - Full-text search (via `text_search` UDTF)
//! - Hybrid search (via `rrf` UDTF combining vector and text search)

use async_trait::async_trait;
use data_components::elasticsearch::query_table::ElasticsearchQueryTable;
use data_components::elasticsearch::schema::mapping_to_schema;
use datafusion::datasource::TableProvider;
use elasticsearch::{Client, Elasticsearch};
use runtime::component::dataset::Dataset;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult,
};
use runtime::parameters::ParameterSpec;
use secrecy::ExposeSecret;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Elasticsearch data connector.
pub struct ElasticsearchConnector {
    client: Arc<dyn Elasticsearch>,
}

impl std::fmt::Debug for ElasticsearchConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElasticsearchConnector")
            .finish_non_exhaustive()
    }
}

/// Factory for creating Elasticsearch connector instances.
#[derive(Default, Copy, Clone)]
pub struct ElasticsearchFactory {}

impl ElasticsearchFactory {
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
    ParameterSpec::component("endpoint")
        .description("Elasticsearch cluster URL (e.g., https://localhost:9200). Required."),
    ParameterSpec::component("user")
        .description("Username for Elasticsearch authentication.")
        .secret(),
    ParameterSpec::component("pass")
        .description("Password for Elasticsearch authentication.")
        .secret(),
];

impl DataConnectorFactory for ElasticsearchFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = runtime::dataconnector::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let endpoint = params
                .parameters
                .get("endpoint")
                .ok()
                .map(|s| s.expose_secret().to_string())
                .ok_or_else(|| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "elasticsearch".to_string(),
                    connector_component: params.component.clone(),
                    source: Box::from(
                        "Missing required parameter 'endpoint'. Provide the Elasticsearch URL.",
                    ),
                })?;

            let user = params
                .parameters
                .get("user")
                .ok()
                .map(|s| s.expose_secret().to_string());
            let pass = params
                .parameters
                .get("pass")
                .ok()
                .map(|s| s.expose_secret().to_string());

            let client = Client::new(&endpoint, user.as_deref(), pass.as_deref()).map_err(|e| {
                DataConnectorError::UnableToConnectInternal {
                    dataconnector: "elasticsearch".to_string(),
                    connector_component: params.component.clone(),
                    source: Box::new(e),
                }
            })?;

            Ok(Arc::new(ElasticsearchConnector {
                client: Arc::new(client) as Arc<dyn Elasticsearch>,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "elasticsearch"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "elasticsearch";

/// Returns a new instance of the Elasticsearch connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    ElasticsearchFactory::new_arc()
}

#[derive(Debug, Snafu)]
enum ReadProviderError {
    #[snafu(display("Unable to get read provider for {dataconnector}: {source}"))]
    UnableToGetReadProvider {
        dataconnector: &'static str,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl From<ReadProviderError> for DataConnectorError {
    fn from(err: ReadProviderError) -> Self {
        match err {
            ReadProviderError::UnableToGetReadProvider {
                dataconnector,
                connector_component,
                source,
            } => DataConnectorError::UnableToGetReadProvider {
                dataconnector: dataconnector.to_string(),
                connector_component,
                source,
            },
        }
    }
}

#[async_trait]
impl DataConnector for ElasticsearchConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let index_name = dataset.path().to_string();

        // Fetch the ES index mapping to derive the Arrow schema.
        let mapping = self.client.get_mapping(&index_name).await.map_err(|e| {
            ReadProviderError::UnableToGetReadProvider {
                dataconnector: "elasticsearch",
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            }
        })?;

        let schema = mapping
            .get(&index_name)
            .map(|m| mapping_to_schema(&m.mappings.properties))
            .ok_or_else(|| ReadProviderError::UnableToGetReadProvider {
                dataconnector: "elasticsearch",
                connector_component: ConnectorComponent::from(dataset),
                source: Box::from(format!(
                    "Elasticsearch index '{index_name}' not found in mapping response"
                )),
            })?;

        Ok(Arc::new(ElasticsearchQueryTable::new(
            Arc::clone(&self.client),
            index_name,
            schema,
        )))
    }
}
