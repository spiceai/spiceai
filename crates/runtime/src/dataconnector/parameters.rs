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

//! The runtime side of the connector-parameter contract: the concrete
//! [`ConnectorContext`] over a component's app + runtime handles, and the
//! builder that resolves a component's spicepod `params` into
//! [`ConnectorParams`]. The contract itself lives in `data-connector-api`,
//! below `runtime`, so a connector can name it without the orchestrator.

use std::sync::Arc;

use app::App;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
pub(crate) use data_connector_api::parameters::{ConnectorContext, ConnectorParams, Validator};
use data_http_rate_control::HttpRateControlRegistry;
use datafusion::execution::context::SessionContext;
use datafusion_table_providers::UnsupportedTypeAction;
use runtime_checkpoint_api::{
    BlobCheckpointStore, CheckpointError, debezium::DebeziumCheckpointStore,
    kafka::KafkaCheckpointStore, mongodb::MongoCheckpointStore, mysql_binlog::MySqlBinlogStore,
};
use runtime_component::dataset::DatasetSpec;
use token_provider::registry::TokenProviderRegistry;
use tokio::{runtime::Handle, sync::RwLock};

use crate::{
    Runtime,
    catalogconnector::CATALOG_CONNECTOR_FACTORY_REGISTRY,
    component::{catalog::Catalog, dataset::Dataset},
    dataaccelerator::spice_sys,
    parameters::Parameters,
};
use runtime_secrets::{Secrets, get_params_with_secrets};

use super::{ConnectorComponent, DATA_CONNECTOR_FACTORY_REGISTRY, DataConnectorError};
use crate::dataconnector::{ODBC_DATACONNECTOR, SCYLLADB_DATACONNECTOR, SCYLLADB_FEATURE};

// The AWS parameter validators moved down with the contract; the runtime's own
// in-body connectors (s3, glue, iceberg) still name them. Crate-visible so
// nothing outside can reacquire the path through here.
pub(crate) use data_connector_api::parameters::aws;

/// [`ConnectorContext`] over the app + runtime handles a component carries.
///
/// Built at each call site that hands a connector a context, and dropped when
/// that call returns. The runtime is therefore held **strongly** with no cycle
/// risk: a connector only ever sees `&dyn ConnectorContext`, so nothing it owns
/// can outlive the call and point back at the runtime.
pub struct RuntimeConnectorContext {
    app: Arc<App>,
    runtime: Arc<Runtime>,
}

impl RuntimeConnectorContext {
    #[must_use]
    pub fn new(app: Arc<App>, runtime: Arc<Runtime>) -> Self {
        Self { app, runtime }
    }

    /// The context for a connector serving `dataset`.
    #[must_use]
    pub fn for_dataset(dataset: &Dataset) -> Self {
        Self::new(dataset.app(), dataset.runtime())
    }

    /// The context for a catalog connector serving `catalog`.
    #[must_use]
    pub fn for_catalog(catalog: &Catalog) -> Self {
        Self::new(catalog.app(), catalog.runtime())
    }

    /// Rebind a configuration spec to the app + runtime handles held here.
    ///
    /// Resolving a dataset to its accelerator needs the engine registry and the secrets,
    /// which hang off the runtime. Rebinding keeps those on this side of the connector
    /// boundary, so the contract can name a spec while the resolution still has
    /// everything it needs.
    fn bind(&self, dataset: &DatasetSpec) -> Dataset {
        Dataset {
            spec: dataset.clone(),
            app: Arc::clone(&self.app),
            runtime: Arc::clone(&self.runtime),
        }
    }
}

#[async_trait]
impl ConnectorContext for RuntimeConnectorContext {
    fn app(&self) -> Arc<App> {
        Arc::clone(&self.app)
    }

    fn http_rate_control_registry(&self) -> Arc<HttpRateControlRegistry> {
        self.runtime.http_rate_control_registry()
    }

    fn token_provider_registry(&self) -> Arc<TokenProviderRegistry> {
        self.runtime.token_provider_registry()
    }

    fn datafusion_session_context(&self) -> Arc<SessionContext> {
        Arc::clone(&self.runtime.datafusion().ctx)
    }

    async fn accelerated_checkpoint_schema(&self, dataset: &DatasetSpec) -> Option<SchemaRef> {
        super::sink::accelerated_checkpoint_schema(&self.bind(dataset)).await
    }

    async fn blob_checkpoint_store(
        &self,
        dataset: &DatasetSpec,
        table_name: &'static str,
    ) -> Option<Arc<dyn BlobCheckpointStore>> {
        spice_sys::checkpoint_store(&self.bind(dataset), table_name).await
    }

    async fn kafka_checkpoint_store(
        &self,
        dataset: &DatasetSpec,
    ) -> Result<Arc<dyn KafkaCheckpointStore>, CheckpointError> {
        spice_sys::kafka_checkpoint_store(&self.bind(dataset)).await
    }

    async fn debezium_checkpoint_store(
        &self,
        dataset: &DatasetSpec,
    ) -> Result<Arc<dyn DebeziumCheckpointStore>, CheckpointError> {
        spice_sys::debezium_checkpoint_store(&self.bind(dataset)).await
    }

    async fn mysql_binlog_store(
        &self,
        dataset: &DatasetSpec,
    ) -> Result<Arc<dyn MySqlBinlogStore>, CheckpointError> {
        spice_sys::mysql_binlog_store(&self.bind(dataset)).await
    }

    async fn mongo_checkpoint_store(
        &self,
        dataset: &DatasetSpec,
    ) -> Result<Arc<dyn MongoCheckpointStore>, CheckpointError> {
        spice_sys::mongo_checkpoint_store(&self.bind(dataset)).await
    }
}

pub struct ConnectorParamsBuilder {
    connector: Arc<str>,
    component: ConnectorComponent,
}

impl ConnectorParamsBuilder {
    /// Parameters for a data connector serving `dataset`.
    #[must_use]
    pub fn for_dataset(connector: Arc<str>, dataset: &Dataset) -> Self {
        Self {
            connector,
            component: ConnectorComponent::from(dataset),
        }
    }

    /// Parameters for a catalog connector serving `catalog`.
    #[must_use]
    pub fn for_catalog(connector: Arc<str>, catalog: &Catalog) -> Self {
        Self {
            connector,
            component: ConnectorComponent::from(catalog),
        }
    }

    pub async fn build(
        self,
        secrets: Arc<RwLock<Secrets>>,
        io_runtime: Handle,
    ) -> Result<ConnectorParams, Box<dyn std::error::Error + Send + Sync>> {
        let name = self.connector.to_string();
        let mut unsupported_type_action = None;
        let (params, prefix, parameters) = match &self.component {
            ConnectorComponent::Catalog(catalog) => {
                let (prefix, parameters) = {
                    let guard = CATALOG_CONNECTOR_FACTORY_REGISTRY.lock().await;
                    let connector_factory = guard.get(&name);

                    let factory = connector_factory.ok_or_else(|| {
                        DataConnectorError::InvalidConnectorType {
                            dataconnector: name.clone(),
                            connector_component: self.component.clone(),
                        }
                    })?;

                    (factory.prefix(), factory.parameters())
                };

                (
                    get_params_with_secrets(Arc::clone(&secrets), &catalog.params).await,
                    prefix,
                    parameters,
                )
            }
            ConnectorComponent::Dataset(dataset) => {
                unsupported_type_action = dataset.unsupported_type_action;

                let (prefix, parameters) = {
                    let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;
                    let connector_factory = guard.get(&name);

                    let factory = connector_factory.ok_or_else(|| {
                        if name == ODBC_DATACONNECTOR {
                            DataConnectorError::OdbcNotInstalled {
                                connector_component: self.component.clone(),
                            }
                        } else if name == SCYLLADB_DATACONNECTOR {
                            DataConnectorError::ConnectorNotInBuild {
                                dataconnector: name.clone(),
                                feature: SCYLLADB_FEATURE.to_string(),
                                connector_component: self.component.clone(),
                            }
                        } else {
                            DataConnectorError::InvalidConnectorType {
                                dataconnector: name.clone(),
                                connector_component: self.component.clone(),
                            }
                        }
                    })?;

                    (factory.prefix(), factory.parameters())
                };

                let params = get_params_with_secrets(Arc::clone(&secrets), &dataset.params).await;

                (params, prefix, parameters)
            }
        };

        let parameters = Parameters::try_new(
            &format!("connector {name}"),
            params.into_iter().collect(),
            prefix,
            secrets,
            parameters,
        )
        .await?;

        Ok(ConnectorParams {
            parameters,
            unsupported_type_action: unsupported_type_action.map(UnsupportedTypeAction::from),
            component: self.component,
            io_runtime,
        })
    }
}
