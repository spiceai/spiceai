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

use std::sync::Arc;

use app::App;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_http_rate_control::HttpRateControlRegistry;
use datafusion::execution::context::SessionContext;
use datafusion_table_providers::UnsupportedTypeAction;
use runtime_checkpoint_api::{
    BlobCheckpointStore, CheckpointError, kafka::KafkaCheckpointStore,
    mongodb::MongoCheckpointStore, mysql_binlog::MySqlBinlogStore,
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

use super::{
    ConnectorComponent, DATA_CONNECTOR_FACTORY_REGISTRY, DataConnectorError, ODBC_DATACONNECTOR,
};

// `pub` (not `pub(crate)`): the AWS config helper is used by extracted AWS
// connector crates (e.g. connector-dynamodb) as well as in-tree ones (glue).
pub mod aws;
pub mod azure;
pub mod gcs;

#[async_trait]
pub trait Validator {
    type Error;

    /// Parameters may be changed while validating.
    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Self::Error>;
}

/// The runtime capabilities a data connector may reach for while it is being
/// built, behind a handle so [`ConnectorParams`] does not name them directly.
/// A connector's *configuration* travels separately, as the
/// [`ConnectorComponent`] spec.
///
/// Each method is a single capability rather than a handle to the orchestrator,
/// so the contract names only types that live below `runtime`: a registry, a
/// session, the loaded app, or an already-resolved answer.
#[async_trait]
pub trait ConnectorContext: Send + Sync {
    /// The loaded app, for the runtime-level configuration a connector consults
    /// (e.g. `runtime.params`, `runtime.flight`).
    fn app(&self) -> Arc<App>;

    /// The process-wide per-origin HTTP rate-control registry, so connectors
    /// sharing an origin share one limiter.
    fn http_rate_control_registry(&self) -> Arc<HttpRateControlRegistry>;

    /// The registry of token providers a connector authenticates through.
    fn token_provider_registry(&self) -> Arc<TokenProviderRegistry>;

    /// The runtime's own `DataFusion` session, for a connector that registers an
    /// object store the main session must resolve at scan time.
    fn datafusion_session_context(&self) -> Arc<SessionContext>;

    /// The accelerated schema recorded in this dataset's acceleration
    /// checkpoint, so a connector can re-advertise the schema a previous run
    /// stored rather than re-deriving it.
    ///
    /// `None` when there is nothing to inherit: the dataset is not
    /// file-accelerated, no checkpoint has been written yet, or the stored
    /// checkpoint cannot be read.
    async fn accelerated_checkpoint_schema(&self, dataset: &DatasetSpec) -> Option<SchemaRef>;

    /// The **blob** checkpoint store over this dataset's accelerator, writing into the
    /// sidecar `table_name`.
    ///
    /// `None` when the dataset has no usable accelerator connection (acceleration
    /// disabled, or the engine is not compiled in); the reason is logged here, so a
    /// caller degrades to running without a persisted checkpoint rather than failing.
    /// Contrast the structured-shape accessors below, which surface the error.
    async fn blob_checkpoint_store(
        &self,
        dataset: &DatasetSpec,
        table_name: &'static str,
    ) -> Option<Arc<dyn BlobCheckpointStore>>;

    /// The Kafka checkpoint store over this dataset's accelerator.
    ///
    /// These structured-shape accessors return the error rather than `None` because
    /// their callers do not share one recovery policy: an unpersistable Kafka
    /// checkpoint fails the dataset, while `MySQL` and `MongoDB` log it and run
    /// ephemerally. Deciding that here would silently change one of them.
    ///
    /// Only meaningful for a file-accelerated dataset — callers check
    /// [`DatasetSpec::is_file_accelerated`] first.
    async fn kafka_checkpoint_store(
        &self,
        dataset: &DatasetSpec,
    ) -> Result<Arc<dyn KafkaCheckpointStore>, CheckpointError>;

    /// The `MySQL` binlog position store over this dataset's accelerator. See
    /// [`Self::kafka_checkpoint_store`] for why this reports failure as an error.
    async fn mysql_binlog_store(
        &self,
        dataset: &DatasetSpec,
    ) -> Result<Arc<dyn MySqlBinlogStore>, CheckpointError>;

    /// The `MongoDB` resume-token store over this dataset's accelerator. See
    /// [`Self::kafka_checkpoint_store`] for why this reports failure as an error.
    async fn mongo_checkpoint_store(
        &self,
        dataset: &DatasetSpec,
    ) -> Result<Arc<dyn MongoCheckpointStore>, CheckpointError>;
}

/// [`ConnectorContext`] over the app + runtime handles a component carries.
pub struct RuntimeConnectorContext {
    app: Arc<App>,
    runtime: Arc<Runtime>,
}

impl RuntimeConnectorContext {
    #[must_use]
    pub fn new(app: Arc<App>, runtime: Arc<Runtime>) -> Self {
        Self { app, runtime }
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

#[derive(Clone)]
pub struct ConnectorParams {
    pub parameters: Parameters,
    pub unsupported_type_action: Option<UnsupportedTypeAction>,
    pub component: ConnectorComponent,
    /// `None` only where no runtime is attached — connector unit tests that
    /// build params directly.
    pub context: Option<Arc<dyn ConnectorContext>>,
    pub io_runtime: Handle,
}

impl ConnectorParams {
    /// The loaded app, if a runtime is attached.
    #[must_use]
    pub fn app(&self) -> Option<Arc<App>> {
        self.context.as_ref().map(|ctx| ctx.app())
    }

    /// The HTTP rate-control registry, if a runtime is attached.
    #[must_use]
    pub fn http_rate_control_registry(&self) -> Option<Arc<HttpRateControlRegistry>> {
        self.context
            .as_ref()
            .map(|ctx| ctx.http_rate_control_registry())
    }

    /// The token-provider registry, if a runtime is attached.
    #[must_use]
    pub fn token_provider_registry(&self) -> Option<Arc<TokenProviderRegistry>> {
        self.context
            .as_ref()
            .map(|ctx| ctx.token_provider_registry())
    }

    /// The runtime's own `DataFusion` session, if a runtime is attached.
    #[must_use]
    pub fn datafusion_session_context(&self) -> Option<Arc<SessionContext>> {
        self.context
            .as_ref()
            .map(|ctx| ctx.datafusion_session_context())
    }

    /// The accelerated schema stored for `dataset`, if a runtime is attached and
    /// a checkpoint holds one.
    pub async fn accelerated_checkpoint_schema(&self, dataset: &DatasetSpec) -> Option<SchemaRef> {
        self.context
            .as_ref()?
            .accelerated_checkpoint_schema(dataset)
            .await
    }

    /// The blob checkpoint store over `dataset`'s accelerator, writing into the sidecar
    /// `table_name`. `None` if no runtime is attached or the dataset has no usable
    /// accelerator connection.
    pub async fn blob_checkpoint_store(
        &self,
        dataset: &DatasetSpec,
        table_name: &'static str,
    ) -> Option<Arc<dyn BlobCheckpointStore>> {
        self.context
            .as_ref()?
            .blob_checkpoint_store(dataset, table_name)
            .await
    }

    /// The Kafka checkpoint store over `dataset`'s accelerator.
    ///
    /// # Errors
    ///
    /// Returns an error if no runtime is attached, or if the dataset's accelerator
    /// cannot be resolved into a store.
    pub async fn kafka_checkpoint_store(
        &self,
        dataset: &DatasetSpec,
    ) -> Result<Arc<dyn KafkaCheckpointStore>, CheckpointError> {
        self.checkpoint_context()?
            .kafka_checkpoint_store(dataset)
            .await
    }

    /// The `MySQL` binlog position store over `dataset`'s accelerator.
    ///
    /// # Errors
    ///
    /// Returns an error if no runtime is attached, or if the dataset's accelerator
    /// cannot be resolved into a store.
    pub async fn mysql_binlog_store(
        &self,
        dataset: &DatasetSpec,
    ) -> Result<Arc<dyn MySqlBinlogStore>, CheckpointError> {
        self.checkpoint_context()?.mysql_binlog_store(dataset).await
    }

    /// The `MongoDB` resume-token store over `dataset`'s accelerator.
    ///
    /// # Errors
    ///
    /// Returns an error if no runtime is attached, or if the dataset's accelerator
    /// cannot be resolved into a store.
    pub async fn mongo_checkpoint_store(
        &self,
        dataset: &DatasetSpec,
    ) -> Result<Arc<dyn MongoCheckpointStore>, CheckpointError> {
        self.checkpoint_context()?
            .mongo_checkpoint_store(dataset)
            .await
    }

    /// The attached context, as a checkpoint-store error when there is none.
    ///
    /// Only connector unit tests build params without a runtime, so this reports the
    /// same "nothing can persist a checkpoint" outcome as an unresolvable accelerator
    /// rather than a distinct case each caller has to handle.
    fn checkpoint_context(&self) -> Result<&Arc<dyn ConnectorContext>, CheckpointError> {
        self.context.as_ref().ok_or_else(|| CheckpointError::Store {
            source: "No runtime is attached to these connector parameters".into(),
        })
    }
}

pub struct ConnectorParamsBuilder {
    connector: Arc<str>,
    component: ConnectorComponent,
    context: Option<Arc<dyn ConnectorContext>>,
}

impl ConnectorParamsBuilder {
    /// Parameters for a data connector serving `dataset`.
    #[must_use]
    pub fn for_dataset(connector: Arc<str>, dataset: &Dataset) -> Self {
        Self {
            connector,
            component: ConnectorComponent::from(dataset),
            context: Some(Arc::new(RuntimeConnectorContext::new(
                dataset.app(),
                dataset.runtime(),
            ))),
        }
    }

    /// Parameters for a catalog connector serving `catalog`.
    #[must_use]
    pub fn for_catalog(connector: Arc<str>, catalog: &Catalog) -> Self {
        Self {
            connector,
            component: ConnectorComponent::from(catalog),
            context: Some(Arc::new(RuntimeConnectorContext::new(
                catalog.app(),
                catalog.runtime(),
            ))),
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
            context: self.context,
            io_runtime,
        })
    }
}
