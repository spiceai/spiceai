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

//! What a data connector is handed when it is built: its resolved
//! [`ConnectorParams`], and the [`ConnectorContext`] through which it reaches
//! the runtime capabilities it needs.
//!
//! The runtime assembles both — `ConnectorParamsBuilder` and the concrete
//! `ConnectorContext` implementation live there — but the *contract* lives here,
//! next to the trait that names it.

use std::sync::Arc;

use app::App;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_http_rate_control::HttpRateControlRegistry;
use datafusion::execution::context::SessionContext;
use datafusion_table_providers::UnsupportedTypeAction;
use runtime_checkpoint_api::{
    BlobCheckpointStore, CheckpointError, debezium::DebeziumCheckpointStore,
    kafka::KafkaCheckpointStore, mongodb::MongoCheckpointStore, mysql_binlog::MySqlBinlogStore,
};
use runtime_component::dataset::DatasetSpec;
use runtime_parameters::Parameters;
use token_provider::registry::TokenProviderRegistry;
use tokio::runtime::Handle;

use crate::ConnectorComponent;

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
    ///
    /// `None` once the runtime has shut down. These three accessors are fallible
    /// because the context holds the runtime weakly — see `RuntimeConnectorContext`.
    fn http_rate_control_registry(&self) -> Option<Arc<HttpRateControlRegistry>>;

    /// The registry of token providers a connector authenticates through. `None` once
    /// the runtime has shut down.
    fn token_provider_registry(&self) -> Option<Arc<TokenProviderRegistry>>;

    /// The runtime's own `DataFusion` session, for a connector that registers an
    /// object store the main session must resolve at scan time. `None` once the runtime
    /// has shut down.
    fn datafusion_session_context(&self) -> Option<Arc<SessionContext>>;

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

    /// The Debezium checkpoint store over this dataset's accelerator. See
    /// [`Self::kafka_checkpoint_store`] for why this reports failure as an error.
    async fn debezium_checkpoint_store(
        &self,
        dataset: &DatasetSpec,
    ) -> Result<Arc<dyn DebeziumCheckpointStore>, CheckpointError>;

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
            .and_then(|ctx| ctx.http_rate_control_registry())
    }

    /// The token-provider registry, if a runtime is attached.
    #[must_use]
    pub fn token_provider_registry(&self) -> Option<Arc<TokenProviderRegistry>> {
        self.context
            .as_ref()
            .and_then(|ctx| ctx.token_provider_registry())
    }

    /// The runtime's own `DataFusion` session, if a runtime is attached.
    #[must_use]
    pub fn datafusion_session_context(&self) -> Option<Arc<SessionContext>> {
        self.context
            .as_ref()
            .and_then(|ctx| ctx.datafusion_session_context())
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
