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
#![allow(clippy::missing_errors_doc)]
#![recursion_limit = "256"]

use ::tools::SpiceModelTool;
use ::tools::rename::with_name;
use async_stream::stream;
use datafusion_expr::Expr;
use init::scheduler::ScheduleRegistry;
use spicepod::component::runtime::TelemetryConfig;
use std::collections::HashSet;
use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Weak;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use token_provider::registry::TokenProviderRegistry;
use tokio::runtime::Handle;
use tokio::{sync::Mutex, task::JoinHandle, time::Instant};
use tools::factory::{ToolFactory, default_catalog_names};
use util::force_shutdown_signal;
use worker::WorkerRegistry;

use crate::dataaccelerator::AcceleratorEngineRegistry;
use crate::datafusion::DataFusion;
use crate::datafusion::error::format_datafusion_error;
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use crate::model::LLMResponsesModelStore;
use crate::{auth::EndpointAuth, dataconnector::DataConnector};

use ::datafusion::error::DataFusionError;
use ::datafusion::sql::{ResolvedTableReference, TableReference, sqlparser};
use app::App;
use datafusion_proto::bytes::Serializeable;

use {crate::Error::FailedToStartClusterExecutor, crate::config::ClusterRole};

use builder::RuntimeBuilder;
use config::Config;
use dataconnector::ConnectorComponent;
use datasets_health_monitor::DatasetsHealthMonitor;
use extension::ExtensionFactory;
use flight::RateLimits;
use futures::{
    Stream, TryFutureExt,
    future::{join_all, try_join_all},
};
use governor::RateLimiter;
#[cfg(feature = "openapi")]
pub use http::get_api_doc;
use llms::rerank::RerankerModelStore;
use model::{EmbeddingModelStore, LLMChatCompletionsModelStore};

use crate::tools::{Tooling, catalog::SpiceToolCatalog, factory::default_available_catalogs};
use model_components::model::Model;
pub use notify::Error as NotifyError;
use snafu::prelude::*;
use status::ComponentStatus;
use tls::TlsConfig;

use tokio::sync::{RwLock, oneshot::error::RecvError};
use tokio_util::sync::CancellationToken;
pub use util::shutdown_signal;

use crate::cluster::{
    ClusterStateStore, DistributedNode, PartitionStore, SchedulerHeartbeatStore, SchedulerPeers,
};
use crate::extension::Extension;
use crate::udtfs::ListUDFTableFunc;
use runtime_async::cancellable_task::{CancellableTaskHandle, spawn_cancellable_task};
pub mod accelerated_table;
pub mod auth;
mod builder;
pub mod catalogconnector;
mod changes;
pub mod component;
pub mod config;
pub mod dataaccelerator;
pub mod dataconnector;
pub mod datafusion;
pub mod datasets_health_monitor;
pub mod dataupdate;
pub mod embeddings;
pub mod execution_plan;
pub mod executor_table;
pub mod extension;
pub mod federated_table;
pub mod flight;
mod http;

pub mod http_types {
    pub use crate::http::v1::queries::SubmitQueryRequest;
}

mod init;
pub mod internal_table;
pub mod jobs;
mod management;
mod metrics;
pub mod metrics_reader;
mod metrics_server;
pub mod model;
mod object_store_state;
mod opentelemetry;
pub mod otel_push_exporter;
pub mod resource_monitor;

pub use runtime_parameters as parameters;

pub mod podswatcher;
pub mod request;
mod scheduling;
pub(crate) mod schema_evolution;
pub mod search;
pub mod secrets {
    pub use runtime_secrets::*;
}
pub mod cluster;
pub mod spice_metrics;
pub mod status;
pub mod task_history;
pub mod tls;
pub mod token_providers;
pub mod tools;
pub(crate) mod tracers;
mod tracing_util;
mod udtfs;
mod view;
mod worker;

pub type PartitionAssignments =
    HashMap<ResolvedTableReference, Vec<::datafusion::logical_expr::Expr>>;
pub type SharedPartitionAssignments = Arc<RwLock<PartitionAssignments>>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to start HTTP server: {source}"))]
    UnableToStartHttpServer { source: http::Error },

    #[snafu(display(
        "Task execution failed: {source} Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToExecuteTask { source: tokio::task::JoinError },

    #[snafu(display("Unable to start Prometheus metrics server: {source}"))]
    UnableToStartMetricsServer { source: metrics_server::Error },

    #[snafu(display("Unable to start Flight server: {source}"))]
    UnableToStartFlightServer { source: flight::Error },

    #[snafu(display("Unable to start internal cluster server: {source}"))]
    UnableToStartClusterServer { source: flight::Error },

    #[snafu(display("Failed to start cluster scheduler: executor registry missing"))]
    MissingSchedulerExecutorRegistry,

    #[snafu(display("Unknown data source: {data_source}"))]
    UnknownDataSource { data_source: String },

    #[snafu(display("Failed to initialize the query engine: {source}"))]
    UnableToCreateBackend { source: datafusion::Error },

    #[snafu(display("Failed to attach view: {source}"))]
    UnableToAttachView { source: datafusion::Error },

    #[snafu(display("Failed to attach dataset index: {source}"))]
    UnableToAttachIndex { source: datafusion::Error },

    #[snafu(display("Failed to start pods watcher: {source}"))]
    UnableToInitializePodsWatcher { source: NotifyError },

    #[snafu(display("Failed to initialize data connector: {source}"))]
    UnableToInitializeDataConnector {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to initialize catalog connector: {source}"))]
    UnableToInitializeCatalogConnector {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to initialize LLM model: {source}"))]
    UnableToInitializeLlm {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to initialize embedding model: {source}"))]
    UnableToInitializeEmbeddingModel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to initialize LLM tool: {source}"))]
    UnableToInitializeLlmTool {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Unknown data connector '{data_connector}'.{}{} For details, visit: https://spiceai.org/docs/components/data-connectors",
        suggestion.as_ref().map(|s| format!(" Did you mean '{s}'?")).unwrap_or_default(),
        if available.is_empty() { String::new() } else { format!(" Available: {}.", available.join(", ")) },
    ))]
    UnknownDataConnector {
        data_connector: String,
        suggestion: Option<String>,
        available: Vec<String>,
    },

    #[snafu(display(
        "Unknown catalog connector '{catalog_connector}'.{}{} For details, visit: https://spiceai.org/docs/components/catalogs",
        suggestion.as_ref().map(|s| format!(" Did you mean '{s}'?")).unwrap_or_default(),
        if available.is_empty() { String::new() } else { format!(" Available: {}.", available.join(", ")) },
    ))]
    UnknownCatalogConnector {
        catalog_connector: String,
        suggestion: Option<String>,
        available: Vec<String>,
    },

    #[snafu(display(
        "The runtime is built without ODBC support. Build Spice.ai OSS with the `odbc` feature enabled or use the Docker image that includes ODBC support. For details, visit: https://spiceai.org/docs/components/data-connectors/odbc"
    ))]
    OdbcNotInstalled,

    #[snafu(display("Unable to load secrets for data connector: {data_connector}"))]
    UnableToLoadDataConnectorSecrets { data_connector: String },

    #[snafu(display("Unable to update cluster partition filters for table {table}: {source}"))]
    UnableToUpdateClusterPartitionFilters {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to deserialize partition expression for table {table}: {source}"))]
    UnableToDeserializeClusterPartitionExpression {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to get secret for data connector {data_connector}: {source}"))]
    UnableToGetSecretForDataConnector {
        source: Box<dyn std::error::Error + Send + Sync>,
        data_connector: String,
    },

    #[snafu(display("Unable to get secret for LLM: {source}"))]
    UnableToGetSecretForLLM {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to setup the {connector_component} ({data_connector}). {source}"))]
    UnableToAttachDataConnector {
        source: datafusion::Error,
        connector_component: ConnectorComponent,
        data_connector: String,
    },

    #[snafu(display("Unable to load SQL file {file}: {source}"))]
    UnableToLoadSqlFile {
        file: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to parse SQL: {source}"))]
    UnableToParseSql {
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display("Unable to create view: {reason}"))]
    UnableToCreateView { reason: String },

    #[snafu(display(
        "Specify the SQL string for view {name} using either `sql: SELECT * FROM...` inline or as a file reference with `sql_ref: my_view.sql`"
    ))]
    NeedToSpecifySQLView { name: String },

    #[snafu(display(
        "An accelerated table for {dataset_name} cannot be configured with both 'on_conflict' and 'acceleration.write_mode: write_back' without 'refresh_mode: changes'. Without CDC, 'on_conflict' forces writes to the accelerator only and there is no sync path back to the federated source. Add 'refresh_mode: changes' to enable CDC-based sync, or remove 'on_conflict'."
    ))]
    AcceleratedWriteBackWithOnConflict { dataset_name: String },

    #[snafu(display(
        "An accelerated table for {dataset_name} was configured with 'acceleration.write_mode: write_back' but 'replication.enabled' is not set. Write-back commits to the local accelerator first and persists to the federated source asynchronously, so source persistence failures are logged rather than returned to the caller. Set 'replication.enabled: true' to opt in to asynchronous source durability, or use a different write_mode."
    ))]
    AcceleratedWriteBackWithoutReplication { dataset_name: String },

    #[snafu(display(
        "An accelerated table for {dataset_name} was configured with 'refresh_mode = changes', but the data connector doesn't support a changes stream."
    ))]
    AcceleratedTableInvalidChanges { dataset_name: String },

    #[snafu(display(
        "An accelerated table has invalid configuration: {source}. Update the configuration and retry. For details, visit: https://spiceai.org/docs/reference/spicepod/datasets#acceleration"
    ))]
    InvalidAccelerationConfiguration {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Expected acceleration settings for {name}, found None"))]
    ExpectedAccelerationSettings { name: String },

    #[cfg(feature = "postgres-accel")]
    #[snafu(display(
        "The accelerator engine {name} is not available. Valid engines are arrow, cayenne, duckdb, sqlite, and postgres."
    ))]
    AcceleratorEngineNotAvailable { name: String },

    #[cfg(not(feature = "postgres-accel"))]
    #[snafu(display(
        "The accelerator engine {name} is not available. Valid engines are arrow, cayenne, duckdb, and sqlite."
    ))]
    AcceleratorEngineNotAvailable { name: String },

    #[snafu(display("The accelerator engine {name} failed to initialize: {source}"))]
    AcceleratorInitializationFailed {
        name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Dataset names should not include a catalog. Unexpected '{}' in '{}'. Remove '{}' from the dataset name and try again.",
        catalog,
        name,
        catalog,
    ))]
    DatasetNameIncludesCatalog { catalog: Arc<str>, name: Arc<str> },

    #[snafu(display("Unable to load dataset connector: {dataset}"))]
    UnableToLoadDatasetConnector { dataset: TableReference },

    #[snafu(display("Unable to load dataset connector: {dataset}. {reason}"))]
    PermanentDatasetFailure {
        dataset: TableReference,
        reason: String,
    },

    #[snafu(display("Unable to load data connector for catalog {catalog}: {source}"))]
    UnableToLoadCatalogConnector {
        catalog: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to load worker: {source}"))]
    UnableToLoadWorker {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("The data connector {dataconnector} doesn't support catalogs."))]
    DataConnectorDoesntSupportCatalogs { dataconnector: String },

    #[snafu(display("Unable to create accelerated table: {dataset}, {source}"))]
    UnableToCreateAcceleratedTable {
        dataset: TableReference,
        source: datafusion::Error,
    },

    #[snafu(display("Unable to receive accelerated table status: {source}"))]
    UnableToReceiveAcceleratedTableStatus { source: RecvError },

    #[snafu(display("Unable to start local metrics: {source}"))]
    UnableToStartLocalMetrics { source: spice_metrics::Error },

    #[snafu(display("Unable to track task history: {source}"))]
    UnableToTrackTaskHistory { source: task_history::Error },

    #[snafu(display("Unable to create metrics table: {}", format_datafusion_error(source)))]
    UnableToCreateMetricsTable { source: DataFusionError },

    #[snafu(display("Unable to register metrics table: {source}"))]
    UnableToRegisterMetricsTable { source: datafusion::Error },

    #[snafu(display("Invalid dataset defined in Spicepod: {source}"))]
    InvalidSpicepodDataset {
        source: crate::component::dataset::Error,
    },

    #[snafu(display("Invalid glob pattern {pattern}: {source}"))]
    InvalidGlobPattern {
        pattern: String,
        source: globset::Error,
    },

    #[snafu(display("Error converting GlobSet to Regex: {source}"))]
    ErrorConvertingGlobSetToRegex { source: globset::Error },

    #[snafu(display("Unable to create directory: {source}"))]
    UnableToCreateDirectory { source: std::io::Error },

    #[snafu(display("Unable to build dataset: {dataset}: {source}"))]
    UnableToBuildDataset {
        dataset: String,
        source: crate::component::dataset::Error,
    },

    #[snafu(display("Unable to build catalog: {catalog}: {source}"))]
    UnableToBuildCatalog {
        catalog: String,
        source: crate::component::catalog::Error,
    },

    #[snafu(display("{source}"))]
    ComponentError { source: component::Error },

    #[snafu(display("{source}"))]
    ComponentsInitializationFailed { source: tokio::task::JoinError },

    #[snafu(display("Initialization has been cancelled"))]
    ComponentsInitializationCancelled,

    #[snafu(display("Force shutdown requested"))]
    ForceTerminated,

    #[snafu(display(
        "Configuration of '{view_name}' view is invalid: {reason}. Update the configuration and retry. For details, visit: https://spiceai.org/docs/components/views"
    ))]
    AcceleratedViewInvalidConfiguration { view_name: String, reason: String },

    #[snafu(display(
        "Failed to start scheduler. {source} Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToStartScheduler { source: scheduler::Error },

    #[snafu(display(
        "Failed to build scheduler. {source} Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToBuildScheduler { source: scheduler::Error },

    #[snafu(display(
        "Failed to add schedule '{name}' to the '{scheduler}' scheduler. {source} Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToAddSchedule {
        source: scheduler::Error,
        scheduler: String,
        name: String,
    },

    #[snafu(display(
        "Failed to create a cron schedule from the provided expression: '{cron}' {source} Ensure the cron expression is valid and try again."
    ))]
    FailedToCreateCronChannel {
        cron: String,
        source: scheduler::Error,
    },

    #[snafu(display(
        "Failed to remove a schedule '{name}' from the '{scheduler}' scheduler. {source} Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToRemoveSchedule {
        source: scheduler::Error,
        scheduler: String,
        name: String,
    },

    #[snafu(display(
        "Failed to infer the worker type for the worker '{name}'. Ensure the worker has a valid configuration, and try again. For details, visit: https://spiceai.org/docs/components/workers"
    ))]
    FailedToInferWorkerType { name: String },

    #[snafu(display(
        "Dataset {dataset_name}: acceleration is required for full text search. Ensure the dataset has an acceleration configuration, and try again. For details, visit: https://spiceai.org/docs/reference/spicepod/datasets#acceleration"
    ))]
    FullTextSearchRequiresAcceleration { dataset_name: String },

    #[snafu(display("Failed to start Ballista scheduler: {source}"))]
    FailedToStartClusterScheduler {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to start or register Ballista executor: {source}"))]
    FailedToStartClusterExecutor {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to register scheduler: {source}"))]
    FailedToRegisterScheduler {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to convert partition expression to SQL: {}",
        format_datafusion_error(source)
    ))]
    UnableToConvertPartitionExpr { source: DataFusionError },
}

const CLUSTER_EXECUTOR: &str = "cluster_executor";
const CLUSTER_INTERNAL_SERVER: &str = "cluster_internal_server";
const CLUSTER_SCHEDULER_REGISTRY: &str = "cluster_scheduler_registry";
const CLUSTER_PARTITION_ASSIGNMENT_TASK: &str = "cluster_partition_assignment_task";
const HTTP_SERVER: &str = "http_server";
const METRICS_SERVER: &str = "metrics_server";
const FLIGHT_SERVER: &str = "flight_server";
const PODS_WATCHER: &str = "pods_watcher";
const COMPONENTS_INITIAL_LOAD: &str = "components_initial_load";
const CACHE_MAINTENANCE: &str = "cache_maintenance";

/// How often [`Runtime::run_cache_maintenance`] drives moka housekeeping.
const CACHE_MAINTENANCE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

// Allow 30 seconds for tasks for graceful shutdown
const RUNTIME_DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Copy)]
pub struct LogErrors(pub bool);

#[derive(Clone)]
#[expect(clippy::struct_field_names)]
pub struct Runtime {
    app: Arc<RwLock<Option<Arc<App>>>>,
    df: Arc<DataFusion>,
    models: Arc<RwLock<HashMap<String, Model>>>,
    completion_llms: Arc<RwLock<LLMChatCompletionsModelStore>>,
    /// Per-model rate controllers for AI UDF concurrency control.
    model_rate_controllers: Arc<RwLock<HashMap<String, Arc<runtime_rate_control::RateController>>>>,
    http_rate_control_registry: Arc<dataconnector::http_rate_control::HttpRateControlRegistry>,
    // LLMs that support the OpenAI Responses API
    responses_llms: Arc<RwLock<LLMResponsesModelStore>>,
    embeds: Arc<RwLock<EmbeddingModelStore>>,
    /// Registered reranker models (native cross-encoders, reranker-API
    /// providers). Consumed by the `rerank()` UDTF; may be empty when only
    /// LLM-as-reranker usage is needed — chat models are resolved from
    /// `completion_llms` as a fallback.
    rerankers: Arc<RwLock<RerankerModelStore>>,
    workers: WorkerRegistry,
    tools: Arc<RwLock<HashMap<String, Tooling>>>,
    tool_factories: Arc<Mutex<HashMap<String, ToolFactory>>>,
    pods_watcher: Arc<RwLock<Option<podswatcher::PodsWatcher>>>,
    secrets: Arc<RwLock<secrets::Secrets>>,
    datasets_health_monitor: Option<Arc<DatasetsHealthMonitor>>,
    metrics_endpoint: Option<SocketAddr>,
    prometheus_registry: Option<prometheus::Registry>,
    /// On-demand metrics reader for cluster observability.
    /// Used by `GetMetrics` RPC and executor control stream to collect local OTLP metrics.
    metrics_reader: Option<metrics_reader::MetricsReader>,
    rate_limits: Arc<RateLimits>,
    io_runtime: Handle,

    autoload_extensions: Arc<HashMap<String, Box<dyn ExtensionFactory>>>,
    extensions: Arc<RwLock<HashMap<String, Arc<dyn Extension>>>>,
    spaced_tracer: Arc<tracers::SpacedTracer>,

    status: Arc<status::RuntimeStatus>,
    tasks: Arc<RwLock<HashMap<String, CancellableTaskHandle>>>,
    accelerator_engine_registry: Arc<AcceleratorEngineRegistry>,
    token_provider_registry: Arc<TokenProviderRegistry>,

    schedulers: Arc<ScheduleRegistry>,

    /// When the runtime is part of a distributed cluster, this holds the node-specific information. It is `None` for stand-alone runtimes.
    distributed: Option<DistributedNode>,
    resource_monitor: resource_monitor::ResourceMonitor,

    config: Arc<Config>,

    /// Shared semaphore that bounds concurrent dataset schema inference
    /// (`read_provider`) calls so that startup loads and on-demand loads both
    /// honor `runtime.dataset_load_parallelism`.
    dataset_load_semaphore: Arc<tokio::sync::Semaphore>,

    /// Handle for resolving the spicepod `TelemetryConfig` for anonymous
    /// telemetry. For executors this is set after the app definition is
    /// fetched from the scheduler; for all other modes it is set before
    /// the runtime starts.
    telemetry_config: Option<Arc<tokio::sync::SetOnce<TelemetryConfig>>>,
}

impl Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Runtime {self:p}")
    }
}

impl Runtime {
    #[must_use]
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::new()
    }

    /// Returns a handle to the Tokio runtime that should be used to spawn IO tasks.
    #[must_use]
    pub fn tokio_io_runtime(&self) -> Handle {
        self.io_runtime.clone()
    }

    #[must_use]
    pub fn datafusion(&self) -> Arc<DataFusion> {
        Arc::clone(&self.df)
    }

    #[must_use]
    pub fn config(&self) -> Arc<Config> {
        Arc::clone(&self.config)
    }

    #[must_use]
    pub fn flight_write_rate_limit_enabled(&self) -> bool {
        self.rate_limits.flight_write_enabled()
    }

    #[must_use]
    pub fn secrets(&self) -> Arc<RwLock<secrets::Secrets>> {
        Arc::clone(&self.secrets)
    }

    #[must_use]
    pub fn secrets_weak(&self) -> Weak<RwLock<secrets::Secrets>> {
        Arc::downgrade(&self.secrets)
    }

    #[must_use]
    pub fn status(&self) -> Arc<status::RuntimeStatus> {
        Arc::clone(&self.status)
    }

    #[must_use]
    pub fn embeds(&self) -> Arc<RwLock<EmbeddingModelStore>> {
        Arc::clone(&self.embeds)
    }

    #[must_use]
    pub fn completion_llms(&self) -> Arc<RwLock<LLMChatCompletionsModelStore>> {
        Arc::clone(&self.completion_llms)
    }

    #[must_use]
    pub fn rerankers(&self) -> Arc<RwLock<RerankerModelStore>> {
        Arc::clone(&self.rerankers)
    }

    #[must_use]
    pub fn model_rate_controllers(
        &self,
    ) -> Arc<RwLock<HashMap<String, Arc<runtime_rate_control::RateController>>>> {
        Arc::clone(&self.model_rate_controllers)
    }

    #[must_use]
    pub fn http_rate_control_registry(
        &self,
    ) -> Arc<dataconnector::http_rate_control::HttpRateControlRegistry> {
        Arc::clone(&self.http_rate_control_registry)
    }

    #[must_use]
    pub fn app(&self) -> Arc<RwLock<Option<Arc<App>>>> {
        Arc::clone(&self.app)
    }

    pub async fn read_app(&self) -> Option<Arc<App>> {
        let guard = self.app.read().await;
        guard.clone()
    }

    #[must_use]
    pub fn tool_factories(&self) -> Arc<Mutex<HashMap<String, ToolFactory>>> {
        Arc::clone(&self.tool_factories)
    }

    #[must_use]
    pub fn accelerator_engine_registry(&self) -> Arc<AcceleratorEngineRegistry> {
        Arc::clone(&self.accelerator_engine_registry)
    }

    #[must_use]
    pub fn resource_monitor(&self) -> resource_monitor::ResourceMonitor {
        self.resource_monitor.clone()
    }

    #[must_use]
    pub fn token_provider_registry(&self) -> Arc<TokenProviderRegistry> {
        Arc::clone(&self.token_provider_registry)
    }

    #[must_use]
    pub fn schedulers(&self) -> Arc<ScheduleRegistry> {
        Arc::clone(&self.schedulers)
    }

    #[must_use]
    pub fn scheduler_peers(&self) -> Option<Arc<RwLock<SchedulerPeers>>> {
        match self.distributed.as_ref() {
            Some(DistributedNode::Scheduler { peers, .. }) => Some(Arc::clone(peers)),
            _ => None,
        }
    }

    #[must_use]
    pub fn partition_assignments(&self) -> Option<Arc<RwLock<PartitionAssignments>>> {
        match self.distributed.as_ref() {
            Some(DistributedNode::Executor {
                partition_assignments,
                ..
            }) => Some(Arc::clone(partition_assignments)),
            _ => None,
        }
    }

    /// Returns the executor outbound broadcaster used to send
    /// `PartitionsLoaded` and other unsolicited messages back to schedulers.
    /// Only available when this runtime is running as a cluster executor.
    #[must_use]
    pub fn executor_outbound_broadcaster(
        &self,
    ) -> Option<crate::cluster::ExecutorOutboundBroadcaster> {
        match self.distributed.as_ref() {
            Some(DistributedNode::Executor {
                outbound_broadcaster,
                ..
            }) => Some(outbound_broadcaster.clone()),
            _ => None,
        }
    }

    pub async fn set_partition_assignments(&self, assignments: PartitionAssignments) {
        if let Some(DistributedNode::Executor {
            partition_assignments,
            ..
        }) = self.distributed.as_ref()
        {
            let mut guard = partition_assignments.write().await;
            guard.clone_from(&assignments);
            drop(guard); // drop lock before updating tables

            self.record_executor_assigned_partitions(&assignments);

            // Update all assigned tables
            for table in assignments.keys() {
                if let Err(e) = self
                    .update_partition_refresh_sql(table.clone(), &assignments)
                    .await
                {
                    tracing::warn!("Failed to update partition refresh SQL for {table}: {e}");
                }
            }
        } else {
            tracing::warn!(
                "Attempted to set partition assignments on a non-executor node. Ignoring."
            );
        }
    }

    /// Emit `executor_assigned_partitions_count` for each assigned table.
    ///
    /// Uses `schema.table` for the `dataset` label so executor-side series
    /// line up with the scheduler-side partition metrics, which build their
    /// label from the user-declared dataset name (typically 2-part). The
    /// `node_id` label uses `metrics_node_id()` (host + bind port, scheme
    /// stripped) to match the executor identity registered with the scheduler.
    fn record_executor_assigned_partitions(&self, assignments: &PartitionAssignments) {
        let node_id = self.df.cluster_config.metrics_node_id();
        for (table, partitions) in assignments {
            let dataset = format!("{}.{}", table.schema, table.table);
            runtime_cluster::metrics::set_executor_assigned_partitions_count(
                &node_id,
                &dataset,
                partitions.len() as u64,
            );
        }
    }

    pub async fn update_partition_assignments(
        &self,
        new_partitions: HashMap<String, Vec<Vec<u8>>>,
        removed_partitions: HashMap<String, Vec<Vec<u8>>>,
    ) -> Result<()> {
        let Some(DistributedNode::Executor {
            partition_assignments,
            ..
        }) = self.distributed.as_ref()
        else {
            tracing::warn!(
                "Attempted to update partition assignments on a non-executor node. Ignoring."
            );
            // Not an executor — there's nothing for us to apply. Report success
            // so the scheduler doesn't retry; the routing layer is what'd be
            // misconfigured here.
            return Ok(());
        };

        // Compute the prospective post-update state from a snapshot of the
        // current map. We apply the DataFusion filter updates against this
        // *before* committing to shared state, so a per-table failure leaves
        // the routing map unchanged and the scheduler's retry sees a
        // consistent starting point on the next attempt.
        let mut prospective: PartitionAssignments = partition_assignments.read().await.clone();

        for (table_name, partitions) in &removed_partitions {
            let table_ref = TableReference::parse_str(table_name)
                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
            if let Some(current_partitions) = prospective.get_mut(&table_ref) {
                for partition_bytes in partitions {
                    let partition_expr =
                        Expr::from_bytes_with_registry(partition_bytes, self.df.ctx.as_ref())
                            .map_err(|source| {
                                Error::UnableToDeserializeClusterPartitionExpression {
                                    table: table_name.clone(),
                                    source: Box::new(source),
                                }
                            })?;
                    current_partitions.retain(|p| p != &partition_expr);
                }
            }
        }

        for (table_name, partitions) in &new_partitions {
            let table_ref = TableReference::parse_str(table_name)
                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
            let current_partitions = prospective.entry(table_ref.clone()).or_default();
            for partition_bytes in partitions {
                let partition_expr = ::datafusion_expr::Expr::from_bytes_with_registry(
                    partition_bytes,
                    self.df.ctx.as_ref(),
                )
                .map_err(|source| {
                    Error::UnableToDeserializeClusterPartitionExpression {
                        table: table_name.clone(),
                        source: Box::new(source),
                    }
                })?;
                if !current_partitions.contains(&partition_expr) {
                    current_partitions.push(partition_expr);
                }
            }
        }

        let affected_tables: HashSet<_> = new_partitions
            .keys()
            .chain(removed_partitions.keys())
            .collect();

        // Apply DataFusion filter updates against the prospective state. If any
        // fails, the shared partition_assignments map is not modified — the
        // scheduler retries the update and we'll re-attempt all tables.
        for table_name in affected_tables {
            let resolved = TableReference::parse_str(table_name)
                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);

            self.update_partition_refresh_sql(resolved.clone(), &prospective)
                .await?;
        }

        self.record_executor_assigned_partitions(&prospective);

        // All filter updates succeeded — commit the new routing state. Last
        // step so the planner's view of executor partitions never gets ahead
        // of what the AcceleratedTables actually know about.
        *partition_assignments.write().await = prospective;
        Ok(())
    }

    pub(crate) async fn update_partition_refresh_sql(
        &self,
        table: ResolvedTableReference,
        assignments: &PartitionAssignments,
    ) -> Result<()> {
        let partition_filters =
            crate::cluster::partition::get_partition_filter_exprs(&table, assignments);

        let table_ref = TableReference::full(
            Arc::<str>::clone(&table.catalog),
            Arc::<str>::clone(&table.schema),
            Arc::<str>::clone(&table.table),
        );
        // Propagate the filter-update error so the caller (and the executor's
        // ack to the scheduler) sees the failure rather than just logging it.
        self.datafusion()
            .update_partition_filters(table_ref.clone(), partition_filters)
            .await
            .map_err(|source| Error::UnableToUpdateClusterPartitionFilters {
                table: table.to_string(),
                source: Box::new(source),
            })?;

        tracing::info!("Updated partition assignments for {table}");

        // Trigger a refresh to load the data for the new partitions, capturing
        // the completion notifier so we can ack the scheduler with a
        // `PartitionsLoaded` once the accelerated table has actually finished
        // loading. Refresh failures are non-fatal — the assignment is still
        // valid; data just hasn't been pulled yet, so we skip the ack.
        let notifier = match self.datafusion().refresh_table(&table_ref, None).await {
            Ok(notifier) => notifier,
            Err(e) => {
                tracing::warn!(
                    "Failed to trigger refresh for {table} after updating partitions: {e}"
                );
                return Ok(());
            }
        };

        // Snapshot the partition exprs assigned to us for this table, encoded
        // the same way the scheduler encodes them when sending UpdatePartitions
        // (`Expr::to_bytes()`). `partition_value_to_bytes` sorts entries by
        // key so the encoding is deterministic across re-serialization.
        let table_str = table.to_string();
        let partition_expr_bytes: Vec<Vec<u8>> = assignments
            .get(&table)
            .map(|exprs| runtime_cluster::encode_partition_exprs(exprs, &table_str))
            .unwrap_or_default();

        if let Some(broadcaster) = self.executor_outbound_broadcaster() {
            // Send the ack even when `partition_expr_bytes` is empty — a
            // legitimately empty assignment (e.g. zero-partition source, or
            // partitions that all failed to serialize) still needs to flip
            // the scheduler-side readiness gate via the
            // `is_table_loaded`/`updated_at` shortcut. Suppressing the empty
            // case here would leave the dataset stuck in `Refreshing`.
            let table_name = table.to_string();
            tokio::spawn(async move {
                if let Some(n) = notifier {
                    n.notified().await;
                }
                // Statistics flow via the periodic ExecutorStatistics reporter, not
                // this readiness ack.
                let sent = broadcaster
                    .broadcast_partitions_loaded(table_name.clone(), partition_expr_bytes)
                    .await;
                tracing::debug!(
                    "Broadcast PartitionsLoaded for {table_name} to {sent} scheduler(s)"
                );
            });
        }

        Ok(())
    }

    /// Periodically drives moka housekeeping so invalidation predicates and
    /// expired entries are reclaimed even on caches with no `get`/`insert`
    /// traffic. Returns immediately when no cache is configured; otherwise loops
    /// until the task is cancelled at shutdown.
    pub(crate) async fn run_cache_maintenance(self: Arc<Self>) -> Result<()> {
        let caching = self.datafusion().caching();
        if caching.results.is_none()
            && caching.plans.is_none()
            && caching.search.is_none()
            && caching.embeddings.is_none()
        {
            return Ok(());
        }

        let mut interval = tokio::time::interval(CACHE_MAINTENANCE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            caching.run_pending_maintenance().await;
        }
    }

    /// Periodically recompute and rebroadcast this executor's per-table row-count
    /// statistics to all schedulers. `PartitionsLoaded` is otherwise only sent on
    /// initial load / assignment change, so during streaming ETL the coordinator's
    /// in-memory stats would reflect only the first snapshot (or nothing if the
    /// table had no data at initial-load time). A periodic rebroadcast keeps the
    /// coordinator's join-sizing statistics fresh as the executor's local data grows.
    pub(crate) async fn run_executor_statistics_reporter(self: Arc<Self>) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(45));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The statistics source (`local_executor_table_statistics`) reads the
        // Cayenne metastore aggregate, which is maintained incrementally on the
        // write path: always-fresh, O(1), and never degrades to a row-count-only
        // result mid-ingest. So there is no non-degrading cache here — each tick
        // simply broadcasts the current aggregate.
        loop {
            interval.tick().await;
            let Some(broadcaster) = self.executor_outbound_broadcaster() else {
                continue;
            };
            let df = self.datafusion();

            // Enumerate the tables this executor serves locally. The q18 tables
            // are cayenne *catalog* tables (not spice.public datasets), so the
            // dataset partition-assignment map doesn't cover them; enumerate the
            // cayenne catalog directly. Also include any dataset assignments.
            let mut tables: Vec<TableReference> = Vec::new();
            #[cfg(not(windows))]
            {
                tables.extend(crate::cluster::discover_cayenne_tables(&df).await);
            }
            if let Some(assignments_lock) = self.partition_assignments() {
                for resolved in assignments_lock.read().await.keys() {
                    tables.push(TableReference::full(
                        Arc::<str>::clone(&resolved.catalog),
                        Arc::<str>::clone(&resolved.schema),
                        Arc::<str>::clone(&resolved.table),
                    ));
                }
            }
            tables.sort_by_key(ToString::to_string);
            tables.dedup_by_key(|t| t.to_string());
            if tables.is_empty() {
                continue;
            }
            tracing::debug!(
                count = tables.len(),
                "Reporting per-executor table statistics to schedulers"
            );
            for table in tables {
                let table_key = table.to_string();
                if let Some((stats, column_names)) =
                    crate::cluster::partition::local_executor_table_statistics(&df, &table).await
                {
                    let encoded = runtime_cluster::encode_statistics(&stats);
                    broadcaster
                        .broadcast_executor_statistics(table_key, encoded, column_names)
                        .await;
                }
            }
        }
    }

    /// Returns the partition store for accelerated table partition metadata (scheduler only).
    #[must_use]
    pub fn partition_store(&self) -> Option<Arc<PartitionStore>> {
        match self.distributed.as_ref() {
            Some(DistributedNode::Scheduler {
                accelerations_partitions_store,
                ..
            }) => Some(Arc::clone(accelerations_partitions_store)),
            _ => None,
        }
    }

    /// Returns the partition load tracker used to aggregate executor
    /// `PartitionsLoaded` acks (scheduler only).
    #[must_use]
    pub fn partition_load_tracker(&self) -> Option<Arc<runtime_cluster::PartitionLoadTracker>> {
        match self.distributed.as_ref() {
            Some(DistributedNode::Scheduler {
                partition_load_tracker,
                ..
            }) => Some(Arc::clone(partition_load_tracker)),
            _ => None,
        }
    }

    /// Returns the cluster state store (scheduler only).
    #[must_use]
    pub fn cluster_state(&self) -> Option<Arc<ClusterStateStore>> {
        match self.distributed.as_ref() {
            Some(DistributedNode::Scheduler { cluster_state, .. }) => {
                Some(Arc::clone(cluster_state))
            }
            _ => None,
        }
    }

    /// Returns the scheduler heartbeat store (scheduler only).
    #[must_use]
    pub fn scheduler_heartbeats(&self) -> Option<Arc<SchedulerHeartbeatStore>> {
        match self.distributed.as_ref() {
            Some(DistributedNode::Scheduler { heartbeats, .. }) => Some(Arc::clone(heartbeats)),
            _ => None,
        }
    }

    /// Returns the metrics reader for on-demand OTLP metrics collection.
    ///
    /// This is used in cluster mode by:
    /// - `GetMetrics` RPC to return local metrics to peer schedulers
    /// - Executors responding to metrics requests from schedulers via control stream
    #[must_use]
    pub fn metrics_reader(&self) -> Option<&metrics_reader::MetricsReader> {
        self.metrics_reader.as_ref()
    }

    /// Returns the job executor for async SQL queries if available (cluster mode only).
    ///
    /// This uses `try_read()` to avoid blocking the caller. If another thread holds
    /// a write lock (e.g., during initialization), this returns `None`. The caller
    /// should be aware that a `None` result could mean either:
    /// 1. Async jobs are not enabled (not in cluster mode), or
    /// 2. The executor is being initialized (rare, transient condition)
    #[must_use]
    pub fn job_executor(&self) -> Option<Arc<jobs::JobExecutor>> {
        match self.distributed.as_ref() {
            Some(DistributedNode::Scheduler { job_executor, .. }) => {
                if let Ok(guard) = job_executor.try_read() {
                    guard.clone()
                } else {
                    tracing::debug!(
                        "Job executor is currently being initialized. Returning None. This is a transient condition during startup."
                    );
                    None
                }
            }
            None | Some(DistributedNode::Executor { .. }) => None,
        }
    }

    /// Sets the job executor for async SQL queries.
    pub async fn set_job_executor(&self, executor: Arc<jobs::JobExecutor>) {
        match self.distributed.as_ref() {
            Some(DistributedNode::Scheduler { job_executor, .. }) => {
                let mut guard = job_executor.write().await;
                *guard = Some(executor);
            }
            Some(DistributedNode::Executor { .. }) => {
                tracing::warn!(
                    "Attempted to set job executor on an executor node. This should only be set on the scheduler. Ignoring."
                );
            }
            None => {
                tracing::warn!(
                    "Attempted to set job executor on a non-cluster runtime. This should only be set in cluster mode on the scheduler node. Ignoring."
                );
            }
        }
    }

    #[must_use]
    pub fn datasets_health_monitor(&self) -> Option<Arc<DatasetsHealthMonitor>> {
        self.datasets_health_monitor.clone()
    }

    /// Initialize cache metrics after OpenTelemetry meter provider is set up.
    /// Must be called after `init_metrics` in spiced to ensure metrics are registered.
    pub fn init_cache_metrics(&self) {
        use cache::metrics::CacheMetrics;
        use cache::result::{
            embeddings::CachedEmbeddingResult, query::CachedQueryResult, search::CachedSearchResult,
        };

        let caching = self.datafusion().caching();
        if caching.results.is_some() {
            CachedQueryResult::init();
        }
        if caching.search.is_some() {
            CachedSearchResult::init();
        }
        if caching.embeddings.is_some() {
            CachedEmbeddingResult::init();
        }
    }

    /// Requests a loaded extension, or will attempt to load it if part of the autoloaded extensions.
    pub async fn extension(self: Arc<Self>, name: &str) -> Option<Arc<dyn Extension>> {
        let extensions = self.extensions.read().await;

        if let Some(extension) = extensions.get(name) {
            return Some(Arc::clone(extension));
        }
        drop(extensions);

        if let Some(autoload_factory) = self.autoload_extensions.get(name) {
            let mut extensions = self.extensions.write().await;
            let mut extension = autoload_factory.create();
            let extension_name = extension.name().to_string();
            if let Err(err) = extension.initialize(self.as_ref()).await {
                tracing::error!("Unable to initialize extension {extension_name}: {err}");
                return None;
            }

            if let Err(err) = extension.on_start(Arc::clone(&self)).await {
                tracing::error!("Unable to start extension {extension_name}: {err}");
                return None;
            }

            extensions.insert(extension_name.clone(), extension.into());
            return extensions.get(&extension_name).cloned();
        }

        None
    }

    /// Starts the HTTP, Flight, OpenTelemetry and Metrics servers all listening on the ports specified in the given `Config`.
    ///
    /// The future returned by this function drives the individual server futures and will only return once the servers are shutdown.
    ///
    /// It is recommended to start the servers in parallel to loading the Runtime components to speed up startup.
    pub async fn start_servers(
        self: Arc<Self>,
        config: Config,
        tls_config: Option<Arc<TlsConfig>>,
        endpoint_auth: EndpointAuth,
    ) -> Result<()> {
        Arc::clone(&self)
            .register_metrics_table(self.prometheus_registry.is_some())
            .await?;

        // Shutdown signal
        let shutdown_signal_future = async {
            let graceful_shutdown = async {
                shutdown_signal().await;
                tracing::debug!("Shutdown signal received. Press Ctrl-C again to force exit.");
                self.shutdown().await;
                Ok(())
            };
            tokio::select! {
                result = graceful_shutdown => result,
                () = force_shutdown_signal() => {
                    tracing::info!("Force shutdown signal received. Terminating immediately.");
                    // return error to force stop waiting for other tasks and terminate immediately
                    Err(Error::ForceTerminated)
                }
            }
        };

        // - Scheduler: does some init, starts internal cluster gRPC server on separate port
        // - Executor: does some init, but has a polling loop to fetch work from scheduler
        #[expect(
            clippy::items_after_statements,
            reason = "type alias scoped to cluster setup"
        )]
        type BoxedClusterFuture = std::pin::Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>;

        // For distributed cluster mode, start the appropriate additional cluster components.
        // For scheduler, this includes cluster-wide metrics collection.
        let (maybe_cluster_future, cluster_collector): (
            Option<BoxedClusterFuture>,
            Option<Arc<metrics_server::cluster::ClusterMetricsCollector>>,
        ) = match self.distributed.as_ref() {
            Some(DistributedNode::Scheduler {
                executor_registry,
                peers,
                ..
            }) => {
                let fut = cluster::initialize_cluster_scheduler_future(
                    &self,
                    Arc::clone(executor_registry),
                    Arc::clone(peers),
                )
                .await?;

                // Create local metrics collector closure that uses MetricsReader
                let metrics_reader_for_collector = self.metrics_reader.clone();
                let local_metrics_collector: Arc<dyn Fn() -> Vec<u8> + Send + Sync> =
                    Arc::new(move || {
                        metrics_reader_for_collector
                            .as_ref()
                            .map(metrics_reader::MetricsReader::collect_otlp)
                            .unwrap_or_default()
                    });
                (
                    fut,
                    Some(Arc::new(
                        metrics_server::cluster::ClusterMetricsCollector::new(
                            Arc::clone(peers),
                            Arc::clone(executor_registry),
                            self.df.cluster_config.client_tls_config(),
                            self.df.cluster_config.node_id(),
                            local_metrics_collector,
                        ),
                    )),
                )
            }
            Some(DistributedNode::Executor { .. }) => {
                let executor_shutdown = CancellationToken::new();
                let executor_fut = cluster::initialize_cluster_executor(
                    Arc::clone(&self),
                    executor_shutdown.clone(),
                )
                .await?;
                let self_ref = Arc::clone(&self);
                (
                    Some(Box::pin(
                        self_ref
                            .start_runtime_task(
                                CLUSTER_EXECUTOR,
                                Some(executor_shutdown),
                                executor_fut,
                            )
                            .await,
                    )),
                    None,
                )
            }
            None => (None, None),
        };

        // Start Flight server
        // On executors, periodically rebroadcast per-table row-count statistics so
        // the coordinator's join-sizing stats stay fresh as streaming ETL grows
        // local data (PartitionsLoaded is otherwise sent only on initial load /
        // assignment change).
        if self.df.cluster_config.effective_role() == Some(ClusterRole::Executor) {
            let reporter_self = Arc::clone(&self);
            tokio::spawn(async move {
                reporter_self.run_executor_statistics_reporter().await;
            });
        }

        let flight_shutdown = CancellationToken::new();
        let self_ref = Arc::clone(&self);
        let cloned_tls_config = tls_config.clone();
        let executor_endpoint_auth = endpoint_auth.clone();
        let flight_future: std::pin::Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> =
            if self.df.cluster_config.effective_role() == Some(ClusterRole::Executor) {
                Box::pin(
                    self.start_runtime_task(
                        FLIGHT_SERVER,
                        Some(flight_shutdown.clone()),
                        async move {
                            cluster::start_executor_flight_server(
                                config.flight_bind_address,
                                Arc::clone(&self_ref),
                                executor_endpoint_auth,
                                Some(flight_shutdown),
                            )
                            .await
                            .context(UnableToStartFlightServerSnafu)
                        },
                    )
                    .await,
                )
            } else {
                let cloned_endpoint_auth = endpoint_auth.clone();
                let cloned_app_ref = self.read_app().await;

                Box::pin(
                    self.start_runtime_task(
                        FLIGHT_SERVER,
                        Some(flight_shutdown.clone()),
                        async move {
                            flight::start(
                                config.flight_bind_address,
                                cloned_app_ref,
                                Arc::clone(&self_ref),
                                cloned_tls_config,
                                cloned_endpoint_auth,
                                Arc::clone(&self_ref.rate_limits),
                                Some(flight_shutdown),
                            )
                            .await
                            .context(UnableToStartFlightServerSnafu)
                        },
                    )
                    .await,
                )
            };

        // If this is an executor, we only need the shutdown signal, flight server, and health endpoint.
        // Early exit to avoid starting unneeded servers: http server, metrics server, pods watcher, etc.
        if matches!(
            self.df.cluster_config.effective_role(),
            Some(ClusterRole::Executor)
        ) {
            let Some(executor_future) = maybe_cluster_future else {
                return Err(FailedToStartClusterExecutor {
                    source: "Executor work loop not bound. Report this bug on GitHub: https://github.com/spiceai/spiceai/issues"
                        .to_string()
                        .into(),
                });
            };

            // Start health-only HTTP server for executor
            let http_shutdown = CancellationToken::new();
            let health_http_future = self
                .start_runtime_task(
                    HTTP_SERVER,
                    Some(http_shutdown.clone()),
                    http::start_health_only(config.http_bind_address, Some(http_shutdown))
                        .map_err(Error::from),
                )
                .await;

            return tokio::try_join!(
                shutdown_signal_future,
                executor_future,
                flight_future,
                health_http_future,
            )
            .map(|_| ());
        }

        // Start Http server
        let cloned_tls_config = tls_config.clone();
        let cloned_config = config.clone();
        let auth = endpoint_auth.http_auth.clone();
        let identity_source = endpoint_auth.identity_source;
        let self_ref = Arc::clone(&self);
        let http_shutdown = CancellationToken::new();

        let http_future = self
            .start_runtime_task(
                HTTP_SERVER,
                Some(http_shutdown.clone()),
                http::start(
                    cloned_config.http_bind_address,
                    self_ref,
                    cloned_config.into(),
                    cloned_tls_config,
                    auth,
                    identity_source,
                    Some(http_shutdown),
                )
                .map_err(Error::from),
            )
            .await;

        // Start Metrics server
        let metrics_endpoint = self.metrics_endpoint;
        let prometheus_registry = self.prometheus_registry.clone();
        let cloned_tls_config = tls_config.clone();
        let metrics_rate_limiter =
            Arc::new(RateLimiter::direct(self.rate_limits.metrics_endpoint_limit));

        let metrics_future = self
            .start_runtime_task(METRICS_SERVER, None, async move {
                metrics_server::start(
                    metrics_endpoint,
                    prometheus_registry,
                    cloned_tls_config,
                    cluster_collector,
                    Some(metrics_rate_limiter),
                )
                .await
                .context(UnableToStartMetricsServerSnafu)
            })
            .await;

        if let Some(tls_config) = tls_config {
            match tls_config.subject_name() {
                Some(subject_name) => {
                    tracing::info!("Endpoints secured with TLS using certificate: {subject_name}");
                }
                None => {
                    tracing::info!("Endpoints secured with TLS");
                }
            }
        }

        // Start Spicepod watcher
        let self_ref = Arc::clone(&self);
        let pods_watcher_future = self
            .start_runtime_task(PODS_WATCHER, None, async move {
                self_ref
                    .start_pods_watcher()
                    .await
                    .context(UnableToInitializePodsWatcherSnafu)
            })
            .await;

        // `None` cancellation token: the loop is aborted at shutdown.
        let maintenance_self = Arc::clone(&self);
        let cache_maintenance_future = self
            .start_runtime_task(
                CACHE_MAINTENANCE,
                None,
                maintenance_self.run_cache_maintenance(),
            )
            .await;

        // wait for all servers to shut down or if any of the servers fail to start
        if let Some(cluster_future) = maybe_cluster_future {
            return match tokio::try_join!(
                http_future,
                flight_future,
                metrics_future,
                pods_watcher_future,
                cache_maintenance_future,
                cluster_future,
                shutdown_signal_future
            ) {
                Err(err) => Err(err),
                _ => Ok(()),
            };
        }

        match tokio::try_join!(
            http_future,
            flight_future,
            metrics_future,
            pods_watcher_future,
            cache_maintenance_future,
            shutdown_signal_future
        ) {
            Err(err) => Err(err),
            _ => Ok(()),
        }
    }

    /// Updates all of the component statuses to `Initializing`.
    pub async fn set_components_initializing(self: Arc<Self>) {
        let Some(app) = self.read_app().await else {
            return;
        };

        let valid_datasets = Arc::clone(&self).get_valid_datasets(&app, LogErrors(false));
        for ds in &valid_datasets {
            self.status
                .update_dataset(&ds.name, ComponentStatus::Initializing);
        }

        if cfg!(feature = "models") {
            for embedding in &app.embeddings {
                self.status
                    .update_embedding(&embedding.name, ComponentStatus::Initializing);
            }

            for reranker in &app.rerankers {
                self.status
                    .update_reranker(&reranker.name, ComponentStatus::Initializing);
            }

            for model in &app.models {
                self.status
                    .update_model(&model.name, ComponentStatus::Initializing);
            }

            for tool in &app.tools {
                self.status
                    .update_tool(&tool.name, ComponentStatus::Initializing);
            }

            for catalog_name in default_catalog_names() {
                self.status
                    .update_tool_catalog(catalog_name, ComponentStatus::Initializing);
            }

            for model in &app.models {
                self.status
                    .update_model(&model.name, ComponentStatus::Initializing);
            }
        }

        let valid_catalogs = Arc::clone(&self).get_valid_catalogs(&app, LogErrors(false));
        for catalog in valid_catalogs {
            self.status
                .update_catalog(&catalog.name, ComponentStatus::Initializing);
        }

        let valid_views = Arc::clone(&self).get_valid_views(&app, LogErrors(false));
        for validated_view in valid_views {
            self.status
                .update_view(&validated_view.view.name, ComponentStatus::Initializing);
        }
    }

    /// Will load all of the components of the Runtime, including `secret_stores`, `catalogs`, `datasets`, `models`, and `embeddings`.
    ///
    /// The future returned by this function will not resolve until all components have been loaded and marked as ready.
    /// This includes waiting for the first refresh of any accelerated tables to complete.
    pub async fn load_components(self: Arc<Self>) {
        Arc::clone(&self).set_components_initializing().await;

        Arc::clone(&self).start_extensions().await;

        // Must be loaded before datasets
        self.load_embeddings().await;
        self.load_rerankers().await;

        // Spawn each component load in its own task to run in parallel
        let task_history = tokio::spawn({
            let self_clone = Arc::clone(&self);
            async move {
                if let Err(err) = self_clone.init_task_history().await {
                    tracing::warn!("Creating internal task history table: {err}");
                }
            }
        });

        let datasets = tokio::spawn({
            let self_clone = Arc::clone(&self);
            async move {
                self_clone.load_datasets().await;
            }
        });

        let catalogs = tokio::spawn({
            let self_clone = Arc::clone(&self);
            async move {
                self_clone.load_catalogs().await;
            }
        });

        let models = tokio::spawn({
            let self_clone = Arc::clone(&self);

            // This cannot be done earlier since we must have a `Arc<Runtime>` to provide to factories.
            tools::factory::register_all_factories(Arc::clone(&self_clone)).await;

            async move {
                Arc::clone(&self_clone).load_models().await;

                #[cfg(feature = "models")]
                {
                    Arc::clone(&self_clone).load_workers().await;
                }
            }
        });

        if let Some(cfg) = self
            .app
            .read()
            .await
            .as_ref()
            .and_then(|app| app.management.as_ref())
            && let Err(err) = management::init_management(Arc::clone(&self), cfg).await
        {
            tracing::error!("Failed to initialize management of the Spice runtime: {err}");
        }

        let ctx = &self.datafusion().ctx;
        ctx.register_udtf(
            udtfs::LIST_UDFS_UDTF_NAME,
            Arc::new(ListUDFTableFunc::new(Arc::clone(ctx))),
        );

        let components = vec![task_history, datasets, catalogs, models];

        // Signal that the load must be canceled if the runtime is shut down before the components are loaded
        let cancel_loading = CancellationToken::new();

        // Wait for all components to load returning the first error
        // or canceling spawned tokio tasks if the runtime is shutting down
        let load_result = self
            .start_runtime_task(
                COMPONENTS_INITIAL_LOAD,
                Some(cancel_loading.clone()),
                async move {
                    let abort_handlers = components
                        .iter()
                        .map(JoinHandle::abort_handle)
                        .collect::<Vec<_>>();

                    tokio::select! {
                        load_result = try_join_all(components) => {
                            load_result.map(|_| ()).context(ComponentsInitializationFailedSnafu)
                        }
                        () = cancel_loading.cancelled() => {
                            for handle in abort_handlers {
                                handle.abort();
                            }
                            ComponentsInitializationCancelledSnafu.fail()
                        }
                    }
                },
            )
            .await;

        if let Err(err) = load_result.await {
            if !matches!(err, Error::ComponentsInitializationCancelled) {
                tracing::error!("Could not start the Spice runtime: {err}");
            }
        } else {
            // Create a background task to report once all components are marked as `Ready`
            let status = self.status();
            tokio::spawn({
                async move {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                        if status.is_shutdown() {
                            break;
                        }
                        if status.is_ready() {
                            if let Some(app) = self.read_app().await {
                                let valid_datasets =
                                    Arc::clone(&self).get_valid_datasets(&app, LogErrors(false));
                                let valid_catalogs =
                                    Arc::clone(&self).get_valid_catalogs(&app, LogErrors(false));
                                if valid_datasets.is_empty() && valid_catalogs.is_empty() {
                                    tracing::info!(
                                        "No datasets or catalogs were configured. If this is unexpected, check the Spicepod configuration."
                                    );
                                }
                            }
                            tracing::info!("All components are loaded. Spice runtime is ready!");
                            break;
                        }
                    }
                }
            });
        }
    }

    // Closes and deallocates all resources (including the static registries)
    pub async fn shutdown(&self) {
        if self.status.is_shutdown() {
            return;
        }

        self.status.mark_shutdown();

        // Tell CDC sources to release their upstream resources NOW, before
        // the connection-drain phase below: a Postgres replication connection
        // holds a single-consumer slot, and releasing it at shutdown start
        // (instead of at process exit) lets a replacement instance attach
        // during a rolling deploy instead of retrying against "slot is
        // active".
        data_components::cdc::begin_shutdown();

        let shutdown_timeout: Duration = self.read_app().await.and_then(|app| {
            app.runtime.shutdown_timeout().unwrap_or_else(|err| {
                tracing::warn!("Invalid shutdown timeout: {err}. Using default: {RUNTIME_DEFAULT_SHUTDOWN_TIMEOUT:?}");
                Some(RUNTIME_DEFAULT_SHUTDOWN_TIMEOUT)
            })
        }).unwrap_or(RUNTIME_DEFAULT_SHUTDOWN_TIMEOUT);
        tracing::info!(
            "Shutdown initiated; waiting up to {shutdown_timeout:?} for connections to drain"
        );

        let start_time = Instant::now();

        // Shutdown running components in phases so request-serving tasks drain
        // before query execution resources are cleaned up.
        let mut runtime_tasks = self.tasks.write().await;

        // Query-serving tasks, including HTTP and Flight, must drain before
        // DataFusion cleanup so in-flight queries still have access to their
        // execution resources during graceful shutdown. Metrics can stay up
        // until the end for health and observability during shutdown.
        let mut first_shutdown_group = Vec::new();
        let mut last_shutdown_group = Vec::new();

        for (name, handle) in runtime_tasks.drain() {
            match name.as_str() {
                METRICS_SERVER => last_shutdown_group.push((name, handle)),
                _ => first_shutdown_group.push((name, handle)),
            }
        }

        let shutdown_futures: Vec<_> = first_shutdown_group
            .into_iter()
            .filter_map(|(name, handle)| {
                if handle.is_finished() {
                    None
                } else {
                    tracing::debug!("Shutting down {name}");
                    Some(handle.cancel(shutdown_timeout))
                }
            })
            .collect();

        join_all(shutdown_futures).await;

        // Clean up DataFusion first as there could be datasets loading and accessing registries below.
        self.df.shutdown().await;
        dataconnector::unregister_all().await;
        catalogconnector::unregister_all().await;
        self.accelerator_engine_registry.unregister_all().await;
        tools::factory::unregister_all_factories(self).await;

        document_parse::unregister_all().await;

        // Measure elapsed time since shutdown started and calculate remaining time within the configured timeout. Remaining shutdown
        // group includes only Metrics endpoints.
        let elapsed = start_time.elapsed();
        let remaining_timeout = shutdown_timeout.saturating_sub(elapsed);

        // Shutdown Metrics server last
        let shutdown_futures: Vec<_> = last_shutdown_group
            .into_iter()
            .map(|(name, handle)| {
                tracing::debug!("Shutting down {name}");
                handle.cancel(remaining_timeout)
            })
            .collect();

        join_all(shutdown_futures).await;

        tracing::debug!("Shutdown completed");
    }

    /// Spawns and registers a runtime task with optional cancellation support.
    pub(crate) async fn start_runtime_task<F>(
        self: &Arc<Self>,
        component_name: &str,
        cancellation_token: Option<CancellationToken>,
        task_fn: F,
    ) -> impl Future<Output = Result<(), Error>> + use<F>
    where
        F: Future<Output = Result<(), Error>> + Send + 'static,
    {
        let (future, handle) = spawn_cancellable_task(cancellation_token, task_fn, |err| {
            Error::FailedToExecuteTask { source: err }
        });

        self.tasks
            .write()
            .await
            .insert(component_name.to_string(), handle);

        future
    }

    /// List all tools available in the runtime, either within a catalog or standalone.
    ///
    /// Tools from default catalogs are also loaded individually, so the default catalogs must be ignored.
    ///
    /// For tools from catalog, the name is prefixed with the catalog name. e.g. `catalog_name/tool_name`.
    fn list_all_tools(self: &Arc<Self>) -> impl Stream<Item = Arc<dyn SpiceModelTool>> {
        let default_catalogs = default_available_catalogs(Arc::clone(self));
        let stream_self = Arc::clone(self);
        stream! {
            let tool_lock = stream_self.tools.read().await;
            let default_catalog_names = default_catalogs
                .iter()
                .map(|c| c.name())
                .collect::<HashSet<_>>();
            for (name, tooling) in tool_lock.iter() {
                match tooling {
                    Tooling::Tool(tool) | Tooling::FunctionTool(tool) => {
                        yield Arc::clone(tool);
                    }
                    Tooling::Catalog(catalog) => {
                        // Do not list tools from default catalogs. They are already listed individually as tools.
                        if default_catalog_names.contains(&name.as_str()) {
                            continue;
                        }
                        let all = catalog.all().await;
                        for tool in all {
                            yield with_name(&tool, format!("{}/{}", catalog.name(), tool.name()).as_str());
                        }
                    }
                }
            }
        }
    }

    pub async fn get_tool(self: &Arc<Self>, tool_name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        let tools = self.tools.read().await;
        let tool: Arc<dyn SpiceModelTool> =
            if let Some((catalog_name, name)) = tool_name.split_once('/') {
                let Some(Tooling::Catalog(catalog)) = tools.get(catalog_name) else {
                    return None;
                };
                return catalog.get(name).await;
            } else {
                let Some(Tooling::Tool(tool) | Tooling::FunctionTool(tool)) = tools.get(tool_name)
                else {
                    return None;
                };
                Arc::clone(tool)
            };
        Some(tool)
    }
}

#[must_use]
pub fn spice_data_base_path() -> String {
    let Ok(working_dir) = std::env::current_dir() else {
        return ".".to_string();
    };

    let base_folder = working_dir.join(".spice/data");
    base_folder.to_str().unwrap_or(".").to_string()
}

#[cfg(any(feature = "duckdb", feature = "sqlite", feature = "turso"))]
#[expect(clippy::result_large_err)]
pub(crate) fn make_spice_data_directory() -> Result<()> {
    make_spice_data_sub_directory(&[])?;
    Ok(())
}

#[expect(clippy::result_large_err)]
pub(crate) fn make_spice_data_sub_directory(directory: &[String]) -> Result<PathBuf> {
    let mut base_folder = PathBuf::from(spice_data_base_path());
    base_folder.extend(directory);
    std::fs::create_dir_all(base_folder.clone()).context(UnableToCreateDirectorySnafu)?;
    Ok(base_folder)
}

impl From<http::Error> for Error {
    fn from(err: http::Error) -> Self {
        Error::UnableToStartHttpServer { source: err }
    }
}
