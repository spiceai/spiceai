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

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec, Parameters, parameters::aws::initiate_config_with_auth_method,
};
use crate::accelerated_table::sink::table::TableSink;
use crate::component::dataset::Dataset;
use crate::component::dataset::acceleration::RefreshMode;
use crate::dataaccelerator::spice_sys::OpenOption;
use crate::dataaccelerator::spice_sys::dynamodb::{DynamoDBCheckpointMetadata, DynamoDBSys};
use crate::dataconnector::schema_projection::{ProjectionPolicy, parse_schema_projection};
use crate::federated_table::FederatedTable;
use async_trait::async_trait;
use data_components::cdc::{
    ChangeEnvelope, ChangesStream, CommitChange, CommitError, InitialSnapshotMode,
    InvalidCheckpointBehavior, StreamError,
};
use data_components::dynamodb::Error;
use data_components::dynamodb::provider::DynamoDBTableProvider;
use data_components::dynamodb::stream::StreamError as DynamoDBStreamError;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use dynamodb_streams::{Checkpoint, Metrics, MetricsCollector};
use futures::stream::{self, StreamExt};
use opentelemetry::KeyValue;
use runtime_api_types::v1::ComponentType;
use runtime_metrics::component::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use runtime_parameters::ExposedParamLookup;
use snafu::ResultExt;
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use std::{any::Any, future::Future, pin::Pin, sync::Arc};
use tokio::sync::Mutex;
use util::time_format::is_valid_format;

// If we get `ShardNotFound` or `StreamBeyondRetention` on startup and checkpoint is old enough,
// behavior will depend on lag_exceeds_shard_retention_behavior param.
// DynamoDB retention is 24h, and shards expire every 4h. 2h are added for safety.
const CHECKPOINT_EXPIRATION_HOURS: u64 = 18;

#[derive(Debug)]
pub struct DynamoDB {
    params: Parameters,
    metrics_collector: Arc<MetricsCollector>,
}

#[derive(Default, Debug, Copy, Clone)]
pub struct DynamoDBFactory {}

impl DynamoDBFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const DEFAULT_SCHEMA_INFER_MAX_RECORDS_STR: &str = "10";
const SEGMENTS_AUTO_STR: &str = "auto";
const DEFAULT_TIME_FORMAT: &str = "2006-01-02T15:04:05.000Z07:00";

/// Resolve the initial-snapshot mode from `dynamodb_replication_initial_snapshot`
/// (`auto|always|disabled`). An unrecognized value warns and falls back to
/// `auto` (the `one_of` on the [`ParameterSpec`] already rejects bad values in
/// production; this is a defensive default).
fn parse_snapshot_mode(params: &Parameters, dataset_name: &TableReference) -> InitialSnapshotMode {
    match params.get("replication_initial_snapshot").expose() {
        ExposedParamLookup::Present(value) if !value.trim().is_empty() => {
            InitialSnapshotMode::from_canonical(value).unwrap_or_else(|| {
                tracing::warn!(
                    dataset = %dataset_name,
                    value = %value,
                    "Unknown 'dynamodb_replication_initial_snapshot' (expected auto|always|disabled); defaulting to 'auto'"
                );
                InitialSnapshotMode::Auto
            })
        }
        _ => InitialSnapshotMode::Auto,
    }
}

/// Resolve the invalid-checkpoint behavior, preferring the canonical
/// `dynamodb_replication_invalid_checkpoint_behavior` (`error|restart`) and
/// falling back to the deprecated `lag_exceeds_shard_retention_behavior`
/// (`error|ready_after_load|ready_before_load`). The removed `ready_before_load`
/// maps to `restart` (it previously served stale data before the reload).
fn parse_invalid_checkpoint_behavior(
    params: &Parameters,
    dataset_name: &TableReference,
) -> InvalidCheckpointBehavior {
    if let ExposedParamLookup::Present(value) = params
        .get("replication_invalid_checkpoint_behavior")
        .expose()
    {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return InvalidCheckpointBehavior::from_canonical(trimmed).unwrap_or_else(|| {
                tracing::warn!(
                    dataset = %dataset_name,
                    value = %trimmed,
                    "Unknown 'dynamodb_replication_invalid_checkpoint_behavior' (expected error|restart); defaulting to 'error'"
                );
                InvalidCheckpointBehavior::Error
            });
        }
    }
    if let ExposedParamLookup::Present(value) =
        params.get("lag_exceeds_shard_retention_behavior").expose()
    {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return match trimmed.to_ascii_lowercase().as_str() {
                "error" => InvalidCheckpointBehavior::Error,
                "ready_after_load" => InvalidCheckpointBehavior::Restart,
                "ready_before_load" => {
                    tracing::warn!(
                        dataset = %dataset_name,
                        "'lag_exceeds_shard_retention_behavior: ready_before_load' has been removed (it served stale data before the reload completed); treating it as 'restart' (Ready after load). Migrate to 'dynamodb_replication_invalid_checkpoint_behavior: restart'."
                    );
                    InvalidCheckpointBehavior::Restart
                }
                other => {
                    tracing::warn!(
                        dataset = %dataset_name,
                        value = %other,
                        "Unknown 'lag_exceeds_shard_retention_behavior'; defaulting to 'error'"
                    );
                    InvalidCheckpointBehavior::Error
                }
            };
        }
    }
    InvalidCheckpointBehavior::default()
}

const PARAMETERS: &[ParameterSpec] = &[
    // Connector parameters
    ParameterSpec::component("aws_region")
        .description("The AWS region to use for DynamoDB.")
        .required()
        .secret(),
    ParameterSpec::component("aws_access_key_id")
        .description("The AWS access key ID to use for DynamoDB.")
        .secret(),
    ParameterSpec::component("aws_secret_access_key")
        .description("The AWS secret access key to use for DynamoDB.")
        .secret(),
    ParameterSpec::component("aws_session_token")
        .description("The AWS session token to use for DynamoDB.")
        .secret(),
    ParameterSpec::component("aws_auth")
        .description("Authentication method. Use 'iam_role' for IAM role-based authentication or 'key' for explicit access key credentials")
        .default("iam_role"),
    ParameterSpec::component("aws_iam_role_source")
        .description("IAM role credential source (only used when aws_auth is 'iam_role'). 'auto' uses the default AWS credential chain, 'metadata' uses only instance/container metadata (IMDS, ECS, EKS/IRSA), 'env' uses only environment variables")
        .one_of(&["auto", "metadata", "env"]),
    ParameterSpec::runtime("unnest_depth")
        .description("Maximum nesting depth for unnesting embedded documents into a flattened structure. Higher values expand deeper nested fields."),
    ParameterSpec::runtime("schema_infer_max_records")
        .description("Number of documents to use to infer the schema. Defaults to 10.")
        .default(DEFAULT_SCHEMA_INFER_MAX_RECORDS_STR),
    ParameterSpec::runtime("scan_segments")
        .description("Number of segments. 'auto' by default.")
        .default(SEGMENTS_AUTO_STR),
    ParameterSpec::runtime("scan_interval")
        .description("Interval in milliseconds between polling for new records in a DynamoDB stream.")
        .default("0s"),
    ParameterSpec::runtime("time_format")
        .description("Go-style time format used for parsing/formatting timestamps")
        .default(DEFAULT_TIME_FORMAT),
    // No `.default` here: runtime-parameters injects a spec default into the
    // params map, which would shadow the deprecated `ready_lag` alias in the
    // dual-read below (a user migrating from `ready_lag` would be silently
    // ignored). The 2s default comes from `DEFAULT_READY_LAG` at the parse
    // site, matching the sibling `dynamodb_replication_invalid_checkpoint_behavior`.
    ParameterSpec::component("replication_ready_lag")
        .description("For `refresh_mode: changes`, the dataset is marked Ready once its replication lag (now minus the newest applied source-commit time) falls below this. It stays not-ready while snapshotting or draining a backlog, so it never serves stale data. Default: 2s."),
    // Deprecated alias of `dynamodb_replication_ready_lag`.
    ParameterSpec::runtime("ready_lag")
        .description("[deprecated] Use `dynamodb_replication_ready_lag` instead.")
        .deprecated("Renamed to 'dynamodb_replication_ready_lag'."),
    ParameterSpec::runtime("endpoint_url")
        .description("Custom endpoint URL for DynamoDB-compatible services (e.g., DynamoDB Local, ScyllaDB Alternator)."),
    ParameterSpec::component("replication_initial_snapshot")
        .description("When `refresh_mode: changes` first loads the table's existing items: 'auto' (default) scans when no resumable stream checkpoint exists and resumes without a scan when one does; 'disabled' streams changes only, from the current stream tip; 'always' scans on every start, discarding any persisted checkpoint. Default: auto.")
        .default("auto")
        .one_of_ignore_ascii_case(InitialSnapshotMode::VALUES),
    ParameterSpec::component("replication_invalid_checkpoint_behavior")
        .description("Behavior when the persisted stream checkpoint can no longer be honored (past the ~24h shard retention). 'error' (default) marks the dataset as Error; 'restart' re-bootstraps from a fresh scan. Default: error.")
        .one_of_ignore_ascii_case(InvalidCheckpointBehavior::VALUES),
    // Deprecated alias of `dynamodb_replication_invalid_checkpoint_behavior`.
    // 'ready_after_load' maps to 'restart'; 'ready_before_load' is removed (it
    // served stale data before the reload) and also maps to 'restart'.
    ParameterSpec::runtime("lag_exceeds_shard_retention_behavior")
        .description("[deprecated] Use `dynamodb_replication_invalid_checkpoint_behavior` (error|restart) instead. 'ready_after_load' maps to 'restart'; 'ready_before_load' is removed and also maps to 'restart'.")
        .deprecated("Renamed to 'dynamodb_replication_invalid_checkpoint_behavior'; 'ready_after_load' -> 'restart', and 'ready_before_load' is removed (maps to 'restart')."),
    ParameterSpec::runtime("write_parallelism")
        .description("Number of parallel operations for writing and deleting data to DynamoDB")
        .default("10"),
];

impl DataConnectorFactory for DynamoDBFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let dynamodb = DynamoDB {
                params: params.parameters,
                metrics_collector: Arc::new(MetricsCollector::default()),
            };
            Ok(Arc::new(dynamodb) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "dynamodb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for DynamoDB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<Result<Arc<dyn TableProvider>, DataConnectorError>> {
        Some(self.read_provider(dataset).await)
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> Result<Arc<dyn TableProvider>, DataConnectorError> {
        if let Some(acceleration) = &dataset.acceleration
            && let Some(refresh_mode) = acceleration.refresh_mode
            && matches!(refresh_mode, RefreshMode::Changes)
            && !acceleration.enabled
        {
            tracing::warn!(
                dataset = %dataset.name,
                "DynamoDB dataset is configured for changes stream, but acceleration is disabled. Enable acceleration to use DynamoDB Streams"
            );
        }

        let table_name = dataset.path();

        let mut config_loader = initiate_config_with_auth_method(
            "DynamoDBTableProvider",
            "aws_auth",
            "aws_iam_role_source",
            "aws_region",
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_session_token",
            &self.params,
        )
        .await
        .map_err(|message| DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: "dynamodb".to_string(),
            connector_component: ConnectorComponent::from(dataset),
            message: message.to_string(),
        })?;

        if let Some(endpoint_url) = self.params.get("endpoint_url").expose().ok() {
            config_loader = config_loader.endpoint_url(endpoint_url.to_string());
        }

        let config = config_loader.load().await;

        let schema_infer_max_records = self
            .params
            .get("schema_infer_max_records")
            .expose()
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(10);

        let scan_interval = self
            .params
            .get("scan_interval")
            .expose()
            .ok()
            .and_then(|v| fundu::parse_duration(v).ok())
            .unwrap_or(Duration::from_secs(0));

        let unnest_depth = match self.params.get("unnest_depth").expose() {
            ExposedParamLookup::Present(unnest_depth_str) => Some(usize::from_str(unnest_depth_str).boxed().context(crate::dataconnector::InvalidConfigurationSnafu {
                dataconnector: "dynamodb".to_string(),
                message: format!(
                    "DynamoDB parameter 'unnest_depth' must be an integer, not {unnest_depth_str}"),
                connector_component: ConnectorComponent::from(dataset)
            })?),
            ExposedParamLookup::Absent(_) => None,
        };

        let config_segments = match self
            .params
            .get("scan_segments")
            .expose()
            .unwrap_or_else(|_| SEGMENTS_AUTO_STR)
            .to_lowercase()
            .as_str()
        {
            SEGMENTS_AUTO_STR => None,
            config_segments_str => {
                let config_segments = usize::from_str(config_segments_str).boxed().context(crate::dataconnector::InvalidConfigurationSnafu {
                    dataconnector: "dynamodb".to_string(),
                    message: format!(
                        "DynamoDB parameter 'scan_segments' must be either an integer > 0 or 'auto', not {config_segments_str}"),
                    connector_component: ConnectorComponent::from(dataset),
                })?;

                if config_segments == 0 {
                    return Err(DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: "dynamodb".to_string(),
                        message: format!(
                            "DynamoDB parameter 'scan_segments' must be either an integer > 0 or 'auto', not {config_segments_str}"
                        ),
                        connector_component: ConnectorComponent::from(dataset),
                    });
                }

                Some(config_segments)
            }
        };

        let time_format = self
            .params
            .get("time_format")
            .expose()
            .unwrap_or_else(|_| DEFAULT_TIME_FORMAT);
        if !is_valid_format(time_format) {
            return Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "dynamodb".to_string(),
                message: format!(
                    "DynamoDB parameter 'time_format' is invalid: \"{time_format}\". Refer to https://spiceai.org/docs/components/data-connectors/dynamodb#time-format"
                ),
                connector_component: ConnectorComponent::from(dataset),
            });
        }

        // Prefer the canonical `dynamodb_replication_ready_lag`, falling back to
        // the deprecated `ready_lag` alias; default to the shared CDC ready-lag.
        let ready_lag = self
            .params
            .get("replication_ready_lag")
            .expose()
            .ok()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| {
                self.params
                    .get("ready_lag")
                    .expose()
                    .ok()
                    .filter(|v| !v.trim().is_empty())
            })
            .and_then(|v| fundu::parse_duration(v).ok())
            .unwrap_or(data_components::cdc::DEFAULT_READY_LAG);

        let write_parallelism = self
            .params
            .get("write_parallelism")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(data_components::dynamodb::dml::DEFAULT_WRITE_PARALLELISM);

        let provider = DynamoDBTableProvider::try_new(
            config,
            Arc::from(table_name),
            unnest_depth,
            schema_infer_max_records,
            config_segments,
            scan_interval,
            time_format.to_string(),
            ready_lag,
            Arc::clone(&self.metrics_collector),
            parse_schema_projection(dataset, &ProjectionPolicy::new("dynamodb"))?.as_ref(),
            write_parallelism,
            dataset.schema.clone(),
        )
        .await
        .map_err(|e| DataConnectorError::UnableToGetReadProvider {
            dataconnector: "dynamodb".to_string(),
            connector_component: ConnectorComponent::from(dataset),
            source: Box::new(e),
        })?;

        let is_changes_mode = dataset
            .acceleration
            .as_ref()
            .is_some_and(|a| a.enabled && a.refresh_mode == Some(RefreshMode::Changes));

        if is_changes_mode && !provider.streams_enabled() {
            return Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "dynamodb".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(data_components::dynamodb::Error::StreamsNotEnabled {
                    table_name: table_name.to_string(),
                }),
            });
        }

        Ok(Arc::new(provider))
    }

    fn supports_changes_stream(&self) -> bool {
        true
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        Some(Arc::new(DynamoDBMetricsProvider::new(Arc::new(
            Metrics::new(Arc::clone(&self.metrics_collector)),
        ))))
    }

    fn changes_stream(
        &self,
        federated_table: Arc<FederatedTable>,
        dataset: &Dataset,
        accelerated_table_provider: Arc<dyn TableProvider>,
        accelerator_write_mutex: Arc<Mutex<()>>,
        cpu_runtime: Option<tokio::runtime::Handle>,
    ) -> Option<ChangesStream> {
        let dataset = dataset.clone();

        let invalid_checkpoint_behavior =
            parse_invalid_checkpoint_behavior(&self.params, &dataset.name);
        let snapshot_mode = parse_snapshot_mode(&self.params, &dataset.name);

        let metrics_collector = Arc::clone(&self.metrics_collector);

        Some(Box::pin(
            stream::once(async move {
                let table_provider = federated_table.table_provider().await;

                let dynamodb_ref = table_provider
                    .downcast_ref::<DynamoDBTableProvider>()?;

                let acceptable_lag = dynamodb_ref.ready_lag;
                let dataset_name = dataset.name.clone();
                let dynamodb = Arc::new(dynamodb_ref.clone());
                let dynamodb_sys = Arc::new(if dataset.is_file_accelerated() {
                    initialize_dynamodb_sys(&dataset).await
                } else {
                    tracing::warn!(
                        dataset = %dataset_name,
                        "DynamoDB Streams dataset is not file-accelerated. Connector state is ephemeral and the stream will restart on every runtime restart"
                    );
                    None
                });

                let (should_bootstrap, checkpoint, checkpoint_updated_at) = match snapshot_mode {
                    // `always`: scan on every start from a fresh stream tip,
                    // discarding any saved checkpoint.
                    InitialSnapshotMode::Always => (
                        true,
                        get_latest_checkpoint(&dynamodb, &dataset_name).await?,
                        None,
                    ),
                    // `disabled`: never scan — resume from the saved checkpoint
                    // when present, otherwise begin at the current stream tip.
                    InitialSnapshotMode::Disabled => {
                        let (_, checkpoint, updated_at) =
                            load_or_initialize_checkpoint(&dynamodb, &dynamodb_sys, &dataset_name)
                                .await?;
                        (false, checkpoint, updated_at)
                    }
                    InitialSnapshotMode::Auto => {
                        load_or_initialize_checkpoint(&dynamodb, &dynamodb_sys, &dataset_name)
                            .await?
                    }
                };

                if should_bootstrap {
                    create_bootstrap_stream(
                        dynamodb,
                        dynamodb_sys,
                        checkpoint,
                        acceptable_lag,
                        dataset_name,
                        accelerated_table_provider,
                        accelerator_write_mutex,
                        cpu_runtime,
                    )
                    .await
                } else {
                    Some(resume_from_checkpoint_stream(
                        dynamodb,
                        dynamodb_sys,
                        checkpoint,
                        checkpoint_updated_at,
                        acceptable_lag,
                        dataset_name,
                        invalid_checkpoint_behavior,
                        accelerated_table_provider,
                        accelerator_write_mutex,
                        metrics_collector,
                        cpu_runtime,
                    ))
                }
            })
            .flat_map(|opt| opt.unwrap_or_else(|| stream::empty().boxed())),
        ))
    }
}

async fn initialize_dynamodb_sys(dataset: &Dataset) -> Option<DynamoDBSys> {
    match DynamoDBSys::try_new(dataset, OpenOption::CreateIfNotExists).await {
        Ok(sys) => Some(sys),
        Err(err) => {
            tracing::error!(
                dataset = %dataset.name,
                error = ?err,
                "Failed to initialize DynamoDB Streams sidecar checkpoint storage. Connector state is ephemeral and the stream will restart on every runtime restart"
            );
            None
        }
    }
}

/// Loads checkpoint from `DynamoDBSys`, or initializes a new checkpoint if none exists.
/// Returns (`should_bootstrap`, checkpoint, `checkpoint_updated_at`).
async fn load_or_initialize_checkpoint(
    dynamodb: &Arc<DynamoDBTableProvider>,
    dynamodb_sys: &Arc<Option<DynamoDBSys>>,
    dataset_name: &TableReference,
) -> Option<(bool, Checkpoint, Option<SystemTime>)> {
    if let Some(ref dynamodb_sys) = **dynamodb_sys {
        if let Some(metadata) = dynamodb_sys.get().await {
            match serde_json::from_str::<Checkpoint>(&metadata.checkpoint_data) {
                Ok(checkpoint) => Some((false, checkpoint, metadata.updated_at)),
                Err(err) => {
                    tracing::warn!(
                        dataset = %dataset_name,
                        error = ?err,
                        "Failed to deserialize lag, falling back to initialization"
                    );
                    get_latest_checkpoint(dynamodb, dataset_name)
                        .await
                        .map(|cp| (true, cp, None))
                }
            }
        } else {
            get_latest_checkpoint(dynamodb, dataset_name)
                .await
                .map(|cp| (true, cp, None))
        }
    } else {
        get_latest_checkpoint(dynamodb, dataset_name)
            .await
            .map(|cp| (true, cp, None))
    }
}

async fn get_latest_checkpoint(
    dynamodb: &Arc<DynamoDBTableProvider>,
    dataset_name: &TableReference,
) -> Option<Checkpoint> {
    match dynamodb.latest_global_checkpoint().await {
        Ok(checkpoint) => Some(checkpoint),
        Err(err) => {
            if let Error::FailedToInitializeStream { source: e } = err {
                tracing::error!(
                    dataset = %dataset_name,
                    error = %e,
                    "Failed to initialize DynamoDB Stream"
                );
            } else {
                tracing::error!(
                    dataset = %dataset_name,
                    error = %err,
                    "Failed to initialize DynamoDB Stream lag"
                );
            }

            None
        }
    }
}

/// Initializes the accelerator from a full `DynamoDB` table scan, then transitions to
/// the changes stream from the checkpoint captured before the scan started.
#[expect(clippy::too_many_arguments)]
async fn create_bootstrap_stream(
    dynamodb: Arc<DynamoDBTableProvider>,
    dynamodb_sys: Arc<Option<DynamoDBSys>>,
    checkpoint: Checkpoint,
    acceptable_lag: Duration,
    dataset_name: TableReference,
    accelerated_table_provider: Arc<dyn TableProvider>,
    accelerator_write_mutex: Arc<Mutex<()>>,
    cpu_runtime: Option<tokio::runtime::Handle>,
) -> Option<ChangesStream> {
    tracing::info!(
        dataset = %dataset_name,
        "No existing lag found for DynamoDB Streams table, starting initialization"
    );

    if !scan_and_overwrite_accelerator(
        &dynamodb,
        accelerated_table_provider,
        accelerator_write_mutex,
        cpu_runtime,
        &dataset_name,
    )
    .await
    {
        return None;
    }

    tracing::info!(
        dataset = %dataset_name,
        ready_lag = %humantime::format_duration(acceptable_lag),
        "DynamoDB Streams table initialization complete, starting to process changes from the Stream. Table will be marked as Ready once lag threshold is reached"
    );

    // Commit the checkpoint that was captured before the scan started.
    let committer = DynamoDBStreamCommitter::new(
        Arc::clone(&dynamodb_sys),
        checkpoint.clone(),
        dataset_name.to_string(),
        None,
    );
    if let Err(e) = committer.commit().await {
        tracing::error!(error = ?e, "Failed to commit initialization checkpoint");
    }

    // Transition to the changes stream from the pre-scan checkpoint.
    match changes_stream_from_checkpoint(
        dynamodb,
        dynamodb_sys,
        checkpoint,
        acceptable_lag,
        dataset_name.clone(),
    )
    .await
    {
        Ok(stream) => Some(stream),
        Err(e) => {
            tracing::error!(
                dataset = %dataset_name,
                error = %e,
                "Failed to start changes stream after initialization"
            );
            None
        }
    }
}

/// Resumes streaming from an existing checkpoint, handling shard expiration scenarios.
#[expect(clippy::too_many_arguments)]
fn resume_from_checkpoint_stream(
    dynamodb: Arc<DynamoDBTableProvider>,
    dynamodb_sys: Arc<Option<DynamoDBSys>>,
    checkpoint: Checkpoint,
    checkpoint_updated_at: Option<SystemTime>,
    acceptable_lag: Duration,
    dataset_name: TableReference,
    invalid_checkpoint_behavior: InvalidCheckpointBehavior,
    accelerated_table_provider: Arc<dyn TableProvider>,
    accelerator_write_mutex: Arc<Mutex<()>>,
    metrics_collector: Arc<MetricsCollector>,
    cpu_runtime: Option<tokio::runtime::Handle>,
) -> ChangesStream {
    stream::once(async move {
            match changes_stream_from_checkpoint(
                Arc::clone(&dynamodb),
                Arc::clone(&dynamodb_sys),
                checkpoint,
                acceptable_lag,
                dataset_name.clone(),
            )
            .await
            {
                Ok(changes_stream) => {
                    // Resume reading from lag normally
                    tracing::info!(
                        dataset = %dataset_name,
                        ready_lag = %humantime::format_duration(acceptable_lag),
                        "Found existing lag for DynamoDB Streams table, resuming. Table will be marked as Ready once lag threshold is reached"
                    );
                    Some(changes_stream)
                }
                Err(Error::FailedToInitializeCheckpoint {
                    source: dynamodb_streams::Error::ShardNotFound,
                }) => {
                    // ShardNotFound - check checkpoint age to determine action
                    const CHECKPOINT_AGE_THRESHOLD: Duration =
                        Duration::from_secs(CHECKPOINT_EXPIRATION_HOURS * 60 * 60);
                    let checkpoint_age = checkpoint_updated_at
                        .and_then(|t| SystemTime::now().duration_since(t).ok())
                        .unwrap_or(Duration::from_hours(24)); // Assume old if no timestamp

                    if checkpoint_age < CHECKPOINT_AGE_THRESHOLD {
                        // Checkpoint is fresh (<18h), ShardNotFound is unexpected - propagate error
                        tracing::warn!(
                            dataset = %dataset_name,
                            lag_age = ?checkpoint_age,
                            "ShardNotFound but lag is recent (< 18h threshold). Propagating error"
                        );
                        return Some(
                            stream::once(async move {
                                Err(StreamError::DynamoDB(
                                    DynamoDBStreamError::FailedToReceiveMessage {
                                        source: dynamodb_streams::Error::ShardNotFound,
                                    },
                                ))
                            })
                            .boxed(),
                        );
                    }

                    // Checkpoint is old enough (> 18h) - apply configured behavior
                    if invalid_checkpoint_behavior == InvalidCheckpointBehavior::Error {
                        // Propagate the original error so downstream marks dataset as Error
                        tracing::error!(
                            dataset = %dataset_name,
                            lag_age = %humantime::format_duration(checkpoint_age),
                            "DynamoDB table lag references expired shard. Configured behavior is 'error'"
                        );
                        Some(
                            stream::once(async move {
                                Err(StreamError::DynamoDB(
                                    DynamoDBStreamError::FailedToReceiveMessage {
                                        source: dynamodb_streams::Error::ShardNotFound,
                                    },
                                ))
                            })
                            .boxed(),
                        )
                    } else {
                        // `restart` - re-bootstrap from a fresh scan
                        tracing::info!(
                            dataset = %dataset_name,
                            lag_age = %humantime::format_duration(checkpoint_age),
                            "DynamoDB table lag references expired shard. Initiating table re-initialization"
                        );
                        rebootstrap_table(
                            &dynamodb,
                            &dynamodb_sys,
                            acceptable_lag,
                            &dataset_name,
                            accelerated_table_provider,
                            accelerator_write_mutex,
                            metrics_collector,
                            cpu_runtime,
                        )
                        .await
                    }
                }
                Err(Error::FailedToInitializeCheckpoint {
                    source: dynamodb_streams::Error::StreamBeyondRetention,
                }) => {
                    // StreamBeyondRetention definitively means checkpoint is >24h old
                    if invalid_checkpoint_behavior == InvalidCheckpointBehavior::Error {
                        tracing::error!(
                            dataset = %dataset_name,
                            "DynamoDB Streams checkpoint is older than the stream retention window; ingestion cannot resume from the saved checkpoint."
                        );
                        Some(
                            stream::once(async move {
                                Err(StreamError::DynamoDB(
                                    DynamoDBStreamError::FailedToReceiveMessage {
                                        source: dynamodb_streams::Error::StreamBeyondRetention,
                                    },
                                ))
                            })
                            .boxed(),
                        )
                    } else {
                        tracing::info!(
                            dataset = %dataset_name,
                            "DynamoDB Streams checkpoint is older than the stream retention window; \
                             ingestion cannot resume from the saved checkpoint. Rebuilding the dataset from a fresh DynamoDB table scan."
                        );
                        rebootstrap_table(
                            &dynamodb,
                            &dynamodb_sys,
                            acceptable_lag,
                            &dataset_name,
                            accelerated_table_provider,
                            accelerator_write_mutex,
                            metrics_collector,
                            cpu_runtime,
                        )
                        .await
                    }
                }
                Err(err) => {
                    // Other errors - log and return None
                    tracing::error!(
                        dataset = %dataset_name,
                        error = %err,
                        "Failed to get stream from lag"
                    );
                    None
                }
            }
        })
        .filter_map(|opt| async move { opt })
        .flatten()
        .boxed()
}

async fn changes_stream_from_checkpoint(
    dynamodb: Arc<DynamoDBTableProvider>,
    dynamodb_sys: Arc<Option<DynamoDBSys>>,
    checkpoint: Checkpoint,
    acceptable_lag: Duration,
    dataset_name: TableReference,
) -> Result<ChangesStream, Error> {
    tracing::debug!(
        dataset = %dataset_name,
        checkpoint = ?checkpoint,
        "Starting DynamoDB stream from lag"
    );

    let stream = dynamodb.stream_from_checkpoint(checkpoint).await?;

    Ok(stream
        .map(move |msg| {
            msg.map(|(change_batch, checkpoint, watermark)| {
                let lag = watermark.and_then(|v| SystemTime::now().duration_since(v).ok());

                tracing::debug!(
                    dataset = %dataset_name,
                    watermark = watermark.map_or_else(|| "-".to_string(), |w| humantime::format_rfc3339(w).to_string()),
                    lag = lag.map_or_else(|| "-".to_string(), |l| humantime::format_duration(l).to_string()),
                    shards = checkpoint.shards.len(),
                    records = change_batch.record.num_rows(),
                    "Processing DynamoDB Streams batch"
                );

                // Stamp the upstream commit timestamp so the unified
                // `dataset_acceleration_cdc_replication_lag_ms` metric can compute
                // `now - source_commit_ts_ms` for DynamoDB like the other CDC
                // connectors. The watermark is the slowest unprocessed shard's
                // newest record time, or — when every shard is caught up — the
                // poll-cycle start, so an idle poll batch keeps the gauge fresh.
                // This is a per-batch stamp on DynamoDB's own polled stream, not
                // an injected heartbeat envelope through a shared pump.
                let change_batch = change_batch.with_source_commit_ts_ms(
                    watermark.and_then(data_components::cdc::system_time_to_unix_ms),
                );

                ChangeEnvelope::new(
                    Box::new(DynamoDBStreamCommitter::new(
                        Arc::clone(&dynamodb_sys),
                        checkpoint,
                        dataset_name.to_string(),
                        watermark.and_then(data_components::cdc::system_time_to_unix_ms),
                    )),
                    change_batch,
                    lag.is_some_and(|l| l < acceptable_lag),
                )
            })
        })
        .boxed())
}

#[expect(clippy::too_many_arguments)]
async fn rebootstrap_table(
    dynamodb: &Arc<DynamoDBTableProvider>,
    dynamodb_sys: &Arc<Option<DynamoDBSys>>,
    acceptable_lag: Duration,
    dataset_name: &TableReference,
    accelerated_table_provider: Arc<dyn TableProvider>,
    accelerator_write_mutex: Arc<Mutex<()>>,
    metrics_collector: Arc<MetricsCollector>,
    cpu_runtime: Option<tokio::runtime::Handle>,
) -> Option<ChangesStream> {
    tracing::debug!(
        dataset = %dataset_name,
        "Initiating re-initialization for DynamoDB table"
    );

    // Re-scan first, then mark Ready once the reload completes (readiness is
    // driven by the normal lag path in `changes_stream_from_checkpoint`), so the
    // dataset never serves stale data during the reload.
    do_rebootstrap(
        dynamodb,
        dynamodb_sys,
        acceptable_lag,
        dataset_name,
        accelerated_table_provider,
        accelerator_write_mutex,
        metrics_collector,
        cpu_runtime,
    )
    .await
}

/// Performs the actual re-bootstrap: scans `DynamoDB`, writes to accelerator, commits checkpoint.
#[expect(clippy::too_many_arguments)]
async fn do_rebootstrap(
    dynamodb: &Arc<DynamoDBTableProvider>,
    dynamodb_sys: &Arc<Option<DynamoDBSys>>,
    acceptable_lag: Duration,
    dataset_name: &TableReference,
    accelerated_table_provider: Arc<dyn TableProvider>,
    accelerator_write_mutex: Arc<Mutex<()>>,
    metrics_collector: Arc<MetricsCollector>,
    cpu_runtime: Option<tokio::runtime::Handle>,
) -> Option<ChangesStream> {
    // 1. Get new global checkpoint FIRST (before re-bootstrap starts)
    let new_checkpoint = match dynamodb.latest_global_checkpoint().await {
        Ok(cp) => cp,
        Err(e) => {
            tracing::error!(
                dataset = %dataset_name,
                error = ?e,
                "Failed to get new lag for re-initialization"
            );
            return None;
        }
    };

    tracing::debug!(
        dataset = %dataset_name,
        shards = new_checkpoint.shards.len(),
        "Got new lag for re-initialization of DynamoDB table"
    );

    // 2. Scan DynamoDB and write to accelerator.
    if !scan_and_overwrite_accelerator(
        dynamodb,
        accelerated_table_provider,
        accelerator_write_mutex,
        cpu_runtime,
        dataset_name,
    )
    .await
    {
        return None;
    }

    // 4. Commit the checkpoint
    let committer = DynamoDBStreamCommitter::new(
        Arc::clone(dynamodb_sys),
        new_checkpoint.clone(),
        dataset_name.to_string(),
        None,
    );
    if let Err(e) = committer.commit().await {
        tracing::error!(
            dataset = %dataset_name,
            error = ?e,
            "Failed to commit lag after re-initialization"
        );
        return None;
    }

    tracing::info!(
        dataset = %dataset_name,
        "Re-initialization complete for DynamoDB table, continuing with changes stream"
    );

    // Increment rebootstrap counter
    metrics_collector
        .rebootstraps
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    // 5. Return changes stream from the checkpoint
    match changes_stream_from_checkpoint(
        Arc::clone(dynamodb),
        Arc::clone(dynamodb_sys),
        new_checkpoint,
        acceptable_lag,
        dataset_name.clone(),
    )
    .await
    {
        Ok(stream) => Some(stream),
        Err(e) => {
            tracing::error!(
                dataset = %dataset_name,
                error = %e,
                "Failed to get changes stream after re-initialization"
            );
            None
        }
    }
}

/// Scans the full `DynamoDB` table and writes all rows into the accelerator, replacing any existing
/// data. The scan runs on `cpu_runtime` when available — mirroring full refresh — so the scan can
/// pre-fill the channel buffer on the dedicated runtime while the write proceeds on the main
/// runtime. Returns `true` on success; errors are logged internally.
async fn scan_and_overwrite_accelerator(
    dynamodb: &Arc<DynamoDBTableProvider>,
    accelerated_table_provider: Arc<dyn TableProvider>,
    accelerator_write_mutex: Arc<Mutex<()>>,
    cpu_runtime: Option<tokio::runtime::Handle>,
    dataset_name: &TableReference,
) -> bool {
    use crate::datafusion::managed_runtime;
    use runtime_request_context::{AsyncMarker, RequestContext};

    let data_stream = if let Some(cpu_runtime) = cpu_runtime {
        let request_context = RequestContext::current(AsyncMarker::new().await);
        let span = tracing::Span::current();
        let dynamodb_for_scan = Arc::clone(dynamodb);
        let dataset_name_for_scan = dataset_name.clone();

        match managed_runtime::run_record_batch_stream_on_runtime(
            cpu_runtime,
            request_context,
            span,
            async move {
                let ctx = SessionContext::new();
                let df = ctx
                    .read_table(Arc::clone(&dynamodb_for_scan) as Arc<dyn TableProvider>)
                    .map_err(|e| {
                        tracing::error!(
                            dataset = %dataset_name_for_scan,
                            error = ?e,
                            "Failed to create DataFrame for scan"
                        );
                        e
                    })?;
                let stream = df.execute_stream().await.map_err(|e| {
                    tracing::error!(
                        dataset = %dataset_name_for_scan,
                        error = ?e,
                        "Failed to execute scan stream"
                    );
                    e
                })?;
                Ok::<_, datafusion::error::DataFusionError>(((), stream))
            },
        )
        .await
        {
            Ok(managed) => {
                let ((), stream) = managed.into_parts();
                stream
            }
            Err(e) => {
                tracing::error!(
                    dataset = %dataset_name,
                    error = ?e,
                    "Failed to start scan on CPU runtime"
                );
                return false;
            }
        }
    } else {
        let ctx = SessionContext::new();
        let df = match ctx.read_table(Arc::clone(dynamodb) as Arc<dyn TableProvider>) {
            Ok(df) => df,
            Err(e) => {
                tracing::error!(
                    dataset = %dataset_name,
                    error = ?e,
                    "Failed to create DataFrame for scan"
                );
                return false;
            }
        };
        match df.execute_stream().await {
            Ok(stream) => stream,
            Err(e) => {
                tracing::error!(
                    dataset = %dataset_name,
                    error = ?e,
                    "Failed to execute scan stream"
                );
                return false;
            }
        }
    };

    let instrumented_stream = with_progress_logging(data_stream, dataset_name.clone());
    let table_sink = TableSink::new(accelerated_table_provider);
    let _guard = accelerator_write_mutex.lock().await;
    if let Err(e) = table_sink
        .insert_into(instrumented_stream, InsertOp::Overwrite)
        .await
    {
        tracing::error!(
            dataset = %dataset_name,
            error = ?e,
            "Failed to write scan data to accelerator"
        );
        return false;
    }
    true
}

/// Wraps `data_stream` to log ingestion progress every 10 seconds in the same format as full
/// refresh (`DataLoadTracing`): `Dataset X received N records (Y uncompressed) in Zs, W/s`.
fn with_progress_logging(
    data_stream: datafusion::physical_plan::SendableRecordBatchStream,
    dataset_name: TableReference,
) -> datafusion::physical_plan::SendableRecordBatchStream {
    let schema = data_stream.schema();
    let mut num_records: usize = 0;
    let mut bytes_received: usize = 0;
    let start_time = std::time::Instant::now();
    let mut last_log_time = start_time;
    let log_interval = std::time::Duration::from_secs(10);

    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        data_stream.map(move |batch_result| {
            batch_result.inspect(|batch| {
                num_records += batch.num_rows();
                bytes_received += batch.get_array_memory_size();
                if last_log_time.elapsed() > log_interval {
                    let pretty_records = util::pretty_print_number(num_records);
                    let elapsed = start_time.elapsed();
                    let elapsed_secs = elapsed.as_secs_f64();
                    #[expect(clippy::cast_precision_loss)]
                    #[expect(clippy::cast_possible_truncation)]
                    #[expect(clippy::cast_sign_loss)]
                    let throughput = if elapsed_secs > 0.0 {
                        let bytes_per_sec = (bytes_received as f64 / elapsed_secs) as usize;
                        format!("{}/s", util::human_readable_bytes(bytes_per_sec))
                    } else {
                        "calculating...".to_string()
                    };
                    let size = util::human_readable_bytes(bytes_received);
                    let elapsed_str = format!("{}s", elapsed.as_secs());
                    tracing::info!(
                        "Dataset {} received {pretty_records} records ({size} uncompressed) in {elapsed_str}, {throughput}",
                        dataset_name
                    );
                    last_log_time = std::time::Instant::now();
                }
            })
        }),
    ))
}

#[derive(Debug, Clone)]
struct DynamoDBMetricsProvider {
    metrics: Arc<Metrics>,
}

impl DynamoDBMetricsProvider {
    fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

const METRICS: &[MetricSpec] = &[
    MetricSpec::new("shards_active", MetricType::ObservableGaugeU64)
        .description("Current number of active shards in the stream."),
    MetricSpec::new("records_consumed_total", MetricType::ObservableCounterU64)
        .description("Total number of records consumed from the stream."),
    MetricSpec::new("lag_ms", MetricType::ObservableGaugeU64)
        .description("Current lag in milliseconds between stream watermark and the current time.")
        .unit("ms"),
    MetricSpec::new("errors_transient_total", MetricType::ObservableCounterU64)
        .description("Total number of transient errors encountered while polling from the stream."),
    MetricSpec::new(
        "reinitializations_on_lag_exceeds_shard_retention_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total number of rebootstrap operations triggered due to expired shards."),
];

impl MetricsProvider for DynamoDBMetricsProvider {
    fn component_type(&self) -> ComponentType {
        ComponentType::Dataset
    }

    fn component_name(&self) -> &'static str {
        "dynamodb"
    }

    fn available_metrics(&self) -> &'static [MetricSpec] {
        METRICS
    }

    fn callback_to_observe_metric(
        &self,
        metric: &MetricSpec,
        attributes: Vec<KeyValue>,
    ) -> Option<ObserveMetricCallback> {
        let metrics = Arc::clone(&self.metrics);
        match metric.name {
            "shards_active" => Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(metrics.active_shards_number() as u64, &attributes);
            }))),
            "records_consumed_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(metrics.records() as u64, &attributes);
                })))
            }
            "lag_ms" => Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                if let Some(lag_ms) = metrics.total_lag_ms() {
                    instrument.observe(lag_ms, &attributes);
                }
            }))),
            "errors_transient_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(metrics.transient_errors() as u64, &attributes);
                })))
            }
            "reinitializations_on_lag_exceeds_shard_retention_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(metrics.rebootstraps() as u64, &attributes);
                })))
            }
            _ => None,
        }
    }
}

pub struct DynamoDBStreamCommitter {
    dynamodb_sys: Arc<Option<DynamoDBSys>>,
    checkpoint: Checkpoint,
    /// Dataset name, for the committer-progress log line.
    dataset: String,
    /// Source-commit timestamp (ms since the Unix epoch) of the batch this
    /// commit acks; `None` for init/re-init checkpoint commits.
    source_commit_ts_ms: Option<i64>,
}

impl DynamoDBStreamCommitter {
    #[must_use]
    pub fn new(
        dynamodb_sys: Arc<Option<DynamoDBSys>>,
        checkpoint: Checkpoint,
        dataset: String,
        source_commit_ts_ms: Option<i64>,
    ) -> Self {
        Self {
            dynamodb_sys,
            checkpoint,
            dataset,
            source_commit_ts_ms,
        }
    }
}

#[async_trait]
impl CommitChange for DynamoDBStreamCommitter {
    async fn commit(&self) -> Result<(), CommitError> {
        tracing::trace!(checkpoint = ?self.checkpoint, "Committing DynamoDB lag");

        let checkpoint_json = serde_json::to_string(&self.checkpoint).map_err(|e| {
            CommitError::UnableToCommitChange {
                source: Box::new(e),
            }
        })?;

        let metadata = DynamoDBCheckpointMetadata {
            checkpoint_data: checkpoint_json,
            updated_at: None, // Set by the database layer on upsert
        };

        if let Some(dynamodb_sys) = self.dynamodb_sys.as_ref() {
            dynamodb_sys.upsert(&metadata).await.map_err(|e| {
                CommitError::UnableToCommitChange {
                    source: Box::new(e),
                }
            })?;
        }
        data_components::cdc::log_committer_progress(
            "dynamodb",
            &self.dataset,
            &format!("shards={}", self.checkpoint.shards.len()),
            self.source_commit_ts_ms,
        );
        Ok(())
    }
}

register_data_connector!("dynamodb", DynamoDBFactory);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::builder::DatasetBuilder;
    use serde_json::json;
    use spicepod::semantic::Column;
    use std::collections::HashMap;

    async fn test_dataset(columns: Vec<Column>) -> Dataset {
        let mut dataset = DatasetBuilder::try_new("test:test_dataset".to_string(), "test_dataset")
            .expect("Failed to create builder")
            .with_app(Arc::new(app::AppBuilder::new("test_app").build()))
            .with_runtime(Arc::new(crate::Runtime::builder().build().await))
            .build()
            .expect("Failed to build dataset");

        dataset.columns = columns;

        dataset
    }

    #[tokio::test]
    async fn test_no_json_object_columns_is_identity() {
        let dataset = test_dataset(vec![
            Column::new("PK"),
            Column::new("SK"),
            Column::new("Data"),
        ])
        .await;

        // Columns with no `json_object` marker yield an identity projection
        // (no catch-all): rows pass through unchanged.
        let result = parse_schema_projection(&dataset, &ProjectionPolicy::new("dynamodb"))
            .expect("should return Ok")
            .expect("columns present → Some");
        assert!(!result.has_catch_all());
        assert!(result.is_identity());
    }

    #[tokio::test]
    async fn test_valid_json_nesting_configuration() {
        let mut metadata = HashMap::new();
        metadata.insert("json_object".to_string(), json!("*"));

        let dataset = test_dataset(vec![
            Column::new("PK"),
            Column::new("SK"),
            Column::new("Baz"),
            Column::new("data_json").with_metadata(metadata),
        ])
        .await;

        let result = parse_schema_projection(&dataset, &ProjectionPolicy::new("dynamodb"))
            .expect("should return Ok")
            .expect("should return Some");

        assert_eq!(result.catch_all_name(), Some("data_json"));
        assert_eq!(result.static_fields().len(), 3);
        assert!(result.static_fields().contains("PK"));
        assert!(result.static_fields().contains("SK"));
        assert!(result.static_fields().contains("Baz"));
    }

    #[tokio::test]
    async fn test_multiple_json_object_columns_errors() {
        let mut metadata1 = HashMap::new();
        metadata1.insert("json_object".to_string(), json!("*"));

        let mut metadata2 = HashMap::new();
        metadata2.insert("json_object".to_string(), json!("*"));

        let dataset = test_dataset(vec![
            Column::new("PK"),
            Column::new("data1").with_metadata(metadata1),
            Column::new("data2").with_metadata(metadata2),
        ])
        .await;

        let err = parse_schema_projection(&dataset, &ProjectionPolicy::new("dynamodb"))
            .expect_err("should fail when multiple json_object columns defined")
            .to_string();
        assert!(err.contains("data1"));
        assert!(err.contains("data2"));
    }

    #[tokio::test]
    async fn test_invalid_json_object_value_errors() {
        let mut metadata = HashMap::new();
        metadata.insert("json_object".to_string(), json!("foo"));

        let dataset = test_dataset(vec![
            Column::new("PK"),
            Column::new("data_json").with_metadata(metadata),
        ])
        .await;

        let err = parse_schema_projection(&dataset, &ProjectionPolicy::new("dynamodb"))
            .expect_err("should fail when invalid value")
            .to_string();
        assert!(err.contains("invalid 'json_object' value"));
        assert!(err.contains("Only '*' is supported"));
    }
}
