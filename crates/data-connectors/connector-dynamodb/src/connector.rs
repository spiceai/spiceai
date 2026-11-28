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

use crate::Error;
use crate::provider::DynamoDBTableProvider;
use crate::stream::StreamError as DynamoDBStreamError;
use async_trait::async_trait;
use data_components::cdc::{
    AccelerationContents, ChangeEnvelope, ChangesStream, CommitChange, CommitError,
    InitialSnapshotMode, InvalidCheckpointBehavior, NoOpCommitter,
};
use data_connector_api::federated::FederatedTableProvider;
use data_connector_api::schema_projection::{ProjectionPolicy, parse_schema_projection};
use data_connector_api::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    parameters::{ConnectorContext, aws::initiate_config_with_auth_method},
};
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use dynamodb_streams::{Checkpoint, Metrics, MetricsCollector};
use futures::stream::{self, StreamExt};
use opentelemetry::KeyValue;
use runtime_api_types::v1::ComponentType;
use runtime_checkpoint_api::BlobCheckpointStore;
use runtime_component::dataset::DatasetSpec;
use runtime_component::dataset::acceleration::RefreshMode;
use runtime_metrics::component::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use runtime_parameters::{ExposedParamLookup, ParameterSpec, Parameters};
use snafu::ResultExt;
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use std::{any::Any, future::Future, pin::Pin, sync::Arc};
use util::retry_strategy::{Backoff, BackoffMethod, RetryBackoff, RetryBackoffBuilder};
use util::time_format::is_valid_format;

// If we get `ShardNotFound` or `StreamBeyondRetention` on startup and checkpoint is old enough,
// behavior will depend on lag_exceeds_shard_retention_behavior param.
// DynamoDB retention is 24h, and shards expire every 4h. 2h are added for safety.
const CHECKPOINT_EXPIRATION_HOURS: u64 = 18;

/// Sidecar table, in the dataset's own accelerator, holding this connector's
/// serialized stream checkpoint.
const CHECKPOINT_TABLE: &str = "spice_sys_dynamodb_streams";

/// What a reload stores in place of the checkpoint while it runs. The reload's
/// stream begins with a truncate, so a start that finds this knows the accelerated
/// table may hold only part of the table, whatever checkpoint an earlier run saved.
/// The post-snapshot barrier replaces it with the new checkpoint. It is not a valid
/// [`Checkpoint`], so a build that predates it reads it as unreadable and reloads.
const RELOAD_IN_PROGRESS: &str = r#"{"reload_in_progress":true}"#;

pub struct DynamoDB {
    params: Parameters,
    metrics_collector: Arc<MetricsCollector>,
}

// Hand-written because `MetricsCollector` is not `Debug`.
impl std::fmt::Debug for DynamoDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamoDB")
            .field("params", &self.params)
            .finish_non_exhaustive()
    }
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

    fn create<'a>(
        &'a self,
        params: ConnectorParams,
        _context: &'a dyn ConnectorContext,
    ) -> Pin<Box<dyn Future<Output = data_connector_api::NewDataConnectorResult> + Send + 'a>> {
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

    fn source_identity(
        &self,
        params: &runtime_parameters::Parameters,
    ) -> Result<Option<Vec<(String, String)>>, String> {
        Ok(Some(data_connector_api::source_identity_of(
            params,
            &["aws_region", "endpoint_url"],
        )))
    }
}

#[async_trait]
impl DataConnector for DynamoDB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_write_provider(
        &self,
        context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> Option<Result<Arc<dyn TableProvider>, DataConnectorError>> {
        Some(self.read_provider(context, dataset).await)
    }

    async fn read_provider(
        &self,
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
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
            ExposedParamLookup::Present(unnest_depth_str) => Some(usize::from_str(unnest_depth_str).boxed().context(data_connector_api::InvalidConfigurationSnafu {
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
                let config_segments = usize::from_str(config_segments_str).boxed().context(data_connector_api::InvalidConfigurationSnafu {
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
            .unwrap_or(crate::dml::DEFAULT_WRITE_PARALLELISM);
        // A zero would flow into `mpsc::channel(write_parallelism * 2)` (0 capacity panics in
        // Tokio) and spawn no writer tasks, so reject it as a configuration error rather than
        // crashing the write path.
        if write_parallelism == 0 {
            return Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "dynamodb".to_string(),
                message: "DynamoDB parameter 'write_parallelism' is invalid: must be a positive integer (got 0). Refer to https://spiceai.org/docs/components/data-connectors/dynamodb".to_string(),
                connector_component: ConnectorComponent::from(dataset),
            });
        }

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
                source: Box::new(crate::Error::StreamsNotEnabled {
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

    async fn changes_stream(
        &self,
        context: &dyn ConnectorContext,
        federated_table: Arc<dyn FederatedTableProvider>,
        dataset: &DatasetSpec,
        _acceleration: AccelerationContents,
    ) -> Option<ChangesStream> {
        let dataset = dataset.clone();

        let invalid_checkpoint_behavior =
            parse_invalid_checkpoint_behavior(&self.params, &dataset.name);
        let snapshot_mode = parse_snapshot_mode(&self.params, &dataset.name);

        let metrics_collector = Arc::clone(&self.metrics_collector);

        // Resolved here rather than inside the stream: the stream outlives this call,
        // and a store holds only a connection to the accelerator, so handing the
        // generator the resolved store keeps it from pinning the runtime.
        let dynamodb_sys: Option<Arc<dyn BlobCheckpointStore>> = if dataset.is_file_accelerated() {
            // `None` here is a graceful "checkpointing unavailable" degradation, and the
            // accessor already logs why, so it is not logged again.
            context
                .blob_checkpoint_store(&dataset, CHECKPOINT_TABLE)
                .await
        } else {
            tracing::warn!(
                dataset = %dataset.name,
                "DynamoDB Streams dataset is not file-accelerated. Connector state is ephemeral and the stream will restart on every runtime restart"
            );
            None
        };

        Some(Box::pin(
            stream::once(async move {
                let table_provider = federated_table.table_provider().await;

                let dynamodb_ref = table_provider.downcast_ref::<DynamoDBTableProvider>()?;

                let acceptable_lag = dynamodb_ref.ready_lag;
                let dataset_name = dataset.name.clone();
                let dynamodb = Arc::new(dynamodb_ref.clone());

                let start = match snapshot_mode {
                    // `always`: scan on every start from a fresh stream tip,
                    // discarding any saved checkpoint.
                    InitialSnapshotMode::Always => get_latest_checkpoint(&dynamodb, &dataset_name)
                        .await
                        .map(|checkpoint| (true, checkpoint, None)),
                    // `disabled`: never scan — resume from the saved checkpoint
                    // when present, otherwise begin at the current stream tip.
                    // An interrupted reload may have left the table partly
                    // loaded, which only a scan repairs, so it is refused.
                    InitialSnapshotMode::Disabled => match load_or_initialize_checkpoint(
                        &dynamodb,
                        dynamodb_sys.as_ref(),
                        &dataset_name,
                    )
                    .await
                    {
                        Ok(SavedStart::Fresh {
                            interrupted: true, ..
                        }) => return Some(reload_interrupted(dynamodb.table_name())),
                        start => start.map(SavedStart::without_scan),
                    },
                    InitialSnapshotMode::Auto => load_or_initialize_checkpoint(
                        &dynamodb,
                        dynamodb_sys.as_ref(),
                        &dataset_name,
                    )
                    .await
                    .map(SavedStart::into_start),
                };
                let (should_bootstrap, checkpoint, checkpoint_updated_at) = match start {
                    Ok(start) => start,
                    Err(error) => return Some(stream_start_failed(dynamodb.table_name(), error)),
                };

                if should_bootstrap {
                    create_bootstrap_stream(
                        dynamodb,
                        dynamodb_sys,
                        checkpoint,
                        acceptable_lag,
                        dataset_name,
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
                        metrics_collector,
                    ))
                }
            })
            .flat_map(|opt| opt.unwrap_or_else(|| stream::empty().boxed())),
        ))
    }
}

/// Where a start that reads the saved checkpoint begins.
enum SavedStart {
    /// Resume from the saved checkpoint.
    Resume {
        checkpoint: Checkpoint,
        updated_at: Option<SystemTime>,
    },
    /// Nothing to resume from: `checkpoint` is the current stream tip. `interrupted`
    /// is set when an earlier reload stopped partway, so the accelerated table may
    /// be missing rows.
    Fresh {
        checkpoint: Checkpoint,
        interrupted: bool,
    },
}

impl SavedStart {
    /// (`should_bootstrap`, checkpoint, `checkpoint_updated_at`) for `auto`: scan
    /// when there is nothing to resume from.
    fn into_start(self) -> (bool, Checkpoint, Option<SystemTime>) {
        match self {
            Self::Resume {
                checkpoint,
                updated_at,
            } => (false, checkpoint, updated_at),
            Self::Fresh { checkpoint, .. } => (true, checkpoint, None),
        }
    }

    /// The same for `disabled`, which never scans.
    fn without_scan(self) -> (bool, Checkpoint, Option<SystemTime>) {
        let (_, checkpoint, updated_at) = self.into_start();
        (false, checkpoint, updated_at)
    }
}

/// Loads the checkpoint from the sidecar [`BlobCheckpointStore`]. Falls back to
/// re-initialization (a fresh scan) when there is no store, no persisted checkpoint,
/// the persisted value can't be deserialized, *or* the store read fails — a store-read
/// failure is logged and treated as an at-least-once re-bootstrap. A stored
/// [`RELOAD_IN_PROGRESS`] reports an interrupted reload.
/// Returns where to start, or the error that stopped a fresh checkpoint from being
/// read after retrying.
async fn load_or_initialize_checkpoint(
    dynamodb: &Arc<DynamoDBTableProvider>,
    dynamodb_sys: Option<&Arc<dyn BlobCheckpointStore>>,
    dataset_name: &TableReference,
) -> Result<SavedStart, Error> {
    let fresh = |interrupted: bool| {
        move |checkpoint: Checkpoint| SavedStart::Fresh {
            checkpoint,
            interrupted,
        }
    };
    let Some(dynamodb_sys) = dynamodb_sys else {
        return get_latest_checkpoint(dynamodb, dataset_name)
            .await
            .map(fresh(false));
    };

    match dynamodb_sys.get().await {
        Ok(Some(metadata)) if metadata.data == RELOAD_IN_PROGRESS => {
            get_latest_checkpoint(dynamodb, dataset_name)
                .await
                .map(fresh(true))
        }
        Ok(Some(metadata)) => match serde_json::from_str::<Checkpoint>(&metadata.data) {
            Ok(checkpoint) => Ok(SavedStart::Resume {
                checkpoint,
                updated_at: metadata.updated_at,
            }),
            Err(err) => {
                tracing::warn!(
                    dataset = %dataset_name,
                    error = ?err,
                    "Failed to deserialize the persisted DynamoDB Streams checkpoint, falling back to re-initialization"
                );
                get_latest_checkpoint(dynamodb, dataset_name)
                    .await
                    .map(fresh(false))
            }
        },
        Ok(None) => get_latest_checkpoint(dynamodb, dataset_name)
            .await
            .map(fresh(false)),
        Err(err) => {
            // Surface the store-read failure (instead of silently treating it as
            // "no checkpoint"), then fall back to a full re-bootstrap — the same
            // safe at-least-once behavior as a missing checkpoint.
            tracing::error!(
                dataset = %dataset_name,
                error = %err,
                "Failed to read the DynamoDB checkpoint from the accelerator; re-bootstrapping from a fresh scan"
            );
            get_latest_checkpoint(dynamodb, dataset_name)
                .await
                .map(fresh(false))
        }
    }
}

async fn get_latest_checkpoint(
    dynamodb: &Arc<DynamoDBTableProvider>,
    dataset_name: &TableReference,
) -> Result<Checkpoint, Error> {
    retry_stream_start(
        dataset_name,
        "read the shards",
        stream_start_backoff(),
        || dynamodb.latest_global_checkpoint(),
    )
    .await
    .inspect_err(|err| {
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
    })
}

/// How many times starting a change stream retries an AWS call that failed, and
/// the longest wait between attempts. The waits follow a Fibonacci sequence from
/// one second, so the attempts span about two minutes: long enough to outlast a
/// stream that is still being enabled or a burst of throttling.
const STREAM_START_MAX_RETRIES: usize = 10;
const STREAM_START_MAX_INTERVAL: Duration = Duration::from_secs(30);

fn stream_start_backoff() -> RetryBackoff {
    RetryBackoffBuilder::new()
        .method(BackoffMethod::Fibonacci)
        .max_retries(Some(STREAM_START_MAX_RETRIES))
        .max_duration(Some(STREAM_START_MAX_INTERVAL))
        .build()
}

/// Whether a failure to start reading the stream may succeed on another attempt.
/// An expired shard or a checkpoint past the stream's retention will not, and
/// each has its own handling (see [`resume_from_checkpoint_stream`]).
fn is_retryable_stream_start_error(error: &Error) -> bool {
    !matches!(
        error,
        Error::FailedToInitializeCheckpoint {
            source: dynamodb_streams::Error::ShardNotFound
                | dynamodb_streams::Error::StreamBeyondRetention
        } | Error::FailedToInitializeStream {
            source: dynamodb_streams::Error::ShardNotFound
                | dynamodb_streams::Error::StreamBeyondRetention
        }
    )
}

/// Runs `attempt`, one of the AWS calls that start a change stream, retrying a
/// retryable failure with `backoff`. A change stream that fails to start is never
/// started again, so without this a single transient error (throttling, or a
/// stream still being enabled) leaves the dataset loading forever.
async fn retry_stream_start<T, F, Fut>(
    dataset_name: &TableReference,
    step: &str,
    mut backoff: RetryBackoff,
    mut attempt: F,
) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut retries = 0usize;
    loop {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) if !is_retryable_stream_start_error(&error) => return Err(error),
            Err(error) => error,
        };
        let Some(delay) = backoff.next_backoff() else {
            return Err(error);
        };
        retries += 1;
        tracing::warn!(
            "Dataset '{dataset_name}' failed to {step} of its DynamoDB Stream, so it is not ready yet; retrying in {} (retry {retries} of {STREAM_START_MAX_RETRIES}). Cause: {error}",
            humantime::format_duration(Duration::from_millis(
                u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)
            )),
        );
        tokio::time::sleep(delay).await;
    }
}

/// A change stream whose only item is the error that stopped it from starting,
/// so the dataset reports that error instead of loading forever.
fn stream_start_failed(table_name: &str, error: Error) -> ChangesStream {
    let table_name = table_name.to_string();
    stream::once(async move {
        Err(DynamoDBStreamError::FailedToStartStream {
            table_name,
            source: error,
        }
        .into())
    })
    .boxed()
}

/// The stream a `disabled` start yields after an interrupted reload: it reports
/// that the table needs a reload rather than serving it partly loaded.
fn reload_interrupted(table_name: &str) -> ChangesStream {
    let table_name = table_name.to_string();
    stream::once(async move { Err(DynamoDBStreamError::ReloadInterrupted { table_name }.into()) })
        .boxed()
}

/// Initializes the accelerator from a full `DynamoDB` table scan, then transitions to
/// the changes stream from the checkpoint captured before the scan started.
async fn create_bootstrap_stream(
    dynamodb: Arc<DynamoDBTableProvider>,
    dynamodb_sys: Option<Arc<dyn BlobCheckpointStore>>,
    checkpoint: Checkpoint,
    acceptable_lag: Duration,
    dataset_name: TableReference,
) -> Option<ChangesStream> {
    tracing::info!(
        dataset = %dataset_name,
        ready_lag = %humantime::format_duration(acceptable_lag),
        "No existing checkpoint found for DynamoDB Streams table, starting initialization. Table will be marked as Ready once lag threshold is reached"
    );

    // A failure to start still yields the stream that reports it.
    Some(
        emit_overwrite_then_live(
            dynamodb,
            dynamodb_sys,
            checkpoint,
            acceptable_lag,
            dataset_name,
        )
        .await
        .unwrap_or_else(|failed| failed),
    )
}

/// Emits a full-overwrite bootstrap through the CDC change contract, then
/// transitions to the live changes stream from `checkpoint`.
///
/// The stream is: a `Truncate` barrier + the table scan as `op="c"` inserts
/// ([`DynamoDBTableProvider::overwrite_bootstrap_stream`]), then a zero-row
/// envelope whose committer persists `checkpoint` (the position captured
/// *before* the scan) once the snapshot is durably applied, then the live
/// changes from `checkpoint`.
///
/// A crash mid-bootstrap must leave nothing to resume from, so the next start
/// re-bootstraps from scratch (the same at-least-once contract as the
/// `MySQL`/Postgres snapshots). Committing on the post-snapshot envelope — not up
/// front — keeps this bootstrap from saving its checkpoint early, and before the
/// stream is returned [`RELOAD_IN_PROGRESS`] replaces any checkpoint an earlier
/// run saved: the truncate leads the stream, so that checkpoint would let the next
/// start resume over a partly reloaded table and never load the rest. When the
/// marker cannot be stored, no bootstrap starts.
///
/// This replaces the old direct `TableSink` overwrite, so the connector needs no
/// runtime accelerator-write internals. Readiness continues to be driven by the
/// live stream's watermark, preserving prior behavior.
///
/// When the stream cannot be set up, `Err` carries the stream to hand on instead:
/// one that reports the failure, so the dataset shows it rather than loading
/// forever.
async fn emit_overwrite_then_live(
    dynamodb: Arc<DynamoDBTableProvider>,
    dynamodb_sys: Option<Arc<dyn BlobCheckpointStore>>,
    checkpoint: Checkpoint,
    acceptable_lag: Duration,
    dataset_name: TableReference,
) -> Result<ChangesStream, ChangesStream> {
    let table_schema = dynamodb.schema();
    let table_name = dynamodb.table_name().to_string();
    let checkpoint_store = dynamodb_sys.clone();

    let snapshot = match Arc::clone(&dynamodb).overwrite_bootstrap_stream().await {
        Ok(stream) => stream
            .map(|res| res.map(|batch| ChangeEnvelope::new(Box::new(NoOpCommitter), batch, false))),
        Err(e) => {
            tracing::error!(
                dataset = %dataset_name,
                error = %e,
                "Failed to start DynamoDB overwrite bootstrap stream"
            );
            return Err(stream::once(async move {
                Err(DynamoDBStreamError::FailedToStartInitialScan {
                    table_name,
                    source: e,
                }
                .into())
            })
            .boxed());
        }
    };

    // Zero-row barrier carrying the pre-scan checkpoint. Its committer runs only
    // after the truncate + snapshot are durably applied (the `CommitChange`
    // ordering contract), mirroring the MySQL bootstrap's `InitialPositionCommitter`.
    let Some(checkpoint_batch) = empty_change_batch(&table_schema) else {
        tracing::error!(
            dataset = %dataset_name,
            "Failed to build DynamoDB bootstrap checkpoint barrier batch; dataset will not start streaming or commit checkpoints"
        );
        return Err(stream::empty().boxed());
    };
    let checkpoint_envelope = ChangeEnvelope::from_parts(
        Box::new(DynamoDBStreamCommitter::new(
            dynamodb_sys.clone(),
            checkpoint.clone(),
            dataset_name.to_string(),
            None,
        )),
        checkpoint_batch,
        false, // readiness comes from the live stream, preserving prior behavior
        false, // not a history-unavailable signal: this is the bootstrap checkpoint barrier
        false,
    );

    let live = match changes_stream_from_checkpoint(
        dynamodb,
        dynamodb_sys,
        checkpoint,
        acceptable_lag,
        dataset_name.clone(),
    )
    .await
    {
        Ok(stream) => stream,
        Err(e) => {
            tracing::error!(
                dataset = %dataset_name,
                error = %e,
                "Failed to start DynamoDB changes stream after initialization"
            );
            return Err(stream_start_failed(&table_name, e));
        }
    };

    // Last, once nothing else can fail: a failed setup keeps the checkpoint, and
    // the accelerated table, as they were.
    if let Err(error) = mark_reload_in_progress(checkpoint_store.as_ref(), &table_name).await {
        return Err(stream::once(async move { Err(error.into()) }).boxed());
    }

    Ok(snapshot
        .chain(stream::once(async move { Ok(checkpoint_envelope) }))
        .chain(live)
        .boxed())
}

/// Stores [`RELOAD_IN_PROGRESS`] in place of the checkpoint, before a reload hands
/// out its truncate (see [`emit_overwrite_then_live`]). Without a store there is
/// no checkpoint to replace.
async fn mark_reload_in_progress(
    store: Option<&Arc<dyn BlobCheckpointStore>>,
    table_name: &str,
) -> Result<(), DynamoDBStreamError> {
    let Some(store) = store else {
        return Ok(());
    };
    store.upsert(RELOAD_IN_PROGRESS).await.map_err(|source| {
        DynamoDBStreamError::FailedToMarkReload {
            table_name: table_name.to_string(),
            source,
        }
    })
}

/// Resumes streaming from an existing checkpoint, handling shard expiration scenarios.
#[expect(clippy::too_many_arguments)]
fn resume_from_checkpoint_stream(
    dynamodb: Arc<DynamoDBTableProvider>,
    dynamodb_sys: Option<Arc<dyn BlobCheckpointStore>>,
    checkpoint: Checkpoint,
    checkpoint_updated_at: Option<SystemTime>,
    acceptable_lag: Duration,
    dataset_name: TableReference,
    invalid_checkpoint_behavior: InvalidCheckpointBehavior,
    metrics_collector: Arc<MetricsCollector>,
) -> ChangesStream {
    stream::once(async move {
            match changes_stream_from_checkpoint(
                Arc::clone(&dynamodb),
                dynamodb_sys.clone(),
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
                                Err(DynamoDBStreamError::FailedToReceiveMessage {
                                    source: dynamodb_streams::Error::ShardNotFound,
                                }
                                .into())
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
                                Err(DynamoDBStreamError::FailedToReceiveMessage {
                                    source: dynamodb_streams::Error::ShardNotFound,
                                }
                                .into())
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
                            dynamodb_sys.as_ref(),
                            acceptable_lag,
                            &dataset_name,
                            metrics_collector,
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
                                Err(DynamoDBStreamError::StreamBeyondRetention {
                                    source: dynamodb_streams::Error::StreamBeyondRetention,
                                }
                                .into())
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
                            dynamodb_sys.as_ref(),
                            acceptable_lag,
                            &dataset_name,
                            metrics_collector,
                        )
                        .await
                    }
                }
                Err(err) => {
                    // Any other error has already been retried; report it so the
                    // dataset shows the failure instead of loading forever.
                    tracing::error!(
                        dataset = %dataset_name,
                        error = %err,
                        "Failed to get stream from lag"
                    );
                    Some(stream_start_failed(dynamodb.table_name(), err))
                }
            }
        })
        .filter_map(|opt| async move { opt })
        .flatten()
        .boxed()
}

async fn changes_stream_from_checkpoint(
    dynamodb: Arc<DynamoDBTableProvider>,
    dynamodb_sys: Option<Arc<dyn BlobCheckpointStore>>,
    checkpoint: Checkpoint,
    acceptable_lag: Duration,
    dataset_name: TableReference,
) -> Result<ChangesStream, Error> {
    tracing::debug!(
        dataset = %dataset_name,
        checkpoint = ?checkpoint,
        "Starting DynamoDB stream from lag"
    );

    let stream = retry_stream_start(
        &dataset_name,
        "open the shards",
        stream_start_backoff(),
        || dynamodb.stream_from_checkpoint(checkpoint.clone()),
    )
    .await?;

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
                        dynamodb_sys.clone(),
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

async fn rebootstrap_table(
    dynamodb: &Arc<DynamoDBTableProvider>,
    dynamodb_sys: Option<&Arc<dyn BlobCheckpointStore>>,
    acceptable_lag: Duration,
    dataset_name: &TableReference,
    metrics_collector: Arc<MetricsCollector>,
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
        metrics_collector,
    )
    .await
}

/// Performs the actual re-bootstrap: captures a fresh checkpoint, then emits a
/// `Truncate` + full-scan snapshot through the CDC change contract, committing the
/// new checkpoint once the snapshot is durably applied (see
/// [`emit_overwrite_then_live`]).
async fn do_rebootstrap(
    dynamodb: &Arc<DynamoDBTableProvider>,
    dynamodb_sys: Option<&Arc<dyn BlobCheckpointStore>>,
    acceptable_lag: Duration,
    dataset_name: &TableReference,
    metrics_collector: Arc<MetricsCollector>,
) -> Option<ChangesStream> {
    // Capture a new global checkpoint FIRST (before the scan) so live changes
    // since the scan are re-delivered by the resumed stream, not missed.
    let new_checkpoint = match retry_stream_start(
        dataset_name,
        "read the shards",
        stream_start_backoff(),
        || dynamodb.latest_global_checkpoint(),
    )
    .await
    {
        Ok(cp) => cp,
        Err(e) => {
            tracing::error!(
                dataset = %dataset_name,
                error = ?e,
                "Failed to get new checkpoint for re-initialization"
            );
            return Some(stream_start_failed(dynamodb.table_name(), e));
        }
    };

    tracing::debug!(
        dataset = %dataset_name,
        shards = new_checkpoint.shards.len(),
        "Got new checkpoint for re-initialization of DynamoDB table"
    );

    let stream = match emit_overwrite_then_live(
        Arc::clone(dynamodb),
        dynamodb_sys.cloned(),
        new_checkpoint,
        acceptable_lag,
        dataset_name.clone(),
    )
    .await
    {
        Ok(stream) => stream,
        // Not counted or logged as started: the stream only reports the failure.
        Err(failed) => return Some(failed),
    };

    // Count the rebootstrap once its stream is successfully set up.
    metrics_collector
        .rebootstraps
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    tracing::info!(
        dataset = %dataset_name,
        "Re-initialization stream started for DynamoDB table"
    );

    Some(stream)
}

/// Builds a zero-row [`ChangeBatch`] in the connector's `changes_schema`.
///
/// Used both for the ready-signal envelope and the post-snapshot checkpoint
/// barrier (see [`emit_overwrite_then_live`]). Matches the original schema
/// `DynamoDB` uses for its `op="c"`/`op="t"` batches so it coalesces with them.
fn empty_change_batch(
    table_schema: &arrow::datatypes::SchemaRef,
) -> Option<data_components::cdc::ChangeBatch> {
    use arrow::record_batch::RecordBatch;
    use data_components::cdc::{ChangeBatch, changes_schema};

    let schema_ref = Arc::new(changes_schema(table_schema.as_ref()));

    // Create empty arrays that match the schema exactly
    let empty_arrays: Vec<arrow::array::ArrayRef> = schema_ref
        .fields()
        .iter()
        .map(|f| arrow::array::new_empty_array(f.data_type()))
        .collect();

    // Log the concrete Arrow/CDC cause: this batch gates both the readiness signal
    // and the post-snapshot checkpoint barrier, so a silent failure here is hard to
    // diagnose. Callers still degrade on `None` (see `emit_overwrite_then_live`).
    let record_batch = RecordBatch::try_new(schema_ref, empty_arrays)
        .inspect_err(|e| tracing::error!(error = %e, "Failed to build empty change RecordBatch"))
        .ok()?;

    ChangeBatch::try_new(record_batch)
        .inspect_err(|e| tracing::error!(error = %e, "Failed to build empty ChangeBatch"))
        .ok()
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
    dynamodb_sys: Option<Arc<dyn BlobCheckpointStore>>,
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
        dynamodb_sys: Option<Arc<dyn BlobCheckpointStore>>,
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

        if let Some(dynamodb_sys) = self.dynamodb_sys.as_ref() {
            dynamodb_sys.upsert(&checkpoint_json).await.map_err(|e| {
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

    /// The same save as [`Self::commit`], for an accelerator that commits it with
    /// the data it covers. Each batch's checkpoint holds every polled shard's
    /// position, so it covers the batches before it; `commit` then saves an
    /// unchanged checkpoint.
    fn record_position(
        &self,
    ) -> Option<futures::future::BoxFuture<'static, Result<(), CommitError>>> {
        let store = Arc::clone(self.dynamodb_sys.as_ref()?);
        let checkpoint = self.checkpoint.clone();
        Some(Box::pin(async move {
            let checkpoint_json = serde_json::to_string(&checkpoint).map_err(|e| {
                CommitError::UnableToCommitChange {
                    source: Box::new(e),
                }
            })?;
            store
                .upsert(&checkpoint_json)
                .await
                .map_err(|e| CommitError::UnableToCommitChange {
                    source: Box::new(e),
                })
        }))
    }
}

#[cfg(test)]
mod reload_marker_tests {
    use super::*;
    use runtime_checkpoint_api::{BlobCheckpoint, CheckpointError};
    use std::sync::Mutex;

    const SAVED: &str = r#"{"shards":{}}"#;

    /// A checkpoint store holding `SAVED`, whose writes fail.
    struct ReadOnlyStore(Mutex<Option<String>>);

    #[async_trait]
    impl BlobCheckpointStore for ReadOnlyStore {
        async fn get(&self) -> Result<Option<BlobCheckpoint>, CheckpointError> {
            let data = self.0.lock().expect("store lock").clone();
            Ok(data.map(|data| BlobCheckpoint {
                data,
                updated_at: None,
            }))
        }

        async fn upsert(&self, _data: &str) -> Result<(), CheckpointError> {
            Err(CheckpointError::Store {
                source: "attempt to write a readonly database".into(),
            })
        }

        async fn delete(&self) -> Result<(), CheckpointError> {
            Err(CheckpointError::Store {
                source: "attempt to write a readonly database".into(),
            })
        }
    }

    /// When the marker cannot be stored, the reload must not start: the earlier
    /// checkpoint is left as it was, and the error names the table, what the user
    /// loses, and what to check.
    #[tokio::test]
    async fn a_failed_marker_write_stops_the_reload() {
        let store = Arc::new(ReadOnlyStore(Mutex::new(Some(SAVED.to_string()))));
        let as_store: Arc<dyn BlobCheckpointStore> = Arc::<ReadOnlyStore>::clone(&store);

        let error = mark_reload_in_progress(Some(&as_store), "orders")
            .await
            .expect_err("the marker write fails");

        assert_eq!(
            error.to_string(),
            "Failed to record that DynamoDB table 'orders' is being reloaded, so the dataset is not reloaded and receives no changes until Spice restarts. Check that the acceleration's files are writable; see https://spiceai.org/docs/components/data-connectors/dynamodb. Cause: Checkpoint store operation failed: attempt to write a readonly database"
        );
        assert_eq!(store.0.lock().expect("store lock").as_deref(), Some(SAVED));
    }

    /// The marker is not a checkpoint, so a build that predates it reads it as
    /// unreadable and reloads instead of resuming over a partial table.
    #[test]
    fn the_marker_is_not_a_checkpoint() {
        let error = serde_json::from_str::<Checkpoint>(RELOAD_IN_PROGRESS)
            .expect_err("the marker does not parse as a checkpoint");
        assert_eq!(
            error.to_string(),
            "missing field `shards` at line 1 column 27"
        );
    }
}

#[cfg(test)]
mod stream_start_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn transient() -> Error {
        Error::FailedToInitializeCheckpoint {
            source: dynamodb_streams::Error::Throttled,
        }
    }

    fn dataset() -> TableReference {
        TableReference::bare("d")
    }

    /// A stream that fails to start a few times (here, a stream still being
    /// enabled) starts once the call succeeds, after waiting between attempts.
    #[tokio::test(start_paused = true)]
    async fn a_transient_failure_is_retried_until_it_succeeds() {
        let calls = AtomicUsize::new(0);
        let started = tokio::time::Instant::now();
        let value = retry_stream_start(
            &dataset(),
            "open the shards",
            stream_start_backoff(),
            || {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                async move { if call < 3 { Err(transient()) } else { Ok(call) } }
            },
        )
        .await
        .expect("succeeds on the fourth call");
        assert_eq!(value, 3);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert!(
            started.elapsed() >= Duration::from_secs(2),
            "waited {:?}",
            started.elapsed()
        );
    }

    /// An expired shard or a checkpoint past retention cannot succeed on retry,
    /// and the resume path handles each, so they are returned at once.
    #[tokio::test(start_paused = true)]
    async fn an_expired_shard_or_checkpoint_is_not_retried() {
        for source in [
            dynamodb_streams::Error::ShardNotFound,
            dynamodb_streams::Error::StreamBeyondRetention,
        ] {
            let calls = AtomicUsize::new(0);
            let mut source = Some(source);
            let error = retry_stream_start(
                &dataset(),
                "open the shards",
                stream_start_backoff(),
                || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let source = source.take().expect("called once");
                    async move { Err::<(), _>(Error::FailedToInitializeCheckpoint { source }) }
                },
            )
            .await
            .expect_err("not retried");
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(!is_retryable_stream_start_error(&error));
        }
    }

    /// A failure that outlasts every retry is returned, and the change stream
    /// then carries it, so the dataset reports the error instead of loading
    /// forever.
    #[tokio::test(start_paused = true)]
    async fn a_persistent_failure_ends_as_a_stream_error() {
        let calls = AtomicUsize::new(0);
        let error = retry_stream_start(
            &dataset(),
            "open the shards",
            stream_start_backoff(),
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(transient()) }
            },
        )
        .await
        .expect_err("gives up");
        assert_eq!(calls.load(Ordering::SeqCst), STREAM_START_MAX_RETRIES + 1);

        let items: Vec<_> = stream_start_failed("orders", error).collect().await;
        assert_eq!(items.len(), 1);
        let Some(Err(item)) = items.into_iter().next() else {
            panic!("expected one error item");
        };
        assert_eq!(
            item.to_string(),
            "DynamoDB error: Failed to start reading the DynamoDB Stream of table 'orders', so the dataset receives no changes until Spice restarts. Check that the table's stream is enabled and reachable; see https://spiceai.org/docs/components/data-connectors/dynamodb. Cause: Failed to initialize DynamoDB Stream checkpoint: Request has been throttled. Try again later"
        );
    }

    /// A failure to start the initial table scan names the table and says what
    /// the dataset is missing, rather than blaming the stream.
    #[test]
    fn an_initial_scan_failure_names_the_table() {
        let error: data_components::cdc::StreamError =
            DynamoDBStreamError::FailedToStartInitialScan {
                table_name: "orders".to_string(),
                source: Error::TableStatusIsNotActive,
            }
            .into();
        assert_eq!(
            error.to_string(),
            "DynamoDB error: Failed to start the initial scan of DynamoDB table 'orders', so the dataset has no data until Spice restarts. See https://spiceai.org/docs/components/data-connectors/dynamodb. Cause: DynamoDB table is not in 'ACTIVE' status. Verify the table status and try again."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime::component::dataset::Dataset;
    use runtime::component::dataset::builder::DatasetBuilder;
    use serde_json::json;
    use spicepod::semantic::Column;
    use std::collections::HashMap;

    async fn test_dataset(columns: Vec<Column>) -> Dataset {
        let mut dataset = DatasetBuilder::try_new("test:test_dataset".to_string(), "test_dataset")
            .expect("Failed to create builder")
            .with_app(Arc::new(app::AppBuilder::new("test_app").build()))
            .with_runtime(Arc::new(runtime::Runtime::builder().build().await))
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
