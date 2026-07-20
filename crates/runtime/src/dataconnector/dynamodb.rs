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
use crate::component::dataset::Dataset;
use crate::component::dataset::acceleration::RefreshMode;
use crate::dataaccelerator::spice_sys::OpenOption;
use crate::dataaccelerator::spice_sys::dynamodb::DynamoDBSys;
use crate::dataconnector::schema_projection::{ProjectionPolicy, parse_schema_projection};
use crate::federated_table::FederatedTable;
use async_trait::async_trait;
use data_components::cdc::{ChangeEnvelope, ChangesStream, CommitChange, CommitError};
use data_components::dynamodb::Error;
use data_components::dynamodb::provider::DynamoDBTableProvider;
use data_components::dynamodb::stream::StreamError as DynamoDBStreamError;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use dynamodb_streams::{Checkpoint, Metrics, MetricsCollector};
use futures::stream::{self, StreamExt};
use opentelemetry::KeyValue;
use runtime_api_types::v1::ComponentType;
use runtime_checkpoint_api::CheckpointStore;
use runtime_metrics::component::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use runtime_parameters::ExposedParamLookup;
use snafu::ResultExt;
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use std::{any::Any, future::Future, pin::Pin, sync::Arc};
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

/// Behavior when the stream lag exceeds shard retention (`ShardNotFound` or `StreamBeyondRetention`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LagExceedsShardRetentionBehavior {
    /// Dataset is marked as Error state.
    #[default]
    Error,
    /// Dataset is marked Ready immediately, then re-bootstrapping happens.
    ReadyBeforeLoad,
    /// Dataset is marked Ready once re-bootstrapping is complete.
    ReadyAfterLoad,
}

impl FromStr for LagExceedsShardRetentionBehavior {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "error" => Ok(Self::Error),
            "ready_before_load" => Ok(Self::ReadyBeforeLoad),
            "ready_after_load" => Ok(Self::ReadyAfterLoad),
            _ => Err(format!(
                "Invalid lag_exceeds_shard_retention_behavior: '{s}'. Valid values: error, ready_before_load, ready_after_load"
            )),
        }
    }
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
    ParameterSpec::runtime("ready_lag")
        .description("When using Streams, once tables reaches this lag, it will be reported as Ready")
        .default("2s"),
    ParameterSpec::runtime("endpoint_url")
        .description("Custom endpoint URL for DynamoDB-compatible services (e.g., DynamoDB Local, ScyllaDB Alternator)."),
    ParameterSpec::runtime("lag_exceeds_shard_retention_behavior")
        .description("Behavior when stream lag exceeds shard retention (24h). 'error' marks dataset as Error, 'ready_before_load' marks Ready then re-bootstraps, 'ready_after_load' re-bootstraps then marks Ready")
        .default("error"),
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

        let ready_lag = self
            .params
            .get("ready_lag")
            .expose()
            .ok()
            .and_then(|v| fundu::parse_duration(v).ok())
            .unwrap_or(Duration::from_secs(2));

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
    ) -> Option<ChangesStream> {
        let dataset = dataset.clone();

        let lag_exceeds_behavior = match self
            .params
            .get("lag_exceeds_shard_retention_behavior")
            .expose()
        {
            ExposedParamLookup::Present(value_str) => {
                match LagExceedsShardRetentionBehavior::from_str(value_str) {
                    Ok(behavior) => behavior,
                    Err(e) => {
                        tracing::warn!(
                            dataset = %dataset.name,
                            error = %e,
                            "Failed to parse 'lag_exceeds_shard_retention_behavior' parameter. Defaulting to 'error'"
                        );
                        LagExceedsShardRetentionBehavior::default()
                    }
                }
            }
            ExposedParamLookup::Absent(_) => LagExceedsShardRetentionBehavior::default(),
        };

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

                let (should_bootstrap, checkpoint, checkpoint_updated_at) =
                    load_or_initialize_checkpoint(&dynamodb, &dynamodb_sys, &dataset_name).await?;

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
                        lag_exceeds_behavior,
                        metrics_collector,
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
            match serde_json::from_str::<Checkpoint>(&metadata.data) {
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
async fn create_bootstrap_stream(
    dynamodb: Arc<DynamoDBTableProvider>,
    dynamodb_sys: Arc<Option<DynamoDBSys>>,
    checkpoint: Checkpoint,
    acceptable_lag: Duration,
    dataset_name: TableReference,
) -> Option<ChangesStream> {
    tracing::info!(
        dataset = %dataset_name,
        ready_lag = %humantime::format_duration(acceptable_lag),
        "No existing checkpoint found for DynamoDB Streams table, starting initialization. Table will be marked as Ready once lag threshold is reached"
    );

    emit_overwrite_then_live(
        dynamodb,
        dynamodb_sys,
        checkpoint,
        acceptable_lag,
        dataset_name,
    )
    .await
}

/// Emits a full-overwrite bootstrap through the CDC change contract, then
/// transitions to the live changes stream from `checkpoint`.
///
/// The stream is: a `Truncate` barrier + the table scan as `op="c"` inserts
/// ([`DynamoDBTableProvider::overwrite_bootstrap_stream`]), then a zero-row
/// envelope whose committer persists `checkpoint` (the position captured
/// *before* the scan) once the snapshot is durably applied, then the live
/// changes from `checkpoint`. Committing on the post-snapshot envelope — not up
/// front — means a crash mid-bootstrap leaves no checkpoint, so the next start
/// re-bootstraps from scratch (the same at-least-once contract as the
/// `MySQL`/Postgres snapshots). This replaces the old direct `TableSink` overwrite,
/// so the connector needs no runtime accelerator-write internals. Readiness
/// continues to be driven by the live stream's watermark, preserving prior
/// behavior.
async fn emit_overwrite_then_live(
    dynamodb: Arc<DynamoDBTableProvider>,
    dynamodb_sys: Arc<Option<DynamoDBSys>>,
    checkpoint: Checkpoint,
    acceptable_lag: Duration,
    dataset_name: TableReference,
) -> Option<ChangesStream> {
    let table_schema = dynamodb.schema();

    let snapshot = match Arc::clone(&dynamodb).overwrite_bootstrap_stream().await {
        Ok(stream) => stream
            .map(|res| res.map(|batch| ChangeEnvelope::new(Box::new(NoOpCommitter), batch, false))),
        Err(e) => {
            tracing::error!(
                dataset = %dataset_name,
                error = %e,
                "Failed to start DynamoDB overwrite bootstrap stream"
            );
            return None;
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
        return None;
    };
    let checkpoint_envelope = ChangeEnvelope::from_parts(
        Box::new(DynamoDBStreamCommitter::new(
            Arc::clone(&dynamodb_sys),
            checkpoint.clone(),
        )),
        checkpoint_batch,
        false, // readiness comes from the live stream, preserving prior behavior
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
            return None;
        }
    };

    Some(
        snapshot
            .chain(stream::once(async move { Ok(checkpoint_envelope) }))
            .chain(live)
            .boxed(),
    )
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
    lag_exceeds_behavior: LagExceedsShardRetentionBehavior,
    metrics_collector: Arc<MetricsCollector>,
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
                                Err(DynamoDBStreamError::FailedToReceiveMessage {
                                    source: dynamodb_streams::Error::ShardNotFound,
                                }
                                .into())
                            })
                            .boxed(),
                        );
                    }

                    // Checkpoint is old enough (> 18h) - apply configured behavior
                    if lag_exceeds_behavior == LagExceedsShardRetentionBehavior::Error {
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
                        // ReadyBeforeLoad or ReadyAfterLoad - do rebootstrap
                        tracing::info!(
                            dataset = %dataset_name,
                            lag_age = %humantime::format_duration(checkpoint_age),
                            behavior = ?lag_exceeds_behavior,
                            "DynamoDB table lag references expired shard. Initiating table re-initialization"
                        );
                        rebootstrap_table(
                            &dynamodb,
                            &dynamodb_sys,
                            acceptable_lag,
                            &dataset_name,
                            lag_exceeds_behavior,
                            metrics_collector,
                        )
                        .await
                    }
                }
                Err(Error::FailedToInitializeCheckpoint {
                    source: dynamodb_streams::Error::StreamBeyondRetention,
                }) => {
                    // StreamBeyondRetention definitively means checkpoint is >24h old
                    if lag_exceeds_behavior == LagExceedsShardRetentionBehavior::Error {
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
                            &dynamodb_sys,
                            acceptable_lag,
                            &dataset_name,
                            lag_exceeds_behavior,
                            metrics_collector,
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

                ChangeEnvelope::new(
                    Box::new(DynamoDBStreamCommitter::new(
                        Arc::clone(&dynamodb_sys),
                        checkpoint,
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
    dynamodb_sys: &Arc<Option<DynamoDBSys>>,
    acceptable_lag: Duration,
    dataset_name: &TableReference,
    behavior: LagExceedsShardRetentionBehavior,
    metrics_collector: Arc<MetricsCollector>,
) -> Option<ChangesStream> {
    tracing::debug!(
        dataset = %dataset_name,
        behavior = ?behavior,
        "Initiating re-initialization for DynamoDB table"
    );

    // For ReadyBeforeLoad, return a stream that emits ready immediately, then does rebootstrap
    if behavior == LagExceedsShardRetentionBehavior::ReadyBeforeLoad {
        tracing::info!(
            dataset = %dataset_name,
            "DynamoDB table will be marked Ready before re-initialization (lag_exceeds_shard_retention_behavior=ready_before_load)"
        );

        // Create an empty change envelope to signal ready immediately
        let table_schema = dynamodb.schema();
        let ready_envelope = create_empty_ready_envelope(&table_schema)?;

        // Clone values needed for the async rebootstrap
        let dynamodb = Arc::clone(dynamodb);
        let dynamodb_sys = Arc::clone(dynamodb_sys);
        let dataset_name = dataset_name.clone();

        // Return stream: ready envelope first, then rebootstrap happens, then changes stream
        return Some(
            stream::once(async move { Ok(ready_envelope) })
                .chain(
                    stream::once(async move {
                        // Perform rebootstrap in this async block
                        do_rebootstrap(
                            &dynamodb,
                            &dynamodb_sys,
                            acceptable_lag,
                            &dataset_name,
                            metrics_collector,
                        )
                        .await
                    })
                    .filter_map(|opt| async move { opt })
                    .flatten(),
                )
                .boxed(),
        );
    }

    // ReadyAfterLoad: do rebootstrap, then return changes stream (ready based on lag)
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
    dynamodb_sys: &Arc<Option<DynamoDBSys>>,
    acceptable_lag: Duration,
    dataset_name: &TableReference,
    metrics_collector: Arc<MetricsCollector>,
) -> Option<ChangesStream> {
    // Capture a new global checkpoint FIRST (before the scan) so live changes
    // since the scan are re-delivered by the resumed stream, not missed.
    let new_checkpoint = match dynamodb.latest_global_checkpoint().await {
        Ok(cp) => cp,
        Err(e) => {
            tracing::error!(
                dataset = %dataset_name,
                error = ?e,
                "Failed to get new checkpoint for re-initialization"
            );
            return None;
        }
    };

    tracing::debug!(
        dataset = %dataset_name,
        shards = new_checkpoint.shards.len(),
        "Got new checkpoint for re-initialization of DynamoDB table"
    );

    let stream = emit_overwrite_then_live(
        Arc::clone(dynamodb),
        Arc::clone(dynamodb_sys),
        new_checkpoint,
        acceptable_lag,
        dataset_name.clone(),
    )
    .await?;

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

    let record_batch = RecordBatch::try_new(schema_ref, empty_arrays).ok()?;

    ChangeBatch::try_new(record_batch).ok()
}

/// Creates an empty `ChangeEnvelope` with `dataset_is_ready = true` to signal ready state.
fn create_empty_ready_envelope(
    table_schema: &arrow::datatypes::SchemaRef,
) -> Option<ChangeEnvelope> {
    let change_batch = empty_change_batch(table_schema)?;

    Some(ChangeEnvelope::new(
        Box::new(NoOpCommitter),
        change_batch,
        true,
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

struct NoOpCommitter;
#[async_trait]
impl CommitChange for NoOpCommitter {
    async fn commit(&self) -> Result<(), CommitError> {
        Ok(())
    }
}

pub struct DynamoDBStreamCommitter {
    dynamodb_sys: Arc<Option<DynamoDBSys>>,
    checkpoint: Checkpoint,
}

impl DynamoDBStreamCommitter {
    #[must_use]
    pub fn new(dynamodb_sys: Arc<Option<DynamoDBSys>>, checkpoint: Checkpoint) -> Self {
        Self {
            dynamodb_sys,
            checkpoint,
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

        match self.dynamodb_sys.as_ref() {
            Some(dynamodb_sys) => dynamodb_sys.upsert(&checkpoint_json).await.map_err(|e| {
                CommitError::UnableToCommitChange {
                    source: Box::new(e),
                }
            }),
            None => Ok(()),
        }
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
