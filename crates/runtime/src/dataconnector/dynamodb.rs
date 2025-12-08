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
    ParameterSpec, Parameters, parameters::aws::initiate_config_with_credentials,
};
use crate::component::ComponentType;
use crate::component::dataset::Dataset;
use crate::component::metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use crate::dataaccelerator::spice_sys::OpenOption;
use crate::dataaccelerator::spice_sys::dynamodb::{DynamoDBCheckpointMetadata, DynamoDBSys};
use crate::federated_table::FederatedTable;
use crate::register_data_connector;
use async_trait::async_trait;
use data_components::cdc::{ChangeEnvelope, ChangesStream, CommitChange, CommitError};
use data_components::dynamodb::provider::DynamoDBTableProvider;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use dynamodb_streams::{Checkpoint, Metrics, MetricsCollector};
use futures::stream::{self, StreamExt};
use opentelemetry::KeyValue;
use runtime_parameters::ExposedParamLookup;
use snafu::ResultExt;
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use std::{any::Any, future::Future, pin::Pin, sync::Arc};
use util::time_format::is_valid_format;

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

    #[expect(clippy::too_many_lines)]
    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> Result<Arc<dyn TableProvider>, DataConnectorError> {
        let table_name = dataset.path();

        let config = initiate_config_with_credentials(
            "DynamoDBTableProvider",
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
        })?
        .load()
        .await;

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
        )
        .await
        .map_err(|e| DataConnectorError::UnableToGetReadProvider {
            dataconnector: "dynamodb".to_string(),
            connector_component: ConnectorComponent::from(dataset),
            source: Box::new(e),
        })?;
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

        Some(Box::pin(
            stream::once(async move {
                let table_provider = federated_table.table_provider().await;

                let dynamodb_ref = table_provider
                    .as_any()
                    .downcast_ref::<DynamoDBTableProvider>()?;

                let acceptable_lag = dynamodb_ref.ready_lag;
                let dataset_name = dataset.name.clone();
                let dataset_name_2 = dataset_name.clone();
                let dataset_name_3 = dataset_name.clone();
                let dataset_name_4 = dataset_name.clone();
                let dynamodb = Arc::new(dynamodb_ref.clone());
                let dynamodb_sys = initialize_dynamodb_sys(&dataset).await?;

                let (should_bootstrap, checkpoint) =
                    load_or_initialize_checkpoint(&dynamodb, &dynamodb_sys, &dataset_name).await?;

                if should_bootstrap {
                    // Initialize bootstrap stream
                    let bootstrap_stream = Arc::clone(&dynamodb)
                        .bootstrap_stream()
                        .await
                        .ok()?
                        .map(move |msg| {
                            msg.map(|change_batch| {
                                tracing::info!("Bootstrapping DynamoDB table {}, records={}", dataset_name.clone(), change_batch.record.num_rows());
                                // Bootstrap stream doesn't commit changes and doesn't mark dataset as ready
                                ChangeEnvelope::new(Box::new(NoOpCommitter), change_batch, false)
                            })
                        });

                    let checkpoint_cloned = checkpoint.clone();
                    let dynamodb_sys_cloned = Arc::clone(&dynamodb_sys);

                    // Attach changes stream from initial checkpoint to bootstrap stream
                    Some(
                        bootstrap_stream
                            .chain(
                                stream::once(async move {
                                    tracing::info!("Bootstrapping DynamoDB table {} complete, starting changes stream. \
                                        Table will be marked as Ready once stream lag reaches < '{}'",
                                        dataset_name_2, humantime::format_duration(acceptable_lag));

                                    let committer = DynamoDBStreamCommitter::new(dynamodb_sys_cloned, checkpoint_cloned);
                                    if let Err(err) = committer.commit() {
                                        tracing::error!("Failed to commit bootstrap checkpoint: {:?}", err);
                                    }

                                    stream::empty()
                                })
                                .flatten()
                            )
                            .chain(
                                stream::once(changes_stream_from_checkpoint(
                                    Arc::clone(&dynamodb),
                                    Arc::clone(&dynamodb_sys),
                                    checkpoint,
                                    acceptable_lag,
                                    dataset_name_3.clone(),
                                ))
                                .filter_map(|opt| async move { opt })
                                .flatten(),
                            )
                            .boxed(),
                    )
                } else {
                    // Resume reading from a checkpoint
                    Some(
                        stream::once(changes_stream_from_checkpoint(
                            Arc::clone(&dynamodb),
                            Arc::clone(&dynamodb_sys),
                            checkpoint,
                            acceptable_lag,
                            dataset_name_4.clone(),
                        ))
                        .filter_map(|opt| async move { opt })
                        .flatten()
                        .boxed(),
                    )
                }
            })
            .flat_map(|opt| opt.unwrap_or_else(|| stream::empty().boxed())),
        ))
    }
}

async fn initialize_dynamodb_sys(dataset: &Dataset) -> Option<Arc<DynamoDBSys>> {
    match DynamoDBSys::try_new(dataset, OpenOption::OpenExisting).await {
        Ok(sys) => Some(Arc::new(sys)),
        Err(err) => {
            tracing::error!(
                "Failed to initialize DynamoDBSys for checkpoint persistence: table={} - {:?}",
                dataset.name,
                err
            );
            None
        }
    }
}

/// Loads checkpoint from `DynamoDBSys`, or initializes a new checkpoint if none exists.
async fn load_or_initialize_checkpoint(
    dynamodb: &Arc<DynamoDBTableProvider>,
    dynamodb_sys: &Arc<DynamoDBSys>,
    dataset_name: &TableReference,
) -> Option<(bool, Checkpoint)> {
    let existing_checkpoint = dynamodb_sys.get().await;

    if let Some(metadata) = existing_checkpoint {
        match serde_json::from_str::<Checkpoint>(&metadata.checkpoint_data) {
            Ok(checkpoint) => {
                tracing::info!(
                    "Found existing checkpoint for DynamoDB table {}, resuming from checkpoint",
                    dataset_name
                );
                Some((false, checkpoint))
            }
            Err(err) => {
                tracing::warn!(
                    "Failed to deserialize checkpoint, falling back to bootstrap: table_name={} - {:?}",
                    dataset_name,
                    err
                );
                get_latest_checkpoint(dynamodb).await.map(|cp| (true, cp))
            }
        }
    } else {
        tracing::info!(
            "No existing checkpoint found for table {}, starting from bootstrap",
            dataset_name
        );
        get_latest_checkpoint(dynamodb).await.map(|cp| (true, cp))
    }
}

async fn get_latest_checkpoint(dynamodb: &Arc<DynamoDBTableProvider>) -> Option<Checkpoint> {
    match dynamodb.latest_global_checkpoint().await {
        Ok(checkpoint) => Some(checkpoint),
        Err(err) => {
            tracing::error!(
                "Failed to get latest global checkpoint for DynamoDB Stream: {:?}",
                err
            );
            None
        }
    }
}

async fn changes_stream_from_checkpoint(
    dynamodb: Arc<DynamoDBTableProvider>,
    dynamodb_sys: Arc<DynamoDBSys>,
    checkpoint: Checkpoint,
    acceptable_lag: Duration,
    dataset_name: TableReference,
) -> Option<ChangesStream> {
    tracing::debug!(
        "Starting DynamoDB stream from checkpoint: table_name={}, checkpoint={:?}",
        dataset_name,
        checkpoint,
    );

    match dynamodb.stream_from_checkpoint(checkpoint).await {
        Ok(stream) => Some(
            stream
                .map(move |msg| {
                    msg.map(|(change_batch, checkpoint, watermark)| {
                        let lag = watermark
                            .and_then(|v| SystemTime::now().duration_since(v).ok());

                        tracing::debug!(
                            "Processing DynamoDB Streams batch: table_name={}, watermark={}, lag={}, shards={}, records={}",
                            dataset_name,
                            watermark
                                .map_or_else(|| "-".to_string(), |w| humantime::format_rfc3339(w).to_string()),
                            lag
                                .map_or_else(|| "-".to_string(), |l| humantime::format_duration(l).to_string()),
                            checkpoint.shards.len(),
                            change_batch.record.num_rows(),
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
                .boxed(),
        ),
        Err(err) => {
            tracing::error!(
                "Failed to get stream from checkpoint for DynamoDB Table: {} - {}",
                dataset_name,
                err
            );
            None
        }
    }
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
            _ => None,
        }
    }
}

struct NoOpCommitter;
impl CommitChange for NoOpCommitter {
    fn commit(&self) -> Result<(), CommitError> {
        Ok(())
    }
}

pub struct DynamoDBStreamCommitter {
    dynamodb_sys: Arc<DynamoDBSys>,
    checkpoint: Checkpoint,
}

impl DynamoDBStreamCommitter {
    #[must_use]
    pub fn new(dynamodb_sys: Arc<DynamoDBSys>, checkpoint: Checkpoint) -> Self {
        Self {
            dynamodb_sys,
            checkpoint,
        }
    }
}

impl CommitChange for DynamoDBStreamCommitter {
    fn commit(&self) -> Result<(), CommitError> {
        tracing::trace!("Committing DynamoDB checkpoint: {:?}", self.checkpoint);

        let checkpoint_json = serde_json::to_string(&self.checkpoint).map_err(|e| {
            CommitError::UnableToCommitChange {
                source: Box::new(e),
            }
        })?;

        let metadata = DynamoDBCheckpointMetadata {
            checkpoint_data: checkpoint_json,
        };

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.dynamodb_sys.upsert(&metadata).await.map_err(|e| {
                    CommitError::UnableToCommitChange {
                        source: Box::new(e),
                    }
                })
            })
        })
    }
}

register_data_connector!("dynamodb", DynamoDBFactory);
