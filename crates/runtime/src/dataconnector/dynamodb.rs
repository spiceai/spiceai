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
use crate::component::dataset::acceleration::RefreshMode;
use crate::component::metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use crate::dataaccelerator::spice_sys::OpenOption;
use crate::dataaccelerator::spice_sys::dynamodb::{DynamoDBCheckpointMetadata, DynamoDBSys};
use crate::federated_table::FederatedTable;
use crate::register_data_connector;
use async_trait::async_trait;
use data_components::cdc::{ChangeEnvelope, ChangesStream, CommitChange, CommitError};
use data_components::dynamodb::provider::DynamoDBTableProvider;
use data_components::dynamodb::{Error, JsonNesting};
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use dynamodb_streams::{Checkpoint, Metrics, MetricsCollector};
use futures::stream::{self, StreamExt};
use opentelemetry::KeyValue;
use runtime_parameters::ExposedParamLookup;
use serde_json::Value;
use snafu::ResultExt;
use spicepod::semantic::Column;
use std::collections::HashSet;
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
    ParameterSpec::runtime("endpoint_url")
        .description("Custom endpoint URL for testing or using DynamoDB-compatible services (e.g., DynamoDB Local).")
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

fn parse_json_nesting_static_fields(
    dataset: &Dataset,
) -> Result<Option<JsonNesting>, DataConnectorError> {
    // Find all columns that have json_object metadata defined
    let json_object_columns: Vec<&Column> = dataset
        .columns
        .iter()
        .filter(|col| col.metadata.contains_key("json_object"))
        .collect();

    // No json_object columns means no JSON nesting configuration
    if json_object_columns.is_empty() {
        return Ok(None);
    }

    // Error if multiple columns have json_object defined
    if json_object_columns.len() > 1 {
        let column_names: Vec<&str> = json_object_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: "dynamodb".to_string(),
            message: format!(
                "Multiple columns have 'json_object' metadata defined: {}. Only one column can be configured as a JSON object column.",
                column_names.join(", ")
            ),
            connector_component: ConnectorComponent::from(dataset),
        });
    }

    let json_column = json_object_columns[0];
    let Some(json_object_value) = json_column.metadata.get("json_object") else {
        unreachable!("json_object key existence was checked above")
    };

    // Validate that json_object value is "*"
    let is_wildcard = match json_object_value {
        Value::String(s) => s == "*",
        _ => false,
    };

    if !is_wildcard {
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: "dynamodb".to_string(),
            message: format!(
                "Column '{}' has invalid 'json_object' value: {:?}. Only '*' is supported.",
                json_column.name, json_object_value
            ),
            connector_component: ConnectorComponent::from(dataset),
        });
    }

    // Collect all other columns as static fields
    let static_fields: HashSet<String> = dataset
        .columns
        .iter()
        .filter(|col| col.name != json_column.name)
        .map(|col| col.name.clone())
        .collect();

    Ok(Some(JsonNesting {
        static_fields,
        json_field_name: json_column.name.clone(),
    }))
}

#[async_trait]
impl DataConnector for DynamoDB {
    fn as_any(&self) -> &dyn Any {
        self
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
                "DynamoDB dataset {} is configured for changes stream, but acceleration is disabled. Enable acceleration to use DynamoDB Streams",
                dataset.name
            );
        }

        let table_name = dataset.path();

        let mut config_loader = initiate_config_with_credentials(
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
            parse_json_nesting_static_fields(dataset)?.as_ref(),
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
                let dynamodb_sys = Arc::new(if dataset.is_file_accelerated() {
                    initialize_dynamodb_sys(&dataset).await
                } else {
                    tracing::warn!("Dataset {dataset_name} is not file-accelerated. DynamoDB Streams checkpoints will not be persisted.");
                    None
                });

                let (should_bootstrap, checkpoint) =
                    load_or_initialize_checkpoint(&dynamodb, &dynamodb_sys, &dataset_name).await?;

                if should_bootstrap {
                    tracing::info!(
                        "No existing checkpoint found for table {dataset_name}, starting from bootstrap"
                    );
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
                    tracing::info!(
                        "Found existing checkpoint for DynamoDB table {dataset_name}, resuming from checkpoint. Table will be marked as Ready once stream lag reaches < '{}'",
                        humantime::format_duration(acceptable_lag),
                    );
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

async fn initialize_dynamodb_sys(dataset: &Dataset) -> Option<DynamoDBSys> {
    match DynamoDBSys::try_new(dataset, OpenOption::OpenExisting).await {
        Ok(sys) => Some(sys),
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
    dynamodb_sys: &Arc<Option<DynamoDBSys>>,
    dataset_name: &TableReference,
) -> Option<(bool, Checkpoint)> {
    if let Some(ref dynamodb_sys) = **dynamodb_sys {
        if let Some(metadata) = dynamodb_sys.get().await {
            match serde_json::from_str::<Checkpoint>(&metadata.checkpoint_data) {
                Ok(checkpoint) => Some((false, checkpoint)),
                Err(err) => {
                    tracing::warn!(
                        "Failed to deserialize checkpoint, falling back to bootstrap: table_name={} - {:?}",
                        dataset_name,
                        err
                    );
                    get_latest_checkpoint(dynamodb, dataset_name)
                        .await
                        .map(|cp| (true, cp))
                }
            }
        } else {
            get_latest_checkpoint(dynamodb, dataset_name)
                .await
                .map(|cp| (true, cp))
        }
    } else {
        get_latest_checkpoint(dynamodb, dataset_name)
            .await
            .map(|cp| (true, cp))
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
                    "Failed to initialize DynamoDB Stream for dataset {dataset_name}: {e}"
                );
            } else {
                tracing::error!(
                    "Failed to initialize DynamoDB Stream checkpoint for dataset {dataset_name}: {err}",
                );
            }

            None
        }
    }
}

async fn changes_stream_from_checkpoint(
    dynamodb: Arc<DynamoDBTableProvider>,
    dynamodb_sys: Arc<Option<DynamoDBSys>>,
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

        match self.dynamodb_sys.as_ref() {
            Some(dynamodb_sys) => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    dynamodb_sys.upsert(&metadata).await.map_err(|e| {
                        CommitError::UnableToCommitChange {
                            source: Box::new(e),
                        }
                    })
                })
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
    async fn test_no_json_object_columns_returns_none() {
        let dataset = test_dataset(vec![
            Column::new("PK"),
            Column::new("SK"),
            Column::new("Data"),
        ])
        .await;

        let result = parse_json_nesting_static_fields(&dataset).expect("should return Ok");
        assert!(result.is_none());
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

        let result = parse_json_nesting_static_fields(&dataset)
            .expect("should return Ok")
            .expect("should return Some");

        assert_eq!(result.json_field_name, "data_json");
        assert_eq!(result.static_fields.len(), 3);
        assert!(result.static_fields.contains("PK"));
        assert!(result.static_fields.contains("SK"));
        assert!(result.static_fields.contains("Baz"));
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

        let result = parse_json_nesting_static_fields(&dataset);
        assert!(result.is_err());

        let err = result
            .expect_err("should fail when multiple json_object columns defined")
            .to_string();
        assert!(err.contains("Multiple columns"));
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

        let result = parse_json_nesting_static_fields(&dataset);
        assert!(result.is_err());

        let err = result
            .expect_err("should fail when invalid value")
            .to_string();
        assert!(err.contains("invalid 'json_object' value"));
        assert!(err.contains("Only '*' is supported"));
    }
}
