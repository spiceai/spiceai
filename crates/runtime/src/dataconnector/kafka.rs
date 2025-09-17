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

use std::{any::Any, pin::Pin, sync::Arc};

use arrow_schema::SchemaRef;
use async_stream::stream;
use data_components::{
    cdc::ChangesStream,
    kafka::{KafkaConfig, KafkaConsumer, KafkaMetrics},
};
use dataformat_json::{SpiceJsonOptions, unnest_struct_schema};
use datafusion::catalog::TableProvider;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tonic::async_trait;

use crate::{
    component::{
        ComponentType,
        dataset::{Dataset, acceleration::RefreshMode},
        metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback},
    },
    dataaccelerator::spice_sys::kafka::KafkaSys,
    dataconnector::{
        ConnectorComponent, DataConnector, DataConnectorFactory, parameters::ConnectorParams,
    },
    datafusion::refresh_sql,
    federated_table::FederatedTable,
    parameters::{ExposedParamLookup, ParameterSpec, Parameters},
};

/// Default max records to scan to infer the schema
pub const DEFAULT_SCHEMA_INFER_MAX_RECORD: usize = 1;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: 'kafka_bootstrap_servers'. Specify a value. For details, visit: https://spiceai.org/docs/components/data-connectors/kafka#parameters"
    ))]
    MissingKafkaBootstrapServers,

    #[snafu(display("Invalid configuration: {msg}"))]
    InvalidConfiguration { msg: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Kafka {
    kafka_config: KafkaConfig,
    json_options: Arc<SpiceJsonOptions>,
}

impl Kafka {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(params: Parameters) -> Result<Self> {
        let kafka_config = KafkaConfig {
            brokers: params
                .get("bootstrap_servers")
                .expose()
                .ok()
                .context(MissingKafkaBootstrapServersSnafu)?
                .to_string(),
            security_protocol: params
                .get("security_protocol")
                .expose()
                .ok()
                .unwrap_or("sasl_ssl")
                .to_string(),
            sasl_mechanism: params
                .get("sasl_mechanism")
                .expose()
                .ok()
                .unwrap_or("SCRAM-SHA-512")
                .to_string(),
            sasl_username: params
                .get("sasl_username")
                .expose()
                .ok()
                .map(ToString::to_string),
            sasl_password: params
                .get("sasl_password")
                .expose()
                .ok()
                .map(ToString::to_string),
            ssl_ca_location: params
                .get("ssl_ca_location")
                .expose()
                .ok()
                .map(ToString::to_string),
            enable_ssl_certificate_verification: params
                .get("enable_ssl_certificate_verification")
                .expose()
                .ok()
                .unwrap_or("true")
                .to_string()
                .parse()
                .unwrap_or(true),
            ssl_endpoint_identification_algorithm: params
                .get("ssl_endpoint_identification_algorithm")
                .expose()
                .ok()
                .unwrap_or("https")
                .try_into()
                .unwrap_or_else(|()| {
                    tracing::warn!("Invalid value for 'kafka_ssl_endpoint_identification_algorithm'. Supported values: 'none', 'https'. Defaulting to 'https'.");
                    data_components::kafka::SslIdentification::Https
                }),
            consumer_group_id: params
                .get("consumer_group_id")
                .expose()
                .ok()
                .map(ToString::to_string),
            // Metrics instance that will be used by the Kafka consumer to update statistics
            metrics_store: Some(Arc::new(KafkaMetrics::new())),
        };

        Ok(Self {
            kafka_config,
            json_options: get_json_format(&params)?,
        })
    }
}

/// Returns a [`SpiceJsonOptions`] based on the provided [`Datasets`] parameters.
///
/// If the [`Dataset`] has the relevant parameter, return an error if the value is invalid.
fn get_json_format(params: &Parameters) -> Result<Arc<SpiceJsonOptions>> {
    let mut options = SpiceJsonOptions::default();

    if let ExposedParamLookup::Present(infer_max_rec_str) =
        params.get("schema_infer_max_records").expose()
    {
        let Ok(schema_infer_max_rec) = infer_max_rec_str.parse() else {
            return Err(Error::InvalidConfiguration {
                msg: format!(
                    "parameter 'schema_infer_max_records' must be an integer, not {infer_max_rec_str}"
                ),
            });
        };
        options.schema_infer_max_rec = Some(schema_infer_max_rec);
    }

    if let ExposedParamLookup::Present(flatten_json) = params.get("flatten_json").expose() {
        if flatten_json.eq_ignore_ascii_case("true") {
            options.flatten_json = Some(".".to_string());
        }
    }

    Ok(Arc::new(options))
}

#[derive(Default, Debug, Copy, Clone)]
pub struct KafkaFactory {}

impl KafkaFactory {
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
    ParameterSpec::component("bootstrap_servers")
        .required()
        .description(
            "A list of host/port pairs for establishing the initial Kafka cluster connection.",
        ),
    ParameterSpec::component("security_protocol")
        .default("sasl_ssl")
        .description("Security protocol for Kafka connections. Default: 'sasl_ssl'. Options: 'plaintext', 'ssl', 'sasl_plaintext', 'sasl_ssl'."),
    ParameterSpec::component("sasl_mechanism")
        .default("SCRAM-SHA-512")
        .description("SASL authentication mechanism. Default: 'SCRAM-SHA-512'. Options: 'PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'."),
    ParameterSpec::component("sasl_username")
        .secret()
        .description("SASL username."),
    ParameterSpec::component("sasl_password")
        .secret()
        .description("SASL password."),
    ParameterSpec::component("ssl_ca_location")
        .secret()
        .description("Path to the SSL/TLS CA certificate file for server verification."),
    ParameterSpec::component("enable_ssl_certificate_verification")
        .default("true")
        .description("Enable SSL/TLS certificate verification. Default: 'true'.")
        .is_boolean(),
    ParameterSpec::component("ssl_endpoint_identification_algorithm")
        .default("https")
        .description("SSL/TLS endpoint identification algorithm. Default: 'https'. Options: 'none', 'https'.")
        .one_of(&["none", "https"]),
    ParameterSpec::runtime("schema_infer_max_records")
        .default("1")
        .description("Number of Kafka messages to sample for schema inference. Default: '1'. Increase if your data has optional fields or varying structure."),
    ParameterSpec::runtime("flatten_json")
        .description("Set true to flatten nested structs in JSON as separate columns.")
        .is_boolean(),
    ParameterSpec::component("consumer_group_id")
        .description("Kafka consumer group id to use for this dataset. If not set, a unique id will be generated."),
];

impl DataConnectorFactory for KafkaFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let kafka = Kafka::new(params.parameters)?;
            Ok(Arc::new(kafka) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "kafka"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }

    fn supports_unsupported_type_action(&self) -> bool {
        false
    }

    fn reserved_keywords(&self) -> &'static [&'static str] {
        &[]
    }
}

#[async_trait]
impl DataConnector for Kafka {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        ensure!(
            dataset.is_accelerated(),
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "kafka",
                message: "The Kafka data connector requires an accelerated dataset. For details, visit: https://spiceai.org/docs/components/data-connectors/kafka",
                connector_component: ConnectorComponent::from(dataset),
            }
        );
        let Some(ref acceleration) = dataset.acceleration else {
            unreachable!("Dataset acceleration already verified. This should never be None here.");
        };
        ensure!(
            acceleration.refresh_mode == Some(RefreshMode::Append),
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "kafka",
                message: "The Kafka connector is only compatible with refresh mode 'append'. For details, visit: https://spiceai.org/docs/components/data-connectors/kafka",
                connector_component: ConnectorComponent::from(dataset),
            }
        );

        let dataset_name = dataset.name.to_string();

        let topic = dataset.path();

        let (kafka_consumer, schema) =
            init_kafka_consumer(dataset, topic, &self.kafka_config, &self.json_options).await?;

        let refresh_sql = dataset.refresh_sql();
        let schema = if let Some(refresh_sql) = &refresh_sql {
            refresh_sql::validate_refresh_sql(dataset.name.clone(), refresh_sql.as_str(), schema)
                .boxed()
                .map_err(|e| super::DataConnectorError::InvalidConfiguration {
                    dataconnector: "kafka".to_string(),
                    message: format!("The refresh SQL is invalid: {e}"),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                })?
        } else {
            schema
        };

        if !dataset.is_file_accelerated() {
            tracing::warn!(
                "Dataset {dataset_name} is not file accelerated. This may result in full message replay from Kafka on restarts. It is recommended to use file acceleration with the Kafka connector for optimal performance. For details, visit: https://spiceai.org/docs/components/data-connectors/kafka",
            );
        }

        Ok(Arc::new(
            data_components::kafka::Kafka::new(schema, kafka_consumer)
                .with_flatten_json(self.json_options.flatten_json.clone()),
        ))
    }

    fn supports_append_stream(&self) -> bool {
        true
    }

    fn append_stream(&self, federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        Some(Box::pin(stream! {
            let table_provider = federated_table.table_provider().await;
            let Some(kafka) = table_provider.as_any().downcast_ref::<data_components::kafka::Kafka>() else {
                return;
            };

            let mut changes_stream = kafka.stream_changes();

            while let Some(item) = changes_stream.next().await {
                yield item;
            }
        }))
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        if let Some(metrics) = self.kafka_config.metrics_store.as_ref() {
            Some(Arc::new(KafkaMetricsProvider::new(Arc::clone(metrics))))
        } else {
            None
        }
    }
}

async fn init_kafka_consumer(
    dataset: &Dataset,
    topic: &str,
    kafka_config: &KafkaConfig,
    json_options: &Arc<SpiceJsonOptions>,
) -> super::DataConnectorResult<(KafkaConsumer, SchemaRef)> {
    let Some(metadata) = get_metadata_from_accelerator(dataset).await else {
        return bootstrap_new_kafka_consumer(dataset, topic, kafka_config, json_options).await;
    };

    ensure!(
        topic == metadata.topic,
        super::InvalidConfigurationNoSourceSnafu {
            dataconnector: "kafka",
            message: format!(
                "Locally accelerated data belongs to a different Kafka topic (was '{}', now '{topic}'). Remove the acceleration file or rename the dataset to proceed.",
                metadata.topic
            ),
            connector_component: ConnectorComponent::from(dataset),
        }
    );

    if let Some(ref group_id) = kafka_config.consumer_group_id {
        ensure!(
            group_id == &metadata.consumer_group_id,
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "kafka",
                message: format!(
                    "Locally accelerated data belongs to a different Kafka consumer group (was '{}', now '{group_id}'). Remove the acceleration file or rename the dataset to proceed.",
                    metadata.consumer_group_id
                ),
                connector_component: ConnectorComponent::from(dataset),
            }
        );
    }

    let kafka_consumer =
        KafkaConsumer::create_with_existing_group_id(&metadata.consumer_group_id, kafka_config)
            .boxed()
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "kafka",
                connector_component: ConnectorComponent::from(dataset),
            })?;

    kafka_consumer
        .subscribe(topic)
        .boxed()
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "kafka",
            connector_component: ConnectorComponent::from(dataset),
        })?;

    Ok((kafka_consumer, metadata.schema))
}

#[derive(Serialize, Deserialize)]
pub(crate) struct KafkaMetadata {
    pub(crate) consumer_group_id: String,
    pub(crate) topic: String,
    pub(crate) schema: SchemaRef,
}

async fn get_metadata_from_accelerator(dataset: &Dataset) -> Option<KafkaMetadata> {
    let kafka_sys = KafkaSys::try_new(dataset).await.ok()?;
    kafka_sys.get().await
}

async fn set_metadata_to_accelerator(
    dataset: &Dataset,
    metadata: &KafkaMetadata,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let debezium_kafka_sys = KafkaSys::try_new_create_if_not_exists(dataset).await?;
    debezium_kafka_sys.upsert(metadata).await
}

async fn bootstrap_new_kafka_consumer(
    dataset: &Dataset,
    topic: &str,
    kafka_config: &KafkaConfig,
    json_options: &Arc<SpiceJsonOptions>,
) -> super::DataConnectorResult<(KafkaConsumer, SchemaRef)> {
    let dataset_name = dataset.name.to_string();
    let kafka_consumer = KafkaConsumer::create_for_dataset(
        &dataset_name,
        kafka_config.consumer_group_id.clone(),
        kafka_config,
    )
    .boxed()
    .context(super::UnableToGetReadProviderSnafu {
        dataconnector: "kafka",
        connector_component: ConnectorComponent::from(dataset),
    })?;

    kafka_consumer
        .subscribe(topic)
        .boxed()
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "kafka",
            connector_component: ConnectorComponent::from(dataset),
        })?;

    let schema_inference_sample_count = json_options
        .schema_infer_max_rec
        .unwrap_or(DEFAULT_SCHEMA_INFER_MAX_RECORD);

    // Read schema_inference_sample_count messages to infer schema
    // this is useful when some of the fields could be optional and use 'null'
    let mut sample_values = Vec::with_capacity(schema_inference_sample_count);

    for _ in 0..schema_inference_sample_count {
        match kafka_consumer
            .next_json::<serde_json::Value, serde_json::Value>()
            .await
        {
            Ok(Some(msg)) => sample_values.push(msg),
            Ok(None) => {
                return Err(super::DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "kafka".to_string(),
                    source: "No message received from Kafka.".into(),
                    connector_component: ConnectorComponent::from(dataset),
                });
            }
            Err(e) => {
                return Err(e).boxed().context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "kafka",
                    connector_component: ConnectorComponent::from(dataset),
                });
            }
        }
    }

    let value_iter = sample_values.into_iter().map(|v| Ok(v.value().clone()));

    // Infer Arrow schema from the JSON value in the message
    let schema = datafusion::arrow::json::reader::infer_json_schema_from_iterator(value_iter)
        .map_err(|e| super::DataConnectorError::UnableToGetReadProvider {
            dataconnector: "kafka".to_string(),
            source: format!("Failed to infer schema from Kafka message: {e}").into(),
            connector_component: ConnectorComponent::from(dataset),
        })
        .map(|schema| {
            // If flatten_json is set, unnest the schema
            if let Some(separator) = &json_options.flatten_json {
                unnest_struct_schema(&schema, separator)
            } else {
                schema
            }
        })?
        .into();

    let metadata = KafkaMetadata {
        consumer_group_id: kafka_consumer.group_id().to_string(),
        topic: topic.to_string(),
        schema: Arc::clone(&schema),
    };

    if dataset.is_file_accelerated() {
        set_metadata_to_accelerator(dataset, &metadata)
            .await
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "kafka",
                connector_component: ConnectorComponent::from(dataset),
            })?;
    }

    // Restart the stream from the beginning
    kafka_consumer
        .restart_topic(topic)
        .boxed()
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "kafka",
            connector_component: ConnectorComponent::from(dataset),
        })?;

    Ok((kafka_consumer, schema))
}

#[derive(Debug, Clone)]
pub(crate) struct KafkaMetricsProvider {
    metrics: Arc<KafkaMetrics>,
}

impl KafkaMetricsProvider {
    pub(crate) fn new(metrics: Arc<KafkaMetrics>) -> Self {
        Self { metrics }
    }
}

const METRICS: &[MetricSpec] = &[
    MetricSpec {
        name: "records_consumed_total",
        description: Some("Total number of records consumed"),
        unit: Some("records"),
        metric_type: MetricType::ObservableCounterU64,
    },
    MetricSpec {
        name: "bytes_consumed_total",
        description: Some("Total bytes consumed"),
        unit: Some("bytes"),
        metric_type: MetricType::ObservableCounterU64,
    },
    MetricSpec {
        name: "records_lag",
        description: Some("Total consumer lag across all partitions"),
        unit: Some("records"),
        metric_type: MetricType::ObservableGaugeU64,
    },
];

impl MetricsProvider for KafkaMetricsProvider {
    fn component_type(&self) -> ComponentType {
        ComponentType::Dataset
    }

    fn component_name(&self) -> &'static str {
        "kafka"
    }

    fn available_metrics(&self) -> &'static [MetricSpec] {
        METRICS
    }

    fn callback_to_observe_metric(
        &self,
        metric: &MetricSpec,
        attributes: Vec<opentelemetry::KeyValue>,
    ) -> Option<ObserveMetricCallback> {
        match metric.name {
            "records_consumed_total" => {
                let metrics = Arc::clone(&self.metrics);
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics
                            .records_consumed
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "bytes_consumed_total" => {
                let metrics = Arc::clone(&self.metrics);
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics
                            .bytes_consumed
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "records_lag" => {
                let metrics = Arc::clone(&self.metrics);
                Some(ObserveMetricCallback::U64(Box::new(move |observer| {
                    observer.observe(
                        metrics
                            .records_lag
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            _ => None,
        }
    }
}
