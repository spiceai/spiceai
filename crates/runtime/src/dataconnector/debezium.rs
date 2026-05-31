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

use super::{ConnectorParams, DataConnector, DataConnectorFactory, ParameterSpec, Parameters};
use crate::component::dataset::Dataset;
use crate::component::dataset::acceleration::{Engine, RefreshMode};
use crate::component::metrics::MetricsProvider;
use crate::dataaccelerator::spice_sys::{self, OpenOption, debezium_kafka::DebeziumKafkaSys};
use crate::dataconnector::{
    ConnectorComponent,
    kafka::{SidecarOffsetCommitHook, SidecarOffsetStore},
};
use crate::datafusion::refresh_sql;
use crate::federated_table::FederatedTable;
use arrow::datatypes::SchemaRef;
use async_stream::stream;
use async_trait::async_trait;
use data_components::cdc::ChangesStream;
use data_components::debezium::change_event::{ChangeEvent, ChangeEventKey};
use data_components::debezium::{self, change_event};
use data_components::debezium_kafka::DebeziumKafka;
use data_components::kafka::{KafkaConfig, KafkaConsumer, KafkaMetrics, KafkaOffset};
use datafusion::datasource::TableProvider;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

const SCHEMA_INFERENCE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Invalid value for 'debezium_transport': {transport}. Supported values: 'kafka' For details, visit: https://spiceai.org/docs/components/data-connectors/debezium#parameters"
    ))]
    InvalidTransport { transport: String },

    #[snafu(display(
        "Invalid value for 'debezium_message_format': {format}. Supported values: 'json' For details, visit: https://spiceai.org/docs/components/data-connectors/debezium#parameters"
    ))]
    InvalidMessageFormat { format: String },

    #[snafu(display(
        "Missing required parameter: 'debezium_kafka_bootstrap_servers'. Specify a value. For details, visit: https://spiceai.org/docs/components/data-connectors/debezium#parameters"
    ))]
    MissingKafkaBootstrapServers,

    #[snafu(display("Failed to generate Debezium refresh SQL: {source}"))]
    RefreshSql { source: refresh_sql::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Debezium {
    kafka_config: KafkaConfig,
    batching: (usize, Duration),
    schema_evolution: bool,
}

impl Debezium {
    #[expect(clippy::needless_pass_by_value)]
    pub fn new(params: Parameters) -> Result<Self> {
        let transport = params.get("transport").expose().ok().unwrap_or("kafka");

        let message_format = params.get("message_format").expose().ok().unwrap_or("json");

        if transport != "kafka" {
            return InvalidTransportSnafu {
                transport: transport.to_string(),
            }
            .fail();
        }
        if message_format != "json" {
            return InvalidMessageFormatSnafu {
                format: message_format.to_string(),
            }
            .fail();
        }

        let kafka_config = KafkaConfig {
            brokers: params
                .get("kafka_bootstrap_servers")
                .expose()
                .ok()
                .context(MissingKafkaBootstrapServersSnafu)?
                .to_string(),
            security_protocol: params
                .get("kafka_security_protocol")
                .expose()
                .ok()
                .unwrap_or("sasl_ssl")
                .to_string(),
            sasl_mechanism: params
                .get("kafka_sasl_mechanism")
                .expose()
                .ok()
                .unwrap_or("SCRAM-SHA-512")
                .to_string(),
            sasl_username: params
                .get("kafka_sasl_username")
                .expose()
                .ok()
                .map(ToString::to_string),
            sasl_password: params
                .get("kafka_sasl_password")
                .expose()
                .ok()
                .map(ToString::to_string),
            ssl_ca_location: params
                .get("kafka_ssl_ca_location")
                .expose()
                .ok()
                .map(ToString::to_string),
            enable_ssl_certificate_verification: params
                .get("kafka_enable_ssl_certificate_verification")
                .expose()
                .ok()
                .unwrap_or("true")
                .to_string()
                .parse()
                .unwrap_or(true),
            ssl_endpoint_identification_algorithm: params
                .get("kafka_ssl_endpoint_identification_algorithm")
                .expose()
                .ok()
                .unwrap_or("https")
                .try_into()
                .unwrap_or_else(|()| {
                    tracing::warn!("Invalid value for 'kafka_ssl_endpoint_identification_algorithm'. Supported values: 'none', 'https'. Defaulting to 'https'.");
                    data_components::kafka::SslIdentification::Https
                }),
            consumer_group_id: params
                .get("kafka_consumer_group_id")
                .expose()
                .ok()
                .map(ToString::to_string),
            // Metrics instance that will be used by the Kafka consumer to update statistics
            metrics_store: Some(Arc::new(KafkaMetrics::new())),
        };

        let batch_max_size = params
            .get("batch_max_size")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(10000);

        let batch_max_duration = params
            .get("batch_max_duration")
            .expose()
            .ok()
            .and_then(|v| fundu::parse_duration(v).ok())
            .unwrap_or(Duration::from_secs(1));

        let schema_evolution = params
            .get("schema_evolution")
            .expose()
            .ok()
            .unwrap_or("false")
            .to_string()
            .parse()
            .unwrap_or(false);

        Ok(Self {
            kafka_config,
            batching: (batch_max_size, batch_max_duration),
            schema_evolution,
        })
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct DebeziumFactory {}

impl DebeziumFactory {
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
    ParameterSpec::component("transport")
        .required()
        .default("kafka")
        .description("The message broker transport to use. The default is kafka."),
    ParameterSpec::component("message_format")
        .required()
        .default("json")
        .description("The message format to use. The default is json."),
    ParameterSpec::runtime("kafka_bootstrap_servers")
        .required()
        .description(
            "A list of host/port pairs for establishing the initial Kafka cluster connection.",
        ),
     ParameterSpec::runtime("kafka_security_protocol")
        .default("sasl_ssl")
        .description("Security protocol for Kafka connections. Default: 'sasl_ssl'. Options: 'plaintext', 'ssl', 'sasl_plaintext', 'sasl_ssl'."),
    ParameterSpec::runtime("kafka_sasl_mechanism")
        .default("SCRAM-SHA-512")
        .description("SASL authentication mechanism. Default: 'SCRAM-SHA-512'. Options: 'PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'."),
    ParameterSpec::runtime("kafka_sasl_username")
        .secret()
        .description("SASL username."),
    ParameterSpec::runtime("kafka_sasl_password")
        .secret()
        .description("SASL password."),
    ParameterSpec::runtime("kafka_ssl_ca_location")
        .secret()
        .description("Path to the SSL/TLS CA certificate file for server verification."),
    ParameterSpec::runtime("kafka_enable_ssl_certificate_verification")
        .default("true")
        .description("Enable SSL/TLS certificate verification. Default: 'true'."),
    ParameterSpec::runtime("kafka_ssl_endpoint_identification_algorithm")
        .default("https")
        .description("SSL/TLS endpoint identification algorithm. Default: 'https'. Options: 'none', 'https'."),
    ParameterSpec::runtime("kafka_consumer_group_id")
        .description("Kafka consumer group id to use for this dataset. If not set, a unique id will be generated."),
    ParameterSpec::runtime("batch_max_size")
        .description("Maximum number of change events to batch together before processing")
        .default("10000"),
    ParameterSpec::runtime("batch_max_duration")
        .description("Maximum time to wait for a batch to fill before processing")
        .default("1s"),
    ParameterSpec::runtime("schema_evolution")
        .default("false")
        .description("Enable automatic schema evolution detection on reload. When true, the connector peeks at the latest Kafka message to detect schema changes. Default: false."),
];

impl DataConnectorFactory for DebeziumFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let debezium = Debezium::new(params.parameters)?;
            Ok(Arc::new(debezium) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "debezium"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

register_data_connector!("debezium", DebeziumFactory);

#[async_trait]
impl DataConnector for Debezium {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn resolve_refresh_mode(&self, refresh_mode: Option<RefreshMode>) -> RefreshMode {
        refresh_mode.unwrap_or(RefreshMode::Changes)
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let Some(acceleration) = dataset
            .acceleration
            .as_ref()
            .filter(|acceleration| acceleration.enabled)
        else {
            return super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "debezium",
                message: "The Debezium data connector requires an accelerated dataset. For details, visit: https://spiceai.org/docs/components/data-connectors/debezium",
                connector_component: ConnectorComponent::from(dataset),
            }
            .fail();
        };

        ensure!(
            self.resolve_refresh_mode(acceleration.refresh_mode) == RefreshMode::Changes,
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "debezium",
                message: "The Debezium connector is only compatible with refresh mode 'changes'. For details, visit: https://spiceai.org/docs/components/data-connectors/debezium",
                connector_component: ConnectorComponent::from(dataset),
            }
        );

        let dataset_name = dataset.name.to_string();

        let debezium_kafka_sys = if dataset.is_file_accelerated() {
            Some(Arc::new(
                DebeziumKafkaSys::try_new(dataset, OpenOption::CreateIfNotExists)
                    .await
                    .boxed()
                    .context(super::UnableToGetReadProviderSnafu {
                        dataconnector: "debezium",
                        connector_component: ConnectorComponent::from(dataset),
                    })?,
            ))
        } else {
            tracing::warn!(
                dataset = %dataset_name,
                "Debezium dataset is not file-accelerated. Connector state is ephemeral and the stream will restart on every runtime restart"
            );
            None
        };

        let topic = dataset.path();

        let metadata_from_accelerator =
            if let Some(debezium_kafka_sys) = debezium_kafka_sys.as_deref() {
                get_metadata_from_accelerator(debezium_kafka_sys)
                    .await
                    .boxed()
                    .context(super::UnableToGetReadProviderSnafu {
                        dataconnector: "debezium",
                        connector_component: ConnectorComponent::from(dataset),
                    })?
            } else {
                None
            };

        let (kafka_consumer, metadata, schema) = match metadata_from_accelerator {
            Some(metadata) => {
                if let Some(config_consumer_group_id) = &self.kafka_config.consumer_group_id {
                    ensure!(
                        config_consumer_group_id == &metadata.consumer_group_id,
                        super::InvalidConfigurationNoSourceSnafu {
                            dataconnector: "debezium",
                            message: format!(
                                "Locally accelerated data belongs to a different Kafka consumer group (was '{}', now '{config_consumer_group_id}'). Remove the acceleration file or rename the dataset to proceed.",
                                metadata.consumer_group_id
                            ),
                            connector_component: ConnectorComponent::from(dataset),
                        }
                    );
                }

                ensure!(
                    topic == metadata.topic,
                    super::InvalidConfigurationNoSourceSnafu {
                        dataconnector: "debezium",
                        message: format!(
                            "The topic has changed from {} to {topic}. The existing accelerator data may be out of date.",
                            metadata.topic
                        ), // TODO: what action can a user take from this error?
                        connector_component: ConnectorComponent::from(dataset),
                    }
                );

                let (metadata, schema) = if self.schema_evolution {
                    // Check for schema evolution by peeking at the latest Kafka message
                    refresh_schema_if_evolved(
                        metadata,
                        dataset,
                        topic,
                        &self.kafka_config,
                        debezium_kafka_sys.as_deref(),
                    )
                    .await?
                } else {
                    let schema = debezium::arrow::convert_fields_to_arrow_schema(
                        metadata.schema_fields.iter().collect(),
                    )
                    .boxed()
                    .context(super::UnableToGetReadProviderSnafu {
                        dataconnector: "debezium",
                        connector_component: ConnectorComponent::from(dataset),
                    })?;
                    (metadata, Arc::new(schema))
                };

                // Build the consumer with the sidecar offsets already stashed so
                // the first rebalance callback after `subscribe` seeks before any
                // messages are delivered.
                let kafka_consumer = KafkaConsumer::create_with_existing_group_id(
                    &metadata.consumer_group_id,
                    &self.kafka_config,
                    &metadata.offsets,
                )
                .boxed()
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "debezium",
                    connector_component: ConnectorComponent::from(dataset),
                })?;

                kafka_consumer.subscribe(topic).boxed().context(
                    super::UnableToGetReadProviderSnafu {
                        dataconnector: "debezium",
                        connector_component: ConnectorComponent::from(dataset),
                    },
                )?;

                (kafka_consumer, metadata, schema)
            }
            None => {
                get_metadata_from_kafka(
                    dataset,
                    topic,
                    &self.kafka_config,
                    debezium_kafka_sys.as_deref(),
                    self.schema_evolution,
                )
                .await?
            }
        };

        ensure!(
            !metadata.primary_keys.is_empty()
                || matches!(acceleration.engine.to_unpartitioned(), Engine::Arrow),
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "debezium",
                message: "The Debezium data connector requires Kafka message keys for accelerators other than Arrow. Configure a primary key or message.key.columns in Debezium, or use the Arrow acceleration engine for full-row CDC matching with full before images for keyless updates and deletes. For details, visit: https://spiceai.org/docs/components/data-connectors/debezium",
                connector_component: ConnectorComponent::from(dataset),
            }
        );

        if metadata.primary_keys.is_empty() {
            tracing::warn!(
                dataset = %dataset_name,
                "Debezium messages do not include primary keys; Arrow acceleration will apply deletes and updates by matching full row values, which requires Debezium full before images for keyless updates and deletes"
            );
        }

        let refresh_sql = dataset.refresh_sql();
        let refresh_schema = if let Some(refresh_sql) = &refresh_sql {
            refresh_sql::parse_refresh_sql(dataset.name.clone(), refresh_sql.as_str(), schema)
                .map(|(_, schema)| schema)
                .boxed()
                .map_err(|e| super::DataConnectorError::InvalidConfiguration {
                    dataconnector: "debezium".to_string(),
                    message: format!("The refresh SQL is invalid: {e}"),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                })?
        } else {
            schema
        };

        let mut debezium_kafka = DebeziumKafka::new(
            refresh_schema,
            metadata.primary_keys,
            kafka_consumer,
            self.batching,
        );

        if let Some(debezium_kafka_sys) = debezium_kafka_sys {
            debezium_kafka = debezium_kafka.with_offset_commit_hook(Arc::new(
                SidecarOffsetCommitHook::new(debezium_kafka_sys),
            ));
        }

        Ok(Arc::new(debezium_kafka))
    }

    fn supports_changes_stream(&self) -> bool {
        true
    }

    fn changes_stream(
        &self,
        federated_table: Arc<FederatedTable>,
        _dataset: &Dataset,
        _accelerated_table_provider: Arc<dyn TableProvider>,
        _accelerator_write_mutex: Arc<Mutex<()>>,
        _cpu_runtime: Option<tokio::runtime::Handle>,
    ) -> Option<ChangesStream> {
        Some(Box::pin(stream! {
            let table_provider = federated_table.table_provider().await;
            let Some(debezium_kafka) = table_provider.as_any().downcast_ref::<DebeziumKafka>() else {
                return;
            };

            let mut changes_stream = debezium_kafka.stream_changes();

            while let Some(item) = changes_stream.next().await {
                yield item;
            }
        }))
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        if let Some(metrics) = self.kafka_config.metrics_store.as_ref() {
            Some(Arc::new(super::kafka::KafkaMetricsProvider::new(
                Arc::clone(metrics),
            )))
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct DebeziumKafkaMetadata {
    pub(crate) consumer_group_id: String,
    pub(crate) topic: String,
    pub(crate) primary_keys: Vec<String>,
    pub(crate) schema_fields: Vec<change_event::Field>,
    #[serde(default)]
    pub(crate) offsets: Vec<KafkaOffset>,
}

async fn get_metadata_from_accelerator(
    debezium_kafka_sys: &DebeziumKafkaSys,
) -> Result<Option<DebeziumKafkaMetadata>, spice_sys::Error> {
    debezium_kafka_sys.get().await
}

async fn set_metadata_to_accelerator(
    debezium_kafka_sys: &DebeziumKafkaSys,
    metadata: &DebeziumKafkaMetadata,
) -> Result<(), spice_sys::Error> {
    debezium_kafka_sys.upsert(metadata).await
}

#[async_trait]
impl SidecarOffsetStore for DebeziumKafkaSys {
    async fn upsert_offsets(&self, offsets: &[KafkaOffset]) -> spice_sys::Result<()> {
        DebeziumKafkaSys::upsert_offsets(self, offsets).await
    }
}

async fn get_metadata_from_kafka(
    dataset: &Dataset,
    topic: &str,
    kafka_config: &KafkaConfig,
    debezium_kafka_sys: Option<&DebeziumKafkaSys>,
    schema_evolution: bool,
) -> super::DataConnectorResult<(KafkaConsumer, DebeziumKafkaMetadata, SchemaRef)> {
    let dataset_name = dataset.name.to_string();
    let kafka_consumer = KafkaConsumer::create_for_dataset(
        &dataset_name,
        kafka_config.consumer_group_id.clone(),
        kafka_config,
    )
    .boxed()
    .context(super::UnableToGetReadProviderSnafu {
        dataconnector: "debezium",
        connector_component: ConnectorComponent::from(dataset),
    })?;

    kafka_consumer
        .subscribe(topic)
        .boxed()
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "debezium",
            connector_component: ConnectorComponent::from(dataset),
        })?;

    // Obtain a schema sample and ensure the real consumer has a partition assignment
    // before `restart_topic` rewinds it.
    let (key, value) = if schema_evolution {
        // Poll the real consumer once for partition assignment, and peek the latest
        // message via a temp consumer for the schema sample.
        let (_, event) = tokio::try_join!(
            fetch_first_event(dataset, topic, &kafka_consumer),
            fetch_latest_change_event(dataset, topic, kafka_config),
        )?;

        event
    } else {
        fetch_first_event(dataset, topic, &kafka_consumer).await?
    };

    let primary_keys = key
        .as_ref()
        .map(ChangeEventKey::get_primary_key)
        .unwrap_or_default();

    let Some(schema_fields) = value.get_schema_fields() else {
        return Err(super::DataConnectorError::UnableToGetReadProvider {
            dataconnector: "debezium".to_string(),
            source: "Could not get Arrow schema from Debezium message".into(), // TODO: what action can a user take from this error?
            connector_component: ConnectorComponent::from(dataset),
        });
    };

    let schema = debezium::arrow::convert_fields_to_arrow_schema(schema_fields.clone())
        .boxed()
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "debezium",
            connector_component: ConnectorComponent::from(dataset),
        })?;

    let metadata = DebeziumKafkaMetadata {
        consumer_group_id: kafka_consumer.group_id().to_string(),
        topic: topic.to_string(),
        primary_keys,
        schema_fields: schema_fields.into_iter().cloned().collect(),
        offsets: Vec::new(),
    };

    if let Some(debezium_kafka_sys) = debezium_kafka_sys {
        set_metadata_to_accelerator(debezium_kafka_sys, &metadata)
            .await
            .boxed()
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "debezium",
                connector_component: ConnectorComponent::from(dataset),
            })?;
    }

    // Restart the stream from the beginning
    kafka_consumer
        .restart_topic(topic)
        .boxed()
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "debezium",
            connector_component: ConnectorComponent::from(dataset),
        })?;

    Ok((kafka_consumer, metadata, Arc::new(schema)))
}

/// Peek at the most recent message on `topic` using a temporary consumer.
/// Does not touch the real consumer or its group offsets.
async fn fetch_latest_change_event(
    dataset: &Dataset,
    topic: &str,
    kafka_config: &KafkaConfig,
) -> super::DataConnectorResult<(Option<ChangeEventKey>, ChangeEvent)> {
    let dataset_name = dataset.name.to_string();

    match KafkaConsumer::fetch_latest_message::<ChangeEventKey, ChangeEvent>(
        topic,
        kafka_config,
        SCHEMA_INFERENCE_TIMEOUT,
    )
    .await
    {
        Ok(Some(pair)) => Ok(pair),
        Ok(None) => Err(super::DataConnectorError::UnableToGetReadProvider {
            dataconnector: "debezium".to_string(),
            source: format!(
                "No messages available on Kafka topic '{topic}' for dataset '{dataset_name}' within {timeout:?} while inferring schema (schema_evolution=true). Verify the topic exists and the Debezium connector is producing messages. For details, visit: https://spiceai.org/docs/components/data-connectors/debezium",
                timeout = SCHEMA_INFERENCE_TIMEOUT,
            )
            .into(),
            connector_component: ConnectorComponent::from(dataset),
        }),
        Err(e) => Err(e).boxed().context(super::UnableToGetReadProviderSnafu {
            dataconnector: "debezium",
            connector_component: ConnectorComponent::from(dataset),
        }),
    }
}

/// Read the first available message.
async fn fetch_first_event(
    dataset: &Dataset,
    topic: &str,
    kafka_consumer: &KafkaConsumer,
) -> super::DataConnectorResult<(Option<ChangeEventKey>, ChangeEvent)> {
    let dataset_name = dataset.name.to_string();

    let msg = tokio::time::timeout(
        SCHEMA_INFERENCE_TIMEOUT,
        kafka_consumer.next_json::<ChangeEventKey, ChangeEvent>(),
    )
    .await
    .map_err(|_elapsed| super::DataConnectorError::UnableToGetReadProvider {
        dataconnector: "debezium".to_string(),
        source: format!(
            "Timed out after {timeout:?} waiting for a message on Kafka topic '{topic}' for dataset '{dataset_name}' while inferring schema. Verify the Debezium connector is producing messages. For details, visit: https://spiceai.org/docs/components/data-connectors/debezium",
            timeout = SCHEMA_INFERENCE_TIMEOUT,
        )
        .into(),
        connector_component: ConnectorComponent::from(dataset),
    })?
    .boxed()
    .context(super::UnableToGetReadProviderSnafu {
        dataconnector: "debezium",
        connector_component: ConnectorComponent::from(dataset),
    })?
    .ok_or_else(|| super::DataConnectorError::UnableToGetReadProvider {
        dataconnector: "debezium".to_string(),
        source: format!(
            "No messages available on Kafka topic '{topic}' for dataset '{dataset_name}' while inferring schema. Verify the topic exists and the Debezium connector is producing messages. For details, visit: https://spiceai.org/docs/components/data-connectors/debezium"
        )
        .into(),
        connector_component: ConnectorComponent::from(dataset),
    })?;

    Ok(msg.into_key_value())
}

/// Peek at the latest Kafka message to detect schema evolution. If the schema has
/// changed from the cached metadata, update the stored metadata and return the fresh
/// schema. Falls back to the cached schema if the peek fails or no messages are available.
async fn refresh_schema_if_evolved(
    metadata: DebeziumKafkaMetadata,
    dataset: &Dataset,
    topic: &str,
    kafka_config: &KafkaConfig,
    debezium_kafka_sys: Option<&DebeziumKafkaSys>,
) -> super::DataConnectorResult<(DebeziumKafkaMetadata, SchemaRef)> {
    let dataset_name = dataset.name.to_string();

    let cached_schema =
        debezium::arrow::convert_fields_to_arrow_schema(metadata.schema_fields.iter().collect())
            .boxed()
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "debezium",
                connector_component: ConnectorComponent::from(dataset),
            })?;

    // Try to peek at the latest Kafka message for the current schema
    let peek_result = KafkaConsumer::fetch_latest_message::<ChangeEventKey, ChangeEvent>(
        topic,
        kafka_config,
        SCHEMA_INFERENCE_TIMEOUT,
    )
    .await;

    let value = match peek_result {
        Ok(Some((_key, value))) => value,
        Ok(None) => {
            tracing::debug!(
                "Could not peek at latest Kafka message for schema check on dataset {dataset_name}. Using cached schema."
            );
            return Ok((metadata, Arc::new(cached_schema)));
        }
        Err(e) => {
            tracing::debug!(
                "Failed to peek at latest Kafka message for schema check on dataset {dataset_name}: {e}. Using cached schema."
            );
            return Ok((metadata, Arc::new(cached_schema)));
        }
    };

    let Some(fresh_fields) = value.get_schema_fields() else {
        return Ok((metadata, Arc::new(cached_schema)));
    };

    let fresh_schema = match debezium::arrow::convert_fields_to_arrow_schema(fresh_fields.clone()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                "Failed to convert fresh schema from Kafka for {dataset_name}: {e}. Using cached schema."
            );
            return Ok((metadata, Arc::new(cached_schema)));
        }
    };

    if fresh_schema == cached_schema {
        return Ok((metadata, Arc::new(cached_schema)));
    }

    tracing::info!("Detected schema evolution for dataset {dataset_name}. Updating cached schema.");

    let updated_metadata = DebeziumKafkaMetadata {
        consumer_group_id: metadata.consumer_group_id,
        topic: metadata.topic,
        primary_keys: metadata.primary_keys,
        schema_fields: fresh_fields.into_iter().cloned().collect(),
        offsets: metadata.offsets,
    };

    if let Some(debezium_kafka_sys) = debezium_kafka_sys
        && let Err(e) = set_metadata_to_accelerator(debezium_kafka_sys, &updated_metadata).await
    {
        tracing::warn!(
            "Failed to persist updated schema for {dataset_name}: {e}. Using fresh schema in-memory only."
        );
    }

    Ok((updated_metadata, Arc::new(fresh_schema)))
}
