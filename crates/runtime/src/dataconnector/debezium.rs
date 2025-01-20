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

use crate::component::dataset::acceleration::{Engine, RefreshMode};
use crate::component::dataset::Dataset;
use crate::dataaccelerator::spice_sys::debezium_kafka::DebeziumKafkaSys;
use crate::dataconnector::ConnectorComponent;
use crate::datafusion::refresh_sql;
use crate::federated_table::FederatedTable;
use arrow::datatypes::SchemaRef;
use async_stream::stream;
use async_trait::async_trait;
use data_components::cdc::ChangesStream;
use data_components::debezium::change_event::{ChangeEvent, ChangeEventKey};
use data_components::debezium::{self, change_event};
use data_components::debezium_kafka::DebeziumKafka;
use data_components::kafka::{KafkaConfig, KafkaConsumer};
use datafusion::datasource::TableProvider;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{ConnectorParams, DataConnector, DataConnectorFactory, ParameterSpec, Parameters};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid value for 'debezium_transport': {transport}.\nSupported values: 'kafka'\nFor details, visit: https://spiceai.org/docs/components/data-connectors/debezium#parameters"))]
    InvalidTransport { transport: String },

    #[snafu(display("Invalid value for 'debezium_message_format': {format}.\nSupported values: 'json'\nFor details, visit: https://spiceai.org/docs/components/data-connectors/debezium#parameters"))]
    InvalidMessageFormat { format: String },

    #[snafu(display("Missing required parameter: 'debezium_kafka_bootstrap_servers'. Specify a value.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/debezium#parameters"))]
    MissingKafkaBootstrapServers,

    #[snafu(display("{source}"))]
    RefreshSql { source: refresh_sql::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Debezium {
    kafka_config: KafkaConfig,
}

impl Debezium {
    #[allow(clippy::needless_pass_by_value)]
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
        };

        Ok(Self { kafka_config })
    }
}

#[derive(Default, Copy, Clone)]
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
    ParameterSpec::connector("transport")
        .required()
        .default("kafka")
        .description("The message broker transport to use. The default is kafka."),
    ParameterSpec::connector("message_format")
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
        ensure!(
            dataset.is_accelerated(),
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "debezium",
                message: "The Debezium data connector only works with accelerated datasets.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/debezium",
                connector_component: ConnectorComponent::from(dataset),
            }
        );
        let Some(ref acceleration) = dataset.acceleration else {
            unreachable!("we just checked above that the dataset is accelerated");
        };
        ensure!(
            acceleration.engine != Engine::Arrow,
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "debezium",
                message:
                    "The Debezium data connector only works with non-Arrow acceleration engines.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/debezium",
                connector_component: ConnectorComponent::from(dataset),
            }
        );
        ensure!(
            self.resolve_refresh_mode(acceleration.refresh_mode) == RefreshMode::Changes,
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "debezium",
                message: "The Debezium data connector only works with 'changes' refresh mode.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/debezium",
                connector_component: ConnectorComponent::from(dataset),
            }
        );

        let dataset_name = dataset.name.to_string();

        if !dataset.is_file_accelerated() {
            tracing::warn!(
                "Dataset {dataset_name} is not file accelerated. This is not recommended as it requires replaying all changes from the beginning on restarts.",
            );
        }

        let topic = dataset.path();

        let (kafka_consumer, metadata, schema) = match get_metadata_from_accelerator(dataset).await
        {
            Some(metadata) => {
                let kafka_consumer = KafkaConsumer::create_with_existing_group_id(
                    &metadata.consumer_group_id,
                    self.kafka_config.clone(),
                )
                .boxed()
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "debezium",
                    connector_component: ConnectorComponent::from(dataset),
                })?;

                ensure!(
                    topic == metadata.topic,
                    super::InvalidConfigurationNoSourceSnafu {
                        dataconnector: "debezium",
                        message: format!("The topic has changed from {} to {topic}. The existing accelerator data may be out of date.", metadata.topic), // TODO: what action can a user take from this error?
                connector_component: ConnectorComponent::from(dataset),
                    }
                );

                let schema = debezium::arrow::convert_fields_to_arrow_schema(
                    metadata.schema_fields.iter().collect(),
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

                (kafka_consumer, metadata, Arc::new(schema))
            }
            None => get_metadata_from_kafka(dataset, topic, self.kafka_config.clone()).await?,
        };

        let refresh_sql = dataset.refresh_sql();
        let refresh_schema = if let Some(refresh_sql) = &refresh_sql {
            refresh_sql::validate_refresh_sql(dataset.name.clone(), refresh_sql.as_str(), schema)
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

        let debezium_kafka = Arc::new(DebeziumKafka::new(
            refresh_schema,
            metadata.primary_keys,
            kafka_consumer,
        ));

        Ok(debezium_kafka)
    }

    fn supports_changes_stream(&self) -> bool {
        true
    }

    fn changes_stream(&self, federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
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
}

#[derive(Serialize, Deserialize)]
pub(crate) struct DebeziumKafkaMetadata {
    pub(crate) consumer_group_id: String,
    pub(crate) topic: String,
    pub(crate) primary_keys: Vec<String>,
    pub(crate) schema_fields: Vec<change_event::Field>,
}

async fn get_metadata_from_accelerator(dataset: &Dataset) -> Option<DebeziumKafkaMetadata> {
    let debezium_kafka_sys = DebeziumKafkaSys::try_new(dataset).await.ok()?;
    debezium_kafka_sys.get().await
}

async fn set_metadata_to_accelerator(
    dataset: &Dataset,
    metadata: &DebeziumKafkaMetadata,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let debezium_kafka_sys = DebeziumKafkaSys::try_new_create_if_not_exists(dataset).await?;
    debezium_kafka_sys.upsert(metadata).await
}

async fn get_metadata_from_kafka(
    dataset: &Dataset,
    topic: &str,
    kafka_config: KafkaConfig,
) -> super::DataConnectorResult<(KafkaConsumer, DebeziumKafkaMetadata, SchemaRef)> {
    let dataset_name = dataset.name.to_string();
    let kafka_consumer = KafkaConsumer::create_with_generated_group_id(&dataset_name, kafka_config)
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

    let msg = match kafka_consumer
        .next_json::<ChangeEventKey, ChangeEvent>()
        .await
    {
        Ok(Some(msg)) => msg,
        Ok(None) => {
            return Err(super::DataConnectorError::UnableToGetReadProvider {
                dataconnector: "debezium".to_string(),
                source: "No message received from Kafka.".into(), // TODO: what action can a user take from this error?
                connector_component: ConnectorComponent::from(dataset),
            });
        }
        Err(e) => {
            return Err(e).boxed().context(super::UnableToGetReadProviderSnafu {
                dataconnector: "debezium",
                connector_component: ConnectorComponent::from(dataset),
            });
        }
    };

    let primary_keys = msg.key().get_primary_key();

    let Some(schema_fields) = msg.value().get_schema_fields() else {
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
    };

    if dataset.is_file_accelerated() {
        set_metadata_to_accelerator(dataset, &metadata)
            .await
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
