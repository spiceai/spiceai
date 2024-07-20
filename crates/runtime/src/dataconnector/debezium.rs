/*
Copyright 2024 The Spice.ai OSS Authors

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
use crate::dataaccelerator::metadata::AcceleratedMetadata;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use data_components::cdc::ChangesStream;
use data_components::debezium::change_event::{ChangeEvent, ChangeEventKey};
use data_components::debezium::{self, change_event};
use data_components::debezium_kafka::DebeziumKafka;
use data_components::kafka::KafkaConsumer;
use datafusion::datasource::TableProvider;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{DataConnector, DataConnectorFactory, ParameterSpec, Parameters};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid value for debezium_transport. Valid values: 'kafka'"))]
    InvalidTransport,

    #[snafu(display("Invalid value for debezium_message_format: Valid values: 'json'"))]
    InvalidMessageFormat,

    #[snafu(display("Missing required parameter: debezium_kafka_bootstrap_servers"))]
    MissingKafkaBootstrapServers,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Debezium {
    kafka_brokers: String,
}

impl Debezium {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(params: Parameters) -> Result<Self> {
        let transport = params.get("transport").expose().ok().unwrap_or("kafka");

        let message_format = params.get("message_format").expose().ok().unwrap_or("json");

        if transport != "kafka" {
            return InvalidTransportSnafu.fail();
        }
        if message_format != "json" {
            return InvalidMessageFormatSnafu.fail();
        }

        let kakfa_brokers = params
            .get("kafka_bootstrap_servers")
            .expose()
            .ok()
            .context(MissingKafkaBootstrapServersSnafu)?;

        Ok(Self {
            kafka_brokers: kakfa_brokers.to_string(),
        })
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
];

impl DataConnectorFactory for DebeziumFactory {
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let debezium = Debezium::new(params)?;
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
                message: "The Debezium data connector only works with accelerated datasets.",
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
                    "The Debezium data connector only works with non-Arrow acceleration engines.",
            }
        );
        ensure!(
            self.resolve_refresh_mode(acceleration.refresh_mode) == RefreshMode::Changes,
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "debezium",
                message: "The Debezium data connector only works with 'changes' refresh mode.",
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
                    self.kafka_brokers.clone(),
                )
                .boxed()
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "debezium",
                })?;

                ensure!(
                    topic == metadata.topic,
                    super::InvalidConfigurationNoSourceSnafu {
                        dataconnector: "debezium",
                        message: format!("The topic has changed from {} to {topic} for dataset {dataset_name}. The existing accelerator data may be out of date.", metadata.topic),
                    }
                );

                let schema = debezium::arrow::convert_fields_to_arrow_schema(
                    metadata.schema_fields.iter().collect(),
                )
                .boxed()
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "debezium",
                })?;

                kafka_consumer.subscribe(&topic).boxed().context(
                    super::UnableToGetReadProviderSnafu {
                        dataconnector: "debezium",
                    },
                )?;

                (kafka_consumer, metadata, Arc::new(schema))
            }
            None => get_metadata_from_kafka(dataset, &topic, self.kafka_brokers.clone()).await?,
        };

        let debezium_kafka = Arc::new(DebeziumKafka::new(
            schema,
            metadata.primary_keys,
            kafka_consumer,
        ));

        Ok(debezium_kafka)
    }

    fn supports_changes_stream(&self) -> bool {
        true
    }

    fn changes_stream(&self, table_provider: Arc<dyn TableProvider>) -> Option<ChangesStream> {
        let debezium_kafka = table_provider.as_any().downcast_ref::<DebeziumKafka>()?;

        Some(debezium_kafka.stream_changes())
    }
}

#[derive(Serialize, Deserialize)]
struct DebeziumKafkaMetadata {
    consumer_group_id: String,
    topic: String,
    primary_keys: Vec<String>,
    schema_fields: Vec<change_event::Field>,
}

async fn get_metadata_from_accelerator(dataset: &Dataset) -> Option<DebeziumKafkaMetadata> {
    let accelerated_metadata = AcceleratedMetadata::new(dataset).await?;
    let metadata = accelerated_metadata.get_metadata().await?;
    Some(metadata)
}

async fn set_metadata_to_accelerator(
    dataset: &Dataset,
    metadata: &DebeziumKafkaMetadata,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let accelerated_metadata = AcceleratedMetadata::new_create_if_not_exists(dataset).await?;
    accelerated_metadata.set_metadata(metadata).await
}

async fn get_metadata_from_kafka(
    dataset: &Dataset,
    topic: &str,
    kafka_brokers: String,
) -> super::DataConnectorResult<(KafkaConsumer, DebeziumKafkaMetadata, SchemaRef)> {
    let dataset_name = dataset.name.to_string();
    let kafka_consumer =
        KafkaConsumer::create_with_generated_group_id(&dataset_name, kafka_brokers)
            .boxed()
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "debezium",
            })?;

    kafka_consumer
        .subscribe(topic)
        .boxed()
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "debezium",
        })?;

    let msg = match kafka_consumer
        .next_json::<ChangeEventKey, ChangeEvent>()
        .await
    {
        Ok(Some(msg)) => msg,
        Ok(None) => {
            return Err(super::DataConnectorError::UnableToGetReadProvider {
                dataconnector: "debezium".to_string(),
                source: "No message received from Kafka".into(),
            });
        }
        Err(e) => {
            return Err(e).boxed().context(super::UnableToGetReadProviderSnafu {
                dataconnector: "debezium",
            });
        }
    };

    let primary_keys = msg.key().get_primary_key();

    let Some(schema_fields) = msg.value().get_schema_fields() else {
        return Err(super::DataConnectorError::UnableToGetReadProvider {
            dataconnector: "debezium".to_string(),
            source: "Could not get Arrow schema from Debezium message".into(),
        });
    };

    let schema = debezium::arrow::convert_fields_to_arrow_schema(schema_fields.clone())
        .boxed()
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "debezium",
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
            })?;
    }

    // Restart the stream from the beginning
    kafka_consumer
        .restart_topic(topic)
        .boxed()
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "debezium",
        })?;

    Ok((kafka_consumer, metadata, Arc::new(schema)))
}
