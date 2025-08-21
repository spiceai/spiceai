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
    kafka::{KafkaConfig, KafkaConsumer},
};
use datafusion::catalog::TableProvider;
use futures::StreamExt;
use snafu::prelude::*;
use tonic::async_trait;

use crate::{
    component::dataset::{Dataset, acceleration::RefreshMode},
    dataconnector::{
        ConnectorComponent, DataConnector, DataConnectorFactory, parameters::ConnectorParams,
    },
    datafusion::refresh_sql,
    federated_table::FederatedTable,
    parameters::{ParameterSpec, Parameters},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: 'kafka_bootstrap_servers'. Specify a value.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/kafka#parameters"
    ))]
    MissingKafkaBootstrapServers,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Kafka {
    kafka_config: KafkaConfig,
}

impl Kafka {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(params: Parameters) -> Result<Self> {
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
                message: "The Kafka data connector requires an accelerated dataset.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/kafka",
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

        if !dataset.is_file_accelerated() {
            tracing::warn!(
                "Dataset {dataset_name} is not file accelerated. This may result in full message replay from Kafka on restarts. It is recommended to use file acceleration with the Kafka connector for optimal performance. For details, visit: https://spiceai.org/docs/components/data-connectors/kafka",
            );
        }

        let topic = dataset.path();

        let (kafka_consumer, schema) =
            bootstrap_kafka_consumer(dataset, topic, self.kafka_config.clone()).await?;

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

        let kafka = Arc::new(data_components::kafka::Kafka::new(schema, kafka_consumer));

        Ok(kafka)
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
}

async fn bootstrap_kafka_consumer(
    dataset: &Dataset,
    topic: &str,
    kafka_config: KafkaConfig,
) -> super::DataConnectorResult<(KafkaConsumer, SchemaRef)> {
    let dataset_name = dataset.name.to_string();
    let kafka_consumer = KafkaConsumer::create_with_generated_group_id(&dataset_name, kafka_config)
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

    let msg = match kafka_consumer
        .next_json::<serde_json::Value, serde_json::Value>()
        .await
    {
        Ok(Some(msg)) => msg,
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
    };

    // Infer Arrow schema from the JSON value in the message
    let schema = datafusion::arrow::json::reader::infer_json_schema_from_iterator(std::iter::once(
        Ok(msg.value()),
    ))
    .map_err(|e| super::DataConnectorError::UnableToGetReadProvider {
        dataconnector: "kafka".to_string(),
        source: format!("Failed to infer schema from Kafka message: {e}").into(),
        connector_component: ConnectorComponent::from(dataset),
    })?;

    // Restart the stream from the beginning
    kafka_consumer
        .restart_topic(topic)
        .boxed()
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "kafka",
            connector_component: ConnectorComponent::from(dataset),
        })?;

    Ok((kafka_consumer, Arc::new(schema)))
}
