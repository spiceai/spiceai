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
use crate::secrets::Secret;
use async_trait::async_trait;
use data_components::debezium;
use data_components::debezium::change_event::ChangeEvent;
use data_components::debezium_kafka::DebeziumKafka;
use data_components::kafka::KafkaConsumer;
use datafusion::datasource::TableProvider;
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid value for debezium_transport. Valid values: 'kafka'"))]
    InvalidTransport,

    #[snafu(display("Invalid value for debezium_message_format: Valid values: 'json'"))]
    InvalidMessageFormat,

    #[snafu(display("Missing required parameter: kafka_bootstrap_servers"))]
    MissingKafkaBootstrapServers,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Debezium {
    kafka_brokers: String,
}

impl Debezium {
    pub fn new(params: &Arc<HashMap<String, String>>) -> Result<Self> {
        let transport = params
            .get("debezium_transport")
            .map_or("kafka", String::as_str);

        let message_format = params
            .get("debezium_message_format")
            .map_or("json", String::as_str);

        if transport != "kafka" {
            return InvalidTransportSnafu.fail();
        }
        if message_format != "json" {
            return InvalidMessageFormatSnafu.fail();
        }

        let kakfa_brokers = params
            .get("kafka_bootstrap_servers")
            .context(MissingKafkaBootstrapServersSnafu)?;

        Ok(Self {
            kafka_brokers: kakfa_brokers.to_string(),
        })
    }
}

impl DataConnectorFactory for Debezium {
    fn create(
        _secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let debezium = Debezium::new(&params)?;
            Ok(Arc::new(debezium) as Arc<dyn DataConnector>)
        })
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

        let topic = dataset.path();

        let consumer = KafkaConsumer::create_with_generated_group_id(
            &dataset.name.to_string(),
            self.kafka_brokers.clone(),
        )
        .boxed()
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "debezium",
        })?;

        consumer
            .subscribe(&topic)
            .boxed()
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "debezium",
            })?;

        let msg = match consumer.next_json::<ChangeEvent>().await {
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

        let Some(schema_fields) = msg.value().get_schema_fields() else {
            return Err(super::DataConnectorError::UnableToGetReadProvider {
                dataconnector: "debezium".to_string(),
                source: "Could not get Arrow schema from Debezium message".into(),
            });
        };

        let schema = debezium::arrow::convert_fields_to_arrow_schema(schema_fields)
            .boxed()
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "debezium",
            })?;

        let debezium_kafka = Arc::new(DebeziumKafka::new(Arc::new(schema), consumer));

        Ok(debezium_kafka)
    }
}
