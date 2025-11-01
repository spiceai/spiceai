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
use crate::component::dataset::Dataset;
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use data_components::dynamodb::provider::DynamoDBTableProvider;
use datafusion::datasource::TableProvider;
use runtime_parameters::ExposedParamLookup;
use snafu::ResultExt;
use std::str::FromStr;
use std::{any::Any, future::Future, pin::Pin, sync::Arc};

#[derive(Debug)]
pub struct DynamoDB {
    params: Parameters,
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

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let table_name = dataset.path();

        let config = initiate_config_with_credentials(
            "DynamoDBTableProvider",
            "aws_region",
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_session_token",
            &self.params,
        )
        .map_err(|message| DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: "dynamodb".to_string(),
            connector_component: ConnectorComponent::from(dataset),
            message: message.to_string(),
        })?
        .load()
        .await;

        let client = Client::new(&config);

        let schema_infer_max_records_str =
            match self.params.get("schema_infer_max_records").expose() {
                ExposedParamLookup::Present(infer_max_rec_str) => infer_max_rec_str,
                ExposedParamLookup::Absent(_) => DEFAULT_SCHEMA_INFER_MAX_RECORDS_STR,
            };

        let schema_infer_max_records = i32::from_str(schema_infer_max_records_str).boxed().context(crate::dataconnector::InvalidConfigurationSnafu {
            dataconnector: "dynamodb".to_string(),
            message: format!(
                "DynamoDB parameter 'schema_infer_max_records' must be an integer, not {schema_infer_max_records_str}"),
            connector_component: ConnectorComponent::from(dataset)
        })?;

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

        let provider = DynamoDBTableProvider::try_new(
            Arc::new(client),
            Arc::from(table_name),
            unnest_depth,
            schema_infer_max_records,
            config_segments,
        )
        .await
        .map_err(|e| DataConnectorError::UnableToGetReadProvider {
            dataconnector: "dynamodb".to_string(),
            connector_component: ConnectorComponent::from(dataset),
            source: Box::new(e),
        })?;
        Ok(Arc::new(provider))
    }
}
