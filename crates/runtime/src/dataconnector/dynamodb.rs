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

use crate::component::dataset::Dataset;
use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::Client;
use aws_sdk_sts::config::Credentials;
use data_components::dynamodb::provider::DynamoDBTableProvider;
use datafusion::datasource::TableProvider;
use std::{any::Any, future::Future, pin::Pin, sync::Arc};

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec, Parameters,
};

pub struct DynamoDB {
    params: Parameters,
}

#[derive(Default, Copy, Clone)]
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

const PARAMETERS: &[ParameterSpec] = &[
    // Connector parameters
    ParameterSpec::connector("aws_region")
        .description("The AWS region to use for DynamoDB.")
        .required()
        .secret(),
    ParameterSpec::connector("aws_access_key_id")
        .description("The AWS access key ID to use for DynamoDB.")
        .secret(),
    ParameterSpec::connector("aws_secret_access_key")
        .description("The AWS secret access key to use for DynamoDB.")
        .secret(),
    ParameterSpec::connector("aws_session_token")
        .description("The AWS session token to use for DynamoDB.")
        .secret(),
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

        // Get and own all parameter values upfront
        let region = self
            .params
            .get("aws_region")
            .expose()
            .ok_or_else(|_| DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "dynamodb".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: "aws_region is required".to_string(),
            })?
            .to_string();

        let access_key_id = self
            .params
            .get("aws_access_key_id")
            .expose()
            .ok()
            .map(ToString::to_string);

        let secret_access_key = self
            .params
            .get("aws_secret_access_key")
            .expose()
            .ok()
            .map(ToString::to_string);

        let session_token = self
            .params
            .get("aws_session_token")
            .expose()
            .ok()
            .map(ToString::to_string);

        let config = match (access_key_id, secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => {
                let credentials = Credentials::new(
                    access_key_id,
                    secret_access_key,
                    session_token,
                    None,
                    "DynamoDBTableProvider",
                );

                aws_config::defaults(BehaviorVersion::v2024_03_28())
                    .region(Region::new(region))
                    .credentials_provider(credentials)
                    .load()
                    .await
            }
            _ => {
                // This will automatically load AWS credentials from the environment, via IAM roles if configured.
                aws_config::defaults(BehaviorVersion::v2024_03_28())
                    .region(Region::new(region))
                    .load()
                    .await
            }
        };

        let client = Client::new(&config);
        let provider = DynamoDBTableProvider::try_new(Arc::new(client), Arc::from(table_name))
            .await
            .map_err(|e| DataConnectorError::UnableToGetReadProvider {
                dataconnector: "dynamodb".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            })?;
        Ok(Arc::new(provider))
    }
}
