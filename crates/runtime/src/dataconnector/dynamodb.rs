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
use datafusion::datasource::TableProvider;
use std::{any::Any, future::Future, pin::Pin, sync::Arc};

use super::{ConnectorParams, DataConnector, DataConnectorFactory, ParameterSpec, Parameters};

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
    ParameterSpec::connector("auth_token")
        .description("The bearer token to use in the GraphQL requests.")
        .secret(),
    ParameterSpec::connector("auth_user")
        .description("The username to use for HTTP Basic Auth.")
        .secret(),
    ParameterSpec::connector("auth_pass")
        .description("The password to use for HTTP Basic Auth.")
        .secret(),
    ParameterSpec::connector("query")
        .description("The GraphQL query to execute.")
        .required(),
    // Runtime parameters
    ParameterSpec::runtime("json_pointer")
        .description("The JSON pointer to the data in the GraphQL response."),
    ParameterSpec::runtime("unnest_depth").description(
        "Depth level to automatically unnest objects to. By default, disabled if unspecified or 0.",
    ),
];

impl DataConnectorFactory for DynamoDBFactory {
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
        _dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        todo!()
    }
}
