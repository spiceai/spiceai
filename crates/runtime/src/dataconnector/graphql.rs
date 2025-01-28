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
use data_components::{
    graphql::{self, client::GraphQLClient, provider::GraphQLTableProviderBuilder},
    token_provider::{StaticTokenProvider, TokenProvider},
};
use datafusion::datasource::TableProvider;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use snafu::ResultExt;
use std::{any::Any, future::Future, pin::Pin, sync::Arc};
use url::Url;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    InvalidConfigurationSnafu, ParameterSpec, Parameters,
};

pub struct GraphQL {
    params: Parameters,
}

#[derive(Default, Copy, Clone)]
pub struct GraphQLFactory {}

impl GraphQLFactory {
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

impl DataConnectorFactory for GraphQLFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let graphql = GraphQL {
                params: params.parameters,
            };
            Ok(Arc::new(graphql) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "graphql"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

pub(crate) fn default_spice_client(content_type: &'static str) -> reqwest::Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.append(CONTENT_TYPE, HeaderValue::from_static(content_type));

    reqwest::Client::builder()
        .user_agent("spice")
        .default_headers(headers)
        .build()
}

impl GraphQL {
    fn get_client(&self, dataset: &Dataset) -> super::DataConnectorResult<GraphQLClient> {
        let token = self.params.get("auth_token").expose().ok().map(|token| {
            Arc::new(StaticTokenProvider::new(token.into())) as Arc<dyn TokenProvider>
        });

        let user = self
            .params
            .get("auth_user")
            .expose()
            .ok()
            .map(str::to_string);
        let pass = self
            .params
            .get("auth_pass")
            .expose()
            .ok()
            .map(str::to_string);

        let endpoint = Url::parse(dataset.path()).map_err(Into::into).context(
            super::InvalidConfigurationSnafu {
                dataconnector: "graphql",
                message: "The specified URL in the dataset 'from' is not valid. Ensure the URL is valid and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/graphql",
                connector_component: ConnectorComponent::from(dataset),
            },
        )?;

        // If json_pointer isn't provided, default to the root of the response
        let json_pointer: Option<&str> = self.params.get("json_pointer").expose().ok();

        let unnest_depth = self
            .params
            .get("unnest_depth")
            .expose()
            .ok()
            .map_or(Ok(0), str::parse)
            .boxed()
            .context(InvalidConfigurationSnafu {
                dataconnector: "graphql",
                message: "The `unnest_depth` parameter must be a positive integer.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/graphql#configuration",
                connector_component: ConnectorComponent::from(dataset),
            })?;

        let client = default_spice_client("application/json")
            .boxed()
            .map_err(|e| DataConnectorError::InternalWithSource {
                dataconnector: "graphql".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            })?;

        GraphQLClient::new(
            client,
            endpoint,
            json_pointer,
            token,
            user,
            pass,
            unnest_depth,
            None,
            None,
        )
        .boxed()
        .context(super::InternalWithSourceSnafu {
            dataconnector: "graphql".to_string(),
            connector_component: ConnectorComponent::from(dataset),
        })
    }
}

#[async_trait]
impl DataConnector for GraphQL {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let client = self.get_client(dataset)?;

        let query = self.params.get("query").expose().ok_or_else(|p| {
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "graphql",
                message: format!("A required parameter was missing: `{}`.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/graphql#configuration", p.0),
                connector_component: ConnectorComponent::from(dataset),
            }
            .build()
        })?;

        Ok(Arc::new(
            GraphQLTableProviderBuilder::new(client)
                .build(query)
                .await
                .map_err(|e| {
                    if matches!(e, graphql::Error::InvalidGraphQLQuery { .. }) {
                        let message = format!("{e}");
                        super::DataConnectorError::InvalidConfiguration {
                            dataconnector: "graphql".to_string(),
                            connector_component: ConnectorComponent::from(dataset),
                            source: e.into(),
                            message,
                        }
                    } else {
                        super::DataConnectorError::InternalWithSource {
                            dataconnector: "graphql".to_string(),
                            connector_component: ConnectorComponent::from(dataset),
                            source: e.into(),
                        }
                    }
                })?,
        ))
    }
}
