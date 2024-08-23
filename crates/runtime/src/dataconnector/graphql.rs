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

use crate::component::dataset::Dataset;
use arrow::{array::RecordBatch, datatypes::SchemaRef, error::ArrowError};
use arrow_json::{reader::infer_json_schema_from_iterator, ReaderBuilder};
use async_trait::async_trait;
use data_components::{arrow::write::MemTable, graphql::{client::GraphQLClient, provider::GraphQLTableProvider}};
use datafusion::datasource::TableProvider;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use snafu::{ResultExt, Snafu};
use std::{any::Any, future::Future, io::Cursor, pin::Pin, sync::Arc};
use url::Url;

use super::{
    DataConnector, DataConnectorError, DataConnectorFactory, InvalidConfigurationSnafu,
    ParameterSpec, Parameters,
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
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let graphql = GraphQL { params };
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

pub fn default_spice_client(content_type: &'static str) -> reqwest::Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.append(CONTENT_TYPE, HeaderValue::from_static(content_type));

    reqwest::Client::builder()
        .user_agent("spice")
        .default_headers(headers)
        .build()
}

impl GraphQL {
    fn get_client(&self, dataset: &Dataset) -> super::DataConnectorResult<GraphQLClient> {
        let mut client_builder = reqwest::Client::builder();
        let token = self
            .params
            .get("auth_token")
            .expose()
            .ok()
            .map(str::to_string);
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

        let query = self
            .params
            .get("query")
            .expose()
            .ok_or_else(|p| {
                super::InvalidConfigurationNoSourceSnafu {
                    dataconnector: "graphql",
                    message: format!("`{}` not found in params", p.0),
                }
                .build()
            })?
            .to_owned();

        let endpoint = Url::parse(&dataset.path()).map_err(Into::into).context(
            super::InvalidConfigurationSnafu {
                dataconnector: "graphql",
                message: "Invalid URL in dataset `from` definition",
            },
        )?;

        // If json_pointer isn't provided, default to the root of the response
        let json_pointer = self
            .params
            .get("json_pointer")
            .expose()
            .ok()
            .unwrap_or_default()
            .to_owned();

        let unnest_depth = self
            .params
            .get("unnest_depth")
            .expose()
            .ok()
            .map(|x| x.parse::<usize>())
            .unwrap_or(Ok(0))
            .boxed()
            .context(InvalidConfigurationSnafu {
                dataconnector: "graphql",
                message: "`unnest_depth` must be a positive integer",
            })?;

        let client = default_spice_client("application/json")
            .boxed()
            .map_err(|e| DataConnectorError::InvalidConfiguration {
                dataconnector: "graphql".to_string(),
                message: "could not configure client".to_string(),
                source: e,
            })?;

        Ok(GraphQLClient::new(
            client,
            endpoint,
            query,
            json_pointer,
            token,
            user,
            pass,
            unnest_depth,
        ))
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

        Ok(Arc::new(
            GraphQLTableProvider::new(client)
                .await
                .map_err(Into::into)
                .context(super::InternalWithSourceSnafu {
                    dataconnector: "graphql".to_string(),
                })?,
        ))
    }
}
