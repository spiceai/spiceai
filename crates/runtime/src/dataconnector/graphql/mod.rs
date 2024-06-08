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

mod client;
mod pagination;
mod table_provider;

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::header::{CONTENT_TYPE, USER_AGENT};
use secrets::{get_secret_or_param, Secret};
use url::Url;

use crate::component::dataset::Dataset;
use datafusion::datasource::TableProvider;
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use self::client::{Auth, GraphQLClient};
use self::pagination::PaginationParameters;
use self::table_provider::GraphQLTableProvider;

use super::{DataConnector, DataConnectorFactory};

pub struct GraphQL {
    secret: Option<Secret>,
    params: Arc<HashMap<String, String>>,
}

impl DataConnectorFactory for GraphQL {
    fn create(
        secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let graphql = Self { secret, params };
            Ok(Arc::new(graphql) as Arc<dyn DataConnector>)
        })
    }
}

impl GraphQL {
    fn get_client(&self, dataset: &Dataset) -> super::DataConnectorResult<GraphQLClient> {
        let mut client_builder = reqwest::Client::builder();
        let token = get_secret_or_param(&self.params, &self.secret, "auth_token_key", "auth_token");
        let user = get_secret_or_param(&self.params, &self.secret, "auth_user_key", "auth_user");
        let pass = get_secret_or_param(&self.params, &self.secret, "auth_pass_key", "auth_pass");

        let query = self
            .params
            .get("query")
            .ok_or("`query` not found in params".into())
            .context(super::InvalidConfigurationSnafu {
                dataconnector: "GraphQL",
                message: "`query` not found in params",
            })?
            .to_owned();
        let endpoint = Url::parse(&dataset.path()).map_err(Into::into).context(
            super::InvalidConfigurationSnafu {
                dataconnector: "GraphQL",
                message: "Invalid URL in dataset `from` definition",
            },
        )?;
        let json_path = self
            .params
            .get("json_path")
            .ok_or("`json_path` not found in params".into())
            .context(super::InvalidConfigurationSnafu {
                dataconnector: "GraphQL",
                message: "`json_path` not found in params",
            })?
            .to_owned();
        let pointer = format!("/{}", json_path.replace('.', "/"));

        let pagination_parameters = PaginationParameters::parse(&query, &pointer);

        let mut headers = HeaderMap::new();
        headers.append(USER_AGENT, HeaderValue::from_static("spice"));
        headers.append(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let mut auth = None;
        if let Some(token) = token {
            auth = Some(Auth::Bearer(token));
        }
        if let Some(user) = user {
            auth = Some(Auth::Basic(user, pass));
        }

        client_builder = client_builder.default_headers(headers);

        Ok(GraphQLClient::new(
            client_builder.build().map_err(|e| {
                super::DataConnectorError::InvalidConfiguration {
                    dataconnector: "GraphQL".to_string(),
                    message: "Failed to set token".to_string(),
                    source: e.into(),
                }
            })?,
            endpoint,
            query,
            pointer,
            pagination_parameters,
            auth,
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
                    dataconnector: "GraphQL".to_string(),
                })?,
        ))
    }
}
