/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this Https except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use crate::component::dataset::Dataset;
use snafu::prelude::*;
use snafu::ResultExt;
use std::{any::Any, future::Future, pin::Pin, sync::Arc};
use url::Url;

use super::{
    DataConnector, DataConnectorError, DataConnectorFactory, InvalidConfigurationSnafu,
    ParameterSpec, Parameters,
};

pub struct Salesforce {
    params: Parameters,
}

#[derive(Default, Copy, Clone)]
pub struct SalesforceFactory {}

impl SalesforceFactory {
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
    ParameterSpec::connector("auth_token")
        .description("The bearer token to use in all requests.")
        .secret(),
    ParameterSpec::connector("endpoint_url")
        .description("The Salesforce endpoint URL.")
        .secret(),
    ParameterSpec::runtime("graphql_query")
        .description("The GraphQL query to execute"),
    ParameterSpec::runtime("json_pointer")
        .description("The JSON pointer to extract the data from the response."),
    ParameterSpec::runtime("unnest_depth")
        .description("Depth level to automatically unnest objects to. By default disabled if unspecified or 0")

];


impl DataConnectorFactory for SalesforceFactory {
    fn create(&self, params: Parameters) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let connector = Salesforce::new(params);
            Ok(Arc::new(connector) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "salesforce"
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

impl Salesforce {
    pub fn new(params: Parameters) -> Self {
        Self { params }
    }

    fn get_client(&self, dataset: &Dataset) -> super::DataConnectorResult<
}