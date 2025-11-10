/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use crate::dataconnector::listing::LISTING_TABLE_PARAMETERS;

use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use url::Url;

use super::{ConnectorComponent, ConnectorParams};
use super::{
    DataConnector, DataConnectorError, DataConnectorFactory, DataConnectorResult, ParameterSpec,
    Parameters,
};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use reqwest::Client;
use std::time::Duration;

const DEFAULT_CLIENT_TIMEOUT_SECS: u64 = 30;

#[derive(Debug)]
pub struct Https {
    params: Parameters,
}

impl std::fmt::Display for Https {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "https")
    }
}

#[async_trait]
impl DataConnector for Https {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let base_url = Url::parse(dataset.from.as_str()).boxed().map_err(|e| {
            DataConnectorError::InvalidConfiguration {
                dataconnector: "https".to_string(),
                message: format!("{} is not a valid URL. Ensure the URL is valid and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/https", dataset.from),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            }
        })?;

        let timeout_secs = self
            .params
            .get("client_timeout")
            .expose()
            .ok()
            .and_then(|t| t.parse::<u64>().ok())
            .unwrap_or(DEFAULT_CLIENT_TIMEOUT_SECS);

        let client = Client::builder()
            .user_agent("spice")
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(timeout_secs))
            .pool_max_idle_per_host(10) // Allow connection reuse
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .boxed()
            .map_err(|e| DataConnectorError::InternalWithSource {
                dataconnector: "https".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            })?;

        // Determine file format - default to "auto" if not specified
        let file_format = self
            .params
            .get("file_format")
            .expose()
            .ok()
            .map_or_else(|| "auto".to_string(), str::to_ascii_lowercase);

        let acceleration_enabled = dataset.is_accelerated();

        // Handle http_max_retries parameter with default of 3
        let max_retries = self
            .params
            .get("http_max_retries")
            .expose()
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(3);

        // Handle http_post_content_type parameter for POST requests
        let content_type = self
            .params
            .get("http_post_content_type")
            .expose()
            .ok()
            .map(std::string::ToString::to_string);

        let provider = data_components::http::provider::HttpTableProvider::new(
            base_url,
            client,
            file_format,
            acceleration_enabled,
        )
        .with_max_retries(max_retries)
        .with_content_type(content_type);

        let provider = Arc::new(provider);

        // Validate the HTTP endpoint (non-blocking, log warnings only)
        let provider_clone = Arc::clone(&provider);
        let dataset_name = dataset.name.clone();
        tokio::spawn(async move {
            if let Err(e) = provider_clone.validate_endpoint().await {
                tracing::warn!(
                    "HTTP endpoint validation failed for dataset '{}': {}. \
                    The endpoint may be temporarily unavailable or misconfigured. \
                    Queries will continue but may fail if the endpoint is not accessible.",
                    dataset_name,
                    e
                );
            }
        });

        Ok(provider)
    }
}

#[derive(Default, Debug, Clone)]
pub struct HttpsFactory {}

impl HttpsFactory {
    #[must_use]
    pub fn new() -> Self {
        HttpsFactory::default()
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    let mut all_parameters = Vec::new();
    all_parameters.extend_from_slice(&[
        ParameterSpec::component("username").secret(),
        ParameterSpec::component("password").secret(),
        ParameterSpec::component("port").description("The port to connect to."),
        ParameterSpec::runtime("client_timeout")
            .description("The timeout setting for HTTP(S) client."),
        ParameterSpec::runtime("http_max_retries")
            .description("Maximum number of retries for HTTP requests. Default: 3"),
        ParameterSpec::runtime("http_post_content_type")
            .description("Content-Type header for POST requests when using _body filter. Defaults to 'application/json'."),
    ]);
    all_parameters.extend_from_slice(LISTING_TABLE_PARAMETERS);
    all_parameters
});

impl DataConnectorFactory for HttpsFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Ok(Arc::new(Https {
                params: params.parameters,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "http"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
    }
}
