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
use tokio::runtime::Handle;
use url::Url;

use super::{ConnectorComponent, ConnectorParams};
use super::{
    DataConnector, DataConnectorError, DataConnectorFactory, DataConnectorResult, ParameterSpec,
    Parameters,
    listing::{self, ListingTableConnector},
};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use reqwest::Client;
use std::time::Duration;

#[derive(Debug)]
pub struct Https {
    params: Parameters,
    tokio_io_runtime: Handle,
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
        // Determine file format
        let format = self
            .params
            .get("file_format")
            .expose()
            .ok()
            .map(str::to_ascii_lowercase);

        // If file_format is not specified, default to auto-detection
        // If file_format is set to 'json' or 'auto', use the HTTP table provider
        // with virtual _path/_query/content columns
        let use_http_provider =
            format.is_none() || matches!(format.as_deref(), Some("json" | "auto"));

        if use_http_provider {
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
                .unwrap_or(30);

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

            let file_format = format.unwrap_or_else(|| "auto".to_string());
            let acceleration_enabled = dataset.is_accelerated();

            // Handle flatten_json parameter
            // If set to empty string or "true", use default delimiter "_"
            // If set to a specific value, use that as the delimiter
            // If not set or "false", don't flatten
            let flatten_json = self
                .params
                .get("flatten_json")
                .expose()
                .ok()
                .and_then(|val| {
                    let val_lower = val.to_lowercase();
                    if val_lower == "false" || val_lower == "no" {
                        None
                    } else if val_lower == "true" || val_lower == "yes" || val.is_empty() {
                        Some(String::new()) // Empty string will be converted to "_" in with_flatten_json
                    } else {
                        Some(val.to_string())
                    }
                });

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
            .with_flatten_json(flatten_json)
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
        } else {
            // For other formats, use the listing table connector
            // Create a wrapper that implements ListingTableConnector
            let wrapper = HttpsListingWrapper {
                params: self.params.clone(),
                tokio_io_runtime: self.tokio_io_runtime.clone(),
            };
            <HttpsListingWrapper as DataConnector>::read_provider(&wrapper, dataset).await
        }
    }
}

// Wrapper struct to use the ListingTableConnector implementation
#[derive(Debug, Clone)]
struct HttpsListingWrapper {
    params: Parameters,
    tokio_io_runtime: Handle,
}

impl std::fmt::Display for HttpsListingWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "https")
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
            .description("Content-Type header for POST requests when using _body filter."),
        ParameterSpec::runtime("flatten_json")
            .description("Flatten JSON response into columns. Use 'true' for default delimiter '_', or specify a custom delimiter."),
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
                tokio_io_runtime: params.io_runtime,
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

impl ListingTableConnector for HttpsListingWrapper {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_tokio_io_runtime(&self) -> tokio::runtime::Handle {
        self.tokio_io_runtime.clone()
    }

    fn get_object_store_url(
        &self,
        dataset: &Dataset,
        url: Option<&str>,
    ) -> DataConnectorResult<Url> {
        let url = url.unwrap_or(dataset.from.as_str());
        let mut u = Url::parse(url).boxed().map_err(|e| {
            DataConnectorError::InvalidConfiguration {
                dataconnector: "https".to_string(),
                message: format!("{url} is not a valid URL. Ensure the URL is valid and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/https"),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            }
        })?;

        if let Some(p) = self.params.get("port").expose().ok() {
            let n = match p.parse::<u16>() {
                Ok(n) => n,
                Err(e) => {
                    return Err(DataConnectorError::InvalidConfiguration {
                        dataconnector: "https".to_string(),
                        message: "The specified `https_port` parameter was invalid. Specify a valid port number and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/https#parameters".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: Box::new(e),
                    });
                }
            };
            let _ = u.set_port(Some(n));
        }

        if let Some(p) = self.params.get("password").expose().ok()
            && u.set_password(Some(p)).is_err()
        {
            return Err(
                DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                    dataconnector: "https".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                },
            );
        }

        if let Some(p) = self.params.get("username").expose().ok()
            && u.set_username(p).is_err()
        {
            return Err(
                DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                    dataconnector: "https".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                },
            );
        }

        u.set_fragment(Some(&listing::build_fragments(
            &self.params,
            vec!["client_timeout"],
        )));

        Ok(u)
    }
}
