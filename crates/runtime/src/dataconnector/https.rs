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
use crate::dataconnector::listing::{
    LISTING_TABLE_PARAMETERS, ListingTableConnector, build_fragments,
};

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

impl Https {
    /// Create HTTP table provider for JSON API endpoints
    fn create_http_table_provider(
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
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .boxed()
            .map_err(|e| DataConnectorError::InternalWithSource {
                dataconnector: "https".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            })?;

        let file_format = self
            .params
            .get("file_format")
            .expose()
            .ok()
            .map_or_else(|| "auto".to_string(), str::to_ascii_lowercase);

        let acceleration_enabled = dataset.is_accelerated();

        let max_retries = self
            .params
            .get("max_retries")
            .expose()
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(3);

        let backoff_method = self
            .params
            .get("retry_backoff_method")
            .expose()
            .ok()
            .and_then(|v| v.parse::<util::retry_strategy::BackoffMethod>().ok())
            .unwrap_or(util::retry_strategy::BackoffMethod::Fibonacci);

        let max_retry_duration = self
            .params
            .get("retry_max_duration")
            .expose()
            .ok()
            .and_then(|v| fundu::parse_duration(v).ok());

        let retry_jitter = self
            .params
            .get("retry_jitter")
            .expose()
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.3);

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
        .with_backoff_method(backoff_method)
        .with_max_retry_duration(max_retry_duration)
        .with_retry_jitter(retry_jitter)
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

#[async_trait]
impl DataConnector for Https {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        // Determine file format - default to "auto" if not specified
        let file_format = self
            .params
            .get("file_format")
            .expose()
            .ok()
            .map_or_else(|| "auto".to_string(), str::to_ascii_lowercase);

        // For structured file formats (parquet, csv, arrow, avro), delegate to ListingTableConnector
        // which properly handles file parsing with correct schemas
        let is_structured_format = matches!(
            file_format.as_str(),
            "parquet" | "csv" | "tsv" | "arrow" | "avro"
        );

        if is_structured_format {
            // Use ListingTableConnector for file-based structured formats
            let listing_connector =
                HttpListingConnector::new(self.params.clone(), Handle::current());
            return listing_connector.read_provider(dataset).await;
        }

        // For JSON API endpoints and other formats, use HttpTableProvider
        self.create_http_table_provider(dataset)
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
        ParameterSpec::runtime("max_retries")
            .description("Maximum number of retries for HTTP requests. Default: 3"),
        ParameterSpec::runtime("retry_backoff_method")
            .description("Retry backoff method: 'fibonacci' (default), 'linear', or 'exponential'."),
        ParameterSpec::runtime("retry_max_duration")
            .description("Maximum total duration for all retries (e.g., '30s', '5m'). If not set, retries will continue up to max_retries."),
        ParameterSpec::runtime("retry_jitter")
            .description("Randomization factor for retry delays (0.0 to 1.0). Default: 0.3 (30% randomization). Set to 0 for no jitter."),
        ParameterSpec::runtime("http_post_content_type")
            .description("Content-Type header for POST requests when using request_body filter. Defaults to 'application/json'."),
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

#[derive(Debug)]
pub struct HttpListingConnector {
    params: Parameters,
    tokio_io_runtime: Handle,
}

impl HttpListingConnector {
    #[must_use]
    pub fn new(params: Parameters, tokio_io_runtime: Handle) -> Self {
        HttpListingConnector {
            params,
            tokio_io_runtime,
        }
    }
}

impl std::fmt::Display for HttpListingConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "http_listing")
    }
}

impl ListingTableConnector for HttpListingConnector {
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

        u.set_fragment(Some(&build_fragments(&self.params, vec!["client_timeout"])));

        Ok(u)
    }
}
