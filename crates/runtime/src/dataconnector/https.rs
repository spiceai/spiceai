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
use reqwest::{
    Client,
    header::{HeaderMap, HeaderName, HeaderValue},
};
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

struct HttpProviderParams {
    file_format: String,
    acceleration_enabled: bool,
    max_retries: u32,
    backoff_method: util::retry_strategy::BackoffMethod,
    max_retry_duration: Option<Duration>,
    retry_jitter: f64,
    custom_headers: HeaderMap,
    allowed_paths: Vec<String>,
    allow_query_filters: bool,
    max_query_length: usize,
    allow_body_filters: bool,
    max_body_bytes: usize,
}

impl Https {
    fn resolve_http_provider_params(&self, dataset: &Dataset) -> HttpProviderParams {
        let file_format = self
            .params
            .get("file_format")
            .expose()
            .ok()
            .map_or_else(|| "auto".to_string(), str::to_ascii_lowercase);

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

        let custom_headers = self.parse_custom_headers(&dataset.name.to_string());

        let allowed_paths = self
            .params
            .get("allowed_request_paths")
            .expose()
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let allow_query_filters = self
            .params
            .get("request_query_filters")
            .expose()
            .ok()
            .is_some_and(util::parse_enabled);

        let max_query_length = self
            .params
            .get("max_request_query_length")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(data_components::http::provider::DEFAULT_MAX_QUERY_LENGTH);

        let allow_body_filters = self
            .params
            .get("request_body_filters")
            .expose()
            .ok()
            .is_some_and(util::parse_enabled);

        let max_body_bytes = self
            .params
            .get("max_request_body_bytes")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(data_components::http::provider::DEFAULT_MAX_BODY_BYTES);

        HttpProviderParams {
            file_format,
            acceleration_enabled: dataset.is_accelerated(),
            max_retries,
            backoff_method,
            max_retry_duration,
            retry_jitter,
            custom_headers,
            allowed_paths,
            allow_query_filters,
            max_query_length,
            allow_body_filters,
            max_body_bytes,
        }
    }

    fn apply_allowed_paths(
        dataset: &Dataset,
        provider: data_components::http::provider::HttpTableProvider,
        allowed_paths: Vec<String>,
    ) -> DataConnectorResult<data_components::http::provider::HttpTableProvider> {
        if allowed_paths.is_empty() {
            return Ok(provider);
        }

        let component = ConnectorComponent::from(dataset);
        provider.with_allowed_paths(allowed_paths).map_err(|e| {
            let message = format!("Invalid allowed_request_paths configuration: {e}");
            DataConnectorError::InvalidConfiguration {
                dataconnector: "https".to_string(),
                message,
                connector_component: component,
                source: Box::new(e),
            }
        })
    }

    fn spawn_endpoint_validation(
        provider: Arc<data_components::http::provider::HttpTableProvider>,
        dataset_name: String,
    ) {
        tokio::spawn(async move {
            if let Err(e) = provider.validate_endpoint().await {
                tracing::warn!(
                    "HTTP endpoint validation failed for dataset '{}': {}. \
                    The endpoint may be temporarily unavailable or misconfigured. \
                    Queries will continue but may fail if the endpoint is not accessible.",
                    dataset_name,
                    e
                );
            }
        });
    }

    /// Parse HTTP headers from the `http_headers` parameter
    fn parse_custom_headers(&self, dataset_name: &str) -> HeaderMap {
        let mut custom_headers = HeaderMap::new();
        if let Some(headers_str) = self.params.get("http_headers").expose().ok() {
            for header in headers_str.split(',') {
                let parts: Vec<&str> = header.splitn(2, ':').collect();
                if parts.len() == 2 {
                    let name = parts[0].trim();
                    let value = parts[1].trim();

                    if let (Ok(header_name), Ok(header_value)) =
                        (HeaderName::try_from(name), HeaderValue::from_str(value))
                    {
                        custom_headers.insert(header_name, header_value);
                    } else {
                        tracing::warn!(
                            "Invalid HTTP header in dataset '{dataset_name}': '{header}'. Skipping this header."
                        );
                    }
                } else {
                    tracing::warn!(
                        "Malformed HTTP header in dataset '{dataset_name}': '{header}'. Expected format 'Name: Value'. Skipping this header."
                    );
                }
            }
        }
        custom_headers
    }

    /// Build HTTP client with configured timeouts and connection pool settings
    fn build_http_client(&self, dataset: &Dataset) -> DataConnectorResult<Client> {
        let timeout_secs = self
            .params
            .get("client_timeout")
            .expose()
            .ok()
            .and_then(|t| t.parse::<u64>().ok())
            .unwrap_or(DEFAULT_CLIENT_TIMEOUT_SECS);

        let connect_timeout_secs = self
            .params
            .get("connect_timeout")
            .expose()
            .ok()
            .and_then(|t| t.parse::<u64>().ok())
            .unwrap_or(10);

        let pool_max_idle_per_host = self
            .params
            .get("pool_max_idle_per_host")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(10);

        let pool_idle_timeout_secs = self
            .params
            .get("pool_idle_timeout")
            .expose()
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(90);

        Client::builder()
            .user_agent("spice")
            .connect_timeout(Duration::from_secs(connect_timeout_secs))
            .timeout(Duration::from_secs(timeout_secs))
            .pool_max_idle_per_host(pool_max_idle_per_host)
            .pool_idle_timeout(Duration::from_secs(pool_idle_timeout_secs))
            .build()
            .boxed()
            .map_err(|e| DataConnectorError::InternalWithSource {
                dataconnector: "https".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            })
    }

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

        let client = self.build_http_client(dataset)?;

        let HttpProviderParams {
            file_format,
            acceleration_enabled,
            max_retries,
            backoff_method,
            max_retry_duration,
            retry_jitter,
            custom_headers,
            allowed_paths,
            allow_query_filters,
            max_query_length,
            allow_body_filters,
            max_body_bytes,
        } = self.resolve_http_provider_params(dataset);

        let mut provider = data_components::http::provider::HttpTableProvider::new(
            base_url,
            client,
            file_format,
            acceleration_enabled,
        )
        .with_max_retries(max_retries)
        .with_backoff_method(backoff_method)
        .with_max_retry_duration(max_retry_duration)
        .with_retry_jitter(retry_jitter)
        .with_headers(custom_headers);

        provider = Self::apply_allowed_paths(dataset, provider, allowed_paths)?;

        tracing::trace!(
            "HTTP provider configuration for {}: allow_query_filters={}, allow_body_filters={}",
            dataset.name,
            allow_query_filters,
            allow_body_filters
        );

        if allow_query_filters {
            tracing::trace!(
                "Enabling query filters with max_length={}",
                max_query_length
            );
            provider = provider.enable_query_filters(max_query_length);
        }

        if allow_body_filters {
            tracing::trace!("Enabling body filters with max_bytes={}", max_body_bytes);
            provider = provider.enable_body_filters(max_body_bytes);
        }

        let provider = Arc::new(provider);
        Self::spawn_endpoint_validation(Arc::clone(&provider), dataset.name.to_string());

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
        let mut is_structured_format = matches!(
            file_format.as_str(),
            "parquet" | "csv" | "tsv" | "arrow" | "avro"
        );

        // If file_format is "auto", try to detect from URL extension
        if file_format == "auto"
            && let Ok(url) = Url::parse(&dataset.from)
            && let Some(mut path) = url.path_segments()
            && let Some(last_segment) = path.next_back()
        {
            let extension = last_segment
                .split('.')
                .next_back()
                .map(str::to_ascii_lowercase)
                .unwrap_or_default();

            is_structured_format = matches!(
                extension.as_str(),
                "parquet" | "csv" | "tsv" | "arrow" | "avro"
            );
        }

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
            .description("The timeout setting for HTTP(S) client requests (in seconds). Default: 30"),
        ParameterSpec::runtime("connect_timeout")
            .description("The timeout for establishing HTTP(S) connections (in seconds). Default: 10"),
        ParameterSpec::runtime("pool_max_idle_per_host")
            .description("Maximum number of idle connections to keep alive per host. Default: 10"),
        ParameterSpec::runtime("pool_idle_timeout")
            .description("Timeout for idle connections in the pool (in seconds). Default: 90"),
        ParameterSpec::runtime("http_headers")
            .description("Custom HTTP headers to include in requests. Format: 'Header1: Value1, Header2: Value2'. Headers are applied to all requests."),
        ParameterSpec::runtime("max_retries")
            .description("Maximum number of retries for HTTP requests. Default: 3"),
        ParameterSpec::runtime("retry_backoff_method")
            .description("Retry backoff method: 'fibonacci' (default), 'linear', or 'exponential'."),
        ParameterSpec::runtime("retry_max_duration")
            .description("Maximum total duration for all retries (e.g., '30s', '5m'). If not set, retries will continue up to max_retries."),
        ParameterSpec::runtime("retry_jitter")
            .description("Randomization factor for retry delays (0.0 to 1.0). Default: 0.3 (30% randomization). Set to 0 for no jitter."),
        ParameterSpec::runtime("allowed_request_paths")
            .description("Comma-separated list of request_path values that users are allowed to query. Required to enable request_path filters."),
        ParameterSpec::runtime("request_query_filters")
            .description("Set to 'enabled' or 'disabled' to control whether request_query filters can be pushed down to HTTP requests.")
            .one_of(&["enabled", "disabled"]),
        ParameterSpec::runtime("max_request_query_length")
            .description("Maximum length (in characters) for request_query filter values. Default: 1024."),
        ParameterSpec::runtime("request_body_filters")
            .description("Set to 'enabled' or 'disabled' to control whether request_body filters can be pushed down as HTTP request bodies.")
            .one_of(&["enabled", "disabled"]),
        ParameterSpec::runtime("max_request_body_bytes")
            .description("Maximum size (in bytes) for request_body filter values. Default: 16384 (16KiB)."),
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
