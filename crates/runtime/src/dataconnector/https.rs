/*
Copyright 2024-2026 The Spice.ai OSS Authors

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
use crate::component::dataset::acceleration::RefreshMode;
use crate::component::metrics::MetricsProvider;
use crate::component::{ComponentInitialization, DatasetHealthMonitor, StartupOptions};
use crate::dataconnector::http_rate_control::{
    self, HttpRateControlConfig, HttpRateControlMetricSource, HttpRateControlMetrics,
    HttpRateControlMetricsProvider,
};
use crate::dataconnector::listing::{
    LISTING_TABLE_PARAMETERS, ListingTableConnector, build_fragments,
    detect_file_extension_from_url_or_path, parse_file_extension_param,
};

use data_components::http::auth::{
    ClientAuthMethod, HttpAuthenticator, RefreshTokenAuth, RefreshTokenConfig,
};
use data_components::http::json_nest::HttpJsonNesting;
use data_components::rate_limit::RateLimiter;
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use snafu::prelude::*;
use spicepod::semantic::Column;
use std::any::Any;
use std::collections::HashMap;
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

fn parse_pagination_max_pages(value: &str) -> Option<usize> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("nolimit") {
        return None;
    }

    if let Ok(max_pages) = trimmed.parse::<usize>() {
        Some(max_pages)
    } else {
        tracing::warn!(
            "Invalid pagination_max_pages value '{}': expected a positive integer or 'nolimit'. The parameter will be ignored.",
            value
        );
        Some(data_components::http::provider::DEFAULT_PAGINATION_MAX_PAGES)
    }
}

#[derive(Debug)]
pub struct Https {
    params: Parameters,
    runtime_rate_control_params: Option<HashMap<String, String>>,
    rate_control_registry: Arc<http_rate_control::HttpRateControlRegistry>,
    metrics: Arc<HttpRateControlMetrics>,
    emit_rate_control_metrics: bool,
    rate_control_metric_source: Option<HttpRateControlMetricSource>,
}

impl std::fmt::Display for Https {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "https")
    }
}

impl Https {
    fn shared_rate_control_metrics_for_dataset(
        rate_control_registry: &http_rate_control::HttpRateControlRegistry,
        rate_control_registry_arc: &Arc<http_rate_control::HttpRateControlRegistry>,
        dataset: &Dataset,
        structured_format: bool,
    ) -> (
        Arc<HttpRateControlMetrics>,
        bool,
        Option<HttpRateControlMetricSource>,
    ) {
        if structured_format {
            return (Arc::new(HttpRateControlMetrics::default()), false, None);
        }

        Url::parse(dataset.from.as_str()).map_or_else(
            |_| (Arc::new(HttpRateControlMetrics::default()), false, None),
            |url| {
                let metric_source = HttpRateControlMetricSource::new(
                    Arc::clone(rate_control_registry_arc),
                    url.clone(),
                    dataset.name.to_string(),
                );
                (
                    rate_control_registry.shared_metrics(&url),
                    true,
                    Some(metric_source),
                )
            },
        )
    }

    /// Determines if the dataset uses a structured file format (parquet, csv, json, etc.)
    /// that would be handled by `ListingTableConnector` rather than `HttpTableProvider`.
    fn is_structured_format(&self, dataset: &Dataset) -> bool {
        let file_format = self
            .params
            .get("file_format")
            .expose()
            .ok()
            .map_or_else(|| "auto".to_string(), str::to_ascii_lowercase);

        // Check if explicitly configured as a structured format
        if matches!(
            file_format.as_str(),
            "parquet"
                | "csv"
                | "tsv"
                | "arrow"
                | "avro"
                | "jsonl"
                | "ndjson"
                | "ldjson"
                | "soda"
                | "socrata"
        ) {
            return true;
        }

        // JSON format is structured only for static file endpoints.
        // Dynamic API endpoints (with allowed_request_paths, request_query_filters, etc.)
        // should use HttpTableProvider instead.
        if file_format == "json" && !self.has_dynamic_api_params() {
            return true;
        }

        // If file_format is "auto", try to detect from URL extension
        if file_format == "auto" {
            let extension = self
                .params
                .get("file_extension")
                .expose()
                .ok()
                .and_then(parse_file_extension_param)
                .or_else(|| detect_file_extension_from_url_or_path(&dataset.from))
                .and_then(|extension| extension.format_extension);

            if matches!(
                extension.as_deref(),
                Some("parquet" | "csv" | "tsv" | "arrow" | "avro" | "jsonl" | "ndjson" | "ldjson")
            ) {
                return true;
            }

            if extension.as_deref() == Some("json") && !self.has_dynamic_api_params() {
                return true;
            }
        }

        false
    }

    /// Returns true if the connector is configured with parameters that indicate
    /// a dynamic HTTP API endpoint (as opposed to a static file download).
    fn has_dynamic_api_params(&self) -> bool {
        params_indicate_dynamic_api(&self.params)
    }

    fn ensure_rate_control_supported_for_structured_dataset(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<()> {
        let rate_control = http_rate_control::resolve_config(
            &self.params,
            self.runtime_rate_control_params.as_ref(),
            dataset,
            "https",
        )?;

        if rate_control.is_enabled() {
            return Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "https".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: "HTTP rate-control parameters are not supported for structured HTTP file datasets that use the listing connector. Remove max_concurrent_requests, requests_per_second_limit, requests_per_minute_limit, rate_control_jitter_min, and rate_control_jitter_max, or use a dynamic JSON HTTP API dataset.".to_string(),
            });
        }

        Ok(())
    }
}

struct RequestFilterParams {
    allow_query_filters: bool,
    max_query_length: usize,
    allow_body_filters: bool,
    max_body_bytes: usize,
    allow_header_filters: bool,
    max_headers_length: usize,
    request_header_allowlist: Vec<String>,
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
    request_filters: RequestFilterParams,
    rate_control: HttpRateControlConfig,
    max_request_partitions: Option<usize>,
    health_probe: Option<String>,
    pagination: Option<data_components::http::provider::PaginationConfig>,
}

impl Https {
    fn resolve_http_provider_params(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<HttpProviderParams> {
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

        let allow_header_filters = self
            .params
            .get("request_header_filters")
            .expose()
            .ok()
            .is_some_and(util::parse_enabled);

        let max_headers_length = self
            .params
            .get("max_request_headers_length")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(data_components::http::provider::DEFAULT_MAX_HEADERS_LENGTH);

        let request_header_allowlist = self
            .params
            .get("request_header_allowlist")
            .expose()
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(|header_name| header_name.trim().to_string())
                    .filter(|header_name| !header_name.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let rate_control = http_rate_control::resolve_config(
            &self.params,
            self.runtime_rate_control_params.as_ref(),
            dataset,
            "https",
        )?;

        let max_request_partitions = self
            .params
            .get("max_request_partitions")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|value| *value > 0);

        let health_probe = self
            .params
            .get("health_probe")
            .expose()
            .ok()
            .map(std::string::ToString::to_string);

        let pagination_mode =
            self.params
                .get("pagination")
                .expose()
                .ok()
                .map_or("auto", |v| match v {
                    "enabled" | "true" => "enabled",
                    "disabled" | "false" => "disabled",
                    _ => "auto",
                });

        let pagination = if pagination_mode == "disabled" {
            None
        } else {
            let next_pointer = self
                .params
                .get("pagination_next_pointer")
                .expose()
                .ok()
                .map(std::string::ToString::to_string);

            let link_header_param = self.params.get("pagination_link_header").expose().ok();
            let use_link_header = link_header_param.is_none_or(util::parse_enabled);

            let token_param = self
                .params
                .get("pagination_token_param")
                .expose()
                .ok()
                .map(std::string::ToString::to_string);

            let data_pointer = self
                .params
                .get("pagination_data_pointer")
                .expose()
                .ok()
                .map(std::string::ToString::to_string);

            let max_pages = self
                .params
                .get("pagination_max_pages")
                .expose()
                .ok()
                .map_or(
                    Some(data_components::http::provider::DEFAULT_PAGINATION_MAX_PAGES),
                    parse_pagination_max_pages,
                );

            let data_map_to_array = self
                .params
                .get("pagination_data_map_to_array")
                .expose()
                .ok()
                .is_some_and(util::parse_enabled);

            let query_params = self
                .params
                .get("pagination_query_params")
                .expose()
                .ok()
                .map(std::string::ToString::to_string);

            let page_size_raw = self.params.get("pagination_page_size").expose().ok();
            let page_size = page_size_raw.and_then(|v| match v.parse::<usize>() {
                Ok(0) => {
                    tracing::warn!(
                        "Invalid pagination_page_size value '0': must be greater than 0. The parameter will be ignored."
                    );
                    None
                }
                Ok(n) => Some(n),
                Err(_) => {
                    tracing::warn!(
                        "Invalid pagination_page_size value '{}': expected a positive integer. The parameter will be ignored.",
                        v
                    );
                    None
                }
            });

            // In 'auto' mode with no explicit pagination sub-params,
            // use Link header detection only (respecting pagination_link_header if set).
            if pagination_mode == "auto"
                && next_pointer.is_none()
                && token_param.is_none()
                && data_pointer.is_none()
                && link_header_param.is_none()
                && query_params.is_none()
                && !data_map_to_array
            {
                Some(data_components::http::provider::PaginationConfig {
                    next_pointer: None,
                    use_link_header: true,
                    token_param: None,
                    data_pointer: None,
                    max_pages,
                    data_map_to_array: false,
                    query_params: None,
                    page_size: None,
                })
            } else {
                Some(data_components::http::provider::PaginationConfig {
                    next_pointer,
                    use_link_header,
                    token_param,
                    data_pointer,
                    max_pages,
                    data_map_to_array,
                    query_params,
                    page_size,
                })
            }
        };

        Ok(HttpProviderParams {
            file_format,
            acceleration_enabled: dataset.is_accelerated(),
            max_retries,
            backoff_method,
            max_retry_duration,
            retry_jitter,
            custom_headers,
            allowed_paths,
            request_filters: RequestFilterParams {
                allow_query_filters,
                max_query_length,
                allow_body_filters,
                max_body_bytes,
                allow_header_filters,
                max_headers_length,
                request_header_allowlist,
            },
            rate_control,
            max_request_partitions,
            health_probe,
            pagination,
        })
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
            // Split by semicolon or comma
            let delimiter = if headers_str.contains(';') { ';' } else { ',' };
            for header in headers_str.split(delimiter) {
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
            .user_agent(util::spiceai_user_agent())
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

    /// Parse `OAuth2` refresh-token parameters.
    ///
    /// Returns `Ok(None)` when no auth is configured. Returns an error when
    /// the auth configuration is incomplete or inconsistent (e.g. a refresh
    /// token without a token URL).
    fn resolve_refresh_token_auth(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Option<(RefreshTokenConfig, SecretString)>> {
        let token_url = self
            .params
            .get("auth_token_url")
            .expose()
            .ok()
            .map(str::trim)
            .filter(|v| !v.is_empty());

        // Treat a blank/whitespace-only refresh token as unset — avoids failing
        // at the token endpoint with "invalid_grant" when the real problem is a
        // misconfigured (empty) secret.
        let refresh_token = self
            .params
            .get("auth_refresh_token")
            .ok()
            .filter(|s| !s.expose_secret().trim().is_empty());

        match (token_url, refresh_token) {
            (None, None) => Ok(None),
            (Some(_), None) => Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "https".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: format!(
                    "'{}' is set but '{}' is missing or empty. Provide a refresh token to use OAuth2 auth.",
                    self.params.user_param("auth_token_url"),
                    self.params.user_param("auth_refresh_token"),
                ),
            }),
            (None, Some(_)) => Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "https".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: format!(
                    "'{}' is set but '{}' is missing. Provide the OAuth2 token endpoint URL.",
                    self.params.user_param("auth_refresh_token"),
                    self.params.user_param("auth_token_url"),
                ),
            }),
            (Some(token_url), Some(refresh_token)) => {
                let client_id = self
                    .params
                    .get("auth_client_id")
                    .expose()
                    .ok()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(str::to_string);
                let client_credential = self.params.get("auth_client_secret").ok().cloned();

                if client_credential.is_some() && client_id.is_none() {
                    return Err(DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: "https".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        message: format!(
                            "'{}' is set but '{}' is missing.",
                            self.params.user_param("auth_client_secret"),
                            self.params.user_param("auth_client_id"),
                        ),
                    });
                }

                let scopes = self
                    .params
                    .get("auth_scopes")
                    .expose()
                    .ok()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(str::to_string);

                let client_auth = match self.params.get("auth_client_auth").expose().ok() {
                    Some(v) => ClientAuthMethod::parse(v).map_err(|bad| {
                        DataConnectorError::InvalidConfigurationNoSource {
                            dataconnector: "https".to_string(),
                            connector_component: ConnectorComponent::from(dataset),
                            message: format!(
                                "'{}' must be 'basic' or 'body', got '{bad}'",
                                self.params.user_param("auth_client_auth"),
                            ),
                        }
                    })?,
                    None => ClientAuthMethod::default(),
                };

                Ok(Some((
                    RefreshTokenConfig {
                        token_url: token_url.to_string(),
                        client_id,
                        client_secret: client_credential,
                        scopes,
                        client_auth,
                    },
                    refresh_token.clone(),
                )))
            }
        }
    }

    /// Classify a [`data_components::http::auth::Error`] as either an invalid-
    /// configuration problem (so the user knows to fix their spicepod) or a
    /// connection / runtime problem. Bad URLs, bad URL schemes, and definitive
    /// credential rejections (400/401/403 from the token endpoint) are
    /// configuration issues; transport, 5xx, 408/429 (transient), and parse
    /// errors are connection-level.
    fn map_auth_error(
        dataset: &Dataset,
        err: data_components::http::auth::Error,
    ) -> DataConnectorError {
        use data_components::http::auth::Error as AuthErr;
        let component = ConnectorComponent::from(dataset);
        let dataconnector = "https".to_string();

        match err {
            AuthErr::InvalidTokenUrl { .. }
            | AuthErr::InsecureTokenUrl { .. }
            | AuthErr::UnsupportedTokenType { .. }
            | AuthErr::TokenEndpointStatus {
                status: 400 | 401 | 403,
                ..
            } => DataConnectorError::InvalidConfiguration {
                dataconnector,
                message: err.to_string(),
                connector_component: component,
                source: Box::new(err),
            },
            _ => DataConnectorError::UnableToConnectInternal {
                dataconnector,
                connector_component: component,
                source: Box::new(err),
            },
        }
    }

    /// Create HTTP table provider for JSON API endpoints
    async fn create_http_table_provider(
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
            request_filters,
            rate_control,
            max_request_partitions,
            health_probe,
            pagination,
        } = self.resolve_http_provider_params(dataset)?;

        let RequestFilterParams {
            allow_query_filters,
            max_query_length,
            allow_body_filters,
            max_body_bytes,
            allow_header_filters,
            max_headers_length,
            request_header_allowlist,
        } = request_filters;

        let mut provider = data_components::http::provider::HttpTableProvider::new(
            base_url.clone(),
            client,
            file_format,
            acceleration_enabled,
        )
        .with_max_retries(max_retries)
        .with_backoff_method(backoff_method)
        .with_max_retry_duration(max_retry_duration)
        .with_retry_jitter(retry_jitter)
        .with_headers(custom_headers)
        .with_max_request_partitions(max_request_partitions)
        .with_health_probe(health_probe)
        .map_err(|e| DataConnectorError::InvalidConfiguration {
            dataconnector: "https".to_string(),
            message: format!("Invalid health_probe configuration: {e}"),
            connector_component: ConnectorComponent::from(dataset),
            source: e.into(),
        })?;

        if let Some(nesting) = parse_http_json_nesting(dataset)? {
            let schema = build_json_nest_schema(dataset, &nesting).map_err(|e| {
                DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: "https".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    message: e.to_string(),
                }
            })?;
            provider = provider.with_json_nesting(nesting, schema);
        }

        if let Some((auth_config, refresh_token)) = self.resolve_refresh_token_auth(dataset)? {
            // Fail fast if the user also set an Authorization custom header:
            // reqwest would append ours after theirs and send two Authorization
            // values, which most servers will reject in non-obvious ways.
            if provider
                .custom_headers()
                .contains_key(reqwest::header::AUTHORIZATION)
            {
                return Err(DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: "https".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    message: format!(
                        "OAuth2 auth is configured (via '{}') but an 'Authorization' header is also set in '{}'. Remove one of them.",
                        self.params.user_param("auth_refresh_token"),
                        self.params.user_param("http_headers"),
                    ),
                });
            }

            let auth = RefreshTokenAuth::try_new(auth_config, refresh_token)
                .await
                .map_err(|e| Self::map_auth_error(dataset, e))?;
            let auth: Arc<dyn HttpAuthenticator> = Arc::new(auth);
            provider = provider.with_auth(auth);
        }

        provider = Self::apply_allowed_paths(dataset, provider, allowed_paths)?;

        tracing::trace!(
            "HTTP provider configuration for {}: allow_query_filters={}, allow_body_filters={}, allow_header_filters={}, max_request_partitions={:?}",
            dataset.name,
            allow_query_filters,
            allow_body_filters,
            allow_header_filters,
            max_request_partitions
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

        if allow_header_filters {
            tracing::trace!(
                "Enabling header filters with max_length={} and {} allowed header names",
                max_headers_length,
                request_header_allowlist.len()
            );
            provider = provider
                .enable_header_filters(max_headers_length, request_header_allowlist)
                .map_err(|e| DataConnectorError::InvalidConfiguration {
                    dataconnector: "https".to_string(),
                    message: format!("Invalid request header filter configuration: {e}"),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e.into(),
                })?;
        }

        if let Some(pagination_config) = pagination {
            let max_pages = pagination_config
                .max_pages
                .map_or_else(|| "nolimit".to_string(), |max_pages| max_pages.to_string());
            tracing::trace!(
                "Enabling pagination for {}: next_pointer={:?}, link_header={}, token_param={:?}, data_pointer={:?}, max_pages={}, data_map_to_array={}, query_params={:?}, page_size={:?}",
                dataset.name,
                pagination_config.next_pointer,
                pagination_config.use_link_header,
                pagination_config.token_param,
                pagination_config.data_pointer,
                max_pages,
                pagination_config.data_map_to_array,
                pagination_config.query_params,
                pagination_config.page_size,
            );
            provider = provider.with_pagination(pagination_config).map_err(|e| {
                DataConnectorError::InvalidConfiguration {
                    dataconnector: "https".to_string(),
                    message: format!("Invalid pagination configuration: {e}"),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e.into(),
                }
            })?;
        }

        let rate_limiter = self
            .rate_control_registry
            .shared_rate_limiter(&base_url)
            .await;
        self.metrics.set_rate_limiter(&rate_limiter);
        let rate_limiter: Arc<dyn RateLimiter> = rate_limiter;
        let rate_controller = Arc::clone(&self.rate_control_registry)
            .reserve_shared_rate_controller(&base_url, &rate_control, dataset, "https")
            .await?;
        self.metrics.set_config(&rate_controller.shared().config);
        self.metrics
            .set_rate_controller(rate_controller.shared().controller.as_ref());
        provider = provider
            .with_rate_limiter(Some(rate_limiter))
            .with_rate_controller(rate_controller.shared().controller.clone());

        let provider = Arc::new(provider);
        if let Some(metric_source) = &self.rate_control_metric_source {
            let _ = metric_source.claim_owner();
        }
        rate_controller.commit().await;
        Self::spawn_endpoint_validation(Arc::clone(&provider), dataset.name.to_string());

        Ok(provider)
    }
}

/// Returns true if the supplied connector parameters indicate a
/// dynamic HTTP API endpoint (as opposed to a static file download).
/// Mirrors `Https::has_dynamic_api_params`, which is the canonical
/// runtime check; kept as a free function so the `HttpsFactory`
/// can run the same gate before constructing a connector.
fn params_indicate_dynamic_api(params: &Parameters) -> bool {
    let has_allowed_paths = params
        .get("allowed_request_paths")
        .expose()
        .ok()
        .is_some_and(|v| !v.is_empty());

    let has_query_filters = params
        .get("request_query_filters")
        .expose()
        .ok()
        .is_some_and(util::parse_enabled);

    let has_body_filters = params
        .get("request_body_filters")
        .expose()
        .ok()
        .is_some_and(util::parse_enabled);

    let has_header_filters = params
        .get("request_header_filters")
        .expose()
        .ok()
        .is_some_and(util::parse_enabled);

    let has_pagination = params
        .get("pagination")
        .expose()
        .ok()
        .is_some_and(|v| v == "enabled" || v == "true" || v == "auto")
        || [
            "pagination_next_pointer",
            "pagination_token_param",
            "pagination_data_pointer",
            "pagination_link_header",
            "pagination_max_pages",
            "pagination_data_map_to_array",
            "pagination_query_params",
            "pagination_page_size",
        ]
        .iter()
        .any(|key| params.get(key).expose().ok().is_some());

    has_allowed_paths
        || has_query_filters
        || has_body_filters
        || has_header_filters
        || has_pagination
}

/// Build the schema for a JSON-nested HTTP table from the user's
/// declared `columns:` block. The schema's field order matches
/// `nesting.column_order` (already validated to equal the declared
/// column order). Each field's Arrow type is the user's declared
/// `type:` if present, defaulting to `Utf8` when omitted (matching
/// the historical behavior). Nullability is the declared `nullable:`,
/// defaulting to `true`.
fn build_json_nest_schema(
    dataset: &Dataset,
    nesting: &HttpJsonNesting,
) -> Result<arrow_schema::SchemaRef, crate::component::dataset::declared_type::ParseTypeError> {
    use crate::component::dataset::declared_type::parse_declared_type;

    let base = data_components::http::provider::HttpTableProvider::base_table_schema();
    let mut fields = Vec::with_capacity(nesting.column_order.len());
    for name in &nesting.column_order {
        // HTTP metadata columns inherit their type from the base schema
        // so the metadata-population path can write the right Arrow type
        // (e.g. `response_status` is `UInt16`, not `Utf8`).
        if nesting.metadata_fields.contains(name)
            && let Ok(f) = base.field_with_name(name)
        {
            fields.push(f.clone());
            continue;
        }
        let column = dataset
            .columns
            .iter()
            .find(|c| &c.name == name)
            .unwrap_or_else(|| {
                unreachable!("nesting.column_order is derived from dataset.columns")
            });
        let dt = match column.r#type.as_deref() {
            Some(t) => parse_declared_type(t)?,
            None => arrow_schema::DataType::Utf8,
        };
        let nullable = column.nullable.unwrap_or(true);
        fields.push(arrow_schema::Field::new(name, dt, nullable));
    }
    Ok(std::sync::Arc::new(arrow_schema::Schema::new(fields)))
}

/// Compute the static schema (no source I/O) for an HTTPS dataset in
/// dynamic API mode. Returns `None` for file-mode datasets so the
/// runtime falls back to either declared columns or eager source
/// inference.
fn static_schema_for_https_dataset(
    params: &Parameters,
    dataset: &Dataset,
) -> Option<arrow_schema::SchemaRef> {
    if !params_indicate_dynamic_api(params) {
        return None;
    }

    match parse_http_json_nesting(dataset) {
        Ok(Some(nesting)) => build_json_nest_schema(dataset, &nesting).ok(),
        Ok(None) => Some(std::sync::Arc::new(
            data_components::http::provider::HttpTableProvider::base_table_schema(),
        )),
        // Defer the user-facing error to the eager path, where it is
        // already produced as a structured `InvalidConfigurationNoSource`
        // error.
        Err(_) => None,
    }
}

/// Parse `dataset.columns` looking for the `metadata.json_object: "*"`
/// marker that enables JSON schema decomposition. Returns `None` when
/// no column is marked, otherwise the full nesting configuration.
///
/// Consistent with the `DynamoDB` connector: exactly one column may be
/// marked, and the only supported marker value is `"*"`.
fn parse_http_json_nesting(dataset: &Dataset) -> DataConnectorResult<Option<HttpJsonNesting>> {
    let marked_columns: Vec<&Column> = dataset
        .columns
        .iter()
        .filter(|col| col.metadata.contains_key("json_object"))
        .collect();

    if marked_columns.is_empty() {
        return Ok(None);
    }

    if marked_columns.len() > 1 {
        let names: Vec<&str> = marked_columns.iter().map(|c| c.name.as_str()).collect();
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: "https".to_string(),
            connector_component: ConnectorComponent::from(dataset),
            message: format!(
                "Multiple columns have 'json_object' metadata defined: {}. Only one column can be configured as a JSON object column.",
                names.join(", ")
            ),
        });
    }

    let json_column = marked_columns[0];
    let Some(marker) = json_column.metadata.get("json_object") else {
        unreachable!("json_object key existence was checked above")
    };

    let is_wildcard = matches!(marker, Value::String(s) if s == "*");
    if !is_wildcard {
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: "https".to_string(),
            connector_component: ConnectorComponent::from(dataset),
            message: format!(
                "Column '{}' has invalid 'json_object' value: {:?}. Only '*' is supported.",
                json_column.name, marker
            ),
        });
    }

    let mut column_order: Vec<String> =
        dataset.columns.iter().map(|col| col.name.clone()).collect();

    // Reject the catch-all column itself being named after a reserved
    // HTTP metadata field — it would be ambiguous whether the column
    // should hold the JSON catch-all or the metadata value.
    if HTTP_METADATA_FIELDS.contains(&json_column.name.as_str()) {
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: "https".to_string(),
            connector_component: ConnectorComponent::from(dataset),
            message: format!(
                "Column '{}' is marked as the JSON catch-all (json_object: \"*\") but its name is reserved for HTTP metadata. Rename the column.",
                json_column.name
            ),
        });
    }

    let mut metadata_fields: std::collections::HashSet<String> = column_order
        .iter()
        .filter(|name| HTTP_METADATA_FIELDS.contains(&name.as_str()))
        .cloned()
        .collect();

    // Ensure `fetched_at` is always present so caching TTL eviction and
    // append-mode `time_column` work even when the user omits the column.
    if !column_order.iter().any(|n| n == "_fetched_at") {
        column_order.push("_fetched_at".to_string());
        metadata_fields.insert("_fetched_at".to_string());
    }

    Ok(Some(HttpJsonNesting::new(
        column_order,
        json_column.name.clone(),
        metadata_fields,
    )))
}

/// Names of columns in [`HttpTableProvider::base_table_schema`].
/// When schema decomposition is enabled, declared columns whose names
/// match one of these are sourced from HTTP request/response metadata
/// instead of being decomposed from the JSON body.
///
/// [`HttpTableProvider::base_table_schema`]: data_components::http::provider::HttpTableProvider::base_table_schema
const HTTP_METADATA_FIELDS: &[&str] = &[
    "request_path",
    "request_query",
    "request_body",
    "request_headers",
    "content",
    "response_status",
    "response_headers",
    "_fetched_at",
];

#[async_trait]
impl DataConnector for Https {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        if self.is_structured_format(dataset) {
            self.ensure_rate_control_supported_for_structured_dataset(dataset)?;
            // Use ListingTableConnector for file-based structured formats (parquet, csv, etc.)
            // which properly handles file parsing with correct schemas
            let listing_connector =
                HttpListingConnector::new(self.params.clone(), Handle::current());
            return listing_connector.read_provider(dataset).await;
        }

        // Validate acceleration mode for HTTP connector (JSON API endpoints only)
        // Structured file formats (parquet, csv, etc.) are handled by ListingTableConnector above
        // and support full refresh mode without refresh_sql
        if let Some(acceleration) = &dataset.acceleration
            && acceleration.enabled
        {
            let refresh_mode = self.resolve_refresh_mode(acceleration.refresh_mode);

            // HTTP connector only supports append or caching mode unless refresh_sql is provided
            if matches!(refresh_mode, RefreshMode::Full) && dataset.refresh_sql().is_none() {
                return Err(DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: "https".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        message: "HTTP connector with acceleration mode 'full' requires 'refresh_sql' to be specified. Supported acceleration modes without refresh_sql are 'append' or 'caching'.".to_string(),
                    });
            }
        }

        // For JSON API endpoints and other formats, use HttpTableProvider
        self.create_http_table_provider(dataset).await
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        if !self.emit_rate_control_metrics {
            return None;
        }

        Some(Arc::new(HttpRateControlMetricsProvider::new(
            "http",
            Arc::clone(&self.metrics),
            self.rate_control_metric_source.clone(),
        )))
    }

    fn initialization_for_dataset(&self, dataset: &Dataset) -> ComponentInitialization {
        // Non-structured HTTP endpoints (using HttpTableProvider) are dynamic datasets
        // that require filters to work properly, so skip health monitoring for them.
        if self.is_structured_format(dataset) {
            ComponentInitialization::default()
        } else {
            ComponentInitialization::OnStartup(StartupOptions {
                dataset_health_monitor: DatasetHealthMonitor::Disabled,
            })
        }
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
        ParameterSpec::runtime("request_header_filters")
            .description("Set to 'enabled' or 'disabled' to control whether request_headers filters can be pushed down as dynamic HTTP request headers.")
            .one_of(&["enabled", "disabled"]),
        ParameterSpec::runtime("request_header_allowlist")
            .description("Comma-separated list of HTTP request header names that request_headers filters may set. Required when request_header_filters is enabled."),
        ParameterSpec::runtime("max_request_headers_length")
            .description("Maximum size (in bytes) for request_headers filter values. Default: 16384 (16KiB)."),
        ParameterSpec::runtime("max_request_partitions")
            .description("Maximum number of HTTP request partitions that can be created from request_path, request_query, request_body, and request_headers filters. If unset, the number of request partitions is not capped."),
        ParameterSpec::runtime("health_probe")
            .description("Custom health probe path for endpoint validation (e.g., '/health', '/api/status'). The endpoint must return a 2xx status code to pass validation. If not set, a random path is used and any status (including 404) is accepted."),
        ParameterSpec::runtime("pagination")
            .description("Pagination mode. 'auto' (default): auto-detects Link headers. 'enabled': explicitly enable with config. 'disabled': no pagination.")
            .one_of(&["auto", "enabled", "disabled"]),
        ParameterSpec::runtime("pagination_next_pointer")
            .description("JSON pointer (RFC 6901) to the next page URL or cursor in the response body (e.g., '/next', '/pagination/cursor', '/links/next')."),
        ParameterSpec::runtime("pagination_link_header")
            .description("Whether to follow HTTP Link headers with rel=\"next\" for pagination. Default: 'enabled' (auto-detected). Set to 'disabled' to ignore Link headers.")
            .one_of(&["enabled", "disabled"]),
        ParameterSpec::runtime("pagination_token_param")
            .description("When set, the value from 'pagination_next_pointer' is treated as a cursor/token and passed as this query parameter name in subsequent requests. When not set, the value is treated as a full URL."),
        ParameterSpec::runtime("pagination_data_pointer")
            .description("JSON pointer (RFC 6901) to the data array in each page's response (e.g., '/data', '/results', '/items'). When set, only the array at this path is returned as data rows."),
        ParameterSpec::runtime("pagination_max_pages")
            .description("Maximum number of pages to fetch for pagination. Default: 100. Set to 'nolimit' to disable the limit."),
        ParameterSpec::runtime("pagination_data_map_to_array")
            .description("When 'enabled', if the data at pagination_data_pointer (or the top-level response) is a JSON object/map, extract its values as rows instead of treating it as a single row. Default: 'disabled'.")
            .one_of(&["enabled", "disabled"]),
        ParameterSpec::runtime("pagination_query_params")
            .description("Query parameter template for client-driven pagination. Supports {offset}, {limit}, and {page} variables. Example: 'offset={offset}&limit={limit}'. Requires pagination_page_size."),
        ParameterSpec::runtime("pagination_page_size")
            .description("Number of items per page for query-parameter pagination. Must be a positive integer greater than 0. Used to expand {limit} in pagination_query_params and to detect the last page (fewer results than page_size = done)."),
        ParameterSpec::runtime("auth_token_url")
            .description("OAuth2 token endpoint URL. When set together with http_auth_refresh_token, the connector exchanges the refresh token for short-lived access tokens (RFC 6749 §6) and attaches 'Authorization: Bearer <token>' to all data requests. Applies to JSON API endpoints only."),
        ParameterSpec::component("auth_refresh_token").secret()
            .description("OAuth2 refresh token exchanged against auth_token_url to obtain access tokens. Required when auth_token_url is set."),
        ParameterSpec::component("auth_client_id").secret()
            .description("OAuth2 client_id presented to the token endpoint. Required for confidential clients; optional for public clients. Paired with http_auth_client_secret."),
        ParameterSpec::component("auth_client_secret").secret()
            .description("OAuth2 client_secret presented to the token endpoint. Required when the client is confidential; must be set together with http_auth_client_id."),
        ParameterSpec::runtime("auth_scopes")
            .description("Space-separated OAuth2 scopes to request when refreshing. Omit to inherit the scopes bound to the refresh token. Optional."),
        // Validation happens via `ClientAuthMethod::parse`, which is case-
        // insensitive. `one_of` would do exact-string matching in
        // `Parameters::try_new` and reject "BASIC" / "BODY" before the parser
        // ever sees them, so we don't use it here.
        ParameterSpec::runtime("auth_client_auth")
            .description("How client credentials are sent to the token endpoint: 'basic' (HTTP Basic header, default per RFC 6749 §2.3.1) or 'body' (client_id/client_secret in the form body). Case-insensitive."),
    ]);
    all_parameters.extend_from_slice(&http_rate_control::parameter_specs());
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
            let runtime_rate_control_params =
                params.app.as_ref().map(|app| app.runtime.params.clone());
            let rate_control_registry = params
                .runtime
                .as_ref()
                .map_or_else(http_rate_control::global_registry, |runtime| {
                    runtime.http_rate_control_registry()
                });
            let (metrics, emit_rate_control_metrics, rate_control_metric_source) =
                if let ConnectorComponent::Dataset(dataset) = &params.component {
                    let structured_format = {
                        let connector = Https {
                            params: params.parameters.clone(),
                            runtime_rate_control_params: runtime_rate_control_params.clone(),
                            rate_control_registry: Arc::clone(&rate_control_registry),
                            metrics: Arc::new(HttpRateControlMetrics::default()),
                            emit_rate_control_metrics: false,
                            rate_control_metric_source: None,
                        };
                        connector.is_structured_format(dataset)
                    };
                    Https::shared_rate_control_metrics_for_dataset(
                        &rate_control_registry,
                        &rate_control_registry,
                        dataset,
                        structured_format,
                    )
                } else {
                    (Arc::new(HttpRateControlMetrics::default()), false, None)
                };

            Ok(Arc::new(Https {
                params: params.parameters,
                runtime_rate_control_params,
                rate_control_registry,
                metrics,
                emit_rate_control_metrics,
                rate_control_metric_source,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "http"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
    }

    /// HTTPS opts in to deferred registration when the dataset is
    /// configured as a dynamic API endpoint, where the schema is
    /// fully determined by configuration:
    ///
    /// * Without a `json_object` marker, the schema is the fixed
    ///   `HttpTableProvider::base_table_schema()` (request/response
    ///   columns).
    /// * With a `json_object` marker, the schema is derived from the
    ///   declared columns, honoring user-specified Arrow types and
    ///   defaulting to `Utf8` when no type is given.
    ///
    /// File-mode endpoints (Parquet/CSV/JSON files) return `None`;
    /// the runtime falls back to the user-declared `columns:` schema
    /// or, if absent, the eager source-inference path.
    fn static_schema(
        &self,
        params: &ConnectorParams,
        dataset: &Dataset,
    ) -> Option<arrow_schema::SchemaRef> {
        static_schema_for_https_dataset(&params.parameters, dataset)
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

register_data_connector!(
    register_http_connector,
    REGISTER_HTTP_CONNECTOR,
    "http",
    HttpsFactory
);
register_data_connector!(
    register_https_connector,
    REGISTER_HTTPS_CONNECTOR,
    "https",
    HttpsFactory
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::acceleration::Acceleration;
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::parameters::Parameters;
    use crate::secrets::Secrets;
    use app::AppBuilder;
    use secrecy::SecretString;
    use std::collections::HashMap;
    use tokio::sync::RwLock;

    async fn test_connector(file_format: Option<&str>) -> Https {
        let extra: Vec<(&str, &str)> = match file_format {
            Some(f) => vec![("file_format", f)],
            None => Vec::new(),
        };
        test_connector_with(&extra).await
    }

    async fn test_connector_with(extra: &[(&str, &str)]) -> Https {
        test_connector_with_runtime_params(extra, &[]).await
    }

    async fn test_connector_with_runtime_params(
        extra: &[(&str, &str)],
        runtime_params: &[(&str, &str)],
    ) -> Https {
        let mut params: Vec<(String, SecretString)> = vec![
            ("client_timeout".to_string(), "1".to_string().into()),
            ("connect_timeout".to_string(), "1".to_string().into()),
        ];

        for (k, v) in extra {
            params.push(((*k).to_string(), (*v).to_string().into()));
        }

        let params = Parameters::try_new(
            "connector https",
            params,
            "http",
            Arc::new(RwLock::new(Secrets::default())),
            &PARAMETERS,
        )
        .await
        .expect("test connector parameters should be valid");

        Https {
            params,
            runtime_rate_control_params: if runtime_params.is_empty() {
                None
            } else {
                Some(
                    runtime_params
                        .iter()
                        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                        .collect::<HashMap<_, _>>(),
                )
            },
            rate_control_registry: http_rate_control::global_registry(),
            metrics: Arc::new(HttpRateControlMetrics::default()),
            emit_rate_control_metrics: true,
            rate_control_metric_source: None,
        }
    }

    async fn test_dataset(
        from: &str,
        refresh_mode: RefreshMode,
        refresh_sql: Option<&str>,
    ) -> Dataset {
        let app = Arc::new(AppBuilder::new("test").build());
        let runtime = Arc::new(crate::Runtime::builder().build().await);

        let mut dataset = DatasetBuilder::try_new(from.to_string(), "http_test")
            .expect("dataset builder should be created")
            .with_app(app)
            .with_runtime(runtime)
            .build()
            .expect("dataset should build");

        dataset.acceleration = Some(Acceleration {
            enabled: true,
            refresh_mode: Some(refresh_mode),
            refresh_sql: refresh_sql.map(std::string::ToString::to_string),
            ..Default::default()
        });

        dataset
    }

    fn assert_invalid_url_error(error: DataConnectorError) {
        match error {
            DataConnectorError::InvalidConfiguration { message, .. } => {
                assert!(
                    message.contains("not a valid URL"),
                    "expected invalid URL error, got: {message}"
                );
            }
            other => panic!("expected invalid URL error, got: {other}"),
        }
    }

    fn assert_conflicting_rate_control_error(error: DataConnectorError) {
        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("different rate-control settings"),
                    "expected shared-origin rate-control conflict, got: {message}"
                );
            }
            other => panic!("expected shared-origin rate-control conflict, got: {other}"),
        }
    }

    #[tokio::test]
    async fn test_http_full_refresh_requires_refresh_sql_for_unstructured_endpoints() {
        let connector = test_connector(None).await;
        let dataset = test_dataset("not a url", RefreshMode::Full, None).await;

        let error = connector
            .read_provider(&dataset)
            .await
            .expect_err("full refresh without refresh_sql should be rejected");

        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("requires 'refresh_sql'"),
                    "expected refresh_sql validation error, got: {message}"
                );
            }
            other => panic!("expected refresh_sql validation error, got: {other}"),
        }
    }

    #[tokio::test]
    async fn test_http_append_refresh_without_refresh_sql_reaches_provider_validation() {
        let connector = test_connector(None).await;
        let dataset = test_dataset("not a url", RefreshMode::Append, None).await;

        let error = connector
            .read_provider(&dataset)
            .await
            .expect_err("append mode should continue to provider validation");

        assert_invalid_url_error(error);
    }

    #[tokio::test]
    async fn test_http_rate_control_parameters_parse() {
        let connector = test_connector_with(&[
            ("max_concurrent_requests", "4"),
            ("requests_per_second_limit", "2"),
            ("requests_per_minute_limit", "60"),
            ("rate_control_jitter_min", "2ms"),
            ("rate_control_jitter_max", "8ms"),
        ])
        .await;
        let dataset = test_dataset("https://api.example.com/data", RefreshMode::Append, None).await;

        let params = connector
            .resolve_http_provider_params(&dataset)
            .expect("rate-control parameters should parse");

        assert_eq!(params.rate_control.max_concurrent_requests, Some(4));
        assert_eq!(
            params
                .rate_control
                .requests_per_second
                .map(std::num::NonZeroU32::get),
            Some(2)
        );
        assert_eq!(
            params
                .rate_control
                .requests_per_minute
                .map(std::num::NonZeroU32::get),
            Some(60)
        );
        assert_eq!(params.rate_control.jitter_min, Duration::from_millis(2));
        assert_eq!(params.rate_control.jitter_max, Duration::from_millis(8));
    }

    #[tokio::test]
    async fn test_http_rate_control_uses_runtime_defaults() {
        let connector = test_connector_with_runtime_params(
            &[],
            &[
                ("http_max_concurrent_requests", "5"),
                ("http_requests_per_second_limit", "3"),
                ("http_requests_per_minute_limit", "90"),
                ("http_rate_control_jitter_min", "1ms"),
                ("http_rate_control_jitter_max", "4ms"),
            ],
        )
        .await;
        let dataset = test_dataset(
            "https://runtime-defaults.example.com/data",
            RefreshMode::Append,
            None,
        )
        .await;

        let params = connector
            .resolve_http_provider_params(&dataset)
            .expect("runtime rate-control defaults should parse");

        assert_eq!(params.rate_control.max_concurrent_requests, Some(5));
        assert_eq!(
            params
                .rate_control
                .requests_per_second
                .map(std::num::NonZeroU32::get),
            Some(3)
        );
        assert_eq!(
            params
                .rate_control
                .requests_per_minute
                .map(std::num::NonZeroU32::get),
            Some(90)
        );
        assert_eq!(params.rate_control.jitter_min, Duration::from_millis(1));
        assert_eq!(params.rate_control.jitter_max, Duration::from_millis(4));
    }

    #[tokio::test]
    async fn test_http_rate_control_dataset_params_override_runtime_defaults() {
        let connector = test_connector_with_runtime_params(
            &[("max_concurrent_requests", "2")],
            &[("http_max_concurrent_requests", "5")],
        )
        .await;
        let dataset = test_dataset(
            "https://runtime-override.example.com/data",
            RefreshMode::Append,
            None,
        )
        .await;

        let params = connector
            .resolve_http_provider_params(&dataset)
            .expect("dataset rate-control override should parse");

        assert_eq!(params.rate_control.max_concurrent_requests, Some(2));
    }

    #[tokio::test]
    async fn test_http_rate_control_metrics_are_available() {
        let connector = test_connector_with(&[
            ("max_concurrent_requests", "4"),
            ("requests_per_second_limit", "2"),
            ("requests_per_minute_limit", "60"),
            ("rate_control_jitter_min", "2ms"),
            ("rate_control_jitter_max", "8ms"),
        ])
        .await;
        let dataset = test_dataset(
            "https://metrics.example.com/data",
            RefreshMode::Append,
            None,
        )
        .await;

        connector
            .create_http_table_provider(&dataset)
            .await
            .expect("HTTP provider should be created");

        assert_eq!(connector.metrics.max_concurrent_requests(), 4);
        assert_eq!(connector.metrics.requests_per_second_limit(), 2);
        assert_eq!(connector.metrics.requests_per_minute_limit(), 60);
        assert_eq!(connector.metrics.available_permits(), 4);

        let metrics_provider = DataConnector::metrics_provider(&connector)
            .expect("HTTP connector should expose metrics");
        for metric_name in [
            "inflight_operations",
            "rate_control_max_concurrent_requests",
            "rate_control_requests_per_second_limit",
            "rate_control_requests_per_minute_limit",
            "rate_control_jitter_min_ms",
            "rate_control_jitter_max_ms",
            "rate_control_available_permits",
            "rate_control_acquisitions_total",
            "rate_control_acquire_errors_total",
            "rate_control_wait_duration_ms",
            "rate_limit_retry_after_updates_total",
            "rate_limit_retry_after_remaining_ms",
        ] {
            let metric = metrics_provider
                .get_metric(metric_name)
                .unwrap_or_else(|| panic!("metric {metric_name} should be registered"));
            assert!(
                metric.auto_register,
                "metric {metric_name} should auto-register"
            );
        }
    }

    #[tokio::test]
    async fn test_http_rate_control_rejects_zero_limits() {
        let connector = test_connector_with(&[("requests_per_second_limit", "0")]).await;
        let dataset = test_dataset(
            "https://zero-limit.example.com/data",
            RefreshMode::Append,
            None,
        )
        .await;

        let Err(error) = connector.resolve_http_provider_params(&dataset) else {
            panic!("zero rate-control limits should be rejected");
        };

        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("must be greater than 0"),
                    "expected zero-limit validation error, got: {message}"
                );
            }
            other => panic!("expected zero-limit validation error, got: {other}"),
        }
    }

    #[tokio::test]
    async fn test_http_rate_control_rejects_invalid_jitter_range() {
        let connector = test_connector_with(&[
            ("requests_per_minute_limit", "60"),
            ("rate_control_jitter_min", "20ms"),
            ("rate_control_jitter_max", "10ms"),
        ])
        .await;
        let dataset = test_dataset(
            "https://invalid-jitter.example.com/data",
            RefreshMode::Append,
            None,
        )
        .await;

        let Err(error) = connector.resolve_http_provider_params(&dataset) else {
            panic!("invalid rate-control jitter ranges should be rejected");
        };

        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("must be less than or equal"),
                    "expected jitter range validation error, got: {message}"
                );
            }
            other => panic!("expected jitter range validation error, got: {other}"),
        }
    }

    #[tokio::test]
    async fn test_http_rate_control_does_not_persist_after_failed_provider_validation() {
        let failing = test_connector_with(&[
            ("max_concurrent_requests", "2"),
            ("health_probe", "not-an-absolute-path"),
        ])
        .await;
        let failing_dataset = test_dataset(
            "https://failed-provider-validation.example.com/data",
            RefreshMode::Append,
            None,
        )
        .await;

        failing
            .create_http_table_provider(&failing_dataset)
            .await
            .expect_err("invalid health_probe should fail provider validation");

        let succeeding = test_connector_with(&[]).await;
        let succeeding_dataset = test_dataset(
            "https://failed-provider-validation.example.com/other",
            RefreshMode::Append,
            None,
        )
        .await;

        succeeding
            .create_http_table_provider(&succeeding_dataset)
            .await
            .expect("failed provider validation should not leave stale origin config");
    }

    #[tokio::test]
    async fn test_http_structured_dataset_rejects_runtime_rate_control_defaults() {
        let connector = test_connector_with_runtime_params(
            &[("file_format", "csv")],
            &[("http_max_concurrent_requests", "5")],
        )
        .await;
        let dataset = test_dataset(
            "https://structured-rate-control.example.com/data.csv",
            RefreshMode::Full,
            None,
        )
        .await;

        let Err(error) = connector.read_provider(&dataset).await else {
            panic!("structured HTTP file datasets should reject HTTP rate-control defaults");
        };

        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("not supported for structured HTTP file datasets"),
                    "expected structured dataset rate-control validation error, got: {message}"
                );
            }
            other => {
                panic!("expected structured dataset rate-control validation error, got: {other}")
            }
        }
    }

    #[tokio::test]
    async fn test_http_rate_control_rejects_mixed_origin_configuration() {
        let configured = test_connector_with(&[("max_concurrent_requests", "2")]).await;
        let unconfigured = test_connector_with(&[]).await;
        let configured_dataset = test_dataset(
            "https://mixed-origin-enabled-first.example.com/data",
            RefreshMode::Append,
            None,
        )
        .await;
        let unconfigured_dataset = test_dataset(
            "https://mixed-origin-enabled-first.example.com/other",
            RefreshMode::Append,
            None,
        )
        .await;

        configured
            .create_http_table_provider(&configured_dataset)
            .await
            .expect("configured HTTP provider should be created");
        let Err(error) = unconfigured
            .create_http_table_provider(&unconfigured_dataset)
            .await
        else {
            panic!("mixed configured/unconfigured origin should be rejected");
        };
        assert_conflicting_rate_control_error(error);

        let unconfigured = test_connector_with(&[]).await;
        let configured = test_connector_with(&[("max_concurrent_requests", "2")]).await;
        let unconfigured_dataset = test_dataset(
            "https://mixed-origin-disabled-first.example.com/data",
            RefreshMode::Append,
            None,
        )
        .await;
        let configured_dataset = test_dataset(
            "https://mixed-origin-disabled-first.example.com/other",
            RefreshMode::Append,
            None,
        )
        .await;

        unconfigured
            .create_http_table_provider(&unconfigured_dataset)
            .await
            .expect("unconfigured HTTP provider should be created");
        let Err(error) = configured
            .create_http_table_provider(&configured_dataset)
            .await
        else {
            panic!("mixed unconfigured/configured origin should be rejected");
        };
        assert_conflicting_rate_control_error(error);
    }

    #[tokio::test]
    async fn test_http_caching_refresh_without_refresh_sql_reaches_provider_validation() {
        let connector = test_connector(None).await;
        let dataset = test_dataset("not a url", RefreshMode::Caching, None).await;

        let error = connector
            .read_provider(&dataset)
            .await
            .expect_err("caching mode should continue to provider validation");

        assert_invalid_url_error(error);
    }

    #[tokio::test]
    async fn test_http_structured_full_refresh_without_refresh_sql_bypasses_json_validation() {
        let connector = test_connector(Some("csv")).await;
        let dataset = test_dataset("not a url", RefreshMode::Full, None).await;

        let error = connector
            .read_provider(&dataset)
            .await
            .expect_err("structured formats should bypass JSON refresh_sql validation");

        assert_invalid_url_error(error);
    }

    #[tokio::test]
    async fn test_http_auto_structured_format_detects_compressed_url_extension() {
        let connector = test_connector(None).await;
        let dataset =
            test_dataset("https://example.com/data.jsonl.gz", RefreshMode::Full, None).await;

        assert!(connector.is_structured_format(&dataset));
    }

    #[tokio::test]
    async fn test_http_auto_structured_format_detects_compressed_file_extension_param() {
        let connector = test_connector_with(&[("file_extension", ".csv.zst")]).await;
        let dataset = test_dataset("https://example.com/download", RefreshMode::Full, None).await;

        assert!(connector.is_structured_format(&dataset));
    }

    #[tokio::test]
    async fn resolve_refresh_token_auth_returns_none_when_unset() {
        let connector = test_connector(None).await;
        let dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;

        let result = connector
            .resolve_refresh_token_auth(&dataset)
            .expect("no auth params should yield Ok(None)");
        assert!(
            result.is_none(),
            "expected None when no auth params are configured"
        );
    }

    #[tokio::test]
    async fn resolve_http_provider_params_parses_request_header_filters() {
        let connector = test_connector_with(&[
            ("request_header_filters", "enabled"),
            ("request_header_allowlist", "x-sandbox-id, x-region"),
            ("max_request_headers_length", "2048"),
            ("max_request_partitions", "7000"),
        ])
        .await;
        let dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;

        let params = connector
            .resolve_http_provider_params(&dataset)
            .expect("request header filter params should be valid");

        assert!(params.request_filters.allow_header_filters);
        assert_eq!(
            params.request_filters.request_header_allowlist,
            vec!["x-sandbox-id", "x-region"]
        );
        assert_eq!(params.request_filters.max_headers_length, 2048);
        assert_eq!(params.max_request_partitions, Some(7000));
    }

    #[tokio::test]
    async fn resolve_http_provider_params_parses_finite_pagination_max_pages() {
        let connector =
            test_connector_with(&[("pagination", "enabled"), ("pagination_max_pages", "250")])
                .await;
        let dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;

        let params = connector
            .resolve_http_provider_params(&dataset)
            .expect("pagination params should be valid");

        assert_eq!(
            params
                .pagination
                .expect("pagination should be configured")
                .max_pages,
            Some(250)
        );
    }

    #[tokio::test]
    async fn resolve_http_provider_params_parses_nolimit_pagination_max_pages() {
        let connector = test_connector_with(&[
            ("pagination", "enabled"),
            ("pagination_max_pages", "nolimit"),
        ])
        .await;
        let dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;

        let params = connector
            .resolve_http_provider_params(&dataset)
            .expect("pagination params should be valid");

        assert_eq!(
            params
                .pagination
                .expect("pagination should be configured")
                .max_pages,
            None
        );
    }

    #[tokio::test]
    async fn resolve_refresh_token_auth_rejects_refresh_token_without_url() {
        let connector = test_connector_with(&[("http_auth_refresh_token", "rt-only")]).await;
        let dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;

        let error = connector
            .resolve_refresh_token_auth(&dataset)
            .expect_err("refresh token without token URL should be rejected");
        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("auth_token_url"),
                    "expected error to mention auth_token_url, got: {message}"
                );
                assert!(
                    message.contains("http_auth_refresh_token"),
                    "error should reference the prefixed user-facing name, got: {message}"
                );
            }
            other => panic!("expected InvalidConfigurationNoSource, got: {other}"),
        }
    }

    #[tokio::test]
    async fn resolve_refresh_token_auth_rejects_url_without_refresh_token() {
        let connector =
            test_connector_with(&[("auth_token_url", "https://example.com/oauth/token")]).await;
        let dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;

        let error = connector
            .resolve_refresh_token_auth(&dataset)
            .expect_err("token URL without refresh token should be rejected");
        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("http_auth_refresh_token"),
                    "expected error to mention http_auth_refresh_token, got: {message}"
                );
            }
            other => panic!("expected InvalidConfigurationNoSource, got: {other}"),
        }
    }

    #[tokio::test]
    async fn resolve_refresh_token_auth_rejects_secret_without_client_id() {
        let connector = test_connector_with(&[
            ("auth_token_url", "https://example.com/oauth/token"),
            ("http_auth_refresh_token", "rt"),
            ("http_auth_client_secret", "csec"),
        ])
        .await;
        let dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;

        let error = connector
            .resolve_refresh_token_auth(&dataset)
            .expect_err("client_secret without client_id should be rejected");
        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("http_auth_client_id"),
                    "expected error to mention http_auth_client_id, got: {message}"
                );
                assert!(
                    message.contains("http_auth_client_secret"),
                    "expected error to mention http_auth_client_secret, got: {message}"
                );
            }
            other => panic!("expected InvalidConfigurationNoSource, got: {other}"),
        }
    }

    #[tokio::test]
    async fn resolve_refresh_token_auth_parses_full_config() {
        let connector = test_connector_with(&[
            ("auth_token_url", "https://example.com/oauth/token"),
            ("http_auth_refresh_token", "rt-seed"),
            ("http_auth_client_id", "cid"),
            ("http_auth_client_secret", "csec"),
            ("auth_scopes", "read:data offline_access"),
            ("auth_client_auth", "body"),
        ])
        .await;
        let dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;

        let (config, _refresh_token) = connector
            .resolve_refresh_token_auth(&dataset)
            .expect("full config should parse")
            .expect("expected Some(config) when auth params are set");

        assert_eq!(config.token_url, "https://example.com/oauth/token");
        assert_eq!(config.client_id.as_deref(), Some("cid"));
        assert!(config.client_secret.is_some());
        assert_eq!(config.scopes.as_deref(), Some("read:data offline_access"));
        assert_eq!(config.client_auth, ClientAuthMethod::Body);
    }

    fn column_with_marker(name: &str, marker: Value) -> Column {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("json_object".to_string(), marker);
        Column::new(name).with_metadata(metadata)
    }

    #[tokio::test]
    async fn parse_http_json_nesting_returns_none_when_no_marker() {
        let dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;
        let result = parse_http_json_nesting(&dataset).expect("parse should succeed");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn parse_http_json_nesting_returns_none_when_columns_have_no_marker() {
        let mut dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;
        dataset.columns = vec![Column::new("id"), Column::new("name")];
        let result = parse_http_json_nesting(&dataset).expect("parse should succeed");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn parse_http_json_nesting_parses_valid_wildcard_marker() {
        let mut dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;
        dataset.columns = vec![
            Column::new("id"),
            Column::new("name"),
            column_with_marker("data", Value::String("*".to_string())),
        ];
        let nesting = parse_http_json_nesting(&dataset)
            .expect("parse should succeed")
            .expect("expected Some(nesting) when marker is present");
        assert_eq!(nesting.json_field_name, "data");
        assert_eq!(
            nesting.column_order,
            vec!["id", "name", "data", "_fetched_at"]
        );
        assert!(nesting.static_fields.contains("id"));
        assert!(nesting.static_fields.contains("name"));
        assert!(!nesting.static_fields.contains("data"));
        assert!(
            nesting.metadata_fields.contains("_fetched_at"),
            "_fetched_at should be auto-injected into metadata_fields"
        );
    }

    #[tokio::test]
    async fn parse_http_json_nesting_rejects_multiple_markers() {
        let mut dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;
        dataset.columns = vec![
            Column::new("id"),
            column_with_marker("data", Value::String("*".to_string())),
            column_with_marker("extra", Value::String("*".to_string())),
        ];
        let error =
            parse_http_json_nesting(&dataset).expect_err("multiple markers should be rejected");
        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("Multiple columns"),
                    "expected multiple-columns error, got: {message}"
                );
                assert!(message.contains("data"), "error should list 'data'");
                assert!(message.contains("extra"), "error should list 'extra'");
            }
            other => panic!("expected InvalidConfigurationNoSource, got: {other}"),
        }
    }

    #[tokio::test]
    async fn parse_http_json_nesting_rejects_invalid_marker_value() {
        let mut dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;
        dataset.columns = vec![
            Column::new("id"),
            column_with_marker("data", Value::String("not-a-wildcard".to_string())),
        ];
        let error =
            parse_http_json_nesting(&dataset).expect_err("non-wildcard marker should be rejected");
        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("invalid 'json_object' value"),
                    "expected invalid-value error, got: {message}"
                );
                assert!(
                    message.contains("Only '*' is supported"),
                    "expected guidance mentioning '*', got: {message}"
                );
            }
            other => panic!("expected InvalidConfigurationNoSource, got: {other}"),
        }
    }

    #[tokio::test]
    async fn parse_http_json_nesting_rejects_non_string_marker_value() {
        let mut dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;
        dataset.columns = vec![
            Column::new("id"),
            column_with_marker("data", Value::Bool(true)),
        ];
        let error =
            parse_http_json_nesting(&dataset).expect_err("non-string marker should be rejected");
        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("invalid 'json_object' value"),
                    "expected invalid-value error, got: {message}"
                );
            }
            other => panic!("expected InvalidConfigurationNoSource, got: {other}"),
        }
    }

    #[tokio::test]
    async fn parse_http_json_nesting_classifies_metadata_columns() {
        let mut dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;
        dataset.columns = vec![
            Column::new("request_path"),
            Column::new("response_status"),
            Column::new("id"),
            column_with_marker("data", Value::String("*".to_string())),
        ];
        let nesting = parse_http_json_nesting(&dataset)
            .expect("parse should succeed")
            .expect("expected Some(nesting) when marker is present");
        assert_eq!(nesting.json_field_name, "data");
        assert_eq!(
            nesting.column_order,
            vec![
                "request_path",
                "response_status",
                "id",
                "data",
                "_fetched_at"
            ]
        );
        assert!(nesting.metadata_fields.contains("request_path"));
        assert!(nesting.metadata_fields.contains("response_status"));
        assert!(
            nesting.metadata_fields.contains("_fetched_at"),
            "_fetched_at should be auto-injected into metadata_fields"
        );
        assert!(
            !nesting.metadata_fields.contains("id"),
            "non-reserved column must not be classified as metadata"
        );
        // Reserved-name columns must not also be treated as static body
        // fields, otherwise the body would shadow the HTTP metadata.
        assert!(!nesting.static_fields.contains("request_path"));
        assert!(!nesting.static_fields.contains("response_status"));
        assert!(!nesting.static_fields.contains("_fetched_at"));
        assert!(nesting.static_fields.contains("id"));
    }

    #[tokio::test]
    async fn parse_http_json_nesting_rejects_catchall_named_after_metadata() {
        let mut dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;
        dataset.columns = vec![
            Column::new("id"),
            column_with_marker("response_status", Value::String("*".to_string())),
        ];
        let error = parse_http_json_nesting(&dataset)
            .expect_err("reserved-name catch-all should be rejected");
        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("reserved for HTTP metadata"),
                    "expected reserved-name error, got: {message}"
                );
            }
            other => panic!("expected InvalidConfigurationNoSource, got: {other}"),
        }
    }

    async fn test_params(extra: &[(&str, &str)]) -> Parameters {
        let mut params: Vec<(String, SecretString)> = Vec::new();
        for (k, v) in extra {
            params.push(((*k).to_string(), (*v).to_string().into()));
        }
        Parameters::try_new(
            "connector https",
            params,
            "http",
            Arc::new(RwLock::new(Secrets::default())),
            &PARAMETERS,
        )
        .await
        .expect("test connector parameters should be valid")
    }

    #[tokio::test]
    async fn static_schema_returns_none_in_file_mode() {
        let params = test_params(&[]).await;
        let dataset = test_dataset(
            "https://example.com/data.parquet",
            RefreshMode::Append,
            None,
        )
        .await;
        assert!(static_schema_for_https_dataset(&params, &dataset).is_none());
    }

    #[tokio::test]
    async fn static_schema_returns_none_in_file_mode_even_with_declared_columns() {
        let params = test_params(&[]).await;
        let mut dataset = test_dataset(
            "https://example.com/data.parquet",
            RefreshMode::Append,
            None,
        )
        .await;
        dataset.columns = vec![
            Column::new("id").with_type("bigint"),
            Column::new("name").with_type("text"),
        ];
        // File mode: declared columns are handled by the runtime
        // dispatch fallback, not by HTTPS' static_schema.
        assert!(static_schema_for_https_dataset(&params, &dataset).is_none());
    }

    #[tokio::test]
    async fn static_schema_in_dynamic_api_mode_without_marker_returns_base_schema() {
        let params = test_params(&[("allowed_request_paths", "/items/*")]).await;
        let dataset = test_dataset("https://api.example.com", RefreshMode::Append, None).await;
        let schema =
            static_schema_for_https_dataset(&params, &dataset).expect("dynamic mode -> Some");
        let expected = data_components::http::provider::HttpTableProvider::base_table_schema();
        assert_eq!(schema.fields().len(), expected.fields().len());
        for (a, b) in schema.fields().iter().zip(expected.fields().iter()) {
            assert_eq!(a.name(), b.name());
            assert_eq!(a.data_type(), b.data_type());
            assert_eq!(a.is_nullable(), b.is_nullable());
        }
    }

    #[tokio::test]
    async fn static_schema_in_dynamic_api_mode_with_json_nest_defaults_to_utf8() {
        let params = test_params(&[("allowed_request_paths", "/items/*")]).await;
        let mut dataset = test_dataset("https://api.example.com", RefreshMode::Append, None).await;
        dataset.columns = vec![
            Column::new("id"),
            Column::new("name"),
            column_with_marker("data", Value::String("*".to_string())),
        ];
        let schema = static_schema_for_https_dataset(&params, &dataset)
            .expect("json_nest dynamic mode -> Some");
        // 3 user-declared columns + auto-injected _fetched_at
        assert_eq!(schema.fields().len(), 4);
        // User-declared columns default to Utf8.
        for name in &["id", "name", "data"] {
            let f = schema.field_with_name(name).unwrap();
            assert_eq!(
                f.data_type(),
                &arrow_schema::DataType::Utf8,
                "untyped json_nest column should default to Utf8 (field {name})",
            );
            assert!(f.is_nullable());
        }
        // Auto-injected _fetched_at gets its type from base_table_schema.
        let fetched_at = schema.field_with_name("_fetched_at").unwrap();
        assert_eq!(
            fetched_at.data_type(),
            &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
        );
        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            vec!["id", "name", "data", "_fetched_at"]
        );
    }

    #[tokio::test]
    async fn static_schema_in_dynamic_api_mode_with_json_nest_honors_typed_columns() {
        let params = test_params(&[("allowed_request_paths", "/items/*")]).await;
        let mut dataset = test_dataset("https://api.example.com", RefreshMode::Append, None).await;
        dataset.columns = vec![
            Column::new("id").with_type("bigint").with_nullable(false),
            Column::new("name"),
            column_with_marker("data", Value::String("*".to_string())),
        ];
        let schema = static_schema_for_https_dataset(&params, &dataset)
            .expect("json_nest dynamic mode -> Some");
        // 3 user-declared columns + auto-injected _fetched_at
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Int64);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &arrow_schema::DataType::Utf8);
        assert_eq!(schema.field(2).name(), "data");
        assert_eq!(schema.field(2).data_type(), &arrow_schema::DataType::Utf8);
        assert_eq!(schema.field(3).name(), "_fetched_at");
        assert_eq!(
            schema.field(3).data_type(),
            &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
        );
    }

    #[tokio::test]
    async fn static_schema_returns_none_when_json_nest_marker_is_invalid() {
        let params = test_params(&[("allowed_request_paths", "/items/*")]).await;
        let mut dataset = test_dataset("https://api.example.com", RefreshMode::Append, None).await;
        dataset.columns = vec![
            Column::new("id"),
            column_with_marker("data", Value::String("not-a-wildcard".to_string())),
        ];
        // Defer the user-facing error to the eager path.
        assert!(static_schema_for_https_dataset(&params, &dataset).is_none());
    }

    #[tokio::test]
    async fn parse_http_json_nesting_auto_injects_fetched_at() {
        let mut dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;
        // User declares only body columns + catch-all, no _fetched_at.
        dataset.columns = vec![
            Column::new("id"),
            Column::new("title"),
            column_with_marker("extra", Value::String("*".to_string())),
        ];
        let nesting = parse_http_json_nesting(&dataset)
            .expect("parse should succeed")
            .expect("expected Some(nesting)");

        assert!(
            nesting.column_order.contains(&"_fetched_at".to_string()),
            "_fetched_at should be auto-injected into column_order"
        );
        assert!(
            nesting.metadata_fields.contains("_fetched_at"),
            "_fetched_at should be in metadata_fields"
        );
        assert!(
            !nesting.static_fields.contains("_fetched_at"),
            "_fetched_at must not be a static body field"
        );
        // Auto-injected at the end, after user-declared columns.
        assert_eq!(nesting.column_order.last().unwrap(), "_fetched_at");
    }

    #[tokio::test]
    async fn parse_http_json_nesting_does_not_duplicate_fetched_at() {
        let mut dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;
        // User explicitly declares _fetched_at.
        dataset.columns = vec![
            Column::new("id"),
            Column::new("fetched_at"),
            column_with_marker("details", Value::String("*".to_string())),
        ];
        let nesting = parse_http_json_nesting(&dataset)
            .expect("parse should succeed")
            .expect("expected Some(nesting)");

        let count = nesting
            .column_order
            .iter()
            .filter(|n| *n == "_fetched_at")
            .count();
        assert_eq!(count, 1, "_fetched_at should not be duplicated");
        assert!(nesting.metadata_fields.contains("_fetched_at"));
        // When user declares it, it stays in the user's position.
        assert_eq!(nesting.column_order[1], "_fetched_at");
    }

    #[tokio::test]
    async fn build_json_nest_schema_includes_fetched_at_when_auto_injected() {
        let mut dataset = test_dataset("http://example.com/api", RefreshMode::Append, None).await;
        dataset.columns = vec![
            Column::new("id"),
            column_with_marker("data", Value::String("*".to_string())),
        ];
        let nesting = parse_http_json_nesting(&dataset)
            .expect("parse should succeed")
            .expect("expected Some(nesting)");
        let schema =
            build_json_nest_schema(&dataset, &nesting).expect("schema build should succeed");

        let fetched_at = schema
            .field_with_name("_fetched_at")
            .expect("_fetched_at should be present in the schema");
        assert_eq!(
            fetched_at.data_type(),
            &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            "_fetched_at should have its base_table_schema type"
        );
    }
}
