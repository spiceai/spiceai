/*
Copyright 2024-2026 The Spice.ai OSS Authors

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
#![allow(clippy::missing_errors_doc)]

//! GraphQL data connector for Spice.ai runtime.

pub mod graphql;

use crate::graphql::{
    builder::GraphQLClientBuilder, client::GraphQLClient, provider::GraphQLTableProviderBuilder,
};
use async_trait::async_trait;
use data_components::rate_limit::RateLimiter;
use data_connector_api::ConnectorContext;
use data_connector_api::{
    ConnectorComponent, ConnectorParams, DEFAULT_SPICE_CLIENT_TIMEOUT, DataConnector,
    DataConnectorError, DataConnectorFactory, DataConnectorResult, NewDataConnectorResult,
    RedirectScope, default_spice_client_builder,
};
use data_http_rate_control as http_rate_control;
use data_http_rate_control::{
    HttpRateControlMetricSource, HttpRateControlMetrics, HttpRateControlMetricsProvider,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::memory_pool::MemoryPool;
use runtime_component::dataset::DatasetSpec;
use runtime_metrics::component::MetricsProvider;
use runtime_parameters::{ParameterSpec, Parameters};
use snafu::prelude::*;
use std::{
    any::Any,
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, LazyLock},
};
use token_provider::{StaticTokenProvider, TokenProvider};
use url::Url;

#[derive(Debug)]
pub struct GraphQL {
    params: Parameters,
    /// Spicepod name, which keys the persisted shared-rate-controller state. Captured at
    /// construction because it is not part of a dataset's configuration spec.
    app_name: Arc<str>,
    runtime_rate_control_params: Option<HashMap<String, String>>,
    rate_control_registry: Arc<http_rate_control::HttpRateControlRegistry>,
    metrics: Arc<HttpRateControlMetrics>,
    emit_rate_control_metrics: bool,
    rate_control_metric_source: Option<HttpRateControlMetricSource>,
}

#[derive(Default, Debug, Copy, Clone)]
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

static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    let mut parameters = Vec::new();
    parameters.extend_from_slice(&[
        // Connector parameters
        ParameterSpec::component("auth_header")
            .description("A custom header name to use for authentication instead of the default 'Authorization: Bearer' header. When set, the value of 'auth_token' is sent as the value of this header."),
        ParameterSpec::component("auth_token")
            .description("The bearer token to use in the GraphQL requests.")
            .secret(),
        ParameterSpec::component("auth_user")
            .description("The username to use for HTTP Basic Auth.")
            .secret(),
        ParameterSpec::component("auth_pass")
            .description("The password to use for HTTP Basic Auth.")
            .secret(),
        ParameterSpec::component("query")
            .description("The GraphQL query to execute.")
            .required(),
        // Runtime parameters
        ParameterSpec::runtime("json_pointer")
            .description("The JSON pointer to the data in the GraphQL response."),
        ParameterSpec::runtime("unnest_depth").description(
            "Depth level to automatically unnest objects to. By default, disabled if unspecified or 0.",
        ),
    ]);
    parameters.extend_from_slice(&http_rate_control::parameter_specs());
    parameters
});

impl DataConnectorFactory for GraphQLFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create<'a>(
        &'a self,
        params: ConnectorParams,
        context: &'a dyn ConnectorContext,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
        Box::pin(async move {
            let app = context.app();
            let runtime_rate_control_params = Some(app.runtime.params.clone());
            let app_name: Arc<str> = Arc::from(app.name.as_str());
            let rate_control_registry = context.http_rate_control_registry();
            let (metrics, emit_rate_control_metrics, rate_control_metric_source) =
                if let ConnectorComponent::Dataset(dataset) = &params.component {
                    Url::parse(dataset.path()).map_or_else(
                        |_| (Arc::new(HttpRateControlMetrics::default()), false, None),
                        |url| {
                            let metric_source = HttpRateControlMetricSource::new(
                                Arc::clone(&rate_control_registry),
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
                } else {
                    (Arc::new(HttpRateControlMetrics::default()), false, None)
                };

            let graphql = GraphQL {
                params: params.parameters,
                app_name,
                runtime_rate_control_params,
                rate_control_registry,
                metrics,
                emit_rate_control_metrics,
                rate_control_metric_source,
            };
            Ok(Arc::new(graphql) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "graphql"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS.as_slice()
    }
}

impl GraphQL {
    /// The client for `dataset`, whose response bodies are charged to `memory_pool`.
    async fn get_client(
        &self,
        dataset: &DatasetSpec,
        memory_pool: Arc<dyn MemoryPool>,
    ) -> DataConnectorResult<(
        GraphQLClient,
        http_rate_control::SharedRateControllerReservation,
    )> {
        let token = self.params.get("auth_token").ok().map(|token| {
            Arc::new(StaticTokenProvider::new(token.clone())) as Arc<dyn TokenProvider>
        });

        let auth_header = self
            .params
            .get("auth_header")
            .expose()
            .ok()
            .map(|h| {
                reqwest::header::HeaderName::try_from(h).map_err(|source| {
                    DataConnectorError::InvalidConfiguration {
                        dataconnector: "graphql".to_string(),
                        message: format!("Invalid 'graphql_auth_header' value: '{h}'. Ensure it is a valid HTTP header name. For details, visit: https://spiceai.org/docs/components/data-connectors/graphql"),
                        connector_component: ConnectorComponent::from(dataset),
                        source: source.into(),
                    }
                })
            })
            .transpose()?;

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

        let endpoint = Url::parse(dataset.path()).boxed().map_err(|source| {
            DataConnectorError::InvalidConfiguration {
                dataconnector: "graphql".to_string(),
                message: "The specified URL in the dataset 'from' is not valid. Ensure the URL is valid and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/graphql".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source,
            }
        })?;

        // If json_pointer isn't provided, default to the root of the response
        let json_pointer: Option<&str> = self.params.get("json_pointer").expose().ok();

        let unnest_depth = self
            .params
            .get("unnest_depth")
            .expose()
            .ok()
            .map_or(Ok(0), str::parse)
            .boxed()
            .map_err(|source| DataConnectorError::InvalidConfiguration {
                dataconnector: "graphql".to_string(),
                message: "The `unnest_depth` parameter must be a positive integer.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/graphql#configuration".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source,
            })?;
        if unnest_depth > 50 {
            return Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "graphql".to_string(),
                message: format!(
                    "The `unnest_depth` parameter must be less than or equal to 50, got {unnest_depth}.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/graphql#configuration"
                ),
                connector_component: ConnectorComponent::from(dataset),
            });
        }

        // A token sent in a custom header is a credential reqwest does not remove when a redirect
        // leaves the endpoint's origin.
        let redirects = if auth_header.is_some() && token.is_some() {
            RedirectScope::RequestOrigin
        } else if token.is_some()
            || user.is_some()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
        {
            RedirectScope::PublicHostsWithCredentials
        } else {
            RedirectScope::PublicHosts
        };
        let client = default_spice_client_builder("application/json", redirects);

        let mut rate_control = http_rate_control::resolve_config_for_component(
            &self.params,
            self.runtime_rate_control_params.as_ref(),
            &ConnectorComponent::from(dataset),
            "graphql",
        )?;
        // The GraphQL client is `default_spice_client_builder`'s, so its request timeout
        // is the `client_timeout` equivalent the acquire bound defaults to.
        rate_control.apply_default_acquire_timeout(DEFAULT_SPICE_CLIENT_TIMEOUT);
        let rate_limiter = self
            .rate_control_registry
            .shared_rate_limiter_for_config(&endpoint, &rate_control)
            .await;
        self.metrics.set_rate_limiter(&rate_limiter);
        let rate_limiter: Arc<dyn RateLimiter> = rate_limiter;
        let rate_controller = Arc::clone(&self.rate_control_registry)
            .reserve_shared_rate_controller_for_component(
                &endpoint,
                &rate_control,
                self.app_name.as_ref(),
                &ConnectorComponent::from(dataset),
                "graphql",
            )
            .await?;
        self.metrics.set_config(&rate_controller.shared().config);
        self.metrics
            .set_rate_controller(rate_controller.shared().controller.as_ref());

        let client_result = GraphQLClientBuilder::new(
            endpoint,
            graphql::client::UnnestBehavior::Depth(unnest_depth),
        )
        .with_json_pointer(json_pointer)
        .with_token_provider(token)
        .with_user(user)
        .with_pass(pass)
        .with_rate_limiter(Some(rate_limiter))
        .with_rate_controller(rate_controller.shared().controller.clone())
        .with_auth_header(auth_header)
        .with_source(dataset.name.to_string(), "graphql")
        .build(client, memory_pool)
        .boxed();

        match client_result {
            Ok(client) => Ok((client, rate_controller)),
            Err(source) => {
                rate_controller.rollback().await;
                Err(DataConnectorError::InternalWithSource {
                    dataconnector: "graphql".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source,
                })
            }
        }
    }
}

#[async_trait]
impl DataConnector for GraphQL {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let query = self.params.get("query").expose().ok_or_else(|p| {
            DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "graphql".to_string(),
                message: format!("A required parameter was missing: `{}`.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/graphql#configuration", p.0),
                connector_component: ConnectorComponent::from(dataset),
            }
        })?;

        // Response bodies are held to the runtime's query memory pool, at registration as
        // in queries.
        let memory_pool = Arc::clone(
            &context
                .datafusion_session_context()
                .runtime_env()
                .memory_pool,
        );
        let (client, rate_controller) = self.get_client(dataset, memory_pool).await?;

        match GraphQLTableProviderBuilder::new(client).build(query).await {
            Ok(provider) => {
                if let Some(metric_source) = &self.rate_control_metric_source {
                    let _ = metric_source.claim_owner();
                }
                rate_controller.commit().await;
                Ok(Arc::new(provider))
            }
            Err(e) => {
                rate_controller.rollback().await;
                Err(read_provider_error(e, ConnectorComponent::from(dataset)))
            }
        }
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        if !self.emit_rate_control_metrics {
            return None;
        }

        Some(Arc::new(HttpRateControlMetricsProvider::new(
            "graphql",
            Arc::clone(&self.metrics),
            self.rate_control_metric_source.clone(),
        )))
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "graphql";

/// Returns a new instance of the `GraphQL` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    GraphQLFactory::new_arc()
}

/// Maps a provider build failure to the connector error the runtime retries on.
///
/// An invalid query, a resource the API reports as `NOT_FOUND`, and a refusal the API
/// named (HTTP 401/403, a `FORBIDDEN` or `UNAUTHENTICATED` GraphQL error) are permanent:
/// retrying cannot change the answer. Everything else, including a refusal inferred from
/// an ambiguous message, is retriable.
fn read_provider_error(
    e: graphql::Error,
    connector_component: ConnectorComponent,
) -> DataConnectorError {
    match &e {
        graphql::Error::InvalidGraphQLQuery { .. } | graphql::Error::ResourceNotFound { .. } => {
            let message = format!("{e}");
            DataConnectorError::InvalidConfiguration {
                dataconnector: "graphql".to_string(),
                connector_component,
                source: e.into(),
                message,
            }
        }
        graphql::Error::InvalidCredentialsOrPermissions {
            kind: graphql::RefusalKind::Explicit,
            ..
        } => DataConnectorError::InsufficientPermissions {
            dataconnector: "graphql".to_string(),
            connector_component,
            source: e.into(),
        },
        _ => DataConnectorError::InternalWithSource {
            dataconnector: "graphql".to_string(),
            connector_component,
            source: e.into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime::Runtime;
    use runtime::component::dataset::Dataset;
    use runtime::component::dataset::builder::DatasetBuilder;
    use runtime_secrets::Secrets;
    use std::collections::HashMap;
    use tokio::sync::RwLock;

    async fn test_params(extra: &[(&str, &str)]) -> Parameters {
        let mut params = vec![(
            "graphql_query".to_string(),
            "query { users { id } }".to_string().into(),
        )];
        params.extend(
            extra
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string().into())),
        );

        Parameters::try_new(
            "connector graphql",
            params,
            "graphql",
            Arc::new(RwLock::new(Secrets::default())),
            PARAMETERS.as_slice(),
        )
        .await
        .expect("test GraphQL parameters should be valid")
    }

    async fn test_dataset(url: &str) -> Dataset {
        let app = app::AppBuilder::new("graphql_test".to_string()).build();
        let runtime = Arc::new(Runtime::builder().with_app(app.clone()).build().await);
        let app = Arc::new(app);

        DatasetBuilder::try_new(format!("graphql:{url}"), "graphql_test")
            .expect("test dataset should be valid")
            .with_app(app)
            .with_runtime(runtime)
            .build()
            .expect("test dataset should build")
    }

    #[test]
    fn graphql_parameters_include_http_rate_control_specs() {
        let parameters = GraphQLFactory::new().parameters();
        for parameter_name in [
            "max_concurrent_requests",
            "requests_per_second_limit",
            "requests_per_minute_limit",
            "rate_control_jitter_min",
            "rate_control_jitter_max",
        ] {
            assert!(
                parameters
                    .iter()
                    .any(|parameter| parameter.name == parameter_name),
                "GraphQL connector should expose {parameter_name}"
            );
        }
    }

    #[tokio::test]
    async fn graphql_rate_control_dataset_params_parse_and_update_metrics() {
        let graphql = GraphQL {
            app_name: std::sync::Arc::from("test_app"),
            params: test_params(&[
                ("max_concurrent_requests", "4"),
                ("requests_per_second_limit", "2"),
                ("requests_per_minute_limit", "60"),
                ("rate_control_jitter_min", "2ms"),
                ("rate_control_jitter_max", "8ms"),
            ])
            .await,
            runtime_rate_control_params: None,
            rate_control_registry: http_rate_control::global_registry(),
            metrics: Arc::new(HttpRateControlMetrics::default()),
            emit_rate_control_metrics: true,
            rate_control_metric_source: None,
        };
        let dataset = test_dataset("https://graphql-dataset-params.example.com/graphql").await;

        let (_, reservation) = graphql
            .get_client(
                &dataset,
                Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
            )
            .await
            .expect("GraphQL client should build with rate-control params");
        reservation.commit().await;

        assert_eq!(graphql.metrics.max_concurrent_requests(), 4);
        assert_eq!(graphql.metrics.requests_per_second_limit(), 2);
        assert_eq!(graphql.metrics.requests_per_minute_limit(), 60);
        assert_eq!(graphql.metrics.available_permits(), 4);
    }

    #[tokio::test]
    async fn graphql_rate_control_uses_runtime_defaults_and_dataset_overrides() {
        let runtime_params = HashMap::from([
            ("http_max_concurrent_requests".to_string(), "5".to_string()),
            (
                "http_requests_per_second_limit".to_string(),
                "3".to_string(),
            ),
        ]);
        let graphql = GraphQL {
            app_name: std::sync::Arc::from("test_app"),
            params: test_params(&[("max_concurrent_requests", "2")]).await,
            runtime_rate_control_params: Some(runtime_params),
            rate_control_registry: http_rate_control::global_registry(),
            metrics: Arc::new(HttpRateControlMetrics::default()),
            emit_rate_control_metrics: true,
            rate_control_metric_source: None,
        };
        let dataset = test_dataset("https://graphql-runtime-defaults.example.com/graphql").await;

        let (_, reservation) = graphql
            .get_client(
                &dataset,
                Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
            )
            .await
            .expect("GraphQL client should build with runtime rate-control defaults");
        reservation.commit().await;

        assert_eq!(graphql.metrics.max_concurrent_requests(), 2);
        assert_eq!(graphql.metrics.requests_per_second_limit(), 3);
    }

    #[tokio::test]
    async fn graphql_rate_control_rejects_invalid_limits() {
        let graphql = GraphQL {
            app_name: std::sync::Arc::from("test_app"),
            params: test_params(&[("requests_per_second_limit", "0")]).await,
            runtime_rate_control_params: None,
            rate_control_registry: http_rate_control::global_registry(),
            metrics: Arc::new(HttpRateControlMetrics::default()),
            emit_rate_control_metrics: true,
            rate_control_metric_source: None,
        };
        let dataset = test_dataset("https://graphql-invalid-limit.example.com/graphql").await;

        let Err(error) = graphql
            .get_client(
                &dataset,
                Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
            )
            .await
        else {
            panic!("zero GraphQL rate-control limit should be rejected");
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
    async fn graphql_metrics_provider_can_be_suppressed_for_non_owner() {
        let graphql = GraphQL {
            app_name: std::sync::Arc::from("test_app"),
            params: test_params(&[]).await,
            runtime_rate_control_params: None,
            rate_control_registry: http_rate_control::global_registry(),
            metrics: Arc::new(HttpRateControlMetrics::default()),
            emit_rate_control_metrics: false,
            rate_control_metric_source: None,
        };

        assert!(DataConnector::metrics_provider(&graphql).is_none());
    }

    const CUSTOM_HEADER_AUTH: [(&str, &str); 3] = [
        ("graphql_auth_header", "x-api-key"),
        ("graphql_auth_token", "test-token"),
        ("json_pointer", "/data/users"),
    ];

    fn users_response() -> wiremock::ResponseTemplate {
        wiremock::ResponseTemplate::new(200)
            .set_body_raw(r#"{"data": {"users": [{"id": "1"}]}}"#, "application/json")
    }

    fn redirect_response(location: &str) -> wiremock::ResponseTemplate {
        wiremock::ResponseTemplate::new(307).insert_header("location", location)
    }

    async fn test_connector(params: &[(&str, &str)]) -> GraphQL {
        GraphQL {
            app_name: std::sync::Arc::from("test_app"),
            params: test_params(params).await,
            runtime_rate_control_params: None,
            rate_control_registry: http_rate_control::global_registry(),
            metrics: Arc::new(HttpRateControlMetrics::default()),
            emit_rate_control_metrics: true,
            rate_control_metric_source: None,
        }
    }

    /// Queries `endpoint` with the client a dataset with `params` gets.
    async fn query_users(
        params: &[(&str, &str)],
        endpoint: &str,
    ) -> graphql::Result<graphql::client::GraphQLQueryResult> {
        let connector = test_connector(params).await;
        let dataset = test_dataset(endpoint).await;
        let (client, reservation) = connector
            .get_client(
                &dataset,
                Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
            )
            .await
            .expect("the GraphQL client builds");
        reservation.commit().await;
        let query =
            graphql::client::GraphQLQuery::try_from(Arc::<str>::from("query { users { id } }"))
                .expect("the query parses");
        client.execute(&query, None, None, None, None, None).await
    }

    /// Builds the table provider of a dataset with `params` at `endpoint` as `read_provider`
    /// does, and returns the connector error its failure maps to.
    async fn read_provider_failure(params: &[(&str, &str)], endpoint: &str) -> DataConnectorError {
        let connector = test_connector(params).await;
        let dataset = test_dataset(endpoint).await;
        let (client, reservation) = connector
            .get_client(
                &dataset,
                Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
            )
            .await
            .expect("the GraphQL client builds");
        let Err(error) = GraphQLTableProviderBuilder::new(client)
            .build("query { users { id } }")
            .await
        else {
            panic!("the table provider must fail to build against {endpoint}");
        };
        reservation.rollback().await;
        read_provider_error(error, ConnectorComponent::from(&dataset))
    }

    /// A GraphQL `NOT_FOUND` names a resource that does not exist, so registration fails with a
    /// configuration error that startup does not retry, even behind an earlier bare timeout.
    #[tokio::test]
    async fn a_not_found_resource_is_a_permanent_configuration_error() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::any())
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_raw(
                r#"{"data": null, "errors": [{"message": "upstream timeout"}, {"message": "no such repository", "type": "NOT_FOUND"}]}"#,
                "application/json",
            ))
            .mount(&server)
            .await;

        let error = read_provider_failure(
            &[("json_pointer", "/data/users")],
            &format!("{}/graphql", server.uri()),
        )
        .await;

        assert!(
            matches!(error, DataConnectorError::InvalidConfiguration { .. }),
            "a NOT_FOUND maps to InvalidConfiguration, got {error:?}"
        );
        assert!(!error.is_retriable());
        assert!(
            error.to_string().contains("no such repository"),
            "the error names the missing resource: {error}"
        );
    }

    #[tokio::test]
    async fn a_custom_auth_header_is_not_sent_on_a_redirect_off_the_endpoint_origin() {
        let target = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::any())
            .respond_with(users_response())
            .mount(&target)
            .await;
        let target_url = format!("{}/graphql", target.uri());
        let endpoint = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::any())
            .respond_with(redirect_response(&target_url))
            .mount(&endpoint)
            .await;

        let Err(error) =
            query_users(&CUSTOM_HEADER_AUTH, &format!("{}/graphql", endpoint.uri())).await
        else {
            panic!("a redirect to another port must be refused");
        };

        assert!(
            error
                .to_string()
                .contains(&format!("Refused to follow the redirect to {target_url}")),
            "unexpected error: {error}"
        );
        let target_requests = target
            .received_requests()
            .await
            .expect("requests are recorded");
        assert!(
            target_requests.is_empty(),
            "the redirect target must receive no request"
        );
    }

    #[tokio::test]
    async fn a_custom_auth_header_follows_a_redirect_within_the_endpoint_origin() {
        let endpoint = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::path("/graphql"))
            .respond_with(redirect_response("/graphql/v2"))
            .mount(&endpoint)
            .await;
        wiremock::Mock::given(wiremock::matchers::path("/graphql/v2"))
            .and(wiremock::matchers::header("x-api-key", "test-token"))
            .respond_with(users_response())
            .mount(&endpoint)
            .await;

        let result = query_users(&CUSTOM_HEADER_AUTH, &format!("{}/graphql", endpoint.uri()))
            .await
            .expect("the redirect is followed with the header");

        let rows: usize = result
            .records
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 1);
    }

    #[tokio::test]
    async fn a_dataset_without_a_custom_header_token_follows_a_redirect_to_another_port() {
        for params in [
            &[
                ("graphql_auth_token", "test-token"),
                ("json_pointer", "/data/users"),
            ][..],
            &[
                ("graphql_auth_header", "x-api-key"),
                ("graphql_auth_user", "user"),
                ("json_pointer", "/data/users"),
            ][..],
        ] {
            let target = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::any())
                .respond_with(users_response())
                .mount(&target)
                .await;
            let endpoint = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::any())
                .respond_with(redirect_response(&format!("{}/graphql", target.uri())))
                .mount(&endpoint)
                .await;

            let result = query_users(params, &format!("{}/graphql", endpoint.uri()))
                .await
                .unwrap_or_else(|error| {
                    panic!("the redirect is followed with {params:?}: {error}")
                });

            let rows: usize = result
                .records
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();
            assert_eq!(rows, 1);
        }
    }

    /// A refusal the API named stays permanent at the connector boundary, so startup
    /// does not retry it; an inferred refusal stays retriable.
    #[tokio::test]
    async fn an_explicit_refusal_is_a_permanent_permission_error() {
        let dataset = test_dataset("http://localhost:1/graphql").await;
        let component = ConnectorComponent::from(&dataset);

        let explicit = read_provider_error(
            graphql::Error::InvalidCredentialsOrPermissions {
                message: "FORBIDDEN".to_string(),
                kind: graphql::RefusalKind::Explicit,
            },
            component.clone(),
        );
        assert!(
            matches!(explicit, DataConnectorError::InsufficientPermissions { .. }),
            "an explicit refusal maps to InsufficientPermissions, got {explicit:?}"
        );
        assert!(!explicit.is_retriable());

        let inferred = read_provider_error(
            graphql::Error::InvalidCredentialsOrPermissions {
                message: "not authorized".to_string(),
                kind: graphql::RefusalKind::Inferred,
            },
            component,
        );
        assert!(inferred.is_retriable());
    }

    /// An endpoint that redirects to `/a` on another port, whose server redirects `/a` to its own
    /// `/b` and serves users at `/b`. Returns the endpoint URL and both servers.
    async fn redirect_chain() -> (String, wiremock::MockServer, wiremock::MockServer) {
        let target = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::path("/a"))
            .respond_with(redirect_response("/b"))
            .mount(&target)
            .await;
        wiremock::Mock::given(wiremock::matchers::path("/b"))
            .respond_with(users_response())
            .mount(&target)
            .await;
        let endpoint = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::any())
            .respond_with(redirect_response(&format!("{}/a", target.uri())))
            .mount(&endpoint)
            .await;
        (format!("{}/graphql", endpoint.uri()), endpoint, target)
    }

    async fn requests_for(server: &wiremock::MockServer, path: &str) -> usize {
        server
            .received_requests()
            .await
            .expect("requests are recorded")
            .iter()
            .filter(|request| request.url.path() == path)
            .count()
    }

    #[tokio::test]
    async fn a_dataset_with_credentials_does_not_follow_a_redirect_chain_that_continues_on_another_host()
     {
        for (userinfo, params) in [
            (
                "",
                &[
                    ("graphql_auth_token", "test-token"),
                    ("json_pointer", "/data/users"),
                ][..],
            ),
            (
                "",
                &[
                    ("graphql_auth_user", "user"),
                    ("json_pointer", "/data/users"),
                ][..],
            ),
            ("svc:s3cret@", &[("json_pointer", "/data/users")][..]),
        ] {
            let (endpoint_url, _endpoint, target) = redirect_chain().await;
            let endpoint_url = endpoint_url.replacen("://", &format!("://{userinfo}"), 1);

            let Err(error) = query_users(params, &endpoint_url).await else {
                panic!("the redirect chain must be refused with {params:?}");
            };

            assert!(
                error.to_string().contains(&format!(
                    "Refused to follow the redirect to {}/b",
                    target.uri()
                )),
                "unexpected error with {params:?}: {error}"
            );
            assert_eq!(requests_for(&target, "/a").await, 1);
            assert_eq!(
                requests_for(&target, "/b").await,
                0,
                "the second hop on the target must not be requested with {params:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_dataset_without_credentials_follows_a_redirect_chain_that_continues_on_another_host()
    {
        let (endpoint_url, _endpoint, target) = redirect_chain().await;

        let result = query_users(&[("json_pointer", "/data/users")], &endpoint_url)
            .await
            .expect("the redirect chain is followed");

        let rows: usize = result
            .records
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 1);
        assert_eq!(requests_for(&target, "/b").await, 1);
    }
}

// Self-register into `data-connector-api`'s linkme `DATA_CONNECTOR_REGISTRATIONS` slice. Any binary/tool that
// should see this connector must force-link the crate (`use connector_graphql as _;`) -- a plain
// Cargo dependency won't link the slice static. See `register_data_connector!` docs.
data_connector_api::register_data_connector!(
    register_graphql_connector,
    GRAPHQL_CONNECTOR_REGISTRATION,
    CONNECTOR_NAME,
    GraphQLFactory
);
