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

use async_trait::async_trait;
use data_components::graphql::{
    self, builder::GraphQLClientBuilder, client::GraphQLClient,
    provider::GraphQLTableProviderBuilder,
};
use data_components::rate_limit::RateLimiter;
use datafusion::datasource::TableProvider;
use runtime::component::dataset::Dataset;
use runtime::component::metrics::MetricsProvider;
use runtime::dataconnector::http_rate_control::{
    HttpRateControlMetricSource, HttpRateControlMetrics, HttpRateControlMetricsProvider,
};
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult, default_spice_client, http_rate_control,
};
use runtime::parameters::{ParameterSpec, Parameters};
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

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
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
    async fn get_client(
        &self,
        dataset: &Dataset,
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

        let client = default_spice_client("application/json")
            .boxed()
            .map_err(|source| DataConnectorError::InternalWithSource {
                dataconnector: "graphql".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source,
            })?;

        let rate_control = http_rate_control::resolve_config(
            &self.params,
            self.runtime_rate_control_params.as_ref(),
            dataset,
            "graphql",
        )?;
        let rate_limiter = self
            .rate_control_registry
            .shared_rate_limiter(&endpoint)
            .await;
        self.metrics.set_rate_limiter(&rate_limiter);
        let rate_limiter: Arc<dyn RateLimiter> = rate_limiter;
        let rate_controller = Arc::clone(&self.rate_control_registry)
            .reserve_shared_rate_controller(&endpoint, &rate_control, dataset, "graphql")
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
        .build(client)
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
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let query = self.params.get("query").expose().ok_or_else(|p| {
            DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "graphql".to_string(),
                message: format!("A required parameter was missing: `{}`.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/graphql#configuration", p.0),
                connector_component: ConnectorComponent::from(dataset),
            }
        })?;

        let (client, rate_controller) = self.get_client(dataset).await?;

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
                if matches!(&e, graphql::Error::InvalidGraphQLQuery { .. }) {
                    let message = format!("{e}");
                    Err(DataConnectorError::InvalidConfiguration {
                        dataconnector: "graphql".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: e.into(),
                        message,
                    })
                } else {
                    Err(DataConnectorError::InternalWithSource {
                        dataconnector: "graphql".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: e.into(),
                    })
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use runtime::Runtime;
    use runtime::component::dataset::builder::DatasetBuilder;
    use runtime::secrets::Secrets;
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
            .get_client(&dataset)
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
            params: test_params(&[("max_concurrent_requests", "2")]).await,
            runtime_rate_control_params: Some(runtime_params),
            rate_control_registry: http_rate_control::global_registry(),
            metrics: Arc::new(HttpRateControlMetrics::default()),
            emit_rate_control_metrics: true,
            rate_control_metric_source: None,
        };
        let dataset = test_dataset("https://graphql-runtime-defaults.example.com/graphql").await;

        let (_, reservation) = graphql
            .get_client(&dataset)
            .await
            .expect("GraphQL client should build with runtime rate-control defaults");
        reservation.commit().await;

        assert_eq!(graphql.metrics.max_concurrent_requests(), 2);
        assert_eq!(graphql.metrics.requests_per_second_limit(), 3);
    }

    #[tokio::test]
    async fn graphql_rate_control_rejects_invalid_limits() {
        let graphql = GraphQL {
            params: test_params(&[("requests_per_second_limit", "0")]).await,
            runtime_rate_control_params: None,
            rate_control_registry: http_rate_control::global_registry(),
            metrics: Arc::new(HttpRateControlMetrics::default()),
            emit_rate_control_metrics: true,
            rate_control_metric_source: None,
        };
        let dataset = test_dataset("https://graphql-invalid-limit.example.com/graphql").await;

        let Err(error) = graphql.get_client(&dataset).await else {
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
            params: test_params(&[]).await,
            runtime_rate_control_params: None,
            rate_control_registry: http_rate_control::global_registry(),
            metrics: Arc::new(HttpRateControlMetrics::default()),
            emit_rate_control_metrics: false,
            rate_control_metric_source: None,
        };

        assert!(DataConnector::metrics_provider(&graphql).is_none());
    }
}
