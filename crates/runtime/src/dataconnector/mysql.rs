/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::component::ComponentType;
use crate::component::dataset::Dataset;
use crate::component::metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use async_trait::async_trait;
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion::sql::sqlparser::dialect::MySqlDialect;
use datafusion_table_providers::mysql::MySQLTableFactory;
use datafusion_table_providers::sql::db_connection_pool::{
    Error as DbConnectionPoolError, dbconnection,
    mysqlpool::{self, MySQLConnectionPool},
};
use mysql_async::Metrics;
use opentelemetry::KeyValue;
use secrecy::ExposeSecret;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create MySQL connection pool: {source}"))]
    UnableToCreateMySQLConnectionPool { source: DbConnectionPoolError },

    #[snafu(display(
        "Invalid connection pool configuration: pool_min ({pool_min}) cannot be greater than pool_max ({pool_max})"
    ))]
    InvalidConnectionPoolConfiguration { pool_min: usize, pool_max: usize },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

const DEFAULT_CONNECTION_POOL_MIN: usize = 1;
const DEFAULT_CONNECTION_POOL_MAX: usize = 5;

pub struct MySQL {
    mysql_factory: MySQLTableFactory,
}

impl std::fmt::Debug for MySQL {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySQL").finish_non_exhaustive()
    }
}

#[derive(Default, Copy, Clone)]
pub struct MySQLFactory {}

impl MySQLFactory {
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
    ParameterSpec::component("connection_string").secret(),
    ParameterSpec::component("user").secret(),
    ParameterSpec::component("pass").secret(),
    ParameterSpec::component("host"),
    ParameterSpec::component("tcp_port"),
    ParameterSpec::component("db"),
    ParameterSpec::component("sslmode"),
    ParameterSpec::component("sslrootcert"),
    ParameterSpec::component("pool_min")
        .description("The minimum number of connections to keep open in the pool, lazily created when requested.")
        .default("1"),
    ParameterSpec::component("pool_max")
        .description("The maximum number of connections created in the connection pool.")
        .default("5"),
    ParameterSpec::component("time_zone")
        .description("The time zone to use for the connection. Default is '+00:00' (UTC)."),
];

// https://github.com/apache/datafusion-sqlparser-rs/blob/87d19073/src/keywords.rs#L1053
const RESERVED_KEYWORDS: &[&str] = &["PARTITION"];

impl DataConnectorFactory for MySQLFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        mut params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let pool_min = params
                .parameters
                .get("pool_min")
                .ok()
                .and_then(|s| {
                    let pool_min_str = s.expose_secret();
                    let parsed_pool_min = pool_min_str.parse::<usize>();
                    if parsed_pool_min.is_err() {
                        tracing::warn!(
                            "Invalid pool_min value: {pool_min_str}, using default of {DEFAULT_CONNECTION_POOL_MIN}"
                        );
                    }
                    parsed_pool_min.ok()
                })
                .unwrap_or(DEFAULT_CONNECTION_POOL_MIN);
            let pool_max = params
                .parameters
                .get("pool_max")
                .ok()
                .and_then(|s| {
                    let pool_max_str = s.expose_secret();
                    let parsed_pool_max = pool_max_str.parse::<usize>();
                    if parsed_pool_max.is_err() {
                        tracing::warn!(
                            "Invalid pool_max value: {pool_max_str}, using default of {DEFAULT_CONNECTION_POOL_MAX}"
                        );
                    }
                    parsed_pool_max.ok()
                })
                .unwrap_or(DEFAULT_CONNECTION_POOL_MAX);

            if pool_min > pool_max {
                return Err(
                    Error::InvalidConnectionPoolConfiguration { pool_min, pool_max }.into(),
                );
            }

            if let Some(time_zone) = params.parameters.get("time_zone").expose().ok() {
                // "LOCAL_SYSTEM" value must be replaced with the actual system time zone information.
                if time_zone.to_uppercase() == "LOCAL_SYSTEM" {
                    let local_offset = format!("{}", chrono::Local::now().offset());
                    tracing::debug!(
                        "Using local system time zone '{local_offset}' to connect to MySQL table '{}'",
                        params.component
                    );
                    params
                        .parameters
                        .insert("time_zone".to_string(), local_offset.into());
                } else {
                    tracing::debug!(
                        "Using time zone '{time_zone}' to connect to MySQL table '{}'",
                        params.component
                    );
                }
            }

            let pool = match MySQLConnectionPool::new(params.parameters.to_secret_map()).await {
                Ok(pool) => Arc::new(pool),
                Err(error) => match error {
                    mysqlpool::Error::InvalidUsernameOrPassword => {
                        return Err(
                            DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                                dataconnector: "mysql".to_string(),
                                connector_component: params.component.clone(),
                            }
                            .into(),
                        );
                    }

                    mysqlpool::Error::InvalidHostOrPortError {
                        source: _,
                        host,
                        port,
                    } => {
                        return Err(DataConnectorError::UnableToConnectInvalidHostOrPort {
                            dataconnector: "mysql".to_string(),
                            connector_component: params.component.clone(),
                            host,
                            port: format!("{port}"),
                        }
                        .into());
                    }

                    _ => {
                        return Err(DataConnectorError::UnableToConnectInternal {
                            dataconnector: "mysql".to_string(),
                            connector_component: params.component.clone(),
                            source: Box::new(error),
                        }
                        .into());
                    }
                },
            };
            let mysql_factory = MySQLTableFactory::new(pool);

            Ok(Arc::new(MySQL { mysql_factory }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "mysql"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }

    fn reserved_keywords(&self) -> &'static [&'static str] {
        RESERVED_KEYWORDS
    }
}

#[async_trait]
impl DataConnector for MySQL {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let tbl = dataset
            .parse_path(true, Some(&MySqlDialect {}))
            .boxed()
            .map_err(|e| super::DataConnectorError::InvalidConfiguration {
                dataconnector: "mysql".to_string(),
                source: e,
                message: format!("The specified table name in dataset path is invalid '{}'.\nEnsure the table name uses valid characters for a MySQL table name and try again.", dataset.path()),
                connector_component: ConnectorComponent::from(dataset),
            })?;

        match Read::table_provider(&self.mysql_factory, tbl).await {
            Ok(provider) => Ok(provider),
            Err(e) => {
                if let Some(err_source) = e.source()
                    && let Some(dbconnection::Error::UndefinedTable {
                        table_name,
                        source: _,
                    }) = err_source.downcast_ref::<dbconnection::Error>()
                {
                    return Err(DataConnectorError::InvalidTableName {
                        dataconnector: "mysql".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        table_name: table_name.clone(),
                    });
                }

                return Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "mysql".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                });
            }
        }
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        Some(Arc::new(MySQLMetricsProvider::new(
            self.mysql_factory.conn_pool_metrics(),
        )))
    }
}

#[derive(Debug, Clone)]
struct MySQLMetricsProvider {
    metrics: Arc<Metrics>,
}

impl MySQLMetricsProvider {
    fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

const METRICS: &[MetricSpec] = &[
    MetricSpec::new("connection_count", MetricType::ObservableGaugeU64)
        .description("Gauge of active connections to the database server"),
    MetricSpec::new("connections_in_pool", MetricType::ObservableGaugeU64)
        .description("Gauge of active connections that are idling in the pool"),
    MetricSpec::new("active_wait_requests", MetricType::ObservableGaugeU64).description(
        "Gauge of requests that are waiting for a connection to be returned to the pool",
    ),
    MetricSpec::new("create_failed", MetricType::ObservableCounterU64)
        .description("Counter of connections that failed to be created"),
    MetricSpec::new(
        "discarded_superfluous_connection",
        MetricType::ObservableCounterU64,
    )
        .description(
            "Counter of connections that were closed because there were already enough idle connections in the pool",
        ),
    MetricSpec::new("discarded_unestablished_connection", MetricType::ObservableCounterU64)
        .description(
            "Counter of connections that were closed because they could not be established",
        ),
    MetricSpec::new("dirty_connection_return", MetricType::ObservableCounterU64)
        .description(
            "Counter of connections that were returned to the pool but were dirty (ie. open transactions, pending queries, etc)",
        ),
    MetricSpec::new("discarded_expired_connection", MetricType::ObservableCounterU64)
        .description(
            "Counter of connections that were discarded because they were expired by the pool constraints (i.e. TTL expired)",
        ),
    MetricSpec::new("resetting_connection", MetricType::ObservableCounterU64)
        .description(
            "Counter of connections that were reset",
        ),
    MetricSpec::new("discarded_error_during_cleanup", MetricType::ObservableCounterU64)
        .description(
            "Counter of connections that were discarded because they returned an error during cleanup",
        ),
    MetricSpec::new("connection_returned_to_pool", MetricType::ObservableCounterU64)
        .description(
            "Counter of connections that were returned to the pool",
        ),
];

impl MetricsProvider for MySQLMetricsProvider {
    fn component_type(&self) -> ComponentType {
        ComponentType::Dataset
    }

    fn component_name(&self) -> &'static str {
        "mysql"
    }

    fn available_metrics(&self) -> &'static [MetricSpec] {
        METRICS
    }

    #[expect(clippy::too_many_lines)]
    fn callback_to_observe_metric(
        &self,
        metric: &MetricSpec,
        attributes: Vec<KeyValue>,
    ) -> Option<ObserveMetricCallback> {
        let metrics = Arc::clone(&self.metrics);
        match metric.name {
            "connection_count" => Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(
                    metrics
                        .connection_count
                        .load(std::sync::atomic::Ordering::Relaxed) as u64,
                    &attributes,
                );
            }))),
            "connections_in_pool" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .connections_in_pool
                            .load(std::sync::atomic::Ordering::Relaxed)
                            as u64,
                        &attributes,
                    );
                })))
            }
            "active_wait_requests" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .active_wait_requests
                            .load(std::sync::atomic::Ordering::Relaxed)
                            as u64,
                        &attributes,
                    );
                })))
            }
            "create_failed" => Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(
                    metrics
                        .create_failed
                        .load(std::sync::atomic::Ordering::Relaxed) as u64,
                    &attributes,
                );
            }))),
            "discarded_superfluous_connection" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .discarded_superfluous_connection
                            .load(std::sync::atomic::Ordering::Relaxed)
                            as u64,
                        &attributes,
                    );
                })))
            }
            "discarded_unestablished_connection" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .discarded_unestablished_connection
                            .load(std::sync::atomic::Ordering::Relaxed)
                            as u64,
                        &attributes,
                    );
                })))
            }
            "dirty_connection_return" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .dirty_connection_return
                            .load(std::sync::atomic::Ordering::Relaxed)
                            as u64,
                        &attributes,
                    );
                })))
            }
            "discarded_expired_connection" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .discarded_expired_connection
                            .load(std::sync::atomic::Ordering::Relaxed)
                            as u64,
                        &attributes,
                    );
                })))
            }
            "resetting_connection" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .resetting_connection
                            .load(std::sync::atomic::Ordering::Relaxed)
                            as u64,
                        &attributes,
                    );
                })))
            }
            "discarded_error_during_cleanup" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .discarded_error_during_cleanup
                            .load(std::sync::atomic::Ordering::Relaxed)
                            as u64,
                        &attributes,
                    );
                })))
            }
            "connection_returned_to_pool" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .connection_returned_to_pool
                            .load(std::sync::atomic::Ordering::Relaxed)
                            as u64,
                        &attributes,
                    );
                })))
            }
            _ => None,
        }
    }
}
