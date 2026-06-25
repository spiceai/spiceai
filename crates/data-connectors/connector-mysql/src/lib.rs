/*
Copyright 2026 The Spice.ai OSS Authors

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
use datafusion::datasource::TableProvider;
use datafusion::sql::sqlparser::dialect::MySqlDialect;
use datafusion_table_providers::mysql::MySQLTableFactory;
use datafusion_table_providers::sql::arrow_sql_gen::mysql::MysqlZeroDateBehavior;
use datafusion_table_providers::sql::db_connection_pool::{
    Error as DbConnectionPoolError, dbconnection,
    mysqlpool::{self, MySQLConnectionPool},
};
use mysql_async::{Metrics, prelude::Queryable};
use opentelemetry::KeyValue;
use runtime::component::ComponentType;
use runtime::component::dataset::Dataset;
use runtime::component::metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult,
};
use runtime::datafusion::udf::deny_spice_functions_for_table_providers;
use runtime::parameters::ParameterSpec;
use secrecy::ExposeSecret;
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

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
    pool: Arc<MySQLConnectionPool>,
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

const MYSQL_DOCS: &str = "https://spiceai.org/docs/components/data-connectors/mysql";

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("connection_string")
        .description("Full MySQL DSN. Overrides other connection params if set.")
        .examples(&["mysql://app:secret@db.internal:3306/analytics"])
        .help_link(MYSQL_DOCS)
        .secret(),
    ParameterSpec::component("user")
        .description("MySQL username.")
        .help_link(MYSQL_DOCS)
        .secret(),
    ParameterSpec::component("pass")
        .description("MySQL password.")
        .help_link(MYSQL_DOCS)
        .secret(),
    ParameterSpec::component("host")
        .description("MySQL server hostname or IP.")
        .examples(&["db.internal", "mysql.cluster"])
        .help_link(MYSQL_DOCS),
    ParameterSpec::component("tcp_port")
        .description("MySQL TCP port.")
        .examples(&["3306"])
        .help_link(MYSQL_DOCS),
    ParameterSpec::component("db")
        .description("Database name.")
        .examples(&["app", "analytics"])
        .help_link(MYSQL_DOCS),
    ParameterSpec::component("sslmode")
        .description(
            "TLS mode for the connection. Common values: 'disabled', 'preferred', 'required'.",
        )
        .help_link(MYSQL_DOCS),
    ParameterSpec::component("sslrootcert")
        .description("Path to a PEM-encoded CA certificate used to verify the server when TLS is enabled.")
        .help_link(MYSQL_DOCS),
    ParameterSpec::component("pool_min")
        .description("The minimum number of connections to keep open in the pool, lazily created when requested.")
        .default("1")
        .help_link(MYSQL_DOCS),
    ParameterSpec::component("pool_max")
        .description("The maximum number of connections created in the connection pool.")
        .default("5")
        .help_link(MYSQL_DOCS),
    ParameterSpec::component("time_zone")
        .description("The time zone to use for the connection. Default is '+00:00' (UTC).")
        .help_link(MYSQL_DOCS),
    ParameterSpec::component("zero_date_behavior")
        .description(
            "How to handle the MySQL '0000-00-00' / '0000-00-00 00:00:00' zero-date sentinel for DATE/DATETIME/TIMESTAMP columns. \
             'null' (default) coerces zero dates to NULL and reports such columns as nullable in the Arrow schema. \
             'error' fails the scan when a zero date is encountered and honors the source NOT NULL constraint exactly.",
        )
        .default("null")
        .one_of_ignore_ascii_case(&["null", "error"])
        .help_link(MYSQL_DOCS),
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
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
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

            let zero_date_behavior = match params
                .parameters
                .get("zero_date_behavior")
                .ok()
                .map(|s| s.expose_secret().to_ascii_lowercase())
                .as_deref()
            {
                Some("error") => MysqlZeroDateBehavior::Error,
                // `one_of_ignore_ascii_case` validation has already rejected anything other
                // than "null" / "error"; default + any other value falls through to Null.
                _ => MysqlZeroDateBehavior::Null,
            };

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
                Ok(pool) => Arc::new(pool.with_zero_date_behavior(zero_date_behavior)),
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
            // Install the Spice function deny-list so federation evaluates
            // Spice-only UDFs (`json_get_str`, the embedding/distance UDFs, etc.)
            // locally instead of pushing them into the SQL sent to MySQL, where
            // those functions don't exist and the query would fail with an
            // "unknown function" error. See issue #10703.
            let mysql_factory = MySQLTableFactory::new(Arc::clone(&pool))
                .with_function_support(deny_spice_functions_for_table_providers());

            Ok(Arc::new(MySQL {
                mysql_factory,
                pool,
            }) as Arc<dyn DataConnector>)
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

async fn mysql_comment_metadata(
    pool: &Arc<MySQLConnectionPool>,
    table_reference: &datafusion::sql::TableReference,
) -> std::result::Result<
    (HashMap<String, String>, data_components::FieldMetadata),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let connection = pool.connect_direct().await?;
    let mut conn = connection.conn.lock().await;
    let table_schema = table_reference
        .schema()
        .or_else(|| table_reference.catalog())
        .map(ToString::to_string);
    let table_name = table_reference.table().to_string();

    let rows: Vec<data_components::mysql::provider::MySqlTableMetadataRow> = conn
        .exec(
            "SELECT \
                 NULLIF(t.TABLE_COMMENT, '') AS TABLE_COMMENT, \
                 c.COLUMN_NAME, \
                 NULLIF(c.COLUMN_COMMENT, '') AS COLUMN_COMMENT, \
                 c.COLUMN_TYPE \
             FROM information_schema.TABLES t \
             LEFT JOIN information_schema.COLUMNS c \
                 ON c.TABLE_SCHEMA = t.TABLE_SCHEMA \
                 AND c.TABLE_NAME = t.TABLE_NAME \
             WHERE t.TABLE_SCHEMA = COALESCE(?, DATABASE()) \
             AND t.TABLE_NAME = ? \
             ORDER BY c.ORDINAL_POSITION",
            (table_schema, table_name),
        )
        .await?;

    Ok(data_components::mysql::provider::mysql_metadata_from_rows(
        rows,
    ))
}

async fn enrich_with_mysql_comments(
    pool: &Arc<MySQLConnectionPool>,
    dataset: &Dataset,
    table_reference: &datafusion::sql::TableReference,
    provider: Arc<dyn TableProvider>,
) -> Arc<dyn TableProvider> {
    match mysql_comment_metadata(pool, table_reference).await {
        Ok((table_metadata, field_metadata)) => {
            if table_metadata.is_empty() && field_metadata.is_empty() {
                provider
            } else {
                data_components::metadata_enriched_table_provider(
                    provider,
                    table_metadata,
                    field_metadata,
                )
            }
        }
        Err(error) => {
            tracing::warn!(
                dataset = %dataset.name,
                source = %dataset.path(),
                error = %error,
                "Failed to query MySQL comments; registering without comment metadata"
            );
            provider
        }
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
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let tbl = dataset
            .parse_path(true, Some(&MySqlDialect {}))
            .boxed()
            .map_err(|e| DataConnectorError::InvalidConfiguration {
                dataconnector: "mysql".to_string(),
                source: e,
                message: format!("The specified table name in dataset path is invalid '{}'.\nEnsure the table name uses valid characters for a MySQL table name and try again.", dataset.path()),
                connector_component: ConnectorComponent::from(dataset),
            })?;

        // Call the inherent method directly instead of using Read trait
        // (orphan rule prevents trait impl in external crate)
        let table_reference = tbl.clone();
        match self.mysql_factory.table_provider(tbl).await {
            Ok(provider) => {
                Ok(
                    enrich_with_mysql_comments(&self.pool, dataset, &table_reference, provider)
                        .await,
                )
            }
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

                Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "mysql".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                })
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

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "mysql";

/// Returns a new instance of the `MySQL` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    MySQLFactory::new_arc()
}
