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
use data_components::cdc::{InitialSnapshotMode, InvalidCheckpointBehavior};
use data_components::inferred_schema::InferredSchema;
use data_components::mysql_replication::{ReplicationMetrics, ReplicationMetricsCollector};
use data_connector_api::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult, parameters::ConnectorContext,
};
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
use runtime_api_types::v1::ComponentType;
use runtime_component::dataset::DatasetSpec;
use runtime_metrics::component::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use runtime_parameters::{ParameterSpec, Parameters};
use runtime_udfs_api::deny_spice_functions_for_table_providers;
use secrecy::{ExposeSecret, SecretBox};
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

pub mod replication;

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
    /// Connector params retained for the replication path, which opens its
    /// own dedicated connections outside the pool.
    params: Parameters,
    replication_metrics: Arc<ReplicationMetricsCollector>,
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
    ParameterSpec::component("replication_server_id")
        .description(
            "The server_id this replica registers on the source with for `refresh_mode: changes`. \
             Must be unique among all replicas attached to the same source. Default: derived from \
             the dataset name and process.",
        )
        .help_link(MYSQL_DOCS),
    ParameterSpec::component("replication_initial_snapshot")
        .description(
            "When `refresh_mode: changes` loads the table's existing rows: 'auto' (default) \
             snapshots when no resumable binlog position exists and resumes without a snapshot \
             when one does; 'disabled' streams changes only; 'always' re-snapshots on every \
             start, discarding any persisted position. Default: auto.",
        )
        .default("auto")
        .one_of_ignore_ascii_case(InitialSnapshotMode::VALUES)
        .help_link(MYSQL_DOCS),
    ParameterSpec::component("replication_checkpoint_interval")
        .description(
            "How often the committed binlog position is persisted to the accelerator sidecar \
             (e.g. '10s'). A crash replays at most this much already-applied change history. \
             Default: 10s.",
        )
        .default("10s")
        .help_link(MYSQL_DOCS),
    ParameterSpec::component("replication_bootstrap_batch_size")
        .description(
            "Rows per emitted batch during the initial replication snapshot. \
             Default: 8192. Maximum: 1048576.",
        )
        .default("8192")
        .help_link(MYSQL_DOCS),
    ParameterSpec::component("replication_invalid_checkpoint_behavior")
        .description(
            "What to do when the persisted binlog position was purged from the source: 'error' \
             (default) surfaces an actionable error; 'restart' drops the saved position and \
             re-snapshots the table. Default: error.",
        )
        .default("error")
        .one_of_ignore_ascii_case(InvalidCheckpointBehavior::VALUES)
        .help_link(MYSQL_DOCS),
    ParameterSpec::component("replication_ready_lag")
        .description(
            "For `refresh_mode: changes`, the dataset is marked Ready once its replication lag \
             (now minus the newest applied commit's binlog-header timestamp) falls below this. It \
             stays not-ready while snapshotting or draining a backlog on resume, so it never serves \
             stale data. Default: 2s.",
        )
        .default("2s")
        .help_link(MYSQL_DOCS),
];

// https://github.com/apache/datafusion-sqlparser-rs/blob/87d19073/src/keywords.rs#L1053
const RESERVED_KEYWORDS: &[&str] = &["PARTITION"];

impl DataConnectorFactory for MySQLFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create<'a>(
        &'a self,
        mut params: ConnectorParams,
        _context: &'a dyn ConnectorContext,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
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

            let params_for_replication = params.parameters.clone();

            let mut param_map = params.parameters.to_secret_map();

            // `refresh_mode: changes` datasets use this pool only for schema
            // probes at initialization — replication runs over its own
            // dedicated connections. Unless the user sized the pool
            // explicitly, keep it minimal: no idle connections held for the
            // lifetime of the dataset, and a small max. This matters at N
            // CDC datasets per source database. (Same policy as the Postgres
            // connector.)
            if let ConnectorComponent::Dataset(dataset) = &params.component {
                let is_changes_mode = dataset.acceleration.as_ref().is_some_and(|acceleration| {
                    acceleration.refresh_mode
                        == Some(runtime::component::dataset::acceleration::RefreshMode::Changes)
                });
                if is_changes_mode {
                    // The injected spec defaults are indistinguishable from
                    // user-set values here, so consult the raw spicepod
                    // params for whether the user chose a size.
                    let user_set = |key: &str| {
                        dataset.params.contains_key(&format!("mysql_{key}"))
                            || dataset.params.contains_key(key)
                    };
                    if !user_set("pool_min") {
                        param_map.insert("pool_min".to_string(), SecretBox::from("0"));
                    }
                    if !user_set("pool_max") {
                        param_map.insert("pool_max".to_string(), SecretBox::from("2"));
                    }
                }
            }

            let pool = match MySQLConnectionPool::new(param_map).await {
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
                params: params_for_replication,
                replication_metrics: ReplicationMetricsCollector::new(),
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

/// Query `information_schema` for the target table's primary key and rough
/// sizing. Returns an [`InferredSchema`]; empty when nothing usable was found
/// (e.g. a table with no primary key). Secondary indexes / sort columns /
/// per-column stats are not inferred for `MySQL` yet — only the primary key
/// (which `refresh_mode: changes` requires to route UPDATE/DELETE events) and
/// the row-count/byte estimates the adaptive tuner warm-starts from.
async fn mysql_inferred_schema_metadata(
    pool: &Arc<MySQLConnectionPool>,
    table_reference: &datafusion::sql::TableReference,
) -> std::result::Result<InferredSchema, Box<dyn std::error::Error + Send + Sync>> {
    let connection = pool.connect_direct().await?;
    let mut conn = connection.conn.lock().await;
    let table_schema = table_reference
        .schema()
        .or_else(|| table_reference.catalog())
        .map(ToString::to_string);
    let table_name = table_reference.table().to_string();

    // Primary-key columns, in key order. `KEY_COLUMN_USAGE` names the PK
    // constraint `PRIMARY`; `ORDINAL_POSITION` is the column's position within
    // the key (1-based), so ordering by it preserves composite-key order.
    let pk_rows: Vec<(String,)> = conn
        .exec(
            "SELECT COLUMN_NAME \
             FROM information_schema.KEY_COLUMN_USAGE \
             WHERE CONSTRAINT_NAME = 'PRIMARY' \
               AND TABLE_SCHEMA = COALESCE(?, DATABASE()) \
               AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (table_schema.clone(), table_name.clone()),
        )
        .await?;
    let primary_key: Vec<String> = pk_rows.into_iter().map(|(name,)| name).collect();

    // Rough sizing (best-effort; a failure here must not fail inference).
    // `TABLE_ROWS` is an estimate for InnoDB, which matches `InferredSchema`'s
    // "estimate, not a precise count" contract.
    let mut row_count: Option<u64> = None;
    let mut table_bytes: Option<u64> = None;
    let size_result: mysql_async::Result<Vec<(Option<u64>, Option<u64>)>> = conn
        .exec(
            "SELECT TABLE_ROWS, DATA_LENGTH \
             FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = COALESCE(?, DATABASE()) \
               AND TABLE_NAME = ?",
            (table_schema, table_name),
        )
        .await;
    match size_result {
        Ok(rows) => {
            if let Some((rows_est, bytes)) = rows.into_iter().next() {
                row_count = rows_est;
                table_bytes = bytes;
            }
        }
        Err(error) => {
            tracing::debug!(%error, "Failed to query MySQL table size; continuing without it");
        }
    }

    Ok(InferredSchema {
        primary_key,
        row_count,
        table_bytes,
        ..Default::default()
    })
}

/// Enrich the provider's schema with `MySQL` metadata: column/table comments and
/// source types, plus the inferred primary key / sizing. Schema inference is
/// always attempted. If the `information_schema` query fails (`Err`) it degrades to
/// base column/type inference with an **info** log (see below); the best-effort
/// sizing sub-query fails at debug level and still returns `Ok`, so a sizing-only
/// gap may surface no info log. Mirrors the `PostgreSQL` connector's
/// `enrich_with_postgres_metadata`.
async fn enrich_with_mysql_metadata(
    pool: &Arc<MySQLConnectionPool>,
    dataset: &DatasetSpec,
    table_reference: &datafusion::sql::TableReference,
    provider: Arc<dyn TableProvider>,
) -> Arc<dyn TableProvider> {
    let (mut table_metadata, field_metadata) =
        match mysql_comment_metadata(pool, table_reference).await {
            Ok(metadata) => metadata,
            Err(error) => {
                tracing::warn!(
                    dataset = %dataset.name,
                    source = %dataset.path(),
                    error = %error,
                    "Failed to query MySQL comments; registering without comment metadata"
                );
                (HashMap::new(), data_components::FieldMetadata::new())
            }
        };

    // Always attempt maximum schema inference; degrade gracefully when the source
    // blocks the catalog queries (commonly the connection user lacks access to
    // information_schema), falling back to base column/type inference only.
    match mysql_inferred_schema_metadata(pool, table_reference).await {
        Ok(inferred) => {
            if !inferred.is_empty() {
                tracing::debug!(
                    dataset = %dataset.name,
                    source = %dataset.path(),
                    primary_key = ?inferred.primary_key,
                    row_count = ?inferred.row_count,
                    table_bytes = ?inferred.table_bytes,
                    "Inferred schema metadata from MySQL catalog"
                );
            }
            table_metadata.extend(inferred.to_metadata());
        }
        Err(error) => {
            // Graceful degradation, not a failure: the source blocked the catalog
            // inference queries (commonly the connection user lacks information_schema
            // access). Fall back to base column/type inference and log at info.
            tracing::info!(
                dataset = %dataset.name,
                source = %dataset.path(),
                error = %error,
                "Schema inference degraded to base column/type inference (mysql): could not read information_schema, usually because the connection user lacks access. Primary key and sizing were not inferred; grant information_schema access for full inference."
            );
        }
    }

    if table_metadata.is_empty() && field_metadata.is_empty() {
        provider
    } else {
        data_components::metadata_enriched_table_provider(provider, table_metadata, field_metadata)
    }
}

#[async_trait]
impl DataConnector for MySQL {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
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
                    enrich_with_mysql_metadata(&self.pool, dataset, &table_reference, provider)
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

    fn supports_changes_stream(&self) -> bool {
        true
    }

    async fn changes_stream(
        &self,
        context: &dyn ConnectorContext,
        federated_table: Arc<dyn data_connector_api::federated::FederatedTableProvider>,
        dataset: &DatasetSpec,
    ) -> Option<data_components::cdc::ChangesStream> {
        let position_store = replication::resolve_position_store(context, dataset).await;
        Some(replication::build_changes_stream(
            &self.params,
            dataset,
            position_store,
            federated_table,
            Arc::clone(&self.replication_metrics),
        ))
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        Some(Arc::new(MySQLMetricsProvider::new(
            self.mysql_factory.conn_pool_metrics(),
            ReplicationMetrics::new(Arc::clone(&self.replication_metrics)),
        )))
    }
}

#[derive(Debug, Clone)]
struct MySQLMetricsProvider {
    metrics: Arc<Metrics>,
    replication: ReplicationMetrics,
}

impl MySQLMetricsProvider {
    fn new(metrics: Arc<Metrics>, replication: ReplicationMetrics) -> Self {
        Self {
            metrics,
            replication,
        }
    }
}

/// Connection-pool metrics plus the `replication_*` set — `available_metrics`
/// needs one `'static` slice covering both.
static ALL_METRICS: LazyLock<Vec<MetricSpec>> = LazyLock::new(|| {
    METRICS
        .iter()
        .chain(replication::REPLICATION_METRICS.iter())
        .copied()
        .collect()
});

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
        &ALL_METRICS
    }

    fn callback_to_observe_metric(
        &self,
        metric: &MetricSpec,
        attributes: Vec<KeyValue>,
    ) -> Option<ObserveMetricCallback> {
        // Dispatch by membership rather than a name prefix so a future spec
        // that breaks the `replication_` naming convention still routes to
        // its callback (and the coverage test still holds).
        if replication::REPLICATION_METRICS
            .iter()
            .any(|spec| spec.name == metric.name)
        {
            return replication::observe_replication_metric(
                &self.replication,
                metric.name,
                attributes,
            );
        }
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

// Self-register into `data-connector-api`'s linkme `DATA_CONNECTOR_REGISTRATIONS` slice. Any binary/tool that
// should see this connector must force-link the crate (`use connector_mysql as _;`) -- a plain
// Cargo dependency won't link the slice static. See `register_data_connector!` docs.
data_connector_api::register_data_connector!(
    register_mysql_connector,
    MYSQL_CONNECTOR_REGISTRATION,
    CONNECTOR_NAME,
    MySQLFactory
);
