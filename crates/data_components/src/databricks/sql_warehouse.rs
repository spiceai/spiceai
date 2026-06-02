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

use arrow::{
    array::{Array, RecordBatch},
    datatypes::{Field, Schema, SchemaRef},
    ipc::reader::StreamReader,
};
use async_trait::async_trait;
use datafusion::{
    datasource::TableProvider, error::DataFusionError, execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter, sql::TableReference,
};
use datafusion_table_providers::sql::{
    db_connection_pool::{
        DbConnectionPool, JoinPushDown,
        dbconnection::{self, AsyncDbConnection, DbConnection},
    },
    sql_provider_datafusion::SqlTable,
};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use reqwest::{Client, ClientBuilder};
use runtime_rate_control::RateController;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snafu::{Snafu, prelude::*};
use std::{
    collections::{HashMap, hash_map::Entry},
    error::Error as StdError,
    fmt::{Display, Formatter},
    io::Cursor,
    pin::Pin,
    str::FromStr,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use token_provider::TokenProvider;
use tokio::sync::Semaphore;
use util::{
    fibonacci_backoff::{Backoff, FibonacciBackoffBuilder},
    format_datafusion_error,
};

#[cfg(test)]
use crate::resilient_http::configure_client_builder;
use crate::resilient_http::{
    DEFAULT_HTTP_CONNECT_TIMEOUT, DEFAULT_HTTP_REQUEST_TIMEOUT, RetryConfig,
    configure_client_builder_with_timeouts, send_request_with_retry_and_concurrency_limit,
};
use crate::schema_discovery::{
    DatasetPermissions, NoPermissionsCheck, PermissionCheckResult, SchemaProbeResult,
    discover_schema,
};
use crate::{DESCRIPTION_METADATA_KEY, PARTITION_METADATA_KEY, SOURCE_TYPE_METADATA_KEY};
use tracing::Instrument;
use util::retry_strategy::BackoffMethod;

mod datatypes;

const SQL_WAREHOUSE_MAX_IN_FLIGHT_REQUESTS: usize = 8;
const SQL_WAREHOUSE_DEFAULT_HTTP_MAX_RETRIES: usize = 3;
const SQL_WAREHOUSE_DEFAULT_STATEMENT_MAX_RETRIES: usize = 14;

type SharedSqlWarehouseKey = (String, String);
type SharedSqlWarehouseEntry = (Arc<Semaphore>, usize);
type SharedSqlWarehouseRegistry = Mutex<HashMap<SharedSqlWarehouseKey, SharedSqlWarehouseEntry>>;
type SharedSqlWarehouseMetricsRegistry =
    Mutex<HashMap<SharedSqlWarehouseKey, Arc<DatabricksMetrics>>>;

static SHARED_SQL_WAREHOUSE_SEMAPHORES: LazyLock<SharedSqlWarehouseRegistry> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static SHARED_SQL_WAREHOUSE_METRICS: LazyLock<SharedSqlWarehouseMetricsRegistry> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Configuration for Databricks SQL Warehouse connection behavior.
///
/// Controls concurrency, retry limits, and permanent error handling to
/// prevent thundering herd issues.
#[derive(Debug, Clone, Copy)]
pub struct SqlWarehouseConfig {
    /// Maximum number of concurrent HTTP requests to the SQL Warehouse API.
    pub max_concurrent_requests: usize,

    /// Maximum number of HTTP-level retries for transient failures (e.g. 429, 5xx).
    pub http_max_retries: usize,

    /// Backoff strategy for transient HTTP retries (fibonacci or exponential).
    pub backoff_method: BackoffMethod,

    /// Maximum number of poll retries when waiting for async statement completion.
    pub statement_max_retries: usize,

    /// When true, non-retryable errors (401, 403, 404) permanently disable
    /// the connector so subsequent queries fail immediately without issuing
    /// further HTTP requests.
    pub disable_on_permanent_error: bool,

    /// Timeout for establishing TCP/TLS connections to the SQL Warehouse API.
    pub connect_timeout: std::time::Duration,

    /// Per-request wall-clock timeout for every HTTP call (statement submit,
    /// status poll, and result-chunk fetch). Must be set to the longest
    /// expected single HTTP call, not the total query duration — the overall
    /// query duration is bounded by `statement_max_retries` × backoff.
    pub request_timeout: std::time::Duration,
}

impl Default for SqlWarehouseConfig {
    fn default() -> Self {
        Self {
            max_concurrent_requests: SQL_WAREHOUSE_MAX_IN_FLIGHT_REQUESTS,
            http_max_retries: SQL_WAREHOUSE_DEFAULT_HTTP_MAX_RETRIES,
            backoff_method: BackoffMethod::Fibonacci,
            statement_max_retries: SQL_WAREHOUSE_DEFAULT_STATEMENT_MAX_RETRIES,
            disable_on_permanent_error: true,
            connect_timeout: DEFAULT_HTTP_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_HTTP_REQUEST_TIMEOUT,
        }
    }
}

/// Shared observable metrics for the Databricks SQL Warehouse connector.
///
/// All fields are atomics so they can be read by an external
/// [`MetricsProvider`] without holding any lock.
#[derive(Debug, Default)]
pub struct DatabricksMetrics {
    // -- Request metrics --
    /// Total logical operations initiated (each `execute_sql_statement`,
    /// `get_sql_statement_status`, or `fetch_chunk_data` call counts as one).
    /// Retries within an operation are tracked separately by `retries_total`.
    pub requests_total: AtomicU64,
    /// Total HTTP retries performed across all operations.
    pub retries_total: AtomicU64,
    /// Total non-retryable (permanent) errors detected.
    pub permanent_errors_total: AtomicU64,
    /// Current number of HTTP requests holding a concurrency permit (gauge).
    /// Bounded by `max_concurrent_requests`.
    pub inflight_operations: AtomicU64,

    // -- Statement metrics --
    /// Total SQL statements that entered execution.
    pub statements_executed_total: AtomicU64,
    /// Total polls made when waiting for async statement completion.
    pub statement_polls_total: AtomicU64,
    /// Total SQL statements that completed with FAILED status.
    pub statements_failed_total: AtomicU64,

    // -- Connection pool metrics --
    /// Total virtual pool `connect()` calls (each returns a lightweight handle).
    pub pool_connections_total: AtomicU64,
    /// Current number of active connections (handles not yet dropped).
    pub pool_active_connections: AtomicU64,

    // -- Concurrency metrics --
    /// Reference to the concurrency semaphore for observing available permits.
    /// Set once during construction; if `None`, concurrency metrics are unavailable.
    pub semaphore: Option<Arc<Semaphore>>,

    // -- Data transfer metrics --
    /// Total Arrow result chunks fetched from external links.
    pub chunks_fetched_total: AtomicU64,

    // -- Connector state --
    /// Whether the connector has been permanently disabled (1 = disabled, 0 = active).
    pub permanently_disabled: Arc<AtomicBool>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "This operation is not supported by the Databricks SQL Warehouse connector. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    NotImplemented,

    #[snafu(display(
        "Failed to initialize the Databricks SQL Warehouse HTTP client: {}",
        format_reqwest_error_chain(source)
    ))]
    ClientBuildFailed { source: reqwest::Error },

    #[snafu(display(
        "Unsupported Databricks data type '{ty}'. For details, visit: https://spiceai.org/docs/components/data-connectors/databricks"
    ))]
    UnsupportedType { ty: String },

    #[snafu(display(
        "The table '{dataset_name}' has no column metadata registered in Unity Catalog. Run 'SELECT * FROM {dataset_name} LIMIT 1' in Databricks SQL to populate the schema, then retry. For details, visit: https://spiceai.org/docs/components/data-connectors/databricks"
    ))]
    TableSchemaNotRegistered { dataset_name: String },

    #[snafu(display(
        "Failed to infer schema for dataset '{dataset_name}' (databricks): unexpected schema response format. Verify the table exists and is accessible, or report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnexpectedSchemaResponse {
        dataset_name: String,
        reason: String,
    },

    #[snafu(display(
        "The dataset '{dataset_name}' in Databricks has no columns. Verify the table exists and has at least one column."
    ))]
    NoColumnsInDataset { dataset_name: String },

    #[snafu(display(
        "Warehouse is not ready (state: '{state}'). Verify the warehouse state and try again later."
    ))]
    InvalidWarehouseState { state: String },

    #[snafu(display(
        "Databricks SQL Warehouse returned an unexpected statement state: '{state}'. Verify the warehouse is operational, or report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnexpectedStatementState { state: String },

    #[snafu(display(
        "The Databricks SQL query was canceled. This may be due to a timeout or manual cancellation. Verify the warehouse is running and try again."
    ))]
    QueryCanceled,

    #[snafu(display(
        "The Databricks SQL query is still running and exceeded the maximum poll attempts. Increase `databricks_max_statement_retries` or simplify the query."
    ))]
    QueryStillRunning,

    #[snafu(display(
        "Connector is permanently disabled due to a previous non-retryable error. Verify credentials and warehouse configuration, then restart."
    ))]
    PermanentlyDisabled,

    #[snafu(display(
        "Invalid Databricks SQL Warehouse configuration: max_concurrent_requests must be >= 1, got {limit}"
    ))]
    InvalidConcurrencyLimit { limit: usize },

    #[snafu(display(
        "Conflicting Databricks SQL Warehouse concurrency limits for endpoint '{endpoint}' and warehouse '{warehouse_id}': requested {requested}, existing {existing}. Use the same max_concurrent_requests value for all datasets and catalogs that share this warehouse."
    ))]
    ConflictingConcurrencyLimit {
        endpoint: String,
        warehouse_id: String,
        requested: usize,
        existing: usize,
    },

    #[snafu(display(
        "Failed to send request to Databricks SQL Warehouse: {}",
        format_reqwest_error_chain(source)
    ))]
    HttpRequestFailed { source: reqwest::Error },

    #[snafu(display("Failed to acquire Databricks SQL Warehouse rate-control permit: {source}"))]
    RateControl { source: runtime_rate_control::Error },

    #[snafu(display(
        "Failed to parse response from Databricks SQL Warehouse: {}",
        format_reqwest_error_chain(source)
    ))]
    JsonParsingFailed { source: reqwest::Error },

    #[snafu(display(
        "Databricks SQL Warehouse response is missing expected field '{field}'. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    MissingJsonField { field: String },

    #[snafu(display(
        "Databricks SQL Warehouse response contains an invalid array for field '{field}'. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    InvalidJsonArray { field: String },

    #[snafu(display("Failed to deserialize Databricks SQL Warehouse result link: {source}"))]
    DeserializeExternalLinkFailed { source: serde_json::Error },

    #[snafu(display("Failed to read Arrow data from Databricks SQL Warehouse: {source}"))]
    ArrowStreamReadFailed { source: arrow::error::ArrowError },

    #[snafu(display(
        "Failed to load the dataset (databricks): {}",
        format_datafusion_error(source)
    ))]
    TableProviderCreationFailed { source: DataFusionError },

    #[snafu(display("Failed to initialize the dataset (databricks): {source}"))]
    SqlTableInitializationFailed {
        source: datafusion_table_providers::sql::sql_provider_datafusion::Error,
    },

    #[snafu(display(
        "A fully-qualified table path (catalog.schema.table) is required for Databricks: {reason}. For details, visit: https://spiceai.org/docs/components/data-connectors/databricks"
    ))]
    FullyQualifiedPath { reason: String },

    #[snafu(display("Failed to parse Databricks datatype: {reason}"))]
    ParseError { reason: String },

    #[snafu(display(
        "Failed to execute the query. {message} Verify the query is valid, or report a bug at: https://github.com/spiceai/spiceai/issues"
    ))]
    QueryFailure { message: String },

    #[snafu(display(
        "The dataset '{dataset_name}' appears to be a Lakehouse Federation foreign table, \
         which is not supported on Classic SQL warehouses. \
         Switch `databricks_sql_warehouse_id` to a Pro or Serverless warehouse. \
         Databricks error: {message}"
    ))]
    ForeignTableOnClassicWarehouse {
        dataset_name: String,
        message: String,
    },

    #[snafu(display(
        "The dataset '{dataset_name}' returned `UNSUPPORTED_DATA_SOURCE` from Databricks SQL Warehouse, \
         and Spice could not determine whether the configured warehouse can query it ({warehouse_lookup_error}). \
         Failing safely rather than returning a schema that may not be queryable. \
         If this dataset is a Lakehouse Federation foreign table, switch `databricks_sql_warehouse_id` to a Pro or Serverless warehouse. \
         Databricks error: {message}"
    ))]
    UnsupportedDataSource {
        dataset_name: String,
        message: String,
        warehouse_lookup_error: String,
    },
}

fn format_reqwest_error_chain(error: &reqwest::Error) -> String {
    let mut message = error.to_string();
    let mut current = StdError::source(error);

    while let Some(source) = current {
        let source_message = source.to_string();
        if !source_message.is_empty() {
            let _ = std::fmt::Write::write_fmt(
                &mut message,
                format_args!("; caused by: {source_message}"),
            );
        }
        current = StdError::source(source);
    }

    message
}

fn databricks_server_message(error: &Error) -> String {
    match error {
        Error::QueryFailure { message } => message.clone(),
        _ => error.to_string(),
    }
}

/// Databricks SQL warehouse compute type. Lakehouse Federation foreign
/// tables are only supported on `Pro` and `Serverless` warehouses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WarehouseType {
    Classic,
    Pro,
    Serverless,
    Unknown,
}

/// Main struct for interacting with Databricks SQL Warehouse
pub struct DatabricksSqlWarehouse {
    pool: Arc<dyn DbConnectionPool<Arc<SqlWarehouseApi>, &'static dyn Sync> + Send + Sync>,
    metrics: Arc<DatabricksMetrics>,
}

impl DatabricksSqlWarehouse {
    /// Creates a new Databricks SQL Warehouse instance with default configuration.
    pub fn new(
        endpoint: &str,
        sql_warehouse_id: &str,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Result<Self, Error> {
        Self::with_config(
            endpoint,
            sql_warehouse_id,
            token_provider,
            SqlWarehouseConfig::default(),
        )
    }

    /// Creates a new Databricks SQL Warehouse instance with explicit configuration.
    pub fn with_config(
        endpoint: &str,
        sql_warehouse_id: &str,
        token_provider: Arc<dyn TokenProvider>,
        config: SqlWarehouseConfig,
    ) -> Result<Self, Error> {
        Self::with_config_and_semaphore(endpoint, sql_warehouse_id, token_provider, config, None)
    }

    /// Creates a new Databricks SQL Warehouse instance with explicit configuration
    /// and a shared concurrency semaphore.
    ///
    /// When `shared_semaphore` is `Some`, the instance uses the provided semaphore
    /// for concurrency limiting instead of creating its own. This ensures a global
    /// concurrency limit across all datasets that share the same semaphore.
    pub fn with_config_and_semaphore(
        endpoint: &str,
        sql_warehouse_id: &str,
        token_provider: Arc<dyn TokenProvider>,
        config: SqlWarehouseConfig,
        shared_semaphore: Option<Arc<Semaphore>>,
    ) -> Result<Self, Error> {
        Self::with_config_semaphore_and_permissions(
            endpoint,
            sql_warehouse_id,
            token_provider,
            config,
            shared_semaphore,
            Arc::new(NoPermissionsCheck),
        )
    }

    /// Creates a new Databricks SQL Warehouse instance with explicit configuration,
    /// a shared concurrency semaphore, and a dataset permissions checker.
    pub fn with_config_semaphore_and_permissions(
        endpoint: &str,
        sql_warehouse_id: &str,
        token_provider: Arc<dyn TokenProvider>,
        config: SqlWarehouseConfig,
        shared_semaphore: Option<Arc<Semaphore>>,
        permissions: Arc<dyn DatasetPermissions>,
    ) -> Result<Self, Error> {
        Self::with_config_semaphore_permissions_and_rate_controller(
            endpoint,
            sql_warehouse_id,
            token_provider,
            config,
            shared_semaphore,
            permissions,
            None,
        )
    }

    /// Creates a new Databricks SQL Warehouse instance with explicit configuration,
    /// a shared concurrency semaphore, a dataset permissions checker, and an
    /// optional shared HTTP rate controller.
    pub fn with_config_semaphore_permissions_and_rate_controller(
        endpoint: &str,
        sql_warehouse_id: &str,
        token_provider: Arc<dyn TokenProvider>,
        config: SqlWarehouseConfig,
        shared_semaphore: Option<Arc<Semaphore>>,
        permissions: Arc<dyn DatasetPermissions>,
        rate_controller: Option<Arc<RateController>>,
    ) -> Result<Self, Error> {
        ensure!(
            config.max_concurrent_requests > 0,
            InvalidConcurrencyLimitSnafu {
                limit: config.max_concurrent_requests,
            }
        );

        let (request_semaphore, metrics) = if let Some(sem) = shared_semaphore {
            let key = (endpoint.to_string(), sql_warehouse_id.to_string());
            let mut registry = SHARED_SQL_WAREHOUSE_METRICS
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let metrics = Arc::clone(registry.entry(key).or_insert_with(|| {
                Arc::new(DatabricksMetrics {
                    semaphore: Some(Arc::clone(&sem)),
                    ..DatabricksMetrics::default()
                })
            }));
            (sem, metrics)
        } else {
            let sem = Arc::new(Semaphore::new(config.max_concurrent_requests));
            let metrics = Arc::new(DatabricksMetrics {
                semaphore: Some(Arc::clone(&sem)),
                ..DatabricksMetrics::default()
            });
            (sem, metrics)
        };
        let api = Arc::new(SqlWarehouseApi::new(
            endpoint,
            sql_warehouse_id,
            token_provider,
            &config,
            Arc::clone(&metrics),
            request_semaphore,
            rate_controller,
        )?);
        let pool = Arc::new(SqlWarehouseConnectionPool {
            api,
            metrics: Arc::clone(&metrics),
            permissions,
        });
        Ok(Self { pool, metrics })
    }

    /// Returns the shared metrics for this SQL Warehouse instance.
    #[must_use]
    pub fn metrics(&self) -> &Arc<DatabricksMetrics> {
        &self.metrics
    }
}

pub fn shared_request_semaphore(
    endpoint: &str,
    sql_warehouse_id: &str,
    max_concurrent_requests: usize,
) -> Result<Arc<Semaphore>, Error> {
    ensure!(
        max_concurrent_requests > 0,
        InvalidConcurrencyLimitSnafu {
            limit: max_concurrent_requests,
        }
    );

    let mut registry = SHARED_SQL_WAREHOUSE_SEMAPHORES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let key = (endpoint.to_string(), sql_warehouse_id.to_string());

    match registry.entry(key) {
        Entry::Occupied(entry) => {
            let (semaphore, existing) = entry.get();
            ensure!(
                *existing == max_concurrent_requests,
                ConflictingConcurrencyLimitSnafu {
                    endpoint: endpoint.to_string(),
                    warehouse_id: sql_warehouse_id.to_string(),
                    requested: max_concurrent_requests,
                    existing: *existing,
                }
            );
            Ok(Arc::clone(semaphore))
        }
        Entry::Vacant(entry) => {
            let semaphore = Arc::new(Semaphore::new(max_concurrent_requests));
            entry.insert((Arc::clone(&semaphore), max_concurrent_requests));
            Ok(semaphore)
        }
    }
}

struct SqlWarehouseConnectionPool {
    api: Arc<SqlWarehouseApi>,
    metrics: Arc<DatabricksMetrics>,
    permissions: Arc<dyn DatasetPermissions>,
}

#[async_trait]
impl DbConnectionPool<Arc<SqlWarehouseApi>, &'static dyn Sync> for SqlWarehouseConnectionPool {
    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<Arc<SqlWarehouseApi>, &'static dyn Sync>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        self.metrics
            .pool_connections_total
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .pool_active_connections
            .fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(SqlWarehouseConnection {
            api: Arc::clone(&self.api),
            metrics: Arc::clone(&self.metrics),
            permissions: Arc::clone(&self.permissions),
        }))
    }

    fn join_push_down(&self) -> JoinPushDown {
        JoinPushDown::AllowedFor(self.api.sql_warehouse_id.clone())
    }
}

// https://docs.databricks.com/api/workspace/statementexecution/executestatement#status-error
// states: Enum: PENDING | RUNNING | SUCCEEDED | FAILED | CANCELED | CLOSED
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ResponseStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
    Closed,
}

impl Display for ResponseStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseStatus::Pending => write!(f, "PENDING"),
            ResponseStatus::Running => write!(f, "RUNNING"),
            ResponseStatus::Succeeded => write!(f, "SUCCEEDED"),
            ResponseStatus::Failed => write!(f, "FAILED"),
            ResponseStatus::Canceled => write!(f, "CANCELED"),
            ResponseStatus::Closed => write!(f, "CLOSED"),
        }
    }
}

impl FromStr for ResponseStatus {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "SUCCEEDED" => Ok(Self::Succeeded),
            "FAILED" => Ok(Self::Failed),
            // waiting for warehouse or async query
            "PENDING" => Ok(Self::Pending),
            "RUNNING" => Ok(Self::Running),
            "CANCELED" => Ok(Self::Canceled),
            "CLOSED" => Ok(Self::Closed),
            other => Err(Error::UnexpectedStatementState {
                state: other.to_string(),
            }),
        }
    }
}

struct SqlWarehouseApi {
    client: Client,
    base_url: String,
    request_semaphore: Arc<Semaphore>,
    http_max_retries: usize,
    backoff_method: BackoffMethod,
    statement_max_retries: usize,
    disable_on_permanent_error: bool,
    sql_warehouse_id: String,
    token_provider: Arc<dyn TokenProvider>,
    metrics: Arc<DatabricksMetrics>,
    rate_controller: Option<Arc<RateController>>,
}

impl SqlWarehouseApi {
    fn new(
        host: &str,
        sql_warehouse_id: &str,
        token_provider: Arc<dyn TokenProvider>,
        config: &SqlWarehouseConfig,
        metrics: Arc<DatabricksMetrics>,
        request_semaphore: Arc<Semaphore>,
        rate_controller: Option<Arc<RateController>>,
    ) -> Result<Self, Error> {
        let client = configure_client_builder_with_timeouts(
            ClientBuilder::new(),
            config.connect_timeout,
            config.request_timeout,
        )
        .user_agent(super::user_agent())
        .build()
        .context(ClientBuildFailedSnafu)?;

        Ok(Self {
            client,
            base_url: format!("https://{host}"),
            request_semaphore,
            http_max_retries: config.http_max_retries,
            backoff_method: config.backoff_method,
            statement_max_retries: config.statement_max_retries,
            disable_on_permanent_error: config.disable_on_permanent_error,
            sql_warehouse_id: sql_warehouse_id.to_string(),
            token_provider,
            metrics,
            rate_controller,
        })
    }

    fn retry_config(&self) -> RetryConfig<'_> {
        RetryConfig {
            concurrency_limit: Some(&self.request_semaphore),
            max_retries: Some(self.http_max_retries),
            backoff_method: Some(self.backoff_method),
            retry_counter: Some(&self.metrics.retries_total),
            inflight_counter: Some(&self.metrics.inflight_operations),
        }
    }

    async fn acquire_rate_controller_permit(
        &self,
    ) -> Result<Option<runtime_rate_control::Permit>, Error> {
        let Some(rate_controller) = &self.rate_controller else {
            return Ok(None);
        };

        rate_controller
            .acquire()
            .await
            .context(RateControlSnafu)
            .map(Some)
    }

    async fn get_schema(
        &self,
        table: &TableReference,
        permissions: &dyn DatasetPermissions,
    ) -> Result<SchemaRef, Error> {
        let table_name = table.to_string();

        async {
            let result = discover_schema(
                &table_name,
                self.probe_information_schema(table),
                self.probe_describe_table(table),
                permissions,
            )
            .await
            .map_err(|e| match e.downcast::<Error>() {
                // Preserve the specific `ForeignTableOnClassicWarehouse`
                // error (and any other typed Databricks error) when it
                // bubbles up — wrapping loses the actionable diagnosis.
                Ok(err) => *err,
                Err(other) => Error::QueryFailure {
                    message: other.to_string(),
                },
            })?;

            result.log_warnings(table);
            let token = self.token_provider.get_token();
            Ok(self
                .enrich_schema_with_partition_columns(&token, table, result.schema)
                .await)
        }
        .instrument(tracing::info_span!(
            target: "task_history",
            "databricks_get_schema",
            input = %table_name,
            warehouse_id = %self.sql_warehouse_id,
        ))
        .await
    }

    /// Probes `information_schema.columns` for the table schema.
    ///
    /// Returns [`SchemaProbeResult::Ok`] on success, [`SchemaProbeResult::AccessDenied`]
    /// if the query fails with a permission error, or [`SchemaProbeResult::Failed`] for
    /// non-permission errors (e.g. `UNSUPPORTED_DATA_SOURCE`, missing metadata).
    async fn probe_information_schema(&self, table: &TableReference) -> SchemaProbeResult {
        let token = self.token_provider.get_token();
        match self.get_schema_from_information_schema(&token, table).await {
            Ok(schema) => SchemaProbeResult::Ok(schema),
            Err(e) if is_access_denied_error(&e) => SchemaProbeResult::AccessDenied(e.to_string()),
            Err(e) if is_unsupported_data_source_error(&e) => {
                self.classify_unsupported_data_source(&token, table, e)
                    .await
            }
            Err(e) => SchemaProbeResult::Failed(Box::new(e)),
        }
    }

    /// Resolves an `UNSUPPORTED_DATA_SOURCE` schema-probe error into either a
    /// specific `ForeignTableOnClassicWarehouse` diagnosis, a safe permanent
    /// `UnsupportedDataSource` error when the warehouse type cannot be
    /// determined, or a generic `Failed` result when the warehouse is known
    /// to be non-Classic and the `DESCRIBE TABLE` fallback remains viable.
    ///
    /// On Classic warehouses this returns [`SchemaProbeResult::Permanent`] so
    /// `discover_schema` surfaces the actionable error immediately without
    /// falling back to `DESCRIBE TABLE` metadata that would never be usable
    /// at query time.
    async fn classify_unsupported_data_source(
        &self,
        token: &str,
        table: &TableReference,
        original: Error,
    ) -> SchemaProbeResult {
        let dataset_name = table.to_string();
        let message = databricks_server_message(&original);

        match self.get_warehouse_type(token).await {
            Ok(WarehouseType::Classic) => {
                SchemaProbeResult::Permanent(Box::new(Error::ForeignTableOnClassicWarehouse {
                    dataset_name,
                    message,
                }))
            }
            Ok(WarehouseType::Pro | WarehouseType::Serverless) => {
                SchemaProbeResult::Failed(Box::new(original))
            }
            Ok(WarehouseType::Unknown) => {
                tracing::warn!(
                    table = %table,
                    "Databricks returned an unknown warehouse type while diagnosing UNSUPPORTED_DATA_SOURCE; failing safely"
                );
                SchemaProbeResult::Permanent(Box::new(Error::UnsupportedDataSource {
                    dataset_name,
                    message,
                    warehouse_lookup_error: "Databricks returned an unknown warehouse type"
                        .to_string(),
                }))
            }
            Err(lookup_err) => {
                tracing::warn!(
                    table = %table,
                    "Failed to query warehouse type to diagnose UNSUPPORTED_DATA_SOURCE: {lookup_err}"
                );
                SchemaProbeResult::Permanent(Box::new(Error::UnsupportedDataSource {
                    dataset_name,
                    message,
                    warehouse_lookup_error: lookup_err.to_string(),
                }))
            }
        }
    }

    /// Probes `DESCRIBE TABLE` for the table schema.
    ///
    /// Returns [`SchemaProbeResult::Ok`] on success, [`SchemaProbeResult::AccessDenied`]
    /// if the query fails with a permission error, or [`SchemaProbeResult::Failed`] for
    /// other errors.
    async fn probe_describe_table(&self, table: &TableReference) -> SchemaProbeResult {
        let token = self.token_provider.get_token();
        let payload = match self.create_describe_payload(table) {
            Ok(p) => p,
            Err(e) => return SchemaProbeResult::Failed(Box::new(e)),
        };
        let response = match self.execute_sql_statement(&token, &payload).await {
            Ok(r) => r,
            Err(e) if is_access_denied_error(&e) => {
                return SchemaProbeResult::AccessDenied(e.to_string());
            }
            Err(e) if is_unsupported_data_source_error(&e) => {
                return self
                    .classify_unsupported_data_source(&token, table, e)
                    .await;
            }
            Err(e) => return SchemaProbeResult::Failed(Box::new(e)),
        };
        let response = match self.wait_for_statement_completion(&token, response).await {
            Ok(r) => r,
            Err(e) if is_access_denied_error(&e) => {
                return SchemaProbeResult::AccessDenied(e.to_string());
            }
            Err(e) if is_unsupported_data_source_error(&e) => {
                return self
                    .classify_unsupported_data_source(&token, table, e)
                    .await;
            }
            Err(e) => return SchemaProbeResult::Failed(Box::new(e)),
        };
        match schema_from_describe_json(&response, &table.to_string()) {
            Ok(schema) => SchemaProbeResult::Ok(schema),
            Err(e) if is_access_denied_error(&e) => SchemaProbeResult::AccessDenied(e.to_string()),
            Err(e) if is_unsupported_data_source_error(&e) => {
                self.classify_unsupported_data_source(&token, table, e)
                    .await
            }
            Err(e) => SchemaProbeResult::Failed(Box::new(e)),
        }
    }

    /// Attempts to read the schema from `information_schema.columns`,
    /// trying `full_data_type` first and falling back to `data_type` if
    /// the column does not exist.
    async fn get_schema_from_information_schema(
        &self,
        token: &str,
        table: &TableReference,
    ) -> Result<SchemaRef, Error> {
        let payload = self.create_schema_payload(table, "full_data_type")?;
        let response = self.execute_sql_statement(token, &payload).await?;
        let response = self.wait_for_statement_completion(token, response).await?;

        match schema_from_json(&response, &table.to_string()) {
            Ok(schema) => Ok(schema),
            Err(Error::QueryFailure { ref message })
                if message.contains("UNRESOLVED_COLUMN") && message.contains("full_data_type") =>
            {
                tracing::warn!(
                    table = %table,
                    "Databricks information_schema does not have 'full_data_type' column, falling back to 'data_type'. Complex types (ARRAY, MAP, STRUCT) may lose inner type details."
                );
                let payload = self.create_schema_payload(table, "data_type")?;
                let response = self.execute_sql_statement(token, &payload).await?;
                let response = self.wait_for_statement_completion(token, response).await?;
                schema_from_json(&response, &table.to_string())
            }
            Err(e) => Err(e),
        }
    }

    fn create_schema_payload(
        &self,
        table: &TableReference,
        data_type_column: &str,
    ) -> Result<Value, Error> {
        let table_schema = table.schema().ok_or_else(|| Error::FullyQualifiedPath {
            reason: "missing schema".into(),
        })?;
        let table_catalog = table.catalog().ok_or_else(|| Error::FullyQualifiedPath {
            reason: "missing catalog".into(),
        })?;
        // Escape single quotes by doubling them to prevent SQL injection
        let escaped_table = table.table().replace('\'', "''");
        let escaped_schema = table_schema.replace('\'', "''");
        let escaped_catalog = table_catalog.replace('\'', "''");
        let sql = format!(
            "SELECT c.column_name, c.{data_type_column}, c.is_nullable, c.comment, t.comment FROM information_schema.columns c LEFT JOIN information_schema.tables t ON c.table_catalog = t.table_catalog AND c.table_schema = t.table_schema AND c.table_name = t.table_name WHERE c.table_name = '{escaped_table}' AND c.table_schema = '{escaped_schema}' AND c.table_catalog = '{escaped_catalog}' ORDER BY c.ordinal_position"
        );
        // Databricks SQL Statements API max wait_timeout is 50s.
        // https://docs.databricks.com/api/workspace/statementexecution/executestatement
        Ok(json!({
            "warehouse_id": self.sql_warehouse_id,
            "catalog": table_catalog,
            "schema": table_schema,
            "statement": sql,
            "format": "JSON_ARRAY",
            "disposition": "INLINE",
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
        }))
    }

    async fn enrich_schema_with_partition_columns(
        &self,
        token: &str,
        table: &TableReference,
        schema: SchemaRef,
    ) -> SchemaRef {
        match self.get_partition_columns(token, table).await {
            Ok(partition_columns) => schema_with_partition_metadata(schema, &partition_columns),
            Err(error) => {
                tracing::warn!(
                    table = %table,
                    error = %error,
                    "Failed to query Databricks partition columns; registering without partition metadata"
                );
                schema
            }
        }
    }

    async fn get_partition_columns(
        &self,
        token: &str,
        table: &TableReference,
    ) -> Result<Vec<String>, Error> {
        let payload = self.create_describe_detail_payload(table)?;
        let response = self.execute_sql_statement(token, &payload).await?;
        let response = self.wait_for_statement_completion(token, response).await?;
        partition_columns_from_describe_detail_json(&response, &table.to_string())
    }

    fn create_describe_detail_payload(&self, table: &TableReference) -> Result<Value, Error> {
        let table_schema = table.schema().ok_or_else(|| Error::FullyQualifiedPath {
            reason: "missing schema".into(),
        })?;
        let table_catalog = table.catalog().ok_or_else(|| Error::FullyQualifiedPath {
            reason: "missing catalog".into(),
        })?;
        let sql = format!(
            "DESCRIBE DETAIL `{}`.`{}`.`{}`",
            table_catalog.replace('`', "``"),
            table_schema.replace('`', "``"),
            table.table().replace('`', "``"),
        );
        Ok(json!({
            "warehouse_id": self.sql_warehouse_id,
            "catalog": table_catalog,
            "schema": table_schema,
            "statement": sql,
            "format": "JSON_ARRAY",
            "disposition": "INLINE",
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
        }))
    }

    /// Builds a `DESCRIBE TABLE` payload for tables where
    /// `information_schema.columns` has no metadata (e.g. Lakehouse
    /// Federation foreign tables).
    fn create_describe_payload(&self, table: &TableReference) -> Result<Value, Error> {
        let table_schema = table.schema().ok_or_else(|| Error::FullyQualifiedPath {
            reason: "missing schema".into(),
        })?;
        let table_catalog = table.catalog().ok_or_else(|| Error::FullyQualifiedPath {
            reason: "missing catalog".into(),
        })?;
        // Use backtick quoting for DESCRIBE TABLE identifiers. Escape
        // embedded backticks by doubling them.
        let sql = format!(
            "DESCRIBE TABLE `{}`.`{}`.`{}`",
            table_catalog.replace('`', "``"),
            table_schema.replace('`', "``"),
            table.table().replace('`', "``"),
        );
        Ok(json!({
            "warehouse_id": self.sql_warehouse_id,
            "catalog": table_catalog,
            "schema": table_schema,
            "statement": sql,
            "format": "JSON_ARRAY",
            "disposition": "INLINE",
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
        }))
    }

    async fn execute_sql_statement(&self, token: &str, payload: &Value) -> Result<Value, Error> {
        self.check_permanently_disabled()?;
        let sql_text = payload
            .get("statement")
            .and_then(|v| v.as_str())
            .unwrap_or("<unknown>");
        self.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
        let url = format!("{}/api/2.0/sql/statements/", self.base_url);
        async {
            let rate_controller_permit = self.acquire_rate_controller_permit().await?;
            let response = send_request_with_retry_and_concurrency_limit(
                "Databricks SQL Warehouse",
                "execute SQL statement",
                || self.client.post(&url).bearer_auth(token).json(payload),
                &self.retry_config(),
            )
            .await
            .context(HttpRequestFailedSnafu)?;
            let value: Value = self
                .check_permanent_http_error(response)
                .error_for_status()
                .context(HttpRequestFailedSnafu)?
                .json()
                .await
                .context(JsonParsingFailedSnafu)?;
            self.metrics
                .statements_executed_total
                .fetch_add(1, Ordering::Relaxed);
            drop(rate_controller_permit);
            Ok(value)
        }
        .instrument(tracing::info_span!(
            target: "task_history",
            "databricks_execute_statement",
            input = sql_text,
            warehouse_id = %self.sql_warehouse_id,
        ))
        .await
    }

    /// Queries the Databricks REST API for this warehouse's type.
    ///
    /// Uses `GET /api/2.0/sql/warehouses/{id}`. The response contains
    /// `warehouse_type` (`"CLASSIC"` or `"PRO"`) and
    /// `enable_serverless_compute` (bool). Serverless is a variant of `PRO`.
    async fn get_warehouse_type(&self, token: &str) -> Result<WarehouseType, Error> {
        self.check_permanently_disabled()?;
        let url = format!(
            "{}/api/2.0/sql/warehouses/{}",
            self.base_url, self.sql_warehouse_id
        );
        let rate_controller_permit = self.acquire_rate_controller_permit().await?;
        let response = send_request_with_retry_and_concurrency_limit(
            "Databricks SQL Warehouse",
            "get warehouse details",
            || self.client.get(&url).bearer_auth(token),
            &self.retry_config(),
        )
        .await
        .context(HttpRequestFailedSnafu)?;
        let value: Value = self
            .check_permanent_http_error(response)
            .error_for_status()
            .context(HttpRequestFailedSnafu)?
            .json()
            .await
            .context(JsonParsingFailedSnafu)?;
        drop(rate_controller_permit);

        let warehouse_type = value
            .get("warehouse_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        let serverless = value
            .get("enable_serverless_compute")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        Ok(match (warehouse_type, serverless) {
            ("PRO", true) => WarehouseType::Serverless,
            ("PRO", false) => WarehouseType::Pro,
            ("CLASSIC", _) => WarehouseType::Classic,
            _ => WarehouseType::Unknown,
        })
    }

    async fn get_sql_statement_status(
        &self,
        token: &str,
        statement_id: &str,
    ) -> Result<Value, Error> {
        self.check_permanently_disabled()?;
        self.metrics
            .statement_polls_total
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
        let url = format!("{}/api/2.0/sql/statements/{statement_id}", self.base_url);
        let rate_controller_permit = self.acquire_rate_controller_permit().await?;
        let response = send_request_with_retry_and_concurrency_limit(
            "Databricks SQL Warehouse",
            "poll SQL statement status",
            || self.client.get(&url).bearer_auth(token),
            &self.retry_config(),
        )
        .await
        .context(HttpRequestFailedSnafu)?;
        let value = response
            .error_for_status()
            .context(HttpRequestFailedSnafu)?
            .json()
            .await
            .context(JsonParsingFailedSnafu)?;
        drop(rate_controller_permit);
        Ok(value)
    }

    // Fetch the arrow data at the external links, repeating for each chunk
    async fn fetch_external_links(
        self: Arc<Self>,
        result_object: Value,
    ) -> Result<SendableRecordBatchStream, Error> {
        let token = self.token_provider.get_token();
        let initial_external_link = Self::extract_external_links(result_object)?;

        // If no external link, return an empty stream
        if initial_external_link.is_none() {
            let empty_stream: Pin<
                Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>,
            > = Box::pin(stream::empty::<Result<RecordBatch, DataFusionError>>());
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                empty_stream,
            )) as SendableRecordBatchStream);
        }

        let token = token.clone();
        let stream = stream::unfold(initial_external_link, move |current_link| {
            let api = Arc::clone(&self);
            let token = token.clone();
            async move {
                let link = current_link?;

                tracing::trace!(
                    "Fetching chunk {} from {}",
                    link.chunk_index,
                    link.external_link
                );

                let bytes = match api.fetch_chunk_data(&link.external_link).await {
                    Ok(bytes) => bytes,
                    Err(e) => return Some((Err(e), None)),
                };

                let batches = match Self::read_arrow_batches(bytes) {
                    Ok(batches) => batches,
                    Err(e) => return Some((Err(e), None)),
                };

                let next_link = match link.next_chunk_internal_link {
                    Some(path) => {
                        let url = format!("{}{path}", api.base_url);

                        if let Err(e) = api.check_permanently_disabled() {
                            return Some((Err(e), None));
                        }
                        api.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
                        let rate_controller_permit =
                            match api.acquire_rate_controller_permit().await {
                                Ok(permit) => permit,
                                Err(e) => return Some((Err(e), None)),
                            };
                        let resp = match send_request_with_retry_and_concurrency_limit(
                            "Databricks SQL Warehouse",
                            "fetch next external chunk link",
                            || api.client.get(&url).bearer_auth(&token),
                            &api.retry_config(),
                        )
                        .await
                        .context(HttpRequestFailedSnafu)
                        {
                            Ok(resp) => {
                                drop(rate_controller_permit);
                                resp
                            }
                            Err(e) => {
                                return Some((Err(e), None));
                            }
                        };

                        match resp.error_for_status().context(HttpRequestFailedSnafu) {
                            Ok(response) => match response
                                .json()
                                .await
                                .context(JsonParsingFailedSnafu)
                                .and_then(Self::extract_external_links)
                            {
                                Ok(next) => next,
                                Err(e) => {
                                    return Some((Err(e), None));
                                }
                            },
                            Err(e) => {
                                return Some((Err(e), None));
                            }
                        }
                    }
                    None => None,
                };

                Some((Ok(batches), next_link))
            }
        });

        // Flatten the stream of Vec<RecordBatch> into individual RecordBatch items
        let batch_stream = stream.flat_map(|result| match result {
            Ok(batches) => Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
                as Pin<Box<dyn Stream<Item = Result<RecordBatch, Error>> + Send>>,
            Err(e) => Box::pin(futures::stream::iter(vec![Err(e)]))
                as Pin<Box<dyn Stream<Item = Result<RecordBatch, Error>> + Send>>,
        });

        // Handle the first batch to extract schema
        let mut batch_stream = batch_stream.boxed();
        let first_batch: RecordBatch = match batch_stream.next().await {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => return Err(e),
            None => {
                let empty_stream: Pin<
                    Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>,
                > = Box::pin(stream::empty::<Result<RecordBatch, DataFusionError>>());
                return Ok(Box::pin(RecordBatchStreamAdapter::new(
                    Arc::new(Schema::empty()),
                    empty_stream,
                )) as SendableRecordBatchStream);
            }
        };

        let schema = first_batch.schema();
        let run_once = stream::once(async move { Ok(first_batch) });

        let stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>> =
            Box::pin(
                run_once
                    .chain(batch_stream)
                    .map_err(|e| DataFusionError::Execution(e.to_string())),
            );

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)) as SendableRecordBatchStream)
    }

    /// Deserializes the first [`ExternalLink`] in the `external_links` array, or None if missing or empty
    fn extract_external_links(mut response: Value) -> Result<Option<ExternalLink>, Error> {
        let Some(links) = response.get_mut("external_links").map(Value::take) else {
            return Ok(None);
        };

        let Value::Array(mut links) = links else {
            return Err(Error::InvalidJsonArray {
                field: "external_links".into(),
            });
        };

        // Return None if the array is empty
        let Some(link) = links.pop() else {
            return Ok(None);
        };

        serde_json::from_value(link)
            .context(DeserializeExternalLinkFailedSnafu)
            .map(Some)
    }

    async fn fetch_chunk_data(&self, url: &str) -> Result<bytes::Bytes, Error> {
        self.check_permanently_disabled()?;
        self.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
        let rate_controller_permit = self.acquire_rate_controller_permit().await?;
        let result = send_request_with_retry_and_concurrency_limit(
            "Databricks SQL Warehouse",
            "fetch statement result chunk",
            || self.client.get(url),
            &self.retry_config(),
        )
        .await
        .context(HttpRequestFailedSnafu)?
        // Skip permanent-error detection for external chunk URLs (pre-signed
        // storage links). A 403/404 here typically means an expired link,
        // not broken credentials/warehouse configuration.
        .error_for_status()
        .context(HttpRequestFailedSnafu)?
        .bytes()
        .await
        .context(HttpRequestFailedSnafu);
        drop(rate_controller_permit);
        if result.is_ok() {
            self.metrics
                .chunks_fetched_total
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn read_arrow_batches(
        bytes: bytes::Bytes,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, Error> {
        let cursor = Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None).context(ArrowStreamReadFailedSnafu)?;
        Ok(reader
            .collect::<Result<Vec<_>, _>>()
            .context(ArrowStreamReadFailedSnafu)?
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect())
    }

    fn extract_response_status(response: &Value) -> Result<ResponseStatus, Error> {
        let state = response
            .get("status")
            .and_then(|s| s.get("state"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::MissingJsonField {
                field: "status.state".to_string(),
            })?;
        ResponseStatus::from_str(state)
    }

    fn extract_statement_id(response: &Value) -> Result<String, Error> {
        response
            .get("statement_id")
            .and_then(|v| v.as_str())
            .map(ToString::to_string)
            .ok_or_else(|| Error::MissingJsonField {
                field: "statement_id".to_string(),
            })
    }

    /// This is an async query running on the Databricks SQL Warehouse
    fn is_async_query(state: ResponseStatus) -> bool {
        matches!(state, ResponseStatus::Pending | ResponseStatus::Running)
    }

    fn verify_response_status(response: &Value) -> Result<(), Error> {
        let state = Self::extract_response_status(response)?;

        match state {
            ResponseStatus::Succeeded => Ok(()),
            ResponseStatus::Failed => {
                let message = Self::extract_error_message(response)
                    .unwrap_or_else(|| "Unknown error".to_string());
                Err(Error::QueryFailure {
                    message: format!("Query failed with state FAILED: {message}"),
                })
            }
            // waiting for warehouse
            ResponseStatus::Pending => Err(Error::InvalidWarehouseState {
                state: state.to_string(),
            }),
            // long-running queries are not currently supported
            ResponseStatus::Running => Err(Error::QueryStillRunning),
            ResponseStatus::Canceled => Err(Error::QueryCanceled),
            ResponseStatus::Closed => Err(Error::QueryFailure {
                message: "Query failed with state CLOSED".to_string(),
            }),
        }
    }

    /// Polls the statement status until it reaches a terminal state, with bounded retries.
    async fn wait_for_statement_completion(
        &self,
        token: &str,
        mut response: Value,
    ) -> Result<Value, Error> {
        let mut state = Self::extract_response_status(&response)?;
        let statement_id = Self::extract_statement_id(&response)?;

        let max_retries = self.statement_max_retries;
        let mut backoff = FibonacciBackoffBuilder::new()
            .max_retries(Some(max_retries))
            .build();

        let span = tracing::info_span!(
            target: "task_history",
            "databricks_poll_statement",
            input = %statement_id,
            warehouse_id = %self.sql_warehouse_id,
        );

        async {
            while Self::is_async_query(state) {
                let Some(backoff_duration) = backoff.next_backoff() else {
                    break;
                };
                tokio::time::sleep(backoff_duration).await;
                response = self.get_sql_statement_status(token, &statement_id).await?;
                state = Self::extract_response_status(&response)?;
            }

            match state {
                ResponseStatus::Pending => Err(Error::InvalidWarehouseState {
                    state: state.to_string(),
                }),
                ResponseStatus::Running => Err(Error::QueryStillRunning),
                _ => Ok(response),
            }
        }
        .instrument(span)
        .await
    }

    fn extract_error_message(response: &Value) -> Option<String> {
        response
            .get("status")
            .and_then(|s| s.get("error"))
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
            .map(ToString::to_string)
    }

    /// Returns `Err(PermanentlyDisabled)` if the connector has been marked disabled.
    fn check_permanently_disabled(&self) -> Result<(), Error> {
        if self.metrics.permanently_disabled.load(Ordering::Relaxed) {
            return Err(Error::PermanentlyDisabled);
        }
        Ok(())
    }

    /// Inspects the HTTP response status for non-retryable errors (401, 403, 404).
    /// If `disable_on_permanent_error` is enabled, marks the connector as permanently
    /// disabled so that future requests fail fast without issuing HTTP calls.
    ///
    /// Returns the response unchanged on success so it can be chained.
    fn check_permanent_http_error(&self, response: reqwest::Response) -> reqwest::Response {
        let status = response.status();
        if self.disable_on_permanent_error && is_permanent_http_status(status) {
            tracing::error!(
                status = %status,
                warehouse_id = %self.sql_warehouse_id,
                "Databricks SQL Warehouse returned a non-retryable HTTP status; disabling connector to prevent further requests"
            );
            self.metrics
                .permanently_disabled
                .store(true, Ordering::Relaxed);
            self.metrics
                .permanent_errors_total
                .fetch_add(1, Ordering::Relaxed);
        }
        response
    }
}

/// HTTP status codes that indicate a non-retryable configuration or
/// authentication problem.  These should not be retried because
/// repeating the same request will produce the same failure.
fn is_permanent_http_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 401 | 403 | 404)
}

/// Returns `true` if the error indicates a SQL-level table permission denial.
///
/// Only matches Databricks SQL query failures that explicitly report
/// permission errors on a specific table. HTTP 403 from the SQL Statements
/// API is NOT matched here — that's an infrastructure auth error (bad token,
/// no warehouse access) and is handled by `check_permanent_http_error`.
fn is_access_denied_error(err: &Error) -> bool {
    match err {
        Error::QueryFailure { message } => {
            message.contains("INSUFFICIENT_PERMISSIONS")
                || message.contains("ACCESS_DENIED")
                || message.contains("PERMISSION_DENIED")
                || message.contains("does not have")
                || message.contains("permission denied")
        }
        _ => false,
    }
}

/// Returns `true` if the error indicates the table is backed by a data
/// source unsupported by the SQL warehouse. This is the signature of a
/// Lakehouse Federation foreign table queried from a Classic warehouse.
fn is_unsupported_data_source_error(err: &Error) -> bool {
    match err {
        Error::QueryFailure { message } => message.contains("UNSUPPORTED_DATA_SOURCE"),
        _ => false,
    }
}

/// Databricks-specific permissions check using the Unity Catalog
/// effective-permissions API.
///
/// For foreign tables (where UC permissions are not authoritative),
/// it always returns [`PermissionCheckResult::Allowed`].
pub struct DatabricksPermissions {
    uc_client: Arc<crate::unity_catalog::UnityCatalog>,
    /// When `true`, an explicit denial from UC is treated as authoritative.
    /// Foreign tables set this to `false`.
    requires_strict_validation: bool,
}

impl DatabricksPermissions {
    /// Creates a new permissions checker.
    ///
    /// `requires_strict_validation` should be `false` for foreign tables where
    /// UC permissions are not authoritative.
    #[must_use]
    pub fn new(
        uc_client: Arc<crate::unity_catalog::UnityCatalog>,
        requires_strict_validation: bool,
    ) -> Self {
        Self {
            uc_client,
            requires_strict_validation,
        }
    }
}

#[async_trait]
impl DatasetPermissions for DatabricksPermissions {
    async fn check_read_permission(&self, table_name: &str) -> PermissionCheckResult {
        if !self.requires_strict_validation {
            return PermissionCheckResult::Allowed;
        }
        match self.uc_client.get_effective_permissions(table_name).await {
            Ok(Some(perms)) if !perms.has_read_permission() => {
                tracing::debug!(
                    table_name,
                    principals = ?perms.principals(),
                    privileges = ?perms.all_privileges(),
                    "Unity Catalog denied read permission"
                );
                PermissionCheckResult::Denied {
                    reason: format!("Unity Catalog reports no read privilege for '{table_name}'"),
                }
            }
            Ok(Some(_)) => PermissionCheckResult::Allowed,
            Ok(None) => PermissionCheckResult::Unavailable {
                reason: format!("Table '{table_name}' not found in UC permissions API"),
            },
            Err(e) => PermissionCheckResult::Unavailable {
                reason: format!("UC permissions API error: {e}"),
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct ExternalLink {
    chunk_index: u64,
    #[expect(clippy::struct_field_names)]
    external_link: String,
    next_chunk_internal_link: Option<String>,
}

fn schema_from_json(json_value: &Value, dataset_name: &str) -> Result<SchemaRef, Error> {
    tracing::trace!("Parsing schema definition from Databricks JSON response: {json_value}");

    SqlWarehouseApi::verify_response_status(json_value)?;

    let result = json_value
        .get("result")
        .ok_or_else(|| Error::UnexpectedSchemaResponse {
            dataset_name: dataset_name.to_string(),
            reason: "missing result object in response".to_string(),
        })?;

    let data_array_value = result.get("data_array");

    let data_array = match data_array_value {
        None | Some(serde_json::Value::Null) => {
            return Err(Error::TableSchemaNotRegistered {
                dataset_name: dataset_name.to_string(),
            });
        }
        Some(v) => v
            .as_array()
            .ok_or_else(|| Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: "result.data_array is not an array".to_string(),
            })?,
    };

    if data_array.is_empty() {
        return Err(Error::NoColumnsInDataset {
            dataset_name: dataset_name.to_string(),
        });
    }

    let mut fields = Vec::new();
    let mut schema_metadata = HashMap::new();

    for (i, row) in data_array.iter().enumerate() {
        let row_array = row
            .as_array()
            .ok_or_else(|| Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}] is not an array"),
            })?;

        if row_array.len() < 3 {
            return Err(Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}] has fewer than 3 fields"),
            });
        }

        let col_name = row_array[0]
            .as_str()
            .ok_or_else(|| Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}][0] (column name) is not a string"),
            })?;

        // If we see #, DB is now providing the clustering metadata info and
        // we have all the columns we need
        if col_name.starts_with('#') {
            break;
        }

        let data_type_str =
            row_array[1]
                .as_str()
                .ok_or_else(|| Error::UnexpectedSchemaResponse {
                    dataset_name: dataset_name.to_string(),
                    reason: format!("data_array[{i}][1] (data type) is not a string"),
                })?;

        let data_type = datatypes::Parser::new(data_type_str)
            .parse()
            .map_err(|reason| Error::ParseError { reason })?;

        let nullable = row_array[2]
            .as_str()
            .map(|s| s.to_lowercase() == "yes")
            .ok_or_else(|| Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}][2] (is_nullable) is not a string"),
            })?;

        if let Some(table_comment) = optional_string(row_array.get(4)) {
            schema_metadata
                .entry(DESCRIPTION_METADATA_KEY.to_string())
                .or_insert_with(|| table_comment.to_string());
        }

        let field = field_with_optional_metadata(
            Field::new(col_name, data_type, nullable),
            optional_string(row_array.get(3)),
            Some(data_type_str),
        );

        fields.push(field);
    }

    if fields.is_empty() {
        return Err(Error::NoColumnsInDataset {
            dataset_name: dataset_name.to_string(),
        });
    }

    Ok(Arc::new(Schema::new_with_metadata(fields, schema_metadata)))
}

/// Parses a schema from a `DESCRIBE TABLE` response.
///
/// `DESCRIBE TABLE` returns rows of `[col_name, data_type, comment]`.
/// Since it does not include nullability information, all columns default
/// to nullable. Blank separator rows are skipped, and metadata rows
/// (starting with `#`) stop parsing.
fn schema_from_describe_json(json_value: &Value, dataset_name: &str) -> Result<SchemaRef, Error> {
    tracing::trace!("Parsing schema from DESCRIBE TABLE response: {json_value}");

    SqlWarehouseApi::verify_response_status(json_value)?;

    let result = json_value
        .get("result")
        .ok_or_else(|| Error::UnexpectedSchemaResponse {
            dataset_name: dataset_name.to_string(),
            reason: "missing result object in DESCRIBE TABLE response".to_string(),
        })?;

    let data_array = result
        .get("data_array")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::UnexpectedSchemaResponse {
            dataset_name: dataset_name.to_string(),
            reason: "missing or invalid data_array in DESCRIBE TABLE response".to_string(),
        })?;

    let mut fields = Vec::new();

    for (i, row) in data_array.iter().enumerate() {
        let row_array = row
            .as_array()
            .ok_or_else(|| Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}] is not an array"),
            })?;

        if row_array.len() < 2 {
            return Err(Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}] has fewer than 2 fields"),
            });
        }

        let col_name = row_array[0]
            .as_str()
            .ok_or_else(|| Error::UnexpectedSchemaResponse {
                dataset_name: dataset_name.to_string(),
                reason: format!("data_array[{i}][0] (column name) is not a string"),
            })?;

        // Metadata rows start with #; stop parsing here.
        if col_name.starts_with('#') {
            break;
        }

        // Skip blank separator rows between columns and metadata.
        if col_name.trim().is_empty() {
            continue;
        }

        let data_type_str =
            row_array[1]
                .as_str()
                .ok_or_else(|| Error::UnexpectedSchemaResponse {
                    dataset_name: dataset_name.to_string(),
                    reason: format!("data_array[{i}][1] (data type) is not a string"),
                })?;

        let data_type = datatypes::Parser::new(data_type_str)
            .parse()
            .map_err(|reason| Error::ParseError { reason })?;

        // DESCRIBE TABLE does not report nullability; default to nullable.
        fields.push(field_with_optional_metadata(
            Field::new(col_name, data_type, true),
            optional_string(row_array.get(2)),
            Some(data_type_str),
        ));
    }

    if fields.is_empty() {
        return Err(Error::NoColumnsInDataset {
            dataset_name: dataset_name.to_string(),
        });
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn optional_string(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn field_with_optional_metadata(
    field: Field,
    comment: Option<&str>,
    source_type: Option<&str>,
) -> Field {
    let mut metadata = HashMap::new();
    if let Some(comment) = comment {
        metadata.insert(DESCRIPTION_METADATA_KEY.to_string(), comment.to_string());
    }
    if let Some(source_type) = source_type.map(str::trim).filter(|value| !value.is_empty()) {
        metadata.insert(
            SOURCE_TYPE_METADATA_KEY.to_string(),
            source_type.to_string(),
        );
    }

    if metadata.is_empty() {
        return field;
    }

    field.with_metadata(metadata)
}

fn schema_with_partition_metadata(schema: SchemaRef, partition_columns: &[String]) -> SchemaRef {
    if partition_columns.is_empty() {
        return schema;
    }

    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if partition_columns
                .iter()
                .any(|partition_column| partition_column == field.name())
            {
                let mut metadata = field.metadata().clone();
                metadata.insert(PARTITION_METADATA_KEY.to_string(), "true".to_string());
                Arc::new(field.as_ref().clone().with_metadata(metadata))
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();

    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn partition_columns_from_describe_detail_json(
    json_value: &Value,
    dataset_name: &str,
) -> Result<Vec<String>, Error> {
    SqlWarehouseApi::verify_response_status(json_value)?;

    let result = json_value
        .get("result")
        .ok_or_else(|| Error::UnexpectedSchemaResponse {
            dataset_name: dataset_name.to_string(),
            reason: "missing result object in DESCRIBE DETAIL response".to_string(),
        })?;

    let data_array = result
        .get("data_array")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::UnexpectedSchemaResponse {
            dataset_name: dataset_name.to_string(),
            reason: "missing or invalid data_array in DESCRIBE DETAIL response".to_string(),
        })?;

    let Some(row) = data_array.first() else {
        return Ok(Vec::new());
    };

    let row_array = row
        .as_array()
        .ok_or_else(|| Error::UnexpectedSchemaResponse {
            dataset_name: dataset_name.to_string(),
            reason: "DESCRIBE DETAIL row is not an array".to_string(),
        })?;

    let Some(partition_columns) = row_array.get(7) else {
        return Ok(Vec::new());
    };

    partition_column_names(partition_columns).ok_or_else(|| Error::UnexpectedSchemaResponse {
        dataset_name: dataset_name.to_string(),
        reason: "DESCRIBE DETAIL partitionColumns is not a string array".to_string(),
    })
}

fn partition_column_names(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::Array(values) => Some(
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .collect(),
        ),
        Value::String(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Some(Vec::new());
            }
            serde_json::from_str::<Vec<String>>(trimmed)
                .ok()
                .or_else(|| {
                    Some(
                        trimmed
                            .trim_matches(['[', ']'])
                            .split(',')
                            .map(|value| value.trim().trim_matches('"'))
                            .filter(|value| !value.is_empty())
                            .map(ToString::to_string)
                            .collect(),
                    )
                })
        }
        Value::Null => Some(Vec::new()),
        _ => None,
    }
}

struct SqlWarehouseConnection {
    api: Arc<SqlWarehouseApi>,
    metrics: Arc<DatabricksMetrics>,
    permissions: Arc<dyn DatasetPermissions>,
}

impl Drop for SqlWarehouseConnection {
    fn drop(&mut self) {
        self.metrics
            .pool_active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }
}

impl<'a> DbConnection<Arc<SqlWarehouseApi>, &'a dyn Sync> for SqlWarehouseConnection {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Arc<SqlWarehouseApi>, &'a dyn Sync>> {
        Some(self)
    }
}

#[async_trait]
impl<'a> AsyncDbConnection<Arc<SqlWarehouseApi>, &'a dyn Sync> for SqlWarehouseConnection {
    fn new(api: Arc<SqlWarehouseApi>) -> Self {
        let metrics = Arc::clone(&api.metrics);
        Self {
            api,
            metrics,
            permissions: Arc::new(NoPermissionsCheck),
        }
    }

    async fn tables(&self, _schema: &str) -> Result<Vec<String>, dbconnection::Error> {
        Err(dbconnection::Error::UnableToGetTables {
            source: "Databricks tables() not implemented".into(),
        })
    }

    async fn schemas(&self) -> Result<Vec<String>, dbconnection::Error> {
        let query = "SELECT schema_name FROM information_schema.schemata";

        let token = self.api.token_provider.get_token();
        let payload = json!({
            "warehouse_id": self.api.sql_warehouse_id,
            "format": "ARROW_STREAM",
            "disposition": "EXTERNAL_LINKS",
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
            "statement": query,
        });

        let response = self
            .api
            .execute_sql_statement(&token, &payload)
            .await
            .map_err(|e| dbconnection::Error::UnableToGetSchemas {
                source: Box::new(e),
            })?;

        SqlWarehouseApi::verify_response_status(&response).map_err(|e| {
            dbconnection::Error::UnableToGetSchemas {
                source: Box::new(e),
            }
        })?;

        let mut stream = Arc::clone(&self.api)
            .fetch_external_links(response)
            .await
            .map_err(|e| dbconnection::Error::UnableToGetSchemas {
                source: Box::new(e),
            })?;

        let mut schemas = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| dbconnection::Error::UnableToGetSchemas {
                source: Box::new(e),
            })?;

            if let Some(name_column) = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
            {
                for value in name_column.iter().flatten() {
                    schemas.push(value.to_string());
                }
            }
        }

        Ok(schemas)
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, dbconnection::Error> {
        self.api
            .get_schema(table_reference, self.permissions.as_ref())
            .await
            .map_err(|source| dbconnection::Error::UnableToGetSchema {
                source: Box::new(source),
            })
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _: &[&'a dyn Sync],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let token = self.api.token_provider.get_token();
        let payload = json!({
            "warehouse_id": self.api.sql_warehouse_id,
            "format": "ARROW_STREAM",
            "disposition": "EXTERNAL_LINKS",
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
            "statement": sql,
        });

        let response = self.api.execute_sql_statement(&token, &payload).await?;

        tracing::trace!("Parsing Databricks JSON response: {response}");

        let mut response = self
            .api
            .wait_for_statement_completion(&token, response)
            .await?;

        if let Err(e) = SqlWarehouseApi::verify_response_status(&response) {
            self.metrics
                .statements_failed_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(e.into());
        }

        let result_object = response.get_mut("result").map(Value::take).ok_or_else(|| {
            MissingJsonFieldSnafu {
                field: "result".to_string(),
            }
            .build()
        })?;

        Ok(SqlWarehouseApi::fetch_external_links(Arc::clone(&self.api), result_object).await?)
    }

    async fn execute(
        &self,
        _query: &str,
        _: &[&'a dyn Sync],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        Ok(NotImplementedSnafu.fail()?)
    }
}

fn databricks_dialect() -> super::dialect::DatabricksDialect {
    super::dialect::DatabricksDialect::new()
}

#[async_trait]
impl crate::Read for DatabricksSqlWarehouse {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let dialect = Arc::new(databricks_dialect());

        let table_provider = Arc::new(
            SqlTable::new("databricks", &self.pool, table_reference)
                .await
                .context(SqlTableInitializationFailedSnafu)?
                .with_dialect(dialect),
        );

        Ok(Arc::new(
            table_provider
                .create_federated_table_provider()
                .context(TableProviderCreationFailedSnafu)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use serde_json::json;

    /// Helper to create a valid Databricks schema response JSON.
    fn make_schema_response(data_array: &Value) -> Value {
        json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "test-stmt-id",
            "result": { "data_array": data_array }
        })
    }

    #[test]
    fn test_schema_from_json_basic() {
        let response = make_schema_response(&json!([
            ["id", "int", "NO"],
            ["name", "string", "YES"],
            ["amount", "double", "NO"]
        ]));

        let schema = schema_from_json(&response, "test_table").expect("should parse schema");
        assert_eq!(schema.fields().len(), 3);

        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(!schema.field(0).is_nullable());

        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(schema.field(1).is_nullable());

        assert_eq!(schema.field(2).name(), "amount");
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert!(!schema.field(2).is_nullable());
    }

    #[test]
    fn test_schema_from_json_preserves_comment_metadata() {
        let response = make_schema_response(&json!([
            ["id", "int", "NO", "stable identifier", "customer dimension"],
            [
                "name",
                "string",
                "YES",
                "display name",
                "customer dimension"
            ],
            ["amount", "double", "NO", "", "customer dimension"]
        ]));

        let schema = schema_from_json(&response, "test_table").expect("should parse schema");

        assert_eq!(
            schema
                .metadata()
                .get(DESCRIPTION_METADATA_KEY)
                .map(String::as_str),
            Some("customer dimension")
        );
        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get(DESCRIPTION_METADATA_KEY)
                .map(String::as_str),
            Some("stable identifier")
        );
        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get(SOURCE_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some("int")
        );
        assert_eq!(
            schema
                .field(1)
                .metadata()
                .get(DESCRIPTION_METADATA_KEY)
                .map(String::as_str),
            Some("display name")
        );
        assert_eq!(
            schema
                .field(1)
                .metadata()
                .get(SOURCE_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some("string")
        );
        assert!(
            schema
                .field(2)
                .metadata()
                .get(DESCRIPTION_METADATA_KEY)
                .is_none()
        );
        assert_eq!(
            schema
                .field(2)
                .metadata()
                .get(SOURCE_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some("double")
        );
    }

    #[test]
    fn test_schema_from_json_many_types() {
        let response = make_schema_response(&json!([
            ["col_bigint", "bigint", "NO"],
            ["col_smallint", "smallint", "YES"],
            ["col_boolean", "boolean", "NO"],
            ["col_float", "float", "YES"],
            ["col_date", "date", "NO"],
            ["col_timestamp", "timestamp", "YES"],
            ["col_binary", "binary", "NO"],
            ["col_decimal", "decimal(10,2)", "YES"]
        ]));

        let schema = schema_from_json(&response, "test_table").expect("should parse schema");
        assert_eq!(schema.fields().len(), 8);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Int16);
        assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(3).data_type(), &DataType::Float32);
        assert_eq!(schema.field(4).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(5).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(6).data_type(), &DataType::Binary);
        assert_eq!(schema.field(7).data_type(), &DataType::Decimal128(10, 2));
    }

    #[test]
    fn test_partition_columns_from_describe_detail_metadata() {
        let response = make_schema_response(&json!([[
            "delta",
            "table-id",
            "customers",
            null,
            "s3://bucket/customers",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
            ["event_date", "region"]
        ]]));

        let partition_columns = partition_columns_from_describe_detail_json(&response, "customers")
            .expect("should parse partition columns");

        assert_eq!(partition_columns, vec!["event_date", "region"]);
    }

    #[test]
    fn test_schema_with_partition_metadata_marks_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_date", DataType::Date32, true),
            Field::new("value", DataType::Int64, true),
        ]));

        let schema = schema_with_partition_metadata(schema, &["event_date".to_string()]);

        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get(PARTITION_METADATA_KEY)
                .map(String::as_str),
            Some("true")
        );
        assert!(
            schema
                .field(1)
                .metadata()
                .get(PARTITION_METADATA_KEY)
                .is_none()
        );
    }

    #[test]
    fn test_schema_from_json_empty_table() {
        let response = make_schema_response(&json!([]));

        let err = schema_from_json(&response, "my_catalog.my_schema.my_table")
            .expect_err("should fail on empty schema");
        assert!(
            matches!(&err, Error::NoColumnsInDataset { dataset_name } if dataset_name == "my_catalog.my_schema.my_table"),
            "unexpected error: {err}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("my_catalog.my_schema.my_table"),
            "error should contain dataset name: {msg}"
        );
        assert!(
            msg.contains("has no columns"),
            "error should mention no columns: {msg}"
        );
    }

    #[test]
    fn test_schema_from_json_stops_at_clustering_metadata() {
        let response = make_schema_response(&json!([
            ["id", "int", "NO"],
            ["name", "string", "YES"],
            ["# Clustering Information", "", ""],
            ["# col_name", "data_type", "comment"]
        ]));

        let schema =
            schema_from_json(&response, "test_table").expect("should stop at clustering marker");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[test]
    fn test_schema_from_json_missing_result() {
        let response = json!({
            "status": { "state": "SUCCEEDED" }
        });

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail without result");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("missing result")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_missing_data_array() {
        let response = json!({
            "status": { "state": "SUCCEEDED" },
            "result": {}
        });

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail without data_array");
        assert!(
            matches!(&err, Error::TableSchemaNotRegistered { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_data_array_not_array() {
        let response = json!({
            "status": { "state": "SUCCEEDED" },
            "result": { "data_array": "not_an_array" }
        });

        let err = schema_from_json(&response, "test_table")
            .expect_err("should fail when data_array is string");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("is not an array")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_empty_data_array() {
        let response = json!({
            "status": { "state": "SUCCEEDED" },
            "result": { "data_array": [] }
        });

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail on empty data_array");
        assert!(
            matches!(&err, Error::NoColumnsInDataset { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_row_not_array() {
        let response = make_schema_response(&json!(["not_an_array"]));

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail on non-array row");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("is not an array")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_row_too_short() {
        let response = make_schema_response(&json!([["id", "int"]]));

        let err = schema_from_json(&response, "test_table").expect_err("should fail on short row");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("fewer than 3 fields")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_column_name_not_string() {
        let response = make_schema_response(&json!([[123, "int", "NO"]]));

        let err = schema_from_json(&response, "test_table")
            .expect_err("should fail on non-string col name");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("[0] (column name) is not a string")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_data_type_not_string() {
        let response = make_schema_response(&json!([["id", 42, "NO"]]));

        let err = schema_from_json(&response, "test_table")
            .expect_err("should fail on non-string data type");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("[1] (data type) is not a string")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_nullable_not_string() {
        let response = make_schema_response(&json!([["id", "int", true]]));

        let err = schema_from_json(&response, "test_table")
            .expect_err("should fail on non-string nullable");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { reason, .. } if reason.contains("[2] (is_nullable) is not a string")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_nullable_case_insensitive() {
        let response = make_schema_response(&json!([
            ["a", "int", "YES"],
            ["b", "int", "Yes"],
            ["c", "int", "yes"],
            ["d", "int", "NO"],
            ["e", "int", "No"],
            ["f", "int", "no"],
            ["g", "int", "anything_else"]
        ]));

        let schema =
            schema_from_json(&response, "test_table").expect("should parse nullable variations");
        assert!(schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        assert!(schema.field(2).is_nullable());
        assert!(!schema.field(3).is_nullable());
        assert!(!schema.field(4).is_nullable());
        assert!(!schema.field(5).is_nullable());
        assert!(!schema.field(6).is_nullable());
    }

    #[test]
    fn test_schema_from_json_failed_status() {
        let response = json!({
            "status": {
                "state": "FAILED",
                "error": { "message": "table not found" }
            },
            "statement_id": "test-stmt-id",
            "result": { "data_array": [] }
        });

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail on FAILED status");
        assert!(
            matches!(&err, Error::QueryFailure { message } if message.contains("table not found")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_pending_status() {
        let response = json!({
            "status": { "state": "PENDING" },
            "statement_id": "test-stmt-id"
        });

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail on PENDING status");
        assert!(
            matches!(&err, Error::InvalidWarehouseState { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_unsupported_type() {
        let response = make_schema_response(&json!([["col", "TOTALLY_FAKE_TYPE", "NO"]]));

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail on unsupported type");
        assert!(
            matches!(&err, Error::ParseError { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_extra_columns_ignored() {
        // Rows with more than 3 elements should still work (extra fields ignored)
        let response =
            make_schema_response(&json!([["id", "int", "NO", "extra_col", "another_extra"]]));

        let schema =
            schema_from_json(&response, "test_table").expect("should parse with extra columns");
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[test]
    fn test_schema_from_json_missing_status() {
        let response = json!({
            "result": { "data_array": [["id", "int", "NO"]] }
        });

        let err =
            schema_from_json(&response, "test_table").expect_err("should fail without status");
        assert!(
            matches!(&err, Error::MissingJsonField { field } if field == "status.state"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_create_schema_payload_includes_inline_disposition() {
        let api = SqlWarehouseApi::new(
            "host.example.com",
            "warehouse-123",
            Arc::new(StaticTokenProvider("token".to_string())),
            &SqlWarehouseConfig::default(),
            Arc::new(DatabricksMetrics::default()),
            Arc::new(Semaphore::new(8)),
            None,
        )
        .expect("should create api");
        let table = TableReference::full("my_catalog", "my_schema", "my_table");

        let payload = api
            .create_schema_payload(&table, "full_data_type")
            .expect("should create payload");

        assert_eq!(payload["format"], "JSON_ARRAY");
        assert_eq!(payload["disposition"], "INLINE");
        assert_eq!(payload["wait_timeout"], "50s");
        assert_eq!(payload["on_wait_timeout"], "CONTINUE");
        assert_eq!(payload["warehouse_id"], "warehouse-123");
        assert!(
            payload["statement"]
                .as_str()
                .expect("statement should be string")
                .contains("my_table"),
            "statement should reference the table"
        );
    }

    #[test]
    fn test_create_schema_payload_missing_schema() {
        let api = SqlWarehouseApi::new(
            "host.example.com",
            "wh-1",
            Arc::new(StaticTokenProvider("t".to_string())),
            &SqlWarehouseConfig::default(),
            Arc::new(DatabricksMetrics::default()),
            Arc::new(Semaphore::new(8)),
            None,
        )
        .expect("should create api");
        let table = TableReference::bare("just_table");

        let err = api
            .create_schema_payload(&table, "full_data_type")
            .expect_err("should fail without schema");
        assert!(
            matches!(&err, Error::FullyQualifiedPath { reason } if reason.contains("missing schema")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_create_schema_payload_missing_catalog() {
        let api = SqlWarehouseApi::new(
            "host.example.com",
            "wh-1",
            Arc::new(StaticTokenProvider("t".to_string())),
            &SqlWarehouseConfig::default(),
            Arc::new(DatabricksMetrics::default()),
            Arc::new(Semaphore::new(8)),
            None,
        )
        .expect("should create api");
        let table = TableReference::partial("my_schema", "my_table");

        let err = api
            .create_schema_payload(&table, "full_data_type")
            .expect_err("should fail without catalog");
        assert!(
            matches!(&err, Error::FullyQualifiedPath { reason } if reason.contains("missing catalog")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_create_schema_payload_sql_injection_prevention() {
        let api = SqlWarehouseApi::new(
            "host.example.com",
            "wh-1",
            Arc::new(StaticTokenProvider("t".to_string())),
            &SqlWarehouseConfig::default(),
            Arc::new(DatabricksMetrics::default()),
            Arc::new(Semaphore::new(8)),
            None,
        )
        .expect("should create api");
        let table = TableReference::full("cat'alog", "sch'ema", "tab'le");

        let payload = api
            .create_schema_payload(&table, "full_data_type")
            .expect("should create payload");
        let stmt = payload["statement"]
            .as_str()
            .expect("statement should be string");

        // Single quotes should be escaped as double single-quotes
        assert!(stmt.contains("tab''le"), "table name not escaped: {stmt}");
        assert!(stmt.contains("sch''ema"), "schema not escaped: {stmt}");
        assert!(stmt.contains("cat''alog"), "catalog not escaped: {stmt}");
        // Should NOT contain unescaped single quotes between the SQL string quotes
        assert!(
            !stmt.contains("tab'le"),
            "unescaped table name found: {stmt}"
        );
    }

    /// Simple test [`TokenProvider`] for unit tests.
    #[derive(Debug)]
    struct StaticTokenProvider(String);

    impl TokenProvider for StaticTokenProvider {
        fn get_token(&self) -> String {
            self.0.clone()
        }

        fn dyn_hash(&self) -> String {
            self.0.clone()
        }
    }

    /// Starts a mock HTTP server that serves JSON responses in order.
    /// Once the queue is exhausted, `default_response` is returned for all subsequent requests.
    async fn start_mock_server(responses: Vec<Value>, default_response: Value) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to a port");
        let port = listener
            .local_addr()
            .expect("should have an address")
            .port();
        let responses = Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::from(
            responses,
        )));
        let default = Arc::new(default_response);

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let responses = Arc::clone(&responses);
                let default = Arc::clone(&default);
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = vec![0u8; 4096];
                    let _ = stream.read(&mut buf).await;

                    let response_json = {
                        let mut q = responses.lock().await;
                        q.pop_front().unwrap_or_else(|| (*default).clone())
                    };

                    let body =
                        serde_json::to_string(&response_json).expect("should serialize response");
                    let http_response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(http_response.as_bytes()).await;
                });
            }
        });

        port
    }

    #[derive(Clone)]
    struct MockHttpResponse {
        status_line: &'static str,
        headers: Vec<(String, String)>,
        body: String,
    }

    impl MockHttpResponse {
        fn json(status_line: &'static str, body: impl Into<String>) -> Self {
            Self {
                status_line,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: body.into(),
            }
        }
    }

    async fn start_mock_http_server(
        responses: Vec<MockHttpResponse>,
        default_response: MockHttpResponse,
    ) -> (u16, Arc<std::sync::atomic::AtomicUsize>) {
        use std::{
            collections::VecDeque,
            sync::atomic::{AtomicUsize, Ordering},
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to a port");
        let port = listener
            .local_addr()
            .expect("should have an address")
            .port();
        let responses = Arc::new(tokio::sync::Mutex::new(VecDeque::from(responses)));
        let default = Arc::new(default_response);
        let requests = Arc::new(AtomicUsize::new(0));

        let requests_for_server = Arc::clone(&requests);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let responses = Arc::clone(&responses);
                let default = Arc::clone(&default);
                let requests = Arc::clone(&requests_for_server);
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};

                    let mut buf = vec![0u8; 4096];
                    let _ = stream.read(&mut buf).await;
                    requests.fetch_add(1, Ordering::SeqCst);

                    let response = {
                        let mut q = responses.lock().await;
                        q.pop_front().unwrap_or_else(|| (*default).clone())
                    };

                    let mut http_response = format!(
                        "HTTP/1.1 {}\r\nContent-Length: {}\r\n",
                        response.status_line,
                        response.body.len()
                    );
                    for (header_name, header_value) in response.headers {
                        let _ = std::fmt::Write::write_fmt(
                            &mut http_response,
                            format_args!("{header_name}: {header_value}\r\n"),
                        );
                    }
                    http_response.push_str("\r\n");
                    http_response.push_str(&response.body);

                    let _ = stream.write_all(http_response.as_bytes()).await;
                });
            }
        });

        (port, requests)
    }

    fn create_test_api(port: u16) -> SqlWarehouseApi {
        create_test_api_with_request_limit(port, SQL_WAREHOUSE_MAX_IN_FLIGHT_REQUESTS)
    }

    fn create_test_api_with_request_limit(
        port: u16,
        max_in_flight_requests: usize,
    ) -> SqlWarehouseApi {
        // Build without connect/request timeouts so that `start_paused = true`
        // tests don't race with tokio's auto-advance clock.
        let client = ClientBuilder::new().build().expect("should build client");
        SqlWarehouseApi {
            client,
            base_url: format!("http://127.0.0.1:{port}"),
            request_semaphore: Arc::new(Semaphore::new(max_in_flight_requests)),
            http_max_retries: SQL_WAREHOUSE_DEFAULT_HTTP_MAX_RETRIES,
            backoff_method: BackoffMethod::Fibonacci,
            statement_max_retries: SQL_WAREHOUSE_DEFAULT_STATEMENT_MAX_RETRIES,
            disable_on_permanent_error: true,
            sql_warehouse_id: "test-warehouse".to_string(),
            token_provider: Arc::new(StaticTokenProvider("test-token".to_string())),
            metrics: Arc::new(DatabricksMetrics::default()),
            rate_controller: None,
        }
    }

    async fn start_blocking_mock_http_server(
        response: MockHttpResponse,
    ) -> (
        u16,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::sync::mpsc::UnboundedReceiver<()>,
        Arc<Semaphore>,
    ) {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to a port");
        let port = listener
            .local_addr()
            .expect("should have an address")
            .port();
        let response = Arc::new(response);
        let max_active_requests = Arc::new(AtomicUsize::new(0));
        let active_requests = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(Semaphore::new(0));

        let max_active_requests_for_server = Arc::clone(&max_active_requests);
        let active_requests_for_server = Arc::clone(&active_requests);
        let gate_for_server = Arc::clone(&gate);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let response = Arc::clone(&response);
                let started_tx = started_tx.clone();
                let max_active_requests = Arc::clone(&max_active_requests_for_server);
                let active_requests = Arc::clone(&active_requests_for_server);
                let gate = Arc::clone(&gate_for_server);
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};

                    let mut buf = vec![0u8; 4096];
                    let _ = stream.read(&mut buf).await;

                    let active = active_requests.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active_requests.fetch_max(active, Ordering::SeqCst);
                    let _ = started_tx.send(());

                    let permit = gate.acquire().await.expect("gate should remain open");
                    drop(permit);

                    let mut http_response = format!(
                        "HTTP/1.1 {}\r\nContent-Length: {}\r\n",
                        response.status_line,
                        response.body.len()
                    );
                    for (header_name, header_value) in &response.headers {
                        let _ = std::fmt::Write::write_fmt(
                            &mut http_response,
                            format_args!("{header_name}: {header_value}\r\n"),
                        );
                    }
                    http_response.push_str("\r\n");
                    http_response.push_str(&response.body);

                    let _ = stream.write_all(http_response.as_bytes()).await;
                    active_requests.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });

        (port, max_active_requests, started_rx, gate)
    }

    fn pending_response() -> Value {
        json!({"status": {"state": "PENDING"}, "statement_id": "stmt-1"})
    }

    fn running_response() -> Value {
        json!({"status": {"state": "RUNNING"}, "statement_id": "stmt-1"})
    }

    fn succeeded_response() -> Value {
        json!({"status": {"state": "SUCCEEDED"}, "statement_id": "stmt-1", "result": {"data_array": []}})
    }

    fn failed_response() -> Value {
        json!({"status": {"state": "FAILED"}, "statement_id": "stmt-1", "status": {"state": "FAILED", "error": {"message": "table not found"}}})
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_immediate_success() {
        let port = start_mock_server(vec![], json!({})).await;
        let api = create_test_api(port);

        let result = api
            .wait_for_statement_completion("token", succeeded_response())
            .await;
        let response = result.expect("SUCCEEDED should return Ok");
        assert_eq!(response["status"]["state"], "SUCCEEDED");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_immediate_failed() {
        let port = start_mock_server(vec![], json!({})).await;
        let api = create_test_api(port);

        let result = api
            .wait_for_statement_completion("token", failed_response())
            .await;
        // FAILED is a terminal state — wait_for_statement_completion returns it as Ok
        let response = result.expect("FAILED should return Ok (terminal state)");
        assert_eq!(response["status"]["state"], "FAILED");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_immediate_canceled() {
        let port = start_mock_server(vec![], json!({})).await;
        let api = create_test_api(port);
        let response = json!({"status": {"state": "CANCELED"}, "statement_id": "stmt-1"});

        let result = api.wait_for_statement_completion("token", response).await;
        let response = result.expect("CANCELED should return Ok (terminal state)");
        assert_eq!(response["status"]["state"], "CANCELED");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_pending_then_success() {
        let port = start_mock_server(vec![succeeded_response()], pending_response()).await;
        let api = create_test_api(port);

        let result = api
            .wait_for_statement_completion("token", pending_response())
            .await;
        let response = result.expect("should eventually succeed");
        assert_eq!(response["status"]["state"], "SUCCEEDED");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_running_then_success() {
        let port = start_mock_server(vec![succeeded_response()], running_response()).await;
        let api = create_test_api(port);

        let result = api
            .wait_for_statement_completion("token", running_response())
            .await;
        let response = result.expect("should eventually succeed");
        assert_eq!(response["status"]["state"], "SUCCEEDED");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_exhaustion_pending() {
        let port = start_mock_server(vec![], pending_response()).await;
        let api = create_test_api(port);

        let result = api
            .wait_for_statement_completion("token", pending_response())
            .await;
        let err = result.expect_err("should fail after exhausting retries");
        assert!(
            matches!(&err, Error::InvalidWarehouseState { state } if state == "PENDING"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_exhaustion_running() {
        let port = start_mock_server(vec![], running_response()).await;
        let api = create_test_api(port);

        let result = api
            .wait_for_statement_completion("token", running_response())
            .await;
        let err = result.expect_err("should fail after exhausting retries");
        assert!(
            matches!(&err, Error::QueryStillRunning),
            "unexpected error: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_missing_status() {
        let port = start_mock_server(vec![], json!({})).await;
        let api = create_test_api(port);
        let response = json!({"statement_id": "stmt-1"});

        let result = api.wait_for_statement_completion("token", response).await;
        let err = result.expect_err("should fail on missing status");
        assert!(
            matches!(&err, Error::MissingJsonField { field } if field == "status.state"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_wait_for_completion_missing_statement_id() {
        let port = start_mock_server(vec![], json!({})).await;
        let api = create_test_api(port);
        let response = json!({"status": {"state": "PENDING"}});

        let result = api.wait_for_statement_completion("token", response).await;
        let err = result.expect_err("should fail on missing statement_id");
        assert!(
            matches!(&err, Error::MissingJsonField { field } if field == "statement_id"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_no_columns_error_includes_dataset_name() {
        let response = make_schema_response(&json!([]));

        let err = schema_from_json(&response, "my_catalog.my_schema.orders")
            .expect_err("should fail when no columns");
        let msg = err.to_string();
        assert!(
            msg.contains("my_catalog.my_schema.orders"),
            "error should contain the full dataset name: {msg}"
        );
        assert!(
            msg.contains("has no columns"),
            "error should mention 'has no columns': {msg}"
        );
        assert!(
            msg.contains("Verify the table exists"),
            "error should suggest verifying table existence: {msg}"
        );
    }

    #[test]
    fn test_schema_from_json_only_clustering_metadata_returns_no_columns_error() {
        // When the data_array only contains clustering metadata markers,
        // no real columns are parsed and we should get a NoColumnsInDataset error.
        let response = make_schema_response(&json!([
            ["# Clustering Information", "", ""],
            ["# col_name", "data_type", "comment"]
        ]));

        let err = schema_from_json(&response, "test_table")
            .expect_err("should fail when only clustering metadata present");
        assert!(
            matches!(&err, Error::NoColumnsInDataset { dataset_name } if dataset_name == "test_table"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_schema_from_json_missing_result_error_is_actionable() {
        let response = json!({
            "status": { "state": "SUCCEEDED" }
        });

        let err = schema_from_json(&response, "catalog.schema.my_orders")
            .expect_err("should fail without result");
        let msg = err.to_string();
        assert!(
            msg.contains("catalog.schema.my_orders"),
            "error should contain dataset name: {msg}"
        );
        assert!(
            msg.contains("Verify the table exists"),
            "error should suggest verifying table: {msg}"
        );
    }

    #[test]
    fn test_schema_from_json_happy_path_with_dataset_name() {
        // Ensure the dataset_name parameter doesn't affect successful parsing.
        let response =
            make_schema_response(&json!([["id", "int", "NO"], ["name", "string", "YES"]]));

        let schema = schema_from_json(&response, "catalog.schema.users")
            .expect("should parse schema successfully");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[test]
    fn test_create_schema_payload_uses_data_type_column() {
        let api = SqlWarehouseApi::new(
            "host.example.com",
            "wh-1",
            Arc::new(StaticTokenProvider("t".to_string())),
            &SqlWarehouseConfig::default(),
            Arc::new(DatabricksMetrics::default()),
            Arc::new(Semaphore::new(8)),
            None,
        )
        .expect("should create api");
        let table = TableReference::full("my_catalog", "my_schema", "my_table");

        let payload_full = api
            .create_schema_payload(&table, "full_data_type")
            .expect("should create payload with full_data_type");
        let stmt_full = payload_full["statement"]
            .as_str()
            .expect("statement should be string");
        assert!(
            stmt_full.contains("full_data_type"),
            "SQL should reference full_data_type: {stmt_full}"
        );
        assert!(
            !stmt_full.contains(", c.data_type,"),
            "SQL should not reference plain data_type: {stmt_full}"
        );
        let payload_plain = api
            .create_schema_payload(&table, "data_type")
            .expect("should create payload with data_type");
        let stmt_plain = payload_plain["statement"]
            .as_str()
            .expect("statement should be string");
        assert!(
            stmt_plain.contains(", c.data_type,"),
            "SQL should reference data_type: {stmt_plain}"
        );
        assert!(
            !stmt_plain.contains("full_data_type"),
            "SQL should not reference full_data_type: {stmt_plain}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_execute_sql_statement_retries_rate_limited_response() {
        let success_body = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-1",
            "result": { "data_array": [] }
        })
        .to_string();

        let (port, requests) = start_mock_http_server(
            vec![
                MockHttpResponse {
                    status_line: "429 Too Many Requests",
                    headers: vec![
                        ("Content-Type".to_string(), "application/json".to_string()),
                        ("Retry-After".to_string(), "0".to_string()),
                    ],
                    body: json!({"error_code": "RATE_LIMITED"}).to_string(),
                },
                MockHttpResponse {
                    status_line: "200 OK",
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: success_body,
                },
            ],
            MockHttpResponse {
                status_line: "200 OK",
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: json!({"ok": true}).to_string(),
            },
        )
        .await;

        let api = create_test_api(port);
        let response = api
            .execute_sql_statement("token", &json!({"statement": "SELECT 1"}))
            .await
            .expect("SQL statement should succeed after retrying the rate-limited response");

        assert_eq!(response["status"]["state"], "SUCCEEDED");
        assert_eq!(
            requests.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "expected the SQL statement request to be retried once"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_execute_sql_statement_limits_concurrent_requests() {
        use std::sync::atomic::Ordering;

        let (port, max_active_requests, mut started_rx, gate) =
            start_blocking_mock_http_server(MockHttpResponse {
                status_line: "200 OK",
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: json!({
                    "status": { "state": "SUCCEEDED" },
                    "statement_id": "stmt-1",
                    "result": { "data_array": [] }
                })
                .to_string(),
            })
            .await;

        let api = Arc::new(create_test_api_with_request_limit(port, 1));

        let first_payload = json!({"statement": "SELECT 1"});
        let first_api = Arc::clone(&api);
        let first = tokio::spawn(async move {
            first_api
                .execute_sql_statement("token", &first_payload)
                .await
        });

        started_rx
            .recv()
            .await
            .expect("the first request should reach the server");

        let second_payload = json!({"statement": "SELECT 2"});
        let second_api = Arc::clone(&api);
        let second = tokio::spawn(async move {
            second_api
                .execute_sql_statement("token", &second_payload)
                .await
        });

        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            max_active_requests.load(Ordering::SeqCst),
            1,
            "only one Databricks HTTP request should be in flight at a time"
        );
        assert!(
            started_rx.try_recv().is_err(),
            "the second request should wait for a permit before reaching the server"
        );

        gate.add_permits(1);

        started_rx
            .recv()
            .await
            .expect("the second request should start after the first finishes");

        gate.add_permits(1);

        let first_response = first
            .await
            .expect("the first task should join")
            .expect("the first request should succeed");
        let second_response = second
            .await
            .expect("the second task should join")
            .expect("the second request should succeed");

        assert_eq!(first_response["status"]["state"], "SUCCEEDED");
        assert_eq!(second_response["status"]["state"], "SUCCEEDED");
        assert_eq!(max_active_requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_execute_sql_statement_uses_rate_controller() {
        let (port, _max_active_requests, mut started_rx, gate) =
            start_blocking_mock_http_server(MockHttpResponse {
                status_line: "200 OK",
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: json!({
                    "status": { "state": "SUCCEEDED" },
                    "statement_id": "stmt-1",
                    "result": { "data_array": [] }
                })
                .to_string(),
            })
            .await;

        let mut api =
            create_test_api_with_request_limit(port, SQL_WAREHOUSE_MAX_IN_FLIGHT_REQUESTS);
        api.rate_controller = Some(
            RateController::builder()
                .with_max_concurrent_requests(1)
                .build(),
        );
        let api = Arc::new(api);

        let first_payload = json!({"statement": "SELECT 1"});
        let first_api = Arc::clone(&api);
        let first = tokio::spawn(async move {
            first_api
                .execute_sql_statement("token", &first_payload)
                .await
        });

        started_rx
            .recv()
            .await
            .expect("the first request should reach the server");

        let second_payload = json!({"statement": "SELECT 2"});
        let second_api = Arc::clone(&api);
        let second = tokio::spawn(async move {
            second_api
                .execute_sql_statement("token", &second_payload)
                .await
        });

        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        assert!(
            started_rx.try_recv().is_err(),
            "the second request should wait for the shared rate-controller permit"
        );

        gate.add_permits(1);
        started_rx
            .recv()
            .await
            .expect("the second request should start after the first releases the permit");
        gate.add_permits(1);

        first
            .await
            .expect("the first task should join")
            .expect("the first request should succeed");
        second
            .await
            .expect("the second task should join")
            .expect("the second request should succeed");
    }

    #[test]
    fn test_with_config_rejects_zero_max_concurrent_requests() {
        let result = DatabricksSqlWarehouse::with_config(
            "host.example.com",
            "warehouse-123",
            Arc::new(StaticTokenProvider("test-token".to_string())),
            SqlWarehouseConfig {
                max_concurrent_requests: 0,
                ..SqlWarehouseConfig::default()
            },
        );

        match result {
            Err(Error::InvalidConcurrencyLimit { limit: 0 }) => {}
            Err(err) => panic!("unexpected error: {err}"),
            Ok(_) => panic!("zero max_concurrent_requests should be rejected"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_execute_sql_statement_disables_on_permanent_http_errors() {
        use std::sync::atomic::Ordering;

        for status_line in ["401 Unauthorized", "403 Forbidden", "404 Not Found"] {
            let (port, requests) = start_mock_http_server(
                vec![MockHttpResponse {
                    status_line,
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: json!({"error": "permanent failure"}).to_string(),
                }],
                MockHttpResponse {
                    status_line: "200 OK",
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: json!({"ok": true}).to_string(),
                },
            )
            .await;

            let api = create_test_api(port);

            let err = api
                .execute_sql_statement("token", &json!({"statement": "SELECT 1"}))
                .await
                .expect_err("permanent HTTP status should fail the request");
            assert!(
                matches!(err, Error::HttpRequestFailed { .. }),
                "unexpected error: {err}"
            );
            assert!(
                api.metrics.permanently_disabled.load(Ordering::Relaxed),
                "connector should be disabled after {status_line}"
            );
            assert_eq!(
                api.metrics.permanent_errors_total.load(Ordering::Relaxed),
                1,
                "permanent error counter should increment after {status_line}"
            );

            let second_err = api
                .execute_sql_statement("token", &json!({"statement": "SELECT 2"}))
                .await
                .expect_err("subsequent requests should fail fast after disable");
            assert!(
                matches!(second_err, Error::PermanentlyDisabled),
                "unexpected error: {second_err}"
            );
            assert_eq!(
                requests.load(Ordering::SeqCst),
                1,
                "disabled connector should not issue additional HTTP requests after {status_line}"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_execute_sql_statement_does_not_disable_when_opted_out() {
        use std::sync::atomic::Ordering;

        let (port, requests) = start_mock_http_server(
            vec![
                MockHttpResponse {
                    status_line: "401 Unauthorized",
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: json!({"error": "permanent failure"}).to_string(),
                },
                MockHttpResponse {
                    status_line: "200 OK",
                    headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                    body: json!({
                        "status": { "state": "SUCCEEDED" },
                        "statement_id": "stmt-1",
                        "result": { "data_array": [] }
                    })
                    .to_string(),
                },
            ],
            MockHttpResponse {
                status_line: "200 OK",
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: json!({"ok": true}).to_string(),
            },
        )
        .await;

        let mut api = create_test_api(port);
        api.disable_on_permanent_error = false;

        let err = api
            .execute_sql_statement("token", &json!({"statement": "SELECT 1"}))
            .await
            .expect_err("401 should still fail the current request");
        assert!(
            matches!(err, Error::HttpRequestFailed { .. }),
            "unexpected error: {err}"
        );
        assert!(
            !api.metrics.permanently_disabled.load(Ordering::Relaxed),
            "connector should remain enabled when disable_on_permanent_error is false"
        );
        assert_eq!(
            api.metrics.permanent_errors_total.load(Ordering::Relaxed),
            0,
            "permanent error counter should remain unchanged when opt-out is enabled"
        );

        let response = api
            .execute_sql_statement("token", &json!({"statement": "SELECT 2"}))
            .await
            .expect("subsequent requests should still succeed");
        assert_eq!(response["status"]["state"], "SUCCEEDED");
        assert_eq!(
            requests.load(Ordering::SeqCst),
            2,
            "connector should continue issuing requests when permanent disable is disabled"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_get_warehouse_type_disables_on_permanent_http_errors() {
        use std::sync::atomic::Ordering;

        let (port, requests) = start_mock_http_server(
            vec![MockHttpResponse {
                status_line: "403 Forbidden",
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: json!({"error": "forbidden"}).to_string(),
            }],
            MockHttpResponse {
                status_line: "200 OK",
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: json!({"warehouse_type": "PRO", "enable_serverless_compute": false})
                    .to_string(),
            },
        )
        .await;

        let api = create_test_api(port);

        let err = api
            .get_warehouse_type("token")
            .await
            .expect_err("403 should fail the warehouse lookup");
        assert!(
            matches!(err, Error::HttpRequestFailed { .. }),
            "unexpected error: {err}"
        );
        assert!(
            api.metrics.permanently_disabled.load(Ordering::Relaxed),
            "connector should be disabled after a permanent warehouse lookup failure"
        );
        assert_eq!(
            api.metrics.permanent_errors_total.load(Ordering::Relaxed),
            1,
            "permanent error counter should increment after a permanent warehouse lookup failure"
        );

        let second_err = api
            .get_warehouse_type("token")
            .await
            .expect_err("subsequent warehouse lookups should fail fast after disable");
        assert!(
            matches!(second_err, Error::PermanentlyDisabled),
            "unexpected error: {second_err}"
        );
        assert_eq!(
            requests.load(Ordering::SeqCst),
            1,
            "disabled connector should not issue additional warehouse lookup requests"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_http_request_failed_displays_source_chain() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should reserve an unused port");
        let port = listener
            .local_addr()
            .expect("listener should have an address")
            .port();
        drop(listener);

        let source = Client::builder()
            .connect_timeout(std::time::Duration::from_millis(100))
            .timeout(std::time::Duration::from_millis(100))
            .build()
            .expect("should build client")
            .post(format!("http://127.0.0.1:{port}/api/2.0/sql/statements/"))
            .send()
            .await
            .expect_err("request should fail for a closed local port");

        let err = Error::HttpRequestFailed { source };
        let msg = err.to_string();

        assert!(
            msg.contains(
                "Failed to send request to Databricks SQL Warehouse: error sending request for url"
            ),
            "error should include the reqwest request message: {msg}"
        );
        assert!(
            msg.contains("caused by:"),
            "error should include the underlying source chain: {msg}"
        );
    }

    #[test]
    fn test_schema_from_json_parameterless_complex_types() {
        // When using `data_type` column, complex types lack inner type info.
        let response = make_schema_response(&json!([
            ["id", "int", "NO"],
            ["tags", "ARRAY", "YES"],
            ["metadata", "MAP", "YES"],
            ["details", "STRUCT", "YES"],
            ["price", "DECIMAL", "YES"]
        ]));

        let schema = schema_from_json(&response, "test_table")
            .expect("should parse parameterless complex types");
        assert_eq!(schema.fields().len(), 5);

        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);

        // ARRAY without type params falls back to List(Utf8)
        assert_eq!(schema.field(1).name(), "tags");
        assert!(
            matches!(schema.field(1).data_type(), DataType::List(_)),
            "ARRAY should become List, got {:?}",
            schema.field(1).data_type()
        );

        // MAP without type params falls back to Map(Utf8, Utf8)
        assert_eq!(schema.field(2).name(), "metadata");
        assert!(
            matches!(schema.field(2).data_type(), DataType::Map(_, _)),
            "MAP should become Map, got {:?}",
            schema.field(2).data_type()
        );

        // STRUCT without type params falls back to Utf8
        assert_eq!(schema.field(3).name(), "details");
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);

        // DECIMAL without precision/scale falls back to Decimal128(38,10)
        assert_eq!(schema.field(4).name(), "price");
        assert_eq!(schema.field(4).data_type(), &DataType::Decimal128(38, 10));
    }

    /// Routing mock server that dispatches based on SQL statement content.
    ///
    /// Routes requests to either the `info_schema_response` or `describe_response`
    /// based on whether the SQL contains `information_schema` or `DESCRIBE TABLE`.
    /// Supports multiple sequential responses per route.
    async fn start_routing_mock_server(
        info_schema_responses: Vec<Value>,
        describe_response: Value,
    ) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to a port");
        let port = listener
            .local_addr()
            .expect("should have an address")
            .port();
        let info_responses = Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::from(
            info_schema_responses,
        )));
        let describe = Arc::new(describe_response);

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let info_responses = Arc::clone(&info_responses);
                let describe = Arc::clone(&describe);
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = vec![0u8; 8192];
                    let n = stream.read(&mut buf).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]);

                    let response_json = if request.contains("information_schema") {
                        let mut q = info_responses.lock().await;
                        q.pop_front().unwrap_or_else(|| (*describe).clone())
                    } else {
                        (*describe).clone()
                    };

                    let body =
                        serde_json::to_string(&response_json).expect("should serialize response");
                    let http_response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(http_response.as_bytes()).await;
                });
            }
        });

        port
    }

    async fn start_schema_discovery_mock_server(
        info_schema_responses: Vec<MockHttpResponse>,
        describe_responses: Vec<MockHttpResponse>,
        warehouse_responses: Vec<MockHttpResponse>,
    ) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to a port");
        let port = listener
            .local_addr()
            .expect("should have an address")
            .port();
        let info_responses = Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::from(
            info_schema_responses,
        )));
        let describe_responses = Arc::new(tokio::sync::Mutex::new(
            std::collections::VecDeque::from(describe_responses),
        ));
        let warehouse_responses = Arc::new(tokio::sync::Mutex::new(
            std::collections::VecDeque::from(warehouse_responses),
        ));

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let info_responses = Arc::clone(&info_responses);
                let describe_responses = Arc::clone(&describe_responses);
                let warehouse_responses = Arc::clone(&warehouse_responses);
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = vec![0u8; 8192];
                    let n = stream.read(&mut buf).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]);

                    let response = if request.starts_with("GET /api/2.0/sql/warehouses/") {
                        warehouse_responses
                            .lock()
                            .await
                            .pop_front()
                            .unwrap_or_else(|| {
                                MockHttpResponse::json(
                                    "500 Internal Server Error",
                                    json!({"error": "missing warehouse mock response"}).to_string(),
                                )
                            })
                    } else if request.contains("information_schema") {
                        info_responses.lock().await.pop_front().unwrap_or_else(|| {
                            MockHttpResponse::json(
                                "500 Internal Server Error",
                                json!({"error": "missing information_schema mock response"})
                                    .to_string(),
                            )
                        })
                    } else if request.contains("DESCRIBE TABLE") {
                        describe_responses
                            .lock()
                            .await
                            .pop_front()
                            .unwrap_or_else(|| {
                                MockHttpResponse::json(
                                    "500 Internal Server Error",
                                    json!({"error": "missing DESCRIBE TABLE mock response"})
                                        .to_string(),
                                )
                            })
                    } else {
                        MockHttpResponse::json(
                            "500 Internal Server Error",
                            json!({"error": "unexpected mock request"}).to_string(),
                        )
                    };

                    let mut http_response = format!(
                        "HTTP/1.1 {}\r\nContent-Length: {}\r\n",
                        response.status_line,
                        response.body.len()
                    );
                    for (header_name, header_value) in response.headers {
                        let _ = std::fmt::Write::write_fmt(
                            &mut http_response,
                            format_args!("{header_name}: {header_value}\r\n"),
                        );
                    }
                    http_response.push_str("\r\n");
                    http_response.push_str(&response.body);

                    let _ = stream.write_all(http_response.as_bytes()).await;
                });
            }
        });

        port
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_falls_back_to_data_type_on_unresolved_column() {
        // First info_schema call returns UNRESOLVED_COLUMN error (full_data_type doesn't exist).
        // Second info_schema call returns a successful schema response using data_type column.
        // DESCRIBE TABLE also succeeds as fallback.
        let unresolved_column_response = json!({
            "status": {
                "state": "FAILED",
                "error": {
                    "message": "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `full_data_type` cannot be resolved."
                }
            },
            "statement_id": "stmt-1"
        });
        let success_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-2",
            "result": {
                "data_array": [
                    ["id", "int", "NO"],
                    ["name", "string", "YES"]
                ]
            }
        });
        let describe_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-3",
            "result": {
                "data_array": [
                    ["id", "int", ""],
                    ["name", "string", ""]
                ]
            }
        });

        let port = start_routing_mock_server(
            vec![unresolved_column_response, success_response],
            describe_response,
        )
        .await;
        let api = create_test_api(port);
        let table = TableReference::full("my_catalog", "my_schema", "my_table");

        let schema = api
            .get_schema(&table, &NoPermissionsCheck)
            .await
            .expect("should succeed via data_type fallback");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_succeeds_with_full_data_type() {
        let info_schema_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-1",
            "result": {
                "data_array": [
                    ["id", "bigint", "NO"],
                    ["amount", "decimal(10,2)", "YES"]
                ]
            }
        });
        let describe_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-2",
            "result": {
                "data_array": [
                    ["id", "int", ""],
                    ["amount", "decimal(10,2)", ""]
                ]
            }
        });

        let port = start_routing_mock_server(vec![info_schema_response], describe_response).await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "orders");

        let schema = api
            .get_schema(&table, &NoPermissionsCheck)
            .await
            .expect("should succeed on first try with full_data_type");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).data_type(), &DataType::Decimal128(10, 2));
    }

    /// Regression test: Databricks sends timestamps as `Timestamp(Microsecond, "Etc/UTC")`
    /// in Arrow IPC. The declared schema must also use Microsecond so that `try_cast_to`
    /// doesn't attempt a µs→ns multiplication that overflows for far-future sentinel
    /// values like year 9999 (253402300799999000 µs × 1000 > `i64::MAX`).
    #[test]
    fn test_schema_from_json_timestamp_microsecond_avoids_overflow() {
        use arrow::array::TimestampMicrosecondArray;
        use arrow_tools::record_batch::try_cast_to;

        // Parse schema from a Databricks JSON response with timestamp columns
        let response = make_schema_response(&json!([
            ["id", "int", "NO"],
            ["end_datetime", "timestamp", "YES"],
            ["created_ntz", "timestamp_ntz", "YES"]
        ]));
        let declared_schema =
            schema_from_json(&response, "test_table").expect("should parse schema");

        // Verify both timestamp fields use Microsecond
        assert_eq!(
            declared_schema.field(1).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            "TIMESTAMP must be Microsecond"
        );
        assert_eq!(
            declared_schema.field(2).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            "TIMESTAMP_NTZ must be Microsecond"
        );

        // Simulate Databricks Arrow IPC: data arrives as Timestamp(Microsecond, "Etc/UTC")
        // with a far-future sentinel value (year 9999).
        let sentinel_us: i64 = 253_402_300_799_999_000; // 9999-12-31T23:59:59.999 in µs
        let ipc_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "end_datetime",
                DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Microsecond,
                    Some("Etc/UTC".into()),
                ),
                true,
            ),
            Field::new(
                "created_ntz",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&ipc_schema),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![sentinel_us]).with_timezone("Etc/UTC"),
                ),
                Arc::new(TimestampMicrosecondArray::from(vec![sentinel_us])),
            ],
        )
        .expect("should create batch");

        // Cast from IPC schema to declared schema — must NOT overflow
        let result = try_cast_to(batch, declared_schema);
        assert!(
            result.is_ok(),
            "try_cast_to should not overflow for year-9999 sentinel: {:?}",
            result.err()
        );

        let casted = result.expect("already checked");
        let ts_col = casted
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("should be TimestampMicrosecondArray");
        assert_eq!(
            ts_col.value(0),
            sentinel_us,
            "sentinel value must be preserved"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_propagates_error_when_both_probes_fail() {
        // Both info_schema and DESCRIBE TABLE fail with non-permission errors.
        let info_failure = json!({
            "status": {
                "state": "FAILED",
                "error": { "message": "Table or view not found: my_table" }
            },
            "statement_id": "stmt-1"
        });
        let describe_failure = json!({
            "status": {
                "state": "FAILED",
                "error": { "message": "Table or view not found: my_table" }
            },
            "statement_id": "stmt-2"
        });

        let port = start_routing_mock_server(vec![info_failure], describe_failure).await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "my_table");

        let err = api
            .get_schema(&table, &NoPermissionsCheck)
            .await
            .expect_err("should propagate error when both probes fail");
        assert!(
            err.to_string().contains("Table or view not found"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_info_schema_fails_describe_succeeds() {
        // information_schema fails with non-permission error, DESCRIBE TABLE succeeds.
        let info_failure = json!({
            "status": {
                "state": "FAILED",
                "error": {
                    "message": "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `some_other_col` cannot be resolved."
                }
            },
            "statement_id": "stmt-1"
        });
        let describe_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-2",
            "result": {
                "data_array": [
                    ["id", "int", ""],
                    ["name", "string", ""]
                ]
            }
        });

        let port = start_routing_mock_server(vec![info_failure], describe_response).await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "my_table");

        let schema = api
            .get_schema(&table, &NoPermissionsCheck)
            .await
            .expect("should fall back to DESCRIBE TABLE");
        assert_eq!(schema.fields().len(), 2);
    }

    /// Regression test: foreign tables on Pro warehouses still need to fall
    /// back to `DESCRIBE TABLE` when `information_schema` returns
    /// `UNSUPPORTED_DATA_SOURCE`.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_falls_back_to_describe_on_unsupported_data_source_for_pro_warehouse() {
        let unsupported_response = json!({
            "status": {
                "state": "FAILED",
                "error": {
                    "message": "[UNSUPPORTED_DATA_SOURCE] The input query contains unsupported data source(s). Only csv, json, avro, delta, kafka, parquet, orc, text, unity_catalog, binaryFile, xml, excel, simplescan, iceberg data sources are supported on Databricks SQL, and only csv, json, avro, delta, kafka, parquet, orc, text, unity_catalog, binaryFile, xml, excel, simplescan, iceberg data sources are allowed to run DML on Databricks SQL. SQLSTATE: 0A000"
                }
            },
            "statement_id": "stmt-1"
        });
        // DESCRIBE TABLE succeeds as fallback.
        let describe_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-2",
            "result": {
                "data_array": [
                    ["id", "int", ""],
                    ["name", "string", ""]
                ]
            }
        });

        let port = start_schema_discovery_mock_server(
            vec![MockHttpResponse::json(
                "200 OK",
                unsupported_response.to_string(),
            )],
            vec![MockHttpResponse::json(
                "200 OK",
                describe_response.to_string(),
            )],
            vec![MockHttpResponse::json(
                "200 OK",
                json!({"warehouse_type": "PRO", "enable_serverless_compute": false}).to_string(),
            )],
        )
        .await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "foreign_table");

        let schema = api
            .get_schema(&table, &NoPermissionsCheck)
            .await
            .expect("should fall back to DESCRIBE TABLE on UNSUPPORTED_DATA_SOURCE");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_surfaces_foreign_table_on_classic_without_wrapping_query_failure() {
        let unsupported_response = json!({
            "status": {
                "state": "FAILED",
                "error": {
                    "message": "[UNSUPPORTED_DATA_SOURCE] The input query contains unsupported data source(s). SQLSTATE: 0A000"
                }
            },
            "statement_id": "stmt-1"
        });
        let describe_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-2",
            "result": {
                "data_array": [
                    ["id", "int", ""],
                    ["name", "string", ""]
                ]
            }
        });

        let port = start_schema_discovery_mock_server(
            vec![MockHttpResponse::json(
                "200 OK",
                unsupported_response.to_string(),
            )],
            vec![MockHttpResponse::json(
                "200 OK",
                describe_response.to_string(),
            )],
            vec![MockHttpResponse::json(
                "200 OK",
                json!({"warehouse_type": "CLASSIC", "enable_serverless_compute": false})
                    .to_string(),
            )],
        )
        .await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "foreign_table");

        let err = api
            .get_schema(&table, &NoPermissionsCheck)
            .await
            .expect_err("Classic warehouses should surface a permanent foreign-table error");
        assert!(
            matches!(&err, Error::ForeignTableOnClassicWarehouse { dataset_name, .. } if dataset_name == "catalog.schema.foreign_table"),
            "unexpected error: {err}"
        );

        let msg = err.to_string();
        assert!(
            msg.contains("Switch `databricks_sql_warehouse_id` to a Pro or Serverless warehouse"),
            "error should reference the correct connector parameter: {msg}"
        );
        assert!(
            msg.contains("[UNSUPPORTED_DATA_SOURCE]"),
            "error should preserve the Databricks server message: {msg}"
        );
        assert!(
            !msg.contains("Verify the query is valid"),
            "error should not wrap the generic QueryFailure guidance: {msg}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_fails_safely_when_warehouse_type_lookup_fails() {
        let unsupported_response = json!({
            "status": {
                "state": "FAILED",
                "error": {
                    "message": "[UNSUPPORTED_DATA_SOURCE] The input query contains unsupported data source(s). SQLSTATE: 0A000"
                }
            },
            "statement_id": "stmt-1"
        });
        let describe_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-2",
            "result": {
                "data_array": [
                    ["id", "int", ""],
                    ["name", "string", ""]
                ]
            }
        });

        let port = start_schema_discovery_mock_server(
            vec![MockHttpResponse::json(
                "200 OK",
                unsupported_response.to_string(),
            )],
            vec![MockHttpResponse::json(
                "200 OK",
                describe_response.to_string(),
            )],
            vec![MockHttpResponse::json(
                "403 Forbidden",
                json!({"error": "forbidden"}).to_string(),
            )],
        )
        .await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "foreign_table");

        let err = api
            .get_schema(&table, &NoPermissionsCheck)
            .await
            .expect_err("warehouse lookup failures should fail safe on UNSUPPORTED_DATA_SOURCE");
        assert!(
            matches!(&err, Error::UnsupportedDataSource { dataset_name, .. } if dataset_name == "catalog.schema.foreign_table"),
            "unexpected error: {err}"
        );

        let msg = err.to_string();
        assert!(
            msg.contains("Failing safely rather than returning a schema that may not be queryable"),
            "error should explain the fail-safe behavior: {msg}"
        );
        assert!(
            msg.contains("[UNSUPPORTED_DATA_SOURCE]"),
            "error should preserve the Databricks server message: {msg}"
        );
    }

    // ── Parallel schema discovery integration tests ──

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_parallel_both_succeed_prefers_info_schema() {
        // When both probes succeed, information_schema is preferred (has nullability).
        let info_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-1",
            "result": {
                "data_array": [
                    ["id", "int", "NO"],
                    ["name", "string", "YES"]
                ]
            }
        });
        let describe_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-2",
            "result": {
                "data_array": [
                    ["id", "int", ""],
                    ["name", "string", ""]
                ]
            }
        });

        let port = start_routing_mock_server(vec![info_response], describe_response).await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "my_table");

        let schema = api
            .get_schema(&table, &NoPermissionsCheck)
            .await
            .expect("should prefer information_schema");
        assert_eq!(schema.fields().len(), 2);
        // information_schema has nullability info: id is NOT NULL
        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_parallel_describe_denied_is_permanent_error() {
        // information_schema succeeds but DESCRIBE TABLE returns access denied.
        // This is a permanent error because the table itself is not accessible.
        let info_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-1",
            "result": {
                "data_array": [
                    ["id", "int", "NO"]
                ]
            }
        });
        let describe_denied = json!({
            "status": {
                "state": "FAILED",
                "error": { "message": "INSUFFICIENT_PERMISSIONS: User does not have permission to access table" }
            },
            "statement_id": "stmt-2"
        });

        let port = start_routing_mock_server(vec![info_response], describe_denied).await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "restricted_table");

        let err = api
            .get_schema(&table, &NoPermissionsCheck)
            .await
            .expect_err("should fail when DESCRIBE TABLE is access denied");
        assert!(
            err.to_string().contains("Access denied"),
            "error should mention access denied: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_parallel_info_schema_denied_describe_ok() {
        // information_schema returns access denied, but DESCRIBE TABLE succeeds.
        // This should warn and fall back to DESCRIBE TABLE.
        let info_denied = json!({
            "status": {
                "state": "FAILED",
                "error": { "message": "INSUFFICIENT_PERMISSIONS: User does not have access to information_schema" }
            },
            "statement_id": "stmt-1"
        });
        let describe_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-2",
            "result": {
                "data_array": [
                    ["id", "int", ""],
                    ["name", "string", ""]
                ]
            }
        });

        let port = start_routing_mock_server(vec![info_denied], describe_response).await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "my_table");

        let schema = api
            .get_schema(&table, &NoPermissionsCheck)
            .await
            .expect("should fall back to DESCRIBE TABLE when info_schema denied");
        assert_eq!(schema.fields().len(), 2);
        // DESCRIBE TABLE defaults to nullable
        assert!(schema.field(0).is_nullable());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_parallel_permissions_denied() {
        use crate::schema_discovery::PermissionCheckResult;

        struct DeniedPermissions;
        #[async_trait]
        impl DatasetPermissions for DeniedPermissions {
            async fn check_read_permission(&self, _: &str) -> PermissionCheckResult {
                PermissionCheckResult::Denied {
                    reason: "UC says no SELECT privilege".into(),
                }
            }
        }

        // Both probes succeed, but permissions are denied.
        let info_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-1",
            "result": { "data_array": [["id", "int", "NO"]] }
        });
        let describe_response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "stmt-2",
            "result": { "data_array": [["id", "int", ""]] }
        });

        let port = start_routing_mock_server(vec![info_response], describe_response).await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "my_table");

        let err = api
            .get_schema(&table, &DeniedPermissions)
            .await
            .expect_err("should fail when permissions denied");
        assert!(
            err.to_string().contains("Access denied"),
            "error should mention access denied: {err}"
        );
        assert!(
            err.to_string().contains("UC says no SELECT privilege"),
            "error should contain denial reason: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_get_schema_parallel_both_denied_is_permanent_error() {
        // Both probes return access denied.
        let info_denied = json!({
            "status": {
                "state": "FAILED",
                "error": { "message": "INSUFFICIENT_PERMISSIONS: no access to information_schema" }
            },
            "statement_id": "stmt-1"
        });
        let describe_denied = json!({
            "status": {
                "state": "FAILED",
                "error": { "message": "INSUFFICIENT_PERMISSIONS: no access to table" }
            },
            "statement_id": "stmt-2"
        });

        let port = start_routing_mock_server(vec![info_denied], describe_denied).await;
        let api = create_test_api(port);
        let table = TableReference::full("catalog", "schema", "restricted_table");

        let err = api
            .get_schema(&table, &NoPermissionsCheck)
            .await
            .expect_err("should fail when both probes denied");
        assert!(
            err.to_string().contains("Access denied"),
            "error should mention access denied: {err}"
        );
    }

    /// Schema parsed from `full_data_type` column values matching a real
    /// Databricks `information_schema` dump (bigint, string, timestamp,
    /// boolean, double).
    #[test]
    fn test_schema_from_json_real_full_data_type_schema() {
        let response = make_schema_response(&json!([
            ["record_skey", "bigint", "YES"],
            ["record_hkey", "string", "NO"],
            ["address_city", "string", "NO"],
            ["address_state", "string", "NO"],
            ["address_latitude", "double", "NO"],
            ["address_longitude", "double", "NO"],
            ["start_datetime", "timestamp", "NO"],
            ["end_datetime", "timestamp", "NO"],
            ["is_current_flag", "boolean", "NO"],
            ["is_deleted_flag", "boolean", "NO"]
        ]));

        let schema = schema_from_json(&response, "catalog.test_schema.dim_records")
            .expect("should parse real full_data_type schema");
        assert_eq!(schema.fields().len(), 10);

        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(schema.field(0).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(4).data_type(), &DataType::Float64);
        assert_eq!(
            schema.field(6).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            schema.field(7).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(8).data_type(), &DataType::Boolean);
    }

    /// Schema parsed from `data_type` column values matching a real
    /// Databricks `information_schema` dump. The fallback path receives
    /// LONG instead of bigint, and all types are uppercase.
    #[test]
    fn test_schema_from_json_real_data_type_fallback_schema() {
        let response = make_schema_response(&json!([
            ["record_skey", "LONG", "YES"],
            ["record_hkey", "STRING", "NO"],
            ["address_city", "STRING", "NO"],
            ["address_latitude", "DOUBLE", "NO"],
            ["address_longitude", "DOUBLE", "NO"],
            ["start_datetime", "TIMESTAMP", "NO"],
            ["end_datetime", "TIMESTAMP", "NO"],
            ["is_current_flag", "BOOLEAN", "NO"],
            ["is_deleted_flag", "BOOLEAN", "NO"]
        ]));

        let schema = schema_from_json(&response, "catalog.test_schema.dim_records")
            .expect("should parse real data_type fallback schema");
        assert_eq!(schema.fields().len(), 9);

        // LONG must map to Int64
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(schema.field(0).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(3).data_type(), &DataType::Float64);
        assert_eq!(
            schema.field(5).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(7).data_type(), &DataType::Boolean);
    }

    /// Full bridge table schema from real Databricks `information_schema`,
    /// using `full_data_type` values (bigint, string, timestamp, boolean).
    #[test]
    fn test_schema_from_json_real_bridge_table_full_data_type() {
        let response = make_schema_response(&json!([
            ["bridge_id", "bigint", "YES"],
            ["entity_id", "bigint", "YES"],
            ["entity_skey", "bigint", "YES"],
            ["entity_hkey", "string", "YES"],
            ["snapshot_id", "bigint", "YES"],
            ["related_id", "bigint", "YES"],
            ["related_address", "string", "YES"],
            ["related_skey", "bigint", "YES"],
            ["related_hkey", "string", "YES"],
            ["created_datetime_utc", "timestamp", "YES"],
            ["updated_datetime_utc", "timestamp", "YES"],
            ["valid_from_datetime", "timestamp", "YES"],
            ["end_datetime", "timestamp", "YES"],
            ["is_current_flag", "boolean", "YES"],
            ["is_deleted_flag", "boolean", "YES"]
        ]));

        let schema = schema_from_json(&response, "catalog.test_schema.bridge_entities")
            .expect("should parse bridge table schema");
        assert_eq!(schema.fields().len(), 15);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(6).data_type(), &DataType::Utf8);
        assert_eq!(
            schema.field(12).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(13).data_type(), &DataType::Boolean);
    }

    /// Same bridge table but using `data_type` column values (LONG instead of bigint).
    #[test]
    fn test_schema_from_json_real_bridge_table_data_type_fallback() {
        let response = make_schema_response(&json!([
            ["bridge_id", "LONG", "YES"],
            ["entity_id", "LONG", "YES"],
            ["entity_skey", "LONG", "YES"],
            ["entity_hkey", "STRING", "YES"],
            ["snapshot_id", "LONG", "YES"],
            ["related_id", "LONG", "YES"],
            ["related_address", "STRING", "YES"],
            ["related_skey", "LONG", "YES"],
            ["related_hkey", "STRING", "YES"],
            ["created_datetime_utc", "TIMESTAMP", "YES"],
            ["updated_datetime_utc", "TIMESTAMP", "YES"],
            ["valid_from_datetime", "TIMESTAMP", "YES"],
            ["end_datetime", "TIMESTAMP", "YES"],
            ["is_current_flag", "BOOLEAN", "YES"],
            ["is_deleted_flag", "BOOLEAN", "YES"]
        ]));

        let schema = schema_from_json(&response, "catalog.test_schema.bridge_entities")
            .expect("should parse bridge table with data_type fallback values");
        assert_eq!(schema.fields().len(), 15);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(6).data_type(), &DataType::Utf8);
        assert_eq!(
            schema.field(12).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(13).data_type(), &DataType::Boolean);
    }

    /// Cohorted spend view — `full_data_type` column: date, string, decimal(26,2), int.
    #[test]
    fn test_schema_from_json_real_cohorted_spend_full_data_type() {
        let response = make_schema_response(&json!([
            ["spend_dt", "date", "YES"],
            ["source_nm", "string", "YES"],
            ["channel_id", "string", "YES"],
            ["campaign_nm", "string", "YES"],
            ["brand_campaign_nm", "string", "YES"],
            ["state_cd", "string", "YES"],
            ["county_cd", "string", "YES"],
            ["designated_market_area", "string", "YES"],
            ["zip_code_cd", "string", "YES"],
            ["region_cd", "string", "YES"],
            ["lead_tier_id", "string", "YES"],
            ["sub_id", "string", "YES"],
            ["publisher_nm", "string", "YES"],
            ["non_experiment__decimal_amt", "decimal(26,2)", "YES"],
            ["mrktng_spnd_amt", "decimal(26,2)", "YES"],
            ["mrktng_spnd_exprmnt_amt", "decimal(26,2)", "YES"],
            ["lead_cnt", "int", "YES"],
            ["new_lead_cnt", "int", "YES"],
            ["returning_lead_cnt", "int", "YES"],
            ["leads_with_assgnmnts_cnt", "int", "YES"],
            ["leads_with_any_rep_assgnmnts_cnt", "int", "YES"],
            ["bind_cnt", "int", "YES"],
            ["cohort_binds_0_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_1_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_3_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_7_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_14_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_30_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_45_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_60_days_amt", "decimal(26,2)", "YES"],
            ["cohort_binds_90_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_0_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_1_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_3_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_7_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_14_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_30_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_45_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_60_days_amt", "decimal(26,2)", "YES"],
            ["cohort_bound_premium_90_days_amt", "decimal(26,2)", "YES"],
            ["bound_premium_amt", "decimal(26,2)", "YES"]
        ]));

        let schema = schema_from_json(
            &response,
            "catalog.ext_dbt_dwh_v.mart_mrktng_cohorted_spend_v",
        )
        .expect("should parse cohorted spend schema");
        assert_eq!(schema.fields().len(), 41);
        assert_eq!(schema.field(0).data_type(), &DataType::Date32);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(13).data_type(), &DataType::Decimal128(26, 2));
        assert_eq!(schema.field(16).data_type(), &DataType::Int32);
    }

    /// Same cohorted spend view using `data_type` column (DATE, STRING, DECIMAL, INT).
    #[test]
    fn test_schema_from_json_real_cohorted_spend_data_type_fallback() {
        let response = make_schema_response(&json!([
            ["spend_dt", "DATE", "YES"],
            ["source_nm", "STRING", "YES"],
            ["channel_id", "STRING", "YES"],
            ["campaign_nm", "STRING", "YES"],
            ["brand_campaign_nm", "STRING", "YES"],
            ["state_cd", "STRING", "YES"],
            ["county_cd", "STRING", "YES"],
            ["designated_market_area", "STRING", "YES"],
            ["zip_code_cd", "STRING", "YES"],
            ["region_cd", "STRING", "YES"],
            ["lead_tier_id", "STRING", "YES"],
            ["sub_id", "STRING", "YES"],
            ["publisher_nm", "STRING", "YES"],
            ["non_experiment__decimal_amt", "DECIMAL", "YES"],
            ["mrktng_spnd_amt", "DECIMAL", "YES"],
            ["mrktng_spnd_exprmnt_amt", "DECIMAL", "YES"],
            ["lead_cnt", "INT", "YES"],
            ["new_lead_cnt", "INT", "YES"],
            ["returning_lead_cnt", "INT", "YES"],
            ["leads_with_assgnmnts_cnt", "INT", "YES"],
            ["leads_with_any_rep_assgnmnts_cnt", "INT", "YES"],
            ["bind_cnt", "INT", "YES"],
            ["cohort_binds_0_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_1_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_3_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_7_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_14_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_30_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_45_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_60_days_amt", "DECIMAL", "YES"],
            ["cohort_binds_90_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_0_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_1_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_3_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_7_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_14_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_30_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_45_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_60_days_amt", "DECIMAL", "YES"],
            ["cohort_bound_premium_90_days_amt", "DECIMAL", "YES"],
            ["bound_premium_amt", "DECIMAL", "YES"]
        ]));

        let schema = schema_from_json(
            &response,
            "catalog.ext_dbt_dwh_v.mart_mrktng_cohorted_spend_v",
        )
        .expect("should parse cohorted spend with data_type fallback");
        assert_eq!(schema.fields().len(), 41);
        assert_eq!(schema.field(0).data_type(), &DataType::Date32);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        // DECIMAL without params falls back to Decimal128(38,10)
        assert_eq!(schema.field(13).data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(schema.field(16).data_type(), &DataType::Int32);
    }

    /// Schema with geometry, decimal(22,4), and all common scalar types
    /// from a real Databricks `information_schema` dump using `full_data_type`.
    #[test]
    fn test_schema_from_json_real_mixed_types_with_geometry() {
        let response = make_schema_response(&json!([
            ["id", "string", "YES"],
            ["distance_ft", "double", "YES"],
            ["state", "string", "YES"],
            ["score", "double", "YES"],
            ["count", "int", "YES"],
            ["flag", "int", "YES"],
            ["created_at", "timestamp", "YES"],
            ["census", "bigint", "YES"],
            ["coverage_a", "decimal(22,4)", "YES"],
            ["spend_dt", "date", "YES"],
            ["geom", "geometry(5070)", "YES"]
        ]));

        let schema = schema_from_json(&response, "catalog.test_schema.mixed_types")
            .expect("should parse mixed types including geometry");
        assert_eq!(schema.fields().len(), 11);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(schema.field(4).data_type(), &DataType::Int32);
        assert_eq!(
            schema.field(6).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(7).data_type(), &DataType::Int64);
        assert_eq!(schema.field(8).data_type(), &DataType::Decimal128(22, 4));
        assert_eq!(schema.field(9).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(10).data_type(),
            &DataType::Binary,
            "GEOMETRY should map to Binary"
        );
    }

    /// Same mixed schema using `data_type` column (uppercase, GEOMETRY without SRID).
    #[test]
    fn test_schema_from_json_real_mixed_types_data_type_fallback() {
        let response = make_schema_response(&json!([
            ["id", "STRING", "YES"],
            ["distance_ft", "DOUBLE", "YES"],
            ["state", "STRING", "YES"],
            ["score", "DOUBLE", "YES"],
            ["count", "INT", "YES"],
            ["flag", "INT", "YES"],
            ["created_at", "TIMESTAMP", "YES"],
            ["census", "LONG", "YES"],
            ["coverage_a", "DECIMAL", "YES"],
            ["spend_dt", "DATE", "YES"],
            ["geom", "GEOMETRY", "YES"]
        ]));

        let schema = schema_from_json(&response, "catalog.test_schema.mixed_types")
            .expect("should parse mixed types with data_type fallback");
        assert_eq!(schema.fields().len(), 11);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(schema.field(4).data_type(), &DataType::Int32);
        assert_eq!(
            schema.field(6).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(7).data_type(), &DataType::Int64);
        // DECIMAL without params falls back to Decimal128(38,10)
        assert_eq!(schema.field(8).data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(schema.field(9).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(10).data_type(),
            &DataType::Binary,
            "GEOMETRY should map to Binary"
        );
    }

    // ---- DESCRIBE TABLE fallback tests ----

    /// Basic DESCRIBE TABLE response: [`col_name`, `data_type`, comment].
    /// All columns should default to nullable.
    #[test]
    fn test_schema_from_describe_json_basic() {
        let response = make_schema_response(&json!([
            ["id", "int", "primary key"],
            ["name", "string", "user name"],
            ["amount", "double", ""]
        ]));

        let schema = schema_from_describe_json(&response, "test_table")
            .expect("should parse DESCRIBE TABLE response");
        assert_eq!(schema.fields().len(), 3);

        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get(DESCRIPTION_METADATA_KEY)
                .map(String::as_str),
            Some("primary key")
        );
        assert!(
            schema.field(0).is_nullable(),
            "DESCRIBE TABLE defaults to nullable"
        );

        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(
            schema
                .field(1)
                .metadata()
                .get(DESCRIPTION_METADATA_KEY)
                .map(String::as_str),
            Some("user name")
        );
        assert!(schema.field(1).is_nullable());

        assert_eq!(schema.field(2).name(), "amount");
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert!(
            schema
                .field(2)
                .metadata()
                .get(DESCRIPTION_METADATA_KEY)
                .is_none()
        );
    }

    /// DESCRIBE TABLE with metadata rows (partition info) after a blank separator.
    #[test]
    fn test_schema_from_describe_json_with_metadata_rows() {
        let response = make_schema_response(&json!([
            ["id", "int", ""],
            ["name", "string", ""],
            ["", "", ""],
            ["# Partition Information", "", ""],
            ["# col_name", "data_type", "comment"],
            ["part_col", "string", ""]
        ]));

        let schema = schema_from_describe_json(&response, "test_table")
            .expect("should stop before metadata rows");
        assert_eq!(schema.fields().len(), 2, "only real columns, not metadata");
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    /// DESCRIBE TABLE for a Lakehouse Federation foreign table using
    /// Spark SQL types (uppercase, no params on DECIMAL).
    #[test]
    fn test_schema_from_describe_json_federation_table() {
        let response = make_schema_response(&json!([
            ["record_skey", "LONG", ""],
            ["record_hkey", "STRING", ""],
            ["address_latitude", "DOUBLE", ""],
            ["start_datetime", "TIMESTAMP", ""],
            ["is_current_flag", "BOOLEAN", ""],
            ["spend_dt", "DATE", ""],
            ["total_amt", "DECIMAL", ""],
            ["count", "INT", ""],
            ["geom", "GEOMETRY", ""]
        ]));

        let schema = schema_from_describe_json(&response, "neon_pg_foreign.public.test_table")
            .expect("should parse federation table DESCRIBE");
        assert_eq!(schema.fields().len(), 9);

        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert_eq!(
            schema.field(3).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(4).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(5).data_type(), &DataType::Date32);
        assert_eq!(schema.field(6).data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(schema.field(7).data_type(), &DataType::Int32);
        assert_eq!(schema.field(8).data_type(), &DataType::Binary);

        // All columns should be nullable
        for field in schema.fields() {
            assert!(field.is_nullable(), "{} should be nullable", field.name());
        }
    }

    /// DESCRIBE TABLE with only blank/metadata rows should return an error.
    #[test]
    fn test_schema_from_describe_json_no_columns() {
        let response = make_schema_response(&json!([
            ["", "", ""],
            ["# Detailed Table Information", "", ""],
            ["Database", "test_schema", ""]
        ]));

        let err = schema_from_describe_json(&response, "test_table")
            .expect_err("should fail with no columns");
        assert!(
            matches!(&err, Error::NoColumnsInDataset { .. }),
            "expected NoColumnsInDataset, got {err:?}"
        );
    }

    /// DESCRIBE TABLE with missing `data_array` should return an error.
    #[test]
    fn test_schema_from_describe_json_missing_data_array() {
        let response = json!({
            "status": { "state": "SUCCEEDED" },
            "statement_id": "test-stmt-id",
            "result": {}
        });

        let err = schema_from_describe_json(&response, "test_table")
            .expect_err("should fail with missing data_array");
        assert!(
            matches!(&err, Error::UnexpectedSchemaResponse { .. }),
            "expected UnexpectedSchemaResponse, got {err:?}"
        );
    }

    /// DESCRIBE TABLE fallback for the `dim_records` CSV schema.
    /// Types match the `data_type` column: LONG, STRING, DOUBLE,
    /// TIMESTAMP, BOOLEAN.
    #[test]
    fn test_schema_from_describe_json_dim_records() {
        let response = make_schema_response(&json!([
            ["record_skey", "LONG", ""],
            ["record_hkey", "STRING", ""],
            ["address_city", "STRING", ""],
            ["address_latitude", "DOUBLE", ""],
            ["address_longitude", "DOUBLE", ""],
            ["start_datetime", "TIMESTAMP", ""],
            ["end_datetime", "TIMESTAMP", ""],
            ["is_current_flag", "BOOLEAN", ""],
            ["is_deleted_flag", "BOOLEAN", ""]
        ]));

        let schema = schema_from_describe_json(&response, "neon_pg_foreign.public.dim_records")
            .expect("should parse dim_records DESCRIBE");
        assert_eq!(schema.fields().len(), 9);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(3).data_type(), &DataType::Float64);
        assert_eq!(
            schema.field(5).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(7).data_type(), &DataType::Boolean);
        for field in schema.fields() {
            assert!(field.is_nullable(), "{} should be nullable", field.name());
        }
    }

    /// DESCRIBE TABLE fallback for the `bridge_entities` CSV schema.
    /// Types: LONG, STRING, TIMESTAMP, BOOLEAN.
    #[test]
    fn test_schema_from_describe_json_bridge_entities() {
        let response = make_schema_response(&json!([
            ["bridge_id", "LONG", ""],
            ["entity_id", "LONG", ""],
            ["entity_skey", "LONG", ""],
            ["entity_hkey", "STRING", ""],
            ["snapshot_id", "LONG", ""],
            ["related_id", "LONG", ""],
            ["related_address", "STRING", ""],
            ["related_skey", "LONG", ""],
            ["related_hkey", "STRING", ""],
            ["created_datetime_utc", "TIMESTAMP", ""],
            ["updated_datetime_utc", "TIMESTAMP", ""],
            ["valid_from_datetime", "TIMESTAMP", ""],
            ["end_datetime", "TIMESTAMP", ""],
            ["is_current_flag", "BOOLEAN", ""],
            ["is_deleted_flag", "BOOLEAN", ""]
        ]));

        let schema = schema_from_describe_json(&response, "neon_pg_foreign.public.bridge_entities")
            .expect("should parse bridge_entities DESCRIBE");
        assert_eq!(schema.fields().len(), 15);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(6).data_type(), &DataType::Utf8);
        assert_eq!(
            schema.field(12).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(13).data_type(), &DataType::Boolean);
        for field in schema.fields() {
            assert!(field.is_nullable(), "{} should be nullable", field.name());
        }
    }

    /// DESCRIBE TABLE fallback for the `cohorted_spend` CSV schema.
    /// Types: DATE, STRING, DECIMAL (bare), INT.
    #[test]
    fn test_schema_from_describe_json_cohorted_spend() {
        let response = make_schema_response(&json!([
            ["spend_dt", "DATE", ""],
            ["source_nm", "STRING", ""],
            ["channel_id", "STRING", ""],
            ["campaign_nm", "STRING", ""],
            ["brand_campaign_nm", "STRING", ""],
            ["state_cd", "STRING", ""],
            ["county_cd", "STRING", ""],
            ["designated_market_area", "STRING", ""],
            ["zip_code_cd", "STRING", ""],
            ["region_cd", "STRING", ""],
            ["lead_tier_id", "STRING", ""],
            ["sub_id", "STRING", ""],
            ["publisher_nm", "STRING", ""],
            ["non_experiment__decimal_amt", "DECIMAL", ""],
            ["mrktng_spnd_amt", "DECIMAL", ""],
            ["mrktng_spnd_exprmnt_amt", "DECIMAL", ""],
            ["lead_cnt", "INT", ""],
            ["new_lead_cnt", "INT", ""],
            ["returning_lead_cnt", "INT", ""],
            ["leads_with_assgnmnts_cnt", "INT", ""],
            ["leads_with_any_rep_assgnmnts_cnt", "INT", ""],
            ["bind_cnt", "INT", ""],
            ["cohort_binds_0_days_amt", "DECIMAL", ""],
            ["cohort_binds_1_days_amt", "DECIMAL", ""],
            ["cohort_binds_3_days_amt", "DECIMAL", ""],
            ["cohort_binds_7_days_amt", "DECIMAL", ""],
            ["cohort_binds_14_days_amt", "DECIMAL", ""],
            ["cohort_binds_30_days_amt", "DECIMAL", ""],
            ["cohort_binds_45_days_amt", "DECIMAL", ""],
            ["cohort_binds_60_days_amt", "DECIMAL", ""],
            ["cohort_binds_90_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_0_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_1_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_3_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_7_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_14_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_30_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_45_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_60_days_amt", "DECIMAL", ""],
            ["cohort_bound_premium_90_days_amt", "DECIMAL", ""],
            ["bound_premium_amt", "DECIMAL", ""]
        ]));

        let schema = schema_from_describe_json(
            &response,
            "neon_pg_foreign.ext_dbt_dwh_v.mart_mrktng_cohorted_spend_v",
        )
        .expect("should parse cohorted spend DESCRIBE");
        assert_eq!(schema.fields().len(), 41);
        assert_eq!(schema.field(0).data_type(), &DataType::Date32);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        // DECIMAL without params falls back to Decimal128(38,10)
        assert_eq!(schema.field(13).data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(schema.field(16).data_type(), &DataType::Int32);
        for field in schema.fields() {
            assert!(field.is_nullable(), "{} should be nullable", field.name());
        }
    }

    /// DESCRIBE TABLE fallback for the mixed types CSV schema with GEOMETRY.
    /// Types: STRING, DOUBLE, INT, TIMESTAMP, LONG, DATE, DECIMAL, GEOMETRY.
    #[test]
    fn test_schema_from_describe_json_mixed_types() {
        let response = make_schema_response(&json!([
            ["id", "STRING", ""],
            ["distance_ft", "DOUBLE", ""],
            ["state", "STRING", ""],
            ["score", "DOUBLE", ""],
            ["count", "INT", ""],
            ["flag", "INT", ""],
            ["created_at", "TIMESTAMP", ""],
            ["census", "LONG", ""],
            ["coverage_a", "DECIMAL", ""],
            ["spend_dt", "DATE", ""],
            ["geom", "GEOMETRY", ""]
        ]));

        let schema = schema_from_describe_json(&response, "neon_pg_foreign.public.mixed_types")
            .expect("should parse mixed types DESCRIBE");
        assert_eq!(schema.fields().len(), 11);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(schema.field(4).data_type(), &DataType::Int32);
        assert_eq!(
            schema.field(6).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(7).data_type(), &DataType::Int64);
        assert_eq!(schema.field(8).data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(schema.field(9).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(10).data_type(),
            &DataType::Binary,
            "GEOMETRY should map to Binary"
        );
        for field in schema.fields() {
            assert!(field.is_nullable(), "{} should be nullable", field.name());
        }
    }

    /// DESCRIBE TABLE response for a Neon `PostgreSQL` foreign table.
    /// DESCRIBE TABLE returns Spark SQL types (consistent with `full_data_type`).
    #[test]
    fn test_schema_from_describe_json_neon_pg_table() {
        let response = make_schema_response(&json!([
            ["id", "int", ""],
            ["name", "string", ""],
            ["amount", "decimal(10,2)", ""],
            ["created_at", "timestamp", ""],
            ["active", "boolean", ""]
        ]));

        let schema =
            schema_from_describe_json(&response, "neon_pg_foreign.public.test_schema_repro")
                .expect("should parse Neon PG DESCRIBE TABLE");
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Decimal128(10, 2));
        assert_eq!(
            schema.field(3).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(4).data_type(), &DataType::Boolean);
        for field in schema.fields() {
            assert!(field.is_nullable(), "{} should be nullable", field.name());
        }
    }

    /// `information_schema.columns.data_type` for a Neon `PostgreSQL` foreign
    /// table returns source-native type names (`integer`, `text`, `numeric`,
    /// `timestamp without time zone`). These must parse correctly.
    #[test]
    fn test_schema_from_json_neon_pg_native_types() {
        let response = make_schema_response(&json!([
            ["id", "integer", "YES"],
            ["name", "text", "YES"],
            ["amount", "numeric", "YES"],
            ["created_at", "timestamp without time zone", "YES"],
            ["active", "boolean", "YES"]
        ]));

        let schema = schema_from_json(&response, "neon_pg_foreign.public.test_schema_repro")
            .expect("should parse Neon PG source-native types");
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(
            schema.field(3).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            "timestamp without time zone maps to Timestamp(Microsecond, None)"
        );
        assert_eq!(schema.field(4).data_type(), &DataType::Boolean);
    }

    /// Two `SqlWarehouseApi` instances sharing the same semaphore must enforce
    /// a single global concurrency limit. A request from either instance
    /// consumes a permit from the shared pool.
    #[tokio::test(flavor = "current_thread")]
    async fn test_shared_semaphore_limits_across_instances() {
        use std::sync::atomic::Ordering;

        let (port, max_active_requests, mut started_rx, gate) =
            start_blocking_mock_http_server(MockHttpResponse {
                status_line: "200 OK",
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: json!({
                    "status": { "state": "SUCCEEDED" },
                    "statement_id": "stmt-1",
                    "result": { "data_array": [] }
                })
                .to_string(),
            })
            .await;

        // A shared semaphore with capacity 1, simulating a global limit.
        let shared_semaphore = Arc::new(Semaphore::new(1));

        let client = configure_client_builder(ClientBuilder::new())
            .build()
            .expect("should build client");

        // Create two independent API instances that share the same semaphore.
        let api_a = Arc::new(SqlWarehouseApi {
            client: client.clone(),
            base_url: format!("http://127.0.0.1:{port}"),
            request_semaphore: Arc::clone(&shared_semaphore),
            http_max_retries: SQL_WAREHOUSE_DEFAULT_HTTP_MAX_RETRIES,
            backoff_method: BackoffMethod::Fibonacci,
            statement_max_retries: SQL_WAREHOUSE_DEFAULT_STATEMENT_MAX_RETRIES,
            disable_on_permanent_error: true,
            sql_warehouse_id: "warehouse-a".to_string(),
            token_provider: Arc::new(StaticTokenProvider("token-a".to_string())),
            metrics: Arc::new(DatabricksMetrics::default()),
            rate_controller: None,
        });
        let api_b = Arc::new(SqlWarehouseApi {
            client,
            base_url: format!("http://127.0.0.1:{port}"),
            request_semaphore: Arc::clone(&shared_semaphore),
            http_max_retries: SQL_WAREHOUSE_DEFAULT_HTTP_MAX_RETRIES,
            backoff_method: BackoffMethod::Fibonacci,
            statement_max_retries: SQL_WAREHOUSE_DEFAULT_STATEMENT_MAX_RETRIES,
            disable_on_permanent_error: true,
            sql_warehouse_id: "warehouse-b".to_string(),
            token_provider: Arc::new(StaticTokenProvider("token-b".to_string())),
            metrics: Arc::new(DatabricksMetrics::default()),
            rate_controller: None,
        });

        // The first request (from api_a) should consume the single permit.
        let first_payload = json!({"statement": "SELECT 1"});
        let first = tokio::spawn({
            let api = Arc::clone(&api_a);
            async move { api.execute_sql_statement("token", &first_payload).await }
        });

        started_rx
            .recv()
            .await
            .expect("the first request should reach the server");

        // The second request (from api_b, a different instance) must wait
        // because the shared semaphore has no remaining permits.
        let second_payload = json!({"statement": "SELECT 2"});
        let second = tokio::spawn({
            let api = Arc::clone(&api_b);
            async move { api.execute_sql_statement("token", &second_payload).await }
        });

        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            max_active_requests.load(Ordering::SeqCst),
            1,
            "only one HTTP request should be in flight even though two different API instances issued requests"
        );
        assert!(
            started_rx.try_recv().is_err(),
            "the second request (from another API instance) should wait for a permit"
        );

        // Verify inflight_operations metrics: only api_a holds a permit, so only
        // it reports 1 in-flight. api_b is blocked waiting for the semaphore.
        assert_eq!(
            api_a.metrics.inflight_operations.load(Ordering::Relaxed),
            1,
            "api_a should report 1 in-flight operation (holding permit)"
        );
        assert_eq!(
            api_b.metrics.inflight_operations.load(Ordering::Relaxed),
            0,
            "api_b should report 0 in-flight operations (waiting for permit)"
        );
        assert_eq!(
            shared_semaphore.available_permits(),
            0,
            "shared semaphore should have no available permits"
        );

        // Release the first request so the second can proceed.
        gate.add_permits(1);

        started_rx
            .recv()
            .await
            .expect("the second request should start after the first finishes");

        gate.add_permits(1);

        let first_response = first
            .await
            .expect("first task should join")
            .expect("first request should succeed");
        let second_response = second
            .await
            .expect("second task should join")
            .expect("second request should succeed");

        assert_eq!(first_response["status"]["state"], "SUCCEEDED");
        assert_eq!(second_response["status"]["state"], "SUCCEEDED");
        assert_eq!(
            max_active_requests.load(Ordering::SeqCst),
            1,
            "max concurrent requests must stay at 1 across both instances"
        );

        // Verify inflight_operations return to zero after both operations complete.
        assert_eq!(
            api_a.metrics.inflight_operations.load(Ordering::Relaxed),
            0,
            "api_a inflight should be 0 after completion"
        );
        assert_eq!(
            api_b.metrics.inflight_operations.load(Ordering::Relaxed),
            0,
            "api_b inflight should be 0 after completion"
        );
        assert_eq!(
            shared_semaphore.available_permits(),
            1,
            "all semaphore permits should be returned after both operations complete"
        );
    }

    // ── is_access_denied_error tests ──

    #[test]
    fn test_is_access_denied_insufficient_permissions() {
        let err = Error::QueryFailure {
            message: "Query failed with state FAILED: INSUFFICIENT_PERMISSIONS: User does not have access".into(),
        };
        assert!(is_access_denied_error(&err));
    }

    #[test]
    fn test_is_access_denied_access_denied_keyword() {
        let err = Error::QueryFailure {
            message: "ACCESS_DENIED: Operation not allowed".into(),
        };
        assert!(is_access_denied_error(&err));
    }

    #[test]
    fn test_is_access_denied_does_not_have() {
        let err = Error::QueryFailure {
            message: "User does not have permission to read this table".into(),
        };
        assert!(is_access_denied_error(&err));
    }

    #[test]
    fn test_is_access_denied_permission_denied() {
        let err = Error::QueryFailure {
            message: "permission denied for table my_table".into(),
        };
        assert!(is_access_denied_error(&err));
    }

    #[test]
    fn test_is_not_access_denied_for_generic_failure() {
        let err = Error::QueryFailure {
            message: "Table or view not found: my_table".into(),
        };
        assert!(!is_access_denied_error(&err));
    }

    #[test]
    fn test_is_not_access_denied_for_permanently_disabled() {
        assert!(!is_access_denied_error(&Error::PermanentlyDisabled));
    }

    #[test]
    fn test_is_not_access_denied_for_other_errors() {
        let err = Error::NotImplemented;
        assert!(!is_access_denied_error(&err));
    }

    /// HTTP 403 from the SQL Statements API is an infrastructure auth error
    /// (bad token, no warehouse access), NOT a SQL-level table permission
    /// denial. It should NOT be classified as access denied.
    #[test]
    fn test_http_403_is_not_sql_access_denied() {
        // Build a reqwest::Error that reports status 403.
        // We can't easily construct one, but we can verify via QueryFailure
        // that only SQL-level messages trigger access denied, not HTTP errors.
        let err = Error::QueryFailure {
            message: "HTTP status client error (403 Forbidden)".into(),
        };
        // Generic "403 Forbidden" text should NOT match — it lacks the specific
        // SQL error codes (INSUFFICIENT_PERMISSIONS, ACCESS_DENIED, etc.)
        assert!(
            !is_access_denied_error(&err),
            "HTTP 403 in a QueryFailure should not be treated as SQL access denied"
        );
    }
}
