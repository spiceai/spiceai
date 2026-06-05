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

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use crate::Read;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::project_schema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion::physical_plan::{Partitioning, PlanProperties};
use datafusion::{
    datasource::{TableProvider, TableType},
    error::Result,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
    sql::TableReference,
};
use datafusion_table_providers::sql::sql_provider_datafusion::expr::{self, Engine};
use futures::Stream;
use runtime_rate_control::RateController;
use spark_connect_rs::errors::SparkError;
use spark_connect_rs::{SparkSession, SparkSessionBuilder, client::ChannelBuilder, functions::col};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use std::error::Error;

pub mod federation;

/// Rebuildable Spark Connect session factory.
///
/// Spark Connect sessions are identified by a `session_id` that is pinned for
/// the lifetime of a session. When the remote (e.g. a Databricks cluster) closes
/// the session, every subsequent request against the same `session_id` fails
/// with `[INVALID_HANDLE.SESSION_CLOSED]` (or similar). To repair a stale or
/// broken session we rebuild the connection with a *fresh* `session_id`, while
/// preserving the latest auth token (which may have rotated since the session
/// was first established).
struct SparkSessionFactory {
    host: String,
    port: u16,
    /// Connection options excluding `session_id` and `token`, which are managed
    /// here and injected on each (re)build. Stored sorted for deterministic
    /// rendering.
    base_options: Vec<(String, String)>,
    /// Current auth token, updated on rotation and applied on each (re)build.
    token: RwLock<Option<String>>,
    rate_controller: Option<Arc<RateController>>,
}

impl SparkSessionFactory {
    /// Parses a Spark Connect connection string into a rebuildable factory.
    fn from_connection(
        connection: &str,
        rate_controller: Option<Arc<RateController>>,
    ) -> Result<(Self, String), Box<dyn Error + Send + Sync>> {
        let (host, port, options, _) = ChannelBuilder::parse_connection_string(connection)?;
        let options = options.unwrap_or_default();

        let token = options.get("token").cloned();
        let mut base_options: Vec<(String, String)> = options
            .iter()
            .filter(|(key, _)| key.as_str() != "session_id" && key.as_str() != "token")
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        base_options.sort();

        // `join_push_down_context` is a stable identifier used to match
        // federation compute contexts. Missing options default to empty, and
        // `token`/`session_id` are deliberately excluded so the identifier
        // stays stable across token rotation and session rebuilds.
        let join_push_down_context = format!(
            "sc://{}:{}/;user_id={};x-databricks-cluster-id={};use_ssl={}",
            host,
            port,
            options.get("user_id").cloned().unwrap_or_default(),
            options
                .get("x-databricks-cluster-id")
                .cloned()
                .unwrap_or_default(),
            options.get("use_ssl").cloned().unwrap_or_default()
        );

        Ok((
            Self {
                host,
                port,
                base_options,
                token: RwLock::new(token),
                rate_controller,
            },
            join_push_down_context,
        ))
    }

    /// Renders a connection string with a freshly generated `session_id` and the
    /// supplied token.
    fn render_connection(&self, token: Option<&str>) -> String {
        let session_id = Uuid::new_v4();
        let mut connection = format!("sc://{}:{}/;session_id={session_id}", self.host, self.port);
        for (key, value) in &self.base_options {
            connection.push(';');
            connection.push_str(key);
            connection.push('=');
            connection.push_str(value);
        }
        if let Some(token) = token {
            connection.push_str(";token=");
            connection.push_str(token);
        }
        connection.push(';');
        connection
    }

    /// Updates the token applied to subsequently built sessions.
    async fn set_token(&self, token: &str) {
        let mut guard = self.token.write().await;
        *guard = Some(token.to_string());
    }

    /// Builds a new [`SparkSession`] with a fresh `session_id` and the current
    /// token.
    async fn build(&self) -> Result<Arc<SparkSession>, Box<dyn Error + Send + Sync>> {
        let token = self.token.read().await.clone();
        let connection = self.render_connection(token.as_deref());
        let rate_controller_permit =
            acquire_rate_controller_permit(self.rate_controller.as_ref()).await?;
        let session = SparkSessionBuilder::remote(&connection)?.build().await?;
        drop(rate_controller_permit);
        Ok(Arc::new(session))
    }
}

#[derive(Clone)]
pub struct SparkConnect {
    inner: Arc<SparkConnectInner>,
}

struct SparkConnectInner {
    /// Current live session. Swapped out atomically when a stale/broken session
    /// is repaired via [`SparkConnect::reconnect`].
    session: RwLock<Arc<SparkSession>>,
    /// Serializes reconnects so a burst of failing queries triggers a single
    /// rebuild. Held across the async session build *instead of* the session
    /// `RwLock`, so query readers are never blocked while a new session is
    /// being established.
    reconnect_lock: Mutex<()>,
    factory: SparkSessionFactory,
    join_push_down_context: String,
    rate_controller: Option<Arc<RateController>>,
}

impl std::fmt::Debug for SparkConnect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparkConnect")
            .field("join_push_down_context", &self.inner.join_push_down_context)
            .finish_non_exhaustive()
    }
}

impl SparkConnect {
    pub fn validate_connection_string(
        connection: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        ChannelBuilder::parse_connection_string(connection)?;
        Ok(())
    }

    pub async fn from_connection(connection: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::from_connection_with_rate_controller(connection, None).await
    }

    pub async fn from_connection_with_rate_controller(
        connection: &str,
        rate_controller: Option<Arc<RateController>>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let (factory, join_push_down_context) =
            SparkSessionFactory::from_connection(connection, rate_controller.clone())?;
        let session = factory.build().await?;

        Ok(Self {
            inner: Arc::new(SparkConnectInner {
                session: RwLock::new(session),
                reconnect_lock: Mutex::new(()),
                factory,
                join_push_down_context,
                rate_controller,
            }),
        })
    }

    /// The join push-down context used for federation compute-context matching.
    fn join_push_down_context(&self) -> &str {
        &self.inner.join_push_down_context
    }

    /// Updates the auth token on both the live session and the rebuild factory,
    /// so a rotated token survives a subsequent reconnect.
    pub async fn set_token(&self, token: &str) {
        self.inner.factory.set_token(token).await;
        let session = self.inner.session.read().await;
        session.set_token(Some(token));
    }

    /// Returns the current live session.
    async fn current_session(&self) -> Arc<SparkSession> {
        Arc::clone(&*self.inner.session.read().await)
    }

    /// Repairs a stale/broken session by rebuilding it with a fresh `session_id`
    /// and replacing the shared session so future queries use the new one.
    ///
    /// Reconnects are serialized by a dedicated `reconnect_lock` so a burst of
    /// concurrently-failing queries triggers a single rebuild (no stampede).
    /// The session `RwLock` is only taken briefly — to read the current handle
    /// and to swap in the rebuilt one — and is never held across the async
    /// `factory.build()`, so query readers are not blocked during a reconnect.
    ///
    /// If another task already reconnected while this caller waited for the
    /// reconnect lock (the shared session is no longer the `stale` one), the
    /// already-established session is returned without rebuilding again.
    async fn reconnect(
        &self,
        stale: &Arc<SparkSession>,
    ) -> Result<Arc<SparkSession>, Box<dyn Error + Send + Sync>> {
        let _reconnect_guard = self.inner.reconnect_lock.lock().await;

        // Another caller may have rebuilt the session while we waited for the
        // reconnect lock; if so, reuse it instead of rebuilding again.
        {
            let current = self.inner.session.read().await;
            if !Arc::ptr_eq(&current, stale) {
                return Ok(Arc::clone(&*current));
            }
        }

        // Build the replacement session without holding the session lock.
        let new_session = self.inner.factory.build().await?;

        // Briefly take the write lock only to swap in the rebuilt session.
        {
            let mut guard = self.inner.session.write().await;
            *guard = Arc::clone(&new_session);
        }

        Ok(new_session)
    }

    /// Runs a Spark operation against the current session, transparently
    /// reconnecting and retrying once if the session has gone stale or the
    /// channel has broken (e.g. the remote closed the session). The operation is
    /// a closure so it can be re-run against a freshly rebuilt session.
    ///
    /// Only read operations are issued through this connector, so retrying after
    /// a reconnect is safe and does not risk duplicating side effects.
    pub(crate) async fn with_session_retry<F, Fut, T>(&self, op: F) -> Result<T, SparkError>
    where
        F: Fn(Arc<SparkSession>) -> Fut,
        Fut: Future<Output = Result<T, SparkError>>,
    {
        let session = self.current_session().await;
        match self.run_attempt(&op, Arc::clone(&session)).await {
            Err(err) if is_recoverable_session_error(&err) => {
                tracing::warn!(
                    "Spark Connect session is stale or broken ({err}); reconnecting and retrying once"
                );
                let new_session = self
                    .reconnect(&session)
                    .await
                    .map_err(SparkError::from_external_error)?;
                self.run_attempt(&op, new_session).await
            }
            result => result,
        }
    }

    async fn run_attempt<F, Fut, T>(
        &self,
        op: &F,
        session: Arc<SparkSession>,
    ) -> Result<T, SparkError>
    where
        F: Fn(Arc<SparkSession>) -> Fut,
        Fut: Future<Output = Result<T, SparkError>>,
    {
        let rate_controller_permit =
            acquire_rate_controller_permit(self.inner.rate_controller.as_ref())
                .await
                .map_err(SparkError::from_external_error)?;
        let result = op(session).await;
        drop(rate_controller_permit);
        result
    }
}

/// Spark `[INVALID_HANDLE.SESSION_*]` error sub-classes raised when the
/// server-side session backing this handle no longer exists.
const SESSION_MARKERS: [&str; 4] = [
    "SESSION_CLOSED",
    "SESSION_NOT_FOUND",
    "SESSION_CHANGED",
    "SESSION_EXPIRED",
];

/// Transport/connection failures that indicate the channel is unusable and a
/// reconnect (new TCP/TLS connection) is warranted.
const CONNECTION_MARKERS: [&str; 6] = [
    "UNAVAILABLE",
    "Broken pipe",
    "connection reset",
    "connection closed",
    "transport error",
    "tcp connect error",
];

/// Returns true if the error indicates the Spark Connect session is no longer
/// usable and rebuilding it (a fresh `session_id`/channel) may recover it.
///
/// Two failure classes are treated as recoverable:
///   1. The remote closed/expired/lost the session (e.g. Databricks compute
///      shut the session down), surfaced as an `[INVALID_HANDLE.SESSION_*]`
///      analysis error.
///   2. The underlying gRPC channel/connection broke (transport error, reset,
///      `UNAVAILABLE`), so the next request needs a fresh connection.
///
/// Deliberately narrow otherwise: ordinary analysis errors (missing table, bad
/// SQL, permission denied) must not trigger a reconnect, since a reconnect
/// cannot fix them and retrying would just fail again.
fn is_recoverable_session_error(err: &SparkError) -> bool {
    if matches!(err, SparkError::TonicTransportError(_)) {
        return true;
    }

    let message = err.to_string();

    SESSION_MARKERS
        .iter()
        .chain(CONNECTION_MARKERS.iter())
        .any(|marker| message.contains(marker))
}

#[async_trait]
impl Read for SparkConnect {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        let provider = get_table_provider(self.clone(), &table_reference).await?;
        let provider = Arc::new(provider.create_federated_table_provider());
        Ok(provider)
    }
}

async fn get_table_provider(
    spark_connect: SparkConnect,
    table_reference: &TableReference,
) -> Result<Arc<SparkConnectTableProvider>, Box<dyn Error + Send + Sync>> {
    let spark_table_reference: Arc<str> = match table_reference {
        TableReference::Bare { table } => format!("`{table}`"),
        TableReference::Partial { table, schema } => format!("`{schema}`.`{table}`"),
        TableReference::Full {
            catalog,
            schema,
            table,
        } => {
            format!("`{catalog}`.`{schema}`.`{table}`")
        }
    }
    .into();

    let schema_table_reference = Arc::clone(&spark_table_reference);
    let arrow_schema = spark_connect
        .with_session_retry(move |session| {
            let schema_table_reference = Arc::clone(&schema_table_reference);
            async move {
                Ok(session
                    .table(schema_table_reference.as_ref())?
                    .limit(0)
                    .collect()
                    .await?
                    .schema())
            }
        })
        .await?;

    let join_push_down_context = spark_connect.join_push_down_context().to_string();

    Ok(Arc::new(SparkConnectTableProvider {
        spark_connect,
        table_reference: spark_table_reference.as_ref().into(),
        spark_table_reference,
        join_push_down_context,
        schema: arrow_schema,
    }))
}

#[derive(Debug)]
struct SparkConnectTableProvider {
    spark_connect: SparkConnect,
    table_reference: TableReference,
    /// Backtick-quoted Spark table name, used to (re)build dataframes against the
    /// current session each time the table is scanned.
    spark_table_reference: Arc<str>,
    join_push_down_context: String,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for SparkConnectTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let mut filter_push_down = vec![];
        for filter in filters {
            match expr::to_sql(filter) {
                Ok(_) => filter_push_down.push(TableProviderFilterPushDown::Exact),
                Err(_) => filter_push_down.push(TableProviderFilterPushDown::Unsupported),
            }
        }

        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SparkConnectExecutionPlan::new(
            self.spark_connect.clone(),
            Arc::clone(&self.spark_table_reference),
            &self.schema,
            projection,
            filters,
            limit,
        )?))
    }
}

#[derive(Debug)]
struct SparkConnectExecutionPlan {
    spark_connect: SparkConnect,
    spark_table_reference: Arc<str>,
    projected_schema: SchemaRef,
    filters: Vec<String>,
    limit: Option<i32>,
    properties: PlanProperties,
}

impl SparkConnectExecutionPlan {
    pub fn new(
        spark_connect: SparkConnect,
        spark_table_reference: Arc<str>,
        schema: &SchemaRef,
        projections: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema(schema, projections)?;
        let limit = limit
            .map(|u| {
                let Ok(u) = u32::try_from(u) else {
                    return Err(DataFusionError::Execution(
                        "Value is too large to fit in a u32".to_string(),
                    ));
                };
                if let Ok(u) = i32::try_from(u) {
                    Ok(u)
                } else {
                    Err(DataFusionError::Execution(
                        "Value is too large to fit in an i32".to_string(),
                    ))
                }
            })
            .transpose()?;
        Ok(Self {
            spark_connect,
            spark_table_reference,
            projected_schema: Arc::clone(&projected_schema),
            filters: filters
                .iter()
                .map(|f| expr::to_sql_with_engine(f, Some(Engine::Spark)))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| DataFusionError::Execution(e.to_string()))?,
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        })
    }
}

impl DisplayAs for SparkConnectExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>();
        let filters = self
            .filters
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        write!(
            f,
            "SparkConnectExecutionPlan projection=[{}] filters=[{}]",
            columns.join(", "),
            filters.join(", "),
        )
    }
}

impl ExecutionPlan for SparkConnectExecutionPlan {
    fn name(&self) -> &'static str {
        "SparkConnectExecutionPlan"
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        tracing::trace!("projected_schema {:#?}", self.projected_schema);
        tracing::trace!("sql columns {:#?}", columns);
        tracing::trace!("filters {:#?}", self.filters);
        let stream_adapter = RecordBatchStreamAdapter::new(
            self.schema(),
            spark_scan_to_stream(
                self.spark_connect.clone(),
                Arc::clone(&self.spark_table_reference),
                self.filters.clone(),
                columns,
                self.limit,
            ),
        );
        Ok(Box::pin(stream_adapter))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Builds a record-batch stream for a table scan, rebuilding the dataframe
/// against the current session on each attempt so a reconnect transparently
/// recovers a stale/broken session.
fn spark_scan_to_stream(
    spark_connect: SparkConnect,
    spark_table_reference: Arc<str>,
    filters: Vec<String>,
    columns: Vec<String>,
    limit: Option<i32>,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    stream! {
        let data = spark_connect
            .with_session_retry(|session| {
                let spark_table_reference = Arc::clone(&spark_table_reference);
                let filters = filters.clone();
                let columns = columns.clone();
                async move {
                    let mut dataframe = session.table(spark_table_reference.as_ref())?;
                    for filter in &filters {
                        dataframe = dataframe.filter(filter.as_str());
                    }
                    dataframe = dataframe
                        .select(columns.iter().map(|c| col(c.as_str())).collect::<Vec<_>>());
                    if let Some(limit) = limit {
                        dataframe = dataframe.limit(limit);
                    }
                    dataframe.collect().await
                }
            })
            .await
            .map_err(map_error_to_datafusion_err)?;
        yield (Ok(data))
    }
}

fn map_error_to_datafusion_err(e: SparkError) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}

pub(super) async fn acquire_rate_controller_permit(
    rate_controller: Option<&Arc<RateController>>,
) -> Result<Option<runtime_rate_control::Permit>, Box<dyn Error + Send + Sync>> {
    let Some(rate_controller) = rate_controller else {
        return Ok(None);
    };

    Ok(Some(rate_controller.acquire().await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CONNECTION: &str = "sc://dbc-abcd.cloud.databricks.com:443/;use_ssl=true;user_id=spice.ai;session_id=00000000-0000-0000-0000-000000000001;token=secret-token;x-databricks-cluster-id=cluster-123;user_agent=SpiceAI_OSS/1.0;";

    #[test]
    fn recoverable_session_errors_are_detected() {
        let closed = SparkError::AnalysisException(
            "[INVALID_HANDLE.SESSION_CLOSED] The handle ... is closed.".to_string(),
        );
        assert!(is_recoverable_session_error(&closed));

        let not_found = SparkError::AnalysisException(
            "[INVALID_HANDLE.SESSION_NOT_FOUND] No such session".to_string(),
        );
        assert!(is_recoverable_session_error(&not_found));

        let unavailable =
            SparkError::AnalysisException("status: UNAVAILABLE, message: ...".to_string());
        assert!(is_recoverable_session_error(&unavailable));

        let broken = SparkError::AnalysisException("transport error: connection reset".to_string());
        assert!(is_recoverable_session_error(&broken));
    }

    #[test]
    fn ordinary_analysis_errors_are_not_recoverable() {
        let missing_table = SparkError::AnalysisException(
            "[TABLE_OR_VIEW_NOT_FOUND] The table or view `foo` cannot be found.".to_string(),
        );
        assert!(!is_recoverable_session_error(&missing_table));

        let bad_sql = SparkError::AnalysisException(
            "[PARSE_SYNTAX_ERROR] Syntax error at or near".to_string(),
        );
        assert!(!is_recoverable_session_error(&bad_sql));

        let permission = SparkError::AnalysisException(
            "[INSUFFICIENT_PERMISSIONS] User does not have permission".to_string(),
        );
        assert!(!is_recoverable_session_error(&permission));
    }

    #[test]
    fn factory_parses_connection_and_context() {
        let (factory, join_push_down_context) =
            SparkSessionFactory::from_connection(TEST_CONNECTION, None)
                .expect("connection string should parse");

        assert_eq!(factory.host, "dbc-abcd.cloud.databricks.com");
        assert_eq!(factory.port, 443);
        assert_eq!(
            join_push_down_context,
            "sc://dbc-abcd.cloud.databricks.com:443/;user_id=spice.ai;x-databricks-cluster-id=cluster-123;use_ssl=true"
        );

        // session_id and token must be managed by the factory, not pinned in base options.
        assert!(
            !factory
                .base_options
                .iter()
                .any(|(key, _)| key == "session_id" || key == "token")
        );
    }

    #[test]
    fn rebuild_uses_fresh_session_id_and_preserves_token() {
        let (factory, _) = SparkSessionFactory::from_connection(TEST_CONNECTION, None)
            .expect("connection string should parse");

        let first = factory.render_connection(Some("secret-token"));
        let second = factory.render_connection(Some("secret-token"));

        // A fresh session_id is generated on each rebuild so a closed session is
        // not reused.
        assert!(first.contains("session_id="));
        assert!(!first.contains("session_id=00000000-0000-0000-0000-000000000001"));
        assert_ne!(
            extract_option(&first, "session_id"),
            extract_option(&second, "session_id"),
            "each rebuild must use a distinct session_id"
        );

        // Auth token and connection options are preserved across rebuilds.
        assert_eq!(
            extract_option(&first, "token").as_deref(),
            Some("secret-token")
        );
        assert_eq!(
            extract_option(&first, "x-databricks-cluster-id").as_deref(),
            Some("cluster-123")
        );
        assert_eq!(extract_option(&first, "use_ssl").as_deref(), Some("true"));
        assert_eq!(
            extract_option(&first, "user_id").as_deref(),
            Some("spice.ai")
        );

        // The rendered string must remain a valid Spark Connect connection string.
        SparkConnect::validate_connection_string(&first)
            .expect("rebuilt connection string should be valid");
    }

    #[test]
    fn rebuild_without_token_omits_token() {
        let connection =
            "sc://localhost:15002/;use_ssl=false;user_id=spice.ai;x-databricks-cluster-id=c1";
        let (factory, _) = SparkSessionFactory::from_connection(connection, None)
            .expect("connection string should parse");

        let rendered = factory.render_connection(None);
        assert!(!rendered.contains("token="));
        SparkConnect::validate_connection_string(&rendered)
            .expect("rebuilt connection string should be valid");
    }

    /// Extracts the value of a `;key=value;` option from a rendered Spark
    /// Connect connection string.
    fn extract_option(connection: &str, key: &str) -> Option<String> {
        connection
            .split(';')
            .filter_map(|pair| pair.split_once('='))
            .find(|(k, _)| *k == key)
            .map(|(_, value)| value.to_string())
    }
}
