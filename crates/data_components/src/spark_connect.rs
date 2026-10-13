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
use crate::function_support::FunctionSupport;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, project_schema};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion::physical_plan::{Partitioning, PlanProperties};
use datafusion::{
    common::TableReference,
    datasource::{TableProvider, TableType},
    error::Result,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
    sql::unparser::{Unparser, dialect::CustomDialectBuilder},
};
use futures::Stream;
use runtime_rate_control::RateController;
use runtime_udfs_api::deny_spice_specific_functions;
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
    /// Which functions may be pushed into the SQL sent to Spark.
    ///
    /// Defaults to the Spice deny-list rather than to "federate everything",
    /// so a connector that forgets to set it is safe: the omission costs a
    /// pushdown, not a query that Spark answers `[UNRESOLVED_ROUTINE]` to.
    /// Behind an `Arc` because `SparkConnect` is cloned per query and per
    /// partition, and the list it holds is one `String` per Spice function.
    function_support: Arc<FunctionSupport>,
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
            function_support: deny_spice_specific_functions(),
        })
    }

    /// Replaces the default Spice deny-list, so a test can pin the exact
    /// policy it exercises.
    #[cfg(test)]
    #[must_use]
    fn with_function_support(mut self, function_support: Arc<FunctionSupport>) -> Self {
        self.function_support = function_support;
        self
    }

    /// The functions this connection may ask Spark to evaluate.
    pub(crate) fn function_support(&self) -> &FunctionSupport {
        &self.function_support
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
        // A filter resolves against the table's own schema, which is what
        // carries the column metadata a per-call support check may need.
        // `None` would be safe but pessimistic -- a check that refuses on an
        // unknown type would drop every such filter from the pushdown.
        let scope = DFSchema::try_from(self.schema()).ok();

        let mut filter_push_down = vec![];
        for filter in filters {
            // The deny-list has to be consulted here as well as in
            // `can_execute_plan`: `PushDownFilter` runs first, so refusing to
            // federate the plan only moves a denied predicate into the
            // `TableScan`, and `scan` then hands it to Spark anyway. Same
            // screen `SqlTable::supports_filters_pushdown` applies.
            if !self
                .spark_connect
                .function_support()
                .supports(filter, scope.as_ref())
            {
                filter_push_down.push(TableProviderFilterPushDown::Unsupported);
                continue;
            }
            match expr_to_sql(filter) {
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
    properties: Arc<PlanProperties>,
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
                .map(expr_to_sql)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| DataFusionError::Execution(e.to_string()))?,
            limit,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        })
    }
}

fn expr_to_sql(expr: &Expr) -> DataFusionResult<String> {
    // Spark Connect parses pushed-down filters as Spark SQL (see `DataFrame::filter` below), which
    // quotes identifiers with backticks. `DefaultDialect` would emit double-quoted identifiers,
    // which Spark interprets as string literals, breaking case-sensitive or keyword column refs.
    let dialect = CustomDialectBuilder::new()
        .with_identifier_quote_style('`')
        .build();
    Unparser::new(&dialect)
        .expr_to_sql(expr)
        .map(|expr| expr.to_string())
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

    fn properties(&self) -> &Arc<PlanProperties> {
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

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> datafusion::error::Result<
            datafusion::common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
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

    /// A Spice-only UDF over a Spark Connect table must be evaluated locally,
    /// not unparsed into the SQL sent to Spark.
    ///
    /// `SparkConnectTableProvider`'s `SQLExecutor` overrode no
    /// `can_execute_plan`, so the default `true` federated every plan and
    /// Spark answered
    /// `[UNRESOLVED_ROUTINE] Cannot resolve routine \`spice_only_udf\``. The
    /// same executor is what the Databricks `spark_connect` mode federates
    /// through, on both the dataset and the `catalogs:` path. Regression test
    /// for #13664.
    ///
    /// Needs a Spark Connect server holding `docs(id INT, body STRING)`:
    ///
    /// ```text
    /// SPARK_REMOTE=sc://127.0.0.1:15002/ cargo test --release -p data_components \
    ///   --no-default-features --features spark_connect -- --ignored spice_only_udf
    /// ```
    #[tokio::test]
    #[ignore = "requires a live Spark Connect server; set SPARK_REMOTE"]
    async fn a_spice_only_udf_is_not_pushed_into_the_spark_statement() {
        let remote = std::env::var("SPARK_REMOTE")
            .expect("SPARK_REMOTE must name a Spark Connect server holding `docs`");

        // The negative control, first: with no deny-list the same plan
        // federates, which is what gives the assertion below teeth -- and is
        // the behaviour this test exists to keep from coming back.
        let unguarded = SparkConnect::from_connection(&remote)
            .await
            .expect("connect to the Spark Connect server")
            .with_function_support(permit_everything());
        let unguarded_sql = federated_sql(&unguarded, SPICE_ONLY_QUERY).await;
        assert!(
            unguarded_sql
                .as_deref()
                .is_some_and(|sql| sql.contains(SPICE_ONLY_UDF)),
            "the control: with no deny-list the UDF is unparsed into the remote statement, \
             so an assertion that it is absent once the deny-list is installed means \
             something. Got: {unguarded_sql:?}"
        );

        // A deny-list naming only the stand-in UDF, so this half does not
        // depend on which functions the default Spice set contains.
        let guarded = SparkConnect::from_connection(&remote)
            .await
            .expect("connect to the Spark Connect server")
            .with_function_support(deny_only(SPICE_ONLY_UDF));
        let guarded_sql = federated_sql(&guarded, SPICE_ONLY_QUERY).await;
        assert!(
            guarded_sql
                .as_deref()
                .is_none_or(|sql| !sql.contains(SPICE_ONLY_UDF)),
            "a denied function must not reach the statement sent to Spark, which cannot \
             resolve it. Got: {guarded_sql:?}"
        );

        // A function Spark does have must still be pushed down, so the
        // deny-list has not simply turned federation off.
        let control_sql = federated_sql(&guarded, "SELECT id, upper(body) AS c FROM docs").await;
        assert!(
            control_sql
                .as_deref()
                .is_some_and(|sql| sql.contains("upper(")),
            "upper() is a Spark function and must keep federating. Got: {control_sql:?}"
        );

        // The default, with no `with_function_support` at all: a connector
        // that sets no policy must still be safe, which is what keeps the
        // next Spark-backed connector from re-opening this bug.
        let defaulted = SparkConnect::from_connection(&remote)
            .await
            .expect("connect to the Spark Connect server");
        let defaulted_sql =
            federated_sql(&defaulted, "SELECT id, json_get_str(body) AS c FROM docs").await;
        assert!(
            defaulted_sql
                .as_deref()
                .is_none_or(|sql| !sql.contains(SPICE_SET_UDF)),
            "a connection that set no policy must still deny the Spice set. \
             Got: {defaulted_sql:?}"
        );

        // The predicate half. `PushDownFilter` runs before the federation
        // decision, so refusing to federate the plan is not enough on its own:
        // a denied predicate lands in the `TableScan` and `scan` hands it to
        // Spark regardless. `supports_filters_pushdown` is what keeps it out.
        let predicate = "SELECT id FROM docs WHERE spice_only_udf(body) = '{\"color\":\"red\"}'";
        let predicate_sql = federated_sql(&guarded, predicate).await;
        assert!(
            predicate_sql
                .as_deref()
                .is_none_or(|sql| !sql.contains(SPICE_ONLY_UDF)),
            "a denied function in a WHERE clause must not reach Spark either. \
             Got: {predicate_sql:?}"
        );
        assert_eq!(
            rows(&guarded, predicate).await,
            vec!["1".to_string()],
            "and the predicate must still select the right row, evaluated locally"
        );

        // Control again, on the predicate path: a Spark function in a WHERE
        // clause must keep being pushed down.
        let control_predicate = federated_sql(
            &guarded,
            "SELECT id FROM docs WHERE upper(body) LIKE '%RED%'",
        )
        .await;
        assert!(
            control_predicate
                .as_deref()
                .is_some_and(|sql| sql.contains("upper(")),
            "a Spark function in a WHERE clause must keep federating. Got: {control_predicate:?}"
        );
    }

    /// A federating session over `spark`'s `docs` table, with the stand-in
    /// UDFs registered.
    async fn docs_ctx(spark: &SparkConnect) -> datafusion::prelude::SessionContext {
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion_federation::{FederatedQueryPlanner, FederationAnalyzerRule};

        let provider = spark
            .table_provider(TableReference::bare("docs"))
            .await
            .expect("build the docs table provider");
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(FederationAnalyzerRule::new()))
            .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
            .build();
        let ctx = datafusion::prelude::SessionContext::new_with_state(state);
        ctx.register_udf(stub_udf(SPICE_ONLY_UDF));
        ctx.register_udf(stub_udf(SPICE_SET_UDF));
        ctx.register_table(TableReference::bare("docs"), provider)
            .expect("register the docs table");
        ctx
    }

    /// The first column of every row `sql` returns, rendered as strings.
    async fn rows(spark: &SparkConnect, sql: &str) -> Vec<String> {
        let batches = docs_ctx(spark)
            .await
            .sql(sql)
            .await
            .expect("plan the query")
            .collect()
            .await
            .expect("run the query");
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch.column(0);
                (0..batch.num_rows())
                    .map(|row| {
                        datafusion::common::ScalarValue::try_from_array(column, row)
                            .expect("read the value")
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    const SPICE_ONLY_UDF: &str = "spice_only_udf";
    const SPICE_ONLY_QUERY: &str = "SELECT id, spice_only_udf(body) AS c FROM docs";

    /// A function that really is in the Spice set the default policy denies,
    /// so the default can be tested without naming the whole set.
    const SPICE_SET_UDF: &str = "json_get_str";

    /// A UDF of `name`, standing in for a Spice function Spark does not have.
    /// The deny-list screens by name, so a stub is denied exactly as the real
    /// function is.
    fn stub_udf(name: &str) -> datafusion::logical_expr::ScalarUDF {
        use arrow::datatypes::DataType;
        use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};

        create_udf(
            name,
            vec![DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
        )
    }

    /// A deny-list naming exactly one function, standing in for the Spice set
    /// a real connection defaults to. Named explicitly so the test does not
    /// depend on which functions that set happens to contain.
    fn deny_only(name: &str) -> Arc<crate::function_support::FunctionSupport> {
        Arc::new(crate::function_support::FunctionSupport::new(
            Some(crate::function_support::FunctionRestriction::Deny(vec![
                name.to_string(),
            ])),
            None,
            None,
        ))
    }

    /// A policy that restricts nothing, for the negative control -- the
    /// default is the deny-list, so "unguarded" has to be asked for.
    fn permit_everything() -> Arc<crate::function_support::FunctionSupport> {
        Arc::new(crate::function_support::FunctionSupport::new(
            None, None, None,
        ))
    }

    /// The statement the federated plan for `sql` would send to Spark, or
    /// `None` when nothing federated.
    async fn federated_sql(spark: &SparkConnect, sql: &str) -> Option<String> {
        use datafusion::physical_plan::displayable;

        let physical = docs_ctx(spark)
            .await
            .sql(sql)
            .await
            .expect("plan the query")
            .create_physical_plan()
            .await
            .expect("build the physical plan");

        displayable(physical.as_ref())
            .indent(false)
            .to_string()
            .lines()
            .find_map(|line| {
                line.split_once("base_sql=")
                    .map(|(_, sql)| sql.trim().to_string())
            })
    }

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

    /// The scheme a connection string with `use_ssl=false` resolves to.
    ///
    /// Only a Spice patch to the `spiceai/spark-connect-rs` fork makes this `http`;
    /// upstream `ChannelBuilder::endpoint` returns `https` unconditionally. The
    /// dial below is what proves the consequence — this asserts the mechanism, so
    /// a failure says which of the two moved.
    #[test]
    fn a_non_tls_connection_string_resolves_to_an_http_endpoint() {
        let channel = ChannelBuilder::create("sc://127.0.0.1:15002/;user_id=spice.ai")
            .expect("a connection string without use_ssl should parse");
        assert!(
            !channel.use_ssl(),
            "this guard needs a connection string that asks for no TLS"
        );
        let endpoint = channel.endpoint();
        assert!(
            endpoint.starts_with("http://"),
            "a Spark Connect endpoint that asks for no TLS resolved to {endpoint}, so the \
             connection is dialled over TLS and a plaintext Spark Connect server rejects it"
        );
    }

    /// A connection string's `user_agent` has to *replace* the client's default,
    /// not extend it.
    ///
    /// Live on every production Databricks connection:
    /// `DatabricksSparkConnect::new_with_rate_controller` formats
    /// `user_agent={user_agent}` into the connection string, `from_connection`
    /// keeps it in `base_options` (only `token` and `session_id` are dropped), and
    /// `render_connection` puts it back for `SparkSessionBuilder::remote`. Fork
    /// PRs #9 and #10 are what make the builder read the option and send it as the
    /// whole `client_type`; without them Databricks attributes Spice's traffic to
    /// the connect library, and nothing fails while it does.
    ///
    /// Read out of `Debug` because the value's only other appearance is the
    /// `client_type` field of an outgoing Spark Connect request, which needs a
    /// gRPC server to observe. The control is the second half: it asserts the
    /// library default is what appears when the option is absent, so the
    /// replacement assertion cannot be met by a builder carrying no user agent at
    /// all.
    #[test]
    fn a_connection_string_user_agent_replaces_the_client_default() {
        const DEFAULT_MARKER: &str = "_SPARK_CONNECT_RUST";

        let configured = SparkSessionBuilder::remote(TEST_CONNECTION)
            .expect("the Databricks connection string should parse");
        let configured = format!("{:?}", configured.channel_builder);
        assert!(
            configured.contains("SpiceAI_OSS/1.0"),
            "the connection string's user agent never reached the client, so Databricks \
             attributes Spice's traffic to the connect library instead: {configured}"
        );
        assert!(
            !configured.contains(DEFAULT_MARKER),
            "the user agent extends the library default rather than replacing it, which is \
             not the attribution Databricks is given: {configured}"
        );

        let defaulted = SparkSessionBuilder::remote("sc://127.0.0.1:15002/;user_id=spice.ai")
            .expect("a connection string without a user agent should parse");
        let defaulted = format!("{:?}", defaulted.channel_builder);
        assert!(
            defaulted.contains(DEFAULT_MARKER),
            "the control has to show the default the assertion above requires to be absent, \
             or a builder that carried no user agent would satisfy it: {defaulted}"
        );
    }

    /// What a non-TLS Spark Connect endpoint actually receives when Spice dials it:
    /// the HTTP/2 connection preface, in the clear.
    ///
    /// Asserted against a listener rather than against the endpoint string because
    /// the string is not what fails — a plaintext Spark Connect endpoint dialled
    /// over TLS never completes a connection, and every dataset on that endpoint
    /// fails to load. Losing the fork patch shows up here in one of two shapes,
    /// and both fail: the client sends a TLS `ClientHello` (record type `0x16`),
    /// or `tonic` refuses an `https` endpoint it has no TLS configuration for and
    /// nothing reaches the listener at all.
    #[tokio::test]
    async fn a_non_tls_spark_endpoint_is_dialled_in_plaintext() {
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;

        /// The client half of the HTTP/2 connection preface (RFC 9113 §3.4),
        /// which a plaintext h2 client sends before anything else.
        const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binds a plaintext listener");
        let port = listener
            .local_addr()
            .expect("the listener has a local address")
            .port();

        let accepted = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.ok()?;
            // TCP is a byte stream, so the preface arrives in as many segments as the
            // client happens to write it in: a single `read` can return one byte of it
            // and the comparison below would fail on a client that behaved correctly.
            // Read until the preface is complete, or until the peer stops sending.
            //
            // Accumulated rather than `read_exact`ed because what arrived is itself a
            // diagnostic: a peer that opens the connection and sends nothing, or sends
            // a `ClientHello` and stops, has to reach the assertions below with the
            // bytes it did send rather than be lost in a read that never returns.
            let mut first = Vec::with_capacity(H2_PREFACE.len());
            while first.len() < H2_PREFACE.len() {
                let mut chunk = [0_u8; H2_PREFACE.len()];
                match stream.read(&mut chunk).await {
                    // The peer closed, or the read failed: report what did arrive.
                    Ok(0) | Err(_) => break,
                    Ok(read) => first.extend_from_slice(&chunk[..read]),
                }
            }
            Some(first)
        });

        // The dial is what is under assertion, not the session: nothing on the
        // other end speaks gRPC, so the handshake never completes and `build`
        // would wait for a `SETTINGS` frame that never comes. Spawned and left
        // running for that reason, and dropped with the runtime.
        let connection = format!("sc://127.0.0.1:{port}/;user_id=spice.ai");
        drop(tokio::spawn(async move {
            let _ = SparkSessionBuilder::remote(&connection)
                .expect("a connection string without use_ssl should parse")
                .build()
                .await;
        }));

        let first = tokio::time::timeout(std::time::Duration::from_secs(10), accepted)
            .await
            .expect(
                "no complete HTTP/2 preface within 10s: either nothing was dialled, because an \
                 endpoint that asks for no TLS was resolved to https and tonic refuses that \
                 without a TLS configuration, or the peer opened the connection and stopped \
                 part-way through sending",
            )
            .expect("the accept task panicked")
            .expect("the accept task saw no connection");

        assert!(
            !first.is_empty(),
            "the connection was opened and then abandoned without a byte sent, which is what \
             tonic does with an https endpoint it has no TLS configuration for"
        );
        assert_ne!(
            first.first(),
            Some(&0x16),
            "the client opened a TLS handshake against a plaintext Spark Connect endpoint, so \
             the connection fails and no dataset on that endpoint loads: {first:02x?}"
        );
        assert_eq!(
            first, H2_PREFACE,
            "a plaintext Spark Connect endpoint must receive the HTTP/2 preface: {first:02x?}"
        );
    }

    /// What a TLS Spark Connect endpoint receives when Spice dials it: a TLS
    /// `ClientHello`, not a plaintext HTTP/2 preface.
    ///
    /// The counterpart to the guard above, and the other half of the fork's TLS
    /// handling (fork PR #7). `Endpoint::connect` attaches no TLS configuration of
    /// its own, so the fork attaches one —
    /// `ClientTlsConfig::new().with_native_roots()` — whenever the connection
    /// string asks for `use_ssl=true`. Lose it and a Databricks endpoint is either
    /// dialled in the clear, which the server rejects, or refused by `tonic` for
    /// having no TLS configuration; either way every dataset on that endpoint
    /// fails to load.
    ///
    /// The listener speaks no TLS, so the handshake never completes and the first
    /// bytes it reads are the assertion. This pins that TLS is configured at all,
    /// which is what the patch provides. It does not distinguish *which* root
    /// store was chosen — the roots a client trusts are not observable from its
    /// `ClientHello` — so the `with_native_roots` half is guarded only by the
    /// accessor the same patch added, which
    /// `a_non_tls_connection_string_resolves_to_an_http_endpoint` calls and the
    /// compiler therefore requires.
    #[tokio::test]
    async fn a_tls_spark_endpoint_is_dialled_with_a_tls_handshake() {
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;

        /// A TLS record begins with the content type — `0x16` for handshake — and
        /// then the two-byte legacy protocol version, `0x03 0x01` for every version
        /// a `ClientHello` may announce (RFC 8446 §5.1).
        const TLS_HANDSHAKE: u8 = 0x16;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binds a plaintext listener");
        let port = listener
            .local_addr()
            .expect("the listener has a local address")
            .port();

        let accepted = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.ok()?;
            // Three bytes is the record header's type and version, which is all
            // that is under assertion; accumulated for the same reason as the
            // plaintext guard — what arrived is itself the diagnostic.
            let mut first = Vec::with_capacity(3);
            while first.len() < 3 {
                let mut chunk = [0_u8; 3];
                match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => first.extend_from_slice(&chunk[..read]),
                }
            }
            Some(first)
        });

        // As in the plaintext guard, the dial is what is under assertion: nothing
        // on the other end speaks TLS, so the handshake never completes and
        // `build` would wait forever.
        let connection = format!("sc://127.0.0.1:{port}/;use_ssl=true;user_id=spice.ai");
        drop(tokio::spawn(async move {
            let _ = SparkSessionBuilder::remote(&connection)
                .expect("a connection string with use_ssl=true should parse")
                .build()
                .await;
        }));

        let first = tokio::time::timeout(std::time::Duration::from_secs(10), accepted)
            .await
            .expect(
                "no TLS record header within 10s: either nothing was dialled, because tonic \
                 refuses an https endpoint it has no TLS configuration for, or the peer opened \
                 the connection and stopped part-way through the record header",
            )
            .expect("the accept task panicked")
            .expect("the accept task saw no connection");

        assert!(
            !first.is_empty(),
            "the connection was opened and then abandoned without a byte sent, which is what \
             tonic does with an https endpoint it has no TLS configuration for"
        );
        assert_eq!(
            first.first(),
            Some(&TLS_HANDSHAKE),
            "a Spark Connect endpoint asked for over TLS must receive a TLS handshake record, \
             not {first:02x?} — a plaintext dial is rejected by the server and every dataset \
             on that endpoint fails to load"
        );
        assert_eq!(
            first.get(1),
            Some(&0x03),
            "the record announced a protocol version no TLS ClientHello uses: {first:02x?}"
        );
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
