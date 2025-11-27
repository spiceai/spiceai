//! # SQL DataFusion `TableProvider`
//!
//! This module implements a SQL `TableProvider` for DataFusion.
//!
//! This is used as a fallback if the `datafusion-federation` optimizer is not enabled.

use crate::{
    sql::db_connection_pool::{
        self,
        dbconnection::{get_schema, query_arrow},
        DbConnectionPool,
    },
    util::supported_functions::FunctionSupport,
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::Constraints,
    sql::unparser::{
        dialect::{DefaultDialect, Dialect},
        Unparser,
    },
};
use expr::Engine;
use futures::TryStreamExt;
use snafu::prelude::*;
use std::fmt::Display;
use std::{any::Any, fmt, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
    },
    sql::TableReference,
};

pub mod expr;
#[cfg(feature = "federation")]
pub mod federation;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get a DB connection from the pool: {source}"))]
    UnableToGetConnectionFromPool { source: db_connection_pool::Error },

    #[snafu(display("Unable to get schema: {source}"))]
    UnableToGetSchema {
        source: db_connection_pool::dbconnection::Error,
    },

    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL { source: expr::Error },

    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQLDataFusion { source: DataFusionError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SqlTable<T: 'static, P: 'static> {
    name: &'static str,
    pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
    schema: SchemaRef,
    pub table_reference: TableReference,
    engine: Option<Engine>,
    pub(crate) dialect: Option<Arc<dyn Dialect + Send + Sync>>,
    constraints: Option<Constraints>,
    pub(crate) function_support: Option<FunctionSupport>,
}

impl<T, P> std::fmt::Debug for SqlTable<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlTable")
            .field("name", &self.name)
            .field("table_reference", &self.table_reference)
            .field("schema", &self.schema)
            .field("engine", &self.engine)
            .field("dialect", &self.dialect.is_some())
            .field("constraints", &self.constraints)
            .field("function_support", &self.function_support)
            .finish_non_exhaustive()
    }
}

impl<T, P> SqlTable<T, P> {
    /// Creates a new SQL table.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be retrieved from the database.
    pub async fn new(
        name: &'static str,
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        table_reference: impl Into<TableReference>,
        engine: Option<expr::Engine>,
    ) -> Result<Self> {
        let table_reference = table_reference.into();
        let conn = pool
            .connect()
            .await
            .context(UnableToGetConnectionFromPoolSnafu)?;

        let schema = get_schema(conn, &table_reference)
            .await
            .context(UnableToGetSchemaSnafu)?;

        Ok(Self {
            name,
            pool: Arc::clone(pool),
            schema,
            table_reference,
            engine,
            dialect: None,
            constraints: None,
            function_support: None,
        })
    }

    pub fn new_with_schema(
        name: &'static str,
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<TableReference>,
        engine: Option<expr::Engine>,
    ) -> Self {
        Self {
            name,
            pool: Arc::clone(pool),
            schema: schema.into(),
            table_reference: table_reference.into(),
            engine,
            dialect: None,
            constraints: None,
            function_support: None,
        }
    }

    #[must_use]
    pub fn with_function_support(mut self, function_support: Option<FunctionSupport>) -> Self {
        self.function_support = function_support;
        self
    }

    #[must_use]
    pub fn with_constraints_opt(mut self, constraints: Option<Constraints>) -> Self {
        self.constraints = constraints;
        self
    }

    #[must_use]
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = Some(constraints);
        self
    }

    #[must_use]
    pub fn with_dialect(self, dialect: Arc<dyn Dialect + Send + Sync>) -> Self {
        Self {
            dialect: Some(dialect),
            ..self
        }
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut exec = SqlExec::new(
            projections,
            schema,
            &self.table_reference,
            Arc::clone(&self.pool),
            filters,
            limit,
            self.engine,
        )?;
        if let Some(dialect) = &self.dialect {
            exec = exec.with_dialect(Arc::clone(dialect));
        }
        Ok(Arc::new(exec))
    }

    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    #[must_use]
    pub fn clone_pool(&self) -> Arc<dyn DbConnectionPool<T, P> + Send + Sync> {
        Arc::clone(&self.pool)
    }
}

#[async_trait]
impl<T, P> TableProvider for SqlTable<T, P> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.constraints.as_ref()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let filter_push_down: Vec<TableProviderFilterPushDown> = filters
            .iter()
            .map(|f| match expr::to_sql_with_engine(f, self.engine) {
                Ok(_) => TableProviderFilterPushDown::Exact,
                Err(_) => TableProviderFilterPushDown::Unsupported,
            })
            .collect();

        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, &self.schema(), filters, limit);
    }
}

impl<T, P> Display for SqlTable<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SqlTable {}", self.name)
    }
}

pub struct SqlExec<T, P> {
    projected_schema: SchemaRef,
    table_reference: TableReference,
    pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,
    engine: Option<Engine>,
    dialect: Option<Arc<dyn Dialect + Send + Sync>>,
    /// Custom table expression to use instead of quoted table_reference
    /// Useful for database-specific syntax like ClickHouse parameterized views
    custom_table_expr: Option<String>,
}

impl<T, P> Clone for SqlExec<T, P> {
    fn clone(&self) -> Self {
        SqlExec {
            projected_schema: Arc::clone(&self.projected_schema),
            table_reference: self.table_reference.clone(),
            pool: Arc::clone(&self.pool),
            filters: self.filters.clone(),
            limit: self.limit,
            properties: self.properties.clone(),
            engine: self.engine,
            dialect: self.dialect.clone(),
            custom_table_expr: self.custom_table_expr.clone(),
        }
    }
}

/// Projects a schema to include only the specified columns.
///
/// # Errors
///
/// Returns an error if the projection fails.
pub fn project_schema_safe(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<SchemaRef> {
    let schema = match projection {
        Some(columns) => {
            if columns.is_empty() {
                Arc::clone(schema)
            } else {
                Arc::new(schema.project(columns)?)
            }
        }
        None => Arc::clone(schema),
    };
    Ok(schema)
}

impl<T, P> SqlExec<T, P> {
    /// Creates a new SQL execution plan.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema projection fails.
    pub fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &TableReference,
        pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        filters: &[Expr],
        limit: Option<usize>,
        engine: Option<Engine>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema_safe(schema, projections)?;

        Ok(Self {
            projected_schema: Arc::clone(&projected_schema),
            table_reference: table_reference.clone(),
            pool,
            filters: filters.to_vec(),
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            engine,
            dialect: None,
            custom_table_expr: None,
        })
    }

    #[must_use]
    pub fn with_dialect(self, dialect: Arc<dyn Dialect + Send + Sync>) -> Self {
        Self {
            dialect: Some(dialect),
            ..self
        }
    }

    #[must_use]
    pub fn with_custom_table_expr(self, custom_table_expr: String) -> Self {
        Self {
            custom_table_expr: Some(custom_table_expr),
            ..self
        }
    }

    #[must_use]
    pub fn clone_pool(&self) -> Arc<dyn DbConnectionPool<T, P> + Send + Sync> {
        Arc::clone(&self.pool)
    }

    /// Generates the SQL query string.
    ///
    /// # Errors
    ///
    /// Returns an error if SQL generation fails.
    pub fn sql(&self) -> Result<String> {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| {
                // To ensure backwards compatibility, dialect only used when explicitly set
                // (i.e. Don't use `DefaultDialect`, don't derive from `self.engine`).
                let quote = if let Some(dialect) = &self.dialect {
                    dialect
                        .identifier_quote_style(f.name())
                        .unwrap_or_default()
                        .to_string()
                } else if matches!(self.engine, Some(Engine::ODBC)) {
                    String::new()
                } else {
                    '"'.to_string()
                };
                format!("{quote}{}{quote}", f.name())
            })
            .collect::<Vec<_>>()
            .join(", ");

        let limit_expr = match self.limit {
            Some(limit) => format!("LIMIT {limit}"),
            None => String::new(),
        };

        let where_expr = if self.filters.is_empty() {
            String::new()
        } else {
            let dialect = self.dialect.clone().unwrap_or(self.engine.map_or(
                Arc::new(DefaultDialect {}) as Arc<dyn Dialect + Send + Sync>,
                |e| e.dialect(),
            ));
            let unparser = Unparser::new(dialect.as_ref());

            let filter_expr = self
                .filters
                .iter()
                .map(|f| unparser.expr_to_sql(f).map(|s| s.to_string()))
                .collect::<Result<Vec<String>, DataFusionError>>()
                .context(UnableToGenerateSQLDataFusionSnafu)?;
            format!("WHERE {}", filter_expr.join(" AND "))
        };

        let table_expr = match &self.custom_table_expr {
            Some(expr) => expr.clone(),
            None => self.table_reference.to_quoted_string(),
        };

        Ok(format!(
            "SELECT {columns} FROM {table_expr} {where_expr} {limit_expr}"
        ))
    }
}

impl<T, P> std::fmt::Debug for SqlExec<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "SqlExec sql={sql}")
    }
}

impl<T, P> DisplayAs for SqlExec<T, P> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "SqlExec sql={sql}")
    }
}

impl<T: 'static, P: 'static> ExecutionPlan for SqlExec<T, P> {
    fn name(&self) -> &'static str {
        "SqlExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
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

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let sql = self.sql().map_err(to_execution_error)?;
        tracing::debug!("SqlExec sql: {sql}");

        let schema = self.schema();

        let fut = get_stream(Arc::clone(&self.pool), sql, Arc::clone(&schema));

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Executes a SQL query and returns a stream of record batches.
///
/// # Errors
///
/// Returns an error if the connection fails or the query execution fails.
pub async fn get_stream<T: 'static, P: 'static>(
    pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
    sql: String,
    projected_schema: SchemaRef,
) -> DataFusionResult<SendableRecordBatchStream> {
    let conn = pool.connect().await.map_err(to_execution_error)?;

    query_arrow(conn, sql, Some(projected_schema))
        .await
        .map_err(to_execution_error)
}

#[allow(clippy::needless_pass_by_value)]
pub fn to_execution_error(
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use datafusion::execution::context::SessionContext;
    use datafusion::sql::TableReference;
    use tracing::{level_filters::LevelFilter, subscriber::DefaultGuard, Dispatch};

    use crate::sql::sql_provider_datafusion::SqlTable;

    fn setup_tracing() -> DefaultGuard {
        let subscriber: tracing_subscriber::FmtSubscriber = tracing_subscriber::fmt()
            .with_max_level(LevelFilter::DEBUG)
            .finish();

        let dispatch = Dispatch::new(subscriber);
        tracing::dispatcher::set_default(&dispatch)
    }

    #[test]
    fn test_references() {
        let table_ref = TableReference::bare("test");
        assert_eq!(format!("{table_ref}"), "test");
    }

    #[cfg(feature = "duckdb")]
    mod duckdb_tests {
        use super::*;
        use crate::sql::db_connection_pool::dbconnection::duckdbconn::{
            DuckDBSyncParameter, DuckDbConnection,
        };
        use crate::sql::db_connection_pool::{duckdbpool::DuckDbConnectionPool, DbConnectionPool};
        use duckdb::DuckdbConnectionManager;

        #[tokio::test]
        async fn test_duckdb_table() -> Result<(), Box<dyn Error + Send + Sync>> {
            let t = setup_tracing();
            let ctx = SessionContext::new();
            let pool: Arc<
                dyn DbConnectionPool<
                        r2d2::PooledConnection<DuckdbConnectionManager>,
                        Box<dyn DuckDBSyncParameter>,
                    > + Send
                    + Sync,
            > = Arc::new(DuckDbConnectionPool::new_memory()?)
                as Arc<
                    dyn DbConnectionPool<
                            r2d2::PooledConnection<DuckdbConnectionManager>,
                            Box<dyn DuckDBSyncParameter>,
                        > + Send
                        + Sync,
                >;
            let conn = pool.connect().await?;
            #[allow(clippy::expect_used)]
            let db_conn = conn
                .as_any()
                .downcast_ref::<DuckDbConnection>()
                .expect("Unable to downcast to DuckDbConnection");
            db_conn.conn.execute_batch(
                "CREATE TABLE test (a INTEGER, b VARCHAR); INSERT INTO test VALUES (3, 'bar');",
            )?;
            let duckdb_table = SqlTable::new("duckdb", &pool, "test", None).await?;
            ctx.register_table("test_datafusion", Arc::new(duckdb_table))?;
            let sql = "SELECT * FROM test_datafusion limit 1";
            let df = ctx.sql(sql).await?;
            df.show().await?;
            drop(t);
            Ok(())
        }

        #[tokio::test]
        async fn test_duckdb_table_filter() -> Result<(), Box<dyn Error + Send + Sync>> {
            let t = setup_tracing();
            let ctx = SessionContext::new();
            let pool: Arc<
                dyn DbConnectionPool<
                        r2d2::PooledConnection<DuckdbConnectionManager>,
                        Box<dyn DuckDBSyncParameter>,
                    > + Send
                    + Sync,
            > = Arc::new(DuckDbConnectionPool::new_memory()?)
                as Arc<
                    dyn DbConnectionPool<
                            r2d2::PooledConnection<DuckdbConnectionManager>,
                            Box<dyn DuckDBSyncParameter>,
                        > + Send
                        + Sync,
                >;
            let conn = pool.connect().await?;
            #[allow(clippy::expect_used)]
            let db_conn = conn
                .as_any()
                .downcast_ref::<DuckDbConnection>()
                .expect("Unable to downcast to DuckDbConnection");
            db_conn.conn.execute_batch(
                "CREATE TABLE test (a INTEGER, b VARCHAR); INSERT INTO test VALUES (3, 'bar');",
            )?;
            let duckdb_table = SqlTable::new("duckdb", &pool, "test", None).await?;
            ctx.register_table("test_datafusion", Arc::new(duckdb_table))?;
            let sql = "SELECT * FROM test_datafusion where a > 1 and b = 'bar' limit 1";
            let df = ctx.sql(sql).await?;
            df.show().await?;
            drop(t);
            Ok(())
        }
    }
}
