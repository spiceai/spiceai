use crate::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use crate::sql::db_connection_pool::DbConnectionPool;
use crate::sql::sql_provider_datafusion::expr::Engine;
use crate::util::supported_functions::FunctionSupport;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Constraints;
use datafusion::sql::unparser::dialect::MySqlDialect;
use futures::TryStreamExt;
use mysql_async::prelude::ToValue;
use std::fmt::Display;
use std::{any::Any, fmt, sync::Arc};

use crate::sql::sql_provider_datafusion::{
    self, get_stream, to_execution_error, Result as SqlResult, SqlExec, SqlTable,
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableProvider,
    error::Result as DataFusionResult,
    execution::TaskContext,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties, SendableRecordBatchStream,
    },
    sql::TableReference,
};

pub struct MySQLTable {
    pool: Arc<MySQLConnectionPool>,
    pub(crate) base_table: SqlTable<mysql_async::Conn, &'static (dyn ToValue + Sync)>,
    pub(crate) function_support: Option<FunctionSupport>,
}

impl std::fmt::Debug for MySQLTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySQLTable")
            .field("base_table", &self.base_table)
            .finish()
    }
}

impl MySQLTable {
    pub async fn new(
        pool: &Arc<MySQLConnectionPool>,
        table_reference: impl Into<TableReference>,
        constraints: Option<Constraints>,
        function_support: Option<FunctionSupport>,
    ) -> Result<Self, sql_provider_datafusion::Error> {
        let dyn_pool = Arc::clone(pool)
            as Arc<
                dyn DbConnectionPool<mysql_async::Conn, &'static (dyn ToValue + Sync)>
                    + Send
                    + Sync,
            >;
        let base_table = SqlTable::new("mysql", &dyn_pool, table_reference, None)
            .await?
            .with_dialect(Arc::new(MySqlDialect {}))
            .with_constraints_opt(constraints);

        Ok(Self {
            pool: Arc::clone(pool),
            base_table,
            function_support,
        })
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MySQLSQLExec::new(
            projections,
            schema,
            &self.base_table.table_reference,
            Arc::clone(&self.pool),
            filters,
            limit,
        )?))
    }
}

#[async_trait]
impl TableProvider for MySQLTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.base_table.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.base_table.constraints()
    }

    fn table_type(&self) -> TableType {
        self.base_table.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.base_table.supports_filters_pushdown(filters)
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

impl Display for MySQLTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MySQLTable {}", self.base_table.name())
    }
}

struct MySQLSQLExec {
    base_exec: SqlExec<mysql_async::Conn, &'static (dyn ToValue + Sync)>,
}

impl MySQLSQLExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &TableReference,
        pool: Arc<MySQLConnectionPool>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let base_exec = SqlExec::new(
            projections,
            schema,
            table_reference,
            pool,
            filters,
            limit,
            Some(Engine::MySQL),
        )?;

        Ok(Self { base_exec })
    }

    fn sql(&self) -> SqlResult<String> {
        self.base_exec.sql()
    }
}

impl std::fmt::Debug for MySQLSQLExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "MySQLSQLExec sql={sql}")
    }
}

impl DisplayAs for MySQLSQLExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "MySQLSQLExec sql={sql}")
    }
}

impl ExecutionPlan for MySQLSQLExec {
    fn name(&self) -> &'static str {
        "MySQLSQLExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.base_exec.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.base_exec.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.base_exec.children()
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
        tracing::debug!("MySQLSQLExec sql: {sql}");

        let fut = get_stream(self.base_exec.clone_pool(), sql, Arc::clone(&self.schema()));

        let stream = futures::stream::once(fut).try_flatten();
        let schema = Arc::clone(&self.schema());
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
