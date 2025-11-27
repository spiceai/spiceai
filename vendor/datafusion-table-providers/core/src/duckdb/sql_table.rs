use crate::sql::db_connection_pool::DbConnectionPool;
use crate::sql::sql_provider_datafusion::expr::Engine;
use crate::sql::sql_provider_datafusion::{
    get_stream, to_execution_error, Result as SqlResult, SqlExec, SqlTable,
};
use crate::util::column_reference::ColumnReference;
use crate::util::indexes::IndexType;
use crate::util::supported_functions::FunctionSupport;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Constraints;
use datafusion::sql::unparser::dialect::Dialect;
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
    sql::{unparser::dialect::DuckDBDialect, TableReference},
};
use futures::TryStreamExt;
use std::collections::HashMap;
use std::fmt::Display;
use std::{any::Any, fmt, sync::Arc};

pub struct DuckDBTable<T: 'static, P: 'static> {
    pub(crate) base_table: SqlTable<T, P>,

    /// A mapping of table/view names to `DuckDB` functions that can instantiate a table (e.g. "`read_parquet`('`my_file.parquet`')").
    pub(crate) table_functions: Option<HashMap<String, String>>,

    pub(crate) function_support: Option<FunctionSupport>,

    /// A list of indexes as expressed by columns reference in their index expressions.
    pub(crate) indexes: Vec<(ColumnReference, IndexType)>,
}

impl<T, P> std::fmt::Debug for DuckDBTable<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDBTable")
            .field("base_table", &self.base_table)
            .finish()
    }
}

impl<T, P> DuckDBTable<T, P> {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_schema(
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<TableReference>,
        table_functions: Option<HashMap<String, String>>,
        dialect: Option<Arc<dyn Dialect + Send + Sync>>,
        constraints: Option<Constraints>,
        function_support: Option<FunctionSupport>,
        indexes: Vec<(ColumnReference, IndexType)>,
    ) -> Self {
        let base_table = SqlTable::new_with_schema(
            "duckdb",
            pool,
            schema,
            table_reference,
            Some(Engine::DuckDB),
        )
        .with_dialect(dialect.unwrap_or(Arc::new(DuckDBDialect::new())))
        .with_constraints_opt(constraints)
        .with_function_support(function_support.clone());

        Self {
            base_table,
            table_functions,
            function_support,
            indexes,
        }
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DuckSqlExec::new(
            projections,
            schema,
            &self.base_table.table_reference,
            self.base_table.clone_pool(),
            filters,
            limit,
            self.table_functions.clone(),
            self.indexes.clone(),
        )?))
    }
}

#[async_trait]
impl<T, P> TableProvider for DuckDBTable<T, P> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.base_table.schema()
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

    fn constraints(&self) -> Option<&Constraints> {
        self.base_table.constraints()
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

impl<T, P> Display for DuckDBTable<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DuckDBTable {}", self.base_table.name())
    }
}

pub struct DuckSqlExec<T, P> {
    base_exec: SqlExec<T, P>,
    table_functions: Option<HashMap<String, String>>,
    indexes: Vec<(ColumnReference, IndexType)>,
    optimized_sql: Option<String>,
    optimized_sql_schema: Option<SchemaRef>,
}

impl<T, P> Clone for DuckSqlExec<T, P> {
    fn clone(&self) -> Self {
        DuckSqlExec {
            base_exec: self.base_exec.clone(),
            table_functions: self.table_functions.clone(),
            indexes: self.indexes.clone(),
            optimized_sql: self.optimized_sql.clone(),
            optimized_sql_schema: self.optimized_sql_schema.clone(),
        }
    }
}

impl<T, P> DuckSqlExec<T, P> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &TableReference,
        pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        filters: &[Expr],
        limit: Option<usize>,
        table_functions: Option<HashMap<String, String>>,
        indexes: Vec<(ColumnReference, IndexType)>,
    ) -> DataFusionResult<Self> {
        let base_exec = SqlExec::new(
            projections,
            schema,
            table_reference,
            pool,
            filters,
            limit,
            Some(Engine::DuckDB),
        )?;

        Ok(Self {
            base_exec,
            table_functions,
            indexes,
            optimized_sql: None,
            optimized_sql_schema: None,
        })
    }

    /// The SQL expression for this execution node. This may differ from `DuckSqlExec::base_sql`
    /// if rewritten by an optimization step.
    pub fn sql(&self) -> SqlResult<String> {
        if let Some(sql) = &self.optimized_sql {
            return Ok(sql.clone());
        }

        let sql = self.base_exec.sql()?;

        Ok(format!(
            "{cte_expr} {sql}",
            cte_expr = get_cte(&self.table_functions)
        ))
    }

    /// Indexes that may be bound for the SQL expression in this execution node
    pub fn indexes(&self) -> &Vec<(ColumnReference, IndexType)> {
        self.indexes.as_ref()
    }

    /// The unoptimized SQL expression for this execution node
    pub fn base_sql(&self) -> SqlResult<String> {
        self.base_exec.sql()
    }

    /// Use this method to bind an optimized SQL expression from a `PhysicalOptimizerRule`. Provide
    /// a `new_schema` if changing the output schema of this node.
    pub fn with_optimized_sql(
        mut self,
        sql: impl Into<String>,
        new_schema: Option<SchemaRef>,
    ) -> Self {
        self.optimized_sql = Some(sql.into());
        self.optimized_sql_schema = new_schema;
        self
    }
}

impl<T, P> std::fmt::Debug for DuckSqlExec<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "DuckSqlExec sql={sql}")
    }
}

impl<T, P> DisplayAs for DuckSqlExec<T, P> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "DuckSqlExec sql={sql}")
    }
}

impl<T: 'static, P: 'static> ExecutionPlan for DuckSqlExec<T, P> {
    fn name(&self) -> &'static str {
        "DuckSqlExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.optimized_sql_schema
            .as_ref()
            .map(Arc::clone)
            .unwrap_or(self.base_exec.schema())
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
        tracing::debug!("DuckSqlExec sql: {sql}");

        let schema = self.schema();

        let fut = get_stream(self.base_exec.clone_pool(), sql, Arc::clone(&schema));

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Create CTE expressions for all the table functions.
pub(crate) fn get_cte(table_functions: &Option<HashMap<String, String>>) -> String {
    if let Some(table_fn) = table_functions {
        let inner = table_fn
            .iter()
            .map(|(name, r#fn)| format!("{name} AS (SELECT * FROM {fn})"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("WITH {inner} ")
    } else {
        String::new()
    }
}
