#![allow(clippy::missing_errors_doc)]

use async_trait::async_trait;
use duckdb::DuckdbConnectionManager;
use snafu::prelude::*;
use std::{any::Any, fmt, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::OwnedTableReference,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{context::SessionState, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::{
        memory::MemoryStream, project_schema, DisplayAs, DisplayFormatType, ExecutionPlan,
        SendableRecordBatchStream,
    },
    sql::TableReference,
};

mod expr;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get a DuckDB connection from the pool: {}", source))]
    UnableToGetConnectionFromPool { source: r2d2::Error },

    #[snafu(display("Unable to query DuckDB: {}", source))]
    UnableToQueryDuckDB { source: duckdb::Error },

    #[snafu(display("Unable to generate SQL: {}", source))]
    UnableToGenerateSQL { source: expr::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDBTable {
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
    schema: SchemaRef,
    table_reference: OwnedTableReference,
}

impl DuckDBTable {
    pub fn new(
        pool: &Arc<r2d2::Pool<DuckdbConnectionManager>>,
        table_reference: impl Into<OwnedTableReference>,
    ) -> Result<Self> {
        let table_reference = table_reference.into();
        let schema = Self::get_schema(pool, &table_reference)?;
        Ok(Self {
            pool: Arc::clone(pool),
            schema,
            table_reference,
        })
    }

    pub fn new_with_schema(
        pool: &Arc<r2d2::Pool<DuckdbConnectionManager>>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<OwnedTableReference>,
    ) -> Self {
        Self {
            pool: Arc::clone(pool),
            schema: schema.into(),
            table_reference: table_reference.into(),
        }
    }

    pub fn get_schema<'a>(
        pool: &Arc<r2d2::Pool<DuckdbConnectionManager>>,
        table_reference: impl Into<TableReference<'a>>,
    ) -> Result<SchemaRef> {
        let table_reference = table_reference.into();
        let conn = pool.get().context(UnableToGetConnectionFromPoolSnafu)?;
        let mut stmt = conn
            .prepare(&format!("SELECT * FROM {table_reference} LIMIT 0"))
            .context(UnableToQueryDuckDBSnafu)?;

        let result: duckdb::Arrow<'_> = stmt.query_arrow([]).context(UnableToQueryDuckDBSnafu)?;

        Ok(result.get_schema())
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DuckDBExec::new(
            projections,
            schema,
            &self.table_reference,
            Arc::clone(&self.pool),
            filters,
            limit,
        )?))
    }
}

#[async_trait]
impl TableProvider for DuckDBTable {
    fn as_any(&self) -> &dyn Any {
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
            match expr::expr_to_sql(filter) {
                Ok(_) => filter_push_down.push(TableProviderFilterPushDown::Exact),
                Err(_) => filter_push_down.push(TableProviderFilterPushDown::Unsupported),
            }
        }

        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, &self.schema(), filters, limit);
    }
}

#[derive(Clone)]
struct DuckDBExec {
    projected_schema: SchemaRef,
    table_reference: OwnedTableReference,
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl DuckDBExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &OwnedTableReference,
        pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema(schema, projections)?;
        Ok(Self {
            projected_schema,
            table_reference: table_reference.clone(),
            pool,
            filters: filters.to_vec(),
            limit,
        })
    }

    fn sql(&self) -> Result<String> {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join(", ");

        let limit_expr = match self.limit {
            Some(limit) => format!("LIMIT {limit}"),
            None => String::new(),
        };

        let where_expr = if self.filters.is_empty() {
            String::new()
        } else {
            let filter_expr = self
                .filters
                .iter()
                .map(expr::expr_to_sql)
                .collect::<expr::Result<Vec<_>>>()
                .context(UnableToGenerateSQLSnafu)?;
            format!("WHERE {}", filter_expr.join(" AND "))
        };

        Ok(format!(
            "SELECT {columns} FROM {table_reference} {where_expr} {limit_expr}",
            table_reference = self.table_reference,
        ))
    }
}

impl std::fmt::Debug for DuckDBExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "DuckDBExec sql={sql}")
    }
}

impl DisplayAs for DuckDBExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "DuckDB sql={sql}")
    }
}

impl ExecutionPlan for DuckDBExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
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
        let conn = self.pool.get().map_err(to_execution_error)?;

        let sql = self.sql().map_err(to_execution_error)?;
        tracing::debug!("duckdb sql: {sql}");

        let mut stmt = conn.prepare(&sql).map_err(to_execution_error)?;

        let result: duckdb::Arrow<'_> = stmt.query_arrow([]).map_err(to_execution_error)?;
        let recs = result.collect::<Vec<_>>();

        Ok(Box::pin(MemoryStream::try_new(recs, self.schema(), None)?))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn to_execution_error(e: impl Into<Box<dyn std::error::Error>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use datafusion::execution::context::SessionContext;
    use duckdb::DuckdbConnectionManager;
    use tracing::{level_filters::LevelFilter, subscriber::DefaultGuard, Dispatch};

    use crate::DuckDBTable;

    fn setup_tracing() -> DefaultGuard {
        let subscriber: tracing_subscriber::FmtSubscriber = tracing_subscriber::fmt()
            .with_max_level(LevelFilter::DEBUG)
            .finish();

        let dispatch = Dispatch::new(subscriber);
        tracing::dispatcher::set_default(&dispatch)
    }

    #[tokio::test]
    async fn test_duckdb_table() -> Result<(), Box<dyn Error>> {
        let t = setup_tracing();
        let ctx = SessionContext::new();
        let conn = DuckdbConnectionManager::memory()?;
        let pool = Arc::new(r2d2::Pool::new(conn)?);
        let db_conn = pool.get()?;
        db_conn.execute_batch(
            "CREATE TABLE test (a INTEGER, b VARCHAR); INSERT INTO test VALUES (3, 'bar');",
        )?;
        let duckdb_table = DuckDBTable::new(&pool, "test")?;
        ctx.register_table("test_datafusion", Arc::new(duckdb_table))?;
        let sql = "SELECT * FROM test_datafusion limit 1";
        let df = ctx.sql(sql).await?;
        df.show().await?;
        drop(t);
        Ok(())
    }

    #[tokio::test]
    async fn test_duckdb_table_filter() -> Result<(), Box<dyn Error>> {
        let t = setup_tracing();
        let ctx = SessionContext::new();
        let conn = DuckdbConnectionManager::memory()?;
        let pool = Arc::new(r2d2::Pool::new(conn)?);
        let db_conn = pool.get()?;
        db_conn.execute_batch(
            "CREATE TABLE test (a INTEGER, b VARCHAR); INSERT INTO test VALUES (3, 'bar');",
        )?;
        let duckdb_table = DuckDBTable::new(&pool, "test")?;
        ctx.register_table("test_datafusion", Arc::new(duckdb_table))?;
        let sql = "SELECT * FROM test_datafusion where a > 1 and b = 'bar' limit 1";
        let df = ctx.sql(sql).await?;
        df.show().await?;
        drop(t);
        Ok(())
    }
}
