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
    logical_expr::{Expr, TableType},
    physical_plan::{
        memory::MemoryStream, project_schema, DisplayAs, DisplayFormatType, ExecutionPlan,
        SendableRecordBatchStream,
    },
    sql::TableReference,
};

#[derive(Debug, Snafu)]
pub enum Error {
    UnableToGetConnectionFromPool { source: r2d2::Error },

    UnableToQueryDuckDB { source: duckdb::Error },

    ResultExpected,
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDBTable {
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
    schema: SchemaRef,
    table_reference: OwnedTableReference,
}

impl DuckDBTable {
    pub fn new(
        pool: impl Into<r2d2::Pool<DuckdbConnectionManager>>,
        table_reference: impl Into<OwnedTableReference>,
    ) -> Result<Self> {
        let pool = pool.into();
        let table_reference = table_reference.into();
        let schema = Self::get_schema(&pool, &table_reference)?;
        Ok(Self {
            pool: Arc::new(pool),
            schema,
            table_reference,
        })
    }

    pub fn new_with_schema(
        pool: impl Into<r2d2::Pool<DuckdbConnectionManager>>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<OwnedTableReference>,
    ) -> Self {
        Self {
            pool: Arc::new(pool.into()),
            schema: schema.into(),
            table_reference: table_reference.into(),
        }
    }

    pub fn get_schema<'a>(
        pool: impl Into<&'a r2d2::Pool<DuckdbConnectionManager>>,
        table_reference: impl Into<TableReference<'a>>,
    ) -> Result<SchemaRef> {
        let pool = pool.into();
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
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DuckDBExec::new(
            projections,
            schema,
            &self.table_reference,
            Arc::clone(&self.pool),
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

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, &self.schema(), limit);
    }
}

#[derive(Clone)]
struct DuckDBExec {
    projected_schema: SchemaRef,
    table_reference: OwnedTableReference,
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
    limit: Option<usize>,
}

impl DuckDBExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &OwnedTableReference,
        pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema(schema, projections)?;
        Ok(Self {
            projected_schema,
            table_reference: table_reference.clone(),
            pool,
            limit,
        })
    }
}

impl std::fmt::Debug for DuckDBExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBExec")
    }
}

impl DisplayAs for DuckDBExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDB")
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

        let sql = format!(
            "SELECT {columns} FROM {table_reference} {limit_expr}",
            table_reference = self.table_reference,
        );
        println!("sql: {sql}");

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

    use crate::DuckDBTable;

    #[tokio::test]
    async fn test_duckdb_table() -> Result<(), Box<dyn Error>> {
        let ctx = SessionContext::new();

        let conn = DuckdbConnectionManager::memory()?;

        let pool = r2d2::Pool::new(conn)?;

        let db_conn = pool.get()?;
        db_conn.execute_batch(
            "CREATE TABLE test (a INTEGER, b VARCHAR); INSERT INTO test VALUES (3, 'bar');",
        )?;

        let duckdb_table = DuckDBTable::new(pool, "test")?;

        ctx.register_table("test_datafusion", Arc::new(duckdb_table))?;

        let sql = "SELECT * FROM test_datafusion limit 1";
        let df = ctx.sql(sql).await?;

        df.show().await?;

        Ok(())
    }
}
