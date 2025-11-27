// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::sql::db_connection_pool::DbConnectionPool;

use async_trait::async_trait;
use futures::TryStreamExt;

use crate::sql::sql_provider_datafusion::{
    get_stream, to_execution_error, Result as SqlResult, SqlExec, SqlTable,
};
use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::Session;
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
    sql::{unparser::dialect::Dialect, TableReference},
};

pub struct AdbcDBTable<T: 'static, P: 'static> {
    pub(crate) base_table: SqlTable<T, P>,
}

impl<T, P> std::fmt::Debug for AdbcDBTable<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("AdbcDBTable")
            .field("base_table", &self.base_table)
            .finish()
    }
}

impl<T, P> AdbcDBTable<T, P> {
    pub fn new_with_schema(
        pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<TableReference>,
        dialect: Option<Arc<dyn Dialect + Send + Sync>>,
    ) -> Self {
        let mut base_table = SqlTable::new_with_schema("adbc", pool, schema, table_reference, None);

        if let Some(d) = dialect {
            base_table = base_table.with_dialect(d);
        }
        Self { base_table }
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &TableReference,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AdbcSqlExec::new(
            projections,
            schema,
            table_reference,
            self.base_table.clone_pool(),
            filters,
            limit,
        )?))
    }
}

#[async_trait]
impl<T, P> TableProvider for AdbcDBTable<T, P> {
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

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(
            projection,
            &self.schema(),
            &self.base_table.table_reference,
            filters,
            limit,
        );
    }
}

#[derive(Clone)]
struct AdbcSqlExec<T, P> {
    base_exec: SqlExec<T, P>,
}

impl<T, P> AdbcSqlExec<T, P> {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &TableReference,
        pool: Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
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
            None,
        )?;
        Ok(Self { base_exec })
    }

    fn sql(&self) -> SqlResult<String> {
        let sql = self.base_exec.sql()?;
        Ok(sql)
    }
}

impl<T, P> std::fmt::Debug for AdbcSqlExec<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AdbcSqlExec sql={}", self.sql().unwrap_or_default())
    }
}

impl<T, P> DisplayAs for AdbcSqlExec<T, P> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AdbcSqlExec sql={}", self.sql().unwrap_or_default())
    }
}

impl<T: 'static, P: 'static> ExecutionPlan for AdbcSqlExec<T, P> {
    fn name(&self) -> &'static str {
        "AdbcSqlExec"
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
        tracing::debug!("AdbcSqlExec sql: {sql}");

        let schema = self.schema();

        let fut = get_stream(self.base_exec.clone_pool(), sql, Arc::clone(&schema));

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
