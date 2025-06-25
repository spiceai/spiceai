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

use std::{any::Any, fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    logical_expr::Expr,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    sql::TableReference,
};

pub type Result<T, E = super::Error> = std::result::Result<T, E>;

use oracle::Row;
use snafu::ResultExt;

use crate::oracle::{QuerySnafu, connection::OracleConnectionPool, convert::rows_to_arrow};
use futures::{StreamExt, TryStreamExt};

#[derive(Clone)]
pub struct OracleExecPlan {
    projected_schema: SchemaRef,
    table_reference: TableReference,
    pool: Arc<OracleConnectionPool>,
    _filters: Vec<Expr>,
    _limit: Option<usize>,
    properties: PlanProperties,
}

impl OracleExecPlan {
    pub fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &TableReference,
        pool: Arc<OracleConnectionPool>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema_safe(schema, projections)?;

        Ok(Self {
            projected_schema: Arc::clone(&projected_schema),
            table_reference: table_reference.clone(),
            pool,
            _filters: filters.to_vec(),
            _limit: limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        })
    }

    #[allow(clippy::unnecessary_wraps)]
    pub fn sql(&self) -> DataFusionResult<String> {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>()
            .join(", ");

        Ok(format!(
            "SELECT {columns} FROM {table_reference}",
            table_reference = self.table_reference
        ))
    }
}

impl std::fmt::Debug for OracleExecPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "OracleExec sql={sql}")
    }
}

impl DisplayAs for OracleExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "OracleExec sql={sql}")
    }
}

impl ExecutionPlan for OracleExecPlan {
    fn name(&self) -> &'static str {
        "OracleExec"
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
        tracing::debug!("OracleExecPlan sql: {sql}");

        let schema = self.schema();

        let fut = query_arrow(Arc::clone(&self.pool), sql, Arc::clone(&schema));

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

async fn query_arrow(
    pool: Arc<OracleConnectionPool>,
    sql: String,
    projected_schema: SchemaRef,
) -> DataFusionResult<SendableRecordBatchStream> {
    let schema = Arc::clone(&projected_schema);

    let conn = pool.get().await.map_err(to_datafusion_err)?;

    let query_res = conn
        .query(&sql, &[])
        .context(QuerySnafu)
        .map_err(to_datafusion_err)?;

    let stream = futures::stream::iter(query_res).chunks(4_000).boxed();

    let stream = stream.map(move |rows| {
        let rows: Vec<Row> = rows
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_execution_error)?;

        rows_to_arrow(&rows, &schema).map_err(to_datafusion_err)
    });

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        projected_schema,
        stream,
    )))
}

fn project_schema_safe(
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

#[allow(clippy::needless_pass_by_value)]
pub fn to_execution_error(
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}

fn to_datafusion_err(e: super::Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}
