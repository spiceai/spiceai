/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::{any::Any, sync::Arc};

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{EquivalenceProperties, Partitioning},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    prelude::Expr,
};
use datafusion_table_providers::duckdb::DuckDB;
use futures::TryStreamExt;
use llms::embeddings::{Embed, EmbeddingInput};

use super::{
    DuckDBVectorQueryContext,
    hnsw::DuckDBHnswOptions,
    resolve_current_table_name,
    sql::{EMPTY_PROJECTION_ROW_COLUMN, duckdb_vector_sql},
    to_execution_error, validate_vector, vector_literal,
};

#[derive(Debug, Clone)]
pub(super) struct DuckDBVectorQueryExec {
    pub(super) projected_schema: SchemaRef,
    pub(super) projected_columns: Vec<String>,
    pub(super) filters: Vec<Expr>,
    pub(super) limit: Option<usize>,
    pub(super) query_text: String,
    pub(super) embedded_column: String,
    pub(super) compute_query: Arc<dyn Embed>,
    pub(super) dims: i32,
    pub(super) hnsw: DuckDBHnswOptions,
    pub(super) context: DuckDBVectorQueryContext,
    pub(super) properties: PlanProperties,
}

impl DuckDBVectorQueryExec {
    async fn query_vector(&self) -> DataFusionResult<Vec<f32>> {
        let mut vectors = self
            .compute_query
            .embed(EmbeddingInput::String(self.query_text.clone()))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let vector = vectors.pop().ok_or_else(|| {
            DataFusionError::Execution("No embedding vector computed for query".to_string())
        })?;
        validate_vector(&vector, self.dims, "query")?;
        Ok(vector)
    }

    fn sql(&self, table_name: &str, query_vector: &[f32]) -> DataFusionResult<String> {
        let vector_lit = vector_literal(query_vector, self.dims)?;
        duckdb_vector_sql(
            table_name,
            &self.embedded_column,
            &self.projected_columns,
            &self.filters,
            self.limit,
            &self.hnsw,
            &vector_lit,
        )
    }
}

/// # Examples
///
/// Without filters (HNSW index scan):
/// ```text
/// DuckDBVectorQueryExec embedded_column=body_embedding, metric=cosine, dims=1536, limit=10, scan=hnsw, projection=[id, title, _score]
/// ```
///
/// With filters (brute-force scan):
/// ```text
/// DuckDBVectorQueryExec embedded_column=body_embedding, metric=l2sq, dims=768, filters=[category = news], limit=50, scan=brute_force, projection=[id, _score]
/// ```
impl DisplayAs for DuckDBVectorQueryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DuckDBVectorQueryExec embedded_column={}, metric={}, dims={}",
            self.embedded_column,
            self.hnsw.metric.duckdb_hnsw_metric(),
            self.dims,
        )?;
        if !self.filters.is_empty() {
            write!(f, ", filters=[")?;
            for (i, filter) in self.filters.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{filter}")?;
            }
            write!(f, "]")?;
        }

        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }

        write!(
            f,
            ", scan={}, projection=[{}]",
            if self.filters.is_empty() {
                "hnsw"
            } else {
                "brute_force"
            },
            self.projected_columns.join(", "),
        )?;

        Ok(())
    }
}

impl ExecutionPlan for DuckDBVectorQueryExec {
    fn name(&self) -> &'static str {
        "DuckDBVectorQueryExec"
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
        let exec = self.clone();
        let schema = self.schema();
        let fut = async move {
            let query_vector = exec.query_vector().await?;
            let context = exec.context.clone();
            let sql_exec = exec.clone();
            let batches = tokio::task::spawn_blocking(move || {
                run_duckdb_vector_query(&context, &sql_exec, &query_vector)
            })
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("DuckDB vector query task failed: {e}"))
            })??;
            let stream =
                futures::stream::iter(batches.into_iter().map(Ok::<RecordBatch, DataFusionError>));
            Ok::<SendableRecordBatchStream, DataFusionError>(Box::pin(
                RecordBatchStreamAdapter::new(schema, stream),
            ))
        };
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

fn run_duckdb_vector_query(
    context: &DuckDBVectorQueryContext,
    exec: &DuckDBVectorQueryExec,
    query_vector: &[f32],
) -> DataFusionResult<Vec<RecordBatch>> {
    let mut db_conn = Arc::clone(&context.pool)
        .connect_sync()
        .map_err(to_execution_error)?;
    let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_execution_error)?;
    let conn = &duckdb_conn.conn;

    let table_name = resolve_current_table_name(context.table_definition.name(), conn)?;
    let sql = exec.sql(&table_name, query_vector)?;
    tracing::debug!(
        table_name = %table_name,
        embedded_column = %exec.embedded_column,
        query_vector_dimension = query_vector.len(),
        "Executing DuckDB vector query"
    );
    tracing::trace!("DuckDB vector query SQL: {sql}");
    let mut stmt = conn.prepare(&sql).map_err(to_execution_error)?;
    let result = stmt.query_arrow([]).map_err(to_execution_error)?;
    let batches = result.collect::<Vec<_>>();
    if exec.projected_columns.is_empty() {
        batches
            .into_iter()
            .map(|batch| {
                empty_projected_batch(Arc::clone(&exec.projected_schema), batch.num_rows())
            })
            .collect()
    } else {
        batches
            .into_iter()
            .map(normalize_fixed_size_list_field_names)
            .collect()
    }
}

/// Re-cast a batch's schema to match `target_schema`.
///
/// DuckDB serializes `FLOAT[N]` columns with an empty inner field name (`field: ''`),
/// whereas Arrow conventionally uses `"item"`. This restores the `"item"` name on any
/// `FixedSizeList` column whose inner field came back with an empty name.
fn normalize_fixed_size_list_field_names(batch: RecordBatch) -> DataFusionResult<RecordBatch> {
    let schema = batch.schema();
    let needs_fix = schema.fields().iter().any(
        |f| matches!(f.data_type(), DataType::FixedSizeList(inner, _) if inner.name().is_empty()),
    );
    if !needs_fix {
        return Ok(batch);
    }

    let new_fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::FixedSizeList(inner, size) if inner.name().is_empty() => {
                let fixed_inner = Arc::new(Field::new(
                    "item",
                    inner.data_type().clone(),
                    inner.is_nullable(),
                ));
                Arc::new(Field::new(
                    f.name(),
                    DataType::FixedSizeList(fixed_inner, *size),
                    f.is_nullable(),
                ))
            }
            _ => Arc::clone(f),
        })
        .collect();
    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, batch.columns().to_vec())
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn empty_projected_batch(schema: SchemaRef, row_count: usize) -> DataFusionResult<RecordBatch> {
    RecordBatch::try_new_with_options(
        schema,
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_projected_batch_preserves_row_count() {
        let schema = Arc::new(Schema::empty());
        let batch = empty_projected_batch(schema, 3).expect("batch should build");

        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 3);
    }
}
