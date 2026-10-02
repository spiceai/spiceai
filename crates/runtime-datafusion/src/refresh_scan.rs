/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Builds and executes the scan a refresh reads its source data through.
//!
//! Either projects every column of the source table provider, or plans the
//! dataset's refresh SQL, then applies the refresh's filters and streams the
//! result. Computed columns (e.g. embeddings) are re-attached to a refresh-SQL
//! projection that would otherwise drop them.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use arrow_tools::schema::schema_meta_get_computed_columns;
use datafusion::common::TableReference;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DataFusionError};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::SessionContext;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, ident};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::sql::unparser::Unparser;
use datafusion_table_providers::util::retriable_error::check_and_mark_retriable_error;
use futures::TryStreamExt;
use tracing::Level;

use crate::error::find_datafusion_root;

/// Gets data from a table provider and returns it as a stream of `RecordBatch`es.
///
/// The scan is planned here, but the plan is executed only when the stream is first polled.
/// The scan computes the dataset's indexes as it reads (`IndexerExec`), and executing a
/// multi-partition plan starts reading its partitions at once, so executing it eagerly would
/// index rows before the sink has opened the index write window — rows that window's
/// replace-all clear then deletes (#14619).
///
/// # Errors
///
/// Returns a `DataFusionError` if the scan cannot be planned — an invalid
/// refresh `sql`, a projection the source schema cannot satisfy, an
/// unrepresentable filter. A failure to execute the plan is the stream's first item, marked
/// retriable as the refresh marks a failure to read its source.
pub async fn get_data(
    ctx: &mut SessionContext,
    table_name: TableReference,
    table_provider: Arc<dyn TableProvider>,
    sql: Option<String>,
    filters: Vec<Expr>,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let mut df = match sql {
        None => {
            let table_source = Arc::new(DefaultTableSource::new(Arc::clone(&table_provider)));

            // Get the columns so we can add projection to the plan. This
            // converts the plan to federated where the correct dialect is
            // applied
            let schema = table_provider.schema();
            let columns: Vec<Expr> = schema.fields().iter().map(|f| ident(f.name())).collect();

            let logical_plan = LogicalPlanBuilder::scan(table_name.clone(), table_source, None)
                .map_err(find_datafusion_root)?
                .project(columns)?
                .build()
                .map_err(find_datafusion_root)?;

            DataFrame::new(ctx.state(), logical_plan)
        }
        Some(sql) => {
            let session = ctx.state();
            let mut plan = session
                .create_logical_plan(&sql)
                .await
                .map_err(find_datafusion_root)?;

            // If the refresh SQL defines a subset of columns to fetch, computed columns such as embeddings
            // are not included automatically, so we verify their presence and add them manually if needed.
            plan = include_computed_columns(plan, &table_provider.schema())?;

            DataFrame::new(session, plan)
        }
    };

    for filter in filters {
        df = df.filter(filter).map_err(find_datafusion_root)?;
    }

    if tracing::enabled!(Level::TRACE)
        && let Ok(explained) = df.clone().explain(false, false)
        && let Ok(explained) = explained.to_string().await
    {
        tracing::trace!("Data refresh plan for {}:\n{}", table_name, explained);
    }

    let sql = Unparser::default()
        .plan_to_sql(df.logical_plan())
        .map_err(find_datafusion_root)?;
    tracing::info!(target: "task_history", sql = %sql, "labels");

    let task_ctx = Arc::new(df.task_ctx());
    let plan = df
        .create_physical_plan()
        .await
        .map_err(find_datafusion_root)?;
    Ok(execute_on_first_poll(plan, task_ctx))
}

/// Returns a stream over `plan`'s output that calls [`execute_stream`] only when first polled.
fn execute_on_first_poll(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> SendableRecordBatchStream {
    let schema = plan.schema();
    let stream = futures::stream::once(async move {
        execute_stream(plan, task_ctx)
            .map_err(|e| check_and_mark_retriable_error(find_datafusion_root(e)))
    })
    .try_flatten();
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

/// Ensures that the associated computed columns (e.g., embeddings) are included
/// in the `LogicalPlan::Projection` node.
/// If any required computed columns are missing, they are automatically added to the projection.
fn include_computed_columns(
    plan: LogicalPlan,
    source_table_schema: &SchemaRef,
) -> DataFusionResult<LogicalPlan> {
    let plan = plan
        .transform_down(|plan| {
            match plan {
                LogicalPlan::Projection(mut proj) => {
                    for (idx, col) in proj.schema.columns().iter().enumerate() {
                        if let Some(computed_columns) = schema_meta_get_computed_columns(
                            source_table_schema.as_ref(),
                            col.name(),
                        ) {
                            for computed_column in computed_columns {
                                if !proj
                                    .schema
                                    .has_column_with_unqualified_name(computed_column.name())
                                {
                                    proj.expr.push(Expr::Column(Column::new(
                                        proj.schema.qualified_field(idx).0.cloned(),
                                        computed_column.name().clone(),
                                    )));
                                }
                            }
                        }
                    }
                    // The Transformed flag is not used, so we always specify it as transformed for simplicity.
                    Ok(Transformed::yes(LogicalPlan::Projection(proj)))
                }
                _ => Ok(Transformed::no(plan)),
            }
        })?
        .data;

    Ok(plan)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::catalog::streaming::StreamingTable;
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion::physical_plan::streaming::PartitionStream;

    use super::*;

    const PARTITIONS: usize = 2;

    /// One partition of a source scan, counting how many partitions have been executed.
    #[derive(Debug)]
    struct CountingPartition {
        schema: SchemaRef,
        id: i64,
        executed: Arc<AtomicUsize>,
    }

    impl PartitionStream for CountingPartition {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            self.executed.fetch_add(1, Ordering::SeqCst);
            let batch = RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![Arc::new(Int64Array::from(vec![self.id]))],
            )
            .expect("build the partition's batch");
            Box::pin(
                MemoryStream::try_new(vec![batch], Arc::clone(&self.schema), None)
                    .expect("build the partition's stream"),
            )
        }
    }

    /// Executing a multi-partition plan starts reading every partition at once, and a refresh
    /// scan computes the dataset's indexes as it reads. So `get_data` must hand back a stream
    /// that has not executed anything: the sink opens the index write window before its first
    /// poll, and rows indexed earlier would be deleted by that window's replace-all clear.
    ///
    /// Regression test for #14619.
    #[tokio::test]
    async fn get_data_executes_the_source_scan_only_when_first_polled() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let executed = Arc::new(AtomicUsize::new(0));
        let partitions = (0..PARTITIONS)
            .map(|id| {
                Arc::new(CountingPartition {
                    schema: Arc::clone(&schema),
                    id: i64::try_from(id).expect("partition id fits in i64"),
                    executed: Arc::clone(&executed),
                }) as Arc<dyn PartitionStream>
            })
            .collect();
        let table: Arc<dyn TableProvider> = Arc::new(
            StreamingTable::try_new(Arc::clone(&schema), partitions).expect("build the source"),
        );
        let mut ctx = SessionContext::new();

        let stream = get_data(&mut ctx, TableReference::bare("docs"), table, None, vec![])
            .await
            .expect("plan the refresh scan");
        // Executing a multi-partition plan spawns one task per partition. On this
        // single-threaded test runtime they run only once the test yields, so yield before
        // looking: an eagerly executed scan has executed its partitions by then.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            executed.load(Ordering::SeqCst),
            0,
            "the source scan started before the stream was polled"
        );

        let batches: Vec<RecordBatch> = stream.try_collect().await.expect("read the scan");
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, PARTITIONS, "every partition's row is read");
        assert_eq!(executed.load(Ordering::SeqCst), PARTITIONS);
    }
}
