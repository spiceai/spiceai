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

//! Finite-stream accelerator appends. Callers own replacement predicates,
//! mutation serialization, publication completion, and source acknowledgements.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::{ExecutionPlan, collect};
use runtime_acceleration::dataupdate::StreamingDataUpdateExecutionPlan;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;

/// Finds a native append target without bypassing a write-transforming wrapper.
/// This walk must not be used to bypass wrapper-owned delete/index side effects.
#[cfg(not(windows))]
pub(crate) fn cayenne_append_target(
    accelerator: &dyn TableProvider,
) -> Option<&cayenne::CayenneTableProvider> {
    spice_table::find_concrete(accelerator, spice_table::LayerWalk::Write)
}

/// A dataset-owned generic append plan, keyed by the storage schema.
///
/// The owner must serialize appends and replacement deletes under the same
/// accelerator mutation lock. An input must already have the storage schema;
/// the cast plan preserves the accelerator's execution-time coercions.
pub(crate) struct AppendPlanCache {
    target_schema: SchemaRef,
    streaming_plan: Arc<StreamingDataUpdateExecutionPlan>,
    insert_plan: Arc<dyn ExecutionPlan>,
}

impl AppendPlanCache {
    async fn try_new(
        accelerator: &Arc<dyn TableProvider>,
        session_state: &SessionState,
        target_schema: SchemaRef,
    ) -> Result<Self> {
        let streaming_plan = Arc::new(StreamingDataUpdateExecutionPlan::new_empty(Arc::clone(
            &target_schema,
        )));
        let streaming_exec: Arc<dyn ExecutionPlan> =
            Arc::<StreamingDataUpdateExecutionPlan>::clone(&streaming_plan);
        let cast_plan: Arc<dyn ExecutionPlan> = Arc::new(SchemaCastScanExec::new(
            streaming_exec,
            Arc::clone(&target_schema),
        ));
        let insert_plan = accelerator
            .insert_into(session_state, cast_plan, InsertOp::Append)
            .await?;

        Ok(Self {
            target_schema,
            streaming_plan,
            insert_plan,
        })
    }

    /// Executes one finite input, rebuilding the plan when its schema changes.
    /// The mutable cache borrow must span execution; sharing a plan's input slot
    /// between concurrent writes would allow one write to consume another's rows.
    pub(crate) async fn append(
        cache: &mut Option<Self>,
        accelerator: &Arc<dyn TableProvider>,
        session_state: &SessionState,
        task_ctx: Arc<TaskContext>,
        input: SendableRecordBatchStream,
    ) -> Result<()> {
        let target_schema = accelerator.schema();
        if cache
            .as_ref()
            .is_some_and(|cached| cached.target_schema.as_ref() != target_schema.as_ref())
        {
            *cache = None;
        }
        let cached = match cache {
            Some(cached) => cached,
            None => cache.insert(Self::try_new(accelerator, session_state, target_schema).await?),
        };
        cached.streaming_plan.set_stream(input)?;
        let result = collect(Arc::clone(&cached.insert_plan), task_ctx).await;
        cached.streaming_plan.clear_stream()?;
        result.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::error::DataFusionError;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    fn schema(column: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            column,
            DataType::Int32,
            false,
        )]))
    }

    fn input(schema: &SchemaRef, value: i32) -> SendableRecordBatchStream {
        let batch = RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int32Array::from(vec![value]))],
        )
        .expect("valid input batch");
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(schema),
            futures::stream::iter([Ok(batch)]),
        ))
    }

    fn table(schema: &SchemaRef) -> Arc<dyn TableProvider> {
        Arc::new(
            MemTable::try_new(Arc::clone(schema), vec![vec![]]).expect("valid empty memory table"),
        )
    }

    async fn values(table: &Arc<dyn TableProvider>, ctx: &SessionContext) -> Vec<i32> {
        let plan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan append destination");
        let batches = collect(plan, ctx.task_ctx())
            .await
            .expect("read appended rows");
        let mut values: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("integer column")
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        values.sort_unstable();
        values
    }

    #[tokio::test]
    async fn cached_plan_consumes_each_input_and_recovers_after_a_stream_error() {
        let ctx = SessionContext::new();
        let schema = schema("id");
        let table = table(&schema);
        let mut cache = None;
        AppendPlanCache::append(
            &mut cache,
            &table,
            &ctx.state(),
            ctx.task_ctx(),
            input(&schema, 3),
        )
        .await
        .expect("first append");
        let failed: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter([Err(DataFusionError::Execution("injected failure".into()))]),
        ));
        AppendPlanCache::append(&mut cache, &table, &ctx.state(), ctx.task_ctx(), failed)
            .await
            .expect_err("the input error must propagate");
        AppendPlanCache::append(
            &mut cache,
            &table,
            &ctx.state(),
            ctx.task_ctx(),
            input(&schema, 7),
        )
        .await
        .expect("append after failure");
        assert_eq!(values(&table, &ctx).await, vec![3, 7]);
    }

    #[tokio::test]
    async fn a_changed_storage_schema_rebuilds_the_insert_plan() {
        let ctx = SessionContext::new();
        let original_schema = schema("id");
        let original = table(&original_schema);
        let replacement_schema = schema("replacement_id");
        let replacement = table(&replacement_schema);
        let mut cache = None;
        AppendPlanCache::append(
            &mut cache,
            &original,
            &ctx.state(),
            ctx.task_ctx(),
            input(&original_schema, 3),
        )
        .await
        .expect("original schema append");
        AppendPlanCache::append(
            &mut cache,
            &replacement,
            &ctx.state(),
            ctx.task_ctx(),
            input(&replacement_schema, 7),
        )
        .await
        .expect("changed schema append");
        assert_eq!(values(&original, &ctx).await, vec![3]);
        assert_eq!(values(&replacement, &ctx).await, vec![7]);
    }
}
