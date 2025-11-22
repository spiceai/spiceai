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

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]

//! Adds telemetry to leaf nodes (i.e. `TableScans`) to track the number of bytes scanned during query execution.
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::common::tree_node::TransformedResult;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::OrderingRequirements;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, InvariantLevel, check_default_invariants,
};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{Distribution, PhysicalExpr, PlanProperties};
use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, stream::RecordBatchStreamAdapter,
    },
};
use futures::{Stream, StreamExt};
use opentelemetry::KeyValue;
use runtime_request_context::{Protocol, RequestContext, RequestContextBuilder};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{any::Any, sync::Arc};

/// A function that receives the number of bytes processed with [`KeyValue`] dimensions from the thread's [`RequestContext`].
pub type BytesEmittedCallback = Box<dyn Fn(u64, &[KeyValue]) + Send + Sync + 'static>;

pub struct BytesProcessedPhysicalOptimizer {
    emit_bytes_callback: Arc<BytesEmittedCallback>,
}

impl BytesProcessedPhysicalOptimizer {
    #[must_use]
    pub fn new(emit_bytes_callback: Arc<BytesEmittedCallback>) -> Self {
        Self {
            emit_bytes_callback,
        }
    }
}

impl std::fmt::Debug for BytesProcessedPhysicalOptimizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BytesProcessedPhysicalOptimizer").finish()
    }
}

impl PhysicalOptimizerRule for BytesProcessedPhysicalOptimizer {
    fn name(&self) -> &'static str {
        "BytesProcessedPhysicalOptimizer"
    }

    fn schema_check(&self) -> bool {
        false
    }

    fn optimize(
        &self,
        plan: std::sync::Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_down(|plan| {
            if plan.as_any().downcast_ref::<BytesProcessedExec>().is_some() {
                return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump));
            }

            if !plan.children().is_empty() {
                return Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue));
            }

            let mut exec_plan =
                BytesProcessedExec::new(plan, Arc::clone(&self.emit_bytes_callback));

            if cfg!(feature = "cluster") {
                exec_plan = exec_plan.fallback_to_new_context();
            }

            Ok(Transformed::new(
                Arc::new(exec_plan),
                true,
                TreeNodeRecursion::Jump,
            ))
        })
        .data()
    }
}

struct BytesProcessedStream {
    inner: SendableRecordBatchStream,
    request_context: Arc<RequestContext>,
    bytes_processed: u64,
    emit_bytes: Arc<BytesEmittedCallback>,
}

impl BytesProcessedStream {
    pub fn new(
        inner: SendableRecordBatchStream,
        request_context: Arc<RequestContext>,
        emit_bytes: Arc<BytesEmittedCallback>,
    ) -> Self {
        Self {
            inner,
            bytes_processed: 0,
            request_context,
            emit_bytes,
        }
    }

    fn emit_bytes_processed(&self) {
        let fnn = &self.emit_bytes;
        fnn(self.bytes_processed, &self.request_context.to_dimensions());
    }
}

impl Stream for BytesProcessedStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let array_size: usize = batch.get_array_memory_size();
                self.bytes_processed += array_size as u64;
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(None) => {
                self.emit_bytes_processed();
                Poll::Ready(None)
            }
            other => other,
        }
    }
}
pub struct BytesProcessedExec {
    input_exec: Arc<dyn ExecutionPlan>,
    emit_bytes_callback: Arc<BytesEmittedCallback>,
    fallback_to_new_context: bool,
}

impl BytesProcessedExec {
    pub fn new(
        input_exec: Arc<dyn ExecutionPlan>,
        emit_bytes_callback: Arc<BytesEmittedCallback>,
    ) -> Self {
        Self {
            input_exec,
            emit_bytes_callback,
            fallback_to_new_context: false,
        }
    }

    #[must_use]
    pub fn fallback_to_new_context(mut self) -> Self {
        self.fallback_to_new_context = true;
        self
    }
}

impl std::fmt::Debug for BytesProcessedExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BytesProcessedExec")
    }
}

impl DisplayAs for BytesProcessedExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "BytesProcessedExec")
            }
        }
    }
}

// if new features are added to ExecutionPlan, we want to know
// it's possible we'll just re-implement the default methods - but that requires attention
// for example, the recently added `gather_filters_for_pushdown` defaults to `all_unsupported` but we likely want `from_children`
#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for BytesProcessedExec {
    fn name(&self) -> &'static str {
        "BytesProcessedExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "BytesProcessedExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.properties().eq_properties.schema())
    }

    fn properties(&self) -> &PlanProperties {
        self.input_exec.properties()
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        check_default_invariants(self, check)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution; self.children().len()]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None; self.children().len()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    /// Prevents the introduction of additional `RepartitionExec` and processing input in parallel.
    /// This guarantees that the input is processed as a single stream, preserving the order of the data.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input_exec]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::External(
                crate::Error::InvalidChildrenCount {
                    children_count: children.len(),
                }
                .into(),
            ));
        }

        let Some(input) = children.into_iter().next() else {
            unreachable!("should have one input");
        };
        Ok(Arc::new(Self {
            input_exec: input,
            emit_bytes_callback: Arc::clone(&self.emit_bytes_callback),
            fallback_to_new_context: self.fallback_to_new_context,
        }))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let children = self.children().into_iter().cloned().collect();
        self.with_new_children(children)
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let stream = self.input_exec.execute(partition, Arc::clone(&context))?;
        let schema = stream.schema();

        let request_context = if let Some(request_context) =
            context.session_config().get_extension::<RequestContext>()
        {
            request_context
        } else if self.fallback_to_new_context {
            Arc::new(RequestContextBuilder::new(Protocol::Internal).build())
        } else {
            // This should never happen if all queries are run through the query builder, so if it does its a bug we need to catch in development.
            panic!(
                "The request context was not provided to BytesProcessedExec, report a bug at https://github.com/spiceai/spiceai/issues"
            )
        };

        let bytes_processed_stream = BytesProcessedStream::new(
            stream,
            request_context,
            Arc::clone(&self.emit_bytes_callback),
        );

        let stream_adapter = RecordBatchStreamAdapter::new(schema, bytes_processed_stream);

        Ok(Box::pin(stream_adapter))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.input_exec.metrics()
    }

    fn statistics(&self) -> Result<Statistics> {
        #[allow(deprecated)]
        self.input_exec.statistics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input_exec.partition_statistics(partition)
    }

    // Allow optimizer to push limits through to inputs
    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn with_new_state(&self, _state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use datafusion::catalog::TableProvider;
    use datafusion::common::Result;
    use datafusion::physical_expr::expressions::col as physical_col;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    use crate::extension::bytes_processed::BytesProcessedExec;

    fn make_test_table() -> Result<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0i64..10000))],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    #[allow(clippy::similar_names)]
    #[tokio::test]
    async fn test_preserve_order_pushdown() -> Result<()> {
        let ctx = SessionContext::new();
        let test_table = make_test_table()?;

        let data_source_exec = test_table.scan(&ctx.state(), None, &[], None).await?;

        let lex_ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new_default(physical_col("id", data_source_exec.schema().as_ref())?)
                .desc()
                .nulls_last(),
        ])
        .expect("could not generate lex ordering");
        let sort_exec = SortExec::new(lex_ordering, data_source_exec);

        let final_plan: Arc<dyn ExecutionPlan> = Arc::new(BytesProcessedExec::new(
            Arc::new(sort_exec),
            Arc::new(Box::new(|_, _| {})),
        ));

        /*
           At this point `final_plan` is:
           ┌───────────────────────────┐
           │     BytesProcessedExec    │
           │    --------------------   │
           │     BytesProcessedExec    │
           └─────────────┬─────────────┘
           ┌─────────────┴─────────────┐
           │          SortExec         │
           │    --------------------   │
           │    id@0 DESC NULLS LAST   │
           └─────────────┬─────────────┘
           ┌─────────────┴─────────────┐
           │       DataSourceExec      │
           │    --------------------   │
           │        bytes: 80096       │
           │       format: memory      │
           │          rows: 1          │
           └───────────────────────────┘
        */

        // Optimizer is a bag of rules
        let optimizer = PhysicalOptimizer::new();
        let config = Arc::clone(ctx.state().config_options());

        // Fold over the default rules to apply the same optimizations DF would at runtime
        let optimized = optimizer
            .rules
            .iter()
            .fold(Arc::clone(&final_plan), |plan, rule| {
                rule.optimize(plan, &config).expect("Must optimize plan")
            });

        // No semantic eq implemented, so this is the easiest way to compare plans
        assert_eq!(
            displayable(final_plan.as_ref()).tree_render().to_string(),
            displayable(optimized.as_ref()).tree_render().to_string()
        );

        Ok(())
    }
}
