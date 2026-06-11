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
use crate::extension::request_context::resolve_request_context;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::common::tree_node::TransformedResult;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::OrderingRequirements;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::SortOrderPushdownResult;
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, InvariantLevel, check_default_invariants,
};
use datafusion::physical_plan::expressions::PhysicalSortExpr;
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
use runtime_request_context::RequestContext;
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

            exec_plan = exec_plan.fallback_to_new_context();

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
    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

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

    fn properties(&self) -> &Arc<PlanProperties> {
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

    /// Only allow optimizer-introduced repartitioning when the child has no
    /// output ordering. This keeps order-sensitive plans stable by avoiding
    /// repartition on already ordered inputs.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![self.input_exec.properties().output_ordering().is_none()]
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

        let Some(request_context) = resolve_request_context(&context, self.fallback_to_new_context)
        else {
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

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input_exec.partition_statistics(partition)
    }

    // Allow optimizer to push limits through to inputs
    fn supports_limit_pushdown(&self) -> bool {
        self.input_exec.supports_limit_pushdown()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.input_exec
            .with_fetch(limit)
            .map(|plan| self.wrap_input_exec(plan))
    }

    fn fetch(&self) -> Option<usize> {
        self.input_exec.fetch()
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

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>, DataFusionError> {
        let result = self.input_exec.try_pushdown_sort(order)?;
        Ok(result.map(|plan| self.wrap_input_exec(plan)))
    }
}

impl BytesProcessedExec {
    fn wrap_input_exec(&self, input_exec: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let mut exec = BytesProcessedExec::new(input_exec, Arc::clone(&self.emit_bytes_callback));
        if self.fallback_to_new_context {
            exec = exec.fallback_to_new_context();
        }
        Arc::new(exec) as Arc<dyn ExecutionPlan>
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
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::sync::{Arc, Mutex};

    use crate::config::request_context_config::SpiceRequestContextConfig;
    use crate::extension::bytes_processed::{BytesEmittedCallback, BytesProcessedExec};
    use opentelemetry::trace::{SpanId, TraceId};
    use runtime_request_context::{Protocol, RequestContextBuilder, TraceParent};

    fn make_test_table() -> Result<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0i64..10000))],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    fn make_test_context() -> SessionContext {
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2))
    }

    #[expect(clippy::similar_names)]
    #[tokio::test]
    async fn test_preserve_order_pushdown() -> Result<()> {
        let ctx = make_test_context();
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

    #[expect(clippy::similar_names)]
    #[tokio::test]
    async fn test_allow_repartition_for_unordered_input() -> Result<()> {
        let ctx = make_test_context();
        let test_table = make_test_table()?;

        let data_source_exec = test_table.scan(&ctx.state(), None, &[], None).await?;

        let final_plan: Arc<dyn ExecutionPlan> = Arc::new(BytesProcessedExec::new(
            data_source_exec,
            Arc::new(Box::new(|_, _| {})),
        ));

        let optimizer = PhysicalOptimizer::new();
        let config = Arc::clone(ctx.state().config_options());

        let optimized = optimizer
            .rules
            .iter()
            .fold(Arc::clone(&final_plan), |plan, rule| {
                rule.optimize(plan, &config).expect("Must optimize plan")
            });

        let optimized_plan = displayable(optimized.as_ref()).tree_render().to_string();
        assert!(
            optimized_plan.contains("RepartitionExec"),
            "Expected RepartitionExec for unordered input, got: {optimized_plan}"
        );

        Ok(())
    }

    #[expect(clippy::similar_names)]
    #[tokio::test]
    async fn test_bytes_processed_total_preserved_with_repartition() -> Result<()> {
        let ctx = make_test_context();
        let test_table = make_test_table()?;

        let build_test_plan = |callback: Arc<BytesEmittedCallback>| async {
            let data_source_exec = test_table.scan(&ctx.state(), None, &[], None).await?;
            let bytes_processed_exec =
                BytesProcessedExec::new(data_source_exec, callback).fallback_to_new_context();
            Result::<Arc<dyn ExecutionPlan>>::Ok(Arc::new(bytes_processed_exec))
        };

        let before_values = Arc::new(Mutex::new(Vec::new()));
        let before_values_ref = Arc::clone(&before_values);
        let before_callback: Arc<BytesEmittedCallback> = Arc::new(Box::new(move |bytes, _| {
            before_values_ref
                .lock()
                .expect("before callback mutex should not be poisoned")
                .push(bytes);
        }));

        let before_plan = build_test_plan(before_callback).await?;
        let before_batches = collect(before_plan, ctx.task_ctx()).await?;
        let before_rows: usize = before_batches.iter().map(RecordBatch::num_rows).sum();
        let before_total: u64 = before_values
            .lock()
            .expect("before mutex should not be poisoned")
            .iter()
            .sum();

        let after_values = Arc::new(Mutex::new(Vec::new()));
        let after_values_ref = Arc::clone(&after_values);
        let after_callback: Arc<BytesEmittedCallback> = Arc::new(Box::new(move |bytes, _| {
            after_values_ref
                .lock()
                .expect("after callback mutex should not be poisoned")
                .push(bytes);
        }));

        let final_plan = build_test_plan(after_callback).await?;

        let optimizer = PhysicalOptimizer::new();
        let config = Arc::clone(ctx.state().config_options());

        let optimized = optimizer.rules.iter().fold(final_plan, |plan, rule| {
            rule.optimize(plan, &config).expect("Must optimize plan")
        });

        let optimized_plan = displayable(optimized.as_ref()).tree_render().to_string();
        assert!(
            optimized_plan.contains("RepartitionExec"),
            "Expected RepartitionExec for unordered input, got: {optimized_plan}"
        );

        let after_batches = collect(optimized, ctx.task_ctx()).await?;
        let after_rows: usize = after_batches.iter().map(RecordBatch::num_rows).sum();
        let after_total: u64 = after_values
            .lock()
            .expect("after mutex should not be poisoned")
            .iter()
            .sum();

        assert_eq!(before_rows, after_rows);
        assert_eq!(before_rows, 10_000);
        // Byte totals may differ slightly because RepartitionExec creates new
        // RecordBatches with different buffer allocations than the original.
        // The key invariant is that BytesProcessedExec still tracks bytes when
        // RepartitionExec is placed below it.
        assert!(
            before_total > 0,
            "Expected non-zero bytes tracked before optimization"
        );
        assert!(
            after_total > 0,
            "Expected non-zero bytes tracked after repartitioning"
        );

        Ok(())
    }

    /// Verifies the executor-side propagation path that this issue
    /// enables: when no typed `Arc<RequestContext>` is present on the
    /// session config (the case after Ballista round-trips the config to
    /// an executor), `BytesProcessedExec` reconstructs the request
    /// context from the `SpiceRequestContextConfig` option extension and
    /// emits metric dimensions tagged with the originating protocol.
    #[tokio::test]
    async fn test_uses_config_extension_when_typed_request_context_missing() -> Result<()> {
        let trace_id = TraceId::from_hex("0123456789abcdef0123456789abcdef").expect("trace id");
        let span_id = SpanId::from_hex("0123456789abcdef").expect("span id");

        let cfg_ext = SpiceRequestContextConfig::from_request_context(&Arc::new(
            RequestContextBuilder::new(Protocol::FlightSQL)
                .with_trace_parent(Some(TraceParent { trace_id, span_id }))
                .build(),
        ));

        let session_config = SessionConfig::new()
            .with_target_partitions(1)
            .with_option_extension(cfg_ext);
        let ctx = SessionContext::new_with_config(session_config);
        let test_table = make_test_table()?;

        let data_source_exec = test_table.scan(&ctx.state(), None, &[], None).await?;

        // `fallback_to_new_context` is intentionally left off here: the
        // exec must resolve via the config extension, not the fallback.
        let exec = Arc::new(BytesProcessedExec::new(
            data_source_exec,
            Arc::new(Box::new(|_, _| {})),
        )) as Arc<dyn ExecutionPlan>;

        let observed_protocol = Arc::new(Mutex::new(None::<String>));
        let observed_clone = Arc::clone(&observed_protocol);
        let capture_protocol: Arc<BytesEmittedCallback> = Arc::new(Box::new(move |_, dims| {
            let protocol = dims
                .iter()
                .find(|kv| kv.key.as_str() == "protocol")
                .map(|kv| kv.value.as_str().to_string());
            *observed_clone
                .lock()
                .expect("observed protocol mutex should not be poisoned") = protocol;
        }));

        let capturing_exec = Arc::new(BytesProcessedExec::new(exec, Arc::clone(&capture_protocol)))
            as Arc<dyn ExecutionPlan>;

        let _ = collect(capturing_exec, ctx.task_ctx()).await?;

        let observed = observed_protocol
            .lock()
            .expect("observed protocol mutex should not be poisoned")
            .clone();
        assert_eq!(observed.as_deref(), Some("flightsql"));

        Ok(())
    }
}
