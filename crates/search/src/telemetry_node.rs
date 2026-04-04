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

//! Logical extension node and physical execution wrapper for search telemetry tracking.
//!
//! [`SearchTelemetryNode`] is a transparent logical plan wrapper produced by
//! [`SearchQueryOptimizerRule`] when a [`SearchQueryProvider`] has a `scan_callback`.
//! It carries the callback through logical planning to physical planning, where
//! [`SearchTelemetryExec`] fires it on the first `poll_next` of partition 0.

use std::{
    any::Any,
    fmt,
    hash::{Hash, Hasher},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    common::{DFSchemaRef, Result as DFResult},
    execution::TaskContext,
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
        SendableRecordBatchStream,
    },
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};
use futures::{Future, Stream, future::BoxFuture};

/// Callback type: async closure fired once per query execution for telemetry.
pub type TelemetryCallback = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

// ── Logical node ─────────────────────────────────────────────────────────────

/// Transparent logical plan wrapper that carries a [`TelemetryCallback`].
///
/// The optimizer sees through this node (it has the same schema and one child).
/// [`SearchTelemetryPlanner`] converts it to [`SearchTelemetryExec`] at physical
/// planning time.
#[derive(Clone)]
pub struct SearchTelemetryNode {
    pub input: Arc<LogicalPlan>,
    pub callback: TelemetryCallback,
    schema: DFSchemaRef,
}

impl fmt::Debug for SearchTelemetryNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SearchTelemetryNode")
            .field("input", &self.input)
            .finish_non_exhaustive()
    }
}

impl SearchTelemetryNode {
    pub fn new(input: LogicalPlan, callback: TelemetryCallback) -> Self {
        let schema = Arc::clone(input.schema());
        Self {
            input: Arc::new(input),
            callback,
            schema,
        }
    }
}

impl Hash for SearchTelemetryNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
    }
}

impl PartialEq for SearchTelemetryNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
    }
}

impl Eq for SearchTelemetryNode {}

impl PartialOrd for SearchTelemetryNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SearchTelemetryNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by the input plan's debug representation as a stable ordering.
        format!("{:?}", self.input).cmp(&format!("{:?}", other.input))
    }
}

impl UserDefinedLogicalNodeCore for SearchTelemetryNode {
    fn name(&self) -> &'static str {
        "SearchTelemetry"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SearchTelemetry")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        let mut inputs = inputs;
        Ok(Self {
            input: Arc::new(inputs.remove(0)),
            callback: Arc::clone(&self.callback),
            schema: Arc::clone(&self.schema),
        })
    }
}

// ── Physical exec ─────────────────────────────────────────────────────────────

/// Physical plan wrapper that fires the [`TelemetryCallback`] on the first
/// `poll_next` of partition 0, then delegates all record batches to the inner plan.
pub struct SearchTelemetryExec {
    inner: Arc<dyn ExecutionPlan>,
    callback: TelemetryCallback,
    properties: PlanProperties,
}

impl fmt::Debug for SearchTelemetryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SearchTelemetryExec")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl SearchTelemetryExec {
    pub fn new(inner: Arc<dyn ExecutionPlan>, callback: TelemetryCallback) -> Self {
        let properties = inner.properties().clone();
        Self {
            inner,
            callback,
            properties,
        }
    }
}

impl DisplayAs for SearchTelemetryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SearchTelemetryExec")
    }
}

impl ExecutionPlan for SearchTelemetryExec {
    fn name(&self) -> &'static str {
        "SearchTelemetryExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.callback),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let inner = self.inner.execute(partition, context)?;
        // Only fire the callback on partition 0 to count each query exactly once.
        if partition == 0 {
            let callback = Arc::clone(&self.callback);
            Ok(Box::pin(TelemetryStream {
                inner,
                pending: Some(Box::pin(async move { callback().await })),
            }))
        } else {
            Ok(inner)
        }
    }
}

// ── Stream wrapper ────────────────────────────────────────────────────────────

/// Thin stream wrapper that polls `pending` to completion before forwarding
/// batches from `inner`.  This fires the callback in the async execution context
/// where `RequestContext` is available as a task-local.
struct TelemetryStream {
    inner: SendableRecordBatchStream,
    pending: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

// Both Pin<Box<...>> fields are Unpin, so the struct is Unpin.
impl Unpin for TelemetryStream {}

impl Stream for TelemetryStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(ref mut fut) = this.pending {
            match fut.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => {}
            }
        }
        this.pending = None;
        this.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for TelemetryStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

// ── Extension planner ─────────────────────────────────────────────────────────

/// Converts [`SearchTelemetryNode`] → [`SearchTelemetryExec`] during physical planning.
pub struct SearchTelemetryPlanner;

#[async_trait]
impl ExtensionPlanner for SearchTelemetryPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &datafusion::execution::SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(telemetry_node) = node.as_any().downcast_ref::<SearchTelemetryNode>() else {
            return Ok(None);
        };
        Ok(Some(Arc::new(SearchTelemetryExec::new(
            Arc::clone(&physical_inputs[0]),
            Arc::clone(&telemetry_node.callback),
        ))))
    }
}
