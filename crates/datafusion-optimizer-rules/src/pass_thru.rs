/*
Copyright 2025 The Spice.ai OSS Authors

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

use datafusion::{
    common::DataFusionError,
    execution::TaskContext,
    physical_expr::OrderingRequirements,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties,
        SendableRecordBatchStream,
    },
};
use std::{any::Any, fmt, sync::Arc};

type DisplayFormattingFn =
    dyn Fn(DisplayFormatType, &mut fmt::Formatter) -> fmt::Result + Send + Sync + 'static;

/// `PassThruExec` is a generic physical [`ExecutionPlan`] wrapper that injects custom logic via a user-provided closure,
/// forwarding execution to its input plan.
/// This avoids the need to reimplement similar wrappers for different custom logic, side effects, or instrumentation.
pub struct PassThruExec<F>
where
    F: Fn(
            &Arc<dyn ExecutionPlan>,
            usize,
            Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream, DataFusionError>
        + Send
        + Sync
        + 'static,
{
    input_exec: Arc<dyn ExecutionPlan>,
    name: &'static str,
    exec: Arc<F>,
    required_input_distribution: Distribution,
    display_fmt_fn: Option<Arc<DisplayFormattingFn>>,
}

impl<F> PassThruExec<F>
where
    F: Fn(
            &Arc<dyn ExecutionPlan>,
            usize,
            Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream, DataFusionError>
        + Send
        + Sync
        + 'static,
{
    pub fn new(input_exec: Arc<dyn ExecutionPlan>, name: &'static str, exec: F) -> Self {
        Self {
            input_exec,
            name,
            exec: Arc::new(exec),
            required_input_distribution: Distribution::UnspecifiedDistribution,
            display_fmt_fn: None,
        }
    }

    /// Override default input partitioning [`Distribution::UnspecifiedDistribution`].
    #[must_use]
    pub fn with_input_partitioning(mut self, dist: Distribution) -> Self {
        self.required_input_distribution = dist;
        self
    }

    /// Set a custom display formatter for this execution plan
    #[must_use]
    pub fn with_display_fmt_fn<FF>(mut self, display_fmt_fn: FF) -> Self
    where
        FF: Fn(DisplayFormatType, &mut fmt::Formatter) -> fmt::Result + Send + Sync + 'static,
    {
        self.display_fmt_fn = Some(Arc::new(display_fmt_fn));
        self
    }
}

impl<F> DisplayAs for PassThruExec<F>
where
    F: Fn(
            &Arc<dyn ExecutionPlan>,
            usize,
            Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream, DataFusionError>
        + Send
        + Sync
        + 'static,
{
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref fmt_fn) = self.display_fmt_fn {
            fmt_fn(t, f)
        } else {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "{}", self.name)
                }
                DisplayFormatType::TreeRender => Ok(()),
            }
        }
    }
}

impl<F> std::fmt::Debug for PassThruExec<F>
where
    F: Fn(
            &Arc<dyn ExecutionPlan>,
            usize,
            Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream, DataFusionError>
        + Send
        + Sync
        + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(self.name)
            .field(
                "required_input_distribution",
                &self.required_input_distribution,
            )
            .field(
                "display_fmt_fn",
                &self.display_fmt_fn.as_ref().map_or("None", |_| "Some(Fn)"),
            )
            .finish_non_exhaustive()
    }
}

impl<F> ExecutionPlan for PassThruExec<F>
where
    F: Fn(
            &Arc<dyn ExecutionPlan>,
            usize,
            Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream, DataFusionError>
        + Send
        + Sync
        + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        self.input_exec.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input_exec]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "{} requires exactly one child",
                self.name
            )));
        }

        let Some(input_exec) = children.into_iter().next() else {
            unreachable!("{} should have one input", self.name);
        };

        Ok(Arc::new(Self {
            input_exec,
            name: self.name,
            exec: Arc::clone(&self.exec),
            required_input_distribution: self.required_input_distribution.clone(),
            display_fmt_fn: self.display_fmt_fn.as_ref().map(Arc::clone),
        }))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![self.required_input_distribution.clone()]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None]
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        (self.exec)(&self.input_exec, partition, ctx)
    }
}
