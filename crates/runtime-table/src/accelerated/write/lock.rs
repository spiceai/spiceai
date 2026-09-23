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

//! Serializes a direct user write against the accelerator write lock.
//!
//! Acceleration snapshot creation holds `accelerator_write_mutex` across the
//! checkpoint and the copy that follows it, so that what it captures is a row
//! set some single point in time produced. Refresh, CDC and the cache writer
//! take the same lock, so their writes wait for it. A direct `INSERT` /
//! `UPDATE` / `DELETE` did not.
//!
//! Taking the lock where the plan is *built* would not have closed that: a
//! `TableProvider`'s write methods return an `ExecutionPlan`, and the rows do
//! not move until DataFusion executes it, which is after those methods have
//! returned and any guard they held has been dropped. So the guard has to be
//! acquired inside `execute` and held for as long as the write's output stream
//! lives — which is what this plan does, and all it does.
//!
//! Lock ordering: this is taken *before* any lock the wrapped write takes
//! (a partitioned Cayenne dual write takes that table's write coordinator
//! inside its own write), which is the order snapshot creation and refresh
//! already use — `accelerator_write_mutex` first, then whatever the accelerator
//! needs underneath.

use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::Result as DataFusionResult;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::Mutex;

/// Wraps a write plan so the accelerator write lock is held for the whole of
/// the write, rather than only while the plan was being built.
pub(crate) struct AcceleratorWriteLockExec {
    input: Arc<dyn ExecutionPlan>,
    accelerator_write_mutex: Arc<Mutex<()>>,
    dataset_name: Arc<str>,
    plan_properties: Arc<PlanProperties>,
}

impl AcceleratorWriteLockExec {
    /// A write sink emits one partition — a single row-count batch — so the
    /// guard is taken once per execution rather than once per partition. The
    /// declared distribution keeps that true for the input as well, so a plan
    /// that fanned out could not leave part of its write outside the lock.
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        accelerator_write_mutex: Arc<Mutex<()>>,
        dataset_name: Arc<str>,
    ) -> Self {
        let plan_properties = Arc::new(
            input
                .properties()
                .as_ref()
                .clone()
                .with_partitioning(Partitioning::UnknownPartitioning(1)),
        );

        Self {
            input,
            accelerator_write_mutex,
            dataset_name,
            plan_properties,
        }
    }

    pub(crate) fn new_arc(
        input: Arc<dyn ExecutionPlan>,
        accelerator_write_mutex: Arc<Mutex<()>>,
        dataset_name: Arc<str>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self::new(input, accelerator_write_mutex, dataset_name))
    }
}

impl fmt::Debug for AcceleratorWriteLockExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AcceleratorWriteLockExec(dataset={})", self.dataset_name)
    }
}

impl DisplayAs for AcceleratorWriteLockExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AcceleratorWriteLockExec: dataset={}", self.dataset_name)
    }
}

impl ExecutionPlan for AcceleratorWriteLockExec {
    fn name(&self) -> &'static str {
        "AcceleratorWriteLockExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let Some(input) = children.into_iter().next() else {
            return Err(DataFusionError::Internal(
                "AcceleratorWriteLockExec requires exactly one child".to_string(),
            ));
        };

        Ok(Arc::new(Self::new(
            input,
            Arc::clone(&self.accelerator_write_mutex),
            Arc::clone(&self.dataset_name),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let schema = self.schema();
        let input = Arc::clone(&self.input);
        let accelerator_write_mutex = Arc::clone(&self.accelerator_write_mutex);
        let dataset_name = Arc::clone(&self.dataset_name);

        // The guard is threaded through the stream's own state, so it is
        // released exactly when the write's output stream is dropped —
        // including on an error or an early abort, which is when leaving it
        // held would wedge every later refresh and snapshot of this table.
        let stream = futures::stream::once(async move {
            let guard = Arc::clone(&accelerator_write_mutex).lock_owned().await;
            tracing::debug!(
                "Holding the accelerator write lock for a direct write to dataset {dataset_name}"
            );
            let input_stream = input.execute(partition, context)?;

            Ok::<_, DataFusionError>(futures::stream::unfold(
                (input_stream, guard),
                |(mut input_stream, guard)| async move {
                    input_stream
                        .next()
                        .await
                        .map(|batch| (batch, (input_stream, guard)))
                },
            ))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
