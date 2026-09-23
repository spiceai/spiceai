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
//! not move until `DataFusion` executes it, which is after those methods have
//! returned and any guard they held has been dropped. So the guard has to be
//! acquired inside `execute` and held for as long as the write's output stream
//! lives — which is what this plan does, and all it does.
//!
//! It also owns *when the table's freshness marker moves*. `last_updated_at`
//! used to be stamped where the plan was built, which was close enough to the
//! write while nothing made the two far apart. Holding the lock does make them
//! far apart, and the marker is what `SnapshotsCreationPolicy::OnChange` compares
//! against the last snapshot's own `snapshot_last_updated_at_ms` to decide
//! whether anything changed. A write that stamped at plan time and then waited
//! would hand its timestamp to the snapshot running ahead of it — which would
//! record it, having not written those rows — and the next on-change snapshot
//! would then read an unchanged marker and skip, leaving the acknowledged write
//! out of every snapshot until some later mutation moved the marker again. So
//! the stamp happens here, once the write's stream has ended without an error
//! and while the guard is still held.
//!
//! Lock ordering: this is taken *before* any lock the wrapped write takes
//! (a partitioned Cayenne dual write takes that table's write coordinator
//! inside its own write), which is the order snapshot creation and refresh
//! already use — `accelerator_write_mutex` first, then whatever the accelerator
//! needs underneath.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

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
    /// Stamped when the write ends without an error, before the guard drops.
    /// `None` for write-back, whose sinks mark the table themselves once the
    /// accelerator accepts the write — stamping here too would move the marker
    /// for a write its own validation went on to refuse.
    last_updated_at: Option<Arc<AtomicI64>>,
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
        last_updated_at: Option<Arc<AtomicI64>>,
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
            last_updated_at,
            dataset_name,
            plan_properties,
        }
    }

    pub(crate) fn new_arc(
        input: Arc<dyn ExecutionPlan>,
        accelerator_write_mutex: Arc<Mutex<()>>,
        last_updated_at: Option<Arc<AtomicI64>>,
        dataset_name: Arc<str>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self::new(
            input,
            accelerator_write_mutex,
            last_updated_at,
            dataset_name,
        ))
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
        let [input] = <[Arc<dyn ExecutionPlan>; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Internal(format!(
                "AcceleratorWriteLockExec wraps exactly one write plan, got {}",
                children.len()
            ))
        })?;

        Ok(Arc::new(Self::new(
            input,
            Arc::clone(&self.accelerator_write_mutex),
            self.last_updated_at.clone(),
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
        let last_updated_at = self.last_updated_at.clone();
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
                Some((input_stream, guard, last_updated_at, false)),
                |state| async move {
                    let (mut input_stream, guard, last_updated_at, failed) = state?;

                    let Some(batch) = input_stream.next().await else {
                        // The write has finished. Move the freshness marker
                        // here, under the guard, so the next holder of the lock
                        // — an acceleration snapshot — reads a timestamp that
                        // already accounts for these rows.
                        if !failed && let Some(last_updated_at) = last_updated_at.as_ref() {
                            crate::accelerated::AcceleratedTable::set_timestamp_to_now(
                                last_updated_at,
                            );
                        }
                        drop(guard);
                        return None;
                    };

                    let failed = failed || batch.is_err();
                    Some((batch, Some((input_stream, guard, last_updated_at, failed))))
                },
            ))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
