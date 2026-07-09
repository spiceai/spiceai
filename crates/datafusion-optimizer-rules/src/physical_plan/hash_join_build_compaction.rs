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

//! [`HashJoinBuildCompaction`] compacts every batch entering a
//! [`HashJoinExec`]'s build (left) side so the join's memory reservation
//! reflects the rows it actually retains.
//!
//! `HashJoinExec` reserves `get_record_batch_memory_size(batch)` for each
//! build batch it collects, which counts each referenced buffer IN FULL. Some
//! operators emit their output as *slices* of one large batch — most notably
//! a grouped `AggregateExec`, whose per-partition output is one set of arrays
//! sliced into `batch_size` chunks. Every slice then reserves the full parent
//! buffer again: a build side holding N slices of a B-byte parent reserves
//! N × B bytes for B bytes of actual data. At scale this exhausts the query
//! memory pool spuriously — TPC-H q17 at SF100 reserved ~37 GB for a ~500 MB
//! aggregated build side and failed with `Resources exhausted` while real
//! usage was fine.
//!
//! [`CompactBuildExec`] re-materializes each batch into freshly allocated,
//! exactly-sized buffers (via `concat_batches` over the single batch). This is
//! one extra copy of the build input — bounded and cheap relative to the hash
//! table the join builds from those same rows, which it concatenates anyway.

use std::fmt;
use std::sync::Arc;

use datafusion::{
    arrow::compute::concat_batches,
    common::tree_node::{Transformed, TransformedResult, TreeNode},
    config::ConfigOptions,
    error::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, joins::HashJoinExec,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;

/// A transparent single-child [`ExecutionPlan`] that copies each input batch
/// into exactly-sized buffers. Row content, schema, partitioning, and ordering
/// are all preserved; only the underlying buffer layout changes.
pub struct CompactBuildExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl CompactBuildExec {
    #[must_use]
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        // Compaction preserves rows, schema, partitioning, and ordering, so the
        // input's plan properties carry over unchanged.
        let properties = Arc::clone(input.properties());
        Self { input, properties }
    }
}

impl fmt::Debug for CompactBuildExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CompactBuildExec")
    }
}

impl DisplayAs for CompactBuildExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CompactBuildExec")
    }
}

impl ExecutionPlan for CompactBuildExec {
    fn name(&self) -> &'static str {
        "CompactBuildExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "CompactBuildExec expects exactly one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(children.swap_remove(0))))
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let schema = stream.schema();
        let out_schema = Arc::clone(&schema);
        let compacted = stream.map(move |item| {
            item.and_then(|batch| {
                // `concat_batches` over a single batch copies it into fresh,
                // exactly-sized buffers, detaching any oversized parent buffer
                // a sliced batch would otherwise keep (and re-reserve) alive.
                concat_batches(&schema, std::iter::once(&batch))
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            out_schema, compacted,
        )))
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<Arc<datafusion::common::Statistics>> {
        self.input.partition_statistics(partition)
    }
}

/// A [`PhysicalOptimizerRule`] that wraps the build (left) input of every
/// [`HashJoinExec`] in a [`CompactBuildExec`]. See the module docs for why the
/// build side's memory reservation requires compact batches.
#[derive(Debug, Default)]
pub struct HashJoinBuildCompaction {}

impl HashJoinBuildCompaction {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for HashJoinBuildCompaction {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let needs_wrap = plan.downcast_ref::<HashJoinExec>().is_some_and(|join| {
                join.left().downcast_ref::<CompactBuildExec>().is_none()
            });
            if !needs_wrap {
                return Ok(Transformed::no(plan));
            }
            let children = plan.children();
            let compacted: Arc<dyn ExecutionPlan> =
                Arc::new(CompactBuildExec::new(Arc::clone(children[0])));
            let probe = Arc::clone(children[1]);
            let rewritten = plan.with_new_children(vec![compacted, probe])?;
            Ok(Transformed::yes(rewritten))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "HashJoinBuildCompaction"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
