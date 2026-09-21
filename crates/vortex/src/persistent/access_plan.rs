// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::Statistics;
use datafusion_common::arrow::record_batch::RecordBatch;
use datafusion_datasource::PartitionedFile;
use datafusion_physical_expr::PhysicalExprRef;
use object_store::ObjectMeta;
use vortex::layout::scan::scan_builder::ScanBuilder;
use vortex::scan::selection::Selection;

/// Custom Vortex-specific information that can be provided by external indexes or other sources.
///
/// This is intended as a low-level interface for users building their own data systems, see the [advanced index] example from the `DataFusion` repo for a similar usage with Parquet.
///
/// [advanced index]: https://github.com/apache/datafusion/blob/47df535d2cd5aac5ad5a92bdc837f38e05ea0f0f/datafusion-examples/examples/data_io/parquet_advanced_index.rs
#[derive(Default, Clone)]
pub struct VortexAccessPlan {
    selection: Option<Selection>,
}

/// Provides per-file access plans and statistics adjustments for Vortex scans.
///
/// This is intended for systems that maintain external indexes, deletion vectors,
/// or other file-level metadata outside the Vortex file footer. Implementations
/// can attach a [`VortexAccessPlan`] to each [`PartitionedFile`] before the scan
/// is built and can adjust the footer-derived [`Statistics`] so `DataFusion` does
/// not apply optimizations using stale metadata.
pub trait VortexAccessPlanProvider: Debug + Send + Sync + 'static {
    /// Returns the access plan to attach to a file, if any.
    fn access_plan_for_file(&self, file: &PartitionedFile) -> Option<Arc<VortexAccessPlan>>;

    /// Adjusts the statistics inferred from a file footer.
    ///
    /// The default preserves the footer statistics unchanged. Providers that
    /// filter rows should downgrade or recompute statistics so exact aggregate
    /// optimizations remain data-correct.
    fn adjust_statistics(&self, _object: &ObjectMeta, statistics: Statistics) -> Statistics {
        statistics
    }
}

/// Resolves a Vortex access plan when an execution-time filter has materialized.
///
/// Unlike [`VortexAccessPlanProvider`], which attaches a plan while the physical
/// scan is built, this hook runs while execution plans each file's I/O. It lets
/// an external index consume a dynamic filter without making the Vortex scan
/// depend on the index implementation. An empty selection prevents the file
/// from producing an I/O planner. Returning `None` preserves the plan already
/// attached to the file.
pub trait VortexDynamicAccessPlanProvider: Debug + Send + Sync + 'static {
    /// Returns already-materialized candidate rows for `file`, if an external
    /// index can serve them without opening the Vortex file.
    ///
    /// The batches use the file schema and are still candidates: the caller
    /// must evaluate `filter` before returning them. `existing` carries any
    /// static row selection (for example, position deletes) that the provider
    /// must honor. `Some([])` proves that this file has no candidate rows;
    /// `None` falls back to [`Self::dynamic_access_plan_for_file`].
    fn dynamic_record_batches_for_file(
        &self,
        _file: &PartitionedFile,
        _filter: Option<&PhysicalExprRef>,
        _existing: Option<&VortexAccessPlan>,
    ) -> Option<Vec<RecordBatch>> {
        None
    }

    /// Returns a replacement access plan for `file`, if the runtime filter can
    /// safely narrow it. `existing` is the static plan attached while planning.
    fn dynamic_access_plan_for_file(
        &self,
        file: &PartitionedFile,
        filter: Option<&PhysicalExprRef>,
        existing: Option<&VortexAccessPlan>,
    ) -> Option<Arc<VortexAccessPlan>>;
}

impl VortexAccessPlan {
    /// Sets a [`Selection`] for this plan.
    #[must_use]
    pub fn with_selection(mut self, selection: Selection) -> Self {
        self.selection = Some(selection);
        self
    }
}

impl VortexAccessPlan {
    /// Returns the selection, if one was set.
    pub fn selection(&self) -> Option<&Selection> {
        self.selection.as_ref()
    }

    /// Apply the plan to the scan's builder.
    pub fn apply_to_builder<A>(&self, mut scan_builder: ScanBuilder<A>) -> ScanBuilder<A>
    where
        A: 'static + Send,
    {
        let Self { selection } = self;

        if let Some(selection) = selection {
            scan_builder = scan_builder.with_selection(selection.clone());
        }

        scan_builder
    }
}
