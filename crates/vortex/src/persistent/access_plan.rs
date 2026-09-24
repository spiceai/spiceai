// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion_common::Statistics;
use datafusion_datasource::PartitionedFile;
use datafusion_physical_expr::PhysicalExprRef;
use object_store::ObjectMeta;
use vortex::buffer::Buffer;
use vortex::layout::scan::scan_builder::ScanBuilder;
use vortex::scan::selection::Selection;
use vortex::scan::strict_sorted_buffer::StrictSortedBuffer;

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

/// Provides access plans that depend on the scan's runtime predicate.
///
/// Unlike a [`VortexAccessPlanProvider`], which plans every file before the scan
/// starts, this is consulted as each file opens, after dynamic expressions such
/// as hash-join filters may have been populated. A scan registers one only when
/// it has something to plan from such a predicate; every other scan opens its
/// files exactly as before. The opener intersects a returned plan with the plan
/// attached during physical planning, so a runtime plan can only narrow the rows
/// read.
#[async_trait]
pub trait VortexRuntimeAccessPlanProvider: Debug + Send + Sync + 'static {
    /// Returns an access plan for `file` derived from the scan's `predicate`, or
    /// `None` to read the file as planned.
    async fn runtime_access_plan_for_file(
        &self,
        file: &PartitionedFile,
        predicate: Option<&PhysicalExprRef>,
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
    #[must_use]
    pub fn selection(&self) -> Option<&Selection> {
        self.selection.as_ref()
    }

    /// Returns whether this plan proves that the file contributes no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self.selection.as_ref() {
            Some(Selection::IncludeByIndex(rows)) => rows.is_empty(),
            Some(Selection::IncludeRoaring(rows)) => rows.is_empty(),
            None
            | Some(Selection::All | Selection::ExcludeByIndex(_) | Selection::ExcludeRoaring(_)) => {
                false
            }
        }
    }

    /// The plan that reads only the rows both plans read.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        let selection = match (self.selection.as_ref(), other.selection.as_ref()) {
            (None | Some(Selection::All), selection) | (selection, None | Some(Selection::All)) => {
                selection.cloned()
            }
            (Some(left), Some(right)) => Some(intersect_selections(left, right)),
        };
        Self { selection }
    }

    /// Apply the plan to the scan's builder.
    #[must_use]
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

/// A selection that reads exactly the rows at `positions`.
///
/// Vortex requires a by-index selection to be strictly increasing. Positions
/// that are not (unsorted or repeated) select the same set of rows through a
/// roaring bitmap instead, so the rows read never depend on the input's order.
#[must_use]
pub fn include_by_index(positions: Buffer<u64>) -> Selection {
    match StrictSortedBuffer::try_new(positions.clone()) {
        Ok(rows) => Selection::IncludeByIndex(rows),
        Err(_) => Selection::IncludeRoaring(positions.iter().copied().collect()),
    }
}

/// A selection that reads every row except those at `positions`; see
/// [`include_by_index`] for how unsorted or repeated positions are handled.
#[must_use]
pub fn exclude_by_index(positions: Buffer<u64>) -> Selection {
    match StrictSortedBuffer::try_new(positions.clone()) {
        Ok(rows) => Selection::ExcludeByIndex(rows),
        Err(_) => Selection::ExcludeRoaring(positions.iter().copied().collect()),
    }
}

/// Whether `selection` reads the row at `position`.
fn selection_keeps(selection: &Selection, position: u64) -> bool {
    match selection {
        Selection::All => true,
        Selection::IncludeByIndex(rows) => rows.binary_search(&position).is_ok(),
        Selection::ExcludeByIndex(rows) => rows.binary_search(&position).is_err(),
        Selection::IncludeRoaring(rows) => rows.contains(position),
        Selection::ExcludeRoaring(rows) => !rows.contains(position),
    }
}

/// Intersects two selections. An include list is filtered by the other side,
/// so it stays sorted; two exclude lists become their sorted union.
fn intersect_selections(left: &Selection, right: &Selection) -> Selection {
    match (left, right) {
        (Selection::All, other) | (other, Selection::All) => other.clone(),
        (Selection::IncludeByIndex(rows), other) | (other, Selection::IncludeByIndex(rows)) => {
            include_by_index(
                rows.iter()
                    .copied()
                    .filter(|&position| selection_keeps(other, position))
                    .collect(),
            )
        }
        (Selection::IncludeRoaring(rows), other) | (other, Selection::IncludeRoaring(rows)) => {
            include_by_index(
                rows.iter()
                    .filter(|&position| selection_keeps(other, position))
                    .collect(),
            )
        }
        (
            Selection::ExcludeByIndex(_) | Selection::ExcludeRoaring(_),
            Selection::ExcludeByIndex(_) | Selection::ExcludeRoaring(_),
        ) => {
            let mut excluded: Vec<u64> = excluded_rows(left).chain(excluded_rows(right)).collect();
            excluded.sort_unstable();
            excluded.dedup();
            exclude_by_index(excluded.into_iter().collect())
        }
    }
}

fn excluded_rows(selection: &Selection) -> Box<dyn Iterator<Item = u64> + '_> {
    match selection {
        Selection::ExcludeByIndex(rows) => Box::new(rows.iter().copied()),
        Selection::ExcludeRoaring(rows) => Box::new(rows.iter()),
        Selection::All | Selection::IncludeByIndex(_) | Selection::IncludeRoaring(_) => {
            Box::new(std::iter::empty())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(selection: Option<Selection>) -> VortexAccessPlan {
        VortexAccessPlan { selection }
    }

    fn rows_read(plan: &VortexAccessPlan, total: u64) -> Vec<u64> {
        (0..total)
            .filter(|&position| {
                plan.selection()
                    .is_none_or(|selection| selection_keeps(selection, position))
            })
            .collect()
    }

    /// Every pair of selection kinds intersects to exactly the rows both read.
    #[test]
    fn intersection_reads_only_rows_both_plans_read() {
        let total = 16;
        let include: Buffer<u64> = [1u64, 3, 5, 7, 9].into_iter().collect();
        let exclude: Buffer<u64> = [3u64, 4, 9, 12].into_iter().collect();
        let exclude_other: Buffer<u64> = [0u64, 4, 5, 15].into_iter().collect();
        let roaring_rows = [2u64, 3, 7, 11];
        let selections = [
            None,
            Some(Selection::All),
            Some(include_by_index(include)),
            Some(include_by_index(Buffer::empty())),
            Some(exclude_by_index(exclude)),
            Some(exclude_by_index(exclude_other)),
            Some(Selection::IncludeRoaring(
                roaring_rows.into_iter().collect(),
            )),
            Some(Selection::ExcludeRoaring(
                roaring_rows.into_iter().collect(),
            )),
        ];
        for left in &selections {
            for right in &selections {
                let left = plan(left.clone());
                let right = plan(right.clone());
                let expected: Vec<u64> = rows_read(&left, total)
                    .into_iter()
                    .filter(|position| rows_read(&right, total).contains(position))
                    .collect();
                let intersection = left.intersect(&right);
                assert_eq!(rows_read(&intersection, total), expected);
                if let Some(Selection::IncludeByIndex(rows) | Selection::ExcludeByIndex(rows)) =
                    intersection.selection()
                {
                    assert!(rows.is_sorted(), "a by-index selection must stay sorted");
                }
            }
        }
    }
}
