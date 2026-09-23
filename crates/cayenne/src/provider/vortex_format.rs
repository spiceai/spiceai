/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Cayenne-specific integration with `vortex-datafusion`.
//!
//! Position-based deletes are exposed through `VortexAccessPlanProvider` so the
//! local Vortex fork can attach per-file access plans and adjust statistics where
//! `DataFusion` may otherwise answer aggregates from stale footer metadata.

use std::sync::Arc;

use super::deletion_strategy::{PositionBitmap, PositionDeletionVector};
use arc_swap::ArcSwap;
use datafusion_common::ColumnStatistics;
use datafusion_common::Statistics;
use datafusion_common::stats::Precision;
use datafusion_datasource::PartitionedFile;
use object_store::ObjectMeta;
use vortex_datafusion::{VortexAccessPlan, VortexAccessPlanProvider};

/// Provides Cayenne's position-based deletion vectors to `vortex-datafusion`.
#[derive(Clone)]
pub(crate) struct PositionDeletionAccessPlanProvider {
    deletion_cache: Arc<ArcSwap<PositionBitmap>>,
}

impl PositionDeletionAccessPlanProvider {
    #[must_use]
    pub(crate) fn new(deletion_cache: Arc<ArcSwap<PositionBitmap>>) -> Self {
        Self { deletion_cache }
    }

    fn deletion_vector_for_path(&self, file_path: &str) -> Option<Arc<PositionDeletionVector>> {
        let snapshot = self.deletion_cache.load();
        snapshot
            .get(file_path)
            .filter(|deletion_vector| !deletion_vector.is_empty())
            .cloned()
    }
}

impl std::fmt::Debug for PositionDeletionAccessPlanProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PositionDeletionAccessPlanProvider")
            .finish_non_exhaustive()
    }
}

impl VortexAccessPlanProvider for PositionDeletionAccessPlanProvider {
    fn access_plan_for_file(&self, file: &PartitionedFile) -> Option<Arc<VortexAccessPlan>> {
        let file_path = file.object_meta.location.to_string();
        let deletion_vector = self.deletion_vector_for_path(&file_path)?;

        tracing::trace!(
            file_path = %file_path,
            deleted_rows = deletion_vector.len(),
            "Attached VortexAccessPlan with deletion vector"
        );

        Some(deletion_vector.access_plan())
    }

    fn adjust_statistics(&self, object: &ObjectMeta, statistics: Statistics) -> Statistics {
        let file_path = object.location.to_string();
        let Some(deletion_vector) = self.deletion_vector_for_path(&file_path) else {
            return statistics;
        };

        Statistics {
            num_rows: adjust_num_rows_for_deletions(statistics.num_rows, &deletion_vector),
            // Position deletes only ever remove rows, so the footer byte size is
            // now an over-estimate. Keep it as an (inexact) upper bound instead of
            // dropping it, so size-based heuristics still have a signal.
            total_byte_size: statistics.total_byte_size.to_inexact(),
            column_statistics: statistics
                .column_statistics
                .into_iter()
                .map(adjust_column_stats_for_deletions)
                .collect(),
        }
    }
}

/// Soundly downgrade a column's footer statistics for a file with position
/// deletes attached.
///
/// Position deletes only ever *remove* rows, so the surviving rows are a subset
/// of the rows the footer described. That makes the footer min/max a valid
/// *superset* bound of the survivors (the true min can only rise, the true max
/// can only fall), which is exactly what min/max pruning needs — but the precise
/// boundary value may belong to a deleted row, so the precision must drop from
/// `Exact` to `Inexact`. Counts (`null_count`, `distinct_count`) can only shrink
/// after deletes, so the footer value is an upper-bound estimate and is likewise
/// kept as `Inexact`. The aggregate `sum_value` cannot be bounded after removing
/// rows of unknown sign, so it is dropped to `Absent` rather than handing an
/// aggregate a value that no longer matches the live data.
fn adjust_column_stats_for_deletions(stats: ColumnStatistics) -> ColumnStatistics {
    ColumnStatistics {
        // Upper-bound estimate after deletes.
        null_count: stats.null_count.to_inexact(),
        // Still-valid superset bounds; precision drops because the boundary row
        // may have been deleted.
        max_value: stats.max_value.to_inexact(),
        min_value: stats.min_value.to_inexact(),
        // Unsound to keep after removing rows of unknown sign.
        sum_value: Precision::Absent,
        // Upper-bound estimate after deletes.
        distinct_count: stats.distinct_count.to_inexact(),
        byte_size: stats.byte_size.to_inexact(),
    }
}

fn adjust_num_rows_for_deletions(
    num_rows: Precision<usize>,
    deletion_vector: &PositionDeletionVector,
) -> Precision<usize> {
    match num_rows {
        Precision::Exact(row_count) => Precision::Exact(
            row_count.saturating_sub(deleted_rows_within_file(deletion_vector, row_count)),
        ),
        Precision::Inexact(row_count) => Precision::Inexact(
            row_count.saturating_sub(deleted_rows_within_file(deletion_vector, row_count)),
        ),
        Precision::Absent => Precision::Absent,
    }
}

fn deleted_rows_within_file(deletion_vector: &PositionDeletionVector, row_count: usize) -> usize {
    deletion_vector.count_before_row(row_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;

    #[test]
    fn inexact_row_count_only_subtracts_deletions_within_file() {
        let deletion_vector = PositionDeletionVector::new(RoaringBitmap::from_iter([0, 2, 9, 10]));

        assert_eq!(
            adjust_num_rows_for_deletions(Precision::Inexact(10), &deletion_vector),
            Precision::Inexact(7)
        );
    }

    #[test]
    fn column_stats_are_downgraded_soundly_for_deletions() {
        use datafusion_common::ScalarValue;

        let exact = ColumnStatistics {
            null_count: Precision::Exact(2),
            max_value: Precision::Exact(ScalarValue::Int64(Some(100))),
            min_value: Precision::Exact(ScalarValue::Int64(Some(1))),
            sum_value: Precision::Exact(ScalarValue::Int64(Some(5050))),
            distinct_count: Precision::Exact(100),
            byte_size: Precision::Exact(800),
        };

        let adjusted = adjust_column_stats_for_deletions(exact);

        // min/max stay valid superset bounds but are no longer exact (the row
        // holding the boundary value may have been deleted).
        assert_eq!(
            adjusted.min_value,
            Precision::Inexact(ScalarValue::Int64(Some(1)))
        );
        assert_eq!(
            adjusted.max_value,
            Precision::Inexact(ScalarValue::Int64(Some(100)))
        );
        // Counts can only shrink after deletes — keep as inexact upper bounds.
        assert_eq!(adjusted.null_count, Precision::Inexact(2));
        assert_eq!(adjusted.distinct_count, Precision::Inexact(100));
        assert_eq!(adjusted.byte_size, Precision::Inexact(800));
        // Sum cannot be bounded after removing rows of unknown sign.
        assert_eq!(adjusted.sum_value, Precision::Absent);
    }

    #[test]
    fn absent_column_stats_stay_absent_after_adjustment() {
        let adjusted = adjust_column_stats_for_deletions(ColumnStatistics::new_unknown());

        assert_eq!(adjusted.min_value, Precision::Absent);
        assert_eq!(adjusted.max_value, Precision::Absent);
        assert_eq!(adjusted.null_count, Precision::Absent);
        assert_eq!(adjusted.distinct_count, Precision::Absent);
        assert_eq!(adjusted.sum_value, Precision::Absent);
    }
}
