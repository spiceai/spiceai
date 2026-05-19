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
            total_byte_size: statistics.total_byte_size,
            column_statistics: vec![
                ColumnStatistics::new_unknown();
                statistics.column_statistics.len()
            ],
        }
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
            row_count.saturating_sub(usize::try_from(deletion_vector.len()).unwrap_or(usize::MAX)),
        ),
        Precision::Absent => Precision::Absent,
    }
}

fn deleted_rows_within_file(deletion_vector: &PositionDeletionVector, row_count: usize) -> usize {
    deletion_vector
        .iter()
        .take_while(|row_id| usize::try_from(*row_id).is_ok_and(|row_id| row_id < row_count))
        .count()
}
