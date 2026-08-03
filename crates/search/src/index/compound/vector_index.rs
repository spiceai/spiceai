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

use std::{any::Any, sync::Arc};

use arrow::array::RecordBatch;
use arrow_schema::Field;
use async_trait::async_trait;
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
};
use futures::future::try_join_all;
use runtime_datafusion_index::Index;

use crate::index::{SearchIndex, VectorIndex, primary_key_projection};

use super::{
    CompoundReadMode, Error, compound_delete_by_keys, compound_on_write_start,
    compound_required_columns, compound_write, fallback::fallback_on_empty_plan,
    validate_compatibility,
};

/// A [`VectorIndex`] counterpart of [`super::CompoundSearchIndex`]: writes through to two
/// compatible vector indexes and serves list & query from the primary (optionally falling
/// back to the secondary on empty results).
///
/// See the [module documentation](super) for semantics and compatibility requirements.
#[derive(Debug, Clone)]
pub struct CompoundVectorIndex {
    primary: Arc<dyn VectorIndex>,
    secondary: Arc<dyn VectorIndex>,
    read_mode: CompoundReadMode,
}

impl CompoundVectorIndex {
    /// Create a compound vector index over `primary` and `secondary`, validating that the two
    /// indexes are compatible (same search column, same primary-key fields, same embedding
    /// dimension).
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] describing the first incompatibility found.
    pub fn try_new(
        primary: Arc<dyn VectorIndex>,
        secondary: Arc<dyn VectorIndex>,
        read_mode: CompoundReadMode,
    ) -> Result<Self, Error> {
        validate_compatibility(
            &(Arc::clone(&primary) as Arc<dyn SearchIndex>),
            &(Arc::clone(&secondary) as Arc<dyn SearchIndex>),
        )?;
        Ok(Self::from_validated(primary, secondary, read_mode))
    }

    /// Construct without re-validating compatibility. Callers must have already run
    /// [`validate_compatibility`] over the pair (directly or via
    /// [`super::CompoundSearchIndex::try_new`]).
    pub(super) fn from_validated(
        primary: Arc<dyn VectorIndex>,
        secondary: Arc<dyn VectorIndex>,
        read_mode: CompoundReadMode,
    ) -> Self {
        Self {
            primary,
            secondary,
            read_mode,
        }
    }

    #[must_use]
    pub fn primary(&self) -> &Arc<dyn VectorIndex> {
        &self.primary
    }

    #[must_use]
    pub fn read_mode(&self) -> CompoundReadMode {
        self.read_mode
    }
}

impl VectorIndex for CompoundVectorIndex {
    fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
        match self.read_mode {
            CompoundReadMode::PrimaryOnly => self.primary.list_table_provider(),
            CompoundReadMode::FallbackToSecondary => {
                let primary = Arc::new(self.primary.list_table_provider()?);
                let secondary = Arc::new(self.secondary.list_table_provider()?);
                fallback_on_empty_plan(primary, secondary)
            }
        }
    }

    /// Both halves, unioned — never narrowed by [`Self::read_mode`].
    ///
    /// `list_table_provider` answers "what should a read see", and for
    /// [`CompoundReadMode::PrimaryOnly`] that is the warm primary alone; the primary only holds
    /// rows the write path has passed through it, so it is not authoritative for what is stored.
    /// A union rather than a fallback because the two halves can disagree in *either* direction:
    /// an entry either one holds is an entry a delete still has to resolve, and
    /// [`Index::delete_by_keys`] already fans out to both.
    ///
    /// Each half is projected to the key columns *before* the union. [`validate_compatibility`]
    /// guarantees the halves agree there on name, type and nullability; it guarantees nothing of
    /// the rest of their listings, which is why [`fallback_on_empty_plan`] has to cast and
    /// re-project to reconcile them for reads.
    fn list_all_entry_keys(&self) -> Result<LogicalPlan, DataFusionError> {
        let keys = |half: &Arc<dyn VectorIndex>| {
            LogicalPlanBuilder::from(half.list_all_entry_keys()?)
                .project(primary_key_projection(&half.primary_fields()))?
                .build()
        };

        LogicalPlanBuilder::from(keys(&self.primary)?)
            .union(keys(&self.secondary)?)?
            .build()
    }

    fn dimension(&self) -> i32 {
        self.primary.dimension()
    }

    fn derived_columns(&self) -> Vec<String> {
        // Writes merge the secondary's output columns into the primary's, so columns derived
        // by either index can appear on the write output.
        let mut columns = self.primary.derived_columns();
        for column in self.secondary.derived_columns() {
            if !columns.contains(&column) {
                columns.push(column);
            }
        }
        columns
    }
}

#[async_trait]
impl Index for CompoundVectorIndex {
    fn name(&self) -> &'static str {
        "CompoundVectorIndex"
    }

    fn required_columns(&self) -> Vec<String> {
        compound_required_columns(self.primary.as_ref(), self.secondary.as_ref())
    }

    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let futs = batches
            .into_iter()
            .map(|rb| async { self.write(rb).await.map_err(DataFusionError::External) });
        try_join_all(futs).await
    }

    async fn on_write_start(&self) -> Result<(), DataFusionError> {
        compound_on_write_start(self.primary.as_ref(), self.secondary.as_ref()).await
    }

    async fn on_write_failed(&self) -> Result<(), DataFusionError> {
        // Drive both to completion — a failure on one index must not skip the other's
        // cleanup — then surface the primary's error first.
        let (primary_result, secondary_result) = futures::join!(
            self.primary.on_write_failed(),
            self.secondary.on_write_failed()
        );
        primary_result.and(secondary_result)
    }

    async fn on_write_complete(&self) -> Result<(), DataFusionError> {
        // As with `on_write_failed`: both completion callbacks must run.
        let (primary_result, secondary_result) = futures::join!(
            self.primary.on_write_complete(),
            self.secondary.on_write_complete()
        );
        primary_result.and(secondary_result)
    }

    async fn delete_by_keys(&self, keys: RecordBatch) -> DataFusionResult<()> {
        compound_delete_by_keys(self.primary.as_ref(), self.secondary.as_ref(), keys).await
    }

    fn deletes_by_partial_key(&self) -> bool {
        // `delete_by_keys` fans out to both halves, so a partial key only clears this compound
        // index when *both* halves act on one.
        self.primary.deletes_by_partial_key() && self.secondary.deletes_by_partial_key()
    }

    fn write_complete_failure_is_fatal(&self) -> bool {
        // Either half failing to finalize leaves this compound index stale.
        self.primary.write_complete_failure_is_fatal()
            || self.secondary.write_complete_failure_is_fatal()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl SearchIndex for CompoundVectorIndex {
    fn search_column(&self) -> String {
        self.primary.search_column()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.primary.primary_fields()
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        compound_write(self.primary.as_ref(), self.secondary.as_ref(), record).await
    }

    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        match self.read_mode {
            CompoundReadMode::PrimaryOnly => self.primary.query_table_provider(query),
            CompoundReadMode::FallbackToSecondary => {
                let primary = self.primary.query_table_provider(query)?;
                let secondary = self.secondary.query_table_provider(query)?;
                Ok(Arc::new(fallback_on_empty_plan(primary, secondary)?))
            }
        }
    }

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        Some(self as Arc<dyn VectorIndex>)
    }
}
