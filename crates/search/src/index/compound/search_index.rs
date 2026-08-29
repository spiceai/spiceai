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
    logical_expr::LogicalPlan,
};
use futures::future::try_join_all;
use spice_table::{Index, WriteWindow};

use crate::index::{SearchIndex, VectorIndex, coalesce};

use super::{
    COMPOUND_WRITE_COMPLETE_FAILURE_IS_FATAL, COMPOUND_WRITE_START_FAILURE_IS_FATAL,
    CompoundReadMode, CompoundVectorIndex, Error, compound_delete_by_keys,
    compound_delete_by_predicate, compound_on_write_complete, compound_on_write_start,
    compound_required_columns, compound_resolve_delete_keys, compound_write,
    fallback::fallback_on_empty_plan, validate_compatibility,
};
use datafusion::catalog::{Session, TableProvider};
use datafusion::prelude::Expr;

/// A [`SearchIndex`] that writes through to two compatible underlying indexes and serves
/// reads from the primary (optionally falling back to the secondary on empty results).
///
/// See the [module documentation](super) for semantics and compatibility requirements.
#[derive(Debug, Clone)]
pub struct CompoundSearchIndex {
    primary: Arc<dyn SearchIndex>,
    secondary: Arc<dyn SearchIndex>,
    read_mode: CompoundReadMode,
}

impl CompoundSearchIndex {
    /// Create a compound index over `primary` and `secondary`, validating that the two indexes
    /// are compatible (same search column, same primary-key fields, same index variant).
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] describing the first incompatibility found.
    pub fn try_new(
        primary: Arc<dyn SearchIndex>,
        secondary: Arc<dyn SearchIndex>,
        read_mode: CompoundReadMode,
    ) -> Result<Self, Error> {
        validate_compatibility(&primary, &secondary)?;
        Ok(Self {
            primary,
            secondary,
            read_mode,
        })
    }

    /// The tier reads and writes are served from first.
    #[must_use]
    pub fn primary(&self) -> &Arc<dyn SearchIndex> {
        &self.primary
    }

    /// The tier reads fall back to (in [`CompoundReadMode::FallbackToSecondary`]) and every
    /// write also reaches.
    #[must_use]
    pub fn secondary(&self) -> &Arc<dyn SearchIndex> {
        &self.secondary
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl Index for CompoundSearchIndex {
    fn name(&self) -> &'static str {
        "CompoundSearchIndex"
    }

    fn required_columns(&self) -> Vec<String> {
        compound_required_columns(self.primary.as_ref(), self.secondary.as_ref())
    }

    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        // Duplicate primary keys in one batch resolve to the last change — see [`coalesce`].
        let primary_key = self.primary_fields();
        let futs = batches.into_iter().map(|rb| {
            coalesce::write_last_write_wins(&primary_key, rb, |b| async {
                self.write(b).await.map_err(DataFusionError::External)
            })
        });
        try_join_all(futs).await
    }

    async fn on_write_start(&self, window: WriteWindow) -> Result<(), DataFusionError> {
        compound_on_write_start(self.primary.as_ref(), self.secondary.as_ref(), window).await
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
        compound_on_write_complete(self.primary.as_ref(), self.secondary.as_ref()).await
    }

    async fn delete_by_keys(&self, keys: RecordBatch) -> DataFusionResult<()> {
        compound_delete_by_keys(self.primary.as_ref(), self.secondary.as_ref(), keys).await
    }

    async fn resolve_delete_keys(
        &self,
        table: &Arc<dyn TableProvider>,
        session: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Option<RecordBatch>> {
        compound_resolve_delete_keys(self.required_columns(), table, session, filters).await
    }

    async fn delete_by_predicate(
        &self,
        table: &Arc<dyn TableProvider>,
        session: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<()> {
        compound_delete_by_predicate(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            table,
            session,
            filters,
        )
        .await
    }

    fn deletes_by_partial_key(&self) -> bool {
        // `delete_by_keys` fans out to both halves, so a partial key only clears this compound
        // index when *both* halves act on one.
        self.primary.deletes_by_partial_key() && self.secondary.deletes_by_partial_key()
    }

    fn write_start_failure_is_fatal(&self) -> bool {
        COMPOUND_WRITE_START_FAILURE_IS_FATAL
    }

    fn write_complete_failure_is_fatal(&self) -> bool {
        COMPOUND_WRITE_COMPLETE_FAILURE_IS_FATAL
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl SearchIndex for CompoundSearchIndex {
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
        // `try_new` guarantees both-or-neither are vector indexes.
        let primary = Arc::clone(&self.primary).as_vector_index()?;
        let secondary = Arc::clone(&self.secondary).as_vector_index()?;
        Some(Arc::new(CompoundVectorIndex::from_validated(
            primary,
            secondary,
            self.read_mode,
        )))
    }
}
