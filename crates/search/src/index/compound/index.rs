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
use arrow_schema::{Field, FieldRef, Schema};
use async_trait::async_trait;
use datafusion::{error::DataFusionError, logical_expr::LogicalPlan};
use futures::future::try_join_all;
use runtime_datafusion_index::Index;
use snafu::ResultExt;

use crate::index::{SearchIndex, VectorIndex};

use super::{
    CompoundReadMode, Error, MergeWriteOutputsSnafu, PrimaryIndexWriteSnafu,
    SecondaryIndexWriteSnafu, WriteRowCountMismatchSnafu, fallback::fallback_on_empty_plan,
    validate_compatibility,
};

/// An index that writes through to two compatible underlying indexes and serves reads from
/// the primary (optionally falling back to the secondary on empty results).
///
/// `A` and `B` are the underlying index types — trait objects in the common case (see the
/// [`super::CompoundSearchIndex`] and [`super::CompoundVectorIndex`] aliases), or concrete
/// types when the pair is known statically. [`CompoundIndex`] is itself a [`SearchIndex`]
/// whenever both sides are, and a [`VectorIndex`] whenever both sides are.
///
/// See the [module documentation](super) for semantics and compatibility requirements.
#[derive(Debug)]
pub struct CompoundIndex<A: ?Sized, B: ?Sized> {
    primary: Arc<A>,
    secondary: Arc<B>,
    read_mode: CompoundReadMode,
}

impl<A: ?Sized, B: ?Sized> Clone for CompoundIndex<A, B> {
    fn clone(&self) -> Self {
        Self {
            primary: Arc::clone(&self.primary),
            secondary: Arc::clone(&self.secondary),
            read_mode: self.read_mode,
        }
    }
}

impl<A, B> CompoundIndex<A, B>
where
    A: SearchIndex + ?Sized,
    B: SearchIndex + ?Sized,
{
    /// Create a compound index over `primary` and `secondary`, validating that the two indexes
    /// are compatible (same search column, same primary-key fields, same index variant with
    /// matching embedding dimensions for vector indexes).
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] describing the first incompatibility found.
    pub fn try_new(
        primary: Arc<A>,
        secondary: Arc<B>,
        read_mode: CompoundReadMode,
    ) -> Result<Self, Error> {
        validate_compatibility(&primary, &secondary)?;
        Ok(Self {
            primary,
            secondary,
            read_mode,
        })
    }
}

/// Merge the outputs of the primary and secondary writes: the primary's columns win, and
/// secondary columns not present on the primary output are appended (so columns derived by
/// either index survive for downstream acceleration).
fn merge_write_outputs(
    primary_out: RecordBatch,
    secondary_out: RecordBatch,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    if primary_out.num_rows() != secondary_out.num_rows() {
        return WriteRowCountMismatchSnafu {
            primary_rows: primary_out.num_rows(),
            secondary_rows: secondary_out.num_rows(),
        }
        .fail()
        .boxed();
    }

    let (schema, mut arrays, _) = primary_out.into_parts();
    let mut fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
    let secondary_schema = secondary_out.schema();
    for (i, field) in secondary_schema.fields().iter().enumerate() {
        if schema.column_with_name(field.name()).is_none() {
            fields.push(Arc::clone(field));
            arrays.push(Arc::clone(secondary_out.column(i)));
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .context(MergeWriteOutputsSnafu)
        .boxed()
}

#[async_trait]
impl<A, B> Index for CompoundIndex<A, B>
where
    A: SearchIndex + ?Sized,
    B: SearchIndex + ?Sized,
{
    fn name(&self) -> &'static str {
        "CompoundIndex"
    }

    /// Union of the two indexes' required columns, preserving the primary's order.
    fn required_columns(&self) -> Vec<String> {
        let mut columns = self.primary.required_columns();
        for column in self.secondary.required_columns() {
            if !columns.contains(&column) {
                columns.push(column);
            }
        }
        columns
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

    /// Start a bounded write window on both indexes. If the secondary fails to start after the
    /// primary already started, the primary's window is rolled back (best effort) so the two
    /// indexes never disagree about whether a write window is open.
    async fn on_write_start(&self) -> Result<(), DataFusionError> {
        self.primary.on_write_start().await?;
        if let Err(secondary_err) = self.secondary.on_write_start().await {
            // Roll back only the primary: the secondary's `on_write_start` is the call that
            // failed, and `on_write_failed` restores state set up by a *successful*
            // `on_write_start` — an implementation whose start fails partway owns its own
            // cleanup. Calling it here could "restore" settings that were never overridden.
            if let Err(rollback_err) = self.primary.on_write_failed().await {
                tracing::warn!(
                    "Failed to roll back the primary index of a compound search index after the secondary index failed to start a write: {rollback_err}"
                );
            }
            return Err(secondary_err);
        }
        Ok(())
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

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl<A, B> SearchIndex for CompoundIndex<A, B>
where
    A: SearchIndex + ?Sized,
    B: SearchIndex + ?Sized,
{
    fn search_column(&self) -> String {
        self.primary.search_column()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.primary.primary_fields()
    }

    /// Write `record` to both indexes and merge their outputs. Both writes run concurrently
    /// and both are driven to completion even if one fails, so neither index is left
    /// mid-write.
    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let (primary_result, secondary_result) = futures::join!(
            self.primary.write(record.clone()),
            self.secondary.write(record)
        );
        let primary_out = primary_result.context(PrimaryIndexWriteSnafu).boxed()?;
        let secondary_out = secondary_result.context(SecondaryIndexWriteSnafu).boxed()?;
        merge_write_outputs(primary_out, secondary_out)
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
        Some(Arc::new(CompoundIndex {
            primary,
            secondary,
            read_mode: self.read_mode,
        }))
    }
}

impl<A, B> VectorIndex for CompoundIndex<A, B>
where
    A: VectorIndex + ?Sized,
    B: VectorIndex + ?Sized,
{
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
