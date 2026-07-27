/*
Copyright 2025 The Spice.ai OSS Authors

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

use async_trait::async_trait;
use std::{any::Any, fmt::Debug};

use datafusion::arrow::array::{BooleanArray, RecordBatch};
use datafusion::error::{DataFusionError, Result};
use snafu::prelude::*;

pub mod analyzer;
mod provider;
pub mod util;
pub use provider::*;
pub use util::{INDEXED_INNER, InnerProviderFn, find_concrete_table_provider_with};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Index table scans should have only one input. Received {input_len} inputs."))]
    MultipleInputs { input_len: usize },

    #[snafu(display(
        "Index table scans should have no expressions. Received {expr_len} expressions."
    ))]
    NoExpressions { expr_len: usize },
}

#[async_trait]
pub trait Index: Debug + Send + Sync + 'static {
    fn name(&self) -> &'static str;

    /// Columns that are required for the index to be computed.
    fn required_columns(&self) -> Vec<String>;

    /// Compute the index - if the index data is represented in the batch itself (i.e. a vector
    /// "*_embedding" column) then modify the provided batches to include the computed column.
    async fn compute_index(&self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        Ok(batches)
    }

    /// Maintain the index for one change-data-capture batch. `deleted` is a row-aligned
    /// mask over `batch` marking the rows the source deleted (`ChangeBatch::deletion_mask`);
    /// every other row is an upsert. Returns the batch with any computed index column, as
    /// [`Index::compute_index`] does — the change stream reattaches the operation column to
    /// the returned rows, so the row count and order must be preserved.
    ///
    /// The default upserts every row, ignoring `deleted`. That is correct only for an index
    /// whose whole state is the column it computes into the batch it returns. An index that
    /// keeps its own copy of the row — a full-text index, or an external vector store — must
    /// override this to remove the deleted rows, or search keeps matching rows the dataset
    /// no longer has.
    ///
    /// Wrapper implementations MUST forward this to the index they wrap: inheriting the
    /// default silently drops the wrapped index's delete handling.
    async fn compute_index_for_changes(
        &self,
        batch: RecordBatch,
        deleted: &BooleanArray,
    ) -> Result<RecordBatch> {
        let _ = deleted;
        let rows = batch.num_rows();
        let computed = self.compute_index(vec![batch]).await?;
        let [batch] = <[RecordBatch; 1]>::try_from(computed).map_err(|batches: Vec<_>| {
            DataFusionError::Internal(format!(
                "index {} returned {} batches for one {rows}-row change batch, expected 1",
                self.name(),
                batches.len(),
            ))
        })?;
        Ok(batch)
    }

    /// Called before data is written via the [`TableSink`] path (full refresh or append).
    ///
    /// Default is a no-op. Implementations use this to prepare external index state for a
    /// bounded write window. Not called for CDC writes.
    async fn on_write_start(&self) -> Result<()> {
        Ok(())
    }

    /// Called if a [`TableSink`] write fails after [`Index::on_write_start`] ran.
    ///
    /// Default is a no-op. Implementations use this to restore temporary external index
    /// settings when a refresh or append fails before [`Index::on_write_complete`] can run.
    async fn on_write_failed(&self) -> Result<()> {
        Ok(())
    }

    /// Called after data has been written via the [`TableSink`] path (full refresh or append).
    ///
    /// Default is a no-op. Implementations use this to create or verify persistent structures
    /// (e.g. a vector HNSW index) after each write. Using `IF NOT EXISTS` semantics makes it
    /// safe to call on both overwrite (recreates on new table) and append (no-op if index
    /// already exists). Not called for CDC writes — those maintain indexes automatically via
    /// `DuckDB` VSS on each insert.
    async fn on_write_complete(&self) -> Result<()> {
        Ok(())
    }

    /// Whether a failure in [`Index::on_write_complete`] must fail the write.
    ///
    /// Defaults to `false` (best-effort), matching indexes whose finalize step has
    /// `IF NOT EXISTS` semantics and is simply redone on the next refresh. An index
    /// that finalizes durable state the written data depends on returns `true`: for
    /// those, a failed finalize leaves the index stale while the write reports
    /// success, so the sink reports the write as failed instead.
    ///
    /// Wrapper implementations MUST forward this to the index they wrap — inheriting
    /// the default silently downgrades a fatal inner index to best-effort.
    fn write_complete_failure_is_fatal(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any;
}
