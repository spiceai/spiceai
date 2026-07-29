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

use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
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

/// Whether a [`TableSink`] write replaces the index's entire contents or adds to them.
///
/// Derived from the `InsertOp` the sink already carries: `refresh_mode: full`
/// (`InsertOp::Overwrite`) yields [`IndexWriteMode::Overwrite`]; every other write yields
/// [`IndexWriteMode::Append`]. It is passed to the write lifecycle hooks so an external
/// index (one whose data does not live in the accelerator's own storage, e.g. Elasticsearch
/// or S3 Vectors) can remove entries that are no longer present in the source on a full
/// refresh. The accelerator's own storage is replaced by the `InsertOp::Overwrite` write
/// itself; this signal exists for the indexes that write side-effects that the `InsertOp`
/// never reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexWriteMode {
    /// The write adds to the existing index contents (`refresh_mode: append`, caching, and
    /// any non-overwrite write). Existing entries are left untouched.
    Append,
    /// The write replaces the index's entire contents with the written rows
    /// (`refresh_mode: full`). Entries whose primary keys are not written this cycle are
    /// stale and must be removed.
    Overwrite,
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

    /// Called before data is written via the [`TableSink`] path (full refresh or append).
    ///
    /// Default is a no-op. Implementations use this to prepare external index state for a
    /// bounded write window — including, for [`IndexWriteMode::Overwrite`], marking the start
    /// of a new generation so stale entries can be removed in [`Index::on_write_complete`].
    /// Guaranteed to run before any [`Index::compute_index`] call for the same write. Not
    /// called for CDC writes.
    ///
    /// Wrapper implementations MUST forward `mode` to the index they wrap.
    async fn on_write_start(&self, mode: IndexWriteMode) -> Result<()> {
        let _ = mode;
        Ok(())
    }

    /// Called if a [`TableSink`] write fails after [`Index::on_write_start`] ran.
    ///
    /// Default is a no-op. Implementations use this to restore temporary external index
    /// settings when a refresh or append fails before [`Index::on_write_complete`] can run.
    /// A failed [`IndexWriteMode::Overwrite`] must NOT delete anything: the previous
    /// generation stays intact and keeps serving queries, and the partial new generation is
    /// reconciled by the next successful full refresh.
    ///
    /// Wrapper implementations MUST forward `mode` to the index they wrap.
    async fn on_write_failed(&self, mode: IndexWriteMode) -> Result<()> {
        let _ = mode;
        Ok(())
    }

    /// Called after data has been written via the [`TableSink`] path (full refresh or append).
    ///
    /// Default is a no-op. Implementations use this to create or verify persistent structures
    /// (e.g. a vector HNSW index) after each write, and — for [`IndexWriteMode::Overwrite`] —
    /// to remove entries that were not written this cycle (stale rows dropped from the source
    /// since the previous full refresh). Using `IF NOT EXISTS` semantics makes finalize steps
    /// safe to call on both overwrite (recreates on new table) and append (no-op if index
    /// already exists). Not called for CDC writes — those maintain indexes automatically via
    /// `DuckDB` VSS on each insert.
    ///
    /// Wrapper implementations MUST forward `mode` to the index they wrap.
    async fn on_write_complete(&self, mode: IndexWriteMode) -> Result<()> {
        let _ = mode;
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
