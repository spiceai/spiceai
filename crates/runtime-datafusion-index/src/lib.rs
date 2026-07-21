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
use std::{any::Any, fmt::Debug, sync::Arc};

use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::Result;
use datafusion::prelude::Expr;
use snafu::prelude::*;

pub mod analyzer;
mod delete;
mod provider;
pub mod util;
pub use delete::{build_key_match_predicate, resolve_keys_matching_predicate};
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

    /// Delete index entries for the given primary-key rows.
    ///
    /// Default is a no-op — correct for indexes whose entries live inside the accelerated
    /// table row itself (co-located; removed automatically when the accelerator deletes the
    /// row). Implementations backed by a separate store (S3 Vectors, Elasticsearch) must
    /// override this to remove the corresponding entries there.
    ///
    /// Full/both-scope by convention: a wrapper composing several backing indexes (e.g. a
    /// writethrough+fallback pair) must fan this out to every index it composes.
    async fn delete_by_keys(&self, keys: RecordBatch) -> Result<()> {
        let _ = keys;
        Ok(())
    }

    /// Delete index entries whose key columns match every column present in `prefix_keys` —
    /// `prefix_keys`'s own schema names the columns to match on, which may be a strict subset of
    /// this index's full key.
    ///
    /// Default: delegates to [`Index::delete_by_keys`], which is correct whenever an index's full
    /// key IS the columns callers ever pass (the overwhelming majority of indexes). The one
    /// exception is a wrapper that augments its inner index's key with extra columns the caller
    /// never sees (`ChunkedSearchIndex`/`ChunkedVectorIndex`, which add a chunk id) — those
    /// implementations call `delete_by_key_prefix` on the *inner* index with the outer
    /// (unaugmented) keys, so the inner index must resolve/delete every entry matching just the
    /// given columns, regardless of the augmented column's value.
    async fn delete_by_key_prefix(&self, prefix_keys: RecordBatch) -> Result<()> {
        self.delete_by_keys(prefix_keys).await
    }

    /// Delete index entries matching `filters` — the same predicate shape
    /// [`TableProvider::delete_from`] receives.
    ///
    /// Default is a no-op. Implementations that need this (anything with primary keys — i.e.
    /// nearly everything except a bare co-located index) resolve `filters` to concrete primary
    /// keys via [`resolve_keys_matching_predicate`], scanning `accelerator` under the original
    /// predicate *before* any row is actually removed, then call [`Index::delete_by_keys`].
    async fn delete_by_predicate(
        &self,
        accelerator: &Arc<dyn TableProvider>,
        session: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<()> {
        let _ = (accelerator, session, filters);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any;
}
