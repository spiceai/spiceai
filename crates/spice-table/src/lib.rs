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

//! The table a Spice dataset presents to DataFusion.
//!
//! A dataset is a connector's own [`TableProvider`] with capabilities stacked
//! on top — indexes, embeddings, vector scans, spicepod metadata, acceleration.
//! [`SpiceTable`] is the single `TableProvider` that composes them, and
//! [`TableLayer`] is what each capability implements.

use async_trait::async_trait;
use std::{any::Any, fmt::Debug, sync::Arc};

use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::Result;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::Expr;

mod delete;
mod layer;
mod provider;
pub use delete::{build_key_match_predicate, resolve_keys_matching_predicate};
pub use layer::{LayerWalk, SpiceTable, TableLayer, find_concrete, peel_to};
pub use provider::IndexLayer;

/// What a `TableSink` write window does to the rows already in the table.
///
/// A replacing write removes rows by simply not re-sending them: it announces no deletions, so
/// neither [`Index::compute_index`] (which only ever sees the rows that *are* present) nor
/// [`Index::delete_by_keys`] (which only ever sees keys someone knows about) can observe the
/// removal. An index therefore has to be told the write's kind up front.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteWindow {
    /// Rows are added to what the table already holds. Index entries for rows absent from
    /// this write belong to rows that still exist, and must be preserved.
    Append,
    /// Every row in the table is replaced by this write's rows. An index whose entries do not
    /// live inside the accelerated table row must clear itself for this window, or it keeps
    /// entries for rows the source dropped.
    ReplaceAll,
}

impl From<InsertOp> for WriteWindow {
    fn from(op: InsertOp) -> Self {
        match op {
            // `UpdateType::Overwrite` — a `refresh_mode: full` refresh, which reproduces the
            // table's entire contents.
            InsertOp::Overwrite => WriteWindow::ReplaceAll,
            // `Append` adds rows. `Replace` is an upsert: it rewrites only the rows whose keys
            // collide and leaves every other row in place. Critically, `Replace` is also what
            // `UpdateType::Changes` maps to (see `DataFusion::write_data`), so it carries CDC
            // change batches — treating it as `ReplaceAll` would clear the whole index on
            // every change batch.
            InsertOp::Append | InsertOp::Replace => WriteWindow::Append,
        }
    }
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

    /// Called before data is written via the `TableSink` path (full refresh or append).
    ///
    /// `window` tells the index whether this write replaces the table's contents
    /// ([`WriteWindow::ReplaceAll`]) or adds to them ([`WriteWindow::Append`]). An index whose
    /// entries live outside the accelerated table row must clear itself on
    /// [`WriteWindow::ReplaceAll`] — ideally staged so the clear and the repopulation become
    /// visible together, so queries never observe a half-empty index.
    ///
    /// Default is a no-op. Implementations use this to prepare external index state for a
    /// bounded write window. Not called for CDC writes.
    async fn on_write_start(&self, _window: WriteWindow) -> Result<()> {
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
    /// `keys` may carry more columns than an implementation's own primary key (see
    /// [`Index::resolve_delete_keys`]'s default, which resolves on [`Index::required_columns`]
    /// rather than a narrower key) — implementations must look up their own known key column(s)
    /// by name and ignore anything else present, rather than assuming `keys`' schema is exactly
    /// their key.
    ///
    /// Full/both-scope by convention: a wrapper composing several backing indexes (e.g. a
    /// writethrough+fallback pair) must fan this out to every index it composes.
    async fn delete_by_keys(&self, keys: RecordBatch) -> Result<()> {
        let _ = keys;
        Ok(())
    }

    /// Whether [`Index::delete_by_keys`] removes *every* entry matching the key columns it finds
    /// in `keys`, even when those are a strict subset of this index's own primary key.
    ///
    /// Defaults to `false`: an index addressed by an exact key (S3 Vectors keys each vector by
    /// its full composite key) cannot act on a partial one, so a caller holding only part of the
    /// key must resolve the complete keys first. `true` says the store filters by field value and
    /// so deletes the whole matching group in one operation — Elasticsearch's `_delete_by_query`.
    ///
    /// The caller this exists for is `ChunkedSearchIndex`: it knows the base row key but not the
    /// chunk ids stored under it, and every chunk of a deleted row has to go. When this is `true`
    /// it hands that base key straight to [`Index::delete_by_keys`]; when `false` it must first
    /// enumerate the index's chunk-keyed entries itself.
    ///
    /// Wrapper implementations MUST forward this to the index they wrap — inheriting the default
    /// silently sends a partial-key-capable inner index down the enumerate-first path.
    fn deletes_by_partial_key(&self) -> bool {
        false
    }

    /// Resolves the primary-key rows of `table` matching `filters`, for a later
    /// [`Index::delete_by_keys`] call — the read half of [`Index::delete_by_predicate`], split
    /// out so a caller can run it *before* an authoritative row delete (while the matching rows
    /// still exist to resolve) and defer the actual [`Index::delete_by_keys`] call until after
    /// that row delete has succeeded.
    ///
    /// Default: resolves `filters` against `table` (scanning under the *original* predicate,
    /// before any row is actually removed — there is nothing left to resolve once they're gone),
    /// projected down to [`Index::required_columns`]. Returns `Ok(None)` when nothing matched.
    /// This is deliberately robust to `filters` referencing columns this index knows nothing
    /// about: the filter is evaluated against `table`'s *own* full schema (`table` is generally
    /// the real base/accelerated table, which has every column), and only the *projection* is
    /// narrowed to this index's columns — so an unrelated filter column never needs to exist in
    /// this index's own store, only on `table`.
    ///
    /// Override to return `Ok(None)` unconditionally for indexes whose [`Index::delete_by_keys`]
    /// stays the default no-op (co-located indexes — see `NativeVectorIndex`, the `DuckDB` VSS
    /// index) so a delete doesn't pay for a pointless resolve scan.
    async fn resolve_delete_keys(
        &self,
        table: &Arc<dyn TableProvider>,
        session: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Option<RecordBatch>> {
        let keys =
            resolve_keys_matching_predicate(table, session, filters, &self.required_columns())
                .await?;
        if keys.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Some(keys))
    }

    /// Delete index entries matching `filters` — the same predicate shape
    /// [`TableProvider::delete_from`] receives.
    ///
    /// Default: [`Index::resolve_delete_keys`] then [`Index::delete_by_keys`] with the result.
    ///
    /// Most implementations should not need to override this — override [`Index::delete_by_keys`]
    /// instead (or [`Index::resolve_delete_keys`] to skip a pointless resolve scan). Override this
    /// only when composing other indexes (see `CompoundSearchIndex`'s fan-out) or when an index's
    /// `required_columns` includes columns not visible to a consistent `delete_by_keys` shape.
    async fn delete_by_predicate(
        &self,
        table: &Arc<dyn TableProvider>,
        session: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<()> {
        let Some(keys) = self.resolve_delete_keys(table, session, filters).await? else {
            return Ok(());
        };
        self.delete_by_keys(keys).await
    }

    /// Whether a failure in [`Index::on_write_start`] must fail the write.
    ///
    /// Defaults to `false` (best-effort), matching indexes whose start step only tunes
    /// something the write does not depend on — Elasticsearch's `refresh_interval`
    /// override is the example: the write is still indexed correctly without it. An
    /// index that *prepares state the write depends on* returns `true`: for those,
    /// writing anyway leaves the index and the rows it indexes diverged, with only a
    /// warning to say so.
    ///
    /// Wrapper implementations MUST forward this to the index they wrap — inheriting
    /// the default silently downgrades a fatal inner index to best-effort.
    fn write_start_failure_is_fatal(&self) -> bool {
        false
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

#[cfg(test)]
mod tests {
    use super::{InsertOp, WriteWindow};

    /// The mapping decides whether an index clears itself, so each arm is spelled out. The
    /// `Replace` arm is the load-bearing one: it is an upsert, and it is also what
    /// `UpdateType::Changes` maps to, so mapping it to `ReplaceAll` would clear the entire
    /// index on every CDC change batch.
    #[test]
    fn write_window_is_derived_from_the_insert_op() {
        assert_eq!(
            WriteWindow::from(InsertOp::Overwrite),
            WriteWindow::ReplaceAll
        );
        assert_eq!(WriteWindow::from(InsertOp::Append), WriteWindow::Append);
        assert_eq!(WriteWindow::from(InsertOp::Replace), WriteWindow::Append);
    }
}
