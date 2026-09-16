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

//! The table a Spice dataset presents to `DataFusion`.
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
pub use layer::{
    LayerWalk, Nodes, SpiceTable, TableLayer, find_concrete, find_layer, nodes, peel_to,
    rebuild_base,
};
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

/// Whether an index removes the rest of a key group around its surviving members — the answer to
/// [`Index::group_pruning`], and what a caller of [`Index::delete_group_remainder`] can expect of
/// it.
///
/// Three states rather than two because an index can compose others: a compound whose one half
/// prunes and whose other half cannot should still be asked (so the half that can does), while
/// the caller still learns which entries stay behind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupPruning {
    /// Every store beneath this index prunes — or holds nothing of its own to prune, which the
    /// no-op default of [`Index::delete_group_remainder`] covers for a co-located index.
    Complete,
    /// Some store beneath this index prunes and the named ones cannot: their superseded
    /// entries stay in place. [`Index::delete_group_remainder`] still reaches the ones that
    /// can.
    Partial { cannot: Vec<&'static str> },
    /// Nothing beneath this index can prune, so there is no point handing it members.
    Unsupported,
}

impl GroupPruning {
    /// The names of the indexes that cannot prune, `own_name` standing for an index that reports
    /// [`GroupPruning::Unsupported`] for itself. What a caller names when it says the superseded
    /// entries stay in place.
    #[must_use]
    pub fn gaps(self, own_name: &'static str) -> Vec<&'static str> {
        match self {
            GroupPruning::Complete => Vec::new(),
            GroupPruning::Partial { cannot } => cannot,
            GroupPruning::Unsupported => vec![own_name],
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

    /// Delete every entry that agrees with a row of `members` on `group_columns` — a strict
    /// subset of this index's primary key — but whose full primary key is not itself a row of
    /// `members`: the rest of each named group, with the listed members kept.
    ///
    /// The caller this exists for is `ChunkedSearchIndex`, which stores one entry per chunk of
    /// a source row under the row's key plus a chunk id. After it has written a row's current
    /// chunks it hands them over as `members`, with the row's key columns as `group_columns`,
    /// and what goes is exactly the chunks a shorter text no longer produces. Nothing in
    /// `members` is touched, so this can run *after* the write: deleting the whole group first
    /// and rewriting it would leave a row with no entries at all if the rewrite then failed.
    ///
    /// `members` carries at least this index's primary-key columns; implementations look their
    /// key columns up by name and ignore anything else present, as [`Index::delete_by_keys`]
    /// does.
    ///
    /// Default is a no-op, which is correct for co-located indexes: their entries live in the
    /// accelerated table row, and the upsert rewrites that row whole. Whether an index is asked
    /// at all is decided by [`Index::group_pruning`], which defaults to "cannot".
    ///
    /// Wrapper implementations MUST forward this to the index they wrap — inheriting the default
    /// silently leaves the inner index's superseded entries in place.
    async fn delete_group_remainder(
        &self,
        group_columns: &[String],
        members: RecordBatch,
    ) -> Result<()> {
        let _ = (group_columns, members);
        Ok(())
    }

    /// Whether [`Index::delete_group_remainder`] leaves each named group holding exactly its
    /// members — see [`GroupPruning`].
    ///
    /// Defaults to [`GroupPruning::Unsupported`], the conservative answer (as
    /// [`Index::deletes_by_partial_key`] defaults to `false`): an index this trait knows nothing
    /// about is assumed to have a store of its own that cannot address entries by part of their
    /// key, so its [`Index::delete_group_remainder`] is never called and the caller says once
    /// that the superseded entries stay in place — a warning, where a default of "complete" would
    /// have let stale entries sit silently. An index that does leave each group holding exactly
    /// its members opts in with [`GroupPruning::Complete`]: one that implements the method, and a
    /// co-located index whose entries live in the accelerated table row (there is no separate
    /// group to prune, so the no-op default of the method already satisfies it). An index
    /// composing others combines their answers, so a caller keeps pruning the part that can
    /// ([`GroupPruning::Partial`]).
    ///
    /// Wrapper implementations MUST forward this to the index they wrap — inheriting the default
    /// reports a pruning the inner index does perform as unsupported.
    fn group_pruning(&self) -> GroupPruning {
        GroupPruning::Unsupported
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
    use super::{GroupPruning, Index, InsertOp, WriteWindow};
    use std::any::Any;

    /// An index this crate knows nothing about: no store declared, nothing overridden.
    #[derive(Debug)]
    struct UnknownIndex;

    impl Index for UnknownIndex {
        fn name(&self) -> &'static str {
            "unknown"
        }
        fn required_columns(&self) -> Vec<String> {
            vec![]
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    /// An index that implements nothing about group pruning must not be *reported* as pruning:
    /// the defaulted [`Index::delete_group_remainder`] is a no-op, so a default of "complete"
    /// would let a chunked wrapper over an out-of-tree store keep superseded chunks with no
    /// warning. The conservative default makes the wrapper say so instead.
    #[test]
    fn group_pruning_defaults_to_unsupported() {
        assert_eq!(UnknownIndex.group_pruning(), GroupPruning::Unsupported);
    }

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
