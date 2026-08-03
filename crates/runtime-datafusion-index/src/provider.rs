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

use std::{borrow::Cow, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{ScanArgs, ScanResult, Session, TableProvider},
    common::{Constraints, Statistics},
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use crate::Index;

/// A `TableProvider` that wraps another `TableProvider` and adds indexing capabilities.
#[derive(Debug, Clone)]
pub struct IndexedTableProvider {
    /// The underlying `TableProvider` that provides the data.
    pub underlying: Arc<dyn TableProvider>,

    /// Indexes that are available to make queries more efficient or enable new functionality (i.e. full text search indexes).
    ///
    /// In the future, indexes will be required to implement a trait - but for now all existing
    /// use-cases are supported via UDTFs that downcast indexes to the correct type.
    pub indexes: Vec<Arc<dyn Index + Send + Sync>>,
}

impl IndexedTableProvider {
    pub fn new(underlying: Arc<dyn TableProvider>) -> Self {
        IndexedTableProvider::with_indexes(underlying, vec![])
    }

    pub fn with_indexes(
        underlying: Arc<dyn TableProvider>,
        indexes: Vec<Arc<dyn Index + Send + Sync>>,
    ) -> Self {
        Self {
            underlying,
            indexes,
        }
    }

    #[must_use]
    pub fn add_index(mut self, index: Arc<dyn Index + Send + Sync>) -> Self {
        self.indexes.push(index);
        self
    }

    #[must_use]
    pub fn get_index<T: Index + 'static>(&self) -> Option<&T> {
        self.indexes
            .iter()
            .find_map(|i| i.as_any().downcast_ref::<T>())
    }

    #[must_use]
    pub fn get_indexes<T: Index + 'static>(&self) -> Vec<&T> {
        self.indexes
            .iter()
            .filter_map(|i| i.as_any().downcast_ref::<T>())
            .collect()
    }

    #[must_use]
    pub fn get_all_indexes(&self) -> Vec<Arc<dyn Index + Send + Sync>> {
        self.indexes.clone()
    }

    #[must_use]
    pub fn get_underlying(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.underlying)
    }

    #[must_use]
    pub fn get_underlying_ref(&self) -> &Arc<dyn TableProvider> {
        &self.underlying
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl TableProvider for IndexedTableProvider {
    fn schema(&self) -> SchemaRef {
        self.underlying.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.underlying.constraints()
    }

    fn table_type(&self) -> TableType {
        self.underlying.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.underlying.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        // Cannot use underlying `get_logical_plan` as `IndexedTableProvider` will be replaced
        // with the `LogicalPlan` during indexing.
        None
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.underlying.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.underlying
            .scan(state, projection, filters, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.underlying.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.underlying.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.underlying.insert_into(state, input, insert_op).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Resolve each attached index's matching keys against `self.underlying`'s *current*
        // (pre-delete) rows first — there's nothing left to resolve once they're gone. The
        // accelerator-table delete below remains authoritative and runs first: only after it
        // succeeds do we best-effort delete the previously-resolved keys from each index. This
        // way a failed/partial row delete never leaves an index missing entries for rows that
        // were never actually removed. A resolve failure just skips that index's cleanup this
        // round (self-heals via full refresh); it never blocks the row delete.
        let mut resolved_keys = Vec::with_capacity(self.indexes.len());
        for index in &self.indexes {
            match index
                .resolve_delete_keys(&self.underlying, state, filters.clone())
                .await
            {
                Ok(Some(keys)) => resolved_keys.push((index, keys)),
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(
                        "Index '{}' failed to resolve entries for a table delete (skipping its cleanup this round): {e}",
                        index.name()
                    );
                }
            }
        }

        let result = self.underlying.delete_from(state, filters).await?;

        for (index, keys) in resolved_keys {
            if let Err(e) = index.delete_by_keys(keys).await {
                tracing::error!(
                    "Index '{}' failed to delete entries for a table delete (best-effort, continuing): {e}",
                    index.name()
                );
            }
        }

        Ok(result)
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // A SQL UPDATE can rewrite an indexed source column (text, embedding source), so an
        // independently stored index entry derived from the pre-update row would keep serving the
        // old value until a later rebuild happened to repair it. Unlike `delete_from`'s
        // best-effort cleanup, UPDATE treats index maintenance as a correctness barrier: resolve
        // each attached index's matching keys against `self.underlying`'s *current* (pre-update)
        // rows — there is nothing left to resolve once the UPDATE has run — and remove those
        // independently stored entries *before* delegating the UPDATE. The updated rows are then
        // reindexed through the normal refresh/CDC/write path (this is an invalidation boundary,
        // not an in-place reindex).
        //
        // A resolve or delete failure aborts with a structured error and the UPDATE is never
        // delegated, rather than reporting a successful update over stale search state — we
        // deliberately prefer temporarily-missing search results to incorrect old results.
        // Co-located indexes (whose entries live in the accelerated row itself) opt out by
        // returning `Ok(None)` from `resolve_delete_keys` and pay nothing here.
        //
        // The non-atomic trade-off: an UPDATE that later fails may have already invalidated index
        // entries for rows that were not actually changed. Without a transactional
        // provider/index contract this is the only ordering that satisfies the correctness
        // requirement, and such invalidation self-heals on the next refresh.
        for index in &self.indexes {
            let keys = index
                .resolve_delete_keys(&self.underlying, state, filters.clone())
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Index '{}' failed to resolve entries for a table update: {e}",
                        index.name()
                    ))
                })?;
            let Some(keys) = keys else { continue };
            index.delete_by_keys(keys).await.map_err(|e| {
                DataFusionError::Execution(format!(
                    "Index '{}' failed to delete entries for a table update: {e}",
                    index.name()
                ))
            })?;
        }

        self.underlying.update(state, assignments, filters).await
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DataFusionResult<ScanResult> {
        self.underlying.scan_with_args(state, args).await
    }

    async fn truncate(&self, state: &dyn Session) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.underlying.truncate(state).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolve_keys_matching_predicate;
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionContext;
    use std::any::Any;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn id_name_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .expect("valid batch")
    }

    /// A `TableProvider` whose reads come from a wrapped `MemTable`, but which records whether its
    /// `update` was delegated to. Lets a test assert the `IndexedTableProvider` only delegates the
    /// UPDATE after every index cleanup has succeeded.
    #[derive(Debug)]
    struct RecordingProvider {
        inner: Arc<dyn TableProvider>,
        updated: Arc<AtomicBool>,
    }

    #[async_trait]
    impl TableProvider for RecordingProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }

        async fn update(
            &self,
            _state: &dyn Session,
            _assignments: Vec<(String, Expr)>,
            _filters: Vec<Expr>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.updated.store(true, Ordering::SeqCst);
            Ok(Arc::new(EmptyExec::new(self.schema())))
        }
    }

    /// An [`Index`] whose default `resolve_delete_keys` scan is exercised for real, and which
    /// records the keys handed to `delete_by_keys`. `fail_resolve`/`fail_delete` inject failures
    /// so a test can assert the UPDATE is not delegated when index cleanup fails.
    #[derive(Debug)]
    struct RecordingIndex {
        required: Vec<String>,
        fail_resolve: bool,
        fail_delete: bool,
        deleted: Mutex<Vec<RecordBatch>>,
    }

    impl RecordingIndex {
        fn new(required: Vec<String>) -> Self {
            Self {
                required,
                fail_resolve: false,
                fail_delete: false,
                deleted: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Index for RecordingIndex {
        fn name(&self) -> &'static str {
            "recording"
        }

        fn required_columns(&self) -> Vec<String> {
            self.required.clone()
        }

        async fn resolve_delete_keys(
            &self,
            table: &Arc<dyn TableProvider>,
            session: &dyn Session,
            filters: Vec<Expr>,
        ) -> DataFusionResult<Option<RecordBatch>> {
            if self.fail_resolve {
                return Err(DataFusionError::Execution("boom-resolve".to_string()));
            }
            let keys =
                resolve_keys_matching_predicate(table, session, filters, &self.required).await?;
            if keys.num_rows() == 0 {
                return Ok(None);
            }
            Ok(Some(keys))
        }

        async fn delete_by_keys(&self, keys: RecordBatch) -> DataFusionResult<()> {
            if self.fail_delete {
                return Err(DataFusionError::Execution("boom-delete".to_string()));
            }
            self.deleted.lock().expect("lock not poisoned").push(keys);
            Ok(())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    /// Builds an `IndexedTableProvider` over a 3-row `MemTable` (id/name) plus the given index,
    /// returning the provider, the `updated` flag of the underlying provider, and a
    /// `SessionContext` to run against.
    fn setup(
        index: Arc<RecordingIndex>,
    ) -> (IndexedTableProvider, Arc<AtomicBool>, SessionContext) {
        let batch = id_name_batch(&[1, 2, 3], &["a", "b", "c"]);
        let schema = batch.schema();
        let mem: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table"));
        let updated = Arc::new(AtomicBool::new(false));
        let underlying: Arc<dyn TableProvider> = Arc::new(RecordingProvider {
            inner: mem,
            updated: Arc::clone(&updated),
        });
        let provider = IndexedTableProvider::with_indexes(
            underlying,
            vec![index as Arc<dyn Index + Send + Sync>],
        );
        (provider, updated, SessionContext::new())
    }

    #[tokio::test]
    async fn update_resolves_and_deletes_matching_pre_update_keys_before_delegating() {
        let index = Arc::new(RecordingIndex::new(vec!["id".to_string()]));
        let (provider, updated, ctx) = setup(Arc::clone(&index));

        provider
            .update(
                &ctx.state(),
                vec![("name".to_string(), lit("z"))],
                vec![col("id").eq(lit(2_i64))],
            )
            .await
            .expect("update should succeed");

        assert!(
            updated.load(Ordering::SeqCst),
            "underlying update must be delegated after index cleanup"
        );
        let deleted = index.deleted.lock().expect("lock not poisoned");
        assert_eq!(deleted.len(), 1, "exactly one delete_by_keys call");
        let id_col = deleted[0]
            .column_by_name("id")
            .expect("id column present")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        assert_eq!(
            id_col.values().to_vec(),
            vec![2],
            "only the pre-update key matching the UPDATE predicate is invalidated"
        );
    }

    #[tokio::test]
    async fn update_skips_index_with_no_matching_keys_but_still_delegates() {
        let index = Arc::new(RecordingIndex::new(vec!["id".to_string()]));
        let (provider, updated, ctx) = setup(Arc::clone(&index));

        provider
            .update(
                &ctx.state(),
                vec![("name".to_string(), lit("z"))],
                vec![col("id").eq(lit(999_i64))],
            )
            .await
            .expect("update should succeed");

        assert!(
            updated.load(Ordering::SeqCst),
            "an index with nothing to clean up must not block the update"
        );
        assert_eq!(
            index.deleted.lock().expect("lock not poisoned").len(),
            0,
            "no keys matched, so delete_by_keys must not be called"
        );
    }

    #[tokio::test]
    async fn update_aborts_without_delegating_when_delete_fails() {
        let index = Arc::new(RecordingIndex {
            required: vec!["id".to_string()],
            fail_resolve: false,
            fail_delete: true,
            deleted: Mutex::new(Vec::new()),
        });
        let (provider, updated, ctx) = setup(Arc::clone(&index));

        let err = provider
            .update(
                &ctx.state(),
                vec![("name".to_string(), lit("z"))],
                vec![col("id").eq(lit(2_i64))],
            )
            .await
            .expect_err("a failed index delete must fail the update");

        assert!(
            err.to_string().contains("failed to delete"),
            "error should describe the index delete failure: {err}"
        );
        assert!(
            !updated.load(Ordering::SeqCst),
            "the underlying update must not run when index cleanup fails"
        );
    }

    #[tokio::test]
    async fn update_aborts_without_delegating_when_resolve_fails() {
        let index = Arc::new(RecordingIndex {
            required: vec!["id".to_string()],
            fail_resolve: true,
            fail_delete: false,
            deleted: Mutex::new(Vec::new()),
        });
        let (provider, updated, ctx) = setup(Arc::clone(&index));

        let err = provider
            .update(
                &ctx.state(),
                vec![("name".to_string(), lit("z"))],
                vec![col("id").eq(lit(2_i64))],
            )
            .await
            .expect_err("a failed key resolution must fail the update");

        assert!(
            err.to_string().contains("failed to resolve"),
            "error should describe the index resolve failure: {err}"
        );
        assert!(
            !updated.load(Ordering::SeqCst),
            "the underlying update must not run when key resolution fails"
        );
    }
}
