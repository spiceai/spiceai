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
    error::Result as DataFusionResult,
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
    use std::any::Any;
    use std::collections::HashSet;
    use std::sync::Mutex;

    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionContext;

    use crate::Index;

    fn id_val_batch(ids: &[i64], vals: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(vals.to_vec())),
            ],
        )
        .expect("valid batch")
    }

    /// An underlying table whose reads come from an inner `MemTable`, but whose `delete_from`
    /// always succeeds without doing the row removal. These tests assert that
    /// [`IndexedTableProvider::delete_from`] forwards the delete into an attached index; the
    /// accelerator's own row delete is covered by that accelerator's tests.
    #[derive(Debug)]
    struct WritableTable {
        inner: Arc<MemTable>,
    }

    impl WritableTable {
        fn new(batch: RecordBatch) -> Self {
            let schema = batch.schema();
            Self {
                inner: Arc::new(
                    MemTable::try_new(schema, vec![vec![batch]]).expect("valid mem table"),
                ),
            }
        }
    }

    #[async_trait]
    impl TableProvider for WritableTable {
        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }

        fn table_type(&self) -> TableType {
            self.inner.table_type()
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

        async fn delete_from(
            &self,
            _state: &dyn Session,
            _filters: Vec<Expr>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(EmptyExec::new(self.schema())))
        }
    }

    /// A fake index that models its store as a set of primary-key values. `delete_by_keys`
    /// removes the keys it is handed, so a test can assert the surviving set — without depending
    /// on how [`IndexedTableProvider`] resolves and forwards the delete.
    #[derive(Debug)]
    struct KeyTrackingIndex {
        key_column: String,
        keys: Arc<Mutex<HashSet<String>>>,
    }

    impl KeyTrackingIndex {
        fn new(key_column: &str, seed: &[i64]) -> Self {
            Self {
                key_column: key_column.to_string(),
                keys: Arc::new(Mutex::new(seed.iter().map(i64::to_string).collect())),
            }
        }
    }

    #[async_trait]
    impl Index for KeyTrackingIndex {
        fn name(&self) -> &'static str {
            "key_tracking_index"
        }

        fn required_columns(&self) -> Vec<String> {
            vec![self.key_column.clone()]
        }

        async fn delete_by_keys(&self, keys: RecordBatch) -> DataFusionResult<()> {
            let column = keys
                .column_by_name(&self.key_column)
                .expect("resolved key batch carries the index's key column");
            let as_string = datafusion::arrow::compute::cast(column, &DataType::Utf8)
                .expect("key column casts to Utf8");
            let values = as_string
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("cast produced a StringArray");
            let mut set = self.keys.lock().expect("lock");
            for value in values.iter().flatten() {
                set.remove(value);
            }
            Ok(())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn sorted_keys(keys: &Arc<Mutex<HashSet<String>>>) -> Vec<String> {
        let mut out: Vec<String> = keys.lock().expect("lock").iter().cloned().collect();
        out.sort();
        out
    }

    #[tokio::test]
    async fn delete_from_removes_the_resolved_keys_from_an_attached_index() {
        let index = KeyTrackingIndex::new("id", &[1, 2, 3]);
        let keys = Arc::clone(&index.keys);

        let provider = IndexedTableProvider::with_indexes(
            Arc::new(WritableTable::new(id_val_batch(
                &[1, 2, 3],
                &["a", "b", "c"],
            ))),
            vec![Arc::new(index)],
        );

        let ctx = SessionContext::new();
        provider
            .delete_from(&ctx.state(), vec![col("id").eq(lit(2_i64))])
            .await
            .expect("delete should succeed");

        assert_eq!(sorted_keys(&keys), vec!["1", "3"]);
    }

    #[tokio::test]
    async fn delete_from_resolves_the_predicate_against_a_non_key_column() {
        // The predicate filters on `val`, which is not the index's key column. The resolve must
        // still evaluate it against the full table and forward only the matching `id`s.
        let index = KeyTrackingIndex::new("id", &[1, 2, 3, 4]);
        let keys = Arc::clone(&index.keys);

        let provider = IndexedTableProvider::with_indexes(
            Arc::new(WritableTable::new(id_val_batch(
                &[1, 2, 3, 4],
                &["a", "b", "a", "b"],
            ))),
            vec![Arc::new(index)],
        );

        let ctx = SessionContext::new();
        provider
            .delete_from(&ctx.state(), vec![col("val").eq(lit("b"))])
            .await
            .expect("delete should succeed");

        assert_eq!(sorted_keys(&keys), vec!["1", "3"], "only val = 'b' rows go");
    }

    #[tokio::test]
    async fn delete_from_leaves_the_index_untouched_when_nothing_matches() {
        let index = KeyTrackingIndex::new("id", &[1, 2, 3]);
        let keys = Arc::clone(&index.keys);

        let provider = IndexedTableProvider::with_indexes(
            Arc::new(WritableTable::new(id_val_batch(
                &[1, 2, 3],
                &["a", "b", "c"],
            ))),
            vec![Arc::new(index)],
        );

        let ctx = SessionContext::new();
        provider
            .delete_from(&ctx.state(), vec![col("id").gt(lit(100_i64))])
            .await
            .expect("delete should succeed");

        assert_eq!(sorted_keys(&keys), vec!["1", "2", "3"]);
    }
}
