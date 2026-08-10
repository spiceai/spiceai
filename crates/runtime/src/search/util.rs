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
#![allow(clippy::implicit_hasher)]

use std::sync::Arc;

use crate::accelerated::AcceleratedTable;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use spice_table::{Index, LayerWalk, SpiceTable, find_concrete};
use runtime_search::table_provider_explorer::TableProviderExplorer;

/// Attempt to return a concrete [`TableProvider`] type from a given
/// [`impl TableProvider`], stepping through every runtime wrapper layer that
/// is read-transparent (including `AcceleratedTable`, which routes a read walk
/// to its federated source). Use [`find_concrete`] directly to walk with a
/// different [`LayerWalk`].
pub fn find_concrete_table_provider<T: TableProvider + 'static>(
    tbl: &Arc<dyn TableProvider>,
) -> Option<&T> {
    find_concrete::<T>(tbl.as_ref(), LayerWalk::Read)
}

pub fn find_index_in_table_provider<T: Index + 'static>(
    tbl: &Arc<dyn TableProvider>,
) -> Option<(Vec<&T>, Arc<dyn TableProvider>)> {
    tbl.downcast_ref::<SpiceTable>()?.find_index::<T>()
}

/// Runtime's implementation of [`TableProviderExplorer`].
#[derive(Debug, Clone)]
pub struct RuntimeTableProviderExplorer;

impl TableProviderExplorer for RuntimeTableProviderExplorer {
    fn find_concrete<'a, T: TableProvider + 'static>(
        &self,
        tbl: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a T> {
        find_concrete_table_provider::<T>(tbl)
    }

    fn find_index<'a, T: Index + 'static>(
        &self,
        tbl: &'a Arc<dyn TableProvider>,
    ) -> Option<(Vec<&'a T>, Arc<dyn TableProvider>)> {
        find_index_in_table_provider::<T>(tbl)
    }

    fn not_ready_error(&self, tbl: &Arc<dyn TableProvider>) -> Option<DataFusionError> {
        spice_table::find_layer::<AcceleratedTable>(tbl.as_ref(), spice_table::LayerWalk::Read)?.not_ready_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataconnector::iceberg_cluster::IcebergClusterTableProvider;
    use crate::table_layers::ICEBERG_CLUSTER_LAYER;
    use arrow_schema::{DataType, Field, Schema};
    use data_components::arrow::write::MemTable;
    use spice_table::find_concrete_table_provider_in;
    use runtime_search::embeddings::table::EmbeddingTable;
    use search::generation::text_search::index::FullTextDatabaseIndex;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_find_concrete_table_provider_direct_match() {
        let base: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(Arc::new(Schema::empty()), vec![]).expect("failed to make table"),
        );

        assert!(find_concrete_table_provider::<EmbeddingTable>(&base).is_none());
    }

    #[test]
    fn test_find_concrete_table_provider_wrapped_in_full_text() {
        let base_table: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "search_field",
                    DataType::Utf8,
                    false,
                )])),
                vec![],
            )
            .expect("failed to make table"),
        );

        let index = Arc::new(
            FullTextDatabaseIndex::try_new(
                Arc::clone(&base_table),
                vec!["search_field".to_string()],
                Some(vec!["search_field".to_string()]),
                None,
                &[],
            )
            .expect("cannot make full text table"),
        );

        let wrapped_table = Arc::new(IndexLayer::new(base_table).add_index(index))
            as Arc<dyn TableProvider>;

        assert!(find_concrete_table_provider::<IndexLayer>(&wrapped_table).is_some());

        assert!(find_concrete_table_provider::<EmbeddingTable>(&wrapped_table).is_none());
    }

    /// A provider with no accelerator behind it has no load to wait on, so it
    /// must never be reported as not-ready — otherwise search would reject
    /// federated-only datasets outright (#10956).
    #[test]
    fn test_not_ready_error_is_none_without_an_accelerated_table() {
        let base: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(Arc::new(Schema::empty()), vec![]).expect("failed to make table"),
        );

        assert!(
            RuntimeTableProviderExplorer
                .not_ready_error(&base)
                .is_none(),
            "a non-accelerated provider must be scannable"
        );

        let wrapped: Arc<dyn TableProvider> = Arc::new(IndexLayer::new(base));
        assert!(
            RuntimeTableProviderExplorer
                .not_ready_error(&wrapped)
                .is_none(),
            "a wrapped non-accelerated provider must be scannable"
        );
    }

    #[test]
    fn test_find_concrete_table_provider_peels_iceberg_cluster_wrapper() {
        use datafusion::sql::TableReference;

        let base: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(Arc::new(Schema::empty()), vec![]).expect("failed to make table"),
        );

        let wrapped: Arc<dyn TableProvider> = Arc::new(IcebergClusterTableProvider::new(
            TableReference::bare("trips"),
            Arc::clone(&base),
        ));

        assert!(
            find_concrete_table_provider::<MemTable>(&wrapped).is_some(),
            "find_concrete_table_provider must peel IcebergClusterTableProvider"
        );
    }

    /// A vector-enabled dataset nests its source under a
    /// `VectorScanTableProvider`; read-path discovery (health checks, CDC
    /// ingest lookup) must see the source through it.
    #[test]
    fn test_find_concrete_table_provider_peels_vector_scan_wrapper() {
        use search::index::VectorScanTableProvider;

        let base: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(Arc::new(Schema::empty()), vec![]).expect("failed to make table"),
        );

        let plan = datafusion::logical_expr::LogicalPlanBuilder::empty(false)
            .build()
            .expect("empty logical plan should build");
        let wrapped: Arc<dyn TableProvider> = Arc::new(VectorScanTableProvider {
            table_provider: Arc::clone(&base),
            vector_index_list: Arc::new(plan),
            primary_key: vec![],
        });

        assert!(
            find_concrete_table_provider::<MemTable>(&wrapped).is_some(),
            "find_concrete_table_provider must peel VectorScanTableProvider"
        );
    }

    #[test]
    fn test_find_concrete_table_provider_in_respects_restricted_layer_set() {
        let base_table: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "search_field",
                    DataType::Utf8,
                    false,
                )])),
                vec![],
            )
            .expect("failed to make table"),
        );

        let index = Arc::new(
            FullTextDatabaseIndex::try_new(
                Arc::clone(&base_table),
                vec!["search_field".to_string()],
                Some(vec!["search_field".to_string()]),
                None,
                &[],
            )
            .expect("cannot make full text table"),
        );

        let wrapped = Arc::new(IndexLayer::new(base_table).add_index(index))
            as Arc<dyn TableProvider>;

        // The default set peels the IndexLayer down to the MemTable.
        assert!(
            find_concrete_table_provider::<MemTable>(&wrapped).is_some(),
            "default layer table must peel IndexLayer"
        );

        // A restricted set that lacks the indexed-table layer must not peel it.
        assert!(
            find_concrete_table_provider_in::<MemTable>(
                &wrapped,
                &[ICEBERG_CLUSTER_LAYER],
                LayerWalk::Read
            )
            .is_none(),
            "restricted layer sets must not peel layers outside the provided set"
        );

        // The layer itself is still reachable directly under a restricted set.
        assert!(
            find_concrete_table_provider_in::<IndexLayer>(&wrapped, &[], LayerWalk::Read)
                .is_some(),
            "an empty layer set must still match the outermost provider"
        );
    }
}
