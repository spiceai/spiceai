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
    use arrow_schema::{DataType, Field, Schema};
    use data_components::arrow::write::MemTable;
    use datafusion::sql::TableReference;
    use runtime_search::embeddings::table::EmbeddingTable;
    use search::generation::text_search::index::FullTextDatabaseIndex;
    use spice_table::{IndexLayer, SpiceTable};
    use std::sync::Arc;

    fn base_table() -> Arc<dyn TableProvider> {
        Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "search_field",
                    DataType::Utf8,
                    false,
                )])),
                vec![vec![]],
            )
            .expect("failed to make table"),
        )
    }

    fn full_text_index(base: &Arc<dyn TableProvider>) -> Arc<dyn Index + Send + Sync> {
        Arc::new(
            FullTextDatabaseIndex::try_new(
                Arc::clone(base),
                vec!["search_field".to_string()],
                Some(vec!["search_field".to_string()]),
                None,
                &[],
            )
            .expect("cannot make full text table"),
        )
    }

    /// An index layer over `base`, as full-text registration builds it.
    fn indexed(base: &Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
        let index = full_text_index(base);
        SpiceTable::over(
            Arc::new(IndexLayer::with_indexes(vec![index])),
            Arc::clone(base),
        ) as Arc<dyn TableProvider>
    }

    #[test]
    fn a_bare_provider_carries_no_layers() {
        let base = base_table();
        assert!(spice_table::find_layer::<EmbeddingTable>(base.as_ref(), LayerWalk::Read).is_none());
        assert!(spice_table::find_layer::<IndexLayer>(base.as_ref(), LayerWalk::Index).is_none());
    }

    #[test]
    fn an_index_layer_is_discoverable_and_does_not_invent_others() {
        let wrapped = indexed(&base_table());

        assert!(spice_table::find_layer::<IndexLayer>(wrapped.as_ref(), LayerWalk::Index).is_some());
        assert!(spice_table::find_layer::<EmbeddingTable>(wrapped.as_ref(), LayerWalk::Read).is_none());
    }

    /// A provider with no accelerator behind it has no load to wait on, so it
    /// must never be reported as not-ready — otherwise search would reject
    /// federated-only datasets outright (#10956).
    #[test]
    fn not_ready_error_is_none_without_an_accelerated_table() {
        let base = base_table();
        assert!(
            RuntimeTableProviderExplorer.not_ready_error(&base).is_none(),
            "a non-accelerated provider must be scannable"
        );

        let wrapped = indexed(&base);
        assert!(
            RuntimeTableProviderExplorer
                .not_ready_error(&wrapped)
                .is_none(),
            "a layered non-accelerated provider must be scannable"
        );
    }

    #[test]
    fn read_discovery_sees_through_the_iceberg_cluster_layer() {
        let base = base_table();
        let wrapped: Arc<dyn TableProvider> = Arc::new(IcebergClusterTableProvider::new(
            TableReference::bare("trips"),
            Arc::clone(&base),
        ))
        .into_table();

        assert!(
            find_concrete_table_provider::<MemTable>(&wrapped).is_some(),
            "read discovery must see through the Iceberg cluster layer"
        );
    }

    /// A vector-enabled dataset nests its source under a vector-scan layer;
    /// read-path discovery (health checks, CDC ingest lookup) must see the
    /// source through it.
    #[test]
    fn read_discovery_sees_through_the_vector_scan_layer() {
        use search::index::VectorScanTableProvider;

        let base = base_table();
        let plan = datafusion::logical_expr::LogicalPlanBuilder::empty(false)
            .build()
            .expect("empty logical plan should build");
        let wrapped: Arc<dyn TableProvider> = Arc::new(VectorScanTableProvider {
            table_provider: Arc::clone(&base),
            vector_index_list: Arc::new(plan),
            primary_key: vec![],
        })
        .into_table();

        assert!(
            find_concrete_table_provider::<MemTable>(&wrapped).is_some(),
            "read discovery must see through the vector-scan layer"
        );
    }

    /// The gap that forced the `install()` seam: an index nested under a wrapper
    /// only `runtime` can name. `runtime-table` drives index discovery but
    /// cannot name `IcebergClusterTableProvider`, so it used to be handed a
    /// table of layer accessors at startup — and a missing entry meant discovery
    /// stopped here and silently reported no indexes.
    ///
    /// Nothing is handed down now: the cluster layer answers for itself, so the
    /// index below it is found. This is the regression test for that whole class
    /// of silent short traversal.
    #[test]
    fn an_index_below_a_runtime_owned_wrapper_is_still_discovered() {
        let base = base_table();
        let outer: Arc<dyn TableProvider> = Arc::new(IcebergClusterTableProvider::new(
            TableReference::bare("trips"),
            indexed(&base),
        ))
        .into_table();

        let (found, bound) = find_index_in_table_provider::<FullTextDatabaseIndex>(&outer)
            .expect("an index below a runtime-owned wrapper must be discovered");
        assert_eq!(found.len(), 1);
        assert!(
            bound.downcast_ref::<MemTable>().is_some(),
            "the index must be bound to the table beneath its own layer"
        );
    }

    /// CDC detection looks *for* an index layer, so it must not see past one.
    /// Were it transparent, a dataset whose indexes a change stream is supposed
    /// to maintain would be treated as having none.
    #[test]
    fn cdc_detection_stops_at_an_index_layer_that_reads_see_through() {
        let base = base_table();
        let wrapped = indexed(&base);

        assert!(
            Arc::ptr_eq(spice_table::peel_to(&wrapped, LayerWalk::Read), &base),
            "a read walk must see past an index layer"
        );
        assert!(
            Arc::ptr_eq(
                spice_table::peel_to(&wrapped, LayerWalk::CdcDetection),
                &wrapped
            ),
            "CDC detection must stop at the index layer it is looking for"
        );
    }
}
