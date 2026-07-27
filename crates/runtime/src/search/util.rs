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

use crate::accelerated_table::AcceleratedTable;
use data_components::MetadataEnrichedTableProvider;
use datafusion::datasource::TableProvider;
use datafusion_federation::FederatedTableProviderAdaptor;
use runtime_datafusion_index::{
    INDEXED_INNER, Index, IndexedTableProvider, InnerProviderFn, find_concrete_table_provider_with,
};
use runtime_search::embeddings::table::EmbeddingTable;
use runtime_search::table_provider_explorer::TableProviderExplorer;

use crate::dataconnector::iceberg_cluster::IcebergClusterTableProvider;
use data_components::iceberg::delete::IcebergDeletionProvider;

/// Inner-provider accessor for [`FederatedTableProviderAdaptor`].
const FEDERATED_ADAPTOR_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<FederatedTableProviderAdaptor>()
        .and_then(|adaptor| adaptor.table_provider.as_ref())
};

/// Inner-provider accessor for [`MetadataEnrichedTableProvider`].
const METADATA_ENRICHED_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<MetadataEnrichedTableProvider>()
        .map(MetadataEnrichedTableProvider::get_inner_ref)
};

/// Inner-provider accessor for [`IcebergClusterTableProvider`].
const ICEBERG_CLUSTER_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<IcebergClusterTableProvider>()
        .map(IcebergClusterTableProvider::inner)
};

/// Inner-provider accessor for [`IcebergDeletionProvider`].
const ICEBERG_DELETION_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<IcebergDeletionProvider>()
        .map(IcebergDeletionProvider::inner)
};

/// Inner-provider accessor for [`EmbeddingTable`].
const EMBEDDING_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<EmbeddingTable>()
        .map(EmbeddingTable::get_underlying_ref)
};

/// Inner-provider accessor for [`AcceleratedTable`]. Resolves to the federated
/// provider only if it is available synchronously (a deferred provider that is
/// not yet ready yields `None`).
const ACCELERATED_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<AcceleratedTable>()
        .and_then(|accelerated| {
            accelerated
                .get_federated_table_ref()
                .try_table_provider_sync_ref()
        })
};

/// The full set of runtime wrapper layers understood by
/// [`find_concrete_table_provider`].
const DEFAULT_INNER_FNS: &[InnerProviderFn] = &[
    INDEXED_INNER,
    FEDERATED_ADAPTOR_INNER,
    METADATA_ENRICHED_INNER,
    ICEBERG_CLUSTER_INNER,
    ICEBERG_DELETION_INNER,
    EMBEDDING_INNER,
    ACCELERATED_INNER,
];

/// Attempt to return a concrete [`TableProvider`] type from a given
/// [`impl TableProvider`], unwrapping all known runtime wrapper layers
/// (including `AcceleratedTable`). See [`find_concrete_table_provider_with`]
/// to restrict which layers are peeled.
pub(crate) fn find_concrete_table_provider<T: TableProvider + 'static>(
    tbl: &Arc<dyn TableProvider>,
) -> Option<&T> {
    find_concrete_table_provider_with::<T>(tbl, DEFAULT_INNER_FNS)
}

pub(crate) fn find_index_in_table_provider<T: Index + 'static>(
    tbl: &Arc<dyn TableProvider>,
) -> Option<(Vec<&T>, Arc<dyn TableProvider>)> {
    if let Some(accelerated_table) = find_concrete_table_provider::<AcceleratedTable>(tbl)
        && let Some(indexes) =
            find_index_in_table_provider::<T>(accelerated_table.get_accelerator_ref())
    {
        return Some(indexes);
    }

    let mut indexed_table_opt = find_concrete_table_provider::<IndexedTableProvider>(tbl);
    while let Some(indexed_table) = indexed_table_opt {
        let indexes = indexed_table.get_indexes::<T>();
        if !indexes.is_empty() {
            return Some((indexes, Arc::clone(&indexed_table.underlying)));
        }
        indexed_table_opt =
            find_concrete_table_provider::<IndexedTableProvider>(&indexed_table.underlying);
    }
    None
}

/// Runtime's implementation of [`TableProviderExplorer`] that knows how to
/// unwrap `AcceleratedTable` and other runtime-specific wrappers.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use data_components::arrow::write::MemTable;
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

        let wrapped_table = Arc::new(IndexedTableProvider::new(base_table).add_index(index))
            as Arc<dyn TableProvider>;

        assert!(find_concrete_table_provider::<IndexedTableProvider>(&wrapped_table).is_some());

        assert!(find_concrete_table_provider::<EmbeddingTable>(&wrapped_table).is_none());
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

    #[test]
    fn test_find_concrete_table_provider_with_respects_restricted_layer_set() {
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

        let wrapped = Arc::new(IndexedTableProvider::new(base_table).add_index(index))
            as Arc<dyn TableProvider>;

        // The default set peels the IndexedTableProvider down to the MemTable.
        assert!(
            find_concrete_table_provider::<MemTable>(&wrapped).is_some(),
            "default unwrappers must peel IndexedTableProvider"
        );

        // A restricted set that lacks the indexed-table accessor must not peel it.
        assert!(
            find_concrete_table_provider_with::<MemTable>(&wrapped, &[ICEBERG_CLUSTER_INNER])
                .is_none(),
            "restricted accessors must not peel layers outside the provided set"
        );

        // The layer itself is still reachable directly under a restricted set.
        assert!(
            find_concrete_table_provider_with::<IndexedTableProvider>(&wrapped, &[]).is_some(),
            "an empty unwrapper set must still match the outermost provider"
        );
    }
}
