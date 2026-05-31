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
use runtime_datafusion_index::{Index, IndexedTableProvider};
use runtime_search::embeddings::table::EmbeddingTable;
use runtime_search::table_provider_explorer::TableProviderExplorer;

/// Attempt to return a concrete [`TableProvider`] type from a given [`impl TableProvider`],
/// unwrapping known wrapper layers including `AcceleratedTable`.
pub fn find_concrete_table_provider<T: TableProvider + 'static>(
    tbl: &Arc<dyn TableProvider>,
) -> Option<&T> {
    let mut current_tbl = tbl;

    loop {
        if let Some(found_table) = current_tbl.as_any().downcast_ref::<T>() {
            return Some(found_table);
        }

        if let Some(index_table) = current_tbl.as_any().downcast_ref::<IndexedTableProvider>() {
            current_tbl = index_table.get_underlying_ref();
            continue;
        }

        if let Some(adaptor) = current_tbl
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>()
            && let Some(adapted_tbl) = adaptor.table_provider.as_ref()
        {
            current_tbl = adapted_tbl;
            continue;
        }

        if let Some(metadata_table) = current_tbl
            .as_any()
            .downcast_ref::<MetadataEnrichedTableProvider>()
        {
            current_tbl = metadata_table.get_inner_ref();
            continue;
        }

        if let Some(embedding_table) = current_tbl.as_any().downcast_ref::<EmbeddingTable>() {
            current_tbl = embedding_table.get_underlying_ref();
            continue;
        }

        if let Some(accelerated_table) = current_tbl.as_any().downcast_ref::<AcceleratedTable>() {
            current_tbl = accelerated_table
                .get_federated_table_ref()
                .try_table_provider_sync_ref()?;
            continue;
        }

        return None;
    }
}

pub fn find_index_in_table_provider<T: Index + 'static>(
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
