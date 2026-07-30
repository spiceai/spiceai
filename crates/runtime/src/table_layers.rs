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

//! The authoritative table of every wrapper layer that can appear in a
//! `TableProvider` stack assembled by the runtime.
//!
//! Dataset registration stacks wrappers around a source or accelerator
//! provider — index, embedding, vector-scan, metadata-enrichment, federation
//! and acceleration layers — and many subsystems then need to see through
//! that stack: search discovery, CDC stream detection, source peeling for
//! change streams, the CDC write path, and metadata enrichment pushdown.
//!
//! Each wrapper is described exactly once here, as a [`TableProviderLayer`]
//! declaring per-walk transparency and (where supported) how to rebuild the
//! layer around a transformed inner provider. Walk call sites select a walk
//! kind ([`runtime_datafusion_index::LayerWalk`]) instead of enumerating
//! wrapper types; a wrapper type
//! that is missing an entry here is opaque to every walk, which silently
//! disables the behavior the walk feeds (index writes on change streams, CDC
//! stream attachment, source peeling, pipelined CDC finalization).
//!
//! When adding a wrapper `TableProvider` to the runtime or a crate below it,
//! add its layer here in the same change and decide each field deliberately —
//! `None` is a semantic statement (the walk must stop), not a default.

use std::sync::Arc;

use datafusion_federation::FederatedTableProviderAdaptor;
use runtime_datafusion_index::{
    INDEXED_INNER, IndexedTableProvider, InnerProviderFn, TableProviderLayer,
};
use runtime_search::embeddings::table::EmbeddingTable;
use search::index::VectorScanTableProvider;

use crate::accelerated_table::AcceleratedTable;
use crate::dataconnector::iceberg_cluster::IcebergClusterTableProvider;
use data_components::MetadataEnrichedTableProvider;
use data_components::iceberg::delete::IcebergDeletionProvider;
use data_components::poly::PolyTableProvider;

/// Inner-provider accessor for [`FederatedTableProviderAdaptor`].
pub(crate) const FEDERATED_ADAPTOR_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<FederatedTableProviderAdaptor>()
        .and_then(|adaptor| adaptor.table_provider.as_ref())
};

/// Inner-provider accessor for [`MetadataEnrichedTableProvider`].
pub(crate) const METADATA_ENRICHED_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<MetadataEnrichedTableProvider>()
        .map(MetadataEnrichedTableProvider::get_inner_ref)
};

/// Inner-provider accessor for [`IcebergClusterTableProvider`].
pub(crate) const ICEBERG_CLUSTER_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<IcebergClusterTableProvider>()
        .map(IcebergClusterTableProvider::inner)
};

/// Inner-provider accessor for [`IcebergDeletionProvider`].
pub(crate) const ICEBERG_DELETION_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<IcebergDeletionProvider>()
        .map(IcebergDeletionProvider::inner)
};

/// Inner-provider accessor for [`EmbeddingTable`]: its base table.
pub(crate) const EMBEDDING_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<EmbeddingTable>()
        .map(EmbeddingTable::get_underlying_ref)
};

/// Inner-provider accessor for [`VectorScanTableProvider`]: its base table.
pub(crate) const VECTOR_SCAN_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<VectorScanTableProvider>()
        .map(|v| &v.table_provider)
};

/// Inner-provider accessor for [`AcceleratedTable`]. Resolves to the federated
/// provider only if it is available synchronously (a deferred provider that is
/// not yet ready yields `None`).
pub(crate) const ACCELERATED_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<AcceleratedTable>()
        .and_then(|accelerated| {
            accelerated
                .get_federated_table_ref()
                .try_table_provider_sync_ref()
        })
};

/// Inner-provider accessor for [`PolyTableProvider`]: its writer side.
pub(crate) const POLY_WRITER_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<PolyTableProvider>()
        .map(PolyTableProvider::writer_ref)
};

/// [`IndexedTableProvider`]: carries the search/vector indexes for a dataset.
/// Opaque to CDC detection — it is one of the layers that detection looks
/// *for* — and write-transparent (`insert_into` is a pass-through).
pub(crate) const INDEXED_LAYER: TableProviderLayer = TableProviderLayer {
    name: "IndexedTableProvider",
    read: Some(INDEXED_INNER),
    cdc_detection: None,
    source: Some(INDEXED_INNER),
    write: Some(INDEXED_INNER),
    rebuild: Some(|outer, rebuild_inner| {
        let indexed = outer.downcast_ref::<IndexedTableProvider>()?;
        Some(Arc::new(IndexedTableProvider::with_indexes(
            rebuild_inner(indexed.get_underlying()),
            indexed.get_all_indexes(),
        )))
    }),
};

/// [`EmbeddingTable`]: merges synthetic `<col>_embedding` columns into its
/// schema. Opaque to CDC detection (detection looks for it); source peels
/// through it so a source connector's bootstrap `SELECT` never references the
/// synthetic columns.
pub(crate) const EMBEDDING_LAYER: TableProviderLayer = TableProviderLayer {
    name: "EmbeddingTable",
    read: Some(EMBEDDING_INNER),
    cdc_detection: None,
    source: Some(EMBEDDING_INNER),
    write: None,
    rebuild: Some(|outer, rebuild_inner| {
        let embedding = outer.downcast_ref::<EmbeddingTable>()?;
        let mut rebuilt = embedding.clone();
        rebuilt.base_table = rebuild_inner(Arc::clone(&embedding.base_table));
        Some(Arc::new(rebuilt))
    }),
};

/// [`VectorScanTableProvider`]: merges vector-index columns into its schema.
/// Same walk profile as [`EMBEDDING_LAYER`] and for the same reasons.
pub(crate) const VECTOR_SCAN_LAYER: TableProviderLayer = TableProviderLayer {
    name: "VectorScanTableProvider",
    read: Some(VECTOR_SCAN_INNER),
    cdc_detection: None,
    source: Some(VECTOR_SCAN_INNER),
    write: None,
    rebuild: Some(|outer, rebuild_inner| {
        let vector_scan = outer.downcast_ref::<VectorScanTableProvider>()?;
        let mut rebuilt = vector_scan.clone();
        rebuilt.table_provider = rebuild_inner(Arc::clone(&vector_scan.table_provider));
        Some(Arc::new(rebuilt))
    }),
};

/// [`MetadataEnrichedTableProvider`]: injects spicepod table/column metadata
/// into the schema; carries no read, CDC or source semantics of its own. Its
/// rebuild *strips* the layer and recurses, because rebuild transforms are
/// how enrichment is (re)applied — the transform at the base of the stack
/// re-adds enrichment with current metadata, and keeping the stale layer
/// would shadow it.
pub(crate) const METADATA_ENRICHED_LAYER: TableProviderLayer = TableProviderLayer {
    name: "MetadataEnrichedTableProvider",
    read: Some(METADATA_ENRICHED_INNER),
    cdc_detection: Some(METADATA_ENRICHED_INNER),
    source: Some(METADATA_ENRICHED_INNER),
    write: None,
    rebuild: Some(|outer, rebuild_inner| {
        let enriched = outer.downcast_ref::<MetadataEnrichedTableProvider>()?;
        Some(rebuild_inner(Arc::clone(enriched.get_inner_ref())))
    }),
};

/// [`FederatedTableProviderAdaptor`]: the federation adaptor around a source
/// provider; transparent everywhere except writes. An adaptor with no inner
/// provider cannot be stepped through or rebuilt around, so walks stop at it
/// and rebuild keeps it unchanged.
pub(crate) const FEDERATED_ADAPTOR_LAYER: TableProviderLayer = TableProviderLayer {
    name: "FederatedTableProviderAdaptor",
    read: Some(FEDERATED_ADAPTOR_INNER),
    cdc_detection: Some(FEDERATED_ADAPTOR_INNER),
    source: Some(FEDERATED_ADAPTOR_INNER),
    write: None,
    rebuild: Some(|outer, rebuild_inner| {
        let adaptor = outer.downcast_ref::<FederatedTableProviderAdaptor>()?;
        let Some(inner) = adaptor.table_provider.as_ref() else {
            return Some(Arc::clone(outer));
        };
        Some(Arc::new(FederatedTableProviderAdaptor::new_with_provider(
            Arc::clone(&adaptor.source),
            rebuild_inner(Arc::clone(inner)),
        )))
    }),
};

/// [`IcebergClusterTableProvider`]: read-discovery only.
pub(crate) const ICEBERG_CLUSTER_LAYER: TableProviderLayer = TableProviderLayer {
    name: "IcebergClusterTableProvider",
    read: Some(ICEBERG_CLUSTER_INNER),
    cdc_detection: None,
    source: None,
    write: None,
    rebuild: None,
};

/// [`IcebergDeletionProvider`]: read-discovery only.
pub(crate) const ICEBERG_DELETION_LAYER: TableProviderLayer = TableProviderLayer {
    name: "IcebergDeletionProvider",
    read: Some(ICEBERG_DELETION_INNER),
    cdc_detection: None,
    source: None,
    write: None,
    rebuild: None,
};

/// [`AcceleratedTable`]: read-discovery steps to the *federated* (source)
/// provider when it is synchronously available. Walks that need the
/// accelerator side instead (index discovery, the CDC write path) branch to
/// it explicitly via [`AcceleratedTable::get_accelerator_ref`] — a single
/// inner accessor cannot express a two-child layer.
pub(crate) const ACCELERATED_LAYER: TableProviderLayer = TableProviderLayer {
    name: "AcceleratedTable",
    read: Some(ACCELERATED_INNER),
    cdc_detection: None,
    source: None,
    write: None,
    rebuild: None,
};

/// [`PolyTableProvider`]: the read/write split around an accelerator. The
/// write walk steps to its writer side; reads of an accelerated dataset reach
/// the accelerator through [`AcceleratedTable`], not by peeling this layer.
pub(crate) const POLY_LAYER: TableProviderLayer = TableProviderLayer {
    name: "PolyTableProvider",
    read: None,
    cdc_detection: None,
    source: None,
    write: Some(POLY_WRITER_INNER),
    rebuild: None,
};

/// `UpsertDedupTableProvider` rewrites writes (dedup / last-write-wins via
/// `UpsertDedupExec`), so every walk is opaque: routing a write — or anything
/// else — past it would bypass those semantics. A dedup-configured table
/// therefore stays on the synchronous CDC path, through the wrapper.
pub(crate) const UPSERT_DEDUP_LAYER: TableProviderLayer =
    TableProviderLayer::opaque("UpsertDedupTableProvider");

/// Every wrapper layer the runtime can stack around a dataset's provider —
/// the layer set walks use unless a call site has a documented reason to
/// restrict it.
pub(crate) const TABLE_PROVIDER_LAYERS: &[TableProviderLayer] = &[
    INDEXED_LAYER,
    EMBEDDING_LAYER,
    VECTOR_SCAN_LAYER,
    METADATA_ENRICHED_LAYER,
    FEDERATED_ADAPTOR_LAYER,
    ICEBERG_CLUSTER_LAYER,
    ICEBERG_DELETION_LAYER,
    ACCELERATED_LAYER,
    POLY_LAYER,
    UPSERT_DEDUP_LAYER,
];

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use data_components::arrow::write::MemTable;
    use data_components::metadata_enriched_table_provider;
    use datafusion::datasource::TableProvider;
    use runtime_datafusion_index::LayerWalk;
    use std::collections::HashMap;

    fn mem_table() -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        Arc::new(MemTable::try_new(schema, vec![]).expect("mem table should be created"))
    }

    #[test]
    fn layer_names_are_unique() {
        let mut names: Vec<&str> = TABLE_PROVIDER_LAYERS.iter().map(|l| l.name).collect();
        names.sort_unstable();
        let total = names.len();
        names.dedup();
        assert_eq!(
            names.len(),
            total,
            "each wrapper type must have exactly one layer entry"
        );
    }

    /// Walk transparency narrows monotonically: a layer with no CDC meaning
    /// must also peel on the source walk (its schema additions may not leak
    /// into source queries), and any layer the source walk peels must also be
    /// read-transparent. A layer breaking this ordering is almost certainly a
    /// table-editing mistake; if it is intentional, document the reason on the
    /// layer and update this test.
    #[test]
    fn layer_walk_transparency_narrows_monotonically() {
        for layer in TABLE_PROVIDER_LAYERS {
            if layer.cdc_detection.is_some() {
                assert!(
                    layer.source.is_some(),
                    "{}: CDC-transparent layers must be source-transparent",
                    layer.name
                );
            }
            if layer.source.is_some() {
                assert!(
                    layer.read.is_some(),
                    "{}: source-transparent layers must be read-transparent",
                    layer.name
                );
            }
        }
    }

    /// Per-layer step behavior for the cheaply-constructible wrappers: each
    /// walk steps through a layer exactly when its descriptor declares it.
    #[test]
    fn layer_walks_match_declared_transparency() {
        let base = mem_table();

        let indexed: Arc<dyn TableProvider> =
            Arc::new(IndexedTableProvider::new(Arc::clone(&base)));
        let enriched = metadata_enriched_table_provider(
            Arc::clone(&base),
            HashMap::from([("k".to_string(), "v".to_string())]),
            data_components::FieldMetadata::new(),
        );

        for (provider, layer, expected) in [
            (&indexed, &INDEXED_LAYER, [true, false, true, true]),
            (
                &enriched,
                &METADATA_ENRICHED_LAYER,
                [true, true, true, false],
            ),
        ] {
            let walks = [
                LayerWalk::Read,
                LayerWalk::CdcDetection,
                LayerWalk::Source,
                LayerWalk::Write,
            ];
            for (walk, expect_step) in walks.into_iter().zip(expected) {
                let stepped = layer
                    .inner_for(walk)
                    .and_then(|inner| inner(provider.as_ref()))
                    .is_some();
                assert_eq!(
                    stepped, expect_step,
                    "{}: {walk:?} transparency mismatch",
                    layer.name
                );
            }
        }
    }
}
