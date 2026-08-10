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

//! Inner-provider accessors for the wrappers this crate has to see through.
//!
//! Retention resolves keys against the *innermost* provider, so it needs to
//! unwrap the layers a dataset may be wrapped in. `runtime::table_layers` owns
//! the full set — including layers that need types from `runtime` itself, such
//! as the Iceberg cluster provider — and re-exports these two, which need only
//! crates at or below this one.

use std::sync::Arc;

use data_components::MetadataEnrichedTableProvider;
use datafusion_federation::FederatedTableProviderAdaptor;
use spice_table::InnerProviderFn;
use spice_table::{INDEXED_INNER, IndexLayer, TableProviderLayer};
use runtime_search::embeddings::table::EmbeddingTable;
use search::index::VectorScanTableProvider;

/// Inner-provider accessor for [`MetadataEnrichedTableProvider`].
pub const METADATA_ENRICHED_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<MetadataEnrichedTableProvider>()
        .map(MetadataEnrichedTableProvider::get_inner_ref)
};

/// Inner-provider accessor for [`EmbeddingTable`]: its base table.
pub const EMBEDDING_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<EmbeddingTable>()
        .map(EmbeddingTable::get_underlying_ref)
};

/// Inner-provider accessor for [`VectorScanTableProvider`]: its base table.
pub const VECTOR_SCAN_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<VectorScanTableProvider>()
        .map(|v| &v.table_provider)
};

/// [`IndexLayer`]: carries the search/vector indexes for a dataset.
/// Opaque to CDC detection — it is one of the layers that detection looks
/// *for* — and write-transparent (`insert_into` is a pass-through).
pub const INDEXED_LAYER: TableProviderLayer = TableProviderLayer {
    name: "IndexLayer",
    read: Some(INDEXED_INNER),
    cdc_detection: None,
    source: Some(INDEXED_INNER),
    write: Some(INDEXED_INNER),
    rebuild: Some(|outer, rebuild_inner| {
        let indexed = outer.downcast_ref::<IndexLayer>()?;
        Some(Arc::new(IndexLayer::with_indexes(
            rebuild_inner(indexed.get_underlying()),
            indexed.get_all_indexes(),
        )))
    }),
};

/// [`EmbeddingTable`]: merges synthetic `<col>_embedding` columns into its
/// schema. Opaque to CDC detection (detection looks for it); source peels
/// through it so a source connector's bootstrap `SELECT` never references the
/// synthetic columns.
pub const EMBEDDING_LAYER: TableProviderLayer = TableProviderLayer {
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
pub const VECTOR_SCAN_LAYER: TableProviderLayer = TableProviderLayer {
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

/// Inner-provider accessor for [`FederatedTableProviderAdaptor`].
pub const FEDERATED_ADAPTOR_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<FederatedTableProviderAdaptor>()
        .and_then(|adaptor| adaptor.table_provider.as_ref())
};

/// [`MetadataEnrichedTableProvider`]: injects spicepod table/column metadata
/// into the schema; carries no read, CDC or source semantics of its own. Its
/// rebuild *strips* the layer and recurses, because rebuild transforms are
/// how enrichment is (re)applied — the transform at the base of the stack
/// re-adds enrichment with current metadata, and keeping the stale layer
/// would shadow it.
pub const METADATA_ENRICHED_LAYER: TableProviderLayer = TableProviderLayer {
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
pub const FEDERATED_ADAPTOR_LAYER: TableProviderLayer = TableProviderLayer {
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

static INSTALLED: std::sync::OnceLock<&'static [TableProviderLayer]> = std::sync::OnceLock::new();

/// Installs the complete layer table.
///
/// `runtime` owns wrappers this crate cannot name — currently the Iceberg
/// cluster provider — so it supplies the full set. This matters for the
/// `LayerWalk::Read` index scan: a layer missing from the table is a wrapper the
/// walk stops at, so indexes beneath it are never discovered. Call once, before
/// any accelerated table is built.
pub fn install(layers: &'static [TableProviderLayer]) {
    if INSTALLED.set(layers).is_err() {
        tracing::warn!(
            "Table provider layers were already installed; ignoring the second set. \
             Layer discovery uses the first."
        );
    }
}

/// The layer table to walk.
///
/// There is exactly one table, and `runtime` owns it — this crate deliberately
/// keeps no list of its own, because a second (necessarily partial) list would
/// drift from the real one with nothing to catch it.
///
/// Empty until [`install`] runs. An empty table makes every walk stop at the
/// outermost provider rather than silently taking a wrong turn, and
/// `runtime`'s `every_layer_runtime_owns_reaches_the_accelerated_table` asserts
/// the install happens. Tests in this crate that exercise a walk install their
/// own table.
#[must_use]
pub fn layers() -> &'static [TableProviderLayer] {
    INSTALLED.get().copied().unwrap_or(&[])
}

/// Installs the layers this crate defines, for tests that exercise a walk
/// without a `Runtime` to install the real table.
///
/// Deliberately not a production constant: `runtime` owns the one table, and a
/// second production list would drift from it. This is scoped to tests, where the
/// wrappers only `runtime` can name cannot occur anyway. Idempotent, so it is safe
/// to call from every test regardless of process-per-test behaviour.
#[cfg(test)]
pub(crate) fn install_for_tests() {
    let _ = INSTALLED.set(&[
        INDEXED_LAYER,
        EMBEDDING_LAYER,
        VECTOR_SCAN_LAYER,
        METADATA_ENRICHED_LAYER,
        FEDERATED_ADAPTOR_LAYER,
    ]);
}
