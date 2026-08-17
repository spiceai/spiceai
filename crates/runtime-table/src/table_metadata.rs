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

//! Enriching a table provider with the spicepod-declared table and field metadata.
//!
//! The enrichment is pushed to the *base* of the provider stack and the read-path
//! layers are rebuilt around it, so each stays discoverable by downcast. The
//! `IndexTableScan` analyzer and the CDC changes stream both rely on that: wrapping
//! a layer opaquely hides it, so no changes stream is attached and
//! `refresh_mode: changes` fails with "a changes stream is required".

use std::collections::HashMap;

use data_components::{FieldMetadata, metadata_enriched_table_provider};
use datafusion::datasource::TableProvider;
use spicepod::semantic::Column;
use std::sync::Arc;

/// Pushes spicepod metadata enrichment to the base of the provider stack,
/// rebuilding the index, embedding and vector-scan layers around it so each
/// stays discoverable by downcast. The `IndexTableScan` analyzer and the CDC
/// changes stream both rely on that discoverability: wrapping one of these
/// layers opaquely instead hides it, so no changes stream is attached and
/// `refresh_mode: changes` fails with "a changes stream is required".
/// Enrichment is applied at the base so every layer above it — indexes,
/// embeddings, vector scans — stays intact and discoverable.
pub fn table_provider_with_spicepod_metadata<S: std::hash::BuildHasher>(
    provider: Arc<dyn TableProvider>,
    table_metadata: &HashMap<String, String, S>,
    columns: &[Column],
) -> Arc<dyn TableProvider> {
    let field_metadata = field_metadata_from_columns(columns);
    if table_metadata.is_empty() && field_metadata.is_empty() {
        return provider;
    }

    // `metadata_enriched_table_provider` takes the default hasher; rebuild once
    // here rather than per layer.
    let table_metadata: HashMap<String, String> = table_metadata
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let enrich = |base: Arc<dyn TableProvider>| {
        metadata_enriched_table_provider(base, table_metadata.clone(), field_metadata.clone())
    };

    match provider.downcast_ref::<spice_table::SpiceTable>() {
        Some(_) => spice_table::rebuild_base(&provider, &enrich),
        None => enrich(provider),
    }
}

fn field_metadata_from_columns(columns: &[Column]) -> FieldMetadata {
    columns
        .iter()
        .filter_map(|column| {
            let metadata = column.metadata();
            (!metadata.is_empty()).then(|| (column.name.clone(), metadata))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use data_components::MetadataEnrichedTableProvider;
    use datafusion::datasource::{MemTable, TableProvider};
    use runtime_search::embeddings::table::EmbeddingTable;
    use spice_table::{IndexLayer, LayerWalk, SpiceTable};
    use spicepod::semantic::Column;

    use super::table_provider_with_spicepod_metadata;

    #[test]
    fn embedding_table_metadata_wrap_preserves_downcast() {
        // Regression test for CDC-over-embeddings: when a dataset carries table- or
        // column-level metadata, `table_provider_with_spicepod_metadata` must keep an
        // `EmbeddingTable` discoverable via `downcast_ref::<EmbeddingTable>()` (pushing
        // the metadata onto the base table) rather than wrapping it opaquely in a
        // `MetadataEnrichedTableProvider`. `EmbeddingConnector::changes_stream` unwraps
        // the `EmbeddingTable` to its base table to build the source changes stream; an
        // opaque wrapper hides it, so no stream is attached and `refresh_mode: changes`
        // fails with "a changes stream is required".
        let base_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("content", DataType::Utf8, true),
        ]));
        let base_table = Arc::new(
            MemTable::try_new(base_schema, vec![vec![]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;

        let embedding_table = Arc::new(EmbeddingTable {
            base_table,
            embedded_columns: HashMap::new(),
            embedding_models: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        })
        .into_table() as Arc<dyn TableProvider>;

        let mut table_metadata = HashMap::new();
        table_metadata.insert("source_owner".to_string(), "analytics".to_string());
        let mut content_column = Column::new("content");
        content_column.description = Some("post body".to_string());
        let columns = vec![content_column];

        let wrapped = table_provider_with_spicepod_metadata(
            Arc::clone(&embedding_table),
            &table_metadata,
            &columns,
        );

        let embedding_node = spice_table::nodes(wrapped.as_ref(), LayerWalk::Read)
            .find(|node| node.layer_as::<EmbeddingTable>().is_some())
            .expect(
                "metadata wrap must keep the embedding layer discoverable for the changes stream",
            );

        // Metadata enrichment is pushed *below* the embedding layer, not stacked on
        // top of it, so the source-facing schema carries no synthetic columns.
        assert!(
            spice_table::find_layer::<MetadataEnrichedTableProvider>(
                embedding_node.below().as_ref(),
                LayerWalk::Read
            )
            .is_some(),
            "enrichment should sit below the embedding layer"
        );
    }

    /// Builds the provider stack a dataset with `embeddings` produces:
    /// `EmbeddingTable` over the source provider.
    fn embedding_table_over_memtable() -> Arc<dyn TableProvider> {
        let base_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("content", DataType::Utf8, true),
        ]));
        let base_table = Arc::new(
            MemTable::try_new(base_schema, vec![vec![]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;

        Arc::new(EmbeddingTable {
            base_table,
            embedded_columns: HashMap::new(),
            embedding_models: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        })
        .into_table() as Arc<dyn TableProvider>
    }

    /// Builds the provider stack a dataset with `vectors` enabled produces:
    /// `VectorScanTableProvider` over the source provider.
    fn vector_scan_over_memtable() -> Arc<dyn TableProvider> {
        let base_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("content", DataType::Utf8, true),
        ]));
        let base_table = Arc::new(
            MemTable::try_new(base_schema, vec![vec![]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;

        Arc::new(search::index::vector_table::VectorScanTableProvider {
            table_provider: base_table,
            indexes: vec![search::index::vector_table::VectorIndexJoin {
                vector_index_list: Arc::new(datafusion::logical_expr::LogicalPlan::EmptyRelation(
                    datafusion::logical_expr::EmptyRelation {
                        produce_one_row: false,
                        schema: Arc::new(datafusion::common::DFSchema::empty()),
                    },
                )),
                primary_key: vec!["id".to_string()],
            }],
        })
        .into_table() as Arc<dyn TableProvider>
    }

    fn spicepod_metadata_fixture() -> (HashMap<String, String>, Vec<Column>) {
        let mut table_metadata = HashMap::new();
        table_metadata.insert("source_owner".to_string(), "analytics".to_string());
        let mut content_column = Column::new("content");
        content_column.description = Some("post body".to_string());
        (table_metadata, vec![content_column])
    }

    #[test]
    fn embedding_table_metadata_wrap_preserves_table_metadata() {
        // `EmbeddingTable::schema()` rebuilds the schema from its base table's fields.
        // Pushing metadata enrichment onto the base table only works if that rebuild
        // carries the base schema's metadata; otherwise dataset-level spicepod
        // `metadata:` silently disappears from the source-facing schema.
        let (table_metadata, columns) = spicepod_metadata_fixture();

        let wrapped = table_provider_with_spicepod_metadata(
            embedding_table_over_memtable(),
            &table_metadata,
            &columns,
        );

        let schema = wrapped.schema();
        assert_eq!(
            schema.metadata().get("source_owner").map(String::as_str),
            Some("analytics"),
            "table-level spicepod metadata must survive the metadata wrap"
        );
        assert_eq!(
            schema
                .field_with_name("content")
                .expect("content field")
                .metadata()
                .get("description")
                .map(String::as_str),
            Some("post body"),
            "column-level spicepod metadata must survive the metadata wrap"
        );
    }

    #[test]
    fn embedding_table_under_indexed_provider_stays_discoverable() {
        // A dataset with both `embeddings` and `full_text_search` nests an
        // An embedding layer under the index layer the FTS connector builds.
        // `FullTextConnector::with_indexed_stream` hands the table beneath the index
        // layer to `EmbeddingConnector::changes_stream`, which looks for the embedding
        // layer; if the metadata wrap hides it there, no changes stream is attached and
        // `refresh_mode: changes` fails with "a changes stream is required".
        let indexed = SpiceTable::over(Arc::new(IndexLayer::new()), embedding_table_over_memtable())
            as Arc<dyn TableProvider>;

        let (table_metadata, columns) = spicepod_metadata_fixture();
        let wrapped = table_provider_with_spicepod_metadata(indexed, &table_metadata, &columns);

        let index_node = spice_table::nodes(wrapped.as_ref(), LayerWalk::Index)
            .find(|node| node.layer_as::<IndexLayer>().is_some())
            .expect("the index layer must remain discoverable for the index analyzer");

        assert!(
            spice_table::find_layer::<EmbeddingTable>(index_node.below().as_ref(), LayerWalk::Read)
                .is_some(),
            "an embedding layer nested under an index layer must stay discoverable"
        );
    }

    #[test]
    fn vector_scan_under_indexed_provider_stays_discoverable() {
        // A dataset with `embeddings`, `full_text_search` and `vectors` enabled nests a
        // `VectorScanTableProvider` under the `IndexLayer` (the FTS index is
        // added to the same provider the vector engine created).
        // `FullTextConnector::with_indexed_stream` hands `get_underlying()` to
        // `EmbeddingConnector::changes_stream`, which downcasts to
        // `VectorScanTableProvider` to reach the raw source; if the metadata wrap hides
        // it there, `refresh_mode: changes` fails with "a changes stream is required".
        let indexed = SpiceTable::over(Arc::new(IndexLayer::new()), vector_scan_over_memtable())
            as Arc<dyn TableProvider>;

        let (table_metadata, columns) = spicepod_metadata_fixture();
        let wrapped = table_provider_with_spicepod_metadata(indexed, &table_metadata, &columns);

        let index_node = spice_table::nodes(wrapped.as_ref(), LayerWalk::Index)
            .find(|node| node.layer_as::<IndexLayer>().is_some())
            .expect("the index layer must remain discoverable for the index analyzer");

        let vector_node = spice_table::nodes(index_node.below().as_ref(), LayerWalk::Read)
            .find(|node| {
                node.layer_as::<search::index::vector_table::VectorScanTableProvider>()
                    .is_some()
            })
            .expect("a vector-scan layer under an index layer must stay discoverable");

        // Enrichment lands below the vector scan, on the raw source provider, so the
        // source-facing bootstrap schema carries no synthetic embedding columns.
        assert!(
            spice_table::find_layer::<MetadataEnrichedTableProvider>(
                vector_node.below().as_ref(),
                LayerWalk::Read
            )
            .is_some(),
            "metadata enrichment should be pushed below the vector-scan layer"
        );

        // Reaching the enrichment layer is not enough: the vector-scan layer
        // rebuilds its schema from the table beneath, so it has to carry that
        // table's schema-level metadata forward or the dataset exposes none.
        assert_eq!(
            wrapped
                .schema()
                .metadata()
                .get("source_owner")
                .map(String::as_str),
            Some("analytics"),
            "a vector-enabled dataset must still expose its spicepod table metadata"
        );
    }
}
