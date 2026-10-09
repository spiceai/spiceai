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

//! Serving a Spice table to Iceberg clients as the Iceberg table it reads.
//!
//! An Iceberg client reads a table's data files itself, so the only table Spice
//! can hand out is one whose every scan returns exactly the rows of an Iceberg
//! table: one it reads from that table unchanged. Anything else (an
//! acceleration, computed columns, a view, a source that is not Iceberg) is
//! refused with the reason, rather than described by metadata a client would
//! read as an empty table.

use std::sync::Arc;

use data_components::poly::PolyTableProvider;
use datafusion::{catalog::TableProvider, common::TableReference, datasource::TableType};
use iceberg_datafusion::IcebergTableProvider;
use runtime_search::embeddings::table::EmbeddingTable;
use runtime_table::accelerated::AcceleratedTable;
use search::index::vector_table::VectorScanTableProvider;
use spice_table::{LayerWalk, SpiceTable, peel_to};

/// Where the `loadTable` behavior is documented.
const LOAD_TABLE_DOCS: &str = "https://spiceai.org/docs/api/HTTP/get-table";

/// Why a table cannot be served as an Iceberg table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NotServedAsIceberg {
    /// Queries are answered from an acceleration, not from the source.
    Accelerated,
    /// Spice computes columns, such as embeddings, that the source lacks.
    ComputedColumns,
    /// Spice changes what a scan of the table returns in some other way.
    Transformed,
    /// A view: there is no table to read.
    View,
    /// The source is not an Iceberg table.
    NotIceberg,
    /// Reads are pinned to one snapshot rather than the table's current one.
    PinnedSnapshot,
}

/// The Iceberg table `table` can be served as: the table reached by peeling
/// every layer that leaves reads unchanged.
pub(crate) fn passthrough_source(
    table: &Arc<dyn TableProvider>,
) -> Result<&IcebergTableProvider, NotServedAsIceberg> {
    let reached = peel_to(table, LayerWalk::Passthrough);
    match reached.downcast_ref::<IcebergTableProvider>() {
        Some(provider) if provider.snapshot_id().is_none() => Ok(provider),
        Some(_) => Err(NotServedAsIceberg::PinnedSnapshot),
        None => Err(why_not(reached)),
    }
}

/// Why the passthrough walk stopped at `stopped_at` without reaching an
/// Iceberg table.
fn why_not(stopped_at: &Arc<dyn TableProvider>) -> NotServedAsIceberg {
    if let Some(table) = stopped_at.downcast_ref::<SpiceTable>() {
        if table.layer_as::<AcceleratedTable>().is_some()
            || table.layer_as::<PolyTableProvider>().is_some()
        {
            return NotServedAsIceberg::Accelerated;
        }
        if table.layer_as::<EmbeddingTable>().is_some()
            || table.layer_as::<VectorScanTableProvider>().is_some()
        {
            return NotServedAsIceberg::ComputedColumns;
        }
        return NotServedAsIceberg::Transformed;
    }
    if stopped_at.table_type() == TableType::View {
        return NotServedAsIceberg::View;
    }
    NotServedAsIceberg::NotIceberg
}

/// The error an Iceberg client gets for a table it cannot read as Iceberg.
pub(crate) fn not_served_as_iceberg_message(
    table: &TableReference,
    reason: NotServedAsIceberg,
) -> String {
    let because = match reason {
        NotServedAsIceberg::Accelerated => {
            "it is accelerated, so Spice answers queries from its acceleration rather than from an Iceberg table that Iceberg clients can read directly"
        }
        NotServedAsIceberg::ComputedColumns => {
            "it has columns that Spice computes, such as embeddings, so an Iceberg client reading its source table would not see them"
        }
        NotServedAsIceberg::Transformed => {
            "Spice changes what a scan of it returns, so an Iceberg client reading its source table would not see the same rows"
        }
        NotServedAsIceberg::View => {
            "it is a view, so there is no Iceberg table for Iceberg clients to read"
        }
        NotServedAsIceberg::NotIceberg => {
            "it does not read from an Iceberg table, so there is no Iceberg table for Iceberg clients to read"
        }
        NotServedAsIceberg::PinnedSnapshot => {
            "it reads a fixed snapshot of its Iceberg table rather than the table's current state"
        }
    };
    format!(
        "Failed to load table '{table}' as an Iceberg table: {because}. Query it with SQL through Spice instead, over HTTP (`/v1/sql`) or Arrow Flight SQL. See: {LOAD_TABLE_DOCS}"
    )
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        datasource::MemTable,
    };

    use super::*;

    fn mem_table() -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("mem table"))
    }

    #[test]
    fn a_table_that_is_not_iceberg_is_refused() {
        assert_eq!(
            passthrough_source(&mem_table()).err(),
            Some(NotServedAsIceberg::NotIceberg)
        );
    }

    #[test]
    fn refusals_name_the_table_the_reason_and_the_fix() {
        let table = TableReference::full("spice", "public", "orders");
        for (reason, clause) in [
            (NotServedAsIceberg::Accelerated, "it is accelerated"),
            (
                NotServedAsIceberg::ComputedColumns,
                "columns that Spice computes",
            ),
            (NotServedAsIceberg::Transformed, "Spice changes what a scan"),
            (NotServedAsIceberg::View, "it is a view"),
            (
                NotServedAsIceberg::NotIceberg,
                "does not read from an Iceberg table",
            ),
            (NotServedAsIceberg::PinnedSnapshot, "a fixed snapshot"),
        ] {
            let message = not_served_as_iceberg_message(&table, reason);
            assert!(
                message.starts_with(
                    "Failed to load table 'spice.public.orders' as an Iceberg table: "
                ),
                "{message}"
            );
            assert!(message.contains(clause), "{message}");
            assert!(
                message.contains("Query it with SQL through Spice instead"),
                "{message}"
            );
            assert!(message.contains(LOAD_TABLE_DOCS), "{message}");
        }
    }

    use std::collections::HashMap;

    use datafusion::prelude::SessionContext;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use spice_table::{IndexLayer, TableLayer};

    use crate::dataconnector::iceberg_cluster::IcebergClusterTableProvider;

    /// An in-memory catalog holding one table with three rows, so the table has
    /// a current snapshot and a metadata file.
    async fn catalog_with_rows() -> (Arc<dyn Catalog>, TableIdent) {
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory:/warehouse".to_string(),
                )]),
            )
            .await
            .expect("memory catalog loads");
        let namespace = NamespaceIdent::new("sales".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("namespace is created");
        let schema = iceberg::spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema builds");
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("orders".to_string())
                    .schema(schema)
                    .build(),
            )
            .await
            .expect("table is created");
        let catalog: Arc<dyn Catalog> = Arc::new(catalog);
        let ident = TableIdent::new(namespace, "orders".to_string());

        let ctx = SessionContext::new();
        ctx.register_table("orders", Arc::new(provider_for(&catalog, &ident).await))
            .expect("provider registers");
        ctx.sql("INSERT INTO orders VALUES (1), (2), (3)")
            .await
            .expect("append plans")
            .collect()
            .await
            .expect("append commits");
        (catalog, ident)
    }

    async fn provider_for(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> IcebergTableProvider {
        IcebergTableProvider::try_new(
            Arc::clone(catalog),
            ident.namespace().clone(),
            ident.name().to_string(),
        )
        .await
        .expect("provider is constructed")
    }

    /// A layer that says nothing about what a scan through it returns.
    #[derive(Debug)]
    struct UnknownLayer;

    impl TableLayer for UnknownLayer {}

    #[tokio::test]
    async fn an_iceberg_table_read_unchanged_is_served_as_itself() {
        let (catalog, ident) = catalog_with_rows().await;
        let provider: Arc<dyn TableProvider> = Arc::new(provider_for(&catalog, &ident).await);
        let indexed: Arc<dyn TableProvider> = SpiceTable::over(
            Arc::new(IndexLayer::with_indexes(vec![])),
            Arc::clone(&provider),
        );

        for table in [&provider, &indexed] {
            let source = passthrough_source(table).expect("the Iceberg table is reached");
            assert_eq!(source.table_ident(), &ident);

            let served = source
                .catalog()
                .load_table(source.table_ident())
                .await
                .expect("the table loads");
            let current = catalog.load_table(&ident).await.expect("the table loads");
            assert!(served.metadata_location().is_some());
            assert_eq!(served.metadata_location(), current.metadata_location());
            assert_eq!(
                served.metadata().current_snapshot_id(),
                current.metadata().current_snapshot_id()
            );
            assert!(
                served.metadata().current_snapshot_id().is_some(),
                "the rows written are in the served snapshot"
            );
            assert_eq!(served.metadata().uuid(), current.metadata().uuid());
        }
    }

    #[tokio::test]
    async fn a_table_behind_a_layer_that_does_not_opt_in_is_refused() {
        let (catalog, ident) = catalog_with_rows().await;
        let provider: Arc<dyn TableProvider> = Arc::new(provider_for(&catalog, &ident).await);
        let transformed: Arc<dyn TableProvider> =
            SpiceTable::over(Arc::new(UnknownLayer), provider);

        assert_eq!(
            passthrough_source(&transformed).err(),
            Some(NotServedAsIceberg::Transformed)
        );
    }

    /// The cluster layer scans the table it wraps, so passthrough serves that
    /// table even when a rebuild has put a different base beneath the layer.
    #[tokio::test]
    async fn the_cluster_layer_serves_the_table_it_scans() {
        let (catalog, ident) = catalog_with_rows().await;
        let scanned: Arc<dyn TableProvider> = Arc::new(provider_for(&catalog, &ident).await);
        let cluster = Arc::new(IcebergClusterTableProvider::new(
            TableReference::bare("orders"),
            Arc::clone(&scanned),
        ));
        let rebuilt: Arc<dyn TableProvider> = SpiceTable::over(cluster, mem_table());

        let source = passthrough_source(&rebuilt).expect("the scanned Iceberg table is reached");
        let expected = scanned
            .downcast_ref::<IcebergTableProvider>()
            .expect("the scanned table is an Iceberg table");
        assert!(std::ptr::eq(source, expected));
    }

    #[tokio::test]
    async fn a_provider_pinned_to_a_snapshot_is_refused() {
        let (catalog, ident) = catalog_with_rows().await;
        let pinned = catalog
            .load_table(&ident)
            .await
            .expect("the table loads")
            .metadata()
            .current_snapshot_id();
        let provider: Arc<dyn TableProvider> = Arc::new(
            provider_for(&catalog, &ident)
                .await
                .with_snapshot_id(pinned),
        );

        assert_eq!(
            passthrough_source(&provider).err(),
            Some(NotServedAsIceberg::PinnedSnapshot)
        );
    }

    /// Iceberg clients parse the body as the REST spec's `LoadTableResult`,
    /// and it carries no catalog or storage configuration of Spice's own.
    #[tokio::test]
    async fn the_served_table_is_a_load_table_result_with_no_config() {
        let (catalog, ident) = catalog_with_rows().await;
        let served = catalog.load_table(&ident).await.expect("the table loads");

        let body = super::super::tables::load_table_body(&served).expect("the body serializes");
        let parsed: iceberg_catalog_rest::LoadTableResult =
            serde_json::from_slice(&body).expect("an Iceberg client parses the body");
        assert_eq!(
            parsed.metadata_location.as_deref(),
            served.metadata_location()
        );
        assert_eq!(&parsed.metadata, served.metadata());
        assert!(parsed.config.is_empty());
        assert!(parsed.storage_credentials.is_none());

        let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
        let mut keys: Vec<&str> = json
            .as_object()
            .expect("an object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["metadata", "metadata-location"]);
    }
}
