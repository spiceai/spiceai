/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::catalog_filter::TableSelector;
use async_trait::async_trait;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider},
    common::TableReference,
    datasource::TableProvider,
    error::DataFusionError,
};
use futures::{StreamExt, future::BoxFuture};
use snafu::prelude::*;
use std::{
    collections::HashMap,
    fmt::Write,
    sync::{Arc, RwLock},
};

use crate::{Read, RefreshableCatalogProvider};

use super::{CatalogId, Result, UCSchema, UCTable, UnityCatalog};

/// Creates `DataFusion` table providers for Unity Catalog tables.
///
/// Unlike [`Read`], implementations receive the full [`UCTable`] so they can
/// use table metadata beyond the storage location — e.g. the `table_id`
/// needed for credential vending.
#[async_trait]
pub trait UCTableProviderFactory: Send + Sync {
    /// The reference used to construct and identify the table.
    ///
    /// Returns `None` when the table cannot be materialized (e.g. it has no
    /// storage location); such tables are skipped.
    fn table_reference(&self, table: &UCTable) -> Option<TableReference>;

    async fn table_provider(
        &self,
        table: &UCTable,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Adapts an `(Arc<dyn Read>, table_reference_creator)` pair to
/// [`UCTableProviderFactory`] for table creators that only need the table
/// reference.
pub struct ReadTableProviderFactory {
    read: Arc<dyn Read>,
    table_reference_creator: fn(&UCTable) -> Option<TableReference>,
}

impl ReadTableProviderFactory {
    pub fn new(
        read: Arc<dyn Read>,
        table_reference_creator: fn(&UCTable) -> Option<TableReference>,
    ) -> Self {
        Self {
            read,
            table_reference_creator,
        }
    }
}

#[async_trait]
impl UCTableProviderFactory for ReadTableProviderFactory {
    fn table_reference(&self, table: &UCTable) -> Option<TableReference> {
        (self.table_reference_creator)(table)
    }

    async fn table_provider(
        &self,
        _table: &UCTable,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        self.read.table_provider(table_reference).await
    }
}

/// Called after a refresh replaced or removed a registered table. Plans and results
/// cached against the previous table keep reading it until they are discarded.
pub type CatalogChangeListener = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

pub struct UnityCatalogProvider {
    schemas: RwLock<HashMap<String, Arc<UnityCatalogSchemaProvider>>>,
    client: Arc<UnityCatalog>,
    catalog_id: CatalogId,
    table_creator: Arc<dyn UCTableProviderFactory>,
    selector: TableSelector,
    on_change: Option<CatalogChangeListener>,
}

impl std::fmt::Debug for UnityCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnityCatalogProvider")
            .field("catalog_id", &self.catalog_id.0)
            .field("schemas", &self.schemas)
            .finish_non_exhaustive()
    }
}

/// Schema refreshes that run at once during a catalog refresh.
const MAX_CONCURRENT_SCHEMA_REFRESHES: usize = 5;

impl UnityCatalogProvider {
    pub async fn try_new(
        client: Arc<UnityCatalog>,
        catalog_id: CatalogId,
        table_creator: Arc<dyn UCTableProviderFactory>,
        selector: TableSelector,
    ) -> Result<Self> {
        let schemas = Self::list_schemas(&client, &catalog_id).await?;

        let mut schemas_map = HashMap::new();
        for schema in schemas {
            let schema_provider = UnityCatalogSchemaProvider::try_new(
                Arc::clone(&client),
                &schema,
                Arc::clone(&table_creator),
                selector.clone(),
            )
            .await?;
            schemas_map.insert(schema.name, Arc::new(schema_provider));
        }
        Ok(Self {
            schemas: RwLock::new(schemas_map),
            client,
            catalog_id,
            table_creator,
            selector,
            on_change: None,
        })
    }

    #[must_use]
    pub fn with_change_listener(mut self, on_change: CatalogChangeListener) -> Self {
        self.on_change = Some(on_change);
        self
    }

    async fn list_schemas(client: &UnityCatalog, catalog_id: &CatalogId) -> Result<Vec<UCSchema>> {
        let schemas =
            client
                .list_schemas(&catalog_id.0)
                .await?
                .context(super::CatalogDoesntExistSnafu {
                    catalog_id: catalog_id.0.clone(),
                })?;
        Ok(schemas
            .into_iter()
            .filter(|schema| schema.name != "information_schema")
            .collect())
    }

    fn registered_schemas(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<String, Arc<UnityCatalogSchemaProvider>>> {
        match self.schemas.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        }
    }
}

impl CatalogProvider for UnityCatalogProvider {
    /// Retrieves the list of available schema names in this catalog.
    fn schema_names(&self) -> Vec<String> {
        self.registered_schemas().keys().cloned().collect()
    }

    /// Retrieves a specific schema from the catalog by name, provided it exists.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.registered_schemas()
            .get(name)
            .map(|s| Arc::clone(s) as Arc<dyn SchemaProvider>)
    }
}

/// What refreshing one listed schema produced.
enum SchemaRefresh {
    /// A registered schema was refreshed; `true` when it replaced or removed a table.
    Existing(bool),
    /// A schema created in Unity Catalog since the last listing.
    New(Arc<UnityCatalogSchemaProvider>),
}

#[async_trait]
impl RefreshableCatalogProvider for UnityCatalogProvider {
    /// Lists the catalog's schemas again: a new schema is registered, a dropped one is
    /// removed, and every other schema is refreshed. A schema that fails to refresh keeps
    /// its tables and does not stop the others; the failures are returned together.
    async fn refresh(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listed = Self::list_schemas(&self.client, &self.catalog_id).await?;
        let registered = self.registered_schemas().clone();

        let refreshes = listed.iter().cloned().map(|schema| {
            let registered_schema = registered.get(&schema.name).map(Arc::clone);
            let client = Arc::clone(&self.client);
            let table_creator = Arc::clone(&self.table_creator);
            let selector = self.selector.clone();
            async move {
                let refreshed = match registered_schema {
                    Some(provider) => provider.refresh().await.map(SchemaRefresh::Existing),
                    None => UnityCatalogSchemaProvider::try_new(
                        client,
                        &schema,
                        table_creator,
                        selector,
                    )
                    .await
                    .map(|provider| SchemaRefresh::New(Arc::new(provider)))
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
                };
                (schema.name, refreshed)
            }
        });
        let results: Vec<_> = futures::stream::iter(refreshes)
            .buffer_unordered(MAX_CONCURRENT_SCHEMA_REFRESHES)
            .collect()
            .await;

        let mut changed = false;
        let mut new_schemas = Vec::new();
        let mut failures = Vec::new();
        for (name, refreshed) in results {
            match refreshed {
                Ok(SchemaRefresh::Existing(schema_changed)) => changed |= schema_changed,
                Ok(SchemaRefresh::New(provider)) => new_schemas.push((name, provider)),
                Err(e) => failures.push(format!("{name}: {e}")),
            }
        }

        let removed_schemas: Vec<String> = registered
            .iter()
            .filter(|(name, _)| !listed.iter().any(|schema| &schema.name == *name))
            .map(|(name, provider)| {
                changed |= !provider.table_names().is_empty();
                name.clone()
            })
            .collect();

        if !removed_schemas.is_empty() || !new_schemas.is_empty() {
            let mut message = format!("Refreshed catalog {}.", self.catalog_id.0);
            if !removed_schemas.is_empty() {
                let _ = write!(message, " Schemas removed: {}.", removed_schemas.join(", "));
            }
            if !new_schemas.is_empty() {
                let added: Vec<&str> = new_schemas.iter().map(|(name, _)| name.as_str()).collect();
                let _ = write!(message, " Schemas added: {}.", added.join(", "));
            }
            tracing::info!("{message}");

            let mut guard = match self.schemas.write() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            for name in &removed_schemas {
                guard.remove(name);
            }
            guard.extend(new_schemas);
        }

        if changed && let Some(on_change) = &self.on_change {
            on_change().await;
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "{} schema(s) of catalog {} failed to refresh: {}",
                failures.len(),
                self.catalog_id.0,
                failures.join("; ")
            )
            .into())
        }
    }
}

/// What building a provider for a listed table produced.
enum TableBuild {
    Built(Arc<dyn TableProvider>),
    /// The table must not be registered: not queryable, not selected, no storage
    /// location, or read access denied.
    Skipped,
    /// The provider could not be built; a later refresh tries again.
    Failed,
}

/// A registered table and the Unity Catalog listing its provider was built from.
#[derive(Debug)]
struct RegisteredTable {
    listing: UCTable,
    provider: Arc<dyn TableProvider>,
}

/// Whether two listings of a table describe the same table. Properties are compared only
/// for the catalog-managed flag, and columns without `type_json`, because both carry
/// values that change on writes: `delta.lastCommitTimestamp`, and a Delta identity
/// column's high-water mark in its field metadata.
fn describes_same_table(registered: &UCTable, listed: &UCTable) -> bool {
    registered.table_id == listed.table_id
        && registered.storage_location == listed.storage_location
        && registered.columns.len() == listed.columns.len()
        && registered
            .columns
            .iter()
            .zip(&listed.columns)
            .all(|(a, b)| {
                a.name == b.name
                    && a.type_text == b.type_text
                    && a.type_name == b.type_name
                    && a.position == b.position
                    && a.type_precision == b.type_precision
                    && a.type_scale == b.type_scale
                    && a.nullable == b.nullable
            })
        && registered.table_type == listed.table_type
        && registered.data_source_format == listed.data_source_format
        && registered.is_catalog_managed() == listed.is_catalog_managed()
}

pub struct UnityCatalogSchemaProvider {
    tables: RwLock<HashMap<String, RegisteredTable>>,
    client: Arc<UnityCatalog>,
    schema: UCSchema,
    selector: TableSelector,
    table_creator: Arc<dyn UCTableProviderFactory>,
}

impl std::fmt::Debug for UnityCatalogSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnityCatalogSchemaProvider")
            .field("schema", &self.schema)
            .field("tables", &self.tables)
            .finish_non_exhaustive()
    }
}

impl UnityCatalogSchemaProvider {
    /// Creates a new instance of the [`UnityCatalogSchemaProvider`].
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be created.
    pub async fn try_new(
        client: Arc<UnityCatalog>,
        schema: &UCSchema,
        table_creator: Arc<dyn UCTableProviderFactory>,
        selector: TableSelector,
    ) -> Result<Self> {
        let tables = client
            .list_tables(&schema.catalog_name, &schema.name)
            .await?
            .context(super::SchemaDoesntExistSnafu {
                schema: schema.name.clone(),
                catalog_id: schema.catalog_name.clone(),
            })?;

        // First pass: filter to queryable, included tables with valid references.
        let mut candidates: Vec<(UCTable, TableReference)> = Vec::new();
        for table in tables {
            if !table.is_queryable() {
                tracing::debug!(
                    table = %table.full_name(),
                    table_type = %table.table_type,
                    "Skipping unsupported Unity Catalog table type"
                );
                continue;
            }

            if !selector.selects_table(&schema.name, &table.name) {
                tracing::debug!(
                    "Table {}.{} is not selected by the catalog's include/exclude patterns, skipping",
                    schema.name,
                    table.name
                );
                continue;
            }

            let Some(table_reference) = table_creator.table_reference(&table) else {
                continue;
            };

            candidates.push((table, table_reference));
        }

        // Second pass: check permissions concurrently (bounded). Explicitly
        // denied tables are excluded; ambiguous/unreachable cases are kept.
        let max_concurrent_permission_checks = 5;
        let permission_results: Vec<Option<(UCTable, TableReference)>> =
            futures::stream::iter(candidates.into_iter().map(|(table, table_ref)| {
                let client = Arc::clone(&client);
                async move {
                    if !table.requires_read_permission_validation() {
                        tracing::debug!(
                            table = %table.full_name(),
                            table_type = %table.table_type,
                            "Skipping strict Unity Catalog permission precheck for foreign table during catalog discovery"
                        );
                        return Some((table, table_ref));
                    }

                    match client.get_effective_permissions(&table.full_name()).await {
                        Ok(Some(perms)) if !perms.has_read_permission() => {
                            // Explicit denial: skip this table for the current
                            // catalog discovery pass. It can be discovered on a
                            // later refresh or restart if permissions change.
                            tracing::warn!(
                                table = %table.full_name(),
                                "Skipping table during catalog discovery: no read-compatible privilege found in effective-permissions response"
                            );
                            return None;
                        }
                        Ok(None) => {
                            tracing::debug!(
                                table = %table.full_name(),
                                "Permission check returned no table during catalog discovery; proceeding without permission validation"
                            );
                        }
                        Err(e) => {
                            tracing::debug!(
                                table = %table.full_name(),
                                error = %e,
                                "Failed to check permissions during catalog discovery; proceeding without permission validation"
                            );
                        }
                        Ok(Some(_)) => {}
                    }

                    Some((table, table_ref))
                }
            }))
            .buffer_unordered(max_concurrent_permission_checks)
            .collect()
            .await;

        // Third pass: create table providers for permitted tables.
        let mut tables_map = HashMap::new();
        for (table, table_reference) in permission_results.into_iter().flatten() {
            let table_provider = match table_creator
                .table_provider(&table, table_reference.clone())
                .await
            {
                Ok(provider) => provider,
                Err(source) => {
                    tracing::warn!("Couldn't get table provider for {table_reference}: {source}");
                    continue;
                }
            };
            tables_map.insert(
                table.name.clone(),
                RegisteredTable {
                    listing: table,
                    provider: table_provider,
                },
            );
        }

        Ok(Self {
            tables: RwLock::new(tables_map),
            client,
            schema: schema.clone(),
            selector,
            table_creator,
        })
    }

    /// Lists the schema's tables again. A new table is registered, a dropped one is removed,
    /// and a table whose listing changed (for example a new column, or a table dropped and
    /// created again at another location) is rebuilt; one that must no longer be registered
    /// is removed, and one whose rebuild fails keeps serving until a later refresh rebuilds
    /// it. Returns `true` when a registered table was replaced or removed.
    pub async fn refresh(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let listed_tables = self
            .client
            .list_tables(&self.schema.catalog_name, &self.schema.name)
            .await?
            .context(super::SchemaDoesntExistSnafu {
                schema: self.schema.name.clone(),
                catalog_id: self.schema.catalog_name.clone(),
            })?;

        let registered: HashMap<String, UCTable> = {
            let guard = match self.tables.read() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            guard
                .iter()
                .map(|(name, table)| (name.clone(), table.listing.clone()))
                .collect()
        };

        let mut removed_tables: Vec<String> = registered
            .keys()
            .filter(|name| !listed_tables.iter().any(|t| &t.name == *name))
            .cloned()
            .collect();
        let mut added_tables = Vec::new();
        let mut rebuilt_tables = Vec::new();
        let mut built = Vec::new();

        for table in listed_tables {
            let previous = registered.get(&table.name);
            if previous.is_some_and(|listing| describes_same_table(listing, &table)) {
                continue;
            }
            let provider = Self::provider_for_uc_table(
                &self.schema,
                &table,
                Arc::clone(&self.table_creator),
                self.selector.clone(),
                Arc::clone(&self.client),
            )
            .await;
            match (previous, provider) {
                (Some(_), TableBuild::Built(provider)) => {
                    rebuilt_tables.push(table.name.clone());
                    built.push(RegisteredTable {
                        listing: table,
                        provider,
                    });
                }
                (Some(_), TableBuild::Skipped) => removed_tables.push(table.name),
                (None, TableBuild::Built(provider)) => {
                    added_tables.push(table.name.clone());
                    built.push(RegisteredTable {
                        listing: table,
                        provider,
                    });
                }
                // A registered table keeps its provider and listing, so the next refresh
                // tries the rebuild again.
                (_, TableBuild::Failed) | (None, TableBuild::Skipped) => {}
            }
        }

        if removed_tables.is_empty() && built.is_empty() {
            return Ok(false);
        }

        let mut message = format!(
            "Refreshed schema {}.{}.",
            self.schema.catalog_name, self.schema.name
        );
        for (label, names) in [
            ("removed", &removed_tables),
            ("added", &added_tables),
            ("changed", &rebuilt_tables),
        ] {
            if !names.is_empty() {
                let _ = write!(message, " Tables {label}: {}.", names.join(", "));
            }
        }
        tracing::info!("{message}");

        let mut guard = match self.tables.write() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        for table_name in &removed_tables {
            guard.remove(table_name);
        }
        for table in built {
            guard.insert(table.listing.name.clone(), table);
        }
        Ok(!removed_tables.is_empty() || !rebuilt_tables.is_empty())
    }

    async fn provider_for_uc_table(
        schema: &UCSchema,
        table: &UCTable,
        table_creator: Arc<dyn UCTableProviderFactory>,
        selector: TableSelector,
        client: Arc<UnityCatalog>,
    ) -> TableBuild {
        if !table.is_queryable() {
            tracing::debug!(
                table = %table.full_name(),
                table_type = %table.table_type,
                "Skipping unsupported Unity Catalog table type"
            );
            return TableBuild::Skipped;
        }

        let table_name = table.name.clone();

        if !selector.selects_table(&schema.name, &table_name) {
            tracing::debug!(
                "Table {}.{table_name} is not selected by the catalog's include/exclude patterns, skipping",
                schema.name
            );
            return TableBuild::Skipped;
        }

        let Some(table_reference) = table_creator.table_reference(table) else {
            return TableBuild::Skipped;
        };

        if table.requires_read_permission_validation() {
            match client.get_effective_permissions(&table.full_name()).await {
                Ok(Some(perms)) if !perms.has_read_permission() => {
                    // Explicit denial: skip this table so a later refresh or
                    // restart can discover it if permissions change.
                    tracing::warn!(
                        table = %table.full_name(),
                        "Skipping table during catalog refresh: no read-compatible privilege found in effective-permissions response"
                    );
                    return TableBuild::Skipped;
                }
                Ok(None) => {
                    tracing::debug!(
                        table = %table.full_name(),
                        "Permission check returned no table during catalog refresh; proceeding without permission validation"
                    );
                }
                Err(e) => {
                    tracing::debug!(
                        table = %table.full_name(),
                        error = %e,
                        "Failed to check permissions during catalog refresh; proceeding without permission validation"
                    );
                }
                Ok(Some(_)) => {}
            }
        } else {
            tracing::debug!(
                table = %table.full_name(),
                table_type = %table.table_type,
                "Skipping strict Unity Catalog permission precheck for foreign table during catalog refresh"
            );
        }

        match table_creator
            .table_provider(table, table_reference.clone())
            .await
        {
            Ok(provider) => TableBuild::Built(provider),
            Err(source) => {
                tracing::warn!("Couldn't get table provider for {table_reference}: {source}");
                TableBuild::Failed
            }
        }
    }
}

#[async_trait]
impl SchemaProvider for UnityCatalogSchemaProvider {
    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String> {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.keys().cloned().collect()
    }

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `Ok(None)`.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        Ok(guard.get(name).map(|table| Arc::clone(&table.provider)))
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_filter::TableSelector;
    use crate::unity_catalog::Endpoint;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Builds a provider whose schema has the listing's columns and records the
    /// listing's storage location, so a test can tell which listing built it.
    struct ListingTableFactory;

    #[async_trait]
    impl UCTableProviderFactory for ListingTableFactory {
        fn table_reference(&self, table: &UCTable) -> Option<TableReference> {
            table.storage_location.as_deref().map(TableReference::bare)
        }

        async fn table_provider(
            &self,
            table: &UCTable,
            table_reference: TableReference,
        ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
            if table_reference.table().contains("unreachable") {
                return Err("storage unreachable".into());
            }
            let fields: Vec<Field> = table
                .columns
                .iter()
                .map(|column| Field::new(&column.name, DataType::Utf8, true))
                .collect();
            let schema = Schema::new_with_metadata(
                fields,
                HashMap::from([("location".to_string(), table_reference.to_string())]),
            );
            Ok(Arc::new(EmptyTable::new(Arc::new(schema))))
        }
    }

    fn table_json(schema: &str, name: &str, columns: &[&str], location: &str) -> serde_json::Value {
        table_json_with_id(schema, name, columns, location, &format!("{location}-id"))
    }

    fn table_json_with_id(
        schema: &str,
        name: &str,
        columns: &[&str],
        location: &str,
        table_id: &str,
    ) -> serde_json::Value {
        let columns: Vec<serde_json::Value> = columns
            .iter()
            .enumerate()
            .map(|(position, column)| {
                serde_json::json!({
                    "name": column,
                    "type_text": "string",
                    "type_name": "STRING",
                    "position": position,
                    "nullable": true
                })
            })
            .collect();
        serde_json::json!({
            "name": name,
            "catalog_name": "cat",
            "schema_name": schema,
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": columns,
            "storage_location": location,
            "table_id": table_id,
        })
    }

    async fn serve_listing(
        server: &MockServer,
        schemas: &[&str],
        tables: &[(&str, Vec<serde_json::Value>)],
    ) {
        server.reset().await;
        let schemas: Vec<serde_json::Value> = schemas
            .iter()
            .map(|name| serde_json::json!({ "name": name, "catalog_name": "cat" }))
            .collect();
        Mock::given(method("GET"))
            .and(path("/api/2.1/unity-catalog/schemas"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "schemas": schemas })),
            )
            .mount(server)
            .await;
        for (schema, schema_tables) in tables {
            Mock::given(method("GET"))
                .and(path("/api/2.1/unity-catalog/tables"))
                .and(query_param("schema_name", *schema))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({ "tables": schema_tables })),
                )
                .mount(server)
                .await;
        }
    }

    async fn new_provider(server: &MockServer, changes: &Arc<AtomicUsize>) -> UnityCatalogProvider {
        let client = UnityCatalog::new(Endpoint(server.uri()), None, None).expect("client");
        let changes = Arc::clone(changes);
        UnityCatalogProvider::try_new(
            Arc::new(client),
            CatalogId("cat".to_string()),
            Arc::new(ListingTableFactory),
            TableSelector::default(),
        )
        .await
        .expect("catalog loads")
        .with_change_listener(Arc::new(move || {
            changes.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {})
        }))
    }

    async fn registered(
        provider: &UnityCatalogProvider,
        schema: &str,
        table: &str,
    ) -> Option<Arc<dyn TableProvider>> {
        provider
            .schema(schema)?
            .table(table)
            .await
            .expect("table lookup")
    }

    fn columns_and_location(table: &Arc<dyn TableProvider>) -> (Vec<String>, String) {
        let schema = table.schema();
        (
            schema.fields().iter().map(|f| f.name().clone()).collect(),
            schema.metadata()["location"].clone(),
        )
    }

    #[tokio::test]
    async fn a_refresh_rebuilds_a_table_whose_listing_changed() {
        let server = MockServer::start().await;
        let changes = Arc::new(AtomicUsize::new(0));
        let orders = |columns: &[&str], location: &str, id: &str| {
            vec![table_json_with_id("sales", "orders", columns, location, id)]
        };
        serve_listing(
            &server,
            &["sales"],
            &[("sales", orders(&["id"], "s3://b/o1", "t1"))],
        )
        .await;
        let provider = new_provider(&server, &changes).await;

        // A column was added in place.
        serve_listing(
            &server,
            &["sales"],
            &[("sales", orders(&["id", "region"], "s3://b/o1", "t1"))],
        )
        .await;
        provider.refresh().await.expect("refresh");
        let altered = registered(&provider, "sales", "orders")
            .await
            .expect("orders stays registered");
        assert_eq!(
            columns_and_location(&altered),
            (
                vec!["id".to_string(), "region".to_string()],
                "s3://b/o1".to_string()
            )
        );
        assert_eq!(changes.load(Ordering::SeqCst), 1);

        // A column's type changed in place, then a field was added to it: only `type_text`
        // tells the second listing from the first.
        let mut previous = altered;
        for (expected_changes, type_text) in [(2, "struct<a:int>"), (3, "struct<a:int,b:string>")] {
            let mut retyped = orders(&["id", "region"], "s3://b/o1", "t1");
            retyped[0]["columns"][1]["type_name"] = serde_json::json!("STRUCT");
            retyped[0]["columns"][1]["type_text"] = serde_json::json!(type_text);
            serve_listing(&server, &["sales"], &[("sales", retyped)]).await;
            provider.refresh().await.expect("refresh");
            let current = registered(&provider, "sales", "orders")
                .await
                .expect("orders stays registered");
            assert!(!Arc::ptr_eq(&previous, &current), "{type_text}");
            assert_eq!(changes.load(Ordering::SeqCst), expected_changes);
            previous = current;
        }

        // The table was dropped and created again elsewhere with the same columns.
        serve_listing(
            &server,
            &["sales"],
            &[("sales", orders(&["id", "region"], "s3://b/o2", "t2"))],
        )
        .await;
        provider.refresh().await.expect("refresh");
        let recreated = registered(&provider, "sales", "orders")
            .await
            .expect("orders stays registered");
        assert_eq!(columns_and_location(&recreated).1, "s3://b/o2");
        assert_eq!(changes.load(Ordering::SeqCst), 4);

        // A write changes only Delta properties and column metadata: no rebuild.
        let mut written = orders(&["id", "region"], "s3://b/o2", "t2");
        written[0]["properties"] = serde_json::json!({ "delta.lastCommitTimestamp": "1" });
        written[0]["columns"][0]["type_json"] = serde_json::json!(
            r#"{"name":"id","type":"long","nullable":true,"metadata":{"delta.identity.highWaterMark":2}}"#
        );
        serve_listing(&server, &["sales"], &[("sales", written)]).await;
        provider.refresh().await.expect("refresh");
        let after_write = registered(&provider, "sales", "orders")
            .await
            .expect("orders stays registered");
        assert!(Arc::ptr_eq(&recreated, &after_write));
        assert_eq!(changes.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn a_changed_table_that_cannot_be_rebuilt_keeps_serving() {
        let server = MockServer::start().await;
        let changes = Arc::new(AtomicUsize::new(0));
        serve_listing(
            &server,
            &["sales"],
            &[(
                "sales",
                vec![table_json("sales", "orders", &["id"], "s3://b/o1")],
            )],
        )
        .await;
        let provider = new_provider(&server, &changes).await;
        let original = registered(&provider, "sales", "orders")
            .await
            .expect("orders registered");

        serve_listing(
            &server,
            &["sales"],
            &[(
                "sales",
                vec![table_json(
                    "sales",
                    "orders",
                    &["id"],
                    "s3://unreachable/o2",
                )],
            )],
        )
        .await;
        provider.refresh().await.expect("refresh");
        let kept = registered(&provider, "sales", "orders")
            .await
            .expect("orders keeps serving");
        assert!(Arc::ptr_eq(&original, &kept));
        assert_eq!(changes.load(Ordering::SeqCst), 0);

        // The next refresh tries the rebuild again.
        serve_listing(
            &server,
            &["sales"],
            &[(
                "sales",
                vec![table_json("sales", "orders", &["id"], "s3://b/o2")],
            )],
        )
        .await;
        provider.refresh().await.expect("refresh");
        let rebuilt = registered(&provider, "sales", "orders")
            .await
            .expect("orders registered");
        assert_eq!(columns_and_location(&rebuilt).1, "s3://b/o2");
        assert_eq!(changes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_refresh_follows_created_and_dropped_schemas() {
        let server = MockServer::start().await;
        let changes = Arc::new(AtomicUsize::new(0));
        serve_listing(
            &server,
            &["sales", "staging", "hr"],
            &[
                (
                    "sales",
                    vec![table_json("sales", "orders", &["id"], "s3://b/orders")],
                ),
                (
                    "staging",
                    vec![table_json("staging", "raw", &["id"], "s3://b/raw")],
                ),
                (
                    "hr",
                    vec![table_json("hr", "staff", &["id"], "s3://b/staff")],
                ),
            ],
        )
        .await;
        let provider = new_provider(&server, &changes).await;

        // `staging` is dropped, `finance` is created, and listing `hr` fails.
        serve_listing(
            &server,
            &["sales", "finance", "hr"],
            &[
                (
                    "sales",
                    vec![table_json("sales", "orders", &["id"], "s3://b/orders")],
                ),
                (
                    "finance",
                    vec![table_json("finance", "ledger", &["id"], "s3://b/ledger")],
                ),
            ],
        )
        .await;
        let err = provider
            .refresh()
            .await
            .expect_err("the failed schema is reported");
        assert!(err.to_string().contains("hr:"), "{err}");

        let mut schemas = provider.schema_names();
        schemas.sort();
        assert_eq!(schemas, ["finance", "hr", "sales"]);
        assert!(registered(&provider, "finance", "ledger").await.is_some());
        assert!(registered(&provider, "hr", "staff").await.is_some());
        assert_eq!(changes.load(Ordering::SeqCst), 1);
    }
}
