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

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use datafusion::{
    catalog::SchemaProvider,
    datasource::TableProvider,
    error::{DataFusionError, Result},
};

/// Copy of default `MemorySchemaProvider` that allows `register_table` to atomically overwrite any existing tables
/// `<https://github.com/apache/datafusion/blob/deebda78a34251b2bddf0c5f66edfaa112c4559b/datafusion/core/src/catalog/schema.rs#L84>`
///
/// Additionally supports *hidden* tables (see [`SpiceSchemaProvider::hide_table`]):
/// registered and fully resolvable by name, but omitted from [`SchemaProvider::table_names`]
/// so they don't surface in `SHOW TABLES` / `information_schema` or anything else
/// that enumerates the schema.
#[derive(Debug)]
pub struct SpiceSchemaProvider {
    tables: DashMap<String, Arc<dyn TableProvider>>,
    /// Names excluded from `table_names()`. Held separately from `tables` so a
    /// name can be hidden before the table it refers to is registered — the
    /// window between registering and hiding would otherwise leak the table into
    /// listings.
    hidden: DashSet<String>,
}

impl SpiceSchemaProvider {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            hidden: DashSet::new(),
        }
    }

    #[must_use]
    pub fn table_sync(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).map(|table| Arc::clone(table.value()))
    }

    /// Omit `name` from [`SchemaProvider::table_names`], leaving lookup by name
    /// (`table`, `table_sync`, `table_exist`) unchanged.
    ///
    /// For tables the runtime registers on a user's behalf and addresses only
    /// internally — the per-table datasets the `PostgreSQL` catalog connector
    /// synthesizes for catalog-level CDC acceleration, which users reach through
    /// the catalog's own namespace and never by their registration name. Listing
    /// them would duplicate every accelerated table in `spice.data` under a name
    /// that is not part of the catalog's interface.
    ///
    /// Takes effect whether or not `name` is registered yet, so a caller that
    /// knows the name up front can hide it before the table appears.
    pub fn hide_table(&self, name: String) {
        self.hidden.insert(name);
    }

    /// Whether `name` is hidden from listings.
    #[must_use]
    pub fn is_table_hidden(&self, name: &str) -> bool {
        self.hidden.contains(name)
    }
}

impl Default for SpiceSchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchemaProvider for SpiceSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| table.key().clone())
            .filter(|name| !self.hidden.contains(name))
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.table_sync(name))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // Clear the hidden marking too, so a name freed by one component can be
        // re-registered by another without staying invisible.
        self.hidden.remove(name);
        Ok(self.tables.remove(name).map(|(_, table)| table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::empty::EmptyTable;

    fn empty_table() -> Arc<dyn TableProvider> {
        Arc::new(EmptyTable::new(Arc::new(
            datafusion::arrow::datatypes::Schema::empty(),
        )))
    }

    #[tokio::test]
    async fn hidden_tables_are_resolvable_but_unlisted() {
        let provider = SpiceSchemaProvider::new();
        provider
            .register_table("visible".to_string(), empty_table())
            .expect("register");
        provider
            .register_table("internal".to_string(), empty_table())
            .expect("register");
        provider.hide_table("internal".to_string());

        assert_eq!(provider.table_names(), vec!["visible".to_string()]);
        // Hiding affects listing only -- every lookup path still resolves it, or
        // the component that registered it could no longer reach its own table.
        assert!(provider.table_exist("internal"));
        assert!(provider.table_sync("internal").is_some());
        assert!(
            provider
                .table("internal")
                .await
                .expect("lookup succeeds")
                .is_some()
        );
        assert!(provider.is_table_hidden("internal"));
    }

    #[tokio::test]
    async fn a_name_can_be_hidden_before_its_table_is_registered() {
        // Callers that know the name up front hide it first, so the table is
        // never briefly listed between registration and hiding.
        let provider = SpiceSchemaProvider::new();
        provider.hide_table("internal".to_string());
        provider
            .register_table("internal".to_string(), empty_table())
            .expect("register");

        assert!(provider.table_names().is_empty());
        assert!(provider.table_sync("internal").is_some());
    }

    #[test]
    fn deregistering_clears_the_hidden_marking() {
        let provider = SpiceSchemaProvider::new();
        provider
            .register_table("internal".to_string(), empty_table())
            .expect("register");
        provider.hide_table("internal".to_string());

        provider.deregister_table("internal").expect("deregister");
        provider
            .register_table("internal".to_string(), empty_table())
            .expect("re-register");

        assert_eq!(provider.table_names(), vec!["internal".to_string()]);
    }
}
