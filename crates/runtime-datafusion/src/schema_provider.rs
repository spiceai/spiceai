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
use dashmap::DashMap;
use datafusion::{
    catalog::SchemaProvider,
    datasource::TableProvider,
    error::{DataFusionError, Result},
    execution::context::SessionContext,
    sql::TableReference,
};
use parking_lot::Mutex;
use snafu::{OptionExt, Snafu};

/// Copy of default `MemorySchemaProvider` that allows `register_table` to atomically overwrite any existing tables
/// `<https://github.com/apache/datafusion/blob/deebda78a34251b2bddf0c5f66edfaa112c4559b/datafusion/core/src/catalog/schema.rs#L84>`
#[derive(Debug)]
pub struct SpiceSchemaProvider {
    tables: DashMap<String, Arc<dyn TableProvider>>,
}

impl SpiceSchemaProvider {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
        }
    }

    #[must_use]
    pub fn table_sync(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).map(|table| Arc::clone(table.value()))
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
        Ok(self.tables.remove(name).map(|(_, table)| table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

/// The catalog a schema was to be created in is not registered on the session.
#[derive(Debug, Snafu)]
pub enum EnsureSchemaError {
    #[snafu(display("The catalog {catalog} is not registered."))]
    CatalogMissing { catalog: String },
}

/// Registers `table_reference`'s schema in `catalog` if it does not already exist.
///
/// A table reference without a schema component needs nothing registered, so
/// that case succeeds without touching the catalog.
///
/// # Errors
///
/// Returns [`EnsureSchemaError::CatalogMissing`] if `catalog` is not registered
/// on the session.
pub fn ensure_schema_exists(
    ctx: &SessionContext,
    catalog: &str,
    table_reference: &TableReference,
) -> Result<(), EnsureSchemaError> {
    // Serializes the exists-check and the create below. `register_schema`
    // REPLACES an existing schema provider wholesale, so two datasets racing
    // into the same not-yet-created schema (datasets initialize concurrently)
    // could both see "missing" and both register: the loser's fresh, empty
    // provider would discard every table already registered into the winner's
    // - a dataset that logged successful registration is then "not found" at
    // query time. The critical section is two in-memory map operations; no
    // I/O and no await happens under the lock.
    static SCHEMA_CREATE_LOCK: Mutex<()> = Mutex::new(());

    let catalog_provider = ctx
        .catalog(catalog)
        .context(CatalogMissingSnafu { catalog })?;

    // This TableReference doesn't have a schema component, nothing to do.
    let Some(schema_name) = table_reference.schema() else {
        return Ok(());
    };

    let _guard = SCHEMA_CREATE_LOCK.lock();

    // If the schema exists, nothing to do.
    if catalog_provider.schema(schema_name).is_some() {
        return Ok(());
    }

    // Create the schema
    let schema_provider = Arc::new(SpiceSchemaProvider::new());
    match catalog_provider.register_schema(schema_name, schema_provider) {
        Ok(_) => Ok(()),
        Err(_) => unreachable!("register_schema will never fail"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;
    use datafusion::sql::TableReference;

    use super::ensure_schema_exists;

    /// Regression test: datasets initialize concurrently, and several of them
    /// can share a schema that does not exist yet (the benchmark harness's
    /// `__test_reference.*` datasets, for example). Each task ensures the
    /// schema then registers its table; no table may be lost to another task
    /// re-creating the schema in between.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn ensure_schema_exists_concurrent_creators_lose_no_tables() {
        const TASKS: usize = 8;
        const ROUNDS: usize = 200;

        let ctx = Arc::new(SessionContext::new());
        let catalog = "datafusion"; // the SessionContext default catalog

        for round in 0..ROUNDS {
            let schema_name = format!("racing_{round}");
            let barrier = Arc::new(tokio::sync::Barrier::new(TASKS));

            let handles = (0..TASKS)
                .map(|task| {
                    let ctx = Arc::clone(&ctx);
                    let barrier = Arc::clone(&barrier);
                    let table_ref =
                        TableReference::partial(schema_name.clone(), format!("t{task}"));
                    tokio::spawn(async move {
                        barrier.wait().await;
                        ensure_schema_exists(&ctx, catalog, &table_ref)
                            .expect("schema creation should succeed");

                        let arrow_schema =
                            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
                        let table = MemTable::try_new(arrow_schema, vec![vec![]])
                            .expect("MemTable should build");
                        ctx.register_table(table_ref, Arc::new(table))
                            .expect("table registration should succeed");
                    })
                })
                .collect::<Vec<_>>();

            for handle in handles {
                handle.await.expect("task should not panic");
            }

            let schema = ctx
                .catalog(catalog)
                .expect("default catalog should exist")
                .schema(&schema_name)
                .expect("schema should exist after ensure_schema_exists");
            let mut table_names = schema.table_names();
            table_names.sort();
            let expected = (0..TASKS)
                .map(|task| format!("t{task}"))
                .collect::<Vec<_>>();
            assert_eq!(
                table_names, expected,
                "a concurrently-registered table vanished from schema {schema_name}"
            );
        }
    }
}
