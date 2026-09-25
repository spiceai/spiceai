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

use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::{
    catalog::SchemaProvider,
    datasource::TableProvider,
    error::{DataFusionError, Result},
    execution::context::SessionContext,
    sql::TableReference,
};
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

/// Serializes the existence check and the registration in [`ensure_schema_exists`].
///
/// `CatalogProvider::register_schema` *replaces* the schema already held under a
/// name, and a schema is published empty and filled in by the registrations that
/// follow, so a replacement discards every table the replaced instance already
/// owned. Those tables resolve as `<catalog>.<schema>.<table>` not found at query
/// time while the registration that owned them reported success. Datasets, views and
/// accelerated refreshes register concurrently, so two callers both seeing an absent
/// schema is an ordinary interleaving rather than a rare one.
static SCHEMA_CREATION: Mutex<()> = Mutex::new(());

fn schema_creation_guard() -> MutexGuard<'static, ()> {
    // A panic elsewhere must not make schema registration permanently unavailable.
    SCHEMA_CREATION
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Registers `table_reference`'s schema in `catalog` if it does not already exist.
///
/// A table reference without a schema component needs nothing registered, so
/// that case succeeds without touching the catalog.
///
/// Concurrent callers are serialized while creating a schema, so the caller that
/// creates one keeps owning it and every table registered into it stays reachable.
/// A schema published by a path that does not take this lock — the SQL `CREATE
/// SCHEMA` statement registers into the catalog directly — can still be replaced
/// here, and nothing in this function can merge it back: the trait offers only a
/// replacing registration. That case is reported rather than dropped silently.
///
/// When the catalog delegates to an external catalog whose `register_schema` performs
/// I/O (a metastore-backed catalog creates the schema remotely), that work happens with
/// the lock held, so other creations wait for it. Creation happens once per schema; the
/// common path, where the schema already exists, takes no lock at all.
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
    let catalog_provider = ctx
        .catalog(catalog)
        .context(CatalogMissingSnafu { catalog })?;

    // This TableReference doesn't have a schema component, nothing to do.
    let Some(schema_name) = table_reference.schema() else {
        return Ok(());
    };

    // An existing schema needs nothing, and reading it without the lock is sound:
    // nothing here removes a schema, so one observed here stays reachable, and
    // registering into an instance another caller just published is the intended
    // outcome. Keeping this off the lock matters because every dataset, view and
    // refresh takes this path, and almost all of them find the schema present.
    if catalog_provider.schema(schema_name).is_some() {
        return Ok(());
    }

    // Held across the second check and the registration: releasing it in between lets a
    // second caller see an absent schema and publish a different instance for the same
    // name, orphaning the tables registered into the first.
    let _guard = schema_creation_guard();

    if catalog_provider.schema(schema_name).is_some() {
        return Ok(());
    }

    // Create the schema
    let schema_provider = Arc::new(SpiceSchemaProvider::new());
    match catalog_provider.register_schema(schema_name, schema_provider) {
        Ok(Some(displaced)) => {
            let tables = displaced.table_names();
            if !tables.is_empty() {
                tracing::warn!(
                    catalog,
                    schema = schema_name,
                    tables = ?tables,
                    "Creating {catalog}.{schema_name} replaced a schema that already owned {} table(s) ({}); they are no longer reachable under that name",
                    tables.len(),
                    tables.join(", "),
                );
            }
            Ok(())
        }
        Ok(_) => Ok(()),
        Err(_) => unreachable!("register_schema will never fail"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        catalog::{MemoryCatalogProvider, SchemaProvider},
        datasource::{TableProvider, empty::EmptyTable},
        execution::context::SessionContext,
        sql::TableReference,
    };

    use super::ensure_schema_exists;

    const CATALOG: &str = "spice";
    const REFERENCE_SCHEMA: &str = "__test_reference";

    fn table() -> Arc<dyn TableProvider> {
        Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]))))
    }

    fn schema_of(ctx: &SessionContext) -> Arc<dyn SchemaProvider> {
        ctx.catalog(CATALOG)
            .and_then(|catalog| catalog.schema(REFERENCE_SCHEMA))
            .unwrap_or_else(|| panic!("{CATALOG}.{REFERENCE_SCHEMA} is not registered"))
    }

    #[test]
    fn every_concurrent_registration_into_a_new_schema_stays_registered() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 8;

        let ctx = SessionContext::new();
        ctx.register_catalog(CATALOG, Arc::new(MemoryCatalogProvider::new()));

        let barrier = Arc::new(Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let ctx = ctx.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for i in 0..PER_THREAD {
                        let name = format!("table_{t}_{i}");
                        ensure_schema_exists(
                            &ctx,
                            CATALOG,
                            &TableReference::partial(REFERENCE_SCHEMA, name.clone()),
                        )
                        .expect("schema ensured");
                        schema_of(&ctx)
                            .register_table(name, table())
                            .expect("table registered");
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("registration thread");
        }

        let expected: Vec<String> = (0..THREADS)
            .flat_map(|t| (0..PER_THREAD).map(move |i| format!("table_{t}_{i}")))
            .collect();
        let schema = schema_of(&ctx);
        let missing: Vec<&str> = expected
            .iter()
            .filter(|name| !schema.table_exist(name))
            .map(String::as_str)
            .collect();
        assert!(
            missing.is_empty(),
            "{} of {} tables are unreachable after concurrent registration into one new schema: {missing:?}",
            missing.len(),
            expected.len(),
        );
    }
}
