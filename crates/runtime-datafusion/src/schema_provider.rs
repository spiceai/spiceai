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
use dashmap::{DashMap, DashSet, mapref::entry::Entry};
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
///
/// Overwriting is deliberate — a dataset whose definition changes is re-registered
/// in place — so the schema cannot tell an internal table from a dataset by
/// looking at the provider. [`SpiceSchemaProvider::reserve_table`] records the
/// names the runtime holds for itself, and every registration path checks that
/// record, so a reserved name is refused rather than silently taken.
#[derive(Debug)]
pub struct SpiceSchemaProvider {
    tables: DashMap<String, Arc<dyn TableProvider>>,
    reserved: DashSet<String>,
}

impl SpiceSchemaProvider {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            reserved: DashSet::new(),
        }
    }

    #[must_use]
    pub fn table_sync(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).map(|table| Arc::clone(table.value()))
    }

    /// Register `table` under `name` and reserve the name, but only if it is free.
    ///
    /// Returns the provider that already holds `name`, untouched, when it is not.
    /// The test and the claim are a single step: everything registering into this
    /// schema competes for the same entry, so a name found free by an earlier call
    /// can be taken by the time that call acts on it.
    ///
    /// A reserved name is refused by [`SchemaProvider::register_table`] and
    /// [`SchemaProvider::deregister_table`] from then on, which is what keeps a
    /// runtime component's table — resolved by name whenever it is written to —
    /// from being replaced or removed by something else. A reservation lasts as
    /// long as the schema; nothing releases it through the general API, because a
    /// caller reaching that API is by definition not the table's owner.
    ///
    /// # Errors
    ///
    /// Returns the provider registered under `name`, still registered, when the
    /// name was already taken. Nothing is reserved and nothing is displaced.
    pub fn reserve_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> std::result::Result<(), Arc<dyn TableProvider>> {
        match self.tables.entry(name) {
            Entry::Occupied(occupied) => Err(Arc::clone(occupied.get())),
            Entry::Vacant(vacant) => {
                // Recorded while the entry is held, so a registration waiting on
                // this same entry cannot pass the reservation check and then find
                // the name reserved.
                self.reserved.insert(vacant.key().clone());
                vacant.insert(table);
                Ok(())
            }
        }
    }

    /// Whether `name` is held for a runtime component.
    #[must_use]
    pub fn is_reserved(&self, name: &str) -> bool {
        self.reserved.contains(name)
    }

    /// Give up a reservation this runtime made, removing its table with it.
    ///
    /// A name that is not reserved belongs to something else and is left alone.
    /// The [`SchemaProvider`] API cannot do this, deliberately: only the component
    /// that claimed a name may hand it back, so a reservation abandoned partway
    /// through bringing several tables up does not outlive the attempt.
    pub fn release_reserved_table(&self, name: &str) {
        match self.tables.entry(name.to_string()) {
            Entry::Occupied(occupied) => {
                if !self.reserved.contains(occupied.key()) {
                    return;
                }
                // Both under the entry, so a reservation being made on this name
                // cannot interleave and be erased by the removal.
                self.reserved.remove(occupied.key());
                occupied.remove();
            }
            Entry::Vacant(_) => {
                self.reserved.remove(name);
            }
        }
    }
}

fn reserved_name_error(name: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "Failed to register table {name}: that name is reserved for an internal Spice runtime table. Rename the dataset or view using it. See: https://spiceai.org/docs/reference/spicepod/datasets"
    ))
}

fn reserved_name_removal_error(name: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "Failed to remove table {name}: that name is reserved for an internal Spice runtime table, which is not removed along with a dataset or view. See: https://spiceai.org/docs/reference/spicepod/datasets"
    ))
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
        // Through the entry, not `insert`, so this shares the one point at which
        // the name is decided with `reserve_table` — a check made outside it
        // could be stale by the time the insert happened.
        match self.tables.entry(name) {
            Entry::Occupied(mut occupied) => {
                if self.reserved.contains(occupied.key()) {
                    return Err(reserved_name_error(occupied.key()));
                }
                let displaced = occupied.insert(table);
                Ok(Some(displaced))
            }
            Entry::Vacant(vacant) => {
                if self.reserved.contains(vacant.key()) {
                    return Err(reserved_name_error(vacant.key()));
                }
                vacant.insert(table);
                Ok(None)
            }
        }
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // Through the entry for the same reason registration is, and refusing
        // rather than removing: a dataset that lost the race for a reserved name
        // was never registered under it, so removing that dataset must not take
        // the runtime's table with it. The marker is never cleared here either —
        // doing so would race a reservation being made on the same name.
        match self.tables.entry(name.to_string()) {
            Entry::Occupied(occupied) => {
                if self.reserved.contains(occupied.key()) {
                    return Err(reserved_name_removal_error(occupied.key()));
                }
                Ok(Some(occupied.remove()))
            }
            Entry::Vacant(_) => Ok(None),
        }
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
    let catalog_provider = ctx
        .catalog(catalog)
        .context(CatalogMissingSnafu { catalog })?;

    // This TableReference doesn't have a schema component, nothing to do.
    let Some(schema_name) = table_reference.schema() else {
        return Ok(());
    };

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
mod reservation_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;

    fn table(field: &str) -> Arc<dyn TableProvider> {
        Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![Field::new(
            field,
            DataType::Int64,
            true,
        )]))))
    }

    #[test]
    fn a_reserved_name_is_refused_to_every_other_registration() {
        let provider = SpiceSchemaProvider::new();
        let internal = table("internal");

        provider
            .reserve_table("task_history".to_string(), Arc::clone(&internal))
            .expect("a free name is reserved");
        assert!(provider.is_reserved("task_history"));

        let error = provider
            .register_table("task_history".to_string(), table("dataset"))
            .expect_err("a reserved name is not something else's to take");
        let rendered = error.to_string();
        assert!(
            rendered.contains("reserved") && rendered.contains("Rename"),
            "the refusal has to say what to do about it: {rendered}"
        );

        let held = provider
            .table_sync("task_history")
            .expect("the name is still registered");
        assert!(
            Arc::ptr_eq(&held, &internal),
            "and the runtime keeps the table it writes to"
        );
    }

    #[test]
    fn a_taken_name_is_reported_without_being_disturbed() {
        let provider = SpiceSchemaProvider::new();
        let dataset = table("dataset");
        provider
            .register_table("task_history".to_string(), Arc::clone(&dataset))
            .expect("a dataset registers first");

        let incumbent = provider
            .reserve_table("task_history".to_string(), table("internal"))
            .expect_err("the name is taken");
        assert!(
            Arc::ptr_eq(&incumbent, &dataset),
            "the caller is told which provider holds it"
        );
        assert!(
            !provider.is_reserved("task_history"),
            "a losing reservation reserves nothing"
        );

        let held = provider
            .table_sync("task_history")
            .expect("the name is still registered");
        assert!(
            Arc::ptr_eq(&held, &dataset),
            "and nothing was displaced to find that out"
        );
    }

    /// A dataset that lost the race for a reserved name was never registered
    /// under it, so removing that dataset — an app diff dropping it, a failed load
    /// being cleaned up — must not take the runtime's table with it.
    #[test]
    fn a_reserved_table_is_not_removed_through_the_schema_api() {
        let provider = SpiceSchemaProvider::new();
        let internal = table("internal");
        provider
            .reserve_table("task_history".to_string(), Arc::clone(&internal))
            .expect("reserve");

        let error = provider
            .deregister_table("task_history")
            .expect_err("a reserved table is not a dataset's to remove");
        let rendered = error.to_string();
        assert!(
            rendered.contains("reserved") && rendered.contains("not removed"),
            "the refusal has to say why: {rendered}"
        );

        let held = provider
            .table_sync("task_history")
            .expect("the runtime still has the table it writes to");
        assert!(Arc::ptr_eq(&held, &internal));
        assert!(
            provider.is_reserved("task_history"),
            "and the reservation is not cleared by a caller that does not own it"
        );
    }

    /// Bringing several internal tables up is not atomic as a whole, so a
    /// component that claims one name and then fails on the next has to be able
    /// to hand the first back — otherwise a failed initialization leaves a name
    /// reserved that nothing owns.
    #[test]
    fn a_reservation_can_be_handed_back_by_the_component_that_made_it() {
        let provider = SpiceSchemaProvider::new();
        provider
            .reserve_table("local_task_history".to_string(), table("internal"))
            .expect("reserve");

        provider.release_reserved_table("local_task_history");
        assert!(!provider.is_reserved("local_task_history"));
        assert!(
            provider.table_sync("local_task_history").is_none(),
            "the released name holds nothing"
        );

        provider
            .register_table("local_task_history".to_string(), table("dataset"))
            .expect("and the name is free for whatever wants it next");
    }

    #[test]
    fn releasing_a_name_the_runtime_never_reserved_leaves_it_alone() {
        let provider = SpiceSchemaProvider::new();
        let dataset = table("dataset");
        provider
            .register_table("orders".to_string(), Arc::clone(&dataset))
            .expect("register a dataset");

        provider.release_reserved_table("orders");
        let held = provider
            .table_sync("orders")
            .expect("an unreserved name is not this API's to take");
        assert!(
            Arc::ptr_eq(&held, &dataset),
            "so the dataset is still registered, untouched"
        );
    }

    #[test]
    fn an_unreserved_table_is_removed_as_before() {
        let provider = SpiceSchemaProvider::new();
        provider
            .register_table("orders".to_string(), table("dataset"))
            .expect("register a dataset");

        provider
            .deregister_table("orders")
            .expect("deregister")
            .expect("the dataset comes back out");
        assert!(provider.table_sync("orders").is_none());
        assert!(
            provider
                .deregister_table("orders")
                .expect("deregister")
                .is_none(),
            "and removing what is not there is not an error"
        );
    }

    /// Component loading registers datasets concurrently with the runtime's own
    /// tables, so the reservation is only worth anything if it holds when both
    /// reach the same name at once.
    ///
    /// The assertion is on the invariant rather than on who wins: whoever takes
    /// the name keeps it, and a name that ends up reserved is held by the table
    /// that reserved it — never by a registration that arrived afterwards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_race_for_one_name_leaves_exactly_one_holder() {
        for round in 0..200 {
            let provider = Arc::new(SpiceSchemaProvider::new());
            let internal = table("internal");

            let claiming = {
                let provider = Arc::clone(&provider);
                let internal = Arc::clone(&internal);
                tokio::spawn(async move {
                    provider
                        .reserve_table("task_history".to_string(), internal)
                        .is_ok()
                })
            };
            let registrars: Vec<_> = (0..4)
                .map(|which| {
                    let provider = Arc::clone(&provider);
                    tokio::spawn(async move {
                        provider
                            .register_table(
                                "task_history".to_string(),
                                table(&format!("dataset{which}")),
                            )
                            .is_ok()
                    })
                })
                .collect();

            let reserved = claiming.await.expect("the reserving task ran");
            let mut any_registered = false;
            for registrar in registrars {
                any_registered |= registrar.await.expect("a registering task ran");
            }

            assert_eq!(
                reserved, !any_registered,
                "round {round}: the name is either reserved or registered, never both"
            );
            assert_eq!(
                reserved,
                provider.is_reserved("task_history"),
                "round {round}: the reservation is recorded exactly when it succeeded"
            );

            let held = provider
                .table_sync("task_history")
                .expect("something holds the name");
            if reserved {
                assert!(
                    Arc::ptr_eq(&held, &internal),
                    "round {round}: a reserved name is held by the table that reserved it"
                );
            }
        }
    }
}
