/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Catalog-level CDC acceleration for the `PostgreSQL` catalog connector.
//!
//! [`AcceleratedCatalogProvider`] discovers schemas/tables the same way
//! [`data_components::postgres::provider::PostgresCatalogProvider`] does, but
//! instead of exposing plain federated tables it synthesizes a normal,
//! per-table `Dataset` (as if the user had hand-written a spicepod
//! `datasets:` entry) for every discovered table, and drives it through the
//! exact same dataset lifecycle as any spicepod-declared dataset (connector
//! creation, `AcceleratedTable` construction, refresh loop, status/metrics —
//! see [`Runtime::load_synthesized_dataset`]).
//!
//! Every synthesized dataset is given the same explicit replication slot
//! name (derived once from the catalog's own name), so every table shares
//! one replication connection and one publication instead of each opening
//! its own — WAL is decoded once for the whole catalog, not once per table.
//!
//! Every included table must have a primary key: catalog setup fails,
//! naming the table, if one is missing. Use `include`/`exclude` to keep
//! tables without a primary key out of an accelerated catalog's scope.
//!
//! Before touching any table, `refresh()` validates the `PostgreSQL`
//! prerequisites CDC needs (`wal_level = logical`, replication privilege)
//! and fails fast with a specific, actionable error if either is missing —
//! a clear pass/fail, not a full per-table CDC-readiness report.
//!
//! There is deliberately no federated stand-in for an accelerated table
//! while its dataset is still bootstrapping — `AcceleratedSchemaProvider`
//! reports it as not-yet-present rather than serving reads through the
//! source.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use app::App;
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::postgres::provider::{
    check_cdc_prerequisites, list_schemas, list_tables, primary_key_columns,
};
use data_components::postgres_replication::config::default_slot_name;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use globset::GlobSet;
use snafu::prelude::*;
use spicepod::acceleration::{
    Acceleration as SpicepodAcceleration, RefreshMode as SpicepodRefreshMode,
};
use spicepod::component::dataset::{Dataset as SpicepodDataset, SchemaInference};
use spicepod::param::Params;

use crate::Runtime;
use crate::component::catalog::Catalog;
use crate::component::dataset::builder::DatasetBuilder;

/// Dataset param key carrying an explicit replication slot name (see
/// `connector-postgres`'s `replication_slot` parameter spec, exposed to
/// datasets as `pg_replication_slot`). Every synthesized per-table dataset
/// is given the *same* slot name so they share one replication connection
/// and one publication instead of each opening its own -- this is the
/// catalog's single shared slot.
const REPLICATION_SLOT_PARAM: &str = "pg_replication_slot";

/// Accelerator engine name written onto every synthesized per-table dataset.
/// Matches `CatalogAccelerationEngine`'s only variant.
const CAYENNE_ENGINE: &str = "cayenne";

fn table_is_selected(
    schema_name: &str,
    table_name: &str,
    include: Option<&GlobSet>,
    exclude: Option<&GlobSet>,
) -> bool {
    let schema_with_table = format!("{schema_name}.{table_name}");
    let included = include.is_none_or(|globset| globset.is_match(&schema_with_table));
    let excluded = exclude.is_some_and(|globset| globset.is_match(&schema_with_table));
    included && !excluded
}

/// A sanitized, collision-safe internal name for the per-table dataset
/// synthesized for `catalog_name.schema_name.table_name`. Never exposed to
/// users directly — they query the table through the catalog's own
/// namespace; this is only the registration key under the default catalog.
fn synthesized_dataset_name(catalog_name: &str, schema_name: &str, table_name: &str) -> String {
    format!("__catalog_accel_{catalog_name}_{schema_name}_{table_name}")
}

/// A catalog provider that CDC-accelerates every table it discovers (subject
/// to `include`/`exclude`), holding its own `PostgreSQL` connection directly
/// rather than wrapping the plain federated catalog provider.
pub struct AcceleratedCatalogProvider {
    catalog_name: String,
    pool: Arc<PostgresConnectionPool>,
    runtime: Arc<Runtime>,
    app: Arc<App>,
    /// Connection params shared with every synthesized per-table dataset —
    /// the same `pg_host`/`pg_port`/... the catalog itself was configured
    /// with.
    dataset_params: HashMap<String, String>,
    /// One replication slot name shared by every synthesized dataset in
    /// this catalog, so WAL is decoded once by one shared connection rather
    /// than once per table.
    slot_name: String,
    include: Option<Arc<GlobSet>>,
    exclude: Option<Arc<GlobSet>>,
    schemas: RwLock<HashMap<String, Arc<AcceleratedSchemaProvider>>>,
}

impl std::fmt::Debug for AcceleratedCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceleratedCatalogProvider")
            .field("catalog_name", &self.catalog_name)
            .finish_non_exhaustive()
    }
}

impl AcceleratedCatalogProvider {
    #[must_use]
    pub fn new(catalog: &Catalog, pool: Arc<PostgresConnectionPool>) -> Self {
        let slot_name = default_slot_name(&catalog.name);

        Self {
            catalog_name: catalog.name.clone(),
            pool,
            runtime: catalog.runtime(),
            app: catalog.app(),
            dataset_params: catalog.params.clone(),
            slot_name,
            include: catalog.include.clone().map(Arc::new),
            exclude: catalog.exclude.clone().map(Arc::new),
            schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Synthesizes and kicks off (fire-and-forget, same retry-forever
    /// semantics as any spicepod-declared dataset) the per-table CDC dataset
    /// for `schema_name.table_name`. Returns the name it was registered
    /// under so the schema provider can look it up later.
    #[expect(clippy::result_large_err)]
    fn spawn_accelerated_dataset(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<String, crate::Error> {
        let dataset_name = synthesized_dataset_name(&self.catalog_name, schema_name, table_name);

        let mut params = self.dataset_params.clone();
        params.insert(REPLICATION_SLOT_PARAM.to_string(), self.slot_name.clone());

        let mut spicepod_ds = SpicepodDataset::new(
            format!("postgres:{schema_name}.{table_name}"),
            dataset_name.clone(),
        )
        .with_params(Params::from_string_map(params));
        spicepod_ds.schema_inference = SchemaInference::Extended;
        spicepod_ds.acceleration = Some(SpicepodAcceleration {
            engine: Some(CAYENNE_ENGINE.to_string()),
            refresh_mode: Some(SpicepodRefreshMode::Changes),
            ..SpicepodAcceleration::default()
        });

        let dataset = DatasetBuilder::try_from(spicepod_ds)?
            .with_app(Arc::clone(&self.app))
            .with_runtime(Arc::clone(&self.runtime))
            .build()
            .context(crate::UnableToBuildDatasetSnafu {
                dataset: dataset_name.clone(),
            })?;

        let runtime = Arc::clone(&self.runtime);
        tokio::spawn(runtime.load_synthesized_dataset(Arc::new(dataset)));

        Ok(dataset_name)
    }

    /// Returns the schema provider along with the number of discovered
    /// tables that `include`/`exclude` excluded from it, for the catalog's
    /// startup summary.
    async fn build_schema_provider(
        &self,
        schema_name: &str,
    ) -> Result<(AcceleratedSchemaProvider, usize), Box<dyn std::error::Error + Send + Sync>> {
        let table_names = list_tables(&self.pool, schema_name)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let mut tables = HashMap::new();
        let mut excluded = 0usize;
        for table_name in table_names {
            if !table_is_selected(
                schema_name,
                &table_name,
                self.include.as_deref(),
                self.exclude.as_deref(),
            ) {
                excluded += 1;
                continue;
            }

            let table_path = format!("{schema_name}.{table_name}");
            let primary_key = primary_key_columns(&self.pool, schema_name, &table_name)
                .await
                .unwrap_or_default();

            if primary_key.is_empty() {
                return Err(format!(
                    "Catalog '{}': table {table_path} has no primary key. Every table \
                    included in an accelerated catalog must have a primary key -- add one, \
                    or exclude the table via the catalog's `include`/`exclude` patterns.",
                    self.catalog_name
                )
                .into());
            }

            let dataset_name = self
                .spawn_accelerated_dataset(schema_name, &table_name)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            tables.insert(table_name, dataset_name);
        }

        Ok((
            AcceleratedSchemaProvider {
                runtime: Arc::clone(&self.runtime),
                tables: RwLock::new(tables),
            },
            excluded,
        ))
    }
}

#[async_trait]
impl RefreshableCatalogProvider for AcceleratedCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Fail fast with a clear, actionable error before touching any
        // tables, rather than only surfacing a wal_level/permission problem
        // later when the first table's CDC pump tries (and fails) to open
        // a replication connection.
        check_cdc_prerequisites(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let schema_names = list_schemas(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let mut schemas = HashMap::new();
        let mut included_tables = 0usize;
        let mut excluded_tables = 0usize;
        for schema_name in &schema_names {
            let (schema_provider, excluded) = self.build_schema_provider(schema_name).await?;
            included_tables += schema_provider.table_names().len();
            excluded_tables += excluded;
            schemas.insert(schema_name.clone(), Arc::new(schema_provider));
        }

        {
            let mut guard = match self.schemas.write() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            *guard = schemas;
        }

        tracing::info!(
            "Catalog '{}': accelerating {included_tables} table{} via CDC (shared replication slot '{}'); {excluded_tables} table{} excluded by include/exclude filters.",
            self.catalog_name,
            if included_tables == 1 { "" } else { "s" },
            self.slot_name,
            if excluded_tables == 1 { "" } else { "s" },
        );

        Ok(())
    }
}

impl CatalogProvider for AcceleratedCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        let guard = match self.schemas.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let guard = match self.schemas.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard
            .get(name)
            .map(|s| Arc::clone(s) as Arc<dyn SchemaProvider>)
    }
}

/// A schema provider whose tables are all CDC-accelerated via a synthesized
/// dataset (`table_name` -> the dataset's registration name).
struct AcceleratedSchemaProvider {
    runtime: Arc<Runtime>,
    tables: RwLock<HashMap<String, String>>,
}

impl std::fmt::Debug for AcceleratedSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceleratedSchemaProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for AcceleratedSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let dataset_name = {
            let guard = match self.tables.read() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            match guard.get(name) {
                Some(dataset_name) => dataset_name.clone(),
                None => return Ok(None),
            }
        };

        // Not yet registered (dataset still bootstrapping) simply reads as
        // "table not found" -- no federated stand-in during bootstrap.
        match self
            .runtime
            .df
            .get_accelerated_table_provider(&dataset_name)
            .await
        {
            Ok(provider) => Ok(Some(provider)),
            Err(_) => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.contains_key(name)
    }
}
