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

//! Cayenne catalog and schema provider implementations for `DataFusion`.
//!
//! Provides a dynamic-namespace catalog backed by a Cayenne [`MetadataCatalog`]
//! (`SQLite`) and local file storage for data files.
//!
//! Tables are organized into namespaces (schemas) specified at DDL time.
//! In the metadata catalog, table names are stored with a namespace prefix
//! (`namespace/table_name`) so that namespace membership survives restarts.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use cayenne::metadata::VortexConfig;
use cayenne::{CayenneCatalog, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result as DFResult;
use datafusion::execution::runtime_env::RuntimeEnv;
use snafu::prelude::*;

use crate::component::catalog::Catalog;
use crate::parameters::Parameters;
use crate::spice_data_base_path;
use data_components::RefreshableCatalogProvider;
use data_components::delete::{DeletionTableProvider, DeletionTableProviderAdapter};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to initialize Cayenne catalog: {source}"))]
    CatalogInit {
        source: cayenne::catalog::CatalogError,
    },

    #[snafu(display("Invalid Cayenne catalog configuration: {message}"))]
    InvalidConfiguration { message: String },

    #[snafu(display("Failed to create Cayenne table provider: {source}"))]
    TableProvider {
        source: cayenne::catalog::CatalogError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Default catalog name used when no catalog ID is specified.
const DEFAULT_CATALOG_NAME: &str = "cayenne";

/// `DataFusion` [`CatalogProvider`] backed by a Cayenne metadata catalog.
///
/// Schemas are created dynamically when tables are created via DDL.
/// There are no automatic "default" or "public" schemas; tables are only
/// accessible through the namespace they were explicitly created in
/// (e.g., `my_catalog.my_namespace.my_table`).
///
/// Data is stored on local disk, metadata in local `SQLite`.
pub struct CayenneCatalogProvider {
    /// The underlying Cayenne metadata catalog (`SQLite`).
    catalog: Arc<dyn MetadataCatalog>,
    /// Vortex configuration for table providers.
    vortex_config: VortexConfig,
    /// Base path for table data on local disk.
    data_base_path: String,
    /// Local metadata directory.
    metadata_dir: String,
    /// Shared `RuntimeEnv` from the main Spice runtime for cache coherence.
    runtime_env: Arc<RuntimeEnv>,
    /// Schema providers keyed by namespace name, created dynamically via DDL.
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl std::fmt::Debug for CayenneCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneCatalogProvider")
            .field("data_base_path", &self.data_base_path)
            .field("metadata_dir", &self.metadata_dir)
            .finish_non_exhaustive()
    }
}

impl CayenneCatalogProvider {
    /// Create a new Cayenne catalog provider.
    ///
    /// Initializes the `SQLite` metadata catalog and local file storage.
    /// The `catalog_id` field is ignored; Cayenne always uses a fixed default name.
    pub async fn try_new(
        params: Parameters,
        _catalog_config: &Catalog,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<Self> {
        let catalog_name = DEFAULT_CATALOG_NAME;

        // Resolve metadata directory
        let metadata_dir = params
            .get("cayenne_metadata_dir")
            .expose()
            .ok()
            .map_or_else(
                || format!("{}/cayenne_{catalog_name}/metadata", spice_data_base_path()),
                String::from,
            );

        // Ensure metadata directory exists
        std::fs::create_dir_all(&metadata_dir).map_err(|e| Error::InvalidConfiguration {
            message: format!("Failed to create metadata directory '{metadata_dir}': {e}"),
        })?;

        // Initialize SQLite catalog
        let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
        let catalog = Arc::new(
            CayenneCatalog::new(connection_string).map_err(|e| Error::CatalogInit { source: e })?,
        ) as Arc<dyn MetadataCatalog>;

        catalog.init().await.context(CatalogInitSnafu)?;

        // Initialize local file storage
        let data_dir = params.get("cayenne_data_dir").expose().ok().map_or_else(
            || format!("{}/cayenne_{catalog_name}/data", spice_data_base_path()),
            String::from,
        );

        std::fs::create_dir_all(&data_dir).map_err(|e| Error::InvalidConfiguration {
            message: format!("Failed to create data directory '{data_dir}': {e}"),
        })?;

        // Ensure trailing slash for consistent path joining
        let data_base_path = if data_dir.ends_with('/') {
            data_dir
        } else {
            format!("{data_dir}/")
        };

        tracing::info!("Cayenne catalog using local file storage at '{data_base_path}'");

        // Parse Vortex config from parameters
        let vortex_config = Self::parse_vortex_config(&params);

        let provider = Self {
            catalog,
            vortex_config,
            data_base_path,
            metadata_dir,
            runtime_env,
            schemas: RwLock::new(HashMap::new()),
        };

        Ok(provider)
    }

    /// Returns a reference to the underlying Cayenne metadata catalog.
    #[must_use]
    pub fn metadata_catalog(&self) -> &Arc<dyn MetadataCatalog> {
        &self.catalog
    }

    /// Returns the base data path on local disk.
    #[must_use]
    pub fn data_base_path(&self) -> &str {
        &self.data_base_path
    }

    /// Returns the Vortex configuration.
    #[must_use]
    pub fn vortex_config(&self) -> &VortexConfig {
        &self.vortex_config
    }

    /// Returns the shared `RuntimeEnv`.
    #[must_use]
    pub fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime_env
    }

    /// Returns the metadata directory path.
    #[must_use]
    pub fn metadata_dir(&self) -> &str {
        &self.metadata_dir
    }

    /// Returns the schema provider for a namespace if it exists.
    #[must_use]
    pub fn schema_provider(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .read()
            .ok()
            .and_then(|schemas| schemas.get(name).cloned())
    }

    /// Registers or replaces a schema provider for a namespace.
    pub fn register_schema_provider(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        match self.schemas.write() {
            Ok(mut schemas) => Ok(schemas.insert(name.to_string(), schema)),
            Err(_) => Err(datafusion::error::DataFusionError::Internal(
                "Failed to acquire write lock on Cayenne schemas".to_string(),
            )),
        }
    }

    /// Parse Vortex configuration from catalog parameters.
    fn parse_vortex_config(params: &Parameters) -> VortexConfig {
        let mut config = VortexConfig::default();

        if let Some(v) = params.get("cayenne_footer_cache_mb").expose().ok()
            && let Ok(val) = v.parse::<usize>()
        {
            config.footer_cache_mb = val;
        }
        if let Some(v) = params.get("cayenne_segment_cache_mb").expose().ok()
            && let Ok(val) = v.parse::<usize>()
        {
            config.segment_cache_mb = val;
        }
        if let Some(v) = params.get("cayenne_target_file_size_mb").expose().ok()
            && let Ok(val) = v.parse::<usize>()
        {
            config.target_vortex_file_size_mb = val;
        }
        if let Some(v) = params.get("cayenne_compression_strategy").expose().ok() {
            match v.to_lowercase().as_str() {
                "zstd" => {
                    config.compression_strategy = cayenne::metadata::CompressionStrategy::Zstd;
                }
                "btrblocks" => {
                    config.compression_strategy = cayenne::metadata::CompressionStrategy::Btrblocks;
                }
                _ => {}
            }
        }

        config
    }
}

impl CatalogProvider for CayenneCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas
            .read()
            .map(|schemas| schemas.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schema_provider(name)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        self.register_schema_provider(name, schema)
    }
}

#[async_trait]
impl RefreshableCatalogProvider for CayenneCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let table_names = self.catalog.list_table_names().await.unwrap_or_else(|e| {
            tracing::warn!("Failed to list existing Cayenne tables: {e}");
            Vec::new()
        });

        // Group tables by namespace (tables stored as "namespace/table_name" in metadata).
        let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
        for full_name in &table_names {
            if let Some((ns, _table)) = full_name.split_once('/') {
                grouped
                    .entry(ns.to_string())
                    .or_default()
                    .push(full_name.clone());
            } else {
                tracing::debug!("Cayenne table '{full_name}' has no namespace prefix, skipping");
            }
        }

        let mut new_schemas: HashMap<String, Arc<dyn SchemaProvider>> = HashMap::new();
        for (ns, full_names) in &grouped {
            let schema_provider = CayenneSchemaProvider::try_new(
                Arc::clone(&self.catalog),
                ns,
                full_names,
                Arc::clone(&self.runtime_env),
            )
            .await?;
            new_schemas.insert(ns.clone(), Arc::new(schema_provider));
        }

        if !new_schemas.is_empty() {
            let total_tables: usize = grouped.values().map(Vec::len).sum();
            tracing::debug!(
                "Loaded {total_tables} existing Cayenne table{} across {} namespace{}",
                if total_tables == 1 { "" } else { "s" },
                new_schemas.len(),
                if new_schemas.len() == 1 { "" } else { "s" },
            );
        }

        match self.schemas.write() {
            Ok(mut schemas) => *schemas = new_schemas,
            Err(poisoned) => *poisoned.into_inner() = new_schemas,
        }

        Ok(())
    }
}

/// Schema provider for a single namespace within a Cayenne catalog.
///
/// Tables in the Cayenne metadata catalog are stored with namespace-prefixed
/// names (`namespace/table_name`). This schema provider manages tables for
/// one namespace, exposing them under their short (unqualified) names.
pub struct CayenneSchemaProvider {
    /// The underlying Cayenne metadata catalog.
    catalog: Arc<dyn MetadataCatalog>,
    /// The namespace this schema provider represents.
    namespace: String,
    /// Shared `RuntimeEnv` for cache coherence with the main runtime.
    runtime_env: Arc<RuntimeEnv>,
    /// Table providers keyed by short (unqualified) table name.
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl std::fmt::Debug for CayenneSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneSchemaProvider").finish()
    }
}

impl CayenneSchemaProvider {
    /// Create a new schema provider for a namespace, loading specified tables.
    ///
    /// `full_table_names` are namespace-prefixed names (`namespace/table_name`) as
    /// stored in the Cayenne metadata catalog.
    pub async fn try_new(
        catalog: Arc<dyn MetadataCatalog>,
        namespace: &str,
        full_table_names: &[String],
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<Self> {
        let ns_prefix = format!("{namespace}/");
        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        for full_name in full_table_names {
            let short_name = full_name.strip_prefix(&ns_prefix).unwrap_or(full_name);
            match Self::load_table(&catalog, full_name, &runtime_env).await {
                Ok(Some(provider)) => {
                    tables.insert(short_name.to_string(), provider);
                }
                Ok(None) => {
                    tracing::debug!(
                        "Table '{full_name}' listed in catalog but could not be loaded"
                    );
                }
                Err(e) => {
                    tracing::warn!("Failed to load Cayenne table '{full_name}': {e}");
                }
            }
        }

        Ok(Self {
            catalog,
            namespace: namespace.to_string(),
            runtime_env,
            tables: RwLock::new(tables),
        })
    }

    /// Create an empty schema provider for a namespace (used by DDL).
    #[must_use]
    pub fn new_empty(
        catalog: Arc<dyn MetadataCatalog>,
        namespace: String,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Self {
        Self {
            catalog,
            namespace,
            runtime_env,
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Returns a reference to the underlying Cayenne metadata catalog.
    #[must_use]
    pub fn metadata_catalog(&self) -> &Arc<dyn MetadataCatalog> {
        &self.catalog
    }

    /// Returns the namespace this schema provider represents.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Construct the full metadata table name for a short table name.
    fn full_table_name(&self, short_name: &str) -> String {
        format!("{}/{short_name}", self.namespace)
    }

    /// Create a [`CayenneTableProvider`] for a table by its full metadata name.
    async fn load_table(
        catalog: &Arc<dyn MetadataCatalog>,
        table_name: &str,
        runtime_env: &Arc<RuntimeEnv>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        // Check if the table exists in the catalog
        match catalog.get_table(table_name).await {
            Ok(_metadata) => {
                let builder =
                    CayenneTableProviderBuilder::new(Arc::clone(catalog), Arc::clone(runtime_env));

                match builder.open(table_name).await {
                    Ok(provider) => {
                        let provider = Arc::new(provider);
                        let deletion_provider: Arc<dyn DeletionTableProvider> = provider;
                        Ok(Some(Arc::new(DeletionTableProviderAdapter::new(
                            deletion_provider,
                        ))))
                    }
                    Err(e) => {
                        tracing::warn!("Failed to open Cayenne table '{table_name}': {e}");
                        Ok(None)
                    }
                }
            }
            Err(cayenne::catalog::CatalogError::TableNotFound { .. }) => Ok(None),
            Err(e) => Err(Error::TableProvider { source: e }),
        }
    }
}

#[async_trait]
impl SchemaProvider for CayenneSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .read()
            .map(|tables| tables.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables
            .read()
            .map(|tables| tables.contains_key(name))
            .unwrap_or(false)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Check in-memory cache first
        if let Ok(tables) = self.tables.read()
            && let Some(provider) = tables.get(name)
        {
            return Ok(Some(Arc::clone(provider)));
        }

        // Try to load from catalog (lazy loading) using namespace-prefixed name
        let full_name = self.full_table_name(name);
        match Self::load_table(&self.catalog, &full_name, &self.runtime_env).await {
            Ok(Some(provider)) => {
                if let Ok(mut tables) = self.tables.write() {
                    tables.insert(name.to_string(), Arc::clone(&provider));
                }
                Ok(Some(provider))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match self.tables.write() {
            Ok(mut tables) => Ok(tables.insert(name, table)),
            Err(_) => Err(datafusion::error::DataFusionError::Internal(
                "Failed to acquire write lock on Cayenne tables".to_string(),
            )),
        }
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match self.tables.write() {
            Ok(mut tables) => Ok(tables.remove(name)),
            Err(_) => Err(datafusion::error::DataFusionError::Internal(
                "Failed to acquire write lock on Cayenne tables".to_string(),
            )),
        }
    }
}
