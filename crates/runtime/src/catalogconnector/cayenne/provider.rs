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

//! Cayenne catalog and schema provider implementations for DataFusion.
//!
//! Provides a flat-namespace catalog backed by a Cayenne [`MetadataCatalog`]
//! (SQLite) and local file storage for data files.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use cayenne::metadata::VortexConfig;
use cayenne::{CayenneCatalog, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result as DFResult;
use snafu::prelude::*;

use crate::component::catalog::Catalog;
use crate::parameters::Parameters;
use crate::spice_data_base_path;
use data_components::RefreshableCatalogProvider;

use super::CAYENNE_DEFAULT_SCHEMA;

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

/// DataFusion [`CatalogProvider`] backed by a Cayenne metadata catalog.
///
/// All tables are in a single "default" schema. Data is stored on local disk,
/// metadata in local SQLite.
pub struct CayenneCatalogProvider {
    /// The underlying Cayenne metadata catalog (SQLite).
    catalog: Arc<dyn MetadataCatalog>,
    /// Vortex configuration for table providers.
    vortex_config: VortexConfig,
    /// Base path for table data on local disk.
    data_base_path: String,
    /// Local metadata directory.
    metadata_dir: String,
    /// Schema providers (just "default" for Cayenne).
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl std::fmt::Debug for CayenneCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneCatalogProvider")
            .field("data_base_path", &self.data_base_path)
            .field("metadata_dir", &self.metadata_dir)
            .finish()
    }
}

impl CayenneCatalogProvider {
    /// Create a new Cayenne catalog provider.
    ///
    /// Initializes the SQLite metadata catalog and local file storage.
    /// The `catalog_id` field is ignored; Cayenne always uses a fixed default name.
    pub async fn try_new(params: Parameters, _catalog_config: &Catalog) -> Result<Self> {
        let catalog_name = DEFAULT_CATALOG_NAME;

        // Resolve metadata directory
        let metadata_dir = params
            .get("cayenne_metadata_dir")
            .expose()
            .ok()
            .map(String::from)
            .unwrap_or_else(|| format!("{}/cayenne_{catalog_name}/metadata", spice_data_base_path()));

        // Ensure metadata directory exists
        std::fs::create_dir_all(&metadata_dir).map_err(|e| Error::InvalidConfiguration {
            message: format!("Failed to create metadata directory '{metadata_dir}': {e}"),
        })?;

        // Initialize SQLite catalog
        let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
        let catalog = Arc::new(
            CayenneCatalog::new(connection_string)
                .map_err(|e| Error::CatalogInit { source: e })?,
        ) as Arc<dyn MetadataCatalog>;

        catalog.init().await.context(CatalogInitSnafu)?;

        // Initialize local file storage
        let data_dir = params
            .get("cayenne_data_dir")
            .expose()
            .ok()
            .map(String::from)
            .unwrap_or_else(|| format!("{}/cayenne_{catalog_name}/data", spice_data_base_path()));

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

    /// Returns the metadata directory path.
    #[must_use]
    pub fn metadata_dir(&self) -> &str {
        &self.metadata_dir
    }

    /// Parse Vortex configuration from catalog parameters.
    fn parse_vortex_config(params: &Parameters) -> VortexConfig {
        let mut config = VortexConfig::default();

        if let Some(v) = params.get("cayenne_footer_cache_mb").expose().ok() {
            if let Ok(val) = v.parse::<usize>() {
                config.footer_cache_mb = val;
            }
        }
        if let Some(v) = params.get("cayenne_segment_cache_mb").expose().ok() {
            if let Ok(val) = v.parse::<usize>() {
                config.segment_cache_mb = val;
            }
        }
        if let Some(v) = params.get("cayenne_target_file_size_mb").expose().ok() {
            if let Ok(val) = v.parse::<usize>() {
                config.target_vortex_file_size_mb = val;
            }
        }
        if let Some(v) = params.get("cayenne_compression_strategy").expose().ok() {
            match v.to_lowercase().as_str() {
                "zstd" => {
                    config.compression_strategy =
                        cayenne::metadata::CompressionStrategy::Zstd;
                }
                "btrblocks" => {
                    config.compression_strategy =
                        cayenne::metadata::CompressionStrategy::Btrblocks;
                }
                _ => {}
            }
        }

        config
    }

    /// Load all tables from the Cayenne catalog into a schema provider.
    async fn load_schema(
        catalog: &Arc<dyn MetadataCatalog>,
    ) -> Result<Arc<dyn SchemaProvider>> {
        let schema_provider = CayenneSchemaProvider::try_new(
            Arc::clone(catalog),
        )
        .await?;

        Ok(Arc::new(schema_provider))
    }
}

impl CatalogProvider for CayenneCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![
            CAYENNE_DEFAULT_SCHEMA.to_string(),
            super::CAYENNE_PUBLIC_SCHEMA.to_string(),
        ]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Both "default" and "public" resolve to the same schema provider.
        let key = if name == super::CAYENNE_PUBLIC_SCHEMA {
            CAYENNE_DEFAULT_SCHEMA
        } else {
            name
        };
        self.schemas
            .read()
            .ok()
            .and_then(|schemas| schemas.get(key).cloned())
    }
}

#[async_trait]
impl RefreshableCatalogProvider for CayenneCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let schema = Self::load_schema(&self.catalog).await?;

        match self.schemas.write() {
            Ok(mut schemas) => {
                schemas.insert(CAYENNE_DEFAULT_SCHEMA.to_string(), schema);
            }
            Err(poisoned) => {
                poisoned
                    .into_inner()
                    .insert(CAYENNE_DEFAULT_SCHEMA.to_string(), schema);
            }
        }

        Ok(())
    }
}

/// Schema provider for a Cayenne catalog's "default" schema.
///
/// Discovers all tables in the Cayenne metadata catalog and creates
/// [`CayenneTableProvider`] instances for each.
pub struct CayenneSchemaProvider {
    /// The underlying Cayenne metadata catalog.
    catalog: Arc<dyn MetadataCatalog>,
    /// Table providers keyed by table name.
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl std::fmt::Debug for CayenneSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneSchemaProvider").finish()
    }
}

impl CayenneSchemaProvider {
    /// Create a new schema provider, loading all existing tables from the catalog.
    pub async fn try_new(
        catalog: Arc<dyn MetadataCatalog>,
    ) -> Result<Self> {
        // Load all existing tables from the metadata catalog
        let table_names = catalog
            .list_table_names()
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to list existing Cayenne tables: {e}");
                Vec::new()
            });

        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        for name in &table_names {
            match Self::load_table(&catalog, name).await {
                Ok(Some(provider)) => {
                    tables.insert(name.clone(), provider);
                }
                Ok(None) => {
                    tracing::debug!("Table '{name}' listed in catalog but could not be loaded");
                }
                Err(e) => {
                    tracing::warn!("Failed to load Cayenne table '{name}': {e}");
                }
            }
        }

        if !tables.is_empty() {
            tracing::info!(
                "Loaded {} existing Cayenne table{}",
                tables.len(),
                if tables.len() == 1 { "" } else { "s" }
            );
        }

        let provider = Self {
            catalog,
            tables: RwLock::new(tables),
        };

        Ok(provider)
    }

    /// Returns a reference to the underlying Cayenne metadata catalog.
    #[must_use]
    pub fn metadata_catalog(&self) -> &Arc<dyn MetadataCatalog> {
        &self.catalog
    }

    /// Create a [`CayenneTableProvider`] for a table by name.
    async fn load_table(
        catalog: &Arc<dyn MetadataCatalog>,
        table_name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        // Check if the table exists in the catalog
        match catalog.get_table(table_name).await {
            Ok(_metadata) => {
                let builder = CayenneTableProviderBuilder::new(Arc::clone(catalog));

                match builder.open(table_name).await {
                    Ok(provider) => Ok(Some(Arc::new(provider))),
                    Err(e) => {
                        tracing::warn!(
                            "Failed to open Cayenne table '{table_name}': {e}"
                        );
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
        if let Ok(tables) = self.tables.read() {
            if let Some(provider) = tables.get(name) {
                return Ok(Some(Arc::clone(provider)));
            }
        }

        // Try to load from catalog (lazy loading)
        match Self::load_table(&self.catalog, name).await {
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
