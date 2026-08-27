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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use data_components::RefreshableCatalogProvider;
use data_components::catalog_filter::TableSelector;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result as DFResult;
use datafusion::execution::runtime_env::RuntimeEnv;
use parking_lot::RwLock;
use snafu::prelude::*;

use crate::catalog::CatalogError;
use crate::metadata::{CompressionStrategy, PkConflictDetection, VortexConfig};
use crate::{CayenneCatalog, CayenneTableProviderBuilder, MetadataCatalog};

/// Configuration for constructing a [`CayenneCatalogProvider`].
#[derive(Debug, Clone)]
pub struct CayenneCatalogProviderConfig {
    /// Local directory for table data files.
    pub data_dir: Option<String>,
    /// Local directory for Cayenne metadata files.
    pub metadata_dir: Option<String>,
    /// Base path used when data/metadata directories are not explicitly set.
    pub spice_data_base_path: String,
    /// Runtime-global footer cache size in MB, when explicitly configured by a caller.
    pub footer_cache_mb: Option<usize>,
    /// Segment cache size in MB.
    pub segment_cache_mb: Option<usize>,
    /// Target file size in MB.
    pub target_file_size_mb: Option<usize>,
    /// Compression strategy for generated Vortex files.
    pub compression_strategy: Option<CompressionStrategy>,
    /// Maximum number of concurrent file uploads when writing multiple Vortex files.
    pub upload_concurrency: Option<usize>,
    /// Number of writer partitions to use when ingesting unsorted data.
    pub write_concurrency: Option<usize>,
    /// Maximum rows in a single write that can be inlined into the metastore.
    pub inline_max_rows: Option<usize>,
    /// Maximum serialized IPC bytes in a single inlined metastore entry.
    pub inline_max_bytes: Option<usize>,
    /// Maximum Arrow in-memory bytes buffered while deciding whether to inline.
    pub inline_max_buffer_bytes: Option<usize>,
    /// Maximum inline rows before checkpointing to Vortex.
    pub inline_flush_max_rows: Option<i64>,
    /// Maximum inline entries before checkpointing to Vortex.
    pub inline_flush_max_segments: Option<i64>,
    /// Maximum inline IPC bytes before checkpointing to Vortex.
    pub inline_flush_max_bytes: Option<i64>,
    /// Primary-key conflict detection behavior for inserts.
    pub pk_conflict_detection: Option<PkConflictDetection>,
    /// Enable the closed-loop adaptive tuner (`cayenne_tuning: adaptive`). When
    /// `true`, the per-table controller in `provider::context` adapts the
    /// inline-flush caps, compaction cadence/trigger, and write concurrency over
    /// time, anchored to the seeded knob values below.
    pub dynamic_tuning: bool,
    /// Hardware-seeded background compaction interval (ms). Seeds the adaptive
    /// controller's starting point; `None` keeps the engine default.
    pub compaction_background_interval_ms: Option<u64>,
    /// Hardware-seeded small-file compaction trigger. Seeds the adaptive
    /// controller's starting point; `None` keeps the engine default.
    pub compaction_trigger_files: Option<usize>,
    /// Hardware-seeded deletion-index size that triggers the seq-prefix bake.
    /// Seeds the adaptive controller's starting point; `None` keeps the engine
    /// default.
    pub bake_deletion_index_trigger: Option<usize>,
}

/// Errors that can occur when interacting with a Cayenne catalog.
#[derive(Debug, Snafu)]
pub enum Error {
    /// Failed to initialize the Cayenne catalog.
    #[snafu(display("Failed to initialize Cayenne catalog: {source}"))]
    CatalogInit {
        /// The underlying catalog error.
        source: CatalogError,
    },

    /// The Cayenne catalog configuration is invalid.
    #[snafu(display("Invalid Cayenne catalog configuration: {message}"))]
    InvalidConfiguration {
        /// A description of the configuration problem.
        message: String,
    },

    /// Failed to create a table provider from the Cayenne catalog.
    #[snafu(display("Failed to create Cayenne table provider: {source}"))]
    TableProvider {
        /// The underlying catalog error.
        source: CatalogError,
    },

    /// The catalog's data directory contains its own metastore.
    #[snafu(display(
        "Failed to load catalog '{catalog_name}' (cayenne): its data directory '{data_dir}' contains the metastore at '{metadata_dir}', so clearing the data directory would delete the catalog that holds the manifests, snapshot pointers and partition rows for every Cayenne table in this instance. Set `cayenne_metadata_dir` to a directory outside `cayenne_data_dir`. See: https://spiceai.org/docs/components/catalogs/cayenne"
    ))]
    MetastoreInsideDataDir {
        /// The catalog whose configuration was refused.
        catalog_name: String,
        /// The resolved data directory.
        data_dir: String,
        /// The resolved metastore directory it contains.
        metadata_dir: String,
    },

    /// Neither directory could be placed on the filesystem, so the check cannot run.
    #[snafu(display(
        "Failed to load catalog '{catalog_name}' (cayenne): could not resolve its data directory '{data_dir}' or metastore directory '{metadata_dir}', so Spice cannot establish that they are separate and will not open a catalog it might later delete. Check that both paths and their parents exist and are readable. Cause: {source}. See: https://spiceai.org/docs/components/catalogs/cayenne"
    ))]
    CayenneDirsUnresolvable {
        /// The catalog whose configuration was refused.
        catalog_name: String,
        /// The resolved data directory.
        data_dir: String,
        /// The resolved metastore directory.
        metadata_dir: String,
        /// Why the path could not be resolved.
        source: std::io::Error,
    },
}

/// A specialized [`Result`](std::result::Result) type for Cayenne catalog operations.
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
    /// Which of the catalog's tables the configuration selects. Applied to the
    /// namespace-qualified name (`"{namespace}.{table}"`), matching the naming
    /// the SQL catalog connectors match `include`/`exclude` against.
    table_selector: TableSelector,
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
    /// Refuse a catalog whose data directory would contain its metastore.
    ///
    /// The dataset-level accelerator runs the same by-name check immediately before the
    /// recursive delete a schema recreate performs. This path has no such delete today,
    /// so the check runs at open time instead — the point where the operator can still
    /// edit the spicepod, rather than on the first teardown a later change introduces.
    /// Both call the one implementation in [`crate::metastore_layout`], so the two
    /// surfaces cannot drift into disagreeing about what overlaps.
    ///
    /// Neither directory need exist yet: an unresolvable *component* is taken as itself
    /// and the comparison degrades to a lexical one. An unresolvable *path* is a
    /// different matter and refuses, because an overlap that cannot be ruled out is one
    /// that has to be assumed.
    async fn ensure_metastore_outside_data_dir(
        catalog_name: &str,
        data_dir: &str,
        metadata_dir: &str,
    ) -> Result<()> {
        let overlap = crate::metastore_layout::overlapping_metastore_dir(data_dir, metadata_dir)
            .await
            .map_err(|source| Error::CayenneDirsUnresolvable {
                catalog_name: catalog_name.to_string(),
                data_dir: data_dir.to_string(),
                metadata_dir: metadata_dir.to_string(),
                source,
            })?;

        if let Some((data, metadata)) = overlap {
            return Err(Error::MetastoreInsideDataDir {
                catalog_name: catalog_name.to_string(),
                data_dir: data.to_string_lossy().into_owned(),
                metadata_dir: metadata.to_string_lossy().into_owned(),
            });
        }
        Ok(())
    }

    /// Create a new Cayenne catalog provider.
    ///
    /// Initializes the `SQLite` metadata catalog and local file storage.
    ///
    /// `table_selector` carries the catalog's `include`/`exclude` patterns;
    /// pass [`TableSelector::select_all`] for a catalog that configured
    /// neither. It is taken as its own argument rather than a
    /// [`CayenneCatalogProviderConfig`] field because it decides which tables
    /// exist for this catalog, not how they are stored or tuned.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata or data directories cannot be created,
    /// if the metadata catalog fails to initialize, or if the data directory would
    /// contain the metastore — see [`Self::ensure_metastore_outside_data_dir`].
    pub async fn try_new(
        config: CayenneCatalogProviderConfig,
        runtime_env: Arc<RuntimeEnv>,
        table_selector: TableSelector,
    ) -> Result<Self> {
        let catalog_name = DEFAULT_CATALOG_NAME;
        let spice_data_base_path = config.spice_data_base_path.as_str();

        // Resolve metadata directory
        let metadata_dir = config
            .metadata_dir
            .clone()
            .unwrap_or_else(|| format!("{spice_data_base_path}/cayenne_{catalog_name}/metadata"));

        // Resolved before either directory is created, because the check below refuses a
        // configuration and a refused catalog must not leave a metastore behind it.
        let data_dir = config
            .data_dir
            .clone()
            .unwrap_or_else(|| format!("{spice_data_base_path}/cayenne_{catalog_name}/data"));

        Self::ensure_metastore_outside_data_dir(catalog_name, &data_dir, &metadata_dir).await?;

        // Ensure metadata directory exists
        tokio::fs::create_dir_all(&metadata_dir)
            .await
            .map_err(|e| Error::InvalidConfiguration {
                message: format!("Failed to create metadata directory '{metadata_dir}': {e}"),
            })?;

        // Initialize SQLite catalog
        let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
        let catalog = Arc::new(CayenneCatalog::new(connection_string).context(CatalogInitSnafu)?)
            as Arc<dyn MetadataCatalog>;

        catalog.init().await.context(CatalogInitSnafu)?;

        // Initialize local file storage
        tokio::fs::create_dir_all(&data_dir)
            .await
            .map_err(|e| Error::InvalidConfiguration {
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
        let vortex_config = Self::vortex_config_from_config(&config);

        let provider = Self {
            catalog,
            vortex_config,
            data_base_path,
            metadata_dir,
            runtime_env,
            table_selector,
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

    /// Returns which of the catalog's tables the configuration selects.
    ///
    /// Schema providers built outside [`Self::refresh`] -- the DDL path -- take
    /// their selector from here, so every namespace in the catalog resolves a
    /// table name the same way.
    #[must_use]
    pub fn table_selector(&self) -> &TableSelector {
        &self.table_selector
    }

    /// Returns the schema provider for a namespace if it exists.
    #[must_use]
    pub fn schema_provider(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.read().get(name).cloned()
    }

    /// Registers or replaces a schema provider for a namespace.
    ///
    /// # Errors
    ///
    pub fn register_schema_provider(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.write().insert(name.to_string(), schema))
    }

    fn vortex_config_from_config(provider_config: &CayenneCatalogProviderConfig) -> VortexConfig {
        let mut config = VortexConfig {
            footer_cache_mb: provider_config.footer_cache_mb,
            ..Default::default()
        };
        if let Some(v) = provider_config.segment_cache_mb {
            config.segment_cache_mb = v;
        }
        if let Some(v) = provider_config.target_file_size_mb {
            config.target_vortex_file_size_mb = v;
        }
        if let Some(v) = provider_config.compression_strategy.as_ref() {
            config.compression_strategy = v.clone();
        }
        if let Some(v) = provider_config.upload_concurrency {
            config.upload_concurrency = v.max(1);
        }
        if let Some(v) = provider_config.write_concurrency {
            config.write_concurrency = Some(v.max(1));
        }
        if let Some(v) = provider_config.inline_max_rows {
            config.inline_max_rows = v;
        }
        if let Some(v) = provider_config.inline_max_bytes {
            config.inline_max_bytes = v;
        }
        if let Some(v) = provider_config.inline_max_buffer_bytes {
            config.inline_max_buffer_bytes = v;
        }
        if let Some(v) = provider_config.inline_flush_max_rows {
            config.inline_flush_max_rows = v.max(0);
        }
        if let Some(v) = provider_config.inline_flush_max_segments {
            config.inline_flush_max_segments = v.max(0);
        }
        if let Some(v) = provider_config.inline_flush_max_bytes {
            config.inline_flush_max_bytes = v.max(0);
        }
        if let Some(v) = provider_config.pk_conflict_detection {
            config.pk_conflict_detection = v;
        }
        if let Some(v) = provider_config.compaction_background_interval_ms {
            config.compaction_background_interval_ms = v;
        }
        if let Some(v) = provider_config.compaction_trigger_files {
            config.compaction_trigger_files = v;
        }
        if let Some(v) = provider_config.bake_deletion_index_trigger {
            config.bake_deletion_index_trigger = v;
        }
        // Enable the closed loop last so it anchors to the seeded knob values
        // above (the controller bounds derive from `[floor, 4×seed]`).
        config.dynamic_tuning = provider_config.dynamic_tuning;
        config
    }
}

impl CatalogProvider for CayenneCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.read().keys().cloned().collect()
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
            if let Some((ns, table)) = full_name.split_once('/') {
                // The catalog's `include`/`exclude` decide membership here, at
                // the point the catalog discovers a table, so a withheld table
                // never reaches a schema provider.
                if let Some(reason) = self
                    .table_selector
                    .rejection_reason(&format!("{ns}.{table}"))
                {
                    tracing::debug!("Cayenne table '{ns}.{table}' {reason}, skipping");
                    continue;
                }
                grouped
                    .entry(ns.to_string())
                    .or_default()
                    .push(full_name.clone());
            } else {
                tracing::debug!("Cayenne table '{full_name}' has no namespace prefix, skipping");
            }
        }

        let existing_schemas = self.schemas.read().clone();

        let mut new_schemas: HashMap<String, Arc<dyn SchemaProvider>> = HashMap::new();
        for (ns, full_names) in &grouped {
            let refreshed_schema = CayenneSchemaProvider::try_new(
                Arc::clone(&self.catalog),
                ns,
                full_names,
                Arc::clone(&self.runtime_env),
                self.table_selector.clone(),
            )
            .await?;

            if let Some(existing_schema) = existing_schemas.get(ns)
                && let Some(existing_cayenne_schema) =
                    existing_schema.downcast_ref::<CayenneSchemaProvider>()
            {
                existing_cayenne_schema.refresh_from(&refreshed_schema);
                new_schemas.insert(ns.clone(), Arc::clone(existing_schema));
            } else {
                new_schemas.insert(ns.clone(), Arc::new(refreshed_schema));
            }
        }

        for (ns, existing_schema) in &existing_schemas {
            if grouped.contains_key(ns) {
                continue;
            }

            if let Some(existing_cayenne_schema) =
                existing_schema.downcast_ref::<CayenneSchemaProvider>()
            {
                existing_cayenne_schema.clear_tables();
            }
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

        *self.schemas.write() = new_schemas;

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
    /// The catalog's selector, carried down so the lazy-load path in
    /// [`SchemaProvider::table`] withholds the same tables discovery does.
    table_selector: TableSelector,
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
    ///
    /// # Errors
    ///
    /// Returns an error if any table fails to load from the catalog.
    pub async fn try_new(
        catalog: Arc<dyn MetadataCatalog>,
        namespace: &str,
        full_table_names: &[String],
        runtime_env: Arc<RuntimeEnv>,
        table_selector: TableSelector,
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
            table_selector,
            tables: RwLock::new(tables),
        })
    }

    /// Create an empty schema provider for a namespace (used by DDL).
    ///
    /// Callers pass the owning catalog's
    /// [`CayenneCatalogProvider::table_selector`] so a namespace created by DDL
    /// resolves table names the same way a discovered one does.
    #[must_use]
    pub fn new_empty(
        catalog: Arc<dyn MetadataCatalog>,
        namespace: String,
        runtime_env: Arc<RuntimeEnv>,
        table_selector: TableSelector,
    ) -> Self {
        Self {
            catalog,
            namespace,
            runtime_env,
            table_selector,
            tables: RwLock::new(HashMap::new()),
        }
    }

    fn tables_snapshot(&self) -> HashMap<String, Arc<dyn TableProvider>> {
        self.tables.read().clone()
    }

    /// Synchronous lookup of a cached table provider by name. Unlike the async
    /// [`SchemaProvider::table`], this reads the in-memory table cache directly,
    /// so callers that need a table's schema from a sync context (e.g. the
    /// `executor_table` UDTF's `TableFunctionImpl::call`) can avoid blocking.
    #[must_use]
    pub fn table_sync(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.read().get(name).cloned()
    }

    fn replace_tables(&self, tables: HashMap<String, Arc<dyn TableProvider>>) {
        *self.tables.write() = tables;
    }

    fn refresh_from(&self, source: &Self) {
        let existing_tables = self.tables_snapshot();
        let refreshed_tables = source.tables_snapshot();
        let mut merged_tables = HashMap::with_capacity(refreshed_tables.len());

        for (table_name, refreshed_provider) in refreshed_tables {
            let provider_to_use = if let Some(existing_provider) = existing_tables.get(&table_name)
            {
                // Existing providers are authoritative — their in-memory state is kept up-to-date by writes (insert, delete, etc).
                // Reloading from the catalog is redundant and leads to unnecessary work and side effects including cache invalidations
                Arc::clone(existing_provider)
            } else {
                refreshed_provider
            };

            merged_tables.insert(table_name, provider_to_use);
        }

        self.replace_tables(merged_tables);
    }

    fn clear_tables(&self) {
        self.replace_tables(HashMap::new());
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

    /// Whether the catalog selects `short_name` in this namespace.
    ///
    /// For callers that enumerate the metadata catalog themselves instead of
    /// reading this provider's tables -- cluster table discovery does, to avoid
    /// depending on the in-memory cache. Asking here keeps them from picking up
    /// a table the catalog's `include`/`exclude` withheld.
    #[must_use]
    pub fn selects_table(&self, short_name: &str) -> bool {
        self.table_selector
            .selects_table(&self.namespace, short_name)
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
                    Ok(provider) => Ok(Some(Arc::new(provider) as Arc<dyn TableProvider>)),
                    Err(e) => {
                        tracing::warn!("Failed to open Cayenne table '{table_name}': {e}");
                        Ok(None)
                    }
                }
            }
            Err(CatalogError::TableNotFound { .. }) => Ok(None),
            Err(e) => Err(Error::TableProvider { source: e }),
        }
    }
}

#[async_trait]
impl SchemaProvider for CayenneSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.read().contains_key(name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Check in-memory cache first. The cache holds what this catalog has
        // already registered -- discovery filtered it on the way in, and DDL
        // registers deliberately -- so it is served without re-deciding, which
        // also keeps `table` and `table_names` reporting the same set.
        if let Some(provider) = self.tables.read().get(name) {
            return Ok(Some(Arc::clone(provider)));
        }

        // A miss falls through to the catalog, which would otherwise re-admit a
        // table `refresh` withheld: naming it in a query is enough. Apply the
        // same decision here so `exclude` cannot be bypassed that way.
        if let Some(reason) = self
            .table_selector
            .rejection_reason(&format!("{}.{name}", self.namespace))
        {
            tracing::debug!(
                "Cayenne table '{}.{name}' {reason}, not loading it",
                self.namespace
            );
            return Ok(None);
        }

        // Try to load from catalog (lazy loading) using namespace-prefixed name
        let full_name = self.full_table_name(name);
        match Self::load_table(&self.catalog, &full_name, &self.runtime_env).await {
            Ok(Some(provider)) => {
                self.tables
                    .write()
                    .insert(name.to_string(), Arc::clone(&provider));
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
        Ok(self.tables.write().insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.write().remove(name))
    }
}

#[cfg(test)]
mod tests {
    use super::{CayenneCatalogProvider, CayenneCatalogProviderConfig, Error};

    /// A config that configures nothing but the base path, so each test states only the
    /// two directories it is about.
    fn config(base: &str) -> CayenneCatalogProviderConfig {
        CayenneCatalogProviderConfig {
            data_dir: None,
            metadata_dir: None,
            spice_data_base_path: base.to_string(),
            footer_cache_mb: None,
            segment_cache_mb: None,
            target_file_size_mb: None,
            compression_strategy: None,
            upload_concurrency: None,
            write_concurrency: None,
            inline_max_rows: None,
            inline_max_bytes: None,
            inline_max_buffer_bytes: None,
            inline_flush_max_rows: None,
            inline_flush_max_segments: None,
            inline_flush_max_bytes: None,
            pk_conflict_detection: None,
            dynamic_tuning: false,
            compaction_background_interval_ms: None,
            compaction_trigger_files: None,
            bake_deletion_index_trigger: None,
        }
    }

    /// The defaults this connector ships are structurally disjoint — `…/data` beside
    /// `…/metadata` — so the guard must not refuse a catalog that configured nothing.
    /// Without this, "the guard fires" and "the guard fires on everything" look alike.
    #[tokio::test]
    async fn the_default_layout_is_accepted() {
        let base = tempfile::tempdir().expect("temp dir");
        let config = config(&base.path().to_string_lossy());

        let data_dir = format!("{}/cayenne_cayenne/data", config.spice_data_base_path);
        let metadata_dir = format!("{}/cayenne_cayenne/metadata", config.spice_data_base_path);

        CayenneCatalogProvider::ensure_metastore_outside_data_dir(
            "cayenne",
            &data_dir,
            &metadata_dir,
        )
        .await
        .expect("the shipped defaults are disjoint");
    }

    /// Regression test for #13105: the catalog connector carries the same
    /// `data_dir`/`metadata_dir` pair as the dataset-level accelerator and validated
    /// neither, so an operator could point the metastore inside the directory a
    /// teardown clears. Refused at open time, where the spicepod can still be edited.
    #[tokio::test]
    async fn a_metastore_inside_the_data_dir_is_refused() {
        let base = tempfile::tempdir().expect("temp dir");
        let data_dir = base.path().join("data").to_string_lossy().into_owned();
        let metadata_dir = format!("{data_dir}/catalog");

        let error = CayenneCatalogProvider::ensure_metastore_outside_data_dir(
            "trades",
            &data_dir,
            &metadata_dir,
        )
        .await
        .expect_err("a metastore inside the data directory must be refused");

        assert!(
            matches!(error, Error::MetastoreInsideDataDir { .. }),
            "expected a metastore-overlap refusal, got: {error}"
        );

        let rendered = error.to_string();
        for expected in [
            "trades",
            "cayenne_metadata_dir",
            "cayenne_data_dir",
            "https://spiceai.org/docs/components/catalogs/cayenne",
        ] {
            assert!(
                rendered.contains(expected),
                "the refusal must name `{expected}` so an operator can act on it: {rendered}"
            );
        }
    }

    /// The two directories being one directory is the same catalog loss, and is what a
    /// single `cayenne_data_dir` pointed at the metastore produces.
    #[tokio::test]
    async fn a_metastore_at_the_data_dir_itself_is_refused() {
        let base = tempfile::tempdir().expect("temp dir");
        let shared = base.path().join("shared").to_string_lossy().into_owned();

        let error =
            CayenneCatalogProvider::ensure_metastore_outside_data_dir("trades", &shared, &shared)
                .await
                .expect_err("one directory serving as both must be refused");
        assert!(
            matches!(error, Error::MetastoreInsideDataDir { .. }),
            "expected a metastore-overlap refusal, got: {error}"
        );
    }

    /// The refusal has to land before either directory is created: a catalog that is
    /// refused must not leave a metastore — or the directory that would hold one —
    /// behind it, since the next start would then find a catalog where the operator
    /// never put one.
    #[tokio::test]
    async fn a_refused_catalog_creates_no_directories() {
        let base = tempfile::tempdir().expect("temp dir");
        let mut config = config(&base.path().to_string_lossy());
        let data_dir = base.path().join("data");
        config.data_dir = Some(data_dir.to_string_lossy().into_owned());
        config.metadata_dir = Some(data_dir.join("catalog").to_string_lossy().into_owned());

        let error = CayenneCatalogProvider::try_new(
            config,
            std::sync::Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default()),
            data_components::catalog_filter::TableSelector::select_all(),
        )
        .await
        .expect_err("an overlapping catalog configuration must be refused");

        assert!(
            matches!(error, Error::MetastoreInsideDataDir { .. }),
            "expected a metastore-overlap refusal, got: {error}"
        );
        assert!(
            !data_dir.exists(),
            "a refused catalog must not have created {}",
            data_dir.display()
        );
    }

    /// A sibling that merely shares a name prefix is disjoint. Refusing it would break
    /// a working configuration, which is the cost of a containment test that compares
    /// strings rather than path components.
    #[tokio::test]
    async fn a_sibling_sharing_a_name_prefix_is_accepted() {
        let base = tempfile::tempdir().expect("temp dir");
        let data_dir = base.path().join("meta").to_string_lossy().into_owned();
        let metadata_dir = base.path().join("metadata").to_string_lossy().into_owned();

        CayenneCatalogProvider::ensure_metastore_outside_data_dir(
            "trades",
            &data_dir,
            &metadata_dir,
        )
        .await
        .expect("`meta` and `metadata` are siblings, not nested");
    }

    /// A data directory on object storage cannot hold the metastore, so a catalog that
    /// puts its data there keeps working with the metastore on local disk.
    #[tokio::test]
    async fn an_object_store_data_dir_is_accepted() {
        CayenneCatalogProvider::ensure_metastore_outside_data_dir(
            "trades",
            "s3://bucket/trades/",
            "/var/spice/metadata",
        )
        .await
        .expect("a metastore cannot live inside an object-store prefix");
    }
}
