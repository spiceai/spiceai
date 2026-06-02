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

use super::{AccelerationSource, BootstrapStatus, DataAccelerator};
use crate::{
    App, Runtime,
    component::{
        dataset::{
            Dataset,
            acceleration::{Acceleration, Engine, Mode, RefreshMode},
        },
        view::View,
    },
    dataaccelerator::{
        FilePathError,
        snapshots::{download_snapshot_if_needed, snapshot_before_recreate},
        storage::{ResolvedAccelerationStorage, resolve_acceleration_storage_async},
    },
    datafusion::dialect::new_duckdb_dialect,
    make_spice_data_directory,
    parameters::ParameterSpec,
    register_data_accelerator, spice_data_base_path,
};
use async_trait::async_trait;
use data_components::poly::PolyTableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::{
    catalog::{Session, TableProviderFactory},
    common::{Constraints, Statistics},
    datasource::{TableProvider, TableType},
    execution::{SendableRecordBatchStream, context::SessionContext},
    logical_expr::{CreateExternalTable, Expr, TableProviderFilterPushDown},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
        coalesce_partitions::CoalescePartitionsExec, stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
};
use datafusion_table_providers::{
    duckdb::{
        DuckDB, DuckDBSettingsRegistry, DuckDBTableProviderFactory, write::DuckDBTableWriter,
    },
    sql::db_connection_pool::duckdbpool::{DuckDbConnectionPool, DuckDbConnectionPoolBuilder},
    util::{column_reference::ColumnReference, indexes::IndexType},
};
use futures::StreamExt;
use itertools::Itertools;
use runtime_acceleration::snapshot::AccelerationEngine;
use runtime_table_partition::expression::PartitionedBy;
use settings::OrderByNonIntegerLiteral;
use snafu::prelude::*;
use spiceai_duckdb::AccessMode;
use std::collections::HashMap;
use std::{
    any::Any,
    cmp::max,
    collections::HashSet,
    ffi::OsStr,
    path::PathBuf,
    sync::{Arc, Once},
};

pub(crate) mod settings;

/// Creates a [`DuckDBTableProviderFactory`] with standard Spice settings (dialect, timezone,
/// index scan tuning, function deny-list). All `DuckDB` accelerator consumers should use this
/// to avoid divergent configurations.
pub(crate) fn create_factory() -> DuckDBTableProviderFactory {
    DuckDBTableProviderFactory::new(AccessMode::ReadWrite)
        .with_dialect(new_duckdb_dialect())
        .with_settings_registry(
            DuckDBSettingsRegistry::new()
                .with_setting(Box::new(OrderByNonIntegerLiteral))
                .with_setting(Box::new(settings::IndexScanPercentage))
                .with_setting(Box::new(settings::IndexScanMaxCount))
                .with_setting(Box::new(settings::TimeZone)),
        )
}

pub(crate) const DEFAULT_CONNECTION_POOL_SIZE: u32 = 10;
pub(crate) const DEFAULT_EBS_CONNECTION_POOL_SIZE: u32 = 4;
pub(crate) const SPICE_ACCELERATOR_METADATA_KEY: &str = "spice.accelerator";
pub(crate) const SPICE_OPT_DUCKDB_AGG_PUSHDOWN_KEY: &str =
    "spice.optimizer.duckdb_aggregate_pushdown";

use super::upsert_dedup;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create table: {source}"))]
    UnableToCreateTable {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Acceleration creation failed: {source}"))]
    AccelerationCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Acceleration initialization failed: {source}"))]
    AccelerationInitializationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(r#"The "duckdb_file" acceleration parameter has an invalid extension. Expected one of "{valid_extensions}" but got "{extension}"."#))]
    InvalidFileExtension {
        valid_extensions: String,
        extension: String,
    },

    #[snafu(display(r#"The "duckdb_file" acceleration parameter is a directory."#))]
    InvalidFileIsDirectory,

    #[snafu(display("Acceleration not enabled for dataset: {dataset}"))]
    AccelerationNotEnabled { dataset: Arc<str> },

    #[snafu(display("Invalid DuckDB acceleration configuration: {detail}"))]
    InvalidConfiguration { detail: Arc<str> },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDBAccelerator {
    duckdb_factory: DuckDBTableProviderFactory,
}

impl DuckDBAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            duckdb_factory: create_factory(),
        }
    }

    /// Returns the `DuckDB` file path that would be used for a file-based `DuckDB` accelerator from this dataset
    pub fn duckdb_file_path(&self, source: &dyn AccelerationSource) -> Result<String> {
        duckdb_file_path(&self.duckdb_factory, source, "accelerated_duckdb")
    }

    /// Returns an existing `DuckDB` connection pool for the given dataset, or creates a new one if it doesn't exist.
    pub async fn get_shared_pool(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<DuckDbConnectionPool> {
        let duckdb_file = self.duckdb_file_path(source);

        let acceleration = source.acceleration().context(AccelerationNotEnabledSnafu {
            dataset: source.name().to_string(),
        })?;

        let pool = match (duckdb_file, acceleration.mode) {
            (Ok(duckdb_file), Mode::File | Mode::FileCreate | Mode::FileUpdate) => {
                let num_accelerating_datasets = self.get_num_accelerating_datasets(
                    Some(duckdb_file.as_str()),
                    &source.app(),
                    source.runtime(),
                );
                let storage =
                    resolve_acceleration_storage_async(acceleration.storage_profile, &duckdb_file)
                        .await;
                tracing::debug!(
                    dataset = %source.name(),
                    storage = %storage,
                    "Resolved DuckDB acceleration storage profile"
                );
                let max_size =
                    Self::get_pool_max_size(num_accelerating_datasets, acceleration, storage);
                let min_idle = Self::get_pool_min_idle(storage, max_size);
                let mut pool_builder = DuckDbConnectionPoolBuilder::file(&duckdb_file)
                    .with_max_size(Some(max_size))
                    .with_min_idle(Some(min_idle))
                    .with_connection_setup_query("PRAGMA enable_checkpoint_on_shutdown");
                for pragma in Self::storage_setup_queries(storage) {
                    pool_builder = pool_builder.with_connection_setup_query(*pragma);
                }
                self.duckdb_factory
                    .get_or_init_instance_with_builder(pool_builder)
                    .await
                    .boxed()
                    .context(AccelerationCreationFailedSnafu)?
            }
            (_, Mode::Memory) => {
                let num_accelerating_datasets =
                    self.get_num_accelerating_datasets(None, &source.app(), source.runtime());
                let max_size = Self::get_pool_max_size(
                    num_accelerating_datasets,
                    acceleration,
                    ResolvedAccelerationStorage::Unknown,
                );
                let min_idle =
                    Self::get_pool_min_idle(ResolvedAccelerationStorage::Unknown, max_size);
                let pool_builder = DuckDbConnectionPoolBuilder::memory()
                    .with_max_size(Some(max_size))
                    .with_min_idle(Some(min_idle))
                    .with_connection_setup_query("PRAGMA enable_checkpoint_on_shutdown");
                self.duckdb_factory
                    .get_or_init_instance_with_builder(pool_builder)
                    .await
                    .boxed()
                    .context(AccelerationCreationFailedSnafu)?
            }
            (Err(e), Mode::File | Mode::FileCreate | Mode::FileUpdate) => {
                return Err(Error::InvalidConfiguration {
                    detail: Arc::from(e.to_string()),
                });
            }
        };

        Ok(pool)
    }

    fn get_num_accelerating_datasets(
        &self,
        path: Option<&str>,
        app: &Arc<App>,
        rt: Arc<Runtime>,
    ) -> u32 {
        let mut instance_usage: u32 = 1;

        let datasets = rt.get_valid_datasets(app, crate::LogErrors(false));
        for ds in datasets {
            if let Some(acceleration) = &ds.acceleration {
                if acceleration.engine != Engine::DuckDB {
                    continue;
                }

                // If the path is Some, we're counting the number of file instances
                if let Some(this_file_path) = path {
                    if matches!(
                        acceleration.mode,
                        Mode::File | Mode::FileCreate | Mode::FileUpdate
                    ) && let Ok(file_path) = self.file_path(ds.as_ref())
                        && this_file_path == file_path
                    {
                        instance_usage += 1;
                    }
                } else {
                    // If the path is None, we're just counting the number of memory instances
                    if acceleration.mode == Mode::Memory {
                        instance_usage += 1;
                    }
                }
            }
        }

        instance_usage
    }

    pub(crate) fn default_connection_pool_size(storage: ResolvedAccelerationStorage) -> u32 {
        match storage {
            ResolvedAccelerationStorage::Ebs => DEFAULT_EBS_CONNECTION_POOL_SIZE,
            ResolvedAccelerationStorage::LocalSsd
            | ResolvedAccelerationStorage::Tmpfs
            | ResolvedAccelerationStorage::Unknown => DEFAULT_CONNECTION_POOL_SIZE,
        }
    }

    pub(crate) fn get_pool_min_idle(storage: ResolvedAccelerationStorage, max_size: u32) -> u32 {
        Self::default_connection_pool_size(storage).min(max_size)
    }

    /// Storage-profile-specific `DuckDB` pragmas applied to every connection in
    /// the pool. These tune `DuckDB`'s I/O behavior to match the underlying
    /// medium's latency and durability profile.
    pub(crate) fn storage_setup_queries(
        storage: ResolvedAccelerationStorage,
    ) -> &'static [&'static str] {
        match storage {
            // Network-attached block storage (e.g. EBS, Azure Managed Disks)
            // pays per-IO latency on every flush. Raise the checkpoint
            // threshold so WAL flushes are larger and less frequent, which
            // reduces write amplification on the slow link.
            ResolvedAccelerationStorage::Ebs => &["PRAGMA checkpoint_threshold='256MiB'"],
            // tmpfs/ramfs is volatile and effectively free to write, but
            // checkpointing still copies pages around. Push the threshold up
            // so steady-state workloads don't pay checkpoint cost on tiny
            // amounts of dirty data.
            ResolvedAccelerationStorage::Tmpfs => &["PRAGMA checkpoint_threshold='1GiB'"],
            // Local SSD/NVMe handles small frequent flushes well; keep
            // DuckDB defaults.
            ResolvedAccelerationStorage::LocalSsd | ResolvedAccelerationStorage::Unknown => &[],
        }
    }

    fn get_pool_max_size(
        num_accelerating_datasets: u32,
        acceleration: &Acceleration,
        storage: ResolvedAccelerationStorage,
    ) -> u32 {
        let pool_size_param = acceleration
            .params
            .get("connection_pool_size")
            .and_then(|size_str| size_str.parse::<u32>().ok());

        pool_size_param.unwrap_or_else(|| {
            max(
                Self::default_connection_pool_size(storage),
                num_accelerating_datasets,
            )
        })
    }
}

/// Returns the `DuckDB` file path that would be used for a file-based `DuckDB` acceleration for this acceleration source
///
/// # Parameters
///
/// * `duckdb_factory` - The `DuckDB` table provider factory used to generate the file path
/// * `source` - The acceleration source (dataset or view) containing acceleration configuration
/// * `default_db_name` - Default database file name to use if the `duckdb_file` parameter is not specified
pub fn duckdb_file_path(
    duckdb_factory: &DuckDBTableProviderFactory,
    source: &dyn AccelerationSource,
    default_db_name: &str,
) -> Result<String> {
    if !source.is_file_accelerated() {
        Err(Error::InvalidConfiguration {
            detail: Arc::from("Dataset is not file accelerated"),
        })
    } else if let Some(acceleration) = source.acceleration().as_ref() {
        let mut params = acceleration.params.clone();
        let mut using_duckdb_data_dir = true;
        let data_directory = params.remove("duckdb_data_dir").unwrap_or_else(|| {
            using_duckdb_data_dir = false;
            spice_data_base_path()
        });
        params.insert("data_directory".to_string(), data_directory);

        if let Some(duckdb_file) = params.remove("duckdb_file") {
            if using_duckdb_data_dir {
                static WARN_ONCE: Once = Once::new();
                WARN_ONCE.call_once(|| {
                    tracing::warn!(
                        "'duckdb_data_dir' and 'duckdb_file' were both specified but 'duckdb_file' ({duckdb_file}) will be used."
                    );
                });
            }
            params.insert("duckdb_open".to_string(), duckdb_file);
        }

        duckdb_factory
            .duckdb_file_path(default_db_name, &mut params)
            .map_err(|err| Error::InvalidConfiguration {
                detail: Arc::from(err.to_string()),
            })
    } else {
        unreachable!("Expected dataset to have acceleration parameters, but none were found")
    }
}

impl Default for DuckDBAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::runtime("file_watcher"),
    ParameterSpec::component("file"),
    ParameterSpec::component("data_dir"),
    ParameterSpec::component("memory_limit"),
    ParameterSpec::component("preserve_insertion_order"),
    ParameterSpec::component("index_scan_percentage"),
    ParameterSpec::component("index_scan_max_count"),
    ParameterSpec::runtime("partition_mode"),
    ParameterSpec::component("partitioned_write_flush_threshold_rows"),
    ParameterSpec::runtime("connection_pool_size").description(
        "The maximum number of client connections created in the duckdb connection pool.",
    ),
    ParameterSpec::runtime("on_refresh_recompute_statistics"),
    ParameterSpec::runtime("on_refresh_sort_columns"),
    ParameterSpec::runtime("partitioned_write_buffer"),
    ParameterSpec::runtime("optimizer_duckdb_aggregate_pushdown"),
];

static DUCKDB_TYPE_REWRITE_RULES: &[&dyn arrow_tools::type_rewrite::TypeRewriteRule] = &[
    &arrow_tools::type_rewrite::DictionaryUnwrap,
    &arrow_tools::type_rewrite::IntervalToMonthDayNano,
    &arrow_tools::type_rewrite::NullToInt32,
];

#[async_trait]
impl DataAccelerator for DuckDBAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "duckdb"
    }

    fn valid_file_extensions(&self) -> Vec<&'static str> {
        vec!["db", "ddb", "duckdb"]
    }

    fn file_path(&self, source: &dyn AccelerationSource) -> Result<String, FilePathError> {
        self.duckdb_file_path(source)
            .map_err(|e| FilePathError::External {
                engine: Engine::DuckDB,
                source: e.into(),
            })
    }

    fn is_initialized(&self, source: &dyn AccelerationSource) -> bool {
        if !source.is_file_accelerated() {
            return true; // memory mode DuckDB is always initialized
        }

        // otherwise, we're initialized if the file exists
        self.has_existing_file(source)
    }

    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<BootstrapStatus, Box<dyn std::error::Error + Send + Sync>> {
        if !source.is_file_accelerated() {
            return Ok(BootstrapStatus::none());
        }

        let path = self.file_path(source)?;

        if let Some(acceleration) = source.acceleration() {
            if !acceleration.params.contains_key("duckdb_file") {
                make_spice_data_directory().map_err(|err| {
                    Error::AccelerationInitializationFailed { source: err.into() }
                })?;
            } else if !self.is_valid_file(source) {
                if std::path::Path::new(&path).is_dir() {
                    return Err(Error::InvalidFileIsDirectory.into());
                }

                let extension = std::path::Path::new(&path)
                    .extension()
                    .and_then(OsStr::to_str)
                    .unwrap_or("");

                return Err(Error::InvalidFileExtension {
                    valid_extensions: self.valid_file_extensions().join(","),
                    extension: extension.to_string(),
                }
                .into());
            }

            // If mode is FileCreate, snapshot the existing file (if enabled) then delete it to start fresh
            if acceleration.mode == Mode::FileCreate {
                let file_path = std::path::Path::new(&path);
                if file_path.exists() {
                    snapshot_before_recreate(
                        acceleration,
                        &source.name().to_string(),
                        runtime_acceleration::snapshot::AccelerationLayout::file(PathBuf::from(
                            &path,
                        )),
                        AccelerationEngine::DuckDB,
                        Arc::new(arrow_schema::Schema::empty()),
                        None,
                    )
                    .await;

                    tracing::warn!(
                        "DuckDB acceleration mode is 'file_create', removing existing file: {}",
                        path
                    );
                    std::fs::remove_file(file_path).map_err(|err| {
                        Error::AccelerationInitializationFailed { source: err.into() }
                    })?;
                }
            }

            let bootstrap_status = download_snapshot_if_needed(
                acceleration,
                source,
                runtime_acceleration::snapshot::AccelerationLayout::file(PathBuf::from(path)),
                AccelerationEngine::DuckDB,
                None,
            )
            .await;

            self.get_shared_pool(source).await?;

            return Ok(bootstrap_status);
        }

        Ok(BootstrapStatus::none())
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        mut cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        _partition_by: Vec<PartitionedBy>,
        _runtime_env: Option<Arc<RuntimeEnv>>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        normalize_schema_for_duckdb(&mut cmd)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        if let Some(duckdb_file) = cmd.options.remove("file") {
            cmd.options.insert("open".to_string(), duckdb_file);
        }

        if let Some(recompute_statistics_on_write) =
            cmd.options.remove("on_refresh_recompute_statistics")
        {
            // Translate Spice parameter to DuckDB write setting
            cmd.options.insert(
                "recompute_statistics_on_write".to_string(),
                recompute_statistics_on_write,
            );
        }

        let is_changes_refresh = source
            .and_then(|src| src.acceleration())
            .and_then(|acceleration| acceleration.refresh_mode)
            .is_some_and(|refresh_mode| refresh_mode == RefreshMode::Changes);
        apply_changes_refresh_write_defaults(&mut cmd, is_changes_refresh);

        // Modify the `cmd` by adding options to attach other databases
        if let Some(source) = source {
            if let Some(temp_directory) = source
                .app()
                .runtime
                .query
                .clone()
                .unwrap_or_default()
                .temp_directory
            {
                cmd.options
                    .insert("temp_directory".to_string(), temp_directory);
            }

            if source.is_file_accelerated() {
                // If the user didn't specify a DuckDB file and this is a file-mode DuckDB,
                // then use the shared DuckDB file `accelerated_duckdb.db`
                if !cmd.options.contains_key("open") {
                    let duckdb_file = self.duckdb_file_path(source)?;
                    cmd.options.insert("open".to_string(), duckdb_file);
                }

                let datasets: Vec<Arc<Dataset>> = Arc::clone(&source.runtime())
                    .get_initialized_datasets(&source.app(), crate::LogErrors(false))
                    .await;

                let views: Vec<Arc<View>> = Arc::clone(&source.runtime())
                    .get_initialized_views(&source.app(), crate::LogErrors(false))
                    .await;

                let self_path = self.file_path(source)?;
                let attach_databases = datasets
                    .into_iter()
                    .map(|ds| ds as Arc<dyn AccelerationSource>)
                    .chain(
                        views
                            .into_iter()
                            .map(|view| view as Arc<dyn AccelerationSource>),
                    )
                    .filter_map(|other_source| {
                        if other_source.acceleration().is_some_and(|a| {
                            a.engine == Engine::DuckDB
                                && matches!(
                                    a.mode,
                                    Mode::File | Mode::FileCreate | Mode::FileUpdate
                                )
                        }) {
                            if other_source.name() == source.name() {
                                None
                            } else {
                                let other_path = self.file_path(other_source.as_ref());
                                other_path.ok().filter(|p| p != &self_path)
                            }
                        } else {
                            None
                        }
                    })
                    .collect::<HashSet<_>>(); // collect unique paths using HashSet

                if !attach_databases.is_empty() {
                    cmd.options.insert(
                        "attach_databases".to_string(),
                        attach_databases.iter().join(";"),
                    );
                }
            }
        }

        if let Some(acceleration) = source.and_then(AccelerationSource::acceleration) {
            if acceleration
                .retention_sql
                .as_deref()
                .map(str::trim)
                .is_some_and(|retention_sql| !retention_sql.is_empty())
            {
                return Err(DataFusionError::NotImplemented(
                    "DuckDB retention_sql is unavailable because datafusion-table-providers 0.11 no longer exposes a pre-commit write hook"
                        .to_string(),
                )
                .into());
            }

            if acceleration
                .params
                .get("on_refresh_sort_columns")
                .map(String::as_str)
                .map(str::trim)
                .is_some_and(|sort_columns| !sort_columns.is_empty())
            {
                return Err(DataFusionError::NotImplemented(
                    "DuckDB on_refresh_sort_columns is unavailable because datafusion-table-providers 0.11 no longer exposes a pre-commit write hook"
                        .to_string(),
                )
                .into());
            }
        }

        Ok(create_table_provider(&self.duckdb_factory, &cmd).await?)
    }

    fn prefix(&self) -> &'static str {
        "duckdb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }

    fn supports_snapshot_reload(&self) -> bool {
        false
    }

    /// Reloads the `DuckDB`-backed table provider from the snapshot file
    /// that was just written to the primary path.
    ///
    /// Drops the previous provider, evicts the cached connection pool from
    /// the upstream `DuckDBTableProviderFactory` registry, and then re-runs
    /// the registry factory to build a fresh provider over the on-disk file.
    /// The pool eviction is required because the registry caches pool
    /// instances by file path; without it, the freshly built provider would
    /// reuse the prior pool's open connections — which keep observing the
    /// previous file inode — and queries would continue to return stale data
    /// even after the file has been atomically replaced on disk.
    async fn reload_from_snapshot(
        &self,
        _source: &dyn AccelerationSource,
        previous_provider: Arc<dyn TableProvider>,
        _provider_factory: super::ReloadProviderFactory,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        drop(previous_provider);
        Err("DuckDB snapshot reload is unavailable because datafusion-table-providers 0.11 no longer exposes safe connection-pool invalidation".into())
    }

    async fn drop_table(
        &self,
        table_name: &str,
        source: &dyn AccelerationSource,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::new(self.get_shared_pool(source).await?);
        let table_name = table_name.to_owned();

        tokio::task::spawn_blocking(
            move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                let mut conn = pool.connect_sync()?;
                let duckdb_conn = DuckDB::duckdb_conn(&mut conn).boxed()?;
                let escaped = table_name.replace('"', "\"\"");
                let drop_sql = format!("DROP TABLE IF EXISTS \"{escaped}\"");
                duckdb_conn
                    .get_underlying_conn_mut()
                    .execute(&drop_sql, [])
                    .boxed()?;
                // Also drop any internal DuckDB tables associated with this table
                let internal_name = format!("__data_{table_name}").replace('"', "\"\"");
                let internal_drop = format!("DROP TABLE IF EXISTS \"{internal_name}\"");
                let _ = duckdb_conn
                    .get_underlying_conn_mut()
                    .execute(&internal_drop, []);
                tracing::info!(
                    "Dropped DuckDB table '{table_name}' for schema recreation (file_update mode)"
                );
                Ok(())
            },
        )
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
    }
}

fn apply_changes_refresh_write_defaults(cmd: &mut CreateExternalTable, is_changes_refresh: bool) {
    if is_changes_refresh && !cmd.options.contains_key("recompute_statistics_on_write") {
        cmd.options.insert(
            "recompute_statistics_on_write".to_string(),
            "false".to_string(),
        );
    }
}

pub(crate) async fn create_table_provider(
    duckdb_factory: &DuckDBTableProviderFactory,
    cmd: &CreateExternalTable,
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
    let ctx = SessionContext::new();

    let table_provider = duckdb_factory
        .create(&ctx.state(), cmd)
        .await
        .context(UnableToCreateTableSnafu)
        .boxed()?;

    let Some(duckdb_writer) = table_provider.as_any().downcast_ref::<DuckDBTableWriter>() else {
        unreachable!("DuckDBTableWriter should be returned from DuckDBTableProviderFactory")
    };

    let read_provider = Arc::clone(&duckdb_writer.read_provider);
    let duckdb_writer: Arc<DuckDBTableWriter> = Arc::new(duckdb_writer.clone());

    // Wrap with upsert deduplication if needed
    let write_provider = upsert_dedup::wrap_with_upsert_dedup_if_needed(
        duckdb_writer,
        &cmd.options,
        cmd.constraints.clone(),
    );
    let write_provider = guard_unique_index_overwrites(write_provider, cmd);

    let mut schema_metadata = HashMap::new();
    schema_metadata.insert(
        SPICE_ACCELERATOR_METADATA_KEY.to_string(),
        "duckdb".to_string(),
    );

    let agg_pushdown_optimization = cmd
        .options
        .get("optimizer_duckdb_aggregate_pushdown")
        .map_or("disabled", |v| v.as_str())
        .to_lowercase();

    schema_metadata.insert(
        SPICE_OPT_DUCKDB_AGG_PUSHDOWN_KEY.to_string(),
        agg_pushdown_optimization,
    );

    let table_provider = Arc::new(PolyTableProvider::new_with_schema_metadata(
        write_provider,
        read_provider,
        schema_metadata,
    ));

    Ok(table_provider)
}

fn guard_unique_index_overwrites(
    write_provider: Arc<dyn TableProvider>,
    cmd: &CreateExternalTable,
) -> Arc<dyn TableProvider> {
    let unique_indexes = duckdb_unique_index_columns(cmd);
    if unique_indexes.is_empty() {
        write_provider
    } else {
        Arc::new(DuckDBUniqueIndexGuardTableProvider {
            inner: write_provider,
            unique_indexes: Arc::from(unique_indexes),
        })
    }
}

fn duckdb_unique_index_columns(cmd: &CreateExternalTable) -> Vec<Vec<String>> {
    let Some(indexes) = cmd.options.get("indexes") else {
        return Vec::new();
    };

    datafusion_table_providers::util::hashmap_from_option_string::<String, IndexType>(indexes)
        .into_iter()
        .filter_map(|(columns, index_type)| {
            if index_type != IndexType::Unique {
                return None;
            }
            ColumnReference::try_from(columns.as_str())
                .ok()
                .map(|columns| columns.iter().map(str::to_string).collect())
        })
        .collect()
}

#[derive(Debug, Clone)]
struct DuckDBUniqueIndexGuardTableProvider {
    inner: Arc<dyn TableProvider>,
    unique_indexes: Arc<[Vec<String>]>,
}

#[async_trait]
impl TableProvider for DuckDBUniqueIndexGuardTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: datafusion::logical_expr::dml::InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let input = if overwrite == datafusion::logical_expr::dml::InsertOp::Overwrite {
            let input = Arc::new(CoalescePartitionsExec::new(input)) as Arc<dyn ExecutionPlan>;
            Arc::new(UniqueIndexValidationExec::try_new(
                input,
                Arc::clone(&self.unique_indexes),
            )?) as Arc<dyn ExecutionPlan>
        } else {
            input
        };
        self.inner.insert_into(state, input, overwrite).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.update(state, assignments, filters).await
    }

    async fn truncate(
        &self,
        state: &dyn Session,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.truncate(state).await
    }
}

#[derive(Debug)]
struct UniqueIndexValidationExec {
    input: Arc<dyn ExecutionPlan>,
    unique_index_names: Arc<[Vec<String>]>,
    unique_index_indices: Arc<[Vec<usize>]>,
    properties: Arc<PlanProperties>,
}

impl UniqueIndexValidationExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        unique_index_names: Arc<[Vec<String>]>,
    ) -> datafusion::common::Result<Self> {
        let schema = input.schema();
        let unique_index_indices = unique_index_names
            .iter()
            .map(|columns| {
                columns
                    .iter()
                    .map(|column| {
                        schema.index_of(column).map_err(|error| {
                            DataFusionError::Plan(format!(
                                "Failed to validate DuckDB unique index column {column}: {error}"
                            ))
                        })
                    })
                    .collect::<datafusion::common::Result<Vec<_>>>()
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;

        Ok(Self {
            properties: Arc::clone(input.properties()),
            input,
            unique_index_names,
            unique_index_indices: Arc::from(unique_index_indices),
        })
    }
}

impl DisplayAs for UniqueIndexValidationExec {
    fn fmt_as(
        &self,
        display_format: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match display_format {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => write!(f, "UniqueIndexValidationExec"),
        }
    }
}

impl ExecutionPlan for UniqueIndexValidationExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut children = children.into_iter();
        let Some(input) = children.next() else {
            return Err(DataFusionError::Internal(
                "UniqueIndexValidationExec expected one child but received none".to_string(),
            ));
        };
        if children.next().is_some() {
            return Err(DataFusionError::Internal(
                "UniqueIndexValidationExec expected exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::try_new(
            input,
            Arc::clone(&self.unique_index_names),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = self.input.schema();
        let unique_index_indices = Arc::clone(&self.unique_index_indices);
        let stream = futures::stream::try_unfold(
            UniqueIndexValidationState::Collecting {
                input,
                unique_index_indices,
                schema: Arc::clone(&schema),
            },
            |state| async move {
                match state {
                    UniqueIndexValidationState::Collecting {
                        mut input,
                        unique_index_indices,
                        schema,
                    } => {
                        let mut batches = Vec::new();
                        while let Some(batch) = input.next().await {
                            batches.push(batch?);
                        }
                        validate_unique_index_batches(&batches, &unique_index_indices, &schema)?;

                        let mut batches = batches.into_iter();
                        Ok(batches
                            .next()
                            .map(|batch| (batch, UniqueIndexValidationState::Emitting { batches })))
                    }
                    UniqueIndexValidationState::Emitting { mut batches } => Ok(batches
                        .next()
                        .map(|batch| (batch, UniqueIndexValidationState::Emitting { batches }))),
                }
            },
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

enum UniqueIndexValidationState {
    Collecting {
        input: SendableRecordBatchStream,
        unique_index_indices: Arc<[Vec<usize>]>,
        schema: arrow::datatypes::SchemaRef,
    },
    Emitting {
        batches: std::vec::IntoIter<arrow::record_batch::RecordBatch>,
    },
}

fn validate_unique_index_batches(
    batches: &[arrow::record_batch::RecordBatch],
    unique_index_indices: &[Vec<usize>],
    schema: &arrow::datatypes::SchemaRef,
) -> datafusion::common::Result<()> {
    for index_columns in unique_index_indices {
        let mut seen = HashSet::new();
        for batch in batches {
            for row_index in 0..batch.num_rows() {
                let values = index_columns
                    .iter()
                    .map(|column_index| {
                        ScalarValue::try_from_array(batch.column(*column_index), row_index)
                    })
                    .collect::<datafusion::common::Result<Vec<_>>>()?;

                if values.iter().any(ScalarValue::is_null) {
                    continue;
                }

                if !seen.insert(values) {
                    let columns = index_columns
                        .iter()
                        .map(|column_index| schema.field(*column_index).name().as_str())
                        .join(", ");
                    return Err(DataFusionError::Execution(format!(
                        "Duplicate values detected for DuckDB unique index on ({columns}); aborting overwrite to preserve existing data"
                    )));
                }
            }
        }
    }

    Ok(())
}

register_data_accelerator!(Engine::DuckDB, DuckDBAccelerator);

fn normalize_schema_for_duckdb(cmd: &mut CreateExternalTable) -> datafusion::common::Result<()> {
    use datafusion::common::ToDFSchema;
    let arrow_schema = cmd.schema.as_arrow();
    let normalized =
        arrow_tools::type_rewrite::apply_rules(arrow_schema, DUCKDB_TYPE_REWRITE_RULES);
    if normalized != *arrow_schema {
        cmd.schema = ToDFSchema::to_dfschema_ref(Arc::new(normalized))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::component::dataset::builder::DatasetBuilder;
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray, TimestampSecondArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{
        common::{Constraints, TableReference, ToDFSchema},
        execution::context::SessionContext,
        logical_expr::{CreateExternalTable, cast, col, dml::InsertOp, lit},
        physical_plan::collect,
        scalar::ScalarValue,
    };
    use datafusion_table_providers::util::test::MockExec;

    use crate::component::dataset::acceleration::Acceleration;
    use crate::component::dataset::acceleration::{Engine, Mode};
    use crate::dataaccelerator::{AccelerationSource, DataAccelerator, duckdb::DuckDBAccelerator};

    fn external_table_with_options(options: HashMap<String, String>) -> CreateExternalTable {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let df_schema = ToDFSchema::to_dfschema_ref(schema)
            .expect("to convert Arrow schema to DataFusion schema");

        CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("write_settings_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            or_replace: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        }
    }

    async fn dataset_with_acceleration(
        name: &str,
        acceleration: Acceleration,
    ) -> crate::component::dataset::Dataset {
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new(name.to_string(), name)
            .expect("to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("to build dataset");

        dataset.acceleration = Some(acceleration);
        dataset
    }

    async fn duckdb_create_external_table_error(acceleration: Acceleration) -> String {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema))
            .expect("to convert Arrow schema to DataFusion schema");
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("unsupported_config_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            or_replace: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };
        let dataset = dataset_with_acceleration("unsupported_config_table", acceleration).await;

        let result = DuckDBAccelerator::new()
            .create_external_table(
                external_table,
                Some(&dataset as &dyn AccelerationSource),
                vec![],
                None,
            )
            .await;

        match result {
            Ok(_) => panic!("unsupported DuckDB acceleration config should fail"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn duckdb_write_settings_changes_refresh_disables_recompute_statistics_by_default() {
        let mut external_table = external_table_with_options(HashMap::new());

        super::apply_changes_refresh_write_defaults(&mut external_table, true);

        assert_eq!(
            external_table.options.get("recompute_statistics_on_write"),
            Some(&"false".to_string())
        );
    }

    #[test]
    fn duckdb_write_settings_changes_refresh_preserves_explicit_recompute_statistics_setting() {
        let mut options = HashMap::new();
        options.insert(
            "recompute_statistics_on_write".to_string(),
            "true".to_string(),
        );
        let mut external_table = external_table_with_options(options);

        super::apply_changes_refresh_write_defaults(&mut external_table, true);

        assert_eq!(
            external_table.options.get("recompute_statistics_on_write"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn duckdb_write_settings_non_changes_refresh_keeps_recompute_statistics_unset() {
        let mut external_table = external_table_with_options(HashMap::new());

        super::apply_changes_refresh_write_defaults(&mut external_table, false);

        assert!(
            !external_table
                .options
                .contains_key("recompute_statistics_on_write")
        );
    }

    #[tokio::test]
    async fn retention_sql_configuration_is_rejected() {
        let error_msg = duckdb_create_external_table_error(Acceleration {
            engine: Engine::DuckDB,
            retention_sql: Some("DELETE FROM unsupported_config_table WHERE value < 5".to_string()),
            ..Default::default()
        })
        .await;

        assert!(
            error_msg.contains("DuckDB retention_sql is unavailable"),
            "expected retention_sql unsupported error, got: {error_msg}"
        );
    }

    #[tokio::test]
    async fn overwrite_index_failure_keeps_previous_duckdb_view() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema))
            .expect("to convert Arrow schema to DataFusion schema");

        let mut options = HashMap::new();
        options.insert("indexes".to_string(), "value:unique".to_string());

        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("indexed_overwrite_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            or_replace: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };

        let duckdb_accelerator = DuckDBAccelerator::new();
        let table =
            super::create_table_provider(&duckdb_accelerator.duckdb_factory, &external_table)
                .await
                .expect("table should be created");

        let write_ctx = SessionContext::new();
        let initial_input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .expect("to create initial RecordBatch");
        let initial_exec = Arc::new(MockExec::new(vec![Ok(initial_input)], Arc::clone(&schema)));
        let initial_insert = table
            .insert_into(&write_ctx.state(), initial_exec, InsertOp::Overwrite)
            .await
            .expect("to create initial insert plan");
        collect(initial_insert, write_ctx.task_ctx())
            .await
            .expect("initial overwrite should succeed");

        let duplicate_input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![3, 3]))],
        )
        .expect("to create duplicate RecordBatch");
        let duplicate_exec = Arc::new(MockExec::new(
            vec![Ok(duplicate_input)],
            Arc::clone(&schema),
        ));
        let duplicate_insert = table
            .insert_into(&write_ctx.state(), duplicate_exec, InsertOp::Overwrite)
            .await
            .expect("to create duplicate insert plan");
        let duplicate_result = collect(duplicate_insert, write_ctx.task_ctx()).await;
        assert!(
            duplicate_result.is_err(),
            "duplicate unique-index overwrite should fail"
        );

        let read_ctx = SessionContext::new();
        let scan_plan = table
            .scan(&read_ctx.state(), None, &[], None)
            .await
            .expect("to create scan plan");
        let batches = collect(scan_plan, read_ctx.task_ctx())
            .await
            .expect("to execute scan");

        let mut values = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("to downcast column to Int64Array");
            values.extend((0..column.len()).map(|idx| column.value(idx)));
        }
        values.sort_unstable();

        assert_eq!(values, vec![1, 2]);
    }

    #[tokio::test]
    async fn retention_sql_fails_with_internal_tables() {
        let error_msg = duckdb_create_external_table_error(Acceleration {
            engine: Engine::DuckDB,
            retention_sql: Some("DELETE FROM taxi_trips WHERE value < 5".to_string()),
            ..Default::default()
        })
        .await;

        assert!(
            error_msg.contains("DuckDB retention_sql is unavailable"),
            "expected retention_sql unsupported error, got: {error_msg}"
        );
    }

    #[tokio::test]
    #[expect(clippy::unreadable_literal)]
    async fn test_round_trip_duckdb() {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("time_in_string", DataType::Utf8, false),
            arrow::datatypes::Field::new(
                "time",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
                false,
            ),
            arrow::datatypes::Field::new("time_int", DataType::Int64, false),
            arrow::datatypes::Field::new(
                "time_with_zone",
                DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Second,
                    Some("Etc/UTC".to_string().into()),
                ),
                false,
            ),
        ]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema))
            .expect("to convert Arrow schema to DataFusion schema");
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            or_replace: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };
        let duckdb_accelerator = DuckDBAccelerator::new();
        let ctx = SessionContext::new();
        let table = duckdb_accelerator
            .create_external_table(external_table, None, vec![], None)
            .await
            .expect("table should be created");

        let arr1 = StringArray::from(vec![
            "1970-01-01",
            "2012-12-01T11:11:11Z",
            "2012-12-01T11:11:12Z",
        ]);
        let arr2 = TimestampSecondArray::from(vec![0, 1354360271, 1354360272]);
        let arr3 = Int64Array::from(vec![0, 1354360271, 1354360272]);
        let arr4 = arrow::compute::cast(
            &arr2,
            &DataType::Timestamp(
                arrow::datatypes::TimeUnit::Second,
                Some("Etc/UTC".to_string().into()),
            ),
        )
        .expect("casting works");
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arr1),
                Arc::new(arr2),
                Arc::new(arr3),
                Arc::new(arr4),
            ],
        )
        .expect("data should be created");

        let exec = Arc::new(MockExec::new(vec![Ok(data)], schema));

        let insertion = table
            .insert_into(
                &ctx.state(),
                Arc::<MockExec>::clone(&exec),
                InsertOp::Append,
            )
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        let filter = cast(
            col("time_in_string"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        .lt(lit(ScalarValue::TimestampMillisecond(
            Some(1354360272000),
            None,
        )));
        let delete_error = table
            .delete_from(&ctx.state(), vec![filter])
            .await
            .expect_err("DuckDB delete should fail safely while provider DML is unavailable");
        assert!(
            delete_error.to_string().contains("DELETE not supported"),
            "expected DuckDB delete to be unsupported, got: {delete_error}"
        );
    }

    #[tokio::test]
    async fn test_duckdb_file_initialization() {
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new(
            "duckdb_file_accelerator_init".to_string(),
            "duckdb_file_accelerator_init",
        )
        .expect("to create builder")
        .with_app(Arc::new(app))
        .with_runtime(Arc::new(rt))
        .build()
        .expect("to build dataset");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::DuckDB,
            mode: Mode::File,
            ..Default::default()
        });

        let accelerator = DuckDBAccelerator::new();
        assert!(!accelerator.is_initialized(&dataset));

        accelerator
            .init(&dataset)
            .await
            .expect("initialization should be successful");

        assert!(accelerator.is_initialized(&dataset));

        let path = accelerator.file_path(&dataset).expect("path should exist");
        assert!(std::path::Path::new(&path).exists());

        // cleanup
        std::fs::remove_file(&path).expect("file should be removed");
    }

    #[tokio::test]
    async fn test_retention_sql_with_duckdb_accelerator() {
        let error_msg = duckdb_create_external_table_error(Acceleration {
            engine: Engine::DuckDB,
            retention_sql: Some("DELETE FROM retention_test_dataset WHERE value < 5".to_string()),
            ..Default::default()
        })
        .await;

        assert!(
            error_msg.contains("DuckDB retention_sql is unavailable"),
            "expected retention_sql unsupported error, got: {error_msg}"
        );
    }

    #[tokio::test]
    async fn on_refresh_sort_columns_configuration_is_rejected() {
        let mut params = HashMap::new();
        params.insert(
            "on_refresh_sort_columns".to_string(),
            "value DESC".to_string(),
        );

        let error_msg = duckdb_create_external_table_error(Acceleration {
            engine: Engine::DuckDB,
            params,
            ..Default::default()
        })
        .await;

        assert!(
            error_msg.contains("DuckDB on_refresh_sort_columns is unavailable"),
            "expected on_refresh_sort_columns unsupported error, got: {error_msg}"
        );
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/2889>.
    ///
    /// Arrow Dictionary-encoded columns (enums) must be transparently unpacked
    /// to their value types before reaching the `DuckDB` accelerator.
    #[tokio::test]
    async fn test_duckdb_dictionary_type_round_trip() {
        use arrow::array::StringDictionaryBuilder;
        use arrow::datatypes::Int32Type;

        // Build a schema containing a Dictionary(Int32, Utf8) column.
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "status",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
        ]));

        // Normalize the schema (Dictionary -> Utf8).
        let accel_schema = Arc::new(arrow_tools::type_rewrite::normalize_dictionary_types(
            &source_schema,
        ));
        assert_eq!(accel_schema.field(1).data_type(), &DataType::Utf8);

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&accel_schema)).expect("df schema");

        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("dict_test"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            or_replace: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };

        let duckdb_accelerator = DuckDBAccelerator::new();
        let table = duckdb_accelerator
            .create_external_table(external_table, None, vec![], None)
            .await
            .expect("DuckDB table with normalized Dictionary types should be created");

        // Build a record batch with Dictionary-encoded data.
        let ids = Int64Array::from(vec![1, 2, 3]);
        let mut status_builder = StringDictionaryBuilder::<Int32Type>::new();
        status_builder.append_value("active");
        status_builder.append_value("inactive");
        status_builder.append_value("active");
        let statuses = status_builder.finish();

        let data = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![Arc::new(ids), Arc::new(statuses)],
        )
        .expect("record batch");

        // Cast from Dictionary to the normalized schema before inserting.
        let casted = arrow_tools::record_batch::try_cast_to(data, Arc::clone(&accel_schema))
            .expect("cast Dictionary to Utf8");

        let exec = MockExec::new(vec![Ok(casted)], accel_schema);
        let ctx = SessionContext::new();

        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion of Dictionary data into DuckDB should succeed");

        let result = collect(insertion, ctx.task_ctx())
            .await
            .expect("insert should succeed");

        assert!(!result.is_empty());

        // Verify the data can be read back.
        let scan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan should succeed");

        let batches = collect(scan, ctx.task_ctx())
            .await
            .expect("should read back data");

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3, "should have 3 rows");
    }

    /// Tests that the DROP TABLE SQL used by `drop_table` correctly removes a table.
    #[tokio::test]
    async fn test_drop_table_sql_removes_table() {
        use datafusion_table_providers::duckdb::DuckDB;
        use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;

        let pool =
            Arc::new(DuckDbConnectionPool::new_memory().expect("to create DuckDB connection pool"));
        let mut conn = pool.connect_sync().expect("to get connection from pool");
        let duckdb_conn = DuckDB::duckdb_conn(&mut conn).expect("to get DuckDB connection");

        let table_name = "drop_test_table";
        let underlying = duckdb_conn.get_underlying_conn_mut();

        // Create a table
        underlying
            .execute(
                &format!("CREATE TABLE \"{table_name}\" (id INTEGER, name VARCHAR)"),
                [],
            )
            .expect("create table should succeed");

        // Insert data
        underlying
            .execute(
                &format!("INSERT INTO \"{table_name}\" VALUES (1, 'alice')"),
                [],
            )
            .expect("insert should succeed");

        // Verify table exists
        let count: i32 = underlying
            .query_row(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
                |row| row.get(0),
            )
            .expect("to query table count");
        assert_eq!(count, 1, "table should exist before drop");

        // Execute the same DROP TABLE SQL that drop_table() uses
        underlying
            .execute(&format!("DROP TABLE IF EXISTS \"{table_name}\""), [])
            .expect("drop should succeed");

        // Verify the table is gone
        let count: i32 = underlying
            .query_row(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
                |row| row.get(0),
            )
            .expect("to query table count");
        assert_eq!(count, 0, "table should not exist after drop");

        // Verify DROP IF EXISTS on non-existent table doesn't error
        underlying
            .execute(&format!("DROP TABLE IF EXISTS \"{table_name}\""), [])
            .expect("drop of non-existent table should succeed");
    }

    #[test]
    fn storage_profile_drives_setup_pragmas() {
        use crate::dataaccelerator::storage::ResolvedAccelerationStorage;

        // EBS bumps the checkpoint threshold to amortize remote-disk writes.
        let ebs = DuckDBAccelerator::storage_setup_queries(ResolvedAccelerationStorage::Ebs);
        assert!(
            ebs.iter().any(|q| q.contains("checkpoint_threshold")),
            "EBS profile should tune checkpoint_threshold, got {ebs:?}"
        );

        // Tmpfs also raises checkpoint threshold (volatile, RAM-backed).
        let tmpfs = DuckDBAccelerator::storage_setup_queries(ResolvedAccelerationStorage::Tmpfs);
        assert!(
            tmpfs.iter().any(|q| q.contains("checkpoint_threshold")),
            "Tmpfs profile should tune checkpoint_threshold, got {tmpfs:?}"
        );

        // Local SSD and Unknown keep DuckDB defaults.
        assert!(
            DuckDBAccelerator::storage_setup_queries(ResolvedAccelerationStorage::LocalSsd)
                .is_empty()
        );
        assert!(
            DuckDBAccelerator::storage_setup_queries(ResolvedAccelerationStorage::Unknown)
                .is_empty()
        );
    }

    fn cmd_with_schema(schema: Arc<Schema>) -> CreateExternalTable {
        let df_schema = ToDFSchema::to_dfschema_ref(schema).expect("valid schema");
        CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("t"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            or_replace: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::default(),
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        }
    }

    #[test]
    fn normalize_schema_for_duckdb_null_to_int32() {
        let mut cmd = cmd_with_schema(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("untyped", DataType::Null, true),
            Field::new("name", DataType::Utf8, true),
        ])));
        super::normalize_schema_for_duckdb(&mut cmd).expect("normalize succeeds");
        let arrow = cmd.schema.as_arrow();
        assert_eq!(
            arrow.field_with_name("id").expect("id field").data_type(),
            &DataType::Int64
        );
        assert_eq!(
            arrow
                .field_with_name("untyped")
                .expect("untyped field")
                .data_type(),
            &DataType::Int32
        );
        assert_eq!(
            arrow
                .field_with_name("name")
                .expect("name field")
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn normalize_schema_for_duckdb_interval_to_month_day_nano() {
        use datafusion::arrow::datatypes::IntervalUnit;
        let mut cmd = cmd_with_schema(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("dur", DataType::Interval(IntervalUnit::YearMonth), true),
        ])));
        super::normalize_schema_for_duckdb(&mut cmd).expect("normalize succeeds");
        let arrow = cmd.schema.as_arrow();
        assert_eq!(
            arrow.field_with_name("dur").expect("dur field").data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn normalize_schema_for_duckdb_is_noop_when_no_rules_match() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut cmd = cmd_with_schema(Arc::clone(&schema));
        let schema_before = Arc::clone(&cmd.schema);
        super::normalize_schema_for_duckdb(&mut cmd).expect("normalize succeeds");
        assert!(
            Arc::ptr_eq(&schema_before, &cmd.schema),
            "schema Arc should be unchanged when no rules match"
        );
    }
}
