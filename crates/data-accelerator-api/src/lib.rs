/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Data-accelerator **contract**.
//!
//! Holds the [`DataAccelerator`] trait, the accelerator registry
//! ([`AcceleratorEngineRegistry`], the `register_data_accelerator!` self-registration
//! slice), the accelerated-table creation flow
//! ([`AcceleratorEngineRegistry::create_accelerator_table`] and
//! [`AcceleratorExternalTableBuilder`]), and the shared table-provider seams
//! ([`swappable`], [`upsert_dedup`]).
//!
//! It names nothing from the `runtime` orchestrator — the trait is abstracted behind
//! [`runtime_acceleration::AccelerationSource`], and secrets/parameters/partitioning are
//! passed in via below-runtime crates — so an accelerator engine (and the `AcceleratedTable`
//! machinery) can implement or consume the contract without depending on `runtime`.

use ::arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::{Constraint, DFSchema};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion::{
    common::{Constraints, TableReference, ToDFSchema},
    datasource::TableProvider,
    logical_expr::CreateExternalTable,
};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, constraints::UpsertOptions, on_conflict::OnConflict,
};
use linkme::distributed_slice;
use runtime_acceleration::Engine;
use runtime_acceleration::acceleration::{self, Acceleration, IndexType, Mode};
use runtime_acceleration::snapshot::AccelerationLayout;
use runtime_parameters::ParameterSpec;
use runtime_parameters::Parameters;
use runtime_secrets::{ExposeSecret, ParamStr, Secrets};
use runtime_table_partition::expression::{PartitionedBy, partition_by_expressions};
use secrecy::SecretString;
use snafu::prelude::*;
use std::path::PathBuf;
use std::{any::Any, collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub mod swappable;
pub mod types;
pub mod upsert_dedup;

/// Base directory Spice stores accelerator data under (`<cwd>/.spice/data`).
///
/// Moved here (from `runtime`) so the builder can name the accelerator data
/// directory without an upward dependency; `runtime` re-exports it.
#[must_use]
pub fn spice_data_base_path() -> String {
    let Ok(working_dir) = std::env::current_dir() else {
        return ".".to_string();
    };

    let base_folder = working_dir.join(".spice/data");
    base_folder.to_str().unwrap_or(".").to_string()
}

pub use runtime_acceleration::BootstrapStatus;
pub use types::{AccelerationSource, AcceleratorEngineRegistry};

#[derive(Clone, Copy)]
pub struct AcceleratorRegistration {
    pub engine: Engine,
    pub constructor: fn() -> Arc<dyn DataAccelerator>,
}

impl AcceleratorRegistration {
    pub const fn new(engine: Engine, constructor: fn() -> Arc<dyn DataAccelerator>) -> Self {
        Self {
            engine,
            constructor,
        }
    }
}

/// Distributed slice that automatically collects all data accelerator registrations at link time
/// via the `linkme` crate. Entries are added using the [`register_data_accelerator!`] macro.
#[distributed_slice]
pub static DATA_ACCELERATOR_REGISTRATIONS: [AcceleratorRegistration] = [..];

/// Registers a data accelerator for a given engine.
///
/// This macro creates a constructor function for the specified accelerator type and
/// registers it in the global distributed slice of data accelerators. This allows
/// the runtime to discover and instantiate accelerators for supported engines.
///
/// # Example (simple form)
///
/// ```ignore
/// register_data_accelerator!(Engine::Foo, FooAccelerator);
/// ```
///
/// # Example (explicit form)
///
/// ```ignore
/// register_data_accelerator!(
///     my_accel_fn,
///     MY_ACCEL_STATIC,
///     Engine::Bar,
///     BarAccelerator
/// );
/// ```
///
/// Using this macro automatically adds the accelerator to the distributed slice,
/// making it available for discovery by the runtime.
#[macro_export]
macro_rules! register_data_accelerator {
    ($fn_name:ident, $static_name:ident, $engine:expr, $accelerator:path) => {
        fn $fn_name() -> ::std::sync::Arc<dyn $crate::DataAccelerator> {
            ::std::sync::Arc::new(<$accelerator>::new())
        }

        #[linkme::distributed_slice($crate::DATA_ACCELERATOR_REGISTRATIONS)]
        pub static $static_name: $crate::AcceleratorRegistration =
            $crate::AcceleratorRegistration::new($engine, $fn_name);
    };

    ($engine:expr, $accelerator:ident) => {
        ::paste::paste! {
            $crate::register_data_accelerator!(
                [<__register_data_accelerator_fn_ $accelerator:snake>],
                [<__REGISTER_DATA_ACCELERATOR_ $accelerator:upper>],
                $engine,
                $accelerator
            );
        }
    };
}

#[derive(Debug, Snafu)]
// Selectors are `pub`: runtime-side engine impls construct e.g. `InvalidConfigurationSnafu` across the crate boundary.
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid configuration: {msg}"))]
    InvalidConfiguration { msg: String },

    #[snafu(display(
        "Unknown acceleration engine '{engine}'. Valid engines are: arrow, duckdb, sqlite, turso, postgres/postgresql, cayenne/vortex. Docs: https://spiceai.org/docs/components/data-accelerators"
    ))]
    UnknownEngine { engine: Arc<str> },

    #[snafu(display("Acceleration creation failed: {source}"))]
    AccelerationCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[derive(Debug, Snafu)]
pub enum FilePathError {
    #[snafu(display("Could not resolve file path. Acceleration is not enabled."))]
    AccelerationNotEnabled,

    #[snafu(display("{engine:?} accelerator engine not available."))]
    AcceleratorEngineUnavailable { engine: Engine },

    #[snafu(display("File mode is not supported for this accelerator engine."))]
    FileModeUnsupported {},

    #[snafu(display("Failed to get file path for {engine} acceleration: {source}"))]
    External {
        engine: Engine,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl AcceleratorEngineRegistry {
    /// Builds the accelerator [`TableProvider`] for a dataset from its acceleration settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the configured engine is unknown or unregistered, or if the
    /// engine fails to create its external table.
    #[expect(clippy::too_many_arguments)]
    pub async fn create_accelerator_table(
        &self,
        table_name: TableReference,
        schema: SchemaRef,
        constraints: Option<&Constraints>,
        acceleration_settings: &acceleration::Acceleration,
        secrets: Arc<RwLock<Secrets>>,
        source: Option<&dyn AccelerationSource>,
        ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>> {
        let engine = acceleration_settings.engine;

        // Normalize Dictionary-encoded types to their value types only for
        // accelerator engines that do not natively support Arrow Dictionary
        // encoding (DuckDB, SQLite, Turso).  Other engines (Arrow, Cayenne,
        // PostgreSQL) handle Dictionary types natively and benefit from the
        // compact encoding.
        let needs_dictionary_normalization = matches!(
            engine.to_unpartitioned(),
            Engine::DuckDB | Engine::Sqlite | Engine::Turso
        );
        let schema = if needs_dictionary_normalization
            && arrow_tools::schema::has_dictionary_types(&schema)
        {
            let normalized = arrow_tools::type_rewrite::normalize_dictionary_types(&schema);
            tracing::debug!(
                "Normalized Arrow Dictionary types in schema for {engine} acceleration"
            );
            Arc::new(normalized)
        } else {
            schema
        };

        let accelerator = self
            .get_accelerator_engine(acceleration_settings.engine)
            .await
            .ok_or_else(|| Error::InvalidConfiguration {
                msg: format!("Unknown engine: {engine}"),
            })?;

        if let Err(e) = acceleration_settings.validate_indexes(&schema) {
            InvalidConfigurationSnafu {
                msg: format!("{e}"),
            }
            .fail()?;
        }

        if let Err(e) = acceleration_settings.validate_primary_key(&schema) {
            InvalidConfigurationSnafu {
                msg: format!("{e}"),
            }
            .fail()?;
        }

        let cloned_secrets = Arc::clone(&secrets);
        let secret_guard = cloned_secrets.read().await;
        let mut params_with_secrets: HashMap<String, SecretString> = HashMap::new();

        // Inject secrets from the user-supplied params.
        // This will replace any instances of `${ store:key }` with the actual secret value.
        for (k, v) in &acceleration_settings.params {
            let secret = secret_guard.inject_secrets(k, ParamStr(v)).await;
            params_with_secrets.insert(k.clone(), secret);
        }

        let params = Parameters::try_new(
            &format!("accelerator {}", accelerator.name()),
            params_with_secrets.into_iter().collect::<Vec<_>>(),
            accelerator.prefix(),
            secrets,
            accelerator.parameters(),
        )
        .await
        .context(AccelerationCreationFailedSnafu)?;

        // Not all acceleration engines support creating tables with schemas so we include the schema as part of the table name.
        // For example, Table {schema: "schema", table: "table_name"} is converted to Table {table: "schema.table_name"}.
        let accelerated_table_name = TableReference::bare(table_name.to_string());

        let mut external_table_builder = AcceleratorExternalTableBuilder::new(
            accelerated_table_name,
            Arc::clone(&schema),
            engine,
        )
        .mode(acceleration_settings.mode)
        .options(params)
        .indexes(acceleration_settings.indexes.clone());
        let suppress_auto_on_conflict = cayenne_pk_conflict_detection_none(acceleration_settings);

        // If there are constraints from the federated table, then add them to the accelerated table
        // For Arrow/MemTable accelerator, on_conflict will be automatically derived from primary key constraints
        if let Some(constraints) = constraints
            && !constraints.is_empty()
        {
            external_table_builder = external_table_builder.constraints(constraints.clone());
            if !suppress_auto_on_conflict {
                let primary_keys: Vec<String> =
                    get_primary_keys_from_constraints(constraints, &schema);
                external_table_builder = external_table_builder
                    .on_conflict(OnConflict::Upsert(ColumnReference::new(primary_keys)));
            }
        }

        if let Some(on_conflict) =
            acceleration_settings
                .on_conflict()
                .map_err(|e| Error::InvalidConfiguration {
                    msg: format!("on_conflict invalid: {e}"),
                })?
        {
            external_table_builder = external_table_builder.on_conflict(on_conflict);
        }

        // Pass UpsertOptions for constraint validation behavior
        external_table_builder =
            external_table_builder.upsert_options(acceleration_settings.upsert_options());

        match acceleration_settings.table_constraints(Arc::clone(&schema)) {
            Ok(Some(constraints)) => {
                if !constraints.is_empty() {
                    external_table_builder =
                        external_table_builder.constraints(constraints.clone());
                    // Update on_conflict to match the new constraints' primary key
                    // if user hasn't explicitly configured on_conflict
                    if acceleration_settings.on_conflict.is_empty() && !suppress_auto_on_conflict {
                        let primary_keys: Vec<String> =
                            get_primary_keys_from_constraints(&constraints, &schema);
                        if !primary_keys.is_empty() {
                            external_table_builder = external_table_builder.on_conflict(
                                OnConflict::Upsert(ColumnReference::new(primary_keys)),
                            );
                        }
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                InvalidConfigurationSnafu {
                    msg: format!("{e}"),
                }
                .fail()?;
            }
        }

        let external_table = external_table_builder.build()?;

        let df_schema = DFSchema::try_from(schema)
            .map_err(|e| Error::AccelerationCreationFailed { source: e.into() })?;

        let partition_by = if acceleration_settings.partition_by.is_empty() {
            vec![]
        } else {
            partition_by_expressions(&acceleration_settings.partition_by, &ctx, &df_schema)
                .map_err(|e| Error::AccelerationCreationFailed { source: e.into() })?
        };

        let table_provider = accelerator
            .create_external_table(
                external_table,
                source,
                partition_by,
                Some(ctx.runtime_env()),
            )
            .await
            .context(AccelerationCreationFailedSnafu)?;

        Ok(table_provider)
    }
}

/// A `DataAccelerator` knows how to read, write and create new tables.
#[async_trait]
pub trait DataAccelerator: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Vec<PartitionedBy>,
        runtime_env: Option<Arc<RuntimeEnv>>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>;

    /// The name of the accelerator
    fn name(&self) -> &'static str;

    /// The prefix of the table name
    fn prefix(&self) -> &'static str;

    /// The parameters of the accelerator
    fn parameters(&self) -> &'static [ParameterSpec];

    /// Returns the storage layout configuration for this accelerator.
    ///
    /// Returns the appropriate `AccelerationLayout` for this engine type:
    /// - File-based accelerators (`DuckDB`, `SQLite`) return `AccelerationLayout::file`
    /// - Directory-based accelerators (Cayenne) return `AccelerationLayout::cayenne`
    ///
    /// This is used for snapshots and size metrics.
    fn acceleration_layout(&self, source: &dyn AccelerationSource) -> AccelerationLayout {
        // Default: use file-based layout if file_path is available
        if let Ok(path) = self.file_path(source) {
            AccelerationLayout::file(PathBuf::from(path))
        } else {
            AccelerationLayout::default()
        }
    }

    /// Initialize the accelerator for a component
    /// Returns `WasBootstrapped::yes()` if the accelerator was initialized from existing data,
    /// `WasBootstrapped::no()` otherwise.
    async fn init(
        &self,
        _source: &dyn AccelerationSource,
        _registry: Arc<AcceleratorEngineRegistry>,
    ) -> Result<BootstrapStatus, Box<dyn std::error::Error + Send + Sync>> {
        Ok(BootstrapStatus::none())
    }

    /// Drops an existing table from the acceleration engine.
    ///
    /// Used by `file_update` mode to remove a table whose schema is incompatible with
    /// the current source, so it can be recreated with the correct schema.
    ///
    /// The default implementation is a no-op; engines that support file-based acceleration
    /// should override this.
    async fn drop_table(
        &self,
        _table_name: &str,
        _source: &dyn AccelerationSource,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    /// Applies a widening schema-evolution plan to an existing table in the acceleration
    /// engine, in place (without dropping the table or its data).
    ///
    /// Implementations must be idempotent: re-applying a plan that was already partially
    /// or fully applied (e.g. after a crash between the engine DDL and the checkpoint
    /// update) must succeed without error, so restart-time re-classification self-heals.
    ///
    /// The default implementation rejects the call; engines that support in-place schema
    /// evolution must override this.
    async fn evolve_table_schema(
        &self,
        _table_name: &str,
        _source: &dyn AccelerationSource,
        _plan: &arrow_tools::schema_evolution::WideningPlan,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err(Box::new(SchemaEvolutionUnsupported {
            engine: self.name(),
        }))
    }

    /// Check if the accelerator is initialized for a component
    fn is_initialized(&self, _source: &dyn AccelerationSource) -> bool {
        true
    }

    /// For file-based accelerators, return the valid file extensions for the file path
    fn valid_file_extensions(&self) -> Vec<&'static str> {
        vec![]
    }

    /// For file-based accelerators, return the file path
    /// For any other accelerator, return None
    ///
    /// # Errors
    ///
    /// Returns an error if the accelerator does not support file mode, or if the engine
    /// cannot resolve a file path for the source.
    fn file_path(&self, _source: &dyn AccelerationSource) -> Result<String, FilePathError> {
        Err(FilePathError::FileModeUnsupported {})
    }

    /// Check if the file path is valid
    fn is_valid_file(&self, source: &dyn AccelerationSource) -> bool {
        if let Ok(path) = self.file_path(source) {
            let path = std::path::Path::new(&path);

            !path.is_dir()
                && path
                    .extension()
                    .is_some_and(|ext| self.valid_file_extensions().iter().any(|&e| e == ext))
        } else {
            false
        }
    }

    /// Check if the file path exists
    fn has_existing_file(&self, source: &dyn AccelerationSource) -> bool {
        if let Ok(path) = self.file_path(source) {
            let path = std::path::Path::new(&path);
            path.is_file()
        } else {
            false
        }
    }

    /// Shutdown the accelerator, performing any necessary cleanup
    /// Default implementation does nothing
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    /// Whether this accelerator supports reloading its data from a freshly
    /// downloaded snapshot file after [`DataAccelerator::init`] has already
    /// produced a [`TableProvider`].
    ///
    /// Returning `true` indicates that [`DataAccelerator::reload_from_snapshot`]
    /// is implemented; in-memory accelerators (e.g. Arrow) and accelerators
    /// without a stable on-disk snapshot format must return `false`.
    fn supports_snapshot_reload(&self) -> bool {
        false
    }

    /// Reload the accelerator from a snapshot file that has already been
    /// downloaded and written to the accelerator's primary path on disk
    /// (i.e. `acceleration_layout(source).primary_path()`).
    ///
    /// The runtime guarantees the per-dataset accelerator write mutex is held
    /// for the duration of this call. Implementations must:
    ///   1. Drop or clear any cached engine state (open connections, pool
    ///      entries, file handles, cached schema views, etc.) holding the
    ///      previous file open.
    ///   2. Invoke `provider_factory` to construct a fresh [`TableProvider`]
    ///      backed by the now-replaced file at the primary path.
    ///
    /// `provider_factory` re-runs the same `create_accelerator_table` flow
    /// used at startup, so the returned provider has the same logical schema,
    /// constraints, and indexes as `previous_provider`.
    ///
    /// The default implementation rejects the call. File-based accelerators
    /// that participate in `refresh_mode: snapshot` must override this.
    async fn reload_from_snapshot(
        &self,
        _source: &dyn AccelerationSource,
        _previous_provider: Arc<dyn TableProvider>,
        _provider_factory: ReloadProviderFactory,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        Err(Box::new(SnapshotReloadUnsupported {
            engine: self.name(),
        }))
    }

    /// Optional engine-specific [`SnapshotEngine`] used for snapshot create/extract.
    ///
    /// Engines that need to customise the on-disk archive contents (e.g.
    /// Cayenne, which ships a per-dataset metastore-slice JSON instead of
    /// the raw `cayenne.db` file) override this to return their engine.
    /// File-based accelerators (`DuckDB` / `SQLite` / `Turso`) return `None` and
    /// the default `SnapshotManager` engine selection applies.
    async fn snapshot_engine_for_source(
        &self,
        _source: &dyn AccelerationSource,
    ) -> Option<Arc<dyn runtime_acceleration::snapshot::engine::SnapshotEngine>> {
        None
    }
}

/// Factory that re-runs the `create_accelerator_table` registry flow to
/// produce a fresh [`TableProvider`] for an already-initialized dataset.
///
/// Used by [`DataAccelerator::reload_from_snapshot`] so engines don't need
/// to re-derive table options, attach databases, write handlers, etc.
pub type ReloadProviderFactory = Arc<
    dyn Fn() -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            Arc<dyn TableProvider>,
                            Box<dyn std::error::Error + Send + Sync>,
                        >,
                    > + Send,
            >,
        > + Send
        + Sync,
>;

/// Error returned by the default [`DataAccelerator::reload_from_snapshot`]
/// implementation when an engine does not support snapshot-based reloads.
#[derive(Debug, Snafu)]
#[snafu(display(
    "Acceleration engine '{engine}' does not support reloading from a snapshot. \
     `refresh_mode: snapshot` requires a snapshot-capable file-based engine \
     (DuckDB, SQLite, Cayenne, or Turso)."
))]
pub struct SnapshotReloadUnsupported {
    pub engine: &'static str,
}

/// Error returned by the default [`DataAccelerator::evolve_table_schema`]
/// implementation when an engine does not support in-place schema evolution.
#[derive(Debug, Snafu)]
#[snafu(display(
    "Acceleration engine '{engine}' does not support in-place schema evolution. \
     The acceleration must be recreated to apply the new schema."
))]
pub struct SchemaEvolutionUnsupported {
    pub engine: &'static str,
}

pub struct AcceleratorExternalTableBuilder {
    table_name: TableReference,
    schema: SchemaRef,
    engine: Engine,
    mode: Mode,
    options: Option<Parameters>,
    indexes: HashMap<ColumnReference, IndexType>,
    constraints: Option<Constraints>,
    on_conflict: Option<OnConflict>,
    upsert_options: UpsertOptions,
}

impl AcceleratorExternalTableBuilder {
    #[must_use]
    pub fn new(table_name: TableReference, schema: SchemaRef, engine: Engine) -> Self {
        Self {
            table_name,
            schema,
            engine,
            mode: Mode::Memory,
            options: None,
            indexes: HashMap::new(),
            constraints: None,
            on_conflict: None,
            upsert_options: UpsertOptions::default(),
        }
    }

    #[must_use]
    pub fn indexes(mut self, indexes: HashMap<ColumnReference, IndexType>) -> Self {
        self.indexes = indexes;
        self
    }

    #[must_use]
    pub fn on_conflict(mut self, on_conflict: OnConflict) -> Self {
        self.on_conflict = Some(on_conflict);
        self
    }

    #[must_use]
    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    #[must_use]
    pub fn options(mut self, options: Parameters) -> Self {
        self.options = Some(options);
        self
    }

    #[must_use]
    pub fn constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = Some(constraints);
        self
    }

    #[must_use]
    pub fn upsert_options(mut self, upsert_options: UpsertOptions) -> Self {
        self.upsert_options = upsert_options;
        self
    }

    fn validate_arrow(&self) -> Result<(), Error> {
        if matches!(self.mode, Mode::File | Mode::FileUpdate) {
            InvalidConfigurationSnafu {
                msg: "File mode not supported for Arrow engine".to_string(),
            }
            .fail()?;
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), Error> {
        match self.engine {
            Engine::Arrow => self.validate_arrow(),
            _ => Ok(()),
        }
    }

    /// Builds the `CREATE EXTERNAL TABLE` command from the accumulated builder state.
    ///
    /// # Errors
    ///
    /// Returns an error if the accumulated acceleration settings are invalid for the
    /// selected engine.
    pub fn build(self) -> Result<CreateExternalTable> {
        self.validate()?;

        let mut options: HashMap<String, String> = self
            .options
            .map(|x| x.to_secret_map())
            .map(|x| {
                x.into_iter()
                    .map(|(k, v)| (k, v.expose_secret().to_string()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();

        options.insert("data_directory".to_string(), spice_data_base_path());

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&self.schema));

        let mode = self.mode;
        options.insert("mode".to_string(), mode.to_string());

        if !self.indexes.is_empty() {
            let indexes_option_str = Acceleration::hashmap_to_option_string(&self.indexes);
            options.insert("indexes".to_string(), indexes_option_str);
        }

        if let Some(on_conflict) = self.on_conflict {
            let on_conflict_str = on_conflict.to_string();
            tracing::debug!("Adding on_conflict to options: {}", on_conflict_str);
            options.insert("on_conflict".to_string(), on_conflict_str);
        }

        // Pass upsert_options as JSON serialized string
        if self.upsert_options.remove_duplicates || self.upsert_options.last_write_wins {
            options.insert(
                "upsert_remove_duplicates".to_string(),
                self.upsert_options.remove_duplicates.to_string(),
            );
            options.insert(
                "upsert_last_write_wins".to_string(),
                self.upsert_options.last_write_wins.to_string(),
            );
        }

        let constraints = match self.constraints {
            Some(constraints) => constraints,
            None => Constraints::new_unverified(vec![]),
        };

        let external_table = CreateExternalTable {
            schema: df_schema.map_err(|e| {
                InvalidConfigurationSnafu {
                    msg: format!("Failed to convert schema: {e}"),
                }
                .build()
            })?,
            name: self.table_name.clone(),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            or_replace: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints,
            column_defaults: HashMap::default(),
            temporary: false,
        };

        Ok(external_table)
    }
}

/// Resolves the on-disk file path for a file-based acceleration source.
///
/// # Errors
///
/// Returns an error if acceleration is not enabled, the engine is unavailable, or the
/// engine does not support file mode.
pub async fn acceleration_file_path(
    source: &dyn AccelerationSource,
    registry: &AcceleratorEngineRegistry,
) -> Result<PathBuf, FilePathError> {
    let acceleration_settings = source.acceleration().context(AccelerationNotEnabledSnafu)?;

    let accelerator = registry
        .get_accelerator_engine(acceleration_settings.engine)
        .await
        .context(AcceleratorEngineUnavailableSnafu {
            engine: acceleration_settings.engine,
        })?;

    let file = accelerator.file_path(source)?;

    Ok(PathBuf::from(file))
}

/// Gets the storage layout for the given acceleration source.
///
/// This function retrieves the registered accelerator for the source's engine
/// and returns the engine-specific layout. Different engines use
/// different layout types:
/// - File-based engines (`DuckDB`, `SQLite`): `AccelerationLayout::file`
/// - Directory-based engines (Cayenne): `AccelerationLayout::cayenne`
///
/// This is used for snapshots and size metrics.
///
/// # Errors
///
/// Returns an error if acceleration is not enabled or the engine is unavailable.
pub async fn get_acceleration_layout(
    source: &dyn AccelerationSource,
    registry: &AcceleratorEngineRegistry,
) -> Result<AccelerationLayout, FilePathError> {
    let acceleration_settings = source.acceleration().context(AccelerationNotEnabledSnafu)?;

    let accelerator = registry
        .get_accelerator_engine(acceleration_settings.engine)
        .await
        .context(AcceleratorEngineUnavailableSnafu {
            engine: acceleration_settings.engine,
        })?;

    Ok(accelerator.acceleration_layout(source))
}

#[must_use]
pub fn get_primary_keys_from_constraints(
    constraints: &Constraints,
    schema: &SchemaRef,
) -> Vec<String> {
    constraints
        .iter()
        .filter_map(|constraint| {
            if let Constraint::PrimaryKey(col_indexes) = constraint {
                Some(
                    col_indexes
                        .iter()
                        .map(|&col_index| schema.field(col_index).name().clone()),
                )
            } else {
                None
            }
        })
        .flatten()
        .collect()
}

#[must_use]
pub fn cayenne_pk_conflict_detection_none(acceleration_settings: &Acceleration) -> bool {
    matches!(acceleration_settings.engine, Engine::Cayenne)
        && ["cayenne_pk_conflict_detection", "pk_conflict_detection"]
            .iter()
            .filter_map(|key| acceleration_settings.params.get(*key))
            .any(|value| value.eq_ignore_ascii_case("none"))
}
