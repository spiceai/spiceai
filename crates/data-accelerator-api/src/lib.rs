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

use crate::snapshots::CayenneSnapshotValidationError;
use ::arrow::datatypes::SchemaRef;
use arrow_tools::type_rewrite::TypeRewriteRules;
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
use runtime_acceleration::sidecar::{AcceleratorSidecar, OpenOption};
use runtime_acceleration::snapshot::AccelerationLayout;
use runtime_checkpoint_api::CheckpointError;
use runtime_parameters::ParameterSpec;
use runtime_parameters::Parameters;
use runtime_secrets::{ExposeSecret, Secrets, get_params_with_secrets};
use runtime_table_partition::expression::{PartitionedBy, partition_by_expressions};
use snafu::prelude::*;
use std::path::PathBuf;
use std::{any::Any, collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub mod snapshots;
pub mod storage;
pub mod swappable;
pub mod types;
pub mod upsert_dedup;

/// Base directory Spice stores accelerator data under (`<cwd>/.spice/data`).
///
/// Lives here so an engine below `runtime` can resolve it without an upward
/// dependency; `runtime` re-exports it.
#[must_use]
pub fn spice_data_base_path() -> String {
    let Ok(working_dir) = std::env::current_dir() else {
        return ".".to_string();
    };

    let base_folder = working_dir.join(".spice/data");
    base_folder.to_str().unwrap_or(".").to_string()
}

/// Creates [`spice_data_base_path`] if it does not already exist, so a file-mode
/// engine can open its database under it.
///
/// # Errors
///
/// Returns the underlying [`std::io::Error`] when the directory cannot be created.
pub fn make_spice_data_directory() -> std::io::Result<()> {
    std::fs::create_dir_all(spice_data_base_path())
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

/// The accelerator engines this build actually linked, as the names a user writes in
/// `acceleration.engine`, sorted and de-duplicated.
///
/// Reads the registration slice rather than a hand-maintained list, so a build that
/// omits an engine crate cannot advertise it. `Engine::Arrow` and
/// `Engine::PartitionedArrow` both spell "arrow", which is why this de-duplicates.
#[must_use]
pub fn registered_engine_names() -> Vec<String> {
    let mut names: Vec<String> = DATA_ACCELERATOR_REGISTRATIONS
        .iter()
        .map(|registration| registration.engine.to_string())
        .collect();
    names.sort();
    names.dedup();
    names
}

/// [`registered_engine_names`] as a user-facing list — `"arrow, duckdb, and sqlite"`.
///
/// Separate from the message that embeds it so the wording can be asserted directly;
/// the link order of the registration slice is not stable, hence the sort above.
#[must_use]
pub fn registered_engine_list() -> String {
    format_engine_list(&registered_engine_names())
}

fn format_engine_list(names: &[String]) -> String {
    match names {
        [] => "none — this build links no accelerator engine".to_string(),
        [only] => only.clone(),
        [first, second] => format!("{first} and {second}"),
        [rest @ .., last] => format!("{}, and {last}", rest.join(", ")),
    }
}

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

        // No lock is held over the expansion: `Parameters::try_new` below
        // takes the same lock for its autoload pass, and tokio's `RwLock` is
        // write-preferring, so nesting the two would deadlock as soon as a
        // writer queued between them.
        let params_with_secrets =
            get_params_with_secrets(Arc::clone(&secrets), &acceleration_settings.params).await;

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

    /// The type rewrites this engine always applies to a schema when it creates a
    /// table, because the storage format cannot represent the incoming type.
    ///
    /// These are *not* schema drift: the accelerated table holding the rewritten type
    /// is the engine working as designed, so a writer comparing the incoming schema
    /// against the accelerated one must normalize with these rules first. Reporting
    /// such a difference as a stale acceleration points operators at
    /// `on_schema_change`, which cannot change what the engine is able to store.
    ///
    /// The default is "no rewrites", which is correct for any engine that stores the
    /// incoming types verbatim.
    fn type_rewrite_rules(&self) -> TypeRewriteRules {
        &[]
    }

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
    ) -> Result<BootstrapStatus, Box<dyn std::error::Error + Send + Sync>> {
        Ok(BootstrapStatus::none())
    }

    /// This engine's sidecar tables for `source` — the `spice_sys_*` metadata the
    /// runtime keeps beside the accelerated data (CDC stream positions, the dataset
    /// schema checkpoint, the caching engine's fetch marker).
    ///
    /// The runtime calls this instead of naming a concrete accelerator to borrow its
    /// connection pool, which is what keeps the engine crates off `runtime`'s
    /// dependency graph.
    ///
    /// `registry` is passed rather than captured because one engine's sidecar can live
    /// in another engine's database: a Cayenne accelerator configured with
    /// `cayenne_metastore: turso` must take the *`Turso`* accelerator's path-keyed pool
    /// for `cayenne.db`, since the lock serializing sidecar DDL against concurrent
    /// writes lives on that pool instance — a pool of its own would hold a lock no
    /// other sidecar observes.
    ///
    /// Deliberately has no default: an engine that hosts nothing must say so with
    /// [`runtime_acceleration::sidecar::unsupported_sidecar`], because a defaulted
    /// no-op would silently disable checkpointing for it.
    async fn sidecar(
        &self,
        source: &dyn AccelerationSource,
        registry: Arc<AcceleratorEngineRegistry>,
        open_option: OpenOption,
    ) -> Result<Arc<dyn AcceleratorSidecar>, CheckpointError>;

    /// Seeds this engine's adaptive-tuning knobs for a catalog, whose tables are
    /// configured before any of them exists.
    ///
    /// `tuning` is the operator's raw `tuning` parameter, interpreted by the engine
    /// because it owns the vocabulary; `data_path` and `metastore_path` are the
    /// directories to probe. A catalog has no schema inference, so the seed comes from
    /// the host alone — which is precisely why it must come from the engine, and why an
    /// engine with no adaptive controller returns the default outcome and keeps its
    /// static values.
    async fn adaptive_tuning_seeds(
        &self,
        _tuning: Option<&str>,
        _data_path: &str,
        _metastore_path: &str,
    ) -> AdaptiveTuningOutcome {
        AdaptiveTuningOutcome::default()
    }

    /// How this engine's writes accumulate for `acceleration`, or `None` when the engine
    /// is not the one that acceleration names.
    ///
    /// `unset_refresh_mode` is what an absent `refresh_mode` resolves to for the source's
    /// connector, which the caller resolves because only it knows the `from:` value (see
    /// `runtime_acceleration::acceleration::unset_refresh_mode_for_connector`).
    ///
    /// Asked of the engine rather than recomputed by the runtime so the budget cannot
    /// disagree with the tables it is budgeting for: the same classification configures
    /// the table itself.
    fn spicepod_write_profile(
        &self,
        _acceleration: &spicepod::acceleration::Acceleration,
        _unset_refresh_mode: runtime_acceleration::acceleration::RefreshMode,
    ) -> Option<SpicepodWriteProfile> {
        None
    }

    /// The identity of the store this acceleration shares with other datasets, when the
    /// engine keeps one — Cayenne's resolved metadata directory.
    ///
    /// Datasets that resolve to the same key share snapshot state, so they must agree on
    /// whether snapshots are enabled; [`validate_snapshot_consistency`] checks that
    /// before any of them loads. The key is the engine's own resolution rule, which is
    /// why it is asked for here rather than recomputed by the caller.
    ///
    /// Defaults to `None`: an engine whose datasets share no store has nothing to agree
    /// about.
    fn shared_store_key(&self, _acceleration: &Acceleration) -> Option<String> {
        None
    }

    /// This engine's contribution to the coordinated memory budget, summarised from the
    /// pod's configuration *before* initialization.
    ///
    /// The runtime plans that budget (see
    /// [`runtime_acceleration::memory_budget::plan`]) and must know how many distinct
    /// engine instances a pod declares, which only the engine can say: instance identity
    /// follows its own path-resolution rules. Answering here keeps that rule in one
    /// place — a second implementation in the planner could key instances differently
    /// and mis-size every cap.
    ///
    /// Defaults to no contribution, which is correct for an engine whose instances do
    /// not compete for a shared memory ceiling.
    fn memory_budget_inputs(
        &self,
        _app: Option<&Arc<app::App>>,
    ) -> runtime_acceleration::memory_budget::DuckDbBudgetInputs {
        runtime_acceleration::memory_budget::DuckDbBudgetInputs::default()
    }

    /// A sidecar over a database this engine owns at `path`, for state that belongs to
    /// the runtime rather than to a dataset — the Cayenne metastore's `cayenne.db`.
    ///
    /// Distinct from [`Self::sidecar`], which derives the path from a source. The
    /// caller has a path and no source to derive one from, and asks the owning engine
    /// rather than opening the file itself: the lock that serializes sidecar DDL
    /// against a concurrent write lives on the engine's pool instance, so a second
    /// pool over the same file would hold a lock nothing else observes.
    ///
    /// Asking through this method is what keeps one engine from naming another's
    /// concrete type. Defaults to unsupported, which is the true answer for an engine
    /// that keeps no path-keyed databases of its own.
    ///
    /// `dataset_name` names the dataset whose state this sidecar holds — the sidecar
    /// stores it as such and namespaces rows by it. It is NOT a `spice_sys_*` table
    /// name; passing one would file a dataset's checkpoints under a table.
    async fn sidecar_for_path(
        &self,
        path: &str,
        _dataset_name: &str,
    ) -> Result<Arc<dyn AcceleratorSidecar>, CheckpointError> {
        Err(CheckpointError::Store {
            source: format!(
                "the {} accelerator does not host databases of its own, so it cannot open '{path}'",
                self.name()
            )
            .into(),
        })
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

/// Starting values for an engine's adaptive-tuning knobs, derived from the host.
///
/// The controller anchors its bounds to whatever it starts from, so a seed that ignores
/// the host leaves it riding the wrong window. Only the engine can derive these — it
/// probes the storage under its own directories — which is why the caller asks rather
/// than computing them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveTuningSeeds {
    pub compaction_background_interval_ms: u64,
    pub compaction_trigger_files: usize,
    /// The inline-flush caps are `i64` because that is what the engine derives and what
    /// its config takes; converting here would only invite a lossy round trip.
    pub inline_flush_max_rows: i64,
    pub inline_flush_max_segments: i64,
    pub inline_flush_max_bytes: i64,
    pub write_concurrency: usize,
}

/// The outcome of asking an engine to seed adaptive tuning.
#[derive(Debug, Clone, Copy, Default)]
pub struct AdaptiveTuningOutcome {
    /// The operator's `tuning` value was not one the engine recognizes. Reported rather
    /// than corrected so the caller can warn once and carry on with the default.
    pub tuning_value_invalid: bool,
    /// `None` when the operator did not ask for adaptive tuning, in which case the engine
    /// keeps its static defaults.
    pub seeds: Option<AdaptiveTuningSeeds>,
}

/// How an engine's writes accumulate for one acceleration, classified from the Spicepod
/// *before* initialization.
///
/// The runtime sizes thread pools and carves memory from the pod's declared
/// accelerations, which means classifying each one before any component exists. The
/// classification is the engine's own — it decides what a given `refresh_mode` does to
/// its files — while enumerating the component kinds that declare an acceleration is the
/// runtime's. This type is the boundary between the two.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SpicepodWriteProfile {
    /// Writes arrive continuously and land in an in-memory tier first, so the table
    /// demands host memory beyond its files.
    pub uses_cdc_tier: bool,
    /// Files accumulate across writes, so a background compactor has something to
    /// consolidate. A whole-table replace discards what the previous refresh wrote and
    /// therefore does not.
    pub needs_compaction: bool,
    /// Small writes are inlined rather than written straight through, which needs a write
    /// buffer sized per table.
    pub inlines_small_writes: bool,
}

/// Rejects a pod whose datasets share an engine's store but disagree about snapshots.
///
/// An engine that keeps a shared store (Cayenne's `SQLite` metadata catalog) puts every
/// dataset in one metadata directory, and enabling snapshots means that catalog joins the
/// snapshot archive. A pod where some datasets in one directory snapshot and others do not
/// cannot be restored consistently, so it is refused up front rather than at restore time.
///
/// Engine-agnostic: it groups by whatever [`DataAccelerator::shared_store_key`] returns,
/// and an engine that returns `None` — or is simply not linked into this build — takes part
/// in no group and so can never fail this check.
///
/// # Errors
///
/// Returns [`CayenneSnapshotValidationError::InconsistentSnapshotSettings`] naming the
/// directory and both sides of the disagreement.
pub fn validate_snapshot_consistency(
    sources: &[Arc<dyn AccelerationSource>],
) -> Result<(), CayenneSnapshotValidationError> {
    let mut store_groups: HashMap<String, Vec<(String, bool)>> = HashMap::new();

    for source in sources {
        let Some(acceleration) = source.acceleration() else {
            continue;
        };
        let Some(engine) = accelerator_for_engine(acceleration.engine) else {
            continue;
        };
        let Some(store_key) = engine.shared_store_key(acceleration) else {
            continue;
        };

        let snapshots_enabled = !matches!(
            acceleration.snapshot_behavior,
            runtime_acceleration::snapshot::SnapshotBehavior::Disabled
        );
        store_groups
            .entry(store_key)
            .or_default()
            .push((source.name().to_string(), snapshots_enabled));
    }

    for (metadata_dir, datasets) in store_groups {
        if datasets.len() <= 1 {
            continue;
        }

        let enabled: Vec<&str> = datasets
            .iter()
            .filter_map(|(name, enabled)| if *enabled { Some(name.as_str()) } else { None })
            .collect();
        let disabled: Vec<&str> = datasets
            .iter()
            .filter_map(|(name, enabled)| if *enabled { None } else { Some(name.as_str()) })
            .collect();

        if !enabled.is_empty() && !disabled.is_empty() {
            return Err(
                CayenneSnapshotValidationError::InconsistentSnapshotSettings {
                    metadata_dir,
                    enabled_datasets: enabled.join(", "),
                    disabled_datasets: disabled.join(", "),
                },
            );
        }

        // Several datasets sharing the store with snapshots all enabled is supported:
        // each snapshot ships a per-dataset metastore slice, so they cannot clobber one
        // another on extract.
    }

    Ok(())
}

/// The registered accelerator for `engine`, or `None` when this build links none.
fn accelerator_for_engine(engine: Engine) -> Option<Arc<dyn DataAccelerator>> {
    DATA_ACCELERATOR_REGISTRATIONS
        .iter()
        .find(|registration| registration.engine == engine)
        .map(|registration| (registration.constructor)())
}

#[cfg(test)]
mod tests {
    use super::{
        AcceleratorExternalTableBuilder, cayenne_pk_conflict_detection_none, format_engine_list,
        get_primary_keys_from_constraints, upsert_dedup::extract_upsert_options,
    };
    use ::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::{Constraint, Constraints, TableReference};
    use datafusion_table_providers::util::{constraints::UpsertOptions, on_conflict::OnConflict};
    use runtime_acceleration::Engine;
    use runtime_acceleration::acceleration::{Acceleration, Mode};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int32, false),
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Utf8, true),
        ]))
    }

    fn builder(engine: Engine) -> AcceleratorExternalTableBuilder {
        AcceleratorExternalTableBuilder::new(TableReference::bare("orders"), schema(), engine)
    }

    fn pk(columns: Vec<usize>) -> Constraints {
        Constraints::new_unverified(vec![Constraint::PrimaryKey(columns)])
    }

    /// The builder writes the upsert flags into the `CREATE EXTERNAL TABLE`
    /// options and `extract_upsert_options` reads them back out on the
    /// accelerator side. A rename on either side would silently disable
    /// deduplication and let duplicate keys through, so pin the round trip.
    #[test]
    fn upsert_options_survive_the_round_trip_through_table_options() {
        for options in [
            UpsertOptions::default().with_remove_duplicates(true),
            UpsertOptions::default().with_last_write_wins(true),
            UpsertOptions::default()
                .with_remove_duplicates(true)
                .with_last_write_wins(true),
        ] {
            let table = builder(Engine::DuckDB)
                .upsert_options(options.clone())
                .build()
                .expect("build succeeds");

            let extracted = extract_upsert_options(&table.options);
            assert_eq!(
                extracted.remove_duplicates, options.remove_duplicates,
                "remove_duplicates lost in transit"
            );
            assert_eq!(
                extracted.last_write_wins, options.last_write_wins,
                "last_write_wins lost in transit"
            );
        }
    }

    /// With both flags off the builder writes nothing, and the reader must land
    /// on the same "no deduplication" answer.
    #[test]
    fn no_upsert_options_means_no_keys_written_and_none_read_back() {
        let table = builder(Engine::DuckDB).build().expect("build succeeds");

        assert!(!table.options.contains_key("upsert_remove_duplicates"));
        assert!(!table.options.contains_key("upsert_last_write_wins"));

        let extracted = extract_upsert_options(&table.options);
        assert!(!extracted.remove_duplicates);
        assert!(!extracted.last_write_wins);
    }

    /// Constraints carry the primary key the accelerator deduplicates and
    /// upserts on. Dropping them turns every upsert into a blind append.
    #[test]
    fn constraints_reach_the_created_table() {
        let table = builder(Engine::DuckDB)
            .constraints(pk(vec![0, 1]))
            .build()
            .expect("build succeeds");

        assert_eq!(table.constraints, pk(vec![0, 1]));
    }

    #[test]
    fn a_table_built_without_constraints_has_none_rather_than_a_stale_key() {
        let table = builder(Engine::DuckDB).build().expect("build succeeds");
        assert!(table.constraints.is_empty());
    }

    #[test]
    fn the_on_conflict_behavior_reaches_the_created_table() {
        let table = builder(Engine::DuckDB)
            .on_conflict(OnConflict::Upsert(
                datafusion_table_providers::util::column_reference::ColumnReference::try_from("id")
                    .expect("column reference"),
            ))
            .build()
            .expect("build succeeds");

        assert!(
            table.options.contains_key("on_conflict"),
            "on_conflict must be encoded into the table options"
        );
    }

    #[test]
    fn the_write_mode_reaches_the_created_table() {
        let table = builder(Engine::DuckDB)
            .mode(Mode::File)
            .build()
            .expect("build succeeds");

        assert_eq!(table.options.get("mode"), Some(&Mode::File.to_string()));
    }

    /// The Arrow engine is in-memory only. Accepting file mode would produce a
    /// table that silently loses every row on restart.
    #[test]
    fn the_arrow_engine_rejects_file_mode() {
        builder(Engine::Arrow)
            .mode(Mode::File)
            .build()
            .expect_err("file mode must be rejected for the Arrow engine");
        builder(Engine::Arrow)
            .mode(Mode::FileUpdate)
            .build()
            .expect_err("file-update mode must be rejected for the Arrow engine");
        builder(Engine::Arrow)
            .mode(Mode::Memory)
            .build()
            .expect("memory mode is valid for the Arrow engine");
    }

    #[test]
    fn primary_key_columns_are_resolved_to_names_in_declaration_order() {
        assert_eq!(
            get_primary_keys_from_constraints(&pk(vec![0, 1]), &schema()),
            vec!["tenant_id".to_string(), "id".to_string()]
        );
    }

    /// A `UNIQUE` constraint is not a primary key: treating it as one would key
    /// upserts on the wrong columns.
    #[test]
    fn unique_constraints_are_not_reported_as_primary_keys() {
        let constraints = Constraints::new_unverified(vec![Constraint::Unique(vec![2])]);
        assert!(get_primary_keys_from_constraints(&constraints, &schema()).is_empty());
    }

    #[test]
    fn no_constraints_yields_no_primary_keys() {
        assert!(get_primary_keys_from_constraints(&Constraints::default(), &schema()).is_empty());
    }

    /// Turning primary-key conflict detection off is a Cayenne-only escape
    /// hatch. Reading it on another engine would disable a check that engine
    /// still depends on.
    #[test]
    fn pk_conflict_detection_none_is_recognised_only_for_cayenne() {
        for key in ["cayenne_pk_conflict_detection", "pk_conflict_detection"] {
            for value in ["none", "NONE", "None"] {
                let mut acceleration = Acceleration {
                    engine: Engine::Cayenne,
                    ..Acceleration::default()
                };
                acceleration
                    .params
                    .insert((*key).to_string(), (*value).to_string());
                assert!(
                    cayenne_pk_conflict_detection_none(&acceleration),
                    "{key}={value} must disable pk conflict detection"
                );

                let other_engine = Acceleration {
                    engine: Engine::DuckDB,
                    params: acceleration.params.clone(),
                    ..Acceleration::default()
                };
                assert!(
                    !cayenne_pk_conflict_detection_none(&other_engine),
                    "{key}={value} must not apply to a non-Cayenne engine"
                );
            }
        }
    }

    #[test]
    fn pk_conflict_detection_stays_on_for_any_other_value() {
        for value in ["auto", "", "off", "false"] {
            let mut acceleration = Acceleration {
                engine: Engine::Cayenne,
                ..Acceleration::default()
            };
            acceleration.params.insert(
                "cayenne_pk_conflict_detection".to_string(),
                (*value).to_string(),
            );
            assert!(
                !cayenne_pk_conflict_detection_none(&acceleration),
                "{value:?} must leave pk conflict detection on"
            );
        }
    }

    #[test]
    fn pk_conflict_detection_stays_on_when_unconfigured() {
        let acceleration = Acceleration {
            engine: Engine::Cayenne,
            params: HashMap::new(),
            ..Acceleration::default()
        };
        assert!(!cayenne_pk_conflict_detection_none(&acceleration));
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    /// The wording a user reads when they name an engine this build does not have.
    #[test]
    fn engine_list_reads_as_prose() {
        assert_eq!(format_engine_list(&names(&["arrow"])), "arrow");
        assert_eq!(
            format_engine_list(&names(&["arrow", "duckdb"])),
            "arrow and duckdb"
        );
        assert_eq!(
            format_engine_list(&names(&["arrow", "cayenne", "duckdb"])),
            "arrow, cayenne, and duckdb"
        );
    }

    /// A build with no engine linked must not claim an empty set is valid.
    #[test]
    fn engine_list_says_so_when_empty() {
        assert_eq!(
            format_engine_list(&[]),
            "none — this build links no accelerator engine"
        );
    }
}
