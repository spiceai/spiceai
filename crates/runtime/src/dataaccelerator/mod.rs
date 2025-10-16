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

use crate::component::dataset::acceleration::{self, Acceleration, Engine, IndexType, Mode};
use crate::parameters::ParameterSpec;
use crate::parameters::Parameters;
use crate::secrets::{ExposeSecret, ParamStr, Secrets};
use crate::{Runtime, spice_data_base_path};
use ::arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::{Constraint, DFSchema};
use datafusion::prelude::SessionContext;
use datafusion::{
    common::{Constraints, TableReference, ToDFSchema},
    datasource::TableProvider,
    logical_expr::CreateExternalTable,
};
use datafusion_table_providers::util::constraints::UpsertOptions;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use runtime_table_partition::expression::{PartitionedBy, partition_by_expressions};
use secrecy::SecretString;
use snafu::prelude::*;
use std::path::PathBuf;
use std::{any::Any, collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use self::arrow::ArrowAccelerator;

#[cfg(feature = "duckdb")]
use self::duckdb::DuckDBAccelerator;
#[cfg(feature = "duckdb")]
use self::partitioned_duckdb::PartitionedDuckDBAccelerator;
#[cfg(feature = "postgres")]
use self::postgres::PostgresAccelerator;
#[cfg(feature = "sqlite")]
use self::sqlite::SqliteAccelerator;
#[cfg(feature = "turso")]
use self::turso::TursoAccelerator;
#[cfg(feature = "vortex")]
use self::vortex::VortexAccelerator;

pub mod arrow;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "duckdb")]
pub mod partitioned_duckdb;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "turso")]
pub mod turso;
#[cfg(feature = "vortex")]
pub mod vortex;

mod snapshots;
pub mod spice_sys;

pub(crate) use snapshots::validate_snapshot_paths;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid configuration: {msg}"))]
    InvalidConfiguration { msg: String },

    #[snafu(display("Unknown engine: {engine}"))]
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

#[derive(Default, Clone)]
pub struct AcceleratorEngineRegistry {
    pub accelerator_engine_registry: Arc<RwLock<HashMap<Engine, Arc<dyn DataAccelerator>>>>,
}

impl AcceleratorEngineRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            accelerator_engine_registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get_accelerator_engine(&self, engine: Engine) -> Option<Arc<dyn DataAccelerator>> {
        let guard = self.accelerator_engine_registry.read().await;
        let engine = guard.get(&engine);
        match engine {
            Some(engine_ref) => Some(Arc::clone(engine_ref)),
            None => None,
        }
    }

    async fn register_accelerator_engine(
        &self,
        engine: Engine,
        accelerator_engine: Arc<dyn DataAccelerator>,
    ) {
        let mut registry = self.accelerator_engine_registry.write().await;
        registry.insert(engine, accelerator_engine);
    }

    pub(crate) async fn register_all(&self) {
        self.register_accelerator_engine(Engine::Arrow, Arc::new(ArrowAccelerator::new()))
            .await;
        #[cfg(feature = "duckdb")]
        self.register_accelerator_engine(Engine::DuckDB, Arc::new(DuckDBAccelerator::new()))
            .await;
        #[cfg(feature = "duckdb")]
        self.register_accelerator_engine(
            Engine::PartitionedDuckDB,
            Arc::new(PartitionedDuckDBAccelerator::new()),
        )
        .await;
        #[cfg(feature = "postgres")]
        self.register_accelerator_engine(Engine::PostgreSQL, Arc::new(PostgresAccelerator::new()))
            .await;
        #[cfg(feature = "sqlite")]
        self.register_accelerator_engine(Engine::Sqlite, Arc::new(SqliteAccelerator::new()))
            .await;
        #[cfg(feature = "turso")]
        self.register_accelerator_engine(Engine::Turso, Arc::new(TursoAccelerator::new()))
            .await;
        #[cfg(feature = "vortex")]
        self.register_accelerator_engine(Engine::Vortex, Arc::new(VortexAccelerator::new()))
            .await;
    }

    pub async fn unregister_all(&self) {
        let mut registry = self.accelerator_engine_registry.write().await;
        registry.clear();
    }

    #[allow(clippy::too_many_arguments)]
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

        // If there are constraints from the federated table, then add them to the accelerated table
        // and automatically configure upsert behavior for them. This can be overridden by the user.
        if let Some(constraints) = constraints
            && !constraints.is_empty()
        {
            external_table_builder = external_table_builder.constraints(constraints.clone());
            let primary_keys: Vec<String> = get_primary_keys_from_constraints(constraints, &schema);
            external_table_builder = external_table_builder.on_conflict(OnConflict::Upsert(
                ColumnReference::new(primary_keys),
                UpsertOptions::default(),
            ));
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

        match acceleration_settings.table_constraints(Arc::clone(&schema)) {
            Ok(Some(constraints)) => {
                if !constraints.is_empty() {
                    external_table_builder = external_table_builder.constraints(constraints);
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
            .create_external_table(external_table, source, partition_by)
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
    ///
    /// Also returns the behaviors of the table provider created by the accelerator engine.
    async fn create_external_table(
        &self,
        cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Vec<PartitionedBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>;

    /// The name of the accelerator
    fn name(&self) -> &'static str;

    /// The prefix of the table name
    fn prefix(&self) -> &'static str;

    /// The parameters of the accelerator
    fn parameters(&self) -> &'static [ParameterSpec];

    /// Initialize the accelerator for a component
    async fn init(
        &self,
        _source: &dyn AccelerationSource,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
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

    fn validate_arrow(&self) -> Result<(), Error> {
        if Mode::File == self.mode {
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
            options.insert("on_conflict".to_string(), on_conflict.to_string());
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

/// Represents acceleration source component, such as a dataset or a view.
/// Provides additional information about the source, such as its name and associated runtime information.
pub trait AccelerationSource: Send + Sync {
    /// Returns a clone of the source as an `Arc<dyn AccelerationSource>`
    fn clone_arc(&self) -> Arc<dyn AccelerationSource>;

    /// Returns true if the source uses file-based acceleration
    fn is_file_accelerated(&self) -> bool;

    /// Returns the application associated with this source
    fn app(&self) -> Arc<app::App>;

    /// Returns the runtime associated with this source
    fn runtime(&self) -> Arc<Runtime>;

    /// Returns the acceleration configuration if it exists
    fn acceleration(&self) -> Option<&Acceleration>;

    /// Returns the name of this source
    fn name(&self) -> &TableReference;
}

pub async fn acceleration_file_path(
    source: &dyn AccelerationSource,
) -> Result<PathBuf, FilePathError> {
    let acceleration_settings = source.acceleration().context(AccelerationNotEnabledSnafu)?;

    let accelerator = get_registered_accelerator(source, acceleration_settings.engine)
        .await
        .context(AcceleratorEngineUnavailableSnafu {
            engine: acceleration_settings.engine,
        })?;

    let file = accelerator.file_path(source)?;

    Ok(PathBuf::from(file))
}

fn get_primary_keys_from_constraints(constraints: &Constraints, schema: &SchemaRef) -> Vec<String> {
    constraints
        .iter()
        .filter_map(|constraint| {
            if let Constraint::PrimaryKey(col_indexes) = constraint {
                Some(
                    col_indexes
                        .iter()
                        .map(|&col_index| schema.field(col_index).name().to_string()),
                )
            } else {
                None
            }
        })
        .flatten()
        .collect()
}

async fn get_registered_accelerator(
    source: &dyn AccelerationSource,
    engine: Engine,
) -> Option<Arc<dyn DataAccelerator>> {
    source
        .runtime()
        .accelerator_engine_registry()
        .get_accelerator_engine(engine)
        .await
}

#[cfg(test)]
mod test {
    use ::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[tokio::test]
    #[cfg(feature = "duckdb")]
    async fn test_file_mode_duckdb_creation() {
        use crate::builder::RuntimeBuilder;
        use std::{fs, path::Path};

        let path = "./abc-duckdb.db".to_string();
        let params = HashMap::from([("duckdb_file".to_string(), path.clone())]);
        let runtime = Arc::new(RuntimeBuilder::new().build().await);
        let ctx = Arc::clone(&runtime.df.ctx);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let acceleration_settings = Acceleration {
            params,
            enabled: true,
            mode: Mode::File,
            engine: Engine::DuckDB,
            ..Acceleration::default()
        };
        let _ = runtime
            .accelerator_engine_registry
            .create_accelerator_table(
                "abc".into(),
                schema,
                None,
                &acceleration_settings,
                Arc::new(RwLock::new(Secrets::new())),
                None,
                ctx,
            )
            .await
            .expect("accelerator table created");

        let path = Path::new(&path);
        assert!(path.is_file());
        fs::remove_file(path).expect("file removed");
    }

    #[tokio::test]
    #[cfg(feature = "sqlite")]
    async fn test_file_mode_sqlite_creation() {
        use crate::builder::RuntimeBuilder;
        use std::{fs, path::Path};

        let path = "./abc-sqlite.db".to_string();
        let params = HashMap::from([("sqlite_file".to_string(), path.clone())]);
        let runtime = Arc::new(RuntimeBuilder::new().build().await);
        let ctx = Arc::clone(&runtime.df.ctx);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let acceleration_settings = Acceleration {
            params: params.clone(),
            enabled: true,
            mode: Mode::File,
            engine: Engine::Sqlite,
            ..Acceleration::default()
        };

        let _ = runtime
            .accelerator_engine_registry
            .create_accelerator_table(
                "abc".into(),
                schema,
                None,
                &acceleration_settings,
                Arc::new(RwLock::new(Secrets::new())),
                None,
                ctx,
            )
            .await
            .expect("accelerator table created");

        let path = Path::new(&path);
        assert!(path.is_file());
        fs::remove_file(path).expect("file removed");
    }

    #[tokio::test]
    #[cfg(feature = "sqlite")]
    async fn test_file_mode_sqlite_creation_default_path() {
        use crate::builder::RuntimeBuilder;
        use crate::make_spice_data_directory;
        use std::{fs, path::Path};

        let spice_data_dir = crate::spice_data_base_path();
        make_spice_data_directory().expect("spice data directory created");
        let path = format!("{spice_data_dir}/abc_sqlite.db");

        let runtime = Arc::new(RuntimeBuilder::new().build().await);
        let ctx = Arc::clone(&runtime.df.ctx);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let acceleration_settings = Acceleration {
            params: HashMap::new(),
            enabled: true,
            mode: Mode::File,
            engine: Engine::Sqlite,
            ..Acceleration::default()
        };
        let _ = runtime
            .accelerator_engine_registry
            .create_accelerator_table(
                "abc".into(),
                schema,
                None,
                &acceleration_settings,
                Arc::new(RwLock::new(Secrets::new())),
                None,
                ctx,
            )
            .await
            .expect("accelerator table created");

        let path = Path::new(&path);
        assert!(path.is_file());
        fs::remove_file(path).expect("file removed");
    }
}

#[cfg(test)]
#[allow(
    clippy::redundant_closure_for_method_calls,
    clippy::uninlined_format_args,
    clippy::bool_assert_comparison,
    clippy::used_underscore_binding,
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
mod accelerator_compat_tests {
    //! Shared compatibility test suite for data accelerators.
    //! These tests ensure accelerators behave consistently for common operations.

    use crate::component::dataset::acceleration::Engine;
    use crate::dataaccelerator::DataAccelerator;
    use ::arrow::{
        array::{
            Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
            DurationMillisecondArray, Float32Array, Float64Array, Int8Array, Int16Array,
            Int32Array, Int32Builder, Int64Array, IntervalYearMonthArray, LargeBinaryArray,
            LargeStringArray, RecordBatch, StringArray, Time32MillisecondArray,
            Time64MicrosecondArray, TimestampMicrosecondArray, UInt8Array, UInt16Array,
            UInt32Array, UInt64Array,
        },
        datatypes::{DataType, Field, Schema, TimeUnit},
    };
    use data_components::delete::get_deletion_provider;
    use datafusion::{
        common::{Constraints, TableReference, ToDFSchema},
        datasource::TableProvider,
        execution::context::SessionContext,
        logical_expr::{CreateExternalTable, col, dml::InsertOp, lit},
        physical_plan::collect,
    };
    use datafusion_table_providers::util::test::MockExec;
    use std::{collections::HashMap, sync::Arc};

    /// Test helper that runs the same test logic against all enabled accelerators
    async fn run_compat_test<F, Fut>(test_fn: F)
    where
        F: Fn(Engine, Arc<dyn TableProvider>, String) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        // Test both memory and file modes for databases
        // For Turso, also test both timestamp formats
        let test_configs = vec![
            #[cfg(feature = "sqlite")]
            (Engine::Sqlite, "memory", None),
            #[cfg(feature = "sqlite")]
            (Engine::Sqlite, "file", None),
            #[cfg(feature = "turso")]
            (Engine::Turso, "memory", Some("rfc3339")),
            #[cfg(feature = "turso")]
            (Engine::Turso, "file", Some("rfc3339")),
            #[cfg(feature = "turso")]
            (Engine::Turso, "memory", Some("integer_millis")),
            #[cfg(feature = "turso")]
            (Engine::Turso, "file", Some("integer_millis")),
            #[cfg(feature = "duckdb")]
            (Engine::DuckDB, "memory", None),
            #[cfg(feature = "duckdb")]
            (Engine::DuckDB, "file", None),
            (Engine::Arrow, "memory", None),
        ];

        for (engine, mode, timestamp_format) in test_configs {
            let mode_label = if let Some(ts_fmt) = timestamp_format {
                format!("{}, timestamp_format={}", mode, ts_fmt)
            } else {
                mode.to_string()
            };

            println!("Testing with engine: {:?} ({})", engine, mode_label);

            let schema = test_schema();
            let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

            // Create appropriate location based on mode
            let location = if mode == "file" {
                format!(
                    "/tmp/spice_benchmark_{:?}_{}_{}.db",
                    engine,
                    timestamp_format.unwrap_or("default"),
                    std::process::id()
                )
            } else {
                String::new()
            };

            let mut options = HashMap::new();
            if mode == "file" {
                options.insert("file".to_string(), location.clone());
            }

            // Add timestamp_format option for Turso
            if let Some(ts_fmt) = timestamp_format {
                options.insert("internal_timestamp_format".to_string(), ts_fmt.to_string());
            }

            let external_table = CreateExternalTable {
                schema: df_schema,
                name: TableReference::bare(format!("test_table_{:?}_{}", engine, mode)),
                location: location.clone(),
                file_type: String::new(),
                table_partition_cols: vec![],
                if_not_exists: true,
                definition: None,
                order_exprs: vec![],
                unbounded: false,
                options,
                constraints: Constraints::new_unverified(vec![]),
                column_defaults: HashMap::default(),
                temporary: false,
            };

            let table = match engine {
                #[cfg(feature = "sqlite")]
                Engine::Sqlite => {
                    use crate::dataaccelerator::sqlite::SqliteAccelerator;
                    match SqliteAccelerator::new()
                        .create_external_table(external_table, None, None)
                        .await
                    {
                        Ok(table) => table,
                        Err(e) => {
                            println!("  Skipping SQLite - unsupported types: {}", e);
                            continue;
                        }
                    }
                }
                #[cfg(feature = "turso")]
                Engine::Turso => {
                    use crate::dataaccelerator::turso::TursoAccelerator;
                    match TursoAccelerator::new()
                        .create_external_table(external_table, None, None)
                        .await
                    {
                        Ok(table) => table,
                        Err(e) => {
                            println!("  Skipping Turso - unsupported types: {}", e);
                            continue;
                        }
                    }
                }
                #[cfg(feature = "duckdb")]
                Engine::DuckDB => {
                    use crate::dataaccelerator::duckdb::DuckDBAccelerator;
                    match DuckDBAccelerator::new()
                        .create_external_table(external_table, None, None)
                        .await
                    {
                        Ok(table) => table,
                        Err(e) => {
                            println!("  Skipping DuckDB - unsupported types: {}", e);
                            continue;
                        }
                    }
                }
                Engine::Arrow => {
                    use crate::dataaccelerator::arrow::ArrowAccelerator;
                    match ArrowAccelerator::new()
                        .create_external_table(external_table, None, None)
                        .await
                    {
                        Ok(table) => table,
                        Err(e) => {
                            println!("  Skipping Arrow - unsupported types: {}", e);
                            continue;
                        }
                    }
                }
                _ => panic!("Unsupported engine for this test"),
            };

            test_fn(engine, table, mode_label.clone()).await;

            // Cleanup file if in file mode
            if mode == "file" && !location.is_empty() {
                let _ = std::fs::remove_file(&location);
            }
        }
    }

    /// Helper function to get the comprehensive test schema covering all major Arrow data types
    /// Note: Some exotic types (`Time64`, `LargeBinary`, `LargeUtf8`) may not be supported by all engines
    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            // Original columns (for backwards compatibility with existing tests)
            Field::new("id", DataType::Int64, false), // Primary key, not null
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
            // Additional integer types
            Field::new("int8_col", DataType::Int8, true),
            Field::new("int16_col", DataType::Int16, true),
            Field::new("int32_col", DataType::Int32, true),
            Field::new("uint8_col", DataType::UInt8, true),
            Field::new("uint16_col", DataType::UInt16, true),
            Field::new("uint32_col", DataType::UInt32, true),
            Field::new("uint64_col", DataType::UInt64, true),
            // Float types
            Field::new("float32_col", DataType::Float32, true),
            // Boolean
            Field::new("bool_col", DataType::Boolean, true),
            // String types
            Field::new("large_utf8_col", DataType::LargeUtf8, true),
            // Binary types
            Field::new("binary_col", DataType::Binary, true),
            Field::new("large_binary_col", DataType::LargeBinary, true),
            // Date/Time types
            Field::new("date32_col", DataType::Date32, true),
            Field::new("date64_col", DataType::Date64, true),
            Field::new(
                "time32_ms_col",
                DataType::Time32(TimeUnit::Millisecond),
                true,
            ),
            Field::new(
                "time64_us_col",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            ),
            Field::new(
                "timestamp_us_col",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            // Duration and Interval types
            Field::new(
                "duration_ms_col",
                DataType::Duration(TimeUnit::Millisecond),
                true,
            ),
            Field::new(
                "interval_ym_col",
                DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::YearMonth),
                true,
            ),
            // List type (list of Int32)
            Field::new(
                "list_col",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            // Map type (map of Utf8 keys to Int32 values)
            Field::new(
                "map_col",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Int32, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false, // keys are not sorted
                ),
                true,
            ),
            // Decimal types (Decimal128 is widely supported, Decimal256 is not)
            Field::new("decimal128_col", DataType::Decimal128(38, 10), true),
        ]))
    }

    /// Helper function to generate test data covering all Arrow data types
    fn generate_test_data(schema: Arc<Schema>, num_records: usize, offset: i64) -> RecordBatch {
        let nullable_mod = 10; // Every 10th value is null for testing null handling

        // Original columns (for backwards compatibility)
        let id_array = Int64Array::from(
            (0..num_records)
                .map(|i| offset + i as i64)
                .collect::<Vec<_>>(),
        );

        let name_array = StringArray::from(
            (0..num_records)
                .map(|i| format!("name_{}", i))
                .collect::<Vec<_>>(),
        );

        let value_array = Float64Array::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some((i as f64) * 1.5)
                    }
                })
                .collect::<Vec<_>>(),
        );

        // Additional integer types
        let int8_array = Int8Array::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some(((offset + i as i64) % 128) as i8)
                    }
                })
                .collect::<Vec<_>>(),
        );

        let int16_array = Int16Array::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some(((offset + i as i64) % 32768) as i16)
                    }
                })
                .collect::<Vec<_>>(),
        );

        let int32_array = Int32Array::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some((offset + i as i64) as i32)
                    }
                })
                .collect::<Vec<_>>(),
        );

        let uint8_array = UInt8Array::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some((i % 256) as u8)
                    }
                })
                .collect::<Vec<_>>(),
        );

        let uint16_array = UInt16Array::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some((i % 65536) as u16)
                    }
                })
                .collect::<Vec<_>>(),
        );

        let uint32_array = UInt32Array::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some(i as u32)
                    }
                })
                .collect::<Vec<_>>(),
        );

        let uint64_array = UInt64Array::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some(i as u64)
                    }
                })
                .collect::<Vec<_>>(),
        );

        // Float types
        let float32_array = Float32Array::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some((i as f32) * 1.5)
                    }
                })
                .collect::<Vec<_>>(),
        );

        let _float64_array = Float64Array::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some((i as f64) * 2.5)
                    }
                })
                .collect::<Vec<_>>(),
        );

        // Boolean
        let bool_array = BooleanArray::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some(i % 2 == 0)
                    }
                })
                .collect::<Vec<_>>(),
        );

        // String types
        let large_utf8_array = LargeStringArray::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some(format!("large_string_{}", offset + i as i64))
                    }
                })
                .collect::<Vec<_>>(),
        );

        // Binary types
        let binary_data: Vec<Option<&[u8]>> = (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some(format!("binary_{}", i).into_bytes().leak() as &[u8])
                }
            })
            .collect();
        let binary_array = BinaryArray::from(binary_data);

        let large_binary_data: Vec<Option<&[u8]>> = (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some(format!("large_binary_{}", i).into_bytes().leak() as &[u8])
                }
            })
            .collect();
        let large_binary_array = LargeBinaryArray::from(large_binary_data);

        // Date/Time types
        let date32_array = Date32Array::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some(18000 + i as i32) // Days since epoch
                    }
                })
                .collect::<Vec<_>>(),
        );

        let date64_array = Date64Array::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some(1_600_000_000_000_i64 + (i as i64 * 86_400_000)) // Milliseconds since epoch
                    }
                })
                .collect::<Vec<_>>(),
        );

        let time32_array = Time32MillisecondArray::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some(((i as i64 * 1_000) % 86_400_000) as i32) // Milliseconds since midnight
                    }
                })
                .collect::<Vec<_>>(),
        );

        let time64_array = Time64MicrosecondArray::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some((i as i64 * 1_000_000) % 86_400_000_000) // Microseconds since midnight
                    }
                })
                .collect::<Vec<_>>(),
        );

        let timestamp_array = TimestampMicrosecondArray::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some(1_600_000_000_000_000_i64 + (i as i64 * 1_000_000))
                    }
                })
                .collect::<Vec<_>>(),
        );

        // Duration and Interval types
        let duration_array = DurationMillisecondArray::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some((i as i64 * 1_000) % 86_400_000) // Duration in milliseconds
                    }
                })
                .collect::<Vec<_>>(),
        );

        let interval_array = IntervalYearMonthArray::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some((i as i32 % 120) * 12) // Interval in months (up to 10 years)
                    }
                })
                .collect::<Vec<_>>(),
        );

        // List type (list of Int32)
        let mut list_builder = arrow::array::ListBuilder::new(Int32Array::builder(num_records * 3));
        for i in 0..num_records {
            if i % nullable_mod == 0 {
                list_builder.append_null();
            } else {
                // Each list contains 3 integers
                list_builder.values().append_value(i as i32);
                list_builder.values().append_value((i as i32) * 2);
                list_builder.values().append_value((i as i32) * 3);
                list_builder.append(true);
            }
        }
        let list_array = list_builder.finish();

        // Map type (map of Utf8 keys to Int32 values)
        // Need to use the same field names as the schema: "key" and "value" (not "keys" and "values")
        use arrow::array::{MapBuilder, MapFieldNames, StringBuilder};

        let field_names = MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut map_builder =
            MapBuilder::new(Some(field_names), StringBuilder::new(), Int32Builder::new());
        for i in 0..num_records {
            if i % nullable_mod == 0 {
                map_builder.append(false).expect("append null map");
            } else {
                // Each map contains 2 key-value pairs
                map_builder.keys().append_value(format!("key_{}", i));
                map_builder.values().append_value(i as i32);
                map_builder.keys().append_value(format!("key2_{}", i));
                map_builder.values().append_value((i as i32) * 10);
                map_builder.append(true).expect("append map");
            }
        }
        let map_array = map_builder.finish();

        // Decimal types
        let decimal128_array = Decimal128Array::from(
            (0..num_records)
                .map(|i| {
                    if i % nullable_mod == 0 {
                        None
                    } else {
                        Some((i as i128 * 1_000_000_000) + 5_000_000_000)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .with_precision_and_scale(38, 10)
        .expect("valid decimal128");

        RecordBatch::try_new(
            schema,
            vec![
                // Original columns first (for backwards compatibility)
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
                // Additional type columns
                Arc::new(int8_array),
                Arc::new(int16_array),
                Arc::new(int32_array),
                Arc::new(uint8_array),
                Arc::new(uint16_array),
                Arc::new(uint32_array),
                Arc::new(uint64_array),
                Arc::new(float32_array),
                Arc::new(bool_array),
                Arc::new(large_utf8_array),
                Arc::new(binary_array),
                Arc::new(large_binary_array),
                Arc::new(date32_array),
                Arc::new(date64_array),
                Arc::new(time32_array),
                Arc::new(time64_array),
                Arc::new(timestamp_array),
                Arc::new(duration_array),
                Arc::new(interval_array),
                Arc::new(list_array),
                Arc::new(map_array),
                Arc::new(decimal128_array),
            ],
        )
        .expect("data should be created")
    }

    /// Helper function to insert test data into a table
    async fn insert_test_data(
        table: &Arc<dyn TableProvider>,
        ctx: &SessionContext,
        data: RecordBatch,
    ) {
        let schema = data.schema();
        let exec = MockExec::new(vec![Ok(data)], schema);
        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");
    }

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
    async fn test_basic_insert_and_query() {
        run_compat_test(|engine, table, _mode| async move {
            let ctx = SessionContext::new();
            let schema = test_schema();

            // Insert test data - 100 records for testing
            let data = generate_test_data(Arc::clone(&schema), 100, 0);
            insert_test_data(&table, &ctx, data).await;

            // Test 1: Full table scan
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan should be successful");
            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");
            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 100, "{:?}: should have 100 rows", engine);

            // Test 2: Filter with WHERE clause (id > 50)
            // Note: Arrow engine doesn't support filter pushdown, so it returns all rows
            let filter = col("id").gt(lit(50_i64));
            let scan = table
                .scan(&ctx.state(), None, &[filter], None)
                .await
                .expect("filtered scan should be successful");
            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("filtered scan successful");
            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            if engine != Engine::Arrow {
                assert!(
                    total_rows <= 50,
                    "{:?}: filtered should have <= 50 rows, got {}",
                    engine,
                    total_rows
                );
            }

            // Test 3: Projection (select only specific columns)
            let projection = Some(vec![0_usize, 2_usize]); // id and value only
            let scan = table
                .scan(&ctx.state(), projection.as_ref(), &[], None)
                .await
                .expect("projection scan should be successful");
            let projected_schema = scan.schema();
            assert_eq!(
                projected_schema.fields().len(),
                2,
                "{:?}: should have 2 projected columns",
                engine
            );

            // Test 4: LIMIT clause
            // Note: Arrow engine doesn't support limit pushdown
            let limit = Some(10);
            let scan = table
                .scan(&ctx.state(), None, &[], limit)
                .await
                .expect("limit scan should be successful");
            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("limit scan successful");
            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            if engine != Engine::Arrow {
                assert!(
                    total_rows <= 10,
                    "{:?}: limit should have <= 10 rows, got {}",
                    engine,
                    total_rows
                );
            }

            // Test 5: Combined filter + projection + limit
            // Note: Arrow engine doesn't support filter/limit pushdown
            let filter = col("id").lt(lit(30_i64));
            let projection = Some(vec![1_usize]); // name only
            let limit = Some(5);
            let scan = table
                .scan(&ctx.state(), projection.as_ref(), &[filter], limit)
                .await
                .expect("combined scan should be successful");
            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("combined scan successful");
            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            if engine != Engine::Arrow {
                assert!(
                    total_rows <= 5,
                    "{:?}: combined should have <= 5 rows, got {}",
                    engine,
                    total_rows
                );
            }

            // Test 6: Verify null handling (every 10th value is null)
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan should be successful");
            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");
            for batch in &results {
                let value_col = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("value should be Float64Array");
                // Check that some values are null
                let null_count = value_col.null_count();
                assert!(null_count > 0, "{:?}: should have some null values", engine);
            }
        })
        .await;
    }

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
    async fn test_delete_operations() {
        run_compat_test(|engine, table, _mode| async move {
            let ctx = SessionContext::new();
            let schema = test_schema();

            // Insert test data - 50 records
            let data = generate_test_data(Arc::clone(&schema), 50, 0);
            insert_test_data(&table, &ctx, data).await;

            // Get deletion provider
            let table = get_deletion_provider(table).expect("should support deletion");

            // Delete rows where id > 3
            let filter = col("id").gt(lit(3_i64));
            let plan = table
                .delete_from(&ctx.state(), &[filter])
                .await
                .expect("deletion should be successful");

            let result = collect(plan, ctx.task_ctx())
                .await
                .expect("deletion successful");

            let actual = result
                .first()
                .expect("result should have at least one batch")
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("result should be UInt64Array");

            assert_eq!(actual.value(0), 2, "{:?}: should delete 2 rows", engine);
        })
        .await;
    }

    #[tokio::test]
    async fn test_null_handling() {
        run_compat_test(|engine, table, _mode| async move {
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, true),
            ]));

            // Insert data with nulls
            let id_array = Int64Array::from(vec![1, 2, 3]);
            let name_array = StringArray::from(vec!["X", "Y", "Z"]);
            let value_array = Float64Array::from(vec![Some(1.0), None, Some(3.0)]);

            let data = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(id_array),
                    Arc::new(name_array),
                    Arc::new(value_array),
                ],
            )
            .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], schema);

            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Query back and verify nulls are preserved
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let batch = &results[0];
            let value_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("value should be Float64Array");

            assert!(
                !value_col.is_null(0),
                "{:?}: row 0 should not be null",
                engine
            );
            assert!(value_col.is_null(1), "{:?}: row 1 should be null", engine);
            assert!(
                !value_col.is_null(2),
                "{:?}: row 2 should not be null",
                engine
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_boolean_values() {
        run_compat_test(|engine, _table, _mode| async move {
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("active", DataType::Boolean, false),
            ]));

            let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
            let external_table = CreateExternalTable {
                schema: df_schema,
                name: TableReference::bare(format!("test_bool_{:?}", engine)),
                location: String::new(),
                file_type: String::new(),
                table_partition_cols: vec![],
                if_not_exists: true,
                definition: None,
                order_exprs: vec![],
                unbounded: false,
                options: HashMap::new(),
                constraints: Constraints::new_unverified(vec![]),
                column_defaults: HashMap::default(),
                temporary: false,
            };

            let bool_table: Arc<dyn TableProvider> = match engine {
                #[cfg(feature = "sqlite")]
                Engine::Sqlite => {
                    use crate::dataaccelerator::sqlite::SqliteAccelerator;
                    SqliteAccelerator::new()
                        .create_external_table(external_table, None, None)
                        .await
                        .expect("SQLite table should be created")
                }
                #[cfg(feature = "turso")]
                Engine::Turso => {
                    use crate::dataaccelerator::turso::TursoAccelerator;
                    TursoAccelerator::new()
                        .create_external_table(external_table, None, None)
                        .await
                        .expect("Turso table should be created")
                }
                _ => panic!("Unsupported engine"),
            };

            // Insert boolean data
            let id_array = Int64Array::from(vec![1, 2, 3]);
            let name_array = StringArray::from(vec!["A", "B", "C"]);
            let bool_array = BooleanArray::from(vec![true, false, true]);

            let data = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(id_array),
                    Arc::new(name_array),
                    Arc::new(bool_array),
                ],
            )
            .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], schema);

            let insertion = bool_table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Query and verify boolean values
            let scan = bool_table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let batch = &results[0];
            let bool_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("active should be BooleanArray");

            assert_eq!(
                bool_col.value(0),
                true,
                "{:?}: row 0 should be true",
                engine
            );
            assert_eq!(
                bool_col.value(1),
                false,
                "{:?}: row 1 should be false",
                engine
            );
            assert_eq!(
                bool_col.value(2),
                true,
                "{:?}: row 2 should be true",
                engine
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_empty_result_set() {
        run_compat_test(|engine, _table, _mode| async move {
            let ctx = SessionContext::new();

            // Query empty table
            let scan = _table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            // Both should return empty results gracefully
            assert!(
                results.is_empty() || results[0].num_rows() == 0,
                "{:?}: empty table should return empty results",
                engine
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_filter_predicates() {
        run_compat_test(|engine, table, _mode| async move {
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, true),
            ]));

            // Insert test data
            let id_array = Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
            let name_array = StringArray::from(vec![
                "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack",
            ]);
            let value_array = Float64Array::from(vec![
                Some(10.5),
                Some(20.5),
                Some(30.5),
                Some(40.5),
                Some(50.5),
                Some(60.5),
                Some(70.5),
                Some(80.5),
                Some(90.5),
                Some(100.5),
            ]);

            let data = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(id_array),
                    Arc::new(name_array),
                    Arc::new(value_array),
                ],
            )
            .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], schema);

            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Test 1: Filter with greater than predicate
            let filter = col("id").gt(lit(5_i64));
            let scan = table
                .scan(&ctx.state(), None, &[filter], None)
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                total_rows, 5,
                "{:?}: should have 5 rows with id > 5",
                engine
            );

            // Test 2: Filter with less than or equal predicate
            let filter = col("value").lt_eq(lit(30.5_f64));
            let scan = table
                .scan(&ctx.state(), None, &[filter], None)
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                total_rows, 3,
                "{:?}: should have 3 rows with value <= 30.5",
                engine
            );

            // Test 3: Filter with equality predicate
            let filter = col("name").eq(lit("Charlie"));
            let scan = table
                .scan(&ctx.state(), None, &[filter], None)
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                total_rows, 1,
                "{:?}: should have 1 row with name = Charlie",
                engine
            );

            // Test 4: Multiple filters (AND condition)
            let filter1 = col("id").gt(lit(3_i64));
            let filter2 = col("value").lt(lit(70.5_f64));
            let scan = table
                .scan(&ctx.state(), None, &[filter1, filter2], None)
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                total_rows, 3,
                "{:?}: should have 3 rows with id > 3 AND value < 70.5",
                engine
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_projection_pushdown() {
        run_compat_test(|engine, table, _mode| async move {
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, true),
            ]));

            // Insert test data
            let id_array = Int64Array::from(vec![1, 2, 3]);
            let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
            let value_array = Float64Array::from(vec![Some(1.5), Some(2.5), Some(3.5)]);

            let data = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(id_array),
                    Arc::new(name_array),
                    Arc::new(value_array),
                ],
            )
            .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], schema);

            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Test projection: select only id and name columns (indices 0 and 1)
            let projection = Some(vec![0_usize, 1_usize]);
            let scan = table
                .scan(&ctx.state(), projection.as_ref(), &[], None)
                .await
                .expect("scan should be successful");

            // Verify projected schema
            let projected_schema = scan.schema();
            assert_eq!(
                projected_schema.fields().len(),
                2,
                "{:?}: should have 2 projected columns",
                engine
            );
            assert_eq!(
                projected_schema.field(0).name(),
                "id",
                "{:?}: first field should be id",
                engine
            );
            assert_eq!(
                projected_schema.field(1).name(),
                "name",
                "{:?}: second field should be name",
                engine
            );

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let batch = &results[0];
            assert_eq!(
                batch.num_columns(),
                2,
                "{:?}: should have 2 columns in result",
                engine
            );
            assert_eq!(
                batch.num_rows(),
                3,
                "{:?}: should have 3 rows in result",
                engine
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_limit_pushdown() {
        run_compat_test(|engine, table, _mode| async move {
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, true),
            ]));

            // Insert 10 rows
            let id_array = Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
            let name_array =
                StringArray::from(vec!["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]);
            let value_array = Float64Array::from(vec![
                Some(1.0),
                Some(2.0),
                Some(3.0),
                Some(4.0),
                Some(5.0),
                Some(6.0),
                Some(7.0),
                Some(8.0),
                Some(9.0),
                Some(10.0),
            ]);

            let data = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(id_array),
                    Arc::new(name_array),
                    Arc::new(value_array),
                ],
            )
            .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], schema);

            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Test limit of 3
            let scan = table
                .scan(&ctx.state(), None, &[], Some(3))
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            assert!(
                total_rows <= 3,
                "{:?}: should have at most 3 rows with limit 3",
                engine
            );
            assert!(total_rows > 0, "{:?}: should have at least 1 row", engine);
        })
        .await;
    }

    #[tokio::test]
    async fn test_combined_filter_projection_limit() {
        run_compat_test(|engine, table, _mode| async move {
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, true),
            ]));

            // Insert test data
            let id_array = Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
            let name_array = StringArray::from(vec![
                "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack",
            ]);
            let value_array = Float64Array::from(vec![
                Some(10.0),
                Some(20.0),
                Some(30.0),
                Some(40.0),
                Some(50.0),
                Some(60.0),
                Some(70.0),
                Some(80.0),
                Some(90.0),
                Some(100.0),
            ]);

            let data = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(id_array),
                    Arc::new(name_array),
                    Arc::new(value_array),
                ],
            )
            .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], schema);

            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Test: projection (only name), filter (id > 3), and limit (2)
            let projection = Some(vec![1_usize]); // name column
            let filter = col("id").gt(lit(3_i64));
            let limit = Some(2);

            let scan = table
                .scan(&ctx.state(), projection.as_ref(), &[filter], limit)
                .await
                .expect("scan should be successful");

            // Verify projected schema
            let projected_schema = scan.schema();
            assert_eq!(
                projected_schema.fields().len(),
                1,
                "{:?}: should have 1 projected column",
                engine
            );
            assert_eq!(
                projected_schema.field(0).name(),
                "name",
                "{:?}: projected field should be name",
                engine
            );

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            assert!(
                total_rows <= 2,
                "{:?}: should have at most 2 rows with limit 2",
                engine
            );
            assert!(
                total_rows > 0,
                "{:?}: should have at least 1 row with id > 3",
                engine
            );

            // Verify only name column is present
            for batch in &results {
                assert_eq!(
                    batch.num_columns(),
                    1,
                    "{:?}: should have 1 column in result",
                    engine
                );
            }
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "Run with --ignored flag: cargo test --features sqlite,turso,duckdb -- --ignored --nocapture benchmark_roundtrip"]
    async fn benchmark_roundtrip() {
        use std::time::Instant;

        run_compat_test(|engine, table, mode| async move {
            let ctx = SessionContext::new();
            let schema = test_schema();

            // Memory mode has limitations, file mode can handle much more
            // Turso has tighter page cache limits than other databases due to the comprehensive test schema
            // Note: mode string may include timestamp format like "memory, timestamp_format=rfc3339"
            let is_memory = mode.starts_with("memory");
            let is_file = mode.starts_with("file");

            let (num_records, num_iterations) = match (engine, is_memory, is_file) {
                (Engine::Turso, true, _) => (100, 3), // 300 total records (very limited due to page cache)
                (Engine::Turso, _, true) => (1_000, 10), // 10K total records (reduced due to complex schema)
                (_, true, _) => (100_000, 10),           // 1M total records
                (_, _, true) => (1_000_000, 10),         // 10M total records
                _ => (10_000, 10),                       // Fallback
            };

            let mut insert_times = Vec::new();
            let mut query_times = Vec::new();

            println!("\n=== Benchmarking {:?} ({}) ===", engine, mode);
            println!("Records per iteration: {}", num_records);
            println!("Number of iterations: {}", num_iterations);

            for iteration in 0..num_iterations {
                // Prepare test data using shared helper
                let id_offset = (iteration * num_records) as i64;
                let data = generate_test_data(Arc::clone(&schema), num_records, id_offset);

                // Benchmark insert
                let insert_start = Instant::now();
                let exec = MockExec::new(vec![Ok(data)], Arc::clone(&schema));
                let insertion = table
                    .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                    .await
                    .expect("insertion should be successful");

                collect(insertion, ctx.task_ctx())
                    .await
                    .expect("insert successful");
                let insert_duration = insert_start.elapsed();
                insert_times.push(insert_duration);

                // Benchmark query (scan all data)
                let query_start = Instant::now();
                let scan = table
                    .scan(&ctx.state(), None, &[], None)
                    .await
                    .expect("scan should be successful");

                let results = collect(scan, ctx.task_ctx())
                    .await
                    .expect("scan successful");
                let query_duration = query_start.elapsed();
                query_times.push(query_duration);

                // Verify data integrity
                let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
                assert_eq!(
                    total_rows,
                    num_records * (iteration + 1),
                    "{:?}: iteration {}: should have {} total rows",
                    engine,
                    iteration,
                    num_records * (iteration + 1)
                );

                if iteration % 3 == 0 {
                    println!(
                        "  Iteration {}: Insert: {:?}, Query: {:?}",
                        iteration, insert_duration, query_duration
                    );
                }
            }

            // Helper function to calculate percentiles
            fn percentile(sorted_times: &[std::time::Duration], p: f64) -> std::time::Duration {
                let idx = ((sorted_times.len() as f64 - 1.0) * p).ceil() as usize;
                sorted_times[idx]
            }

            // Sort times for percentile calculations
            let mut sorted_insert = insert_times.clone();
            sorted_insert.sort();
            let mut sorted_query = query_times.clone();
            sorted_query.sort();

            // Calculate percentiles
            let min_insert = sorted_insert[0];
            let p75_insert = percentile(&sorted_insert, 0.75);
            let p90_insert = percentile(&sorted_insert, 0.90);
            let p95_insert = percentile(&sorted_insert, 0.95);
            let p99_insert = percentile(&sorted_insert, 0.99);
            let max_insert = sorted_insert[sorted_insert.len() - 1];

            let min_query = sorted_query[0];
            let p75_query = percentile(&sorted_query, 0.75);
            let p90_query = percentile(&sorted_query, 0.90);
            let p95_query = percentile(&sorted_query, 0.95);
            let p99_query = percentile(&sorted_query, 0.99);
            let max_query = sorted_query[sorted_query.len() - 1];

            // Calculate round-trip percentiles
            let mut roundtrip_times: Vec<std::time::Duration> = insert_times
                .iter()
                .zip(query_times.iter())
                .map(|(i, q)| *i + *q)
                .collect();
            roundtrip_times.sort();
            let min_roundtrip = roundtrip_times[0];
            let p75_roundtrip = percentile(&roundtrip_times, 0.75);
            let p90_roundtrip = percentile(&roundtrip_times, 0.90);
            let p95_roundtrip = percentile(&roundtrip_times, 0.95);
            let p99_roundtrip = percentile(&roundtrip_times, 0.99);
            let max_roundtrip = roundtrip_times[roundtrip_times.len() - 1];

            println!("\n--- Results for {:?} ({}) ---", engine, mode);
            println!("Insert Performance:");
            println!("  Min: {:?}", min_insert);
            println!("  P75: {:?}", p75_insert);
            println!("  P90: {:?}", p90_insert);
            println!("  P95: {:?}", p95_insert);
            println!("  P99: {:?}", p99_insert);
            println!("  Max: {:?}", max_insert);
            println!(
                "  P50 records/sec: {:.2}",
                num_records as f64 / percentile(&sorted_insert, 0.50).as_secs_f64()
            );

            println!("\nQuery Performance:");
            println!("  Min: {:?}", min_query);
            println!("  P75: {:?}", p75_query);
            println!("  P90: {:?}", p90_query);
            println!("  P95: {:?}", p95_query);
            println!("  P99: {:?}", p99_query);
            println!("  Max: {:?}", max_query);
            println!(
                "  P50 records/sec: {:.2}",
                (num_records * num_iterations) as f64
                    / percentile(&sorted_query, 0.50).as_secs_f64()
            );

            println!("\nRound-trip (Insert + Query):");
            println!("  Min: {:?}", min_roundtrip);
            println!("  P75: {:?}", p75_roundtrip);
            println!("  P90: {:?}", p90_roundtrip);
            println!("  P95: {:?}", p95_roundtrip);
            println!("  P99: {:?}", p99_roundtrip);
            println!("  Max: {:?}", max_roundtrip);
            println!("========================\n");
        })
        .await;
    }
}
