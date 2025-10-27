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
#[cfg(feature = "duckdb")]
use self::partitioned_duckdb::tables_mode::TablesModePartitionedDuckDBAccelerator;
#[cfg(all(feature = "pepper", not(windows)))]
use self::pepper::PepperAccelerator;
#[cfg(feature = "postgres")]
use self::postgres::PostgresAccelerator;
#[cfg(feature = "sqlite")]
use self::sqlite::SqliteAccelerator;

pub mod arrow;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "duckdb")]
pub mod partitioned_duckdb;
#[cfg(all(feature = "pepper", not(windows)))]
pub mod pepper;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

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
        #[cfg(feature = "duckdb")]
        self.register_accelerator_engine(
            Engine::TableModePartitionedDuckDB,
            Arc::new(TablesModePartitionedDuckDBAccelerator::new()),
        )
        .await;
        #[cfg(feature = "postgres")]
        self.register_accelerator_engine(Engine::PostgreSQL, Arc::new(PostgresAccelerator::new()))
            .await;
        #[cfg(feature = "sqlite")]
        self.register_accelerator_engine(Engine::Sqlite, Arc::new(SqliteAccelerator::new()))
            .await;
        #[cfg(all(feature = "pepper", not(windows)))]
        self.register_accelerator_engine(Engine::Pepper, Arc::new(PepperAccelerator::new()))
            .await;
    }

    pub async fn unregister_all(&self) {
        let mut registry = self.accelerator_engine_registry.write().await;

        // Call shutdown on each accelerator before clearing
        for (engine, accelerator) in registry.iter() {
            tracing::debug!("Shutting down {engine:?} accelerator");
            if let Err(e) = accelerator.shutdown().await {
                tracing::error!("Failed to shutdown {engine:?} accelerator: {e}");
            }
        }

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

    /// Shutdown the accelerator, performing any necessary cleanup operations.
    ///
    /// This method is called automatically by the runtime during shutdown,
    /// giving the accelerator an opportunity to:
    /// - Truncate WAL files
    /// - Run optimization/compaction
    /// - Checkpoint data
    /// - Close connections gracefully
    ///
    /// Default implementation does nothing.
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
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

    /// Returns the time column name if configured, None otherwise
    /// Views always return None as they don't support time-based append mode
    fn time_column(&self) -> Option<&str>;

    /// Returns a reference to `Any` for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
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

pub(crate) fn get_primary_keys_from_constraints(
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

    use crate::component::dataset::acceleration::{Acceleration, Engine, Mode};
    use crate::dataaccelerator::DataAccelerator;
    use ::arrow::{
        array::{
            Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
            DurationMillisecondArray, Float32Array, Float64Array, Int8Array, Int16Array,
            Int32Array, Int32Builder, Int64Array, IntervalYearMonthArray, LargeBinaryArray,
            LargeStringArray, RecordBatch, StringArray, StringBuilder, Time32MillisecondArray,
            Time64MicrosecondArray, TimestampMicrosecondArray, UInt8Array, UInt16Array,
            UInt32Array, UInt64Array,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
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
    use tempfile::TempDir;

    /// Helper struct to manage temporary test environment
    /// Ensures unique data directories per test and proper cleanup
    struct TestEnvironment {
        _temp_dir: TempDir,
        data_path: String,
    }

    impl TestEnvironment {
        fn new() -> Self {
            let temp_dir = TempDir::new().expect("Failed to create temp directory");
            let data_path = temp_dir.path().to_string_lossy().to_string();

            Self {
                _temp_dir: temp_dir,
                data_path,
            }
        }

        fn metadata_dir(&self) -> String {
            format!("{}/metadata", self.data_path)
        }
    }

    /// Mock acceleration source for testing
    /// Test helper that runs the same test logic against all enabled accelerators
    async fn run_compat_test<F, Fut>(test_fn: F)
    where
        F: Fn(Engine, Arc<dyn TableProvider>, String, &TestEnvironment) -> Fut,
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
            #[cfg(all(feature = "pepper", not(windows)))]
            (Engine::Pepper, "file", None), // Pepper only supports file mode
        ];

        for (engine, mode, timestamp_format) in test_configs {
            // Create a unique test environment for this test run
            let test_env = TestEnvironment::new();

            let mode_label = if let Some(ts_fmt) = timestamp_format {
                format!("{}, timestamp_format={}", mode, ts_fmt)
            } else {
                mode.to_string()
            };

            println!("Testing with engine: {:?} ({})", engine, mode_label);

            let schema = test_schema(Some(engine));
            let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

            // Create appropriate location based on mode with a unique identifier per test run
            // This ensures tests don't interfere with each other by reusing the same file/directory
            let location = if mode == "file" {
                format!(
                    "/tmp/spice_benchmark_{:?}_{}_{}_{}.db",
                    engine,
                    timestamp_format.unwrap_or("default"),
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_nanos()
                )
            } else {
                String::new()
            };

            let mut options = HashMap::new();
            if mode == "file" {
                options.insert("file".to_string(), location.clone());
            }

            // Add mode option for engines that need it (e.g., Vortex)
            options.insert("mode".to_string(), mode.to_string());

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
                        .create_external_table(external_table, None, Vec::new())
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
                        .create_external_table(external_table, None, Vec::new())
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
                        .create_external_table(external_table, None, Vec::new())
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
                        .create_external_table(external_table, None, Vec::new())
                        .await
                    {
                        Ok(table) => table,
                        Err(e) => {
                            println!("  Skipping Arrow - unsupported types: {}", e);
                            continue;
                        }
                    }
                }
                #[cfg(all(feature = "pepper", not(windows)))]
                Engine::Pepper => {
                    use crate::component::dataset::builder::DatasetBuilder;
                    use crate::dataaccelerator::pepper::PepperAccelerator;

                    // Clean up any existing .pepper files and Pepper metadata
                    // Pepper only supports appends, so we need a clean state for each test
                    if mode == "file" && !location.is_empty() {
                        let test_dir = std::path::Path::new(&location);
                        if test_dir.exists() {
                            if let Ok(entries) = std::fs::read_dir(test_dir) {
                                for entry in entries.flatten() {
                                    let path = entry.path();
                                    // Safety: only delete .pepper files
                                    if path.extension().and_then(|s| s.to_str()) == Some("pepper") {
                                        let _ = std::fs::remove_file(&path);
                                    }
                                }
                            }
                        } else {
                            // Create the directory if it doesn't exist
                            let _ = std::fs::create_dir_all(test_dir);
                        }

                        // Also clean up Pepper metadata to ensure fresh schema
                        // Use test environment's metadata directory
                        let pepper_db_path = format!("{}/pepper.db", test_env.metadata_dir());
                        if std::path::Path::new(&pepper_db_path).exists() {
                            let _ = std::fs::remove_file(&pepper_db_path);
                        }
                    }

                    // Create a proper Dataset that implements AccelerationSource
                    let test_app_obj = app::AppBuilder::new("test").build();
                    let test_app = Arc::new(test_app_obj.clone());
                    let test_runtime = Arc::new(
                        crate::Runtime::builder()
                            .with_app(test_app_obj)
                            .build()
                            .await,
                    );

                    let dataset_name = format!("test_table_{:?}_{}", engine, mode);
                    let mut dataset =
                        match DatasetBuilder::try_new(dataset_name.clone(), &dataset_name)
                            .expect("Failed to create dataset builder")
                            .with_app(Arc::clone(&test_app))
                            .with_runtime(Arc::clone(&test_runtime))
                            .build()
                        {
                            Ok(ds) => ds,
                            Err(e) => {
                                println!("  Skipping Vortex - failed to create dataset: {}", e);
                                continue;
                            }
                        };

                    // Configure acceleration settings
                    let mut params = HashMap::new();
                    if mode == "file" {
                        // Set file_path to use our unique temporary location with timestamp
                        params.insert("pepper_file_path".to_string(), location.clone());
                    }
                    // Use test environment's metadata directory for Pepper
                    params.insert("pepper_metadata_dir".to_string(), test_env.metadata_dir());
                    // Use 'error' mode for tests to fail on unsupported types
                    // This matches the new default production behavior
                    params.insert("unsupported_type_action".to_string(), "error".to_string());

                    dataset.acceleration = Some(Acceleration {
                        enabled: true,
                        mode: if mode == "file" {
                            Mode::File
                        } else {
                            Mode::Memory
                        },
                        engine: Engine::Pepper,
                        params,
                        ..Acceleration::default()
                    });

                    // Vortex may panic on unsupported types (e.g., Duration), so we catch that
                    // We need to catch panics from the async operation by using FutureExt::catch_unwind
                    use futures::FutureExt;
                    use std::panic::AssertUnwindSafe;

                    let accelerator = PepperAccelerator::new();
                    let create_future = AssertUnwindSafe(accelerator.create_external_table(
                        external_table,
                        Some(&dataset),
                        Vec::new(),
                    ))
                    .catch_unwind();

                    match create_future.await {
                        Ok(Ok(table)) => table,
                        Ok(Err(e)) => {
                            println!("  Skipping Vortex - unsupported types: {}", e);
                            continue;
                        }
                        Err(panic_err) => {
                            // Extract panic message if possible
                            let panic_msg = if let Some(s) = panic_err.downcast_ref::<&str>() {
                                (*s).to_string()
                            } else if let Some(s) = panic_err.downcast_ref::<String>() {
                                s.clone()
                            } else {
                                "unknown panic".to_string()
                            };
                            println!(
                                "  Skipping Vortex - unsupported types (panic): {}",
                                panic_msg
                            );
                            continue;
                        }
                    }
                }
                _ => panic!("Unsupported engine for this test"),
            };

            test_fn(engine, table, mode_label.clone(), &test_env).await;

            // Cleanup file if in file mode
            if mode == "file" && !location.is_empty() {
                let _ = std::fs::remove_file(&location);
            }
        }
    }

    /// Helper function to get the comprehensive test schema covering all major Arrow data types
    /// Note: Some exotic types (`Time64`, `LargeBinary`, `LargeUtf8`) may not be supported by all engines
    /// For Vortex, Time32, Time64, Duration, Interval, and Map types are excluded as they are not yet supported
    fn test_schema(engine: Option<Engine>) -> Arc<Schema> {
        let mut fields = vec![
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
        ];

        // Skip Time32 and Time64 for Vortex as they're not yet supported
        if !matches!(engine, Some(Engine::Pepper)) {
            fields.push(Field::new(
                "time32_ms_col",
                DataType::Time32(TimeUnit::Millisecond),
                true,
            ));
            fields.push(Field::new(
                "time64_us_col",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            ));
        }

        fields.push(Field::new(
            "timestamp_us_col",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        // Skip Duration for Vortex as it's not yet supported
        if !matches!(engine, Some(Engine::Pepper)) {
            fields.push(Field::new(
                "duration_ms_col",
                DataType::Duration(TimeUnit::Millisecond),
                true,
            ));
        }

        // Skip Interval for Vortex as it's not yet supported
        if !matches!(engine, Some(Engine::Pepper)) {
            fields.push(Field::new(
                "interval_ym_col",
                DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::YearMonth),
                true,
            ));
        }

        fields.push(Field::new(
            "list_col",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ));

        // Skip Map type for Vortex as it's not yet supported
        if !matches!(engine, Some(Engine::Pepper)) {
            fields.push(Field::new(
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
            ));
        }

        // Decimal types (Decimal128 is widely supported, Decimal256 is not)
        fields.push(Field::new(
            "decimal128_col",
            DataType::Decimal128(38, 10),
            true,
        ));

        Arc::new(Schema::new(fields))
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
        let binary_data: Vec<Option<Vec<u8>>> = (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some(format!("binary_{i}").into_bytes())
                }
            })
            .collect();
        let binary_slices: Vec<Option<&[u8]>> =
            binary_data.iter().map(|value| value.as_deref()).collect();
        let binary_array = BinaryArray::from(binary_slices);

        let large_binary_data: Vec<Option<Vec<u8>>> = (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some(format!("large_binary_{i}").into_bytes())
                }
            })
            .collect();
        let large_binary_slices: Vec<Option<&[u8]>> = large_binary_data
            .iter()
            .map(|value| value.as_deref())
            .collect();
        let large_binary_array = LargeBinaryArray::from(large_binary_slices);

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

        // Build the columns vector based on what's in the schema
        let mut columns: Vec<Arc<dyn Array>> = vec![
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
        ];

        // Add time arrays if they exist in the schema
        if schema.column_with_name("time32_ms_col").is_some() {
            columns.push(Arc::new(time32_array));
        }
        if schema.column_with_name("time64_us_col").is_some() {
            columns.push(Arc::new(time64_array));
        }

        columns.push(Arc::new(timestamp_array));

        // Add duration and interval arrays if they exist in the schema
        if schema.column_with_name("duration_ms_col").is_some() {
            columns.push(Arc::new(duration_array));
        }
        if schema.column_with_name("interval_ym_col").is_some() {
            columns.push(Arc::new(interval_array));
        }

        // Add list array if it exists in the schema
        if schema.column_with_name("list_col").is_some() {
            columns.push(Arc::new(list_array));
        }

        // Add map array if it exists in the schema
        if schema.column_with_name("map_col").is_some() {
            columns.push(Arc::new(map_array));
        }

        columns.push(Arc::new(decimal128_array));

        RecordBatch::try_new(schema, columns).expect("data should be created")
    }

    /// Transform `RecordBatch` to match a target schema by converting unsupported types to strings
    /// This is needed for engines like Vortex that convert unsupported types to Utf8
    fn transform_batch_to_schema(
        batch: &RecordBatch,
        target_schema: SchemaRef,
    ) -> Result<RecordBatch, arrow::error::ArrowError> {
        let source_schema = batch.schema();
        let mut new_columns: Vec<ArrayRef> = Vec::new();

        for target_field in target_schema.fields() {
            // Find the corresponding source field by name
            let source_field_idx = source_schema
                .fields()
                .iter()
                .position(|f| f.name() == target_field.name())
                .ok_or_else(|| {
                    arrow::error::ArrowError::SchemaError(format!(
                        "Field '{}' not found in source schema",
                        target_field.name()
                    ))
                })?;

            let source_array = batch.column(source_field_idx);
            let source_type = source_schema.field(source_field_idx).data_type();

            // If types match, use the column as-is
            if source_type == target_field.data_type() {
                new_columns.push(Arc::clone(source_array));
                continue;
            }

            // If target is Utf8 and source is not, convert to string
            if matches!(target_field.data_type(), DataType::Utf8)
                && !matches!(source_type, DataType::Utf8 | DataType::LargeUtf8)
            {
                let mut builder = StringBuilder::new();

                for row_idx in 0..source_array.len() {
                    if source_array.is_null(row_idx) {
                        builder.append_null();
                    } else {
                        // Convert the value to string representation
                        let string_value = match source_type {
                            DataType::Time32(TimeUnit::Millisecond) => {
                                let Some(arr) = source_array
                                    .as_any()
                                    .downcast_ref::<Time32MillisecondArray>()
                                else {
                                    return Err(arrow::error::ArrowError::ComputeError(
                                        "Failed to downcast to Time32MillisecondArray".to_string(),
                                    ));
                                };
                                format!("{}", arr.value(row_idx))
                            }
                            DataType::Time64(TimeUnit::Microsecond) => {
                                let Some(arr) = source_array
                                    .as_any()
                                    .downcast_ref::<Time64MicrosecondArray>()
                                else {
                                    return Err(arrow::error::ArrowError::ComputeError(
                                        "Failed to downcast to Time64MicrosecondArray".to_string(),
                                    ));
                                };
                                format!("{}", arr.value(row_idx))
                            }
                            DataType::Duration(TimeUnit::Millisecond) => {
                                let Some(arr) = source_array
                                    .as_any()
                                    .downcast_ref::<DurationMillisecondArray>()
                                else {
                                    return Err(arrow::error::ArrowError::ComputeError(
                                        "Failed to downcast to DurationMillisecondArray"
                                            .to_string(),
                                    ));
                                };
                                format!("{}ms", arr.value(row_idx))
                            }
                            DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth) => {
                                let Some(arr) = source_array
                                    .as_any()
                                    .downcast_ref::<IntervalYearMonthArray>()
                                else {
                                    return Err(arrow::error::ArrowError::ComputeError(
                                        "Failed to downcast to IntervalYearMonthArray".to_string(),
                                    ));
                                };
                                format!("{} months", arr.value(row_idx))
                            }
                            DataType::Map(_, _) => {
                                // For Map types, use Arrow's display format
                                format!(
                                    "{:?}",
                                    arrow::util::display::array_value_to_string(
                                        source_array,
                                        row_idx
                                    )
                                    .unwrap_or_else(|_| "null".to_string())
                                )
                            }
                            _ => {
                                // Generic conversion using Arrow's display utilities
                                arrow::util::display::array_value_to_string(source_array, row_idx)
                                    .unwrap_or_else(|_| "null".to_string())
                            }
                        };
                        builder.append_value(string_value);
                    }
                }

                new_columns.push(Arc::new(builder.finish()) as ArrayRef);
            } else {
                // For other type mismatches, use Arrow's cast kernel to handle compatible conversions
                // (e.g., Float16->Float32, Int32->Int64, etc.)
                let casted =
                    arrow::compute::cast(source_array, target_field.data_type()).map_err(|e| {
                        arrow::error::ArrowError::ComputeError(format!(
                            "Failed to cast field '{}' from {:?} to {:?}: {}",
                            target_field.name(),
                            source_type,
                            target_field.data_type(),
                            e
                        ))
                    })?;
                new_columns.push(casted);
            }
        }

        RecordBatch::try_new(target_schema, new_columns)
    }

    /// Helper function to insert test data into a table
    async fn insert_test_data(
        table: &Arc<dyn TableProvider>,
        ctx: &SessionContext,
        data: RecordBatch,
    ) {
        let table_schema = table.schema();

        // Transform the data to match the table schema if needed
        // (e.g., for Vortex which converts unsupported types to strings)
        let transformed_data = if data.schema() == table_schema {
            data
        } else {
            transform_batch_to_schema(&data, Arc::clone(&table_schema))
                .expect("data transformation should succeed")
        };

        let schema = transformed_data.schema();
        let exec = MockExec::new(vec![Ok(transformed_data)], schema);
        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");
    }

    #[tokio::test]
    async fn test_schema_preservation() {
        run_compat_test(|engine, table, _mode, _test_env| async move {
            let original_schema = test_schema(Some(engine));
            let table_schema = table.schema();

            // Verify that the table schema has all fields (count should match)
            assert_eq!(
                table_schema.fields().len(),
                original_schema.fields().len(),
                "{:?}: Schema field count mismatch. Expected {}, got {}. \
                 This indicates that the catalog is not preserving all fields correctly.",
                engine,
                original_schema.fields().len(),
                table_schema.fields().len()
            );

            // Define types that Vortex converts to Utf8 with unsupported_type_action: string
            let vortex_unsupported_types = [
                DataType::Time32(TimeUnit::Millisecond),
                DataType::Time64(TimeUnit::Microsecond),
                DataType::Duration(TimeUnit::Millisecond),
                DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth),
            ];

            // Verify each field matches (or is appropriately converted)
            for (i, (original_field, table_field)) in original_schema
                .fields()
                .iter()
                .zip(table_schema.fields().iter())
                .enumerate()
            {
                assert_eq!(
                    original_field.name(),
                    table_field.name(),
                    "{:?}: Field {} name mismatch",
                    engine,
                    i
                );

                let original_type = original_field.data_type();
                let table_type = table_field.data_type();

                // For Vortex, check if unsupported types are converted to Utf8
                if matches!(engine, Engine::Pepper) {
                    if vortex_unsupported_types.contains(original_type) {
                        assert_eq!(
                            table_type,
                            &DataType::Utf8,
                            "{:?}: Field {} ({}) with unsupported type {:?} should be converted to Utf8, got {:?}",
                            engine,
                            i,
                            original_field.name(),
                            original_type,
                            table_type
                        );
                    } else if matches!(original_type, DataType::Map(_, _)) {
                        // Map types are also converted to Utf8
                        assert_eq!(
                            table_type,
                            &DataType::Utf8,
                            "{:?}: Field {} ({}) with Map type should be converted to Utf8, got {:?}",
                            engine,
                            i,
                            original_field.name(),
                            table_type
                        );
                    } else {
                        // Other types should match exactly (or be compatible conversions like timestamps)
                        assert_eq!(
                            original_type,
                            table_type,
                            "{:?}: Field {} ({}) data type mismatch. Expected {:?}, got {:?}",
                            engine,
                            i,
                            original_field.name(),
                            original_type,
                            table_type
                        );
                    }
                } else {
                    // For non-Vortex engines, types should match exactly
                    assert_eq!(
                        original_type,
                        table_type,
                        "{:?}: Field {} ({}) data type mismatch. Expected {:?}, got {:?}",
                        engine,
                        i,
                        original_field.name(),
                        original_type,
                        table_type
                    );
                }

                assert_eq!(
                    original_field.is_nullable(),
                    table_field.is_nullable(),
                    "{:?}: Field {} ({}) nullable mismatch",
                    engine,
                    i,
                    original_field.name()
                );
            }
        })
        .await;
    }

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
    async fn test_basic_insert_and_query() {
        run_compat_test(|engine, table, _mode, _test_env| async move {
            let ctx = SessionContext::new();
            let schema = test_schema(Some(engine));

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
            assert_eq!(total_rows, 100, "{:?}: should have {} rows", engine, 100);

            // Test 2: Filter with WHERE clause (id > 50)
            // Note: Arrow and Vortex engines don't support filter pushdown, so they return all rows
            let filter = col("id").gt(lit(50_i64));
            let scan = table
                .scan(&ctx.state(), None, &[filter], None)
                .await
                .expect("filtered scan should be successful");
            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("filtered scan successful");
            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            if engine != Engine::Arrow && engine != Engine::Pepper {
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
            // Note: Arrow and Vortex engines don't support limit pushdown
            let limit = Some(10);
            let scan = table
                .scan(&ctx.state(), None, &[], limit)
                .await
                .expect("limit scan should be successful");
            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("limit scan successful");
            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            if engine != Engine::Arrow && engine != Engine::Pepper {
                assert!(
                    total_rows <= 10,
                    "{:?}: limit should have <= 10 rows, got {}",
                    engine,
                    total_rows
                );
            }

            // Test 5: Combined filter + projection + limit
            // Note: Arrow and Vortex engines don't support filter/limit pushdown
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
            if engine != Engine::Arrow && engine != Engine::Pepper {
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

            // Pepper may not preserve nulls properly yet, so skip this check for Pepper
            if engine != Engine::Pepper {
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
            }
        })
        .await;
    }

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
    async fn test_delete_operations() {
        run_compat_test(|engine, table, _mode, _test_env| async move {
            // Skip engines that don't support deletion
            if engine == Engine::Arrow || engine == Engine::Pepper {
                return;
            }

            let ctx = SessionContext::new();
            let schema = test_schema(Some(engine));

            // Insert test data - 50 records
            let data = generate_test_data(Arc::clone(&schema), 50, 0);
            insert_test_data(&table, &ctx, data).await;

            // Get deletion provider
            let table = get_deletion_provider(table).expect("should support deletion");

            // Delete rows where id > 3 (should delete ids 4-49, which is 46 rows)
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

            assert_eq!(
                actual.value(0),
                46,
                "{:?}: should delete 46 rows (ids 4-49)",
                engine
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_null_handling() {
        run_compat_test(|engine, table, _mode, _test_env| async move {
            let ctx = SessionContext::new();
            let schema = test_schema(Some(engine));

            // Insert 3 records with nulls in the value column
            let data = generate_test_data(Arc::clone(&schema), 3, 0);

            // Transform data to match table schema if needed (e.g., for Vortex type conversions)
            let table_schema = table.schema();
            let transformed_data = if data.schema() == table_schema {
                data
            } else {
                transform_batch_to_schema(&data, Arc::clone(&table_schema))
                    .expect("data transformation should succeed")
            };

            let exec = MockExec::new(vec![Ok(transformed_data)], Arc::clone(&table_schema));

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

            // generate_test_data creates nulls at every 10th position (i % 10 == 0)
            // With 3 records (indices 0, 1, 2), only index 0 will be null
            let offset = 0;

            assert!(
                value_col.is_null(offset),
                "{:?}: row {} should be null (0 % 10 == 0)",
                engine,
                offset
            );
            assert!(
                !value_col.is_null(offset + 1),
                "{:?}: row {} should not be null",
                engine,
                offset + 1
            );
            assert!(
                !value_col.is_null(offset + 2),
                "{:?}: row {} should not be null",
                engine,
                offset + 2
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_boolean_values() {
        run_compat_test(|engine, _table, _mode, test_env| {
            let metadata_dir = test_env.metadata_dir();
            async move {
                let ctx = SessionContext::new();
                let schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("active", DataType::Boolean, false),
                ]));

                let df_schema =
                    ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

                // Create location for file-based engines
                let location = if _mode == "file" {
                    format!(
                        "/tmp/spice_benchmark_{:?}_boolean_{}_{}.db",
                        engine,
                        std::process::id(),
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_nanos()
                    )
                } else {
                    String::new()
                };

                let mut options = HashMap::new();
                if _mode == "file" {
                    options.insert("file".to_string(), location.clone());
                }
                options.insert("mode".to_string(), _mode.to_string());

                let external_table = CreateExternalTable {
                    schema: df_schema,
                    name: TableReference::bare(format!("test_bool_{:?}", engine)),
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

                let bool_table: Arc<dyn TableProvider> = match engine {
                    #[cfg(feature = "sqlite")]
                    Engine::Sqlite => {
                        use crate::dataaccelerator::sqlite::SqliteAccelerator;
                        SqliteAccelerator::new()
                            .create_external_table(external_table, None, Vec::new())
                            .await
                            .expect("SQLite table should be created")
                    }
                    #[cfg(feature = "turso")]
                    Engine::Turso => {
                        use crate::dataaccelerator::turso::TursoAccelerator;
                        TursoAccelerator::new()
                            .create_external_table(external_table, None, Vec::new())
                            .await
                            .expect("Turso table should be created")
                    }
                    #[cfg(feature = "duckdb")]
                    Engine::DuckDB => {
                        use crate::dataaccelerator::duckdb::DuckDBAccelerator;
                        DuckDBAccelerator::new()
                            .create_external_table(external_table, None, Vec::new())
                            .await
                            .expect("DuckDB table should be created")
                    }
                    Engine::Arrow => {
                        use crate::dataaccelerator::arrow::ArrowAccelerator;
                        ArrowAccelerator::new()
                            .create_external_table(external_table, None, Vec::new())
                            .await
                            .expect("Arrow table should be created")
                    }
                    #[cfg(all(feature = "pepper", not(windows)))]
                    Engine::Pepper => {
                        use crate::component::dataset::builder::DatasetBuilder;
                        use crate::dataaccelerator::pepper::PepperAccelerator; // Clean up any existing files and metadata
                        if _mode == "file" && !location.is_empty() {
                            let test_dir = std::path::Path::new(&location);
                            if test_dir.exists() {
                                if let Ok(entries) = std::fs::read_dir(test_dir) {
                                    for entry in entries.flatten() {
                                        let path = entry.path();
                                        if path.extension().and_then(|s| s.to_str())
                                            == Some("pepper")
                                        {
                                            let _ = std::fs::remove_file(&path);
                                        }
                                    }
                                }
                            } else {
                                let _ = std::fs::create_dir_all(test_dir);
                            }

                            // Clean up Pepper metadata
                            // Use test environment's metadata directory
                            let pepper_db_path = format!("{}/pepper.db", metadata_dir);
                            if std::path::Path::new(&pepper_db_path).exists() {
                                let _ = std::fs::remove_file(&pepper_db_path);
                            }
                        }

                        let test_app_obj = app::AppBuilder::new("test").build();
                        let test_app = Arc::new(test_app_obj.clone());
                        let test_runtime = Arc::new(
                            crate::Runtime::builder()
                                .with_app(test_app_obj)
                                .build()
                                .await,
                        );

                        let dataset_name = format!("test_bool_{:?}", engine);
                        let mut dataset =
                            match DatasetBuilder::try_new(dataset_name.clone(), &dataset_name)
                                .expect("Failed to create dataset builder")
                                .with_app(Arc::clone(&test_app))
                                .with_runtime(Arc::clone(&test_runtime))
                                .build()
                            {
                                Ok(ds) => ds,
                                Err(e) => {
                                    panic!("Failed to create dataset: {}", e);
                                }
                            };

                        let mut params = HashMap::new();
                        if _mode == "file" {
                            params.insert("pepper_file_path".to_string(), location.clone());
                        }
                        // Use test environment's metadata directory for Pepper
                        params.insert("pepper_metadata_dir".to_string(), metadata_dir.clone());
                        params.insert("unsupported_type_action".to_string(), "error".to_string());

                        dataset.acceleration = Some(Acceleration {
                            enabled: true,
                            mode: if _mode == "file" {
                                Mode::File
                            } else {
                                Mode::Memory
                            },
                            engine: Engine::Pepper,
                            params,
                            ..Acceleration::default()
                        });

                        PepperAccelerator::new()
                            .create_external_table(external_table, Some(&dataset), Vec::new())
                            .await
                            .expect("Vortex table should be created")
                    }
                    _ => panic!("Unsupported engine: {:?}", engine),
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

                // Cleanup
                if _mode == "file" && !location.is_empty() {
                    let _ = std::fs::remove_file(&location);
                }
            }
        })
        .await;
    }

    #[tokio::test]
    async fn test_empty_result_set() {
        run_compat_test(|engine, _table, _mode, _test_env| async move {
            let ctx = SessionContext::new();

            // Query empty table
            let scan = _table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

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
        run_compat_test(|engine, table, _mode, _test_env| async move {
            let ctx = SessionContext::new();
            let schema = test_schema(Some(engine));

            // Insert 10 records for testing filters
            let data = generate_test_data(Arc::clone(&schema), 10, 0);
            insert_test_data(&table, &ctx, data).await;

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
            // Arrow and Vortex don't support filter pushdown, so they return all rows
            // IDs are 0-9, so id > 5 gives IDs 6,7,8,9 = 4 rows
            let expected_rows = if engine == Engine::Arrow || engine == Engine::Pepper {
                10
            } else {
                4
            };
            assert_eq!(
                total_rows, expected_rows,
                "{:?}: should have {} rows with id > 5",
                engine, expected_rows
            );

            // Test 2: Filter with less than predicate (id < 3)
            // IDs are 0-9, so id < 3 gives IDs 0,1,2 = 3 rows
            let filter = col("id").lt(lit(3_i64));
            let scan = table
                .scan(&ctx.state(), None, &[filter], None)
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            // Arrow and Vortex don't support filter pushdown, so they return all rows
            let expected_rows = if engine == Engine::Arrow || engine == Engine::Pepper {
                10
            } else {
                3
            };
            assert_eq!(
                total_rows, expected_rows,
                "{:?}: should have {} rows with id < 3",
                engine, expected_rows
            );

            // Test 3: Filter with equality predicate (id == 5)
            let filter = col("id").eq(lit(5_i64));
            let scan = table
                .scan(&ctx.state(), None, &[filter], None)
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            // Arrow and Vortex don't support filter pushdown, so they return all rows
            let expected_rows = if engine == Engine::Arrow || engine == Engine::Pepper {
                10
            } else {
                1
            };
            assert_eq!(
                total_rows, expected_rows,
                "{:?}: should have {} row with id = 5",
                engine, expected_rows
            );

            // Test 4: Multiple filters (AND condition) - id > 3 AND id < 7
            // IDs are 0-9, so id > 3 AND id < 7 gives IDs 4,5,6 = 3 rows
            let filter1 = col("id").gt(lit(3_i64));
            let filter2 = col("id").lt(lit(7_i64));
            let scan = table
                .scan(&ctx.state(), None, &[filter1, filter2], None)
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            // Arrow and Vortex don't support filter pushdown, so they return all rows
            let expected_rows = if engine == Engine::Arrow || engine == Engine::Pepper {
                10
            } else {
                3
            };
            assert_eq!(
                total_rows, expected_rows,
                "{:?}: should have {} rows with id > 3 AND id < 7",
                engine, expected_rows
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_projection_pushdown() {
        run_compat_test(|engine, table, _mode, _test_env| async move {
            let ctx = SessionContext::new();
            let schema = test_schema(Some(engine));

            // Insert 3 records for testing projection
            let data = generate_test_data(Arc::clone(&schema), 3, 0);
            insert_test_data(&table, &ctx, data).await;

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
        run_compat_test(|engine, table, _mode, _test_env| async move {
            let ctx = SessionContext::new();
            let schema = test_schema(Some(engine));

            // Insert 10 records for testing limit
            let data = generate_test_data(Arc::clone(&schema), 10, 0);
            insert_test_data(&table, &ctx, data).await;

            // Test limit of 3
            let scan = table
                .scan(&ctx.state(), None, &[], Some(3))
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            // Arrow doesn't support limit pushdown, so it returns all rows
            // Vortex (ListingTable) and DuckDB support limit pushdown
            let expected_rows = if engine == Engine::Arrow { 10 } else { 3 };
            assert_eq!(
                total_rows, expected_rows,
                "{:?}: should have {} rows with limit 3",
                engine, expected_rows
            );
            assert!(total_rows > 0, "{:?}: should have at least 1 row", engine);
        })
        .await;
    }

    #[tokio::test]
    async fn test_combined_filter_projection_limit() {
        run_compat_test(|engine, table, _mode, _test_env| async move {
            let ctx = SessionContext::new();
            let schema = test_schema(Some(engine));

            // Insert 10 records for testing combined operations
            let data = generate_test_data(Arc::clone(&schema), 10, 0);
            insert_test_data(&table, &ctx, data).await;

            // Test: projection (only name), filter (id > 3), and limit (2)
            let projection = Some(vec![1_usize]); // name column
            let filter = col("id").gt(lit(3_i64));
            let limit = Some(2);

            let scan = table
                .scan(&ctx.state(), projection.as_ref(), &[filter], limit)
                .await
                .expect("combined scan should be successful");

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
                .expect("combined scan successful");

            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            // Arrow doesn't support filter or limit pushdown, so it returns all rows
            // Vortex doesn't support filter pushdown but does support limit pushdown
            // DuckDB supports both filter and limit pushdown
            if engine == Engine::Arrow {
                // No pushdown - returns all 10 rows
                assert_eq!(
                    total_rows, 10,
                    "{:?}: should have 10 rows (no pushdown)",
                    engine
                );
            } else if engine == Engine::Pepper {
                // Limit pushdown only - id > 3 gives 6 rows, limit 2 gives 2 rows
                assert_eq!(
                    total_rows, 2,
                    "{:?}: should have 2 rows (limit pushdown only)",
                    engine
                );
            } else {
                // Both filter and limit pushdown - id > 3 gives 6 rows, limit 2 gives 2 rows
                assert_eq!(
                    total_rows, 2,
                    "{:?}: should have 2 rows (filter + limit pushdown)",
                    engine
                );
            }
            assert!(total_rows > 0, "{:?}: should have at least 1 row", engine);

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
    async fn test_complex_types_list_and_map() {
        run_compat_test(|engine, table, _mode, _test_env| async move {
            let ctx = SessionContext::new();
            let schema = test_schema(Some(engine));
            let table_schema = table.schema();

            // Check if List and Map columns exist in the schemas
            let has_list = schema.column_with_name("list_col").is_some();
            let has_map = schema.column_with_name("map_col").is_some();
            let table_has_map = table_schema.column_with_name("map_col").is_some();

            // Vortex supports List natively, but Map is excluded from schema
            if engine == Engine::Pepper {
                assert!(
                    has_list,
                    "{:?}: should have list_col (natively supported)",
                    engine
                );
                assert!(
                    !has_map,
                    "{:?}: should not have map_col in source schema (not yet supported)",
                    engine
                );
                assert!(
                    !table_has_map,
                    "{:?}: should not have map_col in table schema (not yet supported)",
                    engine
                );
            } else {
                // For other engines, ensure both List and Map are in the schema
                assert!(has_list, "{:?}: should have list_col in schema", engine);
                assert!(has_map, "{:?}: should have map_col in schema", engine);
            }

            // Insert test data with List and Map values
            let data = generate_test_data(Arc::clone(&schema), 20, 5); // 20 records, every 5th is null
            insert_test_data(&table, &ctx, data).await;

            // Scan and verify the data
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan should be successful");
            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 20, "{:?}: should have 20 rows", engine);

            // Verify List column exists and has correct type
            for batch in &results {
                if let Ok(list_col_idx) = batch.schema().index_of("list_col") {
                    let list_col = batch.column(list_col_idx);
                    assert!(
                        matches!(list_col.data_type(), DataType::List(_)),
                        "{:?}: list_col should be List type, got {:?}",
                        engine,
                        list_col.data_type()
                    );

                    // Verify we have some null and some non-null values
                    let null_count = list_col.null_count();
                    assert!(
                        null_count > 0,
                        "{:?}: list_col should have some nulls",
                        engine
                    );
                    assert!(
                        null_count < total_rows,
                        "{:?}: list_col should have some non-null values",
                        engine
                    );
                }

                // Verify Map column exists and has correct type (only for non-Vortex engines)
                if engine != Engine::Pepper
                    && let Ok(map_col_idx) = batch.schema().index_of("map_col")
                {
                    let map_col = batch.column(map_col_idx);

                    assert!(
                        matches!(map_col.data_type(), DataType::Map(_, _)),
                        "{:?}: map_col should be Map type, got {:?}",
                        engine,
                        map_col.data_type()
                    );

                    // Verify we have some null and some non-null values
                    let null_count = map_col.null_count();
                    assert!(
                        null_count > 0,
                        "{:?}: map_col should have some nulls",
                        engine
                    );
                    assert!(
                        null_count < total_rows,
                        "{:?}: map_col should have some non-null values",
                        engine
                    );
                }
            }

            if engine == Engine::Pepper {
                println!(
                    "✓ {:?}: List type works correctly (Map not yet supported)",
                    engine
                );
            } else {
                println!("✓ {:?}: List and Map types work correctly", engine);
            }
        })
        .await;
    }

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
    async fn test_overwrite_operations() {
        run_compat_test(|engine, table, _mode, _test_env| async move {
            let ctx = SessionContext::new();
            let schema = test_schema(Some(engine));

            // Insert initial data - 50 records
            let initial_data = generate_test_data(Arc::clone(&schema), 50, 0);
            insert_test_data(&table, &ctx, initial_data).await;

            // Verify initial data is there
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan should be successful");
            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");
            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                total_rows, 50,
                "{:?}: should have 50 rows after initial insert",
                engine
            );

            // Now INSERT OVERWRITE with different data - 30 records with offset 100
            let overwrite_data = generate_test_data(Arc::clone(&schema), 30, 100);
            let table_schema = table.schema();
            let transformed_data = if overwrite_data.schema() == table_schema {
                overwrite_data
            } else {
                transform_batch_to_schema(&overwrite_data, Arc::clone(&table_schema))
                    .expect("data transformation should succeed")
            };

            let exec_schema = transformed_data.schema();
            let exec = MockExec::new(vec![Ok(transformed_data)], exec_schema);
            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Overwrite)
                .await
                .expect("overwrite insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("overwrite insert successful");

            // Verify that old data is gone and new data is present
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan should be successful");
            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");
            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                total_rows, 30,
                "{:?}: should have 30 rows after overwrite (not 50 or 80)",
                engine
            );

            // Verify that the new data IDs are from the overwrite batch (offset 100)
            // IDs should be 100, 101, ..., 129
            for batch in &results {
                let id_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id should be Int64Array");

                for i in 0..id_col.len() {
                    if !id_col.is_null(i) {
                        let id_value = id_col.value(i);
                        assert!(
                            (100..130).contains(&id_value),
                            "{:?}: ID should be in range [100, 130), got {}",
                            engine,
                            id_value
                        );
                    }
                }
            }

            // Verify old data (IDs 0-49) is not present
            for batch in &results {
                let id_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id should be Int64Array");

                for i in 0..id_col.len() {
                    if !id_col.is_null(i) {
                        let id_value = id_col.value(i);
                        assert!(
                            !(0..50).contains(&id_value),
                            "{:?}: Old ID {} should not be present after overwrite",
                            engine,
                            id_value
                        );
                    }
                }
            }
        })
        .await;
    }

    // Helper function to format duration in a compact way
    fn format_duration_compact(d: std::time::Duration) -> String {
        let micros = d.as_micros();
        if micros < 1_000 {
            format!("{}µs", micros)
        } else if micros < 1_000_000 {
            format!("{:.2}ms", micros as f64 / 1_000.0)
        } else {
            format!("{:.2}s", d.as_secs_f64())
        }
    }

    // Helper function to print comparison table
    fn print_comparison_table(results: &[BenchmarkResults]) {
        if results.is_empty() {
            return;
        }

        type MetricFormatter = fn(&BenchmarkResults) -> String;
        type Metric<'a> = (&'a str, MetricFormatter);

        println!("\n");
        println!(
            "╔════════════════════════════════════════════════════════════════════════════════════════════╗"
        );
        println!(
            "║                          BENCHMARK COMPARISON TABLE                                        ║"
        );
        println!(
            "╠════════════════════════════════════════════════════════════════════════════════════════════╣"
        );

        // Group results by mode for easier comparison
        let mut by_mode: HashMap<String, Vec<&BenchmarkResults>> = HashMap::new();
        for result in results {
            by_mode.entry(result.mode.clone()).or_default().push(result);
        }

        for (mode, mode_results) in by_mode {
            println!("║ Mode: {:<84} ║", mode);
            println!(
                "╠════════════════════════════════════════════════════════════════════════════════════════════╣"
            );

            // Print header with engine names
            print!("║ {:20}", "Metric");
            for result in &mode_results {
                print!(" │ {:>15}", format!("{:?}", result.engine));
            }
            println!(" ║");
            println!(
                "╠════════════════════════════════════════════════════════════════════════════════════════════╣"
            );

            // Print configuration
            print!("║ {:20}", "Records/iteration");
            for result in &mode_results {
                print!(" │ {:>15}", format!("{}", result.num_records));
            }
            println!(" ║");

            print!("║ {:20}", "Iterations");
            for result in &mode_results {
                print!(" │ {:>15}", format!("{}", result.num_iterations));
            }
            println!(" ║");
            println!(
                "╠════════════════════════════════════════════════════════════════════════════════════════════╣"
            );

            // Insert Performance
            println!(
                "║ {:20}                                                                        ║",
                "INSERT PERFORMANCE"
            );
            println!(
                "╟────────────────────────────────────────────────────────────────────────────────────────────╢"
            );

            let metrics: [Metric<'static>; 7] = [
                (
                    "Min",
                    (|r: &BenchmarkResults| format_duration_compact(r.min_insert))
                        as MetricFormatter,
                ),
                (
                    "P90",
                    (|r: &BenchmarkResults| format_duration_compact(r.p90_insert))
                        as MetricFormatter,
                ),
                (
                    "P95",
                    (|r: &BenchmarkResults| format_duration_compact(r.p95_insert))
                        as MetricFormatter,
                ),
                (
                    "P99",
                    (|r: &BenchmarkResults| format_duration_compact(r.p99_insert))
                        as MetricFormatter,
                ),
                (
                    "P99.9",
                    (|r: &BenchmarkResults| format_duration_compact(r.p99_9_insert))
                        as MetricFormatter,
                ),
                (
                    "Max",
                    (|r: &BenchmarkResults| format_duration_compact(r.max_insert))
                        as MetricFormatter,
                ),
                (
                    "P95 rec/sec",
                    (|r: &BenchmarkResults| format!("{:.0}", r.p95_insert_rec_per_sec))
                        as MetricFormatter,
                ),
            ];

            for (label, formatter) in metrics {
                print!("║ {:20}", label);
                for result in &mode_results {
                    print!(" │ {:>15}", formatter(result));
                }
                println!(" ║");
            }

            println!(
                "╠════════════════════════════════════════════════════════════════════════════════════════════╣"
            );
            println!(
                "║ {:20}                                                                        ║",
                "QUERY PERFORMANCE"
            );
            println!(
                "╟────────────────────────────────────────────────────────────────────────────────────────────╢"
            );

            let query_metrics: [Metric<'static>; 7] = [
                (
                    "Min",
                    (|r: &BenchmarkResults| format_duration_compact(r.min_query))
                        as MetricFormatter,
                ),
                (
                    "P90",
                    (|r: &BenchmarkResults| format_duration_compact(r.p90_query))
                        as MetricFormatter,
                ),
                (
                    "P95",
                    (|r: &BenchmarkResults| format_duration_compact(r.p95_query))
                        as MetricFormatter,
                ),
                (
                    "P99",
                    (|r: &BenchmarkResults| format_duration_compact(r.p99_query))
                        as MetricFormatter,
                ),
                (
                    "P99.9",
                    (|r: &BenchmarkResults| format_duration_compact(r.p99_9_query))
                        as MetricFormatter,
                ),
                (
                    "Max",
                    (|r: &BenchmarkResults| format_duration_compact(r.max_query))
                        as MetricFormatter,
                ),
                (
                    "P95 rec/sec",
                    (|r: &BenchmarkResults| format!("{:.0}", r.p95_query_rec_per_sec))
                        as MetricFormatter,
                ),
            ];

            for (label, formatter) in query_metrics {
                print!("║ {:20}", label);
                for result in &mode_results {
                    print!(" │ {:>15}", formatter(result));
                }
                println!(" ║");
            }

            println!(
                "╠════════════════════════════════════════════════════════════════════════════════════════════╣"
            );
            println!(
                "║ {:20}                                                                        ║",
                "ROUNDTRIP (INSERT+QUERY)"
            );
            println!(
                "╟────────────────────────────────────────────────────────────────────────────────────────────╢"
            );

            let roundtrip_metrics: [Metric<'static>; 6] = [
                (
                    "Min",
                    (|r: &BenchmarkResults| format_duration_compact(r.min_roundtrip))
                        as MetricFormatter,
                ),
                (
                    "P90",
                    (|r: &BenchmarkResults| format_duration_compact(r.p90_roundtrip))
                        as MetricFormatter,
                ),
                (
                    "P95",
                    (|r: &BenchmarkResults| format_duration_compact(r.p95_roundtrip))
                        as MetricFormatter,
                ),
                (
                    "P99",
                    (|r: &BenchmarkResults| format_duration_compact(r.p99_roundtrip))
                        as MetricFormatter,
                ),
                (
                    "P99.9",
                    (|r: &BenchmarkResults| format_duration_compact(r.p99_9_roundtrip))
                        as MetricFormatter,
                ),
                (
                    "Max",
                    (|r: &BenchmarkResults| format_duration_compact(r.max_roundtrip))
                        as MetricFormatter,
                ),
            ];

            for (label, formatter) in roundtrip_metrics {
                print!("║ {:20}", label);
                for result in &mode_results {
                    print!(" │ {:>15}", formatter(result));
                }
                println!(" ║");
            }

            println!(
                "╚════════════════════════════════════════════════════════════════════════════════════════════╝"
            );
            println!();
        }
    }

    // Structure to hold benchmark results for comparison
    #[derive(Debug, Clone)]
    struct BenchmarkResults {
        engine: Engine,
        mode: String,
        num_records: usize,
        num_iterations: usize,
        // Insert metrics
        min_insert: std::time::Duration,
        p90_insert: std::time::Duration,
        p95_insert: std::time::Duration,
        p99_insert: std::time::Duration,
        p99_9_insert: std::time::Duration,
        max_insert: std::time::Duration,
        p95_insert_rec_per_sec: f64,
        // Query metrics
        min_query: std::time::Duration,
        p90_query: std::time::Duration,
        p95_query: std::time::Duration,
        p99_query: std::time::Duration,
        p99_9_query: std::time::Duration,
        max_query: std::time::Duration,
        p95_query_rec_per_sec: f64,
        // Roundtrip metrics
        min_roundtrip: std::time::Duration,
        p90_roundtrip: std::time::Duration,
        p95_roundtrip: std::time::Duration,
        p99_roundtrip: std::time::Duration,
        p99_9_roundtrip: std::time::Duration,
        max_roundtrip: std::time::Duration,
    }

    #[tokio::test]
    #[ignore = "Run with --ignored flag: cargo test --features sqlite,turso,duckdb,pepper -- --ignored --nocapture benchmark_roundtrip"]
    async fn benchmark_roundtrip() {
        use std::sync::Mutex;
        use std::time::Instant;

        // Collect all results for comparison
        let all_results = Arc::new(Mutex::new(Vec::new()));

        run_compat_test(|engine, table, mode, _test_env| {
            let all_results = Arc::clone(&all_results);
            async move {
                let ctx = SessionContext::new();
                let schema = test_schema(Some(engine));

                // Memory mode has limitations, file mode can handle much more
                // Turso has tighter page cache limits than other databases due to the comprehensive test schema
                // Note: mode string may include timestamp format like "memory, timestamp_format=rfc3339"
                let is_memory = mode.starts_with("memory");
                let is_file = mode.starts_with("file");

                let (num_records, num_iterations) = match (engine, is_memory, is_file) {
                    #[cfg(feature = "turso")]
                    (Engine::Turso, true, _) => (100, 3), // 300 total records (very limited due to page cache)
                    #[cfg(feature = "turso")]
                    (Engine::Turso, _, true) => (1_000, 10), // 10K total records (reduced due to complex schema)
                    (_, true, _) => (100_000, 10), // 1M total records
                    (_, _, true) => (1_000_000, 10), // 10M total records
                    _ => (10_000, 10),             // Fallback
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
                    let expected_rows = num_records * (iteration + 1);
                    assert_eq!(
                        total_rows, expected_rows,
                        "{:?}: iteration {}: should have {} total rows",
                        engine, iteration, expected_rows
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
                let p90_insert = percentile(&sorted_insert, 0.90);
                let p95_insert = percentile(&sorted_insert, 0.95);
                let p99_insert = percentile(&sorted_insert, 0.99);
                let p99_9_insert = percentile(&sorted_insert, 0.999);
                let max_insert = sorted_insert[sorted_insert.len() - 1];

                let min_query = sorted_query[0];
                let p90_query = percentile(&sorted_query, 0.90);
                let p95_query = percentile(&sorted_query, 0.95);
                let p99_query = percentile(&sorted_query, 0.99);
                let p99_9_query = percentile(&sorted_query, 0.999);
                let max_query = sorted_query[sorted_query.len() - 1];

                // Calculate round-trip percentiles
                let mut roundtrip_times: Vec<std::time::Duration> = insert_times
                    .iter()
                    .zip(query_times.iter())
                    .map(|(i, q)| *i + *q)
                    .collect();
                roundtrip_times.sort();
                let min_roundtrip = roundtrip_times[0];
                let p90_roundtrip = percentile(&roundtrip_times, 0.90);
                let p95_roundtrip = percentile(&roundtrip_times, 0.95);
                let p99_roundtrip = percentile(&roundtrip_times, 0.99);
                let p99_9_roundtrip = percentile(&roundtrip_times, 0.999);
                let max_roundtrip = roundtrip_times[roundtrip_times.len() - 1];

                let p95_insert_rec_per_sec =
                    num_records as f64 / percentile(&sorted_insert, 0.95).as_secs_f64();
                let p95_query_rec_per_sec = (num_records * num_iterations) as f64
                    / percentile(&sorted_query, 0.95).as_secs_f64();

                // Store results for comparison
                let results = BenchmarkResults {
                    engine,
                    mode: mode.clone(),
                    num_records,
                    num_iterations,
                    min_insert,
                    p90_insert,
                    p95_insert,
                    p99_insert,
                    p99_9_insert,
                    max_insert,
                    p95_insert_rec_per_sec,
                    min_query,
                    p90_query,
                    p95_query,
                    p99_query,
                    p99_9_query,
                    max_query,
                    p95_query_rec_per_sec,
                    min_roundtrip,
                    p90_roundtrip,
                    p95_roundtrip,
                    p99_roundtrip,
                    p99_9_roundtrip,
                    max_roundtrip,
                };

                match all_results.lock() {
                    Ok(mut guard) => guard.push(results),
                    Err(poisoned) => panic!("Failed to lock benchmark results: {poisoned}"),
                }
            }
        })
        .await;

        // Print comparison table
        let results = match all_results.lock() {
            Ok(guard) => guard,
            Err(poisoned) => panic!("Failed to lock benchmark results: {poisoned}"),
        };
        print_comparison_table(&results);
    }
}
