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
use runtime_table_partition::expression::{PartitionBy, partition_by_expressions};
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
            None
        } else {
            Some(
                partition_by_expressions(&acceleration_settings.partition_by, &ctx, &df_schema)
                    .map_err(|e| Error::AccelerationCreationFailed { source: e.into() })?,
            )
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
        partition_by: Option<PartitionBy>,
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

#[cfg(all(test, any(feature = "sqlite", feature = "turso")))]
mod sqlite_compat_tests {
    //! Shared compatibility test suite for SQLite and Turso accelerators.
    //! These tests ensure both accelerators behave identically for common operations.

    use crate::component::dataset::acceleration::Engine;
    use crate::dataaccelerator::DataAccelerator;
    use ::arrow::{
        array::{
            Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
        },
        datatypes::{DataType, Field, Schema},
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

    /// Test helper that runs the same test logic against both SQLite and Turso
    async fn run_compat_test<F, Fut>(test_fn: F)
    where
        F: Fn(Engine, Arc<dyn TableProvider>) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let engines = vec![
            #[cfg(feature = "sqlite")]
            Engine::Sqlite,
            #[cfg(feature = "turso")]
            Engine::Turso,
        ];

        for engine in engines {
            println!("Testing with engine: {:?}", engine);

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, true),
            ]));

            let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
            let external_table = CreateExternalTable {
                schema: df_schema,
                name: TableReference::bare(format!("test_table_{:?}", engine)),
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

            let table = match engine {
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
                _ => panic!("Unsupported engine for this test"),
            };

            test_fn(engine, table).await;
        }
    }

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
    async fn test_basic_insert_and_query() {
        run_compat_test(|engine, table| async move {
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, true),
            ]));

            // Insert test data
            let id_array = Int64Array::from(vec![1, 2, 3]);
            let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
            let value_array = Float64Array::from(vec![Some(1.5), Some(2.5), None]);

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

            // Query back the data
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan should be successful");

            let results = collect(scan, ctx.task_ctx())
                .await
                .expect("scan successful");

            assert_eq!(results.len(), 1, "{:?}: should have 1 batch", engine);
            let batch = &results[0];
            assert_eq!(batch.num_rows(), 3, "{:?}: should have 3 rows", engine);
            assert_eq!(
                batch.num_columns(),
                3,
                "{:?}: should have 3 columns",
                engine
            );

            // Verify data
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id should be Int64Array");
            assert_eq!(id_col.value(0), 1);
            assert_eq!(id_col.value(1), 2);
            assert_eq!(id_col.value(2), 3);

            let name_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name should be StringArray");
            assert_eq!(name_col.value(0), "Alice");
            assert_eq!(name_col.value(1), "Bob");
            assert_eq!(name_col.value(2), "Charlie");

            let value_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("value should be Float64Array");
            assert_eq!(value_col.value(0), 1.5);
            assert_eq!(value_col.value(1), 2.5);
            assert!(value_col.is_null(2));
        })
        .await;
    }

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
    async fn test_delete_operations() {
        run_compat_test(|engine, table| async move {
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, true),
            ]));

            // Insert test data
            let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
            let name_array = StringArray::from(vec!["A", "B", "C", "D", "E"]);
            let value_array = Float64Array::from(vec![
                Some(10.0),
                Some(20.0),
                Some(30.0),
                Some(40.0),
                Some(50.0),
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
        run_compat_test(|engine, table| async move {
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
        run_compat_test(|engine, _table| async move {
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

            let bool_table = match engine {
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
        run_compat_test(|engine, _table| async move {
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
        run_compat_test(|engine, table| async move {
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
        run_compat_test(|engine, table| async move {
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
        run_compat_test(|engine, table| async move {
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
        run_compat_test(|engine, table| async move {
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
}
