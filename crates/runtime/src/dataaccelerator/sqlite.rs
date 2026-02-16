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

use std::sync::Arc;

use crate::{
    component::dataset::acceleration::{Engine, Mode},
    dataaccelerator::{FilePathError, snapshots::download_snapshot_if_needed},
    datafusion::udf::deny_spice_specific_functions,
    make_spice_data_directory,
    parameters::ParameterSpec,
    register_data_accelerator, spice_data_base_path,
};
use async_trait::async_trait;
use data_components::poly::PolyTableProvider;
use datafusion::{
    catalog::TableProviderFactory, datasource::TableProvider, execution::context::SessionContext,
    logical_expr::CreateExternalTable,
};
use datafusion_table_providers::{
    sql::db_connection_pool::sqlitepool::SqliteConnectionPool,
    sqlite::{SqliteTableProviderFactory, write::SqliteTableWriter},
};
use runtime_acceleration::snapshot::AccelerationEngine;
use runtime_table_partition::expression::PartitionedBy;
use rusqlite::ffi::{sqlite3_auto_extension, sqlite3_decimal_init};
use snafu::prelude::*;
use std::{any::Any, ffi::OsStr, os::raw::c_char, path::PathBuf, time::Duration};

use super::{AccelerationSource, BootstrapStatus, DataAccelerator, upsert_dedup};

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

    #[snafu(display(
        "The \"sqlite_file\" acceleration parameter has an invalid extension. Expected one of \"{valid_extensions}\" but got \"{extension}\"."
    ))]
    InvalidFileExtension {
        valid_extensions: String,
        extension: String,
    },

    #[snafu(display("The \"sqlite_file\" acceleration parameter value is a directory."))]
    InvalidFileIsDirectory,

    #[snafu(display(
        "The \"busy_timeout\" acceleration parameter value must be a valid duration."
    ))]
    InvalidBusyTimeoutValue,

    #[snafu(display("Acceleration not enabled for dataset: {dataset}"))]
    AccelerationNotEnabled { dataset: Arc<str> },

    #[snafu(display("Invalid SQLite acceleration configuration: {detail}"))]
    InvalidConfiguration { detail: Arc<str> },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SqliteAccelerator {
    sqlite_factory: SqliteTableProviderFactory,
}

impl Default for SqliteAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

impl SqliteAccelerator {
    /// Wrapper to align the decimal extension signature with `sqlite3_auto_extension`.
    ///
    /// SAFETY: The wrapper only casts the error message pointer to match the expected mutability.
    unsafe extern "C" fn sqlite3_decimal_init_wrapper(
        db: *mut rusqlite::ffi::sqlite3,
        error_message: *mut *mut c_char,
        api: *const rusqlite::ffi::sqlite3_api_routines,
    ) -> std::os::raw::c_int {
        unsafe { sqlite3_decimal_init(db, error_message.cast(), api) }
    }

    #[must_use]
    pub fn new() -> Self {
        // Initialize the decimal extension for SQLite
        //
        // SAFETY: This is safe because sqlite3_decimal_init is a valid function pointer.
        unsafe {
            sqlite3_auto_extension(Some(Self::sqlite3_decimal_init_wrapper));
        }
        Self {
            sqlite_factory: SqliteTableProviderFactory::new()
                .with_batch_insert_use_prepared_statements(true)
                .with_decimal_between(true)
                .with_function_support(deny_spice_specific_functions()),
        }
    }

    /// Returns the `Sqlite` file path that would be used for a file-based `Sqlite` accelerator from this dataset
    pub fn sqlite_file_path(&self, source: &dyn AccelerationSource) -> Result<String> {
        if !source.is_file_accelerated() {
            Err(Error::InvalidConfiguration {
                detail: Arc::from("Dataset is not file accelerated"),
            })
        } else if let Some(acceleration) = source.acceleration() {
            let mut acceleration_params = acceleration.params.clone();

            acceleration_params.insert("data_directory".to_string(), spice_data_base_path());

            self.sqlite_factory
                .sqlite_file_path("accelerated", &acceleration_params)
                .map_err(|err| Error::InvalidConfiguration {
                    detail: Arc::from(err.to_string()),
                })
        } else {
            unreachable!("Expected dataset to have acceleration parameters, but none were found")
        }
    }

    /// Returns the `Sqlite` `busy_timeout` param that would be used for setting the `busy_timeout` in `Sqlite` accelerator for this dataset, default to 5000 milliseconds
    pub fn sqlite_busy_timeout(&self, source: &dyn AccelerationSource) -> Result<Duration> {
        if let Some(acceleration) = source.acceleration() {
            let acceleration_params = acceleration.params.clone();
            return self
                .sqlite_factory
                .sqlite_busy_timeout(&acceleration_params)
                .map_err(|_| InvalidBusyTimeoutValueSnafu.build());
        }
        Ok(Duration::from_millis(5000))
    }

    /// Returns an existing `SQLite` connection pool for the given dataset, or creates a new one if it doesn't exist.
    pub async fn get_shared_pool(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<SqliteConnectionPool> {
        let sqlite_file = self.sqlite_file_path(source)?;

        let acceleration = source.acceleration().context(AccelerationNotEnabledSnafu {
            dataset: source.name().to_string(),
        })?;

        let mode = match acceleration.mode {
            Mode::File | Mode::FileCreate => {
                datafusion_table_providers::sql::db_connection_pool::Mode::File
            }
            Mode::Memory => datafusion_table_providers::sql::db_connection_pool::Mode::Memory,
        };
        let file_path: Arc<str> = sqlite_file.into();
        let busy_timeout = self.sqlite_busy_timeout(source)?;

        let pool = self
            .sqlite_factory
            .get_or_init_instance(file_path, mode, busy_timeout)
            .await
            .boxed()
            .context(AccelerationCreationFailedSnafu)?;

        Ok(pool)
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("file"),
    ParameterSpec::runtime("busy_timeout"),
    ParameterSpec::runtime("file_watcher"),
];

#[async_trait]
impl DataAccelerator for SqliteAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn valid_file_extensions(&self) -> Vec<&'static str> {
        vec!["sqlite", "db"]
    }

    fn file_path(&self, source: &dyn AccelerationSource) -> Result<String, FilePathError> {
        self.sqlite_file_path(source)
            .map_err(|err| FilePathError::External {
                engine: Engine::Sqlite,
                source: err.into(),
            })
    }

    fn is_initialized(&self, source: &dyn AccelerationSource) -> bool {
        if !source.is_file_accelerated() {
            return true; // memory mode SQLite is always initialized
        }

        // otherwise, we're initialized if the file exists
        self.has_existing_file(source)
    }

    /// Initializes an `SQLite` database for the dataset
    /// If the dataset is not file-accelerated, this is a no-op
    /// This step is required for federation, as `SQLite` connections attach to all other configured `SQLite` databases.
    /// Federation then requires that all attached databases exist before dataset registration.
    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<BootstrapStatus, Box<dyn std::error::Error + Send + Sync>> {
        if !source.is_file_accelerated() {
            return Ok(BootstrapStatus::none());
        }

        let path = self.file_path(source)?;

        if let Some(acceleration) = source.acceleration() {
            if !acceleration.params.contains_key("sqlite_file") {
                make_spice_data_directory()
                    .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
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

            // If mode is FileCreate, delete the existing file to start fresh
            if acceleration.mode == Mode::FileCreate {
                let file_path = std::path::Path::new(&path);
                if file_path.exists() {
                    tracing::warn!(
                        "SQLite acceleration mode is 'file_create', removing existing file: {}",
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
                AccelerationEngine::Sqlite,
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
        partition_by: Vec<PartitionedBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        ensure!(
            partition_by.is_empty(),
            super::InvalidConfigurationSnafu {
                msg: "Sqlite data accelerator does not support the `partition_by` parameter but it was provided".to_string()
            }
        );

        if let Some(source) = source
            && source.is_file_accelerated()
        {
            // If the user didn't specify a SQLite file and this is a file-mode SQLite,
            // then use the shared SQLite file `accelerated_sqlite.db`
            if !cmd.options.contains_key("file") {
                let sqlite_file = self.sqlite_file_path(source)?;
                cmd.options.insert("file".to_string(), sqlite_file);
            }

            let datasets = source
                .runtime()
                .get_initialized_datasets(&source.app(), crate::LogErrors(false))
                .await;
            let self_path = self.file_path(source)?;
            let attach_databases = datasets
                .iter()
                .filter_map(|other_dataset| {
                    if other_dataset.acceleration.as_ref().is_some_and(|a| {
                        a.engine == Engine::Sqlite
                            && matches!(a.mode, Mode::File | Mode::FileCreate)
                    }) {
                        if other_dataset.name() == source.name() {
                            None
                        } else {
                            let other_path = self.file_path(other_dataset.as_ref());
                            other_path.ok().filter(|p| p != &self_path)
                        }
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            if !attach_databases.is_empty() {
                cmd.options
                    .insert("attach_databases".to_string(), attach_databases.join(";"));
            }
        }

        let ctx = SessionContext::new();
        let table_provider = TableProviderFactory::create(&self.sqlite_factory, &ctx.state(), &cmd)
            .await
            .context(UnableToCreateTableSnafu)
            .boxed()?;

        let Some(sqlite_writer) = table_provider.as_any().downcast_ref::<SqliteTableWriter>()
        else {
            unreachable!("SqliteTableWriter should be returned from SqliteTableProviderFactory")
        };

        let read_provider = Arc::clone(&sqlite_writer.read_provider);
        let sqlite_writer = Arc::new(sqlite_writer.clone());

        // Wrap with upsert deduplication if needed
        let (write_provider, delete_provider) = upsert_dedup::wrap_with_upsert_dedup_if_needed(
            sqlite_writer,
            &cmd.options,
            cmd.constraints.clone(),
        );

        let table_provider = Arc::new(PolyTableProvider::new(
            write_provider,
            delete_provider,
            read_provider,
        ));

        Ok(table_provider)
    }

    fn prefix(&self) -> &'static str {
        "sqlite"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

register_data_accelerator!(Engine::Sqlite, SqliteAccelerator);

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::dataaccelerator::DataAccelerator;
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Schema},
    };
    use data_components::delete::get_deletion_provider;
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
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::dataaccelerator::sqlite::SqliteAccelerator;

    #[tokio::test]
    #[expect(clippy::unreadable_literal)]
    async fn test_round_trip_sqlite() {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("time_in_string", DataType::Utf8, false),
            arrow::datatypes::Field::new("time_int", DataType::Int64, false),
        ]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
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
        let ctx = SessionContext::new();
        let table = SqliteAccelerator::new()
            .create_external_table(external_table, None, vec![])
            .await
            .expect("table should be created");

        let arr1 = StringArray::from(vec![
            "1970-01-01",
            "2012-12-01T11:11:11Z",
            "2012-12-01T11:11:12Z",
        ]);
        let arr3 = Int64Array::from(vec![0, 1354360271, 1354360272]);
        let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr1), Arc::new(arr3)])
            .expect("data should be created");

        let exec = MockExec::new(vec![Ok(data)], schema);

        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        let table =
            get_deletion_provider(table).expect("table should be returned as deletion provider");

        let filter = cast(
            col("time_in_string"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        .lt(lit(ScalarValue::TimestampMillisecond(
            Some(1354360272000),
            None,
        )));
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
        // Expect 2 rows deleted: "1970-01-01" (epoch=0) and "2012-12-01T11:11:11Z" (1354360271000ms)
        // both are < 1354360272000ms
        let expected = UInt64Array::from(vec![2]);
        assert_eq!(actual, &expected);

        let filter = col("time_int").lt(lit(1354360273));
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
        // Only 1 row remains after the first delete (time_int=1354360272),
        // which matches time_int < 1354360273
        let expected = UInt64Array::from(vec![1]);
        assert_eq!(actual, &expected);
    }

    #[tokio::test]
    async fn test_sqlite_file_initialization() {
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new(
            "sqlite_file_accelerator_init".to_string(),
            "sqlite_file_accelerator_init",
        )
        .expect("Failed to create builder")
        .with_app(Arc::new(app))
        .with_runtime(Arc::new(rt))
        .build()
        .expect("Failed to build dataset");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::Sqlite,
            mode: Mode::File,
            ..Default::default()
        });

        let accelerator = SqliteAccelerator::new();
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
}
