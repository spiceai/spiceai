/*
Copyright 2024 The Spice.ai OSS Authors

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

use async_trait::async_trait;
use data_components::poly::PolyTableProvider;
use datafusion::{
    catalog::TableProviderFactory, datasource::TableProvider, execution::context::SessionContext,
    logical_expr::CreateExternalTable,
};
use datafusion_table_providers::{
    sql::db_connection_pool::sqlitepool::SqliteConnectionPool,
    sqlite::{write::SqliteTableWriter, SqliteTableProviderFactory},
};
use snafu::prelude::*;
use std::{any::Any, ffi::OsStr, sync::Arc};

use crate::{
    component::dataset::{
        acceleration::{Engine, Mode},
        Dataset,
    },
    make_spice_data_directory,
    parameters::ParameterSpec,
    spice_data_base_path, Runtime,
};

use super::DataAccelerator;

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

    #[snafu(display("The \"sqlite_file\" acceleration parameter has an invalid extension. Expected one of \"{valid_extensions}\" but got \"{extension}\"."))]
    InvalidFileExtension {
        valid_extensions: String,
        extension: String,
    },

    #[snafu(display("The \"duckdb_file\" acceleration parameter is a directory."))]
    InvalidFileIsDirectory,
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SqliteAccelerator {
    sqlite_factory: SqliteTableProviderFactory,
}

impl SqliteAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sqlite_factory: SqliteTableProviderFactory::new(),
        }
    }

    /// Returns the `Sqlite` file path that would be used for a file-based `Sqlite` accelerator from this dataset
    #[must_use]
    pub fn sqlite_file_path(&self, dataset: &Dataset) -> Option<String> {
        if !dataset.is_file_accelerated() {
            return None;
        }

        let acceleration = dataset.acceleration.as_ref()?;
        let mut acceleration_params = acceleration.params.clone();

        acceleration_params.insert("data_directory".to_string(), spice_data_base_path());

        Some(
            self.sqlite_factory
                .sqlite_file_path(&dataset.name.to_string(), &acceleration_params),
        )
    }
}

impl Default for SqliteAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

const PARAMETERS: &[ParameterSpec] = &[ParameterSpec::accelerator("file")];

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

    fn file_path(&self, dataset: &Dataset) -> Option<String> {
        self.sqlite_file_path(dataset)
    }

    fn is_initialized(&self, dataset: &Dataset) -> bool {
        if !dataset.is_file_accelerated() {
            return true; // memory mode SQLite is always initialized
        }

        // otherwise, we're initialized if the file exists
        self.is_existing_file(dataset)
    }

    /// Initializes an SQLite database for the dataset
    /// If the dataset is not file-accelerated, this is a no-op
    /// This step is required for federation, as SQLite connections attach to all other configured SQLite databases.
    /// Federation then requires that all attached databases exist before dataset registration.
    async fn init(
        &self,
        dataset: &Dataset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !dataset.is_file_accelerated() {
            return Ok(());
        }

        let path = self.file_path(dataset);

        if let (Some(path), Some(acceleration)) = (&path, &dataset.acceleration) {
            if !acceleration.params.contains_key("sqlite_file") {
                make_spice_data_directory()
                    .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
            } else if !self.is_valid_file(dataset) {
                if std::path::Path::new(path).is_dir() {
                    return Err(Error::InvalidFileIsDirectory.into());
                }

                let extension = std::path::Path::new(path)
                    .extension()
                    .and_then(OsStr::to_str)
                    .unwrap_or("");

                return Err(Error::InvalidFileExtension {
                    valid_extensions: self.valid_file_extensions().join(","),
                    extension: extension.to_string(),
                }
                .into());
            }

            SqliteConnectionPool::init(path, acceleration.mode.to_string().as_str().into())
                .await
                .context(AccelerationInitializationFailedSnafu)?;
        }

        Ok(())
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        cmd: &CreateExternalTable,
        dataset: Option<&Dataset>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let mut cmd = cmd.clone();

        if let Some(this_dataset) = dataset {
            if let Some(app) = &this_dataset.app {
                let datasets =
                    Runtime::get_initialized_datasets(app, crate::LogErrors(false)).await;
                let attach_databases = datasets
                    .iter()
                    .filter_map(|other_dataset| {
                        if other_dataset.acceleration.as_ref().map_or(false, |a| {
                            a.engine == Engine::Sqlite && a.mode == Mode::File
                        }) {
                            if **other_dataset == *this_dataset {
                                None
                            } else {
                                self.file_path(other_dataset)
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
        let cloned_writer = Arc::clone(&sqlite_writer);

        Ok(Arc::new(PolyTableProvider::new(
            cloned_writer,
            sqlite_writer,
            read_provider,
        )))
    }

    fn prefix(&self) -> &'static str {
        "sqlite"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

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
        logical_expr::{cast, col, lit, CreateExternalTable},
        physical_plan::collect,
        scalar::ScalarValue,
    };
    use datafusion_table_providers::util::test::MockExec;

    use crate::component::dataset::acceleration::Acceleration;
    use crate::component::dataset::acceleration::{Engine, Mode};
    use crate::component::dataset::Dataset;
    use crate::dataaccelerator::sqlite::SqliteAccelerator;

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
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
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::empty(),
            column_defaults: HashMap::default(),
        };
        let ctx = SessionContext::new();
        let table = SqliteAccelerator::new()
            .create_external_table(&external_table, None)
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
            .insert_into(&ctx.state(), Arc::new(exec), false)
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
            .delete_from(&ctx.state(), &vec![filter])
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
        let expected = UInt64Array::from(vec![2]);
        assert_eq!(actual, &expected);

        let filter = col("time_int").lt(lit(1354360273));
        let plan = table
            .delete_from(&ctx.state(), &vec![filter])
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
        let expected = UInt64Array::from(vec![1]);
        assert_eq!(actual, &expected);
    }

    #[tokio::test]
    async fn test_sqlite_file_initialization() {
        let mut dataset = Dataset::try_new(
            "sqlite_file_accelerator_init".to_string(),
            "sqlite_file_accelerator_init",
        )
        .expect("dataset should be created");

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
        assert!(accelerator.file_path(&dataset).is_some());

        let path = accelerator.file_path(&dataset).expect("path should exist");
        assert!(std::path::Path::new(&path).exists());

        // cleanup
        std::fs::remove_file(&path).expect("file should be removed");
    }
}
