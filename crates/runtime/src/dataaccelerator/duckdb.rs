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

use async_trait::async_trait;
use data_components::poly::PolyTableProvider;
use datafusion::{
    catalog::TableProviderFactory, datasource::TableProvider, execution::context::SessionContext,
    logical_expr::CreateExternalTable,
};
use datafusion_table_providers::{
    duckdb::{write::DuckDBTableWriter, DuckDBTableProviderFactory},
    sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};
use duckdb::AccessMode;
use snafu::prelude::*;
use std::{any::Any, ffi::OsStr, sync::Arc};

use crate::{
    component::dataset::{
        acceleration::{Engine, Mode},
        Dataset,
    },
    datafusion::dialect::new_duckdb_dialect,
    make_spice_data_directory,
    parameters::ParameterSpec,
    spice_data_base_path, Runtime,
};

use super::{DataAccelerator, Error as DataAcceleratorError};

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
            // DuckDB accelerator uses params.duckdb_file for file connection
            duckdb_factory: DuckDBTableProviderFactory::new(AccessMode::ReadWrite)
                .with_dialect(new_duckdb_dialect()),
        }
    }

    /// Returns the `DuckDB` file path that would be used for a file-based `DuckDB` accelerator from this dataset
    pub fn duckdb_file_path(&self, dataset: &Dataset) -> Result<String> {
        if !dataset.is_file_accelerated() {
            Err(Error::InvalidConfiguration {
                detail: Arc::from("Dataset is not file accelerated"),
            })
        } else if let Some(acceleration) = dataset.acceleration.as_ref() {
            let mut params = acceleration.params.clone();
            params.insert("data_directory".to_string(), spice_data_base_path());

            if let Some(duckdb_file) = params.remove("duckdb_file") {
                params.insert("duckdb_open".to_string(), duckdb_file.to_string());
            }

            self.duckdb_factory
                .duckdb_file_path("accelerated_duckdb", &mut params)
                .map_err(|err| Error::InvalidConfiguration {
                    detail: Arc::from(err.to_string()),
                })
        } else {
            unreachable!("Expected dataset to have acceleration parameters, but none were found")
        }
    }

    /// Returns an existing `DuckDB` connection pool for the given dataset, or creates a new one if it doesn't exist.
    pub async fn get_shared_pool(&self, dataset: &Dataset) -> Result<DuckDbConnectionPool> {
        let duckdb_file = self.duckdb_file_path(dataset);

        let acceleration = dataset
            .acceleration
            .as_ref()
            .context(AccelerationNotEnabledSnafu {
                dataset: dataset.name.to_string(),
            })?;

        let pool = match (duckdb_file, acceleration.mode) {
            (Ok(duckdb_file), Mode::File) => self
                .duckdb_factory
                .get_or_init_file_instance(duckdb_file)
                .await
                .boxed()
                .context(AccelerationCreationFailedSnafu)?,
            (_, Mode::Memory) => self
                .duckdb_factory
                .get_or_init_memory_instance()
                .await
                .boxed()
                .context(AccelerationCreationFailedSnafu)?,
            (Err(e), Mode::File) => {
                return Err(Error::InvalidConfiguration {
                    detail: Arc::from(e.to_string()),
                })
            }
        };

        Ok(pool)
    }
}

impl Default for DuckDBAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::accelerator("file"),
    ParameterSpec::runtime("file_watcher"),
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

    fn file_path(&self, dataset: &Dataset) -> Result<String, DataAcceleratorError> {
        self.duckdb_file_path(dataset)
            .map_err(|e| DataAcceleratorError::InvalidConfiguration { msg: e.to_string() })
    }

    fn is_initialized(&self, dataset: &Dataset) -> bool {
        if !dataset.is_file_accelerated() {
            return true; // memory mode DuckDB is always initialized
        }

        // otherwise, we're initialized if the file exists
        self.has_existing_file(dataset)
    }

    async fn init(
        &self,
        dataset: &Dataset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !dataset.is_file_accelerated() {
            return Ok(());
        }

        let path = self.file_path(dataset)?;

        if let Some(acceleration) = &dataset.acceleration {
            if !acceleration.params.contains_key("duckdb_file") {
                make_spice_data_directory().map_err(|err| {
                    Error::AccelerationInitializationFailed { source: err.into() }
                })?;
            } else if !self.is_valid_file(dataset) {
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

            self.get_shared_pool(dataset).await?;
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
        if let Some(duckdb_file) = cmd.options.remove("file") {
            cmd.options
                .insert("open".to_string(), duckdb_file.to_string());
        }

        if let Some(this_dataset) = dataset {
            if this_dataset.is_file_accelerated() {
                // If the user didn't specify a DuckDB file and this is a file-mode DuckDB,
                // then use the shared DuckDB file `accelerated_duckdb.db`
                if !cmd.options.contains_key("open") {
                    let duckdb_file = self.duckdb_file_path(this_dataset)?;
                    cmd.options.insert("open".to_string(), duckdb_file);
                }

                if let Some(app) = &this_dataset.app {
                    let datasets =
                        Runtime::get_initialized_datasets(app, crate::LogErrors(false)).await;
                    let self_path = self.file_path(this_dataset)?;
                    let attach_databases =
                        datasets
                            .iter()
                            .filter_map(|other_dataset| {
                                if other_dataset.acceleration.as_ref().is_some_and(|a| {
                                    a.engine == Engine::DuckDB && a.mode == Mode::File
                                }) {
                                    if **other_dataset == *this_dataset {
                                        None
                                    } else {
                                        let other_path = self.file_path(other_dataset);
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
            }
        }

        let ctx = SessionContext::new();
        let table_provider = TableProviderFactory::create(&self.duckdb_factory, &ctx.state(), &cmd)
            .await
            .context(UnableToCreateTableSnafu)
            .boxed()?;

        let Some(duckdb_writer) = table_provider.as_any().downcast_ref::<DuckDBTableWriter>()
        else {
            unreachable!("DuckDBTableWriter should be returned from DuckDBTableProviderFactory")
        };

        let read_provider = Arc::clone(&duckdb_writer.read_provider);
        let duckdb_writer = Arc::new(duckdb_writer.clone());
        let cloned_writer = Arc::clone(&duckdb_writer);

        Ok(Arc::new(PolyTableProvider::new(
            cloned_writer,
            duckdb_writer,
            read_provider,
        )))
    }

    fn prefix(&self) -> &'static str {
        "duckdb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{Int64Array, RecordBatch, StringArray, TimestampSecondArray, UInt64Array},
        datatypes::{DataType, Schema},
    };
    use data_components::delete::get_deletion_provider;
    use datafusion::{
        common::{Constraints, TableReference, ToDFSchema},
        execution::context::SessionContext,
        logical_expr::{cast, col, dml::InsertOp, lit, CreateExternalTable},
        physical_plan::collect,
        scalar::ScalarValue,
    };
    use datafusion_table_providers::util::test::MockExec;

    use crate::component::dataset::acceleration::Acceleration;
    use crate::component::dataset::acceleration::{Engine, Mode};
    use crate::component::dataset::Dataset;
    use crate::dataaccelerator::{duckdb::DuckDBAccelerator, DataAccelerator};

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::unreadable_literal)]
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
            temporary: false,
        };
        let duckdb_accelerator = DuckDBAccelerator::new();
        let ctx = SessionContext::new();
        let table = duckdb_accelerator
            .create_external_table(&external_table, None)
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

        let delete_table = get_deletion_provider(Arc::clone(&table))
            .expect("table should be returned as deletion provider");

        let filter = cast(
            col("time_in_string"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        .lt(lit(ScalarValue::TimestampMillisecond(
            Some(1354360272000),
            None,
        )));
        let plan = delete_table
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
        let plan = delete_table
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

        let delete_table = get_deletion_provider(Arc::clone(&table))
            .expect("table should be returned as deletion provider");

        let filter = col("time").lt(lit(ScalarValue::TimestampMillisecond(
            Some(1354360272000),
            None,
        )));
        let plan = delete_table
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

        let insertion = table
            .insert_into(&ctx.state(), exec, InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        let delete_table = get_deletion_provider(Arc::clone(&table))
            .expect("table should be returned as deletion provider");

        let filter = col("time_with_zone").lt(lit(ScalarValue::TimestampMillisecond(
            Some(1354360272000),
            None,
        )));
        let plan = delete_table
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
    }

    #[tokio::test]
    async fn test_duckdb_file_initialization() {
        let mut dataset = Dataset::try_new(
            "duckdb_file_accelerator_init".to_string(),
            "duckdb_file_accelerator_init",
        )
        .expect("dataset should be created");

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
        assert!(accelerator.file_path(&dataset).is_ok());

        let path = accelerator.file_path(&dataset).expect("path should exist");
        assert!(std::path::Path::new(&path).exists());

        // cleanup
        std::fs::remove_file(&path).expect("file should be removed");
    }
}
