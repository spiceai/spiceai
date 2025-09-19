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

use crate::{
    App, Runtime,
    component::{
        dataset::{
            Dataset,
            acceleration::{Engine, Mode},
        },
        view::View,
    },
    datafusion::dialect::new_duckdb_dialect,
    make_spice_data_directory,
    parameters::ParameterSpec,
    spice_data_base_path,
};
use async_trait::async_trait;
use data_components::poly::PolyTableProvider;
use datafusion::{
    catalog::TableProviderFactory, datasource::TableProvider, execution::context::SessionContext,
    logical_expr::CreateExternalTable,
};
use datafusion_table_providers::{
    duckdb::{DuckDBSettingsRegistry, DuckDBTableProviderFactory, write::DuckDBTableWriter},
    sql::db_connection_pool::duckdbpool::{DuckDbConnectionPool, DuckDbConnectionPoolBuilder},
};
use duckdb::AccessMode;
use itertools::Itertools;
use runtime_table_partition::expression::PartitionBy;
use settings::OrderByNonIntegerLiteral;
use snafu::prelude::*;
use std::{
    any::Any,
    cmp::max,
    collections::HashSet,
    ffi::OsStr,
    sync::{Arc, Once},
};

use super::{AccelerationSource, DataAccelerator, Error as DataAcceleratorError};

pub(crate) mod settings;

const DEFAULT_MIN_IDLE_CONNECTIONS: u32 = 10;

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
                .with_dialect(new_duckdb_dialect())
                .with_settings_registry(
                    DuckDBSettingsRegistry::new().with_setting(Box::new(OrderByNonIntegerLiteral)),
                ),
        }
    }

    /// Returns the `DuckDB` file path that would be used for a file-based `DuckDB` accelerator from this dataset
    pub fn duckdb_file_path(&self, source: &dyn AccelerationSource) -> Result<String> {
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
    pub async fn get_shared_pool(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<DuckDbConnectionPool> {
        let duckdb_file = self.duckdb_file_path(source);

        let acceleration = source.acceleration().context(AccelerationNotEnabledSnafu {
            dataset: source.name().to_string(),
        })?;

        let pool = match (duckdb_file, acceleration.mode) {
            (Ok(duckdb_file), Mode::File) => {
                let num_accelerating_datasets = self.get_num_accelerating_datasets(
                    Some(duckdb_file.as_str()),
                    &source.app(),
                    source.runtime(),
                );
                let max_size = Self::get_max_size(num_accelerating_datasets);
                let pool_builder = DuckDbConnectionPoolBuilder::file(&duckdb_file)
                    .with_max_size(Some(max_size))
                    .with_min_idle(Some(DEFAULT_MIN_IDLE_CONNECTIONS));
                self.duckdb_factory
                    .get_or_init_instance_with_builder(pool_builder)
                    .await
                    .boxed()
                    .context(AccelerationCreationFailedSnafu)?
            }
            (_, Mode::Memory) => {
                let num_accelerating_datasets =
                    self.get_num_accelerating_datasets(None, &source.app(), source.runtime());
                let max_size = Self::get_max_size(num_accelerating_datasets);
                let pool_builder = DuckDbConnectionPoolBuilder::memory()
                    .with_max_size(Some(max_size))
                    .with_min_idle(Some(DEFAULT_MIN_IDLE_CONNECTIONS));
                self.duckdb_factory
                    .get_or_init_instance_with_builder(pool_builder)
                    .await
                    .boxed()
                    .context(AccelerationCreationFailedSnafu)?
            }
            (Err(e), Mode::File) => {
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
                    if acceleration.mode == Mode::File {
                        if let Ok(file_path) = self.file_path(ds.as_ref()) {
                            if this_file_path == file_path {
                                instance_usage += 1;
                            }
                        }
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

    fn get_max_size(num_accelerating_datasets: u32) -> u32 {
        max(DEFAULT_MIN_IDLE_CONNECTIONS, num_accelerating_datasets)
    }
}

impl Default for DuckDBAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("file"),
    ParameterSpec::component("data_dir"),
    ParameterSpec::runtime("file_watcher"),
    ParameterSpec::component("memory_limit"),
    ParameterSpec::component("preserve_insertion_order"),
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

    fn file_path(&self, source: &dyn AccelerationSource) -> Result<String, DataAcceleratorError> {
        self.duckdb_file_path(source)
            .map_err(|e| DataAcceleratorError::InvalidConfiguration { msg: e.to_string() })
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
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !source.is_file_accelerated() {
            return Ok(());
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

            // TODO: skip if we are bootstrapping
            self.get_shared_pool(source).await?;
        }

        Ok(())
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        mut cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        _partition_by: Option<PartitionBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(duckdb_file) = cmd.options.remove("file") {
            cmd.options
                .insert("open".to_string(), duckdb_file.to_string());
        }

        // Modify the `cmd` by adding options to attach other databases
        if let Some(source) = source {
            if let Some(temp_directory) = &source.app().runtime.temp_directory.clone() {
                cmd.options
                    .insert("temp_directory".to_string(), temp_directory.to_string());
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
                        if other_source
                            .acceleration()
                            .is_some_and(|a| a.engine == Engine::DuckDB && a.mode == Mode::File)
                        {
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

        Ok(create_table_provider(&self.duckdb_factory, &cmd).await?)
    }

    fn prefix(&self) -> &'static str {
        "duckdb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
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
    let duckdb_writer = Arc::new(duckdb_writer.clone());
    let cloned_writer = Arc::clone(&duckdb_writer);

    let table_provider = Arc::new(PolyTableProvider::new(
        cloned_writer,
        duckdb_writer,
        read_provider,
    ));

    Ok(table_provider)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::component::dataset::builder::DatasetBuilder;
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray, TimestampSecondArray, UInt64Array},
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
    use crate::dataaccelerator::{DataAccelerator, duckdb::DuckDBAccelerator};

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
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };
        let duckdb_accelerator = DuckDBAccelerator::new();
        let ctx = SessionContext::new();
        let table = duckdb_accelerator
            .create_external_table(external_table, None, None)
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
        let expected = UInt64Array::from(vec![2]);
        assert_eq!(actual, &expected);

        let filter = col("time_int").lt(lit(1354360273));
        let plan = delete_table
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
        let expected = UInt64Array::from(vec![2]);
        assert_eq!(actual, &expected);
    }

    #[tokio::test]
    async fn test_duckdb_file_initialization() {
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new(
            "duckdb_file_accelerator_init".to_string(),
            "duckdb_file_accelerator_init",
        )
        .expect("Failed to create builder")
        .with_app(Arc::new(app))
        .with_runtime(Arc::new(rt))
        .build()
        .expect("Failed to build dataset");

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
