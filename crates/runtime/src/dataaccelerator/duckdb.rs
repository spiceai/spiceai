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
    duckdb::{write::DuckDBTableWriter, DuckDBTableProviderFactory},
    sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};
use duckdb::AccessMode;
use snafu::prelude::*;
use std::{any::Any, sync::Arc};

use crate::{
    component::dataset::{acceleration::Engine, Dataset},
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
            duckdb_factory: DuckDBTableProviderFactory::new().access_mode(AccessMode::ReadWrite),
        }
    }

    /// Returns the `DuckDB` file path that would be used for a file-based `DuckDB` accelerator from this dataset
    #[must_use]
    pub fn duckdb_file_path(&self, dataset: &Dataset) -> Option<String> {
        if !dataset.is_file_accelerated() {
            return None;
        }
        let acceleration = dataset.acceleration.as_ref()?;

        let mut params = acceleration.params.clone();
        params.insert("data_directory".to_string(), spice_data_base_path());

        Some(
            self.duckdb_factory
                .duckdb_file_path(&dataset.name.to_string(), &mut params),
        )
    }
}

impl Default for DuckDBAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

const PARAMETERS: &[ParameterSpec] = &[ParameterSpec::accelerator("file")];

#[async_trait]
impl DataAccelerator for DuckDBAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "duckdb"
    }

    async fn init(
        &self,
        dataset: &Dataset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = self.duckdb_file_path(dataset);

        if let (Some(path), Some(acceleration)) = (&path, &dataset.acceleration) {
            if !acceleration.params.contains_key("duckdb_open") {
                make_spice_data_directory().map_err(|err| {
                    Error::AccelerationInitializationFailed { source: err.into() }
                })?;
            }

            DuckDbConnectionPool::new_file(path, &AccessMode::ReadWrite)
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
        if !cmd.options.contains_key("open") {
            make_spice_data_directory()
                .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
        }

        if let Some(Some(attach_databases)) = dataset.map(|this_dataset: &Dataset| {
            this_dataset.app.as_ref().map(|a| {
                let datasets = Runtime::get_valid_datasets(a, crate::LogErrors(false));
                datasets
                    .iter()
                    .filter(|d| {
                        d.acceleration
                            .as_ref()
                            .map_or(false, |a| a.engine == Engine::DuckDB)
                    })
                    .filter_map(|other_dataset| {
                        if **other_dataset == *this_dataset {
                            None
                        } else {
                            self.duckdb_file_path(other_dataset)
                        }
                    })
                    .collect::<Vec<String>>()
            })
        }) {
            if !attach_databases.is_empty() {
                cmd.options
                    .insert("attach_databases".to_string(), attach_databases.join(";"));
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
        logical_expr::{cast, col, lit, CreateExternalTable},
        physical_plan::collect,
        scalar::ScalarValue,
    };
    use datafusion_table_providers::util::test::MockExec;

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
            .insert_into(&ctx.state(), Arc::<MockExec>::clone(&exec), false)
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
            .insert_into(&ctx.state(), Arc::<MockExec>::clone(&exec), false)
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
            .insert_into(&ctx.state(), exec, false)
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
}
