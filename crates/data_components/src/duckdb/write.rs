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

use std::{any::Any, fmt, sync::Arc};

use crate::delete::{DeletionExec, DeletionSink, DeletionTableProvider};
use crate::duckdb::DuckDB;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_plan::{
        insert::{DataSink, FileSinkExec},
        metrics::MetricsSet,
        DisplayAs, DisplayFormatType, ExecutionPlan,
    },
};
use futures::StreamExt;
use snafu::prelude::*;
use sql_provider_datafusion::expr::Engine;

use super::to_datafusion_error;

pub struct DuckDBTableWriter {
    read_provider: Arc<dyn TableProvider>,
    duckdb: Arc<DuckDB>,
}

impl DuckDBTableWriter {
    pub fn create(read_provider: Arc<dyn TableProvider>, duckdb: DuckDB) -> Arc<Self> {
        Arc::new(Self {
            read_provider,
            duckdb: Arc::new(duckdb),
        })
    }
}

#[async_trait]
impl TableProvider for DuckDBTableWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.read_provider.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.read_provider
            .scan(state, projection, filters, limit)
            .await
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FileSinkExec::new(
            input,
            Arc::new(DuckDBDataSink::new(Arc::clone(&self.duckdb), overwrite)),
            self.schema(),
            None,
        )) as _)
    }
}

#[derive(Clone)]
struct DuckDBDataSink {
    duckdb: Arc<DuckDB>,
    overwrite: bool,
}

#[async_trait]
impl DataSink for DuckDBDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let mut num_rows = 0;

        let mut db_conn = self.duckdb.connect().await.map_err(to_datafusion_error)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_datafusion_error)?;

        let data_batches_result = data
            .collect::<Vec<datafusion::common::Result<RecordBatch>>>()
            .await;

        let data_batches: Vec<RecordBatch> = data_batches_result
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        for data_batch in &data_batches {
            num_rows += u64::try_from(data_batch.num_rows()).map_err(|e| {
                DataFusionError::Execution(format!("Unable to convert num_rows() to u64: {e}"))
            })?;
        }

        let tx = duckdb_conn
            .conn
            .transaction()
            .context(super::UnableToBeginTransactionSnafu)
            .map_err(to_datafusion_error)?;

        if self.overwrite {
            self.duckdb
                .delete_all_table_data(&tx)
                .map_err(to_datafusion_error)?;
        }

        for batch in data_batches {
            self.duckdb
                .insert_batch(&tx, &batch)
                .map_err(to_datafusion_error)?;
        }

        tx.commit()
            .context(super::UnableToCommitDuckDBTransactionSnafu)
            .map_err(to_datafusion_error)?;

        Ok(num_rows)
    }
}

impl DuckDBDataSink {
    fn new(duckdb: Arc<DuckDB>, overwrite: bool) -> Self {
        Self { duckdb, overwrite }
    }
}

impl std::fmt::Debug for DuckDBDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBDataSink")
    }
}

impl DisplayAs for DuckDBDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBDataSink")
    }
}

#[async_trait]
impl DeletionTableProvider for DuckDBTableWriter {
    async fn delete_from(
        &self,
        _state: &SessionState,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(
            Arc::new(DuckDBDeletionSink::new(Arc::clone(&self.duckdb), filters)),
            &self.schema(),
        )))
    }
}

struct DuckDBDeletionSink {
    duckdb: Arc<DuckDB>,
    filters: Vec<Expr>,
}

impl DuckDBDeletionSink {
    fn new(duckdb: Arc<DuckDB>, filters: &[Expr]) -> Self {
        Self {
            duckdb,
            filters: filters.to_vec(),
        }
    }
}

#[async_trait]
impl DeletionSink for DuckDBDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let mut db_conn = self.duckdb.connect().await?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)?;
        let sql = crate::util::filters_to_sql(&self.filters, Some(Engine::DuckDB))?;
        let count = self.duckdb.delete_from(duckdb_conn, &sql)?;

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{Int64Array, RecordBatch, StringArray, TimestampSecondArray, UInt64Array},
        datatypes::{DataType, Schema},
    };
    use datafusion::{
        common::{parsers::CompressionTypeVariant, Constraints, OwnedTableReference, ToDFSchema},
        datasource::provider::TableProviderFactory,
        execution::context::SessionContext,
        logical_expr::{cast, col, lit, CreateExternalTable},
        physical_plan::{collect, test::exec::MockExec},
        scalar::ScalarValue,
    };

    use crate::{delete::get_deletion_provider, duckdb::DuckDBTableProviderFactory};

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
        ]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: OwnedTableReference::bare("test_table"),
            location: String::new(),
            file_type: String::new(),
            has_header: false,
            delimiter: ',',
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            file_compression_type: CompressionTypeVariant::UNCOMPRESSED,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::empty(),
            column_defaults: HashMap::default(),
        };
        let ctx = SessionContext::new();
        let table = DuckDBTableProviderFactory::default()
            .create(&ctx.state(), &external_table)
            .await
            .expect("table should be created");

        let arr1 = StringArray::from(vec![
            "1970-01-01",
            "2012-12-01T11:11:11Z",
            "2012-12-01T11:11:12Z",
        ]);
        let arr2 = TimestampSecondArray::from(vec![0, 1354360271, 1354360272]);
        let arr3 = Int64Array::from(vec![0, 1354360271, 1354360272]);
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arr1), Arc::new(arr2), Arc::new(arr3)],
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
            .insert_into(&ctx.state(), exec, false)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        let delete_table = get_deletion_provider(Arc::clone(&table))
            .expect("table should be returned as deletetion provider");

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
    }
}
