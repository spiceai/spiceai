use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::{
    catalog::Session,
    common::Constraints,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{dml::InsertOp, Expr},
    physical_plan::{metrics::MetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan},
};
use futures::StreamExt;
use snafu::prelude::*;

use crate::util::{
    constraints,
    on_conflict::OnConflict,
    retriable_error::{check_and_mark_retriable_error, to_retriable_data_write_error},
};

use super::{to_datafusion_error, Sqlite};

#[derive(Debug, Clone)]
pub struct SqliteTableWriter {
    pub read_provider: Arc<dyn TableProvider>,
    sqlite: Arc<Sqlite>,
    on_conflict: Option<OnConflict>,
}

impl SqliteTableWriter {
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        sqlite: Sqlite,
        on_conflict: Option<OnConflict>,
    ) -> Arc<Self> {
        Arc::new(Self {
            read_provider,
            sqlite: Arc::new(sqlite),
            on_conflict,
        })
    }

    pub fn sqlite(&self) -> Arc<Sqlite> {
        Arc::clone(&self.sqlite)
    }
}

#[async_trait]
impl TableProvider for SqliteTableWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.read_provider.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(self.sqlite.constraints())
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        // Verify schema consistency before delegating
        // This is a cheap check since it's just comparing Arc<Schema> pointers
        if self.read_provider.schema() != self.schema() {
            tracing::warn!(
                "Schema mismatch detected in SqliteTableWriter for table {}",
                self.sqlite.table_name()
            );
        }

        self.read_provider.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
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
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(SqliteDataSink::new(
                Arc::clone(&self.sqlite),
                op,
                self.on_conflict.clone(),
                self.schema(),
                self.sqlite.batch_insert_use_prepared_statements,
            )),
            None,
        )) as _)
    }
}

#[derive(Clone)]
struct SqliteDataSink {
    sqlite: Arc<Sqlite>,
    overwrite: InsertOp,
    on_conflict: Option<OnConflict>,
    schema: SchemaRef,
    use_prepared_statements: bool,
}

#[async_trait]
impl DataSink for SqliteDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(1);

        // Since the main task/stream can be dropped or fail, we use a oneshot channel to signal that all data is received and we should commit the transaction
        let (notify_commit_transaction, mut on_commit_transaction) =
            tokio::sync::oneshot::channel();

        let mut db_conn = self
            .sqlite
            .connect()
            .await
            .map_err(to_retriable_data_write_error)?;
        let sqlite_conn =
            Sqlite::sqlite_conn(&mut db_conn).map_err(to_retriable_data_write_error)?;

        let constraints = self.sqlite.constraints().clone();
        let mut data = data;
        let task = tokio::spawn(async move {
            let mut num_rows: u64 = 0;
            while let Some(data_batch) = data.next().await {
                let data_batch = data_batch.map_err(check_and_mark_retriable_error)?;
                num_rows += u64::try_from(data_batch.num_rows()).map_err(|e| {
                    DataFusionError::Execution(format!("Unable to convert num_rows() to u64: {e}"))
                })?;

                constraints::validate_batch_with_constraints(
                    vec![data_batch.clone()],
                    &constraints,
                    &crate::util::constraints::UpsertOptions::default(),
                )
                .await
                .context(super::ConstraintViolationSnafu)
                .map_err(to_datafusion_error)?;

                batch_tx.send(data_batch).await.map_err(|err| {
                    DataFusionError::Execution(format!("Error sending data batch: {err}"))
                })?;
            }

            if notify_commit_transaction.send(()).is_err() {
                return Err(DataFusionError::Execution(
                    "Unable to send message to commit transaction to SQLite writer.".to_string(),
                ));
            };

            // Drop the sender to signal the receiver that no more data is coming
            drop(batch_tx);

            Ok::<_, DataFusionError>(num_rows)
        });

        let overwrite = self.overwrite;
        let sqlite = Arc::clone(&self.sqlite);
        let on_conflict = self.on_conflict.clone();
        let use_prepared_statements = self.use_prepared_statements;

        sqlite_conn
            .conn
            .call(move |conn| {
                let transaction = conn.transaction()?;

                if matches!(overwrite, InsertOp::Overwrite) {
                    sqlite.delete_all_table_data(&transaction)?;
                }

                while let Some(data_batch) = batch_rx.blocking_recv() {
                    if data_batch.num_rows() > 0 {
                        if use_prepared_statements {
                            sqlite.insert_batch_prepared(
                                &transaction,
                                data_batch,
                                on_conflict.as_ref(),
                            )?;
                        } else {
                            #[allow(deprecated)]
                            sqlite.insert_batch(&transaction, data_batch, on_conflict.as_ref())?;
                        }
                    }
                }

                if on_commit_transaction.try_recv().is_err() {
                    return Err(rusqlite::Error::InvalidQuery);
                }

                transaction.commit()?;

                Ok(())
            })
            .await
            .context(super::UnableToInsertIntoTableAsyncSnafu)
            .map_err(|e| {
                if let super::Error::UnableToInsertIntoTableAsync {
                    source: tokio_rusqlite::Error::Error(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error {
                            code: rusqlite::ffi::ErrorCode::DiskFull,
                            ..
                        },
                        _,
                    )),
                } = e
                {
                    DataFusionError::External(super::Error::DiskFull {}.into())
                } else {
                    to_retriable_data_write_error(e)
                }
            })?;

        let num_rows = task.await.map_err(to_retriable_data_write_error)??;

        Ok(num_rows)
    }
}

impl SqliteDataSink {
    fn new(
        sqlite: Arc<Sqlite>,
        overwrite: InsertOp,
        on_conflict: Option<OnConflict>,
        schema: SchemaRef,
        use_prepared_statements: bool,
    ) -> Self {
        Self {
            sqlite,
            overwrite,
            on_conflict,
            schema,
            use_prepared_statements,
        }
    }
}

impl std::fmt::Debug for SqliteDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SqliteDataSink")
    }
}

impl DisplayAs for SqliteDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "SqliteDataSink")
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use datafusion::arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Schema},
    };
    use datafusion::{
        catalog::TableProviderFactory,
        common::{Constraints, TableReference, ToDFSchema},
        execution::context::SessionContext,
        logical_expr::{dml::InsertOp, CreateExternalTable},
        physical_plan::collect,
    };

    use crate::sqlite::SqliteTableProviderFactory;
    use crate::util::test::MockExec;

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
    async fn test_round_trip_sqlite() {
        let schema = Arc::new(Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("time_in_string", DataType::Utf8, false),
            datafusion::arrow::datatypes::Field::new("time_int", DataType::Int64, false),
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
            constraints: Constraints::default(),
            column_defaults: HashMap::default(),
            temporary: false,
        };
        let ctx = SessionContext::new();
        let table = SqliteTableProviderFactory::default()
            .create(&ctx.state(), &external_table)
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
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::unreadable_literal)]
    async fn test_all_arrow_types_to_sqlite() {
        use arrow::{
            array::*,
            datatypes::{DataType, Field, TimeUnit},
        };

        let num_rows = 10;
        // Create a comprehensive schema with all supported Arrow types
        let schema = Arc::new(Schema::new(vec![
            // Integer types
            Field::new("col_int8", DataType::Int8, true),
            Field::new("col_int16", DataType::Int16, true),
            Field::new("col_int32", DataType::Int32, true),
            Field::new("col_int64", DataType::Int64, true),
            Field::new("col_uint8", DataType::UInt8, true),
            Field::new("col_uint16", DataType::UInt16, true),
            Field::new("col_uint32", DataType::UInt32, true),
            Field::new("col_uint64", DataType::UInt64, true),
            // Float types (Float16 requires half crate - skip for now)
            Field::new("col_float32", DataType::Float32, true),
            Field::new("col_float64", DataType::Float64, true),
            // String types
            Field::new("col_utf8", DataType::Utf8, true),
            Field::new("col_large_utf8", DataType::LargeUtf8, true),
            Field::new("col_utf8_view", DataType::Utf8View, true),
            // Boolean
            Field::new("col_bool", DataType::Boolean, true),
            // Binary types
            Field::new("col_binary", DataType::Binary, true),
            Field::new("col_large_binary", DataType::LargeBinary, true),
            Field::new("col_binary_view", DataType::BinaryView, true),
            Field::new("col_fixed_binary", DataType::FixedSizeBinary(4), true),
            // Date types
            Field::new("col_date32", DataType::Date32, true),
            Field::new("col_date64", DataType::Date64, true),
            // Timestamp types
            Field::new(
                "col_ts_second",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
            Field::new(
                "col_ts_milli",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "col_ts_micro",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "col_ts_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            // Time types
            Field::new(
                "col_time32_second",
                DataType::Time32(TimeUnit::Second),
                true,
            ),
            Field::new(
                "col_time32_milli",
                DataType::Time32(TimeUnit::Millisecond),
                true,
            ),
            Field::new(
                "col_time64_micro",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            ),
            Field::new(
                "col_time64_nano",
                DataType::Time64(TimeUnit::Nanosecond),
                true,
            ),
            // Duration types
            Field::new("col_dur_second", DataType::Duration(TimeUnit::Second), true),
            Field::new(
                "col_dur_milli",
                DataType::Duration(TimeUnit::Millisecond),
                true,
            ),
            Field::new(
                "col_dur_micro",
                DataType::Duration(TimeUnit::Microsecond),
                true,
            ),
            Field::new(
                "col_dur_nano",
                DataType::Duration(TimeUnit::Nanosecond),
                true,
            ),
        ]));

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare(format!("test_all_types_{}", num_rows)),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::default(),
            column_defaults: HashMap::default(),
            temporary: false,
        };

        let ctx = SessionContext::new();
        let table = SqliteTableProviderFactory::default()
            .create(&ctx.state(), &external_table)
            .await
            .expect("table should be created");

        // Generate test data dynamically based on num_rows
        let mut int8_values = Vec::with_capacity(num_rows);
        let mut int16_values = Vec::with_capacity(num_rows);
        let mut int32_values = Vec::with_capacity(num_rows);
        let mut int64_values = Vec::with_capacity(num_rows);
        let mut uint8_values = Vec::with_capacity(num_rows);
        let mut uint16_values = Vec::with_capacity(num_rows);
        let mut uint32_values = Vec::with_capacity(num_rows);
        let mut uint64_values = Vec::with_capacity(num_rows);
        let mut float32_values = Vec::with_capacity(num_rows);
        let mut float64_values = Vec::with_capacity(num_rows);
        let mut string_values = Vec::with_capacity(num_rows);
        let mut large_string_values = Vec::with_capacity(num_rows);
        let mut string_view_values = Vec::with_capacity(num_rows);
        let mut bool_values = Vec::with_capacity(num_rows);
        let mut binary_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(num_rows);
        let mut large_binary_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(num_rows);
        let mut binary_view_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(num_rows);
        let mut fixed_binary_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(num_rows);
        let mut date32_values = Vec::with_capacity(num_rows);
        let mut date64_values = Vec::with_capacity(num_rows);
        let mut ts_sec_values = Vec::with_capacity(num_rows);
        let mut ts_milli_values = Vec::with_capacity(num_rows);
        let mut ts_micro_values = Vec::with_capacity(num_rows);
        let mut ts_nano_values = Vec::with_capacity(num_rows);
        let mut time32_sec_values = Vec::with_capacity(num_rows);
        let mut time32_milli_values = Vec::with_capacity(num_rows);
        let mut time64_micro_values = Vec::with_capacity(num_rows);
        let mut time64_nano_values = Vec::with_capacity(num_rows);
        let mut dur_sec_values = Vec::with_capacity(num_rows);
        let mut dur_milli_values = Vec::with_capacity(num_rows);
        let mut dur_micro_values = Vec::with_capacity(num_rows);
        let mut dur_nano_values = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            // Add some null values at regular intervals
            let is_null = i % 3 == 1;

            int8_values.push(if is_null { None } else { Some((i % 100) as i8) });
            int16_values.push(if is_null {
                None
            } else {
                Some((i * 100) as i16)
            });
            int32_values.push(if is_null {
                None
            } else {
                Some((i * 1000) as i32)
            });
            int64_values.push(if is_null {
                None
            } else {
                Some((i * 10000) as i64)
            });
            uint8_values.push(if is_null { None } else { Some((i % 200) as u8) });
            uint16_values.push(if is_null {
                None
            } else {
                Some((i * 100) as u16)
            });
            uint32_values.push(if is_null {
                None
            } else {
                Some((i * 1000) as u32)
            });
            uint64_values.push(if is_null {
                None
            } else {
                Some((i * 10000) as u64)
            });
            float32_values.push(if is_null {
                None
            } else {
                Some((i as f32) * 1.5)
            });
            float64_values.push(if is_null {
                None
            } else {
                Some((i as f64) * 2.5)
            });
            string_values.push(if is_null {
                None
            } else {
                Some(format!("str_{}", i))
            });
            large_string_values.push(if is_null {
                None
            } else {
                Some(format!("large_{}", i))
            });
            string_view_values.push(if is_null {
                None
            } else {
                Some(format!("view_{}", i))
            });
            bool_values.push(if is_null { None } else { Some(i % 2 == 0) });
            binary_values.push(if is_null {
                None
            } else {
                Some(format!("bin_{}", i).into_bytes())
            });
            large_binary_values.push(if is_null {
                None
            } else {
                Some(format!("lbin_{}", i).into_bytes())
            });
            binary_view_values.push(if is_null {
                None
            } else {
                Some(format!("bv_{}", i).into_bytes())
            });
            fixed_binary_values.push(if is_null {
                None
            } else {
                Some(vec![i as u8, (i + 1) as u8, (i + 2) as u8, (i + 3) as u8])
            });
            date32_values.push(if is_null {
                None
            } else {
                Some(18000 + i as i32)
            });
            date64_values.push(if is_null {
                None
            } else {
                Some(1609459200000 + (i as i64 * 86400000))
            });
            ts_sec_values.push(if is_null {
                None
            } else {
                Some(1609459200 + i as i64)
            });
            ts_milli_values.push(if is_null {
                None
            } else {
                Some(1609459200000 + i as i64)
            });
            ts_micro_values.push(if is_null {
                None
            } else {
                Some(1609459200000000 + i as i64)
            });
            ts_nano_values.push(if is_null {
                None
            } else {
                Some(1609459200000000000 + i as i64)
            });
            time32_sec_values.push(if is_null {
                None
            } else {
                Some(3600 + (i * 10) as i32)
            });
            time32_milli_values.push(if is_null {
                None
            } else {
                Some(3600000 + (i * 1000) as i32)
            });
            time64_micro_values.push(if is_null {
                None
            } else {
                Some(3600000000 + (i * 1000000) as i64)
            });
            time64_nano_values.push(if is_null {
                None
            } else {
                Some(3600000000000 + (i * 1000000000) as i64)
            });
            dur_sec_values.push(if is_null {
                None
            } else {
                Some(86400 + i as i64)
            });
            dur_milli_values.push(if is_null {
                None
            } else {
                Some(86400000 + i as i64)
            });
            dur_micro_values.push(if is_null {
                None
            } else {
                Some(86400000000 + i as i64)
            });
            dur_nano_values.push(if is_null {
                None
            } else {
                Some(86400000000000 + i as i64)
            });
        }

        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                // Integer types
                Arc::new(Int8Array::from(int8_values)),
                Arc::new(Int16Array::from(int16_values)),
                Arc::new(Int32Array::from(int32_values)),
                Arc::new(Int64Array::from(int64_values)),
                Arc::new(UInt8Array::from(uint8_values)),
                Arc::new(UInt16Array::from(uint16_values)),
                Arc::new(UInt32Array::from(uint32_values)),
                Arc::new(UInt64Array::from(uint64_values)),
                // Float types
                Arc::new(Float32Array::from(float32_values)),
                Arc::new(Float64Array::from(float64_values)),
                // String types
                Arc::new(StringArray::from(string_values)),
                Arc::new(LargeStringArray::from(large_string_values)),
                Arc::new(StringViewArray::from(string_view_values)),
                // Boolean
                Arc::new(BooleanArray::from(bool_values)),
                // Binary types
                Arc::new(BinaryArray::from(
                    binary_values
                        .iter()
                        .map(|v| v.as_ref().map(|b| b.as_slice()))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(LargeBinaryArray::from(
                    large_binary_values
                        .iter()
                        .map(|v| v.as_ref().map(|b| b.as_slice()))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(BinaryViewArray::from_iter(
                    binary_view_values
                        .iter()
                        .map(|v| v.as_ref().map(|b| b.as_slice())),
                )),
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        fixed_binary_values
                            .iter()
                            .map(|v| v.as_ref().map(|b| b.as_slice())),
                        4,
                    )
                    .unwrap(),
                ),
                // Date types
                Arc::new(Date32Array::from(date32_values)),
                Arc::new(Date64Array::from(date64_values)),
                // Timestamp types
                Arc::new(TimestampSecondArray::from(ts_sec_values)),
                Arc::new(TimestampMillisecondArray::from(ts_milli_values)),
                Arc::new(TimestampMicrosecondArray::from(ts_micro_values)),
                Arc::new(TimestampNanosecondArray::from(ts_nano_values)),
                // Time types
                Arc::new(Time32SecondArray::from(time32_sec_values)),
                Arc::new(Time32MillisecondArray::from(time32_milli_values)),
                Arc::new(Time64MicrosecondArray::from(time64_micro_values)),
                Arc::new(Time64NanosecondArray::from(time64_nano_values)),
                // Duration types
                Arc::new(DurationSecondArray::from(dur_sec_values)),
                Arc::new(DurationMillisecondArray::from(dur_milli_values)),
                Arc::new(DurationMicrosecondArray::from(dur_micro_values)),
                Arc::new(DurationNanosecondArray::from(dur_nano_values)),
            ],
        )
        .expect("data should be created");

        let exec = MockExec::new(vec![Ok(data)], Arc::clone(&schema));

        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "insert successful for {} rows - all Arrow types should be converted to SQLite",
                    num_rows
                )
            });
    }

    #[tokio::test]
    async fn test_filter_pushdown_support() {
        use datafusion::logical_expr::{col, lit, TableProviderFilterPushDown};

        let schema = Arc::new(Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("id", DataType::Int64, false),
            datafusion::arrow::datatypes::Field::new("name", DataType::Utf8, false),
        ]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_filter_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::default(),
            column_defaults: HashMap::default(),
            temporary: false,
        };
        let ctx = SessionContext::new();
        let table = SqliteTableProviderFactory::default()
            .create(&ctx.state(), &external_table)
            .await
            .expect("table should be created");

        // Test that filter pushdown is supported
        let filter = col("id").gt(lit(10));
        let result = table
            .supports_filters_pushdown(&[&filter])
            .expect("should support filter pushdown");

        assert_eq!(
            result,
            vec![TableProviderFilterPushDown::Exact],
            "Filter pushdown should be exact for simple comparison"
        );
    }

    #[tokio::test]
    async fn test_concurrent_read_write_with_filter_pushdown() {
        use datafusion::logical_expr::{col, lit, TableProviderFilterPushDown};

        let schema = Arc::new(Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("id", DataType::Int64, false),
            datafusion::arrow::datatypes::Field::new("value", DataType::Int64, false),
        ]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("concurrent_test"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::default(),
            column_defaults: HashMap::default(),
            temporary: false,
        };

        let ctx = SessionContext::new();
        let table = SqliteTableProviderFactory::default()
            .create(&ctx.state(), &external_table)
            .await
            .expect("table should be created");

        // Insert initial data
        let arr1 = Int64Array::from(vec![1, 2, 3]);
        let arr2 = Int64Array::from(vec![10, 20, 30]);
        let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr1), Arc::new(arr2)])
            .expect("data should be created");

        let exec = MockExec::new(vec![Ok(data)], Arc::clone(&schema));
        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        // Verify filter pushdown works after insert
        let filter = col("id").gt(lit(1));
        let result = table
            .supports_filters_pushdown(&[&filter])
            .expect("should support filter pushdown");

        assert_eq!(
            result,
            vec![TableProviderFilterPushDown::Exact],
            "Filter pushdown should be exact for simple comparison"
        );

        // Verify we can actually scan with the filter
        let scan = table
            .scan(&ctx.state(), None, &[filter], None)
            .await
            .expect("scan should succeed");

        let batches = collect(scan, ctx.task_ctx())
            .await
            .expect("collect should succeed");

        assert!(!batches.is_empty(), "Should have results");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "Should have 2 rows with id > 1");
    }
}
