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

use arrow_schema::TimeUnit;
use async_trait::async_trait;
use chrono::{NaiveDateTime, TimeZone as _, Utc};
use datafusion::arrow::array::{
    ArrayRef, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::DFSchema;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::context::{ExecutionProps, SessionContext};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, TableProviderFilterPushDown};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion::scalar::ScalarValue;
use datafusion::{arrow, prelude::*};
use runtime_datafusion_udfs::{bucket, truncate};
use runtime_table_partition::expression::PartitionedBy;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use runtime_table_partition::creator;
use runtime_table_partition::provider::PartitionTableProvider;
use runtime_table_partition::{Partition, creator::PartitionCreator};

#[derive(Debug)]
struct PartitionMemTable {
    mem_table: Arc<MemTable>,
    partition_value: ScalarValue,
}

#[async_trait]
impl TableProvider for PartitionMemTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.mem_table.schema()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        self.mem_table.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mem_table_exec = self
            .mem_table
            .scan(state, projection, filters, limit)
            .await?;
        Ok(Arc::new(PartitionMemTableExec {
            mem_table_exec,
            partition_value: self.partition_value.clone(),
            filters: filters.to_vec(),
            limit,
        }))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.mem_table.insert_into(state, input, insert_op).await
    }
}

#[derive(Debug)]
struct PartitionMemTableExec {
    mem_table_exec: Arc<dyn ExecutionPlan>,
    partition_value: ScalarValue,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl ExecutionPlan for PartitionMemTableExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.mem_table_exec.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.mem_table_exec.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.mem_table_exec.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let partition_value = self.partition_value.clone();
        let filters = self.filters.clone();
        let limit = self.limit;
        let new_mem_table_exec = Arc::clone(&self.mem_table_exec).with_new_children(children)?;
        Ok(Arc::new(PartitionMemTableExec {
            mem_table_exec: new_mem_table_exec,
            partition_value,
            filters,
            limit,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, DataFusionError> {
        self.mem_table_exec.execute(partition, context)
    }

    fn name(&self) -> &'static str {
        "PartitionMemTableExec"
    }
}

impl DisplayAs for PartitionMemTableExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "{}: partition_value={}",
            self.name(),
            self.partition_value
        )?;

        if !self.filters.is_empty() {
            write!(f, ", filters=[")?;
            for (i, filter) in self.filters.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{filter}")?;
            }
            write!(f, "]")?;
        }

        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct TestPartitionCreator {
    schema: SchemaRef,
    partitions: Arc<RwLock<HashMap<String, Arc<PartitionMemTable>>>>,
}

impl TestPartitionCreator {
    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            partitions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn get_partitions(&self) -> HashMap<String, Arc<PartitionMemTable>> {
        self.partitions.read().await.clone()
    }
}

#[async_trait]
impl PartitionCreator for TestPartitionCreator {
    async fn create_partition(
        &self,
        partition_value: ScalarValue,
    ) -> Result<Partition, creator::Error> {
        let empty_columns: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .map(|f| arrow::array::new_empty_array(f.data_type()))
            .collect();

        let empty_batch = RecordBatch::try_new(Arc::clone(&self.schema), empty_columns)
            .map_err(|e| creator::Error::CreatePartition { source: e.into() })?;

        let mem_table = Arc::new(
            MemTable::try_new(Arc::clone(&self.schema), vec![vec![empty_batch]])
                .map_err(|e| creator::Error::CreatePartition { source: e.into() })?,
        );
        let partition_mem_table = Arc::new(PartitionMemTable {
            mem_table,
            partition_value: partition_value.clone(),
        });
        self.partitions.write().await.insert(
            partition_value.to_string(),
            Arc::clone(&partition_mem_table),
        );
        Ok(Partition {
            partition_value,
            table_provider: partition_mem_table,
        })
    }

    async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, creator::Error> {
        Ok(vec![])
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

/// Get the partition values out of the execution plan
fn collect_partition_values(plan: &Arc<dyn ExecutionPlan>) -> Vec<ScalarValue> {
    let mut values = Vec::new();
    if let Some(partition_exec) = plan.as_any().downcast_ref::<PartitionMemTableExec>() {
        values.push(partition_exec.partition_value.clone());
    }
    for child in plan.children() {
        values.extend(collect_partition_values(child));
    }
    values
}

#[tokio::test]
async fn test_insert_partitioning() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let creator = Arc::new(TestPartitionCreator::new(Arc::clone(&schema)));
    let partition_by = vec![PartitionedBy {
        name: "region".to_string(),
        expression: col("region"),
    }];
    let table_provider = PartitionTableProvider::new(
        Arc::clone(&creator) as Arc<dyn PartitionCreator>,
        partition_by,
        Arc::clone(&schema),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_table("test_table", Arc::new(table_provider))?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "us-east-1",
                "us-west-1",
                "us-east-1",
                "us-west-1",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
        ],
    )?;

    let df = ctx.read_batch(batch)?;
    df.write_table("test_table", DataFrameWriteOptions::new())
        .await?;

    let partitions = creator.get_partitions().await;
    assert_eq!(partitions.len(), 2, "Expected two partitions");

    for (partition_key, partition_mem_table) in partitions {
        let df = ctx.read_table(Arc::clone(&partition_mem_table) as Arc<dyn TableProvider>)?;
        let batches = df.collect().await?;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let region_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("StringArray");
            for i in 0..batch.num_rows() {
                assert_eq!(
                    region_array.value(i),
                    partition_key,
                    "Data in partition {partition_key} should match its key",
                );
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_explain_plan_filtering() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let creator = Arc::new(TestPartitionCreator::new(Arc::clone(&schema)));
    let partition_by = vec![PartitionedBy {
        name: "region".to_string(),
        expression: col("region"),
    }];
    let table_provider = PartitionTableProvider::new(
        Arc::clone(&creator) as Arc<dyn PartitionCreator>,
        partition_by,
        Arc::clone(&schema),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_table("test_table", Arc::new(table_provider))?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "us-east-1",
                "us-west-1",
                "us-east-1",
                "us-west-1",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
        ],
    )?;
    let df = ctx.read_batch(batch)?;
    df.write_table("test_table", DataFrameWriteOptions::new())
        .await?;

    let df = ctx
        .sql("SELECT * FROM test_table WHERE region = 'us-east-1'")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let partition_values = collect_partition_values(&physical_plan);
    assert_eq!(
        partition_values.len(),
        1,
        "Expected one partition for filtered query"
    );
    assert_eq!(
        partition_values[0],
        ScalarValue::Utf8(Some("us-east-1".to_string())),
        "Expected partition value 'us-east-1'"
    );

    let df = ctx.sql("SELECT * FROM test_table").await?;
    let physical_plan = df.create_physical_plan().await?;
    let partition_values = collect_partition_values(&physical_plan);
    assert_eq!(
        partition_values.len(),
        2,
        "Expected two partitions for unfiltered query"
    );
    assert!(
        partition_values.contains(&ScalarValue::Utf8(Some("us-east-1".to_string()))),
        "Expected 'us-east-1' in unfiltered plan"
    );
    assert!(
        partition_values.contains(&ScalarValue::Utf8(Some("us-west-1".to_string()))),
        "Expected 'us-west-1' in unfiltered plan"
    );

    Ok(())
}

#[tokio::test]
#[expect(clippy::too_many_lines)]
async fn test_bucket_in_list_plan_filtering() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let creator = Arc::new(TestPartitionCreator::new(Arc::clone(&schema)));
    let partition_by = PartitionedBy {
        name: "bucket_id".to_string(),
        expression: Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(bucket::Bucket::new())),
            args: vec![lit(4i64), col("id")],
        }),
    };
    let table_provider = PartitionTableProvider::new(
        Arc::clone(&creator) as Arc<dyn PartitionCreator>,
        vec![partition_by.clone()],
        Arc::clone(&schema),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_udf(bucket::Bucket::new().into());
    ctx.register_table("test_table", Arc::new(table_provider))?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
            Arc::new(StringArray::from(vec![
                "us-east-1",
                "us-west-1",
                "us-east-1",
                "us-west-1",
                "us-east-1",
                "us-west-1",
                "us-east-1",
                "us-west-1",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80])),
        ],
    )?;

    let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
    let execution_props = ExecutionProps::new();
    let physical_expr =
        create_physical_expr(&partition_by.expression, &df_schema, &execution_props)?;
    let batch_values = physical_expr.evaluate(&batch)?;
    let bucket_values = match batch_values {
        ColumnarValue::Array(array) => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Expected Int32Array from bucket function")
            .values()
            .to_vec(),
        ColumnarValue::Scalar(_) => panic!("Expected array from bucket expression"),
    };

    let id_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array for id column");
    let mut bucket_to_ids: HashMap<i32, Vec<i64>> = HashMap::new();
    for (id, bucket) in id_array.values().iter().zip(bucket_values.iter()) {
        bucket_to_ids.entry(*bucket).or_default().push(*id);
    }

    let unique_buckets: Vec<i32> = bucket_to_ids.keys().copied().collect();
    assert!(
        unique_buckets.len() >= 2,
        "Expected at least two distinct buckets, got {}",
        unique_buckets.len()
    );

    let selected_buckets = &unique_buckets[..2.min(unique_buckets.len())];
    let selected_ids: Vec<i64> = selected_buckets
        .iter()
        .flat_map(|bucket| bucket_to_ids.get(bucket).unwrap_or(&vec![]).clone())
        .collect();

    let df = ctx.read_batch(batch)?;
    df.write_table("test_table", DataFrameWriteOptions::new())
        .await?;

    let in_list_str = selected_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!("SELECT * FROM test_table WHERE id IN ({in_list_str})");
    let df = ctx.sql(&query).await?;
    let physical_plan = df.create_physical_plan().await?;
    let partition_values = collect_partition_values(&physical_plan);

    // Verify partition pruning
    assert_eq!(
        partition_values.len(),
        selected_buckets.len(),
        "Expected {} partitions for IN list query",
        selected_buckets.len()
    );
    for bucket in selected_buckets {
        assert!(
            partition_values.contains(&ScalarValue::Int32(Some(*bucket))),
            "Expected bucket {bucket} in filtered plan",
        );
    }
    for bucket in 0..4 {
        if !selected_buckets.contains(&bucket) {
            assert!(
                !partition_values.contains(&ScalarValue::Int32(Some(bucket))),
                "Bucket {bucket} should be pruned",
            );
        }
    }

    Ok(())
}

#[tokio::test]
#[expect(clippy::too_many_lines)]
async fn test_truncate_in_list_plan_filtering() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let creator = Arc::new(TestPartitionCreator::new(Arc::clone(&schema)));
    let partition_by = PartitionedBy {
        name: "truncate_id".to_string(),
        expression: Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(truncate::Truncate::new())),
            args: vec![lit(10i64), col("id")],
        }),
    };
    let table_provider = PartitionTableProvider::new(
        Arc::clone(&creator) as Arc<dyn PartitionCreator>,
        vec![partition_by.clone()],
        Arc::clone(&schema),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_udf(truncate::Truncate::new().into());
    ctx.register_table("test_table", Arc::new(table_provider))?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![11, 12, 23, 24, 35, 36, 47, 48])),
            Arc::new(StringArray::from(vec![
                "us-east-1",
                "us-west-1",
                "us-east-1",
                "us-west-1",
                "us-east-1",
                "us-west-1",
                "us-east-1",
                "us-west-1",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80])),
        ],
    )?;

    let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
    let execution_props = ExecutionProps::new();
    let physical_expr =
        create_physical_expr(&partition_by.expression, &df_schema, &execution_props)?;
    let batch_values = physical_expr.evaluate(&batch)?;
    let truncated_values = match batch_values {
        ColumnarValue::Array(array) => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array from truncate function")
            .values()
            .to_vec(),
        ColumnarValue::Scalar(_) => panic!("Expected array from truncate expression"),
    };

    let id_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array for id column");
    let mut truncate_to_ids: HashMap<i64, Vec<i64>> = HashMap::new();
    for (id, truncated) in id_array.values().iter().zip(truncated_values.iter()) {
        truncate_to_ids.entry(*truncated).or_default().push(*id);
    }

    let unique_truncated: Vec<i64> = truncate_to_ids.keys().copied().collect();
    assert!(
        unique_truncated.len() >= 2,
        "Expected at least two distinct truncated values, got {}",
        unique_truncated.len()
    );

    let selected_truncated = &unique_truncated[..2.min(unique_truncated.len())];
    let selected_ids: Vec<i64> = selected_truncated
        .iter()
        .flat_map(|truncated| truncate_to_ids.get(truncated).unwrap_or(&vec![]).clone())
        .collect();

    let df = ctx.read_batch(batch)?;
    df.write_table("test_table", DataFrameWriteOptions::new())
        .await?;

    let in_list_str = selected_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!("SELECT * FROM test_table WHERE id IN ({in_list_str})");
    let df = ctx.sql(&query).await?;
    let physical_plan = df.create_physical_plan().await?;
    let partition_values = collect_partition_values(&physical_plan);

    assert_eq!(
        partition_values.len(),
        selected_truncated.len(),
        "Expected {} partitions for IN list query",
        selected_truncated.len()
    );
    for truncated in selected_truncated {
        assert!(
            partition_values.contains(&ScalarValue::Int64(Some(*truncated))),
            "Expected truncated value {truncated} in filtered plan",
        );
    }
    for truncated in &[0, 10, 20, 30, 40, 50, 60, 70] {
        if !selected_truncated.contains(truncated) {
            assert!(
                !partition_values.contains(&ScalarValue::Int64(Some(*truncated))),
                "Truncated value {truncated} should be pruned",
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_date_trunc_plan_filtering() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]));

    let creator = Arc::new(TestPartitionCreator::new(Arc::clone(&schema)));
    let granularities = vec!["year", "month", "day", "hour", "minute", "second"];

    for granularity in granularities {
        let partition_by = PartitionedBy {
            name: "date_trunc_timestamp".to_string(),
            expression: date_trunc(lit(granularity), col("timestamp")),
        };
        let table_provider = PartitionTableProvider::new(
            Arc::clone(&creator) as Arc<dyn PartitionCreator>,
            vec![partition_by.clone()],
            Arc::clone(&schema),
        )
        .await?;

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(table_provider))?;

        let timestamps = vec![
            timestamp_nanos("2025-07-15 12:34:56"),
            timestamp_nanos("2025-08-15 12:34:56"),
            timestamp_nanos("2025-07-15 13:00:00"),
        ];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(TimestampNanosecondArray::from(timestamps)),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )?;

        let df = ctx.read_batch(batch)?;
        df.write_table("test_table", DataFrameWriteOptions::new())
            .await?;

        let query = "SELECT * FROM test_table WHERE timestamp = TIMESTAMP '2025-07-15 12:34:56'"
            .to_string();
        let df = ctx.sql(&query).await?;
        let physical_plan = df.create_physical_plan().await?;
        let partition_values = collect_partition_values(&physical_plan);

        // Expected partition value based on granularity
        let expected_timestamp = match granularity {
            "year" => {
                ScalarValue::TimestampNanosecond(Some(timestamp_nanos("2025-01-01 00:00:00")), None)
            }
            "month" => {
                ScalarValue::TimestampNanosecond(Some(timestamp_nanos("2025-07-01 00:00:00")), None)
            }
            "day" => {
                ScalarValue::TimestampNanosecond(Some(timestamp_nanos("2025-07-15 00:00:00")), None)
            }
            "hour" => {
                ScalarValue::TimestampNanosecond(Some(timestamp_nanos("2025-07-15 12:00:00")), None)
            }
            "minute" => {
                ScalarValue::TimestampNanosecond(Some(timestamp_nanos("2025-07-15 12:34:00")), None)
            }
            "second" => {
                ScalarValue::TimestampNanosecond(Some(timestamp_nanos("2025-07-15 12:34:56")), None)
            }
            _ => panic!("Unsupported granularity"),
        };

        assert_eq!(
            partition_values.len(),
            1,
            "Expected one partition for filtered query with granularity {granularity}. Found: {partition_values:?}"
        );
        assert_eq!(
            partition_values[0], expected_timestamp,
            "Expected partition value for granularity {granularity}. Found: {partition_values:?}"
        );

        // Verify unfiltered query includes all partitions
        let df = ctx.sql("SELECT * FROM test_table").await?;
        let physical_plan = df.create_physical_plan().await?;
        let partition_values = collect_partition_values(&physical_plan);
        let expected_partition_count = match granularity {
            "year" => 1,
            "month" | "day" => 2,
            "hour" | "minute" | "second" => 3,
            _ => panic!("Unexpected granularity"),
        };
        assert_eq!(
            partition_values.len(),
            expected_partition_count,
            "Expected {expected_partition_count} partitions for unfiltered query with granularity {granularity}. Found: {partition_values:?}"
        );
    }

    Ok(())
}

#[expect(clippy::expect_used)]
fn timestamp_nanos(datetime: &str) -> i64 {
    let naive =
        NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S").expect("datetime parse");

    // Assume UTC; convert NaiveDateTime to a DateTime<Utc>
    let datetime_utc = Utc.from_utc_datetime(&naive);

    datetime_utc
        .timestamp_nanos_opt()
        .expect("timestamp_nanos_opt is ok")
}

/// Test that verifies partition filter splitting - partition filters should be used for pruning
/// but NOT passed to individual partition scans, while data filters should be passed through.
#[tokio::test]
async fn test_partition_filter_splitting_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let creator = Arc::new(TestPartitionCreator::new(Arc::clone(&schema)));

    // Test 1: Simple column partition
    let partition_by = vec![PartitionedBy {
        name: "region".to_string(),
        expression: col("region"),
    }];
    let table_provider = PartitionTableProvider::new(
        Arc::clone(&creator) as Arc<dyn PartitionCreator>,
        partition_by,
        Arc::clone(&schema),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_table("test_table", Arc::new(table_provider))?;

    // Insert test data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(StringArray::from(vec![
                "us-east-1",
                "us-west-1",
                "us-east-1",
                "us-west-1",
                "eu-west-1",
                "eu-west-1",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60])),
        ],
    )?;
    let df = ctx.read_batch(batch)?;
    df.write_table("test_table", DataFrameWriteOptions::new())
        .await?;

    // Query with partition filter only (region = 'us-east-1')
    // This filter should be used for pruning and NOT passed to partition scan
    let df = ctx
        .sql("SELECT * FROM test_table WHERE region = 'us-east-1'")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    insta::assert_snapshot!("partition_filter_only", explain_plan);

    // Query with partition filter and data filter (region = 'us-east-1' AND value > 20)
    // Partition filter should be used for pruning only, data filter passed to partition scan
    let df = ctx
        .sql("SELECT * FROM test_table WHERE region = 'us-east-1' AND value > 20")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    insta::assert_snapshot!("partition_and_data_filters", explain_plan);

    // Query with data filter only (value > 20)
    // Should scan all partitions with the data filter
    let df = ctx.sql("SELECT * FROM test_table WHERE value > 20").await?;
    let physical_plan = df.create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    // Sort lines to make test deterministic (HashMap iteration order is non-deterministic)
    let mut lines: Vec<&str> = explain_plan.lines().collect();
    lines[1..].sort_unstable(); // Sort all lines except the first (UnionExec)
    explain_plan = lines.join("\n") + "\n";
    insta::assert_snapshot!("data_filter_only", explain_plan);

    // Query with IN list on partition column
    // Should prune to only matching partitions
    let df = ctx
        .sql("SELECT * FROM test_table WHERE region IN ('us-east-1', 'eu-west-1')")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    // Sort lines to make test deterministic (HashMap iteration order is non-deterministic)
    let mut lines: Vec<&str> = explain_plan.lines().collect();
    lines[1..].sort_unstable(); // Sort all lines except the first (UnionExec)
    explain_plan = lines.join("\n") + "\n";
    insta::assert_snapshot!("partition_filter_in_list", explain_plan);

    Ok(())
}

/// Test partition filter splitting with `bucket()` partition function
#[tokio::test]
async fn test_partition_filter_splitting_bucket_snapshot() -> Result<(), Box<dyn std::error::Error>>
{
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let creator = Arc::new(TestPartitionCreator::new(Arc::clone(&schema)));

    // Partition by bucket(4, id)
    let partition_by = vec![PartitionedBy {
        name: "bucket_id".to_string(),
        expression: Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(bucket::Bucket::new())),
            args: vec![lit(4i64), col("id")],
        }),
    }];
    let table_provider = PartitionTableProvider::new(
        Arc::clone(&creator) as Arc<dyn PartitionCreator>,
        partition_by,
        Arc::clone(&schema),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_udf(bucket::Bucket::new().into());
    ctx.register_table("test_table", Arc::new(table_provider))?;

    // Insert test data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
            Arc::new(StringArray::from(vec![
                "us-east-1",
                "us-west-1",
                "us-east-1",
                "us-west-1",
                "us-east-1",
                "us-west-1",
                "us-east-1",
                "us-west-1",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80])),
        ],
    )?;
    let df = ctx.read_batch(batch)?;
    df.write_table("test_table", DataFrameWriteOptions::new())
        .await?;

    // Query with id filter (should map to specific bucket partition)
    let df = ctx.sql("SELECT * FROM test_table WHERE id = 5").await?;
    let physical_plan = df.create_physical_plan().await?;
    let explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    insta::assert_snapshot!("bucket_partition_id_filter", explain_plan);

    // Query with id filter and data filter
    let df = ctx
        .sql("SELECT * FROM test_table WHERE id = 5 AND value > 40")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    // Sort lines to make test deterministic (HashMap iteration order is non-deterministic)
    let mut lines: Vec<&str> = explain_plan.lines().collect();
    lines[1..].sort_unstable(); // Sort all lines except the first (UnionExec)
    explain_plan = lines.join("\n") + "\n";
    insta::assert_snapshot!("bucket_partition_with_data_filter", explain_plan);

    // Query with data filter only (should scan all partitions)
    let df = ctx.sql("SELECT * FROM test_table WHERE value > 40").await?;
    let physical_plan = df.create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    // Sort lines to make test deterministic (HashMap iteration order is non-deterministic)
    let mut lines: Vec<&str> = explain_plan.lines().collect();
    lines[1..].sort_unstable(); // Sort all lines except the first (UnionExec)
    explain_plan = lines.join("\n") + "\n";
    insta::assert_snapshot!("bucket_data_filter_all_partitions", explain_plan);

    Ok(())
}

#[tokio::test]
async fn test_constant_expression_filtering() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let creator = Arc::new(TestPartitionCreator::new(Arc::clone(&schema)));
    let partition_by = vec![PartitionedBy {
        name: "region".to_string(),
        expression: col("region"),
    }];
    let table_provider = PartitionTableProvider::new(
        Arc::clone(&creator) as Arc<dyn PartitionCreator>,
        partition_by,
        Arc::clone(&schema),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_table("test_table", Arc::new(table_provider))?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "us-east-1",
                "us-west-1",
                "us-east-1",
                "us-west-1",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
        ],
    )?;
    let df = ctx.read_batch(batch)?;
    df.write_table("test_table", DataFrameWriteOptions::new())
        .await?;

    // Test with constant true expression - should not prune any partitions
    let df = ctx.sql("SELECT * FROM test_table WHERE true").await?;
    let physical_plan = df.create_physical_plan().await?;
    let partition_values = collect_partition_values(&physical_plan);
    assert_eq!(
        partition_values.len(),
        2,
        "Constant 'true' should not prune any partitions"
    );

    // Test with constant false expression - should prune all partitions
    let df = ctx.sql("SELECT * FROM test_table WHERE false").await?;
    let physical_plan = df.create_physical_plan().await?;
    let partition_values = collect_partition_values(&physical_plan);
    assert_eq!(
        partition_values.len(),
        0,
        "Constant 'false' should prune all partitions"
    );

    // Test with constant expression AND partition filter
    let df = ctx
        .sql("SELECT * FROM test_table WHERE true AND region = 'us-east-1'")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let partition_values = collect_partition_values(&physical_plan);
    assert_eq!(
        partition_values.len(),
        1,
        "Constant 'true' AND partition filter should prune to one partition"
    );
    assert_eq!(
        partition_values[0],
        ScalarValue::Utf8(Some("us-east-1".to_string())),
        "Should only scan us-east-1 partition"
    );

    // Test with 1=1 (another constant true)
    let df = ctx.sql("SELECT * FROM test_table WHERE 1=1").await?;
    let physical_plan = df.create_physical_plan().await?;
    let partition_values = collect_partition_values(&physical_plan);
    assert_eq!(
        partition_values.len(),
        2,
        "Constant '1=1' should not prune any partitions"
    );

    Ok(())
}

#[tokio::test]
async fn test_simple_column_partition_inequality_snapshot() -> Result<(), Box<dyn std::error::Error>>
{
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("age", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let creator = Arc::new(TestPartitionCreator::new(Arc::clone(&schema)));
    let partition_by = vec![PartitionedBy {
        name: "age".to_string(),
        expression: col("age"),
    }];
    let table_provider = PartitionTableProvider::new(
        Arc::clone(&creator) as Arc<dyn PartitionCreator>,
        partition_by,
        Arc::clone(&schema),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_table("test_table", Arc::new(table_provider))?;

    // Insert test data with different ages
    for (age, ids, names) in [
        (10, vec![1, 2, 3], vec!["Alice", "Bob", "Charlie"]),
        (20, vec![4, 5, 6], vec!["David", "Eve", "Frank"]),
        (30, vec![7, 8, 9], vec!["Grace", "Henry", "Ivy"]),
        (40, vec![10, 11, 12], vec!["Jack", "Kate", "Leo"]),
    ] {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(vec![age; 3])),
                Arc::new(StringArray::from(names)),
            ],
        )?;
        let df = ctx.read_batch(batch)?;
        df.write_table("test_table", DataFrameWriteOptions::new())
            .await?;
    }

    // Test 1: Equality (age = 20) - should only scan partition 20
    let df = ctx.sql("SELECT * FROM test_table WHERE age = 20").await?;
    let physical_plan = df.create_physical_plan().await?;
    let explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    insta::assert_snapshot!("simple_column_equality", explain_plan);

    // Test 2: Greater than (age > 25) - should prune partitions 10 and 20
    let df = ctx.sql("SELECT * FROM test_table WHERE age > 25").await?;
    let physical_plan = df.create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut lines: Vec<&str> = explain_plan.lines().collect();
    lines[1..].sort_unstable();
    explain_plan = lines.join("\n") + "\n";
    insta::assert_snapshot!("simple_column_greater_than", explain_plan);

    // Test 3: Range (age >= 20 AND age < 40) - should only scan partitions 20 and 30
    let df = ctx
        .sql("SELECT * FROM test_table WHERE age >= 20 AND age < 40")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut lines: Vec<&str> = explain_plan.lines().collect();
    lines[1..].sort_unstable();
    explain_plan = lines.join("\n") + "\n";
    insta::assert_snapshot!("simple_column_range", explain_plan);

    Ok(())
}

#[tokio::test]
async fn test_modulo_partition_inequality_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let creator = Arc::new(TestPartitionCreator::new(Arc::clone(&schema)));
    let partition_by = vec![PartitionedBy {
        name: "value_mod_10".to_string(),
        expression: col("value") % lit(10),
    }];
    let table_provider = PartitionTableProvider::new(
        Arc::clone(&creator) as Arc<dyn PartitionCreator>,
        partition_by,
        Arc::clone(&schema),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_table("test_table", Arc::new(table_provider))?;

    // Insert test data - values 0-49, which will distribute across partitions 0-9
    for remainder in 0..10 {
        let values: Vec<i32> = (remainder..50).step_by(10).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(values.clone())),
                Arc::new(Int32Array::from(values)),
            ],
        )?;
        let df = ctx.read_batch(batch)?;
        df.write_table("test_table", DataFrameWriteOptions::new())
            .await?;
    }

    // Test 1: Equality (value = 23) - should only scan partition 3 (23 % 10 = 3)
    let df = ctx.sql("SELECT * FROM test_table WHERE value = 23").await?;
    let physical_plan = df.create_physical_plan().await?;
    let explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    insta::assert_snapshot!("modulo_equality", explain_plan);

    // Test 2: Greater than (value > 45) - all partitions can have values > 45
    let df = ctx.sql("SELECT * FROM test_table WHERE value > 45").await?;
    let physical_plan = df.create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut lines: Vec<&str> = explain_plan.lines().collect();
    lines[1..].sort_unstable();
    explain_plan = lines.join("\n") + "\n";
    insta::assert_snapshot!("modulo_greater_than", explain_plan);

    // Test 3: Less than (value < 5) - only partitions 0-4 can have values < 5
    let df = ctx.sql("SELECT * FROM test_table WHERE value < 5").await?;
    let physical_plan = df.create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut lines: Vec<&str> = explain_plan.lines().collect();
    lines[1..].sort_unstable();
    explain_plan = lines.join("\n") + "\n";
    insta::assert_snapshot!("modulo_less_than", explain_plan);

    Ok(())
}

#[tokio::test]
async fn test_truncate_partition_inequality_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("sales", DataType::Int64, false),
    ]));

    let creator = Arc::new(TestPartitionCreator::new(Arc::clone(&schema)));
    let truncate_udf = Arc::new(ScalarUDF::new_from_impl(truncate::Truncate::new()));
    let partition_by = vec![PartitionedBy {
        name: "sales_trunc_1000".to_string(),
        expression: Expr::ScalarFunction(ScalarFunction {
            func: truncate_udf,
            args: vec![lit(1000i64), col("sales")],
        }),
    }];
    let table_provider = PartitionTableProvider::new(
        Arc::clone(&creator) as Arc<dyn PartitionCreator>,
        partition_by,
        Arc::clone(&schema),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_table("test_table", Arc::new(table_provider))?;

    // Insert data into partitions truncated by 1000 (0-999, 1000-1999, 2000-2999, 3000-3999)
    for truncated_value in [0i64, 1000, 2000, 3000] {
        let start = truncated_value;
        let end = truncated_value + 999;
        let sales: Vec<i64> = (start..=end).step_by(100).collect();
        #[expect(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let ids: Vec<i32> = (0..sales.len() as i32).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int64Array::from(sales)),
            ],
        )?;
        let df = ctx.read_batch(batch)?;
        df.write_table("test_table", DataFrameWriteOptions::new())
            .await?;
    }

    // Test 1: Equality (sales = 1500) - should only scan partition 1000
    let df = ctx
        .sql("SELECT * FROM test_table WHERE sales = 1500")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    insta::assert_snapshot!("truncate_equality", explain_plan);

    // Test 2: Greater than (sales > 2500) - should scan partitions 2000 and 3000
    let df = ctx
        .sql("SELECT * FROM test_table WHERE sales > 2500")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut lines: Vec<&str> = explain_plan.lines().collect();
    lines[1..].sort_unstable();
    explain_plan = lines.join("\n") + "\n";
    insta::assert_snapshot!("truncate_greater_than", explain_plan);

    // Test 3: Range (sales >= 1000 AND sales < 2000) - should only scan partition 1000
    let df = ctx
        .sql("SELECT * FROM test_table WHERE sales >= 1000 AND sales < 2000")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    insta::assert_snapshot!("truncate_range", explain_plan);

    Ok(())
}

#[tokio::test]
async fn test_date_trunc_partition_inequality_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("value", DataType::Int32, false),
    ]));

    let creator = Arc::new(TestPartitionCreator::new(Arc::clone(&schema)));
    let partition_by = vec![PartitionedBy {
        name: "day".to_string(),
        expression: date_trunc(lit("day"), col("timestamp")),
    }];
    let table_provider = PartitionTableProvider::new(
        Arc::clone(&creator) as Arc<dyn PartitionCreator>,
        partition_by,
        Arc::clone(&schema),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_table("test_table", Arc::new(table_provider))?;

    // Insert data for 4 consecutive days
    let dates = [
        "2025-01-15 00:00:00",
        "2025-01-16 00:00:00",
        "2025-01-17 00:00:00",
        "2025-01-18 00:00:00",
    ];

    for (idx, date) in dates.iter().enumerate() {
        let partition_ts = timestamp_nanos(date);
        // Create timestamps throughout the day
        let timestamps: Vec<i64> = (0..24)
            .map(|hour| partition_ts + hour * 3600 * 1_000_000_000)
            .collect();
        let ids: Vec<i32> = (0..24).collect();
        #[expect(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let values: Vec<i32> = vec![idx as i32 * 100; 24];

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(TimestampNanosecondArray::from(timestamps)),
                Arc::new(Int32Array::from(values)),
            ],
        )?;
        let df = ctx.read_batch(batch)?;
        df.write_table("test_table", DataFrameWriteOptions::new())
            .await?;
    }

    // Test 1: Exact date match (timestamp = '2025-01-16 10:30:00') - should scan partition for 2025-01-16
    let df = ctx
        .sql("SELECT * FROM test_table WHERE timestamp = '2025-01-16 10:30:00'::timestamp")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    insta::assert_snapshot!("date_trunc_equality", explain_plan);

    // Test 2: Greater than (timestamp > '2025-01-16 12:00:00') - should scan partitions 2025-01-16, 2025-01-17, 2025-01-18
    let df = ctx
        .sql("SELECT * FROM test_table WHERE timestamp > '2025-01-16 12:00:00'::timestamp")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut lines: Vec<&str> = explain_plan.lines().collect();
    lines[1..].sort_unstable();
    explain_plan = lines.join("\n") + "\n";
    insta::assert_snapshot!("date_trunc_greater_than", explain_plan);

    // Test 3: Date range (between two days) - should only scan partition 2025-01-16
    let df = ctx
        .sql("SELECT * FROM test_table WHERE timestamp >= '2025-01-16 00:00:00'::timestamp AND timestamp < '2025-01-17 00:00:00'::timestamp")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    insta::assert_snapshot!("date_trunc_range", explain_plan);

    Ok(())
}

#[tokio::test]
async fn test_bucket_partition_inequality_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let creator = Arc::new(TestPartitionCreator::new(Arc::clone(&schema)));
    let bucket_fn = Arc::new(ScalarUDF::new_from_impl(bucket::Bucket::new()));
    let partition_by = vec![PartitionedBy {
        name: "user_id_bucket_10".to_string(),
        expression: Expr::ScalarFunction(ScalarFunction {
            func: Arc::clone(&bucket_fn),
            args: vec![lit(10i64), col("user_id")],
        }),
    }];
    let table_provider = PartitionTableProvider::new(
        Arc::clone(&creator) as Arc<dyn PartitionCreator>,
        partition_by,
        Arc::clone(&schema),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_table("test_table", Arc::new(table_provider))?;

    // Insert data for user_ids 0-99
    for i in 0..10 {
        let start = i * 10;
        let end = start + 10;
        let user_ids: Vec<i32> = (start..end).collect();
        let names: Vec<String> = user_ids.iter().map(|id| format!("User{id}")).collect();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(user_ids)),
                Arc::new(StringArray::from(names)),
            ],
        )?;
        let df = ctx.read_batch(batch)?;
        df.write_table("test_table", DataFrameWriteOptions::new())
            .await?;
    }

    // Test 1: Range with inequalities (user_id > 50 AND user_id <= 70)
    let df = ctx
        .sql("SELECT * FROM test_table WHERE user_id > 50 AND user_id <= 70")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut lines: Vec<&str> = explain_plan.lines().collect();
    lines[1..].sort_unstable();
    explain_plan = lines.join("\n") + "\n";
    insta::assert_snapshot!("bucket_inequality_range", explain_plan);

    // Test 2: Single inequality (user_id > 80)
    let df = ctx
        .sql("SELECT * FROM test_table WHERE user_id > 80")
        .await?;
    let physical_plan = df.create_physical_plan().await?;
    let mut explain_plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
        .indent(true)
        .to_string();
    let mut lines: Vec<&str> = explain_plan.lines().collect();
    lines[1..].sort_unstable();
    explain_plan = lines.join("\n") + "\n";
    insta::assert_snapshot!("bucket_inequality_unbounded", explain_plan);

    Ok(())
}
