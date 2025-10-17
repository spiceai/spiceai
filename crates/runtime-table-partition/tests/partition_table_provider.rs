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
        let new_mem_table_exec = Arc::clone(&self.mem_table_exec).with_new_children(children)?;
        Ok(Arc::new(PartitionMemTableExec {
            mem_table_exec: new_mem_table_exec,
            partition_value,
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
        )
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
#[allow(clippy::too_many_lines)]
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
#[allow(clippy::too_many_lines)]
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

#[allow(clippy::expect_used)]
fn timestamp_nanos(datetime: &str) -> i64 {
    let naive =
        NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S").expect("datetime parse");

    // Assume UTC; convert NaiveDateTime to a DateTime<Utc>
    let datetime_utc = Utc.from_utc_datetime(&naive);

    datetime_utc
        .timestamp_nanos_opt()
        .expect("timestamp_nanos_opt is ok")
}
