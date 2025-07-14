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
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
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
        let new_mem_table_exec = self.mem_table_exec.clone().with_new_children(children)?;
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
        let empty_batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::new(vec![].into(), None)),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(Int64Array::new(vec![].into(), None)),
            ],
        )
        .map_err(|e| creator::Error::CreatePartition { source: e.into() })?;

        let mem_table = Arc::new(
            MemTable::try_new(self.schema.clone(), vec![vec![empty_batch]])
                .map_err(|e| creator::Error::CreatePartition { source: e.into() })?,
        );
        let partition_mem_table = Arc::new(PartitionMemTable {
            mem_table,
            partition_value: partition_value.clone(),
        });
        self.partitions
            .write()
            .await
            .insert(partition_value.to_string(), partition_mem_table.clone());
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

    let creator = Arc::new(TestPartitionCreator::new(schema.clone()));
    let partition_by = vec![col("region")];
    let table_provider =
        PartitionTableProvider::new(creator.clone(), partition_by, schema.clone()).await?;

    let ctx = SessionContext::new();
    ctx.register_table("test_table", Arc::new(table_provider))?;

    let batch = RecordBatch::try_new(
        schema.clone(),
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
        let df = ctx.read_table(partition_mem_table.clone())?;
        let batches = df.collect().await?;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let region_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                assert_eq!(
                    region_array.value(i),
                    partition_key,
                    "Data in partition {} should match its key",
                    partition_key
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

    let creator = Arc::new(TestPartitionCreator::new(schema.clone()));
    let partition_by = vec![col("region")];
    let table_provider =
        PartitionTableProvider::new(creator.clone(), partition_by, schema.clone()).await?;

    let ctx = SessionContext::new();
    ctx.register_table("test_table", Arc::new(table_provider))?;

    let batch = RecordBatch::try_new(
        schema.clone(),
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
