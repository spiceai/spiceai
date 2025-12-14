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

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::array::{Array, Int64Array, UInt64Array};
use datafusion::arrow::compute;
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, Partitioning, PhysicalExpr, PlanProperties, execute_stream,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use datafusion::{
    arrow::record_batch::RecordBatch,
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
    prelude::Expr,
};
use futures::stream::StreamExt;
use parking_lot::Mutex;
use std::fmt;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio_stream::wrappers::ReceiverStream;

use crate::Partition;
use crate::creator::PartitionCreator;
use crate::creator::filename::encode_key;
use crate::expression::PartitionedBy;
use crate::provider::ScalarValueString;

#[derive(Debug)]
pub struct PartitionerExec {
    input: Arc<dyn ExecutionPlan>,
    creator: Arc<dyn PartitionCreator>,
    partitions: Arc<RwLock<HashMap<String, Partition>>>,
    partition_by: PartitionedBy,
    insert_op: InsertOp,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl PartitionerExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        partition_by: PartitionedBy,
        creator: Arc<dyn PartitionCreator>,
        partitions: Arc<RwLock<HashMap<String, Partition>>>,
        insert_op: InsertOp,
        schema: SchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            input,
            creator,
            partitions,
            partition_by,
            insert_op,
            schema,
            properties,
        }
    }
}

impl DisplayAs for PartitionerExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "{} (partition_by = {} AS {}, insert_op = {})",
            self.name(),
            self.partition_by.expression,
            self.partition_by.name,
            self.insert_op
        )
    }
}

impl ExecutionPlan for PartitionerExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "{} requires exactly one child",
                self.name()
            )));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.partition_by.clone(),
            Arc::clone(&self.creator),
            Arc::clone(&self.partitions),
            self.insert_op,
            Arc::clone(&self.schema),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "PartitionInsertExec only supports single partition".to_string(),
            ));
        }

        let row_count_schema = Arc::new(Schema::new(vec![Field::new(
            "row_count",
            DataType::Int64,
            false,
        )]));

        let row_count_stream = {
            let schema = self.schema();
            let row_count_schema = Arc::clone(&row_count_schema);
            let input = Arc::clone(&self.input);
            let physical_expr = create_physical_expr(&self.partition_by.expression, self.schema())?;
            let creator = Arc::clone(&self.creator);
            let partition_providers = Arc::clone(&self.partitions);
            let insert_op = self.insert_op;

            futures::stream::once(async move {
                let session_config = context.session_config();
                let ctx = SessionContext::new_with_config(session_config.clone());
                let task_ctx = Arc::clone(&context);
                let mut incoming_stream = execute_stream(input, task_ctx)?;

                let mut row_count = 0;
                let mut partition_senders = HashMap::<String, Sender<RecordBatch>>::new();
                let mut handles = Vec::new();

                while let Some(batch) = incoming_stream.next().await {
                    let batch = batch?;
                    if batch.num_rows() == 0 {
                        continue;
                    }

                    // Partition the batch using the partition_by expression
                    // into multiple batches
                    let batches = partition_batch(&batch, physical_expr.as_ref())?;

                    for (partition_key, (partition_value, batch)) in batches {
                        let tx = if let Some(tx) = partition_senders.get(&partition_key) {
                            tx.clone()
                        } else {
                            // spawn the insertion task for this partition
                            let (tx, rx) = channel(10);
                            partition_senders.insert(partition_key.clone(), tx.clone());

                            let providers = partition_providers.read().await;

                            // Get or init table provider
                            let new_provider =
                                if let Some(partition) = providers.get(&partition_key) {
                                    Arc::clone(&partition.table_provider)
                                } else {
                                    drop(providers);

                                    let partition = creator
                                        .create_partition(partition_value)
                                        .await
                                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                                    let new_provider = Arc::clone(&partition.table_provider);
                                    partition_providers
                                        .write()
                                        .await
                                        .insert(partition_key.clone(), partition);
                                    new_provider
                                };

                            let state = ctx.state();
                            let context = Arc::clone(&context);
                            let exec = PartitionInputExec::new(rx, Arc::clone(&schema));
                            let handle = tokio::spawn(async move {
                                let plan = new_provider
                                    .insert_into(&state, Arc::new(exec), insert_op)
                                    .await?;

                                let mut stream = execute_stream(plan, context)?;
                                while let Some(batch) = stream.next().await {
                                    batch?;
                                }

                                Result::<(), DataFusionError>::Ok(())
                            });

                            handles.push(handle);

                            tx
                        };

                        row_count += batch.num_rows();

                        tx.send(batch).await.map_err(|_| {
                            DataFusionError::Execution(
                                "failed to send a RecordBatch to a partition".into(),
                            )
                        })?;
                    }
                }

                // Must drop the sending channels so that the receiving streams
                // can terminate
                drop(partition_senders);

                for handle in handles {
                    if let Ok(output) = handle.await {
                        output.map_err(|e| DataFusionError::Execution(
                            format!("An error occurred while writing to one or more partition files. Some partitions may contain outdated or corrupted data. It is recommended to delete and recreate the accelerated files: {e}")
                        ))?;
                    }
                }

                // Return the number of rows inserted
                let row_count = i64::try_from(row_count).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Number of rows inserted exceeded i64::MAX: {e}"
                    ))
                })?;
                let array = Int64Array::from(vec![row_count]);
                Ok(RecordBatch::try_new(
                    row_count_schema,
                    vec![Arc::new(array)],
                )?)
            })
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            row_count_schema,
            row_count_stream,
        )))
    }

    fn name(&self) -> &'static str {
        "PartitionerExec"
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}

fn create_physical_expr(
    expr: &Expr,
    schema: SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let input_dfschema = DFSchema::try_from(schema)?;
    let execution_props = ExecutionProps::new();
    datafusion::physical_expr::create_physical_expr(expr, &input_dfschema, &execution_props)
}

fn filter_batch_by_indices(
    batch: &RecordBatch,
    indices: &[usize],
) -> Result<RecordBatch, DataFusionError> {
    let indices_array = UInt64Array::from_iter_values(indices.iter().map(|&i| i as u64));
    let indices_array = Arc::new(indices_array) as Arc<dyn Array>;
    let columns = batch
        .columns()
        .iter()
        .map(|col| compute::take(col, &indices_array, None))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

struct PartitionInputExec {
    rx: Mutex<Option<Receiver<RecordBatch>>>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl PartitionInputExec {
    fn new(rx: Receiver<RecordBatch>, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            rx: Mutex::new(Some(rx)),
            schema,
            properties,
        }
    }
}

impl fmt::Debug for PartitionInputExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionInsertExec")
            .field("properties", &self.properties)
            .finish_non_exhaustive()
    }
}

impl ExecutionPlan for PartitionInputExec {
    fn name(&self) -> &'static str {
        "PartitionInsertExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Plan(format!(
            "{} expects no children",
            self.name()
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let rx = {
            let mut rx = self.rx.lock();
            match rx.take() {
                Some(rx) => rx,
                None => {
                    return Err(DataFusionError::Plan(format!(
                        "{} can only be executed once",
                        self.name()
                    )));
                }
            }
        };

        let rx_stream = ReceiverStream::new(rx);
        let stream = RecordBatchStreamAdapter::new(Arc::clone(&self.schema), rx_stream.map(Ok));
        Ok(Box::pin(stream))
    }
}

impl DisplayAs for PartitionInputExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Evaluate the `physical_expr` for each row in `batch`. A partition batch is
/// created for each unique value produced by evaluating the expression
/// containing the rows that produced that unique partition value.
///
/// # Errors
/// Returns an error when the expressions cannot be evaluated, the batch cannot
/// be partitioned, Arrays cannot be created or the batch cannot be filtered.
pub fn partition_batch(
    batch: &RecordBatch,
    physical_expr: &dyn PhysicalExpr,
) -> Result<HashMap<String, (ScalarValue, RecordBatch)>, DataFusionError> {
    let column = physical_expr.evaluate(batch)?;
    let array = match column {
        ColumnarValue::Array(array) => array,
        ColumnarValue::Scalar(_) => {
            return Err(DataFusionError::Execution(
                "Invalid partition expression".to_string(),
            ));
        }
    };

    let partitions = compute::partition(&[Arc::clone(&array)])?;
    let mut batches = HashMap::with_capacity(partitions.len());

    // Group indices by partition value
    let mut value_to_indices: HashMap<String, Vec<usize>> = HashMap::new();
    for partition in partitions.ranges() {
        let partition_value = ScalarValue::try_from_array(&array, partition.start)?;
        let partition_key = encode_key(&partition_value).map_err(|e| {
            DataFusionError::Execution(format!("Failed to encode partition key: {e}"))
        })?;
        let value_indices = value_to_indices.entry(partition_key.clone()).or_default();
        partition.into_iter().for_each(|i| value_indices.push(i));
    }

    // Create batches for each partition
    for (partition_key, indices) in value_to_indices {
        if indices.is_empty() {
            continue;
        }
        let partition_value = ScalarValue::try_from_array(&array, indices[0])?;
        let new_batch = filter_batch_by_indices(batch, &indices)?;
        batches.insert(partition_key, (partition_value, new_batch));
    }

    Ok(batches)
}

/// Strategy for handling custom insertion logic in partition tables
#[async_trait::async_trait]
pub trait InsertStrategy: Send + Sync + std::fmt::Debug {
    /// Handle the insertion with custom logic
    ///
    /// # Arguments
    /// * `input` - The input execution plan
    /// * `insert_op` - The insert operation (append/overwrite)
    /// * `context` - Access to partition context (creator, partitions, schema, etc.)
    ///
    /// # Returns
    /// An execution plan that handles the custom insertion
    async fn execute_insert(
        &self,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
        context: &PartitionContext,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>;
}

/// Context information for custom insertion handlers
#[derive(Debug)]
pub struct PartitionContext {
    pub creator: Arc<dyn PartitionCreator>,
    pub partition_by: PartitionedBy,
    pub partitions: Arc<RwLock<HashMap<ScalarValueString, Partition>>>,
    pub schema: SchemaRef,
}

/// Default insertion strategy that uses the existing [`PartitionerExec`]
#[derive(Debug)]
pub struct DefaultInsertStrategy;

#[async_trait::async_trait]
impl InsertStrategy for DefaultInsertStrategy {
    async fn execute_insert(
        &self,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
        context: &PartitionContext,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(PartitionerExec::new(
            input,
            context.partition_by.clone(),
            Arc::clone(&context.creator),
            Arc::clone(&context.partitions),
            insert_op,
            Arc::clone(&context.schema),
        )))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::array::record_batch,
        prelude::{col, lit},
    };

    use super::*;

    #[test]
    fn test_partition_batch_single() -> Result<(), DataFusionError> {
        let expr = col("region").eq(lit("us-east-1"));

        let batch = record_batch!(
            ("id", Int64, [1, 2, 3]),
            ("region", Utf8, ["us-east-1", "us-east-1", "us-east-1"])
        )?;

        let physical_expr = create_physical_expr(&expr, batch.schema())?;

        let partitions = partition_batch(&batch, physical_expr.as_ref())?;

        assert_eq!(partitions.len(), 1);

        for (partition_value, partitioned_batch) in partitions.into_values() {
            assert_eq!(partition_value, ScalarValue::Boolean(Some(true)));
            assert_eq!(batch, partitioned_batch);
        }

        Ok(())
    }

    #[test]
    fn test_partition_batch_multiple() -> Result<(), DataFusionError> {
        let expr = col("region").eq(lit("us-east-1"));

        let batch = record_batch!(
            ("id", Int64, [1, 2, 3, 4, 5, 6]),
            (
                "region",
                Utf8,
                [
                    "us-east-1",
                    "us-east-2",
                    "us-west-1",
                    "us-east-1",
                    "us-east-2",
                    "us-west-1"
                ]
            )
        )?;

        let physical_expr = create_physical_expr(&expr, batch.schema())?;

        let partitions = partition_batch(&batch, physical_expr.as_ref())?;

        assert_eq!(partitions.len(), 2);

        for (partition_value, partitioned_batch) in partitions.into_values() {
            if partition_value == ScalarValue::Boolean(Some(true)) {
                assert_eq!(
                    record_batch!(
                        ("id", Int64, [1, 4]),
                        ("region", Utf8, ["us-east-1", "us-east-1"])
                    )?,
                    partitioned_batch
                );
            } else {
                assert_eq!(
                    record_batch!(
                        ("id", Int64, [2, 3, 5, 6]),
                        (
                            "region",
                            Utf8,
                            ["us-east-2", "us-west-1", "us-east-2", "us-west-1"]
                        )
                    )?,
                    partitioned_batch
                );
            }
        }

        Ok(())
    }
}
