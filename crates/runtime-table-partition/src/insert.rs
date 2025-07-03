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

use arrow_schema::SchemaRef;
use datafusion::arrow::array::{Array, UInt64Array};
use datafusion::arrow::compute;
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::{EquivalenceProperties, create_physical_expr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::LazyBatchGenerator;
use datafusion::physical_plan::{
    DisplayAs, EmptyRecordBatchStream, Partitioning, PlanProperties, execute_stream,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion::{
    arrow::record_batch::RecordBatch,
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{ExecutionPlan, SendableRecordBatchStream, memory::LazyMemoryExec},
    prelude::Expr,
};
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::block_in_place;

use crate::Partition;
use crate::creator::PartitionCreator;

#[derive(Debug)]
pub struct PartitionInsertExec<ConnectionPool> {
    input: Arc<dyn ExecutionPlan>,
    creator: Arc<dyn PartitionCreator<ConnectionPool = ConnectionPool>>,
    partitions: Arc<RwLock<HashMap<String, Partition<ConnectionPool>>>>,
    partition_by: Expr,
    insert_op: InsertOp,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl<ConnectionPool> PartitionInsertExec<ConnectionPool>
where
    ConnectionPool: std::fmt::Debug + Send + Sync + 'static,
{
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        partition_by: Expr,
        creator: Arc<dyn PartitionCreator<ConnectionPool = ConnectionPool>>,
        partitions: Arc<RwLock<HashMap<String, Partition<ConnectionPool>>>>,
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
            partition_by,
            creator,
            partitions,
            insert_op,
            schema,
            properties,
        }
    }
}

impl<ConnectionPool> DisplayAs for PartitionInsertExec<ConnectionPool> {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "PartitionInsertExec")
    }
}

impl<ConnectionPool> ExecutionPlan for PartitionInsertExec<ConnectionPool>
where
    ConnectionPool: std::fmt::Debug + Send + Sync + 'static,
{
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
            return Err(DataFusionError::Plan(
                "PartitionInsertExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
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
        // We want to stream the input RecordBatches, partition them into
        // RecordBatches destined for a particular partitioned file, stream
        // them into that partitioned file without buffering all of the data,
        // and call `insert_into` on the DuckDB Table Providers only once
        // because data are rewritten every call to `insert_into`.
        //
        // We also have an interesting mix of async/sync contexts here in
        // `execute` which we deal with by using `block_in_place` and
        // `Handle::block_on`.
        //
        // In one task, we stream the input RecordBatches and partition them
        // according to the partition_by expression. If the partition has not
        // yet been created, we create it. We also create a channel for sending
        // RecordBatches. We make a LazyMemExec ExecutionPlan which we pass to
        // that partition's `insert_into` one time. The LazyMemExec is passed
        // a generator that we create which generates record batches out of the
        // receiving end of the partition's RecordBatch channel. Each partition
        // `insert_into` invokation is ran in a separate task. So we have one
        // partitioning task and N insertion tasks where N is the number of
        // partitions.

        let session_config = context.session_config();
        let ctx = SessionContext::new_with_config(session_config.clone());

        if partition != 0 {
            return Err(DataFusionError::Execution(
                "PartitionInsertExec only supports single partition".to_string(),
            ));
        }

        let insert_op = self.insert_op;
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        let props = ExecutionProps::new();
        let physical_expr = create_physical_expr(&self.partition_by, &df_schema, &props)?;
        let schema = Arc::clone(&self.schema);
        let creator = Arc::clone(&self.creator);
        let partition_providers = Arc::clone(&self.partitions);
        let input = Arc::clone(&self.input);
        let task_ctx = Arc::clone(&context);

        let mut partition_senders: HashMap<String, Sender<RecordBatch>> = HashMap::new();

        let mut stream = execute_stream(input, task_ctx)?;

        block_in_place(move || {
            Handle::current().block_on(async move {
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    if batch.num_rows() == 0 {
                        continue;
                    }

                    let column = physical_expr.evaluate(&batch)?;
                    let array = match column {
                        ColumnarValue::Array(array) => array,
                        ColumnarValue::Scalar(_) => {
                            return Err(DataFusionError::Execution(
                                "Invalid partition expression".to_string(),
                            ));
                        }
                    };

                    let partitions = compute::partition(&[Arc::clone(&array)])?;
                    for indices in partitions.ranges() {
                        if indices.is_empty() {
                            continue;
                        }
                        let indices = indices.collect::<Vec<_>>();
                        let partition_value = ScalarValue::try_from_array(&array, indices[0])?;
                        let partition_key = partition_value.to_string();
                        let new_batch = filter_batch_by_indices(&batch, &indices)?;

                        let tx = if let Some(tx) = partition_senders.get(&partition_key) {
                            tx.clone()
                        } else {
                            // Create a new partition
                            let partition = creator
                                .create_partition(partition_value.clone())
                                .await
                                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                            let new_provider = Arc::clone(&partition.table_provider);
                            let mut partitions = partition_providers.write().await;
                            partitions.insert(partition_key.clone(), partition);

                            // Create a new channel
                            let (tx, rx) = channel(100);
                            partition_senders.insert(partition_key.clone(), tx.clone());

                            // Create the generator
                            let generator = BatchGenerator {
                                partition_value,
                                rx,
                            };

                            // Create the Lazy execution plan for the table provider
                            let exec = LazyMemoryExec::try_new(
                                Arc::clone(&schema),
                                vec![Arc::new(parking_lot::RwLock::new(generator))],
                            )?;

                            let state = ctx.state();
                            let context = Arc::clone(&context);

                            // spawn the insertion task for this partition
                            tokio::spawn(async move {
                                let plan = new_provider
                                    .insert_into(&state, Arc::new(exec), insert_op)
                                    .await?;

                                let mut stream = execute_stream(plan, context)?;
                                while let Some(batch) = stream.next().await {
                                    batch?;
                                }

                                Result::<(), DataFusionError>::Ok(())
                            });

                            tx
                        };

                        // Send the partitioned RecordBatch to the partition's
                        // channel
                        let _ = tx.send(new_batch).await;
                    }
                }

                Ok(())
            })
        })?;

        Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
            &self.schema,
        ))))
    }

    fn name(&self) -> &str {
        "PartitionInsertExec"
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}

#[derive(Debug)]
struct BatchGenerator {
    partition_value: ScalarValue,
    rx: Receiver<RecordBatch>,
}

impl fmt::Display for BatchGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchGenerator")
            .field("partition_value", &self.partition_value)
            .finish_non_exhaustive()
    }
}

impl LazyBatchGenerator for BatchGenerator {
    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>, DataFusionError> {
        block_in_place(|| Handle::current().block_on(async { Ok(self.rx.recv().await) }))
    }
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
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| DataFusionError::ArrowError(e, None))
}
