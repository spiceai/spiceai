use std::{any::Any, sync::Arc};

use datafusion::arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::{
    common::Statistics,
    error::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        common,
        stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter},
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
};

/// A Mock ExecutionPlan that can be used for writing tests of other
/// ExecutionPlans
#[derive(Debug)]
pub struct MockExec {
    /// the results to send back
    data: Vec<Result<RecordBatch>>,
    schema: SchemaRef,
    /// if true (the default), sends data using a separate task to ensure the
    /// batches are not available without this stream yielding first
    use_task: bool,
    cache: PlanProperties,
}

impl MockExec {
    /// Create a new `MockExec` with a single partition that returns
    /// the specified `Results`s.
    ///
    /// By default, the batches are not produced immediately (the
    /// caller has to actually yield and another task must run) to
    /// ensure any poll loops are correct. This behavior can be
    /// changed with `with_use_task`
    pub fn new(data: Vec<Result<RecordBatch>>, schema: SchemaRef) -> Self {
        let cache = Self::compute_properties(Arc::clone(&schema));
        Self {
            data,
            schema,
            use_task: true,
            cache,
        }
    }

    /// If `use_task` is true (the default) then the batches are sent
    /// back using a separate task to ensure the underlying stream is
    /// not immediately ready
    pub fn with_use_task(mut self, use_task: bool) -> Self {
        self.use_task = use_task;
        self
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for MockExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "MockExec")
            }
        }
    }
}

impl ExecutionPlan for MockExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    /// Returns a stream which yields data
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0);

        // Result doesn't implement clone, so do it ourself
        let data: Vec<_> = self
            .data
            .iter()
            .map(|r| match r {
                Ok(batch) => Ok(batch.clone()),
                Err(e) => Err(clone_error(e)),
            })
            .collect();

        if self.use_task {
            let mut builder = RecordBatchReceiverStream::builder(self.schema(), 2);
            // send data in order but in a separate task (to ensure
            // the batches are not available without the stream
            // yielding).
            let tx = builder.tx();
            builder.spawn(async move {
                for batch in data {
                    println!("Sending batch via delayed stream");
                    if let Err(e) = tx.send(batch).await {
                        println!("ERROR batch via delayed stream: {e}");
                    }
                }

                Ok(())
            });
            // returned stream simply reads off the rx stream
            Ok(builder.build())
        } else {
            // make an input that will error
            let stream = futures::stream::iter(data);
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                stream,
            )))
        }
    }

    // Panics if one of the batches is an error
    fn statistics(&self) -> Result<Statistics> {
        let data: Result<Vec<_>> = self
            .data
            .iter()
            .map(|r| match r {
                Ok(batch) => Ok(batch.clone()),
                Err(e) => Err(clone_error(e)),
            })
            .collect();

        let data = data?;

        Ok(common::compute_record_batch_statistics(
            &[data],
            &self.schema,
            None,
        ))
    }
}

fn clone_error(e: &DataFusionError) -> DataFusionError {
    use DataFusionError::*;
    match e {
        Execution(msg) => Execution(msg.to_string()),
        _ => unimplemented!(),
    }
}
