// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`MemTable`] for querying `Vec<RecordBatch>` by `DataFusion`.

// This is modified from the DataFusion `MemTable` to support overwrites. This file can be removed once that change is upstreamed.
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::{Constraints, SchemaExt};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::insert::{DataSink, FileSinkExec};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::StreamExt;
use tokio::sync::RwLock;

use crate::DeleteTableProvider;

/// Type alias for partition data
pub type PartitionData = Arc<RwLock<Vec<RecordBatch>>>;

/// In-memory data source for presenting a `Vec<RecordBatch>` as a
/// data source that can be queried by `DataFusion`. This allows data to
/// be pre-loaded into memory and then repeatedly queried without
/// incurring additional file I/O overhead.
#[derive(Debug)]
pub struct MemTable {
    schema: SchemaRef,
    pub(crate) batches: Vec<PartitionData>,
    constraints: Constraints,
    column_defaults: HashMap<String, Expr>,
    /// Optional pre-known sort order(s). Must be `SortExpr`s.
    /// inserting data into this table removes the order
    pub sort_order: Arc<Mutex<Vec<Vec<Expr>>>>,
}

impl MemTable {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn try_new(schema: SchemaRef, mut partitions: Vec<Vec<RecordBatch>>) -> Result<Self> {
        for batches in partitions.iter().flatten() {
            let batches_schema = batches.schema();
            if !schema.contains(&batches_schema) {
                tracing::debug!(
                    "mem table schema does not contain batches schema. \
                        Target_schema: {schema:?}. Batches Schema: {batches_schema:?}"
                );
                return Err(DataFusionError::Plan(
                    "Mismatch between schema and batches".to_string(),
                ));
            }
        }

        // Add at least one partition
        if partitions.is_empty() {
            partitions.extend([vec![]]);
        }

        Ok(Self {
            schema,
            batches: partitions
                .into_iter()
                .map(|e| Arc::new(RwLock::new(e)))
                .collect::<Vec<_>>(),
            constraints: Constraints::empty(),
            column_defaults: HashMap::new(),
            sort_order: Arc::new(Mutex::new(vec![])),
        })
    }

    /// Assign constraints
    #[must_use]
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Assign column defaults
    #[must_use]
    pub fn with_column_defaults(mut self, column_defaults: HashMap<String, Expr>) -> Self {
        self.column_defaults = column_defaults;
        self
    }
}

#[async_trait]
impl TableProvider for MemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut partitions = vec![];
        for arc_inner_vec in &self.batches {
            let inner_vec = arc_inner_vec.read().await;
            partitions.push(inner_vec.clone());
        }
        Ok(Arc::new(MemoryExec::try_new(
            &partitions,
            self.schema(),
            projection.cloned(),
        )?))
    }

    /// Returns an ExecutionPlan that inserts the execution results of a given [`ExecutionPlan`] into this [`MemTable`].
    ///
    /// The [`ExecutionPlan`] must have the same schema as this [`MemTable`].
    ///
    /// # Arguments
    ///
    /// * `state` - The [`SessionState`] containing the context for executing the plan.
    /// * `input` - The [`ExecutionPlan`] to execute and insert.
    ///
    /// # Returns
    ///
    /// * A plan that returns the number of rows written.
    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create a physical plan from the logical plan.
        // Check that the schema of the plan matches the schema of this table.
        if !self
            .schema()
            .logically_equivalent_names_and_types(&input.schema())
        {
            return Err(DataFusionError::Execution(
                "Inserting query must have the same schema with the table.".to_string(),
            ));
        }

        let sink = Arc::new(MemSink::new(self.batches.clone(), overwrite));
        Ok(Arc::new(FileSinkExec::new(
            input,
            sink,
            self.schema.clone(),
            None,
        )))
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.column_defaults.get(column)
    }
}

#[async_trait]
impl DeleteTableProvider for MemTable {
    async fn delete_from(
        &self,
        _state: &SessionState,
        _filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MemDeletionExec::new(
            self.batches.clone(),
            self.schema.clone(),
        )))
    }
}

struct MemDeletionExec {
    batches: Vec<PartitionData>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl MemDeletionExec {
    fn new(batches: Vec<PartitionData>, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            batches,
            schema,
            properties,
        }
    }
}

impl std::fmt::Debug for MemDeletionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DataUpdateExecutionPlan")
    }
}

impl DisplayAs for MemDeletionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DataUpdateExecutionPlan")
    }
}

impl ExecutionPlan for MemDeletionExec {
    fn name(&self) -> &'static str {
        "MemDeletionExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    /// Execute the plan and return a stream of `RecordBatch`es for
    /// the specified partition.
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = futures::stream::once(async move {
            let array = Arc::new(UInt64Array::from(vec![1])) as ArrayRef;

            Ok(RecordBatch::try_from_iter_with_nullable(vec![("count", array, false)]).unwrap())
        })
        .boxed();

        let count_schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema,
            stream,
        )))
    }
}

/// Implements for writing to a [`MemTable`]
struct MemSink {
    /// Target locations for writing data
    batches: Vec<PartitionData>,
    overwrite: bool,
}

#[allow(clippy::missing_fields_in_debug)]
impl Debug for MemSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemSink")
            .field("num_partitions", &self.batches.len())
            .finish()
    }
}

impl DisplayAs for MemSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let partition_count = self.batches.len();
                write!(f, "MemoryTable (partitions={partition_count})")
            }
        }
    }
}

impl MemSink {
    fn new(batches: Vec<PartitionData>, overwrite: bool) -> Self {
        Self { batches, overwrite }
    }
}

#[async_trait]
impl DataSink for MemSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let num_partitions = self.batches.len();

        if self.overwrite {
            for partition in &self.batches {
                let mut partition_vec = partition.write().await;
                partition_vec.clear();
                drop(partition_vec);
            }
        }

        // buffer up the data round robin style into num_partitions

        let mut new_batches = vec![vec![]; num_partitions];
        let mut i = 0;
        let mut row_count = 0;
        while let Some(batch) = data.next().await.transpose()? {
            row_count += batch.num_rows();
            new_batches[i].push(batch);
            i = (i + 1) % num_partitions;
        }

        // write the outputs into the batches
        for (target, mut batches) in self.batches.iter().zip(new_batches.into_iter()) {
            // Append all the new batches in one go to minimize locking overhead
            target.write().await.append(&mut batches);
        }

        Ok(row_count as u64)
    }
}
