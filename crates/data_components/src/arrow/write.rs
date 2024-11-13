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

use datafusion::catalog::Session;
// This is modified from the DataFusion `MemTable` to support overwrites. This file can be removed once that change is upstreamed.
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::dml::InsertOp;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug};

use std::sync::{Arc, Mutex};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::common::{Constraints, SchemaExt};
use datafusion::datasource::{provider_as_source, TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{is_not_true, Expr, LogicalPlanBuilder};
use datafusion::physical_plan::insert::{DataSink, DataSinkExec};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::StreamExt;
use tokio::sync::RwLock;

use crate::delete::{DeletionExec, DeletionSink, DeletionTableProvider};
use datafusion_table_providers::util::retriable_error::check_and_mark_retriable_error;

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
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
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
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
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
        Ok(Arc::new(DataSinkExec::new(
            input,
            sink,
            Arc::clone(&self.schema),
            None,
        )))
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.column_defaults.get(column)
    }
}

/// Implements for writing to a [`MemTable`]
struct MemSink {
    /// Target locations for writing data
    batches: Vec<PartitionData>,
    overwrite: InsertOp,
}

impl Debug for MemSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemSink")
            .field("num_partitions", &self.batches.len())
            .finish_non_exhaustive()
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
    fn new(batches: Vec<PartitionData>, overwrite: InsertOp) -> Self {
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

        // buffer up the data round robin style into num_partitions

        let mut new_batches = vec![vec![]; num_partitions];
        let mut i = 0;
        let mut row_count = 0;
        while let Some(batch) = data
            .next()
            .await
            .transpose()
            .map_err(check_and_mark_retriable_error)?
        {
            row_count += batch.num_rows();
            new_batches[i].push(batch);
            i = (i + 1) % num_partitions;
        }

        let mut writable_targets: Vec<_> =
            futures::future::join_all(self.batches.iter().map(|target| target.write())).await;

        for (target, mut batches) in writable_targets.iter_mut().zip(new_batches.into_iter()) {
            if matches!(self.overwrite, InsertOp::Overwrite) {
                target.clear();
            }
            target.append(&mut batches);
        }

        Ok(row_count as u64)
    }
}

#[async_trait]
impl DeletionTableProvider for MemTable {
    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(
            Arc::new(MemDeletionSink::new(
                self.batches.clone(),
                self.schema(),
                filters,
            )),
            &self.schema(),
        )))
    }
}

struct MemDeletionSink {
    batches: Vec<PartitionData>,
    schema: SchemaRef,
    filters: Vec<Expr>,
}

impl MemDeletionSink {
    fn new(batches: Vec<PartitionData>, schema: SchemaRef, filters: &[Expr]) -> Self {
        Self {
            batches,
            schema,
            filters: filters.to_vec(),
        }
    }
}

#[async_trait]
impl DeletionSink for MemDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let batches = self.batches.clone();

        let ctx = SessionContext::new();
        let mut tmp_batches = vec![vec![]; batches.len()];

        for (i, partition) in batches.iter().enumerate() {
            let mut partition_vec = partition.write().await;
            tmp_batches[i].append(&mut *partition_vec);
        }

        let provider = MemTable::try_new(Arc::clone(&self.schema), tmp_batches)?;

        let mut df = DataFrame::new(
            ctx.state(),
            LogicalPlanBuilder::scan("?table?", provider_as_source(Arc::new(provider)), None)?
                .build()?,
        );

        let mut count = df.clone().count().await?;

        for filter in self.filters.clone() {
            df = df.filter(is_not_true(filter))?;
        }

        count -= df.clone().count().await?;
        let mut new_batches = vec![vec![]; batches.len()];
        let mut i = 0;
        for vec in df.collect_partitioned().await? {
            for batch in vec {
                new_batches[i].push(batch);
            }

            i = (i + 1) % batches.len();
        }

        for (target, mut batches) in batches.iter().zip(new_batches.into_iter()) {
            target.write().await.append(&mut batches);
        }

        Ok(count as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Schema},
    };
    use datafusion::{
        execution::context::SessionContext,
        logical_expr::{cast, col, lit},
        physical_plan::collect,
        scalar::ScalarValue,
    };

    use crate::{arrow::write::MemTable, delete::DeletionTableProvider};

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
    async fn test_delete_from() {
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "time_in_string",
            DataType::Utf8,
            false,
        )]));
        let arr = StringArray::from(vec![
            "1970-01-01",
            "2012-12-01T11:11:11Z",
            "2012-12-01T11:11:12Z",
        ]);

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
            .expect("data should be created");

        let table =
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created");

        let ctx = SessionContext::new();

        let filter = cast(
            col("time_in_string"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        .lt(lit(ScalarValue::TimestampMillisecond(
            Some(1354360272000),
            None,
        )));

        let plan = table
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
