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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Float64Array, StringArray, TimestampSecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::common::Constraints;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use snafu::prelude::*;
use tokio::sync::RwLock;

pub type PartitionData = Arc<RwLock<Vec<RecordBatch>>>;

pub struct MetricsTable {
    pub schema: SchemaRef,
    pub batches: RwLock<Vec<PartitionData>>,
    constraints: Constraints,
}

impl Default for MetricsTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsTable {
    pub fn new() -> Self {
        let fields = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("metric", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("labels", DataType::Utf8, false),
        ];

        let schema = Arc::new(Schema::new(fields));

        Self {
            schema,
            batches: RwLock::new(vec![]),
            constraints: Constraints::empty(),
        }
    }

    pub async fn add_record_batch(&self, record_batch: RecordBatch) {
        let mut batches = self.batches.write().await;
        batches.push(Arc::new(RwLock::new(vec![record_batch])));
        drop(batches);
    }

    pub async fn add_record(
        &self,
        timestamp: i64,
        metric: &str,
        value: f64,
        label: &str,
    ) -> Result<(), super::Error> {
        let mut batches = self.batches.write().await;

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(TimestampSecondArray::from(vec![timestamp])),
                Arc::new(StringArray::from(vec![metric])),
                Arc::new(Float64Array::from(vec![value])),
                Arc::new(StringArray::from(vec![label])),
            ],
        )
        .context(super::UnableToCreateRecordBatchSnafu)?;

        batches.push(Arc::new(RwLock::new(vec![batch])));

        drop(batches);

        Ok(())
    }
}

#[async_trait]
impl TableProvider for MetricsTable {
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
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut partitions = vec![];
        let batches_guard = &self.batches.read().await;
        for arc_inner_vec in batches_guard.iter() {
            let inner_vec = arc_inner_vec.read().await;
            partitions.push(inner_vec.clone());
        }
        Ok(Arc::new(MemoryExec::try_new(
            &partitions,
            self.schema(),
            projection.cloned(),
        )?))
    }
}
