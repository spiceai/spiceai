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

use datafusion::{
    catalog::TableProvider, logical_expr::dml::InsertOp, physical_plan::RecordBatchStream,
};
use multi::MultiSink;
use std::{pin::Pin, sync::Arc};
use table::TableSink;
use util::RetryError;

use super::synchronized_table::SynchronizedTable;

pub(crate) mod multi;
pub(crate) mod table;

pub enum AccelerationSink {
    Table(TableSink),
    Multi(MultiSink),
}

impl AccelerationSink {
    pub fn new(table_provider: Arc<dyn TableProvider>) -> Self {
        Self::Table(TableSink::new(table_provider))
    }

    // Adds a table provider to the AccelerationSink, converting a TableSink to a MultiSink if necessary
    pub fn add_synchronized_table(&mut self, synchronized_table: SynchronizedTable) {
        match self {
            AccelerationSink::Table(table_sink) => {
                let table_provider = Arc::clone(&table_sink.table_provider);
                let multi_sink = MultiSink::new(table_provider, vec![synchronized_table]);
                *self = AccelerationSink::Multi(multi_sink);
            }
            AccelerationSink::Multi(sink) => sink.add_synchronized_table(synchronized_table),
        }
    }

    pub fn synchronized_tables(&self) -> Vec<&SynchronizedTable> {
        match self {
            AccelerationSink::Table(_) => vec![],
            AccelerationSink::Multi(sink) => sink.synchronized_tables().iter().collect(),
        }
    }

    pub async fn insert_into(
        &self,
        record_batch_stream: Pin<Box<dyn RecordBatchStream + Send>>,
        overwrite: InsertOp,
    ) -> Result<(), RetryError<crate::accelerated_table::Error>> {
        match self {
            AccelerationSink::Table(sink) => sink.insert_into(record_batch_stream, overwrite).await,
            AccelerationSink::Multi(sink) => sink.insert_into(record_batch_stream, overwrite).await,
        }
    }
}
