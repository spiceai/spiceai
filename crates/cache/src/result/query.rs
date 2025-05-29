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

use std::collections::HashSet;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::TableReference;
use futures::Stream;
use futures::task::{Context, Poll};

use crate::Sizeable;

use super::CacheStatus;

#[derive(Clone)]
pub struct CachedQueryResult {
    pub records: Arc<Vec<RecordBatch>>,
    pub schema: Arc<Schema>,
    pub input_tables: Arc<HashSet<TableReference>>,
}

impl Sizeable for CachedQueryResult {
    fn get_memory_size(&self) -> usize {
        self.records
            .iter()
            .map(arrow::array::RecordBatch::get_array_memory_size)
            .sum()
    }
}

pub struct CachedStream {
    /// Vector of record batches
    data: Arc<Vec<RecordBatch>>,
    /// Schema representing the data
    schema: SchemaRef,
    index: usize,
}

impl CachedStream {
    #[must_use]
    pub fn try_new(data: Arc<Vec<RecordBatch>>, schema: SchemaRef) -> Self {
        Self {
            data,
            schema,
            index: 0,
        }
    }
}

impl Stream for CachedStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            let index = self.index;
            let batch = self.data.get(index).cloned().map(Ok);
            self.index += 1;
            batch
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl RecordBatchStream for CachedStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

pub struct QueryResult {
    pub data: SendableRecordBatchStream,
    pub cache_status: CacheStatus,
}

impl std::fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResult")
            .field("data", &"<stream>")
            .field("cache_status", &self.cache_status)
            .finish()
    }
}

impl QueryResult {
    #[must_use]
    pub fn new(data: SendableRecordBatchStream, cache_status: CacheStatus) -> Self {
        QueryResult { data, cache_status }
    }
}
