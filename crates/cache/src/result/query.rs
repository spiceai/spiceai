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
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::TableReference;
use futures::Stream;
use futures::task::{Context, Poll};

use crate::AsTableRefs;
use crate::Sizeable;
use crate::encoding::Encoder;

use super::CacheStatus;

/// Cached data storage - either raw `RecordBatches` (no encoding) or encoded bytes.
#[derive(Debug, Clone)]
pub enum CachedData {
    /// Raw `RecordBatches` stored directly (encoding: none)
    Raw(Arc<Vec<RecordBatch>>),
    /// IPC-serialized bytes, additionally compressed (e.g., with zstd)
    Encoded(Bytes),
}

#[derive(Clone)]
pub struct CachedQueryResult {
    /// Cached record batches (raw or encoded)
    data: CachedData,
    /// Schema for the cached data
    pub schema: Arc<Schema>,
    /// Input tables referenced by the query
    pub input_tables: Arc<HashSet<TableReference>>,
    /// Timestamp when the result was cached.
    cached_at: Instant,
    /// Encoder used to decode the data
    encoder: Option<Arc<dyn Encoder>>,
}

impl CachedQueryResult {
    /// Create a new cached query result with raw `RecordBatches`.
    #[must_use]
    pub fn new_raw(
        batches: Vec<RecordBatch>,
        input_tables: Arc<HashSet<TableReference>>,
        cached_at: Instant,
    ) -> Self {
        let schema = if batches.is_empty() {
            Arc::new(Schema::empty())
        } else {
            batches[0].schema()
        };

        Self {
            data: CachedData::Raw(Arc::new(batches)),
            schema,
            input_tables,
            cached_at,
            encoder: None,
        }
    }

    /// Create a new cached query result with encoded data.
    #[must_use]
    pub fn new(
        encoded_data: Bytes,
        schema: Arc<Schema>,
        input_tables: Arc<HashSet<TableReference>>,
        cached_at: Instant,
        encoder: Option<Arc<dyn Encoder>>,
    ) -> Self {
        Self {
            data: CachedData::Encoded(encoded_data),
            schema,
            input_tables,
            cached_at,
            encoder,
        }
    }

    /// Create a cached query result from record batches.
    /// Only store encoded data if an encoder is provided.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding fails.
    pub async fn from_batches(
        records: &[RecordBatch],
        input_tables: Arc<HashSet<TableReference>>,
        cached_at: Instant,
        encoder: Option<Arc<dyn Encoder>>,
    ) -> Result<Self, crate::encoding::Error> {
        let schema = if records.is_empty() {
            Arc::new(Schema::empty())
        } else {
            records[0].schema()
        };

        // Only store encoded data if an encoder is provided
        let data = if let Some(encoder) = encoder.as_ref() {
            let encoded_data = encoder.encode(records).await?;
            CachedData::Encoded(Bytes::from(encoded_data))
        } else {
            CachedData::Raw(Arc::new(records.to_vec()))
        };

        Ok(Self {
            data,
            schema,
            input_tables,
            cached_at,
            encoder,
        })
    }

    /// Decode and return the cached record batches.
    ///
    /// # Errors
    ///
    /// Returns an error if decoding fails.
    pub async fn records(&self) -> Result<Vec<RecordBatch>, crate::encoding::Error> {
        match &self.data {
            CachedData::Raw(batches) => Ok((**batches).clone()),
            CachedData::Encoded(bytes) => {
                if let Some(encoder) = &self.encoder {
                    encoder.decode(bytes).await
                } else {
                    Err(crate::encoding::Error::NoEncoderSpecified)
                }
            }
        }
    }

    /// Check if the cached data is stale (older than the given TTL).
    #[must_use]
    pub fn is_stale(&self, ttl: Duration, now: Instant) -> bool {
        now.duration_since(self.cached_at) > ttl
    }

    #[must_use]
    pub fn cached_at(&self) -> Instant {
        self.cached_at
    }
}

impl Sizeable for CachedQueryResult {
    fn get_memory_size(&self) -> usize {
        match &self.data {
            CachedData::Raw(batches) => batches
                .iter()
                .map(|batch| {
                    batch
                        .columns()
                        .iter()
                        .map(|array| array.get_array_memory_size())
                        .sum::<usize>()
                })
                .sum(),
            CachedData::Encoded(bytes) => bytes.len(),
        }
    }
}

impl AsTableRefs for CachedQueryResult {
    fn as_table_refs(&self) -> Arc<HashSet<TableReference>> {
        Arc::clone(&self.input_tables)
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
    pub fn new(data: Arc<Vec<RecordBatch>>, schema: SchemaRef) -> Self {
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
