/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

    /// Returns the accurate deep memory size of this cache entry.
    /// Includes array data, `RecordBatch` overhead, and schema size.
    #[must_use]
    pub fn memory_size(&self) -> u64 {
        let mut size = std::mem::size_of::<Self>() as u64;

        match &self.data {
            CachedData::Raw(batches) => {
                for batch in batches.iter() {
                    // Use RecordBatch's get_array_memory_size which accounts for all array data
                    size += batch.get_array_memory_size() as u64;
                    // Add RecordBatch struct overhead (small fixed cost per batch)
                    size += std::mem::size_of::<RecordBatch>() as u64;
                }
            }
            CachedData::Encoded(bytes) => {
                size += bytes.len() as u64;
            }
        }

        size
    }
}

impl Sizeable for CachedQueryResult {
    fn get_memory_size(&self) -> usize {
        // Delegate to accurate memory_size() method, cap at usize::MAX.
        // If the value does not fit into usize (e.g., on 32-bit platforms), log and saturate.
        let total_size = self.memory_size();
        if let Ok(size) = usize::try_from(total_size) {
            size
        } else {
            tracing::warn!(
                actual_size = total_size,
                "CachedQueryResult::memory_size exceeds usize::MAX; saturating to usize::MAX"
            );
            usize::MAX
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_memory_size_raw_batches() {
        // Create a schema with different data types
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_col", DataType::Int32, false),
            Field::new("string_col", DataType::Utf8, true),
        ]));

        // Create record batches with known data
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    Some("hello"),
                    Some("world"),
                    Some("test"),
                    None,
                    Some("data"),
                ])),
            ],
        )
        .expect("should create batch");

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![6, 7, 8])),
                Arc::new(StringArray::from(vec![Some("more"), Some("data"), None])),
            ],
        )
        .expect("should create batch");

        let batches = vec![batch1.clone(), batch2.clone()];
        let input_tables = Arc::new(HashSet::new());
        let cached_at = Instant::now();

        let cached_result = CachedQueryResult::new_raw(batches, input_tables, cached_at);

        // Calculate expected size
        let expected_size = std::mem::size_of::<CachedQueryResult>() as u64
            + batch1.get_array_memory_size() as u64
            + std::mem::size_of::<RecordBatch>() as u64
            + batch2.get_array_memory_size() as u64
            + std::mem::size_of::<RecordBatch>() as u64;

        let actual_size = cached_result.memory_size();

        assert_eq!(
            actual_size, expected_size,
            "Memory size should accurately reflect RecordBatch data size"
        );

        // Verify the size is reasonable (not zero, not absurdly large)
        assert!(
            actual_size > 0,
            "Memory size should be greater than zero for non-empty batches"
        );
        assert!(
            actual_size < 10_000,
            "Memory size should be reasonable for small test data"
        );
    }

    #[test]
    fn test_memory_size_encoded_data() {
        let encoded_data = Bytes::from(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "test",
            DataType::Int32,
            false,
        )]));
        let input_tables = Arc::new(HashSet::new());
        let cached_at = Instant::now();

        let cached_result =
            CachedQueryResult::new(encoded_data.clone(), schema, input_tables, cached_at, None);

        let expected_size =
            std::mem::size_of::<CachedQueryResult>() as u64 + encoded_data.len() as u64;
        let actual_size = cached_result.memory_size();

        assert_eq!(
            actual_size, expected_size,
            "Memory size should equal struct size plus encoded data length"
        );
    }

    #[test]
    fn test_memory_size_empty_batches() {
        let batches = Vec::new();
        let input_tables = Arc::new(HashSet::new());
        let cached_at = Instant::now();

        let cached_result = CachedQueryResult::new_raw(batches, input_tables, cached_at);

        let expected_size = std::mem::size_of::<CachedQueryResult>() as u64;
        let actual_size = cached_result.memory_size();

        assert_eq!(
            actual_size, expected_size,
            "Memory size for empty batches should be just struct overhead"
        );
    }

    #[test]
    fn test_sizeable_trait_implementation() {
        // Create a result with known size
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("should create batch");

        let cached_result =
            CachedQueryResult::new_raw(vec![batch], Arc::new(HashSet::new()), Instant::now());

        let memory_size = cached_result.memory_size();
        let sizeable_size = cached_result.get_memory_size();

        // Should match (unless memory_size exceeds usize::MAX, which won't happen in tests)
        assert_eq!(
            sizeable_size as u64, memory_size,
            "Sizeable trait should delegate to memory_size()"
        );
    }
}
