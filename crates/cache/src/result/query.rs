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
use crate::sizing::{
    ARC_HEADER_BYTES, ENTRY_OVERHEAD_BYTES, arc_heap_size, schema_size, table_refs_size,
};

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
    /// When the query that produced this result began reading.
    ///
    /// Serving this entry is only sound while none of [`Self::input_tables`]
    /// has been invalidated since this instant, which is what
    /// [`crate::QueryResultsCacheProvider::get_raw_key`] checks on every hit.
    /// It is deliberately *not* [`Self::cached_at`]: an invalidation landing
    /// between the read and the store must also disqualify the entry, and
    /// `cached_at` is after both.
    pub read_started_at: Instant,
    /// Encoder used to decode the data
    encoder: Option<Arc<dyn Encoder>>,
}

impl CachedQueryResult {
    /// Create a new cached query result with raw `RecordBatches`.
    ///
    /// The `schema` parameter must be provided explicitly to ensure the correct
    /// schema is preserved even when `batches` is empty (e.g., 0-row query results).
    #[must_use]
    pub fn new_raw(
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        input_tables: Arc<HashSet<TableReference>>,
        cached_at: Instant,
        read_started_at: Instant,
    ) -> Self {
        Self {
            data: CachedData::Raw(Arc::new(super::compact_for_storage(batches))),
            schema,
            input_tables,
            cached_at,
            read_started_at,
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
        read_started_at: Instant,
        encoder: Option<Arc<dyn Encoder>>,
    ) -> Self {
        Self {
            data: CachedData::Encoded(encoded_data),
            schema,
            input_tables,
            cached_at,
            read_started_at,
            encoder,
        }
    }

    /// Create a cached query result from record batches.
    /// Only store encoded data if an encoder is provided.
    ///
    /// The `schema` parameter must be provided explicitly to ensure the correct
    /// schema is preserved even when `records` is empty (e.g., 0-row query results).
    ///
    /// # Errors
    ///
    /// Returns an error if encoding fails.
    pub async fn from_batches(
        records: Vec<RecordBatch>,
        schema: SchemaRef,
        input_tables: Arc<HashSet<TableReference>>,
        cached_at: Instant,
        read_started_at: Instant,
        encoder: Option<Arc<dyn Encoder>>,
    ) -> Result<Self, crate::encoding::Error> {
        // Only store encoded data if an encoder is provided
        let data = if let Some(encoder) = encoder.as_ref() {
            let encoded_data = encoder.encode(&records).await?;
            CachedData::Encoded(Bytes::from(encoded_data))
        } else {
            CachedData::Raw(Arc::new(super::compact_for_storage(records)))
        };

        Ok(Self {
            data,
            schema,
            input_tables,
            cached_at,
            read_started_at,
            encoder,
        })
    }

    /// Decode and return the cached record batches.
    ///
    /// # Errors
    ///
    /// Returns an error if decoding fails.
    pub async fn records(&self) -> Result<Arc<Vec<RecordBatch>>, crate::encoding::Error> {
        match &self.data {
            CachedData::Raw(batches) => Ok(Arc::clone(batches)),
            CachedData::Encoded(bytes) => {
                if let Some(encoder) = &self.encoder {
                    encoder.decode(bytes).await.map(Arc::new)
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

    /// The memory this entry holds, as the cache's byte budget sees it.
    ///
    /// Everything reachable from the entry is counted, not just its array
    /// bytes: the schema, the input-table set, and a flat allowance for the
    /// store's own per-entry bookkeeping. A 0-row result carries no array bytes
    /// at all, so counting only those made it weigh a flat `size_of::<Self>()`
    /// regardless of how wide its schema was, and the byte budget could never
    /// evict one. See [`crate::sizing`] for the imprecisions this accepts.
    #[must_use]
    pub fn memory_size(&self) -> u64 {
        let mut size = std::mem::size_of::<Self>();

        match &self.data {
            CachedData::Raw(batches) => {
                size += arc_heap_size::<Vec<RecordBatch>>()
                    + batches.len() * std::mem::size_of::<RecordBatch>();
                for batch in batches.iter() {
                    // get_array_memory_size accounts for all array data.
                    size += batch.get_array_memory_size();
                }
            }
            CachedData::Encoded(bytes) => {
                size += bytes.len();
            }
        }

        size += ARC_HEADER_BYTES + schema_size(&self.schema);
        size += ARC_HEADER_BYTES + table_refs_size(&self.input_tables);
        size += ENTRY_OVERHEAD_BYTES;

        size as u64
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

impl crate::ReadStartedAt for CachedQueryResult {
    fn read_started_at(&self) -> Instant {
        self.read_started_at
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
        let input_tables = Arc::new(HashSet::from([TableReference::bare("sales")]));
        let cached_at = Instant::now();

        let cached_result = CachedQueryResult::new_raw(
            batches,
            Arc::clone(&schema),
            Arc::clone(&input_tables),
            cached_at,
            cached_at,
        );

        let expected_size = std::mem::size_of::<CachedQueryResult>() as u64
            + crate::sizing::arc_heap_size::<Vec<RecordBatch>>() as u64
            + 2 * std::mem::size_of::<RecordBatch>() as u64
            + batch1.get_array_memory_size() as u64
            + batch2.get_array_memory_size() as u64
            + (crate::sizing::ARC_HEADER_BYTES + crate::sizing::schema_size(&schema)) as u64
            + (crate::sizing::ARC_HEADER_BYTES + crate::sizing::table_refs_size(&input_tables))
                as u64
            + crate::sizing::ENTRY_OVERHEAD_BYTES as u64;

        assert_eq!(
            cached_result.memory_size(),
            expected_size,
            "an entry must be billed its batches, its schema, its input tables and the store's per-entry overhead"
        );
        assert!(
            cached_result.memory_size() < 10_000,
            "Memory size should be reasonable for small test data, got {}",
            cached_result.memory_size()
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

        let cached_result = CachedQueryResult::new(
            encoded_data.clone(),
            schema,
            input_tables,
            cached_at,
            cached_at,
            None,
        );

        let expected_size = std::mem::size_of::<CachedQueryResult>() as u64
            + encoded_data.len() as u64
            + (crate::sizing::ARC_HEADER_BYTES + crate::sizing::schema_size(&cached_result.schema))
                as u64
            + (crate::sizing::ARC_HEADER_BYTES
                + crate::sizing::table_refs_size(&cached_result.input_tables)) as u64
            + crate::sizing::ENTRY_OVERHEAD_BYTES as u64;

        assert_eq!(
            cached_result.memory_size(),
            expected_size,
            "an encoded entry must be billed its bytes plus everything it holds around them"
        );
    }

    fn empty_result_of_width(columns: usize) -> CachedQueryResult {
        let schema = Arc::new(Schema::new(
            (0..columns)
                .map(|i| Field::new(format!("column_{i}"), DataType::Int64, true))
                .collect::<Vec<_>>(),
        ));
        let cached_at = Instant::now();

        CachedQueryResult::new_raw(
            Vec::new(),
            schema,
            Arc::new(HashSet::from([TableReference::bare("wide")])),
            cached_at,
            cached_at,
        )
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/12931>.
    ///
    /// A 0-row result contributes no array bytes, so when only those were
    /// counted it weighed a flat `size_of::<Self>()` — 82 bytes, whatever its
    /// schema — and the byte budget could never evict one. Cost has to scale
    /// with what the entry actually holds.
    #[test]
    fn an_empty_result_is_billed_more_than_its_struct() {
        let narrow = empty_result_of_width(4);
        let wide = empty_result_of_width(200);
        let struct_only = std::mem::size_of::<CachedQueryResult>() as u64;

        assert!(
            narrow.memory_size() > struct_only,
            "a 0-row entry still holds a schema and an input-table set, got {} vs {struct_only}",
            narrow.memory_size()
        );
        assert!(
            wide.memory_size() > 10 * narrow.memory_size(),
            "a 200-column 0-row entry must cost far more than a 4-column one, got {} vs {}",
            wide.memory_size(),
            narrow.memory_size()
        );
    }

    /// The bound `max_size` is meant to be: N entries of a known weight must not
    /// fit in a budget smaller than N times that weight. Before the fix a
    /// 1 MiB budget admitted 12,840 wide 0-row entries — ~500 MiB of real memory.
    #[test]
    fn a_byte_budget_bounds_a_stream_of_empty_results() {
        let entry_weight = empty_result_of_width(200).memory_size();
        let budget = 1024 * 1024_u64;

        let admissible = budget / entry_weight;
        assert!(
            admissible < 200,
            "a 1 MiB budget must not admit thousands of wide 0-row entries, it admits {admissible} at {entry_weight} bytes each"
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

        let cached_result = CachedQueryResult::new_raw(
            vec![batch],
            Arc::clone(&schema),
            Arc::new(HashSet::new()),
            Instant::now(),
            Instant::now(),
        );

        let memory_size = cached_result.memory_size();
        let sizeable_size = cached_result.get_memory_size();

        // Should match (unless memory_size exceeds usize::MAX, which won't happen in tests)
        assert_eq!(
            sizeable_size as u64, memory_size,
            "Sizeable trait should delegate to memory_size()"
        );
    }

    use crate::utils::tests::wide_string_batch;

    fn only_payload(batch: &RecordBatch) -> String {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload is a StringArray")
            .value(0)
            .to_string()
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/12921>.
    /// An entry built from a slice must not hold — or be billed — the batch the
    /// slice was carved out of.
    #[test]
    fn a_sliced_entry_is_billed_its_own_rows_new_raw() {
        let scan_batch = wide_string_batch(2_000);
        let sliced = scan_batch.slice(1_000, 1);
        let cached_at = Instant::now();

        let cached_result = CachedQueryResult::new_raw(
            vec![sliced.clone()],
            sliced.schema(),
            Arc::new(HashSet::new()),
            cached_at,
            cached_at,
        );

        assert!(
            cached_result.memory_size() * 100 < scan_batch.get_array_memory_size() as u64,
            "a one-row entry sliced from a 2000-row batch should be billed a small fraction of it, got {} of {}",
            cached_result.memory_size(),
            scan_batch.get_array_memory_size()
        );
    }

    /// The same store path, exercised through `from_batches` — what background
    /// revalidation uses — and asserting the row itself survives compaction.
    #[tokio::test]
    async fn a_sliced_entry_is_billed_its_own_rows_from_batches() {
        let scan_batch = wide_string_batch(2_000);
        let sliced = scan_batch.slice(1_000, 1);
        let expected_payload = only_payload(&sliced);
        let cached_at = Instant::now();

        let cached_result = CachedQueryResult::from_batches(
            vec![sliced.clone()],
            sliced.schema(),
            Arc::new(HashSet::new()),
            cached_at,
            cached_at,
            None,
        )
        .await
        .expect("should create cached result");

        assert!(
            cached_result.memory_size() * 100 < scan_batch.get_array_memory_size() as u64,
            "a one-row entry should be billed a small fraction of its parent, got {} of {}",
            cached_result.memory_size(),
            scan_batch.get_array_memory_size()
        );

        let records = cached_result.records().await.expect("should decode");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].num_rows(), 1);
        assert_eq!(
            only_payload(&records[0]),
            expected_payload,
            "compacting the entry must not change the row it holds"
        );
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/9481>
    /// Empty query results must preserve the correct schema, not `Schema::empty()`.
    #[test]
    fn test_empty_batches_preserve_schema_new_raw() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]));

        let cached_result = CachedQueryResult::new_raw(
            Vec::new(),
            Arc::clone(&schema),
            Arc::new(HashSet::new()),
            Instant::now(),
            Instant::now(),
        );

        assert_eq!(
            cached_result.schema.fields().len(),
            3,
            "Cached empty result must preserve the original 3-field schema"
        );
        assert_eq!(cached_result.schema, schema);
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/9481>
    #[tokio::test]
    async fn test_empty_batches_preserve_schema_from_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]));

        let cached_result = CachedQueryResult::from_batches(
            Vec::new(),
            Arc::clone(&schema),
            Arc::new(HashSet::new()),
            Instant::now(),
            Instant::now(),
            None,
        )
        .await
        .expect("should create cached result");

        assert_eq!(
            cached_result.schema.fields().len(),
            3,
            "Cached empty result must preserve the original 3-field schema"
        );
        assert_eq!(cached_result.schema, schema);

        // Verify the CachedStream also reports the correct schema
        let records = cached_result.records().await.expect("should decode");
        assert!(records.is_empty(), "Should have no record batches");

        let stream = CachedStream::new(records, Arc::clone(&cached_result.schema));
        assert_eq!(
            stream.schema().fields().len(),
            3,
            "CachedStream schema must match the original schema"
        );
    }
}
