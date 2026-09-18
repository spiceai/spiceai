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
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use futures::Stream;
use futures::StreamExt;
use futures::task::{Context, Poll};

use crate::AsTableRefs;
use crate::Sizeable;
use crate::encoding::Encoder;
use crate::intern::Interned;
use crate::sizing::{ARC_HEADER_BYTES, BUFFER_OVERHEAD_BYTES, ENTRY_OVERHEAD_BYTES, arc_heap_size};

use super::CacheStatus;

/// Shared raw result batches: one `Arc` around a slice of pre-`Arc`'d batches.
///
/// A Raw cache hit clones this slice handle once. The SQL serve path
/// ([`CachedRawStream`], [`QueryResult::from_cached_raw`]) yields
/// `Arc<RecordBatch>` so each poll is one atomic, not a `RecordBatch::clone`
/// of every column. [`CachedStream`] is the search-cache path and still
/// materializes an owned `RecordBatch` for `DataFusion`.
pub type CachedBatches = Arc<[Arc<RecordBatch>]>;

/// A boxed Raw SQL serve stream. HTTP and Flight drain this so a hit pays
/// one `Arc` clone per batch.
pub type SendableCachedRawStream =
    Pin<Box<dyn Stream<Item = Result<Arc<RecordBatch>, DataFusionError>> + Send>>;

/// Wrap owned batches for Raw storage / serve. Moves each batch into an `Arc`
/// so a later hit shares the batch with one atomic rather than rebuilding a
/// `Vec` of independently cloned column `ArrayRef`s.
#[must_use]
pub fn wrap_raw_batches(batches: Vec<RecordBatch>) -> CachedBatches {
    batches.into_iter().map(Arc::new).collect()
}

/// Heap billed for a [`CachedBatches`] slice: the outer `Arc<[Arc<RecordBatch>]>`
/// allocation, plus one `Arc<RecordBatch>` header and `RecordBatch` struct per
/// element. Array buffers are charged by the caller.
fn raw_batches_heap_size(batches: &CachedBatches) -> usize {
    ARC_HEADER_BYTES
        + batches.len() * std::mem::size_of::<Arc<RecordBatch>>()
        + batches.len() * arc_heap_size::<RecordBatch>()
}

/// Cached data storage - either raw `RecordBatches` (no encoding) or encoded bytes.
#[derive(Debug, Clone)]
pub enum CachedData {
    /// Raw `RecordBatches` stored directly (encoding: none).
    ///
    /// Each batch is pre-`Arc`'d so a hit clones one slice handle and the SQL
    /// serve path can hand a batch out with a single atomic.
    Raw(CachedBatches),
    /// IPC-serialized bytes, additionally compressed (e.g., with zstd)
    Encoded {
        bytes: Bytes,
        /// The size of the Arrow IPC stream `bytes` decodes to. See
        /// [`crate::encoding::Encoded::decoded_len`].
        decoded_len: usize,
    },
}

#[derive(Clone)]
pub struct CachedQueryResult {
    /// Cached record batches (raw or encoded)
    data: CachedData,
    /// Schema for the cached data.
    ///
    /// [`Interned`] rather than `Arc`: it can only have come from the pool, so
    /// it is shared with every other entry of this shape and the weigher's
    /// decision not to charge for it holds however this struct is built.
    pub schema: Interned<Schema>,
    /// Input tables referenced by the query. [`Interned`] for the same reason
    /// as [`Self::schema`].
    pub input_tables: Interned<HashSet<TableReference>>,
    /// Timestamp when the result was cached.
    cached_at: Instant,
    /// When the query that produced this result began reading.
    ///
    /// Serving this entry *as fresh* is only sound while none of
    /// [`Self::input_tables`] has been invalidated since this instant, which is
    /// what [`crate::QueryResultsCacheProvider::entry_validity`] rules on for
    /// every hit. It is deliberately *not* [`Self::cached_at`]: an invalidation
    /// landing between the read and the store must also disqualify the entry,
    /// and `cached_at` is after both.
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
            data: CachedData::Raw(wrap_raw_batches(super::prepare_for_storage(batches))),
            schema: crate::intern::schema::intern(schema),
            input_tables: crate::intern::table_set::intern(input_tables),
            cached_at,
            read_started_at,
            encoder: None,
        }
    }

    /// Create a new cached query result with encoded data.
    ///
    /// `decoded_len` is the size of the Arrow IPC stream `encoded_data` decodes to.
    #[must_use]
    pub fn new(
        encoded_data: Bytes,
        decoded_len: usize,
        schema: Arc<Schema>,
        input_tables: Arc<HashSet<TableReference>>,
        cached_at: Instant,
        read_started_at: Instant,
        encoder: Option<Arc<dyn Encoder>>,
    ) -> Self {
        Self {
            data: CachedData::Encoded {
                bytes: encoded_data,
                decoded_len,
            },
            schema: crate::intern::schema::intern(schema),
            input_tables: crate::intern::table_set::intern(input_tables),
            cached_at,
            read_started_at,
            encoder,
        }
    }

    /// Create a cached query result from record batches.
    ///
    /// Encoded whenever an encoder is configured, which is what
    /// `caching.sql_results.encoding` selects. A hit on a small encoded entry is
    /// still served where the request arrived: the decode is bounded there by
    /// the runtime's `INLINE_DECODE_MAX_BYTES`, read from [`Self::decoded_len`].
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
        // An encoder is configured for the whole cache, so every entry it can
        // encode is encoded: `caching.sql_results.encoding` says what the cache
        // compresses with, and storing some entries raw would make it describe
        // only part of what it holds. A hit on a small encoded entry is still
        // served where the request arrived — the decode is bounded there by
        // `INLINE_DECODE_MAX_BYTES`, which reads `Self::decoded_len`.
        let data = if let Some(encoder) = encoder.as_ref() {
            let payload = encoder.encode(&records).await?;
            CachedData::Encoded {
                bytes: Bytes::from(payload.bytes),
                decoded_len: payload.decoded_len,
            }
        } else {
            CachedData::Raw(wrap_raw_batches(super::prepare_for_storage(records)))
        };

        Ok(Self {
            data,
            schema: crate::intern::schema::intern(schema),
            input_tables: crate::intern::table_set::intern(input_tables),
            cached_at,
            read_started_at,
            encoder,
        })
    }

    /// Decode and return the cached record batches.
    ///
    /// Raw entries return the stored pre-`Arc`'d slice (`Arc::clone` of the
    /// handle only). Encoded entries decode, then [`wrap_raw_batches`], so
    /// [`QueryResult::from_cached_raw`] can serve either kind without a second
    /// column-`Arc` pass. `encoded_stream_serve` in `cache_hit_costs` measures
    /// that wrap-and-serve against decode-only and the `Arc<Vec<_>>` stream.
    ///
    /// # Errors
    ///
    /// Returns an error if decoding fails.
    pub async fn records(&self) -> Result<CachedBatches, crate::encoding::Error> {
        match &self.data {
            CachedData::Raw(batches) => Ok(Arc::clone(batches)),
            CachedData::Encoded { bytes, .. } => {
                if let Some(encoder) = &self.encoder {
                    encoder.decode(bytes).await.map(wrap_raw_batches)
                } else {
                    Err(crate::encoding::Error::NoEncoderSpecified)
                }
            }
        }
    }

    /// The pre-`Arc`'d batches of a Raw entry, if this value is stored raw.
    ///
    /// Isolated from [`Self::records`]: encoded hits still decode through
    /// that path. A Raw serve uses this so it never goes through a `Vec`
    /// rebuild.
    #[must_use]
    pub fn raw_batches(&self) -> Option<CachedBatches> {
        match &self.data {
            CachedData::Raw(batches) => Some(Arc::clone(batches)),
            CachedData::Encoded { .. } => None,
        }
    }

    /// Whether this entry holds encoded bytes rather than the batches themselves.
    #[must_use]
    pub fn is_encoded(&self) -> bool {
        matches!(self.data, CachedData::Encoded { .. })
    }

    /// The size of the Arrow IPC stream an encoded entry decodes to, or `None` for an entry
    /// held as batches.
    ///
    /// Reading an encoded entry decompresses and decodes all of that stream, so this, not the
    /// size of the stored payload, is what the read costs; reading a raw entry hands out the
    /// batches it already holds.
    #[must_use]
    pub fn decoded_len(&self) -> Option<usize> {
        match &self.data {
            CachedData::Raw(_) => None,
            CachedData::Encoded { decoded_len, .. } => Some(*decoded_len),
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
    /// Everything the entry holds *of its own* is counted, not just its array
    /// bytes: its batches and a flat allowance for the store's own per-entry
    /// bookkeeping. A 0-row result carries no array bytes
    /// at all, so counting only those made it weigh a flat `size_of::<Self>()`
    /// regardless of how wide its schema was, and the byte budget could never
    /// evict one. See [`crate::sizing`] for the imprecisions this accepts.
    ///
    /// The schema and the input-table set are deliberately **not** charged here.
    /// Both are interned, so one allocation is shared by every entry over the
    /// same shape, and charging either per entry would bill a 200-column schema
    /// tens of thousands of times over for memory that exists once.
    ///
    /// This holds even when a shape is **unique to one entry** — that case is
    /// intentional, not an oversight. Such an entry does hold an allocation no
    /// weigher charges, so a workload of unboundedly many distinct output
    /// shapes can exceed `max_size` by the size of its schemas and table sets. The considered
    /// alternative was charging `schema_size / Arc::strong_count` at insert,
    /// which self-corrects because it degrades to the full charge exactly when
    /// deduplication fails. It was rejected: making every entry's weight depend
    /// on how many others happen to share its shape at that instant means an
    /// entry's charge varies with unrelated traffic, and the common case — one
    /// shape behind many entries — is precisely where per-entry billing was
    /// wrong to begin with.
    ///
    /// What keeps that residual case from being silent is reporting rather than
    /// admission: [`crate::intern::schema::SchemaInterner::stats`] counts
    /// each distinct schema once and is published as
    /// `schema_interner_value_bytes` (with `table_set_interner_value_bytes` for
    /// the table sets), so a pool growing without bound is visible even though
    /// it does not trigger eviction.
    #[must_use]
    pub fn memory_size(&self) -> u64 {
        let mut size = std::mem::size_of::<Self>();

        match &self.data {
            CachedData::Raw(batches) => {
                size += raw_batches_heap_size(batches);
                for batch in batches.iter() {
                    // get_array_memory_size accounts for all array data.
                    size += batch.get_array_memory_size();
                    // ...but not for what each of those buffers costs beyond
                    // the bytes it asked the allocator for. See
                    // `BUFFER_OVERHEAD_BYTES`.
                    size +=
                        BUFFER_OVERHEAD_BYTES * arrow_tools::record_batch::buffers_in_batch(batch);
                }
            }
            CachedData::Encoded { bytes, .. } => {
                size += bytes.len();
            }
        }

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
        self.input_tables.arc()
    }
}

/// Search-cache stream: indexes a shared `Arc<Vec<RecordBatch>>` and
/// materializes the owned `RecordBatch` `DataFusion` requires on each poll.
///
/// SQL Raw hits use [`CachedRawStream`] / [`QueryResult::from_cached_raw`]
/// instead — that path yields `Arc<RecordBatch>` so HTTP and Flight do not
/// increment every column `ArrayRef`.
pub struct CachedStream {
    data: Arc<Vec<RecordBatch>>,
    /// Schema representing the data
    schema: SchemaRef,
    index: usize,
}

impl CachedStream {
    /// Serve a shared `Arc<Vec<RecordBatch>>` (search cache, tests).
    ///
    /// Indexes the vec in place. Search aggregation and other `DataFusion`
    /// consumers still need an owned `RecordBatch` per poll. SQL Raw hits
    /// use [`CachedRawStream::from_raw`].
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
        let index = self.index;
        let Some(batch) = self.data.get(index).cloned() else {
            return Poll::Ready(None);
        };
        self.index = index + 1;
        Poll::Ready(Some(Ok(batch)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.data.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl RecordBatchStream for CachedStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Raw SQL results-cache serve stream: each poll is `Arc::clone` of a stored
/// batch (one atomic), not `RecordBatch::clone` of every column.
///
/// HTTP and Flight drain this via [`QueryResult::from_cached_raw`] /
/// [`QueryResultSource::CachedRaw`]. Prefetches the first batch's data
/// buffers and the next batch's headers at construction, and the following
/// batch's headers on each later poll.
pub struct CachedRawStream {
    data: CachedBatches,
    schema: SchemaRef,
    index: usize,
}

impl CachedRawStream {
    /// Serve a Raw (or just-decoded) pre-`Arc`'d slice.
    ///
    /// Prefetches the first batch's data buffers and the next batch's headers
    /// before returning so HTTP JSON / Flight IPC see warm lines on first poll.
    #[must_use]
    pub fn from_raw(data: CachedBatches, schema: SchemaRef) -> Self {
        super::prefetch::prefetch_raw_serve_arced(&data);
        Self {
            data,
            schema,
            index: 0,
        }
    }

    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for CachedRawStream {
    type Item = Result<Arc<RecordBatch>, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let index = self.index;
        let Some(batch) = self.data.get(index).map(Arc::clone) else {
            return Poll::Ready(None);
        };
        self.index = index + 1;
        if let Some(next) = self.data.get(self.index) {
            super::prefetch::prefetch_batch_headers(next.as_ref());
        }
        Poll::Ready(Some(Ok(batch)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.data.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

/// Canonical serve stream for a [`QueryResult`].
///
/// Raw hits stay on [`Self::CachedRaw`] so HTTP and Flight can take the Arc
/// path via [`QueryResult::into_source`]. Polling as a [`Stream`] still
/// yields owned `RecordBatch`s, so `.data` callers share the same
/// cancellation and tracker wrappers.
pub enum QueryResultData {
    /// Planned or search-cache path: `DataFusion`'s owned-batch stream.
    Stream(SendableRecordBatchStream),
    /// Raw SQL cache hit: one `Arc` clone per batch on the HTTP/Flight path.
    /// Prefetch already ran at [`CachedRawStream::from_raw`].
    CachedRaw {
        data: SendableCachedRawStream,
        schema: SchemaRef,
    },
}

impl Stream for QueryResultData {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::Stream(stream) => Pin::new(stream).poll_next(cx),
            Self::CachedRaw { data, .. } => Pin::new(data).poll_next(cx).map(|item| {
                item.map(|result| result.map(|batch| RecordBatch::clone(batch.as_ref())))
            }),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Stream(stream) => stream.size_hint(),
            Self::CachedRaw { data, .. } => data.size_hint(),
        }
    }
}

impl RecordBatchStream for QueryResultData {
    fn schema(&self) -> SchemaRef {
        match self {
            Self::Stream(stream) => stream.schema(),
            Self::CachedRaw { schema, .. } => Arc::clone(schema),
        }
    }
}

/// How a [`QueryResult`] is consumed by HTTP / Flight / `QueryEngine`.
pub enum QueryResultSource {
    /// Planned or search-cache path: `DataFusion`'s owned-batch stream.
    Stream {
        data: SendableRecordBatchStream,
        cache_status: CacheStatus,
    },
    /// Raw SQL cache hit: one `Arc` clone per batch.
    CachedRaw {
        data: SendableCachedRawStream,
        schema: SchemaRef,
        cache_status: CacheStatus,
    },
}

pub struct QueryResult {
    /// The one serve stream. Wrappers (cancel, tracker, span) attach here
    /// so `.data` and [`Self::into_source`] observe the same lifetime.
    pub data: QueryResultData,
    pub cache_status: CacheStatus,
}

impl std::fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResult")
            .field(
                "data",
                &match self.data {
                    QueryResultData::Stream(_) => "<stream>",
                    QueryResultData::CachedRaw { .. } => "<arc-stream>",
                },
            )
            .field("cache_status", &self.cache_status)
            .finish_non_exhaustive()
    }
}

impl QueryResult {
    #[must_use]
    pub fn new(data: SendableRecordBatchStream, cache_status: CacheStatus) -> Self {
        Self {
            data: QueryResultData::Stream(data),
            cache_status,
        }
    }

    /// Serve pre-`Arc`'d Raw (or just-decoded) batches on the SQL path.
    ///
    /// [`Self::into_source`] yields [`QueryResultSource::CachedRaw`] so HTTP
    /// and Flight clone one `Arc<RecordBatch>` per poll. Polling [`Self::data`]
    /// yields owned batches from that same stream.
    #[must_use]
    pub fn from_cached_raw(
        batches: CachedBatches,
        schema: SchemaRef,
        cache_status: CacheStatus,
    ) -> Self {
        Self {
            data: QueryResultData::CachedRaw {
                data: Box::pin(CachedRawStream::from_raw(batches, Arc::clone(&schema))),
                schema,
            },
            cache_status,
        }
    }

    /// Whether this result carries the Arc-clone SQL serve stream.
    #[must_use]
    pub fn has_cached_raw(&self) -> bool {
        matches!(self.data, QueryResultData::CachedRaw { .. })
    }

    /// Schema of the result stream.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        self.data.schema()
    }

    /// Schema of the Arc-clone SQL serve stream, when present.
    #[must_use]
    pub fn cached_schema(&self) -> Option<SchemaRef> {
        match &self.data {
            QueryResultData::CachedRaw { schema, .. } => Some(Arc::clone(schema)),
            QueryResultData::Stream(_) => None,
        }
    }

    /// Replace the Arc-clone SQL serve stream (cancellation / tracker wrap).
    #[must_use]
    pub fn map_cached_raw(
        mut self,
        f: impl FnOnce(SendableCachedRawStream) -> SendableCachedRawStream,
    ) -> Self {
        if let QueryResultData::CachedRaw { data, schema } = self.data {
            self.data = QueryResultData::CachedRaw {
                data: f(data),
                schema,
            };
        }
        self
    }

    /// Replace the owned-batch `DataFusion` stream (planned path, `QueryEngine`).
    #[must_use]
    pub fn map_data(
        mut self,
        f: impl FnOnce(SendableRecordBatchStream) -> SendableRecordBatchStream,
    ) -> Self {
        if let QueryResultData::Stream(data) = self.data {
            self.data = QueryResultData::Stream(f(data));
        }
        self
    }

    /// Consume the result as the stream HTTP and Flight should drain.
    ///
    /// A Raw SQL hit becomes [`QueryResultSource::CachedRaw`] (the wrapped
    /// Arc stream, including cancellation and tracker). Other results stay
    /// on the owned-batch stream.
    #[must_use]
    pub fn into_source(self) -> QueryResultSource {
        match self.data {
            QueryResultData::CachedRaw { data, schema } => QueryResultSource::CachedRaw {
                data,
                schema,
                cache_status: self.cache_status,
            },
            QueryResultData::Stream(data) => QueryResultSource::Stream {
                data,
                cache_status: self.cache_status,
            },
        }
    }

    /// Consume as a `DataFusion` owned-batch stream.
    ///
    /// A Raw SQL hit maps each `Arc<RecordBatch>` with `RecordBatch::clone`
    /// so `QueryEngine` and tests keep the previous item type. HTTP and Flight
    /// should call [`Self::into_source`] instead.
    #[must_use]
    pub fn into_record_batch_stream(self) -> SendableRecordBatchStream {
        match self.into_source() {
            QueryResultSource::Stream { data, .. } => data,
            QueryResultSource::CachedRaw { data, schema, .. } => {
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    data.map(|item| item.map(|batch| RecordBatch::clone(batch.as_ref()))),
                ))
            }
        }
    }

    /// Drain every batch. Prefers the Arc serve path when present so a
    /// cache-hit collect still finishes the tracker attached there.
    ///
    /// # Errors
    ///
    /// Returns the first stream error.
    pub async fn collect_batches(self) -> Result<Vec<RecordBatch>, DataFusionError> {
        match self.into_source() {
            QueryResultSource::CachedRaw { mut data, .. } => {
                let mut batches = Vec::new();
                while let Some(item) = data.next().await {
                    batches.push(RecordBatch::clone(item?.as_ref()));
                }
                Ok(batches)
            }
            QueryResultSource::Stream { data, .. } => {
                futures::TryStreamExt::try_collect(data).await
            }
        }
    }

    /// Poll the stream to completion without retaining batches.
    ///
    /// Intermediate transaction statements use this so a large `SELECT` does
    /// not stay resident until `COMMIT`.
    ///
    /// # Errors
    ///
    /// Returns the first stream error.
    pub async fn drain(self) -> Result<(), DataFusionError> {
        match self.into_source() {
            QueryResultSource::CachedRaw { mut data, .. } => {
                while let Some(item) = data.next().await {
                    item?;
                }
                Ok(())
            }
            QueryResultSource::Stream { mut data, .. } => {
                while let Some(item) = data.next().await {
                    item?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use std::pin::Pin;

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

        let CachedData::Raw(stored) = &cached_result.data else {
            panic!("expected raw batches");
        };
        let expected_size = std::mem::size_of::<CachedQueryResult>() as u64
            + raw_batches_heap_size(stored) as u64
            + batch1.get_array_memory_size() as u64
            + batch2.get_array_memory_size() as u64
            + (crate::sizing::BUFFER_OVERHEAD_BYTES
                * (arrow_tools::record_batch::buffers_in_batch(&batch1)
                    + arrow_tools::record_batch::buffers_in_batch(&batch2))) as u64
            + crate::sizing::ENTRY_OVERHEAD_BYTES as u64;

        assert_eq!(
            cached_result.memory_size(),
            expected_size,
            "an entry must be billed its batches, a per-buffer allowance and the store's per-entry overhead — but not the schema or input-table set it shares"
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
            64,
            schema,
            input_tables,
            cached_at,
            cached_at,
            None,
        );

        let expected_size = std::mem::size_of::<CachedQueryResult>() as u64
            + encoded_data.len() as u64
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
        let struct_only = std::mem::size_of::<CachedQueryResult>() as u64;

        assert!(
            narrow.memory_size() > struct_only,
            "a 0-row entry still holds an input-table set and the store's overhead, got {} vs {struct_only}",
            narrow.memory_size()
        );
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/12933>.
    ///
    /// Every `RecordBatch` carries its own `SchemaRef` and nothing upstream
    /// shares them — a batch does not even share with the stream that carried
    /// it — so a stored vector of small batches otherwise re-holds one schema
    /// per element.
    #[test]
    fn stored_batches_share_one_schema_with_their_entry() {
        // Distinct allocations of equal content, as separately-planned queries
        // produce; `Schema::new` on fresh fields cannot return a shared `Arc`.
        let fields = vec![Field::new("id", DataType::Int32, false)];
        let first = Arc::new(Schema::new(fields.clone()));
        let second = Arc::new(Schema::new(fields));
        assert!(!Arc::ptr_eq(&first, &second), "inputs start out distinct");

        let batch_of = |schema: &SchemaRef| {
            RecordBatch::try_new(
                Arc::clone(schema),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .expect("batch")
        };

        let cached_at = Instant::now();
        let result = CachedQueryResult::new_raw(
            vec![batch_of(&first), batch_of(&second)],
            Arc::clone(&first),
            Arc::new(HashSet::new()),
            cached_at,
            cached_at,
        );

        let CachedData::Raw(batches) = &result.data else {
            panic!("expected raw batches");
        };
        assert_eq!(batches.len(), 2);
        for (i, batch) in batches.iter().enumerate() {
            assert!(
                Arc::ptr_eq(batch.schema_ref(), &result.schema.arc()),
                "batch {i} must share the entry's interned schema"
            );
            assert_eq!(batch.num_rows(), 3, "interning must not disturb the rows");
            assert_eq!(batch.num_columns(), 1);
        }
    }

    /// Two entries over the same shape must point at one schema. Each is built
    /// from its own freshly-allocated `Schema`, so pointer equality here can
    /// only come from interning.
    #[test]
    fn entries_over_the_same_shape_share_one_schema() {
        let first = empty_result_of_width(200);
        let second = empty_result_of_width(200);

        assert!(
            Arc::ptr_eq(&first.schema.arc(), &second.schema.arc()),
            "entries of the same shape must share one schema allocation"
        );
        assert_eq!(
            first.schema.fields().len(),
            200,
            "sharing must not alter the schema"
        );
    }

    /// The counterpart to [`entries_over_the_same_shape_share_one_schema`]: a
    /// shared schema is not a per-entry cost, so widening it must not make the
    /// entry heavier. Charging it per entry billed a 200-column schema once for
    /// every entry that merely pointed at it.
    #[test]
    fn schema_width_does_not_change_what_an_entry_is_billed() {
        assert_eq!(
            empty_result_of_width(4).memory_size(),
            empty_result_of_width(200).memory_size(),
            "an interned schema is shared, so its width is not the entry's cost"
        );
    }

    /// The bound `max_size` is meant to be: N entries of a known weight must not
    /// fit in a budget smaller than N times that weight. Before
    /// <https://github.com/spiceai/spiceai/issues/12931> a 1 MiB budget admitted
    /// 12,840 wide 0-row entries holding ~500 MiB, because a 0-row entry
    /// weighed a flat 82 bytes however wide it was.
    ///
    /// The schema those entries share is no longer charged to any of them, so
    /// what has to bound the stream now is each entry's own cost. This asserts
    /// the memory really held — every entry plus the one schema behind them all
    /// — stays within the budget's order of magnitude.
    #[test]
    fn a_byte_budget_bounds_a_stream_of_empty_results() {
        let entry = empty_result_of_width(200);
        let entry_weight = entry.memory_size();
        let budget = 1024 * 1024_u64;

        let admissible = budget / entry_weight;
        assert!(
            admissible < 3_000,
            "a 1 MiB budget must not admit the pre-fix 12,840 wide 0-row entries, it admits {admissible} at {entry_weight} bytes each"
        );

        let schema_bytes = crate::intern::schema::schema_deep_size(&entry.schema) as u64;
        let really_held = admissible * entry_weight + schema_bytes;
        assert!(
            really_held < 2 * budget,
            "the entries admitted plus the single schema they share must stay near the budget, got {really_held} against {budget}"
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
        assert_eq!(cached_result.schema.arc(), schema);
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
        assert_eq!(cached_result.schema.arc(), schema);

        // Verify the Raw serve stream also reports the correct schema
        let records = cached_result.records().await.expect("should decode");
        assert!(records.is_empty(), "Should have no record batches");

        let stream = CachedRawStream::from_raw(records, cached_result.schema.arc());
        assert_eq!(
            stream.schema().fields().len(),
            3,
            "CachedRawStream schema must match the original schema"
        );
    }

    fn encoder() -> Option<Arc<dyn crate::encoding::Encoder>> {
        crate::encoding::get_encoder(spicepod::component::caching::Encoding::Zstd)
    }

    /// A small result is encoded too when zstd is configured: the setting names
    /// what the cache compresses with, not which entries it compresses. A hit on
    /// one is still served where the request arrived, decoded within the
    /// runtime's inline budget.
    #[tokio::test]
    async fn a_small_result_is_encoded_when_an_encoder_is_configured() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("batch");
        let cached_result = CachedQueryResult::from_batches(
            vec![batch],
            schema,
            Arc::new(HashSet::new()),
            Instant::now(),
            Instant::now(),
            encoder(),
        )
        .await
        .expect("should create cached result");

        assert!(
            cached_result.is_encoded(),
            "`encoding: zstd` encodes every entry it can, whatever the result's size"
        );
        let records = cached_result.records().await.expect("decoded batches");
        assert_eq!(records[0].num_rows(), 3);
    }

    /// A large result is encoded when zstd is configured, and round-trips.
    #[tokio::test]
    async fn a_large_result_is_encoded_when_an_encoder_is_configured() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        // ~20 KiB of zeros: large, and highly compressible.
        let values = vec![0i32; 5_000];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values))],
        )
        .expect("batch");

        let cached_result = CachedQueryResult::from_batches(
            vec![batch],
            schema,
            Arc::new(HashSet::new()),
            Instant::now(),
            Instant::now(),
            encoder(),
        )
        .await
        .expect("should create cached result");

        assert!(
            cached_result.is_encoded(),
            "a result over the raw-store budget must still be encoded under zstd"
        );
        let records = cached_result.records().await.expect("decoded batches");
        assert_eq!(records[0].num_rows(), 5_000);
    }

    /// A compressible result larger than the cache `max_size` raw is stored,
    /// because it is encoded first (regression for
    /// <https://github.com/spiceai/spiceai/issues/8508>).
    #[tokio::test]
    async fn a_result_over_a_small_cache_limit_is_encoded() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let n = 300;
        let col = Arc::new(Int32Array::from(vec![0i32; n]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::clone(&col) as _, Arc::clone(&col) as _],
        )
        .expect("batch");
        let raw_bytes = batch.get_array_memory_size();
        let cache_max = 2 * 1024;
        assert!(
            raw_bytes > cache_max,
            "fixture must exceed a 2 KiB cache, got {raw_bytes}"
        );
        let cached_result = CachedQueryResult::from_batches(
            vec![batch],
            schema,
            Arc::new(HashSet::new()),
            Instant::now(),
            Instant::now(),
            encoder(),
        )
        .await
        .expect("should create cached result");

        assert!(
            cached_result.is_encoded(),
            "a result that cannot fit raw in the cache must still be encoded under zstd"
        );
        let records = cached_result.records().await.expect("decoded batches");
        assert_eq!(records[0].num_rows(), n);
    }

    /// Array bytes under `max_size` can still weigh more than `max_size` once
    /// the weigher adds entry and buffer overhead. Those must encode under
    /// zstd or the store path skips them (the #8508 weigher boundary).
    #[tokio::test]
    async fn a_result_under_max_size_in_array_bytes_is_encoded_when_the_weigher_does_not_fit() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let n = 200;
        let col = Arc::new(Int32Array::from(vec![0i32; n]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::clone(&col) as _, Arc::clone(&col) as _],
        )
        .expect("batch");
        let cache_max = 2 * 1024;
        let raw_bytes = batch.get_array_memory_size();
        assert!(
            raw_bytes <= cache_max,
            "fixture must sit under max_size in array bytes, got {raw_bytes}"
        );

        let now = Instant::now();
        let raw = CachedQueryResult::new_raw(
            vec![batch.clone()],
            Arc::clone(&schema),
            Arc::new(HashSet::new()),
            now,
            now,
        );
        assert!(
            raw.memory_size() > u64::try_from(cache_max).expect("2 KiB"),
            "fixture must exceed max_size once weighed, got {} vs {cache_max}",
            raw.memory_size()
        );

        let cached_result = CachedQueryResult::from_batches(
            vec![batch],
            schema,
            Arc::new(HashSet::new()),
            now,
            now,
            encoder(),
        )
        .await
        .expect("should create cached result");

        assert!(
            cached_result.is_encoded(),
            "a result whose weigher exceeds max_size must still be encoded under zstd"
        );
        assert!(
            cached_result.memory_size() <= u64::try_from(cache_max).expect("2 KiB"),
            "encoded entry must fit the cache, got {}",
            cached_result.memory_size()
        );
        let records = cached_result.records().await.expect("decoded batches");
        assert_eq!(records[0].num_rows(), n);
    }

    fn drain_stream(mut stream: CachedStream) -> Vec<RecordBatch> {
        let mut batches = Vec::new();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        loop {
            match Pin::new(&mut stream).poll_next(&mut cx) {
                Poll::Ready(Some(Ok(batch))) => batches.push(batch),
                Poll::Ready(None) => break,
                Poll::Ready(Some(Err(e))) => panic!("CachedStream yielded an error: {e}"),
                Poll::Pending => panic!("CachedStream must be immediately ready"),
            }
        }
        batches
    }

    fn drain_raw_stream(mut stream: CachedRawStream) -> Vec<Arc<RecordBatch>> {
        let mut batches = Vec::new();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        loop {
            match Pin::new(&mut stream).poll_next(&mut cx) {
                Poll::Ready(Some(Ok(batch))) => batches.push(batch),
                Poll::Ready(None) => break,
                Poll::Ready(Some(Err(e))) => panic!("CachedRawStream yielded an error: {e}"),
                Poll::Pending => panic!("CachedRawStream must be immediately ready"),
            }
        }
        batches
    }

    fn int_batch(schema: &SchemaRef, values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .expect("batch")
    }

    /// Empty Raw serve: no batches, schema preserved, `size_hint` is 0.
    #[test]
    fn cached_raw_stream_empty_preserves_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let stream = CachedRawStream::from_raw(wrap_raw_batches(Vec::new()), Arc::clone(&schema));
        assert_eq!(stream.schema(), schema);
        assert_eq!(stream.size_hint(), (0, Some(0)));
        assert!(drain_raw_stream(stream).is_empty());
    }

    /// Single-batch Raw serve: the yielded handle is the stored `Arc`, so a
    /// poll is one atomic, not a `RecordBatch::clone` of every column.
    #[test]
    fn cached_raw_stream_single_batch_shares_the_stored_arc() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = int_batch(&schema, &[1, 2, 3]);
        let stored = wrap_raw_batches(vec![batch]);
        let stream = CachedRawStream::from_raw(Arc::clone(&stored), Arc::clone(&schema));
        assert_eq!(stream.size_hint(), (1, Some(1)));

        let yielded = drain_raw_stream(stream);
        assert_eq!(yielded.len(), 1);
        assert_eq!(yielded[0].num_rows(), 3);
        assert_eq!(yielded[0].schema(), schema);
        assert!(
            Arc::ptr_eq(&yielded[0], &stored[0]),
            "SQL serve must clone the stored Arc<RecordBatch>, not rebuild the batch"
        );
        assert!(
            Arc::ptr_eq(yielded[0].column(0), stored[0].column(0)),
            "serve must share column arrays with the stored batch, not copy buffers"
        );
    }

    /// Multi-batch Raw serve: order, values, remaining `size_hint`, and a
    /// second consumer of the same `CachedBatches` all match.
    #[test]
    fn cached_raw_stream_multi_batch_is_stable_across_consumers() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let stored = wrap_raw_batches(vec![
            int_batch(&schema, &[1, 2]),
            int_batch(&schema, &[3]),
            int_batch(&schema, &[4, 5, 6]),
        ]);

        let mut first = CachedRawStream::from_raw(Arc::clone(&stored), Arc::clone(&schema));
        assert_eq!(first.size_hint(), (3, Some(3)));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let first_batch = match Pin::new(&mut first).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(batch))) => batch,
            other => panic!("expected first batch, got {other:?}"),
        };
        assert_eq!(first.size_hint(), (2, Some(2)));
        assert_eq!(first_batch.num_rows(), 2);
        assert!(Arc::ptr_eq(&first_batch, &stored[0]));

        let rest = drain_raw_stream(first);
        assert_eq!(rest.len(), 2);
        assert_eq!(rest[0].num_rows(), 1);
        assert_eq!(rest[1].num_rows(), 3);

        let second = drain_raw_stream(CachedRawStream::from_raw(
            Arc::clone(&stored),
            Arc::clone(&schema),
        ));
        assert_eq!(second.len(), 3);
        assert_eq!(
            second
                .iter()
                .map(|batch| batch.num_rows())
                .collect::<Vec<_>>(),
            vec![2, 1, 3]
        );
        let col0 = second[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int column");
        assert_eq!(col0.values(), &[1, 2]);
    }

    fn poll_query_result_data(data: &mut QueryResultData) -> Option<RecordBatch> {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Pin::new(data).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(batch))) => Some(batch),
            Poll::Ready(None) => None,
            Poll::Ready(Some(Err(e))) => panic!("QueryResultData yielded an error: {e}"),
            Poll::Pending => panic!("QueryResultData must be immediately ready"),
        }
    }

    /// `QueryResult::from_cached_raw` / `into_source` is the SQL serve
    /// contract: HTTP and Flight poll `Arc<RecordBatch>`.
    #[test]
    fn from_cached_raw_into_source_clones_the_stored_arc() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let stored = wrap_raw_batches(vec![int_batch(&schema, &[9])]);
        let result = QueryResult::from_cached_raw(
            Arc::clone(&stored),
            Arc::clone(&schema),
            CacheStatus::CacheHit,
        );
        assert!(result.has_cached_raw());
        match result.into_source() {
            QueryResultSource::CachedRaw {
                mut data,
                schema: yielded_schema,
                cache_status,
            } => {
                assert_eq!(yielded_schema, schema);
                assert_eq!(cache_status, CacheStatus::CacheHit);
                let waker = futures::task::noop_waker();
                let mut cx = Context::from_waker(&waker);
                let batch = match Pin::new(&mut data).poll_next(&mut cx) {
                    Poll::Ready(Some(Ok(batch))) => batch,
                    other => panic!("expected stored batch, got {other:?}"),
                };
                assert!(
                    Arc::ptr_eq(&batch, &stored[0]),
                    "into_source must yield the stored Arc, not a RecordBatch::clone"
                );
                assert!(matches!(
                    Pin::new(&mut data).poll_next(&mut cx),
                    Poll::Ready(None)
                ));
            }
            QueryResultSource::Stream { .. } => {
                panic!("from_cached_raw must produce QueryResultSource::CachedRaw")
            }
        }
    }

    /// `.data` and `into_source` are the same stream: polling `.data` advances
    /// the Arc source `into_source` would have handed to HTTP / Flight.
    #[test]
    fn from_cached_raw_data_and_into_source_share_one_stream() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let stored = wrap_raw_batches(vec![int_batch(&schema, &[1, 2]), int_batch(&schema, &[3])]);
        let mut result = QueryResult::from_cached_raw(
            Arc::clone(&stored),
            Arc::clone(&schema),
            CacheStatus::CacheHit,
        );
        assert_eq!(result.data.schema(), schema);
        assert_eq!(result.data.size_hint(), (2, Some(2)));

        let first = poll_query_result_data(&mut result.data).expect("first owned batch");
        assert_eq!(first.num_rows(), 2);
        assert!(
            Arc::ptr_eq(first.column(0), stored[0].column(0)),
            "the DataFusion adapter must share column arrays with the stored batch"
        );
        assert_eq!(result.data.size_hint(), (1, Some(1)));

        match result.into_source() {
            QueryResultSource::CachedRaw { mut data, .. } => {
                let waker = futures::task::noop_waker();
                let mut cx = Context::from_waker(&waker);
                let remaining = match Pin::new(&mut data).poll_next(&mut cx) {
                    Poll::Ready(Some(Ok(batch))) => batch,
                    other => panic!("expected remaining Arc batch, got {other:?}"),
                };
                assert!(
                    Arc::ptr_eq(&remaining, &stored[1]),
                    "into_source must continue the same CachedRawStream `.data` already polled"
                );
                assert!(matches!(
                    Pin::new(&mut data).poll_next(&mut cx),
                    Poll::Ready(None)
                ));
            }
            QueryResultSource::Stream { .. } => {
                panic!("from_cached_raw must produce QueryResultSource::CachedRaw")
            }
        }
    }

    #[tokio::test]
    async fn query_result_collect_and_drain_empty_single_and_multi() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let empty = QueryResult::from_cached_raw(
            wrap_raw_batches(Vec::new()),
            Arc::clone(&schema),
            CacheStatus::CacheHit,
        )
        .collect_batches()
        .await
        .expect("empty collect");
        assert!(empty.is_empty());

        QueryResult::from_cached_raw(
            wrap_raw_batches(Vec::new()),
            Arc::clone(&schema),
            CacheStatus::CacheHit,
        )
        .drain()
        .await
        .expect("empty drain");

        let single = QueryResult::from_cached_raw(
            wrap_raw_batches(vec![int_batch(&schema, &[9])]),
            Arc::clone(&schema),
            CacheStatus::CacheHit,
        )
        .collect_batches()
        .await
        .expect("single collect");
        assert_eq!(single.len(), 1);
        assert_eq!(single[0].num_rows(), 1);

        QueryResult::from_cached_raw(
            wrap_raw_batches(vec![int_batch(&schema, &[9])]),
            Arc::clone(&schema),
            CacheStatus::CacheHit,
        )
        .drain()
        .await
        .expect("single drain");

        let multi = QueryResult::from_cached_raw(
            wrap_raw_batches(vec![
                int_batch(&schema, &[1, 2]),
                int_batch(&schema, &[3]),
                int_batch(&schema, &[4, 5, 6]),
            ]),
            Arc::clone(&schema),
            CacheStatus::CacheHit,
        )
        .collect_batches()
        .await
        .expect("multi collect");
        assert_eq!(
            multi.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            vec![2, 1, 3]
        );

        QueryResult::from_cached_raw(
            wrap_raw_batches(vec![int_batch(&schema, &[1]), int_batch(&schema, &[2, 3])]),
            Arc::clone(&schema),
            CacheStatus::CacheHit,
        )
        .drain()
        .await
        .expect("multi drain");

        QueryResult::new(
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::iter(vec![
                    Ok(int_batch(&schema, &[1])),
                    Ok(int_batch(&schema, &[2, 3])),
                ]),
            )),
            CacheStatus::CacheMiss,
        )
        .drain()
        .await
        .expect("stream drain");
    }

    /// Shared-vec serve (search cache) keeps the same stream contract.
    #[test]
    fn cached_stream_from_shared_vec_multi_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let shared = Arc::new(vec![
            int_batch(&schema, &[10]),
            int_batch(&schema, &[20, 21]),
        ]);
        let yielded = drain_stream(CachedStream::new(Arc::clone(&shared), Arc::clone(&schema)));
        assert_eq!(yielded.len(), 2);
        assert_eq!(yielded[0].num_rows(), 1);
        assert_eq!(yielded[1].num_rows(), 2);
        assert!(Arc::ptr_eq(yielded[0].column(0), shared[0].column(0)));
    }

    /// `raw_batches` is present only for Raw entries and `Arc`-shares the store.
    #[test]
    fn raw_batches_is_the_stored_slice() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let cached = CachedQueryResult::new_raw(
            vec![int_batch(&schema, &[7])],
            Arc::clone(&schema),
            Arc::new(HashSet::new()),
            Instant::now(),
            Instant::now(),
        );
        let raw = cached.raw_batches().expect("raw entry");
        assert_eq!(raw.len(), 1);
        assert_eq!(raw[0].num_rows(), 1);
        let CachedData::Raw(stored) = &cached.data else {
            panic!("expected raw");
        };
        assert!(
            Arc::ptr_eq(stored, &raw),
            "raw_batches must Arc-share the stored slice"
        );
        assert!(
            Arc::ptr_eq(&raw[0], &stored[0]),
            "callers that can hold `Arc<RecordBatch>` share the stored handle"
        );
    }
}
