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

use std::{collections::HashSet, sync::Arc};

use arrow::array::{RecordBatch, UInt16Array};
use arrow::compute::filter_record_batch;
use arrow_tools::metadata_keys::{
    HTTP_RESPONSE_STATUS_METADATA_KEY, HTTP_TRANSIENT_FAILURE_METRIC_NAME,
};
use datafusion::{
    common::tree_node::TreeNodeRecursion,
    execution::SendableRecordBatchStream,
    logical_expr::LogicalPlan,
    physical_plan::{ExecutionPlan, stream::RecordBatchStreamAdapter},
    sql::TableReference,
};

use crate::{CachedQueryResult, QueryResultsCacheProvider, RawCacheKey, Sizeable};

use async_stream::stream;

use futures::StreamExt;

pub const RESPONSE_STATUS_COLUMN: &str = "response_status";

/// Filter out transient HTTP error responses (5xx server errors and 429 Too Many Requests)
/// from record batches before caching.
///
/// If a batch has neither a `response_status` column nor the schema-metadata status marker
/// (i.e., not from an HTTP connector), it is returned unchanged.
#[must_use]
pub fn filter_transient_error_responses(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    if batches.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(batches.len());

    for batch in batches {
        let schema = batch.schema();
        let Some((col_idx, _)) = schema.column_with_name(RESPONSE_STATUS_COLUMN) else {
            // No materialized column: either a decomposed HTTP dataset (fall back to
            // the schema-metadata status, which applies to every row in this batch)
            // or not an HTTP-connector result at all (pass through unchanged).
            match http_fetch_status(&schema) {
                Some(status) if is_retryable_status(status) => {}
                _ => result.push(batch.clone()),
            }
            continue;
        };

        let Some(status_array) = batch.column(col_idx).as_any().downcast_ref::<UInt16Array>()
        else {
            tracing::warn!(
                "'{RESPONSE_STATUS_COLUMN}' column is not UInt16Array, skipping transient error filtering"
            );
            result.push(batch.clone());
            continue;
        };

        // Create boolean mask: true for status codes that should be cached
        // (exclude 5xx server errors and 429 Too Many Requests)
        let mask: arrow::array::BooleanArray = status_array
            .iter()
            .map(|status| status.map(|s| !is_retryable_status(s)))
            .collect();

        match filter_record_batch(batch, &mask) {
            Ok(filtered) if filtered.num_rows() > 0 => {
                result.push(filtered);
            }
            Ok(_) => {} // Empty after filtering, skip
            Err(e) => {
                tracing::warn!("Failed to filter transient error responses: {e}");
                result.push(batch.clone());
            }
        }
    }

    result
}

/// Whether a status code is a transient failure worth `stale_if_error`
/// falling back on, rather than real data: a 5xx server error or 429 Too
/// Many Requests.
fn is_retryable_status(status: u16) -> bool {
    status == 429 || (500..600).contains(&status)
}

/// The HTTP status this batch's fetch actually returned, read from schema
/// metadata rather than a `response_status` column: every batch this
/// connector returns carries [`HTTP_RESPONSE_STATUS_METADATA_KEY`] set to
/// the real per-fetch status (see `HttpTableProvider::schema_with_fetch_status`),
/// whether or not `response_status` is one of the declared columns for this
/// dataset's schema. Only the HTTP connector ever sets this key, so its mere
/// presence is also the provenance signal that lets callers tell a real
/// HTTP-connector batch apart from an unrelated dataset that
/// happens to have its own same-named, same-typed `response_status` column
/// (where a value of `503` would be real data, not an origin failure).
fn http_fetch_status(schema: &arrow::datatypes::Schema) -> Option<u16> {
    schema
        .metadata()
        .get(HTTP_RESPONSE_STATUS_METADATA_KEY)
        .and_then(|v| v.parse().ok())
}

/// `response_status` is force-included as a real column for every HTTP
/// dataset, decomposed or not (see `parse_http_json_nesting` in
/// `runtime::dataconnector::https`) — the schema-metadata status alone isn't
/// reliable once a query plan wraps the scan in another physical operator
/// (a `FilterExec` rebuilds its output against the plan's own, plan-time
/// schema, discarding the per-fetch metadata). The metadata check below is
/// a fallback for the rare batch that reaches this function without the
/// column at all; every row in a batch shares one fetch's status regardless
/// — a single HTTP response never contains a per-row mix of status codes —
/// so the schema-metadata value alone, without a column to scan, is still
/// enough to classify the whole batch in that case.
fn has_transient_http_error_responses(batches: &[RecordBatch]) -> bool {
    let Some(first_batch) = batches.first() else {
        return false;
    };

    if http_fetch_status(&first_batch.schema()).is_none() {
        return false;
    }

    for batch in batches {
        let schema = batch.schema();
        if let Some((col_idx, _)) = schema.column_with_name(RESPONSE_STATUS_COLUMN) {
            let Some(status_array) = batch.column(col_idx).as_any().downcast_ref::<UInt16Array>()
            else {
                tracing::warn!(
                    "'{RESPONSE_STATUS_COLUMN}' column is not UInt16Array, skipping transient HTTP cache validation"
                );
                return false;
            };
            if status_array.iter().flatten().any(is_retryable_status) {
                return true;
            }
            continue;
        }

        // No materialized column (a decomposed dataset): fall back to the
        // schema-metadata status, which applies to every row in this batch.
        match http_fetch_status(&schema) {
            Some(status) if is_retryable_status(status) => return true,
            Some(_) => {}
            None => return false,
        }
    }

    false
}

/// Walks `plan` and its children for `HttpExec`'s
/// [`HTTP_TRANSIENT_FAILURE_METRIC_NAME`] counter, summed across every node.
///
/// `has_transient_http_error_responses` catches a retryable status through
/// the `response_status` column or the schema-metadata fallback, but a user
/// projection (e.g. `SELECT rank FROM http_data`) can prune `response_status`
/// out of the batch entirely before it ever reaches that check.
/// `ExecutionPlan::metrics()` lives on the plan tree, not the batch schema,
/// so no column pruning can remove it — this is the fallback for that case.
fn plan_saw_transient_http_failure(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(metrics) = plan.metrics()
        && let Some(value) = metrics.sum_by_name(HTTP_TRANSIENT_FAILURE_METRIC_NAME)
        && value.as_usize() > 0
    {
        return true;
    }

    plan.children()
        .into_iter()
        .any(plan_saw_transient_http_failure)
}

/// Returns whether the batches should be written to cache.
///
/// For HTTP-shaped results, any presence of a transient error response (5xx/429)
/// skips the entire cache write to avoid storing a partial result set. Non-HTTP
/// results are cacheable, even if they contain a `response_status` column for
/// unrelated business logic.
#[must_use]
pub fn batches_cacheable(batches: &[RecordBatch]) -> bool {
    if has_transient_http_error_responses(batches) {
        return false;
    }

    true
}

/// Whether an in-memory results-cache entry over `batches` can be bounded by
/// the configured `max_size`.
///
/// Expects batches [`arrow_tools::record_batch::compact_retained_buffers`] has
/// already been over — it asks what the copy achieved, rather than predicting it
/// from the column types. Anything still resting on the producer's memory is a
/// copy that did not decouple, and an entry over it would be billed for the
/// buffers it declares while pinning the producer's whole chunk.
///
/// Deliberately separate from [`batches_cacheable`], which answers a different
/// question — whether the *origin* produced a result worth storing — and whose
/// callers treat `false` as a failing origin and keep serving what is cached. A
/// batch declined here is a perfectly good result; it just cannot be held in a
/// budgeted cache. Declining is the conservative half of that trade: a repeat
/// query re-executes, where the alternative is a budget that does not hold.
///
/// There is deliberately no log here: this runs once per storable result, which
/// is as often as the runtime answers a query.
#[must_use]
pub fn batches_boundable(batches: &[RecordBatch]) -> bool {
    !batches
        .iter()
        .any(arrow_tools::record_batch::rests_on_unowned_memory)
}

/// How much larger than the cache limit a raw result may grow while
/// accumulating for an encoded (compressed) cache write. Encoding can shrink
/// a result well below its raw in-memory size, so accumulation continues past
/// the cache limit up to this factor; beyond it the result cannot plausibly
/// fit once encoded, and caching is abandoned to bound the memory held.
const MAX_ENCODING_COMPRESSION_RATIO: usize = 16;

/// Wraps `stream` so its results are stored in the cache once it has been
/// drained.
///
/// `read_started_at` is when the query began, and is recorded on the entry so
/// every later cache hit can check it — see
/// [`QueryResultsCacheProvider::tables_changed_since`], which documents why
/// the comparison is deliberately conservative. It must be the start of the
/// read, not the moment the result is stored: a change landing in between has
/// to disqualify the entry too.
#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn to_cached_record_batch_stream(
    cache_provider: Arc<QueryResultsCacheProvider>,
    mut stream: SendableRecordBatchStream,
    raw_cache_key: RawCacheKey,
    input_tables: Arc<HashSet<TableReference>>,
    read_started_at: std::time::Instant,
    physical_plan: Option<Arc<dyn ExecutionPlan>>,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let cache_schema = Arc::clone(&schema);

    let cached_result_stream = stream! {
        let mut records: Vec<RecordBatch> = Vec::new();
        let mut records_size: usize = 0;
        let has_encoder = cache_provider.encoder().is_some();
        // moka-rs operates by `u32` for records size, so max single record size is `u32::MAX` / 4 GB
        let cache_max_size = usize::try_from(cache_provider.max_size().min(u64::from(u32::MAX))).unwrap_or_default();

        // When an encoder is present, the encoded result may be much smaller
        // than the raw size, so keep accumulating past the cache limit (up to
        // the optimistic compression bound) and check the encoded size after
        // encoding. Without an encoder the raw size is the stored size.
        let raw_size_limit = if has_encoder {
            cache_max_size.saturating_mul(MAX_ENCODING_COMPRESSION_RATIO)
        } else {
            cache_max_size
        };

        while let Some(batch_result) = stream.next().await {
            if records_size < raw_size_limit && let Ok(batch) = &batch_result {
                // Accumulate compacted batches, not the batches as they arrive.
                // A `LIMIT`/`OFFSET` plan yields zero-copy slices, so holding
                // one keeps its whole scan batch alive until the stream drains
                // — and `records_size` would then bound a figure unrelated to
                // what is actually retained.
                //
                // Measure before copying, so the copy is only paid for a result
                // that can still be cached: `compacted_memory_size` is what the
                // batch will occupy, computed without allocating.
                records_size = records_size
                    .saturating_add(arrow_tools::record_batch::compacted_memory_size(batch));
                if records_size < raw_size_limit {
                    records.push(arrow_tools::record_batch::compact_retained_buffers(batch));
                } else {
                    records.clear();
                    records.shrink_to_fit();
                }
            } else if !records.is_empty() && records_size >= raw_size_limit {
                // The result can no longer fit in the cache: eagerly drop the
                // accumulated batches. Caching must be abandoned entirely —
                // a prefix of the result set must never be cached, as it would
                // be served as a complete result.
                records.clear();
                records.shrink_to_fit();
            }

            yield batch_result;
        }

        if records_size < raw_size_limit {
            // `batches_cacheable` is false only when transient HTTP error
            // responses (5xx/429) are present, which requires a non-empty
            // result set — skip the write to avoid caching a partial result.
            // `batches_boundable` is the separate question of whether the entry
            // could be billed for what it would hold.
            if cache_provider.tables_changed_since(&input_tables, read_started_at) {
                // Not the guard — correctness comes from the check every cache
                // hit performs. This only avoids encoding and storing a result
                // already known to be unservable.
                tracing::debug!(
                    "A table read by this query changed while it ran, skipping cache storage"
                );
            } else if !batches_cacheable(&records) {
                tracing::debug!(
                    "The result carried transient HTTP error responses (5xx/429), skipping cache storage"
                );
            } else if physical_plan
                .as_ref()
                .is_some_and(plan_saw_transient_http_failure)
            {
                tracing::debug!(
                    "The result's execution plan recorded a transient HTTP error response (5xx/429) that a projection excluded from the output columns, skipping cache storage"
                );
            } else if !has_encoder && !batches_boundable(&records) {
                // Only a raw entry can be pinned by what its batches rested on.
                // An encoded one keeps the serialized bytes and drops the
                // arrays, so it holds nothing of the producer's either way.
                tracing::debug!(
                    "The result holds a column no copy can decouple from the memory its producer owns, so an entry over it could not be bounded by the cache size limit; skipping cache storage"
                );
            } else {
                // Cache the result, including genuinely empty (0-row / 0-batch)
                // result sets. The schema is stored separately in
                // `CachedQueryResult`, so an empty result round-trips with the
                // correct schema, and caching it lets repeat queries that
                // legitimately return no rows be served from cache instead of
                // re-executing on every request.
                let cached_at = std::time::Instant::now();
                let encoder = cache_provider.encoder();

                match CachedQueryResult::from_batches(
                    records,
                    cache_schema,
                    input_tables,
                    cached_at,
                    read_started_at,
                    encoder,
                )
                .await
                {
                    Ok(cached_result) => {
                        // Check the actual (possibly encoded) size before caching
                        let actual_size = cached_result.get_memory_size();
                        if actual_size > cache_max_size {
                            tracing::debug!(
                                actual_size,
                                cache_max_size,
                                "Encoded query result still exceeds cache max size, skipping"
                            );
                        } else if let Err(e) = cache_provider.put_raw_key(&raw_cache_key, cached_result).await {
                            tracing::error!("Failed to cache query results: {e}");
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to encode query results for caching: {e}");
                    }
                }
            }
        }
    };

    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        Box::pin(cached_result_stream),
    ))
}

/// Collects every table the plan reads.
///
/// The set must be complete: it is what
/// [`crate::TabledCacheProvider::invalidate_for_table`] matches on, so a table
/// missing here yields a cache entry that no refresh and no DML can ever evict,
/// and stale rows are served as fresh hits until `item_ttl` expires.
///
/// Traversal therefore uses [`LogicalPlan::apply_with_subqueries`] rather than
/// walking [`LogicalPlan::inputs`]: subqueries in expressions (`IN (SELECT
/// ...)`, `EXISTS (...)`, a scalar subquery in the select list) are held in the
/// enclosing node's *expressions*, not among its inputs, so an `inputs()` walk
/// never reaches them.
#[must_use]
pub fn get_logical_plan_input_tables(plan: &LogicalPlan) -> HashSet<TableReference> {
    let mut table_names: HashSet<TableReference> = HashSet::new();

    // The closure is infallible, so the returned Result cannot be an error.
    let _ = plan.apply_with_subqueries(|current_plan| {
        if let LogicalPlan::TableScan(source, ..) = current_plan {
            // Clones of TableReferences are cheap - all fields are Arcs
            table_names.insert(source.table_name.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    });

    table_names
}

#[cfg(test)]
pub(crate) mod tests {
    /// A nested view arriving over Flight must be cacheable.
    ///
    /// Every buffer an IPC decode produces is a slice of the gRPC frame's
    /// `Bytes` — the construction `flight_data_to_arrow_batch` performs on a
    /// `FlightData` body — so a `Struct<Utf8View>` from a Flight source rests on
    /// memory the runtime does not own. Before `rebuild_view_leaves` and
    /// `rebuild_dictionary_leaves` the write path could not copy either off and
    /// declined the result outright, so such a source never cached anything.
    ///
    /// The two are rebuilt on opposite sides of the `MutableArrayData` copy, so
    /// a column holding both is the case that catches dropping either pass.
    #[test]
    fn a_nested_view_and_dictionary_decoded_over_ipc_are_copied_rather_than_declined() {
        use arrow::array::{
            Array, ArrayRef, DictionaryArray, ListArray, StringViewArray, StructArray,
            cast::AsArray,
        };
        use arrow::buffer::Buffer;
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{DataType, Field, Fields, Int32Type};
        use arrow::ipc::reader::StreamDecoder;
        use arrow::ipc::writer::StreamWriter;
        use std::sync::Arc;

        // Walks the whole tree: a container's own offsets and validity rest on
        // the frame just as its children's data buffers do.
        fn foreign(column: &ArrayRef) -> bool {
            fn walk(data: &arrow::array::ArrayData) -> bool {
                data.buffers().iter().any(Buffer::has_custom_allocation)
                    || data
                        .nulls()
                        .is_some_and(|nulls| nulls.inner().inner().has_custom_allocation())
                    || data.child_data().iter().any(walk)
            }
            walk(&column.to_data())
        }

        // One inline view and one that must live in a data buffer, so the copy
        // has both cases to carry.
        let rows: Vec<String> = vec![
            "a short one".to_string(),
            "a considerably longer string that will not fit inline at all".to_string(),
        ];
        let nested: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("s", DataType::Utf8View, false),
                Field::new_dictionary("d", DataType::Int32, DataType::Utf8, false),
            ]),
            vec![
                Arc::new(StringViewArray::from(rows.clone())) as ArrayRef,
                // A dictionary below the top level, which `MutableArrayData`
                // shares rather than narrows: it needs the pre-pass, where the
                // view beside it needs the pass after the copy.
                Arc::new(
                    rows.iter()
                        .map(|row| Some(row.as_str()))
                        .collect::<DictionaryArray<Int32Type>>(),
                ) as ArrayRef,
            ],
            None,
        ));
        // A list carries offsets, which the copy has to rebase — get that wrong
        // and a row reads its neighbour's values.
        let listed: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Utf8View, false)),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::new(StringViewArray::from(rows.clone())) as ArrayRef,
            None,
        ));
        let batch =
            RecordBatch::try_from_iter(vec![("n", nested), ("l", listed)]).expect("a batch");

        let mut encoded = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut encoded, &batch.schema()).expect("a stream writer");
            writer.write(&batch).expect("write");
            writer.finish().expect("finish");
        }
        let mut buffer = Buffer::from(bytes::Bytes::from(encoded));
        let decoded = StreamDecoder::new()
            .decode(&mut buffer)
            .expect("decode")
            .expect("a batch in the stream");

        assert!(
            decoded.columns().iter().all(foreign),
            "the fixture must actually rest on the decoded frame, or this proves nothing"
        );
        let stored = arrow_tools::record_batch::compact_retained_buffers(&decoded);
        assert!(
            batches_boundable(std::slice::from_ref(&stored)),
            "the copy must decouple the batch, or the write path declines the result"
        );
        assert!(
            !stored.columns().iter().any(foreign),
            "the stored batch must not keep the decoded frame alive, or `max_size` \
             cannot bound what the cache holds"
        );

        let read = |column: &ArrayRef| -> Vec<String> {
            let views = column.as_string_view();
            (0..views.len())
                .map(|row| views.value(row).to_string())
                .collect()
        };
        assert_eq!(
            read(stored.column(0).as_struct().column(0)),
            rows,
            "copying a nested view must not change what the rows say"
        );
        let nested_dictionary = stored.column(0).as_struct().column(1);
        assert_eq!(
            (0..nested_dictionary.len())
                .map(
                    |row| arrow::util::display::array_value_to_string(nested_dictionary, row)
                        .expect("a displayable value")
                )
                .collect::<Vec<_>>(),
            rows,
            "rebuilding a nested dictionary must not change what the rows say"
        );
        let list = stored.column(1).as_list::<i32>();
        assert_eq!(
            (0..list.len())
                .map(|row| read(&list.value(row)))
                .collect::<Vec<_>>(),
            rows.iter().map(|row| vec![row.clone()]).collect::<Vec<_>>(),
            "rebasing a list's offsets must not change which row owns which value"
        );
    }

    /// A batch still resting on the producer's memory must not be stored.
    ///
    /// The guard behind [`batches_boundable`], exercised on a batch that has not
    /// been through `compact_retained_buffers` — which stands in for the case it
    /// exists to catch: a copy that ran and did not decouple. No arrow type is
    /// known to do that today, since a dictionary-bearing container goes through
    /// `take` and every other copy is a `MutableArrayData` extend that exists for
    /// every type. That is exactly why the check observes the batch instead of
    /// enumerating types: what a kernel shares is arrow's to change, and a list
    /// of types would be wrong silently, billing an entry for the buffers it
    /// declares while it pins the producer's whole chunk.
    #[test]
    fn a_batch_still_resting_on_the_producers_memory_is_not_cacheable() {
        use arrow::array::{ArrayData, ArrayRef, Int32Array, make_array};
        use arrow::buffer::Buffer;
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let backing: Arc<Vec<u8>> = Arc::new(7_i32.to_le_bytes().repeat(4));
        let ptr = std::ptr::NonNull::new(backing.as_ptr().cast_mut()).expect("non-null");
        // SAFETY: `backing` outlives the buffer through the `Allocation`, and is
        // never mutated.
        let foreign = unsafe {
            Buffer::from_custom_allocation(
                ptr,
                backing.len(),
                Arc::clone(&backing) as Arc<dyn arrow::alloc::Allocation>,
            )
        };
        let column: ArrayRef = make_array(
            ArrayData::builder(DataType::Int32)
                .len(4)
                .add_buffer(foreign)
                .build()
                .expect("a valid Int32 array"),
        );
        let pinned = RecordBatch::try_from_iter(vec![("v", column)]).expect("a one-column batch");

        assert!(
            !batches_boundable(std::slice::from_ref(&pinned)),
            "a batch pinning the producer's allocation must be declined, or it is \
             stored holding bytes `max_size` cannot see"
        );

        // And the copy the write path actually takes clears it.
        let stored = arrow_tools::record_batch::compact_retained_buffers(&pinned);
        assert!(batches_boundable(std::slice::from_ref(&stored)));
        assert_eq!(
            stored
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("still an Int32Array")
                .values(),
            &[7_i32; 4],
            "decoupling must not change the rows"
        );
    }

    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::config::SessionConfig;
    use datafusion::execution::context::SessionContext;
    use std::collections::HashSet;

    /// A batch of wide strings, large enough that slicing one row out of it
    /// retains far more than that row needs. Shared with the `result::query`
    /// tests, which assert against the same premise.
    pub(crate) fn wide_string_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )]));
        let payloads: Vec<String> = (0..rows)
            .map(|row| {
                std::iter::repeat_n(
                    char::from(b'a' + u8::try_from(row % 26).unwrap_or_default()),
                    4_096,
                )
                .collect()
            })
            .collect();

        RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::StringArray::from(payloads))],
        )
        .expect("should create batch")
    }

    pub(crate) async fn parse_sql_to_logical_plan(sql: &str) -> LogicalPlan {
        let ctx = create_session_context();

        let plan = &ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("Should parse SQL to logical plan");

        plan.clone()
    }

    #[tokio::test]
    async fn test_collect_table_names_system_query_describe() {
        let sql = "describe customer";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<TableReference> = HashSet::new();
        (table_names == expected)
            .then_some(())
            .expect("table_names should match expected for DESCRIBE query");
    }

    #[tokio::test]
    async fn test_collect_table_names_system_query_show_tables() {
        let sql = "show tables";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<TableReference> = HashSet::from(["information_schema.tables".into()]);
        (table_names == expected)
            .then_some(())
            .expect("table_names should match expected for SHOW TABLES query");
    }

    #[tokio::test]
    async fn test_collect_table_names_simple_select() {
        let sql = "SELECT * FROM customer";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<TableReference> = HashSet::from(["customer".into()]);
        (table_names == expected)
            .then_some(())
            .expect("table_names should match expected for simple SELECT query");
    }

    #[tokio::test]
    async fn test_collect_table_names_join() {
        let sql =
            "SELECT c.first_name, o.quantity FROM customer c JOIN orders o ON c.id = o.customer_id";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<TableReference> = HashSet::from(["customer".into(), "orders".into()]);
        (table_names == expected)
            .then_some(())
            .expect("table_names should match expected for JOIN query");
    }

    #[tokio::test]
    async fn test_collect_table_names_subquery() {
        let sql = "SELECT * FROM (SELECT * FROM state) AS s";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<TableReference> = HashSet::from(["state".into()]);
        (table_names == expected)
            .then_some(())
            .expect("table_names should match expected for subquery");
    }

    #[tokio::test]
    async fn test_collect_table_names_nested_subqueries_with_aliases() {
        let sql = "SELECT c.first_name, c.last_name, sub.total_orders \
                   FROM customer c \
                   JOIN ( \
                       SELECT o.customer_id, COUNT(*) as total_orders \
                       FROM orders o \
                       GROUP BY o.customer_id \
                   ) sub ON c.id = sub.customer_id \
                   WHERE sub.total_orders > 5";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<TableReference> = HashSet::from(["customer".into(), "orders".into()]);
        (table_names == expected)
            .then_some(())
            .expect("table_names should match expected for nested subqueries with aliases");
    }

    #[tokio::test]
    async fn test_collect_table_names_union_with_subqueries() {
        let sql = "SELECT * FROM ( \
                       SELECT c.id, c.first_name, c.last_name \
                       FROM customer c \
                       WHERE c.state = 'NY' \
                       UNION \
                       SELECT o.id, o.customer_id, o.quantity \
                       FROM orders o \
                       WHERE o.quantity > 10 \
                   ) AS combined_results";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<TableReference> = HashSet::from(["customer".into(), "orders".into()]);
        (table_names == expected)
            .then_some(())
            .expect("table_names should match expected for UNION with subqueries");
    }

    #[tokio::test]
    async fn test_collect_table_names_join_with_subquery_in_from_clause() {
        let sql = "SELECT main.customer_id, main.total_spent, c.first_name, c.last_name \
                   FROM ( \
                       SELECT o.customer_id, SUM(o.quantity * o.price) as total_spent \
                       FROM orders o \
                       GROUP BY o.customer_id \
                   ) main \
                   JOIN customer c ON main.customer_id = c.id \
                   WHERE main.total_spent > 500";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<TableReference> = HashSet::from(["customer".into(), "orders".into()]);
        (table_names == expected)
            .then_some(())
            .expect("table_names should match expected for JOIN with subquery in FROM clause");
    }

    fn create_session_context() -> SessionContext {
        let config = SessionConfig::new().with_information_schema(true);
        let ctx = SessionContext::new_with_config(config);
        register_tables(&ctx);

        ctx
    }

    /// Regression test for #12671: a table referenced only inside a subquery
    /// *expression* must still be recorded. These subqueries live in the
    /// enclosing node's expressions rather than its inputs, so an
    /// `inputs()`-only walk missed them, and the resulting cache entry could
    /// never be evicted by a refresh or DML of that table.
    #[rstest::rstest]
    #[case::in_subquery("SELECT * FROM customer WHERE id IN (SELECT id FROM state)")]
    #[case::not_in_subquery("SELECT * FROM customer WHERE id NOT IN (SELECT id FROM state)")]
    #[case::exists(
        "SELECT * FROM customer WHERE EXISTS (SELECT 1 FROM state WHERE state.id = customer.id)"
    )]
    #[case::not_exists(
        "SELECT * FROM customer WHERE NOT EXISTS (SELECT 1 FROM state WHERE state.id = customer.id)"
    )]
    #[case::scalar_subquery_in_select_list(
        "SELECT first_name, (SELECT max(sales_tax) FROM state) AS t FROM customer"
    )]
    #[case::scalar_subquery_in_predicate(
        "SELECT * FROM customer WHERE id > (SELECT max(sales_tax) FROM state)"
    )]
    #[case::subquery_nested_under_conjunction(
        "SELECT * FROM customer WHERE state = 'NY' AND id IN (SELECT id FROM state)"
    )]
    #[tokio::test]
    async fn test_collect_table_names_expression_subqueries(#[case] sql: &str) {
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<TableReference> = HashSet::from(["customer".into(), "state".into()]);
        assert_eq!(
            table_names, expected,
            "a table read only through a subquery expression must still be recorded, \
             otherwise its cache entries can never be invalidated; sql={sql}"
        );
    }

    /// Drains `sql`-less canned batches through the caching wrapper and reports
    /// whether the result was stored.
    async fn stored_after_drain(
        provider: &Arc<QueryResultsCacheProvider>,
        key: RawCacheKey,
        input_tables: HashSet<TableReference>,
        read_started_at: std::time::Instant,
    ) -> bool {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let source = RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(batch)]),
        );

        let mut wrapped = to_cached_record_batch_stream(
            Arc::clone(provider),
            Box::pin(source),
            key,
            Arc::new(input_tables),
            read_started_at,
            None,
        );
        while wrapped.next().await.is_some() {}

        provider.run_pending_tasks().await;
        provider
            .get_raw_key(&key)
            .await
            .expect("cache access should succeed")
            .is_some()
    }

    fn test_cache_provider() -> Arc<QueryResultsCacheProvider> {
        Arc::new(
            QueryResultsCacheProvider::try_new(
                &spicepod::component::caching::SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    ..Default::default()
                },
                Box::new([]),
            )
            .expect("valid cache provider"),
        )
    }

    /// A query that read a table before it was invalidated must not store its
    /// result afterwards. Invalidation can only remove entries that already
    /// exist, so such a write recreates the entry the invalidation just removed
    /// and serves data from the pre-invalidation snapshot.
    #[tokio::test]
    async fn to_cached_record_batch_stream_discards_result_invalidated_during_read() {
        let provider = test_cache_provider();
        let read_started_at = std::time::Instant::now();

        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidation should succeed");

        assert!(
            !stored_after_drain(
                &provider,
                RawCacheKey::new(1),
                HashSet::from([TableReference::bare("customer")]),
                read_started_at,
            )
            .await,
            "a result whose table was invalidated mid-read must not be cached"
        );
    }

    /// The qualification of the invalidated reference must not matter: `customer`
    /// and `spice.public.customer` are the same physical table.
    #[tokio::test]
    async fn to_cached_record_batch_stream_gate_resolves_qualification() {
        let provider = test_cache_provider();
        let read_started_at = std::time::Instant::now();

        provider
            .invalidate_for_table(TableReference::bare("customer"))
            .await
            .expect("invalidation should succeed");

        assert!(
            !stored_after_drain(
                &provider,
                RawCacheKey::new(2),
                HashSet::from([TableReference::full(
                    crate::SPICE_DEFAULT_CATALOG,
                    crate::SPICE_DEFAULT_SCHEMA,
                    "customer",
                )]),
                read_started_at,
            )
            .await,
            "a differently-qualified reference to the invalidated table must also be gated"
        );
    }

    /// The gate must not block ordinary caching: an unrelated invalidation, and
    /// a table-less result, both still get stored.
    #[tokio::test]
    async fn to_cached_record_batch_stream_stores_unaffected_results() {
        let provider = test_cache_provider();
        let read_started_at = std::time::Instant::now();

        provider
            .invalidate_for_table(TableReference::bare("orders"))
            .await
            .expect("invalidation should succeed");

        assert!(
            stored_after_drain(
                &provider,
                RawCacheKey::new(3),
                HashSet::from([TableReference::bare("customer")]),
                read_started_at,
            )
            .await,
            "invalidating a different table must not block this write"
        );

        assert!(
            stored_after_drain(
                &provider,
                RawCacheKey::new(4),
                HashSet::new(),
                read_started_at,
            )
            .await,
            "a table-less result (e.g. SELECT 1) must still be cacheable"
        );
    }

    fn register_tables(ctx: &SessionContext) {
        let customer_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
        ]));
        let customer_data = vec![RecordBatch::new_empty(Arc::clone(&customer_schema))];
        let customer_table =
            MemTable::try_new(customer_schema, vec![customer_data]).expect("Should create table");
        ctx.register_table("customer", Arc::new(customer_table))
            .expect("Should register table");

        let state_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("sales_tax", DataType::Int32, false),
        ]));
        let state_data = vec![RecordBatch::new_empty(Arc::clone(&state_schema))];
        let state_table =
            MemTable::try_new(state_schema, vec![state_data]).expect("Should create table");
        ctx.register_table("state", Arc::new(state_table))
            .expect("Should register table");

        let orders_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, false),
            Field::new("item_id", DataType::Int32, false),
            Field::new("quantity", DataType::Int32, false),
            Field::new("price", DataType::Int32, false),
        ]));
        let orders_data = vec![RecordBatch::new_empty(Arc::clone(&orders_schema))];
        let orders_table =
            MemTable::try_new(orders_schema, vec![orders_data]).expect("Should create table");
        ctx.register_table("orders", Arc::new(orders_table))
            .expect("Should register table");
    }

    // --- filter_transient_error_responses tests ---

    use arrow::array::{StringArray, UInt16Array};

    fn create_http_response_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("content", DataType::Utf8, false),
            Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
        ]))
    }

    /// The `HTTP_RESPONSE_STATUS_METADATA_KEY` marker the real HTTP
    /// connector's `base_table_schema` sets, for tagging a test schema the
    /// same way. It lives on the *schema*, not the `response_status` field
    /// — see [`http_fetch_status`].
    fn http_provenance_metadata() -> std::collections::HashMap<String, String> {
        std::collections::HashMap::from([(
            HTTP_RESPONSE_STATUS_METADATA_KEY.to_string(),
            "1".to_string(),
        )])
    }

    /// Like [`create_http_response_schema`], plus a tagged schema and
    /// `_fetched_at` — what [`http_fetch_status`] actually checks.
    /// Tests exercising `batches_cacheable`/`has_transient_http_error_responses`
    /// need this one; `filter_transient_error_responses` tests don't check
    /// provenance at all, so they stay on the untagged schema above.
    fn create_http_response_schema_with_fetched_at() -> Arc<Schema> {
        Arc::new(
            Schema::new(vec![
                Field::new("content", DataType::Utf8, false),
                Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
                Field::new(
                    "_fetched_at",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                    true,
                ),
            ])
            .with_metadata(http_provenance_metadata()),
        )
    }

    #[tokio::test]
    async fn test_to_cached_record_batch_stream_preserves_non_http_response_status_column() {
        use arrow::array::Int32Array;
        use datafusion::error::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::TryStreamExt;
        use spicepod::component::caching::SQLResultsCacheConfig;

        let cache_provider = Arc::new(
            crate::QueryResultsCacheProvider::try_new(
                &SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    ..Default::default()
                },
                Box::new([]),
            )
            .expect("valid cache provider"),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(UInt16Array::from(vec![500])),
            ],
        )
        .expect("to create batch");

        let raw_cache_key = crate::key::CacheKey::Query("non-http-response-status", None)
            .as_raw_key(cache_provider.hasher());
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok::<RecordBatch, DataFusionError>(batch.clone())]),
        ));

        let cached_stream = to_cached_record_batch_stream(
            Arc::clone(&cache_provider),
            stream,
            raw_cache_key,
            Arc::new(HashSet::from(["local_table".into()])),
            std::time::Instant::now(),
            None,
        );

        let output_batches = cached_stream
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should be collected successfully");
        assert_eq!(output_batches.len(), 1);
        assert_eq!(output_batches[0].num_rows(), 1);

        let cached = cache_provider
            .get_raw_key(&raw_cache_key)
            .await
            .expect("cache lookup should succeed")
            .expect(
                "non-HTTP query results should still be cached even if they contain a response_status column",
            );

        let cached_batches = cached.records().await.expect("cached result should decode");
        assert_eq!(cached_batches.len(), 1);
        assert_eq!(cached_batches[0].num_rows(), 1);

        let cached_status = cached_batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("cached response_status should remain UInt16Array");
        assert_eq!(cached_status.value(0), 500);
    }

    #[tokio::test]
    async fn test_to_cached_record_batch_stream_skips_mixed_http_success_and_transient_rows() {
        use datafusion::error::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::TryStreamExt;
        use spicepod::component::caching::SQLResultsCacheConfig;

        let cache_provider = Arc::new(
            crate::QueryResultsCacheProvider::try_new(
                &SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    ..Default::default()
                },
                Box::new([]),
            )
            .expect("valid cache provider"),
        );

        let schema = create_http_response_schema_with_fetched_at();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["ok", "server error"])),
                Arc::new(UInt16Array::from(vec![200, 500])),
                Arc::new(arrow::array::TimestampNanosecondArray::from(vec![
                    Some(0),
                    Some(0),
                ])),
            ],
        )
        .expect("to create batch");

        let raw_cache_key = crate::key::CacheKey::Query("mixed-http-status-rows", None)
            .as_raw_key(cache_provider.hasher());
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok::<RecordBatch, DataFusionError>(batch.clone())]),
        ));

        let cached_stream = to_cached_record_batch_stream(
            Arc::clone(&cache_provider),
            stream,
            raw_cache_key,
            Arc::new(HashSet::from(["http_table".into()])),
            std::time::Instant::now(),
            None,
        );

        let output_batches = cached_stream
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should be collected successfully");
        assert_eq!(output_batches.len(), 1);
        assert_eq!(output_batches[0].num_rows(), 2);

        let cached = cache_provider
            .get_raw_key(&raw_cache_key)
            .await
            .expect("cache lookup should succeed");
        assert!(
            cached.is_none(),
            "mixed HTTP success/error results should not be cached as a partial result set"
        );
    }

    #[tokio::test]
    async fn test_to_cached_record_batch_stream_skips_http_results_when_any_batch_is_transient_error()
     {
        use datafusion::error::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::TryStreamExt;
        use spicepod::component::caching::SQLResultsCacheConfig;

        let cache_provider = Arc::new(
            crate::QueryResultsCacheProvider::try_new(
                &SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    ..Default::default()
                },
                Box::new([]),
            )
            .expect("valid cache provider"),
        );

        let schema = create_http_response_schema_with_fetched_at();
        let ok_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["ok"])),
                Arc::new(UInt16Array::from(vec![200])),
                Arc::new(arrow::array::TimestampNanosecondArray::from(vec![Some(0)])),
            ],
        )
        .expect("to create ok batch");
        let error_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["rate limited"])),
                Arc::new(UInt16Array::from(vec![429])),
                Arc::new(arrow::array::TimestampNanosecondArray::from(vec![Some(0)])),
            ],
        )
        .expect("to create error batch");

        let raw_cache_key = crate::key::CacheKey::Query("mixed-http-status-batches", None)
            .as_raw_key(cache_provider.hasher());
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![
                Ok::<RecordBatch, DataFusionError>(ok_batch.clone()),
                Ok::<RecordBatch, DataFusionError>(error_batch.clone()),
            ]),
        ));

        let cached_stream = to_cached_record_batch_stream(
            Arc::clone(&cache_provider),
            stream,
            raw_cache_key,
            Arc::new(HashSet::from(["http_table".into()])),
            std::time::Instant::now(),
            None,
        );

        let output_batches = cached_stream
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should be collected successfully");
        assert_eq!(output_batches.len(), 2);
        assert_eq!(output_batches[0].num_rows(), 1);
        assert_eq!(output_batches[1].num_rows(), 1);

        let cached = cache_provider
            .get_raw_key(&raw_cache_key)
            .await
            .expect("cache lookup should succeed");
        assert!(
            cached.is_none(),
            "HTTP results should not be cached if any batch contains only transient errors"
        );
    }

    /// Mirrors `HttpTableProviderBuilder::base_table_schema()` in
    /// `data_components::http::provider` field-for-field, including
    /// `request_headers` — regression test for #14156, where an allowlist
    /// requiring every column to be a known HTTP metadata field rejected
    /// this real 8-column schema outright, so a transient 5xx/429 was never
    /// detected and `caching_stale_if_error` could never fall back to the
    /// cache.
    fn create_real_http_connector_schema() -> Arc<Schema> {
        Arc::new(
            Schema::new(vec![
                Field::new("request_path", DataType::Utf8, false),
                Field::new("request_query", DataType::Utf8, true),
                Field::new("request_body", DataType::Utf8, true),
                Field::new("request_headers", DataType::Utf8, true),
                Field::new("content", DataType::Utf8, false),
                Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
                Field::new("response_headers", DataType::Utf8, true),
                Field::new(
                    "_fetched_at",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                    true,
                ),
            ])
            .with_metadata(http_provenance_metadata()),
        )
    }

    #[test]
    fn test_batches_cacheable_detects_transient_error_on_real_http_connector_schema() {
        let schema = create_real_http_connector_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["/api/users"])),
                Arc::new(StringArray::from(vec![Some("id=1")])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec!["service unavailable"])),
                Arc::new(UInt16Array::from(vec![503])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(arrow::array::TimestampNanosecondArray::from(vec![Some(0)])),
            ],
        )
        .expect("to create batch with the real 8-column HTTP connector schema");

        assert!(
            !batches_cacheable(&[batch]),
            "a transient 5xx on the real HTTP-connector schema (including request_headers) \
            must be recognized so caching_stale_if_error can fall back to the cache"
        );
    }

    #[test]
    fn test_filter_no_response_status_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .expect("to create batch");

        let result = filter_transient_error_responses(&[batch]);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].num_rows(),
            2,
            "Non-HTTP batches pass through unchanged"
        );
    }

    /// A batch shaped like what a narrow `SELECT` (one that doesn't
    /// reference `response_status`) leaves after `DataFusion`'s projection
    /// pushdown prunes that column away — carrying only the schema-level
    /// `HTTP_RESPONSE_STATUS_METADATA_KEY` marker, no materialized column.
    /// `batches_cacheable` and `filter_transient_error_responses` must still
    /// detect a transient origin failure from that marker alone in this
    /// shape, which is the metadata-only fallback path in
    /// `has_transient_http_error_responses`.
    fn create_projected_http_batch_schema_with_status(status: u16) -> Arc<Schema> {
        Arc::new(
            Schema::new(vec![
                Field::new("id", DataType::Utf8, true),
                Field::new(
                    "_fetched_at",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                    true,
                ),
            ])
            .with_metadata(std::collections::HashMap::from([(
                HTTP_RESPONSE_STATUS_METADATA_KEY.to_string(),
                status.to_string(),
            )])),
        )
    }

    #[test]
    fn test_projected_http_batch_schema_has_no_response_status_column() {
        let schema = create_projected_http_batch_schema_with_status(200);
        assert!(
            schema.column_with_name(RESPONSE_STATUS_COLUMN).is_none(),
            "a projected-away response_status column must not reappear on the schema"
        );
    }

    #[test]
    fn test_batches_cacheable_detects_transient_error_on_a_projected_http_batch() {
        let schema = create_projected_http_batch_schema_with_status(503);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(arrow::array::TimestampNanosecondArray::from(vec![Some(0)])),
            ],
        )
        .expect("to create batch with a projected HTTP batch schema");

        assert!(
            !batches_cacheable(&[batch]),
            "a transient 503 must be detected from schema metadata even without a \
            response_status column"
        );
    }

    #[test]
    fn test_batches_cacheable_accepts_ok_status_on_a_projected_http_batch() {
        let schema = create_projected_http_batch_schema_with_status(200);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("row-1")])),
                Arc::new(arrow::array::TimestampNanosecondArray::from(vec![Some(0)])),
            ],
        )
        .expect("to create batch with a projected HTTP batch schema");

        assert!(
            batches_cacheable(&[batch]),
            "a 200 status on a projected HTTP batch should be cacheable"
        );
    }

    #[test]
    fn test_filter_drops_whole_batch_on_a_projected_http_batch_transient_error() {
        let schema = create_projected_http_batch_schema_with_status(500);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(arrow::array::TimestampNanosecondArray::from(vec![Some(0)])),
            ],
        )
        .expect("to create batch with a projected HTTP batch schema");

        let result = filter_transient_error_responses(&[batch]);
        assert!(
            result.is_empty(),
            "a batch carrying a transient 500 status must be dropped entirely"
        );
    }

    #[test]
    fn test_filter_keeps_whole_batch_on_a_projected_http_batch_ok_status() {
        let schema = create_projected_http_batch_schema_with_status(200);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("row-1"), Some("row-2")])),
                Arc::new(arrow::array::TimestampNanosecondArray::from(vec![
                    Some(0),
                    Some(0),
                ])),
            ],
        )
        .expect("to create batch with a decomposed HTTP schema");

        let result = filter_transient_error_responses(&[batch]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[test]
    fn test_filter_keeps_2xx() {
        let schema = create_http_response_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["ok1", "ok2", "ok3"])),
                Arc::new(UInt16Array::from(vec![200, 201, 204])),
            ],
        )
        .expect("to create batch");

        let result = filter_transient_error_responses(&[batch]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3, "All 2xx rows should be kept");
    }

    #[test]
    fn test_filter_keeps_4xx() {
        let schema = create_http_response_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "not found",
                    "bad request",
                    "forbidden",
                ])),
                Arc::new(UInt16Array::from(vec![404, 400, 403])),
            ],
        )
        .expect("to create batch");

        let result = filter_transient_error_responses(&[batch]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3, "All 4xx rows should be kept");
    }

    #[test]
    fn test_filter_removes_5xx() {
        let schema = create_http_response_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["error1", "error2", "error3"])),
                Arc::new(UInt16Array::from(vec![500, 502, 503])),
            ],
        )
        .expect("to create batch");

        let result = filter_transient_error_responses(&[batch]);
        assert!(result.is_empty(), "All 5xx rows should be filtered out");
    }

    #[test]
    fn test_filter_removes_429() {
        let schema = create_http_response_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["rate limited"])),
                Arc::new(UInt16Array::from(vec![429])),
            ],
        )
        .expect("to create batch");

        let result = filter_transient_error_responses(&[batch]);
        assert!(
            result.is_empty(),
            "429 Too Many Requests should be filtered out"
        );
    }

    #[test]
    fn test_filter_mixed_status_codes() {
        let schema = create_http_response_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "ok",
                    "rate limited",
                    "server error",
                    "not found",
                ])),
                Arc::new(UInt16Array::from(vec![200, 429, 500, 404])),
            ],
        )
        .expect("to create batch");

        let result = filter_transient_error_responses(&[batch]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2, "Should keep only 200 and 404");

        let status = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("status column");
        assert_eq!(status.value(0), 200);
        assert_eq!(status.value(1), 404);
    }

    #[test]
    fn test_filter_empty_batches() {
        let result = filter_transient_error_responses(&[]);
        assert!(result.is_empty(), "Empty input should return empty output");
    }

    #[test]
    fn test_filter_multiple_batches() {
        let schema = create_http_response_schema();

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["ok"])),
                Arc::new(UInt16Array::from(vec![200])),
            ],
        )
        .expect("to create batch1");

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["error"])),
                Arc::new(UInt16Array::from(vec![500])),
            ],
        )
        .expect("to create batch2");

        let batch3 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["not found"])),
                Arc::new(UInt16Array::from(vec![404])),
            ],
        )
        .expect("to create batch3");

        let result = filter_transient_error_responses(&[batch1, batch2, batch3]);
        assert_eq!(
            result.len(),
            2,
            "Should have 2 batches (batch2 filtered out entirely)"
        );
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(result[1].num_rows(), 1);
    }

    #[test]
    fn test_filter_boundary_status_codes() {
        let schema = create_http_response_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["499", "500", "599", "600"])),
                Arc::new(UInt16Array::from(vec![499, 500, 599, 600])),
            ],
        )
        .expect("to create batch");

        let result = filter_transient_error_responses(&[batch]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2, "Should keep 499 and 600");

        let status = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("status column");
        assert_eq!(status.value(0), 499);
        assert_eq!(status.value(1), 600);
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/8508>.
    ///
    /// When the results cache uses zstd encoding, a result whose *uncompressed*
    /// size exceeds the cache limit should still be cached if the *compressed*
    /// size fits.
    #[tokio::test]
    async fn test_encoded_result_cached_when_compressed_fits() {
        use arrow::array::{Array, Int32Array};
        use datafusion::error::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::TryStreamExt;
        use spicepod::component::caching::SQLResultsCacheConfig;

        // Create a small cache (e.g., 2 KiB) with zstd encoding.
        let cache_provider = Arc::new(
            crate::QueryResultsCacheProvider::try_new(
                &SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    max_size: Some("2KiB".to_string()),
                    encoding: spicepod::component::caching::Encoding::Zstd,
                    ..Default::default()
                },
                Box::new([]),
            )
            .expect("valid cache provider"),
        );

        // Build a batch of highly compressible data (repeated zeros) whose
        // uncompressed memory size exceeds the 2 KiB cache limit. The store
        // path encodes it, so the entry it writes is the compressed one and
        // fits (see #8508).
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let n = 300; // 300 rows × 2 cols × 4 bytes = 2400 bytes raw > 2048 limit
        let col: Arc<dyn Array> = Arc::new(Int32Array::from(vec![0i32; n]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&col), col])
            .expect("to create batch");

        let raw_size = batch.get_array_memory_size();
        let cache_max = usize::try_from(cache_provider.max_size()).unwrap_or(usize::MAX);
        assert!(
            raw_size > cache_max,
            "Test precondition: raw size ({raw_size}) must exceed cache max ({cache_max})"
        );
        let raw_cache_key = crate::key::CacheKey::Query("zstd-compressible", None)
            .as_raw_key(cache_provider.hasher());
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok::<RecordBatch, DataFusionError>(batch)]),
        ));

        let cached_stream = to_cached_record_batch_stream(
            Arc::clone(&cache_provider),
            stream,
            raw_cache_key,
            Arc::new(HashSet::from(["test_table".into()])),
            std::time::Instant::now(),
            None,
        );

        // Consume the stream to trigger caching.
        let _output = cached_stream
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should be collected successfully");

        // The encoded result should now be in the cache.
        let cached = cache_provider
            .get_raw_key(&raw_cache_key)
            .await
            .expect("cache lookup should succeed");
        assert!(
            cached.is_some(),
            "Compressed result should be cached even though uncompressed size exceeds the limit"
        );

        let cached_batches = cached
            .expect("must be Some")
            .records()
            .await
            .expect("cached result should decode");
        assert_eq!(cached_batches.len(), 1);
        assert_eq!(cached_batches[0].num_rows(), n);
    }

    /// Array bytes can sit under `max_size` while [`CachedQueryResult::memory_size`]
    /// does not. The store path must still encode those under zstd (#8508 weigher
    /// boundary).
    #[tokio::test]
    async fn test_encoded_result_cached_when_weigher_exceeds_max_size() {
        use arrow::array::{Array, Int32Array};
        use datafusion::error::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::TryStreamExt;
        use spicepod::component::caching::SQLResultsCacheConfig;

        let cache_provider = Arc::new(
            crate::QueryResultsCacheProvider::try_new(
                &SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    max_size: Some("2KiB".to_string()),
                    encoding: spicepod::component::caching::Encoding::Zstd,
                    ..Default::default()
                },
                Box::new([]),
            )
            .expect("valid cache provider"),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let n = 200; // 200 rows × 2 cols × 4 bytes = 1600 array bytes < 2048
        let col: Arc<dyn Array> = Arc::new(Int32Array::from(vec![0i32; n]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&col), col])
            .expect("to create batch");

        let raw_size = batch.get_array_memory_size();
        let cache_max = usize::try_from(cache_provider.max_size()).unwrap_or(usize::MAX);
        assert!(
            raw_size <= cache_max,
            "Test precondition: array bytes ({raw_size}) must sit under cache max ({cache_max})"
        );
        let raw_entry = crate::result::query::CachedQueryResult::new_raw(
            vec![batch.clone()],
            Arc::clone(&schema),
            Arc::new(HashSet::new()),
            std::time::Instant::now(),
            std::time::Instant::now(),
        );
        assert!(
            raw_entry.get_memory_size() > cache_max,
            "Test precondition: weigher ({}) must exceed cache max ({cache_max})",
            raw_entry.get_memory_size()
        );

        let raw_cache_key = crate::key::CacheKey::Query("zstd-weigher-boundary", None)
            .as_raw_key(cache_provider.hasher());
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok::<RecordBatch, DataFusionError>(batch)]),
        ));

        let cached_stream = to_cached_record_batch_stream(
            Arc::clone(&cache_provider),
            stream,
            raw_cache_key,
            Arc::new(HashSet::from(["test_table".into()])),
            std::time::Instant::now(),
            None,
        );

        let _output = cached_stream
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should be collected successfully");

        let cached = cache_provider
            .get_raw_key(&raw_cache_key)
            .await
            .expect("cache lookup should succeed");
        assert!(
            cached.is_some(),
            "Compressed result should be cached when array bytes fit but the weigher does not"
        );
        let cached = cached.expect("must be Some");
        assert!(
            cached.is_encoded(),
            "the stored entry must be encoded so the weigher can admit it"
        );
        let cached_batches = cached.records().await.expect("cached result should decode");
        assert_eq!(cached_batches.len(), 1);
        assert_eq!(cached_batches[0].num_rows(), n);
    }

    /// Regression test: with an encoder, accumulation must continue past the
    /// raw cache limit across **multiple batches**. Previously accumulation
    /// stopped at the limit while the encoded write still proceeded, caching a
    /// prefix of the result set that would then be served as a complete result.
    #[tokio::test]
    async fn test_encoded_multi_batch_result_cached_in_full() {
        use arrow::array::{Array, Int32Array};
        use datafusion::error::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::TryStreamExt;
        use spicepod::component::caching::SQLResultsCacheConfig;

        let cache_provider = Arc::new(
            crate::QueryResultsCacheProvider::try_new(
                &SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    max_size: Some("4KiB".to_string()),
                    encoding: spicepod::component::caching::Encoding::Zstd,
                    ..Default::default()
                },
                Box::new([]),
            )
            .expect("valid cache provider"),
        );

        // Two compressible batches, each alone larger than the 4 KiB cache
        // limit and the raw-store budget, so the pair is encoded rather than
        // stored raw (which would not fit). The 4 KiB budget keeps
        // 16 × 4 KiB above the pair's raw size so accumulation is not abandoned.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let n = 2_500; // 2500 rows × 2 cols × 4 bytes = 20_000 bytes raw per batch
        let make_batch = || {
            let col: Arc<dyn Array> = Arc::new(Int32Array::from(vec![0i32; n]));
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&col), col])
                .expect("to create batch")
        };

        let raw_cache_key = crate::key::CacheKey::Query("zstd-multi-batch", None)
            .as_raw_key(cache_provider.hasher());
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![
                Ok::<RecordBatch, DataFusionError>(make_batch()),
                Ok::<RecordBatch, DataFusionError>(make_batch()),
            ]),
        ));

        let cached_stream = to_cached_record_batch_stream(
            Arc::clone(&cache_provider),
            stream,
            raw_cache_key,
            Arc::new(HashSet::from(["test_table".into()])),
            std::time::Instant::now(),
            None,
        );

        let _output = cached_stream
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should be collected successfully");

        let cached = cache_provider
            .get_raw_key(&raw_cache_key)
            .await
            .expect("cache lookup should succeed")
            .expect("compressed multi-batch result should be cached");

        let cached_batches = cached.records().await.expect("cached result should decode");
        let cached_rows: usize = cached_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            cached_rows,
            2 * n,
            "The full result set must be cached, never a prefix"
        );
    }

    /// A result whose raw size exceeds the optimistic compression bound
    /// (`MAX_ENCODING_COMPRESSION_RATIO` × cache limit) must not be cached at
    /// all — in particular, no prefix of it.
    #[tokio::test]
    async fn test_encoded_result_beyond_compression_bound_not_cached() {
        use arrow::array::{Array, Int32Array};
        use datafusion::error::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::TryStreamExt;
        use spicepod::component::caching::SQLResultsCacheConfig;

        let cache_provider = Arc::new(
            crate::QueryResultsCacheProvider::try_new(
                &SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    max_size: Some("2KiB".to_string()),
                    encoding: spicepod::component::caching::Encoding::Zstd,
                    ..Default::default()
                },
                Box::new([]),
            )
            .expect("valid cache provider"),
        );

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        // Each batch is ~8 KiB raw; five batches (~40 KiB) exceed the
        // 16 × 2 KiB = 32 KiB accumulation bound.
        let n = 2048;
        let make_batch = || {
            let col: Arc<dyn Array> = Arc::new(Int32Array::from(vec![0i32; n]));
            RecordBatch::try_new(Arc::clone(&schema), vec![col]).expect("to create batch")
        };
        let batches: Vec<Result<RecordBatch, DataFusionError>> =
            (0..5).map(|_| Ok(make_batch())).collect();

        let raw_cache_key = crate::key::CacheKey::Query("zstd-beyond-bound", None)
            .as_raw_key(cache_provider.hasher());
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(batches),
        ));

        let cached_stream = to_cached_record_batch_stream(
            Arc::clone(&cache_provider),
            stream,
            raw_cache_key,
            Arc::new(HashSet::from(["test_table".into()])),
            std::time::Instant::now(),
            None,
        );

        let output = cached_stream
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should be collected successfully");
        assert_eq!(output.len(), 5, "All batches must reach the caller");

        let cached = cache_provider
            .get_raw_key(&raw_cache_key)
            .await
            .expect("cache lookup should succeed");
        assert!(
            cached.is_none(),
            "A result beyond the accumulation bound must not be cached (not even a prefix)"
        );
    }

    /// Regression test: a query that returns an empty result set by yielding
    /// **zero batches** (e.g. `DataFusion`'s `EmptyExec` for `WHERE 1=0`) must
    /// still be cached, so repeat queries that legitimately return no rows are
    /// served from cache instead of re-executing on every request.
    #[tokio::test]
    async fn test_empty_result_zero_batches_is_cached() {
        use datafusion::error::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::TryStreamExt;
        use spicepod::component::caching::SQLResultsCacheConfig;

        let cache_provider = Arc::new(
            crate::QueryResultsCacheProvider::try_new(
                &SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    ..Default::default()
                },
                Box::new([]),
            )
            .expect("valid cache provider"),
        );

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        // A stream that yields no batches at all (0 batches, 0 rows).
        let raw_cache_key = crate::key::CacheKey::Query("empty-zero-batches", None)
            .as_raw_key(cache_provider.hasher());
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(Vec::<Result<RecordBatch, DataFusionError>>::new()),
        ));

        let cached_stream = to_cached_record_batch_stream(
            Arc::clone(&cache_provider),
            stream,
            raw_cache_key,
            Arc::new(HashSet::from(["local_table".into()])),
            std::time::Instant::now(),
            None,
        );

        let output_batches = cached_stream
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should be collected successfully");
        assert!(output_batches.is_empty());

        let cached = cache_provider
            .get_raw_key(&raw_cache_key)
            .await
            .expect("cache lookup should succeed")
            .expect("empty result sets (zero batches) should be cached");

        let cached_batches = cached.records().await.expect("cached result should decode");
        assert!(
            cached_batches.iter().all(|b| b.num_rows() == 0),
            "cached empty result should contain no rows"
        );
        assert_eq!(
            cached.schema.fields().len(),
            1,
            "cached empty result should preserve the query schema"
        );
    }

    /// A query that returns an empty result set by yielding a single schema-only
    /// batch (0 rows) must also be cached. This is the sibling case to
    /// [`test_empty_result_zero_batches_is_cached`] — both represent zero rows
    /// and must be cached consistently.
    #[tokio::test]
    async fn test_empty_result_zero_row_batch_is_cached() {
        use datafusion::error::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::TryStreamExt;
        use spicepod::component::caching::SQLResultsCacheConfig;

        let cache_provider = Arc::new(
            crate::QueryResultsCacheProvider::try_new(
                &SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    ..Default::default()
                },
                Box::new([]),
            )
            .expect("valid cache provider"),
        );

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let empty_batch = RecordBatch::new_empty(Arc::clone(&schema));

        let raw_cache_key = crate::key::CacheKey::Query("empty-zero-row-batch", None)
            .as_raw_key(cache_provider.hasher());
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok::<RecordBatch, DataFusionError>(empty_batch)]),
        ));

        let cached_stream = to_cached_record_batch_stream(
            Arc::clone(&cache_provider),
            stream,
            raw_cache_key,
            Arc::new(HashSet::from(["local_table".into()])),
            std::time::Instant::now(),
            None,
        );

        let _output = cached_stream
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should be collected successfully");

        let cached = cache_provider
            .get_raw_key(&raw_cache_key)
            .await
            .expect("cache lookup should succeed")
            .expect("empty result sets (zero-row batch) should be cached");

        let cached_batches = cached.records().await.expect("cached result should decode");
        assert!(cached_batches.iter().all(|b| b.num_rows() == 0));
    }

    /// Verify that when there is no encoder and the result exceeds the cache
    /// limit, it is correctly NOT cached (existing behavior preserved).
    #[tokio::test]
    async fn test_unencoded_oversized_result_not_cached() {
        use arrow::array::{Array, Int32Array};
        use datafusion::error::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::TryStreamExt;
        use spicepod::component::caching::SQLResultsCacheConfig;

        let cache_provider = Arc::new(
            crate::QueryResultsCacheProvider::try_new(
                &SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    max_size: Some("2KiB".to_string()),
                    encoding: spicepod::component::caching::Encoding::None,
                    ..Default::default()
                },
                Box::new([]),
            )
            .expect("valid cache provider"),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let n = 300;
        let col: Arc<dyn Array> = Arc::new(Int32Array::from(vec![0i32; n]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&col), col])
            .expect("to create batch");

        let raw_cache_key = crate::key::CacheKey::Query("unencoded-oversized", None)
            .as_raw_key(cache_provider.hasher());
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok::<RecordBatch, DataFusionError>(batch)]),
        ));

        let cached_stream = to_cached_record_batch_stream(
            Arc::clone(&cache_provider),
            stream,
            raw_cache_key,
            Arc::new(HashSet::from(["test_table".into()])),
            std::time::Instant::now(),
            None,
        );

        let _output = cached_stream
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should be collected successfully");

        let cached = cache_provider
            .get_raw_key(&raw_cache_key)
            .await
            .expect("cache lookup should succeed");
        assert!(
            cached.is_none(),
            "Unencoded oversized result should NOT be cached"
        );
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/12921>.
    ///
    /// A `LIMIT`/`OFFSET` plan emits `batch.slice(..)`, which keeps its
    /// parent's buffers alive. A one-row result carved out of a large scan
    /// batch must be stored — and billed — as one row, not as the scan batch
    /// it came from. Before the fix the entry was billed the whole parent,
    /// which is larger than this cache's `max_size`, so it was never stored at
    /// all and every repeat of the query re-executed.
    #[tokio::test]
    async fn a_sliced_result_is_stored_and_billed_as_its_own_rows() {
        use arrow::array::StringArray;
        use datafusion::error::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::TryStreamExt;
        use spicepod::component::caching::SQLResultsCacheConfig;

        let cache_provider = Arc::new(
            crate::QueryResultsCacheProvider::try_new(
                &SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    max_size: Some("1MiB".to_string()),
                    encoding: spicepod::component::caching::Encoding::None,
                    ..Default::default()
                },
                Box::new([]),
            )
            .expect("valid cache provider"),
        );

        // 2,000 rows x 4 KiB is ~8 MiB of payload — well past the 1 MiB budget.
        let scan_batch = wide_string_batch(2_000);
        let schema = scan_batch.schema();
        assert!(
            scan_batch.get_array_memory_size() > 1024 * 1024,
            "the scan batch must exceed the cache budget for this test to mean anything"
        );

        // What `LimitStream` yields for `LIMIT 1 OFFSET 1000`.
        let sliced = scan_batch.slice(1_000, 1);
        let expected_payload = sliced
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload is a StringArray")
            .value(0)
            .to_string();

        let raw_cache_key =
            crate::key::CacheKey::Query("sliced-result", None).as_raw_key(cache_provider.hasher());
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok::<RecordBatch, DataFusionError>(sliced)]),
        ));

        let cached_stream = to_cached_record_batch_stream(
            Arc::clone(&cache_provider),
            stream,
            raw_cache_key,
            Arc::new(HashSet::from(["docs".into()])),
            std::time::Instant::now(),
            None,
        );

        let output = cached_stream
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should be collected successfully");
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 1);

        let cached = cache_provider
            .get_raw_key(&raw_cache_key)
            .await
            .expect("cache lookup should succeed")
            .expect("a one-row result must fit in a 1 MiB cache");

        assert!(
            cached.get_memory_size() < 64 * 1024,
            "a one-row entry should be billed its own row, got {} bytes",
            cached.get_memory_size()
        );

        let cached_batches = cached.records().await.expect("cached result should decode");
        assert_eq!(cached_batches.len(), 1);
        assert_eq!(cached_batches[0].num_rows(), 1);
        assert_eq!(
            cached_batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("payload is a StringArray")
                .value(0),
            expected_payload,
            "compacting the entry must not change the row it holds"
        );
    }
}
