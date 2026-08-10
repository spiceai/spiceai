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
use datafusion::{
    common::tree_node::TreeNodeRecursion, execution::SendableRecordBatchStream,
    logical_expr::LogicalPlan, physical_plan::stream::RecordBatchStreamAdapter,
    sql::TableReference,
};

use crate::{CachedQueryResult, QueryResultsCacheProvider, RawCacheKey, Sizeable};

use async_stream::stream;

use futures::StreamExt;

pub const RESPONSE_STATUS_COLUMN: &str = "response_status";

const HTTP_RESULT_COLUMNS: [&str; 7] = [
    "request_path",
    "request_query",
    "request_body",
    "content",
    RESPONSE_STATUS_COLUMN,
    "response_headers",
    "_fetched_at",
];

/// Filter out transient HTTP error responses (5xx server errors and 429 Too Many Requests)
/// from record batches before caching.
///
/// If the batches don't contain a `response_status` column (i.e., not from an HTTP connector),
/// returns the batches unchanged.
#[must_use]
pub fn filter_transient_error_responses(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    if batches.is_empty() {
        return Vec::new();
    }

    // If schema doesn't have response_status column, this isn't an HTTP result — return as-is
    if batches[0]
        .schema()
        .column_with_name(RESPONSE_STATUS_COLUMN)
        .is_none()
    {
        return batches.to_vec();
    }

    let mut result = Vec::with_capacity(batches.len());

    for batch in batches {
        let Some(col_idx) = batch
            .schema()
            .column_with_name(RESPONSE_STATUS_COLUMN)
            .map(|(idx, _)| idx)
        else {
            result.push(batch.clone());
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
            .map(|status| status.map(|s| !(500..600).contains(&s) && s != 429))
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

fn is_http_result_batch(batch: &RecordBatch) -> bool {
    let schema = batch.schema();

    schema.column_with_name(RESPONSE_STATUS_COLUMN).is_some()
        && schema
            .fields()
            .iter()
            .all(|field| HTTP_RESULT_COLUMNS.contains(&field.name().as_str()))
        && schema
            .fields()
            .iter()
            .any(|field| field.name() != RESPONSE_STATUS_COLUMN)
}

fn has_transient_http_error_responses(batches: &[RecordBatch]) -> bool {
    let Some(first_batch) = batches.first() else {
        return false;
    };

    if !is_http_result_batch(first_batch) {
        return false;
    }

    for batch in batches {
        let Some(col_idx) = batch
            .schema()
            .column_with_name(RESPONSE_STATUS_COLUMN)
            .map(|(idx, _)| idx)
        else {
            return false;
        };

        let Some(status_array) = batch.column(col_idx).as_any().downcast_ref::<UInt16Array>()
        else {
            tracing::warn!(
                "'{RESPONSE_STATUS_COLUMN}' column is not UInt16Array, skipping transient HTTP cache validation"
            );
            return false;
        };

        if status_array
            .iter()
            .flatten()
            .any(|status| status == 429 || (500..600).contains(&status))
        {
            return true;
        }
    }

    false
}

/// Returns the batches that should be written to cache.
///
/// For HTTP-shaped results, any presence of a transient error response (5xx/429)
/// skips the entire cache write to avoid storing a partial result set. Non-HTTP
/// results are returned unchanged, even if they contain a `response_status`
/// column for unrelated business logic.
#[must_use]
pub fn batches_to_cache(batches: &[RecordBatch]) -> Option<Vec<RecordBatch>> {
    if has_transient_http_error_responses(batches) {
        return None;
    }

    Some(batches.to_vec())
}

/// Wraps `stream` so its results are stored in the cache once it has been
/// drained.
///
/// `read_started_at` is when the query began, and is recorded on the entry so
/// every later cache hit can check it — see
/// [`QueryResultsCacheProvider::tables_invalidated_since`], which documents why
/// the comparison is deliberately conservative. It must be the start of the
/// read, not the moment the result is stored: an invalidation landing in
/// between has to disqualify the entry too.
#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn to_cached_record_batch_stream(
    cache_provider: Arc<QueryResultsCacheProvider>,
    mut stream: SendableRecordBatchStream,
    raw_cache_key: RawCacheKey,
    input_tables: Arc<HashSet<TableReference>>,
    read_started_at: std::time::Instant,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let cache_schema = Arc::clone(&schema);

    let cached_result_stream = stream! {
        let mut records: Vec<RecordBatch> = Vec::new();
        let mut records_size: usize = 0;
        let has_encoder = cache_provider.encoder().is_some();
        // moka-rs operates by `u32` for records size, so max single record size is `u32::MAX` / 4 GB
        let cache_max_size = usize::try_from(cache_provider.max_size().min(u64::from(u32::MAX))).unwrap_or_default();

        while let Some(batch_result) = stream.next().await {
            if records_size < cache_max_size && let Ok(batch) = &batch_result {
                records.push(batch.clone());
                records_size += batch.get_array_memory_size();
            } else if !records.is_empty() && records_size >= cache_max_size && !has_encoder {
                // Eagerly clear the cached records when there is no encoder, as
                // the unencoded result won't fit in the cache. When an encoder is
                // present, the encoded size may be much smaller than the raw size,
                // so we keep accumulating and check the encoded size later.
                records.clear();
                records.shrink_to_fit();
            }

            yield batch_result;
        }

        // When an encoder is present, defer the size check until after encoding
        // so that compressed results that fit in the cache are not prematurely rejected.
        if records_size < cache_max_size || has_encoder {
            if cache_provider.tables_invalidated_since(&input_tables, read_started_at) {
                // Not the guard — correctness comes from the check every cache
                // hit performs. This only avoids encoding and storing a result
                // already known to be unservable.
                tracing::debug!(
                    "A table read by this query was invalidated while it ran, skipping cache storage"
                );
            } else {
                match batches_to_cache(&records) {
                    // `batches_to_cache` only returns `None` when transient HTTP
                    // error responses (5xx/429) are present, which requires a
                    // non-empty result set — skip the write to avoid caching a
                    // partial result.
                    None => {
                        tracing::debug!(
                            "Transient HTTP error responses were present, skipping cache storage"
                        );
                    }
                    // Cache the result, including genuinely empty (0-row / 0-batch)
                    // result sets. The schema is stored separately in
                    // `CachedQueryResult`, so an empty result round-trips with the
                    // correct schema, and caching it lets repeat queries that
                    // legitimately return no rows be served from cache instead of
                    // re-executing on every request.
                    Some(records_to_cache) => {
                        let cached_at = std::time::Instant::now();
                        let encoder = cache_provider.encoder();

                        match CachedQueryResult::from_batches(
                            &records_to_cache,
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
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::config::SessionConfig;
    use datafusion::execution::context::SessionContext;
    use std::collections::HashSet;

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

        let schema = create_http_response_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["ok", "server error"])),
                Arc::new(UInt16Array::from(vec![200, 500])),
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

        let schema = create_http_response_schema();
        let ok_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["ok"])),
                Arc::new(UInt16Array::from(vec![200])),
            ],
        )
        .expect("to create ok batch");
        let error_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["rate limited"])),
                Arc::new(UInt16Array::from(vec![429])),
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
        // uncompressed memory size exceeds the 2 KiB cache limit but compresses
        // well under zstd.
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
}
