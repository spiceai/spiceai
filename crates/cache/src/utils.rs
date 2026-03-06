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
    execution::SendableRecordBatchStream, logical_expr::LogicalPlan,
    physical_plan::stream::RecordBatchStreamAdapter, sql::TableReference,
};

use crate::{CachedQueryResult, QueryResultsCacheProvider, RawCacheKey};

use async_stream::stream;

use futures::StreamExt;

pub const RESPONSE_STATUS_COLUMN: &str = "response_status";

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

#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn to_cached_record_batch_stream(
    cache_provider: Arc<QueryResultsCacheProvider>,
    mut stream: SendableRecordBatchStream,
    raw_cache_key: RawCacheKey,
    input_tables: Arc<HashSet<TableReference>>,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let cache_schema = Arc::clone(&schema);

    let cached_result_stream = stream! {
        let mut records: Vec<RecordBatch> = Vec::new();
        let mut records_size: usize = 0;
        // moka-rs operates by `u32` for records size, so max single record size is `u32::MAX` / 4 GB
        let cache_max_size = usize::try_from(cache_provider.max_size().min(u64::from(u32::MAX))).unwrap_or_default();

        while let Some(batch_result) = stream.next().await {
            if records_size < cache_max_size && let Ok(batch) = &batch_result {
                records.push(batch.clone());
                records_size += batch.get_array_memory_size();
            } else if !records.is_empty() && records_size >= cache_max_size {
                // eagerly clear the cached records, as this result won't be cached anyway
                // this allows some memory reclamation if the stream is very large
                records.clear();
                records.shrink_to_fit();
            }

            yield batch_result;
        }

        if records_size < cache_max_size {
            // Filter out transient HTTP error responses (5xx/429) before caching.
            // Non-HTTP results pass through unchanged.
            let records_to_cache = filter_transient_error_responses(&records);
            if records_to_cache.is_empty() && !records.is_empty() {
                tracing::debug!("All query results were transient errors, skipping cache storage");
            } else if !records_to_cache.is_empty() {
                let cached_at = std::time::Instant::now();
                let encoder = cache_provider.encoder();

                match CachedQueryResult::from_batches(
                    &records_to_cache,
                    cache_schema,
                    input_tables,
                    cached_at,
                    encoder,
                )
                .await
                {
                    Ok(cached_result) => {
                        if let Err(e) = cache_provider.put_raw_key(&raw_cache_key, cached_result).await {
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

#[must_use]
pub fn get_logical_plan_input_tables(plan: &LogicalPlan) -> HashSet<TableReference> {
    let mut table_names: HashSet<TableReference> = HashSet::new();
    let mut plan_stack = vec![plan];

    while let Some(current_plan) = plan_stack.pop() {
        if let LogicalPlan::TableScan(source, ..) = current_plan {
            // Clones of TableReferences are cheap - all fields are Arcs
            table_names.insert(source.table_name.clone());
        }

        plan_stack.extend(current_plan.inputs());
    }

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

        let result = filter_transient_error_responses(&[batch.clone()]);
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
}
