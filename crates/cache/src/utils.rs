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

use std::{collections::HashSet, sync::Arc};

use arrow::array::RecordBatch;
use datafusion::{
    execution::SendableRecordBatchStream, logical_expr::LogicalPlan,
    physical_plan::stream::RecordBatchStreamAdapter,
};

use crate::{CachedQueryResult, QueryResultsCacheProvider};

use async_stream::stream;

use futures::StreamExt;

#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn to_cached_record_batch_stream(
    cache_provider: Arc<QueryResultsCacheProvider>,
    mut stream: SendableRecordBatchStream,
    plan: LogicalPlan,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let schema_copy = Arc::clone(&schema);

    let cached_result_stream = stream! {
        let mut records: Vec<RecordBatch> = Vec::new();
        let mut records_size: usize = 0;
        let cache_max_size: usize = cache_provider.max_size().try_into().unwrap_or(usize::MAX);

        while let Some(batch_result) = stream.next().await {
            if records_size < cache_max_size {
                if let Ok(batch) = &batch_result {
                    records.push(batch.clone());
                    records_size += batch.get_array_memory_size();
                }
            }

            yield batch_result;
        }

        if records_size < cache_max_size {
            let cached_result = CachedQueryResult {
                records: Arc::new(records),
                schema: schema_copy,
                input_tables: Arc::new(get_logical_plan_input_tables(&plan)),
            };

            if let Err(e) = cache_provider.put(&plan, cached_result).await {
                tracing::error!("Failed to cache query results: {e}");
            }
        }
    };

    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        Box::pin(cached_result_stream),
    ))
}

#[must_use]
pub fn get_logical_plan_input_tables(plan: &LogicalPlan) -> HashSet<String> {
    let mut table_names: HashSet<String> = HashSet::new();
    collect_table_names(plan, &mut table_names);
    table_names
}

fn collect_table_names(plan: &LogicalPlan, table_names: &mut HashSet<String>) {
    if let LogicalPlan::TableScan(source, ..) = plan {
        table_names.insert(source.table_name.to_string().to_lowercase());
    }

    plan.inputs().iter().for_each(|input| {
        collect_table_names(input, table_names);
    });
}

#[must_use]
pub fn cache_is_enabled_for_plan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan(source, ..) => {
            let table_name = source.table_name.to_string();
            return !(table_name.starts_with("information_schema.")
                || table_name.starts_with("runtime."));
        }
        LogicalPlan::Explain { .. }
        | LogicalPlan::Analyze { .. }
        | LogicalPlan::DescribeTable { .. }
        | LogicalPlan::Statement(..) => return false,
        _ => {}
    }

    for input in plan.inputs() {
        if !is_cache_allowed_for_query(input) {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::config::SessionConfig;
    use datafusion::execution::context::SessionContext;
    use std::collections::HashSet;

    async fn parse_sql_to_logical_plan(sql: &str) -> LogicalPlan {
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

        let expected: HashSet<String> = vec![].into_iter().collect();
        assert_eq!(table_names, expected);
    }

    #[tokio::test]
    async fn test_collect_table_names_system_query_show_tables() {
        let sql = "show tables";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<String> = vec!["information_schema.tables".to_string()]
            .into_iter()
            .collect();
        assert_eq!(table_names, expected);
    }

    #[tokio::test]
    async fn test_collect_table_names_simple_select() {
        let sql = "SELECT * FROM customer";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<String> = vec!["customer".to_string()].into_iter().collect();
        assert_eq!(table_names, expected);
    }

    #[tokio::test]
    async fn test_collect_table_names_join() {
        let sql =
            "SELECT c.first_name, o.quantity FROM customer c JOIN orders o ON c.id = o.customer_id";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<String> = vec!["customer".to_string(), "orders".to_string()]
            .into_iter()
            .collect();
        assert_eq!(table_names, expected);
    }

    #[tokio::test]
    async fn test_collect_table_names_subquery() {
        let sql = "SELECT * FROM (SELECT * FROM state) AS s";
        let logical_plan = parse_sql_to_logical_plan(sql).await;

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<String> = vec!["state".to_string()].into_iter().collect();
        assert_eq!(table_names, expected);
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

        let expected: HashSet<String> = vec!["customer".to_string(), "orders".to_string()]
            .into_iter()
            .collect();
        assert_eq!(table_names, expected);
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

        let expected: HashSet<String> = vec!["customer".to_string(), "orders".to_string()]
            .into_iter()
            .collect();
        assert_eq!(table_names, expected);
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

        let expected: HashSet<String> = vec!["orders".to_string(), "customer".to_string()]
            .into_iter()
            .collect();
        assert_eq!(table_names, expected);
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
}
