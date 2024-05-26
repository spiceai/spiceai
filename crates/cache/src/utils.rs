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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{plan_err, Result};
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::builder::LogicalTableSource;
    use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};
    use datafusion::sql::planner::{ContextProvider, SqlToRel};
    use datafusion::sql::{sqlparser, TableReference};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::{HashMap, HashSet};

    fn parse_sql_to_logical_plan(sql: &str) -> LogicalPlan {
        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
        let ast = Parser::parse_sql(&dialect, sql).expect("Should parse SQL");
        let statement = &ast[0];

        let schema_provider = MyContextProvider::new();
        let sql_to_rel = SqlToRel::new(&schema_provider);
        sql_to_rel
            .sql_statement_to_plan(statement.clone())
            .expect("Should convert statement to plan")
    }

    #[test]
    fn test_collect_table_names_simple_select() {
        let sql = "SELECT * FROM customer";
        let logical_plan = parse_sql_to_logical_plan(sql);

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<String> = vec!["customer".to_string()].into_iter().collect();
        assert_eq!(table_names, expected);
    }

    #[test]
    fn test_collect_table_names_join() {
        let sql =
            "SELECT c.first_name, o.quantity FROM customer c JOIN orders o ON c.id = o.customer_id";
        let logical_plan = parse_sql_to_logical_plan(sql);

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<String> = vec!["customer".to_string(), "orders".to_string()]
            .into_iter()
            .collect();
        assert_eq!(table_names, expected);
    }

    #[test]
    fn test_collect_table_names_subquery() {
        let sql = "SELECT * FROM (SELECT * FROM state) AS s";
        let logical_plan = parse_sql_to_logical_plan(sql);

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<String> = vec!["state".to_string()].into_iter().collect();
        assert_eq!(table_names, expected);
    }

    #[test]
    fn test_collect_table_names_nested_subqueries_with_aliases() {
        let sql = "SELECT c.first_name, c.last_name, sub.total_orders \
                   FROM customer c \
                   JOIN ( \
                       SELECT o.customer_id, COUNT(*) as total_orders \
                       FROM orders o \
                       GROUP BY o.customer_id \
                   ) sub ON c.id = sub.customer_id \
                   WHERE sub.total_orders > 5";
        let logical_plan = parse_sql_to_logical_plan(sql);

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<String> = vec!["customer".to_string(), "orders".to_string()]
            .into_iter()
            .collect();
        assert_eq!(table_names, expected);
    }

    #[test]
    fn test_collect_table_names_union_with_subqueries() {
        let sql = "SELECT * FROM ( \
                       SELECT c.id, c.first_name, c.last_name \
                       FROM customer c \
                       WHERE c.state = 'NY' \
                       UNION \
                       SELECT o.id, o.customer_id, o.quantity \
                       FROM orders o \
                       WHERE o.quantity > 10 \
                   ) AS combined_results";
        let logical_plan = parse_sql_to_logical_plan(sql);

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<String> = vec!["customer".to_string(), "orders".to_string()]
            .into_iter()
            .collect();
        assert_eq!(table_names, expected);
    }

    #[test]
    fn test_collect_table_names_join_with_subquery_in_from_clause() {
        let sql = "SELECT main.customer_id, main.total_spent, c.first_name, c.last_name \
                   FROM ( \
                       SELECT o.customer_id, SUM(o.quantity * o.price) as total_spent \
                       FROM orders o \
                       GROUP BY o.customer_id \
                   ) main \
                   JOIN customer c ON main.customer_id = c.id \
                   WHERE main.total_spent > 500";
        let logical_plan = parse_sql_to_logical_plan(sql);

        let table_names = get_logical_plan_input_tables(&logical_plan);

        let expected: HashSet<String> = vec!["orders".to_string(), "customer".to_string()]
            .into_iter()
            .collect();
        assert_eq!(table_names, expected);
    }

    // Based on datafusion example: https://github.com/apache/datafusion/blob/cafbc9ddceb5af8c6408d0c8bbfed7568f655ddb/datafusion/sql/examples/sql.rs
    struct MyContextProvider {
        options: ConfigOptions,
        tables: HashMap<String, Arc<dyn TableSource>>,
    }

    impl MyContextProvider {
        fn new() -> Self {
            let mut tables = HashMap::new();
            tables.insert(
                "customer".to_string(),
                create_table_source(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("first_name", DataType::Utf8, false),
                    Field::new("last_name", DataType::Utf8, false),
                    Field::new("state", DataType::Utf8, false),
                ]),
            );
            tables.insert(
                "state".to_string(),
                create_table_source(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("sales_tax", DataType::Decimal128(10, 2), false),
                ]),
            );
            tables.insert(
                "orders".to_string(),
                create_table_source(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("customer_id", DataType::Int32, false),
                    Field::new("item_id", DataType::Int32, false),
                    Field::new("quantity", DataType::Int32, false),
                    Field::new("price", DataType::Decimal128(10, 2), false),
                ]),
            );
            Self {
                tables,
                options: ConfigOptions::default(),
            }
        }
    }

    impl ContextProvider for MyContextProvider {
        fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
            match self.tables.get(name.table()) {
                Some(table) => Ok(Arc::clone(table)),
                _ => plan_err!("Table not found: {}", name.table()),
            }
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            None
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            None
        }

        fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
            None
        }

        fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
            None
        }

        fn options(&self) -> &ConfigOptions {
            &self.options
        }

        fn udfs_names(&self) -> Vec<String> {
            Vec::new()
        }

        fn udafs_names(&self) -> Vec<String> {
            Vec::new()
        }

        fn udwfs_names(&self) -> Vec<String> {
            Vec::new()
        }
    }

    fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
        Arc::new(LogicalTableSource::new(Arc::new(
            Schema::new_with_metadata(fields, HashMap::new()),
        )))
    }
}
