/*
Copyright 2025 The Spice.ai OSS Authors

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
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::scalar::ScalarValue;
use std::collections::HashSet;
use std::sync::Arc;

/// Encapsulates `DynamoDB` table schema, keys, and expression conversion logic.
/// This struct knows WHAT the table structure is and WHAT operations are supported.
#[derive(Debug, Clone)]
pub struct DynamoDBTableSchema {
    table_name: Arc<str>,
    table_schema: SchemaRef,
    partition_key: String,
    sort_key: Option<String>,
    flattened_fields: HashSet<String>,
    time_format: Arc<String>,
}

impl DynamoDBTableSchema {
    pub fn new(
        table_name: Arc<str>,
        table_schema: SchemaRef,
        partition_key: String,
        sort_key: Option<String>,
        flattened_fields: HashSet<String>,
        time_format: &str,
    ) -> Self {
        Self {
            table_name,
            table_schema,
            partition_key,
            sort_key,
            flattened_fields,
            time_format: Arc::from(time_format.to_string()),
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    pub fn time_format(&self) -> Arc<String> {
        Arc::clone(&self.time_format)
    }

    pub fn partition_key(&self) -> &str {
        &self.partition_key
    }

    pub fn sort_key(&self) -> Option<&str> {
        self.sort_key.as_deref()
    }

    pub fn is_flattened_field(&self, field_name: &str) -> bool {
        if self.flattened_fields.contains(field_name) {
            return true;
        }

        // Check if any parent prefix is flattened
        let mut parts: Vec<&str> = field_name.split('.').collect();
        while parts.len() > 1 {
            parts.pop();
            let parent = parts.join(".");
            if self.flattened_fields.contains(&parent) {
                return true;
            }
        }

        false
    }

    pub fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Vec<TableProviderFilterPushDown> {
        filters
            .iter()
            .map(|&expr| {
                if self.is_filter_supported(expr, false) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect()
    }

    /// returns true for cases like:
    /// `Column(created_at) > TimestampMillisecond(1_725_366_896_155, Some("+00:00"))`
    fn is_timestamp_filter(&self, left: &Expr, op: Operator, right: &Expr) -> bool {
        (self.is_timestamp_column(left)
            && is_comparison_operator(op)
            && is_timestamp_literal(right))
            || (self.is_timestamp_column(right)
                && is_comparison_operator(op)
                && is_timestamp_literal(left))
    }

    fn is_timestamp_column(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Column(col) => {
                let field = self.table_schema.field_with_name(&col.name);
                field
                    .map(|f| matches!(f.data_type(), &arrow::datatypes::DataType::Timestamp(_, _)))
                    .unwrap_or(false)
            }
            _ => false,
        }
    }

    fn is_filter_supported(&self, expr: &Expr, is_binary_expr_part: bool) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                self.is_timestamp_filter(left, *op, right)
                    || (is_comparison_operator(*op)
                        && self.is_filter_supported(left, true)
                        && self.is_filter_supported(right, true))
            }
            Expr::Column(col) => self.table_schema.field_with_name(&col.name).is_ok(),
            Expr::Literal(scalar, _) => {
                is_binary_expr_part
                    && matches!(
                        scalar,
                        ScalarValue::Utf8(_)
                            | ScalarValue::Int8(_)
                            | ScalarValue::Int16(_)
                            | ScalarValue::Int32(_)
                            | ScalarValue::Int64(_)
                            | ScalarValue::UInt8(_)
                            | ScalarValue::UInt16(_)
                            | ScalarValue::UInt32(_)
                            | ScalarValue::UInt64(_)
                            | ScalarValue::Float32(_)
                            | ScalarValue::Float64(_)
                            | ScalarValue::Boolean(_)
                    )
            }
            _ => false,
        }
    }
}

fn is_timestamp_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(scalar, _) => matches!(scalar, ScalarValue::TimestampMillisecond(_, _)),
        _ => false,
    }
}

fn is_comparison_operator(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::And
            | Operator::Or
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::logical_expr::{Operator, col, lit};
    use std::collections::HashSet;
    use std::sync::Arc;

    fn create_test_schema() -> DynamoDBTableSchema {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("sort_key", DataType::Utf8, false),
            Field::new("age", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ]));

        DynamoDBTableSchema::new(
            Arc::from("test_table"),
            schema,
            "id".to_string(),
            Some("sort_key".to_string()),
            HashSet::new(),
            "2006-01-02T15:04:05.000Z07:00",
        )
    }

    fn create_test_schema_with_flattened() -> DynamoDBTableSchema {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("metadata.name", DataType::Utf8, true),
            Field::new("metadata.tags.version", DataType::Utf8, true),
        ]));

        let mut flattened = HashSet::new();
        flattened.insert("metadata".to_string());

        DynamoDBTableSchema::new(
            Arc::from("test_table"),
            schema,
            "id".to_string(),
            None,
            flattened,
            "2006-01-02T15:04:05.000Z07:00",
        )
    }

    #[test]
    fn test_new_and_getters() {
        let schema = create_test_schema();

        assert_eq!(schema.table_name(), "test_table");
        assert_eq!(schema.partition_key(), "id");
        assert_eq!(schema.sort_key(), Some("sort_key"));
        assert_eq!(schema.schema().fields().len(), 6);
    }

    #[test]
    fn test_sort_key_optional() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));

        let table_schema = DynamoDBTableSchema::new(
            Arc::from("test_table"),
            schema,
            "id".to_string(),
            None,
            HashSet::new(),
            "2006-01-02T15:04:05.000Z07:00",
        );

        assert_eq!(table_schema.sort_key(), None);
    }

    #[test]
    fn test_is_flattened_field_direct_match() {
        let schema = create_test_schema_with_flattened();

        assert!(schema.is_flattened_field("metadata"));
    }

    #[test]
    fn test_is_flattened_field_nested() {
        let schema = create_test_schema_with_flattened();

        // If parent is flattened, children should also be considered flattened
        assert!(schema.is_flattened_field("metadata.name"));
        assert!(schema.is_flattened_field("metadata.tags"));
        assert!(schema.is_flattened_field("metadata.tags.version"));
    }

    #[test]
    fn test_is_flattened_field_not_flattened() {
        let schema = create_test_schema_with_flattened();

        assert!(!schema.is_flattened_field("id"));
        assert!(!schema.is_flattened_field("other.field"));
    }

    #[test]
    fn test_is_flattened_field_empty_set() {
        let schema = create_test_schema();

        assert!(!schema.is_flattened_field("id"));
        assert!(!schema.is_flattened_field("metadata.name"));
    }

    #[test]
    fn test_is_filter_supported_simple_comparison() {
        let schema = create_test_schema();

        // age = 25
        let expr = col("age").eq(lit(25i64));
        assert!(schema.is_filter_supported(&expr, false));
    }

    #[test]
    fn test_is_filter_literal() {
        let schema = create_test_schema();

        assert!(schema.is_filter_supported(&lit(25i64), true));
        assert!(!schema.is_filter_supported(&lit(25i64), false));
    }

    #[test]
    fn test_is_filter_supported_all_operators() {
        let schema = create_test_schema();

        assert!(schema.is_filter_supported(&col("age").eq(lit(25i64)), false));
        assert!(schema.is_filter_supported(&col("age").not_eq(lit(25i64)), false));
        assert!(schema.is_filter_supported(&col("age").lt(lit(25i64)), false));
        assert!(schema.is_filter_supported(&col("age").lt_eq(lit(25i64)), false));
        assert!(schema.is_filter_supported(&col("age").gt(lit(25i64)), false));
        assert!(schema.is_filter_supported(&col("age").gt_eq(lit(25i64)), false));
    }

    #[test]
    fn test_is_filter_supported_and_or() {
        let schema = create_test_schema();

        // age > 18 AND active = true
        let expr = col("age").gt(lit(18i64)).and(col("active").eq(lit(true)));
        assert!(schema.is_filter_supported(&expr, false));

        // age > 18 OR age < 10
        let expr = col("age").gt(lit(18i64)).or(col("age").lt(lit(10i64)));
        assert!(schema.is_filter_supported(&expr, false));
    }

    #[test]
    fn test_is_filter_supported_different_scalar_types() {
        let schema = create_test_schema();

        assert!(schema.is_filter_supported(&col("name").eq(lit("John")), false));
        assert!(schema.is_filter_supported(&col("age").eq(lit(25i32)), false));
        assert!(schema.is_filter_supported(&col("age").eq(lit(25i64)), false));
        assert!(schema.is_filter_supported(&col("active").eq(lit(true)), false));
    }

    #[test]
    fn test_is_filter_supported_unsupported_operators() {
        let schema = create_test_schema();

        // These operators should not be supported
        let unsupported_ops = vec![
            Operator::Plus,
            Operator::Minus,
            Operator::Multiply,
            Operator::Divide,
            Operator::Modulo,
        ];

        for op in unsupported_ops {
            let expr = Expr::BinaryExpr(BinaryExpr {
                left: Box::new(col("age")),
                op,
                right: Box::new(lit(5i64)),
            });
            assert!(!schema.is_filter_supported(&expr, false));
        }
    }

    #[test]
    fn test_is_filter_supported_complex_nested() {
        let schema = create_test_schema();

        // (age > 18 AND active = true) OR (age < 10 AND name = "child")
        let expr = col("age")
            .gt(lit(18i64))
            .and(col("active").eq(lit(true)))
            .or(col("age").lt(lit(10i64)).and(col("name").eq(lit("child"))));

        assert!(schema.is_filter_supported(&expr, false));
    }

    #[test]
    fn test_supports_filters_pushdown() {
        let schema = create_test_schema();

        let supported_filter = col("age").eq(lit(25i64));

        let filters = vec![&supported_filter];
        let result = schema.supports_filters_pushdown(&filters);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn test_supports_filters_pushdown_empty() {
        let schema = create_test_schema();

        let filters: Vec<&Expr> = vec![];
        let result = schema.supports_filters_pushdown(&filters);

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_supports_filters_pushdown_all_supported() {
        let schema = create_test_schema();

        let f1 = col("age").eq(lit(25i64));
        let f2 = col("name").eq(lit("John"));

        let filters = vec![&f1, &f2];
        let result = schema.supports_filters_pushdown(&filters);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], TableProviderFilterPushDown::Exact);
        assert_eq!(result[1], TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn test_timestamp_filter_pushdown() {
        let schema = create_test_schema();

        let f1 = col("created_at").gt(lit(ScalarValue::TimestampMillisecond(
            Some(1_725_366_896_155),
            None,
        )));
        let filters = vec![&f1];
        let result = schema.supports_filters_pushdown(&filters);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], TableProviderFilterPushDown::Exact);

        let f2 = lit(ScalarValue::TimestampMillisecond(
            Some(1_725_366_896_155),
            None,
        ))
        .eq(col("created_at"));
        let filters = vec![&f2];
        let result = schema.supports_filters_pushdown(&filters);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn test_timestamp_filter_pushdown_complex() {
        let schema = create_test_schema();

        let f1 = col("created_at").gt(lit(ScalarValue::TimestampMillisecond(
            Some(1_725_366_896_155),
            None,
        )));
        let f2 = col("age").eq(lit(25i64)).and(f1);
        let f3 = col("name").eq(lit("John"));

        let filters = vec![&f2, &f3];
        let result = schema.supports_filters_pushdown(&filters);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], TableProviderFilterPushDown::Exact);
        assert_eq!(result[1], TableProviderFilterPushDown::Exact);
    }
}
