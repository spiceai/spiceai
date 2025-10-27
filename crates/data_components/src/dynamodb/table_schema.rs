use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Encapsulates `DynamoDB` table schema, keys, and expression conversion logic.
/// This struct knows WHAT the table structure is and WHAT operations are supported.
#[derive(Debug, Clone)]
pub struct DynamoDBTableSchema {
    table_name: Arc<str>,
    table_schema: SchemaRef,
    partition_key: String,
    sort_key: Option<String>,
    column_to_alias_map: HashMap<String, String>, // actual_name -> #c0
}

impl DynamoDBTableSchema {
    pub fn new(
        table_name: Arc<str>,
        table_schema: SchemaRef,
        partition_key: String,
        sort_key: Option<String>,
    ) -> Self {
        let column_to_alias_map = build_column_alias_maps(&table_schema);

        Self {
            table_name,
            table_schema,
            partition_key,
            sort_key,
            column_to_alias_map,
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    pub fn partition_key(&self) -> &str {
        &self.partition_key
    }

    pub fn sort_key(&self) -> Option<&str> {
        self.sort_key.as_deref()
    }

    pub fn get_column_alias(&self, column_name: &str) -> Option<&str> {
        self.column_to_alias_map
            .get(column_name)
            .map(String::as_str)
    }

    pub fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Vec<TableProviderFilterPushDown> {
        filters
            .iter()
            .map(|&expr| {
                if self.is_filter_supported(expr) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect()
    }

    fn is_filter_supported(&self, expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let op_supported = matches!(
                    op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                        | Operator::And
                        | Operator::Or
                );

                op_supported && self.is_filter_supported(left) && self.is_filter_supported(right)
            }
            Expr::Column(col) => {
                self.column_to_alias_map.contains_key(col.name.as_str())
            }
            Expr::Literal(scalar, _) => matches!(
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
            ),
            _ => false,
        }
    }
}

fn build_column_alias_maps(schema: &SchemaRef) -> HashMap<String, String> {
    let mut column_to_alias_map = HashMap::new();
    let mut alias_to_column_map = HashMap::new();

    for (i, field) in schema.fields().iter().enumerate() {
        let column_name = field.name().clone();
        let alias = format!("#c{i}");

        column_to_alias_map.insert(column_name.clone(), alias.clone());
        alias_to_column_map.insert(alias, column_name);
    }

    column_to_alias_map
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit};
    use std::sync::Arc;

    fn create_test_schema() -> DynamoDBTableSchema {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("sort_key", DataType::Utf8, false),
            Field::new("age", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]));

        DynamoDBTableSchema::new(
            Arc::from("test_table"),
            schema,
            "id".to_string(),
            Some("sort_key".to_string()),
        )
    }

    #[test]
    fn test_new_creates_schema_with_aliases() {
        let schema = create_test_schema();

        assert_eq!(schema.table_name(), "test_table");
        assert_eq!(schema.partition_key(), "id");
        assert_eq!(schema.sort_key(), Some("sort_key"));
        assert_eq!(schema.column_to_alias_map.len(), 5);
    }

    #[test]
    fn test_column_alias_mapping() {
        let schema = create_test_schema();

        assert_eq!(schema.get_column_alias("id"), Some("#c0"));
        assert_eq!(schema.get_column_alias("sort_key"), Some("#c1"));
        assert_eq!(schema.get_column_alias("age"), Some("#c2"));
        assert_eq!(schema.get_column_alias("name"), Some("#c3"));
        assert_eq!(schema.get_column_alias("active"), Some("#c4"));
        assert_eq!(schema.get_column_alias("nonexistent"), None);
    }

    #[test]
    fn test_sort_key_optional() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));

        let table_schema =
            DynamoDBTableSchema::new(Arc::from("test_table"), schema, "id".to_string(), None);

        assert_eq!(table_schema.sort_key(), None);
    }

    #[test]
    fn test_is_filter_supported_simple_comparison() {
        let schema = create_test_schema();

        // age = 25
        let expr = col("age").eq(lit(25i64));
        assert!(schema.is_filter_supported(&expr));
    }

    #[test]
    fn test_is_filter_supported_all_operators() {
        let schema = create_test_schema();

        assert!(schema.is_filter_supported(&col("age").eq(lit(25i64))));
        assert!(schema.is_filter_supported(&col("age").not_eq(lit(25i64))));
        assert!(schema.is_filter_supported(&col("age").lt(lit(25i64))));
        assert!(schema.is_filter_supported(&col("age").lt_eq(lit(25i64))));
        assert!(schema.is_filter_supported(&col("age").gt(lit(25i64))));
        assert!(schema.is_filter_supported(&col("age").gt_eq(lit(25i64))));
    }

    #[test]
    fn test_is_filter_supported_and_or() {
        let schema = create_test_schema();

        // age > 18 AND active = true
        let expr = col("age").gt(lit(18i64)).and(col("active").eq(lit(true)));
        assert!(schema.is_filter_supported(&expr));

        // age > 18 OR age < 10
        let expr = col("age").gt(lit(18i64)).or(col("age").lt(lit(10i64)));
        assert!(schema.is_filter_supported(&expr));
    }

    #[test]
    fn test_is_filter_supported_nonexistent_column() {
        let schema = create_test_schema();

        let expr = col("nonexistent").eq(lit(25i64));
        assert!(!schema.is_filter_supported(&expr));
    }

    #[test]
    fn test_is_filter_supported_different_scalar_types() {
        let schema = create_test_schema();

        assert!(schema.is_filter_supported(&col("name").eq(lit("John"))));
        assert!(schema.is_filter_supported(&col("age").eq(lit(25i32))));
        assert!(schema.is_filter_supported(&col("age").eq(lit(25i64))));
        assert!(schema.is_filter_supported(&col("active").eq(lit(true))));
    }

    #[test]
    fn test_supports_filters_pushdown() {
        let schema = create_test_schema();

        let supported_filter = col("age").eq(lit(25i64));
        let unsupported_filter = col("nonexistent").eq(lit(25i64));

        let filters = vec![&supported_filter, &unsupported_filter];
        let result = schema.supports_filters_pushdown(&filters);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], TableProviderFilterPushDown::Exact);
        assert_eq!(result[1], TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn test_build_column_alias_maps() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Utf8, false),
            Field::new("col2", DataType::Int64, false),
            Field::new("col3", DataType::Boolean, false),
        ]));

        let col_to_alias = build_column_alias_maps(&schema);

        assert_eq!(col_to_alias.get("col1"), Some(&"#c0".to_string()));
        assert_eq!(col_to_alias.get("col2"), Some(&"#c1".to_string()));
        assert_eq!(col_to_alias.get("col3"), Some(&"#c2".to_string()));
    }
}
