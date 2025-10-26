use aws_sdk_dynamodb::types::AttributeValue;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub partition_key: String,
    pub sort_key: Option<String>,
}

/// Encapsulates DynamoDB table schema, keys, and expression conversion logic.
/// This struct knows WHAT the table structure is and WHAT operations are supported.
#[derive(Debug, Clone)]
pub struct DynamoDBTableSchema {
    pub table_name: Arc<str>,
    pub table_schema: SchemaRef,
    pub partition_key: String,
    pub sort_key: Option<String>,
    pub global_secondary_indexes: Vec<IndexInfo>,
    pub column_to_alias_map: HashMap<String, String>, // actual_name -> #c0
    pub alias_to_column_map: HashMap<String, String>, // #c0 -> actual_name
}

impl DynamoDBTableSchema {
    pub fn new(
        table_name: Arc<str>,
        table_schema: SchemaRef,
        partition_key: String,
        sort_key: Option<String>,
        gsi_info: Vec<IndexInfo>,
    ) -> Self {
        let (column_to_alias_map, alias_to_column_map) = build_column_alias_maps(&table_schema);

        Self {
            table_name,
            table_schema,
            partition_key,
            sort_key,
            global_secondary_indexes: gsi_info,
            column_to_alias_map,
            alias_to_column_map,
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
            .map(|s| s.as_str())
    }

    pub fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let support: Vec<_> = filters
            .iter()
            .map(|&expr| {
                if self.is_filter_supported(expr) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();

        Ok(support)
    }

    pub fn build_filter_expression(
        &self,
        filters: &[Expr],
    ) -> DataFusionResult<(String, HashMap<String, AttributeValue>)> {
        if filters.is_empty() {
            return Ok((String::new(), HashMap::new()));
        }

        let mut attribute_values = HashMap::new();
        let mut value_counter = 0;

        let filter_parts: Vec<String> = filters
            .iter()
            .filter_map(|expr| {
                self.expr_to_filter_string(expr, &mut attribute_values, &mut value_counter)
                    .ok()
            })
            .collect();

        if filter_parts.is_empty() {
            return Ok((String::new(), HashMap::new()));
        }

        let filter_expr = filter_parts.join(" AND ");
        Ok((filter_expr, attribute_values))
    }

    pub fn extract_attribute_names(&self, filters: &[Expr]) -> HashMap<String, String> {
        let mut attribute_names = HashMap::new();
        for expr in filters {
            self.extract_columns_from_expr(expr, &mut attribute_names);
        }
        attribute_names
    }

    pub fn build_key_condition_expression(
        &self,
        partition_expr: &Expr,
        sort_expr: Option<&Expr>,
    ) -> datafusion::error::Result<(String, HashMap<String, AttributeValue>)> {
        let mut attribute_values = HashMap::new();
        // Filters start with 0, whereas keys start with 1000 to avoid overlapping
        let mut value_counter = 1000;

        let partition_str =
            self.expr_to_filter_string(partition_expr, &mut attribute_values, &mut value_counter)?;

        let key_condition = if let Some(sort) = sort_expr {
            let sort_str =
                self.expr_to_filter_string(sort, &mut attribute_values, &mut value_counter)?;
            format!("{} AND {}", partition_str, sort_str)
        } else {
            partition_str
        };

        Ok((key_condition, attribute_values))
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
                println!("Column: {:?}", col.name);
                println!("Column: {:?}", self.column_to_alias_map.keys());
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

    fn expr_to_filter_string(
        &self,
        expr: &Expr,
        attribute_values: &mut HashMap<String, AttributeValue>,
        value_counter: &mut usize,
    ) -> DataFusionResult<String> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let left_str = self.expr_to_filter_string(left, attribute_values, value_counter)?;
                let right_str =
                    self.expr_to_filter_string(right, attribute_values, value_counter)?;

                let op_str = match op {
                    Operator::Eq => "=",
                    Operator::NotEq => "<>",
                    Operator::Lt => "<",
                    Operator::LtEq => "<=",
                    Operator::Gt => ">",
                    Operator::GtEq => ">=",
                    Operator::And => "AND",
                    Operator::Or => "OR",
                    _ => {
                        return Err(DataFusionError::NotImplemented(format!(
                            "Operator {:?} not supported",
                            op
                        )));
                    }
                };

                Ok(format!("({} {} {})", left_str, op_str, right_str))
            }
            Expr::Column(col) => self
                .column_to_alias_map
                .get(col.name.as_str())
                .cloned()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("Column {} not found", col.name))
                }),
            Expr::Literal(scalar, _) => {
                let value_key = format!(":v{}", value_counter);
                *value_counter += 1;

                let attr_value = scalar_to_attribute_value(scalar)?;
                attribute_values.insert(value_key.clone(), attr_value);

                Ok(value_key)
            }
            _ => Err(DataFusionError::NotImplemented(
                "Expression type not supported in filters".to_string(),
            )),
        }
    }

    fn extract_columns_from_expr(
        &self,
        expr: &Expr,
        attribute_names: &mut HashMap<String, String>,
    ) {
        match expr {
            Expr::Column(col) => {
                if let Some(alias) = self.column_to_alias_map.get(col.name.as_str()) {
                    attribute_names.insert(alias.clone(), col.name.to_string());
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                self.extract_columns_from_expr(left, attribute_names);
                self.extract_columns_from_expr(right, attribute_names);
            }
            _ => {}
        }
    }
}

fn build_column_alias_maps(
    schema: &SchemaRef,
) -> (HashMap<String, String>, HashMap<String, String>) {
    let mut column_to_alias_map = HashMap::new();
    let mut alias_to_column_map = HashMap::new();

    for (i, field) in schema.fields().iter().enumerate() {
        let column_name = field.name().clone();
        let alias = format!("#c{i}");

        column_to_alias_map.insert(column_name.clone(), alias.clone());
        alias_to_column_map.insert(alias, column_name);
    }

    (column_to_alias_map, alias_to_column_map)
}

fn scalar_to_attribute_value(scalar: &ScalarValue) -> datafusion::error::Result<AttributeValue> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => Ok(AttributeValue::S(s.clone())),
        ScalarValue::Int64(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::Int32(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::Float64(Some(f)) => Ok(AttributeValue::N(f.to_string())),
        ScalarValue::Float32(Some(f)) => Ok(AttributeValue::N(f.to_string())),
        ScalarValue::Boolean(Some(b)) => Ok(AttributeValue::Bool(*b)),
        ScalarValue::Null => Ok(AttributeValue::Null(true)),
        _ => Err(DataFusionError::NotImplemented(
            "ScalarValue type not supported".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{BinaryExpr, Operator, col, lit};
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
            Vec::new(),
        )
    }

    #[test]
    fn test_new_creates_schema_with_aliases() {
        let schema = create_test_schema();

        assert_eq!(schema.table_name(), "test_table");
        assert_eq!(schema.partition_key(), "id");
        assert_eq!(schema.sort_key(), Some("sort_key"));
        assert_eq!(schema.column_to_alias_map.len(), 5);
        assert_eq!(schema.alias_to_column_map.len(), 5);
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

        let table_schema = DynamoDBTableSchema::new(
            Arc::from("test_table"),
            schema,
            "id".to_string(),
            None,
            Vec::new(),
        );

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
        let result = schema.supports_filters_pushdown(&filters).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], TableProviderFilterPushDown::Exact);
        assert_eq!(result[1], TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn test_build_filter_expression_simple() {
        let schema = create_test_schema();

        let filter = col("age").eq(lit(25i64));
        let (expr, values) = schema.build_filter_expression(&[filter]).unwrap();

        assert_eq!(expr, "(#c2 = :v0)");
        assert_eq!(values.len(), 1);
        assert!(values.contains_key(":v0"));
    }

    #[test]
    fn test_build_filter_expression_multiple_filters() {
        let schema = create_test_schema();

        let filter1 = col("age").gt(lit(18i64));
        let filter2 = col("active").eq(lit(true));

        let (expr, values) = schema.build_filter_expression(&[filter1, filter2]).unwrap();

        assert!(expr.contains("AND"));
        assert!(expr.contains("#c2"));
        assert!(expr.contains("#c4"));
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_build_filter_expression_empty() {
        let schema = create_test_schema();

        let (expr, values) = schema.build_filter_expression(&[]).unwrap();

        assert!(expr.is_empty());
        assert!(values.is_empty());
    }

    #[test]
    fn test_build_filter_expression_complex() {
        let schema = create_test_schema();

        // (age > 18 AND active = true)
        let filter = col("age").gt(lit(18i64)).and(col("active").eq(lit(true)));
        let (expr, values) = schema.build_filter_expression(&[filter]).unwrap();

        assert!(expr.contains("AND"));
        assert!(expr.contains("#c2"));
        assert!(expr.contains("#c4"));
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_extract_attribute_names() {
        let schema = create_test_schema();

        let filter1 = col("age").eq(lit(25i64));
        let filter2 = col("name").eq(lit("John"));

        let attr_names = schema.extract_attribute_names(&[filter1, filter2]);

        assert_eq!(attr_names.len(), 2);
        assert_eq!(attr_names.get("#c2"), Some(&"age".to_string()));
        assert_eq!(attr_names.get("#c3"), Some(&"name".to_string()));
    }

    #[test]
    fn test_extract_attribute_names_nested() {
        let schema = create_test_schema();

        // age > 18 AND name = "John"
        let filter = col("age").gt(lit(18i64)).and(col("name").eq(lit("John")));

        let attr_names = schema.extract_attribute_names(&[filter]);

        assert_eq!(attr_names.len(), 2);
        assert_eq!(attr_names.get("#c2"), Some(&"age".to_string()));
        assert_eq!(attr_names.get("#c3"), Some(&"name".to_string()));
    }

    #[test]
    fn test_build_key_condition_expression_partition_only() {
        let schema = create_test_schema();

        let partition_expr = col("id").eq(lit("user123"));
        let (expr, values) = schema
            .build_key_condition_expression(&partition_expr, None)
            .unwrap();

        assert_eq!(expr, "(#c0 = :v0)");
        assert_eq!(values.len(), 1);
        assert!(values.contains_key(":v0"));
    }

    #[test]
    fn test_build_key_condition_expression_with_sort() {
        let schema = create_test_schema();

        let partition_expr = col("id").eq(lit("user123"));
        let sort_expr = col("sort_key").gt(lit("2024-01-01"));

        let (expr, values) = schema
            .build_key_condition_expression(&partition_expr, Some(&sort_expr))
            .unwrap();

        assert!(expr.contains("AND"));
        assert!(expr.contains("#c0"));
        assert!(expr.contains("#c1"));
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_scalar_to_attribute_value_string() {
        let scalar = ScalarValue::Utf8(Some("test".to_string()));
        let attr = scalar_to_attribute_value(&scalar).unwrap();

        match attr {
            AttributeValue::S(s) => assert_eq!(s, "test"),
            _ => panic!("Expected String attribute"),
        }
    }

    #[test]
    fn test_scalar_to_attribute_value_numbers() {
        let scalar_i64 = ScalarValue::Int64(Some(42));
        let attr = scalar_to_attribute_value(&scalar_i64).unwrap();
        assert!(matches!(attr, AttributeValue::N(_)));

        let scalar_i32 = ScalarValue::Int32(Some(42));
        let attr = scalar_to_attribute_value(&scalar_i32).unwrap();
        assert!(matches!(attr, AttributeValue::N(_)));

        let scalar_f64 = ScalarValue::Float64(Some(42.5));
        let attr = scalar_to_attribute_value(&scalar_f64).unwrap();
        assert!(matches!(attr, AttributeValue::N(_)));
    }

    #[test]
    fn test_scalar_to_attribute_value_boolean() {
        let scalar = ScalarValue::Boolean(Some(true));
        let attr = scalar_to_attribute_value(&scalar).unwrap();

        match attr {
            AttributeValue::Bool(b) => assert!(b),
            _ => panic!("Expected Boolean attribute"),
        }
    }

    #[test]
    fn test_scalar_to_attribute_value_null() {
        let scalar = ScalarValue::Null;
        let attr = scalar_to_attribute_value(&scalar).unwrap();

        assert!(matches!(attr, AttributeValue::Null(true)));
    }

    #[test]
    fn test_build_column_alias_maps() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Utf8, false),
            Field::new("col2", DataType::Int64, false),
            Field::new("col3", DataType::Boolean, false),
        ]));

        let (col_to_alias, alias_to_col) = build_column_alias_maps(&schema);

        assert_eq!(col_to_alias.get("col1"), Some(&"#c0".to_string()));
        assert_eq!(col_to_alias.get("col2"), Some(&"#c1".to_string()));
        assert_eq!(col_to_alias.get("col3"), Some(&"#c2".to_string()));

        assert_eq!(alias_to_col.get("#c0"), Some(&"col1".to_string()));
        assert_eq!(alias_to_col.get("#c1"), Some(&"col2".to_string()));
        assert_eq!(alias_to_col.get("#c2"), Some(&"col3".to_string()));
    }

    #[test]
    fn test_expr_to_filter_string_all_operators() {
        let schema = create_test_schema();
        let mut values = HashMap::new();
        let mut counter = 0;

        let operators = vec![
            (Operator::Eq, "="),
            (Operator::NotEq, "<>"),
            (Operator::Lt, "<"),
            (Operator::LtEq, "<="),
            (Operator::Gt, ">"),
            (Operator::GtEq, ">="),
        ];

        for (op, expected_str) in operators {
            let expr = Expr::BinaryExpr(BinaryExpr {
                left: Box::new(col("age")),
                op,
                right: Box::new(lit(25i64)),
            });

            let result = schema
                .expr_to_filter_string(&expr, &mut values, &mut counter)
                .unwrap();
            assert!(result.contains(expected_str));
        }
    }

    #[test]
    fn test_filter_with_different_data_types() {
        let schema = create_test_schema();

        let string_filter = col("name").eq(lit("Alice"));
        let int_filter = col("age").eq(lit(30i64));
        let bool_filter = col("active").eq(lit(true));

        let (expr, values) = schema
            .build_filter_expression(&[string_filter, int_filter, bool_filter])
            .unwrap();

        assert!(expr.contains("#c3")); // name
        assert!(expr.contains("#c2")); // age
        assert!(expr.contains("#c4")); // active
        assert_eq!(values.len(), 3);
    }
}
