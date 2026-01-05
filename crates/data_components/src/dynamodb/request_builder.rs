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
use crate::dynamodb::request_plan::{DynamoDBRequestPlan, QueryParamsBuilder, ScanParamsBuilder};
use crate::dynamodb::table_schema::DynamoDBTableSchema;
use crate::dynamodb::utils::FilterStringVisitor;
use aws_sdk_dynamodb::types::AttributeValue;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct DynamoDBRequestPlanBuilder {
    schema: DynamoDBTableSchema,
}

#[derive(Debug)]
enum KeyFilter {
    Partition(Expr),
    Sort(Expr),
}

/// Builds optimized `DynamoDB` request plans (Query or Scan) from `DataFusion` filter expressions and projections.
///
/// The builder automatically determines the most efficient request type:
///  * Query operations are generated when filters include an equality condition on the partition
///    key (and optionally a sort key condition), providing  direct indexed access to items.
///  * Scan operations are used as a fallback when key conditions cannot be met, such as when
///    no partition key filter exists or when filters contain OR operators.
///
/// All column references are automatically aliased using `expression_attribute_names` to ensure compatibility
/// with `DynamoDB` reserved words and special characters.
/// See: <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html#Expressions.ExpressionAttributeNames.ReservedWords>
impl DynamoDBRequestPlanBuilder {
    pub fn new(schema: DynamoDBTableSchema) -> Self {
        Self { schema }
    }

    /// Build a `DynamoDB` request (Query or Scan) based on filters and projections
    pub fn build_request_plan(
        &self,
        filters: &[Expr],
        projection_schema: &SchemaRef,
        limit: Option<usize>,
        json_nesting_static_fields: Option<&HashSet<String>>,
    ) -> DataFusionResult<DynamoDBRequestPlan> {
        // Separate key filters from other filters
        let (key_filters, other_filters) = self.separate_key_filters(filters);

        let mut attribute_names = self.extract_attribute_names(filters);

        if json_nesting_static_fields.is_none() {
            self.add_projection_aliases(projection_schema, &mut attribute_names);
        }

        let projection_expr = if json_nesting_static_fields.is_some() {
            None
        } else {
            self.build_projection_expression(projection_schema)
        };

        let limit_i32 = limit
            .map(|l| {
                i32::try_from(l)
                    .map_err(|_| DataFusionError::Execution("Limit too large".to_string()))
            })
            .transpose()?;

        if let Some((partition_expr, sort_expr)) = key_filters {
            self.build_query_request(
                &partition_expr,
                sort_expr.as_ref(),
                &other_filters,
                projection_expr,
                attribute_names,
                limit_i32,
            )
        } else {
            self.build_scan_request(filters, projection_expr, attribute_names, limit_i32)
        }
    }

    fn build_query_request(
        &self,
        partition_expr: &Expr,
        sort_expr: Option<&Expr>,
        other_filters: &[Expr],
        projection: Option<String>,
        attribute_names: HashMap<String, String>,
        limit: Option<i32>,
    ) -> DataFusionResult<DynamoDBRequestPlan> {
        let mut query_params =
            QueryParamsBuilder::default().table_name(self.schema.table_name().to_string());

        let (key_condition, mut key_values) =
            self.build_key_condition_expression(partition_expr, sort_expr)?;

        query_params = query_params.key_condition_expression(key_condition);

        if other_filters.is_empty() {
            // We only apply limit when there's no filter_expression.
            // This is because in DynamoDB filter is applied before filters.
            // As such, otherwise it may end up returning fewer records than we want.
            if let Some(l) = limit {
                query_params = query_params.limit(l);
            }
        } else {
            let (filter_str, filter_values) = self.build_filter_expression(other_filters)?;
            key_values.extend(filter_values);
            query_params = query_params.filter_expression(filter_str);
        }

        if !key_values.is_empty() {
            query_params = query_params.expression_attribute_values(key_values);
        }

        if let Some(proj) = projection {
            query_params = query_params.projection_expression(proj);
        }

        if !attribute_names.is_empty() {
            query_params = query_params.expression_attribute_names(attribute_names);
        }

        let query = query_params.build();
        Ok(DynamoDBRequestPlan::Query(query))
    }

    fn build_scan_request(
        &self,
        filters: &[Expr],
        projection: Option<String>,
        attribute_names: HashMap<String, String>,
        limit: Option<i32>,
    ) -> DataFusionResult<DynamoDBRequestPlan> {
        let mut scan_params =
            ScanParamsBuilder::default().table_name(self.schema.table_name().to_string());

        if filters.is_empty() {
            // We only apply limit when there's no filter_expression.
            // This is because in DynamoDB filter is applied before filters.
            // As such it may end returning fewer records than we want.
            if let Some(l) = limit {
                scan_params = scan_params.limit(l);
            }
        } else {
            let (filter_str, attribute_values) = self.build_filter_expression(filters)?;
            if !filter_str.is_empty() {
                scan_params = scan_params.filter_expression(filter_str);
            }
            if !attribute_values.is_empty() {
                scan_params = scan_params.expression_attribute_values(attribute_values);
            }
        }

        if let Some(proj) = projection {
            scan_params = scan_params.projection_expression(proj);
        }

        if !attribute_names.is_empty() {
            scan_params = scan_params.expression_attribute_names(attribute_names);
        }

        let scan = scan_params.build();
        Ok(DynamoDBRequestPlan::Scan(scan))
    }

    fn extract_attribute_names(&self, filters: &[Expr]) -> HashMap<String, String> {
        let mut attribute_names = HashMap::new();
        for expr in filters {
            self.extract_columns_from_expr(expr, &mut attribute_names);
        }
        attribute_names
    }

    fn extract_columns_from_expr(
        &self,
        expr: &Expr,
        attribute_names: &mut HashMap<String, String>,
    ) {
        let _ = expr.apply(|expr| {
            match expr {
                Expr::Column(col) => {
                    if self.schema.is_flattened_field(col.name()) {
                        // Add each segment separately for flattened fields
                        for segment in col.name().split('.') {
                            attribute_names.insert(format!("#{segment}"), segment.to_string());
                        }
                    } else {
                        // Add single alias for non-flattened fields
                        attribute_names.insert(format!("#{}", col.name()), col.name().to_string());
                    }
                }
                Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                    self.extract_columns_from_expr(left, attribute_names);
                    self.extract_columns_from_expr(right, attribute_names);
                }
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }

    fn build_key_condition_expression(
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
            format!("{partition_str} AND {sort_str}")
        } else {
            partition_str
        };

        Ok((key_condition, attribute_values))
    }

    fn build_filter_expression(
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
            .map(|expr| self.expr_to_filter_string(expr, &mut attribute_values, &mut value_counter))
            .collect::<DataFusionResult<Vec<String>>>()?;

        if filter_parts.is_empty() {
            return Ok((String::new(), HashMap::new()));
        }

        let filter_expr = filter_parts.join(" AND ");
        Ok((filter_expr, attribute_values))
    }

    fn expr_to_filter_string(
        &self,
        expr: &Expr,
        attribute_values: &mut HashMap<String, AttributeValue>,
        value_counter: &mut usize,
    ) -> DataFusionResult<String> {
        let mut visitor = FilterStringVisitor::new(&self.schema, attribute_values, value_counter);

        expr.visit(&mut visitor)?;

        if let Some(error) = visitor.error {
            return Err(error);
        }

        visitor
            .result_stack
            .pop()
            .ok_or_else(|| DataFusionError::Internal("No result produced".to_string()))
    }

    fn separate_key_filters(&self, filters: &[Expr]) -> (Option<(Expr, Option<Expr>)>, Vec<Expr>) {
        let has_or = filters.iter().any(contains_or);
        if has_or {
            return (None, filters.to_vec());
        }

        if let Some((partition, sort, other)) =
            try_match_index(filters, self.schema.partition_key(), self.schema.sort_key())
        {
            return (Some((partition, sort)), other);
        }

        (None, filters.to_vec())
    }

    fn build_projection_expression(&self, projection: &SchemaRef) -> Option<String> {
        let mut seen_top_level = HashSet::new();
        let mut projection_expr = Vec::new();

        for field in &projection.fields {
            let field_name = field.name();

            if self.schema.is_flattened_field(field_name) {
                if let Some(top_level) = field_name.split('.').next()
                    && seen_top_level.insert(top_level)
                {
                    projection_expr.push(format!("#{top_level}"));
                }
            } else {
                // Also track non-flattened top-level fields
                let top_level = field_name.split('.').next().unwrap_or(field_name);
                if seen_top_level.insert(top_level) {
                    projection_expr.push(format!("#{field_name}"));
                }
            }
        }

        if projection_expr.is_empty() {
            None
        } else {
            Some(projection_expr.join(", "))
        }
    }

    fn add_projection_aliases(
        &self,
        projection: &SchemaRef,
        attribute_names: &mut HashMap<String, String>,
    ) {
        let mut seen_top_level = HashSet::new();

        for field in &projection.fields {
            let field_name = field.name();

            if self.schema.is_flattened_field(field_name) {
                // For flattened fields, add only top-level segment
                if let Some(top_level) = field_name.split('.').next()
                    && seen_top_level.insert(top_level)
                {
                    attribute_names.insert(format!("#{top_level}"), top_level.to_string());
                }
            } else {
                // For non-flattened fields, add the full name
                attribute_names.insert(format!("#{field_name}"), field_name.clone());
            }
        }
    }
}

/// Attempts to match filters against a primary index (`partition_key` + `sort_key`)
fn try_match_index(
    filters: &[Expr],
    partition_key: &str,
    sort_key: Option<&str>,
) -> Option<(Expr, Option<Expr>, Vec<Expr>)> {
    let mut partition_expr = None;
    let mut sort_expr = None;
    let mut other_filters = Vec::new();

    for filter in filters {
        if let Some(extracted) = try_extract_key_filter(filter, partition_key, sort_key) {
            match extracted {
                KeyFilter::Partition(expr) => {
                    if partition_expr.is_some() {
                        return None;
                    }
                    partition_expr = Some(expr);
                }
                KeyFilter::Sort(expr) => {
                    if sort_expr.is_some() {
                        return None;
                    }
                    sort_expr = Some(expr);
                }
            }
        } else {
            other_filters.push(filter.clone());
        }
    }

    partition_expr.map(|p| (p, sort_expr, other_filters))
}

fn contains_or(expr: &Expr) -> bool {
    expr.apply(|expr| match expr {
        Expr::BinaryExpr(BinaryExpr {
            left: _,
            op: Operator::Or,
            ..
        }) => Err(DataFusionError::External("".into())),
        _ => Ok(TreeNodeRecursion::Continue),
    })
    .is_err()
}

/// Extracts key filter if the expression matches the specified partition or sort key
fn try_extract_key_filter(
    expr: &Expr,
    partition_key: &str,
    sort_key: Option<&str>,
) -> Option<KeyFilter> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left_col = match left.as_ref() {
                Expr::Column(col) => Some(col.name.as_str()),
                _ => None,
            };
            let right_col = match right.as_ref() {
                Expr::Column(col) => Some(col.name.as_str()),
                _ => None,
            };

            // Partition key matching (either side)
            if matches!(op, Operator::Eq)
                && (left_col == Some(partition_key) || right_col == Some(partition_key))
            {
                return Some(KeyFilter::Partition(expr.clone()));
            }

            // Sort key matching (either side)
            if let Some(sk) = sort_key
                && (left_col == Some(sk) || right_col == Some(sk))
                && matches!(
                    op,
                    Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
                )
            {
                return Some(KeyFilter::Sort(expr.clone()));
            }
            None
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;
    use aws_sdk_dynamodb::types::AttributeValue;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{col, lit};
    use std::sync::Arc;

    fn create_test_schema() -> DynamoDBTableSchema {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("sort_key", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("user.email", DataType::Utf8, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ]));

        let mut flattened_fields = HashSet::new();
        flattened_fields.insert("user.email".to_string());

        DynamoDBTableSchema::new(
            Arc::from("test_table"),
            schema,
            "id".to_string(),
            Some("sort_key".to_string()),
            flattened_fields,
            "2006-01-02T15:04:05.000Z07:00",
        )
    }

    fn create_projection_schema(fields: &[&str]) -> Arc<Schema> {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn test_plan_query_with_partition_key() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.table_name, "test_table");

                assert_eq!(
                    params.key_condition_expression,
                    Some("(#id = :v1000)".to_string())
                );

                // Should have attribute name for id
                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#id"), Some(&"id".to_string()));

                // Should have attribute value for user123
                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v1000"),
                    Some(&AttributeValue::S("user123".to_string()))
                );

                // No filter expression for partition key only
                assert_eq!(params.filter_expression, None);

                assert_eq!(params.limit, None);

                // Projection should be present
                assert!(params.projection_expression.is_some());
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_query_with_limit() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, Some(10), None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.table_name, "test_table");

                assert_eq!(
                    params.key_condition_expression,
                    Some("(#id = :v1000)".to_string())
                );

                // Should have attribute name for id
                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#id"), Some(&"id".to_string()));

                // Should have attribute value for user123
                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v1000"),
                    Some(&AttributeValue::S("user123".to_string()))
                );

                // No filter expression for partition key only
                assert_eq!(params.filter_expression, None);

                assert_eq!(params.limit, Some(10));

                // Projection should be present
                assert!(params.projection_expression.is_some());
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_query_with_partition_and_sort_key() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![
            col("id").eq(lit("user123")),
            col("sort_key").eq(lit("2024-01-01")),
        ];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.table_name, "test_table");

                // Key condition should be: (#c0 = :v1000) AND (#c1 = :v1001)
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#id = :v1000) AND (#sort_key = :v1001)".to_string())
                );

                // Should have attribute names for id and sort_key
                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#id"), Some(&"id".to_string()));
                assert_eq!(attr_names.get("#sort_key"), Some(&"sort_key".to_string()));

                // Should have attribute values
                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v1000"),
                    Some(&AttributeValue::S("user123".to_string()))
                );
                assert_eq!(
                    attr_values.get(":v1001"),
                    Some(&AttributeValue::S("2024-01-01".to_string()))
                );

                assert_eq!(params.filter_expression, None);
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_query_with_filter_expression() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("id").eq(lit("user123")), col("age").gt(lit(18i64))];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, Some(10), None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.table_name, "test_table");

                // Key condition for partition key: (#c0 = :v1000)
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#id = :v1000)".to_string())
                );

                // Filter expression for age: (#c3 > :v0)
                assert_eq!(params.filter_expression, Some("(#age > :v0)".to_string()));

                // Should have attribute names for id and age
                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#id"), Some(&"id".to_string()));
                assert_eq!(attr_names.get("#age"), Some(&"age".to_string()));

                // Should have attribute values (key values start at 1000, filter values at 0)
                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v1000"),
                    Some(&AttributeValue::S("user123".to_string()))
                );
                assert_eq!(
                    attr_values.get(":v0"),
                    Some(&AttributeValue::N("18".to_string()))
                );

                assert_eq!(params.limit, None);
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_scan_no_filters() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert_eq!(params.table_name, "test_table");
                assert_eq!(params.filter_expression, None);
                assert_eq!(params.expression_attribute_values, None);
                assert!(params.projection_expression.is_some());
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request"),
        }
    }

    #[test]
    fn test_plan_scan_with_filter_no_partition_key() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("name").eq(lit("John"))];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert_eq!(params.table_name, "test_table");

                // Filter expression: (#c2 = :v0)
                assert_eq!(params.filter_expression, Some("(#name = :v0)".to_string()));

                // Should have attribute name for name
                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#name"), Some(&"name".to_string()));

                // Should have attribute value for John
                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v0"),
                    Some(&AttributeValue::S("John".to_string()))
                );
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request"),
        }
    }

    #[test]
    fn test_plan_scan_with_or_filter() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![
            col("id")
                .eq(lit("user123"))
                .or(col("id").eq(lit("user456"))),
        ];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert_eq!(params.table_name, "test_table");

                // Filter expression with OR: ((#c0 = :v0) OR (#c0 = :v1))
                assert_eq!(
                    params.filter_expression,
                    Some("((#id = :v0) OR (#id = :v1))".to_string())
                );

                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#id"), Some(&"id".to_string()));

                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v0"),
                    Some(&AttributeValue::S("user123".to_string()))
                );
                assert_eq!(
                    attr_values.get(":v1"),
                    Some(&AttributeValue::S("user456".to_string()))
                );
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request due to OR"),
        }
    }

    #[test]
    fn test_plan_with_limit() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, Some(10), None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.limit, Some(10));
                assert_eq!(params.table_name, "test_table");
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_with_limit_too_large() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(&["id", "name"]);

        let result =
            builder.build_request_plan(&filters, &projection, Some(i32::MAX as usize + 1), None);

        assert!(result.is_err());
        assert!(
            result
                .expect_err("error")
                .to_string()
                .contains("Limit too large")
        );
    }

    #[test]
    fn test_plan_query_all_sort_key_operators() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let test_cases = vec![
            (col("sort_key").eq(lit("value")), "(#sort_key = :v1001)"),
            (col("sort_key").lt(lit("value")), "(#sort_key < :v1001)"),
            (col("sort_key").lt_eq(lit("value")), "(#sort_key <= :v1001)"),
            (col("sort_key").gt(lit("value")), "(#sort_key > :v1001)"),
            (col("sort_key").gt_eq(lit("value")), "(#sort_key >= :v1001)"),
        ];

        for (sort_op, expected_sort_condition) in test_cases {
            let filters = vec![col("id").eq(lit("user123")), sort_op];
            let projection = create_projection_schema(&["id", "name"]);

            let result = builder
                .build_request_plan(&filters, &projection, None, None)
                .expect("request plan");

            match result {
                DynamoDBRequestPlan::Query(params) => {
                    // Key condition should be: (#c0 = :v1000) AND <sort_condition>
                    let expected = format!("(#id = :v1000) AND {expected_sort_condition}");
                    assert_eq!(params.key_condition_expression, Some(expected));

                    let attr_values = params
                        .expression_attribute_values
                        .expect("expression_attribute_values");
                    assert_eq!(
                        attr_values.get(":v1000"),
                        Some(&AttributeValue::S("user123".to_string()))
                    );
                    assert_eq!(
                        attr_values.get(":v1001"),
                        Some(&AttributeValue::S("value".to_string()))
                    );
                }
                DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
            }
        }
    }

    #[test]
    fn test_multiple_partition_keys_forces_scan() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("id").eq(lit("user123")), col("id").eq(lit("user456"))];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                // Both conditions should be in filter: ((#c0 = :v0) AND (#c0 = :v1))
                assert_eq!(
                    params.filter_expression,
                    Some("(#id = :v0) AND (#id = :v1)".to_string())
                );

                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v0"),
                    Some(&AttributeValue::S("user123".to_string()))
                );
                assert_eq!(
                    attr_values.get(":v1"),
                    Some(&AttributeValue::S("user456".to_string()))
                );
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan due to multiple partition keys"),
        }
    }

    #[test]
    fn test_multiple_sort_keys_forces_scan() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![
            col("id").eq(lit("user123")),
            col("sort_key").gt(lit("2024-01-01")),
            col("sort_key").lt(lit("2024-12-31")),
        ];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                // All conditions in filter: ((#c0 = :v0) AND ((#c1 > :v1) AND (#c1 < :v2)))
                assert_eq!(
                    params.filter_expression,
                    Some("(#id = :v0) AND (#sort_key > :v1) AND (#sort_key < :v2)".to_string())
                );

                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#id"), Some(&"id".to_string()));
                assert_eq!(attr_names.get("#sort_key"), Some(&"sort_key".to_string()));

                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v0"),
                    Some(&AttributeValue::S("user123".to_string()))
                );
                assert_eq!(
                    attr_values.get(":v1"),
                    Some(&AttributeValue::S("2024-01-01".to_string()))
                );
                assert_eq!(
                    attr_values.get(":v2"),
                    Some(&AttributeValue::S("2024-12-31".to_string()))
                );
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan due to multiple sort keys"),
        }
    }

    #[test]
    fn test_partition_key_with_wrong_operator_forces_scan() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("id").gt(lit("user123"))];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                // Filter expression: (#c0 > :v0)
                assert_eq!(params.filter_expression, Some("(#id > :v0)".to_string()));

                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v0"),
                    Some(&AttributeValue::S("user123".to_string()))
                );
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan - partition key must use ="),
        }
    }

    #[test]
    fn test_empty_projection() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = Arc::new(Schema::empty());

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.projection_expression, None);
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#id = :v1000)".to_string())
                );
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_nested_or_in_filter() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![
            col("id").eq(lit("user123")),
            col("age")
                .gt(lit(18i64))
                .and(col("active").eq(lit(true)).or(col("active").eq(lit(false)))),
        ];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        // OR anywhere in the filter tree should force a scan
        match result {
            DynamoDBRequestPlan::Scan(params) => {
                // Complex nested expression with OR
                let filter = params.filter_expression.expect("filter_expression");
                assert!(filter.contains("OR"));
                assert!(filter.contains("#id"));
                assert!(filter.contains("#age"));
                assert!(filter.contains("#active"));
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan due to nested OR"),
        }
    }

    #[test]
    fn test_schema_without_sort_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),  // #c0
            Field::new("name", DataType::Utf8, true), // #c1
        ]));

        let table_schema = DynamoDBTableSchema::new(
            Arc::from("test_table"),
            schema,
            "id".to_string(),
            None, // No sort key
            HashSet::new(),
            "2006-01-02T15:04:05.000Z07:00",
        );

        let builder = DynamoDBRequestPlanBuilder::new(table_schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                // Only partition key condition
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#id = :v1000)".to_string())
                );

                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#id"), Some(&"id".to_string()));
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_scan_with_limit() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("name").eq(lit("John"))];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, Some(25), None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert_eq!(params.limit, None);
                assert_eq!(params.filter_expression, Some("(#name = :v0)".to_string()));
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request"),
        }
    }

    #[test]
    fn test_plan_scan_with_filters_and_empty_values() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("name").eq(col("sort_key"))];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert_eq!(params.limit, None);
                assert_eq!(
                    params.filter_expression,
                    Some("(#name = #sort_key)".to_string())
                );
                assert_eq!(params.expression_attribute_values, None);
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request"),
        }
    }

    #[test]
    fn test_plan_query_with_multiple_filter_expressions() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![
            col("id").eq(lit("user123")),
            col("age").gt(lit(18i64)),
            col("active").eq(lit(true)),
        ];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                // Key condition for partition key
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#id = :v1000)".to_string())
                );

                // Filter expression for age and active: ((#c3 > :v0) AND (#c4 = :v1))
                assert_eq!(
                    params.filter_expression,
                    Some("(#age > :v0) AND (#active = :v1)".to_string())
                );

                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.len(), 4);
                assert_eq!(attr_names.get("#id"), Some(&"id".to_string()));
                assert_eq!(attr_names.get("#age"), Some(&"age".to_string()));
                assert_eq!(attr_names.get("#active"), Some(&"active".to_string()));

                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v1000"),
                    Some(&AttributeValue::S("user123".to_string()))
                );
                assert_eq!(
                    attr_values.get(":v0"),
                    Some(&AttributeValue::N("18".to_string()))
                );
                assert_eq!(attr_values.get(":v1"), Some(&AttributeValue::Bool(true)));
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_scan_with_multiple_filters() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("name").eq(lit("John")), col("age").gt(lit(25i64))];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                // Filter expression: ((#c2 = :v0) AND (#c3 > :v1))
                assert_eq!(
                    params.filter_expression,
                    Some("(#name = :v0) AND (#age > :v1)".to_string())
                );

                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#name"), Some(&"name".to_string()));
                assert_eq!(attr_names.get("#age"), Some(&"age".to_string()));

                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v0"),
                    Some(&AttributeValue::S("John".to_string()))
                );
                assert_eq!(
                    attr_values.get(":v1"),
                    Some(&AttributeValue::N("25".to_string()))
                );
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request"),
        }
    }

    #[test]
    fn test_plan_query_with_not_equal_in_filter() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![
            col("id").eq(lit("user123")),
            col("name").not_eq(lit("Admin")),
        ];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                // Key condition
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#id = :v1000)".to_string())
                );

                // Filter expression with not equal: (#c2 <> :v0)
                assert_eq!(params.filter_expression, Some("(#name <> :v0)".to_string()));

                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v0"),
                    Some(&AttributeValue::S("Admin".to_string()))
                );
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_build_filter_expression_simple() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filter = col("age").eq(lit(25i64));
        let (expr, values) = builder.build_filter_expression(&[filter]).expect("filter");

        assert_eq!(expr, "(#age = :v0)");
        assert_eq!(values.len(), 1);
        assert!(values.contains_key(":v0"));
    }

    #[test]
    fn test_build_filter_expression_multiple_filters() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filter1 = col("age").gt(lit(18i64));
        let filter2 = col("active").eq(lit(true));

        let (expr, values) = builder
            .build_filter_expression(&[filter1, filter2])
            .expect("filter");

        assert_eq!(expr, "(#age > :v0) AND (#active = :v1)");
        assert_eq!(values.len(), 2);
        assert!(values.contains_key(":v0"));
        assert!(values.contains_key(":v1"));
    }

    #[test]
    fn test_build_filter_expression_empty() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let (expr, values) = builder.build_filter_expression(&[]).expect("filter");

        assert!(expr.is_empty());
        assert!(values.is_empty());
    }

    #[test]
    fn test_build_filter_expression_complex() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        // (age > 18 AND active = true)
        let filter = col("age").gt(lit(18i64)).and(col("active").eq(lit(true)));
        let (expr, values) = builder.build_filter_expression(&[filter]).expect("filter");

        assert_eq!(expr, "((#age > :v0) AND (#active = :v1))");
        assert_eq!(values.len(), 2);
        assert!(values.contains_key(":v0"));
        assert!(values.contains_key(":v1"));
    }

    #[test]
    fn test_extract_attribute_names() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filter1 = col("age").eq(lit(25i64));
        let filter2 = col("name").eq(lit("John"));

        let attr_names = builder.extract_attribute_names(&[filter1, filter2]);

        assert_eq!(attr_names.len(), 2);
        assert_eq!(attr_names.get("#name"), Some(&"name".to_string()));
        assert_eq!(attr_names.get("#age"), Some(&"age".to_string()));
    }

    #[test]
    fn test_extract_attribute_names_nested() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        // age > 18 AND name = "John"
        let filter = col("age").gt(lit(18i64)).and(col("name").eq(lit("John")));

        let attr_names = builder.extract_attribute_names(&[filter]);

        assert_eq!(attr_names.len(), 2);
        assert_eq!(attr_names.get("#name"), Some(&"name".to_string()));
        assert_eq!(attr_names.get("#age"), Some(&"age".to_string()));
    }

    #[test]
    fn test_build_key_condition_expression_partition_only() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let partition_expr = col("id").eq(lit("user123"));
        let (expr, values) = builder
            .build_key_condition_expression(&partition_expr, None)
            .expect("build_key_condition_expression");

        assert_eq!(expr, "(#id = :v1000)");
        assert_eq!(values.len(), 1);
        assert!(values.contains_key(":v1000"));
    }

    #[test]
    fn test_build_key_condition_expression_with_sort() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let partition_expr = col("id").eq(lit("user123"));
        let sort_expr = col("sort_key").gt(lit("2024-01-01"));

        let (expr, values) = builder
            .build_key_condition_expression(&partition_expr, Some(&sort_expr))
            .expect("build_key_condition_expression");

        assert_eq!(expr, "(#id = :v1000) AND (#sort_key > :v1001)");
        assert!(values.contains_key(":v1000"));
        assert!(values.contains_key(":v1001"));
    }

    #[test]
    fn test_expr_to_filter_string_all_operators() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

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

            let result = builder
                .expr_to_filter_string(&expr, &mut values, &mut counter)
                .expect("expr_to_filter_string");
            assert!(result.contains(expected_str));
        }
    }

    #[test]
    fn test_filter_with_timestamp_string_comparison() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filter = col("created_at").gt(lit(ScalarValue::TimestampMillisecond(
            Some(1_725_366_896_155),
            None,
        )));
        let (expr, values) = builder.build_filter_expression(&[filter]).expect("filter");
        assert_eq!(expr, "(#created_at > :v0)");
        assert_eq!(values.len(), 1);
        assert_eq!(
            values.get(":v0"),
            Some(&AttributeValue::S("2024-09-03T12:34:56.155Z".to_string()))
        );

        let filter = lit(ScalarValue::TimestampMillisecond(
            Some(1_725_366_896_155),
            None,
        ))
        .eq(col("created_at"));
        let (expr, values) = builder.build_filter_expression(&[filter]).expect("filter");
        assert_eq!(expr, "(:v0 = #created_at)");
        assert_eq!(values.len(), 1);
        assert_eq!(
            values.get(":v0"),
            Some(&AttributeValue::S("2024-09-03T12:34:56.155Z".to_string()))
        );
    }

    #[test]
    fn test_filter_with_timestamp_string_comparison_complex() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let f1 = col("created_at").gt(lit(ScalarValue::TimestampMillisecond(
            Some(1_725_366_896_155),
            None,
        )));
        let f2 = col("age").eq(lit(25)).and(f1);
        let f3 = col("name").eq(lit("John"));
        let (expr, values) = builder.build_filter_expression(&[f2, f3]).expect("filter");
        assert_eq!(
            expr,
            "((#age = :v0) AND (#created_at > :v1)) AND (#name = :v2)"
        );
        assert_eq!(values.len(), 3);
        assert_eq!(
            values.get(":v0"),
            Some(&AttributeValue::N("25".to_string()))
        );
        assert_eq!(
            values.get(":v1"),
            Some(&AttributeValue::S("2024-09-03T12:34:56.155Z".to_string()))
        );
        assert_eq!(
            values.get(":v2"),
            Some(&AttributeValue::S("John".to_string()))
        );
    }

    #[test]
    fn test_filter_with_different_data_types() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let string_filter = col("name").eq(lit("Alice"));
        let int_filter = col("age").eq(lit(30i64));
        let bool_filter = col("active").eq(lit(true));

        let (expr, values) = builder
            .build_filter_expression(&[string_filter, int_filter, bool_filter])
            .expect("filter");

        assert!(expr.contains("#name"));
        assert!(expr.contains("#age"));
        assert!(expr.contains("#active"));
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn test_nested_column_filter() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filter = col(r#""user.email""#).eq(lit("john@example.com"));

        let (expr, values) = builder.build_filter_expression(&[filter]).expect("filter");

        assert_eq!(expr, "(#user.#email = :v0)");
        assert_eq!(values.len(), 1);
        assert_eq!(
            values.get(":v0"),
            Some(&AttributeValue::S("john@example.com".to_string()))
        );
    }

    #[test]
    fn test_scan_with_json_nesting_no_filters() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![];
        let projection = create_projection_schema(&["id", "name"]);

        let static_fields = HashSet::from(["id".to_string(), "sort_key".to_string()]);

        let result = builder
            .build_request_plan(&filters, &projection, None, Some(&static_fields))
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert_eq!(params.table_name, "test_table");

                // No projection expression when json nesting is enabled
                assert_eq!(params.projection_expression, None);

                // No expression attribute names when no filters
                assert!(
                    params.expression_attribute_names.is_none()
                        || params
                            .expression_attribute_names
                            .as_ref()
                            .expect("value")
                            .is_empty()
                );

                assert_eq!(params.filter_expression, None);
                assert_eq!(params.expression_attribute_values, None);
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request"),
        }
    }

    #[test]
    fn test_scan_with_json_nesting_and_filters() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("age").gt(lit(18_i64))];
        let projection = create_projection_schema(&["id", "name", "age"]);

        let static_fields = HashSet::from(["id".to_string(), "sort_key".to_string()]);

        let result = builder
            .build_request_plan(&filters, &projection, None, Some(&static_fields))
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert_eq!(params.table_name, "test_table");

                // No projection expression when json nesting is enabled
                assert_eq!(params.projection_expression, None);

                // Should have attribute names ONLY from filters (not from projection)
                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#age"), Some(&"age".to_string()));
                // Should NOT have projection fields
                assert!(!attr_names.contains_key("#id"));
                assert!(!attr_names.contains_key("#name"));

                // Should have filter expression
                assert_eq!(params.filter_expression, Some("(#age > :v0)".to_string()));

                // Should have attribute values for filter
                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v0"),
                    Some(&AttributeValue::N("18".to_string()))
                );
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request"),
        }
    }

    #[test]
    fn test_query_with_json_nesting() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![col("id").eq(lit("user123")), col("age").gt(lit(25_i64))];
        let projection = create_projection_schema(&["id", "name", "age"]);

        let static_fields = HashSet::from(["id".to_string(), "sort_key".to_string()]);

        let result = builder
            .build_request_plan(&filters, &projection, None, Some(&static_fields))
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.table_name, "test_table");

                // No projection expression when json nesting is enabled
                assert_eq!(params.projection_expression, None);

                // Key condition for partition key
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#id = :v1000)".to_string())
                );

                // Filter expression for non-key filter
                assert_eq!(params.filter_expression, Some("(#age > :v0)".to_string()));

                // Should have attribute names ONLY from filters
                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#id"), Some(&"id".to_string()));
                assert_eq!(attr_names.get("#age"), Some(&"age".to_string()));
                // Should NOT have projection-only fields
                assert!(!attr_names.contains_key("#name"));

                // Should have attribute values for both key condition and filter
                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v1000"),
                    Some(&AttributeValue::S("user123".to_string()))
                );
                assert_eq!(
                    attr_values.get(":v0"),
                    Some(&AttributeValue::N("25".to_string()))
                );
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_query_with_json_nesting_and_sort_key() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![
            col("id").eq(lit("user123")),
            col("sort_key").eq(lit("2024-01-01")),
        ];
        let projection = create_projection_schema(&["id", "sort_key", "name"]);

        let static_fields = HashSet::from(["id".to_string(), "sort_key".to_string()]);

        let result = builder
            .build_request_plan(&filters, &projection, None, Some(&static_fields))
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.table_name, "test_table");

                // No projection expression when json nesting is enabled
                assert_eq!(params.projection_expression, None);

                // Both partition and sort keys in key condition
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#id = :v1000) AND (#sort_key = :v1001)".to_string())
                );

                // No additional filter expression
                assert_eq!(params.filter_expression, None);

                // Should have attribute names for keys only (not projection)
                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.len(), 2);
                assert_eq!(attr_names.get("#id"), Some(&"id".to_string()));
                assert_eq!(attr_names.get("#sort_key"), Some(&"sort_key".to_string()));

                let attr_values = params
                    .expression_attribute_values
                    .expect("expression_attribute_values");
                assert_eq!(
                    attr_values.get(":v1000"),
                    Some(&AttributeValue::S("user123".to_string()))
                );
                assert_eq!(
                    attr_values.get(":v1001"),
                    Some(&AttributeValue::S("2024-01-01".to_string()))
                );
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_without_json_nesting_has_projection() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestPlanBuilder::new(schema);

        let filters = vec![];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                // Should have projection expression when json nesting is NOT enabled
                assert!(params.projection_expression.is_some());

                // Should have attribute names for projection
                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert!(attr_names.contains_key("#id"));
                assert!(attr_names.contains_key("#name"));
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request"),
        }
    }
}
