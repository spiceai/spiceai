use crate::dynamodb::table_schema::DynamoDBTableSchema;
use aws_sdk_dynamodb::types::AttributeValue;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use derive_builder::Builder;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum DynamoDBRequestPlan {
    Query(QueryParams),
    Scan(ScanParams),
}

#[derive(Builder, Debug, Default, Clone)]
#[builder(pattern = "owned")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
pub struct QueryParams {
    pub table_name: String,
    pub key_condition_expression: Option<String>,
    pub filter_expression: Option<String>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub projection_expression: Option<String>,
    pub limit: Option<i32>,
}

#[derive(Builder, Debug, Default, Clone)]
#[builder(pattern = "owned")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
pub struct ScanParams {
    pub table_name: String,
    pub filter_expression: Option<String>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub projection_expression: Option<String>,
    pub limit: Option<i32>,
}

#[derive(Debug)]
pub struct DynamoDBRequestPlanBuilder {
    schema: DynamoDBTableSchema,
}

#[derive(Debug)]
enum KeyFilter {
    Partition(Expr),
    Sort(Expr),
}

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
    ) -> DataFusionResult<DynamoDBRequestPlan> {
        // Separate key filters from other filters
        let (key_filters, other_filters) = self.separate_key_filters(filters);
        println!("key_filters: {key_filters:?}");
        println!("other_filters: {other_filters:?}");

        let mut attribute_names = self.schema.extract_attribute_names(filters);
        self.add_projection_aliases(projection_schema, &mut attribute_names);

        let projection_expr = self.build_projection_expression(projection_schema);

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
        println!("!! QUERY !!");

        let mut builder = QueryParamsBuilder::default();
        builder = builder.table_name(self.schema.table_name().to_string());
        let mut query_params = builder;

        // let mut query_request = self
        //     .client
        //     .query()
        //     .table_name(self.schema.table_name().to_string());

        let (key_condition, mut key_values) = self
            .schema
            .build_key_condition_expression(partition_expr, sort_expr)?;

        println!("Key condition: {key_condition}");
        // query_request = query_request.key_condition_expression(key_condition.clone());
        query_params = query_params.key_condition_expression(key_condition);

        if !other_filters.is_empty() {
            let (filter_str, filter_values) = self.schema.build_filter_expression(other_filters);
            key_values.extend(filter_values);
            println!("filter_expression: {filter_str:?}");
            // query_request = query_request.filter_expression(filter_str.clone());
            query_params = query_params.filter_expression(filter_str);
        }

        if !key_values.is_empty() {
            println!("key_values: {:?}", &key_values);
            // query_request = query_request.set_expression_attribute_values(Some(key_values.clone()));
            query_params = query_params.expression_attribute_values(key_values);
        }

        if let Some(proj) = projection {
            println!("projection_expression: {proj:?}");
            // query_request = query_request.projection_expression(proj.clone());
            query_params = query_params.projection_expression(proj);
        }

        if !attribute_names.is_empty() {
            println!("attribute_names: {:?}", &attribute_names);
            // query_request = query_request.set_expression_attribute_names(Some(attribute_names.clone()));
            query_params = query_params.expression_attribute_names(attribute_names);
        }

        if let Some(l) = limit {
            // query_request = query_request.limit(l);
            query_params = query_params.limit(l);
        }

        Ok(DynamoDBRequestPlan::Query(query_params.build().unwrap())) // TODO
    }

    fn build_scan_request(
        &self,
        filters: &[Expr],
        projection: Option<String>,
        attribute_names: HashMap<String, String>,
        limit: Option<i32>,
    ) -> DataFusionResult<DynamoDBRequestPlan> {
        println!("!! SCAN !!");

        let mut scan_params =
            ScanParamsBuilder::default().table_name(self.schema.table_name().to_string());

        // let mut scan_request = self
        //     .client
        //     .scan()
        //     .table_name(self.schema.table_name().to_string());

        if !filters.is_empty() {
            let (filter_str, attribute_values) = self.schema.build_filter_expression(filters);
            if !filter_str.is_empty() {
                println!("filter_expression: {:?}", &filter_str);
                println!("attribute_values: {:?}", &attribute_values);
                // scan_request = scan_request.filter_expression(filter_str.clone());
                // scan_request = scan_request.set_expression_attribute_values(Some(attribute_values.clone()));
                scan_params = scan_params.filter_expression(filter_str);
                scan_params = scan_params.expression_attribute_values(attribute_values);
            }
        }

        if let Some(proj) = projection {
            println!("projection_expression: {proj:?}");
            // scan_request = scan_request.projection_expression(proj.clone());
            scan_params = scan_params.projection_expression(proj);
        }

        if !attribute_names.is_empty() {
            println!("attribute_names: {:?}", &attribute_names);
            // scan_request = scan_request.set_expression_attribute_names(Some(attribute_names.clone()));
            scan_params = scan_params.expression_attribute_names(attribute_names);
        }

        if let Some(l) = limit {
            // scan_request = scan_request.limit(l);
            scan_params = scan_params.limit(l);
        }

        Ok(DynamoDBRequestPlan::Scan(scan_params.build().unwrap())) // TODO
    }

    fn separate_key_filters(&self, filters: &[Expr]) -> (Option<(Expr, Option<Expr>)>, Vec<Expr>) {
        // Check for OR conditions first - if present, can't use Query
        let has_or = filters.iter().any(contains_or);
        if has_or {
            return (None, filters.to_vec());
        }

        if let Some((partition, sort, other)) = try_match_index(
            filters,
            &self.schema.partition_key,
            self.schema.sort_key.as_deref(),
        ) {
            return (Some((partition, sort)), other);
        }

        // No matching index found - must use Scan
        (None, filters.to_vec())
    }

    fn build_projection_expression(&self, projection: &SchemaRef) -> Option<String> {
        let projection_expr: Vec<String> = projection
            .fields
            .iter()
            .filter_map(|f| self.schema.get_column_alias(f.name()).map(String::from))
            .collect();

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
        for field in &projection.fields {
            if let Some(alias) = self.schema.get_column_alias(field.name()) {
                attribute_names.insert(alias.to_string(), field.name().to_string());
            }
        }
    }
}

/// Attempts to match filters against a specific index (base table or GSI)
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
                        // Multiple partition key filters - invalid
                        return None;
                    }
                    partition_expr = Some(expr);
                }
                KeyFilter::Sort(expr) => {
                    if sort_expr.is_some() {
                        // Multiple sort key filters - invalid
                        return None;
                    }
                    sort_expr = Some(expr);
                }
            }
        } else {
            other_filters.push(filter.clone());
        }
    }

    // Must have partition key to use Query
    partition_expr.map(|p| (p, sort_expr, other_filters))
}

fn contains_or(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            matches!(op, Operator::Or) || contains_or(left) || contains_or(right)
        }
        _ => false,
    }
}

/// Extracts key filter if the expression matches the specified partition or sort key
fn try_extract_key_filter(
    expr: &Expr,
    partition_key: &str,
    sort_key: Option<&str>,
) -> Option<KeyFilter> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, .. }) => {
            if let Expr::Column(col) = left.as_ref() {
                if col.name.as_str() == partition_key {
                    // Partition key must use = operator
                    if matches!(op, Operator::Eq) {
                        return Some(KeyFilter::Partition(expr.clone()));
                    }
                } else if let Some(sk) = sort_key
                    && col.name.as_str() == sk
                {
                    // Sort key can use =, <, >, <=, >=
                    if matches!(
                        op,
                        Operator::Eq
                            | Operator::Lt
                            | Operator::LtEq
                            | Operator::Gt
                            | Operator::GtEq
                    ) {
                        return Some(KeyFilter::Sort(expr.clone()));
                    }
                }
            }
            None
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::AttributeValue;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit};
    use std::sync::Arc;

    fn create_test_schema() -> DynamoDBTableSchema {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),       // #c0
            Field::new("sort_key", DataType::Utf8, true),  // #c1
            Field::new("name", DataType::Utf8, true),      // #c2
            Field::new("age", DataType::Int64, true),      // #c3
            Field::new("active", DataType::Boolean, true), // #c4
        ]));

        DynamoDBTableSchema::new(
            Arc::from("test_table"),
            schema,
            "id".to_string(),
            Some("sort_key".to_string()),
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
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.table_name, "test_table");

                // Key condition should be: (#c0 = :v1000)
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#c0 = :v1000)".to_string())
                );

                // Should have attribute name for id
                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#c0"), Some(&"id".to_string()));

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
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.table_name, "test_table");

                // Key condition should be: (#c0 = :v1000) AND (#c1 = :v1001)
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#c0 = :v1000) AND (#c1 = :v1001)".to_string())
                );

                // Should have attribute names for id and sort_key
                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#c0"), Some(&"id".to_string()));
                assert_eq!(attr_names.get("#c1"), Some(&"sort_key".to_string()));

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
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.table_name, "test_table");

                // Key condition for partition key: (#c0 = :v1000)
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#c0 = :v1000)".to_string())
                );

                // Filter expression for age: (#c3 > :v0)
                assert_eq!(params.filter_expression, Some("(#c3 > :v0)".to_string()));

                // Should have attribute names for id and age
                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#c0"), Some(&"id".to_string()));
                assert_eq!(attr_names.get("#c3"), Some(&"age".to_string()));

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
            .build_request_plan(&filters, &projection, None)
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
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert_eq!(params.table_name, "test_table");

                // Filter expression: (#c2 = :v0)
                assert_eq!(params.filter_expression, Some("(#c2 = :v0)".to_string()));

                // Should have attribute name for name
                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#c2"), Some(&"name".to_string()));

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
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert_eq!(params.table_name, "test_table");

                // Filter expression with OR: ((#c0 = :v0) OR (#c0 = :v1))
                assert_eq!(
                    params.filter_expression,
                    Some("((#c0 = :v0) OR (#c0 = :v1))".to_string())
                );

                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#c0"), Some(&"id".to_string()));

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
            .build_request_plan(&filters, &projection, Some(10))
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

        let result = builder.build_request_plan(&filters, &projection, Some(i32::MAX as usize + 1));

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
            (col("sort_key").eq(lit("value")), "(#c1 = :v1001)"),
            (col("sort_key").lt(lit("value")), "(#c1 < :v1001)"),
            (col("sort_key").lt_eq(lit("value")), "(#c1 <= :v1001)"),
            (col("sort_key").gt(lit("value")), "(#c1 > :v1001)"),
            (col("sort_key").gt_eq(lit("value")), "(#c1 >= :v1001)"),
        ];

        for (sort_op, expected_sort_condition) in test_cases {
            let filters = vec![col("id").eq(lit("user123")), sort_op];
            let projection = create_projection_schema(&["id", "name"]);

            let result = builder
                .build_request_plan(&filters, &projection, None)
                .expect("request plan");

            match result {
                DynamoDBRequestPlan::Query(params) => {
                    // Key condition should be: (#c0 = :v1000) AND <sort_condition>
                    let expected = format!("(#c0 = :v1000) AND {expected_sort_condition}");
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
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                // Both conditions should be in filter: ((#c0 = :v0) AND (#c0 = :v1))
                assert_eq!(
                    params.filter_expression,
                    Some("(#c0 = :v0) AND (#c0 = :v1)".to_string())
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
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                // All conditions in filter: ((#c0 = :v0) AND ((#c1 > :v1) AND (#c1 < :v2)))
                assert_eq!(
                    params.filter_expression,
                    Some("(#c0 = :v0) AND (#c1 > :v1) AND (#c1 < :v2)".to_string())
                );

                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#c0"), Some(&"id".to_string()));
                assert_eq!(attr_names.get("#c1"), Some(&"sort_key".to_string()));

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
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                // Filter expression: (#c0 > :v0)
                assert_eq!(params.filter_expression, Some("(#c0 > :v0)".to_string()));

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
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.projection_expression, None);
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#c0 = :v1000)".to_string())
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
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        // OR anywhere in the filter tree should force a scan
        match result {
            DynamoDBRequestPlan::Scan(params) => {
                // Complex nested expression with OR
                let filter = params.filter_expression.expect("filter_expression");
                assert!(filter.contains("OR"));
                assert!(filter.contains("#c0")); // id
                assert!(filter.contains("#c3")); // age
                assert!(filter.contains("#c4")); // active
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
        );

        let builder = DynamoDBRequestPlanBuilder::new(table_schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(&["id", "name"]);

        let result = builder
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                // Only partition key condition
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#c0 = :v1000)".to_string())
                );

                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#c0"), Some(&"id".to_string()));
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
            .build_request_plan(&filters, &projection, Some(25))
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert_eq!(params.limit, Some(25));
                assert_eq!(params.filter_expression, Some("(#c2 = :v0)".to_string()));
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
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                // Key condition for partition key
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#c0 = :v1000)".to_string())
                );

                // Filter expression for age and active: ((#c3 > :v0) AND (#c4 = :v1))
                assert_eq!(
                    params.filter_expression,
                    Some("(#c3 > :v0) AND (#c4 = :v1)".to_string())
                );

                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.len(), 4);
                assert_eq!(attr_names.get("#c0"), Some(&"id".to_string()));
                assert_eq!(attr_names.get("#c3"), Some(&"age".to_string()));
                assert_eq!(attr_names.get("#c4"), Some(&"active".to_string()));

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
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                // Filter expression: ((#c2 = :v0) AND (#c3 > :v1))
                assert_eq!(
                    params.filter_expression,
                    Some("(#c2 = :v0) AND (#c3 > :v1)".to_string())
                );

                let attr_names = params
                    .expression_attribute_names
                    .expect("expression_attribute_names");
                assert_eq!(attr_names.get("#c2"), Some(&"name".to_string()));
                assert_eq!(attr_names.get("#c3"), Some(&"age".to_string()));

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
            .build_request_plan(&filters, &projection, None)
            .expect("request plan");

        match result {
            DynamoDBRequestPlan::Query(params) => {
                // Key condition
                assert_eq!(
                    params.key_condition_expression,
                    Some("(#c0 = :v1000)".to_string())
                );

                // Filter expression with not equal: (#c2 <> :v0)
                assert_eq!(params.filter_expression, Some("(#c2 <> :v0)".to_string()));

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
}
