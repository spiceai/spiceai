use crate::dynamodb::table_schema::DynamoDBTableSchema;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::operation::scan::builders::ScanFluentBuilder;
use aws_sdk_dynamodb::types::AttributeValue;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use derive_builder::Builder;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum DynamoDBRequest {
    Query(QueryFluentBuilder),
    Scan(ScanFluentBuilder),
}

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

pub struct DynamoDBRequestBuilder<'a> {
    schema: &'a DynamoDBTableSchema,
}

#[derive(Debug)]
enum KeyFilter {
    Partition(Expr),
    Sort(Expr),
}

impl<'a> DynamoDBRequestBuilder<'a> {
    pub fn new(schema: &'a DynamoDBTableSchema) -> Self {
        Self { schema }
    }

    /// Build a DynamoDB request (Query or Scan) based on filters and projections
    pub fn plan(
        &self,
        filters: &[Expr],
        projection_schema: SchemaRef,
        limit: Option<usize>,
    ) -> DataFusionResult<DynamoDBRequestPlan> {
        // Separate key filters from other filters
        let (key_filters, other_filters) = self.separate_key_filters(filters);
        println!("key_filters: {:?}", key_filters);
        println!("other_filters: {:?}", other_filters);

        let mut attribute_names = self.schema.extract_attribute_names(filters);
        self.add_projection_aliases(projection_schema.clone(), &mut attribute_names);

        let projection_expr = self.build_projection_expression(projection_schema);

        let limit_i32 = limit
            .map(|l| {
                i32::try_from(l)
                    .map_err(|_| DataFusionError::Execution("Limit too large".to_string()))
            })
            .transpose()?;

        if let Some((partition_expr, sort_expr)) = key_filters {
            self.plan_query_request(
                &partition_expr,
                sort_expr.as_ref(),
                &other_filters,
                projection_expr,
                attribute_names,
                limit_i32,
            )
        } else {
            self.plan_scan_request(filters, projection_expr, attribute_names, limit_i32)
        }
    }

    fn plan_query_request(
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

        println!("Key condition: {}", key_condition);
        // query_request = query_request.key_condition_expression(key_condition.clone());
        query_params = query_params.key_condition_expression(key_condition);

        if !other_filters.is_empty() {
            let (filter_str, filter_values) = self.schema.build_filter_expression(other_filters)?;
            key_values.extend(filter_values);
            println!("filter_expression: {:?}", filter_str);
            // query_request = query_request.filter_expression(filter_str.clone());
            query_params = query_params.filter_expression(filter_str);
        }

        if !key_values.is_empty() {
            println!("key_values: {:?}", &key_values);
            // query_request = query_request.set_expression_attribute_values(Some(key_values.clone()));
            query_params = query_params.expression_attribute_values(key_values);
        }

        if let Some(proj) = projection {
            println!("projection_expression: {:?}", proj);
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

    fn plan_scan_request(
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
            let (filter_str, attribute_values) = self.schema.build_filter_expression(filters)?;
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
            println!("projection_expression: {:?}", proj);
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
        let has_or = filters.iter().any(|f| self.contains_or(f));
        if has_or {
            return (None, filters.to_vec());
        }

        if let Some((partition, sort, other)) = self.try_match_index(
            filters,
            &self.schema.partition_key,
            self.schema.sort_key.as_deref(),
        ) {
            return (Some((partition, sort)), other);
        }

        // No matching index found - must use Scan
        (None, filters.to_vec())
    }

    /// Attempts to match filters against a specific index (base table or GSI)
    fn try_match_index(
        &self,
        filters: &[Expr],
        partition_key: &str,
        sort_key: Option<&str>,
    ) -> Option<(Expr, Option<Expr>, Vec<Expr>)> {
        let mut partition_expr = None;
        let mut sort_expr = None;
        let mut other_filters = Vec::new();

        for filter in filters {
            if let Some(extracted) = self.try_extract_key_filter(filter, partition_key, sort_key) {
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

    fn build_projection_expression(&self, projection: SchemaRef) -> Option<String> {
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
        projection: SchemaRef,
        attribute_names: &mut HashMap<String, String>,
    ) {
        for field in projection.fields.iter() {
            if let Some(alias) = self.schema.get_column_alias(field.name()) {
                attribute_names.insert(alias.to_string(), field.name().to_string());
            }
        }
    }

    fn contains_or(&self, expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                matches!(op, Operator::Or) || self.contains_or(left) || self.contains_or(right)
            }
            _ => false,
        }
    }

    /// Extracts key filter if the expression matches the specified partition or sort key
    fn try_extract_key_filter(
        &self,
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
                    } else if let Some(sk) = sort_key {
                        if col.name.as_str() == sk {
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
                }
                None
            }
            _ => None,
        }
    }
}

#[deny(unused_variables)]
pub fn build_request_from_plan(client: &Client, request: DynamoDBRequestPlan) -> DynamoDBRequest {
    match request {
        DynamoDBRequestPlan::Query(QueryParams {
            table_name,
            key_condition_expression,
            filter_expression,
            expression_attribute_values,
            expression_attribute_names,
            projection_expression,
            limit,
        }) => {
            DynamoDBRequest::Query(client.query().table_name(table_name)
                .set_key_condition_expression(key_condition_expression)
                .set_filter_expression(filter_expression)
                .set_expression_attribute_values(expression_attribute_values)
                .set_expression_attribute_names(expression_attribute_names)
                .set_projection_expression(projection_expression)
                .set_limit(limit))
        }

        DynamoDBRequestPlan::Scan(ScanParams {
            table_name,
            filter_expression,
            expression_attribute_values,
            expression_attribute_names,
            projection_expression,
            limit,
        }) => {
            DynamoDBRequest::Scan(client.scan()
                .table_name(table_name)
                .set_filter_expression(filter_expression)
                .set_expression_attribute_values(expression_attribute_values)
                .set_expression_attribute_names(expression_attribute_names)
                .set_projection_expression(projection_expression)
                .set_limit(limit))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit};
    use std::sync::Arc;

    fn create_test_schema() -> DynamoDBTableSchema {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("sort_key", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("active", DataType::Boolean, true),
        ]));

        DynamoDBTableSchema::new(
            Arc::from("test_table"),
            schema,
            "id".to_string(),
            Some("sort_key".to_string()),
        )
    }

    fn create_projection_schema(fields: Vec<&str>) -> Arc<Schema> {
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
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.table_name, "test_table");
                assert!(params.key_condition_expression.is_some());
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_query_with_partition_and_sort_key() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![
            col("id").eq(lit("user123")),
            col("sort_key").eq(lit("2024-01-01")),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.table_name, "test_table");
                assert!(params.key_condition_expression.is_some());
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_query_with_filter_expression() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![col("id").eq(lit("user123")), col("age").gt(lit(18i64))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.table_name, "test_table");
                assert!(params.key_condition_expression.is_some());
                assert!(params.filter_expression.is_some());
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_scan_no_filters() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert_eq!(params.table_name, "test_table");
                assert!(params.filter_expression.is_none());
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request"),
        }
    }

    #[test]
    fn test_plan_scan_with_filter_no_partition_key() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![col("name").eq(lit("John"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert_eq!(params.table_name, "test_table");
                assert!(params.filter_expression.is_some());
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request"),
        }
    }

    #[test]
    fn test_plan_scan_with_or_filter() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![
            col("id")
                .eq(lit("user123"))
                .or(col("id").eq(lit("user456"))),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequestPlan::Scan(_) => {}
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request due to OR"),
        }
    }

    #[test]
    fn test_plan_with_limit() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, Some(10)).unwrap();

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert_eq!(params.limit, Some(10));
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_with_limit_too_large() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, Some(i32::MAX as usize + 1));

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Limit too large"));
    }

    #[test]
    fn test_plan_query_all_sort_key_operators() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let sort_operators = vec![
            col("sort_key").eq(lit("value")),
            col("sort_key").lt(lit("value")),
            col("sort_key").lt_eq(lit("value")),
            col("sort_key").gt(lit("value")),
            col("sort_key").gt_eq(lit("value")),
        ];

        for sort_op in sort_operators {
            let filters = vec![col("id").eq(lit("user123")), sort_op];
            let projection = create_projection_schema(vec!["id", "name"]);

            let result = builder.plan(&filters, projection, None).unwrap();

            match result {
                DynamoDBRequestPlan::Query(_) => {}
                DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
            }
        }
    }

    #[test]
    fn test_multiple_partition_keys_forces_scan() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![col("id").eq(lit("user123")), col("id").eq(lit("user456"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequestPlan::Scan(_) => {}
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan due to multiple partition keys"),
        }
    }

    #[test]
    fn test_multiple_sort_keys_forces_scan() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![
            col("id").eq(lit("user123")),
            col("sort_key").gt(lit("2024-01-01")),
            col("sort_key").lt(lit("2024-12-31")),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequestPlan::Scan(_) => {}
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan due to multiple sort keys"),
        }
    }

    #[test]
    fn test_partition_key_with_wrong_operator_forces_scan() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![col("id").gt(lit("user123"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequestPlan::Scan(_) => {}
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan - partition key must use ="),
        }
    }

    #[test]
    fn test_empty_projection() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = Arc::new(Schema::empty());

        let result = builder.plan(&filters, projection, None).unwrap();

        // Should still create a query, just without projection
        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert!(params.projection_expression.is_none());
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_projection_with_unknown_columns() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(vec!["unknown_column"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        // Should still work, projection will just be empty
        match result {
            DynamoDBRequestPlan::Query(_) => {}
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_nested_or_in_filter() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![
            col("id").eq(lit("user123")),
            col("age")
                .gt(lit(18i64))
                .and(col("active").eq(lit(true)).or(col("active").eq(lit(false)))),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        // OR anywhere in the filter tree should force a scan
        match result {
            DynamoDBRequestPlan::Scan(_) => {}
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan due to nested OR"),
        }
    }

    #[test]
    fn test_schema_without_sort_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let table_schema = DynamoDBTableSchema::new(
            Arc::from("test_table"),
            schema,
            "id".to_string(),
            None, // No sort key
        );

        let builder = DynamoDBRequestBuilder::new(&table_schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequestPlan::Query(_) => {}
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_with_projection_expression() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(vec!["id", "name", "age"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert!(params.projection_expression.is_some());
                let proj_expr = params.projection_expression.unwrap();
                // Projection should contain the requested fields
                assert!(proj_expr.contains("id") || proj_expr.contains("#"));
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_scan_with_limit() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![col("name").eq(lit("John"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, Some(25)).unwrap();

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert_eq!(params.limit, Some(25));
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request"),
        }
    }

    #[test]
    fn test_plan_query_with_expression_attribute_values() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![col("id").eq(lit("user123")), col("age").gt(lit(18i64))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequestPlan::Query(params) => {
                assert!(params.expression_attribute_values.is_some());
                let values = params.expression_attribute_values.unwrap();
                assert!(!values.is_empty());
            }
            DynamoDBRequestPlan::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[test]
    fn test_plan_scan_with_expression_attribute_values() {
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&schema);

        let filters = vec![col("name").eq(lit("John")), col("age").gt(lit(25i64))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.plan(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequestPlan::Scan(params) => {
                assert!(params.expression_attribute_values.is_some());
                let values = params.expression_attribute_values.unwrap();
                assert!(!values.is_empty());
            }
            DynamoDBRequestPlan::Query(_) => panic!("Expected Scan request"),
        }
    }
}