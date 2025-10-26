use crate::dynamodb::table_schema::{DynamoDBTableSchema, IndexInfo};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::operation::scan::builders::ScanFluentBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum DynamoDBRequest {
    Query(QueryFluentBuilder),
    Scan(ScanFluentBuilder),
}

pub struct DynamoDBRequestBuilder<'a> {
    client: &'a Client,
    schema: &'a DynamoDBTableSchema,
}

#[derive(Debug)]
enum KeyFilter {
    Partition(Expr),
    Sort(Expr),
}

#[derive(Debug, Clone)]
pub enum IndexSelection {
    BaseTable,
    GSI(String), // GSI name
}

impl<'a> DynamoDBRequestBuilder<'a> {
    pub fn new(client: &'a Client, schema: &'a DynamoDBTableSchema) -> Self {
        Self { client, schema }
    }

    /// Build a DynamoDB request (Query or Scan) based on filters and projections
    pub fn build(
        &self,
        filters: &[Expr],
        projection_schema: SchemaRef,
        limit: Option<usize>,
    ) -> DataFusionResult<DynamoDBRequest> {
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

        if let Some((partition_expr, sort_expr, index_selection)) = key_filters {
            self.build_query_request(
                &partition_expr,
                sort_expr.as_ref(),
                &other_filters,
                projection_expr.as_deref(),
                attribute_names,
                limit_i32,
                index_selection,
            )
        } else {
            self.build_scan_request(
                filters,
                projection_expr.as_deref(),
                attribute_names,
                limit_i32,
            )
        }
    }

    fn build_query_request(
        &self,
        partition_expr: &Expr,
        sort_expr: Option<&Expr>,
        other_filters: &[Expr],
        projection: Option<&str>,
        attribute_names: HashMap<String, String>,
        limit: Option<i32>,
        index_selection: IndexSelection,
    ) -> DataFusionResult<DynamoDBRequest> {
        println!("!! QUERY !!");

        let mut query_request = self
            .client
            .query()
            .table_name(self.schema.table_name().to_string());

        match index_selection {
            IndexSelection::BaseTable => {
                println!("!! QUERY - Base Table !!");
            }
            IndexSelection::GSI(index_name) => {
                println!("!! QUERY - GSI: {} !!", index_name);
                query_request = query_request.index_name(index_name);
            }
        }

        let (key_condition, mut key_values) = self
            .schema
            .build_key_condition_expression(partition_expr, sort_expr)?;

        println!("Key condition: {}", key_condition);
        query_request = query_request.key_condition_expression(key_condition);

        if !other_filters.is_empty() {
            let (filter_str, filter_values) = self.schema.build_filter_expression(other_filters)?;
            key_values.extend(filter_values);
            println!("filter_expression: {:?}", filter_str);
            query_request = query_request.filter_expression(filter_str);
        }

        if !key_values.is_empty() {
            println!("key_values: {:?}", &key_values);
            query_request = query_request.set_expression_attribute_values(Some(key_values));
        }

        if let Some(proj) = projection {
            println!("projection_expression: {:?}", proj);
            query_request = query_request.projection_expression(proj);
        }

        if !attribute_names.is_empty() {
            println!("attribute_names: {:?}", &attribute_names);
            query_request = query_request.set_expression_attribute_names(Some(attribute_names));
        }

        if let Some(l) = limit {
            query_request = query_request.limit(l);
        }

        Ok(DynamoDBRequest::Query(query_request))
    }

    fn build_scan_request(
        &self,
        filters: &[Expr],
        projection: Option<&str>,
        attribute_names: HashMap<String, String>,
        limit: Option<i32>,
    ) -> DataFusionResult<DynamoDBRequest> {
        println!("!! SCAN !!");

        let mut scan_request = self
            .client
            .scan()
            .table_name(self.schema.table_name().to_string());

        if !filters.is_empty() {
            let (filter_str, attribute_values) = self.schema.build_filter_expression(filters)?;
            if !filter_str.is_empty() {
                println!("filter_expression: {:?}", &filter_str);
                println!("attribute_values: {:?}", &attribute_values);
                scan_request = scan_request.filter_expression(filter_str);
                scan_request = scan_request.set_expression_attribute_values(Some(attribute_values));
            }
        }

        if let Some(proj) = projection {
            println!("projection_expression: {:?}", proj);
            scan_request = scan_request.projection_expression(proj);
        }

        if !attribute_names.is_empty() {
            println!("attribute_names: {:?}", &attribute_names);
            scan_request = scan_request.set_expression_attribute_names(Some(attribute_names));
        }

        if let Some(l) = limit {
            scan_request = scan_request.limit(l);
        }

        Ok(DynamoDBRequest::Scan(scan_request))
    }

    fn separate_key_filters(
        &self,
        filters: &[Expr],
    ) -> (Option<(Expr, Option<Expr>, IndexSelection)>, Vec<Expr>) {
        // Check for OR conditions first - if present, can't use Query
        let has_or = filters.iter().any(|f| self.contains_or(f));
        if has_or {
            return (None, filters.to_vec());
        }

        // Try to match base table keys first
        if let Some((partition, sort, other)) = self.try_match_index(
            filters,
            &self.schema.partition_key,
            self.schema.sort_key.as_deref(),
        ) {
            return (Some((partition, sort, IndexSelection::BaseTable)), other);
        }

        // // Try each GSI
        // for gsi in &self.schema.global_secondary_indexes {
        //     if let Some((partition, sort, other)) =
        //         self.try_match_index(filters, &gsi.partition_key, gsi.sort_key.as_deref())
        //     {
        //         return (
        //             Some((partition, sort, IndexSelection::GSI(gsi.name.clone()))),
        //             other,
        //         );
        //     }
        // }

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use aws_config::BehaviorVersion;
    use aws_sdk_dynamodb::Client;
    use datafusion::logical_expr::{col, lit};
    use std::sync::Arc;

    async fn create_test_client() -> Client {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region("us-east-1")
            .load()
            .await;
        Client::new(&config)
    }

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

    fn create_projection_schema(fields: Vec<&str>) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|&name| Field::new(name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ))
    }

    #[tokio::test]
    async fn test_builder_creation() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        assert!(std::ptr::eq(builder.schema, &schema));
    }

    #[tokio::test]
    async fn test_build_query_with_partition_key_only() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {}
            DynamoDBRequest::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[tokio::test]
    async fn test_build_query_with_partition_and_sort_key() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![
            col("id").eq(lit("user123")),
            col("sort_key").gt(lit("2024-01-01")),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {}
            DynamoDBRequest::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[tokio::test]
    async fn test_build_query_with_additional_filters() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![
            col("id").eq(lit("user123")),
            col("age").gt(lit(18i64)),
            col("active").eq(lit(true)),
        ];
        let projection = create_projection_schema(vec!["id", "name", "age"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {}
            DynamoDBRequest::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[tokio::test]
    async fn test_build_scan_without_partition_key() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![col("age").gt(lit(18i64))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Scan(_) => {}
            DynamoDBRequest::Query(_) => panic!("Expected Scan request"),
        }
    }

    #[tokio::test]
    async fn test_build_scan_with_no_filters() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Scan(_) => {}
            DynamoDBRequest::Query(_) => panic!("Expected Scan request"),
        }
    }

    #[tokio::test]
    async fn test_build_scan_with_or_filter() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![
            col("id")
                .eq(lit("user123"))
                .or(col("id").eq(lit("user456"))),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Scan(_) => {}
            DynamoDBRequest::Query(_) => panic!("Expected Scan request due to OR"),
        }
    }

    #[tokio::test]
    async fn test_build_with_limit() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, Some(10)).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {}
            DynamoDBRequest::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[tokio::test]
    async fn test_build_with_limit_too_large() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, Some(i32::MAX as usize + 1));

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_build_query_all_sort_key_operators() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

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

            let result = builder.build(&filters, projection, None).unwrap();

            match result {
                DynamoDBRequest::Query(_) => {}
                DynamoDBRequest::Scan(_) => panic!("Expected Query request"),
            }
        }
    }

    #[tokio::test]
    async fn test_multiple_partition_keys_forces_scan() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![col("id").eq(lit("user123")), col("id").eq(lit("user456"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Scan(_) => {}
            DynamoDBRequest::Query(_) => panic!("Expected Scan due to multiple partition keys"),
        }
    }

    #[tokio::test]
    async fn test_multiple_sort_keys_forces_scan() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![
            col("id").eq(lit("user123")),
            col("sort_key").gt(lit("2024-01-01")),
            col("sort_key").lt(lit("2024-12-31")),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Scan(_) => {}
            DynamoDBRequest::Query(_) => panic!("Expected Scan due to multiple sort keys"),
        }
    }

    #[tokio::test]
    async fn test_partition_key_with_wrong_operator_forces_scan() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![col("id").gt(lit("user123"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Scan(_) => {}
            DynamoDBRequest::Query(_) => panic!("Expected Scan - partition key must use ="),
        }
    }

    #[tokio::test]
    async fn test_empty_projection() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = Arc::new(Schema::empty());

        let result = builder.build(&filters, projection, None).unwrap();

        // Should still create a query, just without projection
        match result {
            DynamoDBRequest::Query(_) => {}
            DynamoDBRequest::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[tokio::test]
    async fn test_projection_with_unknown_columns() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(vec!["unknown_column"]);

        let result = builder.build(&filters, projection, None).unwrap();

        // Should still work, projection will just be empty
        match result {
            DynamoDBRequest::Query(_) => {}
            DynamoDBRequest::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[tokio::test]
    async fn test_nested_or_in_filter() {
        let client = create_test_client().await;
        let schema = create_test_schema();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![
            col("id").eq(lit("user123")),
            col("age")
                .gt(lit(18i64))
                .and(col("active").eq(lit(true)).or(col("active").eq(lit(false)))),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        // OR anywhere in the filter tree should force a scan
        match result {
            DynamoDBRequest::Scan(_) => {}
            DynamoDBRequest::Query(_) => panic!("Expected Scan due to nested OR"),
        }
    }

    #[tokio::test]
    async fn test_schema_without_sort_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let table_schema = DynamoDBTableSchema::new(
            Arc::from("test_table"),
            schema,
            "id".to_string(),
            None, // No sort key
            Vec::new(),
        );

        let client = create_test_client().await;
        let builder = DynamoDBRequestBuilder::new(&client, &table_schema);

        let filters = vec![col("id").eq(lit("user123"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {}
            DynamoDBRequest::Scan(_) => panic!("Expected Query request"),
        }
    }

    // Helper function to create a schema with GSIs
    fn create_schema_with_gsis() -> DynamoDBTableSchema {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("sort_key", DataType::Utf8, true),
            Field::new("gsi1_pk", DataType::Utf8, true),
            Field::new("gsi1_sk", DataType::Utf8, true),
            Field::new("gsi2_pk", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));

        let gsi1 = IndexInfo {
            name: "GSI1".to_string(),
            partition_key: "gsi1_pk".to_string(),
            sort_key: Some("gsi1_sk".to_string()),
        };

        let gsi2 = IndexInfo {
            name: "GSI2".to_string(),
            partition_key: "gsi2_pk".to_string(),
            sort_key: None,
        };

        DynamoDBTableSchema::new(
            Arc::from("test_table"),
            schema,
            "id".to_string(),
            Some("sort_key".to_string()),
            vec![gsi1, gsi2],
        )
    }

    #[tokio::test]
    async fn test_query_gsi_with_partition_key_only() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        // Query using GSI2 which has no sort key
        let filters = vec![col("gsi2_pk").eq(lit("category123"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {
                // Verify it's querying GSI2
                println!("Should use GSI2 index");
            }
            DynamoDBRequest::Scan(_) => panic!("Expected Query request on GSI2"),
        }
    }

    #[tokio::test]
    async fn test_query_gsi_with_partition_and_sort_keys() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        // Query using GSI1 with both partition and sort keys
        let filters = vec![
            col("gsi1_pk").eq(lit("category123")),
            col("gsi1_sk").gt(lit("2024-01-01")),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {
                println!("Should use GSI1 index with sort key condition");
            }
            DynamoDBRequest::Scan(_) => panic!("Expected Query request on GSI1"),
        }
    }

    #[tokio::test]
    async fn test_base_table_preferred_over_gsi() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        // Filters match both base table and GSI - base table should be preferred
        let filters = vec![
            col("id").eq(lit("user123")),
            col("gsi1_pk").eq(lit("category123")),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {
                println!("Should use base table, not GSI");
            }
            DynamoDBRequest::Scan(_) => panic!("Expected Query request on base table"),
        }
    }

    #[tokio::test]
    async fn test_gsi_used_when_base_table_not_available() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        // Only GSI partition key available, not base table key
        let filters = vec![
            col("gsi1_pk").eq(lit("category123")),
            col("name").eq(lit("John")),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {
                println!("Should use GSI1 index");
            }
            DynamoDBRequest::Scan(_) => panic!("Expected Query request on GSI1"),
        }
    }

    #[tokio::test]
    async fn test_gsi_with_or_condition_forces_scan() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        // OR condition should force scan even if GSI partition key is available
        let filters = vec![
            col("gsi1_pk")
                .eq(lit("category123"))
                .or(col("gsi1_pk").eq(lit("category456"))),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Scan(_) => {
                println!("OR condition forces scan even with GSI");
            }
            DynamoDBRequest::Query(_) => panic!("Expected Scan due to OR condition"),
        }
    }

    #[tokio::test]
    async fn test_gsi_with_wrong_partition_operator_forces_scan() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        // GSI partition key with wrong operator (must be =)
        let filters = vec![col("gsi1_pk").gt(lit("category123"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Scan(_) => {
                println!("Wrong operator on GSI partition key forces scan");
            }
            DynamoDBRequest::Query(_) => panic!("Expected Scan - GSI partition key must use ="),
        }
    }

    #[tokio::test]
    async fn test_multiple_gsis_match_uses_first() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        // Multiple GSI keys available - should use first matching GSI
        let filters = vec![
            col("gsi1_pk").eq(lit("category123")),
            col("gsi2_pk").eq(lit("status_active")),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {
                println!("Should query (likely uses first found GSI or base table)");
            }
            DynamoDBRequest::Scan(_) => panic!("Expected Query request"),
        }
    }

    #[tokio::test]
    async fn test_gsi_with_additional_filters() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        // GSI key plus additional non-key filters
        let filters = vec![
            col("gsi1_pk").eq(lit("category123")),
            col("age").gt(lit(18i64)),
            col("name").eq(lit("John")),
        ];
        let projection = create_projection_schema(vec!["id", "name", "age"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {
                println!("Should query GSI1 with filter expression for additional filters");
            }
            DynamoDBRequest::Scan(_) => panic!("Expected Query request on GSI1"),
        }
    }

    #[tokio::test]
    async fn test_gsi_partial_sort_key_match() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        // GSI1 has sort key but we only provide partition key
        let filters = vec![
            col("gsi1_pk").eq(lit("category123")),
            col("name").eq(lit("John")),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {
                println!("Should query GSI1 with only partition key, name goes to filter");
            }
            DynamoDBRequest::Scan(_) => panic!("Expected Query request on GSI1"),
        }
    }

    #[tokio::test]
    async fn test_gsi_multiple_sort_key_conditions() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        // Multiple conditions on GSI sort key should force scan
        let filters = vec![
            col("gsi1_pk").eq(lit("category123")),
            col("gsi1_sk").gt(lit("2024-01-01")),
            col("gsi1_sk").lt(lit("2024-12-31")),
        ];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Scan(_) => {
                println!("Multiple sort key conditions force scan");
            }
            DynamoDBRequest::Query(_) => {
                panic!("Expected Scan due to multiple sort key conditions")
            }
        }
    }

    #[tokio::test]
    async fn test_gsi_with_all_sort_key_operators() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        // Test all valid sort key operators on GSI
        let sort_operators = vec![
            col("gsi1_sk").eq(lit("value")),
            col("gsi1_sk").lt(lit("value")),
            col("gsi1_sk").lt_eq(lit("value")),
            col("gsi1_sk").gt(lit("value")),
            col("gsi1_sk").gt_eq(lit("value")),
        ];

        for sort_op in sort_operators {
            let filters = vec![col("gsi1_pk").eq(lit("category123")), sort_op];
            let projection = create_projection_schema(vec!["id", "name"]);

            let result = builder.build(&filters, projection.clone(), None).unwrap();

            match result {
                DynamoDBRequest::Query(_) => {
                    println!("Should query GSI1 with sort key operator");
                }
                DynamoDBRequest::Scan(_) => panic!("Expected Query request on GSI1"),
            }
        }
    }

    #[tokio::test]
    async fn test_no_matching_keys_forces_scan() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        // Only non-key attributes - should force scan
        let filters = vec![col("name").eq(lit("John")), col("age").gt(lit(25i64))];
        let projection = create_projection_schema(vec!["id", "name", "age"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Scan(_) => {
                println!("No key attributes forces scan");
            }
            DynamoDBRequest::Query(_) => panic!("Expected Scan when no keys match"),
        }
    }

    #[tokio::test]
    async fn test_gsi_with_limit() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![col("gsi1_pk").eq(lit("category123"))];
        let projection = create_projection_schema(vec!["id", "name"]);

        let result = builder.build(&filters, projection, Some(50)).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {
                println!("Should query GSI1 with limit");
            }
            DynamoDBRequest::Scan(_) => panic!("Expected Query request on GSI1"),
        }
    }

    #[tokio::test]
    async fn test_gsi_with_empty_projection() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        let filters = vec![col("gsi1_pk").eq(lit("category123"))];
        let projection = Arc::new(Schema::empty());

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Query(_) => {
                println!("Should query GSI1 without projection");
            }
            DynamoDBRequest::Scan(_) => panic!("Expected Query request on GSI1"),
        }
    }

    #[tokio::test]
    async fn test_gsi_nested_or_in_filter() {
        let client = create_test_client().await;
        let schema = create_schema_with_gsis();
        let builder = DynamoDBRequestBuilder::new(&client, &schema);

        // GSI key with nested OR in additional filters
        let filters = vec![
            col("gsi1_pk").eq(lit("category123")),
            col("age")
                .gt(lit(18i64))
                .and(col("name").eq(lit("John")).or(col("name").eq(lit("Jane")))),
        ];
        let projection = create_projection_schema(vec!["id", "name", "age"]);

        let result = builder.build(&filters, projection, None).unwrap();

        match result {
            DynamoDBRequest::Scan(_) => {
                println!("Nested OR forces scan even with valid GSI key");
            }
            DynamoDBRequest::Query(_) => panic!("Expected Scan due to nested OR"),
        }
    }
}
