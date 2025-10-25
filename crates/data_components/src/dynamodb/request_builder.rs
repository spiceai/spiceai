use crate::dynamodb::table_schema::DynamoDBTableSchema;
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

        if let Some((partition_expr, sort_expr)) = key_filters {
            self.build_query_request(
                &partition_expr,
                sort_expr.as_ref(),
                &other_filters,
                projection_expr.as_deref(),
                attribute_names,
                limit_i32,
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
    ) -> DataFusionResult<DynamoDBRequest> {
        println!("!! QUERY !!");

        let mut query_request = self
            .client
            .query()
            .table_name(self.schema.table_name().to_string());

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

    fn separate_key_filters(&self, filters: &[Expr]) -> (Option<(Expr, Option<Expr>)>, Vec<Expr>) {
        let mut partition_expr = None;
        let mut sort_expr = None;
        let mut other_filters = Vec::new();
        let mut has_or = false;

        for filter in filters {
            if self.contains_or(filter) {
                has_or = true;
                other_filters.push(filter.clone());
                continue;
            }

            println!("filter: {:?}", filter);
            println!("extracted: {:?}", self.try_extract_key_filter(filter));

            if let Some(extracted) = self.try_extract_key_filter(filter) {
                match extracted {
                    KeyFilter::Partition(expr) => {
                        if partition_expr.is_some() {
                            // Multiple partition key filters - invalid
                            return (None, filters.to_vec());
                        }
                        partition_expr = Some(expr);
                    }
                    KeyFilter::Sort(expr) => {
                        if sort_expr.is_some() {
                            // Multiple sort key filters - invalid
                            return (None, filters.to_vec());
                        }
                        sort_expr = Some(expr);
                    }
                }
            } else {
                other_filters.push(filter.clone());
            }
        }

        if has_or || partition_expr.is_none() {
            return (None, filters.to_vec());
        }

        (Some((partition_expr.unwrap(), sort_expr)), other_filters)
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

    fn try_extract_key_filter(&self, expr: &Expr) -> Option<KeyFilter> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, .. }) => {
                if let Expr::Column(col) = left.as_ref() {
                    if col.name.as_str() == self.schema.partition_key.as_str() {
                        // Partition key must use = operator
                        if matches!(op, Operator::Eq) {
                            return Some(KeyFilter::Partition(expr.clone()));
                        }
                    } else if let Some(ref sort_key) = self.schema.sort_key {
                        if col.name.as_str() == sort_key.as_str() {
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
