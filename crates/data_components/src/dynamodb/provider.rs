/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{any::Any, collections::HashMap, fmt, io::Cursor, sync::Arc};

use super::{
    DescribeTableSnafu, Error, Result, ScanSnafu, TableDoesNotExistSnafu,
    TableStatusIsNotActiveSnafu,
};
use crate::dynamodb::arrow::dynamodb_items_to_arrow;
use crate::dynamodb::schema::infer_arrow_schema_from_items;
use crate::dynamodb::unnest::unnest_dynamodb_items;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use aws_sdk_dynamodb::types::KeyType;
use aws_sdk_dynamodb::{
    Client,
    error::SdkError,
    operation::scan::builders::ScanFluentBuilder,
    types::{AttributeValue, TableStatus},
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::{
    catalog::{Session, TableProvider},
    common::project_schema,
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchReceiverStream,
    },
    prelude::Expr,
};
use futures::stream::{StreamExt, TryStreamExt};
use snafu::prelude::*;
// use crate::dynamodb::expression::{combine_exprs_with_and, expr_to_dynamodb_filter};

use datafusion::logical_expr::{BinaryExpr, Operator, TableProviderFilterPushDown};
use datafusion::scalar::ScalarValue;

fn scalar_to_attribute_value(scalar: &ScalarValue) -> datafusion::error::Result<AttributeValue> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => Ok(AttributeValue::S(s.clone())),
        ScalarValue::Int64(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::Int32(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::Float64(Some(f)) => Ok(AttributeValue::N(f.to_string())),
        ScalarValue::Float32(Some(f)) => Ok(AttributeValue::N(f.to_string())),
        ScalarValue::Boolean(Some(b)) => Ok(AttributeValue::Bool(*b)),
        ScalarValue::Null => Ok(AttributeValue::Null(true)),
        _ => Err(DataFusionError::NotImplemented(format!(
            "ScalarValue type not supported"
        ))),
    }
}

#[derive(Debug)]
pub struct DynamoDBTableProvider {
    client: Arc<Client>,
    table_name: Arc<str>,
    table_schema: SchemaRef,
    partition_key: String,
    sort_key: Option<String>,
    column_to_alias_map: HashMap<String, String>, // #c0 -> actual_name
    alias_to_column_map: HashMap<String, String>, // actual_name -> #c0
}

impl DynamoDBTableProvider {
    pub async fn try_new(client: Arc<Client>, table_name: Arc<str>) -> Result<Self, Error> {
        let (table_schema, partition_key, sort_key) =
            Self::schema(Arc::clone(&client), &table_name).await?;

        let (column_to_alias_map, alias_to_column_map) = build_column_alias_maps(&table_schema);

        Ok(Self {
            client,
            table_name,
            table_schema,
            partition_key,
            sort_key,
            column_to_alias_map,
            alias_to_column_map,
        })
    }

    pub async fn schema(
        client: Arc<Client>,
        table_name: &str,
    ) -> Result<(SchemaRef, String, Option<String>)> {
        let response = client
            .describe_table()
            .table_name(table_name)
            .send()
            .await
            .map_err(map_sdk_error)
            .context(DescribeTableSnafu)?;

        let Some(table) = response.table() else {
            return TableDoesNotExistSnafu { table_name }.fail();
        };

        let Some(table_status) = table.table_status() else {
            return TableDoesNotExistSnafu { table_name }.fail();
        };
        if *table_status != TableStatus::Active {
            return TableStatusIsNotActiveSnafu.fail();
        }

        let key_schema = table.key_schema();

        let mut partition_key = None;
        let mut sort_key = None;

        for key in key_schema {
            match key.key_type() {
                KeyType::Hash => {
                    partition_key = Some(key.attribute_name().to_string());
                }
                KeyType::Range => {
                    sort_key = Some(key.attribute_name().to_string());
                }
                _ => {}
            }
        }

        let Some(partition_key) = partition_key else {
            unreachable!("Table must have a partition key")
        };

        let mut request = client.scan().table_name(table_name);

        if let Some(limit) = Some(10) {
            request = request.limit(limit);
        }

        let items: Vec<_> = request
            .send()
            .await
            .map_err(map_sdk_error)
            .context(ScanSnafu)?
            .items()
            .to_vec();

        let unnested_items = match Some(1) {
            None | Some(0) => items,
            Some(unnest_depth) => unnest_dynamodb_items(items, unnest_depth)?,
        };

        Ok((
            infer_arrow_schema_from_items(&unnested_items)?,
            partition_key,
            sort_key,
        ))
    }

    pub async fn schema_old(client: Arc<Client>, table_name: &str) -> Result<SchemaRef> {
        let mut request = client.scan().table_name(table_name);

        // TODO
        if let Some(limit) = Some(10) {
            request = request.limit(limit);
        }

        let items: Vec<_> = request
            .send()
            .await
            .map_err(map_sdk_error)
            .context(ScanSnafu)?
            .items()
            .to_vec();

        let unnested_items = match Some(1) {
            None | Some(0) => items,
            Some(unnest_depth) => unnest_dynamodb_items(items, unnest_depth)?,
        };

        infer_arrow_schema_from_items(&unnested_items)
    }

    fn build_filter_expression(
        &self,
        filters: &[Expr],
    ) -> datafusion::error::Result<(String, HashMap<String, AttributeValue>)> {
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

    fn expr_to_filter_string(
        &self,
        expr: &Expr,
        attribute_values: &mut HashMap<String, AttributeValue>,
        value_counter: &mut usize,
    ) -> datafusion::error::Result<String> {
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
            _ => Err(DataFusionError::NotImplemented(format!(
                "Expression type not supported in filters"
            ))),
        }
    }

    fn add_filter_column_aliases(
        &self,
        filters: &[Expr],
        attribute_names: &mut HashMap<String, String>,
    ) {
        for expr in filters {
            self.extract_columns_from_expr(expr, attribute_names);
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

    fn is_filter_supported(&self, expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                // Check if operator is supported
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
                // Check if column exists in schema
                self.column_to_alias_map.contains_key(col.name.as_str())
            }
            Expr::Literal(scalar, _) => {
                // Check if literal type is supported
                matches!(
                    scalar,
                    ScalarValue::Utf8(_)
                        | ScalarValue::Int64(_)
                        | ScalarValue::Int32(_)
                        | ScalarValue::Float64(_)
                        | ScalarValue::Float32(_)
                        | ScalarValue::Boolean(_)
                        | ScalarValue::Null
                )
            }
            _ => false,
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

/// Creates a projection expression for a `DynamoDB` scan request based on the provided schema and projection indices.
/// Because projection expressions may use reserved words in `DynamoDB`, this function automatically generates expression attribute names for each column to avoid conflicts.
/// The expression format used is `#c{idx}` for each projected column, where `{idx}` is the column projection index.
/// See: <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html#Expressions.ExpressionAttributeNames.ReservedWords>
/// Returns a tuple of (`projection_expression`, `expression_attribute_names`) for the `DynamoDB` scan.
/// Returns None if no projection is required.
fn projection_expression(
    projection: Option<&Vec<usize>>,
    schema: &SchemaRef,
) -> Option<(String, HashMap<String, String>)> {
    let projection = projection?;
    if projection.is_empty() {
        return None;
    }

    // For each projected field, generate a placeholder and mapping
    let mut expr_parts = Vec::with_capacity(projection.len());
    let mut attr_names = HashMap::with_capacity(projection.len());

    for (i, &idx) in projection.iter().enumerate() {
        let field = schema.field(idx);
        let name = field.name();
        let placeholder = format!("#c{i}");
        expr_parts.push(placeholder.clone());
        attr_names.insert(placeholder, name.clone());
    }

    let expr = expr_parts.join(", ");
    if expr.is_empty() {
        None
    } else {
        Some((expr, attr_names))
    }
}

#[async_trait]
impl TableProvider for DynamoDBTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut request = self.client.scan().table_name(self.table_name.to_string());

        // Build projection expression using aliases
        let projected_schema = project_schema(&self.table_schema, projection)?;
        let projection_expr: Vec<_> = projected_schema
            .fields()
            .iter()
            .filter_map(|f| self.column_to_alias_map.get(f.name()))
            .cloned()
            .collect();

        if !projection_expr.is_empty() {
            println!("projection_expr: {:?}", projection_expr);
            request = request.projection_expression(projection_expr.join(", "));
        }

        // Build filter expression (you need to implement this)
        let (filter_str, attribute_values) = self.build_filter_expression(filters)?;
        if !filter_str.is_empty() {
            println!("filter_str: {:?}", filter_str);
            println!("attribute_values: {:?}", attribute_values);
            request = request.filter_expression(filter_str);
            request = request.set_expression_attribute_values(Some(attribute_values));
        }

        // Collect aliases used in projection and filters
        let mut attribute_names = HashMap::new();
        for field in projected_schema.fields() {
            if let Some(alias) = self.column_to_alias_map.get(field.name()) {
                attribute_names.insert(alias.clone(), field.name().clone());
            }
        }
        self.add_filter_column_aliases(filters, &mut attribute_names);

        if !attribute_names.is_empty() {
            println!("attribute_names: {:?}", attribute_names);
            request = request.set_expression_attribute_names(Some(attribute_names));
        }

        if let Some(limit) = limit {
            request = request.limit(
                i32::try_from(limit)
                    .map_err(|_| DataFusionError::Execution("Limit too large".to_string()))?,
            );
        }

        Ok(Arc::new(DynamoDBTableProviderExec::new(
            request,
            projected_schema,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let support: Vec<_> = filters
            .iter()
            .map(|expr| {
                if self.is_filter_supported(expr) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();

        Ok(support)
    }
}

pub struct DynamoDBTableProviderExec {
    request: ScanFluentBuilder,
    table_schema: SchemaRef,
    properties: PlanProperties,
}

impl DynamoDBTableProviderExec {
    #[must_use]
    pub fn new(request: ScanFluentBuilder, table_schema: SchemaRef) -> Self {
        Self {
            request,
            table_schema: Arc::clone(&table_schema),
            properties: PlanProperties::new(
                EquivalenceProperties::new(table_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }
    }
}

impl std::fmt::Debug for DynamoDBTableProviderExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DynamoDBTableProviderExec")
    }
}

impl DisplayAs for DynamoDBTableProviderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DynamoDBTableProviderExec")
    }
}

impl ExecutionPlan for DynamoDBTableProviderExec {
    fn name(&self) -> &'static str {
        "DynamoDBTableProviderExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    // fn execute_old(
    //     &self,
    //     _partition: usize,
    //     _context: Arc<TaskContext>,
    // ) -> DataFusionResult<SendableRecordBatchStream> {
    //     let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&self.table_schema), 2);
    //     let tx = builder.tx();
    //
    //     let schema = Arc::clone(&self.table_schema);
    //     let request = self.request.clone().into_paginator();
    //
    //     builder.spawn(async move {
    //         let mut stream = request.send();
    //
    //         while let Some(item) = stream.next().await {
    //             let scan_output =
    //                 item.map_err(|e| DataFusionError::Execution(map_sdk_error(e).to_string()))?;
    //             for scan_item in scan_output.items() {
    //                 let json_value = attribute_map_to_json(scan_item).to_string();
    //                 let batches = ReaderBuilder::new(Arc::clone(&schema))
    //                     .with_batch_size(1024)
    //                     .build(Cursor::new(json_value.as_bytes()))
    //                     .map_err(|e| DataFusionError::Execution(e.to_string()))?
    //                     .collect::<Result<Vec<_>, _>>()
    //                     .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    //
    //                 for batch in batches {
    //                     tx.send(Ok(batch)).await.map_err(|_| {
    //                         DataFusionError::Execution("Failed to send record batch".to_string())
    //                     })?;
    //                 }
    //             }
    //         }
    //
    //         Ok(())
    //     });
    //
    //     Ok(builder.build())
    // }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&self.table_schema), 2);
        let tx = builder.tx();

        let schema = Arc::clone(&self.table_schema);
        let request = self.request.clone().into_paginator();
        let unnest_depth = Some(1);

        builder.spawn(async move {
            let mut paginator = request.send();
            let mut buffer = Vec::new();
            const CHUNK_SIZE: usize = 4_000;

            while let Some(result) = paginator.next().await {
                let scan_output = result.map_err(to_execution_error)?;

                buffer.extend(scan_output.items().to_vec());

                while buffer.len() >= CHUNK_SIZE {
                    let chunk: Vec<_> = buffer.drain(..CHUNK_SIZE).collect();

                    let unnested_items = match unnest_depth {
                        None | Some(0) => chunk,
                        Some(unnest_depth) => unnest_dynamodb_items(chunk, unnest_depth)
                            .map_err(to_execution_error)?,
                    };

                    let batch = dynamodb_items_to_arrow(&unnested_items, Arc::clone(&schema))
                        .map_err(to_execution_error)?;

                    tx.send(Ok(batch)).await.map_err(to_execution_error)?;
                }
            }

            // Process remaining items in buffer
            if !buffer.is_empty() {
                let unnested_items = match unnest_depth {
                    None | Some(0) => buffer,
                    Some(unnest_depth) => {
                        unnest_dynamodb_items(buffer, unnest_depth).map_err(to_execution_error)?
                    }
                };

                let batch = dynamodb_items_to_arrow(&unnested_items, Arc::clone(&schema))
                    .map_err(to_execution_error)?;

                tx.send(Ok(batch)).await.map_err(to_execution_error)?;
            }

            Ok(())
        });

        Ok(builder.build())
    }
}

#[allow(clippy::needless_pass_by_value)]
pub fn to_execution_error(
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}

fn map_sdk_error<E>(err: SdkError<E>) -> Box<dyn std::error::Error + Send + Sync>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let source = match err.into_source() {
        Ok(source) => source,
        Err(err) => {
            // If there is no error source, then original instance of SdkError is returned
            return err.into();
        }
    };

    if let Some(err) = source.downcast_ref::<aws_sdk_dynamodb::operation::scan::ScanError>() {
        // Error metadata message (if present) contains a specific error message
        if let Some(err_msg) = err.meta().message() {
            return err_msg.into();
        }
    }

    if let Some(err) =
        source.downcast_ref::<aws_sdk_dynamodb::operation::describe_table::DescribeTableError>()
    {
        // Error metadata message (if present) contains a specific error message
        if let Some(err_msg) = err.meta().message() {
            return err_msg.into();
        }
    }

    // If a connection error occurs, provide detailed information available via Debug format.
    // This happens when the request failed during dispatch. An HTTP response was not received, thus no error code or message is available.
    if let Some(conn_error) = source.downcast_ref::<aws_sdk_dynamodb::error::ConnectorError>() {
        return format!(
            "Connection error. This may indicate an invalid region setting, connectivity, or access issue. Details: {conn_error:?}"
        ).into();
    }

    source
}
