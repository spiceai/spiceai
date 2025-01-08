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

use arrow::{
    datatypes::{Field, SchemaRef},
    json::{reader::infer_json_schema_from_iterator, ReaderBuilder},
};
use async_trait::async_trait;
use aws_sdk_dynamodb::{
    operation::{
        describe_table::DescribeTableError,
        scan::{builders::ScanFluentBuilder, ScanError},
    },
    types::{AttributeValue, TableStatus},
    Client,
};
use datafusion::{
    catalog::{Session, TableProvider},
    common::project_schema,
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchReceiverStream, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties,
    },
    prelude::Expr,
};
use itertools::Itertools;
use serde_json::Value;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    DescribeTableError {
        source: aws_sdk_dynamodb::error::SdkError<DescribeTableError>,
    },

    #[snafu(display("{source}"))]
    ScanError {
        source: aws_sdk_dynamodb::error::SdkError<ScanError>,
    },

    #[snafu(display("Table does not exist: {table_name}"))]
    TableDoesNotExist { table_name: Arc<str> },

    #[snafu(display("Table status is not active"))]
    TableStatusIsNotActive,

    #[snafu(display("Failed to infer schema: {source}"))]
    SchemaInferenceError { source: arrow::error::ArrowError },
}

#[derive(Debug)]
pub struct DynamoDBTableProvider {
    client: Arc<Client>,
    table_name: Arc<str>,
    table_schema: SchemaRef,
}

impl DynamoDBTableProvider {
    pub async fn try_new(client: Arc<Client>, table_name: Arc<str>) -> Result<Self, Error> {
        let status = Self::get_table_status(Arc::clone(&client), Arc::clone(&table_name)).await?;
        if status != TableStatus::Active {
            return TableStatusIsNotActiveSnafu.fail();
        }
        let table_schema = Self::schema(Arc::clone(&client), &table_name).await?;
        Ok(Self {
            client,
            table_name,
            table_schema,
        })
    }

    async fn get_table_status(
        client: Arc<Client>,
        table_name: Arc<str>,
    ) -> Result<TableStatus, Error> {
        let response = client
            .describe_table()
            .table_name(table_name.to_string())
            .send()
            .await
            .context(DescribeTableSnafu)?;

        let Some(table) = response.table() else {
            return TableDoesNotExistSnafu {
                table_name: Arc::clone(&table_name),
            }
            .fail();
        };
        let Some(table_status) = table.table_status() else {
            return TableDoesNotExistSnafu {
                table_name: Arc::clone(&table_name),
            }
            .fail();
        };
        Ok(table_status.clone())
    }

    async fn scan(
        client: Arc<Client>,
        table_name: &str,
        limit: Option<i32>,
    ) -> Result<Vec<Value>, Error> {
        let mut request = client.scan().table_name(table_name);
        if let Some(limit) = limit {
            request = request.limit(limit);
        }

        let response = request.send().await.context(ScanSnafu)?;

        let mut result = Vec::new();
        for item in response.items() {
            result.push(attribute_map_to_json(item));
        }
        Ok(result)
    }

    pub async fn schema(client: Arc<Client>, table_name: &str) -> Result<SchemaRef, Error> {
        let json_values = Self::scan(client, table_name, Some(10)).await?;
        infer_schema(&json_values)
    }
}

fn infer_schema(json_values: &[Value]) -> Result<SchemaRef, Error> {
    let schema = infer_json_schema_from_iterator(json_values.iter().map(Result::Ok))
        .context(SchemaInferenceSnafu)?;

    Ok(Arc::new(schema))
}

fn attribute_map_to_json(map: &HashMap<String, AttributeValue>) -> Value {
    Value::Object(
        map.iter()
            .map(|(k, v)| (k.clone(), attribute_value_to_json(v)))
            .collect(),
    )
}

fn attribute_value_to_json(av: &AttributeValue) -> Value {
    match av {
        AttributeValue::S(s) => Value::String(s.clone()),
        AttributeValue::N(n) => {
            // DynamoDB numbers are strings, so we need to parse them
            if let Ok(i) = n.parse::<i64>() {
                Value::Number(i.into())
            } else if let Ok(f) = n.parse::<f64>() {
                // Need to check if it's a valid JSON number
                serde_json::Number::from_f64(f)
                    .map(Value::Number)
                    .unwrap_or(Value::String(n.clone()))
            } else {
                Value::String(n.clone())
            }
        }
        AttributeValue::Bool(b) => Value::Bool(*b),
        AttributeValue::L(list) => Value::Array(list.iter().map(attribute_value_to_json).collect()),
        AttributeValue::M(map) => attribute_map_to_json(map),
        AttributeValue::Null(_) | _ => Value::Null,
    }
}

/// Create a projection expression for a `DynamoDB` scan request based on the provided schema and projection indices.
fn projection_expression(projection: Option<&Vec<usize>>, schema: &SchemaRef) -> Option<String> {
    // If no projection is provided, return None to get all attributes
    let projection = projection?;

    // If projection is empty, return None
    if projection.is_empty() {
        return None;
    }

    // Create the comma-separated list of attribute names
    let expr = projection
        .iter()
        .map(|&idx| schema.field(idx))
        .map(Field::name)
        .join(", ");

    // If we couldn't find any valid field names, return None
    if expr.is_empty() {
        None
    } else {
        Some(expr)
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
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut request = self.client.scan().table_name(self.table_name.to_string());
        if let Some(limit) = limit {
            request = request.limit(
                i32::try_from(limit)
                    .map_err(|_| DataFusionError::Execution("Limit is too large".to_string()))?,
            );
        }
        if let Some(projection) = projection_expression(projection, &self.table_schema) {
            request = request.projection_expression(projection);
        }
        let projected_schema = project_schema(&self.table_schema, projection)?;
        Ok(Arc::new(DynamoDBTableProviderExec::new(
            request,
            projected_schema,
        )))
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
                ExecutionMode::Bounded,
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

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&self.table_schema), 2);
        let tx = builder.tx();

        let schema = Arc::clone(&self.table_schema);
        let request = self.request.clone().into_paginator();

        builder.spawn(async move {
            let mut stream = request.send();

            while let Some(item) = stream.next().await {
                let scan_output = item.map_err(|e| DataFusionError::Execution(e.to_string()))?;
                for scan_item in scan_output.items() {
                    let json_value = attribute_map_to_json(scan_item).to_string();
                    let batches = ReaderBuilder::new(Arc::clone(&schema))
                        .with_batch_size(1024)
                        .build(Cursor::new(json_value.as_bytes()))
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                    for batch in batches {
                        tx.send(Ok(batch)).await.map_err(|_| {
                            DataFusionError::Execution("Failed to send record batch".to_string())
                        })?;
                    }
                }
            }

            Ok(())
        });

        Ok(builder.build())
    }
}
