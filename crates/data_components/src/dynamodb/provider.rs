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

use std::{any::Any, collections::HashMap, fmt, sync::Arc};

use super::{
    DescribeTableSnafu, Error, Result, ScanSnafu, TableDoesNotExistSnafu,
    TableStatusIsNotActiveSnafu,
};
use crate::dynamodb::arrow::dynamodb_items_to_arrow;
use crate::dynamodb::schema::infer_arrow_schema_from_items;
use crate::dynamodb::unnest::unnest_dynamodb_items;
use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use aws_sdk_dynamodb::types::KeyType;
use aws_sdk_dynamodb::{
    Client,
    error::SdkError,
    types::{AttributeValue, TableStatus},
};
use aws_smithy_async::future::pagination_stream::{PaginationStream, TryFlatMap};
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
use snafu::prelude::*;

use crate::dynamodb::request_builder::{DynamoDBRequest, DynamoDBRequestBuilder};
use crate::dynamodb::table_schema::DynamoDBTableSchema;
use datafusion::logical_expr::TableProviderFilterPushDown;
use futures::Stream;
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub struct DynamoDBTableProvider {
    client: Arc<Client>,
    table_schema: DynamoDBTableSchema,
}

impl DynamoDBTableProvider {
    pub async fn try_new(client: Arc<Client>, table_name: Arc<str>) -> Result<Self, Error> {
        let (table_schema, partition_key, sort_key) =
            Self::fetch_table_metadata(Arc::clone(&client), &table_name).await?;

        Ok(Self {
            client,
            table_schema: DynamoDBTableSchema::new(
                table_name,
                table_schema,
                partition_key,
                sort_key,
            ),
        })
    }

    async fn fetch_table_metadata(
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
}

#[async_trait]
impl TableProvider for DynamoDBTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema.schema())
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(self.table_schema.schema(), projection)?;

        let builder = DynamoDBRequestBuilder::new(&self.client, &self.table_schema);
        let request = builder.build(filters, projected_schema.clone(), limit)?;

        Ok(Arc::new(DynamoDBTableProviderExec::new(
            request,
            projected_schema,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        self.table_schema.supports_filters_pushdown(filters)
    }
}

pub struct DynamoDBTableProviderExec {
    request: DynamoDBRequest,
    projected_schema: SchemaRef,
    properties: PlanProperties,
}

impl DynamoDBTableProviderExec {
    #[must_use]
    pub fn new(request: DynamoDBRequest, projected_schema: SchemaRef) -> Self {
        Self {
            request,
            projected_schema: Arc::clone(&projected_schema),
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
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
        Arc::clone(&self.projected_schema)
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
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&self.projected_schema), 2);
        let tx = builder.tx();

        let schema = Arc::clone(&self.projected_schema);
        let request = self.request.clone();
        let unnest_depth = Some(1);

        builder.spawn(async move {
            match request {
                DynamoDBRequest::Query(query) => {
                    let item_stream = TryFlatMap::new(query.into_paginator().send())
                        .flat_map(|output| output.items().to_vec());
                    process_item_stream(item_stream, tx, schema, unnest_depth).await
                }
                DynamoDBRequest::Scan(scan) => {
                    let item_stream = TryFlatMap::new(scan.into_paginator().send())
                        .flat_map(|output| output.items().to_vec());
                    process_item_stream(item_stream, tx, schema, unnest_depth).await
                }
            }
        });

        Ok(builder.build())
    }
}

use futures::pin_mut;
use futures::stream::{self, StreamExt};

async fn process_item_stream<E>(
    pagination_stream: PaginationStream<Result<HashMap<String, AttributeValue>, SdkError<E>>>,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    schema: SchemaRef,
    unnest_depth: Option<usize>,
) -> DataFusionResult<()>
where
    E: std::error::Error + Send + Sync + 'static,
{
    const CHUNK_SIZE: usize = 4_000;

    let item_stream = stream::unfold(pagination_stream, |mut stream| async move {
        stream.next().await.map(|item| (item, stream))
    });

    let chunked_stream = item_stream
        .map(|result| result.map_err(|e| to_execution_error(map_sdk_error(e))))
        .chunks(CHUNK_SIZE);

    pin_mut!(chunked_stream);

    while let Some(chunk) = chunked_stream.next().await {
        let items: Result<Vec<_>, _> = chunk.into_iter().collect();
        let items = items?;

        let unnested_items = match unnest_depth {
            None | Some(0) => items,
            Some(depth) => unnest_dynamodb_items(items, depth).map_err(to_execution_error)?,
        };

        let batch = dynamodb_items_to_arrow(&unnested_items, Arc::clone(&schema))
            .map_err(to_execution_error)?;

        tx.send(Ok(batch)).await.map_err(to_execution_error)?;
    }

    Ok(())
}

/// Creates a projection expression for a `DynamoDB` scan request based on the provided schema and projection indices.
/// Because projection expressions may use reserved words in `DynamoDB`, this function automatically generates expression attribute names for each column to avoid conflicts.
/// The expression format used is `#c{idx}` for each projected column, where `{idx}` is the column projection index.
/// See: <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html#Expressions.ExpressionAttributeNames.ReservedWords>
/// Returns a tuple of (`projection_expression`, `expression_attribute_names`) for the `DynamoDB` scan.
/// Returns None if no projection is required.
fn projection_expression() -> Option<(String, HashMap<String, String>)> {
    todo!()
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

    if let Some(err) = source.downcast_ref::<aws_sdk_dynamodb::operation::query::QueryError>() {
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
