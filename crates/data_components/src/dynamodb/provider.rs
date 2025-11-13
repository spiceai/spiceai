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

use super::{
    DescribeTableSnafu, DowncastBuilderSnafu, Error, Result, ScanSnafu, TableDoesNotExistSnafu,
    TableStatusIsNotActiveSnafu,
};
use crate::arrow::struct_builder::StructBuilder;
use crate::cdc;
use crate::cdc::{
    ChangeBatch, ChangeEnvelope, ChangesStream, CommitChange, CommitError, changes_schema,
};
use crate::dynamodb::arrow::{append_item_to_struct_builder, dynamodb_items_to_arrow};
use crate::dynamodb::conversion::streams_to_dynamodb_item;
use crate::dynamodb::request_builder::DynamoDBRequestPlanBuilder;
use crate::dynamodb::request_plan::{DynamoDBRequestPlan, QueryParams, ScanParams};
use crate::dynamodb::schema::infer_arrow_schema_from_items;
use crate::dynamodb::table_schema::DynamoDBTableSchema;
use crate::dynamodb::unnest::{unnest_dynamodb_item, unnest_dynamodb_items};
use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use arrow_array::builder::{ArrayBuilder, ListBuilder, StringBuilder};
use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_dynamodb::{
    Client as DbClient,
    error::SdkError,
    types::{AttributeValue, KeyType, TableStatus},
};
use aws_sdk_dynamodbstreams::types::OperationType;
use aws_smithy_async::future::pagination_stream::TryFlatMap;
use datafusion::common::{Constraint, Constraints, DFSchema};
use datafusion::logical_expr::TableProviderFilterPushDown;
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
use dynamo_subscriber::{Client as StreamsClient, SDKClient};
use futures::Stream;
use futures::pin_mut;
use futures::stream::{self, StreamExt};
use snafu::prelude::*;
use std::collections::HashSet;
use std::pin::Pin;
use std::time::Duration;
use std::{any::Any, collections::HashMap, fmt, sync::Arc};

#[derive(Debug)]
pub struct DynamoDBTableProvider {
    db_client: Arc<DbClient>,
    streams_client: Arc<StreamsClient<SDKClient>>,
    table_schema: DynamoDBTableSchema,
    constraints: Option<Constraints>,
    request_plan_builder: DynamoDBRequestPlanBuilder,
    unnest_depth: Option<usize>,
    config_partitions: Option<usize>,
    table_total_item_count: Option<i64>,
}

type DynamoDBItemStream =
    dyn Stream<Item = DataFusionResult<HashMap<String, AttributeValue>>> + Send + 'static;

const DEFAULT_PARTITIONS: usize = 8;

impl DynamoDBTableProvider {
    pub async fn try_new(
        sdk_config: SdkConfig,
        table_name: Arc<str>,
        unnest_depth: Option<usize>,
        schema_infer_max_records: i32,
        config_partitions: Option<usize>,
    ) -> Result<Self, Error> {
        let db_client = Arc::new(DbClient::new(&sdk_config));
        let streams_client = Arc::new(
            StreamsClient::builder(sdk_config, table_name.to_string())
                .interval(Some(Duration::from_millis(200)))
                .build(),
        );

        let (table_schema, partition_key, sort_key, flattened_fields, table_total_item_count) =
            Self::fetch_table_metadata(
                Arc::clone(&db_client),
                &table_name,
                unnest_depth,
                schema_infer_max_records,
            )
            .await?;

        let table_schema = DynamoDBTableSchema::new(
            table_name,
            table_schema,
            partition_key,
            sort_key,
            flattened_fields,
        );

        // Create constraints with the primary key indices
        let Ok(df_schema) = DFSchema::try_from(Arc::clone(table_schema.schema())) else {
            unreachable!("DFSchema::try_from is infallible as of DataFusion 38")
        };

        let pk_indices: Vec<usize> = table_schema
            .primary_keys()
            .iter()
            .filter_map(|pk| df_schema.index_of_column_by_name(None, pk))
            .collect();

        let constraints = if pk_indices.is_empty() {
            None
        } else {
            Some(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                pk_indices,
            )]))
        };

        Ok(Self {
            db_client,
            streams_client,
            table_schema: table_schema.clone(),
            constraints,
            request_plan_builder: DynamoDBRequestPlanBuilder::new(table_schema),
            unnest_depth,
            config_partitions,
            table_total_item_count,
        })
    }

    async fn fetch_table_metadata(
        db_client: Arc<DbClient>,
        table_name: &str,
        unnest_depth: Option<usize>,
        schema_infer_max_records: i32,
    ) -> Result<(
        SchemaRef,
        String,
        Option<String>,
        HashSet<String>,
        Option<i64>,
    )> {
        let response = db_client
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
            return Err(Error::MissingPartitionKey);
        };

        let mut request = db_client.scan().table_name(table_name);

        request = request.limit(schema_infer_max_records);

        let items: Vec<_> = request
            .send()
            .await
            .map_err(map_sdk_error)
            .context(ScanSnafu)?
            .items()
            .to_vec();

        let (unnested_items, flattened_fields) = match unnest_depth {
            None => (items, HashSet::new()),
            Some(depth) => unnest_dynamodb_items(items, depth)?,
        };

        Ok((
            infer_arrow_schema_from_items(&unnested_items)?,
            partition_key,
            sort_key,
            flattened_fields,
            table.item_count,
        ))
    }

    fn get_partitions_from_table_size(&self) -> usize {
        match self.table_total_item_count {
            None => DEFAULT_PARTITIONS,
            Some(row_count) => match row_count {
                0..1_000 => 1,
                1_000..10_000 => 2,
                10_000..100_000 => 4,
                100_000..1_000_000 => 8,
                1_000_000..10_000_000 => 16,
                _ => 32,
            },
        }
    }

    pub async fn changes_stream_from_latest(&self) -> ChangesStream {
        let record_schema = Arc::clone(self.table_schema.schema());
        let changes_schema = changes_schema(&record_schema).clone();

        let primary_keys = self.table_schema.primary_keys().clone();
        let unnest_depth = self.unnest_depth;

        let stream = self.streams_client.stream_from_latest().map(move |batch| {

            // TODO: What if a batch is empty?

            let mut changes_struct_builder =
                StructBuilder::from_fields(changes_schema.fields().clone(), batch.len());

            for record in &batch {

                let (op_str, item_data) = match (&record.event_name, &record.dynamodb) {
                    (Some(event_name), Some(dynamodb)) => {
                        match event_name {
                            OperationType::Insert | OperationType::Modify => {
                                let Some(item) = &dynamodb.new_image else {
                                    continue;
                                };
                                let streams_item = streams_to_dynamodb_item(item.clone());

                                let (unnested_streams_item, _) = match unnest_depth {
                                    None => (streams_item, HashSet::new()),
                                    Some(depth) => {
                                        unnest_dynamodb_item(&streams_item, depth)
                                            .map_err(|e| cdc::StreamError::SerdeJsonError(e.to_string()))?
                                    }
                                };

                                let op = if matches!(event_name, OperationType::Insert) {
                                    "c"
                                } else {
                                    "u"
                                };

                                (op, unnested_streams_item)
                            }
                            OperationType::Remove => {
                                let Some(keys_item) = &dynamodb.keys else {
                                    continue;
                                };
                                let streams_keys_item = streams_to_dynamodb_item(keys_item.clone());
                                ("d", streams_keys_item)
                            }
                            _ => continue,
                        }
                    }
                    _ => continue,
                };

                // Append row to changes struct
                changes_struct_builder.append(true);

                // Populate each field in the changes schema
                for (idx, field) in changes_schema.fields().iter().enumerate() {
                    let field_builder = changes_struct_builder.field_builder_array(idx);

                    match field.name().as_str() {
                        "op" => {
                            let str_builder = downcast_builder::<StringBuilder>(field_builder)
                                .map_err(|e| cdc::StreamError::SerdeJsonError(e.to_string()))?;
                            str_builder.append_value(op_str);
                        }
                        "primary_keys" => {
                            let list_builder =
                                downcast_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(field_builder)
                                    .map_err(|e| cdc::StreamError::SerdeJsonError(e.to_string()))?;
                            if primary_keys.is_empty() {
                                list_builder.append(false);
                            } else {
                                let str_builder = downcast_builder::<StringBuilder>(list_builder.values())
                                    .map_err(|e| cdc::StreamError::SerdeJsonError(e.to_string()))?;
                                for key in &primary_keys {
                                    str_builder.append_value(key);
                                }
                                list_builder.append(true);
                            }
                        }
                        "data" => {
                            let data_struct_builder = downcast_builder::<StructBuilder>(field_builder)
                                .map_err(|e| cdc::StreamError::SerdeJsonError(e.to_string()))?;
                            append_item_to_struct_builder(&item_data, data_struct_builder)
                                .map_err(|e| cdc::StreamError::SerdeJsonError(e.to_string()))?;
                        }
                        _ => unreachable!("Unexpected field in changes schema {}", field.name()),
                    }
                }
            }

            let struct_array = changes_struct_builder.finish();
            let record_batch: RecordBatch = struct_array.into();

            let Ok(change_batch) = ChangeBatch::try_new(record_batch) else {
                unreachable!(
                    "We constructed the record batch with the correct schema, so this shouldn't fail"
                );
            };

            Ok(ChangeEnvelope::new(Box::new(DynamoDBStreamCommitter::new()), change_batch))
        });

        Box::pin(stream)
    }
}

struct DynamoDBStreamCommitter;

impl DynamoDBStreamCommitter {
    pub fn new() -> Self {
        Self {}
    }
}

impl CommitChange for DynamoDBStreamCommitter {
    fn commit(&self) -> std::result::Result<(), CommitError> {
        // println!("Committing changes");
        Ok(())
    }
}

pub(crate) fn downcast_builder<T: ArrayBuilder>(builder: &mut dyn ArrayBuilder) -> Result<&mut T> {
    let builder = builder
        .as_any_mut()
        .downcast_mut::<T>()
        .context(DowncastBuilderSnafu)?;
    Ok(builder)
}

#[async_trait]
impl TableProvider for DynamoDBTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.table_schema.schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.constraints.as_ref()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut projected_schema = project_schema(self.table_schema.schema(), projection)?;

        tracing::debug!(
            "Table {:?}, projection: {:?}, filters: {:?}, limit: {:?}",
            self.table_schema.table_name(),
            projection,
            filters,
            limit
        );

        // If no columns are specified, use partition_key - otherwise DynamoDB returns an error
        if projected_schema.fields.is_empty() {
            let idx = self
                .table_schema
                .schema()
                .index_of(self.table_schema.partition_key())?;
            projected_schema = SchemaRef::from(self.table_schema.schema().project(&[idx])?);
        }

        let request_plan =
            self.request_plan_builder
                .build_request_plan(filters, &projected_schema, limit)?;

        tracing::debug!(
            "Table {:?}, request_plan: {:?}",
            self.table_schema.table_name(),
            request_plan
        );

        // If `config_partitions` is empty (i.e. it was set to 'auto' in the config), use table size as a heuristic.
        let total_partitions = self
            .config_partitions
            .unwrap_or_else(|| self.get_partitions_from_table_size());

        tracing::debug!(
            "Table {:?}, total_partitions: {:?}",
            self.table_schema.table_name(),
            total_partitions
        );

        Ok(Arc::new(DynamoDBTableProviderExec::new(
            Arc::clone(&self.db_client),
            request_plan,
            self.unnest_depth,
            projected_schema,
            total_partitions,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(self.table_schema.supports_filters_pushdown(filters))
    }
}

pub struct DynamoDBTableProviderExec {
    client: Arc<DbClient>,
    request_plan: DynamoDBRequestPlan,
    projected_schema: SchemaRef,
    unnest_depth: Option<usize>,
    properties: PlanProperties,
}

impl DynamoDBTableProviderExec {
    #[must_use]
    pub fn new(
        client: Arc<DbClient>,
        request_plan: DynamoDBRequestPlan,
        unnest_depth: Option<usize>,
        projected_schema: SchemaRef,
        partitions: usize,
    ) -> Self {
        Self {
            client,
            request_plan,
            projected_schema: Arc::clone(&projected_schema),
            unnest_depth,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }
    }
}

impl std::fmt::Debug for DynamoDBTableProviderExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("DynamoDBTableProviderExec")
            .field("request_plan", &self.request_plan)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DynamoDBTableProviderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("DynamoDBTableProviderExec")
            .field("request_plan", &self.request_plan)
            .finish_non_exhaustive()
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
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&self.projected_schema), 2);
        let tx = builder.tx();

        let schema = Arc::clone(&self.projected_schema);
        let client = Arc::clone(&self.client);
        let request_plan = self.request_plan.clone();
        let unnest_depth = self.unnest_depth;

        let total_partitions = match self.properties.partitioning {
            Partitioning::RoundRobinBatch(_) | Partitioning::Hash(_, _) => 1,
            Partitioning::UnknownPartitioning(partitions) => partitions,
        };

        let segment: i32 = i32::try_from(partition).map_err(|_| {
            DataFusionError::Execution(
                format!("Partition number too large for DynamoDB segment: {partition}").to_string(),
            )
        })?;

        let total_segments: i32 = i32::try_from(total_partitions).map_err(|_| DataFusionError::Execution(
            format!("Total partitions number too large for DynamoDB total_segments: {total_partitions}").to_string()
        ))?;

        builder.spawn(async move {
            const CHUNK_SIZE: usize = 4_000;

            let item_stream =
                build_stream_from_plan(&client, request_plan, segment, total_segments);
            let chunked_stream = item_stream.chunks(CHUNK_SIZE);
            pin_mut!(chunked_stream);

            while let Some(chunk) = chunked_stream.next().await {
                let items: Result<Vec<_>, _> = chunk.into_iter().collect();
                let items = items?;

                let (unnested_items, _) = match unnest_depth {
                    None => (items, HashSet::new()),
                    Some(depth) => {
                        unnest_dynamodb_items(items, depth).map_err(to_execution_error)?
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

#[deny(unused_variables)]
fn build_stream_from_plan(
    client: &Arc<DbClient>,
    request: DynamoDBRequestPlan,
    segment: i32,
    total_segments: i32,
) -> Pin<Box<DynamoDBItemStream>> {
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
            let request = client
                .query()
                .table_name(table_name)
                .set_key_condition_expression(key_condition_expression)
                .set_filter_expression(filter_expression)
                .set_expression_attribute_values(expression_attribute_values)
                .set_expression_attribute_names(expression_attribute_names)
                .set_projection_expression(projection_expression)
                .set_limit(limit);

            let pagination_stream = TryFlatMap::new(request.into_paginator().send())
                .flat_map(|output| output.items().to_vec());

            let stream = stream::unfold(pagination_stream, |mut s| async move {
                s.next().await.map(|item| {
                    let result = item.map_err(|e| to_execution_error(map_sdk_error(e)));
                    (result, s)
                })
            });

            Box::pin(stream)
        }
        DynamoDBRequestPlan::Scan(ScanParams {
            table_name,
            filter_expression,
            expression_attribute_values,
            expression_attribute_names,
            projection_expression,
            limit,
        }) => {
            let mut request = client
                .scan()
                .table_name(table_name)
                .set_filter_expression(filter_expression)
                .set_expression_attribute_values(expression_attribute_values)
                .set_expression_attribute_names(expression_attribute_names)
                .set_projection_expression(projection_expression)
                .set_limit(limit);

            if total_segments > 1 {
                request = request.segment(segment).total_segments(total_segments);
            }

            let pagination_stream = TryFlatMap::new(request.into_paginator().send())
                .flat_map(|output| output.items().to_vec());

            let stream = stream::unfold(pagination_stream, |mut s| async move {
                s.next().await.map(|item| {
                    let result = item.map_err(|e| to_execution_error(map_sdk_error(e)));
                    (result, s)
                })
            });

            Box::pin(stream)
        }
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
