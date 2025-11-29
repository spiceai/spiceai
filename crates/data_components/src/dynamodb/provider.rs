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
    DescribeTableSnafu, Error, FailedToBootstrapTableSnafu, FailedToInitializeCheckpointSnafu,
    FailedToInitializeStreamSnafu, Result, ScanSnafu, TableDoesNotExistSnafu,
    TableStatusIsNotActiveSnafu,
};
use crate::cdc::ChangeBatch;
use crate::dynamodb::arrow::dynamodb_items_to_arrow;
use crate::dynamodb::request_builder::DynamoDBRequestPlanBuilder;
use crate::dynamodb::request_plan::{DynamoDBRequestPlan, QueryParams, ScanParams};
use crate::dynamodb::schema::infer_arrow_schema_from_items;
use crate::dynamodb::stream::{StreamError, process_batch, record_batch_to_change_batch};
use crate::dynamodb::table_schema::DynamoDBTableSchema;
use crate::dynamodb::unnest::unnest_dynamodb_items;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_dynamodb::{
    Client as DbClient,
    error::SdkError,
    types::{AttributeValue, KeyType, TableStatus},
};
use aws_smithy_async::future::pagination_stream::TryFlatMap;
use datafusion::common::{Constraint, Constraints, DFSchema};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{LogicalPlanBuilder, TableProviderFilterPushDown, ident};
use datafusion::prelude::SessionContext;
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
use dynamodb_streams::Client as StreamsClient;
use dynamodb_streams::checkpoint::Checkpoint;
use futures::Stream;
use futures::pin_mut;
use futures::stream::{self, BoxStream, StreamExt};
use snafu::prelude::*;
use std::collections::HashSet;
use std::pin::Pin;
use std::time::Duration;
use std::{any::Any, collections::HashMap, fmt, sync::Arc};

#[derive(Debug, Clone)]
pub struct DynamoDBTableProvider {
    db_client: Arc<DbClient>,
    streams_client: Arc<StreamsClient>,
    table_schema: DynamoDBTableSchema,
    constraints: Option<Constraints>,
    request_plan_builder: Arc<DynamoDBRequestPlanBuilder>,
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
        stream_poll_interval_ms: u64,
        time_format: String,
    ) -> Result<Self, Error> {
        let db_client = Arc::new(DbClient::new(&sdk_config));
        let streams_client = Arc::new(
            StreamsClient::builder(sdk_config, table_name.to_string())
                .interval(Some(Duration::from_millis(stream_poll_interval_ms)))
                .build(),
        );

        let (table_schema, partition_key, sort_key, flattened_fields, table_total_item_count) =
            Self::fetch_table_metadata(
                Arc::clone(&db_client),
                &table_name,
                unnest_depth,
                schema_infer_max_records,
                &time_format,
            )
            .await?;

        let table_schema = DynamoDBTableSchema::new(
            table_name,
            table_schema,
            partition_key,
            sort_key,
            flattened_fields,
            &time_format,
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
            request_plan_builder: Arc::new(DynamoDBRequestPlanBuilder::new(table_schema)),
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
        time_format: &str,
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

        tracing::debug!(
            "DynamoDB items for schema inference: table_name={:?}, items={:?}",
            table_name,
            &unnested_items[..unnested_items.len().min(2)]
        );

        let schema = infer_arrow_schema_from_items(&unnested_items, time_format)?;

        tracing::debug!(
            "DynamoDB inferred schema: table_name={:?}, schema={:?}",
            table_name,
            schema
        );

        Ok((
            schema,
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

    pub async fn latest_global_checkpoint(&self) -> Result<Checkpoint> {
        self.streams_client
            .latest_global_checkpoint()
            .await
            .context(FailedToInitializeStreamSnafu)
    }

    pub async fn stream_from_checkpoint(
        &self,
        checkpoint: Checkpoint,
    ) -> Result<BoxStream<'static, Result<(ChangeBatch, Checkpoint), crate::cdc::StreamError>>>
    {
        let table_schema = Arc::clone(self.table_schema.schema());
        let primary_keys = self.table_schema.primary_keys().clone();
        let unnest_depth = self.unnest_depth;
        let time_format = Arc::clone(&self.table_schema.time_format());

        let stream = self
            .streams_client
            .stream_from_checkpoint(checkpoint)
            .await
            .context(FailedToInitializeCheckpointSnafu)?
            .map(move |batch| {
                process_batch(
                    batch,
                    &table_schema,
                    &primary_keys,
                    unnest_depth,
                    &time_format,
                )
                .map_err(crate::cdc::StreamError::DynamoDB)
            });

        Ok(Box::pin(stream))
    }

    /// Creates a bootstrap stream for the `DynamoDB` table.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Logical plan construction fails
    /// - Stream execution fails
    pub async fn bootstrap_stream(
        self: Arc<Self>,
    ) -> Result<BoxStream<'static, Result<ChangeBatch, crate::cdc::StreamError>>> {
        let schema = Arc::clone(self.table_schema.schema());
        let table_name = self.table_schema.table_name();
        let primary_keys = self.table_schema.primary_keys();

        let table_source = Arc::new(DefaultTableSource::new(
            Arc::clone(&self) as Arc<dyn TableProvider>
        ));

        let columns: Vec<Expr> = schema.fields().iter().map(|f| ident(f.name())).collect();

        let logical_plan = LogicalPlanBuilder::scan(table_name, table_source, None)
            .and_then(|b| b.project(columns))
            .and_then(datafusion::logical_expr::LogicalPlanBuilder::build)
            .context(FailedToBootstrapTableSnafu)?;

        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), logical_plan);

        let record_batch_stream = df
            .execute_stream()
            .await
            .context(FailedToBootstrapTableSnafu)?;

        let stream =
            record_batch_stream.map(move |record_batch_result| match record_batch_result {
                Ok(record_batch) => {
                    tracing::debug!(
                        "DynamoDB bootstrapping records: table_name={}, records={}",
                        self.table_schema.table_name(),
                        record_batch.num_rows()
                    );
                    record_batch_to_change_batch(record_batch, &schema, &primary_keys)
                        .map_err(crate::cdc::StreamError::DynamoDB)
                }
                Err(e) => Err(crate::cdc::StreamError::DynamoDB(
                    StreamError::FailedToReadRecordBatch { source: e },
                )),
            });

        Ok(stream.boxed())
    }
}

#[async_trait]
impl TableProvider for DynamoDBTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.table_schema.schema())
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.constraints.as_ref()
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
            self.table_schema.time_format(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let result = Ok(self.table_schema.supports_filters_pushdown(filters));

        tracing::debug!(
            "DynamoDBTableProvider supports_filters_pushdown: table={}, filters={:?}, result={:?}",
            self.table_schema.table_name(),
            filters,
            result
        );

        result
    }
}

pub struct DynamoDBTableProviderExec {
    client: Arc<DbClient>,
    request_plan: DynamoDBRequestPlan,
    projected_schema: SchemaRef,
    unnest_depth: Option<usize>,
    time_format: Arc<String>,
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
        time_format: Arc<String>,
    ) -> Self {
        Self {
            client,
            request_plan,
            projected_schema: Arc::clone(&projected_schema),
            unnest_depth,
            time_format,
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
        let time_format = Arc::clone(&self.time_format);

        let total_partitions = match self.properties.partitioning {
            Partitioning::RoundRobinBatch(_) | Partitioning::Hash(_, _) => 1,
            Partitioning::UnknownPartitioning(partitions) => partitions,
        };

        let segment: i32 = i32::try_from(partition).map_err(|_| {
            DataFusionError::Execution(format!(
                "Partition number too large for DynamoDB segment: {partition}"
            ))
        })?;

        let total_segments: i32 = i32::try_from(total_partitions).map_err(|_| {
            DataFusionError::Execution(format!(
                "Total partitions number too large for DynamoDB total_segments: {total_partitions}"
            ))
        })?;

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

                let batch =
                    dynamodb_items_to_arrow(&unnested_items, Arc::clone(&schema), &time_format)
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

pub fn to_execution_error(
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()))
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
