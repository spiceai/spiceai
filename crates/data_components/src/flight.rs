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

use crate::{Read, ReadWrite};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use arrow_flight::error::FlightError;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{project_schema, TableReference},
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties,
    },
    sql::unparser::dialect::Dialect,
};
use datafusion_federation::table_reference::MultiPartTableReference;
use datafusion_table_providers::sql::sql_provider_datafusion::expr;
use flight_client::FlightClient;
use futures::{Stream, StreamExt};
use snafu::prelude::*;
use std::{any::Any, fmt, sync::Arc};
use tonic::metadata::MetadataMap;

use self::write::FlightTableWriter;

pub mod federation;
pub mod stream;
pub mod write;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Query execution failed.\n{source}\nReport a bug to request support: https://github.com/spiceai/spiceai/issues"))]
    UnableToGenerateSQL { source: expr::Error },

    #[snafu(display("Failed to query Arrow Flight.\n{source}"))]
    Flight { source: flight_client::Error },

    #[snafu(display("Failed to get schema from Arrow Flight for table {table}.\n{source}"))]
    UnableToGetSchema {
        source: flight_client::Error,
        table: String,
    },

    #[snafu(display(
        "Query execution failed.\n{source}\nVerify the configuration and try again."
    ))]
    ArrowFlight { source: FlightError },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct FlightFactory {
    name: &'static str,
    client: FlightClient,
    dialect: Arc<dyn Dialect>,
    subquery_use_partial_path: bool,
    extra_compute_context: Option<Arc<str>>,
}

impl FlightFactory {
    #[must_use]
    pub fn new(
        name: &'static str,
        client: FlightClient,
        dialect: Arc<dyn Dialect>,
        subquery_use_partial_path: bool,
    ) -> Self {
        Self {
            name,
            client,
            dialect,
            subquery_use_partial_path,
            extra_compute_context: None,
        }
    }

    #[must_use]
    pub fn client(&self) -> FlightClient {
        self.client.clone()
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: MetadataMap) -> Self {
        self.client = self.client.with_metadata(metadata);
        self
    }

    #[must_use]
    pub fn with_extra_compute_context(mut self, compute_context: &str) -> Self {
        self.extra_compute_context = Some(Arc::from(compute_context));
        self
    }

    pub async fn table_provider(
        &self,
        table_reference: impl Into<MultiPartTableReference>,
        schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let table_provider = match schema {
            Some(schema) => Arc::new(FlightTable::create_with_schema(
                self.name,
                self.client.clone(),
                table_reference,
                schema,
                Arc::clone(&self.dialect),
                self.subquery_use_partial_path,
                self.extra_compute_context.as_ref().map(Arc::clone),
            )),
            None => Arc::new(
                FlightTable::create(
                    self.name,
                    self.client.clone(),
                    table_reference,
                    Arc::clone(&self.dialect),
                    self.subquery_use_partial_path,
                    self.extra_compute_context.as_ref().map(Arc::clone),
                )
                .await?,
            ),
        };

        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }
}

#[async_trait]
impl Read for FlightFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        FlightFactory::table_provider(self, table_reference, schema).await
    }
}

#[async_trait]
impl ReadWrite for FlightFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let read_provider = Read::table_provider(self, table_reference.clone(), schema).await?;

        Ok(FlightTableWriter::create(
            read_provider,
            table_reference,
            self.client.clone(),
        ))
    }
}

pub struct FlightTable {
    name: &'static str,
    join_push_down_context: String,
    client: FlightClient,
    schema: SchemaRef,
    dialect: Arc<dyn Dialect>,
    table_reference: MultiPartTableReference,
    subquery_use_partial_path: bool,
}

impl std::fmt::Debug for FlightTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightTable")
            .field("name", &self.name)
            .field("join_push_down_context", &self.join_push_down_context)
            .field("client", &self.client)
            .field("schema", &self.schema)
            .field("table_reference", &self.table_reference)
            .finish_non_exhaustive()
    }
}

#[allow(clippy::needless_pass_by_value)]
impl FlightTable {
    pub async fn create(
        name: &'static str,
        client: FlightClient,
        table_reference: impl Into<MultiPartTableReference>,
        dialect: Arc<dyn Dialect>,
        subquery_use_partial_path: bool,
        extra_compute_context: Option<Arc<str>>,
    ) -> Result<Self> {
        let table_reference = table_reference.into();
        let schema = Self::get_schema(client.clone(), table_reference.clone()).await?;

        let base_context = Self::get_base_context(&client);
        let join_push_down_context =
            Self::get_extended_context(&base_context, extra_compute_context);

        Ok(Self {
            name,
            client: client.clone(),
            schema,
            table_reference,
            dialect,
            join_push_down_context,
            subquery_use_partial_path,
        })
    }

    pub fn create_with_schema(
        name: &'static str,
        client: FlightClient,
        table_reference: impl Into<MultiPartTableReference>,
        schema: SchemaRef,
        dialect: Arc<dyn Dialect>,
        subquery_use_partial_path: bool,
        extra_compute_context: Option<Arc<str>>,
    ) -> Self {
        let table_reference = table_reference.into();
        tracing::debug!("table_reference={:?}", table_reference);

        let base_context = Self::get_base_context(&client);
        let join_push_down_context =
            Self::get_extended_context(&base_context, extra_compute_context);

        Self {
            name,
            client: client.clone(),
            schema,
            table_reference,
            dialect,
            join_push_down_context,
            subquery_use_partial_path,
        }
    }

    async fn get_schema(
        client: FlightClient,
        table_reference: impl Into<MultiPartTableReference>,
    ) -> Result<SchemaRef> {
        let table_reference = table_reference.into();
        let table_paths = match &table_reference {
            MultiPartTableReference::TableReference(table_reference) => match table_reference {
                TableReference::Bare { table } => vec![table.to_string()],
                TableReference::Partial { schema, table } => {
                    vec![schema.to_string(), table.to_string()]
                }
                TableReference::Full {
                    catalog,
                    schema,
                    table,
                } => {
                    vec![catalog.to_string(), schema.to_string(), table.to_string()]
                }
            },
            MultiPartTableReference::Multi(parts) => {
                parts.iter().map(ToString::to_string).collect::<Vec<_>>()
            }
        };

        let schema = client
            .get_schema(table_paths)
            .await
            .context(UnableToGetSchemaSnafu {
                table: table_reference.to_quoted_string(),
            })?;

        Ok(Arc::new(schema))
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FlightExec::new(
            projections,
            schema,
            &self.table_reference,
            self.client.clone(),
            filters,
            limit,
        )?))
    }

    pub fn get_flight_client(&self) -> FlightClient {
        self.client.clone()
    }

    pub fn get_table_reference(&self) -> String {
        self.table_reference.to_string()
    }

    fn get_base_context(client: &FlightClient) -> String {
        format!(
            "url={},username={}",
            client.url(),
            client.username().unwrap_or_default()
        )
    }

    fn get_extended_context(base_context: &str, compute_context: Option<Arc<str>>) -> String {
        if let Some(compute_context) = compute_context {
            format!("{base_context},{compute_context}")
        } else {
            base_context.to_string()
        }
    }
}

#[async_trait]
impl TableProvider for FlightTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let mut filter_push_down = vec![];
        for filter in filters {
            match expr::to_sql(filter) {
                Ok(_) => filter_push_down.push(TableProviderFilterPushDown::Exact),
                Err(_) => filter_push_down.push(TableProviderFilterPushDown::Unsupported),
            }
        }

        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, &self.schema(), filters, limit);
    }
}

#[derive(Clone)]
struct FlightExec {
    projected_schema: SchemaRef,
    table_reference: MultiPartTableReference,
    client: FlightClient,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl FlightExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &MultiPartTableReference,
        client: FlightClient,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema(schema, projections)?;
        Ok(Self {
            projected_schema: Arc::clone(&projected_schema),
            table_reference: table_reference.clone(),
            client,
            filters: filters.to_vec(),
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        })
    }

    fn sql(&self) -> Result<String> {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let limit_expr = match self.limit {
            Some(limit) => format!("LIMIT {limit}"),
            None => String::new(),
        };

        let where_expr = if self.filters.is_empty() {
            String::new()
        } else {
            let filter_expr = self
                .filters
                .iter()
                .map(expr::to_sql)
                .collect::<expr::Result<Vec<_>>>()
                .context(UnableToGenerateSQLSnafu)?;
            format!("WHERE {}", filter_expr.join(" AND "))
        };

        Ok(format!(
            "SELECT {columns} FROM {table_reference} {where_expr} {limit_expr}",
            table_reference = self.table_reference.to_quoted_string(),
        ))
    }
}

impl std::fmt::Debug for FlightExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "FlightExec sql={sql}")
    }
}

impl DisplayAs for FlightExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "FlightExec sql={sql}")
    }
}

impl ExecutionPlan for FlightExec {
    fn name(&self) -> &'static str {
        "FlightExec"
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
        let sql = self.sql().map_err(to_execution_error)?;

        let stream_adapter = RecordBatchStreamAdapter::new(
            self.schema(),
            query_to_stream(self.client.clone(), sql.as_str()),
        );

        Ok(Box::pin(stream_adapter))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn query_to_stream(
    client: FlightClient,
    sql: &str,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    let sql = sql.to_string();
    stream! {
        match client.query(sql.as_str()).await {
            Ok(mut stream) => {
                while let Some(batch) = stream.next().await {
                    match batch {
                        Ok(batch) => yield Ok(batch),
                        Err(error) => {
                            yield Err(to_execution_error(Error::ArrowFlight { source: error }));
                        }
                    }
                }
            }
            Err(error) => yield Err(to_execution_error(Error::Flight{ source: error}))
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn to_execution_error(e: Error) -> DataFusionError {
    match e {
        Error::Flight { source } => match source {
            flight_client::Error::UnableToQuery { source } => {
                DataFusionError::Execution(format!("{source}"))
            }
            _ => DataFusionError::Execution(format!("{source}")),
        },
        _ => DataFusionError::Execution(format!("{e}")),
    }
}
