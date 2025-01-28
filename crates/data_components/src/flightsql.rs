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

use arrow::{
    array::{array, Array, RecordBatch},
    datatypes::Schema,
};
use async_stream::stream;
use async_trait::async_trait;
use datafusion_table_providers::sql::sql_provider_datafusion::expr;
use flight_client::{
    tls::new_tls_flight_channel, MAX_DECODING_MESSAGE_SIZE, MAX_ENCODING_MESSAGE_SIZE,
};
use futures::{Stream, StreamExt, TryStreamExt};
use snafu::prelude::*;
use std::{any::Any, fmt, sync::Arc, vec};

use arrow_flight::{
    error::FlightError,
    flight_service_client::FlightServiceClient,
    sql::{client::FlightSqlServiceClient, CommandGetTables},
    FlightEndpoint, IpcMessage,
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        project_schema, stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType,
        ExecutionMode, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
    },
    sql::TableReference,
};
use tonic::codegen::Bytes;
use tonic::transport::{channel, Channel};

use crate::Read;

pub mod federation;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to connect to the Flight server.\n{source}\nVerify configuration and try again. For details, visit https://spiceai.org/docs/components/data-connectors/flightsql#params"))]
    UnableToConnectToServer { source: tonic::transport::Error },

    #[snafu(display("Failed to create SQL query (flightsql).\n{source}\nAn unexpected error occurred. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"))]
    UnableToGenerateSQL { source: expr::Error },

    #[snafu(display("Query execution failed (flightsql).\n{source}"))]
    UnableToQueryArrowFlight { source: FlightError },

    #[snafu(display("Failed to retrieve table {table_name} schema (flightsql).\n{source}\nAn internal error occurred. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"))]
    UnableToRetrieveSchemaFromIpcMessage {
        source: arrow::error::ArrowError,
        table_name: String,
    },

    #[snafu(display("Failed to detect table '{table_name}' schema (flightsql).\n{source}\nVerify the connection and try again. If the issue persists, report a bug on GitHub: https://github.com/spiceai/spiceai/issues"))]
    UnableToRetrieveSchemaArrow {
        source: arrow::error::ArrowError,
        table_name: String,
    },

    #[snafu(display("Failed to detect table '{table_name}' schema (flightsql).\n{source}\nVerify the connection and try again. If the issue persists, report a bug on GitHub: https://github.com/spiceai/spiceai/issues"))]
    UnableToRetrieveSchemaFlight {
        source: FlightError,
        table_name: String,
    },

    #[snafu(display("Failed to detect table '{table_name}' schema (flightsql).\nEnsure the table exists and try again."))]
    UnableToRetrieveSchema { table_name: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct FlightSQLFactory {
    client: FlightSqlServiceClient<Channel>,
    endpoint: String,
}

impl FlightSQLFactory {
    #[must_use]
    pub fn new(client: FlightSqlServiceClient<Channel>, endpoint: String) -> Self {
        Self { client, endpoint }
    }
}

#[async_trait]
impl Read for FlightSQLFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let table_provider = match schema {
            Some(schema) => Arc::new(FlightSQLTable::create_with_schema(
                "flightsql",
                &self.endpoint,
                self.client.clone(),
                table_reference,
                schema,
            )),
            None => Arc::new(
                FlightSQLTable::create(
                    "flightsql",
                    &self.endpoint,
                    self.client.clone(),
                    table_reference,
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

#[derive(Debug)]
pub struct FlightSQLTable {
    name: &'static str,
    join_push_down_context: String,
    client: FlightSqlServiceClient<Channel>,
    table_reference: TableReference,
    schema: SchemaRef,
}

#[allow(clippy::needless_pass_by_value)]
impl FlightSQLTable {
    pub async fn create(
        name: &'static str,
        endpoint: &str,
        client: FlightSqlServiceClient<Channel>,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self> {
        let table_reference: TableReference = table_reference.into();
        let schema = Self::get_schema(client.clone(), table_reference.clone()).await?;
        Ok(Self {
            name,
            client,
            table_reference,
            schema,
            join_push_down_context: format!("endpoint={endpoint}"),
        })
    }

    pub fn create_with_schema(
        name: &'static str,
        endpoint: &str,
        client: FlightSqlServiceClient<Channel>,
        table_reference: impl Into<TableReference>,
        schema: SchemaRef,
    ) -> Self {
        let table_reference: TableReference = table_reference.into();
        Self {
            name,
            client,
            table_reference,
            schema,
            join_push_down_context: format!("endpoint={endpoint}"),
        }
    }

    pub async fn from_static(
        s: &'static str,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self> {
        let channel = channel::Endpoint::from_static(s)
            .connect()
            .await
            .context(UnableToConnectToServerSnafu)?;

        let flight_client = FlightServiceClient::new(channel)
            .max_encoding_message_size(MAX_ENCODING_MESSAGE_SIZE)
            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE);

        Self::create(
            "flightsql",
            s,
            FlightSqlServiceClient::new_from_inner(flight_client),
            table_reference.into(),
        )
        .await
    }

    fn get_str_from_record_batch(b: &RecordBatch, row: usize, col_name: &str) -> Option<String> {
        if let Some(col_array) = b.column_by_name(col_name) {
            if let Some(y) = col_array.as_any().downcast_ref::<array::StringArray>() {
                return Some(y.value(row).to_string());
            }
        }
        None
    }

    #[must_use]
    pub fn get_table_schema_if_present(
        batches: Vec<RecordBatch>,
        table_reference: TableReference,
    ) -> Option<SchemaRef> {
        let mut possible_schema_bytz: Vec<Vec<u8>> = vec![];

        for b in batches {
            if let Some(table_schema) = b
                .column_by_name("table_schema")
                .and_then(|ts_array| ts_array.as_any().downcast_ref::<array::BinaryArray>())
                .or(None)
            {
                possible_schema_bytz.extend((0..b.num_rows()).filter_map(|i| {
                    let table_name =
                        Self::get_str_from_record_batch(&b, i, "table_name").unwrap_or_default();
                    let catalog_name =
                        Self::get_str_from_record_batch(&b, i, "catalog_name").unwrap_or_default();
                    let db_schema_name = Self::get_str_from_record_batch(&b, i, "db_schema_name")
                        .unwrap_or_default();

                    // Only check fields in `table_reference` matches.
                    if table_reference.resolved_eq(&TableReference::full(
                        catalog_name,
                        db_schema_name,
                        table_name,
                    )) {
                        Some(table_schema.value(i).to_vec())
                    } else {
                        None
                    }
                }));
            }
        }
        match possible_schema_bytz.len() {
            1 => {
                if let Some(bytz) = possible_schema_bytz.first() {
                    match Schema::try_from(IpcMessage(Bytes::copy_from_slice(bytz))).context(
                        UnableToRetrieveSchemaFromIpcMessageSnafu {
                            table_name: table_reference.to_string(),
                        },
                    ) {
                        Ok(schema) => Some(Arc::new(schema)),
                        Err(e) => {
                            tracing::error!(
                                "Error converting schema from 'table_schema' column: {e}"
                            );
                            None
                        }
                    }
                } else {
                    None
                } // Not possible due to match 1.
            }
            0 => None,
            _ => {
                tracing::error!("Multiple schemas found for table_reference: {table_reference}");
                None
            }
        }
    }

    pub async fn get_schema(
        mut client: FlightSqlServiceClient<Channel>,
        table_reference: TableReference,
    ) -> Result<SchemaRef> {
        let flight_info = client
            .get_tables(CommandGetTables {
                catalog: table_reference.catalog().map(ToString::to_string),
                db_schema_filter_pattern: table_reference.schema().map(ToString::to_string),
                table_name_filter_pattern: Some(table_reference.table().to_string()),
                include_schema: true,
                table_types: [
                    "TABLE",
                    "BASE TABLE",
                    "VIEW",
                    "LOCAL TEMPORARY",
                    "SYSTEM TABLE",
                ]
                .iter()
                .map(|&s| s.into())
                .collect(),
            })
            .await
            .context(UnableToRetrieveSchemaArrowSnafu {
                table_name: table_reference.to_string(),
            })?;

        for tkt in flight_info
            .endpoint
            .iter()
            .filter_map(|ep| ep.ticket.as_ref())
        {
            let stream =
                client
                    .do_get(tkt.clone())
                    .await
                    .context(UnableToRetrieveSchemaArrowSnafu {
                        table_name: table_reference.to_string(),
                    })?;
            let batch = stream.try_collect::<Vec<_>>().await.context(
                UnableToRetrieveSchemaFlightSnafu {
                    table_name: table_reference.to_string(),
                },
            )?;

            // Schema: https://github.com/apache/arrow/blob/44edc27e549d82db930421b0d4c76098941afd71/format/FlightSql.proto#L1182-L1190
            if let Some(schema) = Self::get_table_schema_if_present(batch, table_reference.clone())
            {
                return Ok(schema);
            };
        }

        UnableToRetrieveSchemaSnafu {
            table_name: table_reference.to_string(),
        }
        .fail()
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FlightSqlExec::new(
            projections,
            schema,
            &self.table_reference,
            self.client.clone(),
            filters,
            limit,
        )?))
    }
}

#[async_trait]
impl TableProvider for FlightSQLTable {
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
struct FlightSqlExec {
    projected_schema: SchemaRef,
    table_reference: TableReference,
    client: FlightSqlServiceClient<Channel>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl FlightSqlExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &TableReference,
        client: FlightSqlServiceClient<Channel>,
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

impl std::fmt::Debug for FlightSqlExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "FlightSqlExec sql={sql}")
    }
}

impl DisplayAs for FlightSqlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "FlightSqlExec sql={sql}")
    }
}

impl ExecutionPlan for FlightSqlExec {
    fn name(&self) -> &'static str {
        "FlightSqlExec"
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
    mut client: FlightSqlServiceClient<Channel>,
    sql: &str,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    let sql = sql.to_string();

    stream! {
        let flight_info = client
            .execute(sql.to_string(), None)
            .await
            .map_err(to_execution_error)?;

        for ep in flight_info.endpoint {
            if let Some(tkt) = ep.clone().ticket {
                match get_client_for_flight_endpoint(&client, ep).await
                    .map_err(to_execution_error)?
                    .do_get(tkt.clone()).await {
                        Ok(mut flight_stream) => {
                            while let Some(batch) = flight_stream.next().await {
                                match batch {
                                    Ok(batch) => yield Ok(batch),
                                    Err(error) => yield Err(to_execution_error(Error::UnableToQueryArrowFlight { source: error }))
                                }
                            }
                        },
                        Err(error) => yield Err(to_execution_error(Error::UnableToQueryArrowFlight { source: error.into()} ))
                };
            }
        };
    }
}

#[allow(clippy::needless_pass_by_value)]
fn to_execution_error(e: impl Into<Box<dyn std::error::Error>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}

pub async fn get_client_for_flight_endpoint(
    client: &FlightSqlServiceClient<Channel>,
    ep: FlightEndpoint,
) -> Result<FlightSqlServiceClient<Channel>, Box<dyn std::error::Error>> {
    if ep.location.is_empty() {
        Ok(client.clone())
    } else {
        let channel = new_tls_flight_channel(&ep.location[0].uri).await?;
        Ok(FlightSqlServiceClient::new(channel))
    }
}
