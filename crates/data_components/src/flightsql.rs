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
    array::{Array, RecordBatch, array},
    datatypes::Schema,
};
use async_stream::stream;
use async_trait::async_trait;
use datafusion_table_providers::sql::sql_provider_datafusion::expr;
use flight_client::{
    MAX_DECODING_MESSAGE_SIZE, MAX_ENCODING_MESSAGE_SIZE,
    cookie::{CookieService, CookieStore},
    tls::new_tls_flight_channel,
};
use futures::{Stream, StreamExt, TryStreamExt};
use snafu::prelude::*;
use std::{any::Any, fmt, sync::Arc, vec};

use arrow_flight::{
    FlightEndpoint, IpcMessage,
    error::FlightError,
    flight_service_client::FlightServiceClient,
    sql::{CommandGetTables, client::FlightSqlServiceClient},
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::utils::quote_identifier,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        project_schema,
        stream::RecordBatchStreamAdapter,
    },
    sql::TableReference,
};
use tonic::codegen::Bytes;
use tonic::transport::{Channel, channel};

use crate::Read;

pub mod federation;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to connect to the Flight server. {source} Verify configuration and try again. For details, visit https://spiceai.org/docs/components/data-connectors/flightsql#params"
    ))]
    UnableToConnectToServer { source: tonic::transport::Error },

    #[snafu(display(
        "Failed to create SQL query (flightsql). {source} An unexpected error occurred. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToGenerateSQL { source: expr::Error },

    #[snafu(display("Query execution failed (flightsql). {source}"))]
    UnableToQueryArrowFlight { source: FlightError },

    #[snafu(display(
        "Failed to retrieve table {table_name} schema (flightsql). {source} An internal error occurred. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToRetrieveSchemaFromIpcMessage {
        source: arrow::error::ArrowError,
        table_name: String,
    },

    #[snafu(display(
        "Failed to detect table '{table_name}' schema (flightsql). {source} Verify the connection and try again. If the issue persists, report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToRetrieveSchemaArrow {
        source: arrow::error::ArrowError,
        table_name: String,
    },

    #[snafu(display(
        "Failed to detect table '{table_name}' schema (flightsql). {source} Verify the connection and try again. If the issue persists, report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToRetrieveSchemaFlight {
        source: FlightError,
        table_name: String,
    },

    #[snafu(display(
        "Failed to detect table '{table_name}' schema (flightsql). Ensure the table exists and try again."
    ))]
    UnableToRetrieveSchema { table_name: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

type FlightSqlClient = FlightSqlServiceClient<CookieService<Channel>>;

#[derive(Debug, Clone)]
pub struct FlightSQLFactory {
    client: FlightSqlClient,
    endpoint: String,
    cookie_store: Arc<CookieStore>,
}

impl FlightSQLFactory {
    #[must_use]
    pub fn new(client: FlightSqlClient, endpoint: String, cookie_store: Arc<CookieStore>) -> Self {
        Self {
            client,
            endpoint,
            cookie_store,
        }
    }
}

#[async_trait]
impl Read for FlightSQLFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let table_provider = Arc::new(
            FlightSQLTable::create(
                "flightsql",
                &self.endpoint,
                self.client.clone(),
                table_reference,
                Arc::clone(&self.cookie_store),
            )
            .await?,
        );

        let table_provider = Arc::new(table_provider.create_federated_table_provider());

        Ok(table_provider)
    }
}

#[derive(Debug)]
pub struct FlightSQLTable {
    name: &'static str,
    join_push_down_context: String,
    client: FlightSqlClient,
    table_reference: TableReference,
    schema: SchemaRef,
    cookie_store: Arc<CookieStore>,
}

#[expect(clippy::needless_pass_by_value)]
impl FlightSQLTable {
    pub async fn create(
        name: &'static str,
        endpoint: &str,
        client: FlightSqlClient,
        table_reference: impl Into<TableReference>,
        cookie_store: Arc<CookieStore>,
    ) -> Result<Self> {
        let table_reference: TableReference = table_reference.into();
        let schema = Self::get_schema(client.clone(), table_reference.clone()).await?;
        Ok(Self {
            name,
            client,
            table_reference,
            schema,
            join_push_down_context: format!("endpoint={endpoint}"),
            cookie_store,
        })
    }

    pub fn create_with_schema(
        name: &'static str,
        endpoint: &str,
        client: FlightSqlClient,
        table_reference: impl Into<TableReference>,
        schema: SchemaRef,
        cookie_store: Arc<CookieStore>,
    ) -> Self {
        let table_reference: TableReference = table_reference.into();
        Self {
            name,
            client,
            table_reference,
            schema,
            join_push_down_context: format!("endpoint={endpoint}"),
            cookie_store,
        }
    }

    pub async fn from_static(
        s: &'static str,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self> {
        let cookie_store = Arc::new(CookieStore::new());
        let channel = channel::Endpoint::from_static(s)
            .connect()
            .await
            .context(UnableToConnectToServerSnafu)?;
        let channel = CookieService::new(channel, Arc::clone(&cookie_store));

        let flight_client = FlightServiceClient::new(channel)
            .max_encoding_message_size(MAX_ENCODING_MESSAGE_SIZE)
            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE);

        Self::create(
            "flightsql",
            s,
            FlightSqlServiceClient::new_from_inner(flight_client),
            table_reference.into(),
            cookie_store,
        )
        .await
    }

    fn get_str_from_record_batch(b: &RecordBatch, row: usize, col_name: &str) -> Option<String> {
        if let Some(col_array) = b.column_by_name(col_name)
            && let Some(y) = col_array.as_any().downcast_ref::<array::StringArray>()
        {
            return Some(y.value(row).to_string());
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
        mut client: FlightSqlClient,
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
            }
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
            Arc::clone(&self.cookie_store),
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
    client: FlightSqlClient,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,
    cookie_store: Arc<CookieStore>,
}

impl FlightSqlExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &TableReference,
        client: FlightSqlClient,
        filters: &[Expr],
        limit: Option<usize>,
        cookie_store: Arc<CookieStore>,
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
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            cookie_store,
        })
    }

    fn sql(&self) -> Result<String> {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| quote_identifier(f.name()))
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
            query_to_stream(self.client.clone(), sql, Arc::clone(&self.cookie_store)),
        );

        Ok(Box::pin(stream_adapter))
    }
}

fn query_to_stream(
    mut client: FlightSqlClient,
    sql: String,
    cookie_store: Arc<CookieStore>,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    stream! {
        let flight_info = client
            .execute(sql, None)
            .await
            .map_err(to_execution_error)?;

        for ep in flight_info.endpoint {
            if let Some(tkt) = ep.clone().ticket {
                match get_client_for_flight_endpoint(&client, ep, &cookie_store).await
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
                }
            }
        };
    }
}

fn to_execution_error(e: impl Into<Box<dyn std::error::Error>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()))
}

pub async fn get_client_for_flight_endpoint(
    client: &FlightSqlClient,
    ep: FlightEndpoint,
    cookie_store: &Arc<CookieStore>,
) -> Result<FlightSqlClient, Box<dyn std::error::Error>> {
    if ep.location.is_empty() {
        Ok(client.clone())
    } else {
        let channel = new_tls_flight_channel(&ep.location[0].uri, None).await?;
        let channel = CookieService::new(channel, Arc::clone(cookie_store));
        Ok(FlightSqlServiceClient::new(channel))
    }
}

#[cfg(test)]
mod tests {
    use super::{FlightSqlClient, query_to_stream};
    use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
    use arrow_flight::{
        Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint,
        FlightInfo, Location, PollInfo, PutResult, SchemaResult, Ticket,
    };
    use bytes::Bytes;
    use flight_client::cookie::{CookieService, CookieStore};
    use futures::TryStreamExt;
    use std::net::SocketAddr;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio_stream::Empty as EmptyStream;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Channel;
    use tonic::{Request, Response, Status, async_trait};

    const COOKIE_VALUE: &str = "AWSALB=abc123";

    struct TestServer {
        addr: SocketAddr,
        shutdown: Option<oneshot::Sender<()>>,
        handle: JoinHandle<Result<(), tonic::transport::Error>>,
    }

    impl TestServer {
        async fn start(cookie_seen: Arc<AtomicBool>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener should bind");
            let addr = listener.local_addr().expect("listener should have addr");
            let location = format!("http://{addr}");
            let service = CookieFlightSqlService::new(cookie_seen, location);
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let handle = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(FlightServiceServer::new(service))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
            });
            Self {
                addr,
                shutdown: Some(shutdown_tx),
                handle,
            }
        }

        async fn shutdown(mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            self.handle
                .await
                .expect("server task should finish")
                .expect("server should exit cleanly");
        }
    }

    #[derive(Clone)]
    struct CookieFlightSqlService {
        cookie_required: Arc<AtomicBool>,
        cookie_seen: Arc<AtomicBool>,
        location: String,
    }

    impl CookieFlightSqlService {
        fn new(cookie_seen: Arc<AtomicBool>, location: String) -> Self {
            Self {
                cookie_required: Arc::new(AtomicBool::new(false)),
                cookie_seen,
                location,
            }
        }
    }

    type EmptyResponseStream<T> = EmptyStream<Result<T, Status>>;

    #[async_trait]
    impl FlightService for CookieFlightSqlService {
        type HandshakeStream = EmptyResponseStream<arrow_flight::HandshakeResponse>;
        type ListFlightsStream = EmptyResponseStream<FlightInfo>;
        type DoGetStream = EmptyResponseStream<FlightData>;
        type DoPutStream = EmptyResponseStream<PutResult>;
        type DoExchangeStream = EmptyResponseStream<FlightData>;
        type DoActionStream = EmptyResponseStream<arrow_flight::Result>;
        type ListActionsStream = EmptyResponseStream<ActionType>;

        async fn handshake(
            &self,
            _request: Request<tonic::Streaming<arrow_flight::HandshakeRequest>>,
        ) -> Result<Response<Self::HandshakeStream>, Status> {
            Err(Status::unimplemented("handshake"))
        }

        async fn list_flights(
            &self,
            _request: Request<Criteria>,
        ) -> Result<Response<Self::ListFlightsStream>, Status> {
            Err(Status::unimplemented("list_flights"))
        }

        async fn get_flight_info(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> Result<Response<FlightInfo>, Status> {
            self.cookie_required.store(true, Ordering::SeqCst);
            let endpoint = FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: Bytes::from_static(b"ticket"),
                }),
                location: vec![Location {
                    uri: self.location.clone(),
                }],
                expiration_time: None,
                app_metadata: Bytes::new(),
            };

            let mut response = Response::new(FlightInfo {
                schema: Bytes::new(),
                flight_descriptor: None,
                endpoint: vec![endpoint],
                total_records: -1,
                total_bytes: -1,
                ordered: false,
                app_metadata: Bytes::new(),
            });
            response.metadata_mut().insert(
                "set-cookie",
                format!("{COOKIE_VALUE}; Path=/")
                    .parse()
                    .expect("cookie header should be valid"),
            );
            Ok(response)
        }

        async fn poll_flight_info(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> Result<Response<PollInfo>, Status> {
            Err(Status::unimplemented("poll_flight_info"))
        }

        async fn get_schema(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> Result<Response<SchemaResult>, Status> {
            Err(Status::unimplemented("get_schema"))
        }

        async fn do_get(
            &self,
            request: Request<Ticket>,
        ) -> Result<Response<Self::DoGetStream>, Status> {
            if self.cookie_required.load(Ordering::SeqCst) {
                let cookie_header = request
                    .metadata()
                    .get("cookie")
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| Status::unauthenticated("cookie missing"))?;
                if !cookie_header.contains(COOKIE_VALUE) {
                    return Err(Status::unauthenticated("cookie missing"));
                }
            }
            self.cookie_seen.store(true, Ordering::SeqCst);
            Ok(Response::new(tokio_stream::empty()))
        }

        async fn do_put(
            &self,
            _request: Request<tonic::Streaming<FlightData>>,
        ) -> Result<Response<Self::DoPutStream>, Status> {
            Err(Status::unimplemented("do_put"))
        }

        async fn do_exchange(
            &self,
            _request: Request<tonic::Streaming<FlightData>>,
        ) -> Result<Response<Self::DoExchangeStream>, Status> {
            Err(Status::unimplemented("do_exchange"))
        }

        async fn do_action(
            &self,
            _request: Request<Action>,
        ) -> Result<Response<Self::DoActionStream>, Status> {
            Err(Status::unimplemented("do_action"))
        }

        async fn list_actions(
            &self,
            _request: Request<Empty>,
        ) -> Result<Response<Self::ListActionsStream>, Status> {
            Err(Status::unimplemented("list_actions"))
        }
    }

    #[tokio::test]
    async fn query_to_stream_sends_cookie_to_endpoint_client() {
        let cookie_seen = Arc::new(AtomicBool::new(false));
        let server = TestServer::start(Arc::clone(&cookie_seen)).await;
        let cookie_store = Arc::new(CookieStore::new());
        let channel = Channel::from_shared(format!("http://{}", server.addr))
            .expect("channel should parse")
            .connect()
            .await
            .expect("channel should connect");
        let channel = CookieService::new(channel, Arc::clone(&cookie_store));
        let client: FlightSqlClient =
            arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

        let batches = query_to_stream(client, "SELECT 1".to_string(), Arc::clone(&cookie_store))
            .try_collect::<Vec<_>>()
            .await
            .expect("query should succeed");
        assert!(batches.is_empty());
        assert!(cookie_seen.load(Ordering::SeqCst));

        server.shutdown().await;
    }
}
