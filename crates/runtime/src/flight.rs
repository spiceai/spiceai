use crate::datafusion::DataFusion;
use crate::dataupdate::DataUpdate;
use arrow::datatypes::Schema;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::sql::ProstMessageExt;
use arrow_flight::{sql, Action, ActionType, Criteria, IpcMessage, SchemaResult};
use arrow_ipc::writer::IpcWriteOptions;
use bytes::Bytes;
use datafusion::datasource::TableType;
use datafusion::execution::SendableRecordBatchStream;
use futures::stream::{self, BoxStream, StreamExt};
use futures::{Stream, TryStreamExt};
use prost::Message;
use snafu::prelude::*;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::RwLock;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

mod actions;
mod do_exchange;
mod do_get;
mod do_put;
mod flightsql;
mod get_flight_info;
mod get_schema;
mod handshake;

use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer},
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaAsIpc, Ticket,
};

pub struct Service {
    datafusion: Arc<RwLock<DataFusion>>,
    channel_map: Arc<RwLock<HashMap<String, Arc<Sender<DataUpdate>>>>>,
}

#[tonic::async_trait]
impl FlightService for Service {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        tracing::trace!("handshake");
        handshake::handle()
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        tracing::trace!("list_flights");
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::trace!("get_flight_info");
        get_flight_info::handle(self, request).await
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        tracing::trace!("get_schema");
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        tracing::trace!("do_get");
        do_get::handle(self, request).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        tracing::trace!("do_put");
        do_put::handle(self, request).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        tracing::trace!("do_exchange");
        do_exchange::handle(self, request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        tracing::trace!("do_action");
        actions::do_action(self, request).await
    }

    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        tracing::trace!("list_actions");
        Ok(actions::list())
    }
}

impl Service {
    async fn get_arrow_schema_and_size_sql(
        datafusion: Arc<RwLock<DataFusion>>,
        sql: String,
    ) -> Result<(Schema, usize), Status> {
        let df = datafusion
            .read()
            .await
            .ctx
            .sql(&sql)
            .await
            .map_err(to_tonic_err)?;

        let schema = df.schema();
        let arrow_schema: Schema = schema.into();

        let size = df.count().await.map_err(to_tonic_err)?;

        Ok((arrow_schema, size))
    }

    fn serialize_schema(schema: &Schema) -> Result<Bytes, Status> {
        let message: IpcMessage = SchemaAsIpc::new(schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(to_tonic_err)?;
        let IpcMessage(schema_bytes) = message;

        Ok(schema_bytes)
    }

    async fn sql_to_flight_stream(
        datafusion: Arc<RwLock<DataFusion>>,
        sql: String,
    ) -> Result<BoxStream<'static, Result<FlightData, Status>>, Status> {
        let df = datafusion
            .read()
            .await
            .ctx
            .sql(&sql)
            .await
            .map_err(to_tonic_err)?;
        let schema = df.schema().clone().into();
        let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
        let schema_as_ipc = SchemaAsIpc::new(&schema, &options);
        let schema_flight_data = FlightData::from(schema_as_ipc);

        let batches_stream: SendableRecordBatchStream =
            df.execute_stream().await.map_err(to_tonic_err)?;

        let batches_stream = batches_stream
            .then(move |batch_result| {
                let options_clone = options.clone();
                async move {
                    let encoder = IpcDataGenerator::default();
                    let mut tracker = DictionaryTracker::new(false);

                    match batch_result {
                        Ok(batch) => {
                            let (flight_dictionaries, flight_batch) = encoder
                                .encoded_batch(&batch, &mut tracker, &options_clone)
                                .map_err(|e| Status::internal(e.to_string()))?;

                            let mut flights: Vec<FlightData> =
                                flight_dictionaries.into_iter().map(Into::into).collect();
                            flights.push(flight_batch.into());
                            Ok(flights)
                        }
                        Err(e) => Err(Status::internal(e.to_string())),
                    }
                }
            })
            .map(|result| {
                // Convert Result<Vec<FlightData>, Status> into Stream of Result<FlightData, Status>
                match result {
                    Ok(flights) => stream::iter(flights.into_iter().map(Ok)).left_stream(),
                    Err(e) => stream::once(async { Err(e) }).right_stream(),
                }
            })
            .flatten();

        let flights_stream = stream::once(async { Ok(schema_flight_data) }).chain(batches_stream);

        Ok(flights_stream.boxed())
    }

    /// Get a FlightInfo for listing catalogs.
    #[allow(clippy::unused_async)]
    async fn get_flight_info_catalogs(
        &self,
        query: sql::CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::trace!("get_flight_info_catalogs");
        let fd = request.into_inner();

        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        });

        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(fd);

        Ok(Response::new(info))
    }

    async fn do_get_catalogs(
        &self,
        query: sql::CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        tracing::trace!("do_get_catalogs: {query:?}");
        let mut builder = query.into_builder();

        let catalog_names = self.datafusion.read().await.ctx.catalog_names();

        for catalog in catalog_names {
            builder.append(catalog);
        }

        let record_batch = builder.build().map_err(to_tonic_err)?;

        Ok(Response::new(
            Box::pin(record_batches_to_flight_stream(vec![record_batch])?)
                as <Service as FlightService>::DoGetStream,
        ))
    }

    /// Get a FlightInfo for listing schemas.
    #[allow(clippy::unused_async)]
    async fn get_flight_info_schemas(
        &self,
        query: sql::CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::trace!("get_flight_info_schemas");
        let fd = request.into_inner();

        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        });

        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(fd);

        Ok(Response::new(info))
    }

    async fn do_get_schemas(
        &self,
        query: sql::CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let catalog = &query.catalog;
        tracing::trace!("do_get_schemas: {query:?}");
        let filtered_catalogs = match catalog {
            Some(catalog) => vec![catalog.to_string()],
            None => self.datafusion.read().await.ctx.catalog_names(),
        };
        let mut builder = query.into_builder();

        for catalog in filtered_catalogs {
            let catalog_provider = self
                .datafusion
                .read()
                .await
                .ctx
                .catalog(&catalog)
                .ok_or_else(|| {
                    Status::internal(format!("unable to get catalog provider for {catalog}"))
                })?;
            for schema in catalog_provider.schema_names() {
                builder.append(&catalog, schema);
            }
        }

        let record_batch = builder.build().map_err(to_tonic_err)?;

        Ok(Response::new(
            Box::pin(record_batches_to_flight_stream(vec![record_batch])?)
                as <Service as FlightService>::DoGetStream,
        ))
    }

    async fn do_get_tables(
        &self,
        query: sql::CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let catalog = &query.catalog;
        tracing::trace!("do_get_tables: {query:?}");
        let filtered_catalogs = match catalog {
            Some(catalog) => vec![catalog.to_string()],
            None => self.datafusion.read().await.ctx.catalog_names(),
        };
        let mut builder = query.into_builder();

        for catalog_name in filtered_catalogs {
            let catalog_provider = self
                .datafusion
                .read()
                .await
                .ctx
                .catalog(&catalog_name)
                .ok_or_else(|| {
                    Status::internal(format!("unable to get catalog provider for {catalog_name}"))
                })?;

            for schema_name in catalog_provider.schema_names() {
                let Some(schema_provider) = catalog_provider.schema(&schema_name) else {
                    continue;
                };

                for table_name in schema_provider.table_names() {
                    let Some(table_provider) = schema_provider.table(&table_name).await else {
                        continue;
                    };

                    let table_type = table_type_name(table_provider.table_type());

                    builder.append(
                        &catalog_name,
                        &schema_name,
                        &table_name,
                        table_type,
                        table_provider.schema().as_ref(),
                    )?;
                }
            }
        }

        let record_batch = builder.build().map_err(to_tonic_err)?;

        Ok(Response::new(
            Box::pin(record_batches_to_flight_stream(vec![record_batch])?)
                as <Service as FlightService>::DoGetStream,
        ))
    }

    async fn do_get_statement(
        &self,
        ticket: sql::TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let datafusion = Arc::clone(&self.datafusion);
        tracing::trace!("do_get_statement: {ticket:?}");
        match std::str::from_utf8(&ticket.statement_handle) {
            Ok(sql) => {
                let output = Self::sql_to_flight_stream(datafusion, sql.to_owned()).await?;
                Ok(Response::new(
                    Box::pin(output) as <Service as FlightService>::DoGetStream
                ))
            }
            Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e:?}"))),
        }
    }

    async fn do_get_prepared_statement(
        &self,
        query: sql::CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let datafusion = Arc::clone(&self.datafusion);
        tracing::trace!("do_get_prepared_statement: {query:?}");
        match std::str::from_utf8(&query.prepared_statement_handle) {
            Ok(sql) => {
                let output = Self::sql_to_flight_stream(datafusion, sql.to_owned()).await?;
                Ok(Response::new(
                    Box::pin(output) as <Service as FlightService>::DoGetStream
                ))
            }
            Err(e) => Err(Status::invalid_argument(format!(
                "Invalid prepared statement handle: {e:?}"
            ))),
        }
    }

    // If the ticket isn't a sql command, then try interpreting the ticket as a raw SQL query.
    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: arrow_flight::sql::Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let datafusion = Arc::clone(&self.datafusion);
        let ticket = request.into_inner();
        tracing::trace!("do_get_fallback: {ticket:?}");
        match std::str::from_utf8(&ticket.ticket) {
            Ok(sql) => {
                let output = Self::sql_to_flight_stream(datafusion, sql.to_owned()).await?;
                Ok(Response::new(
                    Box::pin(output) as <Service as FlightService>::DoGetStream
                ))
            }
            Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e:?}"))),
        }
    }

    #[allow(clippy::unused_async)]
    async fn get_flight_info_tables(
        &self,
        query: sql::CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let fd = request.into_inner();
        tracing::trace!("get_flight_info_tables: {query:?}");
        Ok(Response::new(FlightInfo {
            flight_descriptor: Some(fd.clone()),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket { ticket: fd.cmd }),
                ..Default::default()
            }],
            ..Default::default()
        }))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        handle: sql::CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::trace!("get_flight_info_prepared_statement: {handle:?}");

        let sql = match std::str::from_utf8(&handle.prepared_statement_handle) {
            Ok(sql) => sql.to_string(),
            Err(e) => {
                return Err(Status::invalid_argument(format!(
                    "Invalid prepared statement handle: {e:?}"
                )))
            }
        };

        let (arrow_schema, num_rows) =
            Self::get_arrow_schema_and_size_sql(Arc::clone(&self.datafusion), sql)
                .await
                .map_err(to_tonic_err)?;

        tracing::trace!("get_flight_info_prepared_statement: arrow_schema={arrow_schema:?} num_rows={num_rows:?}");

        let fd = request.into_inner();

        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: handle.as_any().encode_to_vec().into(),
        });

        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .try_with_schema(&arrow_schema)
            .map_err(to_tonic_err)?
            .with_descriptor(fd)
            .with_total_records(num_rows.try_into().map_err(to_tonic_err)?);

        Ok(Response::new(info))
    }

    #[allow(clippy::unused_async)]
    async fn get_flight_info_fallback(
        &self,
        cmd: sql::Command,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::trace!("get_flight_info_fallback: {cmd:?}");
        let fd = request.into_inner();
        Ok(Response::new(FlightInfo {
            flight_descriptor: Some(fd.clone()),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket { ticket: fd.cmd }),
                ..Default::default()
            }],
            ..Default::default()
        }))
    }

    /// Create a prepared statement from given SQL statement.
    async fn do_action_create_prepared_statement(
        &self,
        statement: sql::ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<sql::ActionCreatePreparedStatementResult, Status> {
        tracing::trace!("do_action_create_prepared_statement: {statement:?}");
        let (arrow_schema, _) = Self::get_arrow_schema_and_size_sql(
            Arc::clone(&self.datafusion),
            statement.query.clone(),
        )
        .await
        .map_err(to_tonic_err)?;

        let schema_bytes = Self::serialize_schema(&arrow_schema)?;

        Ok(sql::ActionCreatePreparedStatementResult {
            prepared_statement_handle: statement.query.into(),
            dataset_schema: schema_bytes,
            ..Default::default()
        })
    }
}

fn table_type_name(table_type: TableType) -> &'static str {
    match table_type {
        // from https://github.com/apache/arrow-datafusion/blob/26b8377b0690916deacf401097d688699026b8fb/datafusion/core/src/catalog/information_schema.rs#L284-L288
        TableType::Base => "BASE TABLE",
        TableType::View => "VIEW",
        TableType::Temporary => "LOCAL TEMPORARY",
    }
}

pub(crate) fn record_batches_to_flight_stream(
    record_batches: Vec<arrow::record_batch::RecordBatch>,
) -> Result<impl Stream<Item = Result<FlightData, Status>>, Status> {
    let flight_stream = FlightDataEncoderBuilder::new()
        .build(stream::iter(record_batches.into_iter().map(Ok)))
        .map_err(to_tonic_err);

    Ok(flight_stream)
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn to_tonic_err<E>(e: E) -> Status
where
    E: std::fmt::Debug,
{
    Status::internal(format!("{e:?}"))
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to register parquet file: {source}"))]
    RegisterParquet { source: crate::datafusion::Error },

    #[snafu(display("{source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to start Flight server: {source}"))]
    UnableToStartFlightServer { source: tonic::transport::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn start(bind_address: std::net::SocketAddr, df: Arc<RwLock<DataFusion>>) -> Result<()> {
    let service = Service {
        datafusion: df.clone(),
        channel_map: Arc::new(RwLock::new(HashMap::new())),
    };
    let svc = FlightServiceServer::new(service);

    tracing::info!("Spice Runtime Flight listening on {bind_address}");
    metrics::counter!("spiced_runtime_flight_server_start").increment(1);

    Server::builder()
        .add_service(svc)
        .serve(bind_address)
        .await
        .context(UnableToStartFlightServerSnafu)?;

    Ok(())
}
