use crate::datafusion::DataFusion;
use crate::dataupdate::DataUpdate;
use arrow::datatypes::Schema;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::{Action, ActionType, Criteria, IpcMessage, SchemaResult};
use arrow_ipc::writer::IpcWriteOptions;
use bytes::Bytes;
use datafusion::execution::SendableRecordBatchStream;
use futures::stream::{self, BoxStream, StreamExt};
use futures::{Stream, TryStreamExt};
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
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    SchemaAsIpc, Ticket,
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

    // If the ticket isn't a sql command, then try interpreting the ticket as a raw SQL query.
    // async fn do_get_fallback(
    //     &self,
    //     request: Request<Ticket>,
    //     _message: arrow_flight::sql::Any,
    // ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
    //     let datafusion = Arc::clone(&self.datafusion);
    //     let ticket = request.into_inner();
    //     tracing::trace!("do_get_fallback: {ticket:?}");
    //     match std::str::from_utf8(&ticket.ticket) {
    //         Ok(sql) => {
    //             let output = Self::sql_to_flight_stream(datafusion, sql.to_owned()).await?;
    //             Ok(Response::new(
    //                 Box::pin(output) as <Service as FlightService>::DoGetStream
    //             ))
    //         }
    //         Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e:?}"))),
    //     }
    // }

    // #[allow(clippy::unused_async)]
    // async fn get_flight_info_fallback(
    //     &self,
    //     cmd: sql::Command,
    //     request: Request<FlightDescriptor>,
    // ) -> Result<Response<FlightInfo>, Status> {
    //     tracing::trace!("get_flight_info_fallback: {cmd:?}");
    //     let fd = request.into_inner();
    //     Ok(Response::new(FlightInfo {
    //         flight_descriptor: Some(fd.clone()),
    //         endpoint: vec![FlightEndpoint {
    //             ticket: Some(Ticket { ticket: fd.cmd }),
    //             ..Default::default()
    //         }],
    //         ..Default::default()
    //     }))
    // }
}

fn record_batches_to_flight_stream(
    record_batches: Vec<arrow::record_batch::RecordBatch>,
) -> impl Stream<Item = Result<FlightData, Status>> {
    FlightDataEncoderBuilder::new()
        .build(stream::iter(record_batches.into_iter().map(Ok)))
        .map_err(to_tonic_err)
}

#[allow(clippy::needless_pass_by_value)]
fn to_tonic_err<E>(e: E) -> Status
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
