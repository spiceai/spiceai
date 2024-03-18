use crate::datafusion::DataFusion;
use crate::dataupdate::DataUpdate;
use arrow::array::RecordBatch;
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
use std::sync::Arc;
use tokio::sync::broadcast::{error, Sender};
use tokio::sync::RwLock;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

mod actions;
mod do_exchange;
mod do_get;
mod do_put;
mod flightsql;
mod get_flight_info;
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
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        handshake::handle()
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        tracing::trace!("list_flights - unimplemented");
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        get_flight_info::handle(self, request).await
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        tracing::trace!("get_schema - unimplemented");
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        do_get::handle(self, request).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        do_put::handle(self, request).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        do_exchange::handle(self, request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        actions::do_action(self, request).await
    }

    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(actions::list())
    }
}

impl Service {
    async fn get_arrow_schema(
        datafusion: Arc<RwLock<DataFusion>>,
        sql: String,
    ) -> Result<Schema, Status> {
        let df = datafusion
            .read()
            .await
            .ctx
            .sql(&sql)
            .await
            .map_err(to_tonic_err)?;
        Ok(df.schema().into())
    }

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
}

fn record_batches_to_flight_stream(
    record_batches: Vec<RecordBatch>,
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
