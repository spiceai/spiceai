use crate::datafusion::DataFusion;
use crate::dataupdate::{DataUpdate, UpdateType};
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator};
use arrow_flight::{FlightEndpoint, SchemaAsIpc};
use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use datafusion::arrow::error::ArrowError;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use futures::stream::{self, BoxStream, StreamExt};
use futures::Stream;
use snafu::prelude::*;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};

pub struct Service {
    datafusion: Arc<DataFusion>,
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

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let request = request.into_inner();

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let table_path = ListingTableUrl::parse(&request.path[0]).map_err(to_tonic_err)?;

        let schema = listing_options
            .infer_schema(&self.datafusion.ctx.state(), &table_path)
            .await
            .map_err(to_tonic_err)?;

        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema_result = SchemaAsIpc::new(&schema, &options)
            .try_into()
            .map_err(|e: ArrowError| Status::internal(e.to_string()))?;

        Ok(Response::new(schema_result))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        match std::str::from_utf8(&ticket.ticket) {
            Ok(sql) => {
                let df = self.datafusion.ctx.sql(sql).await.map_err(to_tonic_err)?;
                let schema = df.schema().clone().into();
                let results = df.collect().await.map_err(to_tonic_err)?;
                if results.is_empty() {
                    return Err(Status::internal("There were no results from ticket"));
                }

                let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
                let schema_flight_data = SchemaAsIpc::new(&schema, &options);

                let mut flights = vec![FlightData::from(schema_flight_data)];

                let encoder = IpcDataGenerator::default();
                let mut tracker = DictionaryTracker::new(false);

                for batch in &results {
                    let (flight_dictionaries, flight_batch) = encoder
                        .encoded_batch(batch, &mut tracker, &options)
                        .map_err(|e: ArrowError| Status::internal(e.to_string()))?;

                    flights.extend(flight_dictionaries.into_iter().map(Into::into));
                    flights.push(flight_batch.into());
                }

                let output = futures::stream::iter(flights.into_iter().map(Ok));
                Ok(Response::new(Box::pin(output) as Self::DoGetStream))
            }
            Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e:?}"))),
        }
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
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

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        // THIS IS PLACEHOLDER NO-OP AUTH THAT DOES NOT CHECK THE PROVIDED TOKEN AND SIMPLY RETURNS A UUID.
        // TODO: Implement proper auth.
        let token = Uuid::new_v4().to_string();
        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        let str = format!("Bearer {token}");
        let mut resp: Response<Pin<Box<dyn Stream<Item = Result<_, _>> + Send>>> =
            Response::new(Box::pin(output));
        let md = MetadataValue::try_from(str)
            .map_err(|_| Status::internal("generated authorization could not be parsed"))?;
        resp.metadata_mut().insert("authorization", md);
        Ok(resp)
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut streaming_flight = request.into_inner();

        let Ok(Some(message)) = streaming_flight.message().await else {
            return Err(Status::invalid_argument("No flight data provided"));
        };
        let Some(fd) = &message.flight_descriptor else {
            return Err(Status::invalid_argument("No flight descriptor provided"));
        };
        if fd.path.is_empty() {
            return Err(Status::invalid_argument("No path provided"));
        };

        let path = fd.path.join(".");

        let Some(backend) = self.datafusion.get_backend(&path) else {
            return Err(Status::invalid_argument(format!(
                "No backend registered for path: {path:?}",
            )));
        };
        let backend = Arc::clone(backend);

        let schema = try_schema_from_flatbuffer_bytes(&message.data_header).map_err(|e| {
            Status::internal(format!("Unable to get schema from data header: {e:?}"))
        })?;
        let schema = Arc::new(schema);
        let dictionaries_by_id = Arc::new(HashMap::new());

        // Sometimes the first message only contains the schema and no data
        let first_batch = arrow_flight::utils::flight_data_to_arrow_batch(
            &message,
            schema.clone(),
            &dictionaries_by_id,
        )
        .ok();

        let mut batches = vec![];
        if let Some(first_batch) = first_batch {
            batches.push(first_batch);
        }

        let response_stream = stream::unfold(streaming_flight, move |mut flight| {
            let schema = Arc::clone(&schema);
            let dictionaries_by_id = Arc::clone(&dictionaries_by_id);
            let backend = Arc::clone(&backend);
            async move {
                match flight.message().await {
                    Ok(Some(message)) => {
                        let new_batch = match arrow_flight::utils::flight_data_to_arrow_batch(
                            &message,
                            schema.clone(),
                            &dictionaries_by_id,
                        ) {
                            Ok(batches) => batches,
                            Err(e) => {
                                tracing::trace!("Unable to convert flight data to batches: {e:?}");
                                return None;
                            }
                        };
                        tracing::trace!("Received batch with {} rows", new_batch.num_rows());

                        if let Err(err) = backend
                            .add_data(DataUpdate {
                                data: vec![new_batch],
                                log_sequence_number: None,
                                update_type: UpdateType::Append,
                            })
                            .await
                            .map_err(|e| Status::internal(format!("Unable to add data: {e:?}")))
                        {
                            return Some((Err(err), flight));
                        };

                        Some((Ok(PutResult::default()), flight))
                    }
                    Ok(None) => {
                        // End of the stream
                        None
                    }
                    Err(e) => Some((
                        Err(Status::internal(format!("Error reading message: {e:?}"))),
                        flight,
                    )),
                }
            }
        });

        Ok(Response::new(response_stream.boxed()))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
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
    #[snafu(display("A test error"))]
    Arrow { source: ArrowError },

    #[snafu(display("Unable to register parquet file"))]
    RegisterParquet { source: crate::datafusion::Error },

    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to start Flight server"))]
    UnableToStartFlightServer { source: tonic::transport::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn start(bind_address: std::net::SocketAddr, df: Arc<DataFusion>) -> Result<()> {
    let service = Service {
        datafusion: df.clone(),
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
