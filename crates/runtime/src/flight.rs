use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator};
use std::sync::Arc;

use arrow_flight::{FlightEndpoint, SchemaAsIpc};
use datafusion::arrow::error::ArrowError;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use futures::stream::BoxStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};

use crate::datafusion::DataFusion;

pub struct ServiceImpl {
    data_fusion: DataFusion,
}

#[tonic::async_trait]
impl FlightService for ServiceImpl {
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
            .infer_schema(&self.data_fusion.ctx.state(), &table_path)
            .await
            .unwrap();

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
                let df = self.data_fusion.ctx.sql(sql).await.map_err(to_tonic_err)?;
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
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let fd = _request.into_inner();
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
        // TODO: Implement auth
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
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

fn to_tonic_err(e: datafusion::error::DataFusionError) -> Status {
    Status::internal(format!("{e:?}"))
}

pub async fn start(bind_address: std::net::SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let df = DataFusion::new();
    df.register_parquet("test", "./test.parquet").await?;

    let service = ServiceImpl { data_fusion: df };
    let svc = FlightServiceServer::new(service);

    tracing::info!("Spice Runtime Flight listening on {bind_address}");
    metrics::counter!("spiced_runtime_flight_server_start").increment(1);

    Server::builder()
        .add_service(svc)
        .serve(bind_address)
        .await?;

    Ok(())
}
