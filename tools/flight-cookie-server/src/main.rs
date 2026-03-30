/*
Copyright 2026 The Spice.ai OSS Authors

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

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::GetTablesBuilder;
use arrow_flight::sql::{Any, Command};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeResponse, Location, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use clap::Parser;
use futures::{Stream, TryStreamExt};
use prost::Message;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, async_trait};

const TABLES_TICKET: &[u8] = b"tables";
const QUERY_TICKET: &[u8] = b"query";

#[derive(Parser, Debug)]
#[clap(about = "Spice.ai Flight cookie test server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:50051")]
    addr: String,

    #[arg(long, default_value = "AWSALB=abc123")]
    cookie: String,

    #[arg(long, default_value_t = true)]
    require_cookie: bool,

    #[arg(long, default_value = "default")]
    catalog: String,

    #[arg(long, default_value = "public")]
    schema: String,

    #[arg(long, default_value = "cookies")]
    table: String,

    #[arg(long, default_value = "value")]
    column: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let addr: SocketAddr = args
        .addr
        .parse()
        .map_err(|err| format!("Invalid address '{}': {err}", args.addr))?;
    let location = format!("http://{addr}");

    let table_info = Arc::new(TableInfo::new(
        &args.catalog,
        &args.schema,
        &args.table,
        &args.column,
    )?);

    let service = CookieFlightService::new(args.cookie, args.require_cookie, location, table_info);

    tracing::info!("Starting Flight cookie server on {addr}");
    let listener = TcpListener::bind(addr).await?;
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;

    Ok(())
}

#[derive(Clone)]
struct TableInfo {
    table_schema: SchemaRef,
    tables_batch: RecordBatch,
    query_batches: Vec<RecordBatch>,
}

impl TableInfo {
    fn new(
        catalog: &str,
        schema: &str,
        table: &str,
        column: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            column,
            DataType::Int32,
            false,
        )]));
        let query_batch = RecordBatch::try_new(
            Arc::clone(&table_schema),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )?;

        let mut tables_builder = GetTablesBuilder::new(
            None::<String>,
            None::<String>,
            None::<String>,
            Vec::<String>::new(),
            true,
        );
        tables_builder.append(catalog, schema, table, "TABLE", table_schema.as_ref())?;
        let tables_batch = tables_builder.build()?;

        Ok(Self {
            table_schema,
            tables_batch,
            query_batches: vec![query_batch],
        })
    }
}

#[derive(Clone)]
struct CookieFlightService {
    cookie: String,
    require_cookie: bool,
    location: String,
    table_info: Arc<TableInfo>,
}

impl CookieFlightService {
    fn new(
        cookie: String,
        require_cookie: bool,
        location: String,
        table_info: Arc<TableInfo>,
    ) -> Self {
        Self {
            cookie,
            require_cookie,
            location,
            table_info,
        }
    }
}

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[async_trait]
impl FlightService for CookieFlightService {
    type HandshakeStream = ResponseStream<arrow_flight::HandshakeResponse>;
    type ListFlightsStream = ResponseStream<FlightInfo>;
    type DoGetStream = ResponseStream<FlightData>;
    type DoPutStream = ResponseStream<PutResult>;
    type DoExchangeStream = ResponseStream<FlightData>;
    type DoActionStream = ResponseStream<arrow_flight::Result>;
    type ListActionsStream = ResponseStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let response_stream: Self::HandshakeStream =
            Box::pin(tokio_stream::iter(vec![Ok(HandshakeResponse {
                protocol_version: 0,
                payload: Bytes::new(),
            })]));
        let mut response = Response::new(response_stream);
        response.metadata_mut().insert(
            "set-cookie",
            format!("{}; Path=/", self.cookie)
                .parse()
                .map_err(|_| Status::internal("invalid cookie header"))?,
        );
        Ok(response)
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Ok(Response::new(Box::pin(tokio_stream::empty())))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let ticket = match decode_command(&descriptor) {
            Some(Command::CommandGetTables(_)) => TABLES_TICKET,
            _ => QUERY_TICKET,
        };

        let endpoint = FlightEndpoint {
            ticket: Some(Ticket {
                ticket: Bytes::from_static(ticket),
            }),
            location: vec![Location {
                uri: self.location.clone(),
            }],
            expiration_time: None,
            app_metadata: Bytes::new(),
        };

        let mut response = Response::new(FlightInfo {
            schema: Bytes::new(),
            flight_descriptor: Some(descriptor),
            endpoint: vec![endpoint],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: Bytes::new(),
        });
        response.metadata_mut().insert(
            "set-cookie",
            format!("{}; Path=/", self.cookie)
                .parse()
                .map_err(|_| Status::internal("invalid cookie header"))?,
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
        if self.require_cookie {
            let cookie_header = request
                .metadata()
                .get("cookie")
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| Status::unauthenticated("cookie missing"))?;
            if !cookie_header.contains(&self.cookie) {
                return Err(Status::unauthenticated("cookie missing"));
            }
            tracing::info!("Received cookie header: {cookie_header}");
        }

        let ticket = request.into_inner().ticket;
        let (schema, batches) = if ticket.as_ref() == TABLES_TICKET {
            (
                self.table_info.tables_batch.schema(),
                vec![self.table_info.tables_batch.clone()],
            )
        } else {
            (
                Arc::clone(&self.table_info.table_schema),
                self.table_info.query_batches.clone(),
            )
        };

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(
                batches.into_iter().map(Ok::<_, FlightError>),
            ))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
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
        Ok(Response::new(Box::pin(tokio_stream::empty())))
    }
}

fn decode_command(descriptor: &FlightDescriptor) -> Option<Command> {
    if descriptor.cmd.is_empty() {
        return None;
    }
    let any = Any::decode(descriptor.cmd.as_ref()).ok()?;
    Command::try_from(any).ok()
}
