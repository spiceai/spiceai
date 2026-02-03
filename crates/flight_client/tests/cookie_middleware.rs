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

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightDescriptor, FlightEndpoint, FlightInfo, Location,
    PollInfo, SchemaResult, Ticket,
};
use flight_client::cookie::{CookieService, CookieStore};
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
    async fn start(service: CookieFlightService) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
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
struct CookieFlightService {
    cookie_required: Arc<AtomicBool>,
    cookie_seen: Arc<AtomicBool>,
}

impl CookieFlightService {
    fn new(cookie_seen: Arc<AtomicBool>) -> Self {
        Self {
            cookie_required: Arc::new(AtomicBool::new(false)),
            cookie_seen,
        }
    }
}

type EmptyResponseStream<T> = EmptyStream<Result<T, Status>>;

#[async_trait]
impl FlightService for CookieFlightService {
    type HandshakeStream = EmptyResponseStream<arrow_flight::HandshakeResponse>;
    type ListFlightsStream = EmptyResponseStream<FlightInfo>;
    type DoGetStream = EmptyResponseStream<arrow_flight::FlightData>;
    type DoPutStream = EmptyResponseStream<arrow_flight::PutResult>;
    type DoExchangeStream = EmptyResponseStream<arrow_flight::FlightData>;
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
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        if self.cookie_required.load(Ordering::SeqCst) {
            let cookie_header = request
                .metadata()
                .get("cookie")
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| Status::unauthenticated("cookie missing"))?;
            if !cookie_header.contains(COOKIE_VALUE) {
                return Err(Status::unauthenticated("cookie missing"));
            }
            self.cookie_seen.store(true, Ordering::SeqCst);
        }
        self.cookie_required.store(true, Ordering::SeqCst);

        let mut response = Response::new(FlightInfo {
            schema: bytes::Bytes::new(),
            flight_descriptor: None,
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: bytes::Bytes::from_static(b"noop"),
                }),
                location: vec![Location { uri: String::new() }],
                expiration_time: None,
                app_metadata: bytes::Bytes::new(),
            }],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: bytes::Bytes::new(),
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
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get"))
    }

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<arrow_flight::FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<arrow_flight::FlightData>>,
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
async fn cookie_middleware_persists_cookie_across_requests() {
    let cookie_seen = Arc::new(AtomicBool::new(false));
    let service = CookieFlightService::new(Arc::clone(&cookie_seen));
    let server = TestServer::start(service).await;

    let cookie_store = Arc::new(CookieStore::new());
    let channel = Channel::from_shared(format!("http://{}", server.addr))
        .expect("channel should parse")
        .connect()
        .await
        .expect("channel should connect");
    let channel = CookieService::new(channel, Arc::clone(&cookie_store));
    let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel);

    let descriptor = FlightDescriptor::new_path(vec!["cookie".to_string()]);
    client
        .get_flight_info(descriptor.clone())
        .await
        .expect("first request should succeed");
    client
        .get_flight_info(descriptor)
        .await
        .expect("second request should include cookie");

    assert!(cookie_seen.load(Ordering::SeqCst));
    server.shutdown().await;
}
