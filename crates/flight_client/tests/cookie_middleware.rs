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
#![expect(clippy::expect_used)]

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightDescriptor, FlightEndpoint, FlightInfo, Location,
    PollInfo, SchemaResult, Ticket,
};
use flight_client::cookie::{CookieService, CookieStore};
use flight_client::{Credentials, FlightClient};
use secrecy::SecretString;
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
    authorization_seen: Arc<parking_lot::Mutex<Vec<String>>>,
    do_get_reply: Vec<arrow_flight::FlightData>,
}

impl CookieFlightService {
    fn new(cookie_seen: Arc<AtomicBool>) -> Self {
        Self {
            cookie_required: Arc::new(AtomicBool::new(false)),
            cookie_seen,
            authorization_seen: Arc::default(),
            do_get_reply: Vec::new(),
        }
    }
}

type EmptyResponseStream<T> = EmptyStream<Result<T, Status>>;

#[async_trait]
impl FlightService for CookieFlightService {
    type HandshakeStream = EmptyResponseStream<arrow_flight::HandshakeResponse>;
    type ListFlightsStream = EmptyResponseStream<FlightInfo>;
    type DoGetStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<arrow_flight::FlightData, Status>>>;
    type DoPutStream = EmptyResponseStream<arrow_flight::PutResult>;
    type DoExchangeStream = EmptyResponseStream<arrow_flight::FlightData>;
    type DoActionStream = EmptyResponseStream<arrow_flight::Result>;
    type ListActionsStream = EmptyResponseStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Ok(Response::new(tokio_stream::empty()))
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
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        if let Some(value) = request.metadata().get("authorization") {
            self.authorization_seen.lock().push(
                value
                    .to_str()
                    .map_err(|_| Status::invalid_argument("invalid authorization"))?
                    .to_owned(),
            );
        }
        Ok(Response::new(tokio_stream::iter(
            self.do_get_reply
                .iter()
                .cloned()
                .map(Ok)
                .collect::<Vec<_>>(),
        )))
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

#[tokio::test]
async fn anonymous_flight_client_query_does_not_require_authorization() {
    let cookie_seen = Arc::new(AtomicBool::new(false));
    let service = CookieFlightService::new(cookie_seen);
    let server = TestServer::start(service).await;

    let client = FlightClient::try_new(
        format!("http://{}", server.addr).into(),
        Credentials::anonymous(),
        None,
        None,
    )
    .await
    .expect("client should connect");

    client
        .query("SELECT 1")
        .await
        .expect("anonymous query should reach the server");

    server.shutdown().await;
}

#[tokio::test]
async fn username_password_without_handshake_token_returns_unauthorized() {
    let cookie_seen = Arc::new(AtomicBool::new(false));
    let service = CookieFlightService::new(cookie_seen);
    let server = TestServer::start(service).await;

    let client = FlightClient::try_new(
        format!("http://{}", server.addr).into(),
        Credentials::new("user", SecretString::new("password".into())),
        None,
        None,
    )
    .await
    .expect("client should connect");

    let err = client
        .query("SELECT 1")
        .await
        .expect_err("username/password auth without a handshake token should fail");

    assert!(
        matches!(err, flight_client::Error::Unauthorized {}),
        "expected Unauthorized, got {err}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn federation_context_preserves_repeated_authorization_order()
-> Result<(), Box<dyn std::error::Error>> {
    let service = CookieFlightService::new(Arc::new(AtomicBool::new(false)));
    let observed = Arc::clone(&service.authorization_seen);
    let server = TestServer::start(service).await;
    let endpoint: Arc<str> = format!("http://{}", server.addr).into();
    let mut clients = Vec::new();
    for values in [
        ["Bearer alice", "Bearer bob"],
        ["Bearer bob", "Bearer alice"],
    ] {
        let mut metadata = tonic::metadata::MetadataMap::new();
        for value in values {
            metadata.append("authorization", value.parse()?);
        }
        clients.push(
            FlightClient::try_new(
                Arc::clone(&endpoint),
                Credentials::anonymous(),
                Some(metadata),
                None,
            )
            .await?,
        );
    }
    assert_ne!(
        clients[0].federation_context(),
        clients[1].federation_context(),
        "different wire identities must not share a federation context"
    );
    for client in &clients {
        drop(
            client
                .query_substrait(bytes::Bytes::from_static(b"identity probe"), None, None)
                .await?,
        );
    }
    let actual = observed.lock().clone();
    drop(clients);
    server.shutdown().await;
    assert_eq!(actual, ["Bearer bob", "Bearer alice"]);
    Ok(())
}

/// A Substrait `DoGet` stream is held to the same compressed-message bound as every other
/// Flight stream the client decodes.
#[tokio::test]
async fn substrait_do_get_refuses_an_implausible_decompressed_size()
-> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::{ArrayRef, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_ipc::writer::{
        DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
    };
    use futures::TryStreamExt;

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![7; 4096])) as ArrayRef],
    )?;
    let options =
        IpcWriteOptions::default().try_with_compression(Some(arrow_ipc::CompressionType::ZSTD))?;
    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);
    let schema =
        generator.schema_to_bytes_with_dictionary_tracker(&batch.schema(), &mut tracker, &options);
    let (_, encoded) = generator.encode(
        &batch,
        &mut tracker,
        &options,
        &mut IpcWriteContext::default(),
    )?;

    // Rewrite the length prefixes so the values buffer declares one byte over the 64 MiB floor
    // and every other buffer declares nothing.
    let declared = arrow_tools::ipc::DECOMPRESSED_SIZE_FLOOR + 1;
    let mut body = encoded.arrow_data;
    let buffers = arrow_ipc::root_as_message(&encoded.ipc_message)?
        .header_as_record_batch()
        .and_then(|batch| batch.buffers())
        .expect("the batch declares its buffers");
    for (index, buffer) in buffers.iter().enumerate() {
        if buffer.length() < 8 {
            continue;
        }
        let offset = usize::try_from(buffer.offset())?;
        let length: u64 = if index == 1 { declared } else { 0 };
        body[offset..offset + 8].copy_from_slice(&length.to_le_bytes());
    }

    let mut service = CookieFlightService::new(Arc::new(AtomicBool::new(false)));
    service.do_get_reply = vec![
        arrow_flight::FlightData::new().with_data_header(schema.ipc_message),
        arrow_flight::FlightData::new()
            .with_data_header(encoded.ipc_message)
            .with_data_body(body),
    ];
    let server = TestServer::start(service).await;
    let client = FlightClient::try_new(
        format!("http://{}", server.addr).into(),
        Credentials::anonymous(),
        None,
        None,
    )
    .await?;
    let result = client
        .query_substrait(bytes::Bytes::from_static(b"plan"), None, None)
        .await?
        .try_collect::<Vec<_>>()
        .await;
    drop(client);
    server.shutdown().await;

    let error = result
        .expect_err("the oversized message is refused")
        .to_string();
    assert!(
        error.contains(&format!("declares {declared} decompressed bytes")),
        "{error}"
    );
    Ok(())
}
