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

//! Regression test for write-through `DoPut` idle timeout keepalive.
//!
//! Verifies that keepalive `FlightData` messages (`app_metadata` = "spice-keepalive")
//! prevent the executor's `DoPut` idle timeout from firing.
//!
//! Uses a minimal Flight server stub that mirrors the idle-timeout behavior
//! from `create_response_stream` in `do_put.rs`, so no full Spice runtime is needed.

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_flight::encode::FlightDataEncoderBuilder;
    use arrow_flight::flight_service_client::FlightServiceClient;
    use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
    use arrow_flight::{
        Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
        HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    };
    use futures::{Stream, StreamExt};
    use runtime::flight::KEEPALIVE_APP_METADATA;

    use std::pin::Pin;
    use std::sync::Arc;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{IntoStreamingRequest, Request, Response, Status, Streaming};

    /// Minimal Flight service that accepts `DoPut` with an idle timeout.
    /// Mirrors the timeout behavior from `create_response_stream` in `do_put.rs`.
    #[derive(Clone)]
    struct IdleTimeoutFlightService {
        idle_timeout: Duration,
    }

    #[tonic::async_trait]
    impl FlightService for IdleTimeoutFlightService {
        type HandshakeStream =
            Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
        type ListFlightsStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send>>;
        type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;
        type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send>>;
        type DoActionStream =
            Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;
        type ListActionsStream = Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send>>;
        type DoExchangeStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

        async fn handshake(
            &self,
            _: Request<Streaming<HandshakeRequest>>,
        ) -> Result<Response<Self::HandshakeStream>, Status> {
            Err(Status::unimplemented(""))
        }

        async fn list_flights(
            &self,
            _: Request<Criteria>,
        ) -> Result<Response<Self::ListFlightsStream>, Status> {
            Err(Status::unimplemented(""))
        }

        async fn get_flight_info(
            &self,
            _: Request<FlightDescriptor>,
        ) -> Result<Response<FlightInfo>, Status> {
            Err(Status::unimplemented(""))
        }

        async fn get_schema(
            &self,
            _: Request<FlightDescriptor>,
        ) -> Result<Response<SchemaResult>, Status> {
            Err(Status::unimplemented(""))
        }

        async fn do_get(&self, _: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
            Err(Status::unimplemented(""))
        }

        async fn do_put(
            &self,
            request: Request<Streaming<FlightData>>,
        ) -> Result<Response<Self::DoPutStream>, Status> {
            let mut stream = request.into_inner();
            let idle_timeout = self.idle_timeout;

            let output = async_stream::stream! {
                let deadline = tokio::time::sleep(idle_timeout);
                tokio::pin!(deadline);

                loop {
                    tokio::select! {
                        () = &mut deadline => {
                            let secs = idle_timeout.as_secs();
                            yield Err(Status::deadline_exceeded(
                                format!("Timeout: no record batch received within {secs} seconds"),
                            ));
                            break;
                        }
                        message = stream.next() => {
                            match message {
                                Some(Ok(msg)) => {
                                    // Reset idle timer on every message.
                                    deadline.as_mut().reset(
                                        tokio::time::Instant::now() + idle_timeout,
                                    );

                                    // Skip keepalive messages.
                                    if msg.app_metadata.as_ref() == KEEPALIVE_APP_METADATA {
                                        continue;
                                    }

                                    // Accept the data.
                                    yield Ok(PutResult::default());
                                }
                                Some(Err(e)) => {
                                    yield Err(Status::internal(format!("Stream error: {e}")));
                                    break;
                                }
                                None => {
                                    // Stream ended normally.
                                    yield Ok(PutResult::default());
                                    break;
                                }
                            }
                        }
                    }
                }
            };

            Ok(Response::new(Box::pin(output)))
        }

        async fn do_action(
            &self,
            _: Request<Action>,
        ) -> Result<Response<Self::DoActionStream>, Status> {
            Err(Status::unimplemented(""))
        }

        async fn list_actions(
            &self,
            _: Request<Empty>,
        ) -> Result<Response<Self::ListActionsStream>, Status> {
            Err(Status::unimplemented(""))
        }

        async fn do_exchange(
            &self,
            _: Request<Streaming<FlightData>>,
        ) -> Result<Response<Self::DoExchangeStream>, Status> {
            Err(Status::unimplemented(""))
        }

        async fn poll_flight_info(
            &self,
            _: Request<FlightDescriptor>,
        ) -> Result<Response<PollInfo>, Status> {
            Err(Status::unimplemented(""))
        }
    }

    /// Start the stub Flight server, return the port.
    async fn start_server(idle_timeout: Duration) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();

        let svc = IdleTimeoutFlightService { idle_timeout };
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(svc))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("server");
        });

        // Wait for server to be ready.
        let start = std::time::Instant::now();
        let addr = format!("127.0.0.1:{port}");
        let timeout = Duration::from_secs(5);
        loop {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                break;
            }
            assert!(
                start.elapsed() < timeout,
                "Flight server did not become ready within {timeout:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        port
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )
        .expect("batch")
    }

    /// Verify that an idle `DoPut` stream times out when no data or keepalives
    /// are sent within the idle timeout period.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_do_put_idle_timeout_fires() {
        let port = start_server(Duration::from_secs(2)).await;

        let url = format!("http://127.0.0.1:{port}");
        let channel = tonic::transport::Channel::from_shared(url)
            .expect("setup")
            .connect()
            .await
            .expect("setup");
        let mut client = FlightServiceClient::new(channel);

        let batch = test_batch();
        let descriptor = FlightDescriptor::new_path(vec!["test".to_string()]);
        let flight_data: Vec<FlightData> = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(descriptor))
            .build(futures::stream::iter(vec![Ok(batch)]))
            .map(|r| r.expect("encode"))
            .collect()
            .await;

        let (tx, rx) = tokio::sync::mpsc::channel::<FlightData>(16);

        // Send initial flight data.
        for fd in flight_data {
            tx.send(fd).await.expect("setup");
        }

        let request = ReceiverStream::new(rx).into_streaming_request();
        let response = client.do_put(request).await.expect("setup");
        let mut stream = response.into_inner();

        // First batch should succeed.
        let first = stream.next().await.expect("first result");
        assert!(first.is_ok(), "first batch should succeed");

        // Idle for longer than the 2-second timeout.
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Collect remaining results — expect DEADLINE_EXCEEDED.
        let mut got_timeout = false;
        while let Some(result) = stream.next().await {
            if let Err(status) = result {
                assert!(
                    status.code() == tonic::Code::DeadlineExceeded
                        || status.message().contains("Timeout"),
                    "Expected deadline exceeded, got: {status}"
                );
                got_timeout = true;
                break;
            }
        }

        assert!(
            got_timeout,
            "Expected idle timeout error but stream ended normally"
        );
        drop(tx);
    }

    /// Verify that keepalive messages prevent the idle timeout from firing.
    ///
    /// This is the core regression test for the write-through fix.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_do_put_keepalive_prevents_timeout() {
        let port = start_server(Duration::from_secs(2)).await;

        let url = format!("http://127.0.0.1:{port}");
        let channel = tonic::transport::Channel::from_shared(url)
            .expect("setup")
            .connect()
            .await
            .expect("setup");
        let mut client = FlightServiceClient::new(channel);

        let batch1 = test_batch();
        let batch2 = test_batch();

        let descriptor = FlightDescriptor::new_path(vec!["test".to_string()]);
        let fd1: Vec<FlightData> = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(descriptor))
            .build(futures::stream::iter(vec![Ok(batch1)]))
            .map(|r| r.expect("encode"))
            .collect()
            .await;

        let fd2: Vec<FlightData> = FlightDataEncoderBuilder::new()
            .build(futures::stream::iter(vec![Ok(batch2)]))
            .map(|r| r.expect("encode"))
            .collect()
            .await;

        let (tx, rx) = tokio::sync::mpsc::channel::<FlightData>(32);

        // Send batch1.
        for fd in &fd1 {
            tx.send(fd.clone()).await.expect("setup");
        }

        let request = ReceiverStream::new(rx).into_streaming_request();
        let response = client.do_put(request).await.expect("setup");
        let mut stream = response.into_inner();

        // First batch should succeed.
        let first = stream.next().await.expect("first");
        first.expect("first batch ok");

        // Send keepalives for 4 seconds — well beyond the 2s timeout.
        let keepalive_tx = tx.clone();
        let keepalive_task = tokio::spawn(async move {
            for _ in 0..8 {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let keepalive = FlightData {
                    app_metadata: bytes::Bytes::from_static(KEEPALIVE_APP_METADATA),
                    ..Default::default()
                };
                if keepalive_tx.send(keepalive).await.is_err() {
                    break;
                }
            }
        });
        keepalive_task.await.expect("setup");

        // After keepalives, send batch2.
        for fd in &fd2 {
            tx.send(fd.clone()).await.expect("send batch2");
        }
        drop(tx); // End stream.

        // Collect results — should NOT get a timeout.
        let mut got_timeout = false;
        let mut success_count = 0;
        while let Some(result) = stream.next().await {
            match result {
                Ok(_) => success_count += 1,
                Err(status) => {
                    if status.code() == tonic::Code::DeadlineExceeded {
                        got_timeout = true;
                    }
                    break;
                }
            }
        }

        assert!(
            !got_timeout,
            "Keepalives should have prevented the idle timeout"
        );
        assert!(
            success_count >= 1,
            "Expected at least one success for batch2, got {success_count}"
        );
    }
}
