use crate::client::Error as SpiceClientError;
use crate::config::get_user_agent;
use crate::config::GenericError;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::FlightDescriptor;
use arrow_flight::HandshakeRequest;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use futures::stream;
use futures::task::Context;
use futures::task::Poll;
use futures::Future;
use futures::Stream;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::AsciiMetadataKey;
use tonic::transport::Channel;
use tonic::IntoRequest;

#[derive(Clone)]
pub struct SqlFlightClient {
    headers: Arc<HashMap<String, String>>,
    client: FlightServiceClient<Channel>,
    api_key: Option<Arc<str>>,
    max_retries: u32,
}

impl SqlFlightClient {
    pub fn new(
        chan: Channel,
        api_key: Option<String>,
        user_agent: Option<String>,
        cache_control: Option<String>,
        max_retries: u32,
    ) -> Self {
        // Prepend the user agent with the provided user agent if it exists
        let user_agent = match user_agent {
            Some(ua) => format!("{ua} {}", get_user_agent()),
            None => get_user_agent(),
        };

        let mut headers = HashMap::new();
        headers.insert("User-Agent".to_string(), user_agent);

        if let Some(cache_control) = cache_control {
            headers.insert("Cache-Control".to_string(), cache_control);
        }

        SqlFlightClient {
            api_key: api_key.map(|s| Arc::from(s.into_boxed_str())),
            headers: Arc::new(headers),
            client: FlightServiceClient::new(chan),
            max_retries,
        }
    }

    async fn handshake(
        &self,
        username: &str,
        password: &str,
    ) -> Result<Option<String>, ArrowError> {
        let cmd = HandshakeRequest {
            protocol_version: 0,
            payload: Bytes::default(),
        };
        let mut req = tonic::Request::new(stream::iter(vec![cmd]));
        let val = BASE64_STANDARD.encode(format!("{username}:{password}"));
        let val = format!("Basic {val}")
            .parse()
            .map_err(|_| ArrowError::ParseError("Cannot parse header".to_string()))?;
        req.metadata_mut().insert("authorization", val);
        let req = self.set_request_headers(req, None)?;
        let resp = self
            .client
            .clone()
            .handshake(req)
            .await
            .map_err(|e| ArrowError::IpcError(format!("Can't handshake {e}")))?;

        let mut token: Option<String> = None;
        if let Some(auth) = resp.metadata().get("authorization") {
            let auth = auth
                .to_str()
                .map_err(|_| ArrowError::ParseError("Can't read auth header".to_string()))?;
            let bearer = "Bearer ";
            if !auth.starts_with(bearer) {
                Err(ArrowError::ParseError("Invalid auth header!".to_string()))?;
            }
            let auth = auth[bearer.len()..].to_string();
            token = Some(auth);
        }
        Ok(token)
    }

    async fn authenticate(&self) -> std::result::Result<Option<String>, GenericError> {
        let (username, password) = match &self.api_key {
            Some(api_key) => ("", api_key.as_ref()),
            None => return Ok(None),
        };

        let token = self.handshake(username, password).await?;

        Ok(token)
    }

    fn set_request_headers<T>(
        &self,
        mut req: tonic::Request<T>,
        token: Option<String>,
    ) -> Result<tonic::Request<T>, ArrowError> {
        for (k, v) in self.headers.iter() {
            let k = AsciiMetadataKey::from_str(k.as_str()).map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert header key \"{k}\": {e}"))
            })?;
            let v = v.parse().map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert header value \"{v}\": {e}"))
            })?;
            req.metadata_mut().insert(k, v);
        }
        if let Some(token) = token {
            let val = format!("Bearer {token}").parse().map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert token to header value: {e}"))
            })?;
            req.metadata_mut().insert("authorization", val);
        }
        Ok(req)
    }

    pub async fn query(
        &self,
        query: &str,
    ) -> std::result::Result<FlightRecordBatchStream, GenericError> {
        let token = self.authenticate().await?;

        let descriptor = FlightDescriptor::new_cmd(query.to_string());
        let req = self.set_request_headers(descriptor.into_request(), token.clone())?;

        let info = self.client.clone().get_flight_info(req).await?.into_inner();

        for ep in info.endpoint {
            if let Some(tkt) = ep.ticket {
                let req = tkt.into_request();
                let req = self.set_request_headers(req, token.clone())?;
                let (md, response_stream, _ext) =
                    self.client.clone().do_get(req).await?.into_parts();

                return Ok(FlightRecordBatchStream::new_from_flight_data(
                    response_stream.map_err(|e| FlightError::Tonic(Box::new(e))),
                )
                .with_headers(md));
            }
        }
        Err("No endpoints found".into())
    }

    pub async fn query_with_params(
        &self,
        query: &str,
        params: Option<RecordBatch>,
    ) -> std::result::Result<FlightRecordBatchStream, GenericError> {
        if let Some(params) = params {
            Ok(self.execute_prepared_statement(query, params).await?)
        } else {
            Ok(self.query(query).await?)
        }
    }

    async fn execute_prepared_statement(
        &self,
        query: &str,
        parameters: RecordBatch,
    ) -> std::result::Result<FlightRecordBatchStream, GenericError> {
        let mut client = FlightSqlServiceClient::new_from_inner(self.client.clone());
        let mut prepared_stmt = client.prepare(query.to_string(), None).await?;

        prepared_stmt.set_parameters(parameters)?;

        let flight_info = prepared_stmt.execute().await?;

        let endpoint = flight_info
            .endpoint
            .first()
            .ok_or("No endpoint in flight info")?;

        let stream = client
            .do_get(
                endpoint
                    .ticket
                    .clone()
                    .ok_or("No flight ticket in response")?,
            )
            .await?;
        Ok(stream)
    }
}

/// Represents the current state of the `RetryableQueryStream` state machine.
/// Wraps a `FlightRecordBatchStream` and started from `Streaming` stage.
/// If a retryable error occurs during streaming, the stream resets and retries.
/// `Streaming` -> `Ready` → `Executing` → `Streaming` → `Ready`
/// If a non-retryable error occurs during streaming, the stream will be immediately terminated.
/// `Streaming` -> `Terminated`. (non-retryable error)
enum StreamState {
    /// Ready to retry a query
    Ready,
    /// Query is being executed, waiting for the server to return a stream
    Executing(Pin<Box<dyn Future<Output = Result<FlightRecordBatchStream, GenericError>> + Send>>),
    /// Initial state, actively streaming record batches from the server
    Streaming(Pin<Box<FlightRecordBatchStream>>),
    /// Terminal state - stream has ended due to non-retryable error
    Terminated,
}

/// A retryable stream for executing SQL queries with Flight.
///
/// This stream automatically handles streaming failures and immediately retries queries.
/// It yields `RecordBatch` results on success and `SpiceClientError` on failure.
///
/// ## Retry Behavior
///
/// When a connection reset occurs during streaming, the stream will:
/// 1. Yield a `SpiceClientError::ConnectionReset` error to the consumer
/// 2. If the consumer continues polling, automatically retry the entire query from the beginning
/// 3. If the consumer stops polling, the stream will not retry and enters the `Terminated` state
/// 4. Stop retrying and enters the `Terminated` state after reaching `max_retries` attempts
///
/// ## Consumer Options
///
/// **Option 1: Continue polling for automatic retry**
/// ```text
/// Poll 1: Ok(batch1)
/// Poll 2: Ok(batch2)
/// Poll 3: Err(ConnectionReset) → Consumer continues polling
/// Poll 4: Ok(batch1) → Query restarted from beginning
/// Poll 5: Ok(batch2)
/// Poll 6: Ok(batch3)
/// ...
/// ```
///
/// **Option 2: Stop on error**
/// ```text
/// Poll 1: Ok(batch1)
/// Poll 2: Ok(batch2)
/// Poll 3: Err(ConnectionReset) → Consumer stops polling
/// ```
///
/// ## Important Notes
/// - The query restarts from the beginning on retry - previously yielded batches will be re-yielded
/// - Non-retryable errors are returned immediately without retry attempts
/// - Only connection resets and specific gRPC errors trigger retries
///
pub struct RetryableQueryStream {
    client: Arc<SqlFlightClient>,
    sql: Arc<String>,
    params: Option<RecordBatch>,
    state: StreamState,
    max_retries: u32,
    retry_count: u32,
}

impl RetryableQueryStream {
    pub fn new(
        client: Arc<SqlFlightClient>,
        sql: &str,
        params: Option<RecordBatch>,
        stream: Pin<Box<FlightRecordBatchStream>>,
    ) -> Self {
        Self {
            max_retries: client.max_retries,
            client,
            sql: Arc::new(sql.to_string()),
            params,
            state: StreamState::Streaming(stream),
            retry_count: 0,
        }
    }
}

impl Stream for RetryableQueryStream {
    type Item = Result<RecordBatch, SpiceClientError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            StreamState::Ready => {
                let client = Arc::clone(&self.client);
                let sql = Arc::clone(&self.sql);
                let params = self.params.clone();

                let fut = Box::pin(async move { client.query_with_params(&sql, params).await });

                self.state = StreamState::Executing(fut);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            StreamState::Executing(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(stream)) => {
                    self.state = StreamState::Streaming(Box::pin(stream));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Ready(Err(error)) => {
                    if is_connection_reset_generic_error(&error)
                        && self.retry_count < self.max_retries
                    {
                        self.retry_count += 1;
                        self.state = StreamState::Ready;
                        cx.waker().wake_by_ref();
                        return Poll::Ready(Some(Err(SpiceClientError::ConnectionReset {
                            message: error.to_string(),
                        })));
                    }
                    self.state = StreamState::Terminated;
                    Poll::Ready(Some(Err(SpiceClientError::Query { source: error })))
                }
                Poll::Pending => Poll::Pending,
            },
            StreamState::Streaming(stream) => match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
                Poll::Ready(Some(Err(error))) => {
                    if is_connection_reset_flight_error(&error)
                        && self.retry_count < self.max_retries
                    {
                        self.retry_count += 1;
                        self.state = StreamState::Ready;
                        cx.waker().wake_by_ref();
                        return Poll::Ready(Some(Err(SpiceClientError::ConnectionReset {
                            message: error.to_string(),
                        })));
                    }
                    self.state = StreamState::Terminated;
                    Poll::Ready(Some(Err(SpiceClientError::QueryStream { source: error })))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            StreamState::Terminated => Poll::Ready(None),
        }
    }
}

pub fn is_tonic_reset_error(error: &tonic::Status) -> bool {
    match error.code() {
        tonic::Code::Internal | tonic::Code::Cancelled | tonic::Code::Unknown => {
            let error_message = error.message().to_lowercase();
            if error_message.contains("operation was canceled")
                || error_message.contains("http2 error")
                || error_message.contains("grpc-status header missing")
                || error_message.contains("received message with invalid compression flag")
                || error_message.contains("error reading a body from connection")
                || error_message.contains("transport error")
            {
                return true;
            }
            false
        }
        _ => false,
    }
}

fn is_connection_reset_flight_error(error: &FlightError) -> bool {
    if let FlightError::Tonic(status) = error {
        return is_tonic_reset_error(status) || status.metadata().contains_key("spiceai-retryable");
    }
    false
}

pub fn is_connection_reset_generic_error(error: &GenericError) -> bool {
    if let Some(status) = error.downcast_ref::<tonic::Status>() {
        return is_tonic_reset_error(status) || status.metadata().contains_key("spiceai-retryable");
    }
    false
}
