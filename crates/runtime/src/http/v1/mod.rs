/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

pub mod catalogs;
pub mod cdc;
pub mod chat;
pub mod datasets;
pub mod decisions;
pub mod embeddings;
pub mod functions;
pub mod iceberg;
pub mod messages;
pub mod responses;
pub mod snapshots;

pub mod models;
pub mod nsql;
pub mod otlp;
pub mod packages;
pub mod queries;
pub mod query;
pub mod ready;
pub mod search;
pub mod spicepods;
pub mod status;
pub mod tools;
pub mod workers;

use std::sync::Arc;

use crate::{
    component::dataset::Dataset,
    datafusion::{
        DataFusion,
        query::{
            Error as QueryError, QueryBuilder, TransactionError, error_code::ErrorCode,
            is_cancellation_error, is_timeout_error, json_array_writer, run_transaction,
            schema_has_union_columns, single_line, transaction_statements, write_to_json_string,
        },
    },
    egress::EgressAccount,
    http::server_timing::ServerTiming,
    status::ComponentStatus,
};
use arrow::{array::RecordBatch, datatypes::SchemaRef, util::pretty::pretty_format_batches};
use arrow_json::writer::JsonArray;
use async_stream::try_stream;
use axum::{
    body::Body,
    http::{HeaderMap, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use axum_extra::TypedHeader;
use bytes::Bytes;
use cache::result::{
    CacheStatus,
    query::{QueryResult, QueryResultSource, SendableCachedRawStream},
};
use csv::Writer;
use datafusion::common::ParamValues;
use datafusion::execution::{SendableRecordBatchStream, memory_pool::MemoryPool};
use headers_accept::Accept;
use http::{
    HeaderValue,
    header::{CACHE_CONTROL, CONTENT_TYPE},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use snafu::ResultExt;

use futures::{Stream, StreamExt, TryStreamExt};

use runtime_auth::AuthPrincipalRef;
use runtime_request_context::{AsyncMarker, CacheNamespace, RequestContext};

use crate::datafusion::error::is_unavailable;
use crate::datafusion::request_context_extension::DataFusionContextExtension;
#[cfg(feature = "openapi")]
use utoipa::{
    openapi::{
        Required,
        path::{Parameter, ParameterBuilder, ParameterIn},
    },
    schema,
};

#[derive(Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum Format {
    /// JSON format
    #[default]
    Json,

    /// CSV format
    Csv,
}

pub(crate) fn principal_has_write_access(principal: &AuthPrincipalRef) -> bool {
    principal
        .groups()
        .iter()
        .any(|group| *group == "write" || *group == "read_write")
}

pub(crate) async fn current_principal_requires_read_only() -> bool {
    let context = RequestContext::current(AsyncMarker::new().await);
    runtime_auth::AuthRequestContext::auth_principal(context.as_ref())
        .is_some_and(|principal| !principal_has_write_access(principal))
}

/// Denies an OTLP gRPC export from a read-only API key. The OTLP gRPC services run on the
/// Flight server, outside the [`require_write_access`] route layer, so each export handler
/// calls this before it reads the request. With authentication disabled there is no
/// principal, and the export is allowed.
pub(crate) async fn ensure_otlp_grpc_write_access() -> Result<(), tonic::Status> {
    if current_principal_requires_read_only().await {
        return Err(tonic::Status::permission_denied(
            "API key does not allow write access",
        ));
    }
    Ok(())
}

/// Route layer for routes that need a principal with write access.
///
/// It must sit inside the authentication layer, which sets the principal, and run as a
/// layer rather than inside the handler: a handler's body extractors reject a request
/// (415, 422) before the handler body runs, which would tell a read-only caller the
/// route's request schema instead of answering 403.
pub(crate) async fn require_write_access(req: Request<Body>, next: Next) -> Response {
    if current_principal_requires_read_only().await {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(json!({ "message": "API key does not allow write access" })),
        )
            .into_response();
    }
    next.run(req).await
}

fn status_for_sql_error(message: &str) -> StatusCode {
    if message.contains("read-only SQL context") {
        StatusCode::FORBIDDEN
    } else {
        StatusCode::BAD_REQUEST
    }
}

#[cfg(feature = "openapi")]
impl utoipa::IntoParams for Format {
    fn into_params(parameter_in_provider: impl Fn() -> Option<ParameterIn>) -> Vec<Parameter> {
        vec![
            ParameterBuilder::new()
                .description(Some(""))
                .name("format")
                .required(Required::True)
                .parameter_in(parameter_in_provider().unwrap_or_default())
                .schema(Some(schema!(Format)))
                .build(),
        ]
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// The various formats that the Arrow data can be converted and returned from HTTP requests.
pub enum ResponseMimeType {
    #[default]
    Json,
    Csv,
    Plain,
    VndNsqlJsonV1,
    VndSqlJsonV1,
}

/// Represents additional metadata to produce a response, such as the SQL query used, etc.
#[derive(Debug)]
pub struct ResponseMetadata {
    pub sql: Option<String>,
}

impl ResponseMetadata {
    /// Creates an empty `ResponseMetadata`
    #[must_use]
    pub fn empty() -> Self {
        Self { sql: None }
    }

    #[must_use]
    pub fn with_sql(mut self, sql: impl Into<String>) -> Self {
        self.sql = Some(sql.into());
        self
    }
}

/// Gets all possible media types from a `Accept` header without allocating.
pub(crate) fn accept_header_types(accept: &TypedHeader<Accept>) -> impl Iterator<Item = &str> + '_ {
    accept.0.media_types().map(AsRef::<str>::as_ref)
}

impl ResponseMimeType {
    #[must_use]
    pub fn to_accept_header(self) -> Option<http::HeaderValue> {
        let media_type = match self {
            Self::Json => "application/json",
            Self::Csv => "text/csv",
            Self::Plain => "text/plain",
            Self::VndNsqlJsonV1 => "application/vnd.spiceai.nsql.v1+json",
            Self::VndSqlJsonV1 => "application/vnd.spiceai.sql.v1+json",
        };
        HeaderValue::from_str(media_type).ok()
    }

    #[must_use]
    pub fn from_accept_header(accept: Option<&TypedHeader<Accept>>) -> ResponseMimeType {
        accept.map_or(ResponseMimeType::default(), |header| {
            accept_header_types(header)
                .find_map(|h| match h {
                    "application/json" => Some(ResponseMimeType::Json),
                    "application/vnd.spiceai.nsql.v1+json" => Some(ResponseMimeType::VndNsqlJsonV1),
                    "application/vnd.spiceai.sql.v1+json" => Some(ResponseMimeType::VndSqlJsonV1),
                    "text/csv" => Some(ResponseMimeType::Csv),
                    "text/plain" => Some(ResponseMimeType::Plain),
                    _ => None,
                })
                .unwrap_or(ResponseMimeType::default())
        })
    }
}

fn convert_entry_to_csv<T: Serialize>(entries: &[T]) -> Result<String, Box<dyn std::error::Error>> {
    let mut w = Writer::from_writer(vec![]);
    for e in entries {
        w.serialize(e)?;
    }
    w.flush()?;
    Ok(String::from_utf8(w.into_inner()?)?)
}

fn dataset_status(df: &DataFusion, ds: &Dataset) -> ComponentStatus {
    // First check the runtime status which tracks the actual component state
    // (Initializing, Refreshing, Ready, Error, etc.)
    let dataset_statuses = df.runtime_status().get_dataset_statuses();
    if let Some(status) = dataset_statuses.get(&ds.name) {
        return status.clone();
    }

    // Fallback: if not in runtime status, check if table exists
    if df.table_exists(&ds.name) {
        ComponentStatus::Ready
    } else {
        ComponentStatus::error()
    }
}

// Runs a query and converts the results to an HTTP response.
//
// JSON, CSV and the vnd.* JSON envelopes stream batch-by-batch via a chunked
// body, so the full result set is never materialized in memory. `text/plain`
// and results containing union columns buffer via `to_http_response`.
pub async fn sql_to_http_response(
    df: Arc<DataFusion>,
    sql: Arc<str>,
    parameters: Option<ParamValues>,
    format: ResponseMimeType,
    read_only: bool,
) -> Response {
    // A `BEGIN … COMMIT` body is run by the shared transaction orchestrator
    // rather than the ordinary single-statement path.
    if let Some(statements) = transaction_statements(&sql) {
        return transaction_to_http_response(df, statements, parameters, read_only, format).await;
    }

    // Capture the query memory pool before `df` is moved into the builder, so a
    // streamed body can charge its egress buffers against the pool the query ran
    // under (see `EgressAccount`).
    let memory_pool = Arc::clone(&df.ctx.runtime_env().memory_pool);

    let query_res = match QueryBuilder::new_arc(sql, df)
        .parameters(parameters)
        .read_only(read_only)
        .build()
        .run()
        .await
    {
        Ok(res) => res,
        Err(e) => {
            let kind = SqlErrorKind::of_query_error(&e);
            return sql_error_response(e.to_string(), kind);
        }
    };

    query_result_to_http_response(query_res, format, memory_pool).await
}

async fn query_result_to_http_response(
    query_res: QueryResult,
    format: ResponseMimeType,
    memory_pool: Arc<dyn MemoryPool>,
) -> Response {
    match query_res.into_source() {
        QueryResultSource::CachedRaw {
            data,
            schema,
            cache_status,
        } => {
            query_raw_stream_to_http_response(data, schema, cache_status, format, memory_pool).await
        }
        QueryResultSource::Stream { data, cache_status } => {
            query_stream_to_http_response(data, cache_status, format, memory_pool).await
        }
    }
}

/// Converts a query stream to the requested HTTP response format.
///
/// JSON, CSV and vnd JSON responses stream batch-by-batch; see
/// [`streamed_encoder`] for the responses that buffer.
async fn query_stream_to_http_response(
    mut data_stream: SendableRecordBatchStream,
    cache_status: CacheStatus,
    format: ResponseMimeType,
    memory_pool: Arc<dyn MemoryPool>,
) -> Response {
    // Streamability is a schema property, so decide it before pulling any batch.
    let Some(encoder) = streamed_encoder(format, &data_stream.schema()) else {
        return buffered_sql_response(data_stream, cache_status, format).await;
    };

    // Pull the first batch eagerly so the common immediate-execution error still
    // yields a clean status code instead of a truncated 200 response.
    let first = match data_stream.next().await {
        Some(Ok(batch)) => Some(batch),
        Some(Err(e)) => {
            return sql_error_response(e.to_string(), SqlErrorKind::of_datafusion_error(&e));
        }
        None => None,
    };

    let headers = response_headers(format, cache_status).await;
    let account = EgressAccount::register(&memory_pool, "http_egress");
    let body = Body::from_stream(sql_body_from_batches(first, data_stream, encoder, account));
    streamed_response(headers, body)
}

/// Run a `BEGIN … COMMIT` body through the shared transaction orchestrator
/// and shape the outcome (or error) into an HTTP response.
async fn transaction_to_http_response(
    df: Arc<DataFusion>,
    statements: Vec<String>,
    parameters: Option<ParamValues>,
    read_only: bool,
    format: ResponseMimeType,
) -> Response {
    match run_transaction(&df, &statements, parameters, read_only).await {
        Ok(outcome) => match outcome.result {
            // Return the final statement's result (for the canonical gate+write
            // shape, the write's row-count summary).
            Some((batches, cache_status)) => {
                to_http_response(batches, cache_status, format, ResponseMetadata::empty())
                    .await
                    .into_response()
            }
            None => (StatusCode::OK, "COMMIT").into_response(),
        },
        Err(error) => transaction_error_to_response(error),
    }
}

/// Map a [`TransactionError`] to the HTTP response the `/v1/sql` path returns.
fn transaction_error_to_response(error: TransactionError) -> Response {
    match error {
        TransactionError::Rejected(message) => sql_error_response(message, SqlErrorKind::General),
        TransactionError::Plan(e) | TransactionError::Stream(e) => {
            sql_error_response(e.to_string(), SqlErrorKind::of_datafusion_error(&e))
        }
        TransactionError::Query(e) => {
            sql_error_response(e.to_string(), SqlErrorKind::of_query_error(&e))
        }
        TransactionError::Conflict { table } => {
            // Optimistic-concurrency conflict: a participant was committed to
            // between this transaction's start and commit. Retryable — map to
            // 409 so the client can re-run at the newest committed state.
            (
                StatusCode::CONFLICT,
                format!(
                    "transaction write conflict on '{table}': a participant table changed since the transaction started; retry"
                ),
            )
                .into_response()
        }
        TransactionError::Publish(message) => sql_error_response(
            format!("transaction publish failed: {message}"),
            SqlErrorKind::General,
        ),
    }
}

/// Classifies a query error for HTTP status mapping: client-initiated
/// cancellation maps to 499 Client Closed Request, a `runtime.query.timeout`
/// expiry maps to 504 Gateway Timeout, a memory-pool refusal maps to 503
/// Service Unavailable, everything else falls through to
/// [`status_for_sql_error`].
#[derive(Clone, Copy)]
enum SqlErrorKind {
    General,
    Cancellation,
    Timeout,
    /// The query engine refused the query for want of memory. Carried
    /// separately so it can be logged as the operator-actionable condition it
    /// is and answered with a retriable status.
    ResourcesExhausted,
    /// A cluster scheduler cannot yet reach executors that hold the data the
    /// query reads, or Spice is shutting down; a retry can succeed.
    Unavailable,
    /// The request set `Cache-Control: only-if-cached` and no cached result was
    /// servable, so the query was not run.
    NotCached,
}

impl SqlErrorKind {
    fn of_query_error(e: &QueryError) -> Self {
        match e {
            QueryError::QueryCancelled { .. } => Self::Cancellation,
            QueryError::QueryTimedOut { .. } => Self::Timeout,
            QueryError::NotCached => Self::NotCached,
            QueryError::UnableToExecuteQuery { source }
            | QueryError::UnableToCreateMemoryStream { source }
            | QueryError::UnableToCollectResults { source }
                if is_unavailable(source) =>
            {
                Self::Unavailable
            }
            _ => Self::General,
        }
    }

    fn of_datafusion_error(e: &datafusion::error::DataFusionError) -> Self {
        if is_cancellation_error(e) {
            Self::Cancellation
        } else if is_timeout_error(e) {
            Self::Timeout
        } else if ErrorCode::from(e) == ErrorCode::ResourcesExhausted {
            Self::ResourcesExhausted
        } else if is_unavailable(e) {
            Self::Unavailable
        } else {
            Self::General
        }
    }
}

/// Maps a query error message to an HTTP response, distinguishing cancellation
/// (499 Client Closed Request), query timeout (504 Gateway Timeout) and
/// resource exhaustion (503 Service Unavailable) from other errors.
///
/// Logged at `debug` — including a refusal for want of memory, which
/// `QueryTracker::finish` names at `warn` instead. It has to be named there:
/// this function runs after the query has returned and its trace span has
/// closed, so a record written here can carry no `trace_id` and names no
/// particular query. `finish` is also protocol-agnostic, so naming it there
/// covers Flight, which never reaches this function.
fn sql_error_response(message: String, kind: SqlErrorKind) -> Response {
    tracing::debug!("Error executing query: {}", single_line(&message));
    let status = match kind {
        SqlErrorKind::Cancellation => StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST),
        SqlErrorKind::Timeout | SqlErrorKind::NotCached => StatusCode::GATEWAY_TIMEOUT,
        // A refusal for want of memory is the runtime's own capacity condition,
        // not a malformed request: 4xx tells intermediaries the client is at
        // fault and the request should not be retried, so a load balancer keeps
        // routing to a saturated pod and a client that retries on 5xx surfaces
        // a hard failure for a query that would likely succeed on another
        // replica. Flight already reports the same failure as the retriable
        // `RESOURCE_EXHAUSTED`.
        SqlErrorKind::ResourcesExhausted | SqlErrorKind::Unavailable => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        SqlErrorKind::General => status_for_sql_error(&message),
    };
    (status, message).into_response()
}

/// Buffered (non-streaming) response path for formats that cannot stream.
async fn buffered_sql_response(
    data_stream: SendableRecordBatchStream,
    cache_status: CacheStatus,
    format: ResponseMimeType,
) -> Response {
    let data = match data_stream.try_collect::<Vec<RecordBatch>>().await {
        Ok(data) => data,
        Err(e) => return sql_error_response(e.to_string(), SqlErrorKind::of_datafusion_error(&e)),
    };
    to_http_response(data, cache_status, format, ResponseMetadata::empty())
        .await
        .into_response()
}

async fn query_raw_stream_to_http_response(
    mut data_stream: SendableCachedRawStream,
    schema: SchemaRef,
    cache_status: CacheStatus,
    format: ResponseMimeType,
    memory_pool: Arc<dyn MemoryPool>,
) -> Response {
    let Some(encoder) = streamed_encoder(format, &schema) else {
        let data = match data_stream.try_collect::<Vec<_>>().await {
            Ok(batches) => batches
                .iter()
                .map(|batch| RecordBatch::clone(batch.as_ref()))
                .collect(),
            Err(e) => {
                return sql_error_response(e.to_string(), SqlErrorKind::of_datafusion_error(&e));
            }
        };
        return to_http_response(data, cache_status, format, ResponseMetadata::empty())
            .await
            .into_response();
    };

    let first = match data_stream.next().await {
        Some(Ok(batch)) => Some(batch),
        Some(Err(e)) => {
            return sql_error_response(e.to_string(), SqlErrorKind::of_datafusion_error(&e));
        }
        None => None,
    };

    let headers = response_headers(format, cache_status).await;
    let account = EgressAccount::register(&memory_pool, "http_egress");
    let body = Body::from_stream(sql_body_from_batches(first, data_stream, encoder, account));
    streamed_response(headers, body)
}

/// A `200 OK` whose body streams, marked so its `Server-Timing` total is sent
/// as a trailer: the total is not known until the body ends.
fn streamed_response(headers: HeaderMap, body: Body) -> Response {
    let mut response = (StatusCode::OK, headers, body).into_response();
    response.extensions_mut().insert(ServerTiming::Trailer);
    response
}

trait SqlBodyBatch: Send + 'static {
    fn as_record_batch(&self) -> &RecordBatch;
}

impl SqlBodyBatch for RecordBatch {
    fn as_record_batch(&self) -> &RecordBatch {
        self
    }
}

impl SqlBodyBatch for Arc<RecordBatch> {
    fn as_record_batch(&self) -> &RecordBatch {
        self.as_ref()
    }
}

/// Encodes a `/v1/sql` body one record batch at a time. The concatenated
/// chunks are identical to the buffered `to_http_response` body.
enum BodyEncoder {
    /// One JSON-array writer drives the whole response: it emits the opening
    /// `[`, the inter-row/inter-batch commas, and the closing `]`.
    Json(arrow_json::Writer<Vec<u8>, JsonArray>),
    /// The `data` array of the vnd JSON envelope. The envelope head is written
    /// with the first batch, because its schema is that batch's schema.
    VndJson {
        rows: arrow_json::Writer<Vec<u8>, JsonArray>,
        row_count: usize,
        started: bool,
    },
    /// The header row is written with the first batch only.
    Csv { header: bool },
}

/// The encoder that streams `format` over `schema`, or `None` when the
/// response buffers: `text/plain` sizes its columns from the whole result, the
/// Arrow JSON and CSV writers cannot render union columns, and the CSV writer
/// rejects nested columns, an error that must reach the client as a status.
fn streamed_encoder(
    format: ResponseMimeType,
    schema: &arrow::datatypes::Schema,
) -> Option<BodyEncoder> {
    if schema_has_union_columns(schema) {
        return None;
    }
    match format {
        ResponseMimeType::Json => Some(BodyEncoder::Json(json_array_writer())),
        ResponseMimeType::VndSqlJsonV1 | ResponseMimeType::VndNsqlJsonV1 => {
            Some(BodyEncoder::VndJson {
                rows: json_array_writer(),
                row_count: 0,
                started: false,
            })
        }
        ResponseMimeType::Csv if schema.fields().iter().any(|f| f.data_type().is_nested()) => None,
        ResponseMimeType::Csv => Some(BodyEncoder::Csv { header: true }),
        ResponseMimeType::Plain => None,
    }
}

impl BodyEncoder {
    /// Encodes `batch`, returning the bytes it adds to the body.
    fn write(&mut self, batch: &RecordBatch) -> Result<Vec<u8>, BoxedError> {
        match self {
            Self::Json(writer) => {
                writer.write(batch)?;
                Ok(std::mem::take(writer.get_mut()))
            }
            Self::VndJson {
                rows,
                row_count,
                started,
            } => {
                if !*started {
                    write_vnd_json_head(rows.get_mut(), Some(&batch.schema()))?;
                    *started = true;
                }
                *row_count += batch.num_rows();
                rows.write(batch)?;
                Ok(std::mem::take(rows.get_mut()))
            }
            Self::Csv { header } => {
                let mut writer = arrow_csv::WriterBuilder::new()
                    .with_header(*header)
                    .build(Vec::new());
                writer.write(batch)?;
                *header = false;
                Ok(writer.into_inner())
            }
        }
    }

    /// Returns the bytes that end the body.
    fn finish(self) -> Result<Vec<u8>, BoxedError> {
        match self {
            Self::Json(mut writer) => {
                // Closes the array (`]`), or emits `[]` for an empty result.
                writer.finish()?;
                Ok(writer.into_inner())
            }
            Self::VndJson {
                mut rows,
                row_count,
                started,
            } => {
                if !started {
                    write_vnd_json_head(rows.get_mut(), None)?;
                }
                rows.finish()?;
                let mut tail = rows.into_inner();
                write_vnd_json_tail(&mut tail, row_count, None)?;
                Ok(tail)
            }
            Self::Csv { .. } => Ok(Vec::new()),
        }
    }
}

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// Streams the query result one input batch at a time, charging each encoded
/// chunk against the query memory pool via `account`.
fn sql_body_from_batches<S, B>(
    first: Option<B>,
    rest: S,
    mut encoder: BodyEncoder,
    account: Arc<EgressAccount>,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> + Send
where
    S: Stream<Item = Result<B, datafusion::error::DataFusionError>> + Send + Unpin + 'static,
    B: SqlBodyBatch,
{
    let mut batches =
        futures::stream::iter(first.map(Ok::<_, datafusion::error::DataFusionError>)).chain(rest);
    try_stream! {
        while let Some(item) = batches.next().await {
            let batch = item.map_err(|e| std::io::Error::other(e.to_string()))?;
            let chunk = encoder
                .write(batch.as_record_batch())
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            if !chunk.is_empty() {
                let size = chunk.len();
                account.reserve(size).await;
                yield Bytes::from(chunk);
                account.release(size);
            }
        }
        let tail = encoder
            .finish()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        if !tail.is_empty() {
            let size = tail.len();
            account.reserve(size).await;
            yield Bytes::from(tail);
            account.release(size);
        }
    }
}

// Runs query and returns the results as a vector of `RecordBatch`.
pub async fn run_sql(
    df: Arc<DataFusion>,
    sql: &str,
    parameters: Option<ParamValues>,
) -> Result<(Vec<RecordBatch>, CacheStatus), Box<dyn std::error::Error + Send + Sync>> {
    run_sql_with_read_only(df, sql, parameters, false).await
}

// Runs query and returns the results as a vector of `RecordBatch`, enforcing read-only mode when requested.
pub async fn run_sql_with_read_only(
    df: Arc<DataFusion>,
    sql: &str,
    parameters: Option<ParamValues>,
    read_only: bool,
) -> Result<(Vec<RecordBatch>, CacheStatus), Box<dyn std::error::Error + Send + Sync>> {
    let query_res = QueryBuilder::new(sql, df)
        .parameters(parameters)
        .read_only(read_only)
        .build()
        .run()
        .await?;

    let cache_status = query_res.cache_status;
    Ok((query_res.collect_batches().await?, cache_status))
}

// Converts a buffered query result to an HTTP response.
pub async fn to_http_response(
    data: Vec<RecordBatch>,
    cache_status: CacheStatus,
    format: ResponseMimeType,
    meta: ResponseMetadata,
) -> (StatusCode, HeaderMap, String) {
    let res = match format {
        ResponseMimeType::Json => arrow_to_json(&data),
        ResponseMimeType::Csv => arrow_to_csv(&data),
        ResponseMimeType::Plain => arrow_to_plain(&data),
        ResponseMimeType::VndSqlJsonV1 | ResponseMimeType::VndNsqlJsonV1 => {
            arrow_to_vnd_sql_json_v1(&data, &meta)
        }
    };

    let body = match res {
        Ok(body) => body,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                e.to_string(),
            );
        }
    };

    (
        StatusCode::OK,
        response_headers(format, cache_status).await,
        body,
    )
}

/// Builds the content-type + cache-metadata headers for a query response. These
/// are computable before the body since `cache_status` is known once the query
/// starts producing rows, so they can precede a streamed body.
async fn response_headers(format: ResponseMimeType, cache_status: CacheStatus) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let request_context = RequestContext::current(AsyncMarker::new().await);

    if let Some(header_value) = format.to_accept_header() {
        headers.insert(CONTENT_TYPE, header_value);
    }

    attach_cache_headers(
        &mut headers,
        cache_status,
        request_context.client_supplied_cache_key().is_some(),
        &request_context,
    );

    headers
}

fn attach_cache_headers(
    headers: &mut HeaderMap,
    results_cache_status: CacheStatus,
    user_key_specified: bool,
    request_context: &RequestContext,
) {
    if let Some(val) = status_to_x_cache_value(results_cache_status) {
        headers.insert("X-Cache", val);
    }

    if let Some(val) = results_cache_status.to_header_string() {
        headers.insert("Results-Cache-Status", HeaderValue::from_static(val));
    }

    // Surface the cache scope so callers can tell whether a MISS came
    // from per-user isolation (a coworker's cached entry is not visible)
    // versus a true cold cache.
    let cache_namespace = request_context.cache_namespace();
    headers.insert(
        "Results-Cache-Scope",
        HeaderValue::from_static(cache_namespace.as_header_value()),
    );

    // Tell CDN entry is unique per user cache key
    if user_key_specified {
        append_vary(headers, "Spice-Cache-Key");
    }

    // For per-user scope, additionally vary on every header that can
    // identify a principal so an HTTP cache between Spice and the client
    // never collapses entries belonging to different principals.
    // - `Authorization` covers Bearer / Basic / future U2M flows.
    // - `X-API-Key` is the header used by the API-key auth flow today;
    //   without it shared proxies/CDNs would happily reuse Alice's
    //   response for Bob.
    // - `Cookie` covers any future session-cookie based auth.
    if matches!(cache_namespace, CacheNamespace::Principal(_)) {
        append_vary(headers, "Authorization");
        append_vary(headers, "X-API-Key");
        append_vary(headers, "Cookie");
    }

    // Add Cache-Control response header with stale-while-revalidate if configured
    // Access the DataFusion instance to get the pre-parsed cache configuration
    if let Some(df_ext) = request_context.extension::<DataFusionContextExtension>() {
        let df = df_ext.datafusion();
        if let Some(cache_provider) = df.results_cache_provider()
            && let Some(stale_duration) = cache_provider.stale_while_revalidate_ttl()
        {
            // When serving stale content, set max-age=0 to indicate the response is not fresh
            // The Results-Cache-Status header will indicate STALE
            let max_age = if results_cache_status == CacheStatus::CacheStaleWhileRevalidate {
                0
            } else {
                cache_provider.ttl().as_secs()
            };

            let cache_control_value = format!(
                "max-age={}, stale-while-revalidate={}",
                max_age,
                stale_duration.as_secs()
            );

            if let Ok(header_value) = HeaderValue::from_str(&cache_control_value) {
                headers.insert(CACHE_CONTROL, header_value);
            }
        }
    }
}

/// Append `field` to the `Vary` response header, preserving any prior
/// value(s). RFC 7231 §7.1.4 allows comma-separated field lists.
pub(super) fn append_vary(headers: &mut HeaderMap, field: &'static str) {
    use http::header::VARY;
    if let Some(existing) = headers.get(VARY)
        && let Ok(existing_str) = existing.to_str()
    {
        // Skip if already present.
        if existing_str
            .split(',')
            .any(|f| f.trim().eq_ignore_ascii_case(field))
        {
            return;
        }
        let combined = format!("{existing_str}, {field}");
        if let Ok(v) = HeaderValue::from_str(&combined) {
            headers.insert(VARY, v);
        }
        return;
    }
    headers.insert(VARY, HeaderValue::from_static(field));
}

/// This is the legacy cache header, preserved for backwards compatibility.
fn status_to_x_cache_value(results_cache_status: CacheStatus) -> Option<HeaderValue> {
    match results_cache_status {
        CacheStatus::CacheHit | CacheStatus::CacheStaleWhileRevalidate => {
            "Hit from spiceai".parse().ok()
        }
        CacheStatus::CacheMiss => "Miss from spiceai".parse().ok(),
        CacheStatus::CacheDisabled | CacheStatus::CacheBypass => None,
    }
}

/// Converts a vector of `RecordBatch` to a JSON string.
fn arrow_to_json(data: &[RecordBatch]) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    write_to_json_string(data)
}

/// Converts a vector of `RecordBatch` to a CSV string.
fn arrow_to_csv(data: &[RecordBatch]) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let output = Vec::new();
    let mut writer = arrow_csv::Writer::new(output);

    for d in data {
        writer.write(d).boxed()?;
    }

    String::from_utf8(writer.into_inner()).boxed()
}

/// Converts a vector of `RecordBatch` to a pretty formatted string.
/// This is equivalent to [`datafusion::dataframe::DataFrame::show`].
fn arrow_to_plain(
    data: &[RecordBatch],
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    pretty_format_batches(data).map(|d| format!("{d}")).boxed()
}

/// Converts a vector of `RecordBatch` to an application/vnd.spiceai.sql.v1+json / application/vnd.spiceai.nsql.v1+json format
fn arrow_to_vnd_sql_json_v1(
    data: &[RecordBatch],
    meta: &ResponseMetadata,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let row_count = data.iter().map(RecordBatch::num_rows).sum::<usize>();

    let mut body = Vec::new();
    write_vnd_json_head(&mut body, data.first().map(RecordBatch::schema).as_ref())?;
    body.extend_from_slice(write_to_json_string(data)?.as_bytes());
    write_vnd_json_tail(&mut body, row_count, meta.sql.as_deref())?;

    String::from_utf8(body).boxed()
}

/// Writes the vnd JSON envelope up to its `data` array: `{"schema":…,"data":`.
/// The schema uses the Arrow JSON schema representation
/// (<https://github.com/apache/arrow/blob/main/docs/source/format/Integration.rst#json-test-data-format>);
/// an empty result has the schema `{}`.
fn write_vnd_json_head(out: &mut Vec<u8>, schema: Option<&SchemaRef>) -> Result<(), BoxedError> {
    out.extend_from_slice(br#"{"schema":"#);
    match schema {
        Some(schema) => serde_json::to_writer(&mut *out, schema.as_ref())?,
        None => out.extend_from_slice(b"{}"),
    }
    out.extend_from_slice(br#","data":"#);
    Ok(())
}

/// Writes the vnd JSON envelope after its `data` array. `row_count` follows
/// the rows so that the rows can stream before the count is known.
fn write_vnd_json_tail(
    out: &mut Vec<u8>,
    row_count: usize,
    sql: Option<&str>,
) -> Result<(), BoxedError> {
    out.extend_from_slice(br#","row_count":"#);
    out.extend_from_slice(row_count.to_string().as_bytes());
    if let Some(sql) = sql {
        out.extend_from_slice(br#","sql":"#);
        serde_json::to_writer(&mut *out, sql)?;
    }
    out.push(b'}');
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    use crate::{
        dataaccelerator::AcceleratorEngineRegistry, datafusion::builder::DataFusionBuilder,
        status::RuntimeStatus,
    };

    /// A query the results cache refuses (e.g. one calling `random()` or
    /// `now()`) reports `CacheDisabled`, which must emit no `X-Cache` or
    /// `Results-Cache-Status` header rather than a misleading HIT/MISS.
    #[test]
    fn disabled_results_cache_emits_no_cache_status_headers() {
        assert!(status_to_x_cache_value(CacheStatus::CacheDisabled).is_none());
        assert!(CacheStatus::CacheDisabled.to_header_string().is_none());
        assert_eq!(CacheStatus::CacheHit.to_header_string(), Some("HIT"));
    }

    /// `/v1/sql` must let clients distinguish outcomes by status code: a
    /// `runtime.query.timeout` expiry maps to 504 Gateway Timeout, a
    /// client-initiated cancel to 499, and both classifications must hold for
    /// the pre-stream (`QueryError`) and mid-stream (`DataFusionError`) paths.
    #[test]
    fn sql_error_response_status_for_timeout_and_cancellation() {
        let timeout_err = QueryError::QueryTimedOut {
            query_id: "q1".to_string(),
            timeout: "30s".to_string(),
        };
        let response = sql_error_response(
            timeout_err.to_string(),
            SqlErrorKind::of_query_error(&timeout_err),
        );
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);

        let cancel_err = QueryError::QueryCancelled {
            query_id: "q1".to_string(),
        };
        let response = sql_error_response(
            cancel_err.to_string(),
            SqlErrorKind::of_query_error(&cancel_err),
        );
        assert_eq!(response.status().as_u16(), 499);

        // Mid-stream errors arrive as DataFusionError::External wrapping the
        // query error (produced by `attach_cancellation_to_stream`).
        let stream_timeout =
            datafusion::error::DataFusionError::External(Box::new(QueryError::QueryTimedOut {
                query_id: "q1".to_string(),
                timeout: "30s".to_string(),
            }));
        let response = sql_error_response(
            stream_timeout.to_string(),
            SqlErrorKind::of_datafusion_error(&stream_timeout),
        );
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);

        let stream_cancel =
            datafusion::error::DataFusionError::External(Box::new(QueryError::QueryCancelled {
                query_id: "q1".to_string(),
            }));
        let response = sql_error_response(
            stream_cancel.to_string(),
            SqlErrorKind::of_datafusion_error(&stream_cancel),
        );
        assert_eq!(response.status().as_u16(), 499);
    }

    /// A query a cluster scheduler cannot route yet is answered 503, which
    /// clients and load balancers retry: from the planning error the query
    /// API returns and from a streamed error alike.
    #[test]
    fn sql_error_response_classifies_unavailable_partitions() {
        let unavailable = || {
            datafusion::error::DataFusionError::Context(
                "partitioned_table_scan_rewrite".to_string(),
                Box::new(datafusion::error::DataFusionError::External(
                    crate::datafusion::error::SpiceExternalError::partitions_unavailable(
                        "Cannot query t: no ready executor serves 1 of its 2 partitions; retry the query"
                            .to_string(),
                    ),
                )),
            )
        };
        let planning = QueryError::UnableToExecuteQuery {
            source: unavailable(),
        };
        let response = sql_error_response(
            planning.to_string(),
            SqlErrorKind::of_query_error(&planning),
        );
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let streamed = unavailable();
        let response = sql_error_response(
            streamed.to_string(),
            SqlErrorKind::of_datafusion_error(&streamed),
        );
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// A memory-pool refusal has to be told apart from the cancellation and
    /// timeout kinds so it can be logged at `warn` and answered with a status
    /// that intermediaries read as retriable. A plain execution failure stays
    /// on 400: only the capacity condition moves.
    #[test]
    fn sql_error_response_classifies_resource_exhaustion() {
        let exhausted = datafusion::error::DataFusionError::ResourcesExhausted(
            "Additional allocation failed for HashJoinInput[135]".to_string(),
        );
        assert!(matches!(
            SqlErrorKind::of_datafusion_error(&exhausted),
            SqlErrorKind::ResourcesExhausted
        ));
        let response = sql_error_response(
            exhausted.to_string(),
            SqlErrorKind::of_datafusion_error(&exhausted),
        );
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        // Execution commonly wraps the pool error before it reaches the handler.
        let wrapped = datafusion::error::DataFusionError::Context(
            "Join Error".to_string(),
            Box::new(datafusion::error::DataFusionError::ResourcesExhausted(
                "out of memory".to_string(),
            )),
        );
        assert!(matches!(
            SqlErrorKind::of_datafusion_error(&wrapped),
            SqlErrorKind::ResourcesExhausted
        ));
        let wrapped_response = sql_error_response(
            wrapped.to_string(),
            SqlErrorKind::of_datafusion_error(&wrapped),
        );
        assert_eq!(
            wrapped_response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "a wrapped pool refusal must reach the same status as a bare one"
        );

        // A plain execution failure must stay on the quiet path, and on 400.
        let general = datafusion::error::DataFusionError::Execution("boom".to_string());
        assert!(matches!(
            SqlErrorKind::of_datafusion_error(&general),
            SqlErrorKind::General
        ));
        let general_response = sql_error_response(
            general.to_string(),
            SqlErrorKind::of_datafusion_error(&general),
        );
        assert_eq!(
            general_response.status(),
            StatusCode::BAD_REQUEST,
            "only the capacity condition moves off 400"
        );

        // A read-only rejection keeps its 403 rather than being swept into the
        // capacity status.
        let read_only = datafusion::error::DataFusionError::Execution(
            "Insert not allowed in a read-only SQL context".to_string(),
        );
        let read_only_response = sql_error_response(
            read_only.to_string(),
            SqlErrorKind::of_datafusion_error(&read_only),
        );
        assert_eq!(read_only_response.status(), StatusCode::FORBIDDEN);
    }

    /// A join wraps its build-side failure in `Shared`, which renders with no
    /// prefix, so the body reads like a bare refusal while the status stays on
    /// 400. The status has to be derived from the variant, not the text.
    #[test]
    fn a_shared_pool_refusal_reaches_the_same_status() {
        let shared = datafusion::error::DataFusionError::Shared(std::sync::Arc::new(
            datafusion::error::DataFusionError::ResourcesExhausted(
                "Additional allocation failed for HashJoinInput[135]".to_string(),
            ),
        ));
        assert!(
            shared.to_string().starts_with("Resources exhausted: "),
            "`Shared` must delegate its message, or this test is not covering the real shape"
        );
        assert!(matches!(
            SqlErrorKind::of_datafusion_error(&shared),
            SqlErrorKind::ResourcesExhausted
        ));
        let response = sql_error_response(
            shared.to_string(),
            SqlErrorKind::of_datafusion_error(&shared),
        );
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        // A shared failure that is not a refusal must not be swept up with it.
        let shared_other = datafusion::error::DataFusionError::Shared(std::sync::Arc::new(
            datafusion::error::DataFusionError::Execution("boom".to_string()),
        ));
        assert!(matches!(
            SqlErrorKind::of_datafusion_error(&shared_other),
            SqlErrorKind::General
        ));
    }

    /// `TrackConsumersPool` writes its breakdown as `"… (across reservations)
    /// as:\n{consumers}\nError: {msg}"`, so the message reaching
    /// `sql_error_response` is genuinely multi-line. The log record has to stay
    /// on one line for a collector to group it, while the response body keeps
    /// the breakdown the caller needs to act on.
    #[tokio::test]
    async fn resource_exhaustion_logs_on_one_line_but_responds_in_full() {
        let message = concat!(
            "Resources exhausted: Additional allocation failed for HashJoinInput[135] ",
            "with top memory consumers (across reservations) as:\n",
            "  HashJoinInput[135]#12(can spill: false) consumed 1.0 GB, peak 1.0 GB,\n",
            "  ExternalSorter[3]#9(can spill: true) consumed 512.0 MB, peak 600.0 MB.\n",
            "Error: Failed to allocate additional 256.0 MB for HashJoinInput[135]"
        );

        let logged = single_line(message);
        assert!(
            !logged.chars().any(char::is_control),
            "no control character may survive into the log line: {logged}"
        );
        assert!(
            logged.contains("consumed 1.0 GB, peak 1.0 GB,   ExternalSorter[3]"),
            "collapsing must keep the breakdown, only its line breaks: {logged}"
        );
        assert_eq!(
            logged.chars().count(),
            message.chars().count(),
            "each control character is replaced, never dropped, so offsets into \
             the log line still match the message"
        );

        // A message with nothing to collapse is borrowed, not rebuilt.
        let plain = "Resources exhausted: out of memory";
        assert!(matches!(
            single_line(plain),
            std::borrow::Cow::Borrowed(kept) if kept == plain
        ));

        // Collapsing is for the log only: the body the caller reads is the
        // engine's own message, newlines and all.
        let response = sql_error_response(message.to_string(), SqlErrorKind::ResourcesExhausted);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("the error response body must be readable");
        assert_eq!(
            std::str::from_utf8(&body).expect("the body must be utf8"),
            message,
            "the response body must stay verbatim"
        );
    }

    #[test]
    fn transaction_statements_requires_begin_and_commit() {
        assert_eq!(
            transaction_statements("BEGIN; SELECT 1; COMMIT"),
            Some(vec!["SELECT 1".to_string()])
        );
        assert!(transaction_statements("SELECT 1; SELECT 2").is_none());
        assert!(transaction_statements("BEGIN; COMMIT").is_none());
    }

    #[tokio::test]
    async fn transaction_executes_bound_parameters_and_returns_final_result() {
        let df = Arc::new(
            DataFusionBuilder::new(
                RuntimeStatus::new(),
                Arc::new(AcceleratorEngineRegistry::new()),
                tokio::runtime::Handle::current(),
            )
            .build(),
        );
        let parameters = ParamValues::from(vec![
            ScalarValue::Int64(Some(41)),
            ScalarValue::Int64(Some(42)),
        ]);

        let response = sql_to_http_response(
            df,
            Arc::from("BEGIN; SELECT $1 AS ignored; SELECT $2 AS value; COMMIT"),
            Some(parameters),
            ResponseMimeType::Json,
            false,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .expect("transaction response body should be readable");
        assert_eq!(body.as_ref(), br#"[{"value":42}]"#);
    }

    #[test]
    fn test_arrow_to_vnd_json_v1() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("total_sales", DataType::Int64, false),
        ]));

        let customer_ids = StringArray::from(vec!["12345", "67890"]);
        let total_sales = Int64Array::from(vec![150_000, 125_000]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(customer_ids), Arc::new(total_sales)])
                .expect("to create batch");

        // Test conversion without SQL
        let result_without_sql =
            arrow_to_vnd_sql_json_v1(std::slice::from_ref(&batch), &ResponseMetadata::empty())
                .expect("to convert");
        insta::assert_json_snapshot!(
            "vnd_json_v1_without_sql",
            serde_json::from_str::<serde_json::Value>(&result_without_sql).expect("to parse")
        );

        // Test conversion with SQL
        let metadata = ResponseMetadata::empty()
            .with_sql("SELECT customer_id, total_sales FROM sales_summary LIMIT 2;");
        let result_with_sql = arrow_to_vnd_sql_json_v1(&[batch], &metadata).expect("to convert");
        insta::assert_json_snapshot!(
            "vnd_json_v1_with_sql",
            serde_json::from_str::<serde_json::Value>(&result_with_sql).expect("to parse")
        );
    }

    /// Encodes a whole result the way the buffered `to_http_response` does.
    type BufferedEncoding = fn(&[RecordBatch]) -> Result<String, BoxedError>;

    /// The formats `/v1/sql` streams, with the buffered encoding each streamed
    /// body must match byte for byte.
    fn streamed_formats() -> [(ResponseMimeType, BufferedEncoding); 3] {
        [
            (ResponseMimeType::Json, arrow_to_json),
            (ResponseMimeType::VndSqlJsonV1, |data| {
                arrow_to_vnd_sql_json_v1(data, &ResponseMetadata::empty())
            }),
            (ResponseMimeType::Csv, arrow_to_csv),
        ]
    }

    /// Each chunked `/v1/sql` body must be byte-identical to the buffered
    /// encoding of its format across multi-batch, empty, and NULL cases.
    #[tokio::test]
    async fn streamed_body_matches_buffered_output() {
        use arrow::array::ArrayRef;
        use datafusion::error::DataFusionError;
        use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use http_body::Body as _;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let make = |ids: Vec<Option<i64>>, names: Vec<Option<&str>>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)) as ArrayRef,
                    Arc::new(StringArray::from(names)) as ArrayRef,
                ],
            )
            .expect("record batch")
        };

        let cases: Vec<Vec<RecordBatch>> = vec![
            // Multiple batches, an empty batch mid-stream, and NULL values.
            vec![
                make(vec![Some(1), None], vec![Some("a"), Some("b")]),
                make(vec![], vec![]),
                make(vec![Some(3)], vec![None]),
            ],
            // Empty result.
            vec![],
            // Single empty batch.
            vec![make(vec![], vec![])],
        ];

        for (format, buffered) in streamed_formats() {
            for batches in &cases {
                let expected = buffered(batches).expect("buffered body");
                if matches!(format, ResponseMimeType::VndSqlJsonV1) && batches.is_empty() {
                    assert_eq!(expected, r#"{"schema":{},"data":[],"row_count":0}"#);
                }

                let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
                let account = EgressAccount::register(&pool, "test_egress");

                let mut batches = batches.clone().into_iter();
                let first = batches.next();
                let rest: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&schema),
                    futures::stream::iter(batches.map(Ok::<_, DataFusionError>)),
                ));
                let encoder = streamed_encoder(format, &schema).expect("a streamed format");

                let mut body = std::pin::pin!(sql_body_from_batches(first, rest, encoder, account));
                let mut streamed = Vec::new();
                while let Some(chunk) = body.next().await {
                    streamed.extend_from_slice(&chunk.expect("chunk"));
                }

                assert_eq!(
                    String::from_utf8(streamed).expect("utf8"),
                    expected,
                    "{format:?}: streamed body must match the buffered encoding"
                );
            }

            // A complete small result is still a chunked body. Sending it whole with
            // `Content-Length` is an HTTP framing change and needs an Enhancement.
            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            let data: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::iter(std::iter::once(Ok::<_, DataFusionError>(make(
                    vec![Some(1)],
                    vec![Some("a")],
                )))),
            ));
            let response =
                query_stream_to_http_response(data, CacheStatus::CacheMiss, format, pool).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert!(
                response.body().size_hint().exact().is_none(),
                "{format:?} responses must stream; an exact size hint is a Content-Length body"
            );
        }
    }

    /// DECIMAL values keep every digit in each `/v1/sql` format, streamed and
    /// buffered: a 38-digit value does not become a float.
    #[tokio::test]
    async fn decimal_values_keep_every_digit() {
        use arrow::array::{ArrayRef, Decimal128Array};
        use datafusion::error::DataFusionError;
        use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        let schema = Arc::new(Schema::new(vec![
            Field::new("d38", DataType::Decimal128(38, 0), false),
            Field::new("d19", DataType::Decimal128(19, 2), false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(
                    Decimal128Array::from(vec![
                        99_999_999_999_999_999_999_999_999_999_999_999_999_i128,
                    ])
                    .with_precision_and_scale(38, 0)
                    .expect("decimal(38, 0)"),
                ) as ArrayRef,
                Arc::new(
                    Decimal128Array::from(vec![1_234_567_890_123_456_789_i128])
                        .with_precision_and_scale(19, 2)
                        .expect("decimal(19, 2)"),
                ) as ArrayRef,
            ],
        )
        .expect("record batch");
        let row = r#"{"d38":99999999999999999999999999999999999999,"d19":12345678901234567.89}"#;

        for (format, buffered) in streamed_formats() {
            let buffered = buffered(std::slice::from_ref(&batch)).expect("buffered body");

            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            let data: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::iter(std::iter::once(Ok::<_, DataFusionError>(batch.clone()))),
            ));
            let response =
                query_stream_to_http_response(data, CacheStatus::CacheMiss, format, pool).await;
            let streamed = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("streamed body");
            let streamed = String::from_utf8(streamed.to_vec()).expect("utf8");

            for body in [&buffered, &streamed] {
                match format {
                    ResponseMimeType::Json => assert_eq!(*body, format!("[{row}]")),
                    ResponseMimeType::VndSqlJsonV1 => assert!(
                        body.ends_with(&format!(r#","data":[{row}],"row_count":1}}"#)),
                        "{format:?}: `{body}` must end with the exact rows and row count"
                    ),
                    ResponseMimeType::Csv => assert_eq!(
                        *body,
                        "d38,d19\n99999999999999999999999999999999999999,12345678901234567.89\n"
                    ),
                    other => panic!("{other:?} is not a streamed format"),
                }
            }
        }
    }

    /// CSV cannot encode a nested column, so the request fails with an error
    /// status rather than a `200` head over an empty body.
    #[tokio::test]
    async fn csv_with_a_nested_column_is_an_error_response() {
        use arrow::array::{Array, ArrayRef, ListArray};
        use arrow::datatypes::Int64Type;
        use datafusion::error::DataFusionError;
        use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        let list = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(1)])]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(list) as ArrayRef])
            .expect("record batch");
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let data: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(std::iter::once(Ok::<_, DataFusionError>(batch))),
        ));

        let response = query_stream_to_http_response(
            data,
            CacheStatus::CacheMiss,
            ResponseMimeType::Csv,
            pool,
        )
        .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("error body");
        assert!(
            String::from_utf8_lossy(&body).contains("not supported in CSV"),
            "the error names the cause: `{}`",
            String::from_utf8_lossy(&body)
        );
    }

    /// HTTP JSON, vnd JSON and CSV from `QueryResult::from_cached_raw` must
    /// match the owned-stream path: same status, cache header, and body bytes.
    #[tokio::test]
    async fn cached_raw_http_response_matches_owned_stream() {
        use cache::result::query::wrap_raw_batches;
        use datafusion::error::DataFusionError;
        use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let make = |ids: Vec<Option<i64>>, names: Vec<Option<&str>>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)) as ArrayRef,
                    Arc::new(StringArray::from(names)) as ArrayRef,
                ],
            )
            .expect("record batch")
        };

        let cases: Vec<Vec<RecordBatch>> = vec![
            vec![
                make(vec![Some(1), None], vec![Some("a"), Some("b")]),
                make(vec![], vec![]),
                make(vec![Some(3)], vec![None]),
            ],
            vec![],
            vec![make(vec![], vec![])],
            vec![make(vec![Some(1)], vec![Some("a")])],
        ];

        for batches in cases {
            for format in [
                ResponseMimeType::Json,
                ResponseMimeType::VndSqlJsonV1,
                ResponseMimeType::Csv,
            ] {
                let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
                let owned = QueryResult::new(
                    Box::pin(RecordBatchStreamAdapter::new(
                        Arc::clone(&schema),
                        futures::stream::iter(
                            batches.clone().into_iter().map(Ok::<_, DataFusionError>),
                        ),
                    )),
                    CacheStatus::CacheHit,
                );
                let raw = QueryResult::from_cached_raw(
                    wrap_raw_batches(batches.clone()),
                    Arc::clone(&schema),
                    CacheStatus::CacheHit,
                );

                let owned_response =
                    query_result_to_http_response(owned, format, Arc::clone(&pool)).await;
                let raw_response = query_result_to_http_response(raw, format, pool).await;

                assert_eq!(
                    owned_response.status(),
                    raw_response.status(),
                    "{format:?}: status"
                );
                assert_eq!(
                    owned_response.headers().get("Results-Cache-Status"),
                    raw_response.headers().get("Results-Cache-Status"),
                    "{format:?}: Results-Cache-Status"
                );

                let owned_body = axum::body::to_bytes(owned_response.into_body(), 64 * 1024)
                    .await
                    .expect("owned body");
                let raw_body = axum::body::to_bytes(raw_response.into_body(), 64 * 1024)
                    .await
                    .expect("raw body");
                assert_eq!(
                    owned_body, raw_body,
                    "{format:?}: body bytes must match the owned-stream path"
                );
            }
        }
    }
}
