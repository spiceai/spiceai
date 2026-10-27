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
            write_to_json_value,
        },
    },
    egress::EgressAccount,
    http::server_timing::ServerTiming,
    status::ComponentStatus,
};
use arrow::{array::RecordBatch, datatypes::SchemaRef, util::pretty::pretty_format_batches};
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

use crate::datafusion::error::is_partitions_unavailable;
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
// The default JSON format is streamed batch-by-batch via a chunked body, so the
// full result set is never materialized in memory (previously it was buffered
// twice — once as `Vec<RecordBatch>`, then again as the rendered `String`). The
// other formats — csv, plain, the vnd.* envelopes, and JSON containing union
// columns — still buffer via `to_http_response`.
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
/// Default JSON responses stream batch-by-batch. Formats that require complete
/// result metadata, and JSON schemas containing union columns, use the buffered
/// response path.
async fn query_stream_to_http_response(
    mut data_stream: SendableRecordBatchStream,
    cache_status: CacheStatus,
    format: ResponseMimeType,
    memory_pool: Arc<dyn MemoryPool>,
) -> Response {
    // JSON, `vnd.spiceai.sql.v1+json` and CSV stream batch by batch; `text/plain`
    // (column widths need every row) and union columns (which the arrow-json
    // array writer can't render) fall back to the buffered path. Streamability is
    // a schema property, so decide it before pulling any batch.
    let Some(kind) = StreamedBody::for_response(format, &data_stream.schema()) else {
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

    let account = EgressAccount::register(&memory_pool, "http_egress");
    let body = match record_batch_body(kind, first, data_stream, account) {
        Ok(body) => body,
        Err(e) => return encoding_error_response(&e),
    };
    let headers = response_headers(format, cache_status).await;
    streamed_response(headers, Body::from_stream(body))
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
    /// query reads; a retry can succeed.
    Unavailable,
}

impl SqlErrorKind {
    fn of_query_error(e: &QueryError) -> Self {
        match e {
            QueryError::QueryCancelled { .. } => Self::Cancellation,
            QueryError::QueryTimedOut { .. } => Self::Timeout,
            QueryError::UnableToExecuteQuery { source }
            | QueryError::UnableToCreateMemoryStream { source }
            | QueryError::UnableToCollectResults { source }
                if is_partitions_unavailable(source) =>
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
        } else if is_partitions_unavailable(e) {
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
        SqlErrorKind::Timeout => StatusCode::GATEWAY_TIMEOUT,
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
    let Some(kind) = StreamedBody::for_response(format, &schema) else {
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

    let account = EgressAccount::register(&memory_pool, "http_egress");
    let body = match record_batch_body(kind, first, data_stream, account) {
        Ok(body) => body,
        Err(e) => return encoding_error_response(&e),
    };
    let headers = response_headers(format, cache_status).await;
    streamed_response(headers, Body::from_stream(body))
}

/// The response for a result the format cannot encode (CSV rejects nested
/// columns), as the buffered path reports it.
fn encoding_error_response(error: &std::io::Error) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        HeaderMap::new(),
        error.to_string(),
    )
        .into_response()
}

/// A `200 OK` whose body streams, marked so its `Server-Timing` total is sent
/// as a trailer: the total is not known until the body ends.
fn streamed_response(headers: HeaderMap, body: Body) -> Response {
    let mut response = (StatusCode::OK, headers, body).into_response();
    response.extensions_mut().insert(ServerTiming::Trailer);
    response
}

trait SqlJsonBatch: Send + 'static {
    fn as_record_batch(&self) -> &RecordBatch;
}

impl SqlJsonBatch for RecordBatch {
    fn as_record_batch(&self) -> &RecordBatch {
        self
    }
}

impl SqlJsonBatch for Arc<RecordBatch> {
    fn as_record_batch(&self) -> &RecordBatch {
        self.as_ref()
    }
}

/// The response formats `/v1/sql` streams instead of buffering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamedBody {
    /// `application/json`: one JSON array of row objects.
    JsonArray,
    /// `application/vnd.spiceai.sql.v1+json` (and the `nsql` variant):
    /// `{"schema":…,"data":[…],"row_count":N}`.
    VndSqlJsonV1,
    /// `text/csv`: a header line, then one line per row.
    Csv,
}

impl StreamedBody {
    /// The streamed framing for `format`, or `None` when the response has to be
    /// built from the whole result: `text/plain` sizes its columns from every
    /// row, and union columns only render through the `serde_json` path.
    fn for_response(format: ResponseMimeType, schema: &arrow::datatypes::Schema) -> Option<Self> {
        if schema_has_union_columns(schema) {
            return None;
        }
        match format {
            ResponseMimeType::Json => Some(Self::JsonArray),
            ResponseMimeType::VndSqlJsonV1 | ResponseMimeType::VndNsqlJsonV1 => {
                Some(Self::VndSqlJsonV1)
            }
            ResponseMimeType::Csv => Some(Self::Csv),
            ResponseMimeType::Plain => None,
        }
    }
}

/// Serializes record batches into one response body, a chunk per batch. The
/// streamed and buffered paths both encode through this, so a response's bytes
/// do not depend on whether it was streamed.
///
/// Row values are written by the arrow-json / arrow-csv writers straight from
/// the Arrow arrays. Nothing goes through `serde_json::Value`, whose numbers are
/// `f64`: that is what turned `DECIMAL(38, s)` values into rounded floats in
/// `vnd.spiceai.sql.v1+json`.
enum BodyEncoder {
    Json {
        writer: arrow_json::Writer<Vec<u8>, arrow_json::writer::JsonArray>,
        /// The `vnd.spiceai.sql.v1+json` object around the array, if any.
        envelope: Option<VndEnvelope>,
        rows: usize,
    },
    Csv {
        header_pending: bool,
    },
}

/// What the `vnd.spiceai.sql.v1+json` object carries besides `data`.
struct VndEnvelope {
    sql: Option<String>,
}

impl BodyEncoder {
    /// `schema` is the first batch's schema (`None` for an empty result). The
    /// `vnd` envelope writes it ahead of the rows, as `{}` when there are none.
    fn new(kind: StreamedBody, schema: Option<&arrow::datatypes::Schema>) -> std::io::Result<Self> {
        match kind {
            StreamedBody::JsonArray => Ok(Self::Json {
                writer: json_array_writer(),
                envelope: None,
                rows: 0,
            }),
            StreamedBody::VndSqlJsonV1 => Self::vnd(schema, None),
            StreamedBody::Csv => Ok(Self::Csv {
                header_pending: true,
            }),
        }
    }

    fn vnd(
        schema: Option<&arrow::datatypes::Schema>,
        sql: Option<String>,
    ) -> std::io::Result<Self> {
        let mut writer = json_array_writer();
        // `row_count` is only known at the end, so it follows `data`; the keys
        // of a JSON object are unordered.
        let out = writer.get_mut();
        out.extend_from_slice(br#"{"schema":"#);
        match schema {
            Some(schema) => {
                serde_json::to_writer(&mut *out, schema).map_err(std::io::Error::other)?
            }
            None => out.extend_from_slice(b"{}"),
        }
        out.extend_from_slice(br#","data":"#);
        Ok(Self::Json {
            writer,
            envelope: Some(VndEnvelope { sql }),
            rows: 0,
        })
    }

    /// The bytes for `batch`, including any pending prefix.
    fn encode(&mut self, batch: &RecordBatch) -> std::io::Result<Vec<u8>> {
        match self {
            Self::Json { writer, rows, .. } => {
                writer.write(batch).map_err(std::io::Error::other)?;
                *rows += batch.num_rows();
                Ok(std::mem::take(writer.get_mut()))
            }
            Self::Csv { header_pending } => {
                // The arrow-csv writer flushes on every `write`; a writer per
                // batch hands back exactly that batch's bytes. Only the first
                // batch carries the header, as with a single writer.
                let mut writer = arrow_csv::WriterBuilder::new()
                    .with_header(*header_pending)
                    .build(Vec::new());
                writer.write(batch).map_err(std::io::Error::other)?;
                *header_pending = false;
                Ok(writer.into_inner())
            }
        }
    }

    /// The closing bytes: `]` (or `[]`), plus the rest of the `vnd` object.
    fn finish(self) -> std::io::Result<Vec<u8>> {
        match self {
            Self::Json {
                mut writer,
                envelope,
                rows,
            } => {
                writer.finish().map_err(std::io::Error::other)?;
                let mut tail = std::mem::take(writer.get_mut());
                if let Some(VndEnvelope { sql }) = envelope {
                    tail.extend_from_slice(format!(r#","row_count":{rows}"#).as_bytes());
                    if let Some(sql) = sql {
                        tail.extend_from_slice(br#","sql":"#);
                        serde_json::to_writer(&mut tail, &sql).map_err(std::io::Error::other)?;
                    }
                    tail.push(b'}');
                }
                Ok(tail)
            }
            Self::Csv { .. } => Ok(Vec::new()),
        }
    }
}

/// Streams the query result in `kind`'s framing, one input batch at a time,
/// charging each serialized chunk against the query memory pool via `account`.
/// The bytes emitted are identical to the buffered `to_http_response` output.
///
/// The first batch is encoded before this returns, so a result the format
/// cannot encode still fails with an error status instead of a `200` whose
/// body breaks off.
fn record_batch_body<S, B>(
    kind: StreamedBody,
    first: Option<B>,
    mut rest: S,
    account: Arc<EgressAccount>,
) -> std::io::Result<impl futures::Stream<Item = Result<Bytes, std::io::Error>> + Send>
where
    S: Stream<Item = Result<B, datafusion::error::DataFusionError>> + Send + Unpin + 'static,
    B: SqlJsonBatch,
{
    let first = first.as_ref().map(SqlJsonBatch::as_record_batch);
    let mut encoder = BodyEncoder::new(kind, first.map(|batch| batch.schema_ref().as_ref()))?;
    let head = match first {
        Some(batch) => encoder.encode(batch)?,
        None => Vec::new(),
    };
    Ok(try_stream! {
        if !head.is_empty() {
            let size = head.len();
            account.reserve(size).await;
            yield Bytes::from(head);
            account.release(size);
        }
        while let Some(item) = rest.next().await {
            let batch = item.map_err(|e| std::io::Error::other(e.to_string()))?;
            let chunk = encoder.encode(batch.as_record_batch())?;
            if !chunk.is_empty() {
                let size = chunk.len();
                account.reserve(size).await;
                yield Bytes::from(chunk);
                account.release(size);
            }
        }
        let tail = encoder.finish()?;
        if !tail.is_empty() {
            let size = tail.len();
            account.reserve(size).await;
            yield Bytes::from(tail);
            account.release(size);
        }
    })
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
            arrow_to_vnd_sql_json_v1(&data, meta)
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
    meta: ResponseMetadata,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let schema = data.first().map(RecordBatch::schema);
    if schema
        .as_deref()
        .is_some_and(|schema| schema_has_union_columns(schema))
    {
        return arrow_to_vnd_sql_json_v1_via_value(data, meta);
    }

    let mut encoder = BodyEncoder::vnd(schema.as_deref(), meta.sql)?;
    let mut body = Vec::new();
    for batch in data {
        body.extend(encoder.encode(batch)?);
    }
    body.extend(encoder.finish()?);
    String::from_utf8(body).boxed()
}

/// The `serde_json::Value` rendering, kept for union columns, which the
/// arrow-json writer cannot encode.
fn arrow_to_vnd_sql_json_v1_via_value(
    data: &[RecordBatch],
    meta: ResponseMetadata,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Calculate total row count across all batches
    let row_count = data.iter().map(RecordBatch::num_rows).sum::<usize>();

    let schema_json = if let Some(batch) = data.first() {
        // Use built-in Arrow JSON schema representation: https://github.com/apache/arrow/blob/main/docs/source/format/Integration.rst#json-test-data-format
        serde_json::to_value(batch.schema())?
    } else {
        serde_json::json!({})
    };

    let mut result = json!({
        "schema": schema_json,
        "data": write_to_json_value(data)?,
        "row_count": row_count,
    });

    if let Some(sql) = meta.sql {
        result["sql"] = serde_json::Value::String(sql);
    }

    serde_json::to_string(&result).boxed()
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
            arrow_to_vnd_sql_json_v1(std::slice::from_ref(&batch), ResponseMetadata::empty())
                .expect("to convert");
        insta::assert_json_snapshot!(
            "vnd_json_v1_without_sql",
            serde_json::from_str::<serde_json::Value>(&result_without_sql).expect("to parse")
        );

        // Test conversion with SQL
        let metadata = ResponseMetadata::empty()
            .with_sql("SELECT customer_id, total_sales FROM sales_summary LIMIT 2;");
        let result_with_sql = arrow_to_vnd_sql_json_v1(&[batch], metadata).expect("to convert");
        insta::assert_json_snapshot!(
            "vnd_json_v1_with_sql",
            serde_json::from_str::<serde_json::Value>(&result_with_sql).expect("to parse")
        );
    }

    /// One row of the decimals that `serde_json::Value` (f64 numbers) used to
    /// round: 38 digits at scale 10, the largest `DECIMAL(38, 0)`, a zero that
    /// keeps its scale, and a money value whose trailing zero is significant.
    fn decimal_batch() -> RecordBatch {
        use arrow::array::Decimal128Array;
        let column = |value: i128, precision: u8, scale: i8| -> ArrayRef {
            Arc::new(
                Decimal128Array::from(vec![value])
                    .with_precision_and_scale(precision, scale)
                    .expect("valid decimal"),
            )
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("big", DataType::Decimal128(38, 10), false),
            Field::new("max38", DataType::Decimal128(38, 0), false),
            Field::new("zero", DataType::Decimal128(38, 10), false),
            Field::new("small", DataType::Decimal128(10, 2), false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                column(12_345_678_901_234_567_890_123_456_780_123_456_789, 38, 10),
                column(10_i128.pow(38) - 1, 38, 0),
                column(0, 38, 10),
                column(1_234_567_890, 10, 2),
            ],
        )
        .expect("decimal batch")
    }

    const DECIMAL_ROW: &str = r#"{"big":1234567890123456789012345678.0123456789,"max38":99999999999999999999999999999999999999,"zero":0.0000000000,"small":12345678.90}"#;

    fn stream_of(schema: SchemaRef, batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        use datafusion::error::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)),
        ))
    }

    async fn response_text(response: Response) -> String {
        let body = axum::body::to_bytes(response.into_body(), 16 * 1024 * 1024)
            .await
            .expect("response body");
        String::from_utf8(body.to_vec()).expect("utf8 body")
    }

    /// `vnd.spiceai.sql.v1+json` must carry DECIMAL values with the same text
    /// as `application/json`: every digit and the declared scale.
    #[tokio::test]
    async fn vnd_json_v1_keeps_decimal_digits_and_scale() {
        use datafusion::execution::memory_pool::UnboundedMemoryPool;

        let batch = decimal_batch();
        let buffered =
            arrow_to_vnd_sql_json_v1(std::slice::from_ref(&batch), ResponseMetadata::empty())
                .expect("buffered vnd json");
        assert!(
            buffered.contains(&format!(r#""data":[{DECIMAL_ROW}]"#)),
            "buffered vnd json rounded a decimal: {buffered}"
        );

        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let response = query_stream_to_http_response(
            stream_of(batch.schema(), vec![batch.clone()]),
            CacheStatus::CacheMiss,
            ResponseMimeType::VndSqlJsonV1,
            pool,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let streamed = response_text(response).await;
        assert_eq!(streamed, buffered, "streamed and buffered vnd json differ");

        let plain = write_to_json_string(std::slice::from_ref(&batch)).expect("plain json");
        assert_eq!(plain, format!("[{DECIMAL_ROW}]"));
        let parsed: serde_json::Value = serde_json::from_str(&streamed).expect("valid json");
        assert_eq!(parsed["row_count"], 1);
        assert_eq!(
            parsed["schema"]["fields"][0]["data_type"]["Decimal128"][0],
            38
        );
    }

    /// `vnd.spiceai.sql.v1+json` and CSV responses must be byte-identical
    /// whether they stream or buffer, across multi-batch, empty and NULL
    /// results, and must stream (no `Content-Length`).
    #[tokio::test]
    async fn vnd_json_and_csv_stream_the_buffered_bytes() {
        use datafusion::execution::memory_pool::UnboundedMemoryPool;
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
            vec![
                make(vec![Some(1), None], vec![Some("a"), Some("b,\"c\"")]),
                make(vec![], vec![]),
                make(vec![Some(3)], vec![None]),
            ],
            vec![],
            vec![make(vec![], vec![])],
        ];

        for format in [ResponseMimeType::VndSqlJsonV1, ResponseMimeType::Csv] {
            for batches in &cases {
                let (_, _, expected) = to_http_response(
                    batches.clone(),
                    CacheStatus::CacheMiss,
                    format,
                    ResponseMetadata::empty(),
                )
                .await;
                let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
                let response = query_stream_to_http_response(
                    stream_of(Arc::clone(&schema), batches.clone()),
                    CacheStatus::CacheMiss,
                    format,
                    pool,
                )
                .await;
                assert_eq!(response.status(), StatusCode::OK);
                assert!(
                    response.body().size_hint().exact().is_none(),
                    "{format:?} must stream; an exact size hint is a buffered body"
                );
                assert_eq!(
                    response_text(response).await,
                    expected,
                    "{format:?} streamed bytes differ from the buffered response"
                );
            }
        }
    }

    /// CSV cannot encode nested columns. A streamed CSV response must still
    /// fail with the buffered path's status and message, not a `200` whose
    /// body breaks off after the headers.
    #[tokio::test]
    async fn csv_of_a_nested_column_is_an_error_status_not_a_broken_body() {
        use arrow::array::ListArray;
        use arrow::datatypes::Int32Type;
        use datafusion::execution::memory_pool::UnboundedMemoryPool;

        let list =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1), Some(2)])]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(list) as ArrayRef])
            .expect("list batch");

        let (status, _, expected) = to_http_response(
            vec![batch.clone()],
            CacheStatus::CacheMiss,
            ResponseMimeType::Csv,
            ResponseMetadata::empty(),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let response = query_stream_to_http_response(
            stream_of(schema, vec![batch]),
            CacheStatus::CacheMiss,
            ResponseMimeType::Csv,
            pool,
        )
        .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(response_text(response).await, expected);
    }

    /// The first rows of a `vnd.spiceai.sql.v1+json` or CSV response must reach
    /// the client while the query is still producing: a body that waits for the
    /// last batch holds the whole result in memory.
    #[tokio::test]
    async fn vnd_json_and_csv_send_rows_before_the_query_finishes() {
        use datafusion::error::DataFusionError;
        use datafusion::execution::memory_pool::UnboundedMemoryPool;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use http_body_util::BodyExt as _;

        let batch = decimal_batch();
        for format in [ResponseMimeType::VndSqlJsonV1, ResponseMimeType::Csv] {
            // Two batches, then a query that never finishes.
            let never_ends: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                batch.schema(),
                futures::stream::iter(vec![
                    Ok::<_, DataFusionError>(batch.clone()),
                    Ok(batch.clone()),
                ])
                .chain(futures::stream::pending()),
            ));
            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                query_stream_to_http_response(never_ends, CacheStatus::CacheMiss, format, pool),
            )
            .await
            .unwrap_or_else(|_| panic!("{format:?} waited for the whole result before responding"));
            assert_eq!(response.status(), StatusCode::OK);

            let mut body = response.into_body();
            let mut received = Vec::new();
            while !String::from_utf8_lossy(&received).contains("12345678.90") {
                let frame = tokio::time::timeout(std::time::Duration::from_secs(5), body.frame())
                    .await
                    .unwrap_or_else(|_| panic!("{format:?} held back rows it already had"))
                    .expect("a frame")
                    .expect("frame ok");
                if let Ok(data) = frame.into_data() {
                    received.extend_from_slice(&data);
                }
            }
        }
    }

    /// The chunked `/v1/sql` JSON body must be byte-identical to the buffered
    /// `arrow_to_json` output across multi-batch, empty, and NULL cases.
    #[tokio::test]
    async fn json_array_body_stream_matches_buffered_output() {
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

        for batches in cases {
            let expected = write_to_json_string(&batches).expect("buffered json");

            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            let account = EgressAccount::register(&pool, "test_egress");

            let mut batches = batches.into_iter();
            let first = batches.next();
            let rest: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::iter(batches.map(Ok::<_, DataFusionError>)),
            ));

            let mut body = std::pin::pin!(
                record_batch_body(StreamedBody::JsonArray, first, rest, account)
                    .expect("an Int64/Utf8 batch encodes")
            );
            let mut streamed = Vec::new();
            while let Some(chunk) = body.next().await {
                streamed.extend_from_slice(&chunk.expect("chunk"));
            }

            assert_eq!(
                String::from_utf8(streamed).expect("utf8"),
                expected,
                "streamed JSON must match buffered arrow_to_json output"
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
        let response = query_stream_to_http_response(
            data,
            CacheStatus::CacheMiss,
            ResponseMimeType::Json,
            pool,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.body().size_hint().exact().is_none(),
            "JSON responses must stream; an exact size hint is a Content-Length body"
        );
    }

    /// HTTP JSON and buffered CSV from `QueryResult::from_cached_raw` must
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
            for format in [ResponseMimeType::Json, ResponseMimeType::Csv] {
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
