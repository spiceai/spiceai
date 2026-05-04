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

//! T2 Remote user-defined scalar functions over HTTP + JSON.
//!
//! Wire contract:
//!   * Request: `POST <endpoint>` with `Content-Type: application/json` and body `{"rows": [{"<arg_name>": <arg_value>, ...}, ...]}`.
//!   * Response: HTTP 200 with body `{"values": [<row_0_result>, <row_1_result>, ...]}`.
//!   * `values.len()` MUST equal `rows.len()`; mismatch is an error.
//!
//! Inputs are grouped into `batch_size` chunks; up to `batch_concurrency`
//! chunks are issued in parallel, while results are appended in input order.
//!
//! This tier backs both `http://` / `https://` `from:` schemes.
//!
//! ### `params:` knobs
//!   * `timeout` — per-call timeout, default `30s`. Plain integer (seconds) or `Ns` / `Nms` suffix strings.
//!   * `batch_size` — rows per HTTP request, default `1024`.
//!   * `batch_concurrency` — maximum in-flight HTTP batches per invocation, default `4`.
//!   * `auth_bearer` — optional `Authorization: Bearer <value>` header value (already secret-resolved by the caller).
//!
//! Remote beta functions use Arrow's JSON reader/writer, supporting scalar and
//! complex Arrow types that have a JSON representation.

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use arrow::array::{Array, ArrayRef, new_empty_array};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_json::{ReaderBuilder, StructMode, writer::JsonArray, writer::WriterBuilder};
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableType;
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility as DfVolatility,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use futures::{StreamExt, stream};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use runtime_request_context::{AsyncMarker, RequestContext};
use serde_json::{Map, Value};
use snafu::{ResultExt, Snafu};
use spicepod::component::function::{Function, FunctionArg, FunctionReturns, Volatility};
use url::Url;

static NEXT_REMOTE_ID: AtomicU64 = AtomicU64::new(1);

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_BATCH_SIZE: usize = 1024;
const MAX_BATCH_SIZE: usize = 100_000;
const DEFAULT_BATCH_CONCURRENCY: usize = 4;
const MAX_BATCH_CONCURRENCY: usize = 64;

#[derive(Debug, Snafu)]
pub enum RemoteBuildError {
    #[snafu(display(
        "return type is required for a scalar remote function — add `signature.returns: <arrow-type>`"
    ))]
    MissingReturnType,

    #[snafu(display(
        "table return schema is required for a remote table function — set `signature.returns` to a list of output columns, e.g. `returns: [{{ name: value, type: int64 }}]`"
    ))]
    MissingTableReturnSchema,

    #[snafu(display(
        "scalar return type is required for a scalar remote function — set `signature.returns` to a single Arrow type string, not a table column list"
    ))]
    ExpectedScalarReturnType,

    #[snafu(display(
        "table return schema is required for a remote table function — set `signature.returns` to a list of output columns, not a scalar Arrow type string"
    ))]
    ExpectedTableReturnSchema,

    #[snafu(display(
        "unsupported or invalid Arrow type '{arrow_type}' for remote UDF signature. \
        Use Arrow display types like `Int64`, `List(Int64)`, `Struct(\"name\": Utf8)`, \
        or Spicepod aliases like `int64`, `list<int64>`, `struct<name:utf8>`, `decimal(38,10)`."
    ))]
    UnsupportedArrowType { arrow_type: String },

    #[snafu(display("duplicate output column '{column}' in remote table function return schema"))]
    DuplicateOutputColumn { column: String },

    #[snafu(display("failed to parse endpoint URL '{from}': {source}"))]
    InvalidEndpoint {
        from: String,
        source: url::ParseError,
    },

    #[snafu(display("endpoint scheme '{scheme}' is not supported; use `http://` or `https://`"))]
    UnsupportedScheme { scheme: String },

    #[snafu(display("param '{key}' is expected to be a {expected}, got {got}"))]
    InvalidParam {
        key: String,
        expected: String,
        got: String,
    },

    #[snafu(display("param 'auth_bearer' must be a plain string value"))]
    InvalidAuthBearer,

    #[snafu(display("failed to build HTTP client: {source}"))]
    BuildClient { source: reqwest::Error },
}

pub type Result<T, E = RemoteBuildError> = std::result::Result<T, E>;

/// Build a [`ScalarUDF`] (async-backed) from a [`Function`] with a
/// `from: http://…` or `from: https://…` endpoint.
///
/// # Errors
///
/// Returns [`RemoteBuildError`] when the `from` URL cannot be parsed,
/// its scheme is not `http`/`https`, any argument or return type is
/// unsupported, a known `params:` key has the wrong type, or the HTTP
/// client cannot be constructed.
pub fn build_scalar_udf(decl: &Function) -> Result<Arc<ScalarUDF>> {
    let endpoint = parse_endpoint(&decl.from)?;

    let arg_names: Vec<String> = decl.signature.args.iter().map(|a| a.name.clone()).collect();
    let arg_types: Vec<DataType> = decl
        .signature
        .args
        .iter()
        .map(|a| parse_arrow_type(&a.arrow_type))
        .collect::<Result<Vec<_>>>()?;

    let return_type = match decl.signature.returns.as_ref() {
        Some(FunctionReturns::Scalar(arrow_type)) => parse_arrow_type(arrow_type)?,
        Some(FunctionReturns::Table(_)) => return ExpectedScalarReturnTypeSnafu.fail(),
        None => return MissingReturnTypeSnafu.fail(),
    };

    let timeout = parse_timeout(decl.params.get("timeout"))?;
    let batch_size = parse_batch_size(decl.params.get("batch_size"))?;
    let batch_concurrency = parse_batch_concurrency(decl.params.get("batch_concurrency"))?;
    let auth_bearer = parse_auth_bearer(decl.params.get("auth_bearer"))?;

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(ref token) = auth_bearer {
        let hv = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| RemoteBuildError::InvalidAuthBearer)?;
        headers.insert(AUTHORIZATION, hv);
    }

    let client = reqwest::Client::builder()
        .timeout(timeout)
        .default_headers(headers)
        .build()
        .context(BuildClientSnafu)?;

    let signature = Signature::exact(arg_types.clone(), map_volatility(decl.volatility));

    let impl_ = RemoteScalarUdf {
        id: NEXT_REMOTE_ID.fetch_add(1, Ordering::Relaxed),
        name: decl.name.clone(),
        signature,
        return_type,
        arg_names,
        arg_types,
        endpoint,
        client,
        batch_size,
        batch_concurrency,
    };
    let async_udf = AsyncScalarUDF::new(Arc::new(impl_));
    Ok(Arc::new(async_udf.into_scalar_udf()))
}

/// Build a remote table function from a [`Function`] with a `from: http://…`
/// or `from: https://…` endpoint.
///
/// Wire contract:
///   * Request: `POST <endpoint>` with body `{"args": {"<arg_name>": <arg_value>, ...}}`.
///   * Response: HTTP 200 with body `{"rows": [{"<column>": <value>, ...}, ...]}`.
///
/// # Errors
///
/// Returns [`RemoteBuildError`] when the endpoint, signature, params, or HTTP
/// client cannot be constructed.
pub fn build_table_udtf(decl: &Function) -> Result<Arc<dyn TableFunctionImpl>> {
    let endpoint = parse_endpoint(&decl.from)?;
    let arg_schema = function_arg_schema(&decl.signature.args)?;
    let output_schema = table_return_schema(decl)?;
    let timeout = parse_timeout(decl.params.get("timeout"))?;
    let auth_bearer = parse_auth_bearer(decl.params.get("auth_bearer"))?;

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(ref token) = auth_bearer {
        let hv = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| RemoteBuildError::InvalidAuthBearer)?;
        headers.insert(AUTHORIZATION, hv);
    }

    let client = reqwest::Client::builder()
        .timeout(timeout)
        .default_headers(headers)
        .build()
        .context(BuildClientSnafu)?;

    Ok(Arc::new(RemoteTableFunc {
        name: decl.name.clone(),
        arg_schema,
        output_schema,
        endpoint,
        client,
    }))
}

#[derive(Clone)]
struct RemoteTableFunc {
    name: String,
    arg_schema: SchemaRef,
    output_schema: SchemaRef,
    endpoint: Url,
    client: reqwest::Client,
}

impl Debug for RemoteTableFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteTableFunc")
            .field("name", &self.name)
            .field("arg_schema", &self.arg_schema)
            .field("output_schema", &self.output_schema)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl TableFunctionImpl for RemoteTableFunc {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let args = table_arg_values(&self.name, self.arg_schema.as_ref(), exprs)?;
        Ok(Arc::new(RemoteTableProvider {
            name: self.name.clone(),
            arg_schema: Arc::clone(&self.arg_schema),
            schema: Arc::clone(&self.output_schema),
            endpoint: self.endpoint.clone(),
            client: self.client.clone(),
            args,
        }))
    }
}

#[derive(Debug)]
struct RemoteTableProvider {
    name: String,
    arg_schema: SchemaRef,
    schema: SchemaRef,
    endpoint: Url,
    client: reqwest::Client,
    args: Vec<ScalarValue>,
}

#[async_trait::async_trait]
impl TableProvider for RemoteTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        require_read_write_api_key(&self.name).await?;

        let args = encode_single_args_row(Arc::clone(&self.arg_schema), &self.args, &self.name)?;
        let mut rows = self.post_table(args).await?;
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
        let batch = decode_table_rows(&rows, Arc::clone(&self.schema), &self.name)?;
        let memory_source = MemorySourceConfig::try_new(
            &[vec![batch]],
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(Arc::new(DataSourceExec::new(Arc::new(memory_source))))
    }
}

impl RemoteTableProvider {
    async fn post_table(&self, args: Value) -> std::result::Result<Vec<Value>, DataFusionError> {
        let body = serde_json::json!({ "args": args });
        let resp = self
            .client
            .post(self.endpoint.clone())
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "remote table function '{}' request failed: {e}",
                    self.name
                ))
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let snippet = sanitize_body_for_error(&body);
            return Err(DataFusionError::Execution(format!(
                "remote table function '{}' returned HTTP {status}: {snippet}",
                self.name
            )));
        }

        let parsed: RemoteTableResponse = resp.json().await.map_err(|e| {
            DataFusionError::Execution(format!(
                "remote table function '{}' response was not valid JSON: {e}",
                self.name
            ))
        })?;
        Ok(parsed.rows)
    }
}

#[derive(Debug)]
struct RemoteScalarUdf {
    id: u64,
    name: String,
    signature: Signature,
    return_type: DataType,
    arg_names: Vec<String>,
    arg_types: Vec<DataType>,
    endpoint: Url,
    client: reqwest::Client,
    batch_size: usize,
    batch_concurrency: usize,
}

impl PartialEq for RemoteScalarUdf {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for RemoteScalarUdf {}

impl Hash for RemoteScalarUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for RemoteScalarUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> std::result::Result<DataType, DataFusionError> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> std::result::Result<ColumnarValue, DataFusionError> {
        Err(DataFusionError::Execution(format!(
            "remote user function '{}' must be invoked asynchronously",
            self.name
        )))
    }
}

#[async_trait::async_trait]
impl AsyncScalarUDFImpl for RemoteScalarUdf {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> std::result::Result<ColumnarValue, DataFusionError> {
        require_read_write_api_key(&self.name).await?;

        if args.args.len() != self.arg_names.len() {
            return Err(DataFusionError::Execution(format!(
                "remote function '{}' expected {} args, got {}",
                self.name,
                self.arg_names.len(),
                args.args.len()
            )));
        }

        let n = args.number_rows;
        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|cv| cv.to_array(n))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        if n == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(&self.return_type)));
        }

        let mut output_arrays = Vec::new();

        let arrays = arrays.as_slice();
        let mut batch_stream =
            stream::iter((0..n).step_by(self.batch_size).map(|offset| async move {
                let len = std::cmp::min(self.batch_size, n - offset);
                let rows = self.encode_rows(arrays, offset, len)?;
                let values = self.post_batch(rows).await?;
                if values.len() != len {
                    return Err(DataFusionError::Execution(format!(
                        "remote function '{}' returned {} values for a batch of {} rows",
                        self.name,
                        values.len(),
                        len
                    )));
                }
                Ok::<_, DataFusionError>(values)
            }))
            .buffered(self.batch_concurrency);

        while let Some(values) = batch_stream.next().await {
            output_arrays.push(self.decode_values(values?)?);
        }

        Ok(ColumnarValue::Array(concat_arrays(
            &output_arrays,
            &self.return_type,
        )?))
    }
}

impl RemoteScalarUdf {
    fn encode_rows(
        &self,
        arrays: &[ArrayRef],
        offset: usize,
        len: usize,
    ) -> std::result::Result<Vec<Value>, DataFusionError> {
        let schema = Arc::new(Schema::new(
            self.arg_names
                .iter()
                .zip(&self.arg_types)
                .map(|(name, data_type)| Field::new(name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        ));
        let columns = arrays
            .iter()
            .map(|array| array.slice(offset, len))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(schema, columns)?;
        record_batch_to_json_rows(&batch, &self.name)
    }

    fn decode_values(&self, values: Vec<Value>) -> std::result::Result<ArrayRef, DataFusionError> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            self.return_type.clone(),
            true,
        )]));
        let rows = values
            .into_iter()
            .map(|value| {
                let mut row = Map::with_capacity(1);
                row.insert("value".to_string(), value);
                Value::Object(row)
            })
            .collect::<Vec<_>>();
        let mut decoder = ReaderBuilder::new(schema)
            .with_struct_mode(StructMode::ObjectOnly)
            .build_decoder()
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "remote function '{}' failed to build JSON response decoder: {e}",
                    self.name
                ))
            })?;
        decoder.serialize(&rows).map_err(|e| {
            DataFusionError::Execution(format!(
                "remote function '{}' response values did not match declared return type: {e}",
                self.name
            ))
        })?;
        let batch = decoder.flush().map_err(|e| {
            DataFusionError::Execution(format!(
                "remote function '{}' failed to decode JSON response values: {e}",
                self.name
            ))
        })?;
        let Some(batch) = batch else {
            return Ok(new_empty_array(&self.return_type));
        };
        Ok(Arc::clone(batch.column(0)))
    }

    async fn post_batch(
        &self,
        rows: Vec<Value>,
    ) -> std::result::Result<Vec<Value>, DataFusionError> {
        let body = serde_json::json!({ "rows": rows });
        let resp = self
            .client
            .post(self.endpoint.clone())
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "remote function '{}' request failed: {e}",
                    self.name
                ))
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let snippet = sanitize_body_for_error(&body);
            return Err(DataFusionError::Execution(format!(
                "remote function '{}' returned HTTP {status}: {snippet}",
                self.name
            )));
        }

        let parsed: RemoteResponse = resp.json().await.map_err(|e| {
            DataFusionError::Execution(format!(
                "remote function '{}' response was not valid JSON: {e}",
                self.name
            ))
        })?;
        Ok(parsed.values)
    }
}

async fn require_read_write_api_key(
    function_name: &str,
) -> std::result::Result<(), DataFusionError> {
    let context = RequestContext::current(AsyncMarker::new().await);
    let Some(principal) = runtime_auth::AuthRequestContext::auth_principal(context.as_ref()) else {
        return Ok(());
    };
    let groups = principal.groups();
    if groups.contains(&"write") || groups.contains(&"read_write") {
        return Ok(());
    }
    Err(DataFusionError::Execution(format!(
        "remote function '{function_name}' requires a read-write API key"
    )))
}

#[derive(serde::Deserialize)]
struct RemoteResponse {
    values: Vec<Value>,
}

#[derive(serde::Deserialize)]
struct RemoteTableResponse {
    rows: Vec<Value>,
}

/// Collapse newlines and truncate a response body for safe inclusion in
/// an error message. Keeps operators able to diagnose failures without
/// dumping large or sensitive payloads into the log stream, and without
/// violating the repo's single-line log/error convention.
fn sanitize_body_for_error(body: &str) -> String {
    const MAX: usize = 256;
    let one_line: String = body
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect();
    if one_line.chars().count() > MAX {
        let truncated: String = one_line.chars().take(MAX).collect();
        format!("{truncated}… [truncated]")
    } else {
        one_line
    }
}

fn parse_endpoint(from: &str) -> Result<Url> {
    let url = Url::parse(from).map_err(|source| RemoteBuildError::InvalidEndpoint {
        from: from.to_string(),
        source,
    })?;
    match url.scheme() {
        "http" | "https" => Ok(url),
        other => Err(RemoteBuildError::UnsupportedScheme {
            scheme: other.to_string(),
        }),
    }
}

fn parse_timeout(v: Option<&Value>) -> Result<Duration> {
    let Some(v) = v else {
        return Ok(DEFAULT_TIMEOUT);
    };
    match v {
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| RemoteBuildError::InvalidParam {
                key: "timeout".into(),
                expected: "positive integer seconds".into(),
                got: format!("{v}"),
            })
            .map(Duration::from_secs),
        Value::String(s) => {
            parse_duration_string(s).ok_or_else(|| RemoteBuildError::InvalidParam {
                key: "timeout".into(),
                expected: "duration like '2s' or '500ms'".into(),
                got: s.clone(),
            })
        }
        _ => Err(RemoteBuildError::InvalidParam {
            key: "timeout".into(),
            expected: "number or duration string".into(),
            got: format!("{v}"),
        }),
    }
}

fn parse_duration_string(s: &str) -> Option<Duration> {
    let s = s.trim();
    if let Some(rest) = s.strip_suffix("ms") {
        return rest.trim().parse::<u64>().ok().map(Duration::from_millis);
    }
    if let Some(rest) = s.strip_suffix('s') {
        return rest.trim().parse::<u64>().ok().map(Duration::from_secs);
    }
    s.parse::<u64>().ok().map(Duration::from_secs)
}

fn parse_batch_size(v: Option<&Value>) -> Result<usize> {
    let Some(v) = v else {
        return Ok(DEFAULT_BATCH_SIZE);
    };
    let n = v.as_u64().ok_or_else(|| RemoteBuildError::InvalidParam {
        key: "batch_size".into(),
        expected: "positive integer".into(),
        got: format!("{v}"),
    })?;
    let n = usize::try_from(n).map_err(|_| RemoteBuildError::InvalidParam {
        key: "batch_size".into(),
        expected: "positive integer".into(),
        got: format!("{v}"),
    })?;
    if n == 0 || n > MAX_BATCH_SIZE {
        return Err(RemoteBuildError::InvalidParam {
            key: "batch_size".into(),
            expected: format!("positive integer ≤ {MAX_BATCH_SIZE}"),
            got: n.to_string(),
        });
    }
    Ok(n)
}

fn parse_batch_concurrency(v: Option<&Value>) -> Result<usize> {
    let Some(v) = v else {
        return Ok(DEFAULT_BATCH_CONCURRENCY);
    };
    let n = v.as_u64().ok_or_else(|| RemoteBuildError::InvalidParam {
        key: "batch_concurrency".into(),
        expected: "positive integer".into(),
        got: format!("{v}"),
    })?;
    let n = usize::try_from(n).map_err(|_| RemoteBuildError::InvalidParam {
        key: "batch_concurrency".into(),
        expected: "positive integer".into(),
        got: format!("{v}"),
    })?;
    if n == 0 || n > MAX_BATCH_CONCURRENCY {
        return Err(RemoteBuildError::InvalidParam {
            key: "batch_concurrency".into(),
            expected: format!("positive integer ≤ {MAX_BATCH_CONCURRENCY}"),
            got: n.to_string(),
        });
    }
    Ok(n)
}

fn parse_auth_bearer(v: Option<&Value>) -> Result<Option<String>> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(RemoteBuildError::InvalidAuthBearer),
    }
}

fn map_volatility(v: Volatility) -> DfVolatility {
    match v {
        // Remote HTTP-backed functions must never be exposed to DataFusion
        // as `Immutable`: that enables constant-folding and broader result
        // reuse for calls whose behaviour the runtime cannot prove is
        // deterministic (the remote service could be non-deterministic, or
        // change under us). Cap any user-declared `immutable`/`stable` at
        // `Stable` — safe for per-query caching but never plan-time folding.
        Volatility::Immutable | Volatility::Stable => DfVolatility::Stable,
        Volatility::Volatile => DfVolatility::Volatile,
    }
}

fn parse_arrow_type(s: &str) -> Result<DataType> {
    super::arrow_type::parse_arrow_type(s).map_err(|_| RemoteBuildError::UnsupportedArrowType {
        arrow_type: s.to_string(),
    })
}

fn function_arg_schema(args: &[FunctionArg]) -> Result<SchemaRef> {
    let fields = args
        .iter()
        .map(|arg| {
            Ok(Field::new(
                &arg.name,
                parse_arrow_type(&arg.arrow_type)?,
                true,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn table_return_schema(decl: &Function) -> Result<SchemaRef> {
    let columns = match decl.signature.returns.as_ref() {
        Some(FunctionReturns::Table(columns)) => columns,
        Some(FunctionReturns::Scalar(_)) => return ExpectedTableReturnSchemaSnafu.fail(),
        None => return MissingTableReturnSchemaSnafu.fail(),
    };

    let mut names = HashSet::with_capacity(columns.len());
    let fields = columns
        .iter()
        .map(|column| {
            if !names.insert(column.name.to_ascii_lowercase()) {
                return DuplicateOutputColumnSnafu {
                    column: column.name.clone(),
                }
                .fail();
            }
            Ok(Field::new(
                &column.name,
                parse_arrow_type(&column.arrow_type)?,
                true,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn table_arg_values(
    function_name: &str,
    schema: &Schema,
    exprs: &[Expr],
) -> DataFusionResult<Vec<ScalarValue>> {
    let fields = schema.fields();
    let mut values: Vec<Option<ScalarValue>> = vec![None; fields.len()];
    let mut positional_index = 0;

    for expr in exprs {
        let (parameter_name, scalar) = literal_arg(function_name, expr)?;
        let index = if let Some(name) = parameter_name {
            fields
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(&name))
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "remote table function '{function_name}' has no argument named '{name}'"
                    ))
                })?
        } else {
            while positional_index < values.len() && values[positional_index].is_some() {
                positional_index += 1;
            }
            if positional_index >= fields.len() {
                return Err(DataFusionError::Plan(format!(
                    "remote table function '{function_name}' expected {} arguments, got more",
                    fields.len()
                )));
            }
            let index = positional_index;
            positional_index += 1;
            index
        };

        if values[index].is_some() {
            return Err(DataFusionError::Plan(format!(
                "remote table function '{function_name}' argument '{}' was provided more than once",
                fields[index].name()
            )));
        }
        values[index] = Some(cast_scalar_arg(&scalar, fields[index].data_type())?);
    }

    values
        .into_iter()
        .enumerate()
        .map(|(idx, value)| {
            value.ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "remote table function '{function_name}' missing required argument '{}'",
                    fields[idx].name()
                ))
            })
        })
        .collect()
}

fn literal_arg(
    function_name: &str,
    expr: &Expr,
) -> DataFusionResult<(Option<String>, ScalarValue)> {
    if let Expr::Literal(scalar, metadata) = expr {
        let parameter_name = metadata
            .as_ref()
            .and_then(|metadata| metadata.inner().get("spice.parameter_name"))
            .cloned();
        return Ok((parameter_name, scalar.clone()));
    }

    Err(DataFusionError::NotImplemented(format!(
        "remote table function '{function_name}' currently supports literal arguments only; got {expr:?}"
    )))
}

fn cast_scalar_arg(value: &ScalarValue, data_type: &DataType) -> DataFusionResult<ScalarValue> {
    if matches!(value, ScalarValue::Null) {
        return ScalarValue::try_from(data_type);
    }
    value.cast_to(data_type)
}

fn encode_single_args_row(
    schema: SchemaRef,
    values: &[ScalarValue],
    function_name: &str,
) -> DataFusionResult<Value> {
    let columns = values
        .iter()
        .map(|value| value.to_array_of_size(1))
        .collect::<DataFusionResult<Vec<ArrayRef>>>()?;
    let batch = RecordBatch::try_new(schema, columns)?;
    let mut rows = record_batch_to_json_rows(&batch, function_name)?;
    Ok(rows.pop().unwrap_or_else(|| Value::Object(Map::new())))
}

fn decode_table_rows(
    rows: &[Value],
    schema: SchemaRef,
    function_name: &str,
) -> DataFusionResult<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut decoder = ReaderBuilder::new(Arc::clone(&schema))
        .with_struct_mode(StructMode::ObjectOnly)
        .build_decoder()
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "remote table function '{function_name}' failed to build JSON response decoder: {e}"
            ))
        })?;
    decoder.serialize(rows).map_err(|e| {
        DataFusionError::Execution(format!(
            "remote table function '{function_name}' response rows did not match declared return schema: {e}"
        ))
    })?;
    decoder
        .flush()
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "remote table function '{function_name}' failed to decode JSON response rows: {e}"
            ))
        })?
        .map_or_else(|| Ok(RecordBatch::new_empty(schema)), Ok)
}

fn record_batch_to_json_rows(
    batch: &RecordBatch,
    function_name: &str,
) -> std::result::Result<Vec<Value>, DataFusionError> {
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .with_struct_mode(StructMode::ObjectOnly)
        .build::<_, JsonArray>(Vec::new());
    writer.write_batches(&[batch]).map_err(|e| {
        DataFusionError::Execution(format!(
            "remote function '{function_name}' failed to encode request rows as JSON: {e}"
        ))
    })?;
    writer.finish().map_err(|e| {
        DataFusionError::Execution(format!(
            "remote function '{function_name}' failed to finish JSON request encoding: {e}"
        ))
    })?;
    serde_json::from_slice::<Vec<Map<String, Value>>>(&writer.into_inner())
        .map(|rows| rows.into_iter().map(Value::Object).collect())
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "remote function '{function_name}' encoded request rows were not valid JSON: {e}"
            ))
        })
}

fn concat_arrays(
    arrays: &[ArrayRef],
    data_type: &DataType,
) -> std::result::Result<ArrayRef, DataFusionError> {
    if arrays.is_empty() {
        return Ok(new_empty_array(data_type));
    }
    let array_refs = arrays
        .iter()
        .map(|array| array.as_ref() as &dyn Array)
        .collect::<Vec<_>>();
    concat(&array_refs).map_err(|e| {
        DataFusionError::Execution(format!(
            "failed to concatenate remote UDF response arrays: {e}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ListArray};
    use datafusion::arrow::datatypes::Int64Type;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct TestPrincipal {
        groups: &'static [&'static str],
    }

    impl runtime_auth::AuthPrincipal for TestPrincipal {
        fn username(&self) -> &'static str {
            "test"
        }

        fn groups(&self) -> &[&str] {
            self.groups
        }
    }

    fn sample_decl(from: &str) -> Function {
        use spicepod::component::function::{
            FunctionArg, FunctionKind, FunctionReturns, Signature as YamlSig,
        };
        Function {
            name: "remote_fn".into(),
            from: from.into(),
            enabled: true,
            description: None,
            kind: FunctionKind::Scalar,
            volatility: Volatility::Volatile,
            signature: YamlSig {
                args: vec![FunctionArg {
                    name: "x".into(),
                    arrow_type: "int64".into(),
                }],
                returns: Some(FunctionReturns::Scalar("int64".into())),
            },
            body: None,
            body_ref: None,
            metadata: HashMap::default(),
            params: HashMap::default(),
            depends_on: vec![],
            metrics: None,
            as_tool: true,
        }
    }

    fn sample_table_decl(from: &str) -> Function {
        use spicepod::component::function::{
            FunctionArg, FunctionKind, FunctionReturns, Signature as YamlSig,
        };
        Function {
            name: "remote_rows".into(),
            from: from.into(),
            enabled: true,
            description: None,
            kind: FunctionKind::Table,
            volatility: Volatility::Volatile,
            signature: YamlSig {
                args: vec![FunctionArg {
                    name: "x".into(),
                    arrow_type: "int64".into(),
                }],
                returns: Some(FunctionReturns::Table(vec![
                    FunctionArg {
                        name: "value".into(),
                        arrow_type: "int64".into(),
                    },
                    FunctionArg {
                        name: "label".into(),
                        arrow_type: "utf8".into(),
                    },
                ])),
            },
            body: None,
            body_ref: None,
            metadata: HashMap::default(),
            params: HashMap::default(),
            depends_on: vec![],
            metrics: None,
            as_tool: false,
        }
    }

    #[test]
    fn endpoint_parse_requires_http_scheme() {
        let d = sample_decl("file:///etc/passwd");
        let err = build_scalar_udf(&d).expect_err("file scheme rejected");
        match err {
            RemoteBuildError::UnsupportedScheme { scheme } => assert_eq!(scheme, "file"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn endpoint_parse_rejects_invalid_url() {
        let d = sample_decl("not-a-url");
        let err = build_scalar_udf(&d).expect_err("invalid URL rejected");
        assert!(matches!(err, RemoteBuildError::InvalidEndpoint { .. }));
    }

    #[test]
    fn unsupported_arg_type_rejected() {
        let mut d = sample_decl("http://example.com/udf");
        d.signature.args[0].arrow_type = "not_a_type".into();
        let err = build_scalar_udf(&d).expect_err("invalid type rejected");
        assert!(matches!(err, RemoteBuildError::UnsupportedArrowType { .. }));
    }

    #[test]
    fn complex_arrow_type_parses_for_remote() {
        assert_eq!(
            parse_arrow_type("list<int64>").expect("list parses"),
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)))
        );
        assert_eq!(
            parse_arrow_type("struct<name:utf8>").expect("struct parses"),
            DataType::Struct(vec![Field::new("name", DataType::Utf8, true)].into())
        );
    }

    #[test]
    fn timeout_parses_string_forms() {
        assert_eq!(
            parse_duration_string("2s").expect("test"),
            Duration::from_secs(2)
        );
        assert_eq!(
            parse_duration_string("500ms").expect("test"),
            Duration::from_millis(500)
        );
        assert_eq!(
            parse_duration_string("10").expect("test"),
            Duration::from_secs(10)
        );
        assert!(parse_duration_string("bogus").is_none());
    }

    #[test]
    fn builds_valid_decl() {
        let d = sample_decl("http://example.com/udf");
        let udf = build_scalar_udf(&d).expect("builds");
        assert_eq!(udf.name(), "remote_fn");
    }

    #[test]
    fn builds_valid_table_decl() {
        let d = sample_table_decl("http://example.com/udtf");
        let udtf = build_table_udtf(&d).expect("builds");
        assert!(format!("{udtf:?}").contains("remote_rows"));
    }

    #[test]
    fn batch_concurrency_validates_bounds() {
        assert_eq!(
            parse_batch_concurrency(None).expect("default batch concurrency parses"),
            DEFAULT_BATCH_CONCURRENCY
        );
        assert_eq!(
            parse_batch_concurrency(Some(&Value::Number(8_u64.into())))
                .expect("explicit batch concurrency parses"),
            8
        );
        parse_batch_concurrency(Some(&Value::Number(0_u64.into())))
            .expect_err("zero batch concurrency is invalid");
        parse_batch_concurrency(Some(&Value::String("4".into())))
            .expect_err("string batch concurrency is invalid");
    }

    #[test]
    fn complex_json_rows_round_trip() {
        let list_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)));
        let remote = RemoteScalarUdf {
            id: 0,
            name: "remote_fn".to_string(),
            signature: Signature::exact(vec![list_type.clone()], DfVolatility::Volatile),
            return_type: list_type.clone(),
            arg_names: vec!["x".to_string()],
            arg_types: vec![list_type.clone()],
            endpoint: Url::parse("http://example.com/udf").expect("valid URL"),
            client: reqwest::Client::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            batch_concurrency: DEFAULT_BATCH_CONCURRENCY,
        };
        let input: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
        ]));

        let rows = remote
            .encode_rows(&[input], 0, 2)
            .expect("request rows encode");
        assert_eq!(rows[0], serde_json::json!({"x": [1, 2]}));
        assert_eq!(rows[1], serde_json::json!({"x": null}));

        let output = remote
            .decode_values(vec![serde_json::json!([3, 4]), Value::Null])
            .expect("response values decode");
        assert_eq!(output.data_type(), &list_type);
        let list = output
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list array");
        assert_eq!(list.value_length(0), 2);
        assert!(list.is_null(1));
    }

    #[tokio::test]
    async fn remote_table_udtf_round_trips_via_http_json() {
        use axum::{Router, extract::Json as AxJson, routing::post};
        use datafusion::prelude::SessionContext;
        use tokio::net::TcpListener;

        async fn handler(AxJson(body): AxJson<Value>) -> AxJson<Value> {
            let x = body
                .get("args")
                .and_then(|args| args.get("x"))
                .and_then(Value::as_i64)
                .expect("args.x should be int64");
            AxJson(serde_json::json!({
                "rows": [
                    {"value": x, "label": "first"},
                    {"value": x + 1, "label": "second"}
                ]
            }))
        }

        let app = Router::new().route("/rows", post(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });

        let decl = sample_table_decl(&format!("http://{addr}/rows"));
        let udtf = build_table_udtf(&decl).expect("builds");
        let ctx = SessionContext::new();
        ctx.register_udtf(&decl.name, udtf);

        let df = ctx
            .sql("SELECT value, label FROM remote_rows(7) ORDER BY value")
            .await
            .expect("sql compiles");
        let results = df.collect().await.expect("runs");

        assert_eq!(results.len(), 1);
        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("value int64");
        let labels = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("label utf8");
        assert_eq!(values.values(), &[7_i64, 8]);
        assert_eq!(labels.value(0), "first");
        assert_eq!(labels.value(1), "second");
    }

    #[tokio::test]
    async fn read_only_principal_rejected() {
        let context =
            Arc::new(RequestContext::builder(runtime_request_context::Protocol::Http).build());
        runtime_auth::AuthRequestContext::set_auth_principal(
            context.as_ref(),
            Arc::new(TestPrincipal { groups: &["read"] }),
        )
        .expect("principal should be set");

        let err = context
            .scope(async { require_read_write_api_key("remote_fn").await })
            .await
            .expect_err("read-only principal should be rejected");
        assert!(err.to_string().contains("read-write API key"));
    }

    #[tokio::test]
    async fn read_write_principal_allowed() {
        let context =
            Arc::new(RequestContext::builder(runtime_request_context::Protocol::Http).build());
        runtime_auth::AuthRequestContext::set_auth_principal(
            context.as_ref(),
            Arc::new(TestPrincipal {
                groups: &["read_write"],
            }),
        )
        .expect("principal should be set");

        context
            .scope(async { require_read_write_api_key("remote_fn").await })
            .await
            .expect("read-write principal should be allowed");
    }
}
