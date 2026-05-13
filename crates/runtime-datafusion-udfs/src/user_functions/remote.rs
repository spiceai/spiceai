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
//! Scalar wire contract without table arguments:
//!   * Request: `POST <endpoint>` with `Content-Type: application/json` and body `{"rows": [{"<arg_name>": <arg_value>, ...}, ...]}`.
//!   * Response: HTTP 200 with body `{"values": [<row_0_result>, <row_1_result>, ...]}`.
//!   * `values.len()` MUST equal `rows.len()`; mismatch is an error.
//!
//! Table functions, and scalar functions with dynamic table arguments, use a
//! single-call request body of `{"args": {...}, "tables": {"<name>": [...]}}`.
//! The `tables` field is omitted when no table arguments are declared.
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
//!   * `max_response_bytes` — maximum response body size decoded from the function server, default `10MiB`.
//!   * `max_rows` — maximum table-function response rows without a query `LIMIT`, default `100000`.
//!   * `auth_bearer` — optional `Authorization: Bearer <value>` header value (already secret-resolved by the caller).
//!   * `allowed_endpoint_ranges` — optional list of CIDR ranges that may be
//!     targeted even when they are non-public / metadata IPs. Default `[]`; set
//!     to `["*"]` only for trusted deployments to allow
//!     every endpoint range. Literal hosts are checked at build time and DNS
//!     results are checked by the HTTP client before connecting to prevent SSRF.
//!
//! Remote beta functions use Arrow's JSON reader/writer, supporting scalar and
//! complex Arrow types that have a JSON representation.

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
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
use datafusion::catalog::{
    Session, TableFunctionImpl, TableProvider, default_table_source::provider_as_source,
};
use datafusion::common::{Column, DataFusionError, Result as DataFusionResult, Spans};
use datafusion::datasource::TableType;
use datafusion::execution::SessionState;
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{
    ColumnarValue, LogicalPlan, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Subquery,
    TableScan, Volatility as DfVolatility,
    simplify::{ExprSimplifyResult, SimplifyInfo},
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, SessionContext};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use futures::{StreamExt, stream};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use runtime_request_context::{AsyncMarker, RequestContext};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use snafu::{ResultExt, Snafu};
use spicepod::component::function::{
    Function, FunctionArg, FunctionReturns, FunctionTableArg, Volatility,
};
use url::Url;
use util::session_state::builder_from_existing;

static NEXT_REMOTE_ID: AtomicU64 = AtomicU64::new(1);

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_BATCH_SIZE: usize = 1024;
const MAX_BATCH_SIZE: usize = 100_000;
const DEFAULT_BATCH_CONCURRENCY: usize = 4;
const MAX_BATCH_CONCURRENCY: usize = 64;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024 * 1024;
const DEFAULT_MAX_TABLE_RESPONSE_ROWS: usize = 100_000;
const MAX_TABLE_RESPONSE_ROWS: usize = 1_000_000;
const MAX_SCALAR_TABLE_RESPONSE_ROWS: usize = 1;
const MAX_ERROR_BODY_BYTES: usize = 4096;
const ERROR_SNIPPET_CHARS: usize = 256;
const ALLOWED_ENDPOINT_RANGES_PARAM: &str = "allowed_endpoint_ranges";

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

    #[snafu(display("duplicate input table '{table}' in remote function signature"))]
    DuplicateInputTable { table: String },

    #[snafu(display("duplicate column '{column}' in remote function input table '{table}'"))]
    DuplicateInputTableColumn { table: String, column: String },

    #[snafu(display("failed to parse endpoint URL '{from}': {source}"))]
    InvalidEndpoint {
        from: String,
        source: url::ParseError,
    },

    #[snafu(display("endpoint scheme '{scheme}' is not supported; use `http://` or `https://`"))]
    UnsupportedScheme { scheme: String },

    #[snafu(display(
        "endpoint host '{host}' resolves to a non-public address and is rejected \
            to prevent SSRF; add a specific CIDR to \
            `allowed_endpoint_ranges` in `params`, or set it to [\"*\"] only for trusted endpoints"
    ))]
    PrivateEndpoint { host: String },

    #[snafu(display("endpoint URL '{from}' is missing a host"))]
    MissingHost { from: String },

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
    if !decl.signature.tables.is_empty() {
        return build_scalar_table_arg_udf(decl);
    }

    let endpoint_policy = endpoint_access_policy(decl)?;
    let endpoint = parse_endpoint(&decl.from, &endpoint_policy)?;

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
    let max_response_bytes = parse_max_response_bytes(decl.params.get("max_response_bytes"))?;
    let auth_bearer = parse_auth_bearer(decl.params.get("auth_bearer"))?;

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(ref token) = auth_bearer {
        let hv = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| RemoteBuildError::InvalidAuthBearer)?;
        headers.insert(AUTHORIZATION, hv);
    }

    let client = build_http_client(timeout, headers, &endpoint_policy)?;

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
        max_response_bytes,
    };
    let async_udf = AsyncScalarUDF::new(Arc::new(impl_));
    Ok(Arc::new(async_udf.into_scalar_udf()))
}

fn build_scalar_table_arg_udf(decl: &Function) -> Result<Arc<ScalarUDF>> {
    let endpoint_policy = endpoint_access_policy(decl)?;
    let endpoint = parse_endpoint(&decl.from, &endpoint_policy)?;
    let arg_schema = function_arg_schema(&decl.signature.args)?;
    let table_args = table_arg_specs(&decl.signature.tables)?;
    let (return_type, output_schema) = scalar_return_schema(decl)?;
    let timeout = parse_timeout(decl.params.get("timeout"))?;
    let max_response_bytes = parse_max_response_bytes(decl.params.get("max_response_bytes"))?;
    let max_rows = parse_max_rows(decl.params.get("max_rows"), MAX_SCALAR_TABLE_RESPONSE_ROWS)?;
    let auth_bearer = parse_auth_bearer(decl.params.get("auth_bearer"))?;

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(ref token) = auth_bearer {
        let hv = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| RemoteBuildError::InvalidAuthBearer)?;
        headers.insert(AUTHORIZATION, hv);
    }

    let client = build_http_client(timeout, headers, &endpoint_policy)?;

    let table_func = Arc::new(RemoteTableFunc {
        name: decl.name.clone(),
        arg_schema,
        table_args,
        output_schema,
        endpoint,
        client,
        response_kind: RemoteResponseKind::ScalarValues,
        max_response_bytes,
        max_rows,
    });
    let udf_impl = RemoteScalarTableArgUdf {
        id: NEXT_REMOTE_ID.fetch_add(1, Ordering::Relaxed),
        name: decl.name.clone(),
        signature: Signature::variadic_any(map_volatility(decl.volatility)),
        return_type,
        table_func,
    };
    Ok(Arc::new(ScalarUDF::from(udf_impl)))
}

/// Build a remote table function from a [`Function`] with a `from: http://…`
/// or `from: https://…` endpoint.
///
/// Wire contract:
///   * Request: `POST <endpoint>` with body `{"args": {"<arg_name>": <arg_value>, ...}, "tables": {"<table_name>": [<row>, ...]}}`.
///   * Response: HTTP 200 with body `{"rows": [{"<column>": <value>, ...}, ...]}`.
///
/// # Errors
///
/// Returns [`RemoteBuildError`] when the endpoint, signature, params, or HTTP
/// client cannot be constructed.
pub fn build_table_udtf(decl: &Function) -> Result<Arc<dyn TableFunctionImpl>> {
    let endpoint_policy = endpoint_access_policy(decl)?;
    let endpoint = parse_endpoint(&decl.from, &endpoint_policy)?;
    let arg_schema = function_arg_schema(&decl.signature.args)?;
    let table_args = table_arg_specs(&decl.signature.tables)?;
    let output_schema = table_return_schema(decl)?;
    let timeout = parse_timeout(decl.params.get("timeout"))?;
    let max_response_bytes = parse_max_response_bytes(decl.params.get("max_response_bytes"))?;
    let max_rows = parse_max_rows(decl.params.get("max_rows"), DEFAULT_MAX_TABLE_RESPONSE_ROWS)?;
    let auth_bearer = parse_auth_bearer(decl.params.get("auth_bearer"))?;

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(ref token) = auth_bearer {
        let hv = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| RemoteBuildError::InvalidAuthBearer)?;
        headers.insert(AUTHORIZATION, hv);
    }

    let client = build_http_client(timeout, headers, &endpoint_policy)?;

    Ok(Arc::new(RemoteTableFunc {
        name: decl.name.clone(),
        arg_schema,
        table_args,
        output_schema,
        endpoint,
        client,
        response_kind: RemoteResponseKind::Rows,
        max_response_bytes,
        max_rows,
    }))
}

#[derive(Clone)]
struct RemoteTableFunc {
    name: String,
    arg_schema: SchemaRef,
    table_args: Vec<TableArgSpec>,
    output_schema: SchemaRef,
    endpoint: Url,
    client: reqwest::Client,
    response_kind: RemoteResponseKind,
    max_response_bytes: usize,
    max_rows: usize,
}

#[derive(Clone, Copy, Debug)]
enum RemoteResponseKind {
    Rows,
    ScalarValues,
}

#[derive(Clone, Debug)]
struct TableArgSpec {
    name: String,
    schema: SchemaRef,
}

#[derive(Clone, Debug)]
struct TableArgValue {
    name: String,
    schema: SchemaRef,
    source: DynamicTableSource,
}

#[derive(Clone, Debug)]
enum DynamicTableSource {
    Table(TableReference),
    Plan(Arc<LogicalPlan>),
}

impl Debug for RemoteTableFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteTableFunc")
            .field("name", &self.name)
            .field("arg_schema", &self.arg_schema)
            .field("table_args", &self.table_args)
            .field("output_schema", &self.output_schema)
            .field("endpoint", &self.endpoint)
            .field("response_kind", &self.response_kind)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_rows", &self.max_rows)
            .finish_non_exhaustive()
    }
}

impl TableFunctionImpl for RemoteTableFunc {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let (table_args, scalar_exprs) = split_table_and_scalar_exprs(
            &self.name,
            &self.table_args,
            self.arg_schema.as_ref(),
            exprs,
        )?;
        let args = table_arg_values(&self.name, self.arg_schema.as_ref(), scalar_exprs)?;
        Ok(Arc::new(RemoteTableProvider {
            name: self.name.clone(),
            arg_schema: Arc::clone(&self.arg_schema),
            schema: Arc::clone(&self.output_schema),
            endpoint: self.endpoint.clone(),
            client: self.client.clone(),
            args,
            table_args,
            response_kind: self.response_kind,
            max_response_bytes: self.max_response_bytes,
            max_rows: self.max_rows,
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
    table_args: Vec<TableArgValue>,
    response_kind: RemoteResponseKind,
    max_response_bytes: usize,
    max_rows: usize,
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        require_read_write_api_key(&self.name).await?;

        let args = encode_single_args_row(Arc::clone(&self.arg_schema), &self.args, &self.name)?;
        let ctx = context_from_state(state)?;
        let tables = encode_dynamic_tables(&ctx, &self.table_args, &self.name).await?;
        let request_limit = match self.response_kind {
            RemoteResponseKind::Rows => limit,
            RemoteResponseKind::ScalarValues => None,
        };
        let mut rows = match self.response_kind {
            RemoteResponseKind::Rows => self.post_table(args, tables, request_limit).await?,
            RemoteResponseKind::ScalarValues => {
                self.post_scalar(args, tables, request_limit).await?
            }
        };
        if matches!(self.response_kind, RemoteResponseKind::Rows)
            && let Some(limit) = limit
        {
            rows.truncate(limit);
        }
        if rows.len() > self.max_rows {
            let function_kind = match self.response_kind {
                RemoteResponseKind::Rows => "remote table function",
                RemoteResponseKind::ScalarValues => "remote function",
            };
            return Err(DataFusionError::Execution(format!(
                "{function_kind} '{}' returned {} row(s), exceeding configured max_rows {}",
                self.name,
                rows.len(),
                self.max_rows
            )));
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
    async fn post_table(
        &self,
        args: Value,
        tables: Map<String, Value>,
        limit: Option<usize>,
    ) -> std::result::Result<Vec<Value>, DataFusionError> {
        let body = table_request_body(args, tables, limit);
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

        let parsed: RemoteTableResponse = decode_json_response(
            resp,
            &self.name,
            "remote table function",
            self.max_response_bytes,
        )
        .await?;
        Ok(parsed.rows)
    }

    async fn post_scalar(
        &self,
        args: Value,
        tables: Map<String, Value>,
        limit: Option<usize>,
    ) -> std::result::Result<Vec<Value>, DataFusionError> {
        let body = table_request_body(args, tables, limit);
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

        let parsed: RemoteResponse =
            decode_json_response(resp, &self.name, "remote function", self.max_response_bytes)
                .await?;
        Ok(parsed
            .values
            .into_iter()
            .map(|value| {
                let mut row = Map::with_capacity(1);
                row.insert("value".to_string(), value);
                Value::Object(row)
            })
            .collect())
    }
}

fn table_request_body(args: Value, tables: Map<String, Value>, limit: Option<usize>) -> Value {
    let mut body = Map::with_capacity(3);
    body.insert("args".to_string(), args);
    if !tables.is_empty() {
        body.insert("tables".to_string(), Value::Object(tables));
    }
    if let Some(limit) = limit {
        body.insert("limit".to_string(), serde_json::json!(limit));
    }
    Value::Object(body)
}

#[derive(Debug)]
struct RemoteScalarTableArgUdf {
    id: u64,
    name: String,
    signature: Signature,
    return_type: DataType,
    table_func: Arc<RemoteTableFunc>,
}

impl PartialEq for RemoteScalarTableArgUdf {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for RemoteScalarTableArgUdf {}

impl Hash for RemoteScalarTableArgUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for RemoteScalarTableArgUdf {
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
            "remote scalar function '{}' with table arguments must be rewritten to a scalar subquery before execution",
            self.name
        )))
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> std::result::Result<ExprSimplifyResult, DataFusionError> {
        let provider = self.table_func.call(&args)?;
        let table_source = provider_as_source(provider);
        let table_scan = TableScan::try_new(
            TableReference::bare(format!("{}_result", self.name)),
            table_source,
            None,
            vec![],
            None,
        )?;
        Ok(ExprSimplifyResult::Simplified(Expr::ScalarSubquery(
            Subquery {
                subquery: Arc::new(LogicalPlan::TableScan(table_scan)),
                outer_ref_columns: vec![],
                spans: Spans::new(),
            },
        )))
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
    max_response_bytes: usize,
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

        let parsed: RemoteResponse =
            decode_json_response(resp, &self.name, "remote function", self.max_response_bytes)
                .await?;
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

async fn decode_json_response<T: DeserializeOwned>(
    resp: reqwest::Response,
    function_name: &str,
    function_kind: &str,
    max_response_bytes: usize,
) -> std::result::Result<T, DataFusionError> {
    let status = resp.status();
    if !status.is_success() {
        let snippet = error_response_snippet(resp).await;
        return Err(DataFusionError::Execution(format!(
            "{function_kind} '{function_name}' returned HTTP {status}: {snippet}"
        )));
    }

    let body =
        read_response_body_limited(resp, function_name, function_kind, max_response_bytes).await?;
    serde_json::from_slice(&body).map_err(|e| {
        DataFusionError::Execution(format!(
            "{function_kind} '{function_name}' response was not valid JSON: {e}"
        ))
    })
}

async fn read_response_body_limited(
    resp: reqwest::Response,
    function_name: &str,
    function_kind: &str,
    max_response_bytes: usize,
) -> std::result::Result<Vec<u8>, DataFusionError> {
    if resp
        .content_length()
        .and_then(|len| usize::try_from(len).ok())
        .is_some_and(|len| len > max_response_bytes)
    {
        return Err(DataFusionError::Execution(format!(
            "{function_kind} '{function_name}' response exceeded max_response_bytes {max_response_bytes}"
        )));
    }

    let mut body = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            DataFusionError::Execution(format!(
                "{function_kind} '{function_name}' failed to read response body: {e}"
            ))
        })?;
        if body.len().saturating_add(chunk.len()) > max_response_bytes {
            return Err(DataFusionError::Execution(format!(
                "{function_kind} '{function_name}' response exceeded max_response_bytes {max_response_bytes}"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn error_response_snippet(resp: reqwest::Response) -> String {
    let mut body = Vec::new();
    let mut truncated = false;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(e) => return format!("failed to read response body: {e}"),
        };
        let remaining = MAX_ERROR_BODY_BYTES.saturating_sub(body.len());
        if remaining == 0 {
            truncated = true;
            break;
        }
        if chunk.len() > remaining {
            body.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }
        body.extend_from_slice(&chunk);
    }
    let body = String::from_utf8_lossy(&body);
    sanitize_body_for_error(&body, truncated)
}

/// Collapse newlines and truncate a response body for safe inclusion in
/// an error message. Keeps operators able to diagnose failures without
/// dumping large or sensitive payloads into the log stream, and without
/// violating the repo's single-line log/error convention.
fn sanitize_body_for_error(body: &str, already_truncated: bool) -> String {
    let one_line: String = body
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect();
    if one_line.chars().count() > ERROR_SNIPPET_CHARS {
        let truncated: String = one_line.chars().take(ERROR_SNIPPET_CHARS).collect();
        format!("{truncated}… [truncated]")
    } else if already_truncated {
        format!("{one_line}… [truncated]")
    } else {
        one_line
    }
}

fn endpoint_access_policy(decl: &Function) -> Result<EndpointAccessPolicy> {
    parse_endpoint_policy(decl.params.get(ALLOWED_ENDPOINT_RANGES_PARAM))
}

fn build_http_client(
    timeout: Duration,
    headers: HeaderMap,
    endpoint_policy: &EndpointAccessPolicy,
) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .default_headers(headers);
    if !endpoint_policy.allow_all {
        builder = builder.dns_resolver(Arc::new(EndpointFilteringResolver::system(
            endpoint_policy.clone(),
        )));
    }
    builder.build().context(BuildClientSnafu)
}

fn parse_endpoint(from: &str, endpoint_policy: &EndpointAccessPolicy) -> Result<Url> {
    let url = Url::parse(from).map_err(|source| RemoteBuildError::InvalidEndpoint {
        from: from.to_string(),
        source,
    })?;
    match url.scheme() {
        "http" | "https" => {}
        other => {
            return Err(RemoteBuildError::UnsupportedScheme {
                scheme: other.to_string(),
            });
        }
    }
    let host = url.host().ok_or_else(|| RemoteBuildError::MissingHost {
        from: from.to_string(),
    })?;

    if let Some(reason) = host_endpoint_rejection(&host, endpoint_policy) {
        tracing::warn!(
            endpoint_host = %host,
            endpoint_port = ?url.port_or_known_default(),
            reason,
            "rejecting remote UDF endpoint that targets a non-public address"
        );
        return Err(RemoteBuildError::PrivateEndpoint {
            host: host.to_string(),
        });
    }
    Ok(url)
}

fn host_endpoint_rejection(
    host: &url::Host<&str>,
    endpoint_policy: &EndpointAccessPolicy,
) -> Option<&'static str> {
    match host {
        url::Host::Domain(d) => {
            // Treat the literal "localhost" label and metadata-style
            // names that some platforms hand out as if they were the
            // IPs they conventionally resolve to. This is best-effort:
            // an attacker controlling DNS can still point an arbitrary
            // name at a private IP.
            let lower = d.trim_end_matches('.').to_ascii_lowercase();
            if !endpoint_policy.allow_all
                && endpoint_policy.allowed_ranges.is_empty()
                && (lower == "localhost"
                    || lower.ends_with(".localhost")
                    || lower == "metadata"
                    || lower == "metadata.google.internal")
            {
                Some("hostname resolves to a loopback or metadata service")
            } else {
                None
            }
        }
        url::Host::Ipv4(ip) => endpoint_policy.rejection_reason(IpAddr::V4(*ip)),
        url::Host::Ipv6(ip) => endpoint_policy.rejection_reason(IpAddr::V6(*ip)),
    }
}

#[derive(Clone, Debug, Default)]
struct EndpointAccessPolicy {
    allow_all: bool,
    allowed_ranges: Vec<IpCidr>,
}

impl EndpointAccessPolicy {
    fn rejection_reason(&self, ip: IpAddr) -> Option<&'static str> {
        if self.allow_all {
            return None;
        }
        let normalized = normalize_ipv4_mapped(ip);
        let reason = ip_addr_is_disallowed(normalized)?;
        if self.allows_ip(ip) || self.allows_ip(normalized) {
            None
        } else {
            Some(reason)
        }
    }

    fn allows_ip(&self, ip: IpAddr) -> bool {
        self.allow_all || self.allowed_ranges.iter().any(|range| range.contains(ip))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IpCidr {
    network: IpAddr,
    prefix: u8,
}

impl IpCidr {
    fn parse(input: &str) -> Option<Self> {
        let (addr, prefix) = input.split_once('/')?;
        let ip = addr.parse::<IpAddr>().ok()?;
        let prefix = prefix.parse::<u8>().ok()?;
        match ip {
            IpAddr::V4(ip) if prefix <= 32 => Some(Self {
                network: IpAddr::V4(mask_ipv4(ip, prefix)),
                prefix,
            }),
            IpAddr::V6(ip) if prefix <= 128 => Some(Self {
                network: IpAddr::V6(mask_ipv6(ip, prefix)),
                prefix,
            }),
            _ => None,
        }
    }

    fn contains(&self, ip: IpAddr) -> bool {
        match (self.network, ip) {
            (IpAddr::V4(network), IpAddr::V4(ip)) => mask_ipv4(ip, self.prefix) == network,
            (IpAddr::V6(network), IpAddr::V6(ip)) => mask_ipv6(ip, self.prefix) == network,
            _ => false,
        }
    }
}

fn mask_ipv4(ip: Ipv4Addr, prefix: u8) -> Ipv4Addr {
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - u32::from(prefix))
    };
    Ipv4Addr::from(u32::from(ip) & mask)
}

fn mask_ipv6(ip: Ipv6Addr, prefix: u8) -> Ipv6Addr {
    let mask = if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - u32::from(prefix))
    };
    Ipv6Addr::from(u128::from(ip) & mask)
}

fn normalize_ipv4_mapped(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .or_else(|| ipv4_compatible_addr(ip))
            .map_or(IpAddr::V6(ip), IpAddr::V4),
        IpAddr::V4(ip) => IpAddr::V4(ip),
    }
}

fn ipv4_compatible_addr(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    if ip.is_loopback() || ip.is_unspecified() {
        return None;
    }
    let octets = ip.octets();
    if octets[..12].iter().all(|&octet| octet == 0) {
        Some(Ipv4Addr::new(
            octets[12], octets[13], octets[14], octets[15],
        ))
    } else {
        None
    }
}

fn ip_addr_is_disallowed(ip: IpAddr) -> Option<&'static str> {
    match ip {
        IpAddr::V4(ip) => ipv4_addr_is_disallowed(ip),
        IpAddr::V6(ip) => ipv6_addr_is_disallowed(ip),
    }
}

fn ipv4_addr_is_disallowed(ip: Ipv4Addr) -> Option<&'static str> {
    if ipv4_addr_is_global_endpoint(ip) {
        None
    } else {
        Some("IPv4 non-public address")
    }
}

fn ipv6_addr_is_disallowed(ip: Ipv6Addr) -> Option<&'static str> {
    if ipv6_addr_is_global_endpoint(ip) {
        None
    } else {
        Some("IPv6 non-public address")
    }
}

fn ipv4_addr_is_global_endpoint(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    !(octets[0] == 0
        || ip.is_private()
        || (octets[0] == 100 && (octets[1] & 0b1100_0000) == 0b0100_0000)
        || ip.is_loopback()
        || ip.is_link_local()
        || (matches!(octets, [192, 0, 0, d] if d != 9 && d != 10))
        || ip.is_documentation()
        || (octets[0] == 198 && (octets[1] & 0xfe) == 18)
        || ip.is_multicast()
        || octets[0] >= 240)
}

fn ipv6_addr_is_global_endpoint(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    let bits = u128::from(ip);
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || matches!(segments, [0, 0, 0, 0, 0, 0xffff, _, _])
        || matches!(segments, [0x64, 0xff9b, 1, _, _, _, _, _])
        || matches!(segments, [0x100, 0, 0, 0, _, _, _, _])
        || (matches!(segments, [0x2001, b, _, _, _, _, _, _] if b < 0x200)
            && !(bits == 0x2001_0001_0000_0000_0000_0000_0000_0001
                || bits == 0x2001_0001_0000_0000_0000_0000_0000_0002
                || matches!(segments, [0x2001, 3, _, _, _, _, _, _])
                || matches!(segments, [0x2001, 4, 0x112, _, _, _, _, _])
                || matches!(segments, [0x2001, b, _, _, _, _, _, _] if (0x20..=0x3f).contains(&b))))
        || matches!(segments, [0x2002, _, _, _, _, _, _, _])
        || matches!(segments, [0x2001, 0xdb8, ..] | [0x3fff, 0..=0x0fff, ..])
        || matches!(segments, [0x5f00, ..])
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] & 0xffc0) == 0xfec0)
}

fn parse_endpoint_policy(v: Option<&Value>) -> Result<EndpointAccessPolicy> {
    match v {
        None | Some(Value::Null) => Ok(EndpointAccessPolicy::default()),
        Some(Value::Array(values)) => {
            if values.is_empty() {
                return Ok(EndpointAccessPolicy::default());
            }
            if values.len() == 1 && values[0].as_str() == Some("*") {
                return Ok(EndpointAccessPolicy {
                    allow_all: true,
                    allowed_ranges: vec![],
                });
            }
            let mut allowed_ranges = Vec::with_capacity(values.len());
            for value in values {
                let Some(range) = value.as_str() else {
                    return Err(RemoteBuildError::InvalidParam {
                        key: ALLOWED_ENDPOINT_RANGES_PARAM.into(),
                        expected: "array of CIDR range strings or [\"*\"]".into(),
                        got: format!("{value}"),
                    });
                };
                if range == "*" {
                    return Err(RemoteBuildError::InvalidParam {
                        key: ALLOWED_ENDPOINT_RANGES_PARAM.into(),
                        expected: "[\"*\"] by itself or CIDR range strings".into(),
                        got: format!("{values:?}"),
                    });
                }
                let Some(cidr) = IpCidr::parse(range) else {
                    return Err(RemoteBuildError::InvalidParam {
                        key: ALLOWED_ENDPOINT_RANGES_PARAM.into(),
                        expected: "CIDR range like '10.20.5.42/32' or [\"*\"]".into(),
                        got: range.to_string(),
                    });
                };
                allowed_ranges.push(cidr);
            }
            Ok(EndpointAccessPolicy {
                allow_all: false,
                allowed_ranges,
            })
        }
        Some(other) => Err(RemoteBuildError::InvalidParam {
            key: ALLOWED_ENDPOINT_RANGES_PARAM.into(),
            expected: "array of CIDR range strings or [\"*\"]".into(),
            got: format!("{other}"),
        }),
    }
}

struct SystemDnsResolver;

impl Resolve for SystemDnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((host, 0)).await?;
            Ok(Box::new(addrs) as Addrs)
        })
    }
}

struct EndpointFilteringResolver {
    inner: Arc<dyn Resolve>,
    policy: EndpointAccessPolicy,
}

impl EndpointFilteringResolver {
    fn system(policy: EndpointAccessPolicy) -> Self {
        Self {
            inner: Arc::new(SystemDnsResolver),
            policy,
        }
    }

    #[cfg(test)]
    fn with_inner(policy: EndpointAccessPolicy, inner: Arc<dyn Resolve>) -> Self {
        Self { inner, policy }
    }
}

impl Resolve for EndpointFilteringResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        let inner = Arc::clone(&self.inner);
        let policy = self.policy.clone();
        Box::pin(async move {
            let addrs: Vec<SocketAddr> = inner.resolve(name).await?.collect();
            let addrs = filter_resolved_addrs(&policy, &host, addrs)
                .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;
            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}

fn filter_resolved_addrs(
    policy: &EndpointAccessPolicy,
    host: &str,
    addrs: Vec<SocketAddr>,
) -> std::result::Result<Vec<SocketAddr>, std::io::Error> {
    let mut allowed = Vec::with_capacity(addrs.len());
    let mut first_rejected = None;

    for addr in addrs {
        if let Some(reason) = policy.rejection_reason(addr.ip()) {
            first_rejected.get_or_insert((addr.ip(), reason));
        } else {
            allowed.push(addr);
        }
    }

    if allowed.is_empty()
        && let Some((ip, reason)) = first_rejected
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "remote UDF endpoint host '{host}' resolved only to disallowed addresses, including {ip} ({reason}); add a specific CIDR to `{ALLOWED_ENDPOINT_RANGES_PARAM}` or set it to [\"*\"] only for trusted endpoints"
            ),
        ));
    }

    Ok(allowed)
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

fn parse_max_response_bytes(v: Option<&Value>) -> Result<usize> {
    parse_usize_param(
        "max_response_bytes",
        v,
        DEFAULT_MAX_RESPONSE_BYTES,
        1,
        MAX_RESPONSE_BYTES,
    )
}

fn parse_max_rows(v: Option<&Value>, default: usize) -> Result<usize> {
    parse_usize_param("max_rows", v, default, 1, MAX_TABLE_RESPONSE_ROWS)
}

fn parse_usize_param(
    key: &str,
    v: Option<&Value>,
    default: usize,
    min: usize,
    max: usize,
) -> Result<usize> {
    let Some(v) = v else {
        return Ok(default);
    };
    let n = v.as_u64().ok_or_else(|| RemoteBuildError::InvalidParam {
        key: key.into(),
        expected: "positive integer".into(),
        got: format!("{v}"),
    })?;
    let n = usize::try_from(n).map_err(|_| RemoteBuildError::InvalidParam {
        key: key.into(),
        expected: "positive integer".into(),
        got: format!("{v}"),
    })?;
    if n < min || n > max {
        return Err(RemoteBuildError::InvalidParam {
            key: key.into(),
            expected: format!("integer between {min} and {max}"),
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

fn scalar_return_schema(decl: &Function) -> Result<(DataType, SchemaRef)> {
    let return_type = match decl.signature.returns.as_ref() {
        Some(FunctionReturns::Scalar(arrow_type)) => parse_arrow_type(arrow_type)?,
        Some(FunctionReturns::Table(_)) => return ExpectedScalarReturnTypeSnafu.fail(),
        None => return MissingReturnTypeSnafu.fail(),
    };
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        return_type.clone(),
        true,
    )]));
    Ok((return_type, schema))
}

fn table_arg_specs(args: &[FunctionTableArg]) -> Result<Vec<TableArgSpec>> {
    let mut names = HashSet::with_capacity(args.len());
    args.iter()
        .map(|arg| {
            if !names.insert(arg.name.to_ascii_lowercase()) {
                return DuplicateInputTableSnafu {
                    table: arg.name.clone(),
                }
                .fail();
            }
            let mut column_names = HashSet::with_capacity(arg.columns.len());
            let fields = arg
                .columns
                .iter()
                .map(|column| {
                    if !column_names.insert(column.name.to_ascii_lowercase()) {
                        return DuplicateInputTableColumnSnafu {
                            table: arg.name.clone(),
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
            Ok(TableArgSpec {
                name: arg.name.clone(),
                schema: Arc::new(Schema::new(fields)),
            })
        })
        .collect()
}

fn split_table_and_scalar_exprs<'a>(
    function_name: &str,
    table_args: &[TableArgSpec],
    scalar_schema: &Schema,
    exprs: &'a [Expr],
) -> DataFusionResult<(Vec<TableArgValue>, &'a [Expr])> {
    if exprs.len() < table_args.len() {
        return Err(DataFusionError::Plan(format!(
            "remote function '{function_name}' expected {} table argument(s) followed by {} scalar argument(s), got {} total argument(s)",
            table_args.len(),
            scalar_schema.fields().len(),
            exprs.len()
        )));
    }

    let scalar_exprs = &exprs[table_args.len()..];
    let table_values = table_args
        .iter()
        .zip(&exprs[..table_args.len()])
        .map(|(arg, expr)| {
            Ok(TableArgValue {
                name: arg.name.clone(),
                schema: Arc::clone(&arg.schema),
                source: dynamic_table_source_from_expr(function_name, &arg.name, expr)?,
            })
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    Ok((table_values, scalar_exprs))
}

fn dynamic_table_source_from_expr(
    function_name: &str,
    table_arg_name: &str,
    expr: &Expr,
) -> DataFusionResult<DynamicTableSource> {
    match expr {
        Expr::Column(column) => Ok(DynamicTableSource::Table(table_ref_from_column_expr(
            column,
        ))),
        Expr::Literal(ScalarValue::Utf8(Some(table)), _) => {
            Ok(DynamicTableSource::Table(TableReference::parse_str(table)))
        }
        Expr::ScalarSubquery(subquery) => {
            if !subquery.outer_ref_columns.is_empty() {
                return Err(DataFusionError::NotImplemented(format!(
                    "remote function '{function_name}' does not support correlated dynamic table input for argument '{table_arg_name}'"
                )));
            }
            Ok(DynamicTableSource::Plan(Arc::clone(&subquery.subquery)))
        }
        other => Err(DataFusionError::Plan(format!(
            "remote function '{function_name}' requires table argument '{table_arg_name}' to be a table reference or dynamic table input, got: {other:?}"
        ))),
    }
}

fn table_ref_from_column_expr(column: &Column) -> TableReference {
    let table: Arc<str> = column.name.clone().into();
    let schema = column.relation.as_ref().map(TableReference::table);
    let catalog = column.relation.as_ref().and_then(TableReference::schema);
    match (catalog, schema) {
        (None | Some(_), None) => TableReference::Bare { table },
        (None, Some(schema)) => TableReference::Partial {
            schema: schema.into(),
            table,
        },
        (Some(catalog), Some(schema)) => TableReference::Full {
            catalog: catalog.into(),
            schema: schema.into(),
            table,
        },
    }
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

fn context_from_state(state: &dyn Session) -> DataFusionResult<SessionContext> {
    let state = state
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "remote table function execution requires a DataFusion SessionState".to_string(),
            )
        })?;
    Ok(SessionContext::new_with_state(
        builder_from_existing(state).build(),
    ))
}

async fn encode_dynamic_tables(
    ctx: &SessionContext,
    table_args: &[TableArgValue],
    function_name: &str,
) -> DataFusionResult<Map<String, Value>> {
    let mut tables = Map::with_capacity(table_args.len());
    for table_arg in table_args {
        let (schema, batches) = execute_table_source(ctx, &table_arg.source).await?;
        validate_input_schema(function_name, &schema, table_arg.schema.as_ref())?;
        let mut rows = Vec::new();
        for batch in batches {
            rows.extend(record_batch_to_json_rows(&batch, function_name)?);
        }
        tables.insert(table_arg.name.clone(), Value::Array(rows));
    }
    Ok(tables)
}

async fn execute_table_source(
    ctx: &SessionContext,
    source: &DynamicTableSource,
) -> DataFusionResult<(SchemaRef, Vec<RecordBatch>)> {
    let df = match source {
        DynamicTableSource::Table(table) => ctx.table(table.clone()).await?,
        DynamicTableSource::Plan(plan) => ctx.execute_logical_plan((**plan).clone()).await?,
    };
    let schema = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;
    Ok((schema, batches))
}

fn validate_input_schema(
    function_name: &str,
    actual: &Schema,
    expected: &Schema,
) -> DataFusionResult<()> {
    let actual_fields = actual.fields();
    let expected_fields = expected.fields();
    if actual_fields.len() != expected_fields.len() {
        return Err(DataFusionError::Execution(format!(
            "remote function '{function_name}' input table schema mismatch: expected {} column(s), got {} column(s)",
            expected_fields.len(),
            actual_fields.len()
        )));
    }

    for (idx, (actual, expected)) in actual_fields.iter().zip(expected_fields.iter()).enumerate() {
        if actual.name() != expected.name() || actual.data_type() != expected.data_type() {
            return Err(DataFusionError::Execution(format!(
                "remote function '{function_name}' input table schema mismatch at column {}: expected '{}: {}', got '{}: {}'",
                idx + 1,
                expected.name(),
                expected.data_type(),
                actual.name(),
                actual.data_type()
            )));
        }
    }

    Ok(())
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
    use arrow::array::{Array, Int64Array, ListArray};
    use datafusion::arrow::datatypes::Int64Type;
    use datafusion::common::Spans;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::Subquery;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct TestPrincipal {
        groups: &'static [&'static str],
    }

    struct StaticResolver {
        addrs: Vec<SocketAddr>,
    }

    impl Resolve for StaticResolver {
        fn resolve(&self, _name: Name) -> Resolving {
            let addrs = self.addrs.clone();
            Box::pin(async move { Ok(Box::new(addrs.into_iter()) as Addrs) })
        }
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
                tables: vec![],
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
                tables: vec![],
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

    fn sample_dynamic_scalar_decl(from: &str) -> Function {
        let mut decl = sample_decl(from);
        decl.signature.tables = vec![FunctionTableArg {
            name: "input".into(),
            columns: vec![FunctionArg {
                name: "value".into(),
                arrow_type: "int64".into(),
            }],
        }];
        decl
    }

    fn sample_dynamic_table_decl(from: &str) -> Function {
        let mut decl = sample_table_decl(from);
        decl.signature.tables = vec![FunctionTableArg {
            name: "input".into(),
            columns: vec![FunctionArg {
                name: "value".into(),
                arrow_type: "int64".into(),
            }],
        }];
        decl
    }

    /// Round-trip tests bind a mock HTTP server on `127.0.0.1:0`. The endpoint
    /// guard would normally reject loopback endpoints, so allow only that exact
    /// loopback host range for these test declarations.
    fn with_loopback_range_allowed(mut decl: Function) -> Function {
        decl.params.insert(
            ALLOWED_ENDPOINT_RANGES_PARAM.into(),
            serde_json::Value::Array(vec![serde_json::Value::String("127.0.0.1/32".into())]),
        );
        decl
    }

    fn register_numbers(ctx: &SessionContext) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef],
        )
        .expect("record batch");
        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("mem table");
        ctx.register_table("numbers", Arc::new(table))
            .expect("register table");
    }

    async fn filtered_numbers_expr(ctx: &SessionContext) -> Expr {
        let input_df = ctx
            .table("numbers")
            .await
            .expect("table exists")
            .filter(datafusion::prelude::col("value").gt(datafusion::prelude::lit(1_i64)))
            .expect("filters")
            .select(vec![datafusion::prelude::col("value")])
            .expect("projects");
        Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(input_df.into_unoptimized_plan()),
            outer_ref_columns: vec![],
            spans: Spans::new(),
        })
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
    fn endpoint_parse_rejects_malformed_url_with_endpoint_range() {
        let mut d = sample_decl("http://");
        d.params.insert(
            ALLOWED_ENDPOINT_RANGES_PARAM.into(),
            serde_json::Value::Array(vec![serde_json::Value::String("10.0.0.0/8".into())]),
        );
        let err = build_scalar_udf(&d).expect_err("malformed endpoint rejected");
        assert!(matches!(err, RemoteBuildError::InvalidEndpoint { .. }));
    }

    #[test]
    fn endpoint_parse_rejects_private_addresses_by_default() {
        // Cloud metadata IPs (AWS / GCP / Azure all use 169.254.169.254).
        for from in [
            "http://169.254.169.254/latest/meta-data/",
            "http://127.0.0.1/udf",
            "http://10.0.0.1/udf",
            "http://192.168.1.1/udf",
            "http://172.16.0.1/udf",
            "http://0.0.0.0/udf",
            "http://0.0.0.1/udf",
            "http://192.0.2.1/udf",
            "http://198.18.0.1/udf",
            "http://localhost/udf",
            "http://localhost./udf",
            "http://service.localhost./udf",
            "http://metadata.google.internal/computeMetadata/v1/",
            "http://metadata.google.internal./computeMetadata/v1/",
            "http://[::1]/udf",
            "http://[fe80::1]/udf",
            "http://[fc00::1]/udf",
            "http://[2001:db8::1]/udf",
            "http://[::ffff:127.0.0.1]/udf",
            "http://[::ffff:0.0.0.0]/udf",
            "http://[::ffff:0.0.0.1]/udf",
            "http://[::ffff:198.18.0.1]/udf",
            "http://[::ffff:100.64.0.1]/udf",
            "http://[::ffff:224.0.0.1]/udf",
            "http://[::ffff:255.255.255.255]/udf",
            "http://[::127.0.0.1]/udf",
            "http://[::10.0.0.1]/udf",
            "http://[::169.254.169.254]/udf",
            "http://[::100.64.0.1]/udf",
        ] {
            let d = sample_decl(from);
            let err = build_scalar_udf(&d)
                .err()
                .unwrap_or_else(|| panic!("expected rejection of private endpoint {from}"));
            assert!(
                matches!(err, RemoteBuildError::PrivateEndpoint { .. }),
                "expected PrivateEndpoint for {from}, got {err:?}"
            );
        }
    }

    #[test]
    fn endpoint_parse_allows_configured_non_public_range() {
        let mut d = sample_decl("http://198.18.0.1:9000/udf");
        d.params.insert(
            ALLOWED_ENDPOINT_RANGES_PARAM.into(),
            serde_json::Value::Array(vec![serde_json::Value::String("198.18.0.1/32".into())]),
        );
        build_scalar_udf(&d).expect("non-public endpoint allowed by explicit CIDR");
    }

    #[test]
    fn endpoint_parse_rejects_private_address_with_typed_error() {
        let d = sample_decl("http://169.254.169.254/latest/meta-data/");
        let err = build_scalar_udf(&d).expect_err("metadata IP rejected");
        assert!(
            matches!(err, RemoteBuildError::PrivateEndpoint { .. }),
            "expected PrivateEndpoint, got {err:?}"
        );
    }

    #[test]
    fn endpoint_parse_allows_public_addresses() {
        // 8.8.8.8 is public; should be accepted.
        let d = sample_decl("http://8.8.8.8/udf");
        build_scalar_udf(&d).expect("public IP accepted");

        let d = sample_decl("https://example.com/udf");
        build_scalar_udf(&d).expect("public domain accepted");
    }

    #[test]
    fn endpoint_parse_allows_private_with_allowed_range() {
        let mut d = sample_decl("http://127.0.0.1:9000/udf");
        d.params.insert(
            ALLOWED_ENDPOINT_RANGES_PARAM.into(),
            serde_json::Value::Array(vec![serde_json::Value::String("127.0.0.1/32".into())]),
        );
        build_scalar_udf(&d).expect("private endpoint allowed by explicit CIDR");
    }

    #[test]
    fn endpoint_parse_allows_localhost_domain_with_allowed_range() {
        let mut d = sample_decl("http://localhost:9000/udf");
        d.params.insert(
            ALLOWED_ENDPOINT_RANGES_PARAM.into(),
            serde_json::Value::Array(vec![serde_json::Value::String("127.0.0.1/32".into())]),
        );
        build_scalar_udf(&d).expect("localhost endpoint allowed by explicit CIDR");
    }

    #[test]
    fn endpoint_parse_allows_all_ranges_with_wildcard() {
        let mut d = sample_decl("http://metadata.google.internal/computeMetadata/v1/");
        d.params.insert(
            ALLOWED_ENDPOINT_RANGES_PARAM.into(),
            serde_json::Value::Array(vec![serde_json::Value::String("*".into())]),
        );
        build_scalar_udf(&d).expect("wildcard range allows all endpoints");
    }

    #[test]
    fn endpoint_parse_rejects_invalid_allowed_range_value() {
        let mut d = sample_decl("http://127.0.0.1:9000/udf");
        d.params.insert(
            ALLOWED_ENDPOINT_RANGES_PARAM.into(),
            serde_json::Value::Array(vec![serde_json::Value::String("definitely".into())]),
        );
        let err = build_scalar_udf(&d).expect_err("non-CIDR endpoint range rejected");
        assert!(
            matches!(err, RemoteBuildError::InvalidParam { ref key, .. } if key == ALLOWED_ENDPOINT_RANGES_PARAM),
            "expected InvalidParam, got {err:?}"
        );
    }

    #[tokio::test]
    async fn endpoint_filtering_resolver_rejects_disallowed_dns_result() {
        let resolver = EndpointFilteringResolver::with_inner(
            EndpointAccessPolicy::default(),
            Arc::new(StaticResolver {
                addrs: vec!["10.20.5.42:0".parse().expect("valid socket address")],
            }),
        );

        match resolver
            .resolve("udf.internal.svc".parse().expect("valid dns name"))
            .await
        {
            Ok(_) => panic!("private resolved address should be rejected"),
            Err(err) => assert!(err.to_string().contains("10.20.5.42")),
        }
    }

    #[tokio::test]
    async fn endpoint_filtering_resolver_filters_disallowed_dns_results() {
        let resolver = EndpointFilteringResolver::with_inner(
            EndpointAccessPolicy::default(),
            Arc::new(StaticResolver {
                addrs: vec![
                    "10.20.5.42:0".parse().expect("valid socket address"),
                    "8.8.8.8:0".parse().expect("valid socket address"),
                ],
            }),
        );

        match resolver
            .resolve("udf.internal.svc".parse().expect("valid dns name"))
            .await
        {
            Ok(mut addrs) => {
                assert_eq!(
                    addrs.next().expect("one allowed resolved address").ip(),
                    "8.8.8.8".parse::<IpAddr>().expect("valid IP")
                );
                assert!(addrs.next().is_none(), "disallowed address was filtered");
            }
            Err(err) => panic!("mixed DNS results should keep allowed addresses: {err}"),
        }
    }

    #[tokio::test]
    async fn endpoint_filtering_resolver_allows_configured_dns_range() {
        let policy = parse_endpoint_policy(Some(&serde_json::Value::Array(vec![
            serde_json::Value::String("10.20.5.42/32".into()),
        ])))
        .expect("CIDR range parses");
        let resolver = EndpointFilteringResolver::with_inner(
            policy,
            Arc::new(StaticResolver {
                addrs: vec!["10.20.5.42:0".parse().expect("valid socket address")],
            }),
        );

        match resolver
            .resolve("udf.internal.svc".parse().expect("valid dns name"))
            .await
        {
            Ok(mut addrs) => assert_eq!(
                addrs.next().expect("one resolved address").ip(),
                "10.20.5.42".parse::<IpAddr>().expect("valid IP")
            ),
            Err(err) => panic!("configured range should be allowed: {err}"),
        }
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
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
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
    async fn remote_table_udtf_round_trips_via_dataframe_api() {
        use axum::{Router, extract::Json as AxJson, routing::post};
        use datafusion::prelude::{SessionContext, col, lit};
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

        let decl = with_loopback_range_allowed(sample_table_decl(&format!("http://{addr}/rows")));
        let udtf = build_table_udtf(&decl).expect("builds");
        let ctx = SessionContext::new();
        ctx.register_udtf(&decl.name, udtf);
        let provider = ctx
            .table_function(&decl.name)
            .expect("registered UDTF")
            .create_table_provider(&[lit(7_i64)])
            .expect("creates table provider");
        ctx.register_table("remote_rows_result", provider)
            .expect("register UDTF result");

        let results = ctx
            .table("remote_rows_result")
            .await
            .expect("table exists")
            .filter(col("value").gt(lit(0_i64)))
            .expect("filters")
            .sort_by(vec![col("value")])
            .expect("sorts")
            .select(vec![col("value"), col("label")])
            .expect("projects")
            .collect()
            .await
            .expect("runs");

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
    async fn remote_table_udtf_pushes_query_limit_to_request() {
        use axum::{Router, extract::Json as AxJson, routing::post};
        use datafusion::prelude::{SessionContext, col, lit};
        use tokio::net::TcpListener;

        async fn handler(AxJson(body): AxJson<Value>) -> AxJson<Value> {
            assert_eq!(body.get("limit").and_then(Value::as_u64), Some(1));
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

        let decl = with_loopback_range_allowed(sample_table_decl(&format!("http://{addr}/rows")));
        let udtf = build_table_udtf(&decl).expect("builds");
        let ctx = SessionContext::new();
        ctx.register_udtf(&decl.name, udtf);
        let provider = ctx
            .table_function(&decl.name)
            .expect("registered UDTF")
            .create_table_provider(&[lit(7_i64)])
            .expect("creates table provider");
        ctx.register_table("remote_limited_rows", provider)
            .expect("register UDTF result");

        let results = ctx
            .table("remote_limited_rows")
            .await
            .expect("table exists")
            .limit(0, Some(1))
            .expect("limits")
            .select(vec![col("value")])
            .expect("projects")
            .collect()
            .await
            .expect("runs");

        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value int64");
        assert_eq!(values.values(), &[7_i64]);
    }

    #[tokio::test]
    async fn remote_scalar_error_body_is_bounded() {
        use axum::{Router, http::StatusCode, response::IntoResponse, routing::post};
        use tokio::net::TcpListener;

        async fn handler() -> impl IntoResponse {
            (StatusCode::INTERNAL_SERVER_ERROR, "x".repeat(16 * 1024))
        }

        let app = Router::new().route("/scalar", post(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });

        let remote = RemoteScalarUdf {
            id: 0,
            name: "remote_fn".to_string(),
            signature: Signature::exact(vec![DataType::Int64], DfVolatility::Volatile),
            return_type: DataType::Int64,
            arg_names: vec!["x".to_string()],
            arg_types: vec![DataType::Int64],
            endpoint: Url::parse(&format!("http://{addr}/scalar")).expect("valid URL"),
            client: reqwest::Client::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            batch_concurrency: DEFAULT_BATCH_CONCURRENCY,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        };

        let err = remote
            .post_batch(vec![serde_json::json!({"x": 1})])
            .await
            .expect_err("HTTP 500 should fail");
        let message = err.to_string();
        assert!(message.contains("[truncated]"));
        assert!(message.len() < 512);
    }

    #[tokio::test]
    async fn remote_scalar_udf_accepts_dynamic_table_arg_from_sql_subquery() {
        use axum::{Router, extract::Json as AxJson, routing::post};
        use datafusion::prelude::SessionContext;
        use tokio::net::TcpListener;

        async fn handler(AxJson(body): AxJson<Value>) -> AxJson<Value> {
            let x = body
                .get("args")
                .and_then(|args| args.get("x"))
                .and_then(Value::as_i64)
                .expect("args.x should be int64");
            let input = body
                .get("tables")
                .and_then(|tables| tables.get("input"))
                .and_then(Value::as_array)
                .expect("tables.input should be rows");
            let sum = input
                .iter()
                .map(|row| {
                    row.get("value")
                        .and_then(Value::as_i64)
                        .expect("input.value should be int64")
                })
                .sum::<i64>();
            AxJson(serde_json::json!({ "values": [sum + x] }))
        }

        let app = Router::new().route("/scalar", post(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });

        let decl = with_loopback_range_allowed(sample_dynamic_scalar_decl(&format!(
            "http://{addr}/scalar"
        )));
        let udf = build_scalar_udf(&decl).expect("builds");
        let ctx = SessionContext::new();
        register_numbers(&ctx);
        ctx.register_udf(udf.as_ref().clone());

        let results = ctx
            .sql("SELECT remote_fn((SELECT value FROM numbers WHERE value > 1), 10) AS value")
            .await
            .expect("sql compiles")
            .collect()
            .await
            .expect("query runs");
        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 values");
        assert_eq!(values.values(), &[15_i64]);
    }

    #[tokio::test]
    async fn remote_table_udtf_accepts_dynamic_table_arg_via_dataframe_api() {
        use axum::{Router, extract::Json as AxJson, routing::post};
        use datafusion::prelude::{SessionContext, col, lit};
        use tokio::net::TcpListener;

        async fn handler(AxJson(body): AxJson<Value>) -> AxJson<Value> {
            let x = body
                .get("args")
                .and_then(|args| args.get("x"))
                .and_then(Value::as_i64)
                .expect("args.x should be int64");
            let input = body
                .get("tables")
                .and_then(|tables| tables.get("input"))
                .and_then(Value::as_array)
                .expect("tables.input should be rows");
            let rows = input
                .iter()
                .map(|row| {
                    let value = row
                        .get("value")
                        .and_then(Value::as_i64)
                        .expect("input.value should be int64");
                    serde_json::json!({ "value": value + x, "label": format!("row-{value}") })
                })
                .collect::<Vec<_>>();
            AxJson(serde_json::json!({ "rows": rows }))
        }

        let app = Router::new().route("/rows", post(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });

        let decl =
            with_loopback_range_allowed(sample_dynamic_table_decl(&format!("http://{addr}/rows")));
        let udtf = build_table_udtf(&decl).expect("builds");
        let ctx = SessionContext::new();
        register_numbers(&ctx);
        ctx.register_udtf(&decl.name, udtf);
        let input_expr = filtered_numbers_expr(&ctx).await;
        let provider = ctx
            .table_function(&decl.name)
            .expect("registered UDTF")
            .create_table_provider(&[input_expr, lit(10_i64)])
            .expect("creates table provider");
        ctx.register_table("remote_dynamic_rows_result", provider)
            .expect("register UDTF result");

        let results = ctx
            .table("remote_dynamic_rows_result")
            .await
            .expect("table exists")
            .sort_by(vec![col("value")])
            .expect("sorts")
            .select(vec![col("value"), col("label")])
            .expect("projects")
            .collect()
            .await
            .expect("runs");

        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value int64");
        let labels = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("label utf8");
        assert_eq!(values.values(), &[12_i64, 13]);
        assert_eq!(labels.value(0), "row-2");
        assert_eq!(labels.value(1), "row-3");
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
