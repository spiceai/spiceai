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
//! Inputs are grouped into `batch_size` chunks; chunks are issued
//! sequentially today (parallel fan-out will land with the rate-control
//! integration in a later pass).
//!
//! This tier backs both `http://` / `https://` `from:` schemes.
//!
//! ### `params:` knobs
//!   * `timeout` — per-call timeout, default `30s`. Plain integer (seconds) or `Ns` / `Nms` suffix strings.
//!   * `batch_size` — rows per HTTP request, default `1024`.
//!   * `auth_bearer` — optional `Authorization: Bearer <value>` header value (already secret-resolved by the caller).
//!
//! Phase 2 first pass: primitive types only (`int64`, `float64`, `utf8`,
//! `boolean`). Everything else returns a clear `UnsupportedArrowType`
//! error — consistent with the SQL tier.

use std::hash::Hash;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility as DfVolatility,
};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use runtime_request_context::{AsyncMarker, RequestContext};
use serde_json::{Map, Value};
use snafu::{ResultExt, Snafu};
use spicepod::component::function::{Function, Volatility};
use url::Url;

static NEXT_REMOTE_ID: AtomicU64 = AtomicU64::new(1);

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_BATCH_SIZE: usize = 1024;
const MAX_BATCH_SIZE: usize = 100_000;

#[derive(Debug, Snafu)]
pub enum RemoteBuildError {
    #[snafu(display(
        "return type is required for a scalar remote function — add `signature.returns: <arrow-type>`"
    ))]
    MissingReturnType,

    #[snafu(display(
        "unsupported Arrow type '{arrow_type}' for remote tier — phase 2 supports primitives \
        (int64, float64, utf8, boolean). Use the T0 SQL tier, or wait for complex-type support."
    ))]
    UnsupportedArrowType { arrow_type: String },

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

    let return_type = decl
        .signature
        .returns
        .as_deref()
        .ok_or(RemoteBuildError::MissingReturnType)
        .and_then(parse_arrow_type)?;

    let timeout = parse_timeout(decl.params.get("timeout"))?;
    let batch_size = parse_batch_size(decl.params.get("batch_size"))?;
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
    };
    let async_udf = AsyncScalarUDF::new(Arc::new(impl_));
    Ok(Arc::new(async_udf.into_scalar_udf()))
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

        let mut output =
            crate::primitive_json_codec::PrimitiveOutputBuilder::new(&self.return_type, n)?;

        let mut offset = 0;
        while offset < n {
            let len = std::cmp::min(self.batch_size, n - offset);
            let rows = self.encode_rows(&arrays, offset, len)?;
            let values = self.post_batch(rows).await?;
            if values.len() != len {
                return Err(DataFusionError::Execution(format!(
                    "remote function '{}' returned {} values for a batch of {} rows",
                    self.name,
                    values.len(),
                    len
                )));
            }
            output.append_values(&values)?;
            offset += len;
        }

        Ok(ColumnarValue::Array(output.finish()))
    }
}

impl RemoteScalarUdf {
    fn encode_rows(
        &self,
        arrays: &[ArrayRef],
        offset: usize,
        len: usize,
    ) -> std::result::Result<Vec<Value>, DataFusionError> {
        let mut rows = Vec::with_capacity(len);
        for row_idx in offset..offset + len {
            let mut obj = Map::with_capacity(self.arg_names.len());
            for (i, name) in self.arg_names.iter().enumerate() {
                obj.insert(
                    name.clone(),
                    crate::primitive_json_codec::array_cell_to_json(
                        &arrays[i],
                        row_idx,
                        &self.arg_types[i],
                    )?,
                );
            }
            rows.push(Value::Object(obj));
        }
        Ok(rows)
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
    if principal
        .groups()
        .iter()
        .any(|group| *group == "write" || *group == "read_write")
    {
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
    crate::primitive_json_codec::parse_primitive_arrow_type(s).ok_or_else(|| {
        RemoteBuildError::UnsupportedArrowType {
            arrow_type: s.to_string(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct TestPrincipal {
        groups: &'static [&'static str],
    }

    impl runtime_auth::AuthPrincipal for TestPrincipal {
        fn username(&self) -> &str {
            "test"
        }

        fn groups(&self) -> &[&str] {
            self.groups
        }
    }

    fn sample_decl(from: &str) -> Function {
        use spicepod::component::function::{FunctionArg, FunctionKind, Signature as YamlSig};
        Function {
            name: "remote_fn".into(),
            from: from.into(),
            description: None,
            kind: FunctionKind::Scalar,
            volatility: Volatility::Volatile,
            signature: YamlSig {
                args: vec![FunctionArg {
                    name: "x".into(),
                    arrow_type: "int64".into(),
                }],
                returns: Some("int64".into()),
                returns_schema: vec![],
                null_aware: false,
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
        d.signature.args[0].arrow_type = "binary".into();
        let err = build_scalar_udf(&d).expect_err("binary not yet supported");
        assert!(matches!(err, RemoteBuildError::UnsupportedArrowType { .. }));
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
