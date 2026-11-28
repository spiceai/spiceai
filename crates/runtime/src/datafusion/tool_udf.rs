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

//! Adapter that re-exposes a registered LLM tool as a SQL scalar UDF.
//!
//! Mirror of `tools::builtin::function_tool::FunctionAsTool` in the
//! opposite direction. Given a tool and an explicit typed signature,
//! produces an [`AsyncScalarUDFImpl`] that, for each row, packs the
//! arg values into a JSON object matching the tool's JSON Schema,
//! calls [`SpiceModelTool::call`], and parses the result back into
//! an Arrow column value.
//!
//! Opt-in per tool via `as_sql: true` + an explicit `signature:` block.
//! Tools are inherently async RPC-shaped calls, so the derived UDF is
//! always `AsyncScalarUDF`, marked volatile, and automatically added
//! to the federation deny-list.

use std::{
    collections::HashSet,
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::tools::SpiceModelTool;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility as DfVolatility,
};
use futures::{StreamExt, stream};
use runtime_datafusion_udfs::primitive_json_codec::{
    PrimitiveOutputBuilder, array_cell_to_json, parse_primitive_arrow_type,
};
use serde_json::{Map, Value};
use snafu::Snafu;
use spicepod::component::function::Signature as YamlSignature;

/// Maximum number of concurrent tool calls dispatched from a single
/// `ToolAsScalarUdf` invocation. Keeps per-query RPC fan-out bounded so
/// one tool-heavy query cannot exhaust a remote service's capacity.
const DEFAULT_TOOL_CONCURRENCY: usize = 16;

static NEXT_TOOL_UDF_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Snafu)]
pub enum ToolUdfBuildError {
    #[snafu(display(
        "cannot expose tool '{tool}' as SQL: `signature:` is required when `as_sql: true`"
    ))]
    MissingSignature { tool: String },

    #[snafu(display("cannot expose tool '{tool}' as SQL: `signature.returns` is required"))]
    MissingReturnType { tool: String },

    #[snafu(display(
        "cannot expose tool '{tool}' as SQL: duplicate argument name '{arg}' in `signature.args`"
    ))]
    DuplicateArgName { tool: String, arg: String },

    #[snafu(display(
        "cannot expose tool '{tool}' as SQL: arg '{arg}' has unsupported Arrow type '{arrow_type}'. \
        Supported: signed integer aliases/widths (int, int8, int16, int32, int64; widened to int64), \
        float aliases/widths (float, double, float32, float64; widened to float64), utf8/string, boolean/bool."
    ))]
    UnsupportedArgType {
        tool: String,
        arg: String,
        arrow_type: String,
    },

    #[snafu(display(
        "cannot expose tool '{tool}' as SQL: return Arrow type '{arrow_type}' is unsupported. \
        Supported: signed integer aliases/widths (int, int8, int16, int32, int64; widened to int64), \
        float aliases/widths (float, double, float32, float64; widened to float64), utf8/string, boolean/bool."
    ))]
    UnsupportedReturnType { tool: String, arrow_type: String },
}

pub type Result<T, E = ToolUdfBuildError> = std::result::Result<T, E>;

/// Build a [`ScalarUDF`] (async-backed) that dispatches to `tool` for
/// each row. The tool's receiver name and signature drive the resulting
/// UDF's identity and typing.
pub fn build_scalar_udf(
    tool: Arc<dyn SpiceModelTool>,
    tool_name: &str,
    yaml_sig: &YamlSignature,
) -> Result<Arc<ScalarUDF>> {
    let mut seen_arg_names = HashSet::with_capacity(yaml_sig.args.len());
    for arg in &yaml_sig.args {
        if !seen_arg_names.insert(arg.name.as_str()) {
            return Err(ToolUdfBuildError::DuplicateArgName {
                tool: tool_name.to_string(),
                arg: arg.name.clone(),
            });
        }
    }

    let arg_names: Vec<String> = yaml_sig.args.iter().map(|a| a.name.clone()).collect();
    let arg_types: Vec<DataType> = yaml_sig
        .args
        .iter()
        .map(|a| {
            parse_primitive_arrow_type(&a.arrow_type).ok_or_else(|| {
                ToolUdfBuildError::UnsupportedArgType {
                    tool: tool_name.to_string(),
                    arg: a.name.clone(),
                    arrow_type: a.arrow_type.clone(),
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let return_type_str =
        yaml_sig
            .scalar_return_type()
            .ok_or_else(|| ToolUdfBuildError::MissingReturnType {
                tool: tool_name.to_string(),
            })?;
    let return_type = parse_primitive_arrow_type(return_type_str).ok_or_else(|| {
        ToolUdfBuildError::UnsupportedReturnType {
            tool: tool_name.to_string(),
            arrow_type: return_type_str.to_string(),
        }
    })?;

    // Tools are inherently non-deterministic RPC — pin volatility to
    // Volatile and rely on the federation deny-list to prevent pushdown.
    let signature = Signature::exact(arg_types.clone(), DfVolatility::Volatile);

    let impl_ = ToolAsScalarUdf {
        id: NEXT_TOOL_UDF_ID.fetch_add(1, Ordering::Relaxed),
        name: tool_name.to_string(),
        signature,
        return_type,
        arg_names,
        arg_types,
        tool,
    };
    let async_udf = AsyncScalarUDF::new(Arc::new(impl_));
    Ok(Arc::new(async_udf.into_scalar_udf()))
}

struct ToolAsScalarUdf {
    id: u64,
    name: String,
    signature: Signature,
    return_type: DataType,
    arg_names: Vec<String>,
    arg_types: Vec<DataType>,
    tool: Arc<dyn SpiceModelTool>,
}

impl std::fmt::Debug for ToolAsScalarUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolAsScalarUdf")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("arg_names", &self.arg_names)
            .field("return_type", &self.return_type)
            .finish_non_exhaustive()
    }
}

impl PartialEq for ToolAsScalarUdf {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ToolAsScalarUdf {}

impl Hash for ToolAsScalarUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for ToolAsScalarUdf {
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
            "tool-backed function '{}' must be invoked asynchronously",
            self.name
        )))
    }
}

#[async_trait::async_trait]
impl AsyncScalarUDFImpl for ToolAsScalarUdf {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> std::result::Result<ColumnarValue, DataFusionError> {
        // Who may call the tool is decided by `call` for each row, as on every other path
        // that runs a tool. A read-only principal is refused every tool-backed function
        // earlier, when its read-only query is planned.
        if args.args.len() != self.arg_names.len() {
            return Err(DataFusionError::Execution(format!(
                "tool-backed function '{}' expected {} args, got {}",
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

        let name = self.name.clone();
        let tool = Arc::clone(&self.tool);
        let mut output = PrimitiveOutputBuilder::new(&self.return_type, n)?;
        // `stream::iter(...).buffered(N)` builds and dispatches at most N row
        // bodies at a time, while preserving row order for the consumer.
        let mut rpc_stream = stream::iter((0..n).map(|row| {
            let tool = Arc::clone(&tool);
            let name = name.clone();
            let body = self.encode_row_body(&arrays, row);
            async move {
                let body = body?;
                tool.call(&body).await.map_err(|e| {
                    DataFusionError::Execution(format!(
                        "tool-backed function '{name}' call failed: {e}"
                    ))
                })
            }
        }))
        .buffered(DEFAULT_TOOL_CONCURRENCY);

        while let Some(result) = rpc_stream.next().await {
            output.append_value(&result?)?;
        }

        Ok(ColumnarValue::Array(output.finish()))
    }
}

impl ToolAsScalarUdf {
    fn encode_row_body(
        &self,
        arrays: &[ArrayRef],
        row: usize,
    ) -> std::result::Result<String, DataFusionError> {
        let mut obj = Map::with_capacity(self.arg_names.len());
        for (i, name) in self.arg_names.iter().enumerate() {
            obj.insert(
                name.clone(),
                array_cell_to_json(&arrays[i], row, &self.arg_types[i])?,
            );
        }
        Ok(Value::Object(obj).to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::SpiceModelTool;
    use spicepod::component::function::{FunctionArg, FunctionReturns};

    struct StubTool;

    #[async_trait::async_trait]
    impl SpiceModelTool for StubTool {
        fn name(&self) -> std::borrow::Cow<'_, str> {
            "stub".into()
        }

        fn description(&self) -> Option<std::borrow::Cow<'_, str>> {
            None
        }

        fn parameters(&self) -> Option<Value> {
            None
        }

        fn access(&self) -> ::tools::ToolAccess {
            ::tools::ToolAccess::ReadOnly
        }

        async fn run(
            &self,
            _: &str,
        ) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Value::Null)
        }
    }

    /// A read-write tool exposed as a SQL function, counting the calls that reach it.
    struct WriteTool {
        runs: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl SpiceModelTool for WriteTool {
        fn name(&self) -> std::borrow::Cow<'_, str> {
            "send_alert".into()
        }

        fn description(&self) -> Option<std::borrow::Cow<'_, str>> {
            None
        }

        fn parameters(&self) -> Option<Value> {
            None
        }

        fn access(&self) -> ::tools::ToolAccess {
            ::tools::ToolAccess::ReadWrite
        }

        async fn run(
            &self,
            _: &str,
        ) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Ok(Value::from(1))
        }
    }

    fn write_tool_udf(tool: &Arc<WriteTool>) -> ToolAsScalarUdf {
        ToolAsScalarUdf {
            id: 0,
            name: "send_alert".to_string(),
            signature: Signature::exact(vec![DataType::Int64], DfVolatility::Volatile),
            return_type: DataType::Int64,
            arg_names: vec!["x".to_string()],
            arg_types: vec![DataType::Int64],
            tool: Arc::clone(tool) as Arc<dyn SpiceModelTool>,
        }
    }

    fn one_row() -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Int64(Some(7)),
            )],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "send_alert",
                DataType::Int64,
                true,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        }
    }

    /// A tool exposed as a SQL function is held to the tool's access like every other path
    /// that runs it: a request with no principal (anonymous, on a listener beyond loopback) is
    /// refused a read-write tool before it runs, and a principal with write access may call it.
    #[tokio::test]
    async fn tool_function_is_held_to_the_tool_access() {
        use runtime_auth::{AuthPrincipalRef, AuthRequestContext};
        use runtime_request_context::{Protocol, RequestContext};
        use spicepod::component::runtime::ApiKey;

        let tool = Arc::new(WriteTool {
            runs: std::sync::atomic::AtomicUsize::new(0),
        });
        let udf = write_tool_udf(&tool);

        let error = udf
            .invoke_async_with_args(one_row())
            .await
            .expect_err("a request with no principal is refused a read-write tool");
        assert_eq!(
            error.to_string(),
            "Execution error: tool-backed function 'send_alert' call failed: Failed to call tool 'send_alert': the tool can change data or act outside Spice, and the request has no credentials, which this runtime serves read-only because it reached Spice over the network or through a proxy and `runtime.auth` is not configured. Configure `runtime.auth` with a read-write API key (a `runtime.auth.api-key.keys` entry ending in `:rw`) and send it with the request. See https://spiceai.org/docs/api/auth"
        );
        assert_eq!(tool.runs.load(Ordering::SeqCst), 0, "the tool must not run");

        let context = Arc::new(RequestContext::builder(Protocol::Http).build());
        let principal: AuthPrincipalRef = Arc::new(ApiKey::parse_str("writer:rw"));
        context
            .set_auth_principal(principal)
            .expect("a new request has no principal yet");
        let result = context
            .scope(udf.invoke_async_with_args(one_row()))
            .await
            .expect("a read-write key may call it");
        let ColumnarValue::Array(values) = result else {
            panic!("expected an array, got {result:?}");
        };
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .expect("int64 result")
                .values(),
            &[1]
        );
        assert_eq!(tool.runs.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn build_fails_without_signature_returns() {
        let sig = YamlSignature {
            tables: vec![],
            args: vec![FunctionArg {
                name: "x".into(),
                arrow_type: "int64".into(),
            }],
            returns: None,
        };
        let err = build_scalar_udf(Arc::new(StubTool), "stub", &sig).expect_err("missing return");
        assert!(matches!(err, ToolUdfBuildError::MissingReturnType { .. }));
    }

    #[test]
    fn build_rejects_duplicate_arg_names() {
        let sig = YamlSignature {
            tables: vec![],
            args: vec![
                FunctionArg {
                    name: "x".into(),
                    arrow_type: "int64".into(),
                },
                FunctionArg {
                    name: "x".into(),
                    arrow_type: "float64".into(),
                },
            ],
            returns: Some(FunctionReturns::Scalar("int64".into())),
        };

        let err = build_scalar_udf(Arc::new(StubTool), "stub", &sig)
            .expect_err("duplicate arg names should fail");

        assert!(matches!(
            err,
            ToolUdfBuildError::DuplicateArgName { tool, arg }
                if tool == "stub" && arg == "x"
        ));
    }
}
