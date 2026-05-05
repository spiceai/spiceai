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

use std::sync::Arc;

use axum::{
    Extension, Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use datafusion::execution::FunctionRegistry;
use serde::{Deserialize, Serialize};

use crate::Runtime;

/// Summary of a user-defined function declared in the spicepod's
/// `functions:` section.
#[derive(Serialize, Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct ListFunctionElement {
    name: String,
    kind: String,
    volatility: String,
    from: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

/// List user-defined functions registered in the runtime.
///
/// Returns only the functions declared in a spicepod's `functions:`
/// section — Spice built-ins and `DataFusion` standard functions are
/// not listed here (use the `list_udfs()` UDTF in SQL for that).
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/functions",
    operation_id = "list_functions",
    tag = "Functions",
    responses(
        (
            status = 200, body = [ListFunctionElement],
            description = "User-defined functions registered in the runtime",
            example = json!([
                {"name": "haversine_km", "kind": "scalar", "volatility": "immutable", "from": "sql", "description": "Haversine distance in kilometres"},
            ])
        )
    )
))]
pub(crate) async fn list(Extension(rt): Extension<Arc<Runtime>>) -> Response {
    let Some(app) = rt.read_app().await else {
        return (StatusCode::OK, Json(Vec::<ListFunctionElement>::new())).into_response();
    };
    if !app.runtime.functions.enabled {
        return (StatusCode::OK, Json(Vec::<ListFunctionElement>::new())).into_response();
    }

    let scalar_function_names = rt.df.ctx.udfs();
    let table_function_names = rt
        .df
        .ctx
        .state()
        .table_functions()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let functions: Vec<ListFunctionElement> = app
        .functions
        .iter()
        .filter(|decl| {
            decl.enabled
                && match decl.kind {
                    spicepod::component::function::FunctionKind::Scalar => scalar_function_names
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&decl.name)),
                    spicepod::component::function::FunctionKind::Table => table_function_names
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&decl.name)),
                }
        })
        .map(|decl| ListFunctionElement {
            name: decl.name.clone(),
            kind: decl.kind.as_str().to_string(),
            volatility: crate::datafusion::udf::effective_user_function_volatility(decl)
                .to_string(),
            from: decl.from.clone(),
            description: decl.description.clone(),
        })
        .collect();

    (StatusCode::OK, Json(functions)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    use app::AppBuilder;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::catalog::{TableFunctionImpl, TableProvider};
    use datafusion::common::DataFusionError;
    use datafusion::logical_expr::{ColumnarValue, Volatility as DataFusionVolatility, create_udf};
    use datafusion::prelude::Expr;
    use datafusion::scalar::ScalarValue;
    use http_body_util::BodyExt;
    use spicepod::component::function::{
        Function, FunctionArg, FunctionKind, FunctionReturns, Signature,
        Volatility as FunctionVolatility,
    };
    use spicepod::component::runtime::{Functions, Runtime as SpicepodRuntime};
    use std::collections::HashMap;

    #[tokio::test]
    async fn list_returns_empty_when_runtime_functions_disabled() {
        let rt = test_runtime(false, vec![test_function("registered_fn", true)]).await;
        register_stub_udf(&rt, "registered_fn");

        let (status, functions) = list_json(rt).await;

        assert_eq!(status, StatusCode::OK);
        assert!(functions.is_empty());
    }

    #[tokio::test]
    async fn list_filters_to_enabled_registered_functions() {
        let mut remote_function = test_function("Remote_Fn", true);
        remote_function.from = "https://example.com/udf".to_string();
        remote_function.volatility = FunctionVolatility::Immutable;

        let rt = test_runtime(
            true,
            vec![
                test_function("User_Fn", true),
                remote_function,
                test_table_function("Rows_Fn", true),
                test_function("disabled_fn", false),
                test_function("missing_fn", true),
            ],
        )
        .await;
        register_stub_udf(&rt, "user_fn");
        register_stub_udf(&rt, "remote_fn");
        register_stub_udtf(&rt, "rows_fn");
        register_stub_udf(&rt, "disabled_fn");

        let (status, functions) = list_json(rt).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            functions,
            vec![
                ListFunctionElement {
                    name: "User_Fn".to_string(),
                    kind: "scalar".to_string(),
                    volatility: "stable".to_string(),
                    from: "sql".to_string(),
                    description: Some("User_Fn description".to_string()),
                },
                ListFunctionElement {
                    name: "Remote_Fn".to_string(),
                    kind: "scalar".to_string(),
                    volatility: "stable".to_string(),
                    from: "https://example.com/udf".to_string(),
                    description: Some("Remote_Fn description".to_string()),
                },
                ListFunctionElement {
                    name: "Rows_Fn".to_string(),
                    kind: "table".to_string(),
                    volatility: "stable".to_string(),
                    from: "sql".to_string(),
                    description: Some("Rows_Fn description".to_string()),
                },
            ]
        );
    }

    async fn test_runtime(functions_enabled: bool, functions: Vec<Function>) -> Arc<Runtime> {
        let runtime = SpicepodRuntime {
            functions: if functions_enabled {
                Functions::enabled()
            } else {
                Functions::default()
            },
            ..Default::default()
        };

        let mut app_builder = AppBuilder::new("test_app").with_runtime(runtime);
        for function in functions {
            app_builder = app_builder.with_function(function);
        }

        let rt = Arc::new(Runtime::builder().build().await);
        let app_slot = rt.app();
        *app_slot.write().await = Some(Arc::new(app_builder.build()));
        rt
    }

    fn test_function(name: &str, enabled: bool) -> Function {
        Function {
            name: name.to_string(),
            from: "sql".to_string(),
            enabled,
            description: Some(format!("{name} description")),
            kind: FunctionKind::Scalar,
            volatility: FunctionVolatility::Stable,
            signature: Signature {
                tables: vec![],
                args: vec![FunctionArg {
                    name: "x".to_string(),
                    arrow_type: "int64".to_string(),
                }],
                returns: Some(FunctionReturns::Scalar("int64".to_string())),
            },
            body: Some("x".to_string()),
            body_ref: None,
            metadata: HashMap::new(),
            params: HashMap::new(),
            depends_on: vec![],
            metrics: None,
            as_tool: false,
        }
    }

    fn test_table_function(name: &str, enabled: bool) -> Function {
        let mut function = test_function(name, enabled);
        function.kind = FunctionKind::Table;
        function.signature.returns = Some(FunctionReturns::Table(vec![FunctionArg {
            name: "value".to_string(),
            arrow_type: "int64".to_string(),
        }]));
        function.as_tool = false;
        function
    }

    fn register_stub_udf(rt: &Runtime, name: &str) {
        let udf = create_udf(
            name,
            vec![],
            DataType::Int64,
            DataFusionVolatility::Immutable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))))),
        );
        rt.datafusion().ctx.register_udf(udf);
    }

    #[derive(Debug)]
    struct StubTableFunction;

    impl TableFunctionImpl for StubTableFunction {
        fn call(&self, _args: &[Expr]) -> datafusion::common::Result<Arc<dyn TableProvider>> {
            Err(DataFusionError::Plan(
                "stub table function is not executable".to_string(),
            ))
        }
    }

    fn register_stub_udtf(rt: &Runtime, name: &str) {
        rt.datafusion()
            .ctx
            .register_udtf(name, Arc::new(StubTableFunction));
    }

    async fn list_json(rt: Arc<Runtime>) -> (StatusCode, Vec<ListFunctionElement>) {
        let response = list(Extension(rt)).await;
        let status = response.status();
        let body_bytes = response
            .into_body()
            .collect()
            .await
            .expect("list functions response body should collect")
            .to_bytes();
        let functions = serde_json::from_slice(&body_bytes)
            .expect("list functions response should be JSON array");
        (status, functions)
    }
}
