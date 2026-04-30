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

    let registered_udfs = rt.df.ctx.udfs();
    let functions: Vec<ListFunctionElement> = app
        .functions
        .iter()
        .filter(|decl| {
            decl.enabled
                && registered_udfs
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&decl.name))
        })
        .map(|decl| ListFunctionElement {
            name: decl.name.clone(),
            kind: function_kind(decl.kind).to_string(),
            volatility: volatility(decl.volatility).to_string(),
            from: decl.from.clone(),
            description: decl.description.clone(),
        })
        .collect();

    (StatusCode::OK, Json(functions)).into_response()
}

fn function_kind(kind: spicepod::component::function::FunctionKind) -> &'static str {
    match kind {
        spicepod::component::function::FunctionKind::Scalar => "scalar",
        spicepod::component::function::FunctionKind::Aggregate => "aggregate",
        spicepod::component::function::FunctionKind::Window => "window",
        spicepod::component::function::FunctionKind::Table => "table",
    }
}

fn volatility(volatility: spicepod::component::function::Volatility) -> &'static str {
    match volatility {
        spicepod::component::function::Volatility::Immutable => "immutable",
        spicepod::component::function::Volatility::Stable => "stable",
        spicepod::component::function::Volatility::Volatile => "volatile",
    }
}
