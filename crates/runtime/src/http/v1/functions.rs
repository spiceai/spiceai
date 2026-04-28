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
use serde::{Deserialize, Serialize};
#[cfg(feature = "openapi")]
use serde_json::json;

use crate::Runtime;
use crate::datafusion::udf::user_function_infos;

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
pub(crate) async fn list(Extension(_rt): Extension<Arc<Runtime>>) -> Response {
    let functions: Vec<ListFunctionElement> = user_function_infos()
        .into_iter()
        .map(|info| ListFunctionElement {
            name: info.name,
            kind: info.kind,
            volatility: info.volatility,
            from: info.from,
            description: info.description,
        })
        .collect();

    (StatusCode::OK, Json(functions)).into_response()
}
