/*
Copyright 2024 The Spice.ai OSS Authors

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
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{tools::Tooling, Runtime};

/// The structure of the JSON elements returned by the `/v1/tools` endpoint.
#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash, Default, Deserialize)]
struct ListToolElement {
    name: String,
    description: Option<String>,
    parameters: Option<serde_json::Value>,
}

pub(crate) async fn list(Extension(rt): Extension<Arc<Runtime>>) -> Response {
    let tools = &*rt.tools.read().await;
    let tools = tools
        .iter()
        .filter_map(|(name, tool)| match tool {
            Tooling::Tool(tool) => Some(ListToolElement {
                name: name.clone(),
                description: tool.description().map(ToString::to_string),
                parameters: tool.parameters(),
            }),
            Tooling::Catalog(_) => None,
        })
        .collect::<Vec<_>>();

    (StatusCode::OK, Json(tools)).into_response()
}

pub(crate) async fn post(
    Extension(rt): Extension<Arc<Runtime>>,
    Path(tool_name): Path<String>,
    body: String,
) -> Response {
    let tools = &*rt.tools.read().await;

    let Some(Tooling::Tool(tool)) = tools.get(&tool_name) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"message": format!("Tool {tool_name} not found")})),
        )
            .into_response();
    };

    match tool.call(body.as_str(), Arc::clone(&rt)).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"message": format!("Error calling tool {tool_name}: {e}")})),
        )
            .into_response(),
    }
}
