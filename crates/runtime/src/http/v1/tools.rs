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

use std::sync::Arc;

use axum::{
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    tools::{factory::default_available_catalogs, SpiceModelTool, Tooling},
    Runtime,
};

/// Summary of a tool available to run, and the schema of its input parameters.
#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash, Default, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct ListToolElement {
    name: String,
    description: Option<String>,
    parameters: Option<serde_json::Value>,
    is_catalog: bool,
}

/// List Tools
///
/// List available tools in the Spice runtime.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/tools",
    tag = "Tools",
    responses(
        (
            status = 200,  body = [ListToolElement],
            description = "All tools available in the Spice runtime",
            example = json!([
                {"name": "get_readiness", "description": "Retrieves the readiness status of all runtime components including registered datasets, models, and embeddings.", "parameters": null},
                {"name": "list_datasets", "description": "List all SQL tables available.", "parameters": null}
            ])
        )
    )
))]
pub(crate) async fn list(Extension(rt): Extension<Arc<Runtime>>) -> Response {
    let tools = &*rt.tools.read().await;

    let default_catalogs = default_available_catalogs();

    let tools = stream::iter(tools.iter())
        .then(|(name, t)| {
            let default_catalog_names = default_catalogs
                .iter()
                .map(|c| c.name())
                .collect::<Vec<_>>();
            async move {
                match t {
                    Tooling::Tool(tool) => vec![ListToolElement {
                        name: name.to_string(),
                        description: tool.description().map(|d| d.to_string()),
                        parameters: tool.parameters(),
                        is_catalog: false,
                    }],
                    Tooling::Catalog(c) => {
                        // Do not list tools from default catalogs. They are already listed individually as tools.
                        if default_catalog_names.contains(&name.as_str()) {
                            return vec![];
                        };
                        c.all()
                            .await
                            .into_iter()
                            .map(|tool| ListToolElement {
                                name: format!("{name}/{}", tool.name()),
                                description: tool.description().map(|d| d.to_string()),
                                parameters: tool.parameters(),
                                is_catalog: true,
                            })
                            .collect()
                    }
                }
            }
        })
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    (StatusCode::OK, Json(tools)).into_response()
}

/// Run Tool
///
/// The request body and JSON response formats match the tool’s specification.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/tools/{name}",
    tag = "Tools",
    params(
        ("name" = String, Path, description = "Name of the tool")
    ),
    request_body(
        description = "Tool specific input parameters. See /v1/tools for parameter schema.",
        content(
            (serde_json::Value = "application/json", example = json!({
                    "query": "SELECT avg(total_amount), avg(tip_amount), count(1), passenger_count FROM my_table GROUP BY passenger_count ORDER BY passenger_count ASC LIMIT 3"
                })
            )
        )
    ),
    responses(
        (status = 200, description = "Tool Specific response, in JSON format", body=serde_json::Value,  examples((
            "sql" = (value = json!([{
              "AVG(my_table.tip_amount)": 3.072_259_971_396_793,
              "AVG(my_table.total_amount)": 25.327_816_939_456_525,
              "COUNT(Int64(1))": 31_465,
              "passenger_count": 0
            },
            {
              "AVG(my_table.tip_amount)": 3.371_262_288_468_005_7,
              "AVG(my_table.total_amount)": 26.205_230_445_474_996,
              "COUNT(Int64(1))": 2_188_739,
              "passenger_count": 1
            },
            {
              "AVG(my_table.tip_amount)": 3.717_130_211_329_085_4,
              "AVG(my_table.total_amount)": 29.520_659_930_930_304,
              "COUNT(Int64(1))": 405_103,
              "passenger_count": 2
            }]))
        ))),
        (status = 404, description = "Tool not found", body = String, example="Tool no_sql not found"),
        (status = 500, description = "Error occured whilst calling the tool", body = serde_json::Value,
            example=json!({"message": "Error calling tool no_sql: No such tool"}))
    )
))]
pub(crate) async fn post(
    Extension(rt): Extension<Arc<Runtime>>,
    Path(tool_name): Path<String>,
    body: String,
) -> Response {
    let tools = &*rt.tools.read().await;

    // Find tool by first checking if it is a tool catalog (i.e. has a '/'), if not find it as regular tool.
    let tool: Arc<dyn SpiceModelTool> = if let Some((catalog_name, name)) =
        tool_name.split_once('/')
    {
        let Some(Tooling::Catalog(catalog)) = tools.get(catalog_name) else {
            return not_found(format!("Tool '{tool_name}' not found").as_str());
        };
        match catalog.get(name).await {
            Some(tool) => tool,
            None => {
                return not_found(format!("Tool '{name}' not found in '{catalog_name}'").as_str());
            }
        }
    } else {
        let Some(Tooling::Tool(tool)) = tools.get(&tool_name) else {
            return not_found(format!("Tool {tool_name} not found").as_str());
        };
        Arc::clone(tool)
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

fn not_found(message: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"message": message}))).into_response()
}
