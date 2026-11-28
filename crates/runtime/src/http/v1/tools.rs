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
    Extension, Json,
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tools::{SpiceModelTool, ToolAccess, ToolCallDenied};

use crate::{
    Runtime,
    tools::registry::{get_tool_registry_tool, tool_registry_prompt_tools},
};

/// Summary of a tool available to run, and the schema of its input parameters.
#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash, Default, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct ListToolElement {
    name: String,
    description: Option<String>,
    parameters: Option<serde_json::Value>,
}

impl ListToolElement {
    fn from_tool(tool: &Arc<dyn SpiceModelTool>) -> Self {
        Self {
            name: tool.name().to_string(),
            description: tool.description().map(|d| d.to_string()),
            parameters: tool.parameters(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
pub(crate) struct SearchToolsQuery {
    /// Embedding model name to use for searchable tool discovery. Required only when multiple embedding models are configured.
    embedding_model: Option<String>,
}

/// List Tools
///
/// Returns a list of all available tools in the Spice runtime. Tools provide reusable functionality that can be invoked programmatically or by AI agents.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/tools",
    operation_id = "list_tools",
    tag = "Tools",
    responses(
        (
            status = 200,  body = [ListToolElement],
            description = "All tools available in the Spice runtime",
            example = json!([
                {"name": "get_readiness", "description": "Report the readiness state of every Spice runtime component (datasets, accelerators, models, embeddings, catalogs).", "parameters": null},
                {"name": "list_datasets", "description": "List every dataset, view, and catalog visible to this runtime.", "parameters": null}
            ])
        ),
        (
            status = 401,
            description = "Tool routes require runtime auth to be configured",
            body = serde_json::Value,
            example = json!({"message": "Tool invocation (/v1/tools/*) requires `runtime.auth` to be configured."})
        )
    )
))]
pub(crate) async fn list(Extension(rt): Extension<Arc<Runtime>>) -> Response {
    // Only the tools this caller may call; a call is still authorized on its own.
    let can_write = ::tools::caller_can_write().await;
    let tools = rt
        .list_all_tools()
        .filter(|tool| std::future::ready(can_write || tool.access() == ToolAccess::ReadOnly))
        .map(|tool| ListToolElement::from_tool(&tool))
        .collect::<Vec<_>>()
        .await;

    (StatusCode::OK, Json(tools)).into_response()
}

/// List Searchable Tool Registry Tools
///
/// Returns the small set of tool definitions an external LLM client should inject to use Spice's searchable tool registry. Invoke returned tools with `POST /v1/tools/{name}`.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/tools/search",
    operation_id = "list_searchable_tool_registry_tools",
    tag = "Tools",
    params(SearchToolsQuery),
    responses(
        (
            status = 200,  body = [ListToolElement],
            description = "Searchable tool registry tools to inject into an external LLM prompt",
            example = json!([
                {"name": "tool_search", "description": "Search the Spice tool registry for tools relevant to the current task.", "parameters": {"type": "object"}},
                {"name": "tool_invoke", "description": "Invoke one Spice tool returned by tool_search.", "parameters": {"type": "object"}},
                {"name": "list_datasets", "description": "List every dataset, view, and catalog visible to this runtime.", "parameters": null}
            ])
        ),
        (status = 400, description = "Searchable tool registry is not configured", body = serde_json::Value),
        (
            status = 401,
            description = "Tool routes require runtime auth to be configured",
            body = serde_json::Value,
            example = json!({"message": "Tool invocation (/v1/tools/*) requires `runtime.auth` to be configured."})
        )
    )
))]
pub(crate) async fn search(
    Extension(rt): Extension<Arc<Runtime>>,
    Query(query): Query<SearchToolsQuery>,
) -> Response {
    match tool_registry_prompt_tools(Arc::clone(&rt), query.embedding_model.as_deref()).await {
        Ok(tools) => {
            let can_write = ::tools::caller_can_write().await;
            let tools = tools
                .iter()
                .filter(|tool| can_write || tool.access() == ToolAccess::ReadOnly)
                .map(ListToolElement::from_tool)
                .collect::<Vec<_>>();
            (StatusCode::OK, Json(tools)).into_response()
        }
        Err(error) => bad_request(error.to_string().as_str()),
    }
}

/// Run Tool
///
/// Execute a specific tool by name. The request body schema and response format are defined by each individual tool's specification. Use `GET /v1/tools` to discover available tools and their parameter schemas.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/tools/{name}",
    operation_id = "run_tool",
    tag = "Tools",
    params(
        ("name" = String, Path, description = "Name of the tool"),
        ("embedding_model" = Option<String>, Query, description = "Embedding model to use when invoking the searchable tool registry's tool_search meta-tool")
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
        (status = 400, description = "Invalid searchable tool registry configuration", body = serde_json::Value),
        (
            status = 401,
            description = "Tool routes require runtime auth to be configured",
            body = serde_json::Value,
            example = json!({"message": "Tool invocation (/v1/tools/*) requires `runtime.auth` to be configured."})
        ),
        (
            status = 403,
            description = "The tool can change data or act outside Spice, and the credentials on the request do not allow write access",
            body = serde_json::Value,
            example = json!({"message": "Failed to call tool 'store_memory': the tool can change data or act outside Spice, and the credentials on this request do not allow write access. Retry with a read-write API key (a `runtime.auth.api-key.keys` entry ending in `:rw`). See https://spiceai.org/docs/api/auth"})
        ),
        (
            status = 404,
            description = "Tool not found",
            body = serde_json::Value,
            example = json!({"message": "Tool 'no_sql' not found"})
        ),
        (status = 500, description = "An error occurred while calling the tool", body = serde_json::Value,
            example=json!({"message": "Error calling tool no_sql: No such tool"}))
    )
))]
pub(crate) async fn post(
    Extension(rt): Extension<Arc<Runtime>>,
    Path(tool_name): Path<String>,
    Query(query): Query<SearchToolsQuery>,
    body: String,
) -> Response {
    let tool = match get_tool_registry_tool(
        Arc::clone(&rt),
        tool_name.as_str(),
        query.embedding_model.as_deref(),
    )
    .await
    {
        Ok(Some(tool)) => Some(tool),
        Ok(None) => rt.get_tool(tool_name.as_str()).await,
        Err(error) => return bad_request(error.to_string().as_str()),
    };

    let Some(tool) = tool else {
        return not_found(format!("Tool '{tool_name}' not found").as_str());
    };

    match tool.call(body.as_str()).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(e) => {
            // The caller may not call the tool (or a tool `tool_invoke` resolved): a read-write
            // tool needs a principal with write access.
            if let Some(denied) = ToolCallDenied::find(e.as_ref()) {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({"message": denied.to_string()})),
                )
                    .into_response();
            }
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"message": format!("Error calling tool {tool_name}: {e}")})),
            )
                .into_response()
        }
    }
}

fn not_found(message: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"message": message}))).into_response()
}

fn bad_request(message: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"message": message}))).into_response()
}

#[cfg(test)]
#[cfg(feature = "http-functions")]
mod tests {
    use std::{future::Future, sync::Arc};

    use axum::{
        Extension,
        extract::{Path, Query},
        http::StatusCode,
        response::Response,
    };
    use http_body_util::BodyExt;
    use runtime_auth::{AuthPrincipalRef, AuthRequestContext};
    use runtime_request_context::{Protocol, RequestContext};
    use spicepod::component::runtime::ApiKey;

    use super::{SearchToolsQuery, list, post};
    use crate::Runtime;

    fn read_only() -> ApiKey {
        ApiKey::ReadOnly {
            key: "read-only".to_string(),
        }
    }

    fn read_write() -> ApiKey {
        ApiKey::ReadWrite {
            key: "read-write".to_string(),
        }
    }

    /// Runs `future` in a request authenticated with `key`, as the API key layer attaches it.
    async fn as_caller<F: Future>(key: ApiKey, future: F) -> F::Output {
        let context = Arc::new(RequestContext::builder(Protocol::Http).build());
        context
            .set_auth_principal(Arc::new(key) as AuthPrincipalRef)
            .expect("a new request has no principal yet");
        context.scope(future).await
    }

    async fn answer(response: Response) -> (StatusCode, String) {
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("a response body")
            .to_bytes();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    /// The names of the tools `GET /v1/tools` offers the caller with `key`, sorted.
    async fn offered(rt: &Arc<Runtime>, key: ApiKey) -> Vec<String> {
        let (status, text) = answer(as_caller(key, list(Extension(Arc::clone(rt)))).await).await;
        assert_eq!(status, StatusCode::OK, "GET /v1/tools: {text}");
        let tools: Vec<serde_json::Value> =
            serde_json::from_str(&text).expect("a JSON array of tools");
        let mut names: Vec<String> = tools
            .iter()
            .map(|tool| tool["name"].as_str().expect("a tool name").to_string())
            .collect();
        names.sort();
        names
    }

    /// `POST /v1/tools/{name}` with `{"x": 21}`, by the caller with `key`.
    async fn call(rt: &Arc<Runtime>, key: ApiKey, name: &str) -> (StatusCode, String) {
        let response = as_caller(
            key,
            post(
                Extension(Arc::clone(rt)),
                Path(name.to_string()),
                Query(SearchToolsQuery::default()),
                r#"{"x": 21}"#.to_string(),
            ),
        )
        .await;
        answer(response).await
    }

    /// A `functions:` entry, parsed as a Spicepod declares it.
    fn function(declaration: &str) -> spicepod::component::function::Function {
        yaml::from_str(declaration).expect("the function declaration parses")
    }

    /// Every tool `GET /v1/tools` offers a read-only caller is one it may call. A function exposed
    /// as a tool that calls out over HTTP needs write access, as it does in SQL, so only a caller
    /// with write access is offered it, and a read-only caller that names it anyway is refused as
    /// for any tool that needs write access.
    #[tokio::test]
    async fn a_read_only_caller_may_call_every_tool_it_is_offered() {
        let app = app::AppBuilder::new("tool_offers")
            .with_function(function(
                "name: double\nfrom: sql\nsignature:\n  args: [{ name: x, type: int64 }]\n  returns: int64\nbody: \"x * 2\"\n",
            ))
            // Never reached: only a caller with write access may call it, and none does here.
            .with_function(function(
                "name: score\nfrom: https://functions.example.com/score\nsignature:\n  args: [{ name: x, type: int64 }]\n  returns: int64\n",
            ))
            .with_runtime(spicepod::component::runtime::Runtime {
                functions: spicepod::component::runtime::Functions::enabled(),
                ..Default::default()
            })
            .build();
        let rt = Arc::new(Runtime::builder().with_app(app).build().await);

        assert_eq!(
            offered(&rt, read_write()).await,
            ["double", "score"],
            "a caller with write access is offered both functions"
        );

        let offers = offered(&rt, read_only()).await;
        for name in &offers {
            assert_eq!(
                call(&rt, read_only(), name).await,
                (StatusCode::OK, "42".to_string()),
                "tool '{name}', offered to a read-only caller, refused its call"
            );
        }
        assert_eq!(offers, ["double"]);

        assert_eq!(
            call(&rt, read_only(), "score").await,
            (
                StatusCode::FORBIDDEN,
                r#"{"message":"Failed to call tool 'score': the tool can change data or act outside Spice, and the credentials on this request do not allow write access. Retry with a read-write API key (a `runtime.auth.api-key.keys` entry ending in `:rw`). See https://spiceai.org/docs/api/auth"}"#
                    .to_string()
            ),
            "a read-only caller that names the function anyway"
        );
    }
}
