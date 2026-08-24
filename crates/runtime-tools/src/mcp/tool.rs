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

use async_trait::async_trait;
use rmcp::{
    model::{CallToolRequestParams, CallToolResult, JsonObject, Tool, object},
    service::ServiceError,
};
use serde_json::Value;
use snafu::ResultExt;
use std::{borrow::Cow, sync::Arc};
use tokio::sync::RwLock;
use tools::McpProxy;
use tracing::Span;
use tracing_futures::Instrument;
use util::security::{MAX_SAFE_JSON_DEPTH, get_json_depth};

use tools::SpiceModelTool;
use tools::naming::encode_tool_name;

use super::{Result, catalog::McpClient, task_name::task_name_for_exposed_tool};

pub struct McpToolWrapper {
    client: Arc<RwLock<McpClient>>,
    spec: Tool,

    /// Spicepod defined name, not from underlying MCP.
    server_name: String,
}

impl McpToolWrapper {
    pub fn new(client: Arc<RwLock<McpClient>>, spec: Tool, server_name: String) -> Self {
        Self {
            client,
            spec,
            server_name,
        }
    }

    #[must_use]
    pub fn internal_name(&self) -> Cow<'static, str> {
        self.spec.name.clone()
    }

    /// The `task_history` identifiers for a call on `spec` proxied from
    /// `server_name`: the `task` value and the `tool` label, as
    /// `(task, exposed_name)`.
    ///
    /// Both are the name the tool is exposed under. That qualification is
    /// applied *outside* this wrapper — `with_name` in `tooling.rs` /
    /// `runtime::tools::utils`, whose `RenamedTool` delegates `call` straight
    /// back here — so [`Self::name`] is still the bare upstream name and the
    /// exposed name has to be recomputed from its parts. Recording the bare name
    /// (or, before this, a `server/tool` join) labelled the same tool
    /// differently from the `/v1/mcp` gateway, which records the resolved
    /// exposed name.
    fn task_history_labels(server_name: &str, spec: &Tool) -> (String, String) {
        let exposed_name = encode_tool_name(server_name, &spec.name);
        (task_name_for_exposed_tool(&exposed_name), exposed_name)
    }
}

#[async_trait]
impl SpiceModelTool for McpToolWrapper {
    fn name(&self) -> Cow<'_, str> {
        self.internal_name()
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        self.spec.description.clone()
    }

    fn parameters(&self) -> Option<Value> {
        Some(Value::Object(
            self.spec
                .input_schema
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ))
    }

    async fn as_mcp_proxy(&self) -> Option<&dyn McpProxy> {
        Some(self)
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        // Security: Validate input size to prevent excessive memory consumption or processing overhead
        const MAX_INPUT_SIZE: usize = 1024 * 1024; // 1 MB
        if arg.len() > MAX_INPUT_SIZE {
            return Err(format!(
                "Input too large ({} bytes). Maximum allowed: {MAX_INPUT_SIZE} bytes",
                arg.len()
            )
            .into());
        }

        let (task_name, exposed_name) = Self::task_history_labels(&self.server_name, &self.spec);
        let span: Span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::mcp", tool = %exposed_name, input = arg);
        tracing::info!(target: "task_history", parent: &span, task_override = %task_name, mcp_server = %self.server_name, "labels");

        let tool_use_result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let client = self.client.read().await;

            let input: Value = if arg.is_empty() {
                Value::Null
            } else {
                // Security: Use controlled JSON parsing to prevent resource exhaustion
                serde_json::from_str(arg).map_err(|e| {
                    tracing::error!(target: "task_history", parent: &span, "Failed to parse input: {e}");
                    e
                })?
            };

            // Security: Validate JSON depth to prevent stack overflow
            if get_json_depth(&input) > MAX_SAFE_JSON_DEPTH {
                return Err(format!(
                    "Input JSON too deeply nested. Maximum depth: {MAX_SAFE_JSON_DEPTH}"
                )
                .into());
            }

            let response = client
                .call_tool(CallToolRequestParams::new(self.internal_name()).with_arguments(object(input)))
                .await
                .boxed()?;

            let v = serde_json::to_value(response.content).boxed()?;
            Ok(v)
        }
        .instrument(span.clone())
        .await;

        match tool_use_result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}

#[async_trait]
impl McpProxy for McpToolWrapper {
    async fn call_tool(
        &self,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, ServiceError> {
        let inner = self.client.read().await;
        let mut req = CallToolRequestParams::new(self.internal_name());
        if let Some(args) = arguments {
            req = req.with_arguments(args);
        }
        inner.call_tool(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tools::naming::decode_tool_name;

    fn spec(name: &'static str) -> Tool {
        Tool::new(name, "a proxied tool", Arc::new(serde_json::Map::new()))
    }

    #[test]
    fn labels_the_task_with_the_name_the_tool_is_exposed_under() {
        // Regression for https://github.com/spiceai/spiceai/issues/13338: the
        // wrapper joined the two components with the pre-#11629 `/` separator,
        // so the same tool was recorded as `tool_use::github/search_code` here
        // and `tool_use::github__search_code` through the `/v1/mcp` gateway,
        // splitting one logical tool across two `task_history` rows.
        let (task, exposed) = McpToolWrapper::task_history_labels("github", &spec("search_code"));
        assert_eq!(task, "tool_use::github__search_code");
        // The gateway labels the task from the exposed name it resolved the
        // request by, so agreeing on that name is what makes the two match.
        assert_eq!(exposed, "github__search_code");
        assert_eq!(task, task_name_for_exposed_tool(&exposed));
    }

    #[test]
    fn recorded_task_decodes_back_to_its_components() {
        // The recorded name must be the reversible encoding, so a reader can
        // recover which server and tool a row belongs to. The `/` join was not
        // reversible — `decode_tool_name("github/search_code")` is `None` — and
        // it emitted a component's literal `__` unescaped, which decodes to the
        // wrong split.
        for (server, tool) in [
            ("github", "search_code"),
            ("my-catalog", "list_files"),
            ("my__server", "some_tool"),
            ("server", "tool__name"),
        ] {
            let (task, exposed) = McpToolWrapper::task_history_labels(server, &spec(tool));
            let suffix = task
                .strip_prefix(super::super::task_name::TOOL_USE_PREFIX)
                .expect("a tool-use task name carries the prefix");
            assert_eq!(suffix, exposed, "task suffix must be the exposed name");
            assert_eq!(
                decode_tool_name(suffix),
                Some((server.to_string(), tool.to_string())),
                "recorded task {task} does not decode back to ({server}, {tool})"
            );
        }
    }
}
