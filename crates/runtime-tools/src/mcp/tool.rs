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
use datafusion_ddl::component_options::{recorded_call_arguments, redact_values};
use rmcp::{
    model::{CallToolRequestParams, CallToolResponse, CallToolResult, JsonObject, Tool, object},
    service::ServiceError,
};
use serde_json::Value;
use snafu::ResultExt;
use std::{borrow::Cow, io, sync::Arc};
use tokio::sync::RwLock;
use tools::McpProxy;
use tracing::Span;
use tracing_futures::Instrument;
use util::security::{MAX_SAFE_JSON_DEPTH, get_json_depth};

use tools::SpiceModelTool;
use tools::naming::encode_tool_name;

use super::{
    MAX_TOOL_RESULT_SIZE, Result,
    catalog::{McpClient, Reconnector},
    task_name_for_exposed_tool,
};
use crate::builtin::sql::{is_sql_tool, recorded_input};

pub struct McpToolWrapper {
    client: Arc<RwLock<McpClient>>,
    spec: Tool,

    /// Spicepod defined name, not from underlying MCP.
    server_name: String,

    /// Replaces `client` when a call finds its transport closed.
    reconnector: Arc<Reconnector>,
}

impl McpToolWrapper {
    pub(crate) fn new(
        client: Arc<RwLock<McpClient>>,
        spec: Tool,
        server_name: String,
        reconnector: Arc<Reconnector>,
    ) -> Self {
        Self {
            client,
            spec,
            server_name,
            reconnector,
        }
    }

    /// Reconnects when `result` failed because the transport is closed (e.g. a
    /// stdio server crashed). Returns whether the call should be retried once.
    async fn reconnected_after_close<T>(&self, result: &Result<T, ServiceError>) -> bool {
        matches!(result, Err(ServiceError::TransportClosed)) && self.reconnector.reconnect().await
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
    /// `runtime::tools::utils` — so [`Self::name`] is still the bare upstream
    /// name and the exposed name has to be recomputed from its parts. It cannot
    /// be read off the caller either: a `POST /v1/tools/{name}` request resolves
    /// through `Runtime::get_tool`, which hands back the tool un-renamed.
    ///
    /// The qualification is recomputed *unconditionally*, which matches the
    /// exposure layer only because a tool catalog is exempted from it when it is
    /// a default catalog, and the default catalogs are `memory` and `builtin` —
    /// never an MCP server. Were an MCP-backed catalog ever made default, this
    /// would go back to labelling a tool differently from the name it is listed
    /// under.
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

    /// A call to another Spice's `sql` tool, such as one this Spice reaches at
    /// its `/v1/mcp`, is recorded as `sql` records it, so a definition's
    /// credentials stay out of this Spice's task history too; that holds for
    /// any server's tool named `sql`, which fails closed. A tool of another
    /// name, which may be an `sql` tool its Spicepod renamed, records each
    /// statement in its arguments that may define a component the same way.
    fn recorded_arguments<'a>(&self, arg: &'a str) -> (Cow<'a, str>, Vec<String>) {
        if is_sql_tool(&self.spec.name) {
            return recorded_input(arg);
        }
        match recorded_call_arguments(arg) {
            Some((recorded, removed)) => (Cow::Owned(recorded), removed),
            None => (Cow::Borrowed(arg), Vec::new()),
        }
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
        let (input, removed) = self.recorded_arguments(arg);
        let span: Span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::mcp", tool = %exposed_name, input = %input);
        tracing::info!(target: "task_history", parent: &span, task_override = %task_name, mcp_server = %self.server_name, "labels");

        let tool_use_result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
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

            let params = CallToolRequestParams::new(self.internal_name()).with_arguments(object(input));
            let mut result = self.client.read().await.call_tool(params.clone()).await;
            if self.reconnected_after_close(&result).await {
                result = self.client.read().await.call_tool(params).await;
            }
            // The transport error names the HTTP client type. Keep the
            // sentence that says what happened to the call.
            let response = match result {
                Ok(response) => response,
                Err(e) => return Err(tool_facing_error_text(&e.to_string()).into()),
            };

            // The transport already bounds each response body; this also
            // covers results from a stdio server.
            if result_exceeds_limit(&response, MAX_TOOL_RESULT_SIZE) {
                let message = tool_result_too_large_message(&exposed_name);
                tracing::warn!("{message}");
                return Err(message.into());
            }

            if response.is_error == Some(true) {
                let text = response
                    .content
                    .iter()
                    .filter_map(|c| c.as_text().map(|t| t.text.as_str()))
                    .collect::<Vec<_>>()
                    .join("\n");
                let message = if text.is_empty() {
                    "MCP tool reported an error".to_string()
                } else {
                    text
                };
                return Err(message.into());
            }

            let content = serde_json::to_value(response.content).boxed()?;
            Ok(match response.structured_content {
                Some(structured) => {
                    serde_json::json!({"content": content, "structuredContent": structured})
                }
                None => content,
            })
        }
        .instrument(span.clone())
        .await;

        match tool_use_result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                let captured_output = redact_values(&captured_output_json, &removed);
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output);
                Ok(value)
            }
            Err(e) => {
                let message = e.to_string();
                let message = redact_values(&message, &removed);
                tracing::error!(target: "task_history", parent: &span, "{message}");
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
        let mut params = CallToolRequestParams::new(self.internal_name());
        if let Some(arguments) = arguments {
            params = params.with_arguments(arguments);
        }
        let mut result = self.client.read().await.call_tool(params.clone()).await;
        if self.reconnected_after_close(&result).await {
            result = self.client.read().await.call_tool(params).await;
        }
        result
    }

    async fn call_tool_once(
        &self,
        request: CallToolRequestParams,
    ) -> Result<CallToolResponse, ServiceError> {
        let request = rename_proxied_call(request, self.internal_name());
        let mut result = self
            .client
            .read()
            .await
            .call_tool_once(request.clone())
            .await;
        if self.reconnected_after_close(&result).await {
            result = self.client.read().await.call_tool_once(request).await;
        }
        result
    }
}

/// The text of a transport failure, without the client-type prefix the
/// transport puts in front of an over-limit response.
fn tool_facing_error_text(text: &str) -> String {
    text.find("MCP server '")
        .map_or_else(|| text.to_string(), |start| text[start..].to_string())
}

/// The single-line message for a tool result larger than [`MAX_TOOL_RESULT_SIZE`].
/// It is both the `WARN` log line and the tool error, so it never includes the result.
fn tool_result_too_large_message(exposed_name: &str) -> String {
    format!(
        "MCP tool '{exposed_name}' returned a result larger than the {} MiB limit, so the call failed and the result was discarded. Make the tool return a smaller result, for example by paginating it. See https://spiceai.org/docs/components/tools/mcp",
        MAX_TOOL_RESULT_SIZE / (1024 * 1024)
    )
}

/// An [`io::Write`] sink that only counts, and fails once the count passes `limit`
/// so serialization stops there instead of walking the rest of the value.
struct SizeLimitedSink {
    written: usize,
    limit: usize,
}

impl io::Write for SizeLimitedSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.written = self.written.saturating_add(buf.len());
        if self.written > self.limit {
            return Err(io::Error::other("size limit exceeded"));
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Whether the JSON the caller would receive — `content` plus
/// `structuredContent` — is larger than `limit` bytes, measured without
/// allocating it.
fn result_exceeds_limit(result: &CallToolResult, limit: usize) -> bool {
    let mut sink = SizeLimitedSink { written: 0, limit };
    // A serialization error other than the limit surfaces from `to_value` later.
    let _ = serde_json::to_writer(&mut sink, &result.content);
    if let Some(structured) = &result.structured_content
        && sink.written <= limit
    {
        let _ = serde_json::to_writer(&mut sink, structured);
    }
    sink.written > limit
}

/// Rewrite the gateway-exposed name to the upstream tool name.
///
/// Rebuilding via [`CallToolRequestParams::new`] drops `request.meta`
/// (`incoming_meta={"com.example/traceId":"trace-42"} forwarded_meta=null`)
/// while still copying MRTR continuation fields. Rename in place so
/// `_meta` and any later request fields are relayed.
fn rename_proxied_call(
    mut request: CallToolRequestParams,
    internal_name: impl Into<Cow<'static, str>>,
) -> CallToolRequestParams {
    request.name = internal_name.into();
    request
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::RequestMetaObject;
    use serde_json::json;
    use tools::naming::decode_tool_name;

    fn request_with_extension_meta() -> CallToolRequestParams {
        let meta = serde_json::from_value(json!({"com.example/traceId": "trace-42"}))
            .expect("request _meta object");
        let mut request =
            CallToolRequestParams::new("srv__deploy").with_request_state("opaque-server-state");
        request.meta = Some(meta);
        request
    }

    /// The previous rebuild copied MRTR fields onto `CallToolRequestParams::new`
    /// and dropped `_meta` (`incoming_meta=… forwarded_meta=null`).
    #[test]
    fn rebuild_from_new_drops_caller_meta() {
        let incoming = request_with_extension_meta();
        let mut req = CallToolRequestParams::new("deploy");
        if let Some(args) = incoming.arguments.clone() {
            req = req.with_arguments(args);
        }
        if let Some(responses) = incoming.input_responses.clone() {
            req = req.with_input_responses(responses);
        }
        if let Some(state) = incoming.request_state.clone() {
            req = req.with_request_state(state);
        }
        assert_eq!(
            req.request_state.as_deref(),
            Some("opaque-server-state"),
            "MRTR continuation fields survived the rebuild"
        );
        assert!(
            req.meta.is_none(),
            "incoming_meta={:?} forwarded_meta=null",
            incoming
                .meta
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .expect("request meta should serialize")
        );
    }

    #[test]
    fn rename_proxied_call_keeps_caller_meta() {
        let incoming = request_with_extension_meta();
        let forwarded = rename_proxied_call(incoming.clone(), "deploy");
        assert_eq!(forwarded.name.as_ref(), "deploy");
        assert_eq!(
            forwarded.meta, incoming.meta,
            "incoming_meta must be forwarded, not dropped"
        );
        assert_eq!(forwarded.request_state, incoming.request_state);
        let expected: RequestMetaObject =
            serde_json::from_value(json!({"com.example/traceId": "trace-42"}))
                .expect("request _meta object");
        assert_eq!(forwarded.meta, Some(expected));
    }

    fn serialized_len(result: &CallToolResult) -> usize {
        let content = serde_json::to_vec(&result.content).expect("content serializes");
        let structured = result.structured_content.as_ref().map_or(0, |s| {
            serde_json::to_vec(s)
                .expect("structured content serializes")
                .len()
        });
        content.len() + structured
    }

    /// The limit applies to `content` and `structuredContent` together, and a
    /// result of exactly the limit passes.
    #[test]
    fn result_size_limit_is_exact_and_counts_structured_content() {
        let text_only =
            CallToolResult::success(vec![rmcp::model::ContentBlock::text("X".repeat(1000))]);
        let len = serialized_len(&text_only);
        assert!(!result_exceeds_limit(&text_only, len));
        assert!(result_exceeds_limit(&text_only, len - 1));

        let mut with_structured = text_only.clone();
        with_structured.structured_content = Some(json!({"result": "X".repeat(1000)}));
        let len = serialized_len(&with_structured);
        assert!(len > serialized_len(&text_only) + 1000);
        assert!(!result_exceeds_limit(&with_structured, len));
        assert!(
            result_exceeds_limit(&with_structured, len - 1),
            "structuredContent must count toward the limit"
        );
    }

    #[test]
    fn transport_size_error_drops_the_client_type_path() {
        let raw = "Transport send error: Transport [rmcp::transport::worker::WorkerTransport<runtime_tools::mcp::http_client::BoundedHttpClient>] error: unexpected server response: MCP server 'hostile' sent a response larger than the 16 MiB limit, so the request failed and the response was discarded. Make the MCP tool return a smaller result, for example by paginating it. See https://spiceai.org/docs/components/tools/mcp";
        assert_eq!(
            tool_facing_error_text(raw),
            "MCP server 'hostile' sent a response larger than the 16 MiB limit, so the request failed and the response was discarded. Make the MCP tool return a smaller result, for example by paginating it. See https://spiceai.org/docs/components/tools/mcp"
        );
        assert_eq!(
            tool_facing_error_text("connection reset by peer"),
            "connection reset by peer"
        );
    }

    #[test]
    fn tool_result_too_large_message_names_the_tool_the_limit_and_the_fix() {
        assert_eq!(
            tool_result_too_large_message("github__search_code"),
            "MCP tool 'github__search_code' returned a result larger than the 16 MiB limit, so the call failed and the result was discarded. Make the tool return a smaller result, for example by paginating it. See https://spiceai.org/docs/components/tools/mcp"
        );
    }

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
        // (Asserting `task == task_name_for_exposed_tool(&exposed)` here would be
        // a tautology — the value above is what pins it.)
        assert_eq!(exposed, "github__search_code");
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
                .strip_prefix(super::super::TOOL_USE_PREFIX)
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
