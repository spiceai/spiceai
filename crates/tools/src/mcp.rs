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
    model::{CallToolRequestParams, CallToolResponse, CallToolResult, JsonObject},
    service::ServiceError,
};

/// [`McpProxy`] is the minimal interface for tools that are fundamentally proxies around MCP tools.
///
/// This trait lets Spice pass through all details from the underlying MCP server in its (i.e. Spiced's) MCP server implementation.
///
/// [`Self::call_tool`] keeps the complete-only contract (`Option<JsonObject>` →
/// [`CallToolResult`]) so existing implementers still compile. The gateway
/// calls [`Self::call_tool_once`], whose default wraps that result as
/// [`CallToolResponse::Complete`]. Proxies that must relay `input_required`
/// (SEP-2322 MRTR) override [`Self::call_tool_once`] and use the client's
/// `call_tool_once` helper — the high-level `call_tool` consumes
/// `InputRequired` locally.
///
/// A wrapper that impls this trait must forward both methods. Inheriting
/// [`Self::call_tool_once`] is only correct when the wrappee has no
/// `input_required` path of its own.
#[async_trait]
pub trait McpProxy: Send + Sync {
    async fn call_tool(
        &self,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, ServiceError>;

    /// Full `tools/call` response, including `input_required`.
    ///
    /// Default adapter: [`Self::call_tool`] → [`CallToolResponse::Complete`].
    /// Override to relay MRTR. Must be forwarded by any `McpProxy` wrapper.
    async fn call_tool_once(
        &self,
        request: CallToolRequestParams,
    ) -> Result<CallToolResponse, ServiceError> {
        self.call_tool(request.arguments)
            .await
            .map(CallToolResponse::Complete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct LegacyProxy;

    #[async_trait]
    impl McpProxy for LegacyProxy {
        async fn call_tool(
            &self,
            _arguments: Option<JsonObject>,
        ) -> Result<CallToolResult, ServiceError> {
            Ok(CallToolResult::success(vec![]))
        }
    }

    #[tokio::test]
    async fn legacy_call_tool_still_implements_the_trait() {
        let result = LegacyProxy.call_tool(None).await.expect("legacy call_tool");
        assert!(
            result.content.is_empty(),
            "legacy complete-only contract must still compile and run"
        );
    }

    #[tokio::test]
    async fn default_call_tool_once_wraps_complete() {
        let response = LegacyProxy
            .call_tool_once(CallToolRequestParams::new("ping"))
            .await
            .expect("default call_tool_once adapter");
        assert!(
            matches!(response, CallToolResponse::Complete(_)),
            "default adapter must wrap call_tool as Complete, got {response:?}"
        );
    }
}
