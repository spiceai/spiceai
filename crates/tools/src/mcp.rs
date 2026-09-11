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
    model::{CallToolRequestParams, CallToolResponse},
    service::ServiceError,
};

/// [`McpProxy`] is the minimal interface for tools that are fundamentally proxies around MCP tools.
///
/// This trait lets Spice pass through all details from the underlying MCP server in its (i.e. Spiced's) MCP server implementation.
///
/// `call_tool` returns [`CallToolResponse`] so a gateway can relay
/// `input_required` (SEP-2322 MRTR) instead of collapsing every outcome to
/// [`CallToolResponse::Complete`]. The high-level client `call_tool` helper
/// consumes that variant locally; proxies must use `call_tool_once`.
#[async_trait]
pub trait McpProxy: Send + Sync {
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
    ) -> Result<CallToolResponse, ServiceError>;
}
