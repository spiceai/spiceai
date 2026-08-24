/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! `runtime.task_history.task` naming for proxied MCP tool calls.
//!
//! A proxied tool is reachable through two entry points — the `/v1/mcp` gateway,
//! which resolves a request by the name the tool is exposed under, and
//! [`McpToolWrapper::call`](super::tool::McpToolWrapper::call), which a
//! `POST /v1/tools/{name}` request or a model-driven tool call enters directly.
//! Both record the task through this one function, so a tool cannot end up
//! recorded under two spellings: grouping `runtime.task_history` by `task` would
//! then split one tool across two rows, and `spice trace <task>` would show only
//! the half matching the spelling it was given.

/// Prefix shared by every tool-use task (`spice trace` matches on it).
pub(crate) const TOOL_USE_PREFIX: &str = "tool_use::";

/// Task name for a call on the tool exposed as `exposed_name`.
///
/// `exposed_name` is the catalog-qualified name [`tools::naming::encode_tool_name`]
/// produces — the name `/v1/tools` lists, and therefore the only one a user can
/// discover and pass to `spice trace`.
pub(crate) fn task_name_for_exposed_tool(exposed_name: &str) -> String {
    format!("{TOOL_USE_PREFIX}{exposed_name}")
}
