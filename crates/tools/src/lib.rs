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

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]

#[cfg(feature = "mcp")]
pub mod mcp;
#[cfg(feature = "mcp")]
pub use mcp::McpProxy;

pub mod rename;

use async_trait::async_trait;
use serde_json::Value;
use std::borrow::Cow;

/// Tools that implement the [`SpiceModelTool`] trait can automatically be used by LLMs in the runtime.
#[async_trait]
pub trait SpiceModelTool: Sync + Send {
    fn name(&self) -> Cow<'_, str>;
    fn description(&self) -> Option<Cow<'_, str>>;
    fn strict(&self) -> Option<bool> {
        None
    }
    fn parameters(&self) -> Option<Value>;
    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;

    /// If the tool is a proxy around an MCP tool, this method should return the proxy. Otherwise, it should return None.
    ///
    /// This enables direct pass through of MCP tool calls.
    #[cfg(feature = "mcp")]
    async fn as_mcp_proxy(&self) -> Option<&dyn McpProxy> {
        None
    }
}
