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

pub mod access;
pub mod naming;
pub mod rename;

pub use access::{ToolAccess, ToolCallDenied, caller_can_write, caller_may_call};

use async_trait::async_trait;
use serde_json::Value;
use std::borrow::Cow;

/// Tools that implement the [`SpiceModelTool`] trait can automatically be used by LLMs in the runtime.
///
/// **A method added here has to be forwarded in `rename::RenamedTool`.** That wrapper
/// re-exposes a tool under a catalog-qualified name and must answer for the tool it wraps on
/// everything but `name`. A method carrying a default impl is inherited there silently — it
/// compiles, and the wrapper then answers with the trait's fallback rather than the wrapped
/// tool's real value (spiceai/spiceai#13443). Its `assert_answers_for_inner` test asserts the
/// methods below as they stand and cannot fail for one that is added, so extend that too.
/// [`Self::call`] and [`Self::authorized_mcp_proxy`] are the exception: they authorize the
/// caller, and a wrapper inherits them rather than forwarding, so the check runs once, for the
/// tool's [`Self::access`].
#[async_trait]
pub trait SpiceModelTool: Sync + Send {
    fn name(&self) -> Cow<'_, str>;
    fn description(&self) -> Option<Cow<'_, str>>;
    fn strict(&self) -> Option<bool> {
        None
    }
    fn parameters(&self) -> Option<Value>;

    /// The JSON Schema of the object [`Self::call`] returns, when it always returns one. MCP
    /// clients receive it as the tool's `outputSchema` and the result as `structuredContent`.
    fn output_schema(&self) -> Option<Value> {
        None
    }

    /// What a call to this tool can change, which decides who may call it (see [`ToolAccess`]).
    ///
    /// No default, so every tool states its own and a wrapper has to forward it: a default
    /// would make a tool that forgot callable by every caller.
    fn access(&self) -> ToolAccess;

    /// Runs the tool with `arg`, its arguments as a JSON object.
    ///
    /// Only [`Self::call`] runs this, once the caller is authorized, and a wrapper forwards it
    /// to the tool it wraps. Called from anywhere else it skips that check, so `clippy.toml`
    /// disallows the call.
    async fn run(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;

    /// Runs the tool for the caller of the current request, if [`Self::access`] allows that
    /// caller to call it (see [`access`]); otherwise fails with [`ToolCallDenied`].
    ///
    /// Every path that runs a tool calls this, so the decision is the same on all of them. Do
    /// not override it.
    #[expect(
        clippy::disallowed_methods,
        reason = "the one call of `run`, after the caller is authorized"
    )]
    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        access::authorize(&self.name(), self.access(), Some(arg)).await?;
        self.run(arg).await
    }

    /// If the tool is a proxy around an MCP tool, this method should return the proxy. Otherwise, it should return None.
    ///
    /// Only [`Self::authorized_mcp_proxy`] calls this, and a wrapper forwards it. The proxy
    /// calls the MCP server without checking the caller, so `clippy.toml` disallows other calls.
    #[cfg(feature = "mcp")]
    async fn as_mcp_proxy(&self) -> Option<&dyn McpProxy> {
        None
    }

    /// The proxy [`Self::as_mcp_proxy`] returns, if [`Self::access`] allows the caller of the
    /// current request to call the tool; otherwise [`ToolCallDenied`]. `Ok(None)` when the tool
    /// is not an MCP proxy.
    ///
    /// This enables direct pass through of MCP tool calls, under the same decision as
    /// [`Self::call`]. Do not override it.
    #[cfg(feature = "mcp")]
    #[expect(
        clippy::disallowed_methods,
        reason = "the proxy is returned only once the caller is authorized"
    )]
    async fn authorized_mcp_proxy(&self) -> Result<Option<&dyn McpProxy>, ToolCallDenied> {
        let Some(proxy) = self.as_mcp_proxy().await else {
            return Ok(None);
        };
        access::authorize(&self.name(), self.access(), None).await?;
        Ok(Some(proxy))
    }
}
