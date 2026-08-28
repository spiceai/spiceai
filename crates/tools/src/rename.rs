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

use crate::SpiceModelTool;
use async_trait::async_trait;
use serde_json::Value;
use std::{borrow::Cow, sync::Arc};

/// Recreate a tool with a new name.
///
/// Underlying tool is not modified.
pub fn with_name(tool: &Arc<dyn SpiceModelTool>, name: &str) -> Arc<dyn SpiceModelTool> {
    Arc::new(RenamedTool {
        name: name.into(),
        tool: Arc::clone(tool),
    })
}

/// Wraps [`SpiceModelTool`]s to enable renaming them.
///
/// Not intended for broad use, solely [`with_name`].
///
/// Every method other than [`SpiceModelTool::name`] must forward to the wrapped
/// tool. Inheriting a trait default here answers for the inner tool with the
/// trait's fallback instead of the inner tool's real value, which compiles
/// cleanly and is wrong at runtime; `renamed_tool_forwards_every_method`
/// pins the forwarding so a method added to the trait fails a test here rather
/// than silently defaulting.
struct RenamedTool {
    name: String,
    tool: Arc<dyn SpiceModelTool>,
}

#[async_trait]
impl SpiceModelTool for RenamedTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        self.tool.description()
    }

    fn strict(&self) -> Option<bool> {
        self.tool.strict()
    }

    fn parameters(&self) -> Option<Value> {
        self.tool.parameters()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        self.tool.call(arg).await
    }

    #[cfg(feature = "mcp")]
    async fn as_mcp_proxy(&self) -> Option<&dyn crate::McpProxy> {
        self.tool.as_mcp_proxy().await
    }
}

#[cfg(test)]
mod tests {
    use super::{SpiceModelTool, with_name};
    use async_trait::async_trait;
    use serde_json::Value;
    use std::{borrow::Cow, sync::Arc};

    #[cfg(feature = "mcp")]
    struct StubProxy;

    #[cfg(feature = "mcp")]
    #[async_trait]
    impl crate::McpProxy for StubProxy {
        async fn call_tool(
            &self,
            _arguments: Option<rmcp::model::JsonObject>,
        ) -> Result<rmcp::model::CallToolResult, rmcp::service::ServiceError> {
            Ok(rmcp::model::CallToolResult::success(vec![]))
        }
    }

    struct InnerTool {
        strict: Option<bool>,
        #[cfg(feature = "mcp")]
        proxy: Option<StubProxy>,
    }

    impl InnerTool {
        /// A tool answering every optional method with a value the trait default
        /// cannot produce, so a wrapper that inherits a default rather than
        /// forwarding disagrees with it.
        fn answering(strict: bool) -> Arc<dyn SpiceModelTool> {
            Arc::new(Self {
                strict: Some(strict),
                #[cfg(feature = "mcp")]
                proxy: Some(StubProxy),
            })
        }

        /// A tool that answers every optional method exactly as the trait default
        /// does, so forwarding must reproduce the absence rather than invent a
        /// value of the wrapper's own.
        fn silent() -> Arc<dyn SpiceModelTool> {
            Arc::new(Self {
                strict: None,
                #[cfg(feature = "mcp")]
                proxy: None,
            })
        }
    }

    #[async_trait]
    impl SpiceModelTool for InnerTool {
        fn name(&self) -> Cow<'_, str> {
            Cow::Borrowed("inner")
        }

        fn description(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed("the inner tool"))
        }

        fn strict(&self) -> Option<bool> {
            self.strict
        }

        fn parameters(&self) -> Option<Value> {
            Some(serde_json::json!({ "type": "object" }))
        }

        async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Value::String(format!("inner called with {arg}")))
        }

        #[cfg(feature = "mcp")]
        async fn as_mcp_proxy(&self) -> Option<&dyn crate::McpProxy> {
            self.proxy.as_ref().map(|p| p as &dyn crate::McpProxy)
        }
    }

    /// Assert `renamed` answers for `inner` on every method of the trait but
    /// [`SpiceModelTool::name`].
    ///
    /// Kept in one place deliberately: a method added to [`SpiceModelTool`] has
    /// to be forwarded for every wrapping shape at once, so it should have to be
    /// asserted in only one.
    async fn assert_answers_for_inner(
        renamed: &Arc<dyn SpiceModelTool>,
        inner: &Arc<dyn SpiceModelTool>,
    ) {
        assert_eq!(renamed.description(), inner.description());
        assert_eq!(renamed.parameters(), inner.parameters());
        assert_eq!(
            renamed.strict(),
            inner.strict(),
            "strict() must forward the inner tool's value, not the trait's None"
        );
        assert_eq!(
            renamed.call("x").await.expect("the inner call succeeds"),
            inner.call("x").await.expect("the inner call succeeds")
        );
        #[cfg(feature = "mcp")]
        assert_eq!(
            renamed.as_mcp_proxy().await.is_some(),
            inner.as_mcp_proxy().await.is_some(),
            "as_mcp_proxy() must forward the inner tool's proxy, not the trait's None"
        );
    }

    /// Renaming replaces the name and nothing else.
    #[tokio::test]
    async fn renamed_tool_forwards_every_method() {
        let inner = InnerTool::answering(true);
        let renamed = with_name(&inner, "catalog__inner");

        assert_eq!(renamed.name(), "catalog__inner");
        assert_eq!(inner.name(), "inner", "the wrapped tool is not modified");

        // The comparison in `assert_answers_for_inner` is only a regression guard
        // while the fixture's answers differ from the defaults a non-forwarding
        // wrapper would return; assert that here so it cannot pass vacuously.
        assert_eq!(inner.strict(), Some(true));
        #[cfg(feature = "mcp")]
        assert!(inner.as_mcp_proxy().await.is_some());

        assert_answers_for_inner(&renamed, &inner).await;
    }

    /// `Some(false)` and the trait default `None` mean different things to the
    /// callers that read `strict()` into an outgoing function definition, so a
    /// wrapper that collapses one into the other is not forwarding.
    #[tokio::test]
    async fn renamed_tool_forwards_a_false_strict_rather_than_absence() {
        let inner = InnerTool::answering(false);
        let renamed = with_name(&inner, "catalog__inner");

        assert_eq!(renamed.strict(), Some(false));
    }

    /// Forwarding must carry an *absent* answer through unchanged too — a wrapper
    /// that supplies a value of its own is as wrong as one that drops the inner
    /// tool's.
    #[tokio::test]
    async fn renamed_tool_forwards_absent_answers() {
        let inner = InnerTool::silent();
        let renamed = with_name(&inner, "catalog__inner");

        assert_eq!(renamed.strict(), None);
        #[cfg(feature = "mcp")]
        assert!(renamed.as_mcp_proxy().await.is_none());
        assert_answers_for_inner(&renamed, &inner).await;
    }

    /// Renames compose: each layer forwards to the one below, so a tool wrapped
    /// twice still answers for itself on every method but its name.
    #[tokio::test]
    async fn nested_renames_forward_through_every_layer() {
        let inner = InnerTool::answering(true);
        let renamed = with_name(&with_name(&inner, "once"), "twice");

        assert_eq!(renamed.name(), "twice");
        assert_answers_for_inner(&renamed, &inner).await;
    }
}
