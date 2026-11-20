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

use crate::{SpiceModelTool, ToolAccess};
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
/// cleanly and is wrong at runtime.
///
/// `assert_answers_for_inner` holds that for the methods [`SpiceModelTool`]
/// exposes today, and cannot hold it for one added later: a new method with a
/// default impl is inherited here, asserted by nothing, and the test still
/// passes. Adding a method to the trait therefore means forwarding it here and
/// extending that helper — which is why the requirement is also recorded on
/// the trait, where someone adding one would be looking.
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

    fn output_schema(&self) -> Option<Value> {
        self.tool.output_schema()
    }

    fn access(&self) -> ToolAccess {
        self.tool.access()
    }

    // `call` and `authorized_mcp_proxy` are inherited: they authorize the caller for this
    // tool's `access`, the inner tool's, and then reach the inner tool through the two below.

    #[expect(
        clippy::disallowed_methods,
        reason = "forwards a call `SpiceModelTool::call` authorized for the wrapped tool's access"
    )]
    async fn run(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        self.tool.run(arg).await
    }

    #[cfg(feature = "mcp")]
    #[expect(
        clippy::disallowed_methods,
        reason = "forwards to `authorized_mcp_proxy`, which authorizes for the wrapped tool's access"
    )]
    async fn as_mcp_proxy(&self) -> Option<&dyn crate::McpProxy> {
        self.tool.as_mcp_proxy().await
    }
}

#[cfg(test)]
mod tests {
    use super::{SpiceModelTool, with_name};
    use crate::{
        ToolAccess, ToolCallDenied,
        access::test_support::{as_caller, read_only, read_write},
    };
    use async_trait::async_trait;
    use serde_json::Value;
    use std::{
        borrow::Cow,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

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
        access: ToolAccess,
        runs: AtomicUsize,
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
                access: ToolAccess::ReadWrite,
                runs: AtomicUsize::new(0),
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
                access: ToolAccess::ReadOnly,
                runs: AtomicUsize::new(0),
                #[cfg(feature = "mcp")]
                proxy: None,
            })
        }

        fn read_write() -> Arc<Self> {
            Arc::new(Self {
                strict: None,
                access: ToolAccess::ReadWrite,
                runs: AtomicUsize::new(0),
                #[cfg(feature = "mcp")]
                proxy: Some(StubProxy),
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

        fn output_schema(&self) -> Option<Value> {
            self.strict
                .map(|_| serde_json::json!({ "type": "object", "required": ["rows"] }))
        }

        fn access(&self) -> ToolAccess {
            self.access
        }

        async fn run(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Ok(Value::String(format!("inner called with {arg}")))
        }

        #[cfg(feature = "mcp")]
        async fn as_mcp_proxy(&self) -> Option<&dyn crate::McpProxy> {
            self.proxy.as_ref().map(|p| p as &dyn crate::McpProxy)
        }
    }

    /// Assert `renamed` answers for `inner` on every method [`SpiceModelTool`]
    /// exposes today, other than [`SpiceModelTool::name`].
    ///
    /// This records the current trait surface; it does not grow with it. A
    /// method added to the trait is inherited by the wrapper and checked by
    /// nothing below, so extend this whenever the trait gains one. Keeping the
    /// per-method assertions here rather than in each test is what makes that a
    /// single edit instead of one per wrapping shape.
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
            renamed.output_schema(),
            inner.output_schema(),
            "output_schema() must forward the inner tool's schema, not the trait's None"
        );
        assert_eq!(
            renamed.access(),
            inner.access(),
            "access() must forward the inner tool's access, or the wrapper decides who may call it"
        );
        // A caller with write access may call either kind of tool, so the comparison
        // reaches `run` through `call` whatever the fixture's access.
        assert_eq!(
            as_caller(read_write(), renamed.call("x"))
                .await
                .expect("the inner call succeeds"),
            as_caller(read_write(), inner.call("x"))
                .await
                .expect("the inner call succeeds")
        );
        #[cfg(feature = "mcp")]
        assert_eq!(
            as_caller(read_write(), renamed.authorized_mcp_proxy())
                .await
                .expect("a read-write caller is authorized")
                .is_some(),
            as_caller(read_write(), inner.authorized_mcp_proxy())
                .await
                .expect("a read-write caller is authorized")
                .is_some(),
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
        assert_eq!(inner.access(), ToolAccess::ReadWrite);
        #[cfg(feature = "mcp")]
        assert!(
            as_caller(read_write(), inner.authorized_mcp_proxy())
                .await
                .expect("a read-write caller is authorized")
                .is_some()
        );

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
        assert_eq!(renamed.access(), ToolAccess::ReadOnly);
        #[cfg(feature = "mcp")]
        assert!(
            as_caller(read_write(), renamed.authorized_mcp_proxy())
                .await
                .expect("a read-write caller is authorized")
                .is_none()
        );
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

    /// A catalog-qualified name is another way to reach the same tool, so it is held to the
    /// wrapped tool's access: a caller without write access is refused a read-write tool under
    /// its renamed spelling too, before the tool runs.
    #[tokio::test]
    async fn renamed_read_write_tool_refuses_a_caller_without_write_access() {
        let inner = InnerTool::read_write();
        let inner_dyn: Arc<dyn SpiceModelTool> = Arc::clone(&inner) as Arc<dyn SpiceModelTool>;
        let renamed = with_name(&with_name(&inner_dyn, "once"), "twice");

        let error = as_caller(read_only(), renamed.call("x"))
            .await
            .expect_err("a read-only caller is refused a read-write tool");
        assert_eq!(
            error.to_string(),
            "Failed to call tool 'twice': the tool can change data or act outside Spice, and the credentials on this request do not allow write access. Retry with a read-write API key (a `runtime.auth.api-key.keys` entry ending in `:rw`). See https://spiceai.org/docs/api/auth"
        );
        let error = renamed
            .call("x")
            .await
            .expect_err("a caller with no principal is refused a read-write tool");
        assert!(
            matches!(
                error.downcast_ref::<ToolCallDenied>(),
                Some(ToolCallDenied::NoCredentials { tool }) if tool == "twice"
            ),
            "unexpected error: {error}"
        );
        #[cfg(feature = "mcp")]
        assert!(matches!(
            as_caller(read_only(), renamed.authorized_mcp_proxy()).await,
            Err(ToolCallDenied::ReadOnlyCredentials { .. })
        ));
        assert_eq!(
            inner.runs.load(Ordering::SeqCst),
            0,
            "a refused call must not reach the tool"
        );

        as_caller(read_write(), renamed.call("x"))
            .await
            .expect("a read-write caller may call it");
        assert_eq!(inner.runs.load(Ordering::SeqCst), 1);
    }
}
