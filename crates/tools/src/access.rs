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

//! Who may call a tool.
//!
//! Every path that runs a tool — `POST /v1/tools/{name}`, MCP `tools/call` (including its
//! pass-through to an MCP server), a model's tool use from chat completions, Responses,
//! Messages or `ai()`, the searchable registry's `tool_invoke`, and a tool exposed as a SQL
//! function — reaches it through [`crate::SpiceModelTool::call`] or
//! [`crate::SpiceModelTool::authorized_mcp_proxy`], and both decide with [`authorize`]. The
//! rule is one: a tool that can change data or act outside Spice needs a principal with write
//! access, and a read-only principal may call only read-only tools.
//!
//! A request with no principal is decided by how the runtime authenticates (see
//! [`set_runtime_authenticates_callers`]). On a runtime that authenticates no caller (no
//! `runtime.auth`), every request has none, and it may call read-write tools as it may write
//! through `/v1/sql`. On a runtime that authenticates callers, every request carries a
//! principal, so a call with none is the runtime's own work: it may call only read-only tools,
//! unless it runs as a principal (`runtime_auth::SystemPrincipal`).

use std::sync::atomic::{AtomicBool, Ordering};

use runtime_auth::{AuthPrincipalRef, AuthRequestContext, principal_has_write_access};
use runtime_request_context::{AsyncMarker, RequestContext};
use snafu::Snafu;

/// Whether the runtime authenticates its callers; see [`set_runtime_authenticates_callers`].
/// Until it is set, it does, so a request with no principal is refused read-write tools.
static RUNTIME_AUTHENTICATES_CALLERS: AtomicBool = AtomicBool::new(true);

/// Records whether the runtime authenticates its callers (`runtime.auth`, or client
/// certificates as identity), which decides what a request with no principal may call. The
/// runtime sets it when it starts serving.
pub fn set_runtime_authenticates_callers(authenticates: bool) {
    RUNTIME_AUTHENTICATES_CALLERS.store(authenticates, Ordering::Relaxed);
}

/// What a call to a tool can change, which decides who may make it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolAccess {
    /// The tool only reads data the caller could query anyway, or runs each call with the
    /// caller's own access: the `sql` tool runs the query read-only unless the caller may
    /// write. Any caller may call it.
    ReadOnly,
    /// A call can write data or act outside Spice: `store_memory`, or any tool of an MCP
    /// server, whose effects Spice cannot know. Only a principal with write access may call it,
    /// or a request with no principal on a runtime that authenticates no caller.
    ReadWrite,
}

/// Why the caller of the current request may not call a tool.
#[derive(Debug, Snafu)]
pub enum ToolCallDenied {
    #[snafu(display(
        "Failed to call tool '{tool}': the tool can change data or act outside Spice, and the credentials on this request do not allow write access. Retry with a read-write API key (a `runtime.auth.api-key.keys` entry ending in `:rw`). See https://spiceai.org/docs/api/auth"
    ))]
    ReadOnlyCredentials { tool: String },

    #[snafu(display(
        "Failed to call tool '{tool}': the tool can change data or act outside Spice, and the request has no credentials. Configure `runtime.auth` with a read-write API key (a `runtime.auth.api-key.keys` entry ending in `:rw`) and send it with the request. See https://spiceai.org/docs/api/auth"
    ))]
    NoCredentials { tool: String },
}

impl ToolCallDenied {
    /// The refusal `error` is, or was caused by: a tool call that failed because its caller may
    /// not call a tool it reached, directly or through a tool that calls others (`tool_invoke`).
    #[must_use]
    pub fn find<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a Self> {
        std::iter::successors(Some(error), |error| error.source())
            .find_map(|error| error.downcast_ref::<Self>())
    }
}

/// Whether the caller of the current request may write: a principal with write access. A
/// request with no principal may not — a runtime without `runtime.auth`, or work the runtime
/// does on its own unless it runs as a principal (see `runtime_auth::SystemPrincipal`).
pub async fn caller_can_write() -> bool {
    let context = RequestContext::current(AsyncMarker::new().await);
    AuthRequestContext::auth_principal(context.as_ref()).is_some_and(principal_has_write_access)
}

/// Decides whether the caller of the current request may call `tool`, whose calls have
/// `access`. The one place that decision is made; see the module docs.
///
/// A refusal is recorded in `runtime.task_history` as the failure of a `tool_use::<tool>` task
/// with `input`, as a tool records its own failures, so a refused call is audited like one
/// that ran.
pub(crate) async fn authorize(
    tool: &str,
    access: ToolAccess,
    input: Option<&str>,
) -> Result<(), ToolCallDenied> {
    if access == ToolAccess::ReadOnly {
        return Ok(());
    }
    let context = RequestContext::current(AsyncMarker::new().await);
    let Err(denied) = decide(
        tool,
        access,
        AuthRequestContext::auth_principal(context.as_ref()),
        RUNTIME_AUTHENTICATES_CALLERS.load(Ordering::Relaxed),
    ) else {
        return Ok(());
    };
    let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use", tool = %tool, input = input.unwrap_or_default());
    let task = format!("tool_use::{tool}");
    tracing::info!(target: "task_history", parent: &span, task_override = %task, "labels");
    tracing::error!(target: "task_history", parent: &span, "{denied}");
    Err(denied)
}

/// The decision [`authorize`] makes, for a caller with `principal` on a runtime that
/// authenticates its callers or not.
fn decide(
    tool: &str,
    access: ToolAccess,
    principal: Option<&AuthPrincipalRef>,
    runtime_authenticates_callers: bool,
) -> Result<(), ToolCallDenied> {
    match (access, principal) {
        (ToolAccess::ReadOnly, _) => Ok(()),
        (ToolAccess::ReadWrite, Some(principal)) if principal_has_write_access(principal) => Ok(()),
        (ToolAccess::ReadWrite, Some(_)) => ReadOnlyCredentialsSnafu { tool }.fail(),
        (ToolAccess::ReadWrite, None) if !runtime_authenticates_callers => Ok(()),
        (ToolAccess::ReadWrite, None) => NoCredentialsSnafu { tool }.fail(),
    }
}

/// Runs futures as a caller with a chosen principal, for tests of the decision above.
#[cfg(test)]
pub(crate) mod test_support {
    use std::{future::Future, sync::Arc};

    use runtime_auth::{AuthPrincipal, AuthPrincipalRef, AuthRequestContext};
    use runtime_request_context::{Protocol, RequestContext};

    struct TestPrincipal(&'static [&'static str]);

    impl AuthPrincipal for TestPrincipal {
        fn username(&self) -> &'static str {
            "test"
        }

        fn groups(&self) -> &[&str] {
            self.0
        }
    }

    /// A principal in the `read` group, as a read-only API key is.
    pub(crate) fn read_only() -> AuthPrincipalRef {
        Arc::new(TestPrincipal(&["read"]))
    }

    /// A principal in the `read_write` group, as a read-write API key is.
    pub(crate) fn read_write() -> AuthPrincipalRef {
        Arc::new(TestPrincipal(&["read_write"]))
    }

    /// Runs `future` in a request made by `principal`.
    pub(crate) async fn as_caller<F: Future>(principal: AuthPrincipalRef, future: F) -> F::Output {
        let context = Arc::new(RequestContext::builder(Protocol::Http).build());
        context
            .set_auth_principal(principal)
            .expect("a new request has no principal yet");
        context.scope(future).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::test_support::{as_caller, read_only, read_write};
    use super::*;
    use runtime_auth::{AuthPrincipal, AuthPrincipalRef};

    struct WriteGroup;

    impl AuthPrincipal for WriteGroup {
        fn username(&self) -> &'static str {
            "writer"
        }

        fn groups(&self) -> &[&str] {
            &["read", "write"]
        }
    }

    /// Every caller may call a read-only tool, including a request with no principal.
    #[tokio::test]
    async fn any_caller_may_call_a_read_only_tool() {
        authorize("t", ToolAccess::ReadOnly, None)
            .await
            .expect("no principal");
        as_caller(read_only(), authorize("t", ToolAccess::ReadOnly, None))
            .await
            .expect("read-only principal");
        as_caller(read_write(), authorize("t", ToolAccess::ReadOnly, None))
            .await
            .expect("read-write principal");
    }

    /// A read-write tool needs a principal with write access: `read_write` or `write`. A
    /// read-only principal and a request with no principal are refused, each with a message
    /// that names the tool and says how to get write access.
    #[tokio::test]
    async fn only_a_principal_with_write_access_may_call_a_read_write_tool() {
        as_caller(read_write(), authorize("t", ToolAccess::ReadWrite, None))
            .await
            .expect("read_write group");
        let writer: AuthPrincipalRef = Arc::new(WriteGroup);
        as_caller(writer, authorize("t", ToolAccess::ReadWrite, None))
            .await
            .expect("write group");

        let error = as_caller(
            read_only(),
            authorize("send_alert", ToolAccess::ReadWrite, None),
        )
        .await
        .expect_err("a read-only principal is refused");
        assert_eq!(
            error.to_string(),
            "Failed to call tool 'send_alert': the tool can change data or act outside Spice, and the credentials on this request do not allow write access. Retry with a read-write API key (a `runtime.auth.api-key.keys` entry ending in `:rw`). See https://spiceai.org/docs/api/auth"
        );

        let error = authorize("send_alert", ToolAccess::ReadWrite, None)
            .await
            .expect_err("a request with no principal is refused");
        assert_eq!(
            error.to_string(),
            "Failed to call tool 'send_alert': the tool can change data or act outside Spice, and the request has no credentials. Configure `runtime.auth` with a read-write API key (a `runtime.auth.api-key.keys` entry ending in `:rw`) and send it with the request. See https://spiceai.org/docs/api/auth"
        );
    }

    /// On a runtime that authenticates no caller (no `runtime.auth`), a request with no
    /// principal may call a read-write tool, as it may write through `/v1/sql`; a read-only
    /// principal still may not. On a runtime that authenticates callers it may not.
    #[test]
    fn a_request_with_no_principal_follows_how_the_runtime_authenticates() {
        decide("send_alert", ToolAccess::ReadWrite, None, false)
            .expect("no principal on a runtime without authentication");
        assert!(matches!(
            decide("send_alert", ToolAccess::ReadWrite, None, true),
            Err(ToolCallDenied::NoCredentials { .. })
        ));
        assert!(matches!(
            decide(
                "send_alert",
                ToolAccess::ReadWrite,
                Some(&read_only()),
                false
            ),
            Err(ToolCallDenied::ReadOnlyCredentials { .. })
        ));
        decide(
            "send_alert",
            ToolAccess::ReadWrite,
            Some(&read_write()),
            true,
        )
        .expect("read-write principal");
        decide("sql", ToolAccess::ReadOnly, None, true).expect("read-only tool");
    }

    /// `caller_can_write`, which the `sql` tool runs its query by, needs a principal with write
    /// access on every runtime: with none, the query is read-only.
    #[tokio::test]
    async fn caller_can_write_only_with_write_access() {
        assert!(!caller_can_write().await, "no principal");
        assert!(!as_caller(read_only(), caller_can_write()).await);
        assert!(as_caller(read_write(), caller_can_write()).await);
    }
}
