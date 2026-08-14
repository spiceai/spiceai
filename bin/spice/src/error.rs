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

//! Error types for the Spice CLI.

use reqwest::StatusCode;
use snafu::Snafu;
use std::path::PathBuf;

/// Result type alias for the Spice CLI.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Stable machine-readable codes for Spice Cloud failures.
///
/// Scripts and agents branch on these, so the strings are part of the CLI's
/// contract: rename one only with the same care as renaming a flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudErrorCode {
    /// No Spice Cloud credential is available locally.
    NotAuthenticated,
    /// The credential was rejected (401) — expired, revoked, or malformed.
    TokenExpired,
    /// Authenticated, but the identity may not perform this action (403).
    Forbidden,
    /// The named organization does not exist, or is invisible to this identity.
    OrgNotFound,
    /// The identity is not a member of the requested organization.
    OrgForbidden,
    /// Two explicit signals named different organizations.
    OrgConflict,
    /// An organization was named, but no credential is bound to it.
    OrgCredentialMissing,
    /// No project by that name exists in the organization being acted on.
    ProjectNotFound,
    /// The project exists, but under a different organization than the active one.
    WrongOrg,
    /// A deployment is already in flight for this app.
    DeployConflict,
    /// A deployment reached a terminal failed status.
    DeployFailed,
    /// A deployment did not reach a terminal status before the wait elapsed.
    /// Distinct from [`Self::DeployFailed`]: the deployment may still succeed.
    DeployTimeout,
    /// The requested resource does not exist.
    NotFound,
    /// The request conflicts with the resource's current state.
    Conflict,
    /// The Spice Cloud API returned an unexpected response.
    ApiError,
    /// The command's arguments are inconsistent or unusable.
    InvalidRequest,
}

impl CloudErrorCode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotAuthenticated => "not_authenticated",
            Self::TokenExpired => "token_expired",
            Self::Forbidden => "forbidden",
            Self::OrgNotFound => "org_not_found",
            Self::OrgForbidden => "org_forbidden",
            Self::OrgConflict => "org_conflict",
            Self::OrgCredentialMissing => "org_credential_missing",
            Self::ProjectNotFound => "project_not_found",
            Self::WrongOrg => "wrong_org",
            Self::DeployConflict => "deploy_conflict",
            Self::DeployFailed => "deploy_failed",
            Self::DeployTimeout => "deploy_timeout",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::ApiError => "api_error",
            Self::InvalidRequest => "invalid_request",
        }
    }
}

impl std::fmt::Display for CloudErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error types for the Spice CLI.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    /// Runtime is not installed
    #[snafu(display("The Spice runtime is not installed. Run 'spice install' to install it."))]
    RuntimeNotInstalled,

    /// Native Windows runtime execution is unsupported
    #[snafu(display(
        "Native Windows local runtime install and run are not supported. Open WSL and run the Linux Spice CLI there instead."
    ))]
    WindowsNativeRuntimeUnsupported,

    /// Runtime is not running
    #[snafu(display("The Spice runtime is unavailable at {endpoint}. Is it running?"))]
    RuntimeUnavailable { endpoint: String },

    /// Unauthorized access to the runtime API (401 — credentials missing/invalid)
    #[snafu(display(
        "unauthorized: invalid or missing Spice API key. Run `spice login` or set SPICE_API_KEY."
    ))]
    Unauthorized,

    /// Forbidden (403 — authenticated but not allowed)
    #[snafu(display(
        "forbidden: your API key is valid but does not have permission for this resource. Check API key scopes or contact your org admin."
    ))]
    PermissionDenied,

    /// Runtime returned an unexpected HTTP status
    #[snafu(display("request to runtime failed with HTTP {status}: {body}"))]
    RuntimeHttp { status: u16, body: String },

    /// Failed to connect to the runtime
    #[snafu(display("Failed to connect to runtime at {endpoint}: {source}"))]
    ConnectionFailed {
        endpoint: String,
        source: reqwest::Error,
    },

    /// HTTP request failed
    #[snafu(display("HTTP request failed: {source}"))]
    HttpRequestFailed { source: reqwest::Error },

    /// Invalid HTTP response
    #[snafu(display("Invalid HTTP response: {message}"))]
    InvalidResponse { message: String },

    /// A response the runtime reported as successful did not arrive in full
    #[snafu(display(
        "Failed to read the response from {endpoint}: {source}. The request was accepted, so the result may be incomplete rather than empty -- check the runtime's logs, then retry. See: https://spiceai.org/docs/api"
    ))]
    ResponseIncomplete {
        endpoint: String,
        source: reqwest::Error,
    },

    /// A Spicepod registry failed to serve a pod. The message is already fully formed, so it is
    /// displayed verbatim rather than blamed on the user's argument.
    #[snafu(display("{message}"))]
    Registry { message: String },

    /// Failed to read/write configuration
    #[snafu(display("Failed to {operation} configuration at {}: {source}", path.display()))]
    ConfigIo {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },

    /// Failed to parse configuration
    #[snafu(display("Failed to parse configuration: {message}"))]
    ConfigParse { message: String },

    /// Failed to create directory
    #[snafu(display("Failed to create directory {}: {source}", path.display()))]
    CreateDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    /// Failed to execute runtime command
    #[snafu(display("Failed to execute runtime: {source}"))]
    RuntimeExecution { source: std::io::Error },

    /// Failed to get runtime version
    #[snafu(display("Failed to get runtime version: {message}"))]
    RuntimeVersion { message: String },

    /// Environment variable error
    #[snafu(display("Environment variable error: {message}"))]
    Environment { message: String },

    /// Invalid argument
    #[snafu(display("Invalid argument: {message}"))]
    InvalidArgument { message: String },

    /// Invalid command usage that should use Clap's usage-error exit code.
    #[snafu(display("Invalid argument: {message}"))]
    InvalidUsage { message: String },

    /// A Spice Cloud operation failed, carrying a stable code scripts can branch on.
    #[snafu(display(
        "{message}{}",
        hint.as_deref().map(|hint| format!(" {hint}")).unwrap_or_default()
    ))]
    Cloud {
        code: CloudErrorCode,
        message: String,
        /// Actionable next step, appended to the message for humans and kept
        /// separate for `--machine` output.
        hint: Option<String>,
    },

    /// User denied device authorization during cloud login.
    #[snafu(display("Device authorization was denied"))]
    DeviceAuthorizationDenied,

    /// Home directory not found
    #[snafu(display(
        "Could not determine home directory. Set HOME (Unix) or USERPROFILE (Windows) environment variable."
    ))]
    HomeDirectoryNotFound,

    /// The HTTP client could not be built.
    ///
    /// Not recoverable by falling back to a default client: the built one carries the
    /// same-origin redirect policy that keeps the API key from being forwarded off
    /// origin, and a default client would not (#12495).
    #[snafu(display("Could not build the HTTP client: {source}"))]
    HttpClientBuild { source: reqwest::Error },

    /// REPL error
    #[snafu(display("SQL REPL error: {message}"))]
    Repl { message: String },

    /// Failed to get child process ID
    #[snafu(display("Failed to get child process ID"))]
    ChildProcessId,

    /// Failed to register signal handler
    #[snafu(display("Failed to register signal handler: {source}"))]
    SignalHandler { source: std::io::Error },

    /// Model not found
    #[snafu(display("Model '{model}' not found. Available models: {available}"))]
    ModelNotFound { model: String, available: String },

    /// No models configured
    #[snafu(display("No models found. Please configure a model in your Spicepod."))]
    NoModelsConfigured,

    /// Local I/O failure for the Cloud Connect flow.
    #[snafu(display("Cloud Connect I/O error: {message}"))]
    CloudConnectIo { message: String },

    /// Enrollment against the Spice Cloud control plane failed.
    #[snafu(display("Failed to enroll with Spice Cloud: {message}"))]
    CloudConnectEnroll { message: String },

    /// A `spice connect service` action needs a service that is not installed
    /// for the selected instance directory.
    ///
    /// Distinct from a generic failure because nothing went wrong: the request
    /// cannot be carried out as asked, which is what exit code
    /// [`Error::USAGE_EXIT_CODE`] means.
    #[snafu(display("{message}"))]
    ServiceNotInstalled { message: String },

    /// The service supervisor could not be asked, or reports the service as
    /// failed. The status report has already been written; this carries the
    /// diagnosis and the non-zero exit.
    #[snafu(display("{message}"))]
    ServiceUnavailable { message: String },

    /// The viewer was interrupted — `spice connect service logs --follow`
    /// stopped following. The service is unchanged.
    #[snafu(display("Interrupted."))]
    Interrupted,

    /// A callable path that this release does not implement yet.
    ///
    /// A typed error rather than a panic, so the CLI exits with a diagnosis and
    /// a non-zero status instead of aborting.
    #[snafu(display("{message}"))]
    NotImplemented { message: String },

    /// Atomic Cloud Connect project creation/attachment failed.
    #[snafu(display("Failed to create and attach the Spice Cloud project: {message}"))]
    CloudConnectProject { message: String },
}

impl Error {
    /// Build a Spice Cloud error carrying a stable machine code.
    #[must_use]
    pub fn cloud(code: CloudErrorCode, message: impl Into<String>) -> Self {
        Self::Cloud {
            code,
            message: message.into(),
            hint: None,
        }
    }

    /// Build a Spice Cloud error with a stable machine code and a next step.
    #[must_use]
    pub fn cloud_with_hint(
        code: CloudErrorCode,
        message: impl Into<String>,
        hint: impl Into<String>,
    ) -> Self {
        Self::Cloud {
            code,
            message: message.into(),
            hint: Some(hint.into()),
        }
    }

    /// The Spice Cloud code for this error, if it is a cloud failure.
    #[must_use]
    pub fn cloud_code(&self) -> Option<CloudErrorCode> {
        match self {
            Self::Cloud { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// The actionable hint attached to this error, if any.
    #[must_use]
    pub fn hint(&self) -> Option<&str> {
        match self {
            Self::Cloud { hint, .. } => hint.as_deref(),
            _ => None,
        }
    }

    /// The operation failed. The default for anything without a more specific
    /// meaning.
    pub const FAILURE_EXIT_CODE: i32 = 1;

    /// The request cannot be carried out as asked and the caller has to change
    /// something. The same code clap uses for a usage error.
    pub const USAGE_EXIT_CODE: i32 = 2;

    /// Re-authenticate and retry, matching the convention `gh` uses
    /// (<https://cli.github.com/manual/gh_help_exit-codes>).
    pub const AUTH_EXIT_CODE: i32 = 4;

    /// Interrupted by a signal: `128 + SIGINT`, the shell convention.
    pub const INTERRUPTED_EXIT_CODE: i32 = 130;

    /// The process exit code for this error.
    ///
    /// The contract automation branches on:
    ///
    /// - `0` — the command did what it was asked (never produced here).
    /// - `1` — the operation failed.
    /// - `2` — the request was invalid; change something and retry.
    /// - `4` — authenticate again and retry.
    /// - `130` — interrupted; nothing was changed.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        if matches!(
            self.cloud_code(),
            Some(
                CloudErrorCode::NotAuthenticated
                    | CloudErrorCode::TokenExpired
                    | CloudErrorCode::OrgCredentialMissing,
            )
        ) {
            return Self::AUTH_EXIT_CODE;
        }
        match self {
            Self::Unauthorized => Self::AUTH_EXIT_CODE,
            // Both are refusals of the request rather than failures of an
            // attempt: the caller has to change something. `InvalidArgument`
            // covers argument conflicts, preflight refusals, and manifest
            // validation, so it belongs on the same code clap already uses for
            // a usage error — otherwise automation cannot tell bad input from
            // an operation that tried and failed.
            Self::InvalidArgument { .. }
            | Self::InvalidUsage { .. }
            | Self::ServiceNotInstalled { .. } => Self::USAGE_EXIT_CODE,
            Self::Interrupted => Self::INTERRUPTED_EXIT_CODE,
            _ => Self::FAILURE_EXIT_CODE,
        }
    }
}

/// Check an HTTP response status and return an appropriate error for non-success responses.
///
/// Returns `Ok(response)` if the status is successful (2xx).
/// Returns `Err(Unauthorized)` for 401 responses.
/// Returns `Err(RuntimeHttpError)` for other non-success responses.
/// Returns `Err(RuntimeUnavailable)` for 403 responses that indicate API key issues.
pub async fn check_response(
    response: reqwest::Response,
    endpoint: &str,
) -> Result<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status();
    if status == StatusCode::UNAUTHORIZED {
        return Err(UnauthorizedSnafu.build());
    }

    if status == StatusCode::FORBIDDEN {
        return Err(PermissionDeniedSnafu.build());
    }

    // For connection-related errors or 502/503/504, report the runtime as unavailable
    if status == StatusCode::BAD_GATEWAY
        || status == StatusCode::SERVICE_UNAVAILABLE
        || status == StatusCode::GATEWAY_TIMEOUT
    {
        return Err(RuntimeUnavailableSnafu {
            endpoint: endpoint.to_string(),
        }
        .build());
    }

    let status_code = status.as_u16();
    let body = response.text().await.unwrap_or_default();
    Err(RuntimeHttpSnafu {
        status: status_code,
        body,
    }
    .build())
}

/// Read a response's status and body, distinguishing a body that failed to arrive from one
/// that was empty.
///
/// On a non-success status the body only decorates a message that already reports the
/// failure, so a read error there yields an empty string rather than replacing the status
/// the caller is about to report. On a success status the body *is* the result: a read that
/// stopped part-way — a deadline firing mid-response, a reset connection, a truncated
/// response — must be reported rather than defaulted to an empty success.
pub async fn read_response(
    response: reqwest::Response,
    endpoint: &str,
) -> Result<(StatusCode, String)> {
    let status = response.status();
    match response.text().await {
        Ok(body) => Ok((status, body)),
        Err(_) if !status.is_success() => Ok((status, String::new())),
        Err(source) => Err(Error::ResponseIncomplete {
            endpoint: endpoint.to_string(),
            source,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A response whose body fails part-way through, under the caller's choice of status.
    fn response_with_unreadable_body(status: StatusCode) -> reqwest::Response {
        let failure = std::io::Error::other("the connection went away mid-body");
        let body = reqwest::Body::wrap_stream(futures::stream::once(async {
            Err::<Vec<u8>, std::io::Error>(failure)
        }));

        let response = http::Response::builder()
            .status(status)
            .body(body)
            .expect("a response with a status and a body is well-formed");

        reqwest::Response::from(response)
    }

    /// A body that stopped part-way through is not an empty result: reading it with
    /// `unwrap_or_default` reported an empty success, which is
    /// <https://github.com/spiceai/spiceai/issues/12587>.
    #[tokio::test]
    async fn a_body_that_fails_on_a_success_status_is_an_error() {
        let response = response_with_unreadable_body(StatusCode::OK);

        let error = read_response(response, "http://runtime/v1/nsql")
            .await
            .expect_err("a body that did not arrive is not a result");

        let message = error.to_string();
        assert!(
            message.contains("http://runtime/v1/nsql"),
            "the error should name the endpoint, got: {message}"
        );
        assert!(
            matches!(error, Error::ResponseIncomplete { .. }),
            "expected an incomplete-response error, got: {message}"
        );
    }

    /// On a failing status the body is only decoration for a message that already reports the
    /// failure, so a body that could not be read must not replace the status with a transport
    /// error — the caller still has to be able to say what the server answered.
    #[tokio::test]
    async fn a_body_that_fails_on_an_error_status_keeps_the_status() {
        let response = response_with_unreadable_body(StatusCode::INTERNAL_SERVER_ERROR);

        let (status, body) = read_response(response, "http://runtime/v1/nsql")
            .await
            .expect("the status is the result here, not the body");

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body, "");
    }

    #[tokio::test]
    async fn a_body_that_arrives_is_returned_with_its_status() {
        let response = reqwest::Response::from(
            http::Response::builder()
                .status(StatusCode::OK)
                .body("SELECT 1")
                .expect("a response with a status and a body is well-formed"),
        );

        let (status, body) = read_response(response, "http://runtime/v1/nsql")
            .await
            .expect("a body that arrived in full is a result");

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "SELECT 1");
    }

    #[test]
    fn test_unauthorized_error_message() {
        let err = UnauthorizedSnafu.build();
        assert_eq!(
            err.to_string(),
            "unauthorized: invalid or missing Spice API key. Run `spice login` or set SPICE_API_KEY."
        );
    }

    #[test]
    fn cloud_error_appends_hint_to_the_message_on_one_line() {
        let err = Error::cloud_with_hint(
            CloudErrorCode::ProjectNotFound,
            "App 'team-app' not found in org 'lukekim'.",
            "Run 'spice cloud orgs' to see the orgs you can access.",
        );

        let rendered = err.to_string();
        assert_eq!(
            rendered,
            "App 'team-app' not found in org 'lukekim'. Run 'spice cloud orgs' to see the orgs you can access."
        );
        assert!(!rendered.contains('\n'), "error messages stay on one line");
        assert_eq!(err.cloud_code(), Some(CloudErrorCode::ProjectNotFound));
    }

    #[test]
    fn cloud_error_without_hint_renders_only_the_message() {
        let err = Error::cloud(CloudErrorCode::TokenExpired, "Unauthorized: token expired.");
        assert_eq!(err.to_string(), "Unauthorized: token expired.");
        assert!(err.hint().is_none());
    }

    #[test]
    fn cloud_error_codes_are_stable_strings() {
        // Scripts and agents branch on these; changing one is a breaking change.
        assert_eq!(
            CloudErrorCode::NotAuthenticated.as_str(),
            "not_authenticated"
        );
        assert_eq!(CloudErrorCode::TokenExpired.as_str(), "token_expired");
        assert_eq!(CloudErrorCode::OrgNotFound.as_str(), "org_not_found");
        assert_eq!(CloudErrorCode::OrgForbidden.as_str(), "org_forbidden");
        assert_eq!(
            CloudErrorCode::ProjectNotFound.as_str(),
            "project_not_found"
        );
        assert_eq!(CloudErrorCode::WrongOrg.as_str(), "wrong_org");
        assert_eq!(CloudErrorCode::DeployConflict.as_str(), "deploy_conflict");
        assert_eq!(CloudErrorCode::DeployFailed.as_str(), "deploy_failed");
    }

    #[test]
    fn test_runtime_http_error_message() {
        let err: Error = RuntimeHttpSnafu {
            status: 404_u16,
            body: "not found".to_string(),
        }
        .build();
        assert_eq!(
            err.to_string(),
            "request to runtime failed with HTTP 404: not found"
        );
    }

    #[test]
    fn test_runtime_unavailable_error_message() {
        let err: Error = RuntimeUnavailableSnafu {
            endpoint: "http://127.0.0.1:8090".to_string(),
        }
        .build();
        assert_eq!(
            err.to_string(),
            "The Spice runtime is unavailable at http://127.0.0.1:8090. Is it running?"
        );
    }

    #[tokio::test]
    async fn test_check_response_success() {
        let mock_server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&mock_server)
            .await;

        let response = reqwest::get(mock_server.uri())
            .await
            .expect("GET request should succeed");
        let result = check_response(response, &mock_server.uri()).await;
        assert!(result.is_ok(), "expected successful response: {result:?}");
    }

    #[tokio::test]
    async fn test_check_response_unauthorized() {
        let mock_server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(401))
            .mount(&mock_server)
            .await;

        let response = reqwest::get(mock_server.uri())
            .await
            .expect("GET request should succeed");
        let result = check_response(response, &mock_server.uri()).await;
        let Err(err) = result else {
            panic!("expected Unauthorized error for 401 response");
        };
        assert!(
            matches!(err, Error::Unauthorized),
            "Expected Unauthorized, got: {err}"
        );
        assert_eq!(
            err.to_string(),
            "unauthorized: invalid or missing Spice API key. Run `spice login` or set SPICE_API_KEY."
        );
    }

    #[tokio::test]
    async fn test_check_response_forbidden() {
        let mock_server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(403))
            .mount(&mock_server)
            .await;

        let response = reqwest::get(mock_server.uri())
            .await
            .expect("GET request should succeed");
        let result = check_response(response, &mock_server.uri()).await;
        let Err(err) = result else {
            panic!("expected PermissionDenied error for 403 response");
        };
        assert!(
            matches!(err, Error::PermissionDenied),
            "Expected PermissionDenied for 403, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_check_response_service_unavailable() {
        let mock_server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(503))
            .mount(&mock_server)
            .await;

        let response = reqwest::get(mock_server.uri())
            .await
            .expect("GET request should succeed");
        let result = check_response(response, &mock_server.uri()).await;
        let Err(err) = result else {
            panic!("expected RuntimeUnavailable error for 503 response");
        };
        assert!(
            matches!(err, Error::RuntimeUnavailable { .. }),
            "Expected RuntimeUnavailable for 503, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_check_response_not_found() {
        let mock_server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(404).set_body_string("resource not found"),
            )
            .mount(&mock_server)
            .await;

        let response = reqwest::get(mock_server.uri())
            .await
            .expect("GET request should succeed");
        let result = check_response(response, &mock_server.uri()).await;
        let Err(err) = result else {
            panic!("expected RuntimeHttp error for 404 response");
        };
        assert!(
            matches!(err, Error::RuntimeHttp { status: 404, .. }),
            "Expected RuntimeHttp with status 404, got: {err}"
        );
        assert!(
            err.to_string().contains("resource not found"),
            "Error should contain response body, got: {err}"
        );
    }
}
