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

    /// User denied device authorization during cloud login.
    #[snafu(display("Device authorization was denied"))]
    DeviceAuthorizationDenied,

    /// Home directory not found
    #[snafu(display(
        "Could not determine home directory. Set HOME (Unix) or USERPROFILE (Windows) environment variable."
    ))]
    HomeDirectoryNotFound,

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unauthorized_error_message() {
        let err = UnauthorizedSnafu.build();
        assert_eq!(
            err.to_string(),
            "unauthorized: invalid or missing Spice API key. Run `spice login` or set SPICE_API_KEY."
        );
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
