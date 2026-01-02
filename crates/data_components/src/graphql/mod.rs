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

use std::sync::Arc;

use arrow::error::ArrowError;
use client::GraphQLQuery;
use datafusion::{logical_expr::TableProviderFilterPushDown, prelude::Expr};
use http::{HeaderMap, HeaderValue};
use reqwest::StatusCode;
use serde_json::Value;
use snafu::Snafu;

pub mod builder;
pub mod client;
pub mod provider;
pub mod rate_limit;

/// Maximum number of retry attempts for a single page fetch during pagination.
pub const PAGE_RETRY_MAX_ATTEMPTS: u32 = 3;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    ReqwestInternal { source: reqwest::Error },

    #[snafu(display("HTTP {status}: {message}"))]
    InvalidReqwestStatus {
        status: reqwest::StatusCode,
        message: String,
    },

    #[snafu(display("JSON pointer could not be inferred, and none provided"))]
    NoJsonPointerFound {},

    #[snafu(display("Invalid GraphQL 'json_pointer': '{pointer}'"))]
    InvalidJsonPointer { pointer: String },

    #[snafu(display("{source}"))]
    ArrowInternal { source: ArrowError },

    #[snafu(display("Invalid object access. {message}"))]
    InvalidObjectAccess { message: String },

    #[snafu(display("{message}"))]
    InvalidCredentialsOrPermissions { message: String },

    #[snafu(display("{message}"))]
    ResourceNotFound { message: String },

    #[snafu(display("{message}"))]
    RateLimited { message: String },

    #[snafu(display("Query response transformation failed. {source}"))]
    ResultTransformError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "The API returned an invalid response (HTTP {status}). This may indicate a temporary server issue. The data refresh will be retried automatically. If the problem persists, contact support. Technical details: {error}"
    ))]
    JsonDecodeError {
        status: reqwest::StatusCode,
        error: String,
        response_preview: String,
    },

    #[snafu(display(
        "Internal error: {message}. Report a bug at https://github.com/spiceai/spiceai/issues."
    ))]
    InternalError { message: String },

    #[snafu(display("Server returned an error: {message}"))]
    InvalidGraphQLQuery {
        message: String,
        line: usize,
        column: usize,
        query: String,
    },

    #[snafu(display(
        "Failed to build a valid regex from pagination parameters due to the resource name {resource_name}. {source}"
    ))]
    InvalidPaginationRegex {
        source: regex::Error,
        resource_name: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Determines if a GraphQL error is retriable (transient).
///
/// Retriable errors include:
/// - All HTTP 5xx server errors (500, 502, 503, 504, etc.)
/// - HTTP 408 Request Timeout
/// - Connection/timeout errors from reqwest
/// - JSON decode errors (often due to truncated responses from timeouts)
///
/// Note: `Error::RateLimited` is NOT retriable here because rate limiting is handled
/// proactively by the `RateLimiter` trait via `check_rate_limit()`, which sleeps until
/// the rate limit reset time. Any `RateLimited` error reaching this point indicates
/// an unexpected issue that shouldn't be retried with additional backoff delays.
#[must_use]
pub fn is_retriable_error(error: &Error) -> bool {
    match error {
        Error::InvalidReqwestStatus { status, .. } => {
            status.is_server_error() || *status == StatusCode::REQUEST_TIMEOUT
        }
        Error::JsonDecodeError { status, .. } => {
            // JSON decode errors with server error status codes are often due to
            // truncated responses from timeouts or server issues
            status.is_server_error()
        }
        Error::ReqwestInternal { source } => {
            // Check for transient network/connection errors:
            // - is_timeout(): Connection or request timeouts
            // - is_connect(): Failed to establish connection
            // - is_body(): Error reading response body
            // - is_decode(): Error decoding response body (e.g., gzip/brotli decompression
            //   failures, HTTP/2 stream errors - "error decoding response body")
            source.is_timeout()
                || source.is_connect()
                || source.is_body()
                || source.is_decode()
                // Also check if the underlying status code is a retriable server error
                || source.status().is_some_and(|s| s.is_server_error() || s == StatusCode::REQUEST_TIMEOUT)
        }
        _ => false,
    }
}

#[derive(Debug, Clone)]
pub struct FilterPushdownResult {
    pub filter_pushdown: TableProviderFilterPushDown,
    pub expr: Expr,
    pub context: Option<String>,
}

pub type ValuePreprocessor = Arc<dyn Fn(&mut Value) -> Result<()>>;
pub type ErrorChecker = Arc<dyn Fn(&HeaderMap<HeaderValue>, &Value) -> Result<()> + Send + Sync>;

/// A trait optionally provided to GraphQL ``TableProvider``s to alter the behavior of filter push down
pub trait GraphQLContext: Send + Sync + std::fmt::Debug {
    /// A function executed for each filter push down requested from the ``TableProvider``
    /// A custom implementation can override this function to implement custom filter pushdown logic
    fn filter_pushdown(
        &self,
        expr: &Expr,
    ) -> Result<FilterPushdownResult, datafusion::error::DataFusionError> {
        Ok(FilterPushdownResult {
            filter_pushdown: TableProviderFilterPushDown::Unsupported,
            expr: expr.clone(),
            context: None,
        })
    }

    /// This function receives the ``FilterPushdownResult``s from the ``filter_pushdown`` function, before execution of the GraphQL query
    /// A custom implementation can override this function to inject parameters for custom filter pushdown into the GraphQL query
    fn inject_parameters(
        &self,
        _filters: &[FilterPushdownResult],
        _query: &mut GraphQLQuery,
    ) -> Result<(), datafusion::error::DataFusionError> {
        Ok(())
    }

    /// Return a function that will receive the headers from the GraphQL response
    /// A custom implementation can override this function to process the headers and response, and return custom errors or warnings
    fn error_checker(&self) -> Option<ErrorChecker> {
        None
    }

    /// If the query has a cost associated with it, return it
    /// This value is only used when a rate controller with a weighted quota is configured.
    /// When query cost is None, only non-weighted quotas are checked.
    fn query_cost(&self) -> Option<u32> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_all_server_errors_retriable_via_json_decode() {
        // All 5xx errors should be retriable when they cause JSON decode failures
        let server_error_codes = [
            StatusCode::INTERNAL_SERVER_ERROR,      // 500
            StatusCode::NOT_IMPLEMENTED,            // 501
            StatusCode::BAD_GATEWAY,                // 502
            StatusCode::SERVICE_UNAVAILABLE,        // 503
            StatusCode::GATEWAY_TIMEOUT,            // 504
            StatusCode::HTTP_VERSION_NOT_SUPPORTED, // 505
        ];

        for status in server_error_codes {
            let error = Error::JsonDecodeError {
                status,
                error: "expected value at line 1 column 1".to_string(),
                response_preview: "<html>Server Error</html>".to_string(),
            };
            assert!(
                is_retriable_error(&error),
                "JsonDecodeError with status {status} should be retriable"
            );
        }
    }

    #[test]
    fn test_all_server_errors_retriable_via_invalid_reqwest_status() {
        // All 5xx errors should be retriable via InvalidReqwestStatus
        let server_error_codes = [
            StatusCode::INTERNAL_SERVER_ERROR,           // 500
            StatusCode::NOT_IMPLEMENTED,                 // 501
            StatusCode::BAD_GATEWAY,                     // 502
            StatusCode::SERVICE_UNAVAILABLE,             // 503
            StatusCode::GATEWAY_TIMEOUT,                 // 504
            StatusCode::HTTP_VERSION_NOT_SUPPORTED,      // 505
            StatusCode::VARIANT_ALSO_NEGOTIATES,         // 506
            StatusCode::INSUFFICIENT_STORAGE,            // 507
            StatusCode::LOOP_DETECTED,                   // 508
            StatusCode::NOT_EXTENDED,                    // 510
            StatusCode::NETWORK_AUTHENTICATION_REQUIRED, // 511
        ];

        for status in server_error_codes {
            let error = Error::InvalidReqwestStatus {
                status,
                message: format!("Server error: {status}"),
            };
            assert!(
                is_retriable_error(&error),
                "InvalidReqwestStatus with status {status} should be retriable"
            );
        }

        // 408 Request Timeout is also retriable (special case, not a 5xx)
        let timeout_error = Error::InvalidReqwestStatus {
            status: StatusCode::REQUEST_TIMEOUT,
            message: "Request Timeout".to_string(),
        };
        assert!(
            is_retriable_error(&timeout_error),
            "408 Request Timeout should be retriable"
        );
    }

    #[test]
    fn test_json_decode_client_error_not_retriable() {
        // JSON decode errors with client status codes (4xx) should NOT be retriable
        let client_error_codes = [
            StatusCode::BAD_REQUEST,          // 400
            StatusCode::UNAUTHORIZED,         // 401
            StatusCode::FORBIDDEN,            // 403
            StatusCode::NOT_FOUND,            // 404
            StatusCode::UNPROCESSABLE_ENTITY, // 422
        ];

        for status in client_error_codes {
            let error = Error::JsonDecodeError {
                status,
                error: "expected value at line 1 column 1".to_string(),
                response_preview: "invalid response".to_string(),
            };
            assert!(
                !is_retriable_error(&error),
                "JsonDecodeError with client status {status} should NOT be retriable"
            );
        }
    }

    #[test]
    fn test_non_status_errors_not_retriable() {
        // Test that non-HTTP-status error types are not retriable
        // (HTTP status errors are covered by the status-specific tests above)
        let non_retriable_errors = vec![
            Error::InvalidCredentialsOrPermissions {
                message: "Invalid credentials".to_string(),
            },
            Error::ResourceNotFound {
                message: "Resource not found".to_string(),
            },
            Error::InvalidGraphQLQuery {
                message: "Syntax error".to_string(),
                line: 1,
                column: 1,
                query: "{ invalid }".to_string(),
            },
            Error::NoJsonPointerFound {},
            Error::InvalidJsonPointer {
                pointer: "/invalid".to_string(),
            },
            Error::InvalidObjectAccess {
                message: "Invalid access".to_string(),
            },
            Error::InternalError {
                message: "Internal error".to_string(),
            },
        ];

        for error in &non_retriable_errors {
            assert!(
                !is_retriable_error(error),
                "Error should NOT be retriable: {error:?}"
            );
        }
    }

    #[test]
    fn test_fibonacci_backoff_with_retry_strategy() {
        use util::fibonacci_backoff::{Backoff, FibonacciBackoffBuilder};

        // Verify the FibonacciBackoff produces expected Fibonacci delays
        // This mirrors the configuration used in execute_with_retry
        // Fibonacci intervals array: [1000, 1000, 2000, 3000, 5000, 8000, ...] (indices 0, 1, 2, 3, ...)
        // next_backoff() increments num_retries first, then uses it as index:
        //   Call 1: num_retries=1, index 1 -> 1000ms
        //   Call 2: num_retries=2, index 2 -> 2000ms
        //   Call 3: num_retries=3, index 3 -> 3000ms
        let mut backoff = FibonacciBackoffBuilder::new()
            .max_retries(Some(PAGE_RETRY_MAX_ATTEMPTS as usize))
            .randomization_factor(0.0) // No randomization for predictable testing
            .build();

        // Call 1: num_retries=1, index 1 -> 1000ms (1s)
        let delay_1 = backoff.next_backoff().expect("should have delay");
        assert_eq!(delay_1, Duration::from_secs(1));

        // Call 2: num_retries=2, index 2 -> 2000ms (2s)
        let delay_2 = backoff.next_backoff().expect("should have delay");
        assert_eq!(delay_2, Duration::from_secs(2));

        // Call 3: num_retries=3, index 3 -> 3000ms (3s)
        let delay_3 = backoff.next_backoff().expect("should have delay");
        assert_eq!(delay_3, Duration::from_secs(3));

        // After max_retries (3), should return None
        assert!(
            backoff.next_backoff().is_none(),
            "Should return None after max retries"
        );
    }

    #[test]
    fn test_max_attempts_boundary() {
        use util::fibonacci_backoff::{Backoff, FibonacciBackoffBuilder};

        // Verify that PAGE_RETRY_MAX_ATTEMPTS is used correctly with FibonacciBackoff.
        // PAGE_RETRY_MAX_ATTEMPTS represents the maximum number of retry attempts,
        // excluding the initial attempt. With PAGE_RETRY_MAX_ATTEMPTS = 3:
        // - Attempt 1: initial try
        // - Attempt 2: first retry (after first failure) - backoff call 1
        // - Attempt 3: second retry (after second failure) - backoff call 2
        // - Attempt 4: third retry (after third failure) - backoff call 3
        // - After attempt 4 fails, backoff returns None, give up

        let mut backoff = FibonacciBackoffBuilder::new()
            .max_retries(Some(PAGE_RETRY_MAX_ATTEMPTS as usize))
            .build();

        // Should allow 3 retries (backoff returns Some 3 times)
        assert!(
            backoff.next_backoff().is_some(),
            "First retry should be allowed"
        );
        assert!(
            backoff.next_backoff().is_some(),
            "Second retry should be allowed"
        );
        assert!(
            backoff.next_backoff().is_some(),
            "Third retry should be allowed"
        );

        // Fourth call should return None (max retries exhausted)
        assert!(
            backoff.next_backoff().is_none(),
            "Fourth retry should NOT be allowed (max retries exhausted)"
        );
    }
}
