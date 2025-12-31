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

use std::{sync::Arc, time::Duration};

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

/// Initial delay before first retry (doubles with each attempt).
pub const PAGE_RETRY_INITIAL_DELAY: Duration = Duration::from_secs(1);

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
/// - HTTP 502 Bad Gateway
/// - HTTP 503 Service Unavailable
/// - HTTP 504 Gateway Timeout
/// - HTTP 408 Request Timeout
/// - Connection/timeout errors from reqwest
/// - JSON decode errors (often due to truncated responses from timeouts)
#[must_use]
pub fn is_retriable_error(error: &Error) -> bool {
    match error {
        Error::InvalidReqwestStatus { status, .. } => matches!(
            *status,
            StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
                | StatusCode::REQUEST_TIMEOUT
        ),
        Error::JsonDecodeError { status, .. } => {
            // JSON decode errors with server error status codes are often due to
            // truncated responses from timeouts or server issues
            status.is_server_error()
        }
        Error::ReqwestInternal { source } => {
            source.is_timeout() || source.is_connect() || source.is_request()
        }
        Error::RateLimited { .. } => true,
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

    #[test]
    fn test_is_retriable_error_http_502() {
        let error = Error::InvalidReqwestStatus {
            status: StatusCode::BAD_GATEWAY,
            message: "Bad Gateway".to_string(),
        };
        assert!(is_retriable_error(&error));
    }

    #[test]
    fn test_is_retriable_error_http_503() {
        let error = Error::InvalidReqwestStatus {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "Service Unavailable".to_string(),
        };
        assert!(is_retriable_error(&error));
    }

    #[test]
    fn test_is_retriable_error_http_504() {
        let error = Error::InvalidReqwestStatus {
            status: StatusCode::GATEWAY_TIMEOUT,
            message: "Gateway Timeout".to_string(),
        };
        assert!(is_retriable_error(&error));
    }

    #[test]
    fn test_is_retriable_error_http_408() {
        let error = Error::InvalidReqwestStatus {
            status: StatusCode::REQUEST_TIMEOUT,
            message: "Request Timeout".to_string(),
        };
        assert!(is_retriable_error(&error));
    }

    #[test]
    fn test_is_retriable_error_json_decode_server_error() {
        let error = Error::JsonDecodeError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: "expected value at line 1 column 1".to_string(),
            response_preview: "<html>...".to_string(),
        };
        assert!(is_retriable_error(&error));
    }

    #[test]
    fn test_is_retriable_error_json_decode_gateway_timeout() {
        let error = Error::JsonDecodeError {
            status: StatusCode::GATEWAY_TIMEOUT,
            error: "expected value at line 1 column 1".to_string(),
            response_preview: "<html>...".to_string(),
        };
        assert!(is_retriable_error(&error));
    }

    #[test]
    fn test_is_retriable_error_rate_limited() {
        let error = Error::RateLimited {
            message: "Rate limit exceeded".to_string(),
        };
        assert!(is_retriable_error(&error));
    }

    #[test]
    fn test_is_not_retriable_error_http_400() {
        let error = Error::InvalidReqwestStatus {
            status: StatusCode::BAD_REQUEST,
            message: "Bad Request".to_string(),
        };
        assert!(!is_retriable_error(&error));
    }

    #[test]
    fn test_is_not_retriable_error_http_401() {
        let error = Error::InvalidCredentialsOrPermissions {
            message: "Unauthorized".to_string(),
        };
        assert!(!is_retriable_error(&error));
    }

    #[test]
    fn test_is_not_retriable_error_http_403() {
        let error = Error::InvalidCredentialsOrPermissions {
            message: "Forbidden".to_string(),
        };
        assert!(!is_retriable_error(&error));
    }

    #[test]
    fn test_is_not_retriable_error_http_404() {
        let error = Error::ResourceNotFound {
            message: "Not Found".to_string(),
        };
        assert!(!is_retriable_error(&error));
    }

    #[test]
    fn test_is_not_retriable_error_invalid_query() {
        let error = Error::InvalidGraphQLQuery {
            message: "Syntax error".to_string(),
            line: 1,
            column: 5,
            query: "{ invalid }".to_string(),
        };
        assert!(!is_retriable_error(&error));
    }

    #[test]
    fn test_is_not_retriable_error_json_decode_client_error() {
        // JSON decode error with client status code (e.g., 400) should not be retriable
        let error = Error::JsonDecodeError {
            status: StatusCode::BAD_REQUEST,
            error: "expected value at line 1 column 1".to_string(),
            response_preview: "invalid".to_string(),
        };
        assert!(!is_retriable_error(&error));
    }

    #[test]
    fn test_page_retry_constants() {
        // Verify the constants are reasonable
        assert_eq!(PAGE_RETRY_MAX_ATTEMPTS, 3);
        assert_eq!(PAGE_RETRY_INITIAL_DELAY, Duration::from_secs(1));

        // Verify exponential backoff produces expected delays
        let delay_1 = PAGE_RETRY_INITIAL_DELAY * 2u32.pow(0); // 1s
        let delay_2 = PAGE_RETRY_INITIAL_DELAY * 2u32.pow(1); // 2s
        let delay_3 = PAGE_RETRY_INITIAL_DELAY * 2u32.pow(2); // 4s

        assert_eq!(delay_1, Duration::from_secs(1));
        assert_eq!(delay_2, Duration::from_secs(2));
        assert_eq!(delay_3, Duration::from_secs(4));
    }

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
    fn test_retry_behavior_classification() {
        // Test comprehensive classification of errors
        let retriable_errors = vec![
            Error::InvalidReqwestStatus {
                status: StatusCode::BAD_GATEWAY,
                message: "Bad Gateway".to_string(),
            },
            Error::InvalidReqwestStatus {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "Service Unavailable".to_string(),
            },
            Error::InvalidReqwestStatus {
                status: StatusCode::GATEWAY_TIMEOUT,
                message: "Gateway Timeout".to_string(),
            },
            Error::InvalidReqwestStatus {
                status: StatusCode::REQUEST_TIMEOUT,
                message: "Request Timeout".to_string(),
            },
            Error::JsonDecodeError {
                status: StatusCode::GATEWAY_TIMEOUT,
                error: "parse error".to_string(),
                response_preview: String::new(),
            },
            Error::RateLimited {
                message: "Rate limit exceeded".to_string(),
            },
        ];

        let non_retriable_errors = vec![
            Error::InvalidReqwestStatus {
                status: StatusCode::BAD_REQUEST,
                message: "Bad Request".to_string(),
            },
            Error::InvalidReqwestStatus {
                status: StatusCode::UNAUTHORIZED,
                message: "Unauthorized".to_string(),
            },
            Error::InvalidReqwestStatus {
                status: StatusCode::FORBIDDEN,
                message: "Forbidden".to_string(),
            },
            Error::InvalidReqwestStatus {
                status: StatusCode::NOT_FOUND,
                message: "Not Found".to_string(),
            },
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

        for error in &retriable_errors {
            assert!(
                is_retriable_error(error),
                "Error should be retriable: {:?}",
                error
            );
        }

        for error in &non_retriable_errors {
            assert!(
                !is_retriable_error(error),
                "Error should NOT be retriable: {:?}",
                error
            );
        }
    }

    #[test]
    fn test_exponential_backoff_calculation() {
        // Verify the exponential backoff formula used in execute_with_retry
        // delay = PAGE_RETRY_INITIAL_DELAY * 2^(attempt - 1)

        // Attempt 1: 1s * 2^0 = 1s
        let attempt_1_delay = PAGE_RETRY_INITIAL_DELAY * 2u32.pow(1 - 1);
        assert_eq!(attempt_1_delay, Duration::from_secs(1));

        // Attempt 2: 1s * 2^1 = 2s
        let attempt_2_delay = PAGE_RETRY_INITIAL_DELAY * 2u32.pow(2 - 1);
        assert_eq!(attempt_2_delay, Duration::from_secs(2));

        // Attempt 3: 1s * 2^2 = 4s (this would be the last retry before giving up)
        let attempt_3_delay = PAGE_RETRY_INITIAL_DELAY * 2u32.pow(3 - 1);
        assert_eq!(attempt_3_delay, Duration::from_secs(4));

        // Total maximum wait time for retries: 1 + 2 = 3s (we don't wait after the last attempt)
        let total_wait_time = attempt_1_delay + attempt_2_delay;
        assert_eq!(total_wait_time, Duration::from_secs(3));
    }

    #[test]
    fn test_max_attempts_boundary() {
        // Verify that PAGE_RETRY_MAX_ATTEMPTS is used correctly
        // With MAX_ATTEMPTS = 3:
        // - Attempt 1: initial try
        // - Attempt 2: first retry (if attempt 1 < 3)
        // - Attempt 3: second retry (if attempt 2 < 3), then give up

        // Test using runtime values to avoid constant assertion warnings
        let max_attempts = PAGE_RETRY_MAX_ATTEMPTS;
        assert!(1 < max_attempts, "Attempt 1 should allow retry");
        assert!(2 < max_attempts, "Attempt 2 should allow retry");
        assert!(
            !(3 < max_attempts),
            "Attempt 3 should NOT allow retry (max reached)"
        );
    }
}
