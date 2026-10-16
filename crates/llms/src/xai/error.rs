/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Reads xAI error bodies, which do not have the `OpenAI` error shape.
//!
//! ```text
//! OpenAI: {"error": {"message": "...", "type": "...", "param": "...", "code": "..."}}
//! xAI:    {"code": "invalid-argument", "error": "Temperature must be less than 2 ..."}
//! ```
//!
//! `async-openai` can only parse the `OpenAI` shape, and it drops the HTTP status. An xAI error
//! therefore became a deserialization failure, which Spice returned as an empty HTTP 500.
//! [`XaiErrorService`] reads the xAI body while the status is still known, and returns an
//! [`ApiError`] whose `type` gives the same HTTP status back to the Spice client.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use async_openai::{
    error::{ApiError, OpenAIError},
    middleware::{HttpRequestFactory, ReqwestService},
};
use reqwest::{Response, StatusCode};
use serde::Deserialize;

/// The error body that xAI returns for a refused request.
#[derive(Debug, Deserialize)]
struct XaiErrorBody {
    /// A gRPC status name, for example `invalid-argument` or `permission-denied`.
    code: String,
    error: String,
}

impl XaiErrorBody {
    fn into_api_error(self, status: StatusCode) -> ApiError {
        ApiError {
            message: self.error,
            r#type: Some(error_type(status).to_string()),
            param: None,
            // `OpenAI` codes are snake case. No gRPC status name is also an `OpenAI` code that
            // `ApiErrorKind` reads, so the HTTP status (through `type`) sets the error kind.
            code: Some(self.code.replace('-', "_")),
        }
    }
}

/// The `OpenAI` error `type` that `ApiErrorKind` maps back to `status`.
fn error_type(status: StatusCode) -> &'static str {
    match status {
        StatusCode::UNAUTHORIZED => "authentication_error",
        StatusCode::PAYMENT_REQUIRED => "insufficient_quota",
        StatusCode::FORBIDDEN => "permission_error",
        StatusCode::NOT_FOUND => "not_found_error",
        StatusCode::TOO_MANY_REQUESTS => "rate_limit_error",
        s if s.is_client_error() => "invalid_request_error",
        _ => "api_error",
    }
}

/// Wraps [`ReqwestService`] and turns an xAI error response into [`OpenAIError::ApiError`].
///
/// A failed response with a body in another shape is given back unchanged, for `async-openai`
/// to read as usual.
#[derive(Clone, Debug)]
pub(super) struct XaiErrorService {
    inner: ReqwestService,
}

impl XaiErrorService {
    pub(super) fn new(http_client: reqwest::Client) -> Self {
        Self {
            inner: ReqwestService::new(http_client),
        }
    }
}

impl tower::Service<HttpRequestFactory> for XaiErrorService {
    type Response = Response;
    type Error = OpenAIError;
    type Future = Pin<Box<dyn Future<Output = Result<Response, OpenAIError>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequestFactory) -> Self::Future {
        let response = self.inner.call(request);
        Box::pin(async move {
            let response = response.await?;
            let status = response.status();
            if status.is_success() {
                return Ok(response);
            }

            // Reading the body consumes the response, so rebuild it for `async-openai` when the
            // body is not an xAI error.
            let mut rebuilt = http::Response::builder()
                .status(status)
                .version(response.version());
            if let Some(headers) = rebuilt.headers_mut() {
                headers.extend(response.headers().clone());
            }
            let body = response.bytes().await.map_err(OpenAIError::Reqwest)?;

            match serde_json::from_slice::<XaiErrorBody>(&body) {
                Ok(xai) => Err(OpenAIError::ApiError(xai.into_api_error(status))),
                Err(_) => rebuilt
                    .body(body)
                    .map(Response::from)
                    .map_err(|e| OpenAIError::InvalidArgument(e.to_string())),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn api_error(status: StatusCode, body: &str) -> ApiError {
        serde_json::from_str::<XaiErrorBody>(body)
            .expect("xAI error body")
            .into_api_error(status)
    }

    #[test]
    fn xai_body_keeps_message_and_code() {
        let error = api_error(
            StatusCode::BAD_REQUEST,
            r#"{"code":"invalid-argument","error":"Temperature must be less than 2 but temperature = 9"}"#,
        );
        assert_eq!(
            error.message,
            "Temperature must be less than 2 but temperature = 9"
        );
        assert_eq!(error.r#type.as_deref(), Some("invalid_request_error"));
        assert_eq!(error.code.as_deref(), Some("invalid_argument"));
        assert_eq!(error.param, None);
    }

    #[test]
    fn status_sets_error_type() {
        for (status, expected) in [
            (StatusCode::BAD_REQUEST, "invalid_request_error"),
            (StatusCode::UNAUTHORIZED, "authentication_error"),
            (StatusCode::PAYMENT_REQUIRED, "insufficient_quota"),
            (StatusCode::FORBIDDEN, "permission_error"),
            (StatusCode::NOT_FOUND, "not_found_error"),
            (StatusCode::UNPROCESSABLE_ENTITY, "invalid_request_error"),
            (StatusCode::TOO_MANY_REQUESTS, "rate_limit_error"),
            (StatusCode::INTERNAL_SERVER_ERROR, "api_error"),
            (StatusCode::SERVICE_UNAVAILABLE, "api_error"),
        ] {
            assert_eq!(error_type(status), expected, "{status}");
        }
    }

    #[test]
    fn openai_body_is_not_an_xai_body() {
        let openai =
            r#"{"error":{"message":"m","type":"invalid_request_error","param":null,"code":null}}"#;
        assert!(serde_json::from_str::<XaiErrorBody>(openai).is_err());
    }
}
