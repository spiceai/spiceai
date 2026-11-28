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

//! Errors in the Messages API's own shape: an HTTP status, and an envelope whose `error.type`
//! the Anthropic SDKs map to their typed exceptions.

use std::fmt;

use serde_json::{Value, json};

/// The `type` of a Messages API error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorType {
    InvalidRequest,
    Authentication,
    Billing,
    Permission,
    NotFound,
    RequestTooLarge,
    RateLimit,
    Api,
    Timeout,
    Overloaded,
    /// A type not named above, as a provider reported it.
    Other(String),
}

impl ErrorType {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            ErrorType::InvalidRequest => "invalid_request_error",
            ErrorType::Authentication => "authentication_error",
            ErrorType::Billing => "billing_error",
            ErrorType::Permission => "permission_error",
            ErrorType::NotFound => "not_found_error",
            ErrorType::RequestTooLarge => "request_too_large",
            ErrorType::RateLimit => "rate_limit_error",
            ErrorType::Api => "api_error",
            ErrorType::Timeout => "timeout_error",
            ErrorType::Overloaded => "overloaded_error",
            ErrorType::Other(other) => other,
        }
    }

    #[must_use]
    pub fn parse(error_type: &str) -> Self {
        match error_type {
            "invalid_request_error" => ErrorType::InvalidRequest,
            "authentication_error" => ErrorType::Authentication,
            "billing_error" => ErrorType::Billing,
            "permission_error" => ErrorType::Permission,
            "not_found_error" => ErrorType::NotFound,
            "request_too_large" => ErrorType::RequestTooLarge,
            "rate_limit_error" => ErrorType::RateLimit,
            "api_error" => ErrorType::Api,
            "timeout_error" => ErrorType::Timeout,
            "overloaded_error" => ErrorType::Overloaded,
            other => ErrorType::Other(other.to_string()),
        }
    }

    /// The HTTP status the Messages API answers this type of error with.
    #[must_use]
    pub fn status(&self) -> u16 {
        match self {
            ErrorType::InvalidRequest => 400,
            ErrorType::Authentication => 401,
            ErrorType::Billing => 402,
            ErrorType::Permission => 403,
            ErrorType::NotFound => 404,
            ErrorType::RequestTooLarge => 413,
            ErrorType::RateLimit => 429,
            ErrorType::Api | ErrorType::Other(_) => 500,
            ErrorType::Timeout => 504,
            ErrorType::Overloaded => 529,
        }
    }
}

/// A Messages API error: the HTTP status, and the envelope's `type`, `message` and `request_id`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessagesError {
    pub status: u16,
    pub error_type: ErrorType,
    pub message: String,
    /// The provider's ID for the request that failed, when a provider answered.
    pub request_id: Option<String>,
}

impl MessagesError {
    /// An error of `error_type`, with the status the Messages API answers it with.
    #[must_use]
    pub fn new(error_type: ErrorType, message: impl Into<String>) -> Self {
        Self {
            status: error_type.status(),
            error_type,
            message: message.into(),
            request_id: None,
        }
    }

    #[must_use]
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(ErrorType::InvalidRequest, message)
    }

    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorType::NotFound, message)
    }

    #[must_use]
    pub fn api(message: impl Into<String>) -> Self {
        Self::new(ErrorType::Api, message)
    }

    #[must_use]
    pub fn with_status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }

    /// The error a provider answered with `status` and `body`: its envelope when `body` is one,
    /// else an error of the type the Messages API gives `status`, carrying `body` as its message.
    #[must_use]
    pub fn from_response(status: u16, body: &[u8]) -> Self {
        let envelope: Option<Value> = serde_json::from_slice(body).ok();
        if let Some(error) = envelope.as_ref().and_then(Self::from_envelope) {
            return error.with_status(status);
        }
        let error_type = match status {
            400 => ErrorType::InvalidRequest,
            401 => ErrorType::Authentication,
            402 => ErrorType::Billing,
            403 => ErrorType::Permission,
            404 => ErrorType::NotFound,
            413 => ErrorType::RequestTooLarge,
            429 => ErrorType::RateLimit,
            504 => ErrorType::Timeout,
            529 => ErrorType::Overloaded,
            _ => ErrorType::Api,
        };
        let text = String::from_utf8_lossy(body);
        let message = if text.trim().is_empty() {
            format!("The provider answered with HTTP status {status} and no body.")
        } else {
            text.into_owned()
        };
        Self::new(error_type, message).with_status(status)
    }

    /// The error in a Messages API error envelope, or in an `error` stream event; `None` when
    /// `value` is neither.
    #[must_use]
    pub fn from_envelope(value: &Value) -> Option<Self> {
        let error = value.get("error")?;
        let error_type = error.get("type")?.as_str()?;
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut parsed = Self::new(ErrorType::parse(error_type), message);
        parsed.request_id = value
            .get("request_id")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        Some(parsed)
    }

    /// The error as the Messages API's error envelope, which is also the data of an `error`
    /// stream event.
    #[must_use]
    pub fn to_envelope(&self) -> Value {
        let mut envelope = json!({
            "type": "error",
            "error": {"type": self.error_type.as_str(), "message": self.message},
        });
        if let (Some(request_id), Some(object)) = (&self.request_id, envelope.as_object_mut()) {
            object.insert("request_id".to_string(), Value::String(request_id.clone()));
        }
        envelope
    }
}

impl fmt::Display for MessagesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.error_type.as_str(), self.message)
    }
}

impl std::error::Error for MessagesError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_named_type_round_trips_and_has_the_documented_status() {
        let types = [
            ("invalid_request_error", 400),
            ("authentication_error", 401),
            ("billing_error", 402),
            ("permission_error", 403),
            ("not_found_error", 404),
            ("request_too_large", 413),
            ("rate_limit_error", 429),
            ("api_error", 500),
            ("timeout_error", 504),
            ("overloaded_error", 529),
        ];
        for (name, status) in types {
            let parsed = ErrorType::parse(name);
            assert_eq!((parsed.as_str(), parsed.status()), (name, status));
        }
        assert_eq!(
            ErrorType::parse("brand_new_error"),
            ErrorType::Other("brand_new_error".to_string())
        );
    }

    #[test]
    fn a_provider_envelope_is_kept_with_its_status_and_request_id() {
        let body = br#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"},"request_id":"req_1"}"#;
        let error = MessagesError::from_response(529, body);
        assert_eq!(
            error,
            MessagesError {
                status: 529,
                error_type: ErrorType::Overloaded,
                message: "Overloaded".to_string(),
                request_id: Some("req_1".to_string()),
            }
        );
        assert_eq!(
            error.to_envelope(),
            json!({"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}, "request_id": "req_1"})
        );
    }

    #[test]
    fn a_body_that_is_not_an_envelope_becomes_the_message() {
        assert_eq!(
            MessagesError::from_response(502, b"<html>Bad Gateway</html>"),
            MessagesError {
                status: 502,
                error_type: ErrorType::Api,
                message: "<html>Bad Gateway</html>".to_string(),
                request_id: None,
            }
        );
        assert_eq!(
            MessagesError::from_response(429, b"").message,
            "The provider answered with HTTP status 429 and no body."
        );
        assert_eq!(
            MessagesError::from_response(429, b"").error_type,
            ErrorType::RateLimit
        );
    }
}
