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

//! Errors in the Gemini API's own shape: an HTTP status, and the body
//! `{"error": {"code": 400, "message": "...", "status": "INVALID_ARGUMENT"}}`, whose `status`
//! is a canonical Google RPC code that the Google SDKs map to their typed errors.

use std::fmt;

use serde_json::{Value, json};

/// The `status` of a Gemini API error: a canonical Google RPC code
/// (`google.rpc.Code`), each answered with a fixed HTTP status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GeminiStatus {
    Cancelled,
    Unknown,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    Unauthenticated,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
}

impl GeminiStatus {
    /// Every status, in the order of their RPC code numbers.
    pub const ALL: [GeminiStatus; 16] = [
        GeminiStatus::Cancelled,
        GeminiStatus::Unknown,
        GeminiStatus::InvalidArgument,
        GeminiStatus::DeadlineExceeded,
        GeminiStatus::NotFound,
        GeminiStatus::AlreadyExists,
        GeminiStatus::PermissionDenied,
        GeminiStatus::ResourceExhausted,
        GeminiStatus::FailedPrecondition,
        GeminiStatus::Aborted,
        GeminiStatus::OutOfRange,
        GeminiStatus::Unimplemented,
        GeminiStatus::Internal,
        GeminiStatus::Unavailable,
        GeminiStatus::DataLoss,
        GeminiStatus::Unauthenticated,
    ];

    /// The status as the error body names it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            GeminiStatus::Cancelled => "CANCELLED",
            GeminiStatus::Unknown => "UNKNOWN",
            GeminiStatus::InvalidArgument => "INVALID_ARGUMENT",
            GeminiStatus::DeadlineExceeded => "DEADLINE_EXCEEDED",
            GeminiStatus::NotFound => "NOT_FOUND",
            GeminiStatus::AlreadyExists => "ALREADY_EXISTS",
            GeminiStatus::PermissionDenied => "PERMISSION_DENIED",
            GeminiStatus::Unauthenticated => "UNAUTHENTICATED",
            GeminiStatus::ResourceExhausted => "RESOURCE_EXHAUSTED",
            GeminiStatus::FailedPrecondition => "FAILED_PRECONDITION",
            GeminiStatus::Aborted => "ABORTED",
            GeminiStatus::OutOfRange => "OUT_OF_RANGE",
            GeminiStatus::Unimplemented => "UNIMPLEMENTED",
            GeminiStatus::Internal => "INTERNAL",
            GeminiStatus::Unavailable => "UNAVAILABLE",
            GeminiStatus::DataLoss => "DATA_LOSS",
        }
    }

    /// The status an error body names, or `None` when it names none of them.
    #[must_use]
    pub fn parse(status: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|candidate| candidate.as_str() == status)
    }

    /// The HTTP status Google answers this status with.
    #[must_use]
    pub fn http_status(self) -> u16 {
        match self {
            GeminiStatus::InvalidArgument
            | GeminiStatus::FailedPrecondition
            | GeminiStatus::OutOfRange => 400,
            GeminiStatus::Unauthenticated => 401,
            GeminiStatus::PermissionDenied => 403,
            GeminiStatus::NotFound => 404,
            GeminiStatus::AlreadyExists | GeminiStatus::Aborted => 409,
            GeminiStatus::ResourceExhausted => 429,
            GeminiStatus::Cancelled => 499,
            GeminiStatus::Unknown | GeminiStatus::Internal | GeminiStatus::DataLoss => 500,
            GeminiStatus::Unimplemented => 501,
            GeminiStatus::Unavailable => 503,
            GeminiStatus::DeadlineExceeded => 504,
        }
    }
}

impl fmt::Display for GeminiStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A Gemini API error: its status, and the message the body carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeminiError {
    pub status: GeminiStatus,
    pub message: String,
}

impl GeminiError {
    #[must_use]
    pub fn new(status: GeminiStatus, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(GeminiStatus::InvalidArgument, message)
    }

    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(GeminiStatus::NotFound, message)
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(GeminiStatus::Internal, message)
    }

    #[must_use]
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(GeminiStatus::Unavailable, message)
    }

    /// The HTTP status the error is answered with.
    #[must_use]
    pub fn http_status(&self) -> u16 {
        self.status.http_status()
    }

    /// The error body, in Google's format:
    /// `{"error": {"code": 400, "message": "...", "status": "INVALID_ARGUMENT"}}`. A streamed
    /// answer that fails part way sends the same body as its last event.
    #[must_use]
    pub fn body(&self) -> Value {
        json!({
            "error": {
                "code": self.http_status(),
                "message": self.message,
                "status": self.status.as_str(),
            }
        })
    }
}

impl fmt::Display for GeminiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.status, self.message)
    }
}

impl std::error::Error for GeminiError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical codes and their HTTP statuses, as `google/rpc/code.proto` documents them.
    #[test]
    fn every_status_round_trips_and_has_googles_http_status() {
        let documented = [
            ("CANCELLED", 499),
            ("UNKNOWN", 500),
            ("INVALID_ARGUMENT", 400),
            ("DEADLINE_EXCEEDED", 504),
            ("NOT_FOUND", 404),
            ("ALREADY_EXISTS", 409),
            ("PERMISSION_DENIED", 403),
            ("RESOURCE_EXHAUSTED", 429),
            ("FAILED_PRECONDITION", 400),
            ("ABORTED", 409),
            ("OUT_OF_RANGE", 400),
            ("UNIMPLEMENTED", 501),
            ("INTERNAL", 500),
            ("UNAVAILABLE", 503),
            ("DATA_LOSS", 500),
            ("UNAUTHENTICATED", 401),
        ];
        let actual: Vec<(&str, u16)> = GeminiStatus::ALL
            .iter()
            .map(|status| (status.as_str(), status.http_status()))
            .collect();
        assert_eq!(actual, documented);
        for (name, _) in documented {
            assert_eq!(
                GeminiStatus::parse(name).map(GeminiStatus::as_str),
                Some(name)
            );
        }
        assert_eq!(GeminiStatus::parse("OK"), None);
        assert_eq!(GeminiStatus::parse("invalid_argument"), None);
    }

    #[test]
    fn the_body_is_in_googles_format() {
        let error = GeminiError::invalid_argument("Please use a valid role: user, model.");
        assert_eq!(error.http_status(), 400);
        assert_eq!(
            error.body(),
            json!({"error": {"code": 400, "message": "Please use a valid role: user, model.", "status": "INVALID_ARGUMENT"}})
        );
        assert_eq!(
            GeminiError::unavailable("The model is overloaded.").body(),
            json!({"error": {"code": 503, "message": "The model is overloaded.", "status": "UNAVAILABLE"}})
        );
        assert_eq!(
            error.to_string(),
            "INVALID_ARGUMENT: Please use a valid role: user, model."
        );
    }
}
