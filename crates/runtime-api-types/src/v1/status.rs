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

//! Component status types shared across API responses.

use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ComponentErrorCategory {
    /// Error originating from a dataset component.
    Dataset,
    /// Error originating from a model component.
    Model,
    /// Error originating from a worker component.
    Worker,
    /// Error originating from runtime infrastructure.
    Runtime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ComponentErrorType {
    /// Authentication or credential errors (invalid API key, username/password, token).
    Auth,
    /// Connectivity or transport errors (DNS/TLS/socket/network/connect failures).
    Connection,
    /// Timeout and deadline errors.
    Timeout,
    /// Invalid input or request validation errors.
    Validation,
    /// Missing resource errors.
    NotFound,
    /// Permission/authorization errors.
    Permission,
    /// Upstream rate limiting errors.
    RateLimit,
    /// Internal system errors.
    Internal,
    /// Unclassified error type.
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ComponentError {
    /// High-level component category where the error originated.
    pub category: ComponentErrorCategory,
    /// Canonical error type intended for programmatic handling.
    #[serde(rename = "type")]
    pub error_type: ComponentErrorType,
    /// Stable machine-readable code (`{category}.{type}`), e.g. `dataset.auth`.
    pub code: String,
}

impl ComponentError {
    #[must_use]
    pub fn from_status_message(category: ComponentErrorCategory, message: Option<&str>) -> Self {
        let error_type = ComponentErrorType::from_message(message);
        let code = format!("{}.{}", category.code_prefix(), error_type.code_suffix());

        Self {
            category,
            error_type,
            code,
        }
    }
}

impl ComponentErrorCategory {
    fn code_prefix(&self) -> &'static str {
        match self {
            ComponentErrorCategory::Dataset => "dataset",
            ComponentErrorCategory::Model => "model",
            ComponentErrorCategory::Worker => "worker",
            ComponentErrorCategory::Runtime => "runtime",
        }
    }
}

impl ComponentErrorType {
    fn from_message(message: Option<&str>) -> Self {
        let Some(message) = message else {
            return ComponentErrorType::Unknown;
        };

        let message = message.to_lowercase();

        if message.contains("rate limit") || message.contains("too many requests") {
            return ComponentErrorType::RateLimit;
        }

        if message.contains("timeout") || message.contains("timed out") {
            return ComponentErrorType::Timeout;
        }

        if message.contains("not found") || message.contains("does not exist") {
            return ComponentErrorType::NotFound;
        }

        if message.contains("forbidden")
            || message.contains("permission")
            || message.contains("access denied")
        {
            return ComponentErrorType::Permission;
        }

        if message.contains("auth")
            || message.contains("unauthorized")
            || message.contains("invalid api key")
            || message.contains("invalid_api_key")
            || message.contains("credential")
            || message.contains("username")
            || message.contains("password")
            || message.contains("token")
        {
            return ComponentErrorType::Auth;
        }

        if message.contains("connect")
            || message.contains("connection")
            || message.contains("network")
            || message.contains("dns")
            || message.contains("socket")
            || message.contains("tls")
            || message.contains("handshake")
        {
            return ComponentErrorType::Connection;
        }

        if message.contains("invalid")
            || message.contains("malformed")
            || message.contains("parse")
            || message.contains("unsupported")
            || message.contains("missing")
            || message.contains("bad request")
        {
            return ComponentErrorType::Validation;
        }

        if message.contains("internal")
            || message.contains("unexpected")
            || message.contains("panic")
        {
            return ComponentErrorType::Internal;
        }

        ComponentErrorType::Unknown
    }

    fn code_suffix(&self) -> &'static str {
        match self {
            ComponentErrorType::Auth => "auth",
            ComponentErrorType::Connection => "connection",
            ComponentErrorType::Timeout => "timeout",
            ComponentErrorType::Validation => "validation",
            ComponentErrorType::NotFound => "not_found",
            ComponentErrorType::Permission => "permission",
            ComponentErrorType::RateLimit => "rate_limit",
            ComponentErrorType::Internal => "internal",
            ComponentErrorType::Unknown => "unknown",
        }
    }
}

/// Represents the status of a component (e.g. dataset, model, etc).
///
/// The `Error` variant optionally carries a human-readable error message describing
/// what caused the component to enter the error state. Use [`ComponentStatus::error`]
/// for an error without a message, or [`ComponentStatus::error_with_message`] to
/// include one.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(as = String, example = "Ready"))]
pub enum ComponentStatus {
    /// The component is initializing and not yet ready
    Initializing,

    /// The component is ready to accept connections
    Ready,

    /// The component is disabled and not running
    Disabled,

    /// An error occurred in the component, with an optional error message
    Error(Option<String>),

    /// The component is in the process of refreshing its state
    Refreshing,

    /// The component is in the process of shutting down
    ShuttingDown,

    /// The component is configured but not loaded yet
    NotLoaded,
}

impl ComponentStatus {
    /// Returns the numeric discriminant for this status, matching the original enum ordering.
    /// Used for metrics recording.
    #[must_use]
    pub fn discriminant(&self) -> u64 {
        match self {
            ComponentStatus::Initializing => 0,
            ComponentStatus::Ready => 1,
            ComponentStatus::Disabled => 2,
            ComponentStatus::Error(_) => 3,
            ComponentStatus::Refreshing => 4,
            ComponentStatus::ShuttingDown => 5,
            ComponentStatus::NotLoaded => 6,
        }
    }

    /// Creates an `Error` status with no message.
    #[must_use]
    pub fn error() -> Self {
        ComponentStatus::Error(None)
    }

    /// Creates an `Error` status with the given message.
    #[must_use]
    pub fn error_with_message(message: impl Into<String>) -> Self {
        ComponentStatus::Error(Some(message.into()))
    }

    /// Returns the error message if this is an `Error` status with a message.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        match self {
            ComponentStatus::Error(msg) => msg.as_deref(),
            _ => None,
        }
    }

    /// Returns `true` if this status is an `Error` variant (regardless of message content).
    #[must_use]
    pub fn is_error(&self) -> bool {
        matches!(self, ComponentStatus::Error(_))
    }
}

/// Two `ComponentStatus` values are considered equal if they are the same variant,
/// regardless of any inner data (e.g. the error message in `Error`).
impl PartialEq for ComponentStatus {
    fn eq(&self, other: &Self) -> bool {
        self.discriminant() == other.discriminant()
    }
}

impl Eq for ComponentStatus {}

impl Display for ComponentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentStatus::Initializing => write!(f, "Initializing"),
            ComponentStatus::Ready => write!(f, "Ready"),
            ComponentStatus::Disabled => write!(f, "Disabled"),
            ComponentStatus::Error(_) => write!(f, "Error"),
            ComponentStatus::Refreshing => write!(f, "Refreshing"),
            ComponentStatus::ShuttingDown => write!(f, "ShuttingDown"),
            ComponentStatus::NotLoaded => write!(f, "NotLoaded"),
        }
    }
}

/// Serializes as a plain string (e.g. `"Error"`) regardless of any inner data,
/// preserving backward compatibility with existing API consumers.
impl Serialize for ComponentStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Deserializes from a plain string (e.g. `"Error"`). The `Error` variant
/// always deserializes with `None` for the inner message.
impl<'de> Deserialize<'de> for ComponentStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "Initializing" => Ok(ComponentStatus::Initializing),
            "Ready" => Ok(ComponentStatus::Ready),
            "Disabled" => Ok(ComponentStatus::Disabled),
            "Error" => Ok(ComponentStatus::Error(None)),
            "Refreshing" => Ok(ComponentStatus::Refreshing),
            "ShuttingDown" => Ok(ComponentStatus::ShuttingDown),
            "NotLoaded" => Ok(ComponentStatus::NotLoaded),
            other => Err(serde::de::Error::unknown_variant(
                other,
                &[
                    "Initializing",
                    "Ready",
                    "Disabled",
                    "Error",
                    "Refreshing",
                    "ShuttingDown",
                    "NotLoaded",
                ],
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ComponentError, ComponentErrorCategory, ComponentErrorType};

    #[test]
    fn test_component_error_type_from_message_classifies_expected_patterns() {
        assert_eq!(
            ComponentErrorType::from_message(Some("too many requests from upstream")),
            ComponentErrorType::RateLimit
        );
        assert_eq!(
            ComponentErrorType::from_message(Some("request timed out after 30s")),
            ComponentErrorType::Timeout
        );
        assert_eq!(
            ComponentErrorType::from_message(Some("resource not found")),
            ComponentErrorType::NotFound
        );
        assert_eq!(
            ComponentErrorType::from_message(Some("access denied by policy")),
            ComponentErrorType::Permission
        );
        assert_eq!(
            ComponentErrorType::from_message(Some("invalid_api_key")),
            ComponentErrorType::Auth
        );
        assert_eq!(
            ComponentErrorType::from_message(Some("tls handshake failed")),
            ComponentErrorType::Connection
        );
        assert_eq!(
            ComponentErrorType::from_message(Some("unsupported format in request")),
            ComponentErrorType::Validation
        );
        assert_eq!(
            ComponentErrorType::from_message(Some("unexpected internal failure")),
            ComponentErrorType::Internal
        );
    }

    #[test]
    fn test_component_error_type_from_message_handles_ambiguous_and_missing_inputs() {
        assert_eq!(
            ComponentErrorType::from_message(Some("Invalid token provided")),
            ComponentErrorType::Auth
        );
        assert_eq!(
            ComponentErrorType::from_message(Some("INVALID INPUT payload")),
            ComponentErrorType::Validation
        );
        assert_eq!(
            ComponentErrorType::from_message(Some("")),
            ComponentErrorType::Unknown
        );
        assert_eq!(
            ComponentErrorType::from_message(None),
            ComponentErrorType::Unknown
        );
    }

    #[test]
    fn test_component_error_from_status_message_generates_stable_code() {
        let dataset_auth = ComponentError::from_status_message(
            ComponentErrorCategory::Dataset,
            Some("invalid_api_key"),
        );
        assert_eq!(dataset_auth.error_type, ComponentErrorType::Auth);
        assert_eq!(dataset_auth.code, "dataset.auth");

        let model_unknown = ComponentError::from_status_message(
            ComponentErrorCategory::Model,
            Some("totally novel failure"),
        );
        assert_eq!(model_unknown.error_type, ComponentErrorType::Unknown);
        assert_eq!(model_unknown.code, "model.unknown");
    }

    #[test]
    fn test_databricks_permission_error_classifies_as_permission() {
        // A representative message from DataConnectorError::InsufficientPermissions
        // as produced by the Databricks connector's UC permission check.
        let msg = "Insufficient permissions to access the catalog.schema.table dataset (databricks). Grant SELECT or ALL PRIVILEGES on the table";
        let error = ComponentError::from_status_message(ComponentErrorCategory::Dataset, Some(msg));
        assert_eq!(
            error.error_type,
            ComponentErrorType::Permission,
            "Databricks permission denial message should classify as Permission"
        );
        assert_eq!(error.code, "dataset.permission");
    }

    #[test]
    fn test_error_type_priority_permission_before_auth() {
        // "permission" should match Permission, not fall through to Auth
        // (which also matches "token").
        let msg = "permission denied for token-based access";
        let error_type = ComponentErrorType::from_message(Some(msg));
        assert_eq!(
            error_type,
            ComponentErrorType::Permission,
            "Permission keywords should take precedence over auth keywords"
        );
    }
}
