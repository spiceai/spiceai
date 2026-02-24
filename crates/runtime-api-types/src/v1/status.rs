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

/// Represents the status of a component (e.g. dataset, model, etc).
///
/// The `Error` variant optionally carries a human-readable error message describing
/// what caused the component to enter the error state. Use [`ComponentStatus::error`]
/// for an error without a message, or [`ComponentStatus::error_with_message`] to
/// include one.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
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
            other => Err(serde::de::Error::unknown_variant(
                other,
                &[
                    "Initializing",
                    "Ready",
                    "Disabled",
                    "Error",
                    "Refreshing",
                    "ShuttingDown",
                ],
            )),
        }
    }
}
