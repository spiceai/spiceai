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
#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum ComponentStatus {
    /// The component is initializing and not yet ready
    Initializing = 0,

    /// The component is ready to accept connections
    Ready = 1,

    /// The component is disabled and not running
    Disabled = 2,

    /// An error occurred in the component
    Error = 3,

    /// The component is in the process of refreshing its state
    Refreshing = 4,

    /// The component is in the process of shutting down
    ShuttingDown = 5,
}

impl Display for ComponentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentStatus::Initializing => write!(f, "Initializing"),
            ComponentStatus::Ready => write!(f, "Ready"),
            ComponentStatus::Disabled => write!(f, "Disabled"),
            ComponentStatus::Error => write!(f, "Error"),
            ComponentStatus::Refreshing => write!(f, "Refreshing"),
            ComponentStatus::ShuttingDown => write!(f, "ShuttingDown"),
        }
    }
}
