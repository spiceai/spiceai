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

//! Conversion helpers between [`ComponentStatus`] and proto `i32` representations
//! used in `ComponentStatusUpdate` and `ComponentStatusAck` messages.

use crate::status::ComponentStatus;

/// Converts a [`ComponentStatus`] to its proto `i32` representation.
/// Uses the same discriminant values as [`ComponentStatus::discriminant`].
#[must_use]
#[expect(clippy::cast_possible_truncation)]
pub fn component_status_to_proto(status: &ComponentStatus) -> i32 {
    status.discriminant() as i32
}

/// Converts a proto `i32` value back to a [`ComponentStatus`].
/// Unknown values default to `Initializing`.
/// Note: `Error` messages are lost in the proto round-trip.
#[must_use]
pub fn component_status_from_proto(value: i32) -> ComponentStatus {
    match value {
        1 => ComponentStatus::Ready,
        2 => ComponentStatus::Disabled,
        3 => ComponentStatus::Error(None),
        4 => ComponentStatus::Refreshing,
        5 => ComponentStatus::ShuttingDown,
        _ => ComponentStatus::Initializing,
    }
}
