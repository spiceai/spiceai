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

//! Request context extension for Flight SQL sessions

use datafusion::prelude::SessionContext;
use runtime_request_context::Extension;
use std::sync::Arc;

/// Extension that holds a Flight SQL session-specific `SessionContext`.
///
/// When present in the request context, this indicates that the request should use
/// the session-specific context instead of the shared global context. This enables
/// stateful operations like SQL PREPARE/EXECUTE/DEALLOCATE to work across requests.
#[derive(Clone)]
pub struct FlightSessionExtension {
    session_ctx: Arc<SessionContext>,
}

impl FlightSessionExtension {
    #[must_use]
    pub fn new(session_ctx: Arc<SessionContext>) -> Self {
        Self { session_ctx }
    }

    #[must_use]
    pub fn session_context(&self) -> &Arc<SessionContext> {
        &self.session_ctx
    }
}

impl Extension for FlightSessionExtension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
