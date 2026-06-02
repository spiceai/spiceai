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

use std::sync::Arc;

use datafusion::execution::{SessionState, SessionStateBuilder};

/// Returns a [`SessionStateBuilder`] cloned from `existing` while preserving custom rules.
#[must_use]
pub fn builder_from_existing(existing: &SessionState) -> SessionStateBuilder {
    SessionStateBuilder::new_from_existing(existing.clone())
        .with_analyzer_rules(existing.analyzer().rules.iter().map(Arc::clone).collect())
        .with_optimizer_rules(existing.optimizers().iter().map(Arc::clone).collect())
        .with_physical_optimizer_rules(
            existing
                .physical_optimizers()
                .iter()
                .map(Arc::clone)
                .collect(),
        )
}
