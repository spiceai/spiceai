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

//! Source schema policy evaluated before an accepted CDC burst mutates storage.

use arrow::datatypes::SchemaRef;
use arrow_tools::schema_evolution::WideningPlan;
use datafusion::error::Result;

use super::ChangeCapabilities;

pub enum SchemaDecision {
    Proceed,
    Evolve(WideningPlan),
}

/// Policy data must not retain the sink, refresh task, or source committers.
/// Classification is synchronous and must not submit work to the owner.
pub trait CdcPolicy: Send + Sync {
    /// Classify the incoming schema against the target and backend capabilities.
    ///
    /// # Errors
    /// Returns an error if the source policy refuses the incoming schema.
    fn classify(
        &self,
        incoming: &SchemaRef,
        target: &SchemaRef,
        capabilities: ChangeCapabilities,
    ) -> Result<SchemaDecision>;

    /// Called only after the backend successfully applies an evolution plan.
    fn applied(&self, plan: &WideningPlan);

    /// Whether successive source schemas form separate ordered write groups.
    fn split_on_schema_change(&self) -> bool;
}
