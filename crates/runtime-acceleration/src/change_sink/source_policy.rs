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
//!
//! The CDC source implements `CdcPolicy` from its `on_schema_change`
//! settings and attaches it to its `CdcIngress` lane. The owner calls it while
//! it prepares a burst, and acts on the `SchemaDecision`.

use arrow::datatypes::SchemaRef;
use arrow_tools::schema_evolution::WideningPlan;
use datafusion::error::Result;

use super::ChangeCapabilities;

/// The policy's verdict for one prepared CDC group.
pub enum SchemaDecision {
    /// Write the rows to the current table schema. Changed numeric types are
    /// cast, and a value that does not cast exactly is refused.
    Proceed,
    /// Evolve the table with this plan before the group is written. The owner
    /// refuses the burst if the backend cannot evolve live.
    Evolve(WideningPlan),
}

/// Policy data must not retain the sink, refresh task, or source committers.
/// Classification is synchronous and must not submit work to the owner.
///
/// Decides how the owner handles a CDC schema that differs from the table.
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
