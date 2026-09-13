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

use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::OptimizerRule;
use datafusion_federation::{FederationAnalyzerForLogicalPlan, FederationProvider};

/// A table whose acceleration must not federate publishes no federated source at
/// all (see `create_federated_table_source`), so every provider that reaches
/// this type is federating; the only question left is whether its accelerator
/// holds data yet.
#[derive(Debug)]
pub struct AcceleratedTableFederationProvider {
    provider: Arc<dyn FederationProvider>,
    refresher: Arc<crate::accelerated::refresh::Refresher>,
}

impl AcceleratedTableFederationProvider {
    pub fn new(
        provider: Arc<dyn FederationProvider>,
        refresher: Arc<crate::accelerated::refresh::Refresher>,
    ) -> Self {
        Self {
            provider,
            refresher,
        }
    }

    /// The accelerator can only answer a federated sub-plan once its initial
    /// load has completed; until then the source must serve the query.
    fn federation_provider(&self) -> Option<&Arc<dyn FederationProvider>> {
        self.refresher
            .initial_load_completed()
            .then_some(&self.provider)
    }
}

impl FederationProvider for AcceleratedTableFederationProvider {
    fn name(&self) -> &'static str {
        "FederationProviderForAcceleratedDataset"
    }

    fn compute_context(&self) -> Option<String> {
        self.federation_provider()
            .and_then(|provider| provider.compute_context())
    }

    fn pre_federation_optimizer_rules(&self) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
        self.federation_provider()
            .map_or_else(Vec::new, |provider| {
                provider.pre_federation_optimizer_rules()
            })
    }

    fn analyzer(&self, plan: &LogicalPlan) -> Option<FederationAnalyzerForLogicalPlan> {
        self.federation_provider()?.analyzer(plan)
    }
}
