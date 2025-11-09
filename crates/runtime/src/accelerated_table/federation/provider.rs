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

use datafusion_expr::LogicalPlan;
use datafusion_federation::FederationProvider;

#[allow(clippy::struct_field_names)]
#[derive(Debug)]
pub struct AcceleratedTableFederationProvider {
    enabled: bool,
    provider: Option<Arc<dyn FederationProvider>>,
    refresher: Arc<crate::accelerated_table::refresh::Refresher>,
}

impl AcceleratedTableFederationProvider {
    pub fn new(
        enabled: bool,
        provider: Option<Arc<dyn FederationProvider>>,
        refresher: Arc<crate::accelerated_table::refresh::Refresher>,
    ) -> Self {
        Self {
            enabled,
            provider,
            refresher,
        }
    }

    fn federation_provider(&self) -> Option<Arc<dyn FederationProvider>> {
        // If the initial load has completed and this provider is enabled, we can use the accelerated table federation provider.
        match (self.enabled, self.refresher.initial_load_completed()) {
            (true, true) => self.provider.clone(),
            _ => None,
        }
    }
}

impl FederationProvider for AcceleratedTableFederationProvider {
    fn name(&self) -> &'static str {
        "FederationProviderForAcceleratedDataset"
    }

    fn compute_context(&self) -> Option<String> {
        if !self.enabled {
            return None;
        }
        self.federation_provider().and_then(|x| x.compute_context())
    }

    fn analyzer(&self, plan: &LogicalPlan) -> Option<Arc<datafusion::optimizer::Analyzer>> {
        if !self.enabled {
            return None;
        }
        self.federation_provider().and_then(|x| x.analyzer(plan))
    }
}
