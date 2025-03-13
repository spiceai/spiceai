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

use datafusion_federation::FederationProvider;

use crate::component::dataset::ReadyState;

#[allow(clippy::struct_field_names)]
pub struct AcceleratedTableFederationProvider {
    enabled: bool,
    accelerated_table_federation_provider: Option<Arc<dyn FederationProvider>>,
    federated_table_federation_provider: Option<Arc<dyn FederationProvider>>,
    ready_state: ReadyState,
    refresher: Arc<crate::accelerated_table::refresh::Refresher>,
}

impl AcceleratedTableFederationProvider {
    pub fn new(
        enabled: bool,
        accelerated_table_federation_provider: Option<Arc<dyn FederationProvider>>,
        federated_table_federation_provider: Option<Arc<dyn FederationProvider>>,
        ready_state: ReadyState,
        refresher: Arc<crate::accelerated_table::refresh::Refresher>,
    ) -> Self {
        Self {
            enabled,
            accelerated_table_federation_provider,
            federated_table_federation_provider,
            ready_state,
            refresher,
        }
    }

    fn federation_provider(&self) -> Option<Arc<dyn FederationProvider>> {
        if !self.enabled {
            return None;
        }

        // If the initial load has completed, we can use the accelerated table federation provider.
        if self.refresher.initial_load_completed() {
            return self.accelerated_table_federation_provider.clone();
        }

        // Otherwise, we need to use the federated table federation provider if the ready state is OnRegistration.
        if self.ready_state == ReadyState::OnRegistration {
            return self.federated_table_federation_provider.clone();
        }

        // If we get here then we need to wait for the initial load to complete.
        None
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
        self.federation_provider()
            .clone()
            .and_then(|x| x.compute_context())
    }

    fn analyzer(&self) -> Option<Arc<datafusion::optimizer::Analyzer>> {
        if !self.enabled {
            return None;
        }
        self.federation_provider()
            .clone()
            .and_then(|x| x.analyzer())
    }
}
