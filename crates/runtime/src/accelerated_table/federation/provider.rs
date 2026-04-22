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
use datafusion_federation::{FederationAnalyzerForLogicalPlan, FederationProvider};

#[derive(Debug)]
pub struct AcceleratedTableFederationProvider {
    enabled: bool,
    /// If true, federate queries to the source during initial load (for `on_schema_resolved` /
    /// `on_registration` ready states). If false (default `on_load`), return `None` so that
    /// `AcceleratedTable::scan()` can surface the "Acceleration not ready" error.
    fallback_during_initial_load: bool,
    /// Federation provider for the accelerated layer (e.g. DuckDB). Used post-load.
    provider: Option<Arc<dyn FederationProvider>>,
    /// Federation provider for the federated source (e.g. Databricks). Used during initial load
    /// to ensure queries are federated correctly while acceleration is being populated.
    fallback_provider: Option<Arc<dyn FederationProvider>>,
    refresher: Arc<crate::accelerated_table::refresh::Refresher>,
}

impl AcceleratedTableFederationProvider {
    pub fn new(
        enabled: bool,
        fallback_during_initial_load: bool,
        provider: Option<Arc<dyn FederationProvider>>,
        fallback_provider: Option<Arc<dyn FederationProvider>>,
        refresher: Arc<crate::accelerated_table::refresh::Refresher>,
    ) -> Self {
        Self {
            enabled,
            fallback_during_initial_load,
            provider,
            fallback_provider,
            refresher,
        }
    }

    fn federation_provider(&self) -> Option<Arc<dyn FederationProvider>> {
        if self.refresher.initial_load_completed() {
            // Post-load: use the accelerated provider (e.g. DuckDB) if federation is enabled.
            if self.enabled {
                self.provider.clone()
            } else {
                None
            }
        } else if self.fallback_during_initial_load {
            // on_schema_resolved / on_registration: federate to the source during initial load.
            self.fallback_provider.clone()
        } else {
            // on_load (default): don't federate; let AcceleratedTable::scan() return "not ready".
            None
        }
    }
}

impl FederationProvider for AcceleratedTableFederationProvider {
    fn name(&self) -> &'static str {
        "FederationProviderForAcceleratedDataset"
    }

    fn compute_context(&self) -> Option<String> {
        self.federation_provider().and_then(|x| x.compute_context())
    }

    fn analyzer(&self, plan: &LogicalPlan) -> Option<FederationAnalyzerForLogicalPlan> {
        self.federation_provider()?.analyzer(plan)
    }
}
