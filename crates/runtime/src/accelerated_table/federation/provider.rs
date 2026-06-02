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

use datafusion::common::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableType;
use datafusion::logical_expr::TableSource;
use datafusion::optimizer::optimizer::Optimizer;
use datafusion_federation::{FederatedTableSource, FederationProvider, sql::SQLTableSource};

pub struct AcceleratedTableFederatedTableSource {
    table_source: SQLTableSource,
    federation_provider: Arc<AcceleratedTableFederationProvider>,
}

impl AcceleratedTableFederatedTableSource {
    pub fn new(
        table_source: SQLTableSource,
        federation_provider: Arc<AcceleratedTableFederationProvider>,
    ) -> Self {
        Self {
            table_source,
            federation_provider,
        }
    }
}

impl std::fmt::Debug for AcceleratedTableFederatedTableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceleratedTableFederatedTableSource")
            .field("table_reference", &self.table_source.table_reference())
            .finish_non_exhaustive()
    }
}

impl TableSource for AcceleratedTableFederatedTableSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self.table_source.as_any()
    }

    fn schema(&self) -> SchemaRef {
        self.table_source.schema()
    }

    fn table_type(&self) -> TableType {
        self.table_source.table_type()
    }
}

impl FederatedTableSource for AcceleratedTableFederatedTableSource {
    fn federation_provider(&self) -> Arc<dyn FederationProvider> {
        Arc::clone(&self.federation_provider) as Arc<dyn FederationProvider>
    }
}

pub struct AcceleratedTableFederationProvider {
    enabled: bool,
    provider: Option<Arc<dyn FederationProvider>>,
    refresher: Arc<crate::accelerated_table::refresh::Refresher>,
}

impl std::fmt::Debug for AcceleratedTableFederationProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceleratedTableFederationProvider")
            .field("enabled", &self.enabled)
            .field("has_provider", &self.provider.is_some())
            .finish_non_exhaustive()
    }
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

    fn optimizer(&self) -> Option<Arc<Optimizer>> {
        if !self.enabled {
            return None;
        }
        self.federation_provider()?.optimizer()
    }
}
