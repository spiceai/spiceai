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
    /// Federation provider for the accelerated layer (e.g. `DuckDB`). Used post-load.
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
        if !self.enabled {
            return None;
        }
        if self.refresher.initial_load_completed() {
            // Post-load: use the accelerated provider (e.g. DuckDB).
            self.provider.clone()
        } else if self.fallback_during_initial_load {
            // on_schema_resolved: federate to the source during initial load.
            self.fallback_provider.clone()
        } else {
            // on_load (default): don't federate; let AcceleratedTable::scan() return "not ready".
            None
        }
    }
}

#[cfg(test)]
pub(super) fn make_refresher()
-> datafusion::common::Result<Arc<crate::accelerated_table::refresh::Refresher>> {
    use crate::accelerated_table::refresh::{Refresh, Refresher};
    use crate::component::dataset::acceleration::RefreshMode;
    use crate::federated_table::FederatedTable;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use datafusion::common::TableReference;
    use tokio::runtime::Handle;
    use tokio::sync::{Mutex, RwLock};

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let mem_table = Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]])?);
    let federated = Arc::new(FederatedTable::Immediate(
        mem_table.clone() as Arc<dyn datafusion::datasource::TableProvider>
    ));
    Ok(Arc::new(Refresher::new(
        crate::status::RuntimeStatus::new(),
        TableReference::bare("test"),
        federated,
        None,
        Arc::new(RwLock::new(Refresh::new(RefreshMode::Full))),
        mem_table,
        None,
        Handle::current(),
        Arc::new(Mutex::new(())),
    )))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct MockFederationProvider {
        context: &'static str,
    }

    impl FederationProvider for MockFederationProvider {
        fn name(&self) -> &'static str {
            "mock"
        }

        fn compute_context(&self) -> Option<String> {
            Some(self.context.to_string())
        }

        fn analyzer(&self, _: &LogicalPlan) -> Option<FederationAnalyzerForLogicalPlan> {
            None
        }
    }

    fn accelerated() -> Arc<dyn FederationProvider> {
        Arc::new(MockFederationProvider {
            context: "accelerated",
        })
    }

    fn fallback() -> Arc<dyn FederationProvider> {
        Arc::new(MockFederationProvider {
            context: "fallback",
        })
    }

    /// Pre-load + fallback enabled → federate to source (e.g. Databricks).
    #[tokio::test]
    async fn test_pre_load_fallback_enabled_uses_fallback_provider() {
        let refresher = make_refresher().expect("make_refresher");
        assert!(!refresher.initial_load_completed());

        let provider = AcceleratedTableFederationProvider::new(
            true,
            true,
            Some(accelerated()),
            Some(fallback()),
            refresher,
        );

        assert_eq!(provider.compute_context(), Some("fallback".to_string()));
    }

    /// Pre-load + fallback disabled (`on_load` default) → no federation.
    #[tokio::test]
    async fn test_pre_load_fallback_disabled_returns_none() {
        let refresher = make_refresher().expect("make_refresher");

        let provider = AcceleratedTableFederationProvider::new(
            true,
            false,
            Some(accelerated()),
            Some(fallback()),
            refresher,
        );

        assert!(provider.compute_context().is_none());
    }

    /// Pre-load + federation disabled → no federation even when fallback is configured.
    #[tokio::test]
    async fn test_pre_load_federation_disabled_returns_none() {
        let refresher = make_refresher().expect("make_refresher");

        let provider = AcceleratedTableFederationProvider::new(
            false,
            true,
            Some(accelerated()),
            Some(fallback()),
            refresher,
        );

        assert!(provider.compute_context().is_none());
    }

    /// Post-load + enabled → federate to accelerated layer (e.g. `DuckDB`).
    #[tokio::test]
    async fn test_post_load_enabled_uses_accelerated_provider() {
        let refresher = make_refresher().expect("make_refresher");
        refresher.set_initial_load_completed(true);

        let provider = AcceleratedTableFederationProvider::new(
            true,
            true,
            Some(accelerated()),
            Some(fallback()),
            refresher,
        );

        assert_eq!(provider.compute_context(), Some("accelerated".to_string()));
    }

    /// Post-load + disabled → no federation.
    #[tokio::test]
    async fn test_post_load_disabled_returns_none() {
        let refresher = make_refresher().expect("make_refresher");
        refresher.set_initial_load_completed(true);

        let provider = AcceleratedTableFederationProvider::new(
            false,
            false,
            Some(accelerated()),
            Some(fallback()),
            refresher,
        );

        assert!(provider.compute_context().is_none());
    }
}
