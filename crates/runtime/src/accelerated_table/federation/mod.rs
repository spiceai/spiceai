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

use std::any::Any;
use std::sync::Arc;

use super::AcceleratedTable;
use crate::component::dataset::{ReadyState, acceleration::ZeroResultsAction};
use data_components::poly::PolyTableProvider;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion_federation::{
    FederatedTableProviderAdaptor, FederatedTableSource,
    sql::{MultiPartTableReference, SQLTable, SQLTableSource},
};
use provider::AcceleratedTableFederationProvider;

mod provider;

/// A [`SQLTable`] that dynamically switches the remote table reference based on
/// whether the initial acceleration load has completed.
///
/// During initial load, queries are federated to the original source (e.g. Databricks)
/// using `fallback_table_ref`. After load, they target the accelerated layer (e.g. DuckDB)
/// using `accelerated_table_ref`.
#[derive(Debug)]
struct DynamicSQLTable {
    fallback_table_ref: MultiPartTableReference,
    accelerated_table_ref: MultiPartTableReference,
    schema: SchemaRef,
    refresher: Arc<crate::accelerated_table::refresh::Refresher>,
}

impl SQLTable for DynamicSQLTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_reference(&self) -> MultiPartTableReference {
        if self.refresher.initial_load_completed() {
            self.accelerated_table_ref.clone()
        } else {
            self.fallback_table_ref.clone()
        }
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl AcceleratedTable {
    #[must_use]
    fn create_federated_table_source(&self) -> Option<Arc<dyn FederatedTableSource>> {
        let accelerated_table_federation_provider = Arc::new(
            self.accelerator
                .as_any()
                .downcast_ref::<PolyTableProvider>()?
                .clone(),
        );

        let accelerated_table_ref = accelerated_table_federation_provider
            .get_table_source()?
            .as_any()
            .downcast_ref::<SQLTableSource>()
            .map(SQLTableSource::table_reference)?;

        // Try to get the federated source's (e.g. Databricks) SQLTableSource so that during
        // initial load we can federate queries with the correct fully-qualified table reference.
        let fallback = self.federated.try_table_provider_sync().and_then(|tp| {
            tp.as_any()
                .downcast_ref::<FederatedTableProviderAdaptor>()
                .and_then(|adaptor| {
                    adaptor
                        .source
                        .as_any()
                        .downcast_ref::<SQLTableSource>()
                        .map(|s| (s.federation_provider(), s.table_reference()))
                })
        });

        let enabled =
            self.zero_results_action != ZeroResultsAction::UseSource && !self.disable_federation;

        // Only fall back to the federated source during initial load for ready states that
        // are designed to serve queries before acceleration completes. The default `OnLoad`
        // state must NOT federate during initial load — it returns "Acceleration not ready".
        let fallback_during_initial_load = matches!(
            self.ready_state,
            ReadyState::OnSchemaResolved | ReadyState::OnRegistration
        );

        let table_source: Arc<dyn FederatedTableSource> = match fallback {
            Some((fb_provider, fb_table_ref)) if fb_table_ref != accelerated_table_ref => {
                // Federated source uses a different (typically fully-qualified) table reference.
                // Use a dynamic table that presents the correct reference during each phase.
                let dynamic_table: Arc<dyn SQLTable> = Arc::new(DynamicSQLTable {
                    fallback_table_ref: fb_table_ref,
                    accelerated_table_ref,
                    schema: Arc::clone(&self.schema()),
                    refresher: self.refresher(),
                });
                let fed_provider = Arc::new(AcceleratedTableFederationProvider::new(
                    enabled,
                    fallback_during_initial_load,
                    Some(accelerated_table_federation_provider),
                    Some(fb_provider),
                    self.refresher(),
                ));
                Arc::new(SQLTableSource::new_with_table(
                    fed_provider as Arc<_>,
                    dynamic_table,
                ))
            }
            fallback_same_ref => {
                // No useful fallback source or same table reference — use the simple path.
                let fb_provider = fallback_same_ref.map(|(p, _)| p);
                let fed_provider = Arc::new(AcceleratedTableFederationProvider::new(
                    enabled,
                    fallback_during_initial_load,
                    Some(accelerated_table_federation_provider),
                    fb_provider,
                    self.refresher(),
                ));
                Arc::new(SQLTableSource::new_with_schema(
                    fed_provider as Arc<_>,
                    accelerated_table_ref,
                    Arc::clone(&self.schema()),
                ))
            }
        };

        Some(table_source)
    }

    #[must_use]
    pub fn table_provider(self: Arc<Self>) -> Arc<dyn TableProvider> {
        match Arc::clone(&self).create_federated_table_source() {
            Some(table_source) => Arc::new(FederatedTableProviderAdaptor::new_with_provider(
                table_source,
                self,
            )),
            None => self,
        }
    }
}
