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

use super::AcceleratedTable;
use crate::component::dataset::acceleration::ZeroResultsAction;
use data_components::poly::PolyTableProvider;
use datafusion::datasource::TableProvider;
use datafusion_federation::{
    FederatedTableProviderAdaptor, FederatedTableSource, sql::SQLTableSource,
};
use provider::AcceleratedTableFederationProvider;

mod provider;

impl AcceleratedTable {
    #[must_use]
    fn create_federated_table_source(&self) -> Option<Arc<dyn FederatedTableSource>> {
        let accelerated_table_federation_provider = Arc::new(
            self.accelerator
                .as_any()
                .downcast_ref::<PolyTableProvider>()?
                .clone(),
        );

        let remote_table_name = accelerated_table_federation_provider
            .get_table_source()?
            .as_any()
            .downcast_ref::<SQLTableSource>()
            .map(SQLTableSource::table_reference)?;

        let enabled =
            self.zero_results_action != ZeroResultsAction::UseSource && !self.disable_federation;

        let fed_provider = Arc::new(AcceleratedTableFederationProvider::new(
            enabled,
            Some(accelerated_table_federation_provider),
            self.refresher(),
        ));

        Some(Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            remote_table_name,
            Arc::clone(&self.schema()),
        )))
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
