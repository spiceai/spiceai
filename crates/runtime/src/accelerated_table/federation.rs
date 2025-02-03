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

use std::{any::Any, sync::Arc};

use super::AcceleratedTable;
use crate::component::dataset::acceleration::ZeroResultsAction;
use arrow::datatypes::SchemaRef;
use data_components::poly::PolyTableProvider;
use datafusion::datasource::TableType;
use datafusion::error::Result as DataFusionResult;
use datafusion::{datasource::TableProvider, logical_expr::TableSource};
use datafusion_federation::{
    FederatedTableProviderAdaptor, FederatedTableSource, FederationProvider,
};

impl AcceleratedTable {
    fn get_federation_provider_for_accelerator(&self) -> Option<Arc<PolyTableProvider>> {
        let poly = self
            .accelerator
            .as_any()
            .downcast_ref::<PolyTableProvider>()?;

        Some(Arc::new(poly.clone()))
    }

    fn create_federated_table_source(&self) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let schema = Arc::clone(&self.schema());
        let inner = self.get_federation_provider_for_accelerator();

        let enabled = self.zero_results_action != ZeroResultsAction::UseSource
            && !self.disable_query_push_down;

        let fed_provider = Arc::new(FederationAdaptor::new(
            inner.clone().map(|x| x as Arc<dyn FederationProvider>),
            enabled,
        ));

        if enabled {
            if let Some(inner) = inner {
                if let Some(table_source) = inner.get_table_source() {
                    return Ok(table_source);
                }
            }
        }

        // For other queries to continue running without enabling federation
        Ok(Arc::new(
            AcceleratedTableFederatedTableSource::new_with_schema(fed_provider, schema)?,
        ))
    }

    pub fn create_federated_table_provider(
        self: Arc<Self>,
    ) -> DataFusionResult<FederatedTableProviderAdaptor> {
        let table_source = self.create_federated_table_source()?;
        Ok(FederatedTableProviderAdaptor::new_with_provider(
            table_source,
            self,
        ))
    }
}

pub struct FederationAdaptor {
    pub inner: Option<Arc<dyn FederationProvider>>,
    pub enabled: bool,
}

impl FederationAdaptor {
    fn new(inner: Option<Arc<dyn FederationProvider>>, enabled: bool) -> Self {
        Self { inner, enabled }
    }
}

impl FederationProvider for FederationAdaptor {
    fn name(&self) -> &'static str {
        "FederationProviderForAcceleratedDataset"
    }

    fn compute_context(&self) -> Option<String> {
        if !self.enabled {
            return None;
        }
        self.inner.clone().and_then(|x| x.compute_context())
    }

    fn analyzer(&self) -> Option<Arc<datafusion::optimizer::Analyzer>> {
        if !self.enabled {
            return None;
        }
        self.inner.clone().and_then(|x| x.analyzer())
    }
}

pub struct AcceleratedTableFederatedTableSource {
    provider: Arc<FederationAdaptor>,
    schema: SchemaRef,
}

impl AcceleratedTableFederatedTableSource {
    pub fn new_with_schema(
        provider: Arc<FederationAdaptor>,
        schema: SchemaRef,
    ) -> DataFusionResult<Self> {
        Ok(Self { provider, schema })
    }
}

impl FederatedTableSource for AcceleratedTableFederatedTableSource {
    fn federation_provider(&self) -> Arc<dyn FederationProvider> {
        Arc::clone(&self.provider) as Arc<dyn FederationProvider>
    }
}

impl TableSource for AcceleratedTableFederatedTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
}
