use std::{any::Any, sync::Arc};

use crate::component::dataset::acceleration::ZeroResultsAction;
use arrow::datatypes::SchemaRef;
use data_components::poly::PolyTableProvider;
use datafusion::datasource::TableType;
use datafusion::error::Result as DataFusionResult;
use datafusion::{datasource::TableProvider, logical_expr::TableSource};
use datafusion_federation::{
    FederatedTableProviderAdaptor, FederatedTableSource, FederationProvider,
};

use super::AcceleratedTable;

impl AcceleratedTable {
    fn get_federation_provider_for_accelerator(&self) -> Option<Arc<dyn FederationProvider>> {
        let poly = self
            .accelerator
            .as_any()
            .downcast_ref::<PolyTableProvider>()?;

        Some(Arc::new(poly.clone()))
    }

    fn create_federated_table_source(&self) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let schema = Arc::clone(&self.schema());
        let fed_provider = Arc::new(FederationProviderAdapter::new(
            self.get_federation_provider_for_accelerator(),
            self.zero_results_action != ZeroResultsAction::UseSource,
        ));
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

pub struct FederationProviderAdapter {
    pub inner: Option<Arc<dyn FederationProvider>>,
    pub enabled: bool,
}

impl FederationProviderAdapter {
    fn new(inner: Option<Arc<dyn FederationProvider>>, enabled: bool) -> Self {
        Self { inner, enabled }
    }
}

impl FederationProvider for FederationProviderAdapter {
    fn name(&self) -> &str {
        "FederationProviderForAcceleratedDataset"
    }

    fn compute_context(&self) -> Option<String> {
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
    provider: Arc<FederationProviderAdapter>,
    schema: SchemaRef,
}

impl AcceleratedTableFederatedTableSource {
    pub fn new_with_schema(
        provider: Arc<FederationProviderAdapter>,
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
