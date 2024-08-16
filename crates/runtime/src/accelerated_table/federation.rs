use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use data_components::delete::get_deletion_provider;
use datafusion::datasource::TableType;
use datafusion::error::Result as DataFusionResult;
use datafusion::{datasource::TableProvider, logical_expr::TableSource};
use datafusion_federation::{
    FederatedTableProviderAdaptor, FederatedTableSource, FederationProvider,
};

use super::AcceleratedTable;

impl AcceleratedTable {
    fn get_federation_provider_for_accelerator(&self) -> Option<Arc<dyn FederationProvider>> {
        if let Some(deletion_provider) = get_deletion_provider(Arc::clone(&self.accelerator)) {
            if let Some(federated) = deletion_provider
                .as_any()
                .downcast_ref::<Arc<FederatedTableProviderAdaptor>>()
            {
                return Some(federated.source.federation_provider());
            }
        }

        todo!()
    }

    fn create_federated_table_source(&self) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let schema = Arc::clone(&self.schema());
        let fed_provider = Arc::new(FederationProviderAdapter::new(
            "test".to_string(),
            self.get_federation_provider_for_accelerator().unwrap(),
        ));
        Ok(Arc::new(SomeTableSource::new_with_schema(
            fed_provider,
            schema,
        )?))
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

impl FederationProvider for AcceleratedTable {
    fn name(&self) -> &str {
        "AcceleratorTableFederatedProvider"
    }

    fn compute_context(&self) -> Option<String> {
        // Return uuid for now which doesn't plan to be accelerating
        Some(uuid::Uuid::new_v4().to_string())
    }

    fn analyzer(&self) -> Option<Arc<datafusion::optimizer::Analyzer>> {
        self.get_federation_provider_for_accelerator()
            .and_then(|x| x.analyzer())
    }
}

pub struct FederationProviderAdapter {
    pub name: String,
    pub inner: Arc<dyn FederationProvider>,
}

impl FederationProviderAdapter {
    fn new(name: String, inner: Arc<dyn FederationProvider>) -> Self {
        Self { name, inner }
    }
}

impl FederationProvider for FederationProviderAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn compute_context(&self) -> Option<String> {
        self.inner.compute_context()
    }

    fn analyzer(&self) -> Option<Arc<datafusion::optimizer::Analyzer>> {
        self.inner.analyzer()
    }
}

pub struct SomeTableSource {
    provider: Arc<FederationProviderAdapter>,
    schema: SchemaRef,
}

impl SomeTableSource {
    pub fn new_with_schema(
        provider: Arc<FederationProviderAdapter>,
        schema: SchemaRef,
    ) -> DataFusionResult<Self> {
        Ok(Self { provider, schema })
    }
}

impl FederatedTableSource for SomeTableSource {
    fn federation_provider(&self) -> Arc<dyn FederationProvider> {
        Arc::clone(&self.provider) as Arc<dyn FederationProvider>
    }
}

impl TableSource for SomeTableSource {
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
