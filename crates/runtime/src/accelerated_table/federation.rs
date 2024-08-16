use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use data_components::sandwich::SandwichTableProvider;
use datafusion::datasource::TableType;
use datafusion::error::Result as DataFusionResult;
use datafusion::{datasource::TableProvider, logical_expr::TableSource};
use datafusion_federation::{
    FederatedTableProviderAdaptor, FederatedTableSource, FederationProvider,
};

use super::AcceleratedTable;

impl AcceleratedTable {
    fn get_federation_provider_for_accelerator(&self) -> Option<Arc<dyn FederationProvider>> {
        let sandwich = self
            .accelerator
            .as_any()
            .downcast_ref::<SandwichTableProvider>()?;

        Some(Arc::new(sandwich.clone()))
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
        dbg!("here");
        let table_source = self.create_federated_table_source()?;
        Ok(FederatedTableProviderAdaptor::new_with_provider(
            table_source,
            self,
        ))
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
        let a = self.inner.compute_context();
        dbg!(&a);
        a
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
