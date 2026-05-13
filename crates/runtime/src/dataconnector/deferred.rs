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

use super::DataConnector;
use crate::component::{ComponentInitialization, dataset::Dataset};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Constraints, project_schema},
    datasource::{TableProvider, TableType},
    logical_expr::Expr,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, empty::EmptyExec},
};

#[derive(Clone)]
pub struct DeferredConnector {
    schema: SchemaRef,
    inner: Arc<dyn DataConnector>,
}

impl DeferredConnector {
    pub fn new(inner: Arc<dyn DataConnector>) -> Self {
        Self {
            inner,
            schema: Arc::new(Schema::new(vec![Field::new(
                "placeholder",
                DataType::Utf8,
                false,
            )])),
        }
    }

    #[must_use]
    pub fn source(&self) -> Arc<dyn DataConnector> {
        Arc::clone(&self.inner)
    }
}

#[async_trait]
impl DataConnector for DeferredConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Arc::new(self.clone()))
    }

    async fn read_write_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    async fn register_object_stores(
        &self,
        dataset: &Dataset,
        runtime_env: &Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> super::DataConnectorResult<()> {
        self.inner
            .register_object_stores(dataset, runtime_env)
            .await
    }

    fn resolve_refresh_mode(
        &self,
        refresh_mode: Option<crate::component::dataset::acceleration::RefreshMode>,
    ) -> crate::component::dataset::acceleration::RefreshMode {
        self.inner.resolve_refresh_mode(refresh_mode)
    }

    fn supports_changes_stream(&self) -> bool {
        false
    }

    fn changes_stream(
        &self,
        _federated_table: Arc<crate::federated_table::FederatedTable>,
        _dataset: &Dataset,
        _accelerated_table_provider: Arc<dyn TableProvider>,
        _accelerator_write_mutex: Arc<tokio::sync::Mutex<()>>,
        _cpu_runtime: Option<tokio::runtime::Handle>,
    ) -> Option<data_components::cdc::ChangesStream> {
        None
    }

    fn supports_append_stream(&self) -> bool {
        false
    }

    fn append_stream(
        &self,
        _federated_table: Arc<crate::federated_table::FederatedTable>,
    ) -> Option<data_components::cdc::ChangesStream> {
        None
    }

    async fn metadata_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    async fn on_accelerator_setup(
        &self,
        dataset: &Dataset,
        builder: &mut crate::accelerated_table::Builder,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.on_accelerator_setup(dataset, builder).await
    }

    async fn on_accelerated_table_registration(
        &self,
        dataset: &Dataset,
        accelerated_table: &mut crate::accelerated_table::AcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner
            .on_accelerated_table_registration(dataset, accelerated_table)
            .await
    }

    fn metrics_provider(
        &self,
    ) -> Option<Arc<dyn crate::component::metrics::MetricsProvider>> {
        self.inner.metrics_provider()
    }

    fn initialization(&self) -> ComponentInitialization {
        ComponentInitialization::OnTrigger
    }

    fn initialization_for_dataset(&self, _dataset: &Dataset) -> ComponentInitialization {
        ComponentInitialization::OnTrigger
    }
}

#[async_trait]
impl TableProvider for DeferredConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(project_schema(
            &self.schema,
            projection,
        )?)))
    }
}

impl std::fmt::Debug for DeferredConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeferredConnector")
    }
}

impl DisplayAs for DeferredConnector {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeferredConnector")
    }
}
