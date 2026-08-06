/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use async_trait::async_trait;
use data_components::cdc::ChangesStream;
use datafusion::datasource::TableProvider;
use futures::StreamExt;
use runtime_metrics::component::MetricsProvider;

use crate::accelerated_table::{self, AcceleratedTable};
use crate::component::{
    ComponentInitialization,
    dataset::{Dataset, acceleration::RefreshMode},
};
use crate::dataconnector::{DataConnector, DataConnectorResult};
use crate::drasi::{DeliveryMode, forward_change_envelope};
use crate::federated_table::FederatedTable;

/// A [`DataConnector`] middleware that publishes the wrapped connector's change
/// stream to a Drasi source before it reaches the accelerator.
///
/// Every other capability is delegated unchanged — the wrapper exists only to
/// decorate the stream. `missing_trait_methods` is denied so that a method added
/// to [`DataConnector`] later fails to compile here rather than silently
/// inheriting a default and dropping the inner connector's behavior.
#[derive(Debug)]
pub(crate) struct DrasiConnector {
    inner_connector: Arc<dyn DataConnector>,
    delivery: DeliveryMode,
}

impl DrasiConnector {
    pub(crate) fn new(inner_connector: Arc<dyn DataConnector>, delivery: DeliveryMode) -> Self {
        Self {
            inner_connector,
            delivery,
        }
    }

    fn with_forwarded_stream(&self, stream: Option<ChangesStream>) -> Option<ChangesStream> {
        let delivery = self.delivery.clone();
        Some(
            stream?
                .then(move |item| forward_change_envelope(item, delivery.clone()))
                .boxed(),
        )
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl DataConnector for DrasiConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        self.inner_connector.read_provider(dataset).await
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        self.inner_connector.read_write_provider(dataset).await
    }

    async fn metadata_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        self.inner_connector.metadata_provider(dataset).await
    }

    async fn register_object_stores(
        &self,
        dataset: &Dataset,
        runtime_env: &Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> DataConnectorResult<()> {
        self.inner_connector
            .register_object_stores(dataset, runtime_env)
            .await
    }

    fn initialization(&self) -> ComponentInitialization {
        self.inner_connector.initialization()
    }

    fn initialization_for_dataset(&self, dataset: &Dataset) -> ComponentInitialization {
        self.inner_connector.initialization_for_dataset(dataset)
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        self.inner_connector.metrics_provider()
    }

    async fn on_accelerator_setup(
        &self,
        dataset: &Dataset,
        builder: &mut accelerated_table::Builder,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner_connector
            .on_accelerator_setup(dataset, builder)
            .await
    }

    async fn on_accelerated_table_registration(
        &self,
        dataset: &Dataset,
        accelerated_table: &mut AcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner_connector
            .on_accelerated_table_registration(dataset, accelerated_table)
            .await
    }

    fn resolve_refresh_mode(&self, refresh_mode: Option<RefreshMode>) -> RefreshMode {
        self.inner_connector.resolve_refresh_mode(refresh_mode)
    }

    fn supports_durable_write_back_delivery(&self) -> bool {
        self.inner_connector.supports_durable_write_back_delivery()
    }

    fn supports_changes_stream(&self) -> bool {
        self.inner_connector.supports_changes_stream()
    }

    fn changes_stream(
        &self,
        federated_table: Arc<FederatedTable>,
        dataset: &Dataset,
    ) -> Option<ChangesStream> {
        self.with_forwarded_stream(self.inner_connector.changes_stream(federated_table, dataset))
    }

    fn supports_append_stream(&self) -> bool {
        self.inner_connector.supports_append_stream()
    }

    fn append_stream(&self, federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        self.with_forwarded_stream(self.inner_connector.append_stream(federated_table))
    }
}
