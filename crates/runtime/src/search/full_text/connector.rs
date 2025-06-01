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
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use std::any::Any;
use std::sync::Arc;

use crate::accelerated_table::AcceleratedTable;
use crate::component::{ComponentInitialization, dataset::Dataset, metrics::MetricsProvider};
use crate::dataconnector::{DataConnector, DataConnectorError, DataConnectorResult};

use super::table::TableWithFullText;

#[derive(Debug)]
pub struct FullTextConnector {
    inner_connector: Arc<dyn DataConnector>,
}

impl FullTextConnector {
    pub fn new(inner_connector: Arc<dyn DataConnector>) -> Self {
        Self { inner_connector }
    }

    /// Wrap an existing [`TableProvider`] with a [`TableWithFullText`] provider. If no embeddings
    /// are needed for the [`Dataset`], it is not unnecessarily nested.
    pub(crate) async fn wrap_table(
        &self,
        inner_table_provider: Arc<dyn TableProvider>,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let search_field_opt = dataset.columns.iter().find_map(|c| {
            if c.full_text_search.as_ref().is_some_and(|cfg| cfg.enabled) {
                let primary_key_overrides = c
                    .full_text_search
                    .as_ref()
                    .and_then(|cfg| cfg.row_ids.clone());
                Some((c.name.clone(), primary_key_overrides))
            } else {
                None
            }
        });

        let Some((search_field, primary_key_overrides)) = search_field_opt else {
            return Ok(inner_table_provider);
        };

        let tbl =
            TableWithFullText::try_new(inner_table_provider, search_field, primary_key_overrides)
                .await
                .map_err(|e| DataConnectorError::InvalidConfiguration {
                    dataconnector: dataset.source().to_string(),
                    message: e.to_string(),
                    connector_component: dataset.into(),
                    source: Box::new(e),
                })?;

        Ok(Arc::new(tbl) as Arc<dyn TableProvider>)
    }
}

#[async_trait]
impl DataConnector for FullTextConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        self.wrap_table(self.inner_connector.read_provider(dataset).await?, dataset)
            .await
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        match self.inner_connector.read_write_provider(dataset).await {
            Some(Ok(inner)) => Some(self.wrap_table(inner, dataset).await),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }

    async fn metadata_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        self.inner_connector.metadata_provider(dataset).await
    }

    fn initialization(&self) -> ComponentInitialization {
        self.inner_connector.initialization()
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        self.inner_connector.metrics_provider()
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
}
