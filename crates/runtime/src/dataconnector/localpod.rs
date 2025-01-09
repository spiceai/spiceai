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

//! Data connector that reads from datasets already registered in the current Spicepod.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::sql::TableReference;

use crate::datafusion::DataFusion;
use crate::DataConnector;
use crate::{component::dataset::Dataset, parameters::ParameterSpec};

use super::{ConnectorComponent, ConnectorParams, DataConnectorFactory};

pub const LOCALPOD_DATACONNECTOR: &str = "localpod";

#[derive(Default, Copy, Clone)]
pub struct LocalPodFactory {}

impl LocalPodFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for LocalPodFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Err(Box::new(super::DataConnectorError::Internal {
                dataconnector: LOCALPOD_DATACONNECTOR.to_string(),
                code: "LPF-Create".to_string(), // LocalPodFactory - Create
                source: "Unexpected error. Localpod connector should not be created directly from the factory.".into(),
                connector_component: params.component.clone()
            }) as Box<dyn std::error::Error + Send + Sync>)
        })
    }

    fn prefix(&self) -> &'static str {
        "localpod"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &[]
    }
}

#[derive(Clone)]
pub struct LocalPodConnector {
    datafusion: Arc<DataFusion>,
}

impl LocalPodConnector {
    #[must_use]
    pub fn new(datafusion: Arc<DataFusion>) -> Self {
        Self { datafusion }
    }
}

#[async_trait]
impl DataConnector for LocalPodConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset.path();
        let path_table_ref = TableReference::parse_str(path);
        self.datafusion.get_table(&path_table_ref).await.ok_or(
            super::DataConnectorError::InvalidTableName {
                dataconnector: LOCALPOD_DATACONNECTOR.to_string(),
                table_name: path_table_ref.to_string(),
                connector_component: ConnectorComponent::from(dataset),
            },
        )
    }
}
