/*
Copyright 2024 The Spice.ai OSS Authors

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

//! Data connector that reads from datasets already registered in Spice.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::sql::TableReference;

use crate::component::dataset::Dataset;
use crate::datafusion::DataFusion;
use crate::DataConnector;

#[derive(Clone)]
pub struct LocalConnector {
    datafusion: Arc<DataFusion>,
}

impl LocalConnector {
    #[must_use]
    pub fn new(datafusion: Arc<DataFusion>) -> Self {
        Self { datafusion }
    }
}

#[async_trait]
impl DataConnector for LocalConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset.path();
        let path_table_ref = TableReference::parse_str(&path);
        self.datafusion.get_table(&path_table_ref).await.ok_or(
            super::DataConnectorError::InvalidTableName {
                dataconnector: "local".to_string(),
                dataset_name: dataset.name.to_string(),
                table_name: path_table_ref.to_string(),
            },
        )
    }
}
