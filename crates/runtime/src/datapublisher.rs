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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use data_components::Write;
use datafusion::common::OwnedTableReference;
use datafusion::execution::context::SessionContext;
use spicepod::component::dataset::Dataset;

use crate::dataupdate::DataUpdate;

#[async_trait]
pub trait DataPublisher: Send + Sync {
    async fn add_data(
        &self,
        dataset: Arc<Dataset>,
        data_update: DataUpdate,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

#[async_trait]
impl DataPublisher for dyn Write + '_ {
    async fn add_data(
        &self,
        dataset: Arc<Dataset>,
        data_update: DataUpdate,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let write_table_provider = self
            .table_provider(OwnedTableReference::from(dataset.name.clone()))
            .await?;

        let ctx = SessionContext::new();

        Ok(())
    }
}
