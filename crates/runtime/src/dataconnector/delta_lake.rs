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

use crate::component::dataset::Dataset;
use async_trait::async_trait;
use data_components::delta_lake::DeltaTableFactory;
use data_components::Read;
use datafusion::datasource::TableProvider;
use secrecy::SecretString;
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorFactory};

pub struct DeltaLake {
    delta_table_factory: DeltaTableFactory,
}

impl DeltaLake {
    #[must_use]
    pub fn new(params: HashMap<String, SecretString>) -> Self {
        Self {
            delta_table_factory: DeltaTableFactory::new(params),
        }
    }
}

#[derive(Default, Copy, Clone)]
pub struct DeltaLakeFactory {}

impl DeltaLakeFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for DeltaLakeFactory {
    fn create(
        &self,
        params: HashMap<String, SecretString>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let delta = DeltaLake::new(params);
            Ok(Arc::new(delta) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "delta_lake"
    }

    fn autoload_secrets(&self) -> &'static [&'static str] {
        &[
            // S3 Parameters
            "aws_region",
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_endpoint",
            // Azure Parameters
            "azure_storage_account_name",
            "azure_storage_account_key",
            "azure_storage_client_id",
            "azure_storage_client_secret",
            "azure_storage_sas_key",
            "azure_storage_endpoint",
            // Google Storage Parameters
            "google_service_account",
        ]
    }
}

#[async_trait]
impl DataConnector for DeltaLake {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Read::table_provider(
            &self.delta_table_factory,
            dataset.path().into(),
            dataset.schema(),
        )
        .await
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "delta_lake",
        })?)
    }
}
