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

use crate::component::dataset::Dataset;
use async_trait::async_trait;
use data_components::delta_lake::DeltaTableFactory;
use data_components::Read;
use datafusion::datasource::TableProvider;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorFactory, ParameterSpec,
    Parameters,
};

pub struct DeltaLake {
    delta_table_factory: DeltaTableFactory,
}

impl DeltaLake {
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(params: Parameters) -> Self {
        Self {
            delta_table_factory: DeltaTableFactory::new(params.to_secret_map()),
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

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::runtime("client_timeout")
        .description("The timeout setting for object store client."),
    // S3 storage options
    ParameterSpec::connector("aws_region")
        .description("The AWS region to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("aws_access_key_id")
        .description("The AWS access key ID to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("aws_secret_access_key")
        .description("The AWS secret access key to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("aws_endpoint")
        .description("The AWS endpoint to use for S3 storage.")
        .secret(),
    // Azure storage options
    ParameterSpec::connector("azure_storage_account_name")
        .description("The storage account to use for Azure storage.")
        .secret(),
    ParameterSpec::connector("azure_storage_account_key")
        .description("The storage account key to use for Azure storage.")
        .secret(),
    ParameterSpec::connector("azure_storage_client_id")
        .description("The service principal client id for accessing the storage account.")
        .secret(),
    ParameterSpec::connector("azure_storage_client_secret")
        .description("The service principal client secret for accessing the storage account.")
        .secret(),
    ParameterSpec::connector("azure_storage_sas_key")
        .description("The shared access signature key for accessing the storage account.")
        .secret(),
    ParameterSpec::connector("azure_storage_endpoint")
        .description("The endpoint for the Azure Blob storage account.")
        .secret(),
    // GCS storage options
    ParameterSpec::connector("google_service_account")
        .description("Filesystem path to the Google service account JSON key file.")
        .secret(),
];

impl DataConnectorFactory for DeltaLakeFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let delta = DeltaLake::new(params.parameters);
            Ok(Arc::new(delta) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "delta_lake"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
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
            connector_component: ConnectorComponent::from(dataset),
        })?)
    }
}
