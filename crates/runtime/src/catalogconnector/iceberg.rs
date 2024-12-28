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

use super::{CatalogConnector, ConnectorComponent, ParameterSpec, Parameters, Result};
use crate::{
    component::catalog::Catalog, dataconnector::ConnectorParams, get_params_with_secrets, Runtime,
};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use secrecy::SecretString;
use std::{any::Any, collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct IcebergCatalog {
    params: Parameters,
}

impl IcebergCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self {
            params: params.parameters,
        })
    }
}

pub(crate) const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::connector("token").secret().description(
        "The personal access token used to authenticate against the Iceberg REST Catalog API.",
    ),
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

#[async_trait]
impl CatalogConnector for IcebergCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: &Runtime,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        todo!();

        //Ok(Arc::new(catalog_provider) as Arc<dyn RefreshableCatalogProvider>)
    }
}
