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

use async_trait::async_trait;
use data_components::Read;
use data_components::delta_lake::DeltaTableFactory;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use runtime::component::dataset::Dataset;
use runtime::dataconnector::listing::build_table_parquet_options;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult,
};
use runtime::parameters::{ParameterSpec, Parameters};
use secrecy::ExposeSecret;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::runtime::Handle;

#[derive(Debug)]
pub struct DeltaLake {
    delta_table_factory: DeltaTableFactory,
    /// Retained so `register_object_stores` can encode the AWS/Azure/GCS
    /// params into the executor's object store registry URL fragment.
    params: Parameters,
}

impl DeltaLake {
    #[must_use]
    pub fn new(
        params: Parameters,
        io_runtime: Handle,
        table_parquet_options: TableParquetOptions,
    ) -> Self {
        Self {
            delta_table_factory: DeltaTableFactory::new(params.to_secret_map(), io_runtime)
                .with_table_parquet_options(table_parquet_options),
            params,
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
    ParameterSpec::component("aws_region")
        .description("The AWS region to use for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_access_key_id")
        .description("The AWS access key ID to use for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_secret_access_key")
        .description("The AWS secret access key to use for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_session_token")
        .description("The AWS session token to use for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_endpoint")
        .description("The AWS endpoint to use for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_allow_http")
        .description("The AWS endpoint allow http scheme")
        .secret(),
    // Azure storage options
    ParameterSpec::component("azure_storage_account_name")
        .description("The storage account to use for Azure storage.")
        .secret(),
    ParameterSpec::component("azure_storage_account_key")
        .description("The storage account key to use for Azure storage.")
        .secret(),
    ParameterSpec::component("azure_storage_client_id")
        .description("The service principal client id for accessing the storage account.")
        .secret(),
    ParameterSpec::component("azure_storage_client_secret")
        .description("The service principal client secret for accessing the storage account.")
        .secret(),
    ParameterSpec::component("azure_storage_tenant_id")
        .description("The service principal tenant id for accessing the storage account.")
        .secret(),
    ParameterSpec::component("azure_storage_sas_key")
        .description("The shared access signature key for accessing the storage account.")
        .secret(),
    ParameterSpec::component("azure_storage_endpoint")
        .description("The endpoint for the Azure Blob storage account.")
        .secret(),
    // GCS storage options
    ParameterSpec::component("google_service_account")
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
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
        let aws_region = params
            .parameters
            .get("aws_region")
            .expose()
            .ok()
            .map(ToString::to_string);
        let param_map = params.parameters.to_secret_map();
        Box::pin(async move {
            // Initialize AWS SDK credentials if not using explicit credentials
            if !aws_sdk_credential_bridge::has_explicit_credentials(
                &param_map,
                "aws_access_key_id",
                "aws_secret_access_key",
            ) && let Err(err) =
                aws_sdk_credential_bridge::get_or_init_sdk_config_with_region(aws_region.as_deref())
                    .await
            {
                tracing::warn!(
                    "Unable to initialize AWS credentials for Delta Lake connector: {err}"
                );
            }

            let parquet_opts = build_table_parquet_options(params.runtime.as_deref()).await?;

            tracing::debug!(
                ?parquet_opts,
                "Creating Delta Lake connector with parquet options"
            );
            let delta = DeltaLake::new(params.parameters, params.io_runtime, parquet_opts);
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
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        match Read::table_provider(&self.delta_table_factory, dataset.path().into()).await {
            Ok(provider) => Ok(provider),
            Err(e) => Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "delta_lake".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            }),
        }
    }

    /// Registers the underlying object store (S3/Azure/GCS) on the executor's
    /// `RuntimeEnv` so decoded `ParquetSource` plans, which lose their
    /// per-scan `parquet_file_reader_factory` during proto round-trip, can
    /// resolve the bucket via `runtime_env().object_store(url)` with the
    /// correct region/credentials.
    ///
    /// Without this, executors fall back to a default S3 store with no region
    /// set, which surfaces as `Received redirect without LOCATION` against
    /// buckets outside `us-east-1`.
    async fn register_object_stores(
        &self,
        dataset: &Dataset,
        runtime_env: &Arc<RuntimeEnv>,
    ) -> DataConnectorResult<()> {
        let storage_location = dataset.path();

        // Delta tables backed by a local filesystem (file://, file paths,
        // or relative paths like `my_delta_table`) don't need an object
        // store registration. Match the listing connector's behavior:
        // emit a warning so misconfigured cluster setups are diagnosable,
        // then no-op rather than failing executor startup.
        let parsed = match url::Url::parse(storage_location) {
            Ok(parsed) => parsed,
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                tracing::warn!(
                    "Dataset {} delta_lake path `{}` is not an absolute URL; \
                     skipping cluster object store registration. Cluster \
                     executors will not be able to resolve this path without \
                     a shared mount.",
                    dataset.name,
                    storage_location,
                );
                return Ok(());
            }
            Err(source) => {
                return Err(DataConnectorError::UnableToConnectInternal {
                    dataconnector: "delta_lake".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(source),
                });
            }
        };

        if parsed.scheme() == "file" {
            tracing::warn!(
                "Dataset {} has a file:// scheme and may not be resolvable on cluster executors without a shared mount.",
                dataset.name,
            );
            return Ok(());
        }

        // Encode the connector's storage params as the URL fragment so
        // `SpiceObjectStoreRegistry::get_store` can build the right object
        // store. `storage_registry_params` returns just the AWS/Azure/GCS
        // entries with their prefixed names rewritten to the registry's
        // canonical names.
        let storage_params = self.params.storage_registry_params();
        if storage_params.is_empty() {
            // Nothing connector-specific to register; leave the default
            // registry behavior in place.
            return Ok(());
        }

        let mut parsed = parsed;
        let mut fragment_builder = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in storage_params {
            fragment_builder.append_pair(&key, value.expose_secret());
        }
        parsed.set_fragment(Some(fragment_builder.finish().as_str()));

        let listing_url = ListingTableUrl::parse(parsed).map_err(|source| {
            DataConnectorError::UnableToConnectInternal {
                dataconnector: "delta_lake".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(source),
            }
        })?;

        runtime_env.object_store(&listing_url).map_err(|source| {
            DataConnectorError::UnableToConnectInternal {
                dataconnector: "delta_lake".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(source),
            }
        })?;

        let mut redacted = <ListingTableUrl as AsRef<url::Url>>::as_ref(&listing_url).clone();
        redacted.set_fragment(None);
        tracing::debug!(
            "Configured object storage for Delta Lake Dataset {} ({redacted})",
            dataset.name,
        );
        Ok(())
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "delta_lake";

/// Returns a new instance of the `Delta Lake` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    DeltaLakeFactory::new_arc()
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime::secrets::Secrets;
    use secrecy::SecretString;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn tenant_id_parameter_is_accepted_and_registered() {
        let parameters = Parameters::try_new(
            "connector delta_lake",
            vec![(
                "delta_lake_azure_storage_tenant_id".to_string(),
                SecretString::new("tenant-id".to_string().into()),
            )],
            "delta_lake",
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("tenant id should be accepted for delta_lake");

        assert_eq!(
            parameters.get("azure_storage_tenant_id").expose().ok(),
            Some("tenant-id")
        );

        let delta_table_options = parameters.to_secret_map();
        assert_eq!(
            delta_table_options
                .get("azure_storage_tenant_id")
                .map(ExposeSecret::expose_secret),
            Some("tenant-id")
        );

        let registry_params = parameters.storage_registry_params();
        let tenant_id = registry_params
            .iter()
            .find(|(key, _)| key == "tenant_id")
            .map(|(_, value)| value.expose_secret());

        assert_eq!(tenant_id, Some("tenant-id"));
    }
}
