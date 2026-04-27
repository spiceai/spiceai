/*
Copyright 2026 The Spice.ai OSS Authors

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

use runtime::dataconnector::listing::{ListingTableConnector, build_fragments};
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, ParameterSpec, Parameters,
    parameters::{Validator, gcs::GcsAuthValidator},
};

use datafusion::parquet::arrow::async_reader::ObjectVersionType;
use runtime::{
    Runtime, component::dataset::Dataset, dataconnector::listing::LISTING_TABLE_PARAMETERS,
};
use snafu::prelude::*;
use std::any::Any;
use std::clone::Clone;
use std::future::Future;
use std::pin::Pin;
use std::string::String;
use std::sync::{Arc, LazyLock};
use tokio::runtime::Handle;
use url::Url;

static PREFIX: &str = "gcs";

static VALIDATORS: LazyLock<
    Vec<
        Box<
            dyn Validator<Error = runtime::dataconnector::parameters::gcs::Error>
                + Send
                + Sync
                + 'static,
        >,
    >,
> = LazyLock::new(|| vec![Box::new(GcsAuthValidator)]);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "The specified URL is not valid: {url}. Ensure the URL is valid and try again. {source}"
    ))]
    UnableToParseURL {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display(
        "GCS service account authentication failed. Verify your service account credentials are correct. {source} For details, visit: https://spiceai.org/docs/components/data-connectors/gcs#auth"
    ))]
    ServiceAccountAuthenticationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "GCS application default credentials authentication failed. Ensure you have valid ADC configured. {source} For details, visit: https://spiceai.org/docs/components/data-connectors/gcs#auth"
    ))]
    ApplicationDefaultCredentialsAuthenticationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub struct GoogleCloudStorage {
    params: Parameters,
    runtime: Option<Runtime>,
    tokio_io_runtime: Handle,
}

impl std::fmt::Debug for GoogleCloudStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GoogleCloudStorage(params: {:?})", self.params)
    }
}

#[derive(Default, Clone)]
pub struct GoogleCloudStorageFactory {}

impl GoogleCloudStorageFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    let mut all_parameters = Vec::new();
    all_parameters.extend_from_slice(&[
        ParameterSpec::component("service_account_path")
            .description("Path to a GCS service account JSON key file.")
            .secret(),
        ParameterSpec::component("service_account_key")
            .description("GCS service account JSON key as a string.")
            .secret(),
        ParameterSpec::component("application_default_credentials")
            .description("Use Google Application Default Credentials for authentication. If GOOGLE_APPLICATION_CREDENTIALS env var is set, uses that path.")
            .is_boolean()
            .default("false"),
        ParameterSpec::runtime("allow_http")
            .description("Allow insecure HTTP connections.")
            .is_boolean()
            .default("false"),
        ParameterSpec::component("max_retries")
            .description("The maximum number of retries.")
            .default("3"),
        ParameterSpec::component("retry_timeout")
            .description("Retry timeout."),
        ParameterSpec::component("backoff_initial_duration")
            .description("Initial backoff duration."),
        ParameterSpec::component("backoff_max_duration")
            .description("Maximum backoff duration."),
        ParameterSpec::component("backoff_base")
            .description("The base of the exponential to use"),
        ParameterSpec::component("skip_signature")
            .description("Skip signing requests. Used for public buckets.")
            .is_boolean(),
        ParameterSpec::runtime("client_timeout")
            .description("The timeout setting for GCS client."),
    ]);
    all_parameters.extend_from_slice(LISTING_TABLE_PARAMETERS);
    all_parameters
});

impl DataConnectorFactory for GoogleCloudStorageFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        mut params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = runtime::dataconnector::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            // Run all validators
            for validator in VALIDATORS.iter() {
                validator.validate(&mut params).await?;
            }

            let gcs = GoogleCloudStorage {
                params: params.parameters,
                runtime: params.runtime.map(Arc::unwrap_or_clone),
                tokio_io_runtime: params.io_runtime,
            };
            Ok(Arc::new(gcs) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        PREFIX
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
    }
}

impl std::fmt::Display for GoogleCloudStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{PREFIX}")
    }
}

impl ListingTableConnector for GoogleCloudStorage {
    fn object_versioning_type(&self) -> Option<ObjectVersionType> {
        // GCS uses generation numbers for versioning, not directly supported yet
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_tokio_io_runtime(&self) -> Handle {
        self.tokio_io_runtime.clone()
    }

    fn get_object_store_url(
        &self,
        dataset: &Dataset,
        url: Option<&str>,
    ) -> DataConnectorResult<Url> {
        let url = url.unwrap_or(dataset.from.as_str());

        let mut gcs_url =
            Url::parse(url)
                .boxed()
                .context(runtime::dataconnector::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!("The specified URL is not valid: {url}. Ensure the URL is valid and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/{PREFIX}#from"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;

        let params = build_fragments(
            &self.params,
            vec![
                "service_account_path",
                "service_account_key",
                "application_default_credentials",
                "allow_http",
                "max_retries",
                "retry_timeout",
                "backoff_initial_duration",
                "backoff_max_duration",
                "backoff_base",
                "skip_signature",
                "client_timeout",
            ],
        );
        gcs_url.set_fragment(Some(&params));
        Ok(gcs_url)
    }

    fn get_runtime(&self) -> Option<Runtime> {
        self.runtime.clone()
    }

    fn handle_object_store_error(
        &self,
        dataset: &Dataset,
        error: object_store::Error,
    ) -> DataConnectorError {
        match error {
            object_store::Error::Generic { source, .. } => {
                // Try to provide more specific error messages based on auth method
                let has_service_account_path = self
                    .params
                    .get("service_account_path")
                    .expose()
                    .ok()
                    .is_some();
                let has_service_account_key = self
                    .params
                    .get("service_account_key")
                    .expose()
                    .ok()
                    .is_some();
                let has_application_default_credentials = self
                    .params
                    .get("application_default_credentials")
                    .expose()
                    .ok()
                    .is_some_and(|v| v.eq_ignore_ascii_case("true"));

                if has_service_account_path || has_service_account_key {
                    let err = Error::ServiceAccountAuthenticationFailed { source };
                    DataConnectorError::InvalidConfiguration {
                        dataconnector: format!("{self}"),
                        message: format!("{err}"),
                        connector_component: ConnectorComponent::from(dataset),
                        source: err.into(),
                    }
                } else if has_application_default_credentials {
                    let err = Error::ApplicationDefaultCredentialsAuthenticationFailed { source };
                    DataConnectorError::InvalidConfiguration {
                        dataconnector: format!("{self}"),
                        message: format!("{err}"),
                        connector_component: ConnectorComponent::from(dataset),
                        source: err.into(),
                    }
                } else {
                    DataConnectorError::UnableToConnectInternal {
                        dataconnector: format!("{self}"),
                        connector_component: ConnectorComponent::from(dataset),
                        source,
                    }
                }
            }
            error => DataConnectorError::UnableToConnectInternal {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
                source: error.into(),
            },
        }
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "gcs";

/// Returns a new instance of the `GoogleCloudStorage` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    GoogleCloudStorageFactory::new_arc()
}
