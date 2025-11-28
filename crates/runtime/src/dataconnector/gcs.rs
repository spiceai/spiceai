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

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, ParameterSpec, Parameters,
    listing::{self, ListingTableConnector},
    parameters::Validator,
};

use crate::{
    Runtime, component::dataset::Dataset, dataconnector::listing::LISTING_TABLE_PARAMETERS,
};

use snafu::prelude::*;
use std::any::Any;
use std::clone::Clone;
use std::future::Future;
use std::pin::Pin;
use std::string::String;
use std::sync::{Arc, LazyLock};
use url::Url;

static PREFIX: &str = "gcs";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "GCS auth method 'service_account' requires a service account key. Specify a service account key with the `gcs_service_account_key` parameter or a path with `gcs_service_account_path`. For details, visit: https://spiceai.org/docs/components/data-connectors/gcs#auth"
    ))]
    NoServiceAccountKey,

    #[snafu(display(
        "Unsupported GCS auth method '{method}'. Use 'public', 'application_default', or 'service_account' for `gcs_auth` parameter. For details, visit: https://spiceai.org/docs/components/data-connectors/gcs#auth"
    ))]
    UnsupportedAuthenticationMethod { method: String },

    #[snafu(display(
        "The '{parameter}' parameter requires `gcs_auth` set to '{auth}'. For details, visit: https://spiceai.org/docs/components/data-connectors/gcs#auth"
    ))]
    InvalidAuthParameterCombination { parameter: String, auth: String },

    #[snafu(display(
        "The `gcs_endpoint` parameter must be a HTTP/S URL, but '{endpoint}' was provided. For details, visit: https://spiceai.org/docs/components/data-connectors/gcs#params"
    ))]
    InvalidEndpoint { endpoint: String },

    #[snafu(display(
        "Application default credentials authentication failed. Are you sure you're running in an environment with application default credentials? {source} For details, visit: https://spiceai.org/docs/components/data-connectors/gcs#auth"
    ))]
    InvalidApplicationDefaultAuthentication {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "The '{endpoint}' is a HTTP URL, but `allow_http` is not enabled. Set the parameter `allow_http: true` and retry. For details, visit: https://spiceai.org/docs/components/data-connectors/gcs#params"
    ))]
    InsecureEndpointWithoutAllowHTTP { endpoint: String },
}

pub struct Gcs {
    pub(crate) params: Parameters,
    pub(crate) runtime: Option<Runtime>,
    pub(crate) tokio_io_runtime: tokio::runtime::Handle,
}

impl std::fmt::Debug for Gcs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Gcs(params: {:?})", self.params)
    }
}

#[derive(Default, Copy, Clone)]
pub struct GcsFactory {}

impl GcsFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

pub(crate) static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    let mut all_parameters = Vec::new();
    all_parameters.extend_from_slice(&[
            ParameterSpec::component("bucket").secret(),
            ParameterSpec::component("service_account_key").secret(),
            ParameterSpec::component("service_account_path").secret(),
            ParameterSpec::component("application_credentials").secret(),
            ParameterSpec::component("auth")
                .description("Configures the authentication method for GCS. Supported methods are: public (i.e. no auth), application_default, service_account.")
                .secret(),
            ParameterSpec::component("endpoint").secret(),
            ParameterSpec::runtime("client_timeout")
                .description("The timeout setting for GCS client."),
            ParameterSpec::runtime("allow_http")
                .description("Allow HTTP protocol for GCS endpoint.")
        ]);
    all_parameters.extend_from_slice(LISTING_TABLE_PARAMETERS);
    all_parameters
});

// Validator for GCS authentication
struct GcsAuthValidator;

#[async_trait::async_trait]
impl Validator for GcsAuthValidator {
    type Error = Error;

    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Self::Error> {
        let auth = params
            .parameters
            .get("auth")
            .expose()
            .ok()
            .unwrap_or("public");

        match auth {
            "public" | "application_default" | "service_account" => {
                // Valid auth methods
            }
            method => {
                return Err(Error::UnsupportedAuthenticationMethod {
                    method: method.to_string(),
                });
            }
        }

        // Validate service_account auth requirements
        if auth == "service_account" {
            let has_key = params.parameters.get("service_account_key").expose().ok();
            let has_path = params.parameters.get("service_account_path").expose().ok();

            if !has_key && !has_path {
                return Err(Error::NoServiceAccountKey);
            }
        }

        // Validate that service account parameters are only used with service_account auth
        if auth != "service_account" {
            if params.parameters.get("service_account_key").expose().ok() {
                return Err(Error::InvalidAuthParameterCombination {
                    parameter: "gcs_service_account_key".to_string(),
                    auth: "service_account".to_string(),
                });
            }
            if params.parameters.get("service_account_path").expose().ok() {
                return Err(Error::InvalidAuthParameterCombination {
                    parameter: "gcs_service_account_path".to_string(),
                    auth: "service_account".to_string(),
                });
            }
        }

        // Validate that application_credentials is only used with application_default auth
        if auth != "application_default"
            && params
                .parameters
                .get("application_credentials")
                .expose()
                .ok()
        {
            return Err(Error::InvalidAuthParameterCombination {
                parameter: "gcs_application_credentials".to_string(),
                auth: "application_default".to_string(),
            });
        }

        Ok(())
    }
}

// Validator for GCS endpoint
struct GcsEndpointValidator;

#[async_trait::async_trait]
impl Validator for GcsEndpointValidator {
    type Error = Error;

    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Self::Error> {
        if let Some(endpoint) = params.parameters.get("endpoint").expose().ok() {
            let url = Url::parse(endpoint).map_err(|_| Error::InvalidEndpoint {
                endpoint: endpoint.to_string(),
            })?;

            if url.scheme() != "http" && url.scheme() != "https" {
                return Err(Error::InvalidEndpoint {
                    endpoint: endpoint.to_string(),
                });
            }

            // Check for HTTP without allow_http
            if url.scheme() == "http" {
                let allow_http = params
                    .parameters
                    .get("allow_http")
                    .expose()
                    .ok()
                    .and_then(|v| v.parse::<bool>().ok())
                    .unwrap_or(false);

                if !allow_http {
                    return Err(Error::InsecureEndpointWithoutAllowHTTP {
                        endpoint: endpoint.to_string(),
                    });
                }
            }
        }

        Ok(())
    }
}

static VALIDATORS: LazyLock<Vec<Box<dyn Validator<Error = Error> + Send + Sync + 'static>>> =
    LazyLock::new(|| vec![Box::new(GcsAuthValidator), Box::new(GcsEndpointValidator)]);

impl DataConnectorFactory for GcsFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        mut params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        if let Some(endpoint) = params.parameters.get("endpoint").expose().ok()
            && endpoint.ends_with('/')
        {
            tracing::warn!("Trimming trailing '/' from GCS endpoint {endpoint}");
            params.parameters.insert(
                "endpoint".to_string(),
                endpoint.trim_end_matches('/').to_string().into(),
            );
        }

        Box::pin(async move {
            for validator in VALIDATORS.iter() {
                validator.validate(&mut params).await?;
            }

            let gcs = Gcs {
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

impl std::fmt::Display for Gcs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{PREFIX}")
    }
}

impl ListingTableConnector for Gcs {
    fn object_versioning_type(
        &self,
    ) -> Option<datafusion::parquet::arrow::async_reader::ObjectVersionType> {
        // GCS doesn't use the same versioning type as S3
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_tokio_io_runtime(&self) -> tokio::runtime::Handle {
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
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!("The specified URL is not valid: {url}. Ensure the URL is valid and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/{PREFIX}#from"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;

        gcs_url.set_fragment(Some(&listing::build_fragments(
            &self.params,
            vec![
                "bucket",
                "service_account_key",
                "service_account_path",
                "application_credentials",
                "endpoint",
                "client_timeout",
                "allow_http",
                "auth",
            ],
        )));

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
                if self.params.get("auth").expose().ok() == Some("application_default") {
                    let err = Error::InvalidApplicationDefaultAuthentication { source };

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
