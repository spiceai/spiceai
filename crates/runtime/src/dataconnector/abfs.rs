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

use super::listing::{build_fragments, ListingTableConnector};
use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorFactory, DataConnectorResult,
    ParameterSpec, Parameters,
};

use crate::component::dataset::Dataset;
use crate::dataconnector::listing::LISTING_TABLE_PARAMETERS;
use snafu::prelude::*;
use std::any::Any;
use std::clone::Clone;
use std::future::Future;
use std::pin::Pin;
use std::string::String;
use std::sync::{Arc, LazyLock};
use url::Url;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "The specified URL is not valid: {url}.\nEnsure the URL is valid and try again.\n{source}"
    ))]
    UnableToParseURL {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display(
        "Multiple authentication methods were provided.\nSpecify only one of the following: access key, bearer token, or client credentials.\nUse skip_signature to disable all authentication."
    ))]
    InvalidKeyAuthCombination,

    #[snafu(display(
        "The 'abfs_endpoint' parameter must be a HTTP/S URL, but '{endpoint}' was provided.\nSpecify a valid HTTP/S URL."
    ))]
    InvalidEndpoint { endpoint: String },

    #[snafu(display("The '{endpoint}' is a HTTP URL, but `allow_http` is not enabled. Set the parameter `allow_http: true` and retry.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/abfs#params"))]
    InsecureEndpointWithoutAllowHTTP { endpoint: String },
}

pub struct AzureBlobFS {
    params: Parameters,
}

#[derive(Default, Clone)]
pub struct AzureBlobFSFactory {}

impl AzureBlobFSFactory {
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
        ParameterSpec::connector("account")
            .description("Azure Storage account name.")
            .secret(),
        ParameterSpec::connector("container_name")
            .description("Azure Storage container name.")
            .secret(),
        ParameterSpec::connector("access_key")
            .description("Azure Storage account access key.")
            .secret(),
        ParameterSpec::connector("bearer_token")
            .description("Bearer token to use in Azure requests.")
            .secret(),
        ParameterSpec::connector("client_id")
            .description("Azure client ID.")
            .secret(),
        ParameterSpec::connector("client_secret")
            .description("Azure client secret.")
            .secret(),
        ParameterSpec::connector("tenant_id")
            .description("Azure tenant ID.")
            .secret(),
        ParameterSpec::connector("sas_string")
            .description("Azure SAS string.")
            .secret(),
        ParameterSpec::connector("endpoint")
            .description("Azure Storage endpoint.")
            .secret(),
        ParameterSpec::connector("use_emulator")
            .description("Use the Azure Storage emulator.")
            .default("false"),
        ParameterSpec::connector("use_fabric_endpoint")
            .description("Use the Azure Storage fabric endpoint.")
            .default("false"),
        ParameterSpec::runtime("allow_http")
            .description("Allow insecure HTTP connections.")
            .default("false"),
        ParameterSpec::connector("authority_host")
            .description("Sets an alternative authority host."),
        ParameterSpec::connector("max_retries")
            .description("The maximum number of retries.")
            .default("3"),
        ParameterSpec::connector("retry_timeout")
            .description("Retry timeout."),
        ParameterSpec::connector("backoff_initial_duration")
            .description("Initial backoff duration."),
        ParameterSpec::connector("backoff_max_duration")
            .description("Maximum backoff duration."),
        ParameterSpec::connector("backoff_base")
            .description("The base of the exponential to use"),
        ParameterSpec::connector("proxy_url")
            .description("Proxy URL to use when connecting"),
        ParameterSpec::connector("proxy_ca_certificate")
            .description("CA certificate for the proxy.")
            .secret(),
        ParameterSpec::connector("proxy_excludes")
            .description("Set list of hosts to exclude from proxy connections"),
        ParameterSpec::connector("msi_endpoint")
            .description("Sets the endpoint for acquiring managed identity tokens.")
            .secret(),
        ParameterSpec::connector("federated_token_file")
            .description("Sets a file path for acquiring Azure federated identity token in Kubernetes"),
        ParameterSpec::connector("use_cli")
            .description("Set if the Azure CLI should be used for acquiring access tokens."),
        ParameterSpec::connector("skip_signature")
            .description("Skip fetching credentials and skip signing requests. Used for interacting with public containers."),
        ParameterSpec::connector("disable_tagging")
            .description("Ignore any tags provided to put_opts"),

    ]);
    all_parameters.extend_from_slice(LISTING_TABLE_PARAMETERS);
    all_parameters
});

impl DataConnectorFactory for AzureBlobFSFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        mut params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        if let Some(sas_token) = params.parameters.get("sas_string").expose().ok() {
            if let Some(sas_token) = sas_token.strip_prefix('?') {
                params
                    .parameters
                    .insert("sas_string".to_string(), sas_token.to_string().into());
            }
        }

        Box::pin(async move {
            if let Some(endpoint) = params.parameters.get("endpoint").expose().ok() {
                if !(endpoint.starts_with("https://") || endpoint.starts_with("http://")) {
                    return Err(Box::new(Error::InvalidEndpoint {
                        endpoint: endpoint.to_string(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }

                if endpoint.starts_with("http://")
                    && params.parameters.get("allow_http").expose().ok() != Some("true")
                {
                    return Err(Box::new(Error::InsecureEndpointWithoutAllowHTTP {
                        endpoint: endpoint.to_string(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }
            }

            let access_key = params.parameters.get("access_key").expose().ok();
            let bearer_token = params.parameters.get("bearer_token").expose().ok();
            let sas_string = params.parameters.get("sas_string").expose().ok();
            let skip_signature = params.parameters.get("skip_signature").expose().ok();
            let use_emulator = params.parameters.get("use_emulator").expose().ok();

            let use_emulator = use_emulator.is_some_and(|b| b.parse::<bool>().unwrap_or(false));

            if use_emulator {
                let azure = AzureBlobFS {
                    params: params.parameters,
                };
                Ok(Arc::new(azure) as Arc<dyn DataConnector>)
            } else {
                let conflicting = [
                    access_key.is_some(),
                    bearer_token.is_some(),
                    sas_string.is_some(),
                    skip_signature.is_some(),
                ];
                if conflicting.iter().filter(|b| **b).count() > 1 {
                    Err(Box::new(Error::InvalidKeyAuthCombination)
                        as Box<dyn std::error::Error + Send + Sync>)
                } else {
                    let azure = AzureBlobFS {
                        params: params.parameters,
                    };
                    Ok(Arc::new(azure) as Arc<dyn DataConnector>)
                }
            }
        })
    }

    fn prefix(&self) -> &'static str {
        "abfs"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
    }
}

impl std::fmt::Display for AzureBlobFS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "abfs")
    }
}

impl ListingTableConnector for AzureBlobFS {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let mut azure_url =
            Url::parse(&dataset.from)
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!("{} is not a valid URL. Ensure the URL is valid and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/abfs#from", &dataset.from),
                    connector_component: ConnectorComponent::from(dataset)
                })?;

        let params = build_fragments(
            &self.params,
            vec![
                "account",
                "container_name",
                "access_key",
                "bearer_token",
                "client_id",
                "client_secret",
                "tenant_id",
                "sas_string",
                "endpoint",
                "use_emulator",
                "use_fabric_endpoint",
                "allow_http",
                "authority_host",
                "max_retries",
                "retry_timeout",
                "backoff_initial_duration",
                "backoff_max_duration",
                "backoff_base",
                "proxy_url",
                "proxy_ca_certificate",
                "proxy_excludes",
                "msi_endpoint",
                "federated_token_file",
                "use_cli",
                "skip_signature",
                "disable_tagging",
            ],
        );
        azure_url.set_fragment(Some(&params));
        Ok(azure_url)
    }
}
