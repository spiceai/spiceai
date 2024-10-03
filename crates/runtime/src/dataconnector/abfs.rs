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

use super::{
    DataConnector, DataConnectorFactory, DataConnectorResult, ListingTableConnector, ParameterSpec,
    Parameters,
};

use crate::component::dataset::Dataset;
use snafu::prelude::*;
use std::any::Any;
use std::clone::Clone;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::string::String;
use std::sync::Arc;
use url::Url;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse URL {url}: {source}"))]
    UnableToParseURL {
        url: String,
        source: url::ParseError,
    },
    #[snafu(display(
        "Provide only one of the following: access key, bearer token, or client credentials. Use skip_signature to disable all authentication."
    ))]
    InvalidKeyAuthCombination,
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

const PARAMETERS: &[ParameterSpec] = &[
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
    ParameterSpec::connector("allow_http")
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

    // Common listing table parameters
    ParameterSpec::runtime("file_format"),
    ParameterSpec::runtime("file_extension"),
    ParameterSpec::runtime("csv_has_header")
        .description("Set true to indicate that the first line is a header."),
    ParameterSpec::runtime("csv_quote").description("The quote character in a row."),
    ParameterSpec::runtime("csv_escape").description("The escape character in a row."),
    ParameterSpec::runtime("csv_schema_infer_max_records")
        .description("Set a limit in terms of records to scan to infer the schema."),
    ParameterSpec::runtime("csv_delimiter")
        .description("The character separating values within a row."),
    ParameterSpec::runtime("file_compression_type")
        .description("The type of compression used on the file. Supported types are: gzip, bzip2, xz, zstd, uncompressed"),
];

impl DataConnectorFactory for AzureBlobFSFactory {
    fn create(
        &self,
        mut params: Parameters,
        _metadata: Option<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        if let Some(sas_token) = params.get("sas_string").expose().ok() {
            if let Some(sas_token) = sas_token.strip_prefix('?') {
                params.insert("sas_string".to_string(), sas_token.to_string().into());
            }
        }

        Box::pin(async move {
            let access_key = params.get("access_key").expose().ok();
            let bearer_token = params.get("bearer_token").expose().ok();
            let sas_string = params.get("sas_string").expose().ok();
            let skip_signature = params.get("skip_signature").expose().ok();
            let use_emulator = params.get("use_emulator").expose().ok();

            let use_emulator = use_emulator.is_some_and(|b| b.parse::<bool>().unwrap_or(false));

            if use_emulator {
                let azure = AzureBlobFS { params };
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
                    let azure = AzureBlobFS { params };
                    Ok(Arc::new(azure) as Arc<dyn DataConnector>)
                }
            }
        })
    }

    fn prefix(&self) -> &'static str {
        "abfs"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
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
                    message: format!("{} is not a valid URL", &dataset.from),
                })?;

        let params = super::build_fragments(
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
