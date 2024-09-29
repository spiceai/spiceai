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
        "Only one of access_key, bearer token or client credentials must be provided"
    ))]
    InvalidKeyAuthCombination,
    #[snafu(display("Invalid value for {param}, expected {expected_type}"))]
    InvalidParamValue {
        param: String,
        expected_type: String,
    },
}

pub struct Azure {
    params: Parameters,
}

#[derive(Default, Copy, Clone)]
pub struct AzureFactory {}

impl AzureFactory {
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
        .description("The Azure Storage account name.")
        .secret(),
    ParameterSpec::connector("container_name")
        .description("The Azure Storage container name.")
        .secret(),
    ParameterSpec::connector("access_key")
        .description("The Azure Storage account access key.")
        .secret(),
    ParameterSpec::connector("bearer_token")
        .description("The bearer token to use in the Azure requests.")
        .secret(),
    ParameterSpec::connector("client_id")
        .description("The Azure client ID.")
        .secret(),
    ParameterSpec::connector("client_secret")
        .description("The Azure client secret.")
        .secret(),
    ParameterSpec::connector("tenant_id")
        .description("The Azure tenant ID.")
        .secret(),
    ParameterSpec::connector("sas_string")
        .description("The Azure SAS string.")
        .secret(),
    ParameterSpec::connector("endpoint")
        .description("The Azure Storage endpoint."),
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
        .description("The retry timeout."),
    ParameterSpec::connector("backoff_initial_duration")
        .description("The initial backoff duration."),
    ParameterSpec::connector("backoff_max_duration")
        .description("The maximum backoff duration."),
    ParameterSpec::connector("backoff_base")
        .description("The base of the exponential to use"),
    ParameterSpec::connector("proxy_url")
        .description("The proxy URL to use."),
    ParameterSpec::connector("proxy_ca_certificate")
        .description("The CA certificate for the proxy."),
    ParameterSpec::connector("proxy_excludes")
        .description("Set list of hosts to exclude from proxy connections"),
    ParameterSpec::connector("msi_endpoint")
        .description("Sets the endpoint for acquiring managed identity tokens."),
    ParameterSpec::connector("federated_token_file")
        .description("Sets a file path for acquiring Azure federated identity token in Kubernetes"),
    ParameterSpec::connector("use_cli")
        .description("Set if the Azure Cli should be used for acquiring access tokens."),
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
        .description("The type of compression used on the file. Supported types are: GZIP, BZIP2, XZ, ZSTD, UNCOMPRESSED"),
];

impl DataConnectorFactory for AzureFactory {
    fn create(
        &self,
        mut params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        if let Some(sas_token) = params.get("sas_string").expose().ok() {
            if sas_token.starts_with('?') {
                tracing::warn!("Removing leading '?' from SAS token");
                let sas_token = sas_token[1..].to_string();
                params.insert("sas_string".to_string(), sas_token.into());
            }
        }

        Box::pin(async move {
            let access_key = params.get("access_key").expose().ok();
            let bearer_token = params.get("bearer_token").expose().ok();
            let sas_string = params.get("sas_string").expose().ok();

            match (access_key, bearer_token, sas_string) {
                (Some(_), None, None)
                | (None, Some(_), None)
                | (None, None, Some(_))
                | (None, None, None) => {
                    let azure = Azure { params };
                    Ok(Arc::new(azure) as Arc<dyn DataConnector>)
                }
                _ => Err(Box::new(Error::InvalidKeyAuthCombination)
                    as Box<dyn std::error::Error + Send + Sync>),
            }
        })
    }

    fn prefix(&self) -> &'static str {
        "azure"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

impl std::fmt::Display for Azure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "azure")
    }
}

impl ListingTableConnector for Azure {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let prefixed_url = dataset.from.replace("azure:", "azure+");
        let mut azure_url =
            Url::parse(&prefixed_url)
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!("{} is not a valid URL", prefixed_url),
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
