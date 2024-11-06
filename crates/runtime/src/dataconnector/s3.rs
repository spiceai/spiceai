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
    listing::{self, ListingTableConnector},
    DataConnector, DataConnectorFactory, DataConnectorResult, ParameterSpec, Parameters,
};

use crate::component::dataset::Dataset;
use crate::parameters::ParamLookup;
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
    #[snafu(display("No AWS access secret provided for credentials"))]
    NoAccessSecret,

    #[snafu(display("No AWS access key provided for credentials"))]
    NoAccessKey,

    #[snafu(display("Unable to parse URL {url}: {source}"))]
    UnableToParseURL {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display("Unsupported S3 authentication method '{method}', supported methods are: 'public' (i.e. no auth), 'iam_role', and 'key'."))]
    UnsupportedAuthenticationMethod { method: String },

    #[snafu(display(
        "The 's3_key' parameter cannot be set unless the `s3_auth` parameter is set to 'key'."
    ))]
    InvalidKeyAuthCombination,
}

pub struct S3 {
    params: Parameters,
}

#[derive(Default, Copy, Clone)]
pub struct S3Factory {}

impl S3Factory {
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
    ParameterSpec::connector("region").secret(),
    ParameterSpec::connector("endpoint").secret(),
    ParameterSpec::connector("key").secret(),
    ParameterSpec::connector("secret").secret(),
    ParameterSpec::connector("auth").description("Configures the authentication method for S3. Supported methods are: public (i.e. no auth), iam_role, key.").secret(),
    ParameterSpec::runtime("client_timeout")
        .description("The timeout setting for S3 client."),
    ParameterSpec::runtime("allow_http")
        .description("Allow HTTP protocol for S3 endpoint."),

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
    ParameterSpec::runtime("hive_partitioning_enabled")
        .description("Enable partitioning using hive-style partitioning from the folder structure. Defaults to false."),
];

impl DataConnectorFactory for S3Factory {
    fn create(
        &self,
        mut params: Parameters,
        _metadata: Option<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        if let Some(endpoint) = params.get("endpoint").expose().ok() {
            if endpoint.ends_with('/') {
                tracing::warn!("Trimming trailing '/' from S3 endpoint {endpoint}");
                params.insert(
                    "endpoint".to_string(),
                    endpoint.trim_end_matches('/').to_string().into(),
                );
            }
        }

        Box::pin(async move {
            if let Some(auth) = params.get("auth").expose().ok() {
                if auth != "public" && auth != "iam_role" && auth != "key" {
                    return Err(Box::new(Error::UnsupportedAuthenticationMethod {
                        method: auth.to_string(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }

                if matches!(params.get("key"), ParamLookup::Present(_)) && auth != "key" {
                    return Err(Box::new(Error::InvalidKeyAuthCombination)
                        as Box<dyn std::error::Error + Send + Sync>);
                }
            }
            let s3 = S3 { params };
            Ok(Arc::new(s3) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "s3"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

impl std::fmt::Display for S3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s3")
    }
}

impl ListingTableConnector for S3 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let mut s3_url =
            Url::parse(&dataset.from)
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!("{} is not a valid URL", dataset.from),
                })?;

        s3_url.set_fragment(Some(&listing::build_fragments(
            &self.params,
            vec![
                "region",
                "endpoint",
                "key",
                "secret",
                "client_timeout",
                "allow_http",
                "auth",
            ],
        )));

        Ok(s3_url)
    }
}
