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
use secrecy::ExposeSecret;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use url::{form_urlencoded, Url};

use super::{
    DataConnector, DataConnectorFactory, DataConnectorResult, ListingTableConnector, ParameterSpec,
    Parameters,
};

pub struct FTP {
    params: Parameters,
}

impl std::fmt::Display for FTP {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ftp")
    }
}

#[derive(Default, Copy, Clone)]
pub struct FTPFactory {}

impl FTPFactory {
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
    ParameterSpec::connector("user").secret(),
    ParameterSpec::connector("pass").secret(),
    ParameterSpec::connector("port").description("The port to connect to."),

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

impl DataConnectorFactory for FTPFactory {
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let ftp = FTP { params };
            Ok(Arc::new(ftp) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "ftp"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

impl ListingTableConnector for FTP {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let mut fragments = vec![];
        let mut fragment_builder = form_urlencoded::Serializer::new(String::new());

        if let Some(ftp_port) = self.params.get("port").map(ExposeSecret::expose_secret) {
            fragment_builder.append_pair("port", ftp_port);
        }
        if let Some(ftp_user) = self.params.get("user").map(ExposeSecret::expose_secret) {
            fragment_builder.append_pair("user", ftp_user);
        }
        if let Some(ftp_password) = self.params.get("pass").map(ExposeSecret::expose_secret) {
            fragment_builder.append_pair("password", ftp_password);
        }
        fragments.push(fragment_builder.finish());

        let mut ftp_url =
            Url::parse(&dataset.from)
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!("{} is not a valid URL", dataset.from),
                })?;

        if dataset.from.ends_with('/') {
            fragments.push("dfiscollectionbugworkaround=hack/".into());
        }

        ftp_url.set_fragment(Some(&fragments.join("&")));

        Ok(ftp_url)
    }
}
