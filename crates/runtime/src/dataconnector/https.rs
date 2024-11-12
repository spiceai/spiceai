/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this Https except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use crate::component::dataset::Dataset;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use url::Url;

use super::DataConnectorParams;
use super::{
    listing::{self, ListingTableConnector},
    DataConnector, DataConnectorError, DataConnectorFactory, DataConnectorResult, ParameterSpec,
    Parameters,
};

pub struct Https {
    params: Parameters,
}

impl std::fmt::Display for Https {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "https")
    }
}

#[derive(Default, Copy, Clone)]
pub struct HttpsFactory {}

impl HttpsFactory {
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
    ParameterSpec::connector("username").secret(),
    ParameterSpec::connector("password").secret(),
    ParameterSpec::connector("port").description("The port to connect to."),
    ParameterSpec::runtime("client_timeout")
        .description("The timeout setting for HTTP(S) client."),

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

impl DataConnectorFactory for HttpsFactory {
    fn create(
        &self,
        params: DataConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Ok(Arc::new(Https {
                params: params.parameters,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "http"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

impl ListingTableConnector for Https {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let mut u = Url::parse(&dataset.from).boxed().map_err(|e| {
            DataConnectorError::InvalidConfiguration {
                dataconnector: "https".to_string(),
                message: format!("Invalid URL: {e}"),
                source: e,
            }
        })?;

        if let Some(p) = self.params.get("port").expose().ok() {
            let n = match p.parse::<u16>() {
                Ok(n) => n,
                Err(e) => {
                    return Err(DataConnectorError::InvalidConfiguration {
                        dataconnector: "https".to_string(),
                        message: format!(
                            "Invalid `{}` parameter: {e}",
                            self.params.user_param("port")
                        ),
                        source: Box::new(e),
                    });
                }
            };
            let _ = u.set_port(Some(n));
        };

        if let Some(p) = self.params.get("password").expose().ok() {
            if u.set_password(Some(p)).is_err() {
                return Err(
                    DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                        dataconnector: "https".to_string(),
                    },
                );
            };
        }

        if let Some(p) = self.params.get("username").expose().ok() {
            if u.set_username(p).is_err() {
                return Err(
                    DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                        dataconnector: "https".to_string(),
                    },
                );
            };
        }

        u.set_fragment(Some(&listing::build_fragments(
            &self.params,
            vec!["client_timeout"],
        )));

        Ok(u)
    }
}
