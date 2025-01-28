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

use crate::component::dataset::Dataset;
use crate::dataconnector::listing::LISTING_TABLE_PARAMETERS;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use url::Url;

use super::{listing, ConnectorComponent, ConnectorParams};
use super::{
    listing::ListingTableConnector, DataConnector, DataConnectorFactory, DataConnectorResult,
    ParameterSpec, Parameters,
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

static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    let mut all_parameters = Vec::new();
    all_parameters.extend_from_slice(&[
        ParameterSpec::connector("user").secret(),
        ParameterSpec::connector("pass").secret(),
        ParameterSpec::connector("port").description("The port to connect to."),
        ParameterSpec::runtime("client_timeout").description("The timeout setting for FTP client."),
    ]);
    all_parameters.extend_from_slice(LISTING_TABLE_PARAMETERS);
    all_parameters
});

impl DataConnectorFactory for FTPFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let ftp = FTP {
                params: params.parameters,
            };
            Ok(Arc::new(ftp) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "ftp"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
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
        let mut ftp_url =
            Url::parse(&dataset.from)
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!("{} is not a valid URL. Ensure the URL is valid and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/ftp", dataset.from),
                    connector_component: ConnectorComponent::from(dataset),
                })?;

        ftp_url.set_fragment(Some(&listing::build_fragments(
            &self.params,
            vec!["port", "user", "pass", "client_timeout"],
        )));

        Ok(ftp_url)
    }
}
