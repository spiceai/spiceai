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

use runtime::component::dataset::Dataset;
use runtime::dataconnector::listing::{self, LISTING_TABLE_PARAMETERS, ListingTableConnector};
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorFactory, DataConnectorResult,
    NewDataConnectorResult,
};
use runtime::parameters::{ParameterSpec, Parameters};
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use url::Url;

#[derive(Debug)]
pub struct SMB {
    params: Parameters,
}

impl std::fmt::Display for SMB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "smb")
    }
}

#[derive(Default, Copy, Clone)]
pub struct SMBFactory {}

impl SMBFactory {
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
        ParameterSpec::component("user")
            .secret()
            .description("The username for SMB authentication."),
        ParameterSpec::component("pass")
            .secret()
            .description("The password for SMB authentication."),
        ParameterSpec::component("port").description("The port to connect to. Defaults to 445."),
        ParameterSpec::runtime("client_timeout")
            .description("The timeout setting for SMB client connections."),
    ]);
    all_parameters.extend_from_slice(LISTING_TABLE_PARAMETERS);
    all_parameters
});

impl std::fmt::Debug for SMBFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SMBFactory").finish()
    }
}

impl DataConnectorFactory for SMBFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let smb = SMB {
                params: params.parameters,
            };
            Ok(Arc::new(smb) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "smb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
    }
}

impl ListingTableConnector for SMB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_tokio_io_runtime(&self) -> tokio::runtime::Handle {
        tokio::runtime::Handle::current()
    }

    fn get_object_store_url(
        &self,
        dataset: &Dataset,
        url: Option<&str>,
    ) -> DataConnectorResult<Url> {
        let url = url.unwrap_or(dataset.from.as_str());
        let mut smb_url = Url::parse(url).boxed().map_err(|source| {
            runtime::dataconnector::DataConnectorError::InvalidConfiguration {
                dataconnector: format!("{self}"),
                message: format!("The specified URL is not valid: {url}. Ensure the URL is valid and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/smb"),
                connector_component: ConnectorComponent::from(dataset),
                source,
            }
        })?;

        smb_url.set_fragment(Some(&listing::build_fragments(
            &self.params,
            vec!["port", "user", "pass", "client_timeout"],
        )));

        Ok(smb_url)
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "smb";

/// Returns a new instance of the `SMB` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    SMBFactory::new_arc()
}
