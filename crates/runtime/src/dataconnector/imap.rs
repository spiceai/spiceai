/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use async_trait::async_trait;
use data_components::imap::ImapTableProvider;
use datafusion::datasource::TableProvider;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use snafu::ResultExt;
use std::{any::Any, future::Future, pin::Pin, sync::Arc};
use url::Url;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    InvalidConfigurationSnafu, ParameterSpec, Parameters,
};

pub struct Imap {
    params: Parameters,
}

#[derive(Default, Copy, Clone)]
pub struct ImapFactory {}

impl ImapFactory {
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
    ParameterSpec::connector("host"),
];

impl DataConnectorFactory for ImapFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Ok(Arc::new(Imap {
                params: params.parameters,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "imap"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Imap {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let client = imap::ClientBuilder::new(&self.params.get("host").expose().ok().unwrap(), 993)
            .connect()
            .unwrap();

        let session = client
            .login(
                &self.params.get("username").expose().ok().unwrap(),
                &self.params.get("password").expose().ok().unwrap(),
            )
            .unwrap();

        Ok(Arc::new(ImapTableProvider::new(session)))
    }
}
