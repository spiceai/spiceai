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

use secrets::{get_secret_or_param, Secret};
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};
use url::{form_urlencoded, Url};

use super::{DataConnector, DataConnectorFactory, DataConnectorResult, ListingTableConnector};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse URL {url}: {source}"))]
    UnableToParseURL {
        url: String,
        source: url::ParseError,
    },
}

pub struct SFTP {
    secret: Option<Secret>,
    params: HashMap<String, String>,
}

impl std::fmt::Display for SFTP {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SFTP")
    }
}

impl DataConnectorFactory for SFTP {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let sftp = Self {
                secret,
                params: params.as_ref().clone().map_or_else(HashMap::new, |x| x),
            };
            Ok(Arc::new(sftp) as Arc<dyn DataConnector>)
        })
    }
}

impl ListingTableConnector for SFTP {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &HashMap<String, String> {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let mut fragments = vec![];
        let mut fragment_builder = form_urlencoded::Serializer::new(String::new());

        if let Some(sftp_port) = self.params.get("sftp_port") {
            fragment_builder.append_pair("port", sftp_port);
        }
        if let Some(sftp_user) = self.params.get("sftp_user") {
            fragment_builder.append_pair("user", sftp_user);
        }
        if let Some(sftp_password) = get_secret_or_param(
            Some(&self.params),
            &self.secret,
            "sftp_pass_key",
            "sftp_pass",
        ) {
            fragment_builder.append_pair("password", &sftp_password);
        }
        fragments.push(fragment_builder.finish());

        let mut ftp_url =
            Url::parse(&dataset.from)
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: "ftp".to_string(),
                    message: format!("{} is not a valid URL", dataset.from),
                })?;

        if dataset.from.ends_with('/') {
            fragments.push("dfiscollectionbugworkaround=hack/".into());
        }

        ftp_url.set_fragment(Some(&fragments.join("&")));

        Ok(ftp_url)
    }
}
