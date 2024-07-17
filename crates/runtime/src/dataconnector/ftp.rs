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
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};
use url::{form_urlencoded, Url};

use super::{DataConnector, DataConnectorFactory, DataConnectorResult, ListingTableConnector};

pub struct FTP {
    params: HashMap<String, SecretString>,
}

impl std::fmt::Display for FTP {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FTP")
    }
}

impl DataConnectorFactory for FTP {
    fn create(
        params: HashMap<String, SecretString>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let ftp = Self { params };
            Ok(Arc::new(ftp) as Arc<dyn DataConnector>)
        })
    }
}

impl ListingTableConnector for FTP {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &HashMap<String, SecretString> {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let mut fragments = vec![];
        let mut fragment_builder = form_urlencoded::Serializer::new(String::new());

        if let Some(ftp_port) = self.params.get("ftp_port").map(ExposeSecret::expose_secret) {
            fragment_builder.append_pair("port", ftp_port);
        }
        if let Some(ftp_user) = self.params.get("ftp_user").map(ExposeSecret::expose_secret) {
            fragment_builder.append_pair("user", ftp_user);
        }
        if let Some(ftp_password) = self.params.get("ftp_pass").map(ExposeSecret::expose_secret) {
            fragment_builder.append_pair("password", &ftp_password);
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
