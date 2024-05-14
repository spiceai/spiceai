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

use secrets::{AnyErrorResult, Secret};
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};
use url::{form_urlencoded, Url};

use super::{DataConnector, DataConnectorFactory, ListingTableConnector};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse URL {url}: {source}"))]
    UnableToParseURL {
        url: String,
        source: url::ParseError,
    },
}

pub struct FTP {
    secret: Option<Secret>,
    params: HashMap<String, String>,
}

impl DataConnectorFactory for FTP {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let ftp = Self {
                secret,
                params: params.as_ref().clone().map_or_else(HashMap::new, |x| x),
            };
            Ok(Arc::new(ftp) as Arc<dyn DataConnector>)
        })
    }
}

impl ListingTableConnector for FTP {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &HashMap<String, String> {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> AnyErrorResult<Url> {
        let mut fragments = vec![];
        let mut fragment_builder = form_urlencoded::Serializer::new(String::new());

        if let Some(ftp_port) = self.params.get("ftp_port") {
            fragment_builder.append_pair("port", ftp_port);
        }
        if let Some(ftp_user) = self.params.get("ftp_user") {
            fragment_builder.append_pair("user", ftp_user);
        }
        if let Some(ftp_password) =
            get_secret_or_param(Some(&self.params), &self.secret, "ftp_pass_key", "ftp_pass")
        {
            fragment_builder.append_pair("password", &ftp_password);
        }
        fragments.push(fragment_builder.finish());

        let mut ftp_url =
            Url::parse(&dataset.from).context(UnableToParseURLSnafu { url: &dataset.from })?;

        if dataset.from.clone().ends_with('/') {
            fragments.push("dfiscollectionbugworkaround=hack/".into());
        }

        ftp_url.set_fragment(Some(&fragments.join("&")));

        Ok(ftp_url)
    }
}

pub(crate) fn get_secret_or_param(
    params: Option<&HashMap<String, String>>,
    secret: &Option<Secret>,
    secret_param_key: &str,
    param_key: &str,
) -> Option<String> {
    let secret_param_val = match params.and_then(|p| p.get(secret_param_key)) {
        Some(val) => val,
        None => param_key,
    };

    if let Some(secrets) = secret {
        if let Some(secret_val) = secrets.get(secret_param_val) {
            return Some(secret_val.to_string());
        };
    };

    if let Some(param_val) = params.and_then(|p| p.get(param_key)) {
        return Some(param_val.to_string());
    };

    None
}
