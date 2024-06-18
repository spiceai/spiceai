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

use super::{DataConnector, DataConnectorFactory, DataConnectorResult, ListingTableConnector};

use crate::component::dataset::Dataset;
use crate::secrets::Secret;
use snafu::prelude::*;
use std::any::Any;
use std::clone::Clone;
use std::pin::Pin;
use std::string::String;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};
use url::{form_urlencoded, Url};

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
}

pub struct S3 {
    secret: Option<Secret>,
    params: Arc<HashMap<String, String>>,
}

impl DataConnectorFactory for S3 {
    fn create(
        secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let s3 = Self { secret, params };
            Ok(Arc::new(s3) as Arc<dyn DataConnector>)
        })
    }
}

impl std::fmt::Display for S3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "S3")
    }
}

impl ListingTableConnector for S3 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &HashMap<String, String> {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let mut fragments = vec![];
        let mut fragment_builder = form_urlencoded::Serializer::new(String::new());

        if let Some(region) = self.params.get("region") {
            fragment_builder.append_pair("region", region);
        }
        if let Some(endpoint) = self.params.get("endpoint") {
            fragment_builder.append_pair("endpoint", endpoint);
        }
        if let Some(secret) = &self.secret {
            if let Some(key) = secret.get("key") {
                fragment_builder.append_pair("key", key);
            };
            if let Some(secret) = secret.get("secret") {
                fragment_builder.append_pair("secret", secret);
            };
        }
        if let Some(timeout) = self.params.get("timeout") {
            fragment_builder.append_pair("timeout", timeout);
        }
        fragments.push(fragment_builder.finish());

        let mut s3_url =
            Url::parse(&dataset.from)
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!("{} is not a valid URL", dataset.from),
                })?;

        // infer_schema has a bug using is_collection which is determined by if url contains suffix of /
        // using a fragment with / suffix to trick df to think this is still a collection
        // will need to raise an issue with DF to use url without query and fragment to decide if
        // is_collection
        // PR: https://github.com/apache/datafusion/pull/10419/files
        if dataset.from.ends_with('/') {
            fragments.push("dfiscollectionbugworkaround=hack/".into());
        }

        s3_url.set_fragment(Some(&fragments.join("&")));

        Ok(s3_url)
    }
}
