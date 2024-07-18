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
use secrecy::SecretString;
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};
use url::Url;

use super::{
    DataConnector, DataConnectorFactory, DataConnectorResult, InvalidConfigurationSnafu,
    ListingTableConnector,
};

pub struct File {
    params: HashMap<String, SecretString>,
}

impl std::fmt::Display for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "file")
    }
}

#[derive(Default, Copy, Clone)]
pub struct FileFactory {}

impl FileFactory {
    pub fn new() -> Self {
        Self {}
    }

    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for FileFactory {
    fn create(
        &self,
        params: HashMap<String, SecretString>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(File { params }) as Arc<dyn DataConnector>) })
    }

    fn prefix(&self) -> &'static str {
        "file"
    }

    fn autoload_secrets(&self) -> &'static [&'static str] {
        &[]
    }
}

impl ListingTableConnector for File {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &HashMap<String, SecretString> {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let clean_from = dataset.from.replace("file://", "file:/");

        Url::parse(&clean_from)
            .boxed()
            .context(InvalidConfigurationSnafu {
                dataconnector: "File".to_string(),
                message: "Invalid URL".to_string(),
            })
    }
}
