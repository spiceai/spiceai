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

use async_trait::async_trait;
use data_components::{Read, ReadWrite};
use ns_lookup::verify_endpoint_connection;
use secrets::Secret;
use std::any::Any;
use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorFactory};
use data_components::databricks::Databricks;

impl DataConnectorFactory for Databricks {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let url: String = params
                .as_ref() // &Option<HashMap<String, String>>
                .as_ref() // Option<&HashMap<String, String>>
                .and_then(|params| params.get("endpoint").cloned())
                .ok_or_else(|| super::Error::UnableToCreateDataConnector {
                    source: "Missing required parameter: endpoint".into(),
                })?;

            verify_endpoint_connection(&url)
                .await
                .map_err(|e| super::Error::UnableToCreateDataConnector { source: e.into() })?;

            Ok(Box::new(Databricks::new(Arc::new(secret), params)) as Box<dyn DataConnector>)
        })
    }
}

pub type Result<T, E = Box<dyn Error + Send + Sync>> = std::result::Result<T, E>;

#[async_trait]
impl DataConnector for Databricks {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn read_provider(&self) -> &dyn Read {
        self
    }

    fn write_provider(&self) -> Option<&dyn ReadWrite> {
        Some(self)
    }
}
