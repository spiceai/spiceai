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
use datafusion::datasource::TableProvider;
use secrets::Secret;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorFactory};
pub use data_components::databricks::Databricks;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: endpoint"))]
    MissingEndpoint,

    #[snafu(display("Endpoint {endpoint} is invalid: {source}"))]
    InvalidEndpoint {
        endpoint: String,
        source: ns_lookup::Error,
    },

    #[snafu(display("{source}"))]
    UnableToGetReadProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{source}"))]
    UnableToGetReadWriteProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl DataConnectorFactory for Databricks {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let databricks = Databricks::new(Arc::new(secret), params).await?;
            Ok(Arc::new(databricks) as Arc<dyn DataConnector>)
        })
    }
}

#[async_trait]
impl DataConnector for Databricks {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::AnyErrorResult<Arc<dyn TableProvider>> {
        Ok(Read::table_provider(self, dataset.path().into())
            .await
            .context(UnableToGetReadProviderSnafu)?)
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<super::AnyErrorResult<Arc<dyn TableProvider>>> {
        let read_write_result = ReadWrite::table_provider(self, dataset.path().into())
            .await
            .context(UnableToGetReadWriteProviderSnafu)
            .boxed();
        match read_write_result {
            Ok(provider) => Some(Ok(provider)),
            Err(e) => Some(Err(e)),
        }
    }
}
