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
use crate::secrets::{Secret, SecretMap};
use async_trait::async_trait;
use data_components::delta_lake::DeltaTableFactory;
use data_components::Read;
use datafusion::datasource::TableProvider;
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorFactory};

pub struct DeltaLake {
    delta_table_factory: DeltaTableFactory,
}

impl DeltaLake {
    #[must_use]
    pub fn new(secret: Option<Secret>, params: &Arc<HashMap<String, String>>) -> Self {
        let mut params: SecretMap = params.as_ref().into();

        if let Some(secret) = secret {
            for (key, value) in secret.iter() {
                params.insert(key.to_string(), value.clone());
            }
        }

        Self {
            delta_table_factory: DeltaTableFactory::new(Arc::new(params.into_map())),
        }
    }
}

impl DataConnectorFactory for DeltaLake {
    fn create(
        secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let delta = DeltaLake::new(secret, &params);
            Ok(Arc::new(delta) as Arc<dyn DataConnector>)
        })
    }
}

#[async_trait]
impl DataConnector for DeltaLake {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Read::table_provider(
            &self.delta_table_factory,
            dataset.path().into(),
            dataset.schema(),
        )
        .await
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "delta_lake",
        })?)
    }
}
