/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use super::CatalogConnector;
use crate::{
    Runtime,
    component::catalog::Catalog,
    dataconnector::{
        ConnectorComponent,
        parameters::{
            self, ConnectorParams, Validator,
            aws::{AuthValidator, RegionValidator},
        },
    },
};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider as _;
use std::any::Any;
use std::sync::{Arc, LazyLock};

mod provider;

use provider::GlueCatalogProvider;

pub static PREFIX: &str = "glue";

static VALIDATORS: LazyLock<
    Vec<Box<dyn Validator<Error = parameters::aws::Error> + Send + Sync + 'static>>,
> = LazyLock::new(|| vec![Box::new(RegionValidator), Box::new(AuthValidator)]);

type DatabaseName = String;

/// A catalog connector for AWS Glue, providing access to database and table metadata.
#[derive(Clone)]
pub struct GlueCatalog {
    params: ConnectorParams,
}

impl GlueCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self { params })
    }
}

#[async_trait]
impl CatalogConnector for GlueCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn data_components::RefreshableCatalogProvider>> {
        let app = match runtime.app.read().await.as_ref() {
            Some(app) => Arc::clone(app),
            None => {
                return Err(super::Error::FailedToGetAppFromRuntime {});
            }
        };

        let refreshable_provider = Arc::new(
            GlueCatalogProvider::new(self.params.clone(), catalog, runtime, app)
                .await
                .map_err(|e| super::Error::UnableToGetCatalogProvider {
                    connector: PREFIX.to_string(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })?,
        );

        refreshable_provider.refresh().await.map_err(|source| {
            super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: ConnectorComponent::from(catalog),
                source,
            }
        })?;

        Ok(refreshable_provider)
    }
}
