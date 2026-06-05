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
use provider::GlueDataConnectorFactory;
use std::any::Any;
use std::sync::{Arc, LazyLock, OnceLock};

mod provider;

use provider::GlueCatalogProvider;

pub static PREFIX: &str = "glue";

/// Global slot for the Glue data connector factory, populated by `spiced` at startup.
/// Using `OnceLock` avoids a circular dependency: `catalogconnector/glue.rs` can't import
/// `connector_glue::GlueDataConnector` (that crate depends on runtime).
static GLUE_DATA_CONNECTOR_FACTORY: OnceLock<GlueDataConnectorFactory> = OnceLock::new();

/// Registers the factory used by the Glue catalog connector to create Glue data connectors.
/// Must be called before any Glue catalog datasets are loaded.
/// Typically called from `bin/spiced` after `connector-glue` is registered.
pub fn register_glue_data_connector_factory(factory: GlueDataConnectorFactory) {
    // Silently ignore if already set (idempotent for hot-reloads).
    let _ = GLUE_DATA_CONNECTOR_FACTORY.set(factory);
}

/// Combined parameter spec for the Glue catalog connector:
/// the `catalog_id` parameter plus all S3 parameters (region, key, secret, etc.).
pub static PARAMETERS: LazyLock<Vec<crate::parameters::ParameterSpec>> = LazyLock::new(|| {
    let mut params = Vec::new();
    params.push(
        crate::parameters::ParameterSpec::component("catalog_id")
            .description(
                "Optional AWS Glue catalog ID (account ID). Defaults to the caller's account.",
            )
            .secret(),
    );
    params.extend_from_slice(crate::dataconnector::s3::PARAMETERS.as_ref());
    params
});

static VALIDATORS: LazyLock<
    Vec<Box<dyn Validator<Error = parameters::aws::Error> + Send + Sync + 'static>>,
> = LazyLock::new(|| vec![Box::new(RegionValidator), Box::new(AuthValidator)]);

type DatabaseName = String;

/// A catalog connector for AWS Glue, providing access to database and table metadata.
#[derive(Clone)]
pub struct GlueCatalog {
    params: ConnectorParams,
    data_connector_factory: GlueDataConnectorFactory,
}

impl GlueCatalog {
    #[must_use]
    pub fn new_connector_with_factory(
        params: ConnectorParams,
        data_connector_factory: GlueDataConnectorFactory,
    ) -> Arc<dyn CatalogConnector> {
        Arc::new(Self {
            params,
            data_connector_factory,
        })
    }

    /// Creates a Glue catalog connector using the factory registered via
    /// [`register_glue_data_connector_factory`]. Panics if no factory has been registered.
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        let factory = GLUE_DATA_CONNECTOR_FACTORY
            .get()
            .cloned()
            .unwrap_or_else(|| {
                // Provide a clearly-erroring fallback so misconfigured builds fail loudly.
                Arc::new(
                    |_params, _io_runtime| -> Arc<dyn crate::dataconnector::DataConnector> {
                        panic!(
                            "GlueCatalog: no data connector factory registered. \
                         Call register_glue_data_connector_factory() before using the Glue catalog."
                        )
                    },
                )
            });
        Arc::new(Self {
            params,
            data_connector_factory: factory,
        })
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
            GlueCatalogProvider::new(
                self.params.clone(),
                catalog,
                runtime,
                app,
                Arc::clone(&self.data_connector_factory),
            )
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
