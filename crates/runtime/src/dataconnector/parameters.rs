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

use std::sync::Arc;

use app::App;
use async_trait::async_trait;
use datafusion_table_providers::UnsupportedTypeAction;
use tokio::{runtime::Handle, sync::RwLock};

use crate::{
    Runtime,
    catalogconnector::CATALOG_CONNECTOR_FACTORY_REGISTRY,
    component::{catalog::Catalog, dataset::Dataset},
    parameters::Parameters,
};
use runtime_secrets::{Secrets, get_params_with_secrets};

use super::{
    ConnectorComponent, DATA_CONNECTOR_FACTORY_REGISTRY, DataConnectorError, ODBC_DATACONNECTOR,
};

// `pub` (not `pub(crate)`): the AWS config helper is used by extracted AWS
// connector crates (e.g. connector-dynamodb) as well as in-tree ones (glue).
pub mod aws;
pub mod azure;
pub mod gcs;

#[async_trait]
pub trait Validator {
    type Error;

    /// Parameters may be changed while validating.
    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Self::Error>;
}

/// The runtime capabilities a data connector may reach for while it is being
/// built, behind a handle so [`ConnectorParams`] does not name them directly.
/// A connector's *configuration* travels separately, as the
/// [`ConnectorComponent`] spec.
pub trait ConnectorContext: Send + Sync {
    /// The loaded app, for the runtime-level configuration a connector consults
    /// (e.g. `runtime.params`).
    fn app(&self) -> Arc<App>;

    /// The live runtime, for connectors that register object stores or reach
    /// runtime-wide registries during construction.
    fn runtime(&self) -> Arc<Runtime>;
}

/// [`ConnectorContext`] over the app + runtime handles a component carries.
pub struct RuntimeConnectorContext {
    app: Arc<App>,
    runtime: Arc<Runtime>,
}

impl RuntimeConnectorContext {
    #[must_use]
    pub fn new(app: Arc<App>, runtime: Arc<Runtime>) -> Self {
        Self { app, runtime }
    }
}

impl ConnectorContext for RuntimeConnectorContext {
    fn app(&self) -> Arc<App> {
        Arc::clone(&self.app)
    }

    fn runtime(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime)
    }
}

#[derive(Clone)]
pub struct ConnectorParams {
    pub parameters: Parameters,
    pub unsupported_type_action: Option<UnsupportedTypeAction>,
    pub component: ConnectorComponent,
    /// `None` only where no runtime is attached — connector unit tests that
    /// build params directly.
    pub context: Option<Arc<dyn ConnectorContext>>,
    pub io_runtime: Handle,
}

impl ConnectorParams {
    /// The loaded app, if a runtime is attached.
    #[must_use]
    pub fn app(&self) -> Option<Arc<App>> {
        self.context.as_ref().map(|ctx| ctx.app())
    }

    /// The live runtime, if one is attached.
    #[must_use]
    pub fn runtime(&self) -> Option<Arc<Runtime>> {
        self.context.as_ref().map(|ctx| ctx.runtime())
    }
}

pub struct ConnectorParamsBuilder {
    connector: Arc<str>,
    component: ConnectorComponent,
    context: Option<Arc<dyn ConnectorContext>>,
}

impl ConnectorParamsBuilder {
    /// Parameters for a data connector serving `dataset`.
    #[must_use]
    pub fn for_dataset(connector: Arc<str>, dataset: &Dataset) -> Self {
        Self {
            connector,
            component: ConnectorComponent::from(dataset),
            context: Some(Arc::new(RuntimeConnectorContext::new(
                dataset.app(),
                dataset.runtime(),
            ))),
        }
    }

    /// Parameters for a catalog connector serving `catalog`.
    #[must_use]
    pub fn for_catalog(connector: Arc<str>, catalog: &Catalog) -> Self {
        Self {
            connector,
            component: ConnectorComponent::from(catalog),
            context: Some(Arc::new(RuntimeConnectorContext::new(
                catalog.app(),
                catalog.runtime(),
            ))),
        }
    }

    pub async fn build(
        self,
        secrets: Arc<RwLock<Secrets>>,
        io_runtime: Handle,
    ) -> Result<ConnectorParams, Box<dyn std::error::Error + Send + Sync>> {
        let name = self.connector.to_string();
        let mut unsupported_type_action = None;
        let (params, prefix, parameters) = match &self.component {
            ConnectorComponent::Catalog(catalog) => {
                let (prefix, parameters) = {
                    let guard = CATALOG_CONNECTOR_FACTORY_REGISTRY.lock().await;
                    let connector_factory = guard.get(&name);

                    let factory = connector_factory.ok_or_else(|| {
                        DataConnectorError::InvalidConnectorType {
                            dataconnector: name.clone(),
                            connector_component: self.component.clone(),
                        }
                    })?;

                    (factory.prefix(), factory.parameters())
                };

                (
                    get_params_with_secrets(Arc::clone(&secrets), &catalog.params).await,
                    prefix,
                    parameters,
                )
            }
            ConnectorComponent::Dataset(dataset) => {
                unsupported_type_action = dataset.unsupported_type_action;

                let (prefix, parameters) = {
                    let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;
                    let connector_factory = guard.get(&name);

                    let factory = connector_factory.ok_or_else(|| {
                        if name == ODBC_DATACONNECTOR {
                            DataConnectorError::OdbcNotInstalled {
                                connector_component: self.component.clone(),
                            }
                        } else {
                            DataConnectorError::InvalidConnectorType {
                                dataconnector: name.clone(),
                                connector_component: self.component.clone(),
                            }
                        }
                    })?;

                    (factory.prefix(), factory.parameters())
                };

                let params = get_params_with_secrets(Arc::clone(&secrets), &dataset.params).await;

                (params, prefix, parameters)
            }
        };

        let parameters = Parameters::try_new(
            &format!("connector {name}"),
            params.into_iter().collect(),
            prefix,
            secrets,
            parameters,
        )
        .await?;

        Ok(ConnectorParams {
            parameters,
            unsupported_type_action: unsupported_type_action.map(UnsupportedTypeAction::from),
            component: self.component,
            context: self.context,
            io_runtime,
        })
    }
}
