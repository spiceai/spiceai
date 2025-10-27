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
    Runtime, catalogconnector::CATALOG_CONNECTOR_FACTORY_REGISTRY, parameters::Parameters,
};
use runtime_secrets::{Secrets, get_params_with_secrets};

use super::{
    ConnectorComponent, DATA_CONNECTOR_FACTORY_REGISTRY, DataConnectorError, ODBC_DATACONNECTOR,
};

pub(crate) mod aws;

#[async_trait]
pub(crate) trait Validator {
    type Error;

    /// Parameters may be changed while validating.
    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Self::Error>;
}

#[derive(Clone)]
pub struct ConnectorParams {
    pub(crate) parameters: Parameters,
    pub(crate) unsupported_type_action: Option<UnsupportedTypeAction>,
    pub(crate) component: ConnectorComponent,
    pub(crate) app: Option<Arc<App>>,
    pub(crate) runtime: Option<Arc<Runtime>>,
    pub(crate) io_runtime: Handle,
}

pub struct ConnectorParamsBuilder {
    connector: Arc<str>,
    component: ConnectorComponent,
}

impl ConnectorParamsBuilder {
    #[must_use]
    pub fn new(connector: Arc<str>, component: ConnectorComponent) -> Self {
        Self {
            connector,
            component,
        }
    }

    pub async fn build(
        self,
        secrets: Arc<RwLock<Secrets>>,
        io_runtime: Handle,
    ) -> Result<ConnectorParams, Box<dyn std::error::Error + Send + Sync>> {
        let name = self.connector.to_string();
        let mut unsupported_type_action = None;
        let (params, prefix, parameters, app, runtime) = match &self.component {
            ConnectorComponent::Catalog(catalog) => {
                let guard = CATALOG_CONNECTOR_FACTORY_REGISTRY.lock().await;
                let connector_factory = guard.get(&name);

                let factory =
                    connector_factory.ok_or_else(|| DataConnectorError::InvalidConnectorType {
                        dataconnector: name.clone(),
                        connector_component: self.component.clone(),
                    })?;

                (
                    get_params_with_secrets(Arc::clone(&secrets), &catalog.params).await,
                    factory.prefix(),
                    factory.parameters(),
                    Some(catalog.app()),
                    Some(catalog.runtime()),
                )
            }
            ConnectorComponent::Dataset(dataset) => {
                let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;
                let connector_factory = guard.get(&name);

                unsupported_type_action = dataset.unsupported_type_action;

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

                let params = get_params_with_secrets(Arc::clone(&secrets), &dataset.params).await;

                (
                    params,
                    factory.prefix(),
                    factory.parameters(),
                    Some(dataset.app()),
                    Some(dataset.runtime()),
                )
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
            app,
            runtime,
            io_runtime,
        })
    }
}
