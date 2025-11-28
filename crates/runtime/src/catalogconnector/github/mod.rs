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
    dataconnector::{ConnectorComponent, parameters::ConnectorParams},
    parameters::ParameterSpec,
};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider as _;
use std::any::Any;
use std::sync::Arc;

mod provider;

use provider::GitHubCatalogProvider;

pub static PREFIX: &str = "github";

/// Parameters for the GitHub catalog connector
pub static PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("token")
        .description("A GitHub personal access token.")
        .secret(),
    ParameterSpec::component("client_id")
        .description("The GitHub App Client ID.")
        .secret(),
    ParameterSpec::component("private_key")
        .description("The GitHub App private key.")
        .secret(),
    ParameterSpec::component("installation_id")
        .description("The GitHub App installation ID.")
        .secret(),
    ParameterSpec::component("endpoint")
        .description("The GitHub API endpoint.")
        .default("https://api.github.com"),
];

/// The resource type to fetch from GitHub (pulls, issues, or projects)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitHubResourceType {
    Pulls,
    Issues,
    Projects,
}

impl std::fmt::Display for GitHubResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pulls => write!(f, "pulls"),
            Self::Issues => write!(f, "issues"),
            Self::Projects => write!(f, "projects"),
        }
    }
}

impl std::str::FromStr for GitHubResourceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pulls" => Ok(Self::Pulls),
            "issues" => Ok(Self::Issues),
            "projects" => Ok(Self::Projects),
            _ => Err(format!(
                "Invalid GitHub resource type: {s}. Supported values are 'pulls', 'issues', 'projects'."
            )),
        }
    }
}

/// A catalog connector for GitHub, providing access to organization repositories' issues, PRs, and projects.
#[derive(Clone)]
pub struct GitHubCatalog {
    params: ConnectorParams,
}

impl GitHubCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self { params })
    }
}

#[async_trait]
impl CatalogConnector for GitHubCatalog {
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
            GitHubCatalogProvider::new(self.params.clone(), catalog, runtime, app)
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
