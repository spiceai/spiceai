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

use crate::{
    catalogconnector::{self, get_catalog_provider, CatalogConnector},
    component::catalog::Catalog,
    dataconnector::ConnectorParamsBuilder,
    metrics, status, warn_spaced, LogErrors, Result, Runtime,
    UnableToInitializeCatalogConnectorSnafu, UnableToLoadCatalogConnectorSnafu,
};
use app::App;
use futures::future::join_all;
use snafu::prelude::*;
use util::{fibonacci_backoff::FibonacciBackoffBuilder, retry, RetryError};

impl Runtime {
    pub(crate) async fn load_catalogs(&self) {
        let app_lock = self.app.read().await;
        let Some(app) = app_lock.as_ref() else {
            return;
        };

        let valid_catalogs = Self::get_valid_catalogs(app, LogErrors(true));
        let mut futures = vec![];
        for catalog in &valid_catalogs {
            self.status
                .update_catalog(&catalog.name, status::ComponentStatus::Initializing);
            futures.push(self.load_catalog(catalog));
        }

        let _ = join_all(futures).await;
    }

    async fn load_catalog(&self, catalog: &Catalog) {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        let retry_strategy = FibonacciBackoffBuilder::new().max_retries(None).build();

        let _ = retry(retry_strategy, || async {
            let connector = match self.load_catalog_connector(catalog).await {
                Ok(connector) => connector,
                Err(err) => {
                    let catalog_name = &catalog.name;
                    self.status
                        .update_catalog(catalog_name, status::ComponentStatus::Error);
                    metrics::catalogs::LOAD_ERROR.add(1, &[]);
                    warn_spaced!(spaced_tracer, "{} {err}", catalog_name);
                    return Err(RetryError::transient(err));
                }
            };

            if let Err(err) = self.register_catalog(catalog, connector).await {
                tracing::error!("{err}");
                return Err(RetryError::transient(err));
            };

            self.status
                .update_catalog(&catalog.name, status::ComponentStatus::Ready);

            Ok(())
        })
        .await;
    }

    async fn load_catalog_connector(&self, catalog: &Catalog) -> Result<Arc<dyn CatalogConnector>> {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);
        let catalog = catalog.clone();

        let source = catalog.provider.clone();
        let params = ConnectorParamsBuilder::new(source.clone().into(), (&catalog).into())
            .build(self.secrets())
            .await
            .context(UnableToInitializeCatalogConnectorSnafu)?;

        let Some(catalog_connector) = catalogconnector::create_new_connector(&source, params).await
        else {
            let catalog_name = &catalog.name;
            self.status
                .update_catalog(catalog_name, status::ComponentStatus::Error);
            metrics::catalogs::LOAD_ERROR.add(1, &[]);
            let err = crate::Error::UnknownCatalogConnector {
                catalog_connector: source,
            };
            warn_spaced!(spaced_tracer, "{} {err}", catalog_name);
            return Err(err);
        };

        Ok(catalog_connector)
    }

    fn catalogs_iter<'a>(app: &Arc<App>) -> impl Iterator<Item = Result<Catalog>> + 'a {
        app.catalogs.clone().into_iter().map(Catalog::try_from)
    }

    /// Returns a list of valid catalogs from the given App, skipping any that fail to parse and logging an error for them.
    pub(crate) fn get_valid_catalogs(app: &Arc<App>, log_errors: LogErrors) -> Vec<Catalog> {
        Self::catalogs_iter(app)
            .zip(&app.catalogs)
            .filter_map(|(catalog, spicepod_catalog)| match catalog {
                Ok(catalog) => Some(catalog),
                Err(e) => {
                    if log_errors.0 {
                        metrics::catalogs::LOAD_ERROR.add(1, &[]);
                        tracing::error!(catalog = &spicepod_catalog.name, "{e}");
                    }
                    None
                }
            })
            .collect()
    }

    async fn register_catalog(
        &self,
        catalog: &Catalog,
        catalog_connector: Arc<dyn CatalogConnector>,
    ) -> Result<()> {
        tracing::info!(
            "Registering catalog '{}' for {}",
            &catalog.name,
            &catalog.provider
        );
        let catalog_provider = get_catalog_provider(catalog_connector, self, catalog, None)
            .await
            .boxed()
            .context(UnableToInitializeCatalogConnectorSnafu)?;
        let num_schemas = catalog_provider
            .schema_names()
            .iter()
            .fold(0, |acc, schema| {
                acc + catalog_provider
                    .schema(schema)
                    .map_or(0, |s| i32::from(!s.table_names().is_empty()))
            });
        let num_tables = catalog_provider
            .schema_names()
            .iter()
            .fold(0, |acc, schema| {
                acc + catalog_provider
                    .schema(schema)
                    .map_or(0, |s| s.table_names().len())
            });

        self.df
            .register_catalog(&catalog.name, catalog_provider)
            .boxed()
            .context(UnableToLoadCatalogConnectorSnafu {
                catalog: catalog.name.clone(),
            })?;

        tracing::info!(
            "Registered catalog '{}' with {num_schemas} schema{} and {num_tables} table{}",
            &catalog.name,
            if num_schemas == 1 { "" } else { "s" },
            if num_tables == 1 { "" } else { "s" },
        );

        Ok(())
    }

    pub(crate) async fn apply_catalog_diff(&self, current_app: &Arc<App>, new_app: &Arc<App>) {
        let valid_catalogs = Self::get_valid_catalogs(new_app, LogErrors(true));
        let existing_catalogs = Self::get_valid_catalogs(current_app, LogErrors(false));

        for catalog in &valid_catalogs {
            if let Some(current_catalog) = existing_catalogs.iter().find(|c| c.name == catalog.name)
            {
                if catalog != current_catalog {
                    // It isn't currently possible to remove catalogs once they have been loaded in DataFusion. `load_catalog` will overwrite the existing catalog.
                    self.load_catalog(catalog).await;
                }
            } else {
                self.status
                    .update_catalog(&catalog.name, status::ComponentStatus::Initializing);
                self.load_catalog(catalog).await;
            }
        }

        // Process catalogs that are no longer in the app
        for catalog in &existing_catalogs {
            if !valid_catalogs.iter().any(|c| c.name == catalog.name) {
                tracing::warn!(
                    "Failed to deregister catalog '{}'. Removing loaded catalogs is not currently supported.",
                    catalog.name
                );
            }
        }
    }
}
