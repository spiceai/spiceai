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
    LogErrors, Result, Runtime, UnableToBuildCatalogSnafu, UnableToInitializeCatalogConnectorSnafu,
    UnableToLoadCatalogConnectorSnafu,
    catalogconnector::{self, CatalogConnector, get_catalog_provider},
    component::catalog::{Catalog, CatalogBuilder},
    dataconnector::parameters::ConnectorParamsBuilder,
    status, warn_spaced,
};
use app::App;
use futures::future::join_all;
use runtime_metrics as metrics;
use snafu::prelude::*;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

impl Runtime {
    pub(crate) async fn load_catalogs(self: Arc<Self>) {
        let Some(ref app) = self.read_app().await else {
            return;
        };

        let valid_catalogs = Arc::clone(&self).get_valid_catalogs(app, LogErrors(true));
        let mut futures = vec![];
        for catalog in &valid_catalogs {
            self.status
                .update_catalog(&catalog.name, status::ComponentStatus::Initializing);
            futures.push(Arc::clone(&self).load_catalog(catalog));
        }

        let _ = join_all(futures).await;
    }

    async fn load_catalog(self: Arc<Self>, catalog: &Catalog) {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        let retry_strategy = FibonacciBackoffBuilder::new().max_retries(None).build();

        let _ = retry(retry_strategy, || async {
            let connector = match self.load_catalog_connector(catalog).await {
                Ok(connector) => connector,
                Err(err) => {
                    let catalog_name = &catalog.name;
                    self.status.update_catalog(
                        catalog_name,
                        status::ComponentStatus::error_with_message(err.to_string()),
                    );
                    metrics::catalogs::LOAD_ERROR.add(1, &[]);
                    warn_spaced!(spaced_tracer, "{} {err}", catalog_name);
                    return Err(RetryError::transient(err));
                }
            };

            if let Err(err) = Arc::clone(&self).register_catalog(catalog, connector).await {
                tracing::error!("{err}");
                if matches!(
                    &err,
                    crate::Error::UnableToInitializeCatalogConnector { source }
                        if source.downcast_ref::<catalogconnector::Error>()
                            .is_some_and(catalogconnector::Error::is_configuration_error)
                ) {
                    let catalog_name = &catalog.name;
                    self.status.update_catalog(
                        catalog_name,
                        status::ComponentStatus::error_with_message(err.to_string()),
                    );
                    metrics::catalogs::LOAD_ERROR.add(1, &[]);
                    return Err(RetryError::permanent(err));
                }
                return Err(RetryError::transient(err));
            }

            self.status
                .update_catalog(&catalog.name, status::ComponentStatus::Ready);

            Ok(())
        })
        .await;
    }

    async fn load_catalog_connector(&self, catalog: &Catalog) -> Result<Arc<dyn CatalogConnector>> {
        let catalog = catalog.clone();

        let source = catalog.provider.clone();

        // Resolve the provider before building parameters. The builder resolves it too — it
        // reads the factory's prefix and parameter list — and fails with
        // `InvalidConnectorType`, which names no alternative, so it used to answer every
        // typo'd provider before `UnknownCatalogConnector` could. See #12415.
        if !catalogconnector::is_registered(&source).await {
            return Err(unknown_catalog_connector(&source).await);
        }

        let params = ConnectorParamsBuilder::for_catalog(source.clone().into(), &catalog)
            .build(self.secrets(), self.tokio_io_runtime())
            .await
            .context(UnableToInitializeCatalogConnectorSnafu)?;

        let Some(catalog_connector) = catalogconnector::create_new_connector(&source, params).await
        else {
            // Only reachable if the provider is deregistered between the check above and this
            // lookup; report the same error rather than a second, blunter one.
            return Err(unknown_catalog_connector(&source).await);
        };

        Ok(catalog_connector)
    }

    #[expect(clippy::result_large_err)]
    fn catalogs_iter(
        self: Arc<Self>,
        app: &Arc<App>,
    ) -> impl Iterator<Item = Result<Catalog>> + '_ {
        app.catalogs
            .clone()
            .into_iter()
            .map(CatalogBuilder::try_from)
            .map(move |catalog_builder_result| {
                catalog_builder_result.and_then(|catalog_builder| {
                    let catalog_name = catalog_builder.name.clone();
                    catalog_builder
                        .with_app(Arc::clone(app))
                        .with_runtime(Arc::clone(&self))
                        .build()
                        .context(UnableToBuildCatalogSnafu {
                            catalog: catalog_name,
                        })
                })
            })
    }

    /// Returns a list of valid catalogs from the given App, skipping any that fail to parse and logging an error for them.
    pub(crate) fn get_valid_catalogs(
        self: Arc<Self>,
        app: &Arc<App>,
        log_errors: LogErrors,
    ) -> Vec<Catalog> {
        self.catalogs_iter(app)
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
        self: Arc<Self>,
        catalog: &Catalog,
        catalog_connector: Arc<dyn CatalogConnector>,
    ) -> Result<()> {
        tracing::info!(
            "Registering catalog '{}' for {}",
            &catalog.name,
            &catalog.provider
        );
        let catalog_provider =
            get_catalog_provider(catalog_connector, Arc::clone(&self), catalog, None)
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
            .register_catalog(&catalog.name, &catalog.access, catalog_provider)
            .await
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

    pub(crate) async fn apply_catalog_diff(
        self: Arc<Self>,
        current_app: &Arc<App>,
        new_app: &Arc<App>,
    ) {
        let valid_catalogs = Arc::clone(&self).get_valid_catalogs(new_app, LogErrors(true));
        let existing_catalogs = Arc::clone(&self).get_valid_catalogs(current_app, LogErrors(false));

        for catalog in &valid_catalogs {
            if let Some(current_catalog) = existing_catalogs.iter().find(|c| c.name == catalog.name)
            {
                if catalog != current_catalog {
                    // It isn't currently possible to remove catalogs once they have been loaded in DataFusion. `load_catalog` will overwrite the existing catalog.
                    Arc::clone(&self).load_catalog(catalog).await;
                }
            } else {
                self.status
                    .update_catalog(&catalog.name, status::ComponentStatus::Initializing);
                Arc::clone(&self).load_catalog(catalog).await;
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

/// The error for a catalog naming a provider this build does not register: the closest
/// registered name plus the full list, so the message names a fix.
///
/// Reporting is the caller's. `load_catalog` counts `catalogs::LOAD_ERROR` and writes the
/// component status for every error `load_catalog_connector` returns, so doing either here
/// too would report one misconfigured catalog twice per attempt. See #12442.
async fn unknown_catalog_connector(source: &str) -> crate::Error {
    crate::Error::UnknownCatalogConnector {
        catalog_connector: source.to_string(),
        suggestion: catalogconnector::suggest_catalog_connector(source).await,
        available: catalogconnector::registered_catalog_names().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_catalog(from: &str, runtime: &Arc<Runtime>) -> Catalog {
        CatalogBuilder::try_new(from.to_string(), "test_catalog")
            .expect("valid catalog builder")
            .with_app(Arc::new(app::AppBuilder::new("catalog_typo").build()))
            .with_runtime(Arc::clone(runtime))
            .build()
            .expect("valid runtime catalog")
    }

    /// The #12415 regression: `ConnectorParamsBuilder::build` resolves the provider first and
    /// fails with `InvalidConnectorType`, which names no alternative, so the
    /// suggestion-bearing `UnknownCatalogConnector` written for this case was unreachable.
    #[tokio::test]
    async fn a_misspelled_catalog_provider_suggests_the_closest_provider() {
        catalogconnector::register_all().await;

        let runtime = Arc::new(Runtime::builder().build().await);
        let catalog = test_catalog("iceber:some_catalog", &runtime);

        // `CatalogConnector` is not `Debug`, so the success case cannot be unwrapped for the
        // error the way `expect_err` would.
        let Err(err) = runtime.load_catalog_connector(&catalog).await else {
            panic!("a provider that is not registered must fail")
        };

        assert!(
            matches!(err, crate::Error::UnknownCatalogConnector { .. }),
            "expected UnknownCatalogConnector, got: {err}"
        );
        assert!(
            err.to_string().contains("Did you mean 'iceberg'?"),
            "the error should name the closest registered provider: {err}"
        );
    }

    /// #12442: `load_catalog` counts `catalogs::LOAD_ERROR` and writes the component status for
    /// every error `load_catalog_connector` returns, so reporting the unknown-provider failure
    /// here as well would report one misconfigured catalog twice per attempt.
    #[tokio::test]
    async fn an_unknown_catalog_provider_leaves_reporting_to_the_caller() {
        let runtime = Arc::new(Runtime::builder().build().await);
        let catalog = test_catalog("not_a_real_catalog_connector:some_catalog", &runtime);

        let Err(err) = runtime.load_catalog_connector(&catalog).await else {
            panic!("a provider that is not registered must fail")
        };
        assert!(
            matches!(err, crate::Error::UnknownCatalogConnector { .. }),
            "expected UnknownCatalogConnector, got: {err}"
        );

        let statuses = runtime.status().get_catalog_statuses();
        assert!(
            !statuses.contains_key("test_catalog"),
            "load_catalog_connector must leave the status to load_catalog, wrote: {statuses:?}"
        );
    }
}
