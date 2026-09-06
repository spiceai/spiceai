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

use std::collections::HashSet;
use std::sync::Arc;

use crate::{
    LogErrors, Result, Runtime, UnableToBuildCatalogSnafu, UnableToInitializeCatalogConnectorSnafu,
    UnableToLoadCatalogConnectorSnafu,
    catalogconnector::{self, CatalogConnector, get_catalog_provider},
    component::catalog::{Catalog, CatalogBuilder},
    dataconnector::parameters::ConnectorParamsBuilder,
    status,
};
use app::App;
use futures::future::join_all;
use runtime_metrics as metrics;
use snafu::prelude::*;
use util::warn_spaced;
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
                    // Every failure this call can raise used to land in the
                    // transient arm below, so a catalog naming an unregistered
                    // provider — or missing a required parameter — was rebuilt
                    // with unbounded backoff for the life of the process, and
                    // counted again on each attempt. See #12417.
                    if is_permanent_catalog_failure(&err) {
                        tracing::error!("{catalog_name} {err}");
                        return Err(RetryError::permanent(err));
                    }
                    warn_spaced!(spaced_tracer, "{} {err}", catalog_name);
                    return Err(RetryError::transient(err));
                }
            };

            if let Err(err) = Arc::clone(&self).register_catalog(catalog, connector).await {
                tracing::error!("{err}");
                if is_permanent_catalog_failure(&err) {
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

        // A DataFusion catalog registration outlives the declaration that created it: the
        // removal branch below can only warn, so a provider an earlier reload registered stays
        // registered after its declaration is dropped or stops validating. `existing_catalogs`
        // is the *valid* subset of one app revision, so it under-reports what is registered —
        // a name missing from it can still have a live provider that cached plans resolved
        // through. Both remaining signals are consulted before deciding a catalog is new:
        // the raw declarations of the current app (which include ones that failed to
        // validate), and what DataFusion currently has registered. The second reads the
        // session's catalog registry, so a *deferred* catalog — which `register_catalog`
        // holds in its own map rather than the session — is answered by the declaration
        // check, not by that one. See #13910.
        let declared_by_current_app: HashSet<&str> = current_app
            .catalogs
            .iter()
            .map(|c| c.name.as_str())
            .collect();

        let mut replaced_a_catalog = false;

        for catalog in &valid_catalogs {
            if let Some(current_catalog) = existing_catalogs.iter().find(|c| c.name == catalog.name)
            {
                if catalog != current_catalog {
                    // It isn't currently possible to remove catalogs once they have been loaded in DataFusion. `load_catalog` will overwrite the existing catalog.
                    Arc::clone(&self).load_catalog(catalog).await;
                    replaced_a_catalog = true;
                }
            } else {
                // Checked before the load, which is what registers the provider.
                let replaces_a_registered_provider = declared_by_current_app
                    .contains(catalog.name.as_str())
                    || self.df.catalog_exists(&catalog.name);

                self.status
                    .update_catalog(&catalog.name, status::ComponentStatus::Initializing);
                Arc::clone(&self).load_catalog(catalog).await;

                if replaces_a_registered_provider {
                    replaced_a_catalog = true;
                }
            }
        }

        // A cached logical plan embeds the `TableSource` it was planned against, and so keeps
        // reading through the provider the *previous* declaration built. Reloading a catalog
        // registers a fresh provider under the same name, so every plan that resolved a table in
        // it is now stale and must be replanned — the same reason dataset and view registration
        // discard cached plans. Registering a catalog that was never registered before
        // invalidates nothing: no cached plan can have resolved a table in it.
        if replaced_a_catalog {
            self.df.clear_cached_plans().await;
        }

        // Process catalogs that are no longer in the app
        for catalog in &existing_catalogs {
            if !valid_catalogs.iter().any(|c| c.name == catalog.name) {
                tracing::warn!(
                    "Catalog '{}' was removed from the Spicepod, but a loaded catalog cannot be removed while Spice is running: its tables stay queryable until Spice restarts.",
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

/// Returns `true` when a catalog load failure cannot be cleared by retrying it.
///
/// `load_catalog` retries with unbounded backoff, so a failure that is a pure
/// function of the Spicepod would otherwise be retried for the life of the
/// process, incrementing `catalogs::LOAD_ERROR` on every attempt. This is the
/// catalog analogue of `is_permanent_dataset_failure`, which #12345 added for
/// the dataset load path.
///
/// Everything else stays retriable, so a catalog whose source is merely
/// unreachable still recovers on its own — including
/// `UnableToLoadCatalogConnector`, which reports a registration failure rather
/// than a configuration one.
fn is_permanent_catalog_failure(err: &crate::Error) -> bool {
    match err {
        // The Spicepod names a provider this build cannot supply.
        crate::Error::UnknownCatalogConnector { .. } => true,
        crate::Error::UnableToInitializeCatalogConnector { source } => {
            is_permanent_catalog_source(source.as_ref())
        }
        _ => false,
    }
}

/// Returns `true` when a boxed catalog-connector error is a configuration error
/// that no retry can clear.
///
/// Two unrelated types box into `UnableToInitializeCatalogConnector`: the
/// connector's own [`catalogconnector::Error`], and [`runtime_parameters::Error`]
/// from `ConnectorParamsBuilder` when parameter validation rejects the catalog
/// before any connector exists. Only the first was classified before, so a
/// missing required parameter read as transient.
///
/// The `runtime_parameters` variant is matched by name rather than accepting any
/// error of that type, so a future retriable variant does not silently inherit
/// "permanent" from this arm.
fn is_permanent_catalog_source(source: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
    if let Some(err) = source.downcast_ref::<catalogconnector::Error>() {
        return err.is_configuration_error();
    }
    matches!(
        source.downcast_ref::<runtime_parameters::Error>(),
        Some(runtime_parameters::Error::InvalidConfigurationNoSource { .. })
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::access::AccessMode;
    use crate::component::catalog::CatalogSpec;
    use crate::dataconnector::ConnectorComponent;
    use std::collections::HashMap;

    fn catalog_component() -> ConnectorComponent {
        ConnectorComponent::Catalog(Arc::new(CatalogSpec {
            provider: "unity_catalog".to_string(),
            catalog_id: None,
            from: "unity_catalog".to_string(),
            name: "uc".to_string(),
            access: AccessMode::default(),
            orig_include: Vec::new(),
            include: None,
            orig_exclude: Vec::new(),
            exclude: None,
            params: HashMap::new(),
            dataset_params: HashMap::new(),
            acceleration: None,
        }))
    }

    fn initialize_error(source: Box<dyn std::error::Error + Send + Sync>) -> crate::Error {
        crate::Error::UnableToInitializeCatalogConnector { source }
    }

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
        // `Runtime::builder().build()` registers the default catalog connectors itself, so
        // `iceberg` is present for the suggestion to find without this test touching the
        // process-global registry on its own account.
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

    /// The #12417 regression: before this fix every failure `load_catalog_connector`
    /// could raise reached `RetryError::transient`, so a Spicepod naming a provider
    /// that is not registered was rebuilt with unbounded backoff forever.
    #[test]
    fn an_unregistered_catalog_provider_is_permanent() {
        let err = crate::Error::UnknownCatalogConnector {
            catalog_connector: "unity_catlog".to_string(),
            suggestion: Some("unity_catalog".to_string()),
            available: vec!["unity_catalog".to_string()],
        };
        assert!(
            is_permanent_catalog_failure(&err),
            "only a Spicepod edit can register the missing provider — retrying cannot"
        );
    }

    /// The other half of #12417: parameter validation rejects the catalog before
    /// any connector exists, so it raises `runtime_parameters::Error` rather than
    /// a `catalogconnector::Error` and the original downcast missed it.
    #[test]
    fn a_catalog_parameter_validation_failure_is_permanent() {
        let source = runtime_parameters::Error::InvalidConfigurationNoSource {
            component: "catalog uc".to_string(),
            message: "Missing required parameter: unity_catalog_token".to_string(),
        };
        let err = initialize_error(Box::new(source));
        assert!(
            is_permanent_catalog_failure(&err),
            "a missing required parameter is a pure function of the Spicepod"
        );
    }

    /// Guards the refactor from the inline `matches!` to the shared helper: the
    /// connector's own configuration errors must still classify permanent.
    #[test]
    fn a_catalog_connector_configuration_error_is_still_permanent() {
        let source = catalogconnector::Error::InvalidConfigurationNoSource {
            connector: "unity_catalog".to_string(),
            connector_component: catalog_component(),
            message: "invalid endpoint".to_string(),
        };
        let err = initialize_error(Box::new(source));
        assert!(
            is_permanent_catalog_failure(&err),
            "is_configuration_error() classified this before the helper existed"
        );
    }

    #[test]
    fn an_unreachable_catalog_source_stays_retriable() {
        let source = catalogconnector::Error::UnableToGetCatalogProvider {
            connector: "unity_catalog".to_string(),
            connector_component: catalog_component(),
            source: "connection refused".into(),
        };
        let err = initialize_error(Box::new(source));
        assert!(
            !is_permanent_catalog_failure(&err),
            "a source that is merely down must keep retrying so it recovers on its own"
        );
    }

    /// An error type neither downcast recognises must not be assumed permanent —
    /// failing open here would strand a catalog that would have recovered.
    #[test]
    fn an_unclassified_boxed_error_stays_retriable() {
        let err = initialize_error("some transport failure".into());
        assert!(
            !is_permanent_catalog_failure(&err),
            "an unrecognised error is not evidence the configuration is wrong"
        );
    }

    /// Registration failures are reported through a different variant, which this
    /// change deliberately leaves retriable.
    #[test]
    fn a_registration_failure_stays_retriable() {
        let err = crate::Error::UnableToLoadCatalogConnector {
            catalog: "uc".to_string(),
            source: "table already exists".into(),
        };
        assert!(
            !is_permanent_catalog_failure(&err),
            "registering into DataFusion can fail transiently"
        );
    }

    fn app_with_catalog(from: &str) -> Arc<App> {
        Arc::new(
            app::AppBuilder::new("catalog_reload")
                .with_catalog(spicepod::component::catalog::Catalog::new(
                    from.to_string(),
                    "reloaded".to_string(),
                ))
                .build(),
        )
    }

    /// The same catalog name, declared so `CatalogBuilder::try_from` rejects it: an
    /// uncompilable glob in `include` fails before any of the later validation, so the
    /// declaration is present in the raw app but absent from `get_valid_catalogs`.
    fn app_with_invalid_catalog(from: &str) -> Arc<App> {
        let mut catalog =
            spicepod::component::catalog::Catalog::new(from.to_string(), "reloaded".to_string());
        catalog.include = vec!["[".to_string()];
        Arc::new(
            app::AppBuilder::new("catalog_reload")
                .with_catalog(catalog)
                .build(),
        )
    }

    /// #13910: a cached logical plan embeds the `TableSource` the *previous* declaration built,
    /// so a reload that re-registers a catalog under the same name leaves every plan that
    /// resolved a table in it reading through the connector the user just stopped pointing at.
    ///
    /// The declaration is what decides this, not the outcome of the load: `load_catalog`
    /// retries a transient failure internally and only returns once it has either registered a
    /// provider or failed permanently, so discarding plans for a permanent failure — as this
    /// test's unregistered provider is — only costs a replan.
    #[tokio::test]
    async fn replacing_a_catalog_discards_cached_plans() {
        let runtime = Arc::new(Runtime::builder().build().await);
        runtime
            .df
            .cache_one_plan("SELECT 1")
            .await
            .expect("SELECT 1 should plan");
        assert_eq!(runtime.df.cached_plan_count().await, Some(1));

        Arc::clone(&runtime)
            .apply_catalog_diff(
                &app_with_catalog("not_a_real_catalog_connector:before"),
                &app_with_catalog("not_a_real_catalog_connector:after"),
            )
            .await;

        assert_eq!(
            runtime.df.cached_plan_count().await,
            Some(0),
            "a catalog whose declaration changed is re-registered with a fresh provider, so every cached plan holding the old one is stale"
        );
    }

    /// The other half of #13910's acceptance: a reload that leaves the catalog set untouched
    /// must not throw the plan cache away.
    #[tokio::test]
    async fn a_reload_that_changes_no_catalog_keeps_cached_plans() {
        let runtime = Arc::new(Runtime::builder().build().await);
        runtime
            .df
            .cache_one_plan("SELECT 1")
            .await
            .expect("SELECT 1 should plan");

        Arc::clone(&runtime)
            .apply_catalog_diff(
                &app_with_catalog("not_a_real_catalog_connector:same"),
                &app_with_catalog("not_a_real_catalog_connector:same"),
            )
            .await;

        assert_eq!(runtime.df.cached_plan_count().await, Some(1));
    }

    /// Registering a catalog that did not exist before invalidates nothing: no cached plan can
    /// have resolved a table in a catalog that was not there when it was planned.
    #[tokio::test]
    async fn adding_a_catalog_keeps_cached_plans() {
        let runtime = Arc::new(Runtime::builder().build().await);
        runtime
            .df
            .cache_one_plan("SELECT 1")
            .await
            .expect("SELECT 1 should plan");

        Arc::clone(&runtime)
            .apply_catalog_diff(
                &Arc::new(app::AppBuilder::new("catalog_reload").build()),
                &app_with_catalog("not_a_real_catalog_connector:added"),
            )
            .await;

        assert_eq!(runtime.df.cached_plan_count().await, Some(1));
    }

    /// #13910, second gap: a declaration that stops validating does not deregister the
    /// provider an earlier reload put in DataFusion — the removal branch can only warn. So a
    /// name absent from `existing_catalogs` because its *declaration* was rejected still has a
    /// live provider with cached plans resolved through it, and the reload that corrects the
    /// declaration overwrites that provider. That is a replacement, not an addition.
    ///
    /// Before the fix this took the `else` branch as "new" and left `replaced_a_catalog`
    /// false, so the assertion below read `Some(1)`.
    #[tokio::test]
    async fn correcting_an_invalid_catalog_declaration_discards_cached_plans() {
        let runtime = Arc::new(Runtime::builder().build().await);
        runtime
            .df
            .cache_one_plan("SELECT 1")
            .await
            .expect("SELECT 1 should plan");
        assert_eq!(runtime.df.cached_plan_count().await, Some(1));

        Arc::clone(&runtime)
            .apply_catalog_diff(
                &app_with_invalid_catalog("not_a_real_catalog_connector:before"),
                &app_with_catalog("not_a_real_catalog_connector:after"),
            )
            .await;

        assert_eq!(
            runtime.df.cached_plan_count().await,
            Some(0),
            "the previous declaration failing to validate does not unregister the provider it left behind, so re-registering under that name still strands every plan holding it"
        );
    }

    /// The premise the test above rests on: the invalid declaration really is dropped from
    /// `get_valid_catalogs`, so it is the raw-declaration check — not `existing_catalogs` —
    /// that classifies the reload as a replacement.
    #[tokio::test]
    async fn an_invalid_catalog_declaration_is_not_a_valid_catalog() {
        let runtime = Arc::new(Runtime::builder().build().await);
        let valid = Arc::clone(&runtime).get_valid_catalogs(
            &app_with_invalid_catalog("not_a_real_catalog_connector:before"),
            LogErrors(false),
        );
        assert!(
            valid.is_empty(),
            "an uncompilable include glob must make the declaration invalid for this test to mean anything"
        );
    }
}
