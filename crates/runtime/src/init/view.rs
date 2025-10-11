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

use std::{collections::HashMap, collections::HashSet, sync::Arc};

use crate::{
    AcceleratorEngineNotAvailableSnafu, AcceleratorInitializationFailedSnafu,
    FullTextSearchNotSupportedForViewSnafu, LogErrors, Result, Runtime, UnableToAttachViewSnafu,
    component::view::{View, ViewBuilder},
    metrics,
    secrets::Secrets,
    status,
    topological_ordering::construct_effected_in_topological_order,
    view, warn_spaced,
};
use app::App;
use datafusion::sql::{TableReference, parser::DFParser, sqlparser::dialect::PostgreSqlDialect};
use futures::stream::StreamExt;
use itertools::Itertools;
use snafu::prelude::*;
use tokio::sync::RwLock;

impl Runtime {
    pub(crate) fn load_views(self: Arc<Self>, app: &Arc<App>) {
        let views = Arc::clone(&self).get_valid_views(app, LogErrors(true));

        for view in views {
            let runtime = Arc::clone(&self);
            let secrets = runtime.secrets();
            if let Err(e) = runtime.load_view(&view, secrets) {
                let view_name = &view.name;
                tracing::error!("Unable to load view {view_name}: {e}");
            }
        }
    }

    /// Returns a list of valid views from the given App, skipping any that fail to parse and logging an error for them.
    pub(crate) fn get_valid_views(
        self: Arc<Self>,
        app: &Arc<App>,
        log_errors: LogErrors,
    ) -> Vec<Arc<View>> {
        let rt_ref = Arc::clone(&self);

        let datasets = self
            .get_valid_datasets(app, log_errors)
            .iter()
            .map(|ds| ds.name.clone())
            .collect::<HashSet<_>>();

        app.views
            .iter()
            .cloned()
            .map(|spicepod_view| {
                ViewBuilder::try_from(spicepod_view)
                    .map(|builder| builder.build_with(Arc::clone(&rt_ref), Arc::clone(app)))
            })
            .zip(&app.views)
            .filter_map(|(view, spicepod_view)| match view {
                Ok(view) => {
                    // only load this view if the name isn't used by an existing dataset
                    if datasets.contains(&view.name) {
                        if log_errors.0 {
                            metrics::views::LOAD_ERROR.add(1, &[]);
                            tracing::error!(
                                view = &spicepod_view.name,
                                "View name is already in use by a dataset."
                            );
                        }
                        None
                    } else {
                        Some(Arc::new(view))
                    }
                }
                Err(e) => {
                    if log_errors.0 {
                        metrics::views::LOAD_ERROR.add(1, &[]);
                        tracing::error!(view = &spicepod_view.name, "{e}");
                    }
                    None
                }
            })
            .collect()
    }

    /// Initialize views configured with accelerators before registering the datasets.
    /// This ensures that the required resources for acceleration are available before registration,
    /// which is important for acceleration federation for some acceleration engines (e.g. `DuckDB`).
    pub(crate) async fn initialize_views_accelerators(&self, views: &[Arc<View>]) {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        for view in views {
            if let Some(acceleration_settings) = &view.acceleration {
                let accelerator = match self
                    .accelerator_engine_registry
                    .get_accelerator_engine(acceleration_settings.engine)
                    .await
                    .context(AcceleratorEngineNotAvailableSnafu {
                        name: acceleration_settings.engine.to_string(),
                    }) {
                    Ok(accelerator) => accelerator,
                    Err(err) => {
                        let view_name = &view.name;
                        self.status
                            .update_view(view_name, status::ComponentStatus::Error);
                        metrics::views::LOAD_ERROR.add(1, &[]);
                        warn_spaced!(spaced_tracer, "{} {err}", view_name.table());
                        continue;
                    }
                };

                if let Err(err) = accelerator.init(view.as_ref()).await.context(
                    AcceleratorInitializationFailedSnafu {
                        name: acceleration_settings.engine.to_string(),
                    },
                ) {
                    let view_name = &view.name;
                    self.status
                        .update_view(view_name, status::ComponentStatus::Error);
                    metrics::views::LOAD_ERROR.add(1, &[]);
                    warn_spaced!(spaced_tracer, "{} {err}", view_name.table());
                }
            }
        }
    }

    pub(crate) async fn get_initialized_views(
        self: Arc<Self>,
        app: &Arc<App>,
        log_errors: LogErrors,
    ) -> Vec<Arc<View>> {
        let valid_views = Arc::clone(&self).get_valid_views(app, log_errors);
        futures::stream::iter(valid_views)
            .filter_map(|view| async move {
                match (view.is_accelerated(), view.is_accelerator_initialized().await) {
                    (true, true) | (false, _) => Some(Arc::clone(&view)),
                    (true, false) => {
                        if log_errors.0 {
                            metrics::views::LOAD_ERROR.add(1, &[]);
                            tracing::error!(
                                "View {view_name} is accelerated but the accelerator failed to initialize.",
                                view_name = &view.name.to_string(),
                            );
                        }
                        None
                    }
                }
            })
            .collect()
            .await
    }

    #[allow(clippy::result_large_err)]
    fn load_view(self: Arc<Self>, view: &Arc<View>, secrets: Arc<RwLock<Secrets>>) -> Result<()> {
        if let Err(err) = validate_view(view) {
            let view_name = &view.name;
            metrics::views::LOAD_ERROR.add(1, &[]);
            self.status
                .update_view(view_name, status::ComponentStatus::Error);
            return Err(err);
        }

        let df = Arc::clone(&self.df);
        let register_task = df
            .register_view(Arc::clone(view), secrets)
            .context(UnableToAttachViewSnafu)
            .inspect_err(|_| {
                self.status
                    .update_view(&view.name, status::ComponentStatus::Error);
            })?;

        let runtime = Arc::clone(&self);
        let view = Arc::clone(view);

        tokio::task::spawn(async move {
            let view_name = view.name.clone();
            let notifier = register_task.await;
            match notifier {
                Ok(Some(notifier)) => {
                    notifier.notified().await;
                    if let Err(e) = runtime.create_dataset_or_view_schedule(view).await {
                        tracing::error!(
                            "Failed to create refresh schedule for accelerated view '{view_name}': {e}."
                        );
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(
                        "Failed to create refresh schedule for accelerated view '{view_name}': {e}"
                    );
                }
            }
        });

        Ok(())
    }

    async fn remove_view(self: Arc<Self>, name: &TableReference) {
        if self.df.table_exists(name.clone()) {
            if self.df.is_accelerated(name).await
                && let Err(e) = Arc::clone(&self)
                    .remove_dataset_or_view_schedule(name)
                    .await
            {
                tracing::warn!(
                    "Failed to remove refresh schedule for accelerated view {}: {e}",
                    &name
                );
            }

            if let Err(e) = self.df.remove_view(name).await {
                tracing::warn!("Unable to unload view {}: {}", name, e);
                return;
            }
        }
        tracing::info!("Unloaded view {}", name);
    }

    async fn update_view(self: Arc<Self>, view: &Arc<View>) {
        self.status
            .update_view(&view.name, status::ComponentStatus::Refreshing);
        Arc::clone(&self).remove_view(&view.name).await;
        let secrets = self.secrets();
        let _ = self.load_view(view, secrets);
    }

    /// Update views based on changed between the current and new app.
    /// This function will update views that have changed, and remove views that are no longer in the app.
    /// It will also update views that have dependencies that have changed.
    pub(crate) async fn apply_view_diff(
        self: Arc<Self>,
        current_app: &Arc<App>,
        new_app: &Arc<App>,
    ) {
        let valid_views = Arc::clone(&self).get_valid_views(new_app, LogErrors(true));
        let existing_views = Arc::clone(&self).get_valid_views(current_app, LogErrors(false));

        let views_that_changed = valid_views
            .iter()
            .filter_map(|v| {
                match existing_views.iter().find(|vv| v.name == vv.name) {
                    Some(old_v) => {
                        if old_v == v {
                            None // No change, don't include
                        } else {
                            Some(v.name.clone()) // Changed, include the name
                        }
                    }
                    None => Some(v.name.clone()), // New view, include the name
                }
            })
            .collect_vec();

        // Remove views that are no longer in the app
        for view in &current_app.views {
            if !new_app.views.iter().any(|v| v.name == view.name) {
                let view_builder = match ViewBuilder::try_from(view.clone()) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("Could not remove view {}: {e}", view.name);
                        continue;
                    }
                };
                self.status
                    .update_view(&view_builder.name, status::ComponentStatus::Disabled);
                Arc::clone(&self).remove_view(&view_builder.name).await;
            }
        }

        // Get ordering of views to load, including those unchanged but with dependencies that have changed
        // If we can't determine the order, we'll just load the views in the order they are in the app
        let affected_views_in_order_of_dependencies = match valid_views
            .iter()
            .map(|v| {
                let Some(statement) =
                    DFParser::parse_sql_with_dialect(v.sql.as_ref(), &PostgreSqlDialect {})
                        .boxed()?.pop_front() else {
                            return Err(Box::<dyn std::error::Error + Send + Sync>::from(format!("no statements found in view {}", v.name)));
                        };

                let deps = view::get_dependent_table_names(&statement);
                Ok((v.name.clone(), deps))
            })
            .collect::<Result<HashMap<TableReference, Vec<TableReference>>, _>>()
        {
            Err(e) => {
                tracing::warn!("Unable to determine order to update views: {e}. Will still attempt to update views.");
                None
            }
            Ok(deps) => construct_effected_in_topological_order(deps,&views_that_changed ),
        }.unwrap_or(valid_views.iter().map(|v| v.name.clone()).collect());

        for view_name in affected_views_in_order_of_dependencies {
            if let Some(view) = valid_views.iter().find(|v| v.name == view_name) {
                let runtime = Arc::clone(&self);
                if existing_views.iter().any(|v| v.name == view.name) {
                    // Update view even if unchanged, as it may have dependencies that have changed
                    runtime.update_view(view).await;
                } else {
                    runtime
                        .status
                        .update_view(&view.name, status::ComponentStatus::Initializing);
                    let secrets = runtime.secrets();
                    let _ = runtime.load_view(view, secrets);
                }
            }
        }
    }
}

#[allow(clippy::result_large_err)]
fn validate_view(view: &Arc<View>) -> Result<()> {
    if view.has_full_text_column() {
        return Err(FullTextSearchNotSupportedForViewSnafu.build());
    }

    Ok(())
}
