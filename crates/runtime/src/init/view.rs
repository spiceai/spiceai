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
    component::view::View, metrics, status,
    topological_ordering::construct_effected_in_topological_order, view, LogErrors, Result,
    Runtime, UnableToAttachViewSnafu,
};
use app::App;
use datafusion::sql::{parser::DFParser, sqlparser::dialect::PostgreSqlDialect, TableReference};
use itertools::Itertools;
use snafu::prelude::*;

impl Runtime {
    pub(crate) fn load_views(&self, app: &Arc<App>) {
        let views: Vec<View> = Self::get_valid_views(app, LogErrors(true));

        for view in &views {
            if let Err(e) = self.load_view(view) {
                tracing::error!("Unable to load view: {e}");
            };
        }
    }

    /// Returns a list of valid views from the given App, skipping any that fail to parse and logging an error for them.
    pub(crate) fn get_valid_views(app: &Arc<App>, log_errors: LogErrors) -> Vec<View> {
        let datasets = Self::get_valid_datasets(app, log_errors)
            .iter()
            .map(|ds| ds.name.clone())
            .collect::<HashSet<_>>();

        app.views
            .iter()
            .cloned()
            .map(View::try_from)
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
                        Some(view)
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

    fn load_view(&self, view: &View) -> Result<()> {
        let df = Arc::clone(&self.df);
        df.register_view(view.name.clone(), view.sql.clone())
            .context(UnableToAttachViewSnafu)
            .inspect_err(|_| {
                self.status
                    .update_view(&view.name, status::ComponentStatus::Error);
            })?;

        self.status
            .update_view(&view.name, status::ComponentStatus::Ready);
        Ok(())
    }

    fn remove_view(&self, view: &View) {
        if self.df.table_exists(view.name.clone()) {
            if let Err(e) = self.df.remove_view(&view.name) {
                tracing::warn!("Unable to unload view {}: {}", &view.name, e);
                return;
            }
        }
        tracing::info!("Unloaded view {}", &view.name);
    }

    fn update_view(&self, view: &View) {
        self.status
            .update_view(&view.name, status::ComponentStatus::Refreshing);
        self.remove_view(view);
        let _ = self.load_view(view);
    }

    /// Update views based on changed between the current and new app.
    /// This function will update views that have changed, and remove views that are no longer in the app.
    /// It will also update views that have dependencies that have changed.
    pub(crate) fn apply_view_diff(&self, current_app: &Arc<App>, new_app: &Arc<App>) {
        let valid_views = Self::get_valid_views(new_app, LogErrors(true));
        let existing_views = Self::get_valid_views(current_app, LogErrors(false));

        let views_that_changed = valid_views
            .iter()
            .filter_map(|v| {
                let old_v = existing_views.iter().find(|vv| v.name == vv.name)?;
                if old_v == v {
                    None
                } else {
                    Some(v.name.clone())
                }
            })
            .collect_vec();

        // Remove views that are no longer in the app
        for view in &current_app.views {
            if !new_app.views.iter().any(|v| v.name == view.name) {
                let view = match View::try_from(view.clone()) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("Could not remove view {}: {e}", view.name);
                        continue;
                    }
                };
                self.status
                    .update_view(&view.name, status::ComponentStatus::Disabled);
                self.remove_view(&view);
            }
        }

        // Get ordering of views to load, including those unchanged but with dependencies that have changed
        // If we can't determine the order, we'll just load the views in the order they are in the app
        let affected_views_in_order_of_dependencies = match valid_views
            .iter()
            .map(|v| {
                let Some(statement) =
                    DFParser::parse_sql_with_dialect(v.sql.as_str(), &PostgreSqlDialect {})
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
                if existing_views.iter().any(|v| v.name == view.name) {
                    // Update view even if unchanged, as it may have dependencies that have changed
                    self.update_view(view);
                } else {
                    self.status
                        .update_view(&view.name, status::ComponentStatus::Initializing);
                    let _ = self.load_view(view);
                }
            }
        }
    }
}
