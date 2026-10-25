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

use std::{collections::HashMap, collections::HashSet, collections::VecDeque, sync::Arc};

use tokio_util::sync::CancellationToken;

use crate::{
    AcceleratorEngineNotAvailableSnafu, AcceleratorInitializationFailedSnafu, LogErrors, Result,
    Runtime, UnableToAttachViewSnafu,
    component::view::{View, ViewBuilder},
    datafusion::{DeferredRefreshOutcome, TableInstance, resolve_table_reference},
    secrets::Secrets,
    status,
    view::{self, ViewStart},
};
use app::App;
use datafusion::{
    common::{ResolvedTableReference, TableReference},
    sql::{parser::DFParser, sqlparser::dialect::PostgreSqlDialect},
};
#[cfg(feature = "duckdb")]
use futures::stream::StreamExt;
use itertools::Itertools;
use runtime_metrics as metrics;
use snafu::prelude::*;
use tokio::sync::RwLock;
use util::topological_ordering::{
    construct_effected_in_topological_order, construct_topological_ordering,
};
use util::warn_spaced;

/// Represents a validated view with its parsed dependencies
pub(crate) struct ValidatedView {
    pub(crate) view: Arc<View>,
    pub(crate) dependencies: Vec<TableReference>,
}

/// The views that read `view`, directly or through other views, each with those of them
/// and `view` that it reads itself. A view reads the tables its SQL resolves
/// ([`view_reads`]), however qualified and including those only a subquery reads. A view
/// in or behind a cycle of views reading each other is left out: it is not registered.
fn views_reading(
    state: &datafusion::execution::SessionState,
    validated_views: &[ValidatedView],
    view: &TableReference,
) -> Vec<(Arc<View>, Vec<ResolvedTableReference>)> {
    let reads: HashMap<ResolvedTableReference, Option<TablesRead>> = validated_views
        .iter()
        .map(|vv| {
            (
                resolve_table_reference(vv.view.name.clone()),
                view_reads(state, &vv.view.sql),
            )
        })
        .collect();
    let in_or_behind_cycles = views_in_or_behind_cycles(&reads);
    let reads_table = |reader: &ResolvedTableReference, table: &ResolvedTableReference| {
        reader != table
            && reads
                .get(reader)
                .and_then(Option::as_ref)
                .is_some_and(|tables| tables.contains_key(table))
    };

    let view = resolve_table_reference(view.clone());
    let mut affected = HashSet::from([view.clone()]);
    let mut queue = VecDeque::from([view.clone()]);
    while let Some(table) = queue.pop_front() {
        for reader in reads.keys() {
            if !in_or_behind_cycles.contains(reader)
                && reads_table(reader, &table)
                && affected.insert(reader.clone())
            {
                queue.push_back(reader.clone());
            }
        }
    }

    validated_views
        .iter()
        .filter_map(|vv| {
            let name = resolve_table_reference(vv.view.name.clone());
            if name == view || !affected.contains(&name) {
                return None;
            }
            let reads = affected
                .iter()
                .filter(|table| reads_table(&name, table))
                .cloned()
                .collect();
            Some((Arc::clone(&vv.view), reads))
        })
        .collect()
}

/// Extract view dependencies from SQL and return them in topological order.
/// Returns None if a cycle is detected.
fn order_views_by_dependencies(validated_views: &[ValidatedView]) -> Option<Vec<Arc<View>>> {
    if validated_views.is_empty() {
        return None;
    }

    // Build dependency map
    let deps_map: HashMap<TableReference, Vec<TableReference>> = validated_views
        .iter()
        .map(|vv| (vv.view.name.clone(), vv.dependencies.clone()))
        .collect();

    // Filter out dependencies that are not views (e.g., datasets) to prevent false cycle detection
    let view_names: HashSet<_> = deps_map.keys().cloned().collect();
    let view_only_deps: HashMap<_, _> = deps_map
        .into_iter()
        .map(|(view, deps)| {
            let filtered_deps: Vec<_> = deps
                .into_iter()
                .filter(|dep| view_names.contains(dep))
                .collect();
            (view, filtered_deps)
        })
        .collect();

    if let Some(ordered_names) = construct_topological_ordering(view_only_deps) {
        // Return views in topological order
        Some(
            ordered_names
                .into_iter()
                .filter_map(|name| {
                    let view = validated_views
                        .iter()
                        .find(|vv| vv.view.name == name)
                        .map(|vv| Arc::clone(&vv.view));
                    if view.is_none() {
                        tracing::warn!("View {name} returned by topological ordering but not found in views vector");
                    }
                    view
                })
                .collect::<Vec<_>>(),
        )
    } else {
        tracing::warn!("Circular dependency detected in views. Loading views in original order.");
        Some(
            validated_views
                .iter()
                .map(|vv| Arc::clone(&vv.view))
                .collect(),
        )
    }
}

/// The tables a view's SQL reads: each resolved name, with the name as written.
type TablesRead = HashMap<ResolvedTableReference, TableReference>;

/// The tables a view's SQL reads: every table the planner looks up to plan it,
/// including tables only a subquery reads, and no CTEs. `None` when the SQL
/// does not resolve, in which case registering the view reports the error.
fn view_reads(state: &datafusion::execution::SessionState, sql: &str) -> Option<TablesRead> {
    let mut statements = DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {}).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let statement = statements.pop_front()?;
    let tables = state.resolve_table_references(&statement).ok()?;
    Some(
        tables
            .into_iter()
            .map(|table| (resolve_table_reference(table.clone()), table))
            .collect(),
    )
}

/// The views in a cycle of views reading each other, and the views that read,
/// directly or not, a view in one: the views Kahn's algorithm never takes. A
/// view reading itself is not a cycle.
fn views_in_or_behind_cycles(
    reads: &HashMap<ResolvedTableReference, Option<TablesRead>>,
) -> HashSet<ResolvedTableReference> {
    // Kahn's algorithm: repeatedly take a view none of whose views is left.
    let mut unresolved: HashMap<&ResolvedTableReference, usize> = HashMap::new();
    let mut readers: HashMap<&ResolvedTableReference, Vec<&ResolvedTableReference>> =
        HashMap::new();
    for (view, tables) in reads {
        let mut count = 0;
        for table in tables.iter().flat_map(HashMap::keys) {
            if table != view && reads.contains_key(table) {
                count += 1;
                readers.entry(table).or_default().push(view);
            }
        }
        unresolved.insert(view, count);
    }
    let mut queue: Vec<&ResolvedTableReference> = unresolved
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(view, _)| *view)
        .collect();
    let mut taken: HashSet<&ResolvedTableReference> = HashSet::new();
    while let Some(view) = queue.pop() {
        taken.insert(view);
        for reader in readers.get(view).into_iter().flatten() {
            if let Some(count) = unresolved.get_mut(reader) {
                *count -= 1;
                if *count == 0 {
                    queue.push(reader);
                }
            }
        }
    }
    reads
        .keys()
        .filter(|view| !taken.contains(view))
        .cloned()
        .collect()
}

/// How many names a view's waiting message lists before it summarizes the rest.
const MAX_LISTED_WAITS: usize = 10;

/// How long a startup view waits for what it reads before it logs what it is
/// waiting for. The same cadence as the dataset load summary.
const VIEW_WAIT_REPORT_AFTER: std::time::Duration = std::time::Duration::from_secs(30);

/// How long a startup view waits for each dataset it reads only in a subquery
/// to be registered: the minute `register_view` waits for a missing table in a
/// view's `FROM` clause.
const SUBQUERY_DATASET_GRACE: std::time::Duration = std::time::Duration::from_mins(1);

/// What a startup view waits for before it registers.
#[derive(Clone, Copy)]
enum ViewWait<'a> {
    /// The datasets and views it reads.
    WhatItReads,
    /// Every startup dataset, because it reads a table that is not a dataset or
    /// view loading at startup: the first such table, when its SQL resolved.
    EveryDataset(Option<&'a TableReference>),
}

/// The line a startup view logs when it has to wait before it registers.
/// `pending` names what it is still waiting for.
fn view_waiting_message(view: &TableReference, pending: &[String], wait: ViewWait) -> String {
    let mut listed = pending
        .iter()
        .take(MAX_LISTED_WAITS)
        .map(|name| format!("'{name}'"))
        .join(", ");
    if pending.len() > MAX_LISTED_WAITS {
        listed = format!("{listed} and {} more", pending.len() - MAX_LISTED_WAITS);
    }
    match wait {
        ViewWait::WhatItReads => {
            format!("View '{view}' is still waiting for {listed} to load before it registers.")
        }
        ViewWait::EveryDataset(Some(table)) => format!(
            "View '{view}' is still waiting for every dataset to load before it registers, because it reads '{table}', which is not a dataset or view loading at startup. Still loading: {listed}."
        ),
        ViewWait::EveryDataset(None) => format!(
            "View '{view}' is still waiting for every dataset to load before it registers. Still loading: {listed}."
        ),
    }
}

impl Runtime {
    /// Loads the startup views. Each view registers as soon as the datasets and
    /// views it reads from have finished loading, so a dataset that keeps
    /// failing holds back only the views that read from it.
    ///
    /// `dataset_done` maps each startup dataset whose load is running to a token
    /// cancelled when that load ends. A view that reads anything else — a
    /// catalog table, a dataset that is not loading (it failed accelerator
    /// initialization, waits on a snapshot, or is chained deeper behind another
    /// dataset), or a view in or behind a cycle of views — waits for every startup dataset instead,
    /// which is what every view did before. Each dataset the view reads only in
    /// a subquery also gets up to a minute to be registered once its load has
    /// ended, as `register_view` waits for a missing `FROM` table. A view a
    /// Spicepod change has already taken over is left to that change. An
    /// accelerated view with an existing acceleration to serve from waits for
    /// nothing: it registers from that acceleration, and refreshes once what it
    /// reads is ready.
    ///
    /// Returns the tasks loading the views.
    pub(crate) fn load_views(
        self: Arc<Self>,
        app: &Arc<App>,
        dataset_done: &HashMap<ResolvedTableReference, CancellationToken>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        // `LogErrors(false)`: `load_datasets` is this function's only caller and it has
        // already validated the same views with `LogErrors(true)` before its snapshot
        // checks, so reporting again here only prints each view's load error, and each
        // view's discarded-acceleration warning, twice per startup.
        let validated_views = Arc::clone(&self).get_valid_views(app, LogErrors(false));

        // Determine the dependency order for views based on their SQL dependencies
        let views_in_dependency_order = order_views_by_dependencies(&validated_views)
            .unwrap_or_else(|| {
                validated_views
                    .iter()
                    .map(|vv| Arc::clone(&vv.view))
                    .collect()
            });

        let state = self.df.ctx.state();
        let reads: HashMap<ResolvedTableReference, Option<TablesRead>> = validated_views
            .iter()
            .map(|vv| {
                (
                    resolve_table_reference(vv.view.name.clone()),
                    view_reads(&state, &vv.view.sql),
                )
            })
            .collect();
        // A view in a cycle that waited on the views it reads would wait on
        // itself, and a view behind one on a view that never registers, so those
        // views wait for every startup dataset instead. Every other view reads
        // only views outside any cycle, and waits on them.
        let in_or_behind_cycles = views_in_or_behind_cycles(&reads);
        let dataset_names: HashMap<ResolvedTableReference, TableReference> = app
            .datasets
            .iter()
            .filter_map(|ds| {
                crate::component::dataset::Dataset::parse_table_reference(&ds.name).ok()
            })
            .map(|name| (resolve_table_reference(name.clone()), name))
            .collect();

        // The tables in each view's `FROM` clauses, which `register_view` itself
        // waits to exist.
        let from_tables: HashMap<ResolvedTableReference, HashSet<ResolvedTableReference>> =
            validated_views
                .iter()
                .map(|vv| {
                    (
                        resolve_table_reference(vv.view.name.clone()),
                        vv.dependencies
                            .iter()
                            .cloned()
                            .map(resolve_table_reference)
                            .collect(),
                    )
                })
                .collect();

        // Created for every view up front and compared by resolved name, so a view
        // that reads `public.v1` waits on `v1` whatever order they are spawned in.
        let view_done: HashMap<ResolvedTableReference, CancellationToken> = reads
            .keys()
            .map(|view| (view.clone(), CancellationToken::new()))
            .collect();
        let mut tasks = Vec::new();

        for view in views_in_dependency_order {
            let resolved = resolve_table_reference(view.name.clone());
            let wait_on_views = !in_or_behind_cycles.contains(&resolved);
            let done = view_done.get(&resolved).cloned().unwrap_or_default();
            let Some(view_load) = self.view_loads.begin(&view.name) else {
                tracing::debug!(
                    "Not registering view '{}' at startup: a Spicepod change has already taken it over.",
                    view.name
                );
                done.cancel();
                continue;
            };

            let mut waits: Vec<(String, CancellationToken)> = Vec::new();
            let mut waits_for_every_dataset = false;
            // The first table the view reads that is not loading at startup.
            let mut not_loading: Option<TableReference> = None;
            if let Some(Some(tables)) = reads.get(&resolved) {
                for (table, written) in tables.iter().sorted_by_key(|(table, _)| table.to_string())
                {
                    let view_token = view_done
                        .get(table)
                        .filter(|_| wait_on_views && table != &resolved);
                    if let Some(token) = view_token.or_else(|| dataset_done.get(table)) {
                        waits.push((written.to_string(), token.clone()));
                    } else {
                        waits_for_every_dataset = true;
                        not_loading.get_or_insert_with(|| written.clone());
                    }
                }
            } else {
                waits_for_every_dataset = true;
            }
            // Startup datasets the view reads only in a subquery, which
            // `register_view` does not wait for.
            let subquery_datasets: Vec<TableReference> = match reads.get(&resolved) {
                Some(Some(tables)) => tables
                    .keys()
                    .filter(|table| {
                        dataset_done.contains_key(*table)
                            && !from_tables
                                .get(&resolved)
                                .is_some_and(|from| from.contains(*table))
                    })
                    .map(|table| {
                        TableReference::full(
                            Arc::clone(&table.catalog),
                            Arc::clone(&table.schema),
                            Arc::clone(&table.table),
                        )
                    })
                    .collect(),
                _ => Vec::new(),
            };
            if waits_for_every_dataset {
                waits.extend(dataset_done.iter().map(|(name, token)| {
                    let name = dataset_names
                        .get(name)
                        .map_or_else(|| name.to_string(), ToString::to_string);
                    (name, token.clone())
                }));
            }

            let runtime = Arc::clone(&self);
            tasks.push(tokio::spawn(async move {
                let done = done.drop_guard();
                let superseded = view_load.token();
                let shutdown = runtime.status.shutdown_token();
                let pending = || -> Vec<String> {
                    waits
                        .iter()
                        .filter(|(_, token)| !token.is_cancelled())
                        .map(|(name, _)| name.clone())
                        .sorted()
                        .dedup()
                        .collect()
                };
                let served_from_acceleration = view.existing_acceleration_schema().await.is_some();
                if !served_from_acceleration && !pending().is_empty() {
                    runtime
                        .status
                        .update_view(&view.name, status::ComponentStatus::Initializing);
                    let loaded =
                        futures::future::join_all(waits.iter().map(|(_, token)| token.cancelled()));
                    tokio::pin!(loaded);
                    // At a healthy startup every view waits briefly, so only a view
                    // still waiting after `VIEW_WAIT_REPORT_AFTER` logs what holds it.
                    let report = tokio::time::sleep(VIEW_WAIT_REPORT_AFTER);
                    tokio::pin!(report);
                    let mut reported = false;
                    loop {
                        tokio::select! {
                            _ = &mut loaded => break,
                            () = shutdown.cancelled() => return,
                            () = superseded.cancelled() => return,
                            () = &mut report, if !reported => {
                                reported = true;
                                tracing::info!(
                                    "{}",
                                    view_waiting_message(
                                        &view.name,
                                        &pending(),
                                        if waits_for_every_dataset {
                                            ViewWait::EveryDataset(not_loading.as_ref())
                                        } else {
                                            ViewWait::WhatItReads
                                        }
                                    )
                                );
                            }
                        }
                    }
                }
                // A dataset's startup load can end before the dataset is
                // registered: a Spicepod change that replaces it stops the load and
                // registers the replacement itself. Planning the view before then
                // fails it for good, so give each dataset it reads only in a
                // subquery its own minute to be registered. A view served from its
                // acceleration builds its plan only once what it reads is ready.
                if !served_from_acceleration {
                    for table in &subquery_datasets {
                        let deadline = tokio::time::Instant::now() + SUBQUERY_DATASET_GRACE;
                        while !runtime.df.table_exists(table)
                            && tokio::time::Instant::now() < deadline
                        {
                            tokio::select! {
                                () = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
                                () = shutdown.cancelled() => return,
                                () = superseded.cancelled() => return,
                            }
                        }
                    }
                }
                if superseded.is_cancelled() {
                    return;
                }
                let secrets = runtime.secrets();
                if let Err(e) = runtime.load_view_signalling(
                    &view,
                    secrets,
                    Some(done),
                    Some(view_load),
                    ViewStart::ExistingAcceleration,
                ) {
                    let view_name = &view.name;
                    tracing::error!("Unable to load view {view_name}: {e}");
                }
            }));
        }

        tasks
    }

    /// Returns a list of valid views from the given App, with SQL validated and dependencies extracted.
    /// Skips any that fail to parse and logs an error for them.
    pub(crate) fn get_valid_views(
        self: Arc<Self>,
        app: &Arc<App>,
        log_errors: LogErrors,
    ) -> Vec<ValidatedView> {
        let rt_ref = Arc::clone(&self);
        let status = Arc::clone(&self.status);

        // `LogErrors(false)` regardless of this call's own flag: this is a name
        // lookup, not a dataset load. Both paths that reach here with
        // `LogErrors(true)` — `load_datasets`, and `apply_dataset_diff` ahead of
        // `apply_view_diff` — already call `get_valid_datasets(app,
        // LogErrors(true))` themselves, so nothing is silenced by this; passing
        // the flag through only made each dataset's load error, and each
        // dataset's discarded-acceleration warning, print twice per load.
        let datasets = Arc::clone(&self)
            .get_valid_datasets(app, LogErrors(false))
            .iter()
            .map(|ds| ds.name.clone())
            .collect::<HashSet<_>>();

        app.views
            .iter()
            .cloned()
            .map(|spicepod_view| {
                ViewBuilder::try_from(spicepod_view)
                    .map_err(crate::Error::from)
                    .map(|builder| {
                        View::new(builder.build(), Arc::clone(&rt_ref), Arc::clone(app))
                    })
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
                        return None;
                    }

                    // Validate SQL and extract dependencies in a single parse
                    let sql = view.sql.as_ref();
                    let statements = match DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {}) {
                        Ok(stmts) => stmts,
                        Err(e) => {
                            if log_errors.0 {
                                metrics::views::LOAD_ERROR.add(1, &[]);
                                tracing::error!("View '{}' has invalid SQL: {}", view.name, e);
                                status.update_view(&view.name, status::ComponentStatus::error_with_message(e.to_string()));
                            }
                            return None;
                        }
                    };

                    if statements.is_empty() {
                        if log_errors.0 {
                            metrics::views::LOAD_ERROR.add(1, &[]);
                            tracing::error!("View '{}' has empty SQL statement", view.name);
                            status.update_view(&view.name, status::ComponentStatus::error_with_message("Empty SQL statement".to_string()));
                        }
                        return None;
                    }

                    if statements.len() > 1 {
                        if log_errors.0 {
                            metrics::views::LOAD_ERROR.add(1, &[]);
                            tracing::error!(
                                "View '{}' contains multiple SQL statements. Only one SELECT statement is allowed per view",
                                view.name
                            );
                            status.update_view(&view.name, status::ComponentStatus::error_with_message("Multiple SQL statements".to_string()));
                        }
                        return None;
                    }

                    let statement = statements.into_iter().next()?;

                    // Validate it's a SELECT statement (views should only contain queries)
                    // DataFusion's Statement enum wraps sqlparser statements
                    let is_query = match &statement {
                        datafusion::sql::parser::Statement::Statement(boxed_stmt) => {
                            matches!(**boxed_stmt, datafusion::sql::sqlparser::ast::Statement::Query(_))
                        }
                        _ => false,
                    };

                    if !is_query {
                        if log_errors.0 {
                            metrics::views::LOAD_ERROR.add(1, &[]);
                            tracing::error!(
                                "View '{}' must contain a SELECT query",
                                view.name
                            );
                            status.update_view(&view.name, status::ComponentStatus::error_with_message("View must contain a SELECT query".to_string()));
                        }
                        return None;
                    }

                    // Extract dependencies from the validated statement
                    let dependencies = view::get_dependent_table_names(&statement);

                    // What the acceleration block asks for and the runtime will not do as
                    // written — settings `enabled: false` discards (#13514), the deprecated
                    // `acceleration.ready_state` (#13749) — is the same for a view as for a
                    // dataset. Reported from here — the load path, behind `log_errors` —
                    // rather than from `ViewBuilder::try_from`, which read-only callers run too.
                    //
                    // Last, after every rejection above has had its chance: a view that
                    // collides with a dataset name or fails SQL validation never loads, so
                    // nothing of its acceleration block is silently discarded and warning
                    // about it would only add noise to the error that actually matters.
                    crate::init::dataset::warn_about_acceleration_block(
                        crate::component::AcceleratedComponent::View,
                        &spicepod_view.name,
                        spicepod_view.acceleration.as_ref(),
                        log_errors,
                    );

                    Some(ValidatedView {
                        view: Arc::new(view),
                        dependencies,
                    })
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

    #[cfg(feature = "duckdb")]
    pub(crate) async fn get_initialized_views(
        self: Arc<Self>,
        app: &Arc<App>,
        log_errors: LogErrors,
    ) -> Vec<Arc<View>> {
        let valid_views = Arc::clone(&self).get_valid_views(app, log_errors);
        futures::stream::iter(valid_views)
            .filter_map(|validated_view| async move {
                let view = &validated_view.view;
                match (view.is_accelerated(), view.is_accelerator_initialized().await) {
                    (true, true) | (false, _) => Some(Arc::clone(view)),
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

    /// Initialize views configured with accelerators before registering the datasets.
    /// This ensures that the required resources for acceleration are available before registration,
    /// which is important for acceleration federation for some acceleration engines (e.g. `DuckDB`).
    pub(crate) async fn initialize_views_accelerators(&self, views: &[ValidatedView]) {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        for validated_view in views {
            let view = &validated_view.view;
            // Non-accelerated views or disabled acceleration don't need initialization
            if view.acceleration.as_ref().is_none_or(|acc| !acc.enabled) {
                continue;
            }

            let Some(acceleration_settings) = &view.acceleration else {
                unreachable!("acceleration is Some and enabled");
            };

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
                    self.status.update_view(
                        view_name,
                        status::ComponentStatus::error_with_message(err.to_string()),
                    );
                    metrics::views::LOAD_ERROR.add(1, &[]);
                    warn_spaced!(spaced_tracer, "{} {err}", view_name.table());
                    continue;
                }
            };

            match accelerator.init(view.as_ref()).await.context(
                AcceleratorInitializationFailedSnafu {
                    name: acceleration_settings.engine.to_string(),
                },
            ) {
                Ok(_) => {
                    // Initialization successful, continue to next view
                }
                Err(err) => {
                    let view_name = &view.name;
                    self.status.update_view(
                        view_name,
                        status::ComponentStatus::error_with_message(err.to_string()),
                    );
                    metrics::views::LOAD_ERROR.add(1, &[]);
                    warn_spaced!(spaced_tracer, "{} {err}", view_name.table());
                }
            }
        }
    }

    fn load_view(self: Arc<Self>, view: &Arc<View>, secrets: Arc<RwLock<Secrets>>) -> Result<()> {
        self.load_view_signalling(view, secrets, None, None, ViewStart::ExistingAcceleration)
    }

    /// Registers `view` again from its definition, after the plan of its registration
    /// from its existing acceleration (`registered`) showed that acceleration was built
    /// from another definition. Nothing is done once `registered` is no longer the
    /// view's table: a Spicepod change removed or replaced it.
    ///
    /// The views that read it planned against the schema it was served with, or failed
    /// to plan against it, so they are registered again too, each once the views it
    /// reads among them are. A view whose registration is still pending has not planned
    /// yet, and plans against the new definition itself.
    pub(crate) async fn reregister_view_from_definition(
        self: Arc<Self>,
        view: Arc<View>,
        registered: TableInstance,
    ) {
        let (ended, readers) = {
            let _serialize = self.apply_app_lock.lock().await;
            if !self.df.table_instance_is_current(&registered).await {
                return;
            }
            tracing::info!(
                "View {} was redefined since its acceleration was built; rebuilding the acceleration from its definition.",
                view.name
            );
            let Some(app) = self.read_app().await else {
                return;
            };
            let validated_views = Arc::clone(&self).get_valid_views(&app, LogErrors(false));
            let readers = views_reading(&self.df.ctx.state(), &validated_views, &view.name);
            let ended = Arc::clone(&self)
                .replace_view(&view, ViewStart::Definition)
                .await;
            (ended, readers)
        };

        // Cancelled once each view's registration has ended, so a reader waits only for
        // the views it reads, and one that cannot register holds up no other branch.
        let mut registered: HashMap<ResolvedTableReference, CancellationToken> =
            HashMap::from([(resolve_table_reference(view.name.clone()), ended)]);
        for (reader, _) in &readers {
            registered.insert(
                resolve_table_reference(reader.name.clone()),
                CancellationToken::new(),
            );
        }
        let registered = Arc::new(registered);
        for (reader, reads) in readers {
            let runtime = Arc::clone(&self);
            let registered = Arc::clone(&registered);
            tokio::spawn(async move {
                let _registered = registered
                    .get(&resolve_table_reference(reader.name.clone()))
                    .map(|token| token.clone().drop_guard());
                for table in &reads {
                    if let Some(token) = registered.get(table) {
                        token.cancelled().await;
                    }
                }
                if let Some(ended) = runtime.replace_reader(&reader).await {
                    ended.cancelled().await;
                }
            });
        }
    }

    /// Registers `reader` again, unless a Spicepod change since removed or redefined it
    /// (and registered what it changed itself) or its registration is still pending.
    async fn replace_reader(self: Arc<Self>, reader: &Arc<View>) -> Option<CancellationToken> {
        let _serialize = self.apply_app_lock.lock().await;
        let app = self.read_app().await?;
        let current = Arc::clone(&self)
            .get_valid_views(&app, LogErrors(false))
            .into_iter()
            .find(|vv| vv.view.name == reader.name)?;
        if current.view != *reader || self.view_loads.is_pending(&reader.name) {
            return None;
        }
        Some(
            Arc::clone(&self)
                .replace_view(&current.view, ViewStart::ExistingAcceleration)
                .await,
        )
    }

    /// Registers `view` again as a Spicepod change would, in a registration that a
    /// later Spicepod change stops as it stops a startup registration. Returns a token
    /// cancelled once that registration has ended, registered or not.
    async fn replace_view(
        self: Arc<Self>,
        view: &Arc<View>,
        start: ViewStart,
    ) -> CancellationToken {
        let load = self.view_loads.begin_replacement(&view.name).await;
        self.status
            .update_view(&view.name, status::ComponentStatus::Refreshing);
        Arc::clone(&self).remove_view(&view.name).await;
        let secrets = self.secrets();
        let ended = CancellationToken::new();
        if let Err(e) = self.load_view_signalling(
            view,
            secrets,
            Some(ended.clone().drop_guard()),
            Some(load),
            start,
        ) {
            tracing::error!("Unable to load view {}: {e}", view.name);
        }
        ended
    }

    /// Like [`Self::load_view`], holding `registered` until the view's registration
    /// task ends (registered or failed), so views depending on it wait for that
    /// rather than for the task to be spawned.
    fn load_view_signalling(
        self: Arc<Self>,
        view: &Arc<View>,
        secrets: Arc<RwLock<Secrets>>,
        registered: Option<tokio_util::sync::DropGuard>,
        startup_load: Option<crate::init::view_loads::ViewLoad>,
        start: ViewStart,
    ) -> Result<()> {
        let df = Arc::clone(&self.df);
        let register_task = df
            .register_view(Arc::clone(view), secrets, start)
            .context(UnableToAttachViewSnafu)
            .inspect_err(|e| {
                self.status.update_view(
                    &view.name,
                    status::ComponentStatus::error_with_message(e.to_string()),
                );
            })?;

        let runtime = Arc::clone(&self);
        let view = Arc::clone(view);
        let df = Arc::clone(&self.df);

        tokio::task::spawn(async move {
            let view_name = view.name.clone();
            // A startup registration superseded by a Spicepod change is aborted
            // before it can register the replaced definition.
            let superseded = startup_load
                .as_ref()
                .map(crate::init::view_loads::ViewLoad::token);
            let mut register_task = register_task;
            let notifier = match superseded {
                Some(superseded) => tokio::select! {
                    notifier = &mut register_task => notifier,
                    () = superseded.cancelled() => {
                        register_task.abort();
                        let _ = register_task.await;
                        tracing::debug!("Startup registration of view '{view_name}' superseded by a Spicepod change.");
                        return;
                    }
                },
                None => register_task.await,
            };
            drop(startup_load);
            drop(registered);
            match notifier {
                Ok(Some((instance, completion))) => {
                    // `instance` was captured where the view was registered, so
                    // the schedule below is checked against the view this task
                    // actually registered rather than against whatever the name
                    // resolves to once the refresh lands.
                    match df
                        .await_refresh_completion(instance, Some(completion))
                        .await
                    {
                        DeferredRefreshOutcome::Apply => {}
                        DeferredRefreshOutcome::Abandoned => {
                            // The accelerated table was dropped before its initial
                            // refresh landed. Creating the schedule now would
                            // resurrect one for a view that is no longer there.
                            tracing::debug!(
                                "Accelerated view '{view_name}' was removed before its initial refresh completed; not creating a refresh schedule."
                            );
                            return;
                        }
                        DeferredRefreshOutcome::Failed => {
                            tracing::debug!(
                                "Accelerated view '{view_name}' initial refresh failed terminally; not creating a refresh schedule."
                            );
                            return;
                        }
                        DeferredRefreshOutcome::TableChanged => {
                            // A `remove_view` landed after the completion was
                            // recorded, so this would give a removed view a live
                            // refresh schedule, or a replaced one a schedule built
                            // from its predecessor's settings.
                            tracing::debug!(
                                "Accelerated view '{view_name}' was removed or replaced after its initial refresh completed; not creating a refresh schedule."
                            );
                            return;
                        }
                    }

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
        if self.df.table_exists(name) {
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

        // Removing a view may cause cached LogicalPlans that reference it
        // to be obsolete, so we invalidate them.
        self.df.clear_cached_plans().await;

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
        let validated_views = Arc::clone(&self).get_valid_views(new_app, LogErrors(true));
        let existing_validated_views =
            Arc::clone(&self).get_valid_views(current_app, LogErrors(false));

        let views_that_changed = validated_views
            .iter()
            .filter_map(|vv| {
                match existing_validated_views
                    .iter()
                    .find(|evv| vv.view.name == evv.view.name)
                {
                    Some(old_vv) => {
                        if old_vv.view == vv.view {
                            None // No change, don't include
                        } else {
                            Some(vv.view.name.clone()) // Changed, include the name
                        }
                    }
                    None => Some(vv.view.name.clone()), // New view, include the name
                }
            })
            .collect_vec();

        // Stop startup registrations still waiting for views this change replaces
        // or removes, so they cannot register the old definition afterwards.
        for name in &views_that_changed {
            self.view_loads.supersede(name).await;
        }
        for view in &current_app.views {
            if !new_app.views.iter().any(|v| v.name == view.name)
                && let Ok(builder) = ViewBuilder::try_from(view.clone())
            {
                self.view_loads.supersede(&builder.name).await;
            }
        }

        // Remove views that are no longer in the app
        for view in &current_app.views {
            if !new_app.views.iter().any(|v| v.name == view.name) {
                let view_builder =
                    match ViewBuilder::try_from(view.clone()).map_err(crate::Error::from) {
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
        let affected_views_in_order_of_dependencies = {
            // Build dependency map from already-parsed dependencies
            let view_names: HashSet<_> = validated_views
                .iter()
                .map(|vv| vv.view.name.clone())
                .collect();

            // Filter out dependencies that are not views (e.g., datasets) to prevent false cycle detection
            let view_only_deps: HashMap<_, _> = validated_views
                .iter()
                .map(|vv| {
                    let filtered_deps: Vec<_> = vv
                        .dependencies
                        .iter()
                        .filter(|dep| view_names.contains(dep))
                        .cloned()
                        .collect();
                    (vv.view.name.clone(), filtered_deps)
                })
                .collect();

            construct_effected_in_topological_order(view_only_deps, &views_that_changed)
                .unwrap_or_else(|| {
                    validated_views
                        .iter()
                        .map(|vv| vv.view.name.clone())
                        .collect()
                })
        };

        for view_name in affected_views_in_order_of_dependencies {
            // An unchanged view re-applied because a dependency changed is
            // registered here, so its startup registration must not also run.
            self.view_loads.supersede(&view_name).await;
            if let Some(validated_view) =
                validated_views.iter().find(|vv| vv.view.name == view_name)
            {
                let runtime = Arc::clone(&self);
                if existing_validated_views
                    .iter()
                    .any(|evv| evv.view.name == validated_view.view.name)
                {
                    // Update view even if unchanged, as it may have dependencies that have changed
                    runtime.update_view(&validated_view.view).await;
                } else {
                    runtime.status.update_view(
                        &validated_view.view.name,
                        status::ComponentStatus::Initializing,
                    );
                    let secrets = runtime.secrets();
                    let _ = runtime.load_view(&validated_view.view, secrets);
                }
            }
        }
    }
}

#[cfg(test)]
mod startup_view_wait_tests {
    use super::*;

    fn r(name: &str) -> ResolvedTableReference {
        resolve_table_reference(TableReference::parse_str(name))
    }

    fn reads(views: &[(&str, &[&str])]) -> HashMap<ResolvedTableReference, Option<TablesRead>> {
        views
            .iter()
            .map(|(view, tables)| {
                (
                    r(view),
                    Some(
                        tables
                            .iter()
                            .map(|table| (r(table), TableReference::parse_str(table)))
                            .collect(),
                    ),
                )
            })
            .collect()
    }

    fn flagged(views: &HashMap<ResolvedTableReference, Option<TablesRead>>) -> Vec<String> {
        let mut flagged: Vec<String> = views_in_or_behind_cycles(views)
            .iter()
            .map(ToString::to_string)
            .collect();
        flagged.sort();
        flagged
    }

    #[test]
    fn detects_cycle_through_qualified_names() {
        assert_eq!(
            flagged(&reads(&[("a", &["public.b"]), ("b", &["spice.public.a"])])),
            ["spice.public.a", "spice.public.b"]
        );
    }

    #[test]
    fn chain_and_self_reference_are_not_cycles() {
        assert!(
            flagged(&reads(&[
                ("a", &["orders", "a"]),
                ("b", &["public.a", "a"]),
                ("c", &["b"]),
            ]))
            .is_empty()
        );
    }

    #[test]
    fn a_view_whose_sql_did_not_resolve_links_nothing() {
        let mut views = reads(&[("a", &["b"])]);
        views.insert(r("b"), None);
        assert!(flagged(&views).is_empty());
    }

    /// Only the views in a cycle, and those reading one, give up waiting on the
    /// views they read: an unrelated cycle elsewhere in the Spicepod must not
    /// leave a view that reads another view only in a subquery to race it.
    #[test]
    fn only_views_in_or_behind_a_cycle_are_flagged() {
        assert_eq!(
            flagged(&reads(&[
                ("a", &["b"]),
                ("b", &["a"]),
                ("behind", &["public.a"]),
                ("reader", &["v1", "orders"]),
                ("v1", &["orders"]),
            ])),
            ["spice.public.a", "spice.public.b", "spice.public.behind"]
        );
    }

    /// A view waits on exactly what planning it looks up, so a table read only
    /// by a subquery is waited for too, and a CTE is not mistaken for a table.
    #[test]
    fn view_reads_include_subquery_tables_and_exclude_ctes() {
        let state = datafusion::prelude::SessionContext::new().state();
        let tables = view_reads(
            &state,
            "WITH recent AS (SELECT * FROM orders) \
             SELECT * FROM recent WHERE customer_id IN (SELECT id FROM public.customers) \
             AND EXISTS (SELECT 1 FROM spice.public.flags)",
        )
        .expect("the SQL resolves");
        let mut resolved: Vec<String> = tables.keys().map(ToString::to_string).collect();
        resolved.sort();
        assert_eq!(
            resolved,
            [
                "spice.public.customers",
                "spice.public.flags",
                "spice.public.orders"
            ]
        );
        assert_eq!(view_reads(&state, "SELECT 1; SELECT 2"), None);
    }

    #[test]
    fn waiting_message_names_the_view_and_what_it_waits_for() {
        let view = TableReference::bare("v_bad");
        assert_eq!(
            view_waiting_message(&view, &["bad".to_string()], ViewWait::WhatItReads),
            "View 'v_bad' is still waiting for 'bad' to load before it registers."
        );
        let pending: Vec<String> = (0..12).map(|i| format!("ds{i:02}")).collect();
        assert_eq!(
            view_waiting_message(
                &view,
                &pending,
                ViewWait::EveryDataset(Some(&TableReference::parse_str("pg.public.events")))
            ),
            "View 'v_bad' is still waiting for every dataset to load before it registers, because it reads 'pg.public.events', which is not a dataset or view loading at startup. Still loading: 'ds00', 'ds01', 'ds02', 'ds03', 'ds04', 'ds05', 'ds06', 'ds07', 'ds08', 'ds09' and 2 more."
        );
        assert_eq!(
            view_waiting_message(&view, &["bad".to_string()], ViewWait::EveryDataset(None)),
            "View 'v_bad' is still waiting for every dataset to load before it registers. Still loading: 'bad'."
        );
    }
}
