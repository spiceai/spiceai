/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! SQL results-cache warmup, enabled with
//! `runtime.caching.sql_results.warmup: on_first_refresh` (and
//! `sql_results.enabled: true`).
//!
//! The first [`MAX_WARMUP_PLANS`] distinct query *shapes* (logical plans with
//! equality-filter values replaced by placeholders) are remembered and written
//! under `.spice/data`, or to `runtime.state.location` when that is set. After
//! a process restart, once accelerated full/append datasets finish their first
//! refresh, those shapes are replayed with `SELECT DISTINCT` of the bound
//! columns until the cache is full. Templates that fail to plan or execute
//! are dropped from the catalog so a full set of stale shapes cannot block
//! recording current queries. Datasets stay not ready until that warmup
//! completes, so `/v1/ready` does not succeed on a cold cache. Each replay
//! (and its stream drain) is bounded by `runtime.query.timeout` or a default,
//! and cancelled on runtime shutdown, so a stalled Internal-protocol query
//! cannot hold readiness forever.
//!
//! Only [`CacheNamespace::Public`] plans are recorded and replayed. Authenticated
//! (principal-scoped) and system traffic is skipped: results-cache keys include the
//! namespace, so warming into `Public` could never satisfy a principal-scoped
//! request, and persisting principal ids into the warmup catalog is undesirable.

use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use app::App;
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use runtime_request_context::{
    CacheControl, CacheKeyType, CacheNamespace, Protocol, RequestContext,
};
use runtime_status as status;

use object_store::ObjectStore;
use object_store_occ::{InsertResult, ObjectState, UpdateResult};
use runtime_secrets::Secrets;
use spicepod::component::runtime::RuntimeState;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::accelerated::AcceleratedTable;
use crate::accelerated::RefreshCompletion;
use crate::component::dataset::acceleration::RefreshMode;
use crate::datafusion::{DataFusion, SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

use super::QueryBuilder;
use super::ResultsCacheMode;
use super::warmup_plan::{
    WarmupTemplate, distinct_keys_sql, template_can_warm, template_from_plan, template_id,
};

/// Distinct plan shapes kept for the next cold start. First N, not hottest N.
const MAX_WARMUP_PLANS: usize = 10;

const WARMUP_STORE_RELATIVE: &str = ".spice/data/results_cache_warmup.json";

/// Object-store key; [`ObjectState`] writes `{prefix}{key}.json`.
const WARMUP_STATE_KEY: &str = "results_cache_warmup";

const MAX_REMOTE_PERSIST_ATTEMPTS: usize = 8;

/// Per-replay wall-clock bound. Warmup uses [`Protocol::Internal`], which does
/// not inherit `runtime.query.timeout`; without an explicit bound a stalled
/// DISTINCT or drain can hold `/v1/ready` forever.
const DEFAULT_WARMUP_REPLAY_TIMEOUT: Duration = Duration::from_secs(60);

struct WarmupCatalog {
    templates: Vec<WarmupTemplate>,
    ids: HashSet<u64>,
}

enum WarmupPersist {
    Local(PathBuf),
    Remote(Arc<ObjectState<Vec<WarmupTemplate>>>),
}

pub(crate) struct ResultsCacheWarmer {
    enabled: bool,
    catalog: Arc<parking_lot::Mutex<WarmupCatalog>>,
    persist_lock: Arc<parking_lot::Mutex<()>>,
    /// Serializes remote `ObjectState` persists. Concurrent `update` calls share
    /// one cached version, so a stale task can overwrite a newer catalog without
    /// a conflict; one writer at a time keeps recorded shapes.
    remote_persist_lock: Arc<tokio::sync::Mutex<()>>,
    count: Arc<AtomicUsize>,
    started: AtomicBool,
    persist: WarmupPersist,
}

impl ResultsCacheWarmer {
    /// Load a local warmup catalog without blocking a Tokio worker.
    pub(crate) async fn new(store_path: PathBuf, enabled: bool) -> Self {
        let loaded = if enabled {
            load_templates(&store_path).await
        } else {
            Vec::new()
        };
        Self::from_loaded(&loaded, enabled, WarmupPersist::Local(store_path))
    }

    /// Local warmer that does not read the catalog. Sync `DataFusion` construction
    /// uses this fallback; callers that need persisted shapes must use [`Self::new`].
    pub(crate) fn new_unloaded(store_path: PathBuf, enabled: bool) -> Self {
        Self::from_loaded(&[], enabled, WarmupPersist::Local(store_path))
    }

    async fn from_object_store(
        store: Arc<dyn ObjectStore>,
        base_prefix: &str,
        enabled: bool,
    ) -> Self {
        let prefix = object_state_prefix(base_prefix);
        let state = Arc::new(ObjectState::new(store).with_prefix(prefix));
        let loaded = if enabled {
            match state.get(WARMUP_STATE_KEY).await {
                Ok(Some(templates)) => templates,
                Ok(None) => Vec::new(),
                Err(e) => {
                    tracing::debug!("Failed to load SQL results cache warmup catalog: {e}");
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };
        Self::from_loaded(&loaded, enabled, WarmupPersist::Remote(state))
    }

    fn from_loaded(loaded: &[WarmupTemplate], enabled: bool, persist: WarmupPersist) -> Self {
        if enabled {
            tracing::info!(
                "SQL results cache warmup is enabled: the first {MAX_WARMUP_PLANS} distinct query plans will be recorded and replayed after the first full or append refresh until the cache is full"
            );
        }
        // Persisted catalogs are not trusted: a stale or edited file can
        // exceed the advertised cap or repeat the same shape. Dedup and
        // cap here so replay (which holds readiness) cannot run unbounded.
        let loaded = merge_templates(&[], loaded);
        let ids = loaded.iter().map(template_id).collect::<HashSet<_>>();
        let count = loaded.len();
        Self {
            enabled,
            catalog: Arc::new(parking_lot::Mutex::new(WarmupCatalog {
                templates: loaded,
                ids,
            })),
            persist_lock: Arc::new(parking_lot::Mutex::new(())),
            remote_persist_lock: Arc::new(tokio::sync::Mutex::new(())),
            count: Arc::new(AtomicUsize::new(count)),
            started: AtomicBool::new(false),
            persist,
        }
    }

    fn claim_warmup(&self) -> bool {
        !self.started.swap(true, Ordering::Relaxed)
    }

    fn templates_snapshot(&self) -> Vec<WarmupTemplate> {
        self.catalog.lock().templates.clone()
    }

    /// Remember this plan if we do not yet have [`MAX_WARMUP_PLANS`] distinct
    /// shapes. Cheap no-op once the set is full. Must not run on the warmup
    /// path itself (those queries use `CurrentRuntimeUngated`).
    ///
    /// Only [`CacheNamespace::Public`] plans are kept; principal-scoped and
    /// system plans are skipped (see module docs).
    pub(crate) fn observe_plan(&self, plan: &LogicalPlan, namespace: &CacheNamespace) {
        if !self.enabled {
            return;
        }
        if !matches!(namespace, CacheNamespace::Public) {
            return;
        }
        if self.count.load(Ordering::Relaxed) >= MAX_WARMUP_PLANS {
            return;
        }
        if matches!(
            plan,
            LogicalPlan::Dml(_) | LogicalPlan::Ddl(_) | LogicalPlan::Statement(_)
        ) {
            return;
        }
        if cache::get_logical_plan_input_tables(plan)
            .iter()
            .any(|table| {
                matches!(
                    table.schema(),
                    Some(crate::datafusion::SPICE_RUNTIME_SCHEMA)
                )
            })
        {
            return;
        }
        let Some(template) = template_from_plan(plan) else {
            return;
        };
        if !template_can_warm(&template) {
            return;
        }
        let id = template_id(&template);
        {
            let mut catalog = self.catalog.lock();
            if catalog.ids.contains(&id) || catalog.ids.len() >= MAX_WARMUP_PLANS {
                return;
            }
            catalog.ids.insert(id);
            catalog.templates.push(template);
            self.count.store(catalog.templates.len(), Ordering::Relaxed);
        }
        self.schedule_persist();
    }

    /// Drop templates that failed to plan or execute so a full catalog can
    /// record current query shapes.
    fn drop_and_persist(&self, ids: &[u64]) {
        if ids.is_empty() {
            return;
        }
        let exclude: HashSet<u64> = ids.iter().copied().collect();
        {
            let mut catalog = self.catalog.lock();
            catalog
                .templates
                .retain(|template| !exclude.contains(&template_id(template)));
            catalog.ids = catalog.templates.iter().map(template_id).collect();
            self.count.store(catalog.templates.len(), Ordering::Relaxed);
        }
        self.schedule_persist_excluding(exclude);
    }

    fn schedule_persist(&self) {
        self.schedule_persist_excluding(HashSet::new());
    }

    fn schedule_persist_excluding(&self, exclude: HashSet<u64>) {
        match &self.persist {
            WarmupPersist::Local(path) => {
                let catalog = Arc::clone(&self.catalog);
                let persist_lock = Arc::clone(&self.persist_lock);
                let path = path.clone();
                tokio::spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        let _persist = persist_lock.lock();
                        let snapshot = catalog.lock().templates.clone();
                        save_templates(&path, &snapshot)
                    })
                    .await;
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            tracing::debug!(
                                "Failed to persist SQL results cache warmup catalog: {e}"
                            );
                        }
                        Err(e) => {
                            tracing::debug!(
                                "Failed to persist SQL results cache warmup catalog: {e}"
                            );
                        }
                    }
                });
            }
            WarmupPersist::Remote(state) => {
                let catalog = Arc::clone(&self.catalog);
                let count = Arc::clone(&self.count);
                let state = Arc::clone(state);
                let remote_persist_lock = Arc::clone(&self.remote_persist_lock);
                tokio::spawn(async move {
                    let _persist = remote_persist_lock.lock().await;
                    // Re-snapshot under the lock so this write includes any
                    // templates observed while we waited for earlier persists.
                    if exclude.is_empty() {
                        persist_remote(state, catalog, count).await;
                    } else {
                        persist_remote_excluding(state, catalog, count, exclude).await;
                    }
                });
            }
        }
    }
}

async fn load_templates(path: &Path) -> Vec<WarmupTemplate> {
    let Ok(bytes) = tokio::fs::read(path).await else {
        return Vec::new();
    };
    serde_json::from_slice(&bytes).unwrap_or_default()
}

fn save_templates(path: &Path, templates: &[WarmupTemplate]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(templates).map_err(std::io::Error::other)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, path)
}

fn object_state_prefix(base_prefix: &str) -> String {
    if base_prefix.is_empty() {
        String::new()
    } else {
        format!("{}/", base_prefix.trim_end_matches('/'))
    }
}

fn without_ids(templates: &[WarmupTemplate], exclude: &HashSet<u64>) -> Vec<WarmupTemplate> {
    if exclude.is_empty() {
        return templates.to_vec();
    }
    templates
        .iter()
        .filter(|template| !exclude.contains(&template_id(template)))
        .cloned()
        .collect()
}

fn merge_templates(base: &[WarmupTemplate], extra: &[WarmupTemplate]) -> Vec<WarmupTemplate> {
    let mut out = Vec::new();
    let mut ids = HashSet::new();
    for template in base.iter().chain(extra.iter()) {
        if out.len() >= MAX_WARMUP_PLANS {
            break;
        }
        let id = template_id(template);
        if ids.insert(id) {
            out.push(template.clone());
        }
    }
    out
}

fn apply_catalog(
    catalog: &parking_lot::Mutex<WarmupCatalog>,
    count: &AtomicUsize,
    templates: &[WarmupTemplate],
) {
    let mut catalog = catalog.lock();
    // A concurrent observation may have added templates after this task
    // snapped `local`. Keep any live entries that are not in `templates`
    // so a stale remote apply cannot permanently drop them.
    let merged = merge_templates(templates, &catalog.templates);
    catalog.ids = merged.iter().map(template_id).collect();
    count.store(merged.len(), Ordering::Relaxed);
    catalog.templates = merged;
}

async fn persist_remote(
    state: Arc<ObjectState<Vec<WarmupTemplate>>>,
    catalog: Arc<parking_lot::Mutex<WarmupCatalog>>,
    count: Arc<AtomicUsize>,
) {
    persist_remote_excluding(state, catalog, count, HashSet::new()).await;
}

async fn persist_remote_excluding(
    state: Arc<ObjectState<Vec<WarmupTemplate>>>,
    catalog: Arc<parking_lot::Mutex<WarmupCatalog>>,
    count: Arc<AtomicUsize>,
    exclude: HashSet<u64>,
) {
    let mut local = without_ids(&catalog.lock().templates, &exclude);
    for _ in 0..MAX_REMOTE_PERSIST_ATTEMPTS {
        // Compare `merged` to the stored catalog, not the already-filtered
        // remote. Filtering stale ids in memory and then returning because
        // `merged == remote` would leave those ids on the object store.
        let (stored, version) = match state.get_with_version(WARMUP_STATE_KEY).await {
            Ok(Some((templates, version))) => (templates, Some(version)),
            Ok(None) => (Vec::new(), None),
            Err(e) => {
                tracing::debug!("Failed to persist SQL results cache warmup catalog: {e}");
                return;
            }
        };
        let remote = without_ids(&stored, &exclude);
        let merged = merge_templates(&remote, &local);
        if merged == stored {
            apply_catalog(catalog.as_ref(), count.as_ref(), &merged);
            return;
        }
        if let Some(version) = version {
            match state
                .update_with_version(WARMUP_STATE_KEY, &merged, version)
                .await
            {
                Ok(UpdateResult::Ok) => {
                    apply_catalog(catalog.as_ref(), count.as_ref(), &merged);
                    return;
                }
                Ok(UpdateResult::NotFound) => {
                    local = merged;
                }
                Ok(UpdateResult::Conflict { current }) => {
                    local = merge_templates(&without_ids(&current, &exclude), &merged);
                }
                Err(e) => {
                    tracing::debug!("Failed to persist SQL results cache warmup catalog: {e}");
                    return;
                }
            }
        } else {
            match state.insert(WARMUP_STATE_KEY, &merged).await {
                Ok(InsertResult::Ok) => {
                    apply_catalog(catalog.as_ref(), count.as_ref(), &merged);
                    return;
                }
                Ok(InsertResult::AlreadyExists) => {
                    local = merged;
                }
                Err(e) => {
                    tracing::debug!("Failed to persist SQL results cache warmup catalog: {e}");
                    return;
                }
            }
        }
    }
    tracing::debug!("Failed to persist SQL results cache warmup catalog after retries");
}

/// Build the warmer, loading from `runtime.state` when that is set.
pub(crate) async fn build_results_cache_warmer(
    enabled: bool,
    runtime_state: Option<&RuntimeState>,
    secrets: Arc<RwLock<Secrets>>,
    io_runtime: Handle,
) -> ResultsCacheWarmer {
    let Some(state) = runtime_state.filter(|_| enabled) else {
        return ResultsCacheWarmer::new(default_warmup_store_path(), enabled).await;
    };

    match crate::object_store_state::build_object_store(
        secrets,
        io_runtime,
        &state.location,
        state.params.as_ref(),
        "SQL results cache warmup state",
    )
    .await
    {
        Ok((store, prefix)) => {
            tracing::info!(
                "SQL results cache warmup will store query plans at '{}'",
                state.location
            );
            ResultsCacheWarmer::from_object_store(store, &prefix, true).await
        }
        Err(error) => {
            tracing::warn!(
                "Failed to initialize SQL results cache warmup state at '{}', so plan shapes will be stored on the local disk instead. Cause: {error}",
                state.location
            );
            ResultsCacheWarmer::new(default_warmup_store_path(), true).await
        }
    }
}

impl DataFusion {
    /// Record a cacheable user query's plan shape for the next cold start.
    pub(crate) fn observe_results_cache_warmup_plan(
        &self,
        plan: &LogicalPlan,
        namespace: &CacheNamespace,
    ) {
        self.results_cache_warmer.observe_plan(plan, namespace);
    }

    /// Whether warmup will replay stored plans this process, so dataset
    /// `Ready` must wait until that replay finishes.
    pub(crate) fn results_cache_warmup_holds_ready(&self) -> bool {
        self.results_cache_warmer.enabled
            && self.results_cache_provider().is_some()
            && !self.results_cache_warmer.templates_snapshot().is_empty()
    }

    /// Replay persisted plan shapes after the first full/append refresh, then
    /// release held dataset `Ready`. Once-only for this process.
    pub(crate) fn spawn_results_cache_warmup(
        self: &Arc<Self>,
        status: Arc<status::RuntimeStatus>,
        app: Option<Arc<App>>,
    ) {
        let templates = self.results_cache_warmer.templates_snapshot();
        if templates.is_empty() || !self.results_cache_warmer.claim_warmup() {
            status.release_dataset_ready();
            return;
        }

        let df = Arc::clone(self);
        let refresh_runtime = self.refresh_runtime().cloned();
        tokio::spawn(async move {
            if !df.wait_for_first_full_append_refresh(&status).await {
                status.release_dataset_ready();
                return;
            }
            let replay_timeout = warmup_replay_timeout(app.as_ref());
            let shutdown = status.shutdown_token();
            // Build the hold *before* hopping onto the refresh runtime so
            // dropping an unpolled spawned task still releases `/v1/ready`.
            let run = run_warmup_releasing_ready(Arc::clone(&status), shutdown.clone(), {
                let df = Arc::clone(&df);
                async move {
                    df.run_warmup_templates_bounded(
                        &templates,
                        app.as_ref(),
                        shutdown,
                        replay_timeout,
                    )
                    .await;
                }
            });
            if let Some(runtime) = refresh_runtime {
                runtime.spawn(run);
            } else {
                run.await;
            }
        });
    }

    async fn wait_for_first_full_append_refresh(&self, status: &status::RuntimeStatus) -> bool {
        let shutdown = status.shutdown_token();
        loop {
            if status.is_shutdown() {
                return false;
            }
            if self.accelerated_initial_loads_done(status).await {
                return true;
            }
            tokio::select! {
                () = shutdown.cancelled() => return false,
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    }

    async fn accelerated_initial_loads_done(&self, runtime_status: &status::RuntimeStatus) -> bool {
        let names = self.accelerated_table_names().await;
        for name in names {
            let Ok(provider) = self.get_accelerated_table_provider(&name.to_string()).await else {
                return false;
            };
            let Some(table) = spice_table::find_layer::<AcceleratedTable>(
                provider.as_ref(),
                spice_table::LayerWalk::Read,
            ) else {
                continue;
            };
            if !first_full_or_append_refresh_settled(table, runtime_status, &name).await {
                return false;
            }
        }
        true
    }

    #[cfg(test)]
    async fn run_warmup_templates(
        self: &Arc<Self>,
        templates: &[WarmupTemplate],
        app: Option<&Arc<App>>,
    ) {
        self.run_warmup_templates_bounded(
            templates,
            app,
            CancellationToken::new(),
            warmup_replay_timeout(app),
        )
        .await;
    }

    async fn run_warmup_templates_bounded(
        self: &Arc<Self>,
        templates: &[WarmupTemplate],
        app: Option<&Arc<App>>,
        shutdown: CancellationToken,
        replay_timeout: Duration,
    ) {
        let Some(cache_provider) = self.results_cache_provider() else {
            return;
        };

        tracing::info!(
            "Warming SQL results cache from {} stored query plan{}, using distinct dataset keys, until the cache is full",
            templates.len(),
            if templates.len() == 1 { "" } else { "s" },
        );

        let cache_key_type = CacheKeyType::from_app_runtime(app);
        let request_context = Arc::new(
            RequestContext::builder(Protocol::Internal)
                .with_cache_control(CacheControl::Cache(cache_key_type))
                .with_cache_namespace(CacheNamespace::Public)
                .with_query_timeout(Some(replay_timeout))
                .with_cancellation_token(shutdown.clone())
                .build(),
        );

        let mut stored = 0_u64;
        let mut unusable = Vec::new();
        for template in templates {
            if shutdown.is_cancelled() {
                break;
            }
            cache_provider.run_pending_tasks().await;
            if cache_provider.size().await >= cache_provider.max_size() {
                break;
            }
            match self
                .warm_one_template(template, &request_context, cache_provider.as_ref())
                .await
            {
                Ok(WarmupReplay::Stored(count)) => stored += count,
                Ok(WarmupReplay::Unusable) => unusable.push(template_id(template)),
                Err(WarmupBound::TimedOut) => {
                    tracing::warn!(
                        "SQL results cache warmup skipped a stored query plan that exceeded {replay_timeout:?}, so `/v1/ready` will not wait for that plan. Increase `runtime.query.timeout` if warmup queries need more time. See: https://spiceai.org/docs/reference/spicepod"
                    );
                }
                Err(WarmupBound::Cancelled) => break,
            }
        }

        if !unusable.is_empty() {
            let dropped = unusable.len();
            tracing::info!(
                "SQL results cache warmup dropped {dropped} stored query plan{} that failed to replay, so later queries can be recorded for the next cold start",
                if dropped == 1 { "" } else { "s" }
            );
            self.results_cache_warmer.drop_and_persist(&unusable);
        }

        cache_provider.run_pending_tasks().await;
        tracing::info!(
            "Finished warming SQL results cache: stored {stored} results ({:.2}). Later refreshes will not re-warm",
            byte_unit::Byte::from_u64(cache_provider.size().await)
                .get_adjusted_unit(byte_unit::Unit::MiB),
        );
    }

    async fn warm_one_template(
        self: &Arc<Self>,
        template: &WarmupTemplate,
        request_context: &Arc<RequestContext>,
        cache_provider: &cache::QueryResultsCacheProvider,
    ) -> Result<WarmupReplay, WarmupBound> {
        if template.bindings.is_empty() {
            return execute_warmup_sql(
                self,
                &template.sql,
                None,
                request_context,
                request_context.cancellation_token(),
            )
            .await
            .map(|ok| {
                if ok {
                    WarmupReplay::Stored(1)
                } else {
                    WarmupReplay::Unusable
                }
            });
        }
        let Some(distinct_sql) = distinct_keys_sql(template) else {
            return Ok(WarmupReplay::Unusable);
        };

        warm_distinct_key_rows(
            self,
            &template.sql,
            &distinct_sql,
            request_context,
            cache_provider,
        )
        .await
    }
}

/// Whether warmup may start for this accelerated table.
///
/// Full/Append wait for a per-process refresh outcome recorded after
/// cache invalidation: a successful completion, or a terminal (non-retrying)
/// failure. The reusable `initial_load_completed` flag is not enough:
/// checkpoint-backed tables publish it at construction, before this
/// process's startup refresh (if any) has run.
///
/// A cluster scheduler closes that completion because it never refreshes
/// locally. Warmup then waits for the dataset's `Ready` update from
/// executor `PartitionsLoaded` acks. That update is often held (so
/// `/v1/ready` stays false until warmup finishes); look at the hold, not
/// the visible status.
async fn first_full_or_append_refresh_settled(
    table: &AcceleratedTable,
    status: &status::RuntimeStatus,
    name: &TableReference,
) -> bool {
    first_full_or_append_refresh_settled_for(
        table.refresher().refresh_mode().await,
        table.refresher().refresh_completion().as_ref(),
        status,
        name,
    )
}

fn first_full_or_append_refresh_settled_for(
    mode: RefreshMode,
    completion: Option<&RefreshCompletion>,
    status: &status::RuntimeStatus,
    name: &TableReference,
) -> bool {
    if !matches!(mode, RefreshMode::Full | RefreshMode::Append) {
        return true;
    }
    let Some(completion) = completion else {
        return true;
    };
    if completion.closed_without_a_refresh() {
        // Scheduler: no local refresh will run. Wait for the distributed
        // Ready (often held so `/v1/ready` stays false during warmup), or
        // stop waiting if this dataset or view will never become ready.
        // Accelerated views publish under `view:*`, not `dataset:*`.
        return table_ready_for_warmup(status, name) || table_will_not_become_ready(status, name);
    }
    if completion.has_recorded() || completion.has_terminal_failure() {
        return true;
    }
    // Local Full/Append still in progress or retrying. Disabled is
    // terminal. Error is not: a periodic refresh can still succeed,
    // and warmup is once-only. A one-shot failure records a
    // terminal-failure outcome in `after_refresh_task_completed`
    // instead of settling on Error or on a successful completion.
    table_is_disabled(status, name)
}

fn same_spice_table(left: &TableReference, right: &TableReference) -> bool {
    left.clone()
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
        == right
            .clone()
            .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
}

fn spice_table_status_is(
    statuses: impl IntoIterator<Item = (TableReference, status::ComponentStatus)>,
    name: &TableReference,
    pred: impl Fn(&status::ComponentStatus) -> bool,
) -> bool {
    statuses
        .into_iter()
        .any(|(key, st)| same_spice_table(&key, name) && pred(&st))
}

fn table_ready_for_warmup(status: &status::RuntimeStatus, name: &TableReference) -> bool {
    status
        .dataset_ready_or_held_keys()
        .into_iter()
        .any(|key| same_spice_table(&key, name))
        || spice_table_status_is(status.get_view_statuses(), name, |st| {
            matches!(st, status::ComponentStatus::Ready)
        })
}

fn table_will_not_become_ready(status: &status::RuntimeStatus, name: &TableReference) -> bool {
    let terminal = |st: &status::ComponentStatus| {
        matches!(
            st,
            status::ComponentStatus::Error(_) | status::ComponentStatus::Disabled
        )
    };
    spice_table_status_is(status.get_dataset_statuses(), name, terminal)
        || spice_table_status_is(status.get_view_statuses(), name, terminal)
}

fn table_is_disabled(status: &status::RuntimeStatus, name: &TableReference) -> bool {
    let disabled = |st: &status::ComponentStatus| matches!(st, status::ComponentStatus::Disabled);
    spice_table_status_is(status.get_dataset_statuses(), name, disabled)
        || spice_table_status_is(status.get_view_statuses(), name, disabled)
}

fn warmup_replay_timeout(app: Option<&Arc<App>>) -> Duration {
    app.and_then(|app| app.runtime.query.as_ref())
        .and_then(|query| query.timeout().ok().flatten())
        .unwrap_or(DEFAULT_WARMUP_REPLAY_TIMEOUT)
}

/// Releases the dataset ready-hold when dropped so `/v1/ready` recovers after
/// warmup finishes, times out, is cancelled, or errors.
#[must_use]
struct DatasetReadyHold {
    status: Arc<status::RuntimeStatus>,
}

impl Drop for DatasetReadyHold {
    fn drop(&mut self) {
        self.status.release_dataset_ready();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WarmupBound {
    TimedOut,
    Cancelled,
}

/// Outcome of replaying one stored template. Timeouts stay [`WarmupBound`]
/// so a slow plan is not dropped; planning and execution failures are
/// [`Self::Unusable`] so the catalog can learn current shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WarmupReplay {
    Stored(u64),
    Unusable,
}

async fn bound_warmup_op<T>(
    shutdown: &CancellationToken,
    query_cancel: &CancellationToken,
    timeout: Duration,
    fut: impl Future<Output = T>,
) -> Result<T, WarmupBound> {
    tokio::select! {
        biased;
        () = shutdown.cancelled() => Err(WarmupBound::Cancelled),
        result = tokio::time::timeout(timeout, fut) => {
            result.map_err(|_elapsed| {
                // Query::run is armed with this child; cancel it so
                // in-flight replay work stops. Do not cancel `shutdown`:
                // that would abort the rest of warmup (and the runtime).
                query_cancel.cancel();
                WarmupBound::TimedOut
            })
        }
    }
}

/// Run warmup, then always release the ready-hold. The hold is created
/// before the returned future is polled, so dropping an unpolled task
/// (refresh runtime shutting down) still releases `/v1/ready`. A stalled
/// replay is interrupted when `shutdown` is cancelled (runtime shutdown).
#[must_use]
fn run_warmup_releasing_ready<F>(
    status: Arc<status::RuntimeStatus>,
    shutdown: CancellationToken,
    warmup: F,
) -> impl Future<Output = ()>
where
    F: Future<Output = ()>,
{
    let release = DatasetReadyHold { status };
    async move {
        let _release = release;
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                tracing::debug!(
                    "SQL results cache warmup stopped because the runtime is shutting down"
                );
            }
            () = warmup => {}
        }
    }
}

/// Replay one parameterized template from streamed DISTINCT rows.
///
/// Rows are applied batch-by-batch so a high-cardinality binding cannot
/// materialize every key before the first replay. Stop when the cache is at
/// `max_size`, or when a successful store does not increase `size` — Spice
/// evicts on insert, so `size` may never reach `max_size`.
async fn warm_distinct_key_rows(
    df: &Arc<DataFusion>,
    template_sql: &str,
    distinct_sql: &str,
    request_context: &Arc<RequestContext>,
    cache_provider: &cache::QueryResultsCacheProvider,
) -> Result<WarmupReplay, WarmupBound> {
    let timeout = request_context
        .query_timeout()
        .unwrap_or(DEFAULT_WARMUP_REPLAY_TIMEOUT);
    let shutdown = request_context.cancellation_token();
    let query_cancel = request_context.child_cancellation_token();
    let query = QueryBuilder::new(distinct_sql, Arc::clone(df))
        .for_results_cache_warming()
        .results_cache_mode(ResultsCacheMode::Bypass)
        .cancellation_token(query_cancel.clone())
        .build();
    let result = match bound_warmup_op(
        shutdown,
        &query_cancel,
        timeout,
        Arc::clone(request_context).scope(async move { query.run().await }),
    )
    .await?
    {
        Ok(result) => result,
        Err(e) => {
            query_error_to_warmup_bound(shutdown, &e)?;
            return Ok(WarmupReplay::Unusable);
        }
    };

    let mut stream = result.data;
    let mut stored = 0_u64;
    let mut had_row = false;
    let max_size = cache_provider.max_size();

    loop {
        let batch =
            match bound_warmup_op(shutdown, &query_cancel, timeout, stream.try_next()).await? {
                Ok(Some(batch)) => batch,
                Ok(None) => break,
                Err(e) => {
                    stream_error_to_warmup_bound(shutdown, &e)?;
                    if stored == 0 && !had_row {
                        return Ok(WarmupReplay::Unusable);
                    }
                    break;
                }
            };
        for row_idx in 0..batch.num_rows() {
            // The DISTINCT query lifetime timer cancels `query_cancel`
            // independently of runtime shutdown. Stop before starting
            // another nested replay so a large batch cannot outlive the
            // replay deadline and hold `/v1/ready`.
            warmup_row_deadline(shutdown, &query_cancel)?;
            had_row = true;
            cache_provider.run_pending_tasks().await;
            let size_before = cache_provider.size().await;
            if size_before >= max_size {
                return Ok(WarmupReplay::Stored(stored));
            }

            let Ok(values) = (0..batch.num_columns())
                .map(|col_idx| ScalarValue::try_from_array(batch.column(col_idx), row_idx))
                .collect::<Result<Vec<_>, _>>()
            else {
                continue;
            };

            match execute_warmup_sql(
                df,
                template_sql,
                Some(values),
                request_context,
                &query_cancel,
            )
            .await
            {
                Ok(true) => {
                    stored += 1;
                    cache_provider.run_pending_tasks().await;
                    if cache_provider.size().await <= size_before {
                        return Ok(WarmupReplay::Stored(stored));
                    }
                }
                Ok(false) => {}
                Err(bound) => return Err(bound),
            }
        }
    }
    if stored > 0 || !had_row {
        Ok(WarmupReplay::Stored(stored))
    } else {
        Ok(WarmupReplay::Unusable)
    }
}

async fn execute_warmup_sql(
    df: &Arc<DataFusion>,
    sql: &str,
    parameters: Option<Vec<ScalarValue>>,
    request_context: &Arc<RequestContext>,
    parent_cancel: &CancellationToken,
) -> Result<bool, WarmupBound> {
    let timeout = request_context
        .query_timeout()
        .unwrap_or(DEFAULT_WARMUP_REPLAY_TIMEOUT);
    let shutdown = request_context.cancellation_token();
    // Child of the DISTINCT (or request) token so a lifetime-timer fire
    // cancels this replay. Nested timeout cancels only this child.
    let query_cancel = parent_cancel.child_token();
    let work = execute_warmup_sql_inner(
        df,
        sql,
        parameters,
        request_context,
        shutdown,
        query_cancel.clone(),
        timeout,
    );
    tokio::select! {
        biased;
        () = parent_cancel.cancelled() => {
            query_cancel.cancel();
            Err(warmup_bound_from_shutdown(shutdown))
        }
        result = work => result
    }
}

async fn execute_warmup_sql_inner(
    df: &Arc<DataFusion>,
    sql: &str,
    parameters: Option<Vec<ScalarValue>>,
    request_context: &Arc<RequestContext>,
    shutdown: &CancellationToken,
    query_cancel: CancellationToken,
    timeout: Duration,
) -> Result<bool, WarmupBound> {
    let mut builder = QueryBuilder::new(sql, Arc::clone(df))
        .for_results_cache_warming()
        .cancellation_token(query_cancel.clone());
    if let Some(values) = parameters {
        builder = builder.parameters(Some(ParamValues::from(values)));
    }
    let query = builder.build();
    let result = bound_warmup_op(
        shutdown,
        &query_cancel,
        timeout,
        Arc::clone(request_context).scope(async move { query.run().await }),
    )
    .await?;
    match result {
        // Drain without retaining batches. The results-cache wrapper stores as
        // the stream is consumed; warmup does not need the rows itself, and
        // holding them here would scale RAM with the full result while
        // datasets stay not ready.
        Ok(query_result) => {
            match bound_warmup_op(shutdown, &query_cancel, timeout, query_result.drain()).await? {
                Ok(()) => Ok(true),
                Err(e) => {
                    stream_error_to_warmup_bound(shutdown, &e)?;
                    tracing::debug!("SQL results cache warmup query failed: {e}");
                    Ok(false)
                }
            }
        }
        Err(e) => {
            query_error_to_warmup_bound(shutdown, &e)?;
            tracing::debug!("SQL results cache warmup query failed: {e}");
            Ok(false)
        }
    }
}

fn warmup_bound_from_shutdown(shutdown: &CancellationToken) -> WarmupBound {
    if shutdown.is_cancelled() {
        WarmupBound::Cancelled
    } else {
        WarmupBound::TimedOut
    }
}

/// Stop DISTINCT-key replay when runtime shutdown or the DISTINCT query
/// lifetime timer has fired. A cancelled child with shutdown still live is
/// TimedOut so later templates can still warm.
fn warmup_row_deadline(
    shutdown: &CancellationToken,
    query_cancel: &CancellationToken,
) -> Result<(), WarmupBound> {
    if shutdown.is_cancelled() || query_cancel.is_cancelled() {
        return Err(warmup_bound_from_shutdown(shutdown));
    }
    Ok(())
}

/// Map a DISTINCT/`drain` stream error. Timeout and cancel become
/// [`WarmupBound`] so the caller warns and skips the plan instead of
/// treating a partial result as a successful store.
fn stream_error_to_warmup_bound(
    shutdown: &CancellationToken,
    err: &datafusion::error::DataFusionError,
) -> Result<(), WarmupBound> {
    if super::is_timeout_error(err) || super::is_cancellation_error(err) {
        return Err(warmup_bound_from_shutdown(shutdown));
    }
    Ok(())
}

fn query_error_to_warmup_bound(
    shutdown: &CancellationToken,
    err: &super::Error,
) -> Result<(), WarmupBound> {
    if matches!(
        err,
        super::Error::QueryTimedOut { .. } | super::Error::QueryCancelled { .. }
    ) {
        return Err(warmup_bound_from_shutdown(shutdown));
    }
    Ok(())
}

#[must_use]
pub(crate) fn default_warmup_store_path() -> PathBuf {
    PathBuf::from(WARMUP_STORE_RELATIVE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use cache::result::CacheStatus;
    use cache::{Caching, QueryResultsCacheProvider, SimpleCache};
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::sql::TableReference;
    use spicepod::component::caching::SQLResultsCacheConfig;
    use tokio::runtime::Handle;

    use super::super::warmup_plan::{WarmupBinding, WarmupTemplate};
    use super::MAX_WARMUP_PLANS;
    use crate::{
        accelerated::RefreshCompletion,
        builder::RuntimeBuilder,
        component::dataset::acceleration::RefreshMode,
        datafusion::query::{QueryBuilder as QBuilder, ResultsCacheMode},
        status,
    };
    use runtime_request_context::{
        CacheControl, CacheKeyType, CacheNamespace, Protocol, RequestContext,
    };

    fn request_context() -> Arc<RequestContext> {
        Arc::new(
            RequestContext::builder(Protocol::Internal)
                .with_cache_control(CacheControl::Cache(CacheKeyType::Default))
                .with_cache_namespace(CacheNamespace::Public)
                .build(),
        )
    }

    async fn prepare_runtime(max_size: Option<&str>, store: PathBuf) -> Arc<DataFusion> {
        let plans_cache = Arc::new(SimpleCache::new(
            512,
            Duration::from_hours(1),
            std::hash::BuildHasherDefault::<twox_hash::XxHash3_64>::default(),
        ));
        let results_cache_config = SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            max_size: max_size.map(ToString::to_string),
            ..Default::default()
        };
        let cache_provider =
            QueryResultsCacheProvider::try_new(&results_cache_config, Box::new([]))
                .expect("valid cache provider");
        let warmer = ResultsCacheWarmer::new(store, true).await;
        let runtime = RuntimeBuilder::new().build().await;
        Arc::new(
            DataFusion::builder(
                status::RuntimeStatus::new(),
                runtime.accelerator_engine_registry(),
                Handle::current(),
            )
            .with_caching(Arc::new(
                Caching::new()
                    .with_results_cache(Arc::new(cache_provider))
                    .with_plans_cache(plans_cache),
            ))
            .with_results_cache_warmer(warmer)
            .build(),
        )
    }

    async fn prepare_runtime_disabled(store: PathBuf) -> Arc<DataFusion> {
        let warmer = ResultsCacheWarmer::new(store, false).await;
        let runtime = RuntimeBuilder::new().build().await;
        Arc::new(
            DataFusion::builder(
                status::RuntimeStatus::new(),
                runtime.accelerator_engine_registry(),
                Handle::current(),
            )
            .with_results_cache_warmer(warmer)
            .build(),
        )
    }

    /// `scan` never completes, so a warmup replay of this table hangs until the
    /// per-op bound fires. Used to exercise `QueryBuilder` + `query.run()`.
    #[derive(Debug)]
    struct PendingScanTable {
        schema: arrow::datatypes::SchemaRef,
    }

    #[async_trait::async_trait]
    impl TableProvider for PendingScanTable {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> datafusion::datasource::TableType {
            datafusion::datasource::TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn datafusion::catalog::Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[datafusion::prelude::Expr],
            _limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            std::future::pending().await
        }
    }

    fn register_table(df: &Arc<DataFusion>, name: &str, values: Vec<i64>) {
        register_table_partitions(df, name, vec![values]);
    }

    fn register_table_partitions(df: &Arc<DataFusion>, name: &str, partitions: Vec<Vec<i64>>) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batches: Vec<Vec<RecordBatch>> = partitions
            .into_iter()
            .map(|values| {
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(values))],
                )
                .expect("batch");
                vec![batch]
            })
            .collect();
        let table = Arc::new(MemTable::try_new(schema, batches).expect("mem table"));
        df.ctx
            .register_table(TableReference::bare(name), table as Arc<dyn TableProvider>)
            .expect("register table");
    }

    async fn run_sql(df: &Arc<DataFusion>, sql: &str) -> CacheStatus {
        let result = QBuilder::new(sql, Arc::clone(df))
            .results_cache_mode(ResultsCacheMode::Default)
            .build()
            .run()
            .await
            .expect("query should succeed");
        let status = result.cache_status;
        result.data.try_collect::<Vec<_>>().await.expect("collect");
        status
    }

    #[tokio::test]
    async fn first_ten_plan_shapes_are_kept_not_literal_variants() {
        let store =
            std::env::temp_dir().join(format!("spice-warmup-shapes-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&store);
        let df = prepare_runtime(None, store.clone()).await;
        register_table(&df, "orders", vec![1, 2, 3]);

        let ctx = datafusion::prelude::SessionContext::new();
        ctx.sql("CREATE TABLE orders (id INT)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");
        for id in 1..=5 {
            let plan = ctx
                .sql(&format!("SELECT id FROM orders WHERE id = {id}"))
                .await
                .expect("sql")
                .logical_plan()
                .clone();
            df.observe_results_cache_warmup_plan(&plan, &CacheNamespace::Public);
        }
        assert_eq!(
            df.results_cache_warmer.templates_snapshot().len(),
            1,
            "five literal variants of one plan must count as one template"
        );
        let _ = std::fs::remove_file(&store);
    }

    #[tokio::test]
    async fn warmup_replays_a_stored_plan_across_distinct_keys() {
        let store =
            std::env::temp_dir().join(format!("spice-warmup-keys-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&store);
        let df = prepare_runtime(None, store.clone()).await;
        register_table(&df, "orders", vec![1, 2, 3]);

        let template = WarmupTemplate {
            sql: "SELECT id FROM orders WHERE id = $1".to_string(),
            bindings: vec![WarmupBinding {
                table: "orders".to_string(),
                column: "id".to_string(),
            }],
        };
        df.run_warmup_templates(&[template], None).await;

        let hit = request_context()
            .scope(run_sql(&df, "SELECT id FROM orders WHERE id = 2"))
            .await;
        assert_eq!(
            hit,
            CacheStatus::CacheHit,
            "warmup must fill the cache for a distinct key that was never queried in this process"
        );
        let _ = std::fs::remove_file(&store);
    }

    #[tokio::test]
    async fn disabled_warmup_does_not_record_plans() {
        let store =
            std::env::temp_dir().join(format!("spice-warmup-disabled-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&store);
        let df = prepare_runtime_disabled(store.clone()).await;

        let ctx = datafusion::prelude::SessionContext::new();
        ctx.sql("CREATE TABLE orders (id INT)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");
        let plan = ctx
            .sql("SELECT id FROM orders WHERE id = 1")
            .await
            .expect("sql")
            .logical_plan()
            .clone();
        df.observe_results_cache_warmup_plan(&plan, &CacheNamespace::Public);
        assert!(
            df.results_cache_warmer.templates_snapshot().is_empty(),
            "warmup: disabled must not record plans"
        );
        assert!(
            !store.exists(),
            "warmup: disabled must not write a warmup catalog"
        );
    }

    async fn wait_for_catalog(path: &std::path::Path) {
        let start = std::time::Instant::now();
        loop {
            if path.exists() {
                return;
            }
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "warmup catalog was not written to {}",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn enabled_warmup_persists_and_reloads_plan_shapes() {
        let store =
            std::env::temp_dir().join(format!("spice-warmup-persist-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&store);
        let _ = std::fs::remove_file(store.with_extension("json.tmp"));
        let df = prepare_runtime(None, store.clone()).await;

        let ctx = datafusion::prelude::SessionContext::new();
        ctx.sql("CREATE TABLE orders (id INT)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");
        let plan = ctx
            .sql("SELECT id FROM orders WHERE id = 1")
            .await
            .expect("sql")
            .logical_plan()
            .clone();
        df.observe_results_cache_warmup_plan(&plan, &CacheNamespace::Public);
        wait_for_catalog(&store).await;

        let reloaded = ResultsCacheWarmer::new(store.clone(), true).await;
        assert_eq!(
            reloaded.templates_snapshot().len(),
            1,
            "a new process must load the persisted plan shape"
        );
        let _ = std::fs::remove_file(&store);
        let _ = std::fs::remove_file(store.with_extension("json.tmp"));
    }

    #[tokio::test]
    async fn new_loads_local_catalog_via_tokio_fs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("results_cache_warmup.json");
        let templates = vec![WarmupTemplate {
            sql: "SELECT id FROM orders WHERE id = $1".to_string(),
            bindings: vec![WarmupBinding {
                table: "orders".to_string(),
                column: "id".to_string(),
            }],
        }];
        tokio::fs::write(
            &path,
            serde_json::to_vec(&templates).expect("serialize catalog"),
        )
        .await
        .expect("write catalog");

        let warmer = ResultsCacheWarmer::new(path, true).await;
        assert_eq!(
            warmer.templates_snapshot(),
            templates,
            "async local load must deserialize the persisted catalog"
        );
    }

    #[tokio::test]
    async fn new_returns_empty_catalog_when_file_is_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("missing-results_cache_warmup.json");
        let warmer = ResultsCacheWarmer::new(path, true).await;
        assert!(
            warmer.templates_snapshot().is_empty(),
            "a missing catalog is an empty start, not an error"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn load_templates_does_not_block_runtime_on_pending_fifo() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("warmup.fifo");
        let status = std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .expect("run mkfifo");
        assert!(
            status.success(),
            "mkfifo should create the pending catalog path"
        );

        let load_path = path.clone();
        let load = tokio::spawn(async move { load_templates(&load_path).await });

        // On the current-thread test runtime, a blocking `std::fs::read` of this
        // FIFO would stall this timeout. `tokio::fs` yields, so the runtime
        // stays responsive while the catalog has no writer.
        tokio::time::timeout(
            Duration::from_millis(250),
            tokio::time::sleep(Duration::from_millis(50)),
        )
        .await
        .expect("runtime must stay responsive while the catalog read is pending");
        assert!(
            !load.is_finished(),
            "FIFO with no writer must still be pending; read_returned={}",
            load.is_finished()
        );

        // `tokio::fs::read` uses `spawn_blocking`; the FIFO open finishes
        // only after a writer appears, so complete the read before the
        // test runtime shuts down.
        let write_path = path;
        let writer = tokio::task::spawn_blocking(move || std::fs::write(write_path, b"[]"));
        let loaded = load
            .await
            .expect("catalog load should finish after a writer opens the FIFO");
        writer
            .await
            .expect("writer task")
            .expect("write empty catalog");
        assert!(
            loaded.is_empty(),
            "an empty JSON array is a valid empty catalog"
        );
    }

    #[tokio::test]
    async fn only_the_first_ten_distinct_plan_shapes_are_kept() {
        let store =
            std::env::temp_dir().join(format!("spice-warmup-cap-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&store);
        let df = prepare_runtime(None, store.clone()).await;

        let ctx = datafusion::prelude::SessionContext::new();
        ctx.sql("CREATE TABLE orders (id INT, status VARCHAR)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");
        let sqls = [
            "SELECT id FROM orders",
            "SELECT status FROM orders",
            "SELECT id, status FROM orders",
            "SELECT count(*) FROM orders",
            "SELECT id FROM orders ORDER BY id",
            "SELECT id FROM orders LIMIT 1",
            "SELECT DISTINCT id FROM orders",
            "SELECT id FROM orders WHERE id > 0",
            "SELECT id FROM orders WHERE id < 0",
            "SELECT id FROM orders WHERE id >= 0",
            "SELECT id FROM orders WHERE id <= 0",
        ];
        for sql in sqls {
            let plan = ctx.sql(sql).await.expect("sql").logical_plan().clone();
            df.observe_results_cache_warmup_plan(&plan, &CacheNamespace::Public);
        }
        assert_eq!(
            df.results_cache_warmer.templates_snapshot().len(),
            MAX_WARMUP_PLANS,
            "the 11th distinct plan shape must not be recorded"
        );
        let _ = std::fs::remove_file(&store);
    }

    #[test]
    fn merge_templates_keeps_base_order_and_caps_at_ten() {
        let base = vec![WarmupTemplate {
            sql: "SELECT 1".to_string(),
            bindings: vec![],
        }];
        let extra = vec![
            WarmupTemplate {
                sql: "SELECT 1".to_string(),
                bindings: vec![],
            },
            WarmupTemplate {
                sql: "SELECT 2".to_string(),
                bindings: vec![],
            },
        ];
        let merged = merge_templates(&base, &extra);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].sql, "SELECT 1");
        assert_eq!(merged[1].sql, "SELECT 2");

        let full: Vec<WarmupTemplate> = (0..MAX_WARMUP_PLANS)
            .map(|i| WarmupTemplate {
                sql: format!("SELECT {i}"),
                bindings: vec![],
            })
            .collect();
        let overflow = vec![WarmupTemplate {
            sql: "SELECT overflow".to_string(),
            bindings: vec![],
        }];
        let capped = merge_templates(&full, &overflow);
        assert_eq!(capped.len(), MAX_WARMUP_PLANS);
        assert_eq!(capped[0].sql, "SELECT 0");
        assert_eq!(
            capped[MAX_WARMUP_PLANS - 1].sql,
            format!("SELECT {}", MAX_WARMUP_PLANS - 1)
        );

        // An already-oversized base (stale or edited persist) must also
        // drop duplicates and stop at the advertised cap.
        let mut oversized = full;
        oversized.push(WarmupTemplate {
            sql: "SELECT 0".to_string(),
            bindings: vec![],
        });
        oversized.extend((MAX_WARMUP_PLANS..25).map(|i| WarmupTemplate {
            sql: format!("SELECT {i}"),
            bindings: vec![],
        }));
        assert_eq!(oversized.len(), 26);
        let normalized = merge_templates(&oversized, &[]);
        assert_eq!(normalized.len(), MAX_WARMUP_PLANS);
        assert_eq!(normalized[0].sql, "SELECT 0");
        assert_eq!(
            normalized[MAX_WARMUP_PLANS - 1].sql,
            format!("SELECT {}", MAX_WARMUP_PLANS - 1)
        );
    }

    /// Copilot model: a 25-template persist must not replay 25 shapes.
    #[test]
    fn from_loaded_caps_and_dedups_persisted_templates() {
        let mut persisted: Vec<WarmupTemplate> = (0..25)
            .map(|i| WarmupTemplate {
                sql: format!("SELECT {i}"),
                bindings: vec![],
            })
            .collect();
        persisted.push(WarmupTemplate {
            sql: "SELECT 0".to_string(),
            bindings: vec![],
        });

        let warmer = ResultsCacheWarmer::from_loaded(
            &persisted,
            true,
            WarmupPersist::Local(std::env::temp_dir().join("spice-warmup-from-loaded.json")),
        );
        let snapshot = warmer.templates_snapshot();
        let loaded_templates = snapshot.len();

        eprintln!(
            "loaded_templates={loaded_templates} replayed_templates={loaded_templates} cap_enforced={}",
            loaded_templates <= MAX_WARMUP_PLANS
        );

        assert_eq!(
            loaded_templates, MAX_WARMUP_PLANS,
            "persisted catalogs must be capped at {MAX_WARMUP_PLANS} distinct plans before replay"
        );
        assert_eq!(snapshot[0].sql, "SELECT 0");
        assert_eq!(
            snapshot[MAX_WARMUP_PLANS - 1].sql,
            format!("SELECT {}", MAX_WARMUP_PLANS - 1)
        );
        assert_eq!(
            snapshot
                .iter()
                .map(template_id)
                .collect::<HashSet<_>>()
                .len(),
            MAX_WARMUP_PLANS,
            "loaded catalog must not keep duplicate plan shapes"
        );
    }

    /// A full persisted catalog of obsolete shapes must drop them after
    /// replay fails so a current query can be recorded.
    #[tokio::test]
    async fn failed_replay_frees_catalog_slot_for_a_new_plan() {
        let store = std::env::temp_dir().join(format!(
            "spice-warmup-stale-full-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&store);
        let stale: Vec<WarmupTemplate> = (0..MAX_WARMUP_PLANS)
            .map(|i| WarmupTemplate {
                sql: format!("SELECT id FROM missing_{i}"),
                bindings: vec![],
            })
            .collect();
        tokio::fs::write(
            &store,
            serde_json::to_vec(&stale).expect("serialize stale catalog"),
        )
        .await
        .expect("write stale catalog");

        let df = prepare_runtime(None, store.clone()).await;
        let loaded = df.results_cache_warmer.templates_snapshot().len();
        assert_eq!(loaded, MAX_WARMUP_PLANS);

        register_table(&df, "orders", vec![1, 2, 3]);
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.sql("CREATE TABLE orders (id INT)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");
        let new_plan = ctx
            .sql("SELECT id FROM orders WHERE id = 1")
            .await
            .expect("sql")
            .logical_plan()
            .clone();
        df.observe_results_cache_warmup_plan(&new_plan, &CacheNamespace::Public);
        assert_eq!(
            df.results_cache_warmer.templates_snapshot().len(),
            MAX_WARMUP_PLANS,
            "a full catalog must not record a new shape before stale entries are dropped"
        );

        df.run_warmup_templates(&df.results_cache_warmer.templates_snapshot(), None)
            .await;
        let after_replay = df.results_cache_warmer.templates_snapshot().len();
        let replay_failures = loaded.saturating_sub(after_replay);

        df.observe_results_cache_warmup_plan(&new_plan, &CacheNamespace::Public);
        let final_catalog = df.results_cache_warmer.templates_snapshot();
        // `template_from_plan` stores quoted table idents (`"orders"`).
        let recorded_new = final_catalog.iter().any(|template| {
            template.sql.contains("orders")
                || template
                    .bindings
                    .iter()
                    .any(|binding| binding.table.contains("orders"))
        });
        eprintln!(
            "loaded={loaded} replay_failures={replay_failures} recorded_new={recorded_new} final_catalog_size={} final_catalog={final_catalog:?}",
            final_catalog.len()
        );
        assert_eq!(
            replay_failures, MAX_WARMUP_PLANS,
            "every stale template must be dropped after it fails to replay"
        );
        assert!(
            recorded_new,
            "a current plan must be recorded after stale entries are dropped"
        );
        assert_eq!(final_catalog.len(), 1);
        let _ = std::fs::remove_file(&store);
    }

    #[tokio::test]
    async fn empty_distinct_replay_keeps_a_valid_template() {
        let store = std::env::temp_dir().join(format!(
            "spice-warmup-empty-keep-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&store);
        let template = WarmupTemplate {
            sql: "SELECT id FROM orders WHERE id = $1".to_string(),
            bindings: vec![WarmupBinding {
                table: "orders".to_string(),
                column: "id".to_string(),
            }],
        };
        tokio::fs::write(
            &store,
            serde_json::to_vec(std::slice::from_ref(&template)).expect("serialize"),
        )
        .await
        .expect("write catalog");

        let df = prepare_runtime(None, store.clone()).await;
        register_table(&df, "orders", vec![]);
        df.run_warmup_templates(&df.results_cache_warmer.templates_snapshot(), None)
            .await;
        assert_eq!(
            df.results_cache_warmer.templates_snapshot().len(),
            1,
            "an empty DISTINCT result is not a failed plan and must stay in the catalog"
        );
        let _ = std::fs::remove_file(&store);
    }

    fn warmup_tpl(sql: &str) -> WarmupTemplate {
        WarmupTemplate {
            sql: sql.to_string(),
            bindings: vec![],
        }
    }

    fn catalog_mutex(templates: Vec<WarmupTemplate>) -> Arc<parking_lot::Mutex<WarmupCatalog>> {
        let ids = templates.iter().map(template_id).collect();
        Arc::new(parking_lot::Mutex::new(WarmupCatalog { templates, ids }))
    }

    fn template_sqls(templates: &[WarmupTemplate]) -> Vec<&str> {
        templates
            .iter()
            .map(|template| template.sql.as_str())
            .collect()
    }

    /// Regression for Copilot on #14178: `ObjectState::update` reads the shared
    /// cached etag, so a stale persist that `get`s `[seed]` and later
    /// `update`s `[seed, A]` can overwrite a newer `[seed, A, B]` without a
    /// conflict. Persist now binds If-Match to the `get` version.
    #[tokio::test]
    async fn persist_remote_keeps_newer_templates_when_object_state_cache_advances() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let state = Arc::new(ObjectState::new(store));
        let seed = warmup_tpl("SELECT seed");
        let template_a = warmup_tpl("SELECT A");
        let template_b = warmup_tpl("SELECT B");
        state
            .insert(WARMUP_STATE_KEY, &vec![seed.clone()])
            .await
            .expect("seed catalog");

        let (stale_remote, stale_version) = state
            .get_with_version(WARMUP_STATE_KEY)
            .await
            .expect("stale get")
            .expect("seed exists");
        let stale_merged = merge_templates(&stale_remote, std::slice::from_ref(&template_a));
        assert_eq!(template_sqls(&stale_merged), ["SELECT seed", "SELECT A"]);

        persist_remote(
            Arc::clone(&state),
            catalog_mutex(vec![template_a.clone(), template_b.clone()]),
            Arc::new(AtomicUsize::new(2)),
        )
        .await;
        match state
            .update_with_version(WARMUP_STATE_KEY, &stale_merged, stale_version)
            .await
            .expect("stale versioned write")
        {
            UpdateResult::Conflict { current } => {
                assert_eq!(
                    template_sqls(&current),
                    ["SELECT seed", "SELECT A", "SELECT B"]
                );
            }
            other => panic!("expected Conflict so B is not dropped, got {other:?}"),
        }

        persist_remote(
            Arc::clone(&state),
            catalog_mutex(vec![template_a]),
            Arc::new(AtomicUsize::new(1)),
        )
        .await;
        let persisted = state
            .get(WARMUP_STATE_KEY)
            .await
            .expect("final get")
            .expect("catalog exists");
        assert_eq!(
            template_sqls(&persisted),
            ["SELECT seed", "SELECT A", "SELECT B"]
        );
    }

    #[tokio::test]
    async fn persist_remote_excluding_drops_unusable_templates() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let state = Arc::new(ObjectState::new(store));
        let stale = warmup_tpl("SELECT stale");
        let keep = warmup_tpl("SELECT keep");
        state
            .insert(WARMUP_STATE_KEY, &vec![stale.clone(), keep.clone()])
            .await
            .expect("seed catalog");

        persist_remote_excluding(
            Arc::clone(&state),
            catalog_mutex(vec![keep.clone()]),
            Arc::new(AtomicUsize::new(1)),
            HashSet::from([template_id(&stale)]),
        )
        .await;

        let persisted = state
            .get(WARMUP_STATE_KEY)
            .await
            .expect("final get")
            .expect("catalog exists");
        assert_eq!(template_sqls(&persisted), ["SELECT keep"]);
    }

    #[tokio::test]
    async fn concurrent_persist_remote_keeps_both_observed_templates() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let state = Arc::new(ObjectState::new(store));
        let seed = warmup_tpl("SELECT seed");
        let template_a = warmup_tpl("SELECT A");
        let template_b = warmup_tpl("SELECT B");
        state
            .insert(WARMUP_STATE_KEY, &vec![seed])
            .await
            .expect("seed catalog");

        let stale_persist = persist_remote(
            Arc::clone(&state),
            catalog_mutex(vec![template_a.clone()]),
            Arc::new(AtomicUsize::new(1)),
        );
        let newer = persist_remote(
            Arc::clone(&state),
            catalog_mutex(vec![template_a, template_b]),
            Arc::new(AtomicUsize::new(2)),
        );
        tokio::join!(stale_persist, newer);

        let persisted = state
            .get(WARMUP_STATE_KEY)
            .await
            .expect("final get")
            .expect("catalog exists");
        assert!(
            persisted.iter().any(|template| template.sql == "SELECT A"),
            "A missing from {persisted:?}"
        );
        assert!(
            persisted.iter().any(|template| template.sql == "SELECT B"),
            "B missing from {persisted:?}"
        );
    }

    #[tokio::test]
    async fn object_store_warmup_persists_and_reloads_plan_shapes() {
        let dir = std::env::temp_dir().join(format!("spice-warmup-remote-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store_occ::LocalConditionalPut::new(&dir).expect("local store"));

        let warmer = ResultsCacheWarmer::from_object_store(Arc::clone(&store), "", true).await;
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.sql("CREATE TABLE orders (id INT)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");
        let plan = ctx
            .sql("SELECT id FROM orders WHERE id = 1")
            .await
            .expect("sql")
            .logical_plan()
            .clone();
        warmer.observe_plan(&plan, &CacheNamespace::Public);

        let state = ObjectState::<Vec<WarmupTemplate>>::new(Arc::clone(&store));
        let start = std::time::Instant::now();
        loop {
            if state
                .get(WARMUP_STATE_KEY)
                .await
                .ok()
                .flatten()
                .is_some_and(|templates| !templates.is_empty())
            {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "warmup catalog was not written to object storage"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let reloaded = ResultsCacheWarmer::from_object_store(store, "", true).await;
        assert_eq!(
            reloaded.templates_snapshot().len(),
            1,
            "a new process must load the persisted plan shape from object storage"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn warmup_rejects_persisted_dml_even_without_bindings() {
        let dir = tempfile::tempdir().expect("tempdir");
        let df = prepare_runtime(None, dir.path().join("warmup.json")).await;
        register_table(&df, "orders", vec![1, 2, 3]);

        // Binding-free templates replay the stored SQL as-is. Without read-only
        // enforcement, a corrupted catalog could INSERT/DDL at startup.
        let ctx = request_context();
        let ok = execute_warmup_sql(
            &df,
            "INSERT INTO orders VALUES (1)",
            None,
            &ctx,
            ctx.cancellation_token(),
        )
        .await
        .expect("warmup DML is rejected, not timed out");
        assert!(
            !ok,
            "warmup must reject DML from a persisted template via read-only validation"
        );
    }

    #[tokio::test]
    async fn concurrent_remote_persists_keep_all_observed_templates() {
        let dir =
            std::env::temp_dir().join(format!("spice-warmup-remote-race-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store_occ::LocalConditionalPut::new(&dir).expect("local store"));

        let warmer = ResultsCacheWarmer::from_object_store(Arc::clone(&store), "", true).await;
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.sql("CREATE TABLE orders (id INT, status VARCHAR)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");

        // Two distinct shapes observed back-to-back each spawn a remote persist.
        // Without serializing those writers, ObjectState::update can let the
        // older snapshot overwrite the newer catalog (losing template B).
        let plan_a = ctx
            .sql("SELECT id FROM orders WHERE id = 1")
            .await
            .expect("sql a")
            .logical_plan()
            .clone();
        let plan_b = ctx
            .sql("SELECT id FROM orders WHERE status = 'open'")
            .await
            .expect("sql b")
            .logical_plan()
            .clone();
        warmer.observe_plan(&plan_a, &CacheNamespace::Public);
        warmer.observe_plan(&plan_b, &CacheNamespace::Public);

        let state = ObjectState::<Vec<WarmupTemplate>>::new(Arc::clone(&store));
        let start = std::time::Instant::now();
        loop {
            let templates = state
                .get(WARMUP_STATE_KEY)
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            if templates.len() >= 2 {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "expected both observed templates in object storage, got {templates:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let reloaded = ResultsCacheWarmer::from_object_store(store, "", true).await;
        assert_eq!(
            reloaded.templates_snapshot().len(),
            2,
            "serialized remote persists must keep both plan shapes, got {:?}",
            reloaded.templates_snapshot()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn build_results_cache_warmer_uses_runtime_state_location() {
        let dir =
            std::env::temp_dir().join(format!("spice-warmup-runtime-state-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let runtime_state = RuntimeState {
            location: format!("file://{}", dir.display()),
            params: None,
        };
        let secrets = Arc::new(RwLock::new(Secrets::new()));
        let warmer = build_results_cache_warmer(
            true,
            Some(&runtime_state),
            Arc::clone(&secrets),
            Handle::current(),
        )
        .await;

        let ctx = datafusion::prelude::SessionContext::new();
        ctx.sql("CREATE TABLE orders (id INT)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");
        let plan = ctx
            .sql("SELECT id FROM orders WHERE id = 1")
            .await
            .expect("sql")
            .logical_plan()
            .clone();
        warmer.observe_plan(&plan, &CacheNamespace::Public);

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store_occ::LocalConditionalPut::new(&dir).expect("local store"));
        let state = ObjectState::<Vec<WarmupTemplate>>::new(store);
        let start = std::time::Instant::now();
        loop {
            if state
                .get(WARMUP_STATE_KEY)
                .await
                .ok()
                .flatten()
                .is_some_and(|templates| !templates.is_empty())
            {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "warmup catalog was not written to runtime.state location"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let reloaded =
            build_results_cache_warmer(true, Some(&runtime_state), secrets, Handle::current())
                .await;
        assert_eq!(
            reloaded.templates_snapshot().len(),
            1,
            "a new process must load plan shapes from runtime.state"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn principal_scoped_plans_are_not_recorded_for_warmup() {
        let store = std::env::temp_dir().join(format!(
            "spice-warmup-principal-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&store);
        let df = prepare_runtime(None, store.clone()).await;

        let ctx = datafusion::prelude::SessionContext::new();
        ctx.sql("CREATE TABLE orders (id INT)")
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");
        let plan = ctx
            .sql("SELECT id FROM orders WHERE id = 1")
            .await
            .expect("sql")
            .logical_plan()
            .clone();

        let principal = CacheNamespace::Principal(Arc::from("apikey:test-principal"));
        df.observe_results_cache_warmup_plan(&plan, &principal);
        df.observe_results_cache_warmup_plan(&plan, &CacheNamespace::System);
        assert!(
            df.results_cache_warmer.templates_snapshot().is_empty(),
            "principal-scoped and system plans must not enter the warmup catalog"
        );

        df.observe_results_cache_warmup_plan(&plan, &CacheNamespace::Public);
        assert_eq!(
            df.results_cache_warmer.templates_snapshot().len(),
            1,
            "public plans must still be recorded"
        );
        let _ = std::fs::remove_file(&store);
    }

    /// Best-effort lifecycle regression: record → persist → "restart" → warmup →
    /// ready released → cache hit. Does not drive `load_components` (no accelerated
    /// table in this harness); that path is covered by the integration binary when
    /// present.
    #[tokio::test]
    async fn record_persist_restart_warmup_ready_and_cache_hit() {
        let store = std::env::temp_dir().join(format!(
            "spice-warmup-lifecycle-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&store);
        let _ = std::fs::remove_file(store.with_extension("json.tmp"));

        // Process A: record a plan shape and persist it.
        {
            let df = prepare_runtime(None, store.clone()).await;
            let ctx = datafusion::prelude::SessionContext::new();
            ctx.sql("CREATE TABLE orders (id INT)")
                .await
                .expect("create")
                .collect()
                .await
                .expect("collect");
            let plan = ctx
                .sql("SELECT id FROM orders WHERE id = 1")
                .await
                .expect("sql")
                .logical_plan()
                .clone();
            df.observe_results_cache_warmup_plan(&plan, &CacheNamespace::Public);
            wait_for_catalog(&store).await;
        }

        // Process B: reload catalog, hold ready, run warmup, release, observe hit.
        let df = prepare_runtime(None, store.clone()).await;
        register_table(&df, "orders", vec![1, 2, 3]);
        assert!(
            df.results_cache_warmup_holds_ready(),
            "reloaded templates must hold ready until warmup finishes"
        );

        let status = status::RuntimeStatus::new();
        status.set_ready_state(status::RuntimeReadyState::OnRegistration);
        status.update_dataset(
            &TableReference::bare("orders"),
            status::ComponentStatus::Initializing,
        );
        assert!(status.is_ready());
        status.hold_dataset_ready();
        assert!(
            !status.is_ready(),
            "an active ready-hold must keep is_ready false under OnRegistration"
        );

        assert_eq!(df.results_cache_warmer.templates_snapshot().len(), 1);
        df.run_warmup_templates(&df.results_cache_warmer.templates_snapshot(), None)
            .await;
        status.release_dataset_ready();

        let hit = request_context()
            .scope(run_sql(&df, "SELECT id FROM orders WHERE id = 2"))
            .await;
        assert_eq!(
            hit,
            CacheStatus::CacheHit,
            "after restart + warmup, a distinct key of the recorded shape must hit the cache"
        );

        let _ = std::fs::remove_file(&store);
        let _ = std::fs::remove_file(store.with_extension("json.tmp"));
    }

    #[test]
    fn warmup_replay_timeout_uses_query_timeout_or_default() {
        assert_eq!(warmup_replay_timeout(None), DEFAULT_WARMUP_REPLAY_TIMEOUT);

        let mut app = app::AppBuilder::new("test").build();
        app.runtime.query = Some(spicepod::component::runtime::Query {
            timeout: Some("5s".to_string()),
            ..Default::default()
        });
        assert_eq!(
            warmup_replay_timeout(Some(&Arc::new(app))),
            Duration::from_secs(5)
        );
    }

    /// Copilot on #14178: a non-completing Internal-protocol replay held
    /// readiness forever (`warmup_hung=true ready_released=false task_done=false`)
    /// because release ran only after the replay await returned.
    #[tokio::test]
    async fn stalled_warmup_replay_releases_ready_hold() {
        let status = status::RuntimeStatus::new();
        status.set_ready_state(status::RuntimeReadyState::OnRegistration);
        status.update_dataset(
            &TableReference::bare("orders"),
            status::ComponentStatus::Initializing,
        );
        assert!(status.is_ready());
        status.hold_dataset_ready();
        assert!(
            !status.is_ready(),
            "an active ready-hold must keep is_ready false"
        );

        let shutdown = CancellationToken::new();
        let start = std::time::Instant::now();
        let task = tokio::spawn({
            let status = Arc::clone(&status);
            async move {
                run_warmup_releasing_ready(status, shutdown, async {
                    let bound = bound_warmup_op(
                        &CancellationToken::new(),
                        &CancellationToken::new(),
                        Duration::from_millis(50),
                        std::future::pending::<()>(),
                    )
                    .await;
                    assert_eq!(
                        bound,
                        Err(WarmupBound::TimedOut),
                        "a non-completing replay must be bounded"
                    );
                })
                .await;
            }
        });
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("warmup task must finish after the bound fires")
            .expect("warmup task must not panic");
        let task_done = true;
        let warmup_hung = !task_done;
        let ready_released = status.is_ready();
        eprintln!(
            "warmup_hung={warmup_hung} ready_released={ready_released} task_done={task_done} elapsed_ms={}",
            start.elapsed().as_millis()
        );
        assert!(
            ready_released,
            "a stalled replay must release the ready-hold so /v1/ready can recover"
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "the stalled replay must not block readiness, took {:?}",
            start.elapsed()
        );
    }

    /// Dropping the warmup future before its first poll (refresh runtime
    /// shutting down) must still release the ready-hold.
    #[tokio::test]
    async fn dropped_unpolled_warmup_future_releases_ready_hold() {
        let status = status::RuntimeStatus::new();
        status.set_ready_state(status::RuntimeReadyState::OnRegistration);
        status.update_dataset(
            &TableReference::bare("orders"),
            status::ComponentStatus::Initializing,
        );
        status.hold_dataset_ready();
        assert!(!status.is_ready());

        let fut = run_warmup_releasing_ready(
            Arc::clone(&status),
            CancellationToken::new(),
            std::future::pending(),
        );
        let guard_body_ran = false;
        drop(fut);
        let ready_released = status.is_ready();
        eprintln!("guard_body_ran={guard_body_ran} ready_released={ready_released}");
        assert!(
            ready_released,
            "dropping an unpolled warmup future must release the ready-hold"
        );
    }

    /// Stalls the real warmup replay (`run_warmup_templates_bounded` →
    /// `QueryBuilder` → `query.run()` → table `scan`) and proves the bound
    /// finishes the task and releases readiness.
    #[tokio::test]
    async fn stalled_warmup_query_run_releases_ready_hold() {
        let store = std::env::temp_dir().join(format!(
            "spice-warmup-stall-scan-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&store);
        let df = prepare_runtime(None, store.clone()).await;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        df.ctx
            .register_table(
                TableReference::bare("orders"),
                Arc::new(PendingScanTable { schema }) as Arc<dyn TableProvider>,
            )
            .expect("register pending table");

        let status = status::RuntimeStatus::new();
        status.set_ready_state(status::RuntimeReadyState::OnRegistration);
        status.update_dataset(
            &TableReference::bare("orders"),
            status::ComponentStatus::Initializing,
        );
        status.hold_dataset_ready();
        assert!(!status.is_ready());

        let template = WarmupTemplate {
            sql: "SELECT id FROM orders".to_string(),
            bindings: Vec::new(),
        };
        let shutdown = CancellationToken::new();
        let start = std::time::Instant::now();
        let task = tokio::spawn({
            let status = Arc::clone(&status);
            async move {
                run_warmup_releasing_ready(
                    status,
                    shutdown,
                    df.run_warmup_templates_bounded(
                        std::slice::from_ref(&template),
                        None,
                        CancellationToken::new(),
                        Duration::from_millis(200),
                    ),
                )
                .await;
            }
        });
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("warmup task must finish after the bound fires")
            .expect("warmup task must not panic");
        let task_done = true;
        let warmup_hung = !task_done;
        let ready_released = status.is_ready();
        eprintln!(
            "warmup_hung={warmup_hung} ready_released={ready_released} task_done={task_done} elapsed_ms={}",
            start.elapsed().as_millis()
        );
        assert!(
            ready_released,
            "a stalled QueryBuilder replay must release the ready-hold"
        );

        let _ = std::fs::remove_file(&store);
        let _ = std::fs::remove_file(store.with_extension("json.tmp"));
    }

    /// A timed-out first plan must not abort later plans.
    #[tokio::test]
    async fn timed_out_plan_does_not_skip_remaining_warmup_templates() {
        let store = std::env::temp_dir().join(format!(
            "spice-warmup-timeout-continue-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&store);
        let df = prepare_runtime(None, store.clone()).await;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        df.ctx
            .register_table(
                TableReference::bare("stuck"),
                Arc::new(PendingScanTable {
                    schema: Arc::clone(&schema),
                }) as Arc<dyn TableProvider>,
            )
            .expect("register pending table");
        register_table(&df, "orders", vec![1, 2, 3]);

        let templates = [
            WarmupTemplate {
                sql: "SELECT id FROM stuck".to_string(),
                bindings: Vec::new(),
            },
            WarmupTemplate {
                sql: "SELECT id FROM orders".to_string(),
                bindings: Vec::new(),
            },
        ];
        df.run_warmup_templates_bounded(
            &templates,
            None,
            CancellationToken::new(),
            Duration::from_millis(200),
        )
        .await;

        let hit = request_context()
            .scope(run_sql(&df, "SELECT id FROM orders"))
            .await;
        assert_eq!(
            hit,
            CacheStatus::CacheHit,
            "timing out the first plan must still warm the next plan"
        );

        let _ = std::fs::remove_file(&store);
        let _ = std::fs::remove_file(store.with_extension("json.tmp"));
    }

    #[tokio::test]
    async fn stalled_warmup_replay_releases_ready_hold_on_shutdown() {
        let status = status::RuntimeStatus::new();
        status.set_ready_state(status::RuntimeReadyState::OnRegistration);
        status.update_dataset(
            &TableReference::bare("orders"),
            status::ComponentStatus::Initializing,
        );
        status.hold_dataset_ready();
        assert!(!status.is_ready());

        let shutdown = CancellationToken::new();
        let task = tokio::spawn({
            let status = Arc::clone(&status);
            let shutdown = shutdown.clone();
            async move {
                run_warmup_releasing_ready(status, shutdown, std::future::pending()).await;
            }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !status.is_ready(),
            "the ready-hold must stay until shutdown cancels warmup"
        );
        shutdown.cancel();

        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("warmup task must finish after shutdown cancel")
            .expect("join");

        assert!(
            status.is_ready(),
            "shutdown cancel must release the ready-hold so /v1/ready recovers"
        );
    }

    #[tokio::test]
    async fn bound_warmup_op_prefers_shutdown_over_pending() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let result = bound_warmup_op(
            &shutdown,
            &CancellationToken::new(),
            Duration::from_secs(60),
            std::future::pending::<()>(),
        )
        .await;
        assert_eq!(result, Err(WarmupBound::Cancelled));
    }

    #[tokio::test]
    async fn bound_warmup_op_cancels_token_on_timeout() {
        let shutdown = CancellationToken::new();
        let query_cancel = CancellationToken::new();
        let result = bound_warmup_op(
            &shutdown,
            &query_cancel,
            Duration::from_millis(20),
            std::future::pending::<()>(),
        )
        .await;
        assert_eq!(result, Err(WarmupBound::TimedOut));
        assert!(
            query_cancel.is_cancelled(),
            "timeout must cancel the replay token so Query::run stops"
        );
        assert!(
            !shutdown.is_cancelled(),
            "a per-query timeout must not cancel runtime shutdown"
        );
    }

    /// Query::lifetime_guards cancels the child token when `runtime.query.timeout`
    /// fires. That must be TimedOut (skip this plan), not Cancelled (abort the rest).
    #[tokio::test]
    async fn query_lifetime_cancel_is_timeout_not_shutdown() {
        let shutdown = CancellationToken::new();
        let query_cancel = CancellationToken::new();
        let query_cancel_timer = query_cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            query_cancel_timer.cancel();
        });
        let start = std::time::Instant::now();
        let result = bound_warmup_op(
            &shutdown,
            &query_cancel,
            Duration::from_millis(200),
            std::future::pending::<()>(),
        )
        .await;
        eprintln!(
            "drain_result={result:?} elapsed_ms={}",
            start.elapsed().as_millis()
        );
        assert_eq!(
            result,
            Err(WarmupBound::TimedOut),
            "a Query lifetime cancel during drain must skip one plan, not abort warmup"
        );
        assert!(
            start.elapsed() >= Duration::from_millis(180),
            "must wait for the per-op bound, not return at the child-token fire, took {:?}",
            start.elapsed()
        );
    }

    fn timeout_stream_error() -> datafusion::error::DataFusionError {
        datafusion::error::DataFusionError::External(Box::new(super::super::Error::QueryTimedOut {
            query_id: "warmup-test".to_string(),
            timeout: "50ms".to_string(),
        }))
    }

    fn cancelled_stream_error() -> datafusion::error::DataFusionError {
        datafusion::error::DataFusionError::External(Box::new(
            super::super::Error::QueryCancelled {
                query_id: "warmup-test".to_string(),
            },
        ))
    }

    fn classify_stream_result<T>(
        shutdown: &CancellationToken,
        result: Result<Result<T, datafusion::error::DataFusionError>, WarmupBound>,
    ) -> &'static str {
        match result {
            Err(WarmupBound::TimedOut) => "TimedOut",
            Err(WarmupBound::Cancelled) => "Cancelled",
            Ok(Ok(_)) => "success",
            Ok(Err(e)) => match stream_error_to_warmup_bound(shutdown, &e) {
                Err(WarmupBound::TimedOut) => "TimedOut",
                Err(WarmupBound::Cancelled) => "Cancelled",
                Ok(()) => "ordinary_query_error",
            },
        }
    }

    fn classify_query_result<T>(
        shutdown: &CancellationToken,
        result: Result<Result<T, super::super::Error>, WarmupBound>,
    ) -> &'static str {
        match result {
            Err(WarmupBound::TimedOut) => "TimedOut",
            Err(WarmupBound::Cancelled) => "Cancelled",
            Ok(Ok(_)) => "success",
            Ok(Err(e)) => match query_error_to_warmup_bound(shutdown, &e) {
                Err(WarmupBound::TimedOut) => "TimedOut",
                Err(WarmupBound::Cancelled) => "Cancelled",
                Ok(()) => "ordinary_query_error",
            },
        }
    }

    #[test]
    fn query_timeout_stream_error_is_warmup_timeout() {
        let shutdown = CancellationToken::new();
        assert_eq!(
            stream_error_to_warmup_bound(&shutdown, &timeout_stream_error()),
            Err(WarmupBound::TimedOut)
        );
        assert_eq!(
            stream_error_to_warmup_bound(
                &shutdown,
                &datafusion::error::DataFusionError::Internal("not a timeout".to_string())
            ),
            Ok(())
        );
        assert_eq!(
            query_error_to_warmup_bound(
                &shutdown,
                &super::super::Error::QueryTimedOut {
                    query_id: "warmup-test".to_string(),
                    timeout: "50ms".to_string(),
                }
            ),
            Err(WarmupBound::TimedOut)
        );
        assert_eq!(
            query_error_to_warmup_bound(
                &shutdown,
                &super::super::Error::UnableToExecuteQuery {
                    source: datafusion::error::DataFusionError::Internal("plan".to_string()),
                }
            ),
            Ok(())
        );

        // DISTINCT query.run() used to collapse QueryTimedOut into Ok(0).
        let distinct_run = classify_query_result::<()>(
            &shutdown,
            Ok(Err(super::super::Error::QueryTimedOut {
                query_id: "warmup-test".to_string(),
                timeout: "50ms".to_string(),
            })),
        );
        assert_eq!(
            distinct_run, "TimedOut",
            "a timed-out DISTINCT query.run() must skip the plan, not look like an empty success"
        );

        let shutdown_cancelled = CancellationToken::new();
        shutdown_cancelled.cancel();
        assert_eq!(
            stream_error_to_warmup_bound(&shutdown_cancelled, &cancelled_stream_error()),
            Err(WarmupBound::Cancelled)
        );
        assert_eq!(
            stream_error_to_warmup_bound(&shutdown, &cancelled_stream_error()),
            Err(WarmupBound::TimedOut),
            "QueryCancelled without runtime shutdown must skip one plan, not abort warmup"
        );
    }

    /// Copilot: a Query lifetime timer that fires during DISTINCT/`drain`
    /// yields `Ok(Err(QueryTimedOut))` from `bound_warmup_op`. That must be
    /// `TimedOut` (warn and skip the plan), not `ordinary_query_error`.
    #[tokio::test]
    async fn query_timeout_during_distinct_drain_is_not_ordinary_error() {
        let shutdown = CancellationToken::new();
        let query_cancel = CancellationToken::new();
        let start = std::time::Instant::now();
        let result = bound_warmup_op(
            &shutdown,
            &query_cancel,
            Duration::from_millis(200),
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Err::<(), _>(timeout_stream_error())
            },
        )
        .await;
        let classification = classify_stream_result(&shutdown, result);
        let elapsed_ms = start.elapsed().as_millis();
        eprintln!("classification={classification} elapsed_ms={elapsed_ms}");
        assert_eq!(
            classification, "TimedOut",
            "QueryTimedOut from DISTINCT drain must skip the plan, not look like success"
        );
        assert!(
            elapsed_ms < 180,
            "must return when the stream yields QueryTimedOut, not wait for the outer bound, took {elapsed_ms}ms"
        );
    }

    #[tokio::test]
    async fn query_timeout_from_query_run_is_not_ordinary_error() {
        let shutdown = CancellationToken::new();
        let query_cancel = CancellationToken::new();
        let start = std::time::Instant::now();
        let result = bound_warmup_op(
            &shutdown,
            &query_cancel,
            Duration::from_millis(200),
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Err::<(), _>(super::super::Error::QueryTimedOut {
                    query_id: "warmup-test".to_string(),
                    timeout: "50ms".to_string(),
                })
            },
        )
        .await;
        let classification = classify_query_result(&shutdown, result);
        let elapsed_ms = start.elapsed().as_millis();
        eprintln!("classification={classification} elapsed_ms={elapsed_ms}");
        assert_eq!(
            classification, "TimedOut",
            "QueryTimedOut from query.run() must skip the plan, not look like an ordinary failure"
        );
        assert!(
            elapsed_ms < 180,
            "must return when query.run() yields QueryTimedOut, took {elapsed_ms}ms"
        );
    }

    #[tokio::test]
    async fn query_cancel_during_drain_is_shutdown_cancelled() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let result = bound_warmup_op(
            &shutdown,
            &CancellationToken::new(),
            Duration::from_millis(200),
            async { Err::<(), _>(cancelled_stream_error()) },
        )
        .await;
        assert_eq!(
            classify_stream_result(&shutdown, result),
            "Cancelled",
            "a QueryCancelled observed after shutdown must abort remaining warmup"
        );
    }

    /// Copilot: after the DISTINCT lifetime timer cancels `query_cancel`,
    /// the row loop kept issuing nested replays (`query_cancelled=true
    /// processed=40 elapsed_ms=83 timeout_ms=20`) and held `/v1/ready`.
    #[tokio::test]
    async fn distinct_row_loop_stops_when_query_token_cancels() {
        let shutdown = CancellationToken::new();
        let query_cancel = CancellationToken::new();
        let query_cancel_timer = query_cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            query_cancel_timer.cancel();
        });

        let start = std::time::Instant::now();
        let mut processed = 0_u32;
        let mut result = Ok(());
        for _ in 0..40 {
            if let Err(bound) = warmup_row_deadline(&shutdown, &query_cancel) {
                result = Err(bound);
                break;
            }
            processed += 1;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let elapsed_ms = start.elapsed().as_millis();
        eprintln!(
            "query_cancelled={} processed={processed} elapsed_ms={elapsed_ms} timeout_ms=20",
            query_cancel.is_cancelled()
        );
        assert_eq!(
            result,
            Err(WarmupBound::TimedOut),
            "a DISTINCT lifetime cancel mid-batch must skip the rest of the batch"
        );
        assert!(
            processed < 40,
            "must stop before the remaining DISTINCT keys, processed={processed}"
        );
        assert!(
            query_cancel.is_cancelled(),
            "the DISTINCT query token must have fired"
        );
        assert!(
            !shutdown.is_cancelled(),
            "a query-token cancel must not look like runtime shutdown"
        );
    }

    /// Copilot: nested `execute_warmup_sql` used a sibling of the DISTINCT
    /// token, so a lifetime-timer fire at 20 ms left the nested replay
    /// running (`nested_cancelled=false` finished at 102 ms).
    #[tokio::test]
    async fn nested_replay_cancels_when_distinct_token_fires() {
        let store = std::env::temp_dir().join(format!(
            "spice-warmup-nested-cancel-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&store);
        let df = prepare_runtime(None, store.clone()).await;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        df.ctx
            .register_table(
                TableReference::bare("orders"),
                Arc::new(PendingScanTable { schema }) as Arc<dyn TableProvider>,
            )
            .expect("register pending table");

        let distinct_cancel = CancellationToken::new();
        let ctx = Arc::new(
            RequestContext::builder(Protocol::Internal)
                .with_cache_control(CacheControl::Cache(CacheKeyType::Default))
                .with_cache_namespace(CacheNamespace::Public)
                .with_query_timeout(Some(Duration::from_millis(200)))
                .build(),
        );
        let distinct_timer = distinct_cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            distinct_timer.cancel();
        });

        let start = std::time::Instant::now();
        let result =
            execute_warmup_sql(&df, "SELECT id FROM orders", None, &ctx, &distinct_cancel).await;
        let elapsed_ms = start.elapsed().as_millis();
        let nested_cancelled = distinct_cancel.is_cancelled();
        eprintln!("nested_cancelled={nested_cancelled} elapsed_ms={elapsed_ms}");
        assert_eq!(
            result,
            Err(WarmupBound::TimedOut),
            "a DISTINCT token fire must drop the nested replay, not wait for its own timeout"
        );
        assert!(nested_cancelled, "the DISTINCT token must have fired");
        assert!(
            elapsed_ms < 80,
            "nested replay must stop at the DISTINCT cancel, not finish at the 200ms bound, took {elapsed_ms}ms"
        );

        let _ = std::fs::remove_file(&store);
        let _ = std::fs::remove_file(store.with_extension("json.tmp"));
    }

    #[tokio::test]
    async fn multi_table_plans_are_not_recorded_for_warmup() {
        let store = std::env::temp_dir().join(format!(
            "spice-warmup-join-skip-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&store);
        let df = prepare_runtime(None, store.clone()).await;

        let ctx = datafusion::prelude::SessionContext::new();
        ctx.sql("CREATE TABLE orders (id INT, customer_id INT)")
            .await
            .expect("create orders")
            .collect()
            .await
            .expect("collect create orders");
        ctx.sql("CREATE TABLE customers (id INT, name VARCHAR)")
            .await
            .expect("create customers")
            .collect()
            .await
            .expect("collect create customers");
        let plan = ctx
            .sql(
                "SELECT orders.id FROM orders JOIN customers ON orders.customer_id = customers.id \
                 WHERE orders.id = 1 AND customers.name = 'acme'",
            )
            .await
            .expect("sql")
            .logical_plan()
            .clone();
        df.observe_results_cache_warmup_plan(&plan, &CacheNamespace::Public);
        assert!(
            df.results_cache_warmer.templates_snapshot().is_empty(),
            "join plans with bindings on more than one table must not occupy a warmup slot"
        );
        let _ = std::fs::remove_file(&store);
    }

    #[tokio::test]
    async fn persisted_multi_table_template_is_not_executed_at_replay() {
        let store = std::env::temp_dir().join(format!(
            "spice-warmup-join-replay-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&store);
        let df = prepare_runtime(None, store.clone()).await;
        register_table(&df, "orders", vec![1, 2, 3]);
        register_table(&df, "customers", vec![10, 20]);

        let template = WarmupTemplate {
            sql: "SELECT id FROM orders WHERE id = $1".to_string(),
            bindings: vec![
                WarmupBinding {
                    table: "orders".to_string(),
                    column: "id".to_string(),
                },
                WarmupBinding {
                    table: "customers".to_string(),
                    column: "id".to_string(),
                },
            ],
        };
        df.run_warmup_templates(&[template], None).await;

        let cache = df.results_cache_provider().expect("results cache");
        cache.run_pending_tasks().await;
        assert_eq!(
            cache.size().await,
            0,
            "a multi-table template must not replay placeholder SQL without parameters"
        );
        let miss = request_context()
            .scope(run_sql(&df, "SELECT id FROM orders WHERE id = 2"))
            .await;
        assert_eq!(
            miss,
            CacheStatus::CacheMiss,
            "skipping a multi-table template must leave the cache empty for that shape"
        );
        let _ = std::fs::remove_file(&store);
    }

    #[tokio::test]
    async fn empty_distinct_keys_do_not_execute_placeholder_sql() {
        let store = std::env::temp_dir().join(format!(
            "spice-warmup-empty-keys-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&store);
        let df = prepare_runtime(None, store.clone()).await;
        register_table(&df, "orders", vec![]);

        let template = WarmupTemplate {
            sql: "SELECT id FROM orders WHERE id = $1".to_string(),
            bindings: vec![WarmupBinding {
                table: "orders".to_string(),
                column: "id".to_string(),
            }],
        };
        df.run_warmup_templates(&[template], None).await;

        let cache = df.results_cache_provider().expect("results cache");
        cache.run_pending_tasks().await;
        assert_eq!(
            cache.size().await,
            0,
            "an empty DISTINCT result must not run the template SQL without parameters"
        );
        let _ = std::fs::remove_file(&store);
    }

    #[tokio::test]
    async fn high_cardinality_warmup_stops_when_cache_cannot_grow() {
        let store = std::env::temp_dir().join(format!(
            "spice-warmup-cardinality-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&store);
        let key_count = 64_i64;
        let df = prepare_runtime(Some("2KiB"), store.clone()).await;
        register_table(&df, "orders", (1..=key_count).collect());

        let template = WarmupTemplate {
            sql: "SELECT id FROM orders WHERE id = $1".to_string(),
            bindings: vec![WarmupBinding {
                table: "orders".to_string(),
                column: "id".to_string(),
            }],
        };
        df.run_warmup_templates(&[template], None).await;

        let cache = df.results_cache_provider().expect("results cache");
        cache.run_pending_tasks().await;
        let size = cache.size().await;
        let max = cache.max_size();
        let items = cache.item_count().await;
        assert!(
            size <= max,
            "warmup must not leave the cache over its byte budget: size={size} max={max}"
        );
        assert!(
            items < u64::try_from(key_count).expect("key_count fits u64"),
            "a 2KiB cache must not retain every distinct key; LRU eviction on insert has to stop replay, got {items} items"
        );
        let _ = std::fs::remove_file(&store);
    }

    /// A broad, unparameterized scan yields many batches. Warmup must drain
    /// them so the cache wrapper can store, without holding the full result
    /// until the query finishes.
    #[tokio::test]
    async fn unparameterized_warmup_drains_multi_batch_result_and_caches() {
        let store =
            std::env::temp_dir().join(format!("spice-warmup-drain-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&store);
        let df = prepare_runtime(None, store.clone()).await;
        let partitions: Vec<Vec<i64>> = (0..16)
            .map(|partition| ((partition * 8 + 1)..=(partition * 8 + 8)).collect())
            .collect();
        register_table_partitions(&df, "orders", partitions);

        let template = WarmupTemplate {
            sql: "SELECT id FROM orders".to_string(),
            bindings: vec![],
        };
        df.run_warmup_templates(&[template], None).await;

        let hit = request_context()
            .scope(run_sql(&df, "SELECT id FROM orders"))
            .await;
        assert_eq!(
            hit,
            CacheStatus::CacheHit,
            "draining a multi-batch warmup stream must still store the result"
        );
        let _ = std::fs::remove_file(&store);
    }

    #[test]
    fn warmup_waits_on_per_process_completion_not_reusable_flag() {
        let status = status::RuntimeStatus::new();
        let name = TableReference::bare("orders");
        let completion = RefreshCompletion::new();
        assert!(
            !first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&completion),
                &status,
                &name
            ),
            "Full must wait for this process's refresh completion, even when the reusable flag is already true"
        );
        assert!(
            !first_full_or_append_refresh_settled_for(
                RefreshMode::Append,
                Some(&completion),
                &status,
                &name
            ),
            "Append must wait for this process's refresh completion"
        );
        assert!(
            first_full_or_append_refresh_settled_for(
                RefreshMode::Changes,
                Some(&completion),
                &status,
                &name
            ),
            "Changes is not a warmup gate"
        );
        assert!(
            first_full_or_append_refresh_settled_for(
                RefreshMode::Caching,
                Some(&completion),
                &status,
                &name
            ),
            "Caching is not a warmup gate"
        );

        completion.record_untriggered();
        assert!(
            first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&completion),
                &status,
                &name
            ),
            "Disabled startup (no scheduled refresh) must release warmup"
        );

        let recorded = RefreshCompletion::new();
        let id = recorded.issue();
        recorded.record(id);
        assert!(
            first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&recorded),
                &status,
                &name
            ),
            "a recorded refresh after invalidation must release warmup"
        );

        assert!(
            first_full_or_append_refresh_settled_for(RefreshMode::Full, None, &status, &name),
            "a table with no completion signal cannot be waited on"
        );
    }

    /// Scheduler tables call `RefreshCompletion::close()` because they never
    /// refresh locally. `has_recorded()` is then true while the dataset is
    /// still `Refreshing`, waiting on executor `PartitionsLoaded` acks.
    /// Warmup is once-only, so settling on `close()` would replay before
    /// distributed data is queryable and never retry.
    #[test]
    fn scheduler_close_does_not_settle_warmup_before_distributed_ready() {
        let completion = RefreshCompletion::new();
        completion.close();
        let status = status::RuntimeStatus::new();
        let name = TableReference::bare("orders");
        status.update_dataset(&name, status::ComponentStatus::Refreshing);
        status.hold_dataset_ready();

        // Reproduction of the interleaving Copilot reported: close() answers
        // has_recorded, visible status is still Refreshing, and the old settle
        // predicate would have returned true.
        let completion_closed = completion.has_recorded();
        let dataset_ready =
            status.get_dataset_status(&name) == Some(status::ComponentStatus::Ready);
        let old_settled = completion_closed;
        eprintln!(
            "scheduler warmup interleaving: completion_closed={completion_closed} dataset_ready={dataset_ready} old_settled={old_settled}"
        );
        assert!(
            completion_closed && !dataset_ready && old_settled,
            "the close-before-PartitionsLoaded interleaving must still be observable"
        );

        assert!(
            !first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&completion),
                &status,
                &name
            ),
            "scheduler close must not start warmup before PartitionsLoaded Ready"
        );

        status.update_dataset(&name, status::ComponentStatus::Ready);
        assert_eq!(
            status.get_dataset_status(&name),
            Some(status::ComponentStatus::Refreshing),
            "the ready-hold must keep Ready invisible so /v1/ready stays false"
        );
        assert!(
            first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&completion),
                &status,
                &name
            ),
            "a held Ready from PartitionsLoaded must release warmup"
        );

        let qualified = TableReference::full("spice", "public", "orders");
        assert!(
            first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&completion),
                &status,
                &qualified
            ),
            "bare and spice.public names must resolve as the same dataset"
        );
    }

    #[test]
    fn scheduler_error_or_disabled_does_not_block_warmup_forever() {
        let completion = RefreshCompletion::new();
        completion.close();
        let status = status::RuntimeStatus::new();
        let name = TableReference::bare("orders");
        status.hold_dataset_ready();
        status.update_dataset(
            &name,
            status::ComponentStatus::error_with_message("partition load failed"),
        );

        assert!(
            first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&completion),
                &status,
                &name
            ),
            "a scheduler dataset that errored will never become Ready; do not hang warmup"
        );

        let disabled = TableReference::bare("legacy");
        status.update_dataset(&disabled, status::ComponentStatus::Disabled);
        assert!(
            first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&completion),
                &status,
                &disabled
            ),
            "Disabled is terminal for warmup the same way Error is"
        );
    }

    /// Scheduler accelerated views close `RefreshCompletion` the same way
    /// datasets do, but readiness is published with `update_view`.
    #[test]
    fn scheduler_accelerated_view_ready_settles_warmup() {
        let completion = RefreshCompletion::new();
        completion.close();
        let status = status::RuntimeStatus::new();
        let name = TableReference::bare("sales_by_region");
        status.hold_dataset_ready();
        status.update_view(&name, status::ComponentStatus::Refreshing);

        assert!(
            !first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&completion),
                &status,
                &name
            ),
            "a scheduler view still Refreshing must not start warmup"
        );

        status.update_view(&name, status::ComponentStatus::Ready);
        let view_status = status.get_view_statuses().get(&name).cloned();
        let dataset_status = status.get_dataset_status(&name);
        let current_settled = first_full_or_append_refresh_settled_for(
            RefreshMode::Full,
            Some(&completion),
            &status,
            &name,
        );
        eprintln!(
            "scheduler view warmup: view_status={view_status:?} dataset_status={dataset_status:?} current_settled={current_settled}"
        );
        assert_eq!(
            view_status,
            Some(status::ComponentStatus::Ready),
            "readiness for an accelerated view is published under view:*"
        );
        assert_eq!(
            dataset_status, None,
            "an accelerated view must not require a dataset:* status"
        );
        assert!(
            current_settled,
            "view Ready with no dataset status must release warmup"
        );

        let qualified = TableReference::full("spice", "public", "sales_by_region");
        assert!(
            first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&completion),
                &status,
                &qualified
            ),
            "bare and spice.public names must resolve as the same view"
        );

        let failed = TableReference::bare("broken_view");
        status.update_view(
            &failed,
            status::ComponentStatus::error_with_message("partition load failed"),
        );
        assert!(
            first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&completion),
                &status,
                &failed
            ),
            "a scheduler view that errored will never become Ready; do not hang warmup"
        );

        let disabled = TableReference::bare("legacy_view");
        status.update_view(&disabled, status::ComponentStatus::Disabled);
        assert!(
            first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&completion),
                &status,
                &disabled
            ),
            "a Disabled view is terminal for warmup the same way Error is"
        );
    }

    #[test]
    fn local_refresh_error_is_not_settled_until_completion_or_disable() {
        let completion = RefreshCompletion::new();
        let status = status::RuntimeStatus::new();
        let name = TableReference::bare("orders");
        status.hold_dataset_ready();
        status.update_dataset(
            &name,
            status::ComponentStatus::error_with_message("source refresh failed"),
        );

        // Reproduction of the interleaving Copilot reported: a local Full
        // refresh failed, completion is unrecorded (retries may still run),
        // and the warmup hold keeps /v1/ready false even in OnRegistration.
        status.set_ready_state(status::RuntimeReadyState::OnRegistration);
        let settled_after_permanent_refresh_error = first_full_or_append_refresh_settled_for(
            RefreshMode::Full,
            Some(&completion),
            &status,
            &name,
        );
        let on_registration_without_warmup_hold = {
            let unlocked = status::RuntimeStatus::new();
            unlocked.set_ready_state(status::RuntimeReadyState::OnRegistration);
            unlocked.update_dataset(
                &name,
                status::ComponentStatus::error_with_message("source refresh failed"),
            );
            unlocked.is_ready()
        };
        let on_registration_with_warmup_hold = status.is_ready();
        eprintln!(
            "local refresh error interleaving: settled_after_permanent_refresh_error={settled_after_permanent_refresh_error} on_registration_without_warmup_hold={on_registration_without_warmup_hold} on_registration_with_warmup_hold={on_registration_with_warmup_hold}"
        );
        assert!(
            !settled_after_permanent_refresh_error
                && on_registration_without_warmup_hold
                && !on_registration_with_warmup_hold,
            "Error without a recorded completion must not start once-only warmup"
        );

        let id = completion.issue();
        completion.record_terminal_failure(id);
        assert!(
            !completion.has_recorded(),
            "a one-shot failure must not look like a successful completion"
        );
        assert!(
            first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&completion),
                &status,
                &name
            ),
            "a one-shot failure records a terminal-failure outcome so warmup can finish"
        );

        let disabled = RefreshCompletion::new();
        let disabled_name = TableReference::bare("legacy");
        status.update_dataset(&disabled_name, status::ComponentStatus::Disabled);
        assert!(
            first_full_or_append_refresh_settled_for(
                RefreshMode::Full,
                Some(&disabled),
                &status,
                &disabled_name
            ),
            "Disabled is terminal for a local refresh that will never complete"
        );
    }
}
