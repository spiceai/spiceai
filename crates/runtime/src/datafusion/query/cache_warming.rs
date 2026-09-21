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
//! columns until the cache is full. Datasets stay not ready until that warmup
//! completes, so `/v1/ready` does not succeed on a cold cache.
//!
//! Only [`CacheNamespace::Public`] plans are recorded and replayed. Authenticated
//! (principal-scoped) and system traffic is skipped: results-cache keys include the
//! namespace, so warming into `Public` could never satisfy a principal-scoped
//! request, and persisting principal ids into the warmup catalog is undesirable.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use app::App;
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::logical_expr::LogicalPlan;
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

use crate::accelerated::AcceleratedTable;
use crate::component::dataset::acceleration::RefreshMode;
use crate::datafusion::DataFusion;

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
    count: Arc<AtomicUsize>,
    started: AtomicBool,
    persist: WarmupPersist,
}

impl ResultsCacheWarmer {
    pub(crate) fn new(store_path: PathBuf, enabled: bool) -> Self {
        let loaded = if enabled {
            load_templates(&store_path)
        } else {
            Vec::new()
        };
        Self::from_loaded(loaded, enabled, WarmupPersist::Local(store_path))
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
        Self::from_loaded(loaded, enabled, WarmupPersist::Remote(state))
    }

    fn from_loaded(loaded: Vec<WarmupTemplate>, enabled: bool, persist: WarmupPersist) -> Self {
        if enabled {
            tracing::info!(
                "SQL results cache warmup is enabled: the first {MAX_WARMUP_PLANS} distinct query plans will be recorded and replayed after the first full or append refresh until the cache is full"
            );
        }
        let ids = loaded.iter().map(template_id).collect::<HashSet<_>>();
        let count = loaded.len();
        Self {
            enabled,
            catalog: Arc::new(parking_lot::Mutex::new(WarmupCatalog {
                templates: loaded,
                ids,
            })),
            persist_lock: Arc::new(parking_lot::Mutex::new(())),
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

    fn schedule_persist(&self) {
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
                tokio::spawn(async move {
                    persist_remote(state, catalog, count).await;
                });
            }
        }
    }
}

fn load_templates(path: &Path) -> Vec<WarmupTemplate> {
    let Ok(bytes) = std::fs::read(path) else {
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

fn merge_templates(base: &[WarmupTemplate], extra: &[WarmupTemplate]) -> Vec<WarmupTemplate> {
    let mut out = base.to_vec();
    let mut ids: HashSet<u64> = out.iter().map(template_id).collect();
    for template in extra {
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
    let mut local = catalog.lock().templates.clone();
    for _ in 0..MAX_REMOTE_PERSIST_ATTEMPTS {
        let remote = match state.get(WARMUP_STATE_KEY).await {
            Ok(templates) => templates.unwrap_or_default(),
            Err(e) => {
                tracing::debug!("Failed to persist SQL results cache warmup catalog: {e}");
                return;
            }
        };
        let merged = merge_templates(&remote, &local);
        if merged == remote {
            apply_catalog(catalog.as_ref(), count.as_ref(), &merged);
            return;
        }
        if remote.is_empty() {
            match state.insert(WARMUP_STATE_KEY, &merged).await {
                Ok(InsertResult::Ok) => {
                    apply_catalog(catalog.as_ref(), count.as_ref(), &merged);
                    return;
                }
                Ok(InsertResult::AlreadyExists) => {
                    local = merged;
                    continue;
                }
                Err(e) => {
                    tracing::debug!("Failed to persist SQL results cache warmup catalog: {e}");
                    return;
                }
            }
        }
        match state.update(WARMUP_STATE_KEY, &merged).await {
            Ok(UpdateResult::Ok) => {
                apply_catalog(catalog.as_ref(), count.as_ref(), &merged);
                return;
            }
            Ok(UpdateResult::NotFound) => {
                local = merged;
            }
            Ok(UpdateResult::Conflict { current }) => {
                local = merge_templates(&current, &merged);
            }
            Err(e) => {
                tracing::debug!("Failed to persist SQL results cache warmup catalog: {e}");
                return;
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
        return ResultsCacheWarmer::new(default_warmup_store_path(), enabled);
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
            ResultsCacheWarmer::new(default_warmup_store_path(), true)
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
                return;
            }
            let run = {
                let df = Arc::clone(&df);
                let status = Arc::clone(&status);
                async move {
                    df.run_warmup_templates(&templates, app.as_ref()).await;
                    status.release_dataset_ready();
                }
            };
            if let Some(runtime) = refresh_runtime {
                runtime.spawn(run);
            } else {
                run.await;
            }
        });
    }

    async fn wait_for_first_full_append_refresh(&self, status: &status::RuntimeStatus) -> bool {
        loop {
            if status.is_shutdown() {
                return false;
            }
            if self.accelerated_initial_loads_done().await {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    async fn accelerated_initial_loads_done(&self) -> bool {
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
            let mode = table.refresher().refresh_mode().await;
            if matches!(mode, RefreshMode::Full | RefreshMode::Append)
                && !table.refresher().initial_load_completed()
            {
                return false;
            }
        }
        true
    }

    async fn run_warmup_templates(
        self: &Arc<Self>,
        templates: &[WarmupTemplate],
        app: Option<&Arc<App>>,
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
                .build(),
        );

        let mut stored = 0_u64;
        for template in templates {
            cache_provider.run_pending_tasks().await;
            if cache_provider.size().await >= cache_provider.max_size() {
                break;
            }
            stored += self
                .warm_one_template(template, &request_context, cache_provider.as_ref())
                .await;
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
    ) -> u64 {
        if template.bindings.is_empty() {
            return u64::from(execute_warmup_sql(self, &template.sql, None, request_context).await);
        }
        let Some(distinct_sql) = distinct_keys_sql(template) else {
            return 0;
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
) -> u64 {
    let query = QueryBuilder::new(distinct_sql, Arc::clone(df))
        .for_results_cache_warming()
        .results_cache_mode(ResultsCacheMode::Bypass)
        .build();
    let Ok(result) = Arc::clone(request_context)
        .scope(async move { query.run().await })
        .await
    else {
        return 0;
    };

    let mut stream = result.data;
    let mut stored = 0_u64;
    let max_size = cache_provider.max_size();

    while let Ok(Some(batch)) = stream.try_next().await {
        for row_idx in 0..batch.num_rows() {
            cache_provider.run_pending_tasks().await;
            let size_before = cache_provider.size().await;
            if size_before >= max_size {
                return stored;
            }

            let Ok(values) = (0..batch.num_columns())
                .map(|col_idx| ScalarValue::try_from_array(batch.column(col_idx), row_idx))
                .collect::<Result<Vec<_>, _>>()
            else {
                continue;
            };

            if execute_warmup_sql(df, template_sql, Some(values), request_context).await {
                stored += 1;
                cache_provider.run_pending_tasks().await;
                if cache_provider.size().await <= size_before {
                    return stored;
                }
            }
        }
    }
    stored
}

async fn execute_warmup_sql(
    df: &Arc<DataFusion>,
    sql: &str,
    parameters: Option<Vec<ScalarValue>>,
    request_context: &Arc<RequestContext>,
) -> bool {
    let mut builder = QueryBuilder::new(sql, Arc::clone(df)).for_results_cache_warming();
    if let Some(values) = parameters {
        builder = builder.parameters(Some(ParamValues::from(values)));
    }
    let query = builder.build();
    let result = Arc::clone(request_context)
        .scope(async move { query.run().await })
        .await;
    match result {
        Ok(query_result) => query_result.data.try_collect::<Vec<_>>().await.is_ok(),
        Err(e) => {
            tracing::debug!("SQL results cache warmup query failed: {e}");
            false
        }
    }
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
        builder::RuntimeBuilder,
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
            .with_results_cache_warmup_store(store)
            .with_results_cache_warmup_enabled(true)
            .build(),
        )
    }

    async fn prepare_runtime_disabled(store: PathBuf) -> Arc<DataFusion> {
        let runtime = RuntimeBuilder::new().build().await;
        Arc::new(
            DataFusion::builder(
                status::RuntimeStatus::new(),
                runtime.accelerator_engine_registry(),
                Handle::current(),
            )
            .with_results_cache_warmup_store(store)
            .with_results_cache_warmup_enabled(false)
            .build(),
        )
    }

    async fn register_table(df: &Arc<DataFusion>, name: &str, values: Vec<i64>) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .expect("batch");
        let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table"));
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
        register_table(&df, "orders", vec![1, 2, 3]).await;

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
        register_table(&df, "orders", vec![1, 2, 3]).await;

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

        let reloaded = ResultsCacheWarmer::new(store.clone(), true);
        assert_eq!(
            reloaded.templates_snapshot().len(),
            1,
            "a new process must load the persisted plan shape"
        );
        let _ = std::fs::remove_file(&store);
        let _ = std::fs::remove_file(store.with_extension("json.tmp"));
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
        register_table(&df, "orders", vec![1, 2, 3]).await;
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
        register_table(&df, "orders", vec![1, 2, 3]).await;
        register_table(&df, "customers", vec![10, 20]).await;

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
        register_table(&df, "orders", vec![]).await;

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
        register_table(&df, "orders", (1..=key_count).collect()).await;

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
}
