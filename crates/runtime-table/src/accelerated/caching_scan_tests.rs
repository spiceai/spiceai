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

use super::*;
use arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampNanosecondArray, UInt16Array};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow_tools::metadata_keys::HTTP_RESPONSE_STATUS_METADATA_KEY;
use cache::utils::RESPONSE_STATUS_COLUMN;
use datafusion::datasource::TableType;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::optimizer::{PhysicalOptimizerContext, PhysicalOptimizerRule};
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::{ExecutionPlanProperties, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_datasource::{memory::MemorySourceConfig, source::DataSourceExec};
use runtime_request_context::{CacheNamespace, Protocol, RequestContext};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

const QUERY: &str =
    "SELECT content FROM registry WHERE request_path = '/devices' AND request_query = 'key=a'";

#[derive(Debug)]
struct ScanObservation {
    session: String,
    partitions: usize,
    filters: Vec<String>,
    limit: Option<usize>,
}

/// Records plan construction separately from stream execution. The rows include
/// keys and namespaces the accelerator declines to filter, exercising the real
/// accelerated layer's residual predicates.
#[derive(Debug)]
struct ScanProvider {
    schema: SchemaRef,
    batches: parking_lot::RwLock<Vec<Vec<RecordBatch>>>,
    scans: AtomicUsize,
    active_scans: AtomicUsize,
    fail: AtomicBool,
    block: AtomicBool,
    entered: Notify,
    release: Notify,
    observations: parking_lot::Mutex<Vec<ScanObservation>>,
}

struct ActiveScan<'a>(&'a AtomicUsize);

impl Drop for ActiveScan<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ScanProvider {
    fn new(schema: SchemaRef, batches: Vec<Vec<RecordBatch>>) -> Self {
        Self {
            schema,
            batches: parking_lot::RwLock::new(batches),
            scans: AtomicUsize::new(0),
            active_scans: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            block: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
            observations: parking_lot::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl TableProvider for ScanProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        self.active_scans.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveScan(&self.active_scans);
        self.observations.lock().push(ScanObservation {
            session: state.session_id().to_string(),
            partitions: state.config().target_partitions(),
            filters: filters.iter().map(ToString::to_string).collect(),
            limit,
        });
        self.entered.notify_one();
        if self.block.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
        if self.fail.load(Ordering::SeqCst) {
            return Err(DataFusionError::Execution("test scan failed".to_string()));
        }
        let batches = self.batches.read();
        let schema = batches
            .iter()
            .flatten()
            .next()
            .map_or_else(|| self.schema(), RecordBatch::schema);
        Ok(Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&batches, schema, projection.cloned())?,
        ))))
    }
}

fn source_schema() -> SchemaRef {
    Arc::new(
        Schema::new(vec![
            Field::new("request_path", DataType::Utf8, true),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, true),
            Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
            Field::new(
                caching::CACHE_REFRESHED_AT_COLUMN,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ])
        .with_metadata(HashMap::from([(
            HTTP_RESPONSE_STATUS_METADATA_KEY.to_string(),
            "1".to_string(),
        )])),
    )
}

fn row(schema: &SchemaRef, key: &str, content: &str, status: u16, namespace: &str) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["/devices"])),
        Arc::new(StringArray::from(vec![key])),
        Arc::new(StringArray::from(vec![content])),
        Arc::new(UInt16Array::from(vec![status])),
        Arc::new(TimestampNanosecondArray::from(vec![0])),
    ];
    if schema.fields().len() == 6 {
        columns.push(Arc::new(StringArray::from(vec![namespace])));
    }
    RecordBatch::try_new(Arc::clone(schema), columns).expect("test response")
}

struct Fixture {
    ctx: SessionContext,
    table: Arc<AcceleratedTable>,
    source: Arc<ScanProvider>,
    cache: Arc<ScanProvider>,
    _writes: caching::CacheWriteReceiver,
}

impl Fixture {
    async fn new(ttl: Duration, swr: Duration, stale_if_error: StaleIfError, status: u16) -> Self {
        let schema = source_schema();
        let source = Arc::new(ScanProvider::new(
            Arc::clone(&schema),
            vec![vec![row(&schema, "key=a", "origin", status, "")]],
        ));
        let storage = Arc::new(
            caching::extend_schema_with_cache_namespace("registry", &schema)
                .expect("storage schema"),
        );
        let cache = Arc::new(ScanProvider::new(
            Arc::clone(&storage),
            vec![
                vec![
                    row(&storage, "key=b", "wrong-key", 200, "system"),
                    row(&storage, "key=a", "cached-b", 200, "system"),
                ],
                vec![
                    row(&storage, "key=a", "wrong-namespace", 200, "public"),
                    row(&storage, "key=a", "cached-a", 200, "system"),
                ],
            ],
        ));
        let mut builder = Builder::new(
            status::RuntimeStatus::new(),
            TableReference::bare("registry"),
            Arc::new(FederatedTable::new_unchecked(
                Arc::clone(&source) as Arc<dyn TableProvider>
            )),
            "http".to_string(),
            Arc::clone(&cache) as Arc<dyn TableProvider>,
            refresh::Refresh::new(RefreshMode::Caching),
            Handle::current(),
        );
        builder
            .user_facing_schema(schema)
            .caching_ttl(Some(ttl))
            .caching_stale_while_revalidate_ttl(Some(swr))
            .caching_stale_if_error(stale_if_error);
        let mut table = builder.build().await.expect("accelerated table");
        // Keep write-side scans out of the read-plan counter. The channel still
        // exercises real enqueueing and the in-flight claim stays held by it.
        for handler in table.handlers.drain(..) {
            handler.abort();
            let _ = handler.await;
        }
        let (tx, rx) = caching::create_cache_write_channel();
        table.batch_write_tx = Some(tx);
        let table = Arc::new(table);
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        ctx.register_table("registry", Arc::clone(&table).into_table())
            .expect("register");
        Self {
            ctx,
            table,
            source,
            cache,
            _writes: rx,
        }
    }

    async fn source_first(status: u16) -> Self {
        Self::new(
            Duration::ZERO,
            Duration::ZERO,
            StaleIfError::Enabled,
            status,
        )
        .await
    }

    async fn plan(&self, sql: &str) -> Arc<dyn ExecutionPlan> {
        self.ctx
            .sql(sql)
            .await
            .expect("SQL")
            .create_physical_plan()
            .await
            .expect("physical plan")
    }
}

fn contents(batches: &[RecordBatch]) -> Vec<String> {
    let mut values = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("content")
            .expect("content")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string content");
        values.extend(column.iter().map(|v| v.expect("content value").to_string()));
    }
    values
}

#[tokio::test]
async fn healthy_queries_never_plan_the_accelerator() {
    let fixture = Fixture::source_first(200).await;
    fixture.cache.fail.store(true, Ordering::SeqCst);
    let plan = fixture.plan(QUERY).await;
    assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 0);
    let mut node = Arc::clone(&plan);
    while node.name() != "CachingAccelerationScanExec" {
        assert_eq!(node.children().len(), 1);
        node = Arc::clone(node.children()[0]);
    }
    assert!(node.children().is_empty());
    assert!(node.required_input_distribution().is_empty());
    assert_eq!(node.output_partitioning().partition_count(), 1);
    assert!(node.equivalence_properties().oeq_class().is_empty());
    assert_eq!(
        node.partition_statistics(None)
            .expect("statistics")
            .num_rows,
        datafusion::common::stats::Precision::Absent
    );
    assert!(Arc::ptr_eq(
        &Arc::clone(&node)
            .with_new_children(vec![])
            .expect("no children"),
        &node
    ));
    Arc::clone(&node)
        .with_new_children(vec![Arc::clone(&plan)])
        .expect_err("deferred input must reject a physical child");
    assert!(node.execute(1, fixture.ctx.task_ctx()).is_err());
    let batches = collect(plan, fixture.ctx.task_ctx())
        .await
        .expect("healthy source");
    assert_eq!(contents(&batches), ["origin"]);
    assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.source.scans.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fallback_plans_once_and_preserves_filters_namespace_session_and_partitions() {
    for status in [429, 503] {
        let fixture = Fixture::source_first(status).await;
        let plan = fixture.plan(&format!("{QUERY} ORDER BY content")).await;
        assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 0);
        let batches = collect(plan, fixture.ctx.task_ctx())
            .await
            .expect("fallback");
        assert_eq!(contents(&batches), ["cached-a", "cached-b"]);
        assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 1);
        let observations = fixture.cache.observations.lock();
        assert_eq!(observations[0].session, fixture.ctx.session_id());
        assert_eq!(observations[0].partitions, 4);
        assert!(
            observations[0]
                .filters
                .iter()
                .any(|f| f.contains("__spice_cache_namespace"))
        );
        assert_eq!(observations[0].limit, None);
    }
}

#[tokio::test]
async fn fallback_limit_and_aggregate_keep_all_partitions_before_reducing() {
    let fixture = Fixture::source_first(503).await;
    let batches = collect(
        fixture
            .plan(&format!("{QUERY} ORDER BY content LIMIT 1"))
            .await,
        fixture.ctx.task_ctx(),
    )
    .await
    .expect("limited fallback");
    assert_eq!(contents(&batches), ["cached-a"]);
    let sql = QUERY.replace("SELECT content", "SELECT count(*) AS n");
    let batches = collect(fixture.plan(&sql).await, fixture.ctx.task_ctx())
        .await
        .expect("count fallback");
    let counts = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count");
    assert_eq!(counts.value(0), 2);
}

#[tokio::test]
async fn fallback_failure_or_missing_rows_preserves_the_origin_response() {
    for fail_scan in [false, true] {
        let fixture = Fixture::source_first(503).await;
        fixture.cache.fail.store(fail_scan, Ordering::SeqCst);
        *fixture.cache.batches.write() = vec![vec![]];
        let batches = collect(fixture.plan(QUERY).await, fixture.ctx.task_ctx())
            .await
            .expect("origin response");
        assert_eq!(contents(&batches), ["origin"]);
        assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn fallback_transport_error_and_recovery_use_the_same_plan() {
    let fixture = Fixture::source_first(200).await;
    fixture.source.fail.store(true, Ordering::SeqCst);
    let plan = fixture.plan(&format!("{QUERY} ORDER BY content")).await;
    let batches = collect(Arc::clone(&plan), fixture.ctx.task_ctx())
        .await
        .expect("transport fallback");
    assert_eq!(contents(&batches), ["cached-a", "cached-b"]);
    fixture.source.fail.store(false, Ordering::SeqCst);
    let plan = reset_plan_states(plan).expect("reset execution state");
    let batches = collect(plan, fixture.ctx.task_ctx())
        .await
        .expect("recovery");
    assert_eq!(contents(&batches), ["origin"]);
    assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 1);
}

#[derive(Debug)]
struct ContextualOptimizer(Arc<AtomicUsize>);

impl PhysicalOptimizerRule for ContextualOptimizer {
    fn optimize(
        &self,
        _plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "optimizer requires the session context".to_string(),
        ))
    }

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(context.config_options().execution.target_partitions, 3);
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(plan)
    }

    fn name(&self) -> &'static str {
        "contextual_test_optimizer"
    }
    fn schema_check(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn fallback_runs_the_callers_contextual_optimizer() {
    let fixture = Fixture::source_first(503).await;
    let optimizations = Arc::new(AtomicUsize::new(0));
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(3))
            .with_physical_optimizer_rule(Arc::new(ContextualOptimizer(Arc::clone(&optimizations))))
            .build(),
    );
    ctx.register_table("registry", Arc::clone(&fixture.table).into_table())
        .expect("register");
    let plan = ctx
        .sql(&format!("{QUERY} ORDER BY content"))
        .await
        .expect("SQL")
        .create_physical_plan()
        .await
        .expect("plan");
    assert_eq!(optimizations.load(Ordering::SeqCst), 1);
    let batches = collect(plan, ctx.task_ctx())
        .await
        .expect("optimized fallback");
    assert_eq!(contents(&batches), ["cached-a", "cached-b"]);
    assert_eq!(optimizations.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn fallback_reads_replacements_and_evictions_after_outer_planning() {
    for evicted in [false, true] {
        let fixture = Fixture::source_first(503).await;
        let plan = fixture.plan(QUERY).await;
        *fixture.cache.batches.write() = if evicted {
            vec![vec![]]
        } else {
            vec![vec![row(
                &fixture.cache.schema,
                "key=a",
                "replacement",
                200,
                "system",
            )]]
        };
        let batches = collect(plan, fixture.ctx.task_ctx())
            .await
            .expect("fallback after mutation");
        assert_eq!(
            contents(&batches),
            [if evicted { "origin" } else { "replacement" }]
        );
        assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn fallback_accepts_scan_schema_without_table_metadata() {
    for transport_error in [false, true] {
        let fixture = Fixture::source_first(503).await;
        fixture.source.fail.store(transport_error, Ordering::SeqCst);
        let plan = fixture.plan(QUERY).await;
        let expected_schema = plan.schema();
        let scan_schema = Arc::new(Schema::new(fixture.cache.schema.fields().clone()));
        *fixture.cache.batches.write() =
            vec![vec![row(&scan_schema, "key=a", "cached", 200, "system")]];
        let batches = collect(plan, fixture.ctx.task_ctx())
            .await
            .expect("fallback with matching fields");
        assert_eq!(contents(&batches), ["cached"]);
        assert_eq!(batches[0].schema(), expected_schema);
        assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn fallback_rejects_a_changed_storage_schema() {
    let fixture = Fixture::source_first(503).await;
    let plan = fixture.plan(QUERY).await;
    let mut fields: Vec<_> = fixture.cache.schema.fields().iter().cloned().collect();
    fields[2] = Arc::new(Field::new("different_content", DataType::Utf8, true));
    let changed = Arc::new(Schema::new_with_metadata(
        fields,
        fixture.cache.schema.metadata().clone(),
    ));
    *fixture.cache.batches.write() =
        vec![vec![row(&changed, "key=a", "wrong-schema", 200, "system")]];
    let batches = collect(plan, fixture.ctx.task_ctx())
        .await
        .expect("origin response");
    assert_eq!(contents(&batches), ["origin"]);
    assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fallback_keeps_null_values_and_reordered_projection() {
    let fixture = Fixture::source_first(503).await;
    let batch = row(&fixture.cache.schema, "key=a", "placeholder", 200, "system");
    let mut columns = batch.columns().to_vec();
    columns[2] = Arc::new(StringArray::from(vec![None::<&str>]));
    *fixture.cache.batches.write() = vec![vec![
        RecordBatch::try_new(batch.schema(), columns).expect("nullable row"),
    ]];
    let sql = QUERY.replace(
        "SELECT content",
        "SELECT request_query, content, request_path",
    );
    let batches = collect(fixture.plan(&sql).await, fixture.ctx.task_ctx())
        .await
        .expect("null fallback");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let batch = batches.iter().find(|b| b.num_rows() == 1).expect("one row");
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["request_query", "content", "request_path"]
    );
    assert_eq!(batch.column(1).null_count(), 1);
}

#[tokio::test]
async fn fallback_uses_each_callers_saved_namespace() {
    let fixture = Fixture::source_first(503).await;
    let namespaces = [
        CacheNamespace::Public,
        CacheNamespace::Principal(Arc::from("user:a")),
        CacheNamespace::Principal(Arc::from("user:b")),
    ];
    *fixture.cache.batches.write() = namespaces
        .iter()
        .map(|ns| {
            vec![row(
                &fixture.cache.schema,
                "key=a",
                ns.storage_id(),
                200,
                ns.storage_id(),
            )]
        })
        .collect();
    let mut plans = Vec::new();
    for ns in &namespaces {
        let context = Arc::new(
            RequestContext::builder(Protocol::Http)
                .with_cache_namespace(ns.clone())
                .build(),
        );
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_extension(context));
        ctx.register_table("registry", Arc::clone(&fixture.table).into_table())
            .expect("register private table");
        let plan = ctx
            .sql(QUERY)
            .await
            .expect("SQL")
            .create_physical_plan()
            .await
            .expect("private plan");
        plans.push((ctx, plan, ns.storage_id()));
    }
    assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 0);
    for (ctx, plan, expected) in plans {
        let batches = collect(plan, ctx.task_ctx())
            .await
            .expect("private fallback");
        assert_eq!(contents(&batches), [expected]);
    }
    assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn cache_first_modes_still_plan_the_accelerator() {
    for (ttl, swr, policy, sql) in [
        (
            Duration::from_mins(1),
            Duration::ZERO,
            StaleIfError::Enabled,
            QUERY,
        ),
        (
            Duration::ZERO,
            Duration::from_mins(1),
            StaleIfError::Enabled,
            QUERY,
        ),
        (
            Duration::ZERO,
            Duration::ZERO,
            StaleIfError::Disabled,
            QUERY,
        ),
        (
            Duration::ZERO,
            Duration::ZERO,
            StaleIfError::Enabled,
            "SELECT content FROM registry",
        ),
    ] {
        let fixture = Fixture::new(ttl, swr, policy, 200).await;
        let _plan = fixture.plan(sql).await;
        assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn expired_stale_window_does_not_serve_cached_rows() {
    let fixture = Fixture::new(
        Duration::ZERO,
        Duration::ZERO,
        StaleIfError::For(Duration::from_secs(1)),
        503,
    )
    .await;
    let batches = collect(fixture.plan(QUERY).await, fixture.ctx.task_ctx())
        .await
        .expect("origin response");
    assert_eq!(contents(&batches), ["origin"]);
    assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn coalesced_healthy_requests_never_plan_the_accelerator() {
    let fixture = Fixture::source_first(200).await;
    fixture.source.block.store(true, Ordering::SeqCst);
    let first = collect(fixture.plan(QUERY).await, fixture.ctx.task_ctx());
    let second = collect(fixture.plan(QUERY).await, fixture.ctx.task_ctx());
    tokio::pin!(first, second);
    assert!(futures::poll!(&mut first).is_pending());
    assert!(futures::poll!(&mut second).is_pending());
    fixture.source.release.notify_one();
    let (first, second) = futures::join!(first, second);
    assert_eq!(contents(&first.expect("leader")), ["origin"]);
    assert_eq!(contents(&second.expect("follower")), ["origin"]);
    assert_eq!(fixture.source.scans.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.cache.scans.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancellation_drops_the_in_progress_fallback_plan_and_claim() {
    let fixture = Fixture::source_first(503).await;
    fixture.cache.block.store(true, Ordering::SeqCst);
    let plan = fixture.plan(QUERY).await;
    let context = fixture.ctx.task_ctx();
    let query = tokio::spawn(async move { collect(plan, context).await });
    tokio::time::timeout(Duration::from_secs(5), fixture.cache.entered.notified())
        .await
        .expect("fallback started");
    assert_eq!(fixture.cache.active_scans.load(Ordering::SeqCst), 1);
    query.abort();
    assert!(query.await.expect_err("cancelled query").is_cancelled());
    assert_eq!(fixture.cache.active_scans.load(Ordering::SeqCst), 0);
    assert!(fixture.table.in_flight_revalidations.lock().is_empty());
}
