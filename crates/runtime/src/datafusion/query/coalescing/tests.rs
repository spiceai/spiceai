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
use arrow::array::{Int64Array, StringArray};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;

fn coalescer(max_batch: usize, window: Option<Duration>) -> Arc<Coalescer> {
    Arc::new(Coalescer {
        max_batch,
        window,
        families: Mutex::new(HashMap::new()),
        shutdown: CancellationToken::new(),
        finished: Notify::new(),
    })
}

fn fixture() -> SessionContext {
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("a"),
                Some("a"),
                None,
            ])) as arrow::array::ArrayRef,
        ),
        (
            "group_key",
            Arc::new(StringArray::from(vec!["x", "y", "y", "x", "z"])) as arrow::array::ArrayRef,
        ),
        (
            "score",
            Arc::new(Int64Array::from(vec![10, 20, 30, 10, 40])) as arrow::array::ArrayRef,
        ),
    ])
    .expect("fixture batch");
    ctx.register_batch("lookup", batch)
        .expect("register fixture");
    ctx
}

async fn lookup(ctx: &SessionContext, sql: &str) -> Lookup {
    let session = ctx.state();
    let plan = session.create_logical_plan(sql).await.expect("plan lookup");
    recognize(
        &plan,
        &session,
        runtime_request_context::CacheNamespace::Public,
    )
    .expect("eligible lookup")
}

fn ticket(admission: Admission) -> Ticket {
    match admission {
        Admission::Coalesced(ticket) => ticket,
        Admission::Individual(_) => panic!("expected a shared scan"),
    }
}

async fn receive(ticket: Ticket) -> (RecordBatch, Arc<dyn ExecutionPlan>) {
    tokio::time::timeout(Duration::from_secs(10), ticket)
        .await
        .expect("shared scan should finish")
        .expect("producer should reply")
        .expect("lookup should succeed")
}

async fn wait_until_idle(coalescer: &Coalescer) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if coalescer.families.lock().is_empty() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("family state should be released");
}

#[tokio::test]
async fn rejects_queries_whose_semantics_cannot_be_preserved() {
    let ctx = fixture();
    let session = ctx.state();
    for sql in [
        "SELECT * FROM lookup WHERE key = 'a'",
        "SELECT * FROM lookup WHERE key = 'a' LIMIT 2",
        "SELECT * FROM lookup WHERE key = 'a' LIMIT 1 OFFSET 1",
        "SELECT * FROM lookup WHERE key = 'a' ORDER BY score LIMIT 1",
        "SELECT score + 1 FROM lookup WHERE key = 'a' LIMIT 1",
        "SELECT * FROM lookup WHERE key = 'a' OR key = 'b' LIMIT 1",
        "SELECT * FROM lookup WHERE score > 10 LIMIT 1",
        "SELECT * FROM lookup WHERE score = 10.0 LIMIT 1",
        "SELECT * FROM lookup WHERE score = '10' LIMIT 1",
        "SELECT * FROM lookup WHERE key = NULL LIMIT 1",
        "SELECT * FROM lookup WHERE key = 'a' AND key = 'b' LIMIT 1",
    ] {
        let plan = session.create_logical_plan(sql).await.expect("valid SQL");
        assert!(
            recognize(
                &plan,
                &session,
                runtime_request_context::CacheNamespace::Public
            )
            .is_none(),
            "must bypass: {sql}"
        );
    }
}

#[tokio::test]
async fn shared_scan_matches_individual_queries_and_exact_composite_keys() {
    let ctx = fixture();
    let coalescer = coalescer(64, None);
    let semaphore = Arc::new(Semaphore::new(1));
    let held = Arc::clone(&semaphore)
        .acquire_owned()
        .await
        .expect("hold admission");
    let queries = [
        "SELECT score AS result, key, score FROM lookup WHERE key = 'a' AND group_key = 'x' LIMIT 1",
        "SELECT score AS result, key, score FROM lookup WHERE key = 'b' AND group_key = 'y' LIMIT 1",
        "SELECT score AS result, key, score FROM lookup WHERE key = 'b' AND group_key = 'x' LIMIT 1",
        "SELECT score AS result, key, score FROM lookup WHERE key = 'a' AND group_key = 'x' LIMIT 1",
        "SELECT score AS result, key, score FROM lookup WHERE key = 'missing' AND group_key = 'x' LIMIT 1",
    ];
    let mut pending = Vec::new();
    for sql in queries {
        let expected = ctx
            .sql(sql)
            .await
            .expect("individual query")
            .collect()
            .await
            .expect("individual results");
        pending.push((
            ticket(coalescer.admit(
                lookup(&ctx, sql).await,
                &ctx.state(),
                Arc::clone(&semaphore),
            )),
            expected,
        ));
    }
    drop(held);
    let mut physical: Option<Arc<dyn ExecutionPlan>> = None;
    for (ticket, expected) in pending {
        let (actual, plan) = receive(ticket).await;
        let expected = arrow::compute::concat_batches(&actual.schema(), &expected)
            .expect("combine expected batches");
        assert_eq!(actual, expected);
        if let Some(physical) = &physical {
            assert!(Arc::ptr_eq(physical, &plan));
        }
        physical = Some(plan);
    }
    wait_until_idle(&coalescer).await;
    assert_eq!(semaphore.available_permits(), 1);
}

#[tokio::test]
async fn queued_mode_uses_free_permit_without_retaining_family_state() {
    let ctx = fixture();
    let coalescer = coalescer(64, None);
    let semaphore = Arc::new(Semaphore::new(1));
    let admission = coalescer.admit(
        lookup(&ctx, "SELECT * FROM lookup WHERE key = 'a' LIMIT 1").await,
        &ctx.state(),
        Arc::clone(&semaphore),
    );
    assert!(matches!(admission, Admission::Individual(_)));
    assert!(coalescer.families.lock().is_empty());
    assert_eq!(semaphore.available_permits(), 0);
    drop(admission);
    assert_eq!(semaphore.available_permits(), 1);
}

#[tokio::test]
async fn minimum_window_keeps_collecting_while_admission_is_blocked() {
    let ctx = fixture();
    let coalescer = coalescer(64, Some(Duration::from_millis(1)));
    let semaphore = Arc::new(Semaphore::new(1));
    let held = Arc::clone(&semaphore)
        .acquire_owned()
        .await
        .expect("hold admission");
    let first = ticket(coalescer.admit(
        lookup(&ctx, "SELECT * FROM lookup WHERE key = 'a' LIMIT 1").await,
        &ctx.state(),
        Arc::clone(&semaphore),
    ));
    // Time is under test: the collection deadline must expire while admission is held.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let second = ticket(coalescer.admit(
        lookup(&ctx, "SELECT * FROM lookup WHERE key = 'b' LIMIT 1").await,
        &ctx.state(),
        Arc::clone(&semaphore),
    ));
    assert_eq!(
        coalescer
            .families
            .lock()
            .values()
            .next()
            .expect("family")
            .producers,
        1
    );
    drop(held);
    let (_, first_plan) = receive(first).await;
    let (_, second_plan) = receive(second).await;
    assert!(Arc::ptr_eq(&first_plan, &second_plan));
    wait_until_idle(&coalescer).await;
}

#[tokio::test]
async fn cancelled_batch_releases_state_without_waiting_for_admission() {
    let ctx = fixture();
    let coalescer = coalescer(64, None);
    let semaphore = Arc::new(Semaphore::new(1));
    let _held = Arc::clone(&semaphore)
        .acquire_owned()
        .await
        .expect("hold admission");
    let receive = ticket(coalescer.admit(
        lookup(&ctx, "SELECT * FROM lookup WHERE key = 'a' LIMIT 1").await,
        &ctx.state(),
        Arc::clone(&semaphore),
    ));
    drop(receive);
    wait_until_idle(&coalescer).await;
    assert_eq!(semaphore.available_permits(), 0);
}

#[tokio::test]
async fn cancelling_one_member_does_not_cancel_other_members() {
    let ctx = fixture();
    let coalescer = coalescer(64, None);
    let semaphore = Arc::new(Semaphore::new(1));
    let held = Arc::clone(&semaphore)
        .acquire_owned()
        .await
        .expect("hold admission");
    let first = ticket(coalescer.admit(
        lookup(&ctx, "SELECT * FROM lookup WHERE key = 'a' LIMIT 1").await,
        &ctx.state(),
        Arc::clone(&semaphore),
    ));
    let second = ticket(coalescer.admit(
        lookup(&ctx, "SELECT * FROM lookup WHERE key = 'b' LIMIT 1").await,
        &ctx.state(),
        Arc::clone(&semaphore),
    ));
    drop(first);
    drop(held);
    let (row, _) = receive(second).await;
    assert_eq!(row.num_rows(), 1);
    assert_eq!(
        ScalarValue::try_from_array(row.column(0), 0).expect("key"),
        ScalarValue::Utf8(Some("b".into()))
    );
    wait_until_idle(&coalescer).await;
}

#[tokio::test]
async fn full_batches_share_a_family_gate_and_release_it() {
    let ctx = fixture();
    let coalescer = coalescer(2, None);
    let semaphore = Arc::new(Semaphore::new(1));
    let held = Arc::clone(&semaphore)
        .acquire_owned()
        .await
        .expect("hold admission");
    let mut tickets = Vec::new();
    for _ in 0..5 {
        tickets.push(ticket(coalescer.admit(
            lookup(&ctx, "SELECT * FROM lookup WHERE key = 'a' LIMIT 1").await,
            &ctx.state(),
            Arc::clone(&semaphore),
        )));
    }
    assert_eq!(
        coalescer
            .families
            .lock()
            .values()
            .next()
            .expect("family")
            .producers,
        3
    );
    drop(held);
    for ticket in tickets {
        assert_eq!(receive(ticket).await.0.num_rows(), 1);
    }
    wait_until_idle(&coalescer).await;
    assert_eq!(semaphore.available_permits(), 1);
}

#[tokio::test]
async fn closed_admission_fails_members_and_releases_family() {
    let ctx = fixture();
    let coalescer = coalescer(64, None);
    let semaphore = Arc::new(Semaphore::new(0));
    let pending = ticket(coalescer.admit(
        lookup(&ctx, "SELECT * FROM lookup WHERE key = 'a' LIMIT 1").await,
        &ctx.state(),
        Arc::clone(&semaphore),
    ));
    semaphore.close();
    tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("bounded failure")
        .expect("producer reply")
        .expect_err("closed admission must fail the lookup");
    wait_until_idle(&coalescer).await;
}

#[tokio::test]
async fn producer_preserves_namespace_and_shutdown_drains_queued_work() {
    let ctx = fixture();
    let coalescer = coalescer(64, None);
    let semaphore = Arc::new(Semaphore::new(0));
    let pending = ticket(coalescer.admit(
        lookup(&ctx, "SELECT * FROM lookup WHERE key = 'a' LIMIT 1").await,
        &ctx.state(),
        semaphore,
    ));
    {
        let families = coalescer.families.lock();
        let batch = families
            .values()
            .next()
            .expect("family")
            .pending
            .as_ref()
            .expect("pending batch");
        let context = batch
            .session
            .config()
            .get_extension::<RequestContext>()
            .expect("producer context");
        assert_eq!(
            context.cache_namespace(),
            runtime_request_context::CacheNamespace::Public
        );
    }
    tokio::time::timeout(Duration::from_secs(2), coalescer.shutdown())
        .await
        .expect("shutdown drains shared work");
    pending.await.expect_err("shutdown closes queued producer");
    assert!(coalescer.families.lock().is_empty());
}

#[cfg(not(windows))]
#[test]
fn transaction_context_is_ineligible() {
    let context = RequestContext::builder(Protocol::Http).build();
    assert!(outside_transaction(&context));
    context.insert_extension(cayenne::CayenneTransaction::new());
    assert!(!outside_transaction(&context));
}

#[derive(Debug)]
struct BlockedPlanning {
    schema: SchemaRef,
    started: Arc<Notify>,
    stopped: Arc<AtomicBool>,
}

struct PlanningGuard(Arc<AtomicBool>);
impl Drop for PlanningGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for BlockedPlanning {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }
    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _guard = PlanningGuard(Arc::clone(&self.stopped));
        self.started.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
async fn cancellation_and_shutdown_drop_in_progress_physical_planning() {
    for shutdown in [false, true] {
        let ctx = fixture();
        let provider = Arc::new(BlockedPlanning {
            schema: Arc::clone(
                ctx.table("lookup")
                    .await
                    .expect("fixture table")
                    .schema()
                    .inner(),
            ),
            started: Arc::new(Notify::new()),
            stopped: Arc::new(AtomicBool::new(false)),
        });
        ctx.register_table(
            "blocked",
            Arc::clone(&provider) as Arc<dyn datafusion::catalog::TableProvider>,
        )
        .expect("register blocked provider");
        let coalescer = coalescer(64, Some(Duration::from_millis(1)));
        let semaphore = Arc::new(Semaphore::new(1));
        let pending = ticket(coalescer.admit(
            lookup(&ctx, "SELECT * FROM blocked WHERE key = 'a' LIMIT 1").await,
            &ctx.state(),
            Arc::clone(&semaphore),
        ));
        tokio::time::timeout(Duration::from_secs(2), provider.started.notified())
            .await
            .expect("physical planning started");
        if shutdown {
            tokio::time::timeout(Duration::from_secs(2), coalescer.shutdown())
                .await
                .expect("shutdown cancels planning");
            pending
                .await
                .expect("shutdown error response")
                .expect_err("shutdown cancels execution");
        } else {
            drop(pending);
            wait_until_idle(&coalescer).await;
        }
        assert!(provider.stopped.load(Ordering::Acquire));
        assert_eq!(semaphore.available_permits(), 1);
        assert!(coalescer.families.lock().is_empty());
    }
}

#[tokio::test]
async fn query_runtime_routes_http_lookups_through_a_supported_accelerated_table() {
    use crate::dataaccelerator::AcceleratorEngineRegistry;
    use crate::datafusion::{builder::DataFusionBuilder, query::QueryBuilder};
    use datafusion::common::TableReference;
    use runtime_component::dataset::acceleration::ZeroResultsAction;
    use runtime_status::RuntimeStatus;
    use runtime_table::{
        accelerated::{AcceleratedTable, refresh::Refresh},
        federated::FederatedTable,
    };

    let source = fixture()
        .table_provider("lookup")
        .await
        .expect("fixture provider");
    let mut builder = AcceleratedTable::builder(
        RuntimeStatus::new(),
        TableReference::bare("lookup"),
        Arc::new(FederatedTable::new_unchecked(Arc::clone(&source))),
        "memory".into(),
        source,
        Refresh::default(),
        tokio::runtime::Handle::current(),
    );
    builder.initial_load_complete(true);
    builder.zero_results_action(ZeroResultsAction::ReturnEmpty);
    let table = builder.build().await.expect("accelerated table");
    assert!(table.supports_shared_lookup());
    let mut df = DataFusionBuilder::new(
        RuntimeStatus::new(),
        Arc::new(AcceleratorEngineRegistry::new()),
        tokio::runtime::Handle::current(),
    )
    .max_concurrent_queries(Some(1))
    .build();
    df.query_coalescer = coalescer(64, None);
    df.ctx
        .register_table("lookup", Arc::new(table).into_table())
        .expect("register accelerated provider");
    df.accelerated_tables
        .write()
        .await
        .insert(TableReference::bare("lookup"));
    let df = Arc::new(df);
    let semaphore = df
        .query_admission_semaphore()
        .expect("query admission enabled");
    let held = Arc::clone(&semaphore)
        .acquire_owned()
        .await
        .expect("hold admission");
    let mut handles = Vec::new();
    for key in ["a", "b"] {
        let df = Arc::clone(&df);
        handles.push(tokio::spawn(async move {
            Arc::new(RequestContext::builder(Protocol::Http).build())
                .scope(async move {
                    let sql = format!("SELECT score FROM lookup WHERE key = '{key}' LIMIT 1");
                    let result = QueryBuilder::new(&sql, df)
                        .build()
                        .run()
                        .await
                        .expect("runtime query");
                    result
                        .data
                        .try_collect::<Vec<_>>()
                        .await
                        .expect("runtime output")
                })
                .await
        }));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let joined = df
                .query_coalescer
                .families
                .lock()
                .values()
                .filter_map(|state| state.pending.as_ref())
                .map(|batch| batch.members.lock().requests.len())
                .sum::<usize>();
            if joined == 2 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both HTTP lookups should join shared work");
    drop(held);
    for (handle, score) in handles.into_iter().zip([10, 20]) {
        let rows = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("bounded runtime query")
            .expect("query task");
        assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(
            ScalarValue::try_from_array(rows[0].column(0), 0).expect("score"),
            ScalarValue::Int64(Some(score))
        );
    }
    df.shutdown().await;
    assert!(df.query_coalescer.families.lock().is_empty());
}

#[tokio::test]
async fn unsupported_table_shapes_bypass_shared_lookup_admission() {
    use datafusion::common::TableReference;
    use runtime_component::dataset::acceleration::{RefreshMode, ZeroResultsAction};
    use runtime_table::{
        accelerated::{AcceleratedTable, refresh::Refresh},
        federated::FederatedTable,
    };

    let ctx = fixture();
    assert!(
        !lookup(&ctx, "SELECT * FROM lookup WHERE key = 'a' LIMIT 1")
            .await
            .supported_table
    );
    for (mode, action, supported) in [
        (RefreshMode::Full, ZeroResultsAction::ReturnEmpty, true),
        (RefreshMode::Full, ZeroResultsAction::UseSource, false),
        (RefreshMode::Caching, ZeroResultsAction::ReturnEmpty, false),
    ] {
        let source = ctx
            .table_provider("lookup")
            .await
            .expect("fixture provider");
        let mut builder = AcceleratedTable::builder(
            runtime_status::RuntimeStatus::new(),
            TableReference::bare("accelerated"),
            Arc::new(FederatedTable::new_unchecked(Arc::clone(&source))),
            "memory".into(),
            source,
            Refresh::new(mode),
            tokio::runtime::Handle::current(),
        );
        builder.initial_load_complete(true);
        builder.zero_results_action(action);
        let table = Arc::new(builder.build().await.expect("accelerated table"));
        assert_eq!(table.supports_shared_lookup(), supported);
        if supported {
            table.refresher().set_initial_load_completed(false);
            assert!(
                !table.supports_shared_lookup(),
                "an unavailable acceleration must bypass"
            );
            table.refresher().set_initial_load_completed(true);
        }
        ctx.register_table("accelerated", Arc::clone(&table).into_table())
            .expect("register acceleration");
        let recognized = lookup(&ctx, "SELECT * FROM accelerated WHERE key = 'a' LIMIT 1").await;
        assert_eq!(recognized.supported_table, supported);
        ctx.deregister_table("accelerated")
            .expect("remove acceleration");
    }
}
