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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::window::PlainAggregateWindowExpr;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::windows::WindowAggExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_federation::schema_cast::SchemaCastScanExec;
use datafusion_federation::sql::{SQLFederationPlanner, VirtualExecutionPlan};
use datafusion_federation::{FederatedPlanNode, FederatedQueryType, FederationPlanner};
use datafusion_table_providers::sql::db_connection_pool::JoinPushDown;
use datafusion_table_providers::sql::db_connection_pool::adbcpool::AdbcConnectionPoolBuilder;
use futures::TryStreamExt;
use runtime::Runtime;
use runtime::component::dataset::builder::DatasetBuilder;
use runtime_datafusion::extension::bytes_processed::BytesProcessedExec;
use serde::Deserialize;

use super::{AdbcTableFactoryWithPolicy, StubDatabase, dialect_for_driver};
use crate::{ConnectorComponent, ConnectorParams, PARAMETERS, Parameters, resolve_pool_identity};

#[derive(Deserialize)]
struct Fixture {
    alias: String,
    project: String,
    dataset: String,
    table: String,
    schema: Schema,
}

impl Fixture {
    fn remote_table(&self) -> TableReference {
        TableReference::full(
            self.project.clone(),
            self.dataset.clone(),
            self.table.clone(),
        )
    }
}

fn corpus() -> Vec<(usize, &'static str)> {
    let mut sections = include_str!("fixtures/bigquery/queries.sql").split("-- query ");
    assert_eq!(sections.next(), Some(""));
    let queries: Vec<_> = sections
        .map(|section| {
            let (index, sql) = section.split_once('\n').expect("query header and SQL");
            (index.parse::<usize>().expect("numeric query index"), sql)
        })
        .collect();
    assert_eq!(
        queries.len(),
        271,
        "every corpus statement must be accounted for"
    );
    for (expected, (actual, sql)) in queries.iter().enumerate() {
        assert_eq!(*actual, expected, "unique, consecutive corpus indices");
        assert!(!sql.trim().is_empty(), "query {actual} must not be empty");
    }
    queries
}

async fn register_fixture(
    runtime: &Arc<Runtime>,
    fixture: &Fixture,
    schemas: &Arc<HashMap<TableReference, Schema>>,
    statement_attempts: &Arc<AtomicUsize>,
) {
    let remote_table = fixture.remote_table();
    let dataset = DatasetBuilder::try_new(format!("adbc:{remote_table}"), &fixture.alias)
        .expect("fixture dataset")
        .with_app(Arc::new(app::AppBuilder::new("bigquery-corpus").build()))
        .with_runtime(Arc::clone(runtime))
        .build()
        .expect("build fixture dataset");
    let uri = format!("bigquery:///{}", fixture.project);
    let params = ConnectorParams {
        parameters: Parameters::new(
            vec![
                ("driver".into(), "bigquery".into()),
                ("uri".into(), uri.clone().into()),
            ],
            "adbc",
            PARAMETERS,
        ),
        component: ConnectorComponent::from(&dataset),
        unsupported_type_action: None,
        io_runtime: tokio::runtime::Handle::current(),
    };
    let identity =
        resolve_pool_identity("bigquery", &uri, &params).expect("connector pool identity");
    let pool = AdbcConnectionPoolBuilder::new(StubDatabase {
        schemas: Arc::clone(schemas),
        statement_attempts: Arc::clone(statement_attempts),
    })
    .with_max_size(Some(1))
    .with_min_idle(Some(0))
    .with_join_push_down(JoinPushDown::AllowedFor(identity.join_context))
    .build()
    .expect("offline ADBC pool");
    let provider = AdbcTableFactoryWithPolicy::new(Arc::new(pool), true, "bigquery")
        .table_provider(remote_table, dialect_for_driver("bigquery"))
        .await
        .expect("production ADBC factory with saved metadata");
    let field_metadata = fixture
        .schema
        .fields()
        .iter()
        .filter_map(|field| {
            let source_type = field.metadata().get("BIGQUERY:type")?;
            let source_type = match source_type.as_str() {
                "INTEGER" => "INT64",
                "FLOAT" => "FLOAT64",
                "BOOLEAN" => "BOOL",
                other => other,
            };
            Some((
                field.name().clone(),
                [(
                    data_components::SOURCE_TYPE_METADATA_KEY.into(),
                    source_type.into(),
                )]
                .into(),
            ))
        })
        .collect();
    let provider = data_components::metadata_enriched_table_provider(
        provider,
        HashMap::<String, String>::new(),
        field_metadata,
    );
    let previous = runtime
        .datafusion()
        .ctx
        .register_table(TableReference::bare(fixture.alias.clone()), provider)
        .expect("register corpus alias");
    assert!(
        previous.is_none(),
        "duplicate fixture alias {}",
        fixture.alias
    );
}

fn remote_nodes(plan: &dyn ExecutionPlan) -> Vec<&VirtualExecutionPlan> {
    let mut nodes = Vec::new();
    if let Some(remote) = plan.downcast_ref::<VirtualExecutionPlan>() {
        nodes.push(remote);
    }
    for child in plan.children() {
        nodes.extend(remote_nodes(child.as_ref()));
    }
    nodes
}

fn transport(node: &dyn ExecutionPlan, child: &dyn ExecutionPlan) -> bool {
    (node.is::<SchemaCastScanExec>()
        || node.is::<CoalescePartitionsExec>()
        || node.is::<CooperativeExec>()
        || node.is::<BytesProcessedExec>())
        && node.fetch().is_none()
        && node.schema() == child.schema()
}

/// A single remote leaf must do all semantic work. Schema adaptation is allowed
/// only with an identical schema; partition coalescing must not impose a limit.
fn fully_federated(plan: &dyn ExecutionPlan) -> std::result::Result<(), String> {
    let remotes = remote_nodes(plan).len();
    if remotes != 1 {
        return Err(format!("expected one remote node, got {remotes}"));
    }
    let mut node = plan;
    while !node.is::<VirtualExecutionPlan>() {
        let children = node.children();
        let [child] = children.as_slice() else {
            return Err(format!("local work: {}", node.name()));
        };
        let child = *child;
        if !transport(node, child.as_ref()) {
            return Err(format!("local work: {}", node.name()));
        }
        node = child.as_ref();
    }
    if !node.children().is_empty() {
        return Err("remote node must be a leaf in an execution plan".into());
    }
    Ok(())
}

/// Query 241 computes median and approximate-percentile windows locally because
/// the `BigQuery` policy rejects those window forms. The source joins, JSON
/// extraction and aggregation must still form a single remote subtree.
fn percentile_window_partial(plan: &dyn ExecutionPlan) -> std::result::Result<(), String> {
    if remote_nodes(plan).len() != 1 {
        return Err("percentile case requires exactly one remote subtree".into());
    }
    let mut node = plan;
    let mut windows = 0;
    while !node.is::<VirtualExecutionPlan>() {
        let children = node.children();
        let [child] = children.as_slice() else {
            return Err(format!("unexpected percentile branch: {}", node.name()));
        };
        let child = *child;
        if let Some(window) = node.downcast_ref::<WindowAggExec>() {
            let mut functions = window
                .window_expr()
                .iter()
                .map(|expr| {
                    expr.as_any()
                        .downcast_ref::<PlainAggregateWindowExpr>()
                        .map(|expr| expr.get_aggregate_expr().fun().name())
                        .ok_or_else(|| "unexpected local window expression".to_string())
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            functions.sort_unstable();
            if functions != ["approx_percentile_cont", "median"] {
                return Err(format!("unexpected local window functions: {functions:?}"));
            }
            windows += 1;
        } else if !(transport(node, child.as_ref())
            || (node.is::<ProjectionExec>() && windows == 0)
            || node.is::<SortExec>()
            || node.is::<SortPreservingMergeExec>()
            || node.is::<RepartitionExec>())
        {
            return Err(format!("unexpected local percentile work: {}", node.name()));
        }
        node = child.as_ref();
    }
    if windows != 1 {
        return Err(format!(
            "expected one local percentile window node, got {windows}"
        ));
    }
    Ok(())
}

/// Normal physical planning can defer unparsing until execution. Use the same
/// federation planner's EXPLAIN path to force final SQL generation and propagate
/// dialect errors. The ADBC executor has no remote EXPLAIN implementation.
async fn check_remote_sql(remote: &VirtualExecutionPlan, ctx: &SessionContext) -> Result<()> {
    let planner = Arc::new(SQLFederationPlanner::new(Arc::clone(remote.executor())));
    let node = FederatedPlanNode::new_with_query_type(
        remote.plan().clone(),
        Arc::clone(&planner) as Arc<dyn FederationPlanner>,
        Some(FederatedQueryType::Explain),
    );
    planner.plan_federation(&node, &ctx.state()).await?;
    Ok(())
}

async fn check_query(
    ctx: &SessionContext,
    index: usize,
    sql: &str,
) -> std::result::Result<(), String> {
    let df = ctx.sql(sql).await.map_err(|error| error.to_string())?;
    // VALUES examples, a typed NULL, and two literal UNION ALL reports have no
    // source tables. They must stay local, and may not silently acquire a scan.
    let table_free = matches!(index, 0..=4 | 80 | 229 | 230);
    if table_free {
        df.logical_plan()
            .apply(|plan| {
                assert!(
                    !matches!(plan, LogicalPlan::TableScan(_)),
                    "table-free query {index}"
                );
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("inspect table-free logical plan");
    }
    let plan = df
        .create_physical_plan()
        .await
        .map_err(|error| error.to_string())?;
    if table_free {
        if !remote_nodes(plan.as_ref()).is_empty() {
            return Err("table-free query has a remote node".into());
        }
    } else {
        let verdict = if index == 241 {
            percentile_window_partial(plan.as_ref())
        } else {
            fully_federated(plan.as_ref())
        };
        verdict
            .map_err(|error| format!("{error}\n{}", displayable(plan.as_ref()).indent(false)))?;
        for remote in remote_nodes(plan.as_ref()) {
            check_remote_sql(remote, ctx)
                .await
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

async fn run_corpus() {
    let started = Instant::now();
    let runtime = Arc::new(Runtime::builder().build().await);
    let fixtures: Vec<Fixture> =
        serde_json::from_str(include_str!("fixtures/bigquery/schemas.json"))
            .expect("saved BigQuery schemas");
    assert_eq!(fixtures.len(), 72);
    let statement_attempts = Arc::new(AtomicUsize::new(0));
    let schemas: Arc<HashMap<_, _>> = Arc::new(
        fixtures
            .iter()
            .map(|f| (f.remote_table(), f.schema.clone()))
            .collect(),
    );
    assert_eq!(
        schemas.len(),
        70,
        "aliases must preserve shared physical tables"
    );
    for fixture in &fixtures {
        assert_eq!(
            schemas.get(&fixture.remote_table()),
            Some(&fixture.schema),
            "shared aliases must have identical remote schemas"
        );
        register_fixture(&runtime, fixture, &schemas, &statement_attempts).await;
    }
    let df = runtime.datafusion();
    let mut failures = Vec::new();
    for (index, sql) in corpus() {
        if let Err(error) = check_query(&df.ctx, index, sql).await {
            failures.push(format!("query {index:03}: {error}"));
        }
    }
    assert_eq!(
        statement_attempts.load(Ordering::SeqCst),
        0,
        "planning attempted remote execution"
    );
    eprintln!(
        "BigQuery corpus: 271 checked (262 full, 1 percentile partial, 8 table-free), {} failures, {:.2}s; no remote statements",
        failures.len(),
        started.elapsed().as_secs_f64()
    );
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));

    // These guards exercise the validator with real plans. One remote node by
    // itself cannot prove full federation when a projection remains above it.
    let control_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let controls = [
        Fixture {
            alias: "control_a".into(),
            project: "control-one".into(),
            dataset: "a".into(),
            table: "items".into(),
            schema: control_schema.clone(),
        },
        Fixture {
            alias: "control_b".into(),
            project: "control-two".into(),
            dataset: "b".into(),
            table: "items".into(),
            schema: control_schema,
        },
    ];
    let schemas: Arc<HashMap<_, _>> = Arc::new(
        controls
            .iter()
            .map(|f| (f.remote_table(), f.schema.clone()))
            .collect(),
    );
    for fixture in &controls {
        register_fixture(&runtime, fixture, &schemas, &statement_attempts).await;
    }
    let remote = df
        .ctx
        .sql("SELECT id FROM control_a")
        .await
        .expect("control SQL")
        .create_physical_plan()
        .await
        .expect("control plan");
    fully_federated(remote.as_ref()).expect("single-table control fully federates");
    let residual = ProjectionExec::try_new(
        vec![(
            Arc::new(Literal::new(ScalarValue::Int64(Some(7))))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            "constant".to_string(),
        )],
        Arc::clone(&remote),
    )
    .expect("local projection mutation");
    assert_eq!(remote_nodes(&residual).len(), 1);
    assert!(
        fully_federated(&residual)
            .expect_err("reject semantic local work")
            .contains("ProjectionExec")
    );
    let split = df
        .ctx
        .sql("SELECT a.id FROM control_a a JOIN control_b b ON a.id = b.id")
        .await
        .expect("split SQL")
        .create_physical_plan()
        .await
        .expect("split plan");
    assert_eq!(
        remote_nodes(split.as_ref()).len(),
        2,
        "projects have distinct production join identities"
    );
    fully_federated(split.as_ref()).expect_err("reject split federation");
    assert_eq!(statement_attempts.load(Ordering::SeqCst), 0);

    // Changing only source metadata must exercise the connector's capability
    // policy: the direct JSON null-check rewrite is safe for STRING, not JSON.
    for source_type in ["STRING", "JSON"] {
        let fixture = Fixture {
            alias: format!("json_{}", source_type.to_lowercase()),
            project: "control-one".into(),
            dataset: "json".into(),
            table: source_type.into(),
            schema: Schema::new(vec![
                Field::new("payload", DataType::Utf8, true)
                    .with_metadata([("BIGQUERY:type".into(), source_type.into())].into()),
            ]),
        };
        let schemas = Arc::new([(fixture.remote_table(), fixture.schema.clone())].into());
        register_fixture(&runtime, &fixture, &schemas, &statement_attempts).await;
        let plan = df
            .ctx
            .sql(&format!(
                "SELECT payload FROM {} WHERE json_get(payload, 'key') IS NULL",
                fixture.alias
            ))
            .await
            .expect("metadata control SQL")
            .create_physical_plan()
            .await
            .expect("metadata control plan");
        assert_eq!(remote_nodes(plan.as_ref()).len(), 1);
        if source_type == "STRING" {
            fully_federated(plan.as_ref()).expect("STRING null check must federate");
        } else {
            fully_federated(plan.as_ref()).expect_err("native JSON null check must stay local");
        }
    }
    assert_eq!(statement_attempts.load(Ordering::SeqCst), 0);

    // A mistakenly executed plan must error; the fixture must never masquerade
    // as a successfully executed empty table.
    let error = remote
        .execute(0, df.ctx.task_ctx())
        .expect("lazy ADBC stream")
        .try_next()
        .await
        .expect_err("offline execution must fail");
    assert!(
        error
            .to_string()
            .contains("offline fixture statement execution")
    );
    assert_eq!(statement_attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn bigquery_federation_corpus() {
    // Deep CTEs need room for debug-build planner and SQL-AST clone frames.
    std::thread::Builder::new()
        .name("bigquery-corpus".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("planning runtime")
                .block_on(run_corpus());
        })
        .expect("planning thread")
        .join()
        .expect("BigQuery corpus regression");
}
