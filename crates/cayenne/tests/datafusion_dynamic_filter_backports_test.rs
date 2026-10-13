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

//! Guards for the upstream `DataFusion` filter-pushdown fixes the `spiceai-54` fork
//! carries as backports (see `docs/dev/fork_patches.md`). Each test fails if its
//! backport is lost when the fork is re-cut.
//!
//! Every bug here returns wrong rows only when a scan applies a pushed filter row
//! by row. A Parquet scan does that under `pushdown_filters`, which the
//! configuration every Spice session starts from turns on, and a Cayenne scan
//! does it for every predicate it accepts. So each query runs against Parquet
//! files in a session built from that configuration, mirroring the upstream
//! regression test, and, where the same shape is reachable, against a Cayenne
//! table.
//!
//! The runtime is multi-threaded so the scans run as parallel partitions, as they
//! do in `spiced`.

#![allow(clippy::expect_used)]

use crate::common;

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::array_value_to_string;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use common::TestFixture;
use datafusion::common::config::ConfigNonZeroUsize;
use datafusion::datasource::TableProvider;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use runtime_datafusion::session_config::get_df_default_config;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Above any file count a test writes, so compaction never merges the files.
const NO_AUTOMATIC_COMPACTION: usize = 1_000_000;

/// The answer to a query that selects no row.
const NO_ROWS: &[&str] = &[];

/// A session built from the configuration every Spice session starts from.
fn spice_session(configure: impl FnOnce(&mut SessionConfig)) -> SessionContext {
    let mut config = get_df_default_config();
    configure(&mut config);
    SessionContext::new_with_config(config)
}

/// Writes the rows of `select` to `path` as Parquet, with at most
/// `max_row_group_size` rows per row group when one is given.
async fn write_parquet(
    ctx: &SessionContext,
    select: &str,
    path: &Path,
    max_row_group_size: Option<usize>,
) -> TestResult<()> {
    let options = max_row_group_size
        .map(|rows| format!(" OPTIONS ('format.max_row_group_size' '{rows}')"))
        .unwrap_or_default();
    ctx.sql(&format!(
        "COPY ({select}) TO '{}' STORED AS PARQUET{options}",
        path.display()
    ))
    .await?
    .collect()
    .await?;
    Ok(())
}

async fn register_parquet(ctx: &SessionContext, name: &str, path: &Path) -> TestResult<()> {
    ctx.register_parquet(name, &path.to_string_lossy(), ParquetReadOptions::default())
        .await?;
    Ok(())
}

/// Every row rendered as `a,b,…` with NULL spelled out, sorted, so the comparison
/// ignores row order.
async fn sorted_rows(ctx: &SessionContext, sql: &str) -> TestResult<Vec<String>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let mut rendered = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                rendered.push(if column.is_null(row) {
                    "NULL".to_string()
                } else {
                    array_value_to_string(column, row)?
                });
            }
            rows.push(rendered.join(","));
        }
    }
    rows.sort_unstable();
    Ok(rows)
}

/// Runs each query and collects every answer that differs from its expected rows.
async fn wrong_answers(ctx: &SessionContext, cases: &[(&str, &[&str])]) -> TestResult<Vec<String>> {
    let mut wrong = Vec::new();
    for (sql, expected) in cases {
        let rows = sorted_rows(ctx, sql).await?;
        if rows != *expected {
            wrong.push(format!("{sql}: got {rows:?}, expected {expected:?}"));
        }
    }
    Ok(wrong)
}

/// A PK-less Cayenne table whose every insert lands in its own Vortex file.
async fn file_backed_table(
    fixture: &TestFixture,
    ctx: &SessionContext,
    name: &str,
    schema: &Arc<Schema>,
) -> TestResult<Arc<CayenneTableProvider>> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: Arc::clone(schema),
        primary_key: Vec::new(),
        on_conflict: None,
        base_path: fixture
            .data_path
            .join("cayenne")
            .join(name)
            .to_string_lossy()
            .to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            compaction_trigger_files: NO_AUTOMATIC_COMPACTION,
            compaction_trigger_protected_snapshots: NO_AUTOMATIC_COMPACTION,
            compaction_background_interval_ms: 0,
            ..VortexConfig::default()
        },
    };
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok(table)
}

fn nullable_i64(name: &str) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]))
}

async fn insert_i64(
    table: &CayenneTableProvider,
    schema: &Arc<Schema>,
    values: Vec<Option<i64>>,
) -> TestResult<()> {
    common::insert_batch(
        table,
        RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(Int64Array::from(values))])?,
    )
    .await?;
    Ok(())
}

fn tagged(source: &str, answers: Vec<String>) -> impl Iterator<Item = String> + '_ {
    answers
        .into_iter()
        .map(move |answer| format!("{source}: {answer}"))
}

/// apache/datafusion#24816, fixed by #24817. An aggregate's dynamic filter was built
/// from its plain-column `MIN`/`MAX` aggregates alone, so it pruned rows that could
/// not improve those but held the answer to `MIN(c + 1)`.
async fn an_aggregate_dynamic_filter_keeps_rows_an_expression_aggregate_needs_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    const SQL: &str = "SELECT MIN(a), MAX(a), MAX(b), MIN(c + 1) FROM agg_dyn_mixed";
    const EXPECTED: &[&str] = &["1,8,12,71"];

    // Each file's first row group fixes MIN(a), MAX(a) and MAX(b); its second
    // holds the minimum c and cannot improve the other three. Two rows per batch
    // let the filter tighten between the two, whichever file is read first.
    let parquet = spice_session(|config| {
        config.options_mut().execution.batch_size =
            ConfigNonZeroUsize::try_new(2).expect("non-zero batch size");
    });
    let dir = fixture.data_path.join("agg_dyn_mixed");
    std::fs::create_dir_all(&dir)?;
    for file in ["file_0.parquet", "file_1.parquet"] {
        write_parquet(
            &parquet,
            "SELECT * FROM (VALUES (1, 12, 100), (8, 4, 100), (1, 6, 70), (8, 12, 110)) AS v(a, b, c)",
            &dir.join(file),
            Some(2),
        )
        .await?;
    }
    register_parquet(&parquet, "agg_dyn_mixed", &dir).await?;
    let mut wrong = tagged(
        "Parquet",
        wrong_answers(&parquet, &[(SQL, EXPECTED)]).await?,
    )
    .collect::<Vec<_>>();

    // The same answer through Cayenne: 63 files fix the three plain aggregates and
    // the last holds the only row with the minimum c.
    let cayenne = spice_session(|_| {});
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Int64, false),
    ]));
    let table = file_backed_table(&fixture, &cayenne, "agg_dyn_mixed", &schema).await?;
    let batch = |rows: &[(i64, i64, i64)]| -> TestResult<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
            ],
        )?)
    };
    for _ in 0..63 {
        common::insert_batch(table.as_ref(), batch(&[(1, 6, 90), (8, 12, 110)])?).await?;
    }
    common::insert_batch(table.as_ref(), batch(&[(1, 12, 100), (8, 4, 70)])?).await?;
    wrong.extend(tagged(
        "Cayenne",
        wrong_answers(&cayenne, &[(SQL, EXPECTED)]).await?,
    ));

    assert!(
        wrong.is_empty(),
        "MIN(c + 1) was computed from a subset of the rows: {wrong:#?}"
    );
    Ok(())
}

/// apache/datafusion#25244, fixed by #25259. A join dynamic filter pushed below an
/// operator whose output holds two columns of one name was resolved by name, so a
/// filter on `b.id` landed on `a.id` and the join lost its only match.
async fn a_join_dynamic_filter_maps_same_named_columns_by_position_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let ctx = spice_session(|config| config.options_mut().optimizer.join_reordering = false);
    let a = fixture.data_path.join("issue_25244_a.parquet");
    let b = fixture.data_path.join("issue_25244_b.parquet");
    write_parquet(&ctx, "SELECT 'a1' AS id, 'x1' AS ty", &a, None).await?;
    write_parquet(&ctx, "SELECT 'x1' AS id", &b, None).await?;
    register_parquet(&ctx, "issue_25244_a", &a).await?;
    register_parquet(&ctx, "issue_25244_b", &b).await?;

    let wrong = wrong_answers(
        &ctx,
        &[
            // Nested joins, with a repartition between them.
            (
                "SELECT s.id AS sid, a.id AS aid, b.id AS bid, c.id AS cid FROM issue_25244_b s JOIN ((issue_25244_a a LEFT JOIN issue_25244_b b ON a.ty = b.id) LEFT JOIN issue_25244_b c ON b.id = c.id) ON s.id = b.id",
                &["x1,a1,x1,x1"],
            ),
            // A filter with an embedded projection.
            (
                "SELECT s.id AS sid, a.id AS aid, b.id AS bid FROM issue_25244_b s JOIN (SELECT a.id, b.id FROM issue_25244_a a LEFT JOIN issue_25244_b b ON a.ty = b.id WHERE (a.ty || '!') IS DISTINCT FROM b.id) ON s.id = b.id",
                &["x1,a1,x1"],
            ),
            // A projection with two outputs named `id`.
            (
                "SELECT s.id AS sid, a.id AS aid, b.id AS bid, tag FROM issue_25244_a s JOIN (SELECT a.id, b.id, a.ty || '!' AS tag FROM issue_25244_a a JOIN issue_25244_b b ON a.ty = b.id) ON s.id = a.id",
                &["a1,a1,x1,x1!"],
            ),
            // An aggregate grouping on two same-named columns.
            (
                "SELECT s.id AS sid, a.id AS aid, b.id AS bid FROM issue_25244_b s JOIN (SELECT a.id, b.id FROM issue_25244_a a LEFT JOIN issue_25244_b b ON a.ty = b.id GROUP BY a.id, b.id) ON s.id = b.id",
                &["x1,a1,x1"],
            ),
        ],
    )
    .await?;
    assert!(
        wrong.is_empty(),
        "a join lost its match to a filter on the wrong same-named column: {wrong:#?}"
    );
    Ok(())
}

/// apache/datafusion#25296, fixed by #25259. The same name-based remapping moved a
/// `TopK` filter on `p.amount` onto `o.amount`, so orders read after the first
/// match were pruned and the query returned the wrong row.
async fn a_topk_dynamic_filter_maps_same_named_columns_by_position_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    // One partition, and one row per batch and per row group, so the TopK
    // tightens its filter before the later orders are read.
    let ctx = spice_session(|config| {
        config.options_mut().execution.target_partitions = 1;
        config.options_mut().execution.batch_size =
            ConfigNonZeroUsize::try_new(1).expect("non-zero batch size");
    });
    let orders = fixture.data_path.join("issue_25296_orders.parquet");
    let payments = fixture.data_path.join("issue_25296_payments.parquet");
    let customers = fixture.data_path.join("issue_25296_customers.parquet");
    write_parquet(
        &ctx,
        "SELECT * FROM (VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500), (6, 600)) AS v(id, amount)",
        &orders,
        Some(1),
    )
    .await?;
    write_parquet(
        &ctx,
        "SELECT * FROM (VALUES (1, 30), (2, 20), (3, 10)) AS v(id, amount)",
        &payments,
        None,
    )
    .await?;
    write_parquet(
        &ctx,
        "SELECT * FROM (VALUES (1), (2), (3)) AS v(id)",
        &customers,
        None,
    )
    .await?;
    register_parquet(&ctx, "issue_25296_orders", &orders).await?;
    register_parquet(&ctx, "issue_25296_payments", &payments).await?;
    register_parquet(&ctx, "issue_25296_customers", &customers).await?;

    let wrong = wrong_answers(
        &ctx,
        &[(
            "SELECT o.amount, p.amount FROM issue_25296_orders o JOIN issue_25296_payments p ON o.id = p.id JOIN issue_25296_customers c ON p.id = c.id ORDER BY p.amount LIMIT 1",
            &["300,10"],
        )],
    )
    .await?;
    assert!(
        wrong.is_empty(),
        "ORDER BY p.amount LIMIT 1 returned a row pruned by a filter on o.amount: {wrong:#?}"
    );
    Ok(())
}

/// apache/datafusion#23103, fixed by #23104. `x NOT IN (subquery)` needs the
/// subquery's NULLs, because a single NULL makes every non-matching row unknown.
/// The filter a hash join pushed into the subquery's scan dropped them.
async fn not_in_keeps_the_subquerys_nulls_impl(fixture: TestFixture) -> TestResult<()> {
    const SQL: &str = "SELECT id FROM asa_outer WHERE id NOT IN (SELECT eid FROM asa_inner)";

    let parquet = spice_session(|_| {});
    let outer = fixture.data_path.join("asa_outer.parquet");
    let inner = fixture.data_path.join("asa_inner.parquet");
    write_parquet(
        &parquet,
        "SELECT * FROM (VALUES (1), (2), (3)) AS v(id)",
        &outer,
        None,
    )
    .await?;
    write_parquet(
        &parquet,
        "SELECT * FROM (VALUES (2), (NULL)) AS v(eid)",
        &inner,
        None,
    )
    .await?;
    register_parquet(&parquet, "asa_outer", &outer).await?;
    register_parquet(&parquet, "asa_inner", &inner).await?;
    let mut wrong =
        tagged("Parquet", wrong_answers(&parquet, &[(SQL, NO_ROWS)]).await?).collect::<Vec<_>>();

    let cayenne = spice_session(|_| {});
    let outer_schema = nullable_i64("id");
    let inner_schema = nullable_i64("eid");
    let outer = file_backed_table(&fixture, &cayenne, "asa_outer", &outer_schema).await?;
    let inner = file_backed_table(&fixture, &cayenne, "asa_inner", &inner_schema).await?;
    insert_i64(&outer, &outer_schema, vec![Some(1), Some(2), Some(3)]).await?;
    insert_i64(&inner, &inner_schema, vec![Some(2), None]).await?;
    wrong.extend(tagged(
        "Cayenne",
        wrong_answers(&cayenne, &[(SQL, NO_ROWS)]).await?,
    ));

    assert!(
        wrong.is_empty(),
        "NOT IN returned rows although the subquery holds a NULL: {wrong:#?}"
    );
    Ok(())
}

/// apache/datafusion#23126, fixed by #23173. `x NOT IN (subquery)` plans as a
/// null-aware anti join with `x` on the build side. The filter built from `x`
/// emptied the subquery's scan, and the join read an empty subquery as a truly
/// empty one, so it kept the NULL `x`: `NULL NOT IN (2, 3)` is unknown, not true.
async fn not_in_drops_a_null_value_impl(fixture: TestFixture) -> TestResult<()> {
    const SQL: &str = "SELECT id FROM ao WHERE id NOT IN (SELECT eid FROM i_disj)";
    const EXPECTED: &[&str] = &["5"];

    let parquet = spice_session(|_| {});
    let outer = fixture.data_path.join("ao.parquet");
    let inner = fixture.data_path.join("i_disj.parquet");
    write_parquet(
        &parquet,
        "SELECT * FROM (VALUES (5), (NULL)) AS v(id)",
        &outer,
        None,
    )
    .await?;
    write_parquet(
        &parquet,
        "SELECT * FROM (VALUES (2), (3)) AS v(eid)",
        &inner,
        None,
    )
    .await?;
    register_parquet(&parquet, "ao", &outer).await?;
    register_parquet(&parquet, "i_disj", &inner).await?;
    let mut wrong = tagged(
        "Parquet",
        wrong_answers(&parquet, &[(SQL, EXPECTED)]).await?,
    )
    .collect::<Vec<_>>();

    let cayenne = spice_session(|_| {});
    let outer_schema = nullable_i64("id");
    let inner_schema = nullable_i64("eid");
    let outer = file_backed_table(&fixture, &cayenne, "ao", &outer_schema).await?;
    let inner = file_backed_table(&fixture, &cayenne, "i_disj", &inner_schema).await?;
    insert_i64(&outer, &outer_schema, vec![Some(5), None]).await?;
    insert_i64(&inner, &inner_schema, vec![Some(2), Some(3)]).await?;
    wrong.extend(tagged(
        "Cayenne",
        wrong_answers(&cayenne, &[(SQL, EXPECTED)]).await?,
    ));

    assert!(
        wrong.is_empty(),
        "NOT IN kept a NULL value it must drop: {wrong:#?}"
    );
    Ok(())
}

/// apache/datafusion#22964, fixed by #22965 and refined by #23106. A null-equal
/// join (`IS NOT DISTINCT FROM`, and the join `INTERSECT` plans as) matches NULL to
/// NULL, but the filter built from its build side is NULL, so not true, for a
/// NULL probe key, and the scan dropped the row that should have matched.
async fn a_null_equal_join_keeps_its_null_match_impl(fixture: TestFixture) -> TestResult<()> {
    const CASES: &[(&str, &[&str])] = &[
        (
            "SELECT nej_build.id, nej_probe.id FROM nej_build JOIN nej_probe ON nej_build.id IS NOT DISTINCT FROM nej_probe.id",
            &["11,11", "NULL,NULL"],
        ),
        (
            "SELECT id FROM nej_build INTERSECT SELECT id FROM nej_probe",
            &["11", "NULL"],
        ),
    ];

    let parquet = spice_session(|_| {});
    let probe = fixture.data_path.join("nej_probe.parquet");
    let build = fixture.data_path.join("nej_build.parquet");
    write_parquet(
        &parquet,
        "SELECT * FROM (VALUES (11), (22), (NULL)) AS v(id)",
        &probe,
        None,
    )
    .await?;
    write_parquet(
        &parquet,
        "SELECT * FROM (VALUES (11), (NULL)) AS v(id)",
        &build,
        None,
    )
    .await?;
    register_parquet(&parquet, "nej_probe", &probe).await?;
    register_parquet(&parquet, "nej_build", &build).await?;
    let mut wrong = tagged("Parquet", wrong_answers(&parquet, CASES).await?).collect::<Vec<_>>();

    let cayenne = spice_session(|_| {});
    let schema = nullable_i64("id");
    let probe = file_backed_table(&fixture, &cayenne, "nej_probe", &schema).await?;
    let build = file_backed_table(&fixture, &cayenne, "nej_build", &schema).await?;
    insert_i64(&probe, &schema, vec![Some(11), Some(22), None]).await?;
    insert_i64(&build, &schema, vec![Some(11), None]).await?;
    wrong.extend(tagged("Cayenne", wrong_answers(&cayenne, CASES).await?));

    assert!(
        wrong.is_empty(),
        "a null-equal join lost its NULL = NULL match: {wrong:#?}"
    );
    Ok(())
}

/// apache/datafusion#22925, fixed by #22926. An aggregate reordered the parent
/// filters it was handed, so the optimizer took the pushed-down grouping-column
/// predicate for the aggregate-output one and removed the latter. Reached when a
/// single filter above the aggregate holds both, which the logical optimizer
/// would otherwise have split.
async fn a_filter_over_an_aggregate_keeps_its_aggregate_output_predicate_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let ctx = spice_session(|config| config.options_mut().optimizer.max_passes = 0);
    let path = fixture.data_path.join("agg_filter_pushdown.parquet");
    write_parquet(
        &ctx,
        "SELECT * FROM (VALUES ('x', 'foo'), ('y', 'bar')) AS v(a, b)",
        &path,
        None,
    )
    .await?;
    register_parquet(&ctx, "agg_filter_pushdown", &path).await?;

    let wrong = wrong_answers(
        &ctx,
        &[(
            "SELECT count(*) FROM (SELECT a, b, count(b) AS cnt FROM agg_filter_pushdown GROUP BY a, b) q WHERE cnt = 2 AND b = 'foo'",
            &["0"],
        )],
    )
    .await?;
    assert!(
        wrong.is_empty(),
        "the filter on the aggregate's output was dropped: {wrong:#?}"
    );
    Ok(())
}

/// apache/datafusion#24002, fixed by #24045. A filter above an anti join was taken
/// as handled when only the join's non-output side accepted it, although filtering
/// that side adds anti-join output rather than removing it. Reached, like the
/// aggregate case, when the filter survives to the physical plan.
async fn a_filter_over_an_anti_join_stays_above_it_impl(fixture: TestFixture) -> TestResult<()> {
    let ctx = spice_session(|config| {
        config.options_mut().optimizer.max_passes = 0;
        config.options_mut().optimizer.join_reordering = false;
    });
    ctx.sql(
        "CREATE TABLE join_left(id INT, data VARCHAR) AS VALUES (1, 'left1'), (2, 'left2'), (3, 'left3'), (4, 'left4'), (5, 'left5')",
    )
    .await?
    .collect()
    .await?;
    let right = fixture.data_path.join("join_right.parquet");
    write_parquet(
        &ctx,
        "SELECT * FROM (VALUES (1, 'right1'), (3, 'right3'), (5, 'right5')) AS v(id, info)",
        &right,
        None,
    )
    .await?;
    register_parquet(&ctx, "right_parquet", &right).await?;

    let wrong = wrong_answers(
        &ctx,
        &[
            (
                "SELECT count(*) FROM join_left l LEFT ANTI JOIN right_parquet r USING (id) WHERE false",
                &["0"],
            ),
            (
                "SELECT count(*) FROM right_parquet r RIGHT ANTI JOIN join_left l USING (id) WHERE false",
                &["0"],
            ),
        ],
    )
    .await?;
    assert!(
        wrong.is_empty(),
        "a filter above an anti join was dropped after reaching only its non-output side: {wrong:#?}"
    );
    Ok(())
}

fn run_on_multi_thread<F, Fut>(body: F) -> Result<(), String>
where
    F: FnOnce(TestFixture) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = TestResult<()>>,
{
    let outcome = std::thread::Builder::new()
        .stack_size(common::TEST_STACK_SIZE)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(common::TEST_STACK_SIZE)
                .enable_all()
                .build()
                .map_err(|e| format!("failed to build tokio runtime: {e}"))?
                .block_on(async move {
                    let fixture = TestFixture::new(common::BackendType::Sqlite)
                        .await
                        .map_err(|e| e.to_string())?;
                    body(fixture).await.map_err(|e| e.to_string())
                })
        })
        .map_err(|e| format!("failed to spawn test thread: {e}"))?
        .join();
    match outcome {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

#[test]
fn an_aggregate_dynamic_filter_keeps_rows_an_expression_aggregate_needs() -> Result<(), String> {
    run_on_multi_thread(an_aggregate_dynamic_filter_keeps_rows_an_expression_aggregate_needs_impl)
}

#[test]
fn a_join_dynamic_filter_maps_same_named_columns_by_position() -> Result<(), String> {
    run_on_multi_thread(a_join_dynamic_filter_maps_same_named_columns_by_position_impl)
}

#[test]
fn a_topk_dynamic_filter_maps_same_named_columns_by_position() -> Result<(), String> {
    run_on_multi_thread(a_topk_dynamic_filter_maps_same_named_columns_by_position_impl)
}

#[test]
fn not_in_keeps_the_subquerys_nulls() -> Result<(), String> {
    run_on_multi_thread(not_in_keeps_the_subquerys_nulls_impl)
}

#[test]
fn not_in_drops_a_null_value() -> Result<(), String> {
    run_on_multi_thread(not_in_drops_a_null_value_impl)
}

#[test]
fn a_null_equal_join_keeps_its_null_match() -> Result<(), String> {
    run_on_multi_thread(a_null_equal_join_keeps_its_null_match_impl)
}

#[test]
fn a_filter_over_an_aggregate_keeps_its_aggregate_output_predicate() -> Result<(), String> {
    run_on_multi_thread(a_filter_over_an_aggregate_keeps_its_aggregate_output_predicate_impl)
}

#[test]
fn a_filter_over_an_anti_join_stays_above_it() -> Result<(), String> {
    run_on_multi_thread(a_filter_over_an_anti_join_stays_above_it_impl)
}
