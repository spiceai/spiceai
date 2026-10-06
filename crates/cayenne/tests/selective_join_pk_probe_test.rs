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

//! A selective join whose key is the probe table's primary key.
//!
//! ```sql
//! SELECT ... FROM parent p INNER JOIN child c ON p.id = c.parent_id
//! WHERE c.sel = <one value>
//! ```
//!
//! Both sides are large, the filter is on the CHILD and matches one row, and the
//! join key on the PARENT side is its primary key. The whole query is worth one
//! row, and the parent is the wide table — so the property that matters is that
//! the parent is never materialised to answer it.
//!
//! These tests pin the plan-level behaviour. `benches/selective_join_pk_probe.rs`
//! measures what it costs.

#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

test_with_backends!(selective_join_probe_emits_one_row_impl);
test_with_backends!(selective_join_pushes_a_dynamic_filter_to_the_probe_impl);
test_with_backends!(mixed_tier_join_keeps_the_inlined_side_in_the_metastore_impl);

const PARENT_ROWS: i64 = 20_000;

fn parent_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        // Padding, not decoration: byte-range fan-out only happens once a file
        // clears DataFusion's `repartition_file_min_size` (10 MB). A narrow
        // table yields one group either way and the gap under test cannot
        // appear at all.
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn child_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("child_id", DataType::Int64, false),
        Field::new("parent_id", DataType::Int64, false),
        Field::new("sel", DataType::Utf8, false),
    ]))
}

async fn make_table(
    fixture: &common::TestFixture,
    name: &str,
    schema: Arc<Schema>,
    pk: Vec<String>,
) -> CayenneTableProvider {
    let ctx = SessionContext::new();
    CayenneTableProvider::create_table(
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
        CreateTableOptions {
            table_name: name.to_string(),
            schema,
            primary_key: pk,
            on_conflict: None,
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await
    .expect("create table")
}

/// Register both tables in one context and return the `EXPLAIN ANALYZE` text
/// alongside the rows the query actually produced.
async fn explain_and_run(
    parent: &Arc<CayenneTableProvider>,
    child: &Arc<CayenneTableProvider>,
    sql: &str,
) -> (String, usize) {
    let ctx = SessionContext::new();
    ctx.register_table("p", Arc::clone(parent) as Arc<dyn TableProvider>)
        .expect("register p");
    ctx.register_table("c", Arc::clone(child) as Arc<dyn TableProvider>)
        .expect("register c");

    let rows: usize = ctx
        .sql(sql)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect")
        .iter()
        .map(RecordBatch::num_rows)
        .sum();

    let explain = ctx
        .sql(&format!("EXPLAIN ANALYZE {sql}"))
        .await
        .expect("explain")
        .collect()
        .await
        .expect("explain run");
    let text = arrow::util::pretty::pretty_format_batches(&explain)
        .expect("format")
        .to_string();
    (text, rows)
}

/// Every `output_rows=N` the plan reports, in plan order.
fn output_rows(plan: &str) -> Vec<usize> {
    plan.split("output_rows=")
        .skip(1)
        .filter_map(|tail| {
            tail.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse::<usize>()
                .ok()
        })
        .collect()
}

async fn seed(
    fixture: &common::TestFixture,
) -> (Arc<CayenneTableProvider>, Arc<CayenneTableProvider>) {
    let parent = Arc::new(make_table(fixture, "p", parent_schema(), vec!["id".to_string()]).await);
    let child =
        Arc::new(make_table(fixture, "c", child_schema(), vec!["child_id".to_string()]).await);

    let ids: Vec<i64> = (0..PARENT_ROWS).collect();
    let values: Vec<i64> = ids.iter().map(|i| i * 7).collect();
    // ~600 B/row over 20k rows ≈ 12 MB, comfortably past the split threshold.
    let payloads: Vec<String> = ids.iter().map(|i| format!("{i:0600}")).collect();
    common::insert_batch(
        &parent,
        RecordBatch::try_new(
            parent_schema(),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(Int64Array::from(values)),
                Arc::new(StringArray::from(payloads)),
            ],
        )
        .expect("parent batch"),
    )
    .await
    .expect("insert parent");

    let sels: Vec<String> = ids.iter().map(|i| format!("sel_{i}")).collect();
    common::insert_batch(
        &child,
        RecordBatch::try_new(
            child_schema(),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(sels)),
            ],
        )
        .expect("child batch"),
    )
    .await
    .expect("insert child");

    (parent, child)
}

/// The load-bearing property: the one-row build side's key reaches the probe
/// scan, so the WIDE parent yields one row instead of being materialised.
///
/// If this regresses, the plan still returns the right answer — it just reads
/// the whole parent to do it, which is invisible in a correctness test and
/// ruinous at serving QPS. That is exactly why it is asserted on the plan.
async fn selective_join_probe_emits_one_row_impl(fixture: common::TestFixture) -> TestResult {
    let (parent, child) = seed(&fixture).await;

    let key = PARENT_ROWS / 2;
    let sql = format!(
        "SELECT p.id, p.value FROM p INNER JOIN c ON p.id = c.parent_id WHERE c.sel = 'sel_{key}'"
    );
    let (plan, rows) = explain_and_run(&parent, &child, &sql).await;

    assert_eq!(rows, 1, "the join must resolve to exactly one row");

    let seen = output_rows(&plan);
    assert!(
        !seen.is_empty(),
        "EXPLAIN ANALYZE reported no output_rows; plan:\n{plan}"
    );
    // No operator may emit more than a handful of rows. A probe that scanned the
    // parent would report ~PARENT_ROWS here.
    let worst = seen.iter().copied().max().unwrap_or(usize::MAX);
    assert!(
        worst <= 16,
        "an operator emitted {worst} rows for a one-row join — the selective key \
         did not reach the probe scan, so the parent was materialised. \
         Expected every operator at or near 1. Plan:\n{plan}"
    );
    Ok(())
}

/// The dynamic filter must actually be present on the probe side.
///
/// This is the mechanism behind the test above, asserted separately so a failure
/// says WHICH half broke: `output_rows` regressing means the filter stopped
/// pruning, this one failing means it stopped being generated at all.
async fn selective_join_pushes_a_dynamic_filter_to_the_probe_impl(
    fixture: common::TestFixture,
) -> TestResult {
    let (parent, child) = seed(&fixture).await;

    let key = PARENT_ROWS / 3;
    let sql = format!(
        "SELECT p.id, p.value FROM p INNER JOIN c ON p.id = c.parent_id WHERE c.sel = 'sel_{key}'"
    );
    let (plan, _) = explain_and_run(&parent, &child, &sql).await;

    assert!(
        plan.contains("DynamicFilter") || plan.contains("dynamic_filter"),
        "no dynamic filter reached the probe scan — sideways information passing \
         is off, and every such join now reads its whole probe table. Plan:\n{plan}"
    );
    Ok(())
}

/// The same row fetched two ways must cost the probe the same number of file
/// groups.
///
/// A literal `WHERE id = K` reaches `scan()` as a literal, so
/// `is_pk_selective_scan` suppresses byte-range fan-out. A join key does not —
/// it arrives later as a dynamic filter, and `pk_column_equals_literal` requires
/// a literal — so the suppression cannot engage on the join path.
///
/// At THIS scale both paths yield one group and the test passes, because
/// `DataFusion` only byte-range splits when each resulting group would still clear
/// `repartition_file_min_size` (10 MB): a ~12 MB parent is never split into 16.
/// The divergence therefore needs a parent in the hundreds of MB, which belongs
/// in `benches/selective_join_pk_probe.rs` rather than here — it was measured at
/// 17 groups versus 2 on a 415 MiB parent.
///
/// Keeping it as a parity assertion is still worth it: it catches a regression
/// that made the join path fan out even at small scale, and it will start
/// failing if the split threshold or the suppression heuristic moves — at which
/// point this comment is the record of why the number is what it is.
///
/// Sqlite only: this is a plan-shape assertion, and the metastore backend does
/// not participate in file-group fan-out.
#[tokio::test]
async fn join_driven_lookup_costs_the_same_as_a_literal_lookup() -> TestResult {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite).await?;
    let (parent, child) = seed(&fixture).await;
    let key = PARENT_ROWS / 2;

    let (literal_plan, literal_rows) = explain_and_run(
        &parent,
        &child,
        &format!("SELECT p.id, p.value FROM p WHERE p.id = {key}"),
    )
    .await;
    let (join_plan, join_rows) = explain_and_run(
        &parent,
        &child,
        &format!(
            "SELECT p.id, p.value FROM p INNER JOIN c ON p.id = c.parent_id \
             WHERE c.sel = 'sel_{key}'"
        ),
    )
    .await;
    assert_eq!(literal_rows, 1);
    assert_eq!(join_rows, 1);

    // The join plan contains TWO scans (parent and child). Compare only the
    // parent's, identified by its projection — counting both would measure the
    // child's scan, not the fan-out under test.
    //
    // `None` rather than a fallback count when the plan cannot be read: a
    // defaulted number would let the comparison below pass against something
    // the plan never said, so a change to the EXPLAIN format would silently
    // stop this test from measuring anything.
    let probe_groups = |plan: &str| -> Option<usize> {
        plan.split("DataSourceExec")
            .find(|frag| frag.contains("projection=[id, value"))
            .and_then(|frag| {
                frag.split_once("file_groups={")
                    .and_then(|(_, tail)| tail.split_once(" group"))
                    .and_then(|(n, _)| n.trim().parse::<usize>().ok())
            })
    };
    let (Some(literal_groups), Some(join_groups)) =
        (probe_groups(&literal_plan), probe_groups(&join_plan))
    else {
        panic!(
            "could not read the probe scan's file-group count from one of the plans — \
             it is located by its `projection=[id, value` and its `file_groups={{N group`, \
             so either the scan is gone or EXPLAIN no longer renders it that way.\
             \n\nliteral:\n{literal_plan}\n\njoin:\n{join_plan}"
        )
    };

    assert!(
        join_groups <= literal_groups,
        "the join path opened {join_groups} file groups against the literal path's \
         {literal_groups} for the SAME single row — the PK-selective fan-out \
         suppression did not engage for a dynamic filter.\n\nliteral:\n{literal_plan}\n\njoin:\n{join_plan}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Mixed-tier joins: one side small enough to inline, the other file-backed.
// ---------------------------------------------------------------------------

/// Under `DEFAULT_INLINE_MAX_ROWS` (1024), matching the production dimensions
/// that inline (`integration_points` 13 rows, `module_specifications` 117).
const DIM_ROWS: i64 = 64;

fn dim_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("dim_id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]))
}

/// A join across the tier boundary: a fact table on Vortex against a dimension
/// small enough to live in the metastore.
///
/// This is the customer's real shape, and the case where inlining changes the
/// PLAN rather than only the storage: an inlined table is a single-partition
/// in-memory source, so when it meets a repartitionable file scan
/// `EnforceDistribution` resolves the mismatch by coalescing the other side.
/// That is the mechanism behind the +11% measured on TPC-DS q64.
///
/// What is asserted here is the precondition the bench depends on — that the
/// dimension really is in the inline tier and the join is still correct across
/// it. The COST is measured by `bench_mixed_tier_dim_join`, because a
/// partitioning penalty is a timing property and asserting a coalesce count
/// would pin today's plan shape and fight every future optimiser change.
async fn mixed_tier_join_keeps_the_inlined_side_in_the_metastore_impl(
    fixture: common::TestFixture,
) -> TestResult {
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("dim_id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let fact = Arc::new(
        make_table(
            &fixture,
            "f",
            Arc::clone(&fact_schema),
            vec!["id".to_string()],
        )
        .await,
    );
    let dim = Arc::new(make_table(&fixture, "d", dim_schema(), vec!["dim_id".to_string()]).await);

    let ids: Vec<i64> = (0..PARENT_ROWS).collect();
    let dim_ids: Vec<i64> = ids.iter().map(|i| i % DIM_ROWS).collect();
    let values: Vec<i64> = ids.iter().map(|i| i * 7).collect();
    common::insert_batch(
        &fact,
        RecordBatch::try_new(
            fact_schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(dim_ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .expect("fact batch"),
    )
    .await
    .expect("insert fact");

    let d_ids: Vec<i64> = (0..DIM_ROWS).collect();
    let labels: Vec<String> = d_ids.iter().map(|i| format!("label_{i}")).collect();
    common::insert_batch(
        &dim,
        RecordBatch::try_new(
            dim_schema(),
            vec![
                Arc::new(Int64Array::from(d_ids)),
                Arc::new(StringArray::from(labels)),
            ],
        )
        .expect("dim batch"),
    )
    .await
    .expect("insert dim");

    // The precondition: a 64-row write clears the admission caps, so the
    // dimension is a metastore row rather than a Vortex file. If this ever stops
    // holding, the mixed-tier bench is silently comparing two file-backed
    // tables and measuring nothing.
    let dim_id = fixture.catalog.get_table("d").await?.table_id;
    let inlined_rows = fixture.catalog.get_inlined_data_count(&dim_id).await?;
    assert_eq!(
        inlined_rows, DIM_ROWS,
        "the {DIM_ROWS}-row dimension should be admitted to the inline tier \
         (get_inlined_data_count sums record_count, so this counts ROWS)"
    );

    // And the join across the tier boundary is correct: every fact row finds its
    // dimension, and the group count is the dimension's cardinality.
    let ctx = SessionContext::new();
    ctx.register_table("f", Arc::clone(&fact) as Arc<dyn TableProvider>)?;
    ctx.register_table("d", Arc::clone(&dim) as Arc<dyn TableProvider>)?;
    let rows = ctx
        .sql("SELECT d.label, count(*) AS n FROM f INNER JOIN d ON f.dim_id = d.dim_id GROUP BY d.label")
        .await?
        .collect()
        .await?;
    let groups: usize = rows.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        groups,
        usize::try_from(DIM_ROWS).expect("DIM_ROWS fits usize"),
        "a join across the inline/file tier boundary must not drop or duplicate groups"
    );
    let total: i64 = rows
        .iter()
        .flat_map(|b| {
            b.column_by_name("n")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .map(|a| a.values().to_vec())
                .unwrap_or_default()
        })
        .sum();
    assert_eq!(
        total, PARENT_ROWS,
        "every fact row must join to exactly one dimension row"
    );
    Ok(())
}
