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

//! An unfiltered maintained aggregate view describes the whole table, so the
//! rewriter may only substitute it for a Cayenne scan whose output *is* the whole
//! table.
//!
//! Every query here runs twice over the same table: once in a session with
//! `CayenneMaintainedAggregateRewriter` appended after `DataFusion`'s default
//! physical rules (the runtime's order), and once without it. The rows must match.
//! The unfiltered query must also be served by the view, so a run where the rewriter
//! never fires cannot pass.
//!
//! The table takes each shape a CDC table goes through:
//! - every row still in the in-memory tier (a table right after its initial load);
//! - rows in a file plus newer rows in memory;
//! - rows only in files.
//!
//! A query's `WHERE` can disappear from above the scan in two ways. A predicate
//! the Vortex source accepts becomes a file-source predicate. A predicate
//! `FilterPushdown` hands to a branch that cannot evaluate it becomes a
//! `FilterExec` inside the scan: the in-memory branch Cayenne already wraps in the
//! query's filter, or a union branch that rejects the predicate. A `LIMIT` in a
//! subquery becomes a fetch inside the scan. None of these leave anything above
//! the scan for the rewriter to see.

#![allow(clippy::expect_used)]

use crate::common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::array_value_to_string;
use cayenne::maintained_aggregate::{
    MaintainedAggregateExpr, MaintainedAggregateFunction, MaintainedAggregateSpec,
};
use cayenne::metadata::{CdcDurability, CreateTableOptions, VortexConfig};
use cayenne::optimizer_rules::CayenneMaintainedAggregateRewriter;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog, SlotAdvancer};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// First CDC batch: two groups, each with a row below and a row above 100.
const INITIAL_ROWS: &[(i64, i64, i64)] = &[(1, 10, 5), (2, 10, 200), (3, 20, 50), (4, 20, 300)];
/// Second CDC batch: two new keys and an update of key 4.
const LATER_ROWS: &[(i64, i64, i64)] = &[(5, 10, 1000), (6, 20, 7), (4, 20, 301)];

/// Predicates that must each change the answer. The first is one the Vortex source
/// accepts; the others it cannot evaluate, so on a union they stay as per-branch
/// `FilterExec`s.
const PREDICATES: &[&str] = &[
    "v >= 100",
    "(v % 1000) >= 100",
    "abs(v) >= 100",
    "v % 7 = 4",
    "v IN (5, 300, 1000)",
];

#[derive(Clone, Copy, Debug)]
enum Shape {
    /// Every row is still in the in-memory CDC tier.
    RamOnly,
    /// The first batch was checkpointed into a file; the second is in memory.
    FileAndRam,
    /// Every batch was checkpointed into files.
    FileOnly,
}

fn table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn unfiltered_sum_v_by_k() -> MaintainedAggregateSpec {
    MaintainedAggregateSpec {
        group_by: vec!["k".to_string()],
        aggregates: vec![MaintainedAggregateExpr {
            function: MaintainedAggregateFunction::Sum,
            column: Some("v".to_string()),
        }],
        filter: None,
    }
}

/// `DataFusion`'s default physical rules followed by the maintained-aggregate rewrite.
fn rewriter_ctx() -> SessionContext {
    rewriter_ctx_with(SessionConfig::new())
}

fn rewriter_ctx_with(config: SessionConfig) -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(CayenneMaintainedAggregateRewriter::new()))
        .build();
    SessionContext::new_with_state(state)
}

/// The same rules without the rewrite: the answer the rewrite must reproduce.
fn reference_ctx() -> SessionContext {
    reference_ctx_with(SessionConfig::new())
}

fn reference_ctx_with(config: SessionConfig) -> SessionContext {
    SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build(),
    )
}

struct NoopSlotAdvancer;

#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

async fn cdc_apply(table: &Arc<CayenneTableProvider>, rows: &[(i64, i64, i64)]) -> TestResult<()> {
    let batch = RecordBatch::try_new(
        table_schema(),
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
    )?;
    let ctx = SessionContext::new();
    let write = table
        .write_cdc_append_stream(common::single_batch_stream(batch), &ctx.task_ctx())
        .await?;
    if write.has_pending_finalize() {
        write.finish().await?;
    }
    Ok(())
}

/// An upsert table on the in-memory CDC tier, with every background trigger off so
/// the test alone decides when rows move into files, and the unfiltered view.
async fn table_in_shape(
    fixture: &TestFixture,
    name: &str,
    shape: Shape,
) -> TestResult<Arc<CayenneTableProvider>> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: table_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.join(name).to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cdc_durability: CdcDurability::Memory,
            cdc_mem_tier_checkpoint_interval_ms: 0,
            cdc_mem_tier_max_age_ms: 0,
            cdc_mem_tier_seal_age_ms: 0,
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
    };
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .with_maintained_aggregates(vec![unfiltered_sum_v_by_k()])
            .create(options)
            .await?,
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));

    cdc_apply(&table, INITIAL_ROWS).await?;
    match shape {
        Shape::RamOnly => {}
        Shape::FileAndRam => {
            let flushed = table.checkpoint_mem_tier().await?;
            assert!(
                flushed > 0,
                "precondition: the first batch must move into a file"
            );
            cdc_apply(&table, LATER_ROWS).await?;
        }
        Shape::FileOnly => {
            cdc_apply(&table, LATER_ROWS).await?;
            let flushed = table.checkpoint_mem_tier().await?;
            assert!(
                flushed > 0,
                "precondition: every batch must move into files"
            );
        }
    }
    Ok(table)
}

async fn served_by_view(ctx: &SessionContext, sql: &str) -> TestResult<bool> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    Ok(displayable(plan.as_ref())
        .indent(true)
        .to_string()
        .contains("MaintainedAggregateExec"))
}

/// Every row rendered as `a,b,…`, sorted, so the comparison ignores row order.
async fn sorted_rows(ctx: &SessionContext, sql: &str) -> TestResult<Vec<String>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let mut rendered = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                rendered.push(array_value_to_string(column, row)?);
            }
            rows.push(rendered.join(","));
        }
    }
    rows.sort_unstable();
    Ok(rows)
}

async fn unfiltered_view_answers_only_whole_table_queries(
    fixture: TestFixture,
    shape: Shape,
) -> TestResult<()> {
    let name = format!("maintained_serve_soundness_{shape:?}").to_lowercase();
    let table = table_in_shape(&fixture, &name, shape).await?;
    let rewriter = rewriter_ctx();
    let reference = reference_ctx();
    rewriter.register_table(&name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    reference.register_table(&name, Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // The rewrite must still fire where it is sound, or the comparisons below
    // would pass without ever exercising it.
    let unfiltered = format!("SELECT k, SUM(v) FROM {name} GROUP BY k");
    assert!(
        served_by_view(&rewriter, &unfiltered).await?,
        "{shape:?}: the unfiltered query must be answered by the maintained view"
    );
    assert_eq!(
        sorted_rows(&rewriter, &unfiltered).await?,
        sorted_rows(&reference, &unfiltered).await?,
        "{shape:?}: the maintained view must equal a full recompute"
    );

    let mut wrong = Vec::new();
    for predicate in PREDICATES {
        let sql = format!("SELECT k, SUM(v) FROM {name} WHERE {predicate} GROUP BY k");
        let got = sorted_rows(&rewriter, &sql).await?;
        let expected = sorted_rows(&reference, &sql).await?;
        if got != expected {
            wrong.push(format!(
                "WHERE {predicate}: got {got:?}, expected {expected:?} (served_by_view={})",
                served_by_view(&rewriter, &sql).await?
            ));
        }
    }

    // A projection that computes a new `v`: the view holds sums of the stored `v`.
    // With one partition nothing separates the projection from the scan, so it is
    // pushed into the scan and the scan itself outputs the computed `v`.
    let one_partition = || SessionConfig::new().with_target_partitions(1);
    let one_partition_rewriter = rewriter_ctx_with(one_partition());
    let one_partition_reference = reference_ctx_with(one_partition());
    one_partition_rewriter.register_table(&name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    one_partition_reference.register_table(&name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    for (rewriter, reference, partitions) in [
        (&rewriter, &reference, "default partitions"),
        (
            &one_partition_rewriter,
            &one_partition_reference,
            "one partition",
        ),
    ] {
        for computed in [
            format!("SELECT k, SUM(v) FROM (SELECT k, v + 1 AS v FROM {name}) q GROUP BY k"),
            format!("SELECT k, SUM(v) FROM (SELECT k, v * 2 AS v FROM {name}) q GROUP BY k"),
            format!("SELECT k, SUM(v) FROM (SELECT k, id AS v FROM {name}) q GROUP BY k"),
            format!("SELECT v AS k, SUM(k) FROM (SELECT v AS k, k AS v FROM {name}) q GROUP BY v"),
        ] {
            let got = sorted_rows(rewriter, &computed).await?;
            let expected = sorted_rows(reference, &computed).await?;
            if got != expected {
                wrong.push(format!(
                    "{partitions}: {computed}: got {got:?}, expected {expected:?} (served_by_view={})",
                    served_by_view(rewriter, &computed).await?
                ));
            }
        }
    }

    // `LIMIT` without `ORDER BY` may pick any two rows, so the two sessions need
    // not agree on the answer; what must never happen is the whole-table view
    // answering for two rows.
    let limited = format!("SELECT k, SUM(v) FROM (SELECT * FROM {name} LIMIT 2) GROUP BY k");
    if served_by_view(&rewriter, &limited).await? {
        wrong.push(format!(
            "{limited}: answered by the whole-table view, rows {:?}",
            sorted_rows(&rewriter, &limited).await?
        ));
    }

    assert!(
        wrong.is_empty(),
        "{shape:?}: the unfiltered maintained view answered queries it does not describe: {wrong:#?}"
    );
    Ok(())
}

async fn ram_only_impl(fixture: TestFixture) -> TestResult<()> {
    unfiltered_view_answers_only_whole_table_queries(fixture, Shape::RamOnly).await
}

async fn file_and_ram_impl(fixture: TestFixture) -> TestResult<()> {
    unfiltered_view_answers_only_whole_table_queries(fixture, Shape::FileAndRam).await
}

async fn file_only_impl(fixture: TestFixture) -> TestResult<()> {
    unfiltered_view_answers_only_whole_table_queries(fixture, Shape::FileOnly).await
}

test_with_backends!(ram_only_impl);
test_with_backends!(file_and_ram_impl);
test_with_backends!(file_only_impl);
