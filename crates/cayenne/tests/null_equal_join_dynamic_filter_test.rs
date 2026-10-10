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

//! Null-equal joins and null-aware `NOT IN` must keep NULL keys when Cayenne
//! `mode:file` pushes a hash-join min/max dynamic filter into the Vortex scan.
//!
//! A min/max bound is NULL, not true, for a NULL probe key, so a scan that
//! evaluated only the bound would drop the NULL group from
//! `IS NOT DISTINCT FROM` and the NULL that makes `NOT IN` unknown. When the
//! join needs those keys, the hash join adds `key IS NULL` as a disjunct of the
//! filter it publishes. Memory mode has no Vortex file scan and is the control.

#![expect(clippy::expect_used, reason = "tests use expect for assertion context")]

use crate::common;

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use runtime_datafusion::session_config::get_df_default_config;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Above any file count this test writes, so compaction never merges the files.
const NO_AUTOMATIC_COMPACTION: usize = 1_000_000;

const NULL_EQUAL_DISTINCT_JOIN: &str = "\
    SELECT count(*) FROM (\
        SELECT DISTINCT region FROM orders\
    ) a JOIN (\
        SELECT DISTINCT region FROM orders\
    ) b ON a.region IS NOT DISTINCT FROM b.region";

const NULL_EQUAL_CTE_JOIN: &str = "\
    WITH s AS (SELECT DISTINCT region FROM orders) \
    SELECT count(*) FROM s a JOIN s b ON a.region IS NOT DISTINCT FROM b.region";

const NOT_IN_NULL_SUBQUERY: &str = "\
    SELECT count(*) FROM orders \
    WHERE region NOT IN (SELECT region FROM orders WHERE qty = 20)";

const EQUALS_DISTINCT_JOIN: &str = "\
    SELECT count(*) FROM (\
        SELECT DISTINCT region FROM orders\
    ) a JOIN (\
        SELECT DISTINCT region FROM orders\
    ) b ON a.region = b.region";

fn spice_session() -> SessionContext {
    SessionContext::new_with_config(get_df_default_config())
}

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("qty", DataType::Int64, true),
    ]))
}

/// Five distinct regions including NULL: `ap`, `eu`, `na`, `us`, and NULL.
/// Only the NULL region has `qty = 20`, so a `NOT IN` subquery that dropped
/// that NULL would become empty and count every outer row instead of 0.
fn orders_batch(schema: &Arc<Schema>) -> TestResult<RecordBatch> {
    let mut ids = Vec::new();
    let mut regions: Vec<Option<&str>> = Vec::new();
    let mut qtys: Vec<Option<i64>> = Vec::new();
    let named = ["ap", "eu", "na", "us"];
    let mut id = 1_i64;
    for region in named {
        for qty in [1_i64, 5] {
            ids.push(id);
            regions.push(Some(region));
            qtys.push(Some(qty));
            id += 1;
        }
    }
    for qty in [7_i64, 20] {
        ids.push(id);
        regions.push(None);
        qtys.push(Some(qty));
        id += 1;
    }
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(regions)),
            Arc::new(Int64Array::from(qtys)),
        ],
    )?)
}

async fn file_backed_orders(
    fixture: &TestFixture,
    ctx: &SessionContext,
) -> TestResult<Arc<CayenneTableProvider>> {
    let schema = orders_schema();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let options = CreateTableOptions {
        table_name: "orders".to_string(),
        schema: Arc::clone(&schema),
        primary_key: Vec::new(),
        on_conflict: None,
        base_path: fixture
            .data_path
            .join("cayenne")
            .join("orders_file")
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
    common::insert_batch(table.as_ref(), orders_batch(&schema)?).await?;
    ctx.register_table("orders", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok(table)
}

async fn memory_orders(
    fixture: &TestFixture,
    ctx: &SessionContext,
) -> TestResult<Arc<CayenneTableProvider>> {
    let schema = orders_schema();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let options = CreateTableOptions {
        table_name: "orders".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture
            .data_path
            .join("cayenne")
            .join("orders_memory")
            .to_string_lossy()
            .to_string(),
        partition_column: None,
        vortex_config: common::lookup_index::memory_mode_config(),
    };
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    common::insert_batch(table.as_ref(), orders_batch(&schema)?).await?;
    ctx.register_table("orders", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok(table)
}

async fn count_star(ctx: &SessionContext, sql: &str) -> TestResult<i64> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut total = 0_i64;
    for batch in &batches {
        let column = batch.column(0);
        let values = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count(*) is Int64");
        for row in 0..batch.num_rows() {
            if values.is_null(row) {
                return Err(format!("count(*) returned NULL for {sql}").into());
            }
            total += values.value(row);
        }
    }
    Ok(total)
}

async fn physical_plan_text(ctx: &SessionContext, sql: &str) -> TestResult<String> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    Ok(displayable(plan.as_ref()).indent(true).to_string())
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

async fn file_mode_keeps_null_keys_under_dynamic_filters_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let ctx = spice_session();
    file_backed_orders(&fixture, &ctx).await?;

    let null_equal = count_star(&ctx, NULL_EQUAL_DISTINCT_JOIN).await?;
    assert_eq!(
        null_equal,
        5,
        "null-equal DISTINCT self-join must count the NULL group; plan:\n{}",
        physical_plan_text(&ctx, NULL_EQUAL_DISTINCT_JOIN).await?
    );

    let cte = count_star(&ctx, NULL_EQUAL_CTE_JOIN).await?;
    assert_eq!(
        cte, 5,
        "CTE form of the null-equal self-join must also count 5"
    );

    let equals = count_star(&ctx, EQUALS_DISTINCT_JOIN).await?;
    assert_eq!(
        equals,
        4,
        "plain `=` must still exclude the NULL group; plan:\n{}",
        physical_plan_text(&ctx, EQUALS_DISTINCT_JOIN).await?
    );

    let not_in = count_star(&ctx, NOT_IN_NULL_SUBQUERY).await?;
    assert_eq!(
        not_in,
        0,
        "NOT IN must be unknown when the subquery holds NULL; plan:\n{}",
        physical_plan_text(&ctx, NOT_IN_NULL_SUBQUERY).await?
    );

    let plan = physical_plan_text(&ctx, NULL_EQUAL_DISTINCT_JOIN).await?;
    assert!(
        plan.contains("HashJoinExec")
            && (plan.contains("NullsEqual") || plan.contains("NullEquals")),
        "precondition: the query must plan as a null-equal hash join:\n{plan}"
    );
    assert!(
        plan.lines()
            .any(|line| line.contains("file_type=vortex") && line.contains("DynamicFilter")),
        "precondition: the hash join's dynamic filter must reach the Vortex scan:\n{plan}"
    );

    Ok(())
}

async fn memory_mode_null_equal_join_counts_five_impl(fixture: TestFixture) -> TestResult<()> {
    let ctx = spice_session();
    memory_orders(&fixture, &ctx).await?;

    let null_equal = count_star(&ctx, NULL_EQUAL_DISTINCT_JOIN).await?;
    assert_eq!(
        null_equal, 5,
        "Cayenne mode:memory must keep the NULL group on a null-equal DISTINCT self-join"
    );

    let not_in = count_star(&ctx, NOT_IN_NULL_SUBQUERY).await?;
    assert_eq!(
        not_in, 0,
        "Cayenne mode:memory NOT IN must be unknown when the subquery holds NULL"
    );

    Ok(())
}

#[test]
fn file_mode_keeps_null_keys_under_dynamic_filters() -> Result<(), String> {
    run_on_multi_thread(file_mode_keeps_null_keys_under_dynamic_filters_impl)
}

#[test]
fn memory_mode_null_equal_join_counts_five() -> Result<(), String> {
    run_on_multi_thread(memory_mode_null_equal_join_counts_five_impl)
}
