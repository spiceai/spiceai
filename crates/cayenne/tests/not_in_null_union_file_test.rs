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

//! `x NOT IN (SELECT x FROM t … UNION ALL SELECT NULL)` over a file-backed
//! Cayenne table selects no row, so its `count(*)` is 0. On a file acceleration
//! `spiced` instead failed with "Column 'count(*)' is declared as non-nullable but
//! contains null values", while the same table in memory and federated answered
//! 0; three rows did not reproduce it.
//!
//! The table is spread over several Vortex files and scanned on a multi-threaded
//! runtime in a session built from the configuration every Spice session starts
//! from, with the join dynamic filter on (the default) and off, so a failure
//! points at the scan and the pushdown rather than at a Cayenne optimizer rule;
//! `test/spicescript/known-failing/cayenne/cayenne-not-in-null-subquery.yaml`
//! covers the full runtime, with its optimizer rules on and off.

#![allow(clippy::expect_used)]

use crate::common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::array_value_to_string;
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
/// Files the table is spread over, and rows in each.
const FILES: i64 = 8;
const ROWS_PER_FILE: i64 = 12_500;

/// `qty` for row `id`: never NULL, but the column is nullable, as a PostgreSQL
/// `int` column is.
fn qty(id: i64) -> i64 {
    (id * 17) % 50 - 5
}

fn session(join_dynamic_filter: bool) -> SessionContext {
    let mut config = get_df_default_config();
    config
        .options_mut()
        .optimizer
        .enable_join_dynamic_filter_pushdown = join_dynamic_filter;
    SessionContext::new_with_config(config)
}

async fn events_table(fixture: &TestFixture) -> TestResult<Arc<CayenneTableProvider>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("qty", DataType::Int64, true),
    ]));
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let options = CreateTableOptions {
        table_name: "ev".to_string(),
        schema: Arc::clone(&schema),
        primary_key: Vec::new(),
        on_conflict: None,
        base_path: fixture.data_path.join("ev").to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            compaction_trigger_files: NO_AUTOMATIC_COMPACTION,
            compaction_trigger_protected_snapshots: NO_AUTOMATIC_COMPACTION,
            compaction_background_interval_ms: 0,
            ..VortexConfig::default()
        },
    };
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    for file in 0..FILES {
        let ids: Vec<i64> = (file * ROWS_PER_FILE + 1..=(file + 1) * ROWS_PER_FILE).collect();
        let qtys: Vec<Option<i64>> = ids.iter().map(|&id| Some(qty(id))).collect();
        common::insert_batch(
            table.as_ref(),
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(qtys)),
                ],
            )?,
        )
        .await?;
    }
    Ok(table)
}

async fn rows(ctx: &SessionContext, sql: &str) -> TestResult<Vec<String>> {
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

async fn plan_of(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Ok(frame) => match frame.create_physical_plan().await {
            Ok(plan) => displayable(plan.as_ref()).indent(true).to_string(),
            Err(e) => format!("<physical planning failed: {e}>"),
        },
        Err(e) => format!("<planning failed: {e}>"),
    }
}

async fn not_in_a_subquery_with_a_null_branch_selects_nothing_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    const WITH_NULL: &str = "SELECT qty FROM ev x WHERE x.id IN (1, 2, 3) \
         UNION ALL SELECT CAST(NULL AS BIGINT)";
    let table = events_table(&fixture).await?;
    let total = FILES * ROWS_PER_FILE;
    let listed = [qty(1), qty(2), qty(3)];
    let not_listed = (1..=total).filter(|id| !listed.contains(&qty(*id))).count();
    let not_listed_or_7 = (1..=total)
        .filter(|id| !listed.contains(&qty(*id)) && qty(*id) != 7)
        .count();

    let cases: Vec<(String, Vec<String>)> = vec![
        // The failing shape, as a count and as rows.
        (
            format!("SELECT count(*) AS n FROM ev WHERE qty NOT IN ({WITH_NULL})"),
            vec!["0".to_string()],
        ),
        (
            format!("SELECT id FROM ev WHERE qty NOT IN ({WITH_NULL})"),
            Vec::new(),
        ),
        (
            format!("SELECT count(*) AS n, sum(qty) AS s FROM ev WHERE qty NOT IN ({WITH_NULL})"),
            vec!["0,NULL".to_string()],
        ),
        // Controls: no NULL in the subquery, a non-NULL literal branch, a bare
        // NULL subquery, and the NOT EXISTS that keeps every unmatched row.
        (
            "SELECT count(*) AS n FROM ev WHERE qty NOT IN \
             (SELECT qty FROM ev x WHERE x.id IN (1, 2, 3))"
                .to_string(),
            vec![not_listed.to_string()],
        ),
        (
            "SELECT count(*) AS n FROM ev WHERE qty NOT IN \
             (SELECT qty FROM ev x WHERE x.id IN (1, 2, 3) UNION ALL SELECT 7)"
                .to_string(),
            vec![not_listed_or_7.to_string()],
        ),
        (
            "SELECT count(*) AS n FROM ev WHERE qty NOT IN (SELECT CAST(NULL AS BIGINT))"
                .to_string(),
            vec!["0".to_string()],
        ),
        (
            format!(
                "SELECT count(*) AS n FROM ev e WHERE NOT EXISTS \
                 (SELECT 1 FROM ({WITH_NULL}) s WHERE s.qty = e.qty)"
            ),
            vec![not_listed.to_string()],
        ),
    ];

    let mut wrong = Vec::new();
    for join_dynamic_filter in [true, false] {
        let ctx = session(join_dynamic_filter);
        ctx.register_table("ev", Arc::clone(&table) as Arc<dyn TableProvider>)?;
        for (sql, expected) in &cases {
            let got = match rows(&ctx, sql).await {
                Ok(rows) => format!("{rows:?}"),
                Err(e) => format!("error: {e}"),
            };
            if got != format!("{expected:?}") {
                wrong.push(format!(
                    "join_dynamic_filter={join_dynamic_filter}: {sql}: got {got}, \
                     expected {expected:?}\n{}",
                    plan_of(&ctx, sql).await
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "NOT IN over a file-backed table answered wrongly: {wrong:#?}"
    );
    Ok(())
}

#[test]
fn not_in_a_subquery_with_a_null_branch_selects_nothing() -> Result<(), String> {
    let outcome = std::thread::Builder::new()
        .stack_size(common::TEST_STACK_SIZE)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(common::TEST_STACK_SIZE)
                .enable_all()
                .build()
                .map_err(|e| format!("failed to build tokio runtime: {e}"))?
                .block_on(async {
                    let fixture = TestFixture::new(common::BackendType::Sqlite)
                        .await
                        .map_err(|e| e.to_string())?;
                    not_in_a_subquery_with_a_null_branch_selects_nothing_impl(fixture)
                        .await
                        .map_err(|e| e.to_string())
                })
        })
        .map_err(|e| format!("failed to spawn test thread: {e}"))?
        .join();
    match outcome {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}
