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

//! `CayenneDynamicFilterSharing` copies a join's dynamic filter from one scan of a
//! table to another scan of the same table when a hash join equates the two. That
//! is sound only when the join key on each side *is* the scan's column. A key that
//! merely has the same name — `t.c + 1 AS c`, or the output of a `UNION` that also
//! reads another table — carries other values, and the copied filter drops rows the
//! join would have matched.
//!
//! Every query runs in a session with the rule and in one without it; the rows must
//! match.

#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::array_value_to_string;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::optimizer_rules::CayenneDynamicFilterSharing;
use cayenne::{CayenneTableProvider, MetadataCatalog};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionContext;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

async fn file_backed_table(
    fixture: &TestFixture,
    name: &str,
    column: &str,
    values: Vec<i64>,
) -> TestResult<Arc<CayenneTableProvider>> {
    let schema = Arc::new(Schema::new(vec![Field::new(column, DataType::Int64, false)]));
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: Vec::new(),
        on_conflict: None,
        base_path: fixture.data_path.join(name).to_string_lossy().to_string(),
        partition_column: None,
        // Every insert becomes a Vortex file, so the scan accepts the pushed
        // dynamic filter.
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
    };
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?,
    );
    common::insert_batch(
        table.as_ref(),
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])?,
    )
    .await?;
    Ok(table)
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

async fn shared_filters_follow_join_key_lineage_impl(fixture: TestFixture) -> TestResult<()> {
    let t = file_backed_table(&fixture, "t", "c", (1_i64..=100_000).collect()).await?;
    let small = file_backed_table(&fixture, "small", "k", vec![1]).await?;
    let w = file_backed_table(&fixture, "w", "c", vec![5]).await?;

    let with_rule = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(CayenneDynamicFilterSharing::new()))
            .build(),
    );
    let reference =
        SessionContext::new_with_state(SessionStateBuilder::new().with_default_features().build());
    for ctx in [&with_rule, &reference] {
        ctx.register_table("t", Arc::clone(&t) as Arc<dyn TableProvider>)?;
        ctx.register_table("small", Arc::clone(&small) as Arc<dyn TableProvider>)?;
        ctx.register_table("w", Arc::clone(&w) as Arc<dyn TableProvider>)?;
    }

    let queries = [
        // The key `c` is `t.c + 1`, not the scan's `c`.
        "SELECT t2.c FROM (SELECT t.c + 1 AS c FROM t JOIN small ON t.c = small.k) u \
         JOIN t AS t2 ON u.c = t2.c",
        // The same, with the computed key grouped.
        "SELECT t2.c FROM (SELECT t.c + 1 AS c FROM t JOIN small ON t.c = small.k GROUP BY t.c + 1) u \
         JOIN t AS t2 ON u.c = t2.c",
        // `c` comes from two tables, and only `t`'s rows are filtered.
        "SELECT t2.c FROM (SELECT t.c FROM t JOIN small ON t.c = small.k UNION ALL SELECT w.c FROM w) u \
         JOIN t AS t2 ON u.c = t2.c",
        // The key is the scan's own column: sharing is sound here.
        "SELECT t2.c FROM (SELECT t.c FROM t JOIN small ON t.c = small.k) u JOIN t AS t2 ON u.c = t2.c",
        // The same through a group-by key.
        "SELECT t2.c FROM (SELECT t.c FROM t JOIN small ON t.c = small.k GROUP BY t.c) u \
         JOIN t AS t2 ON u.c = t2.c",
    ];
    let mut wrong = Vec::new();
    for sql in queries {
        let got = sorted_rows(&with_rule, sql).await?;
        let expected = sorted_rows(&reference, sql).await?;
        if got != expected {
            wrong.push(format!("{sql}: got {got:?}, expected {expected:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "a shared dynamic filter restricted rows its join does not constrain: {wrong:#?}"
    );
    Ok(())
}

test_with_backends!(shared_filters_follow_join_key_lineage_impl);
