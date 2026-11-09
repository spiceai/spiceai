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

//! `IN` and `NOT IN` must follow SQL's three-valued logic when the Vortex scan
//! evaluates them.
//!
//! `x IN (…)` is NULL when `x` is NULL, and when `x` matches nothing in a list that
//! holds a NULL. Vortex's `list_contains` answers FALSE in both cases. A filter
//! drops a row for FALSE and NULL alike, so the two agree for `WHERE x IN (…)` —
//! but not once the result is negated (`NOT IN` would keep a NULL `x`), compared,
//! tested with `IS NULL`, or returned as a value.
//!
//! Every query runs against a Cayenne table whose rows are in a Vortex file and
//! against an in-memory `DataFusion` table holding the same rows; the answers must
//! match. The lists have four elements because `DataFusion` rewrites an `IN` list of
//! three or fewer into equality comparisons, which already handle NULL correctly.

#![allow(clippy::expect_used)]

use crate::common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::array_value_to_string;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use common::TestFixture;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::prelude::SessionContext;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

const CAYENNE_TABLE: &str = "in_list_nulls";
const REFERENCE_TABLE: &str = "in_list_nulls_reference";

fn table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("x", DataType::Int64, true),
    ]))
}

/// `x` is 1 (in every list), 5 (in none), and NULL.
fn rows() -> TestResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        table_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(Int64Array::from(vec![Some(1_i64), Some(5), None])),
        ],
    )?)
}

/// Every row rendered as `a,b,…`, sorted, so the comparison ignores row order.
async fn sorted_rows(ctx: &SessionContext, sql: &str) -> TestResult<Vec<String>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut out = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let mut rendered = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                rendered.push(array_value_to_string(column, row)?);
            }
            out.push(rendered.join(","));
        }
    }
    out.sort_unstable();
    Ok(out)
}

async fn in_list_follows_sql_null_semantics_impl(fixture: TestFixture) -> TestResult<()> {
    let ctx = SessionContext::new();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let options = CreateTableOptions {
        table_name: CAYENNE_TABLE.to_string(),
        schema: table_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        // Every insert becomes a Vortex file, so the scan evaluates pushed predicates.
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            ..VortexConfig::default()
        },
    };
    let table = Arc::new(
        CayenneTableProvider::create_table(Arc::clone(&catalog), options, ctx.runtime_env())
            .await?,
    );
    let inserted = common::insert_batch(table.as_ref(), rows()?).await?;
    assert_eq!(inserted, 3, "all three rows must be written");
    let table_id = catalog.get_table(CAYENNE_TABLE).await?.table_id;
    assert_eq!(
        catalog.get_inlined_data_count(&table_id).await?,
        0,
        "precondition: the rows must be in a Vortex file, not inlined"
    );
    ctx.register_table(CAYENNE_TABLE, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    ctx.register_table(
        REFERENCE_TABLE,
        Arc::new(MemTable::try_new(table_schema(), vec![vec![rows()?]])?),
    )?;

    let queries = [
        "SELECT id FROM {t} WHERE x IN (1, 2, 3, 4)",
        "SELECT id FROM {t} WHERE x IN (1, 2, 3, NULL)",
        "SELECT id FROM {t} WHERE x NOT IN (1, 2, 3, 4)",
        "SELECT id FROM {t} WHERE NOT (x IN (1, 2, 3, 4))",
        "SELECT id FROM {t} WHERE x NOT IN (1, 2, 3, NULL)",
        "SELECT id FROM {t} WHERE x NOT IN (1, 2, 3, 4) OR x IS NULL",
        "SELECT id FROM {t} WHERE id > 0 AND x NOT IN (1, 2, 3, 4)",
        "SELECT id FROM {t} WHERE (x IN (1, 2, 3, 4)) IS NULL",
        "SELECT id FROM {t} WHERE (x IN (1, 2, 3, 4)) = false",
        "SELECT id, x IN (1, 2, 3, 4) FROM {t}",
        "SELECT id, x NOT IN (1, 2, 3, 4) FROM {t}",
        "SELECT id, x NOT IN (1, 2, 3, NULL) FROM {t}",
        "SELECT id, CASE WHEN x IN (1, 2, 3, 4) THEN 'in' ELSE 'out' END FROM {t}",
    ];
    let mut wrong = Vec::new();
    for query in queries {
        let got = sorted_rows(&ctx, &query.replace("{t}", CAYENNE_TABLE)).await?;
        let expected = sorted_rows(&ctx, &query.replace("{t}", REFERENCE_TABLE)).await?;
        if got != expected {
            wrong.push(format!("{query}: got {got:?}, expected {expected:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "the Vortex scan evaluated an IN list with the wrong NULL semantics: {wrong:#?}"
    );
    Ok(())
}

test_with_backends!(in_list_follows_sql_null_semantics_impl);

/// Creates a file-backed table holding [`rows`], with or without a primary key.
/// Without one the table deletes by row position, which evaluates the `DELETE`
/// predicate inside the Vortex scan.
async fn file_backed_table(
    fixture: &TestFixture,
    ctx: &SessionContext,
    name: &str,
    with_primary_key: bool,
) -> TestResult<Arc<CayenneTableProvider>> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: table_schema(),
        primary_key: if with_primary_key {
            vec!["id".to_string()]
        } else {
            vec![]
        },
        on_conflict: None,
        base_path: fixture.data_path.join(name).to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            ..VortexConfig::default()
        },
    };
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    let inserted = common::insert_batch(table.as_ref(), rows()?).await?;
    assert_eq!(inserted, 3, "all three rows must be written");
    Ok(table)
}

async fn delete_with_in_list_follows_sql_null_semantics_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let ctx = SessionContext::new();
    ctx.register_table(
        REFERENCE_TABLE,
        Arc::new(MemTable::try_new(table_schema(), vec![vec![rows()?]])?),
    )?;

    let predicates = [
        "x NOT IN (1, 2, 3, 4)",
        "x NOT IN (1, 2, 3, NULL)",
        "NOT (x IN (1, 2, 3, 4))",
        "x IN (1, 2, 3, 4)",
    ];
    let mut wrong = Vec::new();
    for (index, predicate) in predicates.iter().enumerate() {
        // A DELETE removes exactly the rows its predicate is TRUE for.
        let expected = sorted_rows(
            &ctx,
            &format!(
                "SELECT id FROM {REFERENCE_TABLE} EXCEPT SELECT id FROM {REFERENCE_TABLE} WHERE {predicate}"
            ),
        )
        .await?;
        for with_primary_key in [false, true] {
            let name = format!("in_list_delete_{index}_{with_primary_key}");
            let table = file_backed_table(&fixture, &ctx, &name, with_primary_key).await?;
            ctx.register_table(&name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
            let deleted = match ctx
                .sql(&format!("DELETE FROM {name} WHERE {predicate}"))
                .await
            {
                Ok(frame) => frame.collect().await.map(|_| ()),
                Err(error) => Err(error),
            };
            if let Err(error) = deleted {
                wrong.push(format!(
                    "DELETE WHERE {predicate} (primary key: {with_primary_key}): failed: {error}"
                ));
                continue;
            }
            let remaining = sorted_rows(&ctx, &format!("SELECT id FROM {name}")).await?;
            if remaining != expected {
                wrong.push(format!(
                    "DELETE WHERE {predicate} (primary key: {with_primary_key}): left {remaining:?}, expected {expected:?}"
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "a DELETE with an IN list removed the wrong rows: {wrong:#?}"
    );
    Ok(())
}

test_with_backends!(delete_with_in_list_follows_sql_null_semantics_impl);
