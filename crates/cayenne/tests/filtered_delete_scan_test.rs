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

//! Key-based deletes served by the filtered scan, which reads only the key and
//! filter columns (#14364).

#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::{CayenneTableProvider, MetadataCatalog, metadata::CreateTableOptions};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

async fn setup(
    fixture: &TestFixture,
    name: &str,
    schema: &Arc<Schema>,
    primary_key: &[&str],
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            catalog,
            CreateTableOptions {
                table_name: name.to_string(),
                schema: Arc::clone(schema),
                primary_key: primary_key.iter().map(ToString::to_string).collect(),
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((table, ctx))
}

/// Inserts `columns` and checkpoints them to a file, so deletes take the file path.
async fn insert_file(
    table: &CayenneTableProvider,
    schema: &Arc<Schema>,
    columns: Vec<ArrayRef>,
) -> TestResult<()> {
    common::insert_batch(table, RecordBatch::try_new(Arc::clone(schema), columns)?).await?;
    table.checkpoint_inlined_data().await?;
    Ok(())
}

fn strings(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

/// `DELETE` through `TableProvider::delete_from`, returning the reported count.
async fn delete(table: &CayenneTableProvider, filter: Expr) -> TestResult<u64> {
    let ctx = SessionContext::new();
    let plan = table.delete_from(&ctx.state(), vec![filter]).await?;
    let batches = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<UInt64Array>())
        .map_or(0, |a| a.value(0)))
}

async fn payloads(ctx: &SessionContext, table: &str) -> TestResult<Vec<String>> {
    let batches = ctx
        .sql(&format!("SELECT payload FROM {table} ORDER BY payload"))
        .await?
        .collect()
        .await?;
    Ok(batches
        .iter()
        .flat_map(|b| {
            let column = b
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("payload is Utf8");
            (0..column.len())
                .map(|i| column.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect())
}

/// Composite key: filters on non-key columns remove exactly the matching rows
/// across files, and the reported count is exact.
async fn composite_key_filtered_delete_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("code", DataType::Utf8, false),
        Field::new("tag", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let (table, ctx) = setup(&fixture, "composite", &schema, &["region", "code"]).await?;
    insert_file(
        &table,
        &schema,
        vec![
            strings(&["us", "us", "eu"]),
            strings(&["a", "b", "a"]),
            strings(&["x", "y", "x"]),
            strings(&["us-a", "us-b", "eu-a"]),
        ],
    )
    .await?;
    insert_file(
        &table,
        &schema,
        vec![
            strings(&["us", "eu"]),
            strings(&["c", "b"]),
            strings(&["x", "y"]),
            strings(&["us-c", "eu-b"]),
        ],
    )
    .await?;

    // Key column and non-key column together, matching one row per file.
    let filter = col("region").eq(lit("us")).and(col("tag").eq(lit("x")));
    assert_eq!(delete(&table, filter).await?, 2);
    assert_eq!(
        payloads(&ctx, "composite").await?,
        vec!["eu-a", "eu-b", "us-b"]
    );

    // Non-key column only.
    assert_eq!(delete(&table, col("payload").eq(lit("eu-b"))).await?, 1);
    assert_eq!(payloads(&ctx, "composite").await?, vec!["eu-a", "us-b"]);

    // No match.
    assert_eq!(delete(&table, col("tag").eq(lit("z"))).await?, 0);
    assert_eq!(payloads(&ctx, "composite").await?, vec!["eu-a", "us-b"]);
    Ok(())
}
test_with_backends!(composite_key_filtered_delete_impl);

/// `Int64` key: same, through its own key strategy.
async fn int64_key_filtered_delete_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let (table, ctx) = setup(&fixture, "int64", &schema, &["id"]).await?;
    insert_file(
        &table,
        &schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            strings(&["x", "y", "x"]),
            strings(&["p1", "p2", "p3"]),
        ],
    )
    .await?;
    insert_file(
        &table,
        &schema,
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            strings(&["x", "y"]),
            strings(&["p4", "p5"]),
        ],
    )
    .await?;

    let filter = col("tag").eq(lit("x")).and(col("id").gt(lit(1_i64)));
    assert_eq!(delete(&table, filter).await?, 2);
    assert_eq!(payloads(&ctx, "int64").await?, vec!["p1", "p2", "p5"]);
    Ok(())
}
test_with_backends!(int64_key_filtered_delete_impl);
