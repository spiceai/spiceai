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

//! A delete whose predicate is an OR of conjunctions that each pin the whole
//! composite primary key, plus a condition on a non-key column — the shape a
//! caching dataset's replacement delete has — removes exactly the live rows that
//! satisfy the whole predicate and reports exactly that count, whether the rows
//! are in files or still inline. With a secondary index on the key, the rows are
//! located by key and verified instead of found by scanning the table.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

const NAME: &str = "verified";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("query", DataType::Utf8, false),
        Field::new("ns", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, false),
    ]))
}

/// A composite-PK upsert table indexed on its key. `inline_max_rows: 0` writes
/// every upsert to a file; a large value keeps the rows inline.
async fn open(
    fixture: &common::TestFixture,
    mode: DeletionMode,
    inline_max_rows: usize,
) -> Arc<CayenneTableProvider> {
    let runtime_env = Arc::new(RuntimeEnv::default());
    let vortex_config = VortexConfig {
        deletion_mode: mode,
        inline_max_rows,
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env), NAME);
    let key = vec!["path".to_string(), "query".to_string()];
    let options = CreateTableOptions {
        table_name: NAME.to_string(),
        schema: schema(),
        primary_key: key.clone(),
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(key.clone()))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    Arc::new(
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_context(context)
            .with_secondary_indexes(vec![key])
            .create(options)
            .await
            .expect("create table"),
    )
}

fn context(table: &Arc<CayenneTableProvider>) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_table(NAME, Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    ctx
}

/// Upserts rows `(path, query, ns)`; `content` is `path|query|ns`.
async fn upsert(table: &Arc<CayenneTableProvider>, rows: &[(&str, &str, &str)]) {
    let path: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let query: Vec<&str> = rows.iter().map(|r| r.1).collect();
    let ns: Vec<&str> = rows.iter().map(|r| r.2).collect();
    let content: Vec<String> = rows
        .iter()
        .map(|r| format!("{}|{}|{}", r.0, r.1, r.2))
        .collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(path)),
            Arc::new(StringArray::from(query)),
            Arc::new(StringArray::from(ns)),
            Arc::new(StringArray::from(content)),
        ],
    )
    .expect("batch");
    let ctx = context(table);
    let mem = datafusion::datasource::MemTable::try_new(schema(), vec![vec![batch]]).expect("mem");
    ctx.register_table("src", Arc::new(mem)).expect("src");
    ctx.sql(&format!("INSERT INTO {NAME} SELECT * FROM src"))
        .await
        .expect("upsert plan")
        .collect()
        .await
        .expect("upsert");
}

/// Deletes the way a caching dataset replaces entries: `delete_from` called
/// directly with one OR-of-AND expression, not a SQL `DELETE` (whose optimizer
/// would factor conjuncts shared by every disjunct out of the OR).
async fn delete(table: &Arc<CayenneTableProvider>, entries: &[(&str, &str, &str)]) -> u64 {
    use datafusion::logical_expr::{col, lit};
    let disjuncts = entries.iter().map(|(p, q, ns)| {
        col("path")
            .eq(lit(*p))
            .and(col("query").eq(lit(*q)))
            .and(col("ns").eq(lit(*ns)))
    });
    let predicate = datafusion::logical_expr::utils::disjunction(disjuncts).expect("predicate");
    let ctx = context(table);
    let plan = table
        .delete_from(&ctx.state(), vec![predicate])
        .await
        .expect("delete plan");
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("delete");
    batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<UInt64Array>().map(|a| a.value(0)))
        .expect("delete count")
}

async fn remaining(table: &Arc<CayenneTableProvider>) -> Vec<String> {
    let batches = context(table)
        .sql(&format!("SELECT content FROM {NAME} ORDER BY content"))
        .await
        .expect("select plan")
        .collect()
        .await
        .expect("select");
    let mut out = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("content is utf8");
        for row in 0..column.len() {
            out.push(column.value(row).to_string());
        }
    }
    out
}

async fn deletes_exactly_the_matching_rows(mode: DeletionMode, inline_max_rows: usize) {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let table = open(&fixture, mode, inline_max_rows).await;
    upsert(
        &table,
        &[
            ("/v1", "k1", "public"),
            ("/v1", "k2", "public"),
            ("/v1", "k3", "other"),
            ("/v2", "k1", "public"),
        ],
    )
    .await;
    upsert(&table, &[("/v1", "k4", "public")]).await;
    let what = format!("{mode:?}, inline_max_rows={inline_max_rows}");

    // k1 and k4 match on key and namespace; k3's key matches but its namespace
    // does not; k9 does not exist.
    let probes = |t: &Arc<CayenneTableProvider>| {
        let c = t.lookup_index_counters().expect("indexed");
        c.selected + c.empty + c.unbuilt
    };
    let probes_before = probes(&table);
    let deleted = delete(
        &table,
        &[
            ("/v1", "k1", "public"),
            ("/v1", "k3", "public"),
            ("/v1", "k4", "public"),
            ("/v1", "k9", "public"),
        ],
    )
    .await;
    assert_eq!(deleted, 2, "{what}: deleted-row count");
    assert!(
        probes(&table) >= probes_before + 4,
        "{what}: the delete should locate each of its 4 keys through the index"
    );
    assert_eq!(
        remaining(&table).await,
        ["/v1|k2|public", "/v1|k3|other", "/v2|k1|public"],
        "{what}: surviving rows"
    );

    // Nothing matches: a no-op with a zero count.
    let deleted = delete(&table, &[("/v1", "k3", "public")]).await;
    assert_eq!(deleted, 0, "{what}: non-matching delete count");
    assert_eq!(remaining(&table).await.len(), 3, "{what}: no-op delete");

    // A deleted key re-inserted afterwards is live again.
    upsert(&table, &[("/v1", "k1", "public")]).await;
    assert_eq!(
        remaining(&table).await,
        ["/v1|k1|public", "/v1|k2|public", "/v1|k3|other", "/v2|k1|public"],
        "{what}: re-insert after delete"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_rows_key_mode() {
    deletes_exactly_the_matching_rows(DeletionMode::Key, 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inline_rows_key_mode() {
    deletes_exactly_the_matching_rows(DeletionMode::Key, 1_000_000).await;
}
