/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Autocommit `UPDATE` on an upsert-on-key table: the affected-row count is
//! the rows the predicate matched however often they were updated before, and
//! a concurrent reader never sees the matched rows missing mid-statement.

use super::*;
use crate::metadata::DeletionMode;
use crate::provider::append_commit::test_seams;
use arrow::array::{AsArray, StringArray};
use datafusion::prelude::lit;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

const ROWS: i64 = 20;

async fn seeded(mode: DeletionMode) -> (CayenneTableProvider, Arc<dyn MetadataCatalog>, TempDir) {
    let runtime_env = SessionContext::new().runtime_env();
    let (provider, catalog, dir) = create_cdc_table_with_schema(
        "t",
        runtime_env,
        schema(),
        vec!["id".to_string()],
        VortexConfig {
            deletion_mode: mode,
            compaction_background_interval_ms: 3_600_000,
            // Staged (non-inline) writes: the staged-commit pause hook below
            // only fires on that path.
            inline_max_rows: 0,
            ..VortexConfig::default()
        },
        OnConflict::Upsert(
            datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                "id".to_string(),
            ]),
        ),
    )
    .await;
    let seed = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(1..=ROWS)),
            Arc::new(StringArray::from_iter_values((1..=ROWS).map(|_| "v0"))),
        ],
    )
    .expect("batch");
    let ctx = SessionContext::new();
    let source = MemorySourceConfig::try_new_exec(&[vec![seed]], schema(), None).expect("source");
    let plan = provider
        .insert_into(&ctx.state(), source, InsertOp::Append)
        .await
        .expect("insert plan");
    collect(plan, ctx.task_ctx()).await.expect("seed");
    (provider, catalog, dir)
}

async fn update_pair(provider: &CayenneTableProvider, a: i64, b: i64, value: &str) -> u64 {
    let ctx = SessionContext::new();
    let plan = provider
        .update(
            &ctx.state(),
            vec![("value".to_string(), lit(value.to_string()))],
            vec![col("id").in_list(vec![lit(a), lit(b)], false)],
        )
        .await
        .expect("update plan");
    let batches = collect(plan, ctx.task_ctx()).await.expect("update");
    batches[0]
        .column(0)
        .as_primitive::<arrow::datatypes::UInt64Type>()
        .value(0)
}

/// (rows, distinct ids, rows holding `value`)
async fn read(provider: &CayenneTableProvider, value: &str) -> (i64, i64, i64) {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider.clone_for_write()))
        .expect("register");
    let batches = ctx
        .sql(&format!(
            "SELECT COUNT(*), COUNT(DISTINCT id), SUM(CASE WHEN value = '{value}' THEN 1 ELSE 0 END) FROM t"
        ))
        .await
        .expect("query")
        .collect()
        .await
        .expect("collect");
    let col = |i: usize| {
        batches[0]
            .column(i)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0)
    };
    (col(0), col(1), col(2))
}

/// Every UPDATE matching two keys reports 2, including keys an earlier
/// UPDATE already rewrote (the delete leg's tombstone count missed those).
#[tokio::test(flavor = "multi_thread")]
async fn update_counts_matched_rows_after_earlier_updates() {
    for mode in [
        DeletionMode::Auto,
        DeletionMode::Key,
        DeletionMode::Position,
    ] {
        let (provider, _catalog, _dir) = seeded(mode).await;
        let mut wrong = Vec::new();
        for step in 0..60_i64 {
            // Small key range so most statements hit already-updated keys.
            let a = 1 + (step * 7) % 6;
            let b = 1 + (a % 6);
            let count = update_pair(&provider, a, b, &format!("v{}", step + 1)).await;
            if count != 2 {
                wrong.push((step, a, b, count));
            }
        }
        assert!(wrong.is_empty(), "{mode:?}: miscounted updates {wrong:?}");
        assert_eq!(read(&provider, "").await.0, ROWS, "{mode:?}: row count");
    }
}

/// A reader that resolves the snapshot while an UPDATE is about to publish
/// sees the pre-update table whole, then the post-update table whole — never
/// the matched rows tombstoned without their replacements.
#[tokio::test(flavor = "multi_thread")]
async fn update_is_invisible_until_its_single_publish() {
    for mode in [
        DeletionMode::Auto,
        DeletionMode::Key,
        DeletionMode::Position,
    ] {
        let (provider, _catalog, _dir) = seeded(mode).await;
        let (reached_tx, reached) = tokio::sync::oneshot::channel::<()>();
        let release = Arc::new(tokio::sync::Notify::new());
        let release_in_commit = Arc::clone(&release);
        test_seams::pause_before_commit(
            provider.table_id(),
            Box::new(move || {
                Box::pin(async move {
                    let _ = reached_tx.send(());
                    release_in_commit.notified().await;
                })
            }),
        );
        let writer = provider.clone_for_write();
        let update = tokio::spawn(async move { update_pair(&writer, 3, 4, "new").await });
        tokio::time::timeout(std::time::Duration::from_secs(30), reached)
            .await
            .expect("the update reaches its commit")
            .expect("the commit signals");
        assert_eq!(
            read(&provider, "new").await,
            (ROWS, ROWS, 0),
            "{mode:?}: mid-update reader sees the whole pre-update table"
        );
        release.notify_one();
        let count = tokio::time::timeout(std::time::Duration::from_secs(30), update)
            .await
            .expect("the update finishes")
            .expect("the update task");
        assert_eq!(count, 2, "{mode:?}");
        assert_eq!(
            read(&provider, "new").await,
            (ROWS, ROWS, 2),
            "{mode:?}: post-update reader sees both replacements"
        );
    }
}
