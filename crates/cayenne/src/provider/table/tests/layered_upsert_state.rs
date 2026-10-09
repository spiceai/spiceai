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

//! The state a layered streaming append publishes — a main snapshot, protected snapshots
//! above it, and key tombstones between them — is exactly what an overwrite
//! followed by upsert appends produces. This checks that state keeps the last
//! copy of every key through a reopen and every compaction, in both deletion modes.

use super::*;
use crate::metadata::DeletionMode;
use arrow::array::AsArray;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn rows(ids: std::ops::Range<i64>, value: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(ids.clone())),
            Arc::new(Int64Array::from_iter_values(ids.map(|_| value))),
        ],
    )
    .expect("batch")
}

async fn write(provider: &CayenneTableProvider, op: InsertOp, batch: RecordBatch) {
    let ctx = SessionContext::new();
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema(), None).expect("source");
    let plan = provider
        .insert_into(&ctx.state(), source, op)
        .await
        .expect("plan");
    collect(plan, ctx.task_ctx()).await.expect("write");
}

async fn visible(provider: &CayenneTableProvider) -> (Vec<(i64, i64)>, i64) {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider.clone_for_write()))
        .expect("register");
    let batches = ctx
        .sql("SELECT id, value FROM t ORDER BY id")
        .await
        .expect("query")
        .collect()
        .await
        .expect("collect");
    let mut out = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>();
        let values = batch
            .column(1)
            .as_primitive::<arrow::datatypes::Int64Type>();
        for row in 0..batch.num_rows() {
            out.push((ids.value(row), values.value(row)));
        }
    }
    let count = ctx
        .sql("SELECT COUNT(*) FROM t")
        .await
        .expect("query")
        .collect()
        .await
        .expect("collect")[0]
        .column(0)
        .as_primitive::<arrow::datatypes::Int64Type>()
        .value(0);
    (out, count)
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_then_upsert_layers_keep_the_last_copy_through_its_lifecycle() {
    // 0..10 at 0, then layer k (1..=6) upserts 2k..2k+10 at k.
    let layers: Vec<(std::ops::Range<i64>, i64)> =
        (1..=6).map(|k| (2 * k..2 * k + 10, k)).collect();
    let mut model: std::collections::BTreeMap<i64, i64> = (0..10).map(|id| (id, 0)).collect();
    for (ids, value) in &layers {
        for id in ids.clone() {
            model.insert(id, *value);
        }
    }
    let expected: Vec<(i64, i64)> = model.into_iter().collect();
    let expected_count = i64::try_from(expected.len()).expect("count");
    let mut failures = Vec::new();
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let runtime_env = SessionContext::new().runtime_env();
        let (provider, catalog, _dir) = create_cdc_table_with_schema(
            "t",
            Arc::clone(&runtime_env),
            schema(),
            vec!["id".to_string()],
            VortexConfig {
                deletion_mode: mode,
                inline_max_rows: 0,
                compaction_background_interval_ms: 3_600_000,
                compaction_trigger_protected_snapshots: 2,
                ..VortexConfig::default()
            },
            OnConflict::Upsert(
                datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                    "id".to_string(),
                ]),
            ),
        )
        .await;
        write(&provider, InsertOp::Overwrite, rows(0..10, 0)).await;
        for (ids, value) in &layers {
            write(&provider, InsertOp::Append, rows(ids.clone(), *value)).await;
        }

        let mut check = |stage: &str, (rows, count): (Vec<(i64, i64)>, i64), layers: usize| {
            let ok = rows == expected && count == expected_count;
            eprintln!(
                "{mode:?} {stage}: protected layers {layers}, {} rows, COUNT(*) {count}: {}",
                rows.len(),
                if ok { "ok" } else { "WRONG" }
            );
            if !ok {
                failures.push(format!("{mode:?} {stage}: {rows:?} count {count}"));
            }
        };
        check(
            "after writes",
            visible(&provider).await,
            provider.protected_snapshot_ids().len(),
        );
        let reopened =
            CayenneTableProviderBuilder::new(Arc::clone(&catalog), Arc::clone(&runtime_env))
                .open("t")
                .await
                .expect("reopen");
        check(
            "after reopen",
            visible(&reopened).await,
            reopened.protected_snapshot_ids().len(),
        );
        let merged = reopened
            .compact_protected_snapshots_subset(8)
            .await
            .expect("protected merge");
        check(
            &format!("after protected merge ({merged})"),
            visible(&reopened).await,
            reopened.protected_snapshot_ids().len(),
        );
        let baked = reopened
            .bake_seq_prefix_protected_snapshots()
            .await
            .expect("bake");
        check(
            &format!("after seq-prefix bake ({baked})"),
            visible(&reopened).await,
            reopened.protected_snapshot_ids().len(),
        );
        let small = reopened
            .compact_current_snapshot_small_files()
            .await
            .expect("small files");
        check(
            &format!("after small-file compaction ({small})"),
            visible(&reopened).await,
            reopened.protected_snapshot_ids().len(),
        );
        reopened
            .sort_and_rewrite_data(64 * 1024 * 1024)
            .await
            .expect("full rewrite");
        check(
            "after full rewrite",
            visible(&reopened).await,
            reopened.protected_snapshot_ids().len(),
        );
        let reopened =
            CayenneTableProviderBuilder::new(Arc::clone(&catalog), Arc::clone(&runtime_env))
                .open("t")
                .await
                .expect("reopen");
        check(
            "after second reopen",
            visible(&reopened).await,
            reopened.protected_snapshot_ids().len(),
        );
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
