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

//! Cost of resolving keys a full refresh repeats across record batches.
//!
//! Streams a generated refresh of `--keys` keys written `--passes` times (pass
//! `p` rewrites every key with value `p`; `--passes 1` has no repeats) into a
//! file-backed Cayenne table, then times queries against the result, before and
//! after compaction folds the layers. Peak RSS comes from the caller
//! (`/usr/bin/time -l`), which is why the source is generated lazily.
//!
//! ```text
//! cargo run --release -p cayenne --example layered_refresh_bench -- \
//!     --policy <none|drop|upsert|keep_last> --keys 1000000 --passes 4 [--refreshes 2]
//! ```
//!
//! `--refreshes` repeats the refresh over the table the previous one left; every
//! refresh after the first replaces a table of known size, which is the common
//! shape of a scheduled refresh.

#![expect(
    clippy::print_stdout,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "benchmark"
)]

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProviderBuilder, MetadataCatalog, UpsertDedup};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;

const BATCH: usize = 8_192;

fn arg(name: &str, default: &str) -> String {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
        .unwrap_or_else(|| default.to_string())
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("pass", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// `passes` sweeps over `keys` keys in a scrambled order, generated lazily.
fn source(keys: usize, passes: usize) -> SendableRecordBatchStream {
    let total = keys * passes;
    let batches = (0..total.div_ceil(BATCH)).map(move |b| {
        let start = b * BATCH;
        let end = (start + BATCH).min(total);
        let ids: Int64Array = (start..end)
            .map(|row| {
                let k = (row % keys) as u64;
                // A bijection on 0..keys when keys is odd-coprime; spreads keys
                // across the key space so every batch spans the range.
                ((k.wrapping_mul(2_654_435_761)) % keys as u64) as i64
            })
            .collect();
        let pass: Int64Array = (start..end).map(|row| (row / keys) as i64).collect();
        let payload: StringArray = (start..end)
            .map(|row| {
                Some(format!(
                    "payload-{row:016}-{:08x}",
                    row.wrapping_mul(40_503)
                ))
            })
            .collect();
        Ok(RecordBatch::try_new(
            schema(),
            vec![Arc::new(ids), Arc::new(pass), Arc::new(payload)],
        )
        .expect("batch"))
    });
    Box::pin(RecordBatchStreamAdapter::new(
        schema(),
        futures::stream::iter(batches),
    ))
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
}

async fn time_queries(ctx: &SessionContext, keys: usize, label: &str) {
    let run = |sql: String| {
        let ctx = ctx.clone();
        async move {
            let start = Instant::now();
            let batches = ctx
                .sql(&sql)
                .await
                .expect("plan")
                .collect()
                .await
                .expect("query");
            (start.elapsed().as_secs_f64() * 1e3, batches)
        }
    };
    let (_, count) = run("SELECT COUNT(*) FROM t".to_string()).await;
    let count = count[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0);
    let mut scans = Vec::new();
    for _ in 0..20 {
        scans.push(run("SELECT SUM(pass), MAX(id) FROM t".to_string()).await.0);
    }
    let mut lookups = Vec::new();
    for i in 0..200_u64 {
        let key = (i.wrapping_mul(7_919) % keys as u64) as i64;
        lookups.push(run(format!("SELECT pass FROM t WHERE id = {key}")).await.0);
    }
    scans.sort_by(f64::total_cmp);
    lookups.sort_by(f64::total_cmp);
    println!(
        "  {label}: COUNT(*)={count} | full scan ms p50={:.1} p99={:.1} max={:.1} | point lookup ms p50={:.2} p99={:.2} max={:.2}",
        percentile(&scans, 0.5),
        percentile(&scans, 0.99),
        scans[scans.len() - 1],
        percentile(&lookups, 0.5),
        percentile(&lookups, 0.99),
        lookups[lookups.len() - 1],
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let policy = arg("--policy", "upsert");
    let keys: usize = arg("--keys", "1000000").parse().expect("keys");
    let passes: usize = arg("--passes", "1").parse().expect("passes");
    let refreshes: usize = arg("--refreshes", "1").parse().expect("refreshes");
    let deletion_mode = match arg("--deletion-mode", "position").as_str() {
        "key" => cayenne::metadata::DeletionMode::Key,
        "position" => cayenne::metadata::DeletionMode::Position,
        other => panic!("unknown deletion mode {other}"),
    };
    let dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}/cayenne.db", metadata_dir.display()))
            .expect("catalog"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("init");
    let ctx = SessionContext::new();
    let key = ColumnReference::new(vec!["id".to_string()]);
    let (on_conflict, dedup) = match policy.as_str() {
        "none" => (None, UpsertDedup::None),
        "drop" => (Some(OnConflict::DoNothing(key)), UpsertDedup::None),
        "upsert" => (Some(OnConflict::Upsert(key)), UpsertDedup::None),
        "keep_last" => (Some(OnConflict::Upsert(key)), UpsertDedup::KeepLast),
        other => panic!("unknown policy {other}"),
    };
    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
        .with_upsert_dedup(dedup)
        .create(CreateTableOptions {
            table_name: "t".to_string(),
            schema: schema(),
            primary_key: vec!["id".to_string()],
            on_conflict,
            base_path: dir.path().join("data").display().to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                deletion_mode,
                inline_max_rows: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("table");
    let provider = Arc::new(provider);

    let mut refresh_s = 0.0;
    let mut written = 0;
    for refresh in 1..=refreshes {
        let start = Instant::now();
        let prepared = provider
            .begin_overwrite(
                source(keys, passes),
                ctx.state().config().target_partitions(),
            )
            .await
            .expect("refresh");
        written = prepared.row_count();
        prepared.apply_owned_txn().await.expect("commit");
        prepared.finish().await.expect("publish");
        refresh_s = start.elapsed().as_secs_f64();
        if refreshes > 1 {
            println!("  refresh {refresh} of {refreshes}: refresh_s={refresh_s:.2}");
        }
    }
    let layers = catalog
        .get_all_snapshot_sequences(provider.table_id())
        .await
        .expect("layers")
        .len();
    println!(
        "policy={policy} deletion_mode={deletion_mode:?} keys={keys} passes={passes} rows_in={} rows_written={written} layers={layers} refresh_s={refresh_s:.2} rows_per_s={:.0}",
        keys * passes,
        (keys * passes) as f64 / refresh_s,
    );
    // Vortex files per snapshot directory (the main snapshot and each layer).
    let mut files_per_dir: Vec<usize> = std::fs::read_dir(dir.path().join("data"))
        .into_iter()
        .flatten()
        .flatten()
        .filter(|table| table.path().is_dir())
        .flat_map(|table| {
            std::fs::read_dir(table.path())
                .into_iter()
                .flatten()
                .flatten()
        })
        .filter(|snapshot| snapshot.path().is_dir())
        .map(|snapshot| {
            std::fs::read_dir(snapshot.path())
                .into_iter()
                .flatten()
                .flatten()
                .filter(|f| f.path().extension().is_some_and(|e| e == "vortex"))
                .count()
        })
        .filter(|&n| n > 0)
        .collect();
    files_per_dir.sort_unstable();
    println!(
        "  snapshot dirs with files={} vortex files per dir={files_per_dir:?} input batches={}",
        files_per_dir.len(),
        (keys * passes).div_ceil(BATCH)
    );
    ctx.register_table(
        "t",
        Arc::clone(&provider) as Arc<dyn datafusion::datasource::TableProvider>,
    )
    .expect("register");
    time_queries(&ctx, keys, "before compaction").await;
    let start = Instant::now();
    provider
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("compaction");
    println!("  compaction_s={:.2}", start.elapsed().as_secs_f64());
    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::clone(&provider) as Arc<dyn datafusion::datasource::TableProvider>,
    )
    .expect("register");
    time_queries(&ctx, keys, "after compaction").await;
}
