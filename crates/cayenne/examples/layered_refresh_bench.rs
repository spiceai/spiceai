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
//!     --policy <none|keep_last> --keys 1000000 --passes 4 [--refreshes 2]
//! ```
//!
//! `--append` writes the generated data as an append refresh instead, through
//! the streaming append path (`stream_publish_interval_ms: 0`), so its repeats
//! are resolved as an append resolves them; `--refreshes` then appends again
//! over the rows the previous append left.
//!
//! `--refreshes` repeats the refresh over the table the previous one left; every
//! refresh after the first replaces a table of known size, which is the common
//! shape of a scheduled refresh.
//!
//! `--progress-rows N` prints the rate the refresh pulls rows from its source
//! over each `N` rows, so a rate that falls as the write grows shows up.
//! `--memory-limit-mb M` runs the refresh under an `M` MiB memory pool, as
//! `spiced` does under `runtime.query.memory_limit`. `--skip-queries` stops
//! after the refresh.

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
use cayenne::{CayenneCatalog, CayenneTableProviderBuilder, MetadataCatalog};
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

/// Rows the source emits: `keys * passes`, or, with `--dup-fraction f`, every
/// key once followed by a second copy of `f * keys` of them.
fn total_rows(keys: usize, passes: usize) -> usize {
    match arg("--dup-fraction", "").parse::<f64>() {
        Ok(fraction) => keys + (keys as f64 * fraction).round() as usize,
        Err(_) => keys * passes,
    }
}

/// The primary key's shape (`--key-kind`): one `Int64` (`int`), one 32-character
/// hex string (`string`), or an `Int64` tenant plus a string id (`composite`).
static KEY_KIND: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| arg("--key-kind", "int"));

fn key_columns() -> Vec<String> {
    match KEY_KIND.as_str() {
        "int" | "string" => vec!["id".to_string()],
        "composite" => vec!["tenant".to_string(), "id".to_string()],
        other => panic!("unknown key kind {other}"),
    }
}

fn schema() -> SchemaRef {
    let mut fields = Vec::new();
    match KEY_KIND.as_str() {
        "int" => fields.push(Field::new("id", DataType::Int64, false)),
        "string" => fields.push(Field::new("id", DataType::Utf8, false)),
        _ => {
            fields.push(Field::new("tenant", DataType::Int64, false));
            fields.push(Field::new("id", DataType::Utf8, false));
        }
    }
    fields.push(Field::new("pass", DataType::Int64, false));
    fields.push(Field::new("payload", DataType::Utf8, false));
    Arc::new(Schema::new(fields))
}

/// `passes` sweeps over `keys` keys in a scrambled order, generated lazily,
/// printing the rate rows are pulled at over each `progress_rows` rows.
fn source(keys: usize, passes: usize, progress_rows: usize) -> SendableRecordBatchStream {
    let started = Instant::now();
    let mut mark = (0_usize, started);
    let total = total_rows(keys, passes);
    // `--key-order sorted` emits each pass in key order instead, which the first
    // load cannot cut split points from, so it hashes its key.
    let sorted = arg("--key-order", "scrambled") == "sorted";
    let batches = (0..total.div_ceil(BATCH)).map(move |b| {
        let start = b * BATCH;
        let end = (start + BATCH).min(total);
        let scrambled: Vec<u64> = (start..end)
            .map(|row| {
                let k = (row % keys) as u64;
                if sorted {
                    return k;
                }
                // A bijection on 0..keys when keys is odd-coprime; spreads keys
                // across the key space so every batch spans the range.
                (k.wrapping_mul(2_654_435_761)) % keys as u64
            })
            .collect();
        let mut key_arrays: Vec<arrow::array::ArrayRef> = Vec::new();
        match KEY_KIND.as_str() {
            "int" => key_arrays.push(Arc::new(
                scrambled.iter().map(|k| *k as i64).collect::<Int64Array>(),
            )),
            "string" => key_arrays.push(Arc::new(StringArray::from_iter_values(
                scrambled
                    .iter()
                    .map(|k| format!("{:032x}", k.wrapping_mul(0x9e37_79b9_7f4a_7c15))),
            ))),
            _ => {
                key_arrays.push(Arc::new(
                    scrambled
                        .iter()
                        .map(|k| (k % 1024) as i64)
                        .collect::<Int64Array>(),
                ));
                key_arrays.push(Arc::new(StringArray::from_iter_values(
                    scrambled.iter().map(|k| format!("{:024x}", k / 1024)),
                )));
            }
        }
        if progress_rows > 0 && end - mark.0 >= progress_rows {
            let now = Instant::now();
            println!(
                "  progress rows={end} elapsed_s={:.1} interval_rows_per_s={:.0}",
                now.duration_since(started).as_secs_f64(),
                (end - mark.0) as f64 / now.duration_since(mark.1).as_secs_f64()
            );
            mark = (end, now);
        }
        let pass: Int64Array = (start..end).map(|row| (row / keys) as i64).collect();
        let payload: StringArray = (start..end)
            .map(|row| {
                Some(format!(
                    "payload-{row:016}-{:08x}",
                    row.wrapping_mul(40_503)
                ))
            })
            .collect();
        key_arrays.push(Arc::new(pass));
        key_arrays.push(Arc::new(payload));
        Ok(RecordBatch::try_new(schema(), key_arrays).expect("batch"))
    });
    Box::pin(RecordBatchStreamAdapter::new(
        schema(),
        futures::stream::iter(batches),
    ))
}

/// A plan that yields `stream` once, so an append reads the generated source
/// lazily through `insert_into`.
struct OneShot {
    schema: SchemaRef,
    stream: std::sync::Mutex<Option<SendableRecordBatchStream>>,
}

impl std::fmt::Debug for OneShot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OneShot")
    }
}

impl datafusion::physical_plan::streaming::PartitionStream for OneShot {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<datafusion::execution::TaskContext>) -> SendableRecordBatchStream {
        self.stream
            .lock()
            .expect("lock")
            .take()
            .expect("the source is read once")
    }
}

async fn append(
    provider: &Arc<cayenne::CayenneTableProvider>,
    ctx: &SessionContext,
    stream: SendableRecordBatchStream,
) -> u64 {
    use datafusion::datasource::TableProvider;
    let partition: Arc<dyn datafusion::physical_plan::streaming::PartitionStream> =
        Arc::new(OneShot {
            schema: schema(),
            stream: std::sync::Mutex::new(Some(stream)),
        });
    let source = Arc::new(
        datafusion::physical_plan::streaming::StreamingTableExec::try_new(
            schema(),
            vec![partition],
            None,
            Vec::new(),
            false,
            None,
        )
        .expect("source"),
    );
    let plan = provider
        .insert_into(
            &ctx.state(),
            source,
            datafusion::logical_expr::dml::InsertOp::Append,
        )
        .await
        .expect("plan");
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("append");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("count")
        .value(0)
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
    let policy = arg("--policy", "keep_last");
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
    let progress_rows: usize = arg("--progress-rows", "0").parse().expect("progress rows");
    let memory_limit_mb: usize = arg("--memory-limit-mb", "0").parse().expect("memory limit");
    let skip_queries = std::env::args().any(|a| a == "--skip-queries");
    let append_mode = std::env::args().any(|a| a == "--append");
    let ctx = if memory_limit_mb == 0 {
        SessionContext::new()
    } else {
        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(
                datafusion::execution::memory_pool::GreedyMemoryPool::new(
                    memory_limit_mb * 1024 * 1024,
                ),
            ))
            .build_arc()
            .expect("runtime env");
        SessionContext::new_with_config_rt(datafusion::prelude::SessionConfig::new(), runtime)
    };
    let key = ColumnReference::new(key_columns());
    let on_conflict = match policy.as_str() {
        "none" => None,
        "keep_last" => Some(OnConflict::Upsert(key)),
        other => panic!("unknown policy {other}"),
    };
    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
        .create(CreateTableOptions {
            table_name: "t".to_string(),
            schema: schema(),
            primary_key: key_columns(),
            on_conflict,
            base_path: dir.path().join("data").display().to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                deletion_mode,
                inline_max_rows: 0,
                compaction_background_interval_ms: 3_600_000,
                stream_publish_interval_ms: if append_mode
                    && !std::env::args().any(|a| a == "--segmented")
                {
                    0
                } else {
                    VortexConfig::default().stream_publish_interval_ms
                },
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
        if append_mode {
            written = append(&provider, &ctx, source(keys, passes, progress_rows)).await;
            refresh_s = start.elapsed().as_secs_f64();
            if refreshes > 1 {
                println!("  append {refresh} of {refreshes}: refresh_s={refresh_s:.2}");
            }
            continue;
        }
        let prepared = provider
            .begin_overwrite(
                source(keys, passes, progress_rows),
                ctx.state().config().target_partitions(),
            )
            .await
            .expect("refresh");
        written = prepared.row_count();
        let begun = start.elapsed();
        let phase = Instant::now();
        prepared.apply_owned_txn().await.expect("commit");
        let committed = phase.elapsed();
        let phase = Instant::now();
        prepared.finish().await.expect("publish");
        eprintln!(
            "PHASE begin_overwrite {begun:?} commit {committed:?} publish {:?}",
            phase.elapsed()
        );
        refresh_s = start.elapsed().as_secs_f64();
        if refreshes > 1 {
            println!("  refresh {refresh} of {refreshes}: refresh_s={refresh_s:.2}");
        }
    }
    // A later, smaller append over the first keys: what the next refresh pays
    // to check its keys against the rows the first load left.
    let second_keys: usize = arg("--second-append-keys", "0")
        .parse()
        .expect("second keys");
    if second_keys > 0 {
        let start = Instant::now();
        let rows = append(&provider, &ctx, source(second_keys, 1, 0)).await;
        println!(
            "second_append keys={second_keys} rows={rows} second_append_s={:.2}",
            start.elapsed().as_secs_f64()
        );
    }
    let layers = catalog
        .get_all_snapshot_sequences(provider.table_id())
        .await
        .expect("layers")
        .len();
    println!(
        "mode={} policy={policy} deletion_mode={deletion_mode:?} keys={keys} passes={passes} rows_in={} rows_written={written} layers={layers} refresh_s={refresh_s:.2} rows_per_s={:.0}",
        if append_mode { "append" } else { "overwrite" },
        total_rows(keys, passes),
        total_rows(keys, passes) as f64 / refresh_s,
    );
    if std::env::args().any(|a| a == "--dup-query") {
        // The cost of finding repeated keys after the write, as a post-pass
        // would: a parallel aggregate over the key column.
        let ctx =
            SessionContext::new_with_config_rt(ctx.state().config().clone(), ctx.runtime_env());
        ctx.register_table(
            "t",
            Arc::clone(&provider) as Arc<dyn datafusion::datasource::TableProvider>,
        )
        .expect("register");
        let plan = ctx
            .sql("EXPLAIN ANALYZE SELECT id FROM t GROUP BY id HAVING COUNT(*) > 1")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("explain");
        println!(
            "{}",
            arrow::util::pretty::pretty_format_batches(&plan).expect("format")
        );
        for attempt in 1..=3 {
            let start = Instant::now();
            let repeated = ctx
                .sql("SELECT id FROM t GROUP BY id HAVING COUNT(*) > 1")
                .await
                .expect("plan")
                .collect()
                .await
                .expect("query")
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>();
            println!(
                "  dup_query attempt={attempt} repeated_keys={repeated} s={:.2}",
                start.elapsed().as_secs_f64()
            );
        }
    }
    let sql = arg("--sql", "");
    if !sql.is_empty() {
        let ctx =
            SessionContext::new_with_config_rt(ctx.state().config().clone(), ctx.runtime_env());
        ctx.register_table(
            "t",
            Arc::clone(&provider) as Arc<dyn datafusion::datasource::TableProvider>,
        )
        .expect("register");
        for attempt in 1..=3 {
            let start = Instant::now();
            let batches = ctx
                .sql(&sql)
                .await
                .expect("plan")
                .collect()
                .await
                .expect("query");
            println!(
                "  sql attempt={attempt} s={:.2} result={}",
                start.elapsed().as_secs_f64(),
                arrow::util::pretty::pretty_format_batches(&batches)
                    .expect("format")
                    .to_string()
                    .replace('\n', " ")
            );
        }
    }
    if skip_queries {
        return;
    }
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
        total_rows(keys, passes).div_ceil(BATCH)
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
