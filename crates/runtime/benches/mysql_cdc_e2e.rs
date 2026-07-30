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

//! End-to-end `MySQL` CDC throughput: replay a REAL captured binlog file
//! through the production envelope shape (`mysql_replication::replay`, the
//! same envelopes the shared pump delivers) into the real apply loop
//! (`RefreshTask::start_changes_stream`) writing a Cayenne accelerator —
//! decode + coalesce + upsert validate + mem-tier write, everything
//! downstream of the network.
//!
//! Fixture: set `MYSQL_BINLOG_REPLAY_FILE` to a binlog file captured from a CH-benCH
//! `MySQL` source (the `chbench.order_line` table must appear in it). The arm
//! replays the first `MYSQL_BINLOG_REPLAY_BENCH_BYTES` (default 64 MiB) and reports
//! bytes/second of binlog processed — the end-to-end yardstick for CDC
//! throughput work (see `docs/mysql-cdc-sf1000-root-cause.md` in
//! `tools/chbench-driver`).

// Bench setup has no error path worth threading — a failed fixture should
// abort the run loudly.
#![expect(clippy::expect_used, reason = "bench setup panics are acceptable")]

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{Criterion, Throughput};
use data_components::cdc::{ChangeEnvelope, ChangesStream, StreamError};
use data_components::mysql_replication::replay::{ReplayTable, replay_binlog_envelopes};
use data_components::mysql_replication::setup::{SourceColumn, TableLayout};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;
use futures::StreamExt;
use futures::stream as fstream;
use runtime::accelerated_table::refresh::Refresh;
use runtime::accelerated_table::refresh_task::{RefreshTask, RefreshTaskBuilder};
use runtime::federated_table::FederatedTable;
use runtime::status::RuntimeStatus;
use tempfile::TempDir;
use tokio::runtime::{Handle, Runtime as TokioRuntime};
use tokio::sync::RwLock;

const DEFAULT_BENCH_BYTES: usize = 64 * 1024 * 1024;

fn order_line_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ol_o_id", DataType::Int32, false),
        Field::new("ol_d_id", DataType::Int32, false),
        Field::new("ol_w_id", DataType::Int32, false),
        Field::new("ol_number", DataType::Int32, false),
        Field::new("ol_i_id", DataType::Int32, false),
        Field::new("ol_supply_w_id", DataType::Int32, false),
        Field::new(
            "ol_delivery_d",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("ol_quantity", DataType::Int32, false),
        Field::new("ol_amount", DataType::Decimal128(6, 2), false),
        Field::new("ol_dist_info", DataType::Utf8, false),
        Field::new(
            "_bench_ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn source_col(name: &str, column_type: &str, is_primary_key: bool) -> SourceColumn {
    SourceColumn {
        name: name.to_string(),
        column_type: column_type.to_string(),
        enum_variants: None,
        set_variants: None,
        is_primary_key,
    }
}

fn order_line_layout() -> TableLayout {
    TableLayout {
        columns: vec![
            source_col("ol_o_id", "int", true),
            source_col("ol_d_id", "int", true),
            source_col("ol_w_id", "int", true),
            source_col("ol_number", "int", true),
            source_col("ol_i_id", "int", false),
            source_col("ol_supply_w_id", "int", false),
            source_col("ol_delivery_d", "timestamp", false),
            source_col("ol_quantity", "int", false),
            source_col("ol_amount", "decimal(6,2)", false),
            source_col("ol_dist_info", "char(24)", false),
            source_col("_bench_ts", "datetime(3)", false),
        ],
    }
}

fn order_line_primary_keys() -> Vec<String> {
    ["ol_w_id", "ol_d_id", "ol_o_id", "ol_number"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

struct CayenneFixture {
    _temp: TempDir,
    table: Arc<CayenneTableProvider>,
    table_name: String,
}

async fn make_cayenne_fixture(table_name: &str) -> CayenneFixture {
    let temp = TempDir::new().expect("temp dir");
    let data_path = temp.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = temp.path().join("test.db");
    let conn = format!("sqlite://{}", db_path.to_string_lossy());

    let catalog = Arc::new(CayenneCatalog::new(conn).expect("CayenneCatalog::new"));
    catalog.init().await.expect("catalog init");

    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(
        Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
        CreateTableOptions {
            table_name: table_name.to_string(),
            schema: order_line_schema(),
            primary_key: order_line_primary_keys(),
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(
                order_line_primary_keys(),
            ))),
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await
    .expect("create_table");

    CayenneFixture {
        _temp: temp,
        table: Arc::new(table),
        table_name: table_name.to_string(),
    }
}

fn make_refresh_task(fixture: &CayenneFixture) -> RefreshTask {
    let accelerator: Arc<dyn TableProvider> = Arc::clone(&fixture.table) as _;
    let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));
    RefreshTaskBuilder::new(
        RuntimeStatus::new(),
        TableReference::bare(fixture.table_name.clone()),
        federated,
        None,
        accelerator,
        Handle::current(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .build()
}

/// Replayed envelopes for one iteration. Envelopes are single-use (their
/// payloads move into the apply), so each iteration re-replays outside the
/// timed region. The replay materializes batches exactly as the pump's
/// delivery does, so the timed region measures pure apply — the consumer's
/// ceiling when pump-side decode runs concurrently on its own task, as in
/// production.
fn replay_envelopes(binlog: &[u8], cap: usize) -> Vec<ChangeEnvelope> {
    let table = ReplayTable {
        database: "chbench".to_string(),
        table: "order_line".to_string(),
        schema: order_line_schema(),
        primary_keys: order_line_primary_keys(),
        layout: order_line_layout(),
    };
    replay_binlog_envelopes(binlog, &table, Some(cap)).expect("replay parses")
}

fn main() {
    let Some(path) = std::env::var_os("MYSQL_BINLOG_REPLAY_FILE") else {
        eprintln!(
            "mysql_cdc_e2e: set MYSQL_BINLOG_REPLAY_FILE to a captured CH-benCH binlog file \
             (see docs in tools/chbench-driver); skipping"
        );
        return;
    };
    let binlog = std::fs::read(&path).expect("read binlog file");
    let cap: usize = std::env::var("MYSQL_BINLOG_REPLAY_BENCH_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_BENCH_BYTES)
        .min(binlog.len());

    let replay_start = std::time::Instant::now();
    let probe = replay_envelopes(&binlog, cap);
    let replay_elapsed = replay_start.elapsed();
    let rows: usize = probe.iter().map(ChangeEnvelope::num_rows_hint).sum();
    eprintln!(
        "mysql_cdc_e2e: {} envelopes (~{rows} rows) from the first {} MiB; \
         replay (producer side) took {replay_elapsed:?} = {:.1} MiB/s",
        probe.len(),
        cap / (1024 * 1024),
        f64::from(u32::try_from(cap).expect("bench byte cap fits in u32"))
            / (1024.0 * 1024.0)
            / replay_elapsed.as_secs_f64(),
    );
    drop(probe);

    let rt = TokioRuntime::new().expect("tokio runtime");
    let mut criterion = Criterion::default().configure_from_args();
    let mut group = criterion.benchmark_group("mysql_cdc_e2e");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(cap as u64));
    group.bench_function("order_line_apply", |b| {
        b.iter_batched(
            || {
                let envelopes: Vec<Result<ChangeEnvelope, StreamError>> =
                    replay_envelopes(&binlog, cap).into_iter().map(Ok).collect();
                let table_name = format!("ol_bench_{}", uuid::Uuid::now_v7());
                let fixture = rt.block_on(make_cayenne_fixture(&table_name));
                (envelopes, fixture)
            },
            |(envelopes, fixture)| {
                rt.block_on(async {
                    let task = make_refresh_task(&fixture);
                    let stream: ChangesStream = fstream::iter(envelopes).boxed();
                    let refresh = Arc::new(RwLock::new(Refresh::default()));
                    task.start_changes_stream(
                        refresh,
                        stream,
                        None,
                        None,
                        Arc::new(AtomicBool::new(false)),
                    )
                    .await
                    .expect("start_changes_stream");
                    black_box(fixture);
                });
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
    criterion.final_summary();
}
