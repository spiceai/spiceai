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

//! Planning cost of a time-windowed query as the cold tier accumulates files.
//!
//! Ignored by default: it is a measurement, not a pass/fail check. Run with
//! `cargo nextest run -p cayenne --test integration -E 'test(/cold_planning_cost/)' --run-ignored only --no-capture`.

use crate::common;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

const TABLE: &str = "planning_t";
const ROWS_PER_PROMOTION: i64 = 20_000;
const PLANS_PER_SAMPLE: usize = 40;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("time_unix_nano", DataType::Int64, false),
        Field::new("point_hash", DataType::Int64, false),
        Field::new("cluster", DataType::Utf8, false),
        Field::new("verb", DataType::Utf8, false),
        Field::new("code", DataType::Utf8, false),
        Field::new("resource", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("count", DataType::Int64, false),
    ]))
}

fn batch(start: i64) -> TestResult<RecordBatch> {
    let ts: Vec<i64> = (start..start + ROWS_PER_PROMOTION).map(|i| i * 1_000_000).collect();
    let hash: Vec<i64> = (start..start + ROWS_PER_PROMOTION).map(|i| i.wrapping_mul(0x9E37_79B9)).collect();
    let pick = |vals: &[&str], salt: i64| -> StringArray {
        (start..start + ROWS_PER_PROMOTION)
            .map(|i| Some(vals[usize::try_from((i + salt).rem_euclid(vals.len() as i64)).expect("idx")]))
            .collect()
    };
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(Int64Array::from(hash)),
            Arc::new(pick(&["prod-00", "prod-01", "prod-02"], 0)),
            Arc::new(pick(&["GET", "LIST", "WATCH", "PUT", "POST", "DELETE"], 1)),
            Arc::new(pick(&["200", "201", "404", "500", "503"], 2)),
            Arc::new(pick(&["pods", "nodes", "services", "deployments"], 3)),
            Arc::new(Float64Array::from_iter_values(
                (start..start + ROWS_PER_PROMOTION).map(|i| (i % 997) as f64 * 0.5),
            )),
            Arc::new(Int64Array::from_iter_values((start..start + ROWS_PER_PROMOTION).map(|i| i % 13))),
        ],
    )?)
}

async fn create_table(fixture: &common::TestFixture, ctx: &SessionContext) -> TestResult<Arc<CayenneTableProvider>> {
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;
    let options = CreateTableOptions {
        table_name: TABLE.to_string(),
        schema: schema(),
        primary_key: vec!["time_unix_nano".to_string(), "point_hash".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cold_tier_location: Some(format!("file://{}", cold_dir.to_string_lossy())),
            cold_tier_warm_max_files: 1,
            // Small target so each promotion lands in its own cold file.
            cold_target_file_size_mb: 1,
            deletion_mode: DeletionMode::Key,
            ..VortexConfig::default()
        },
    };
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table(TABLE, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok(table)
}

/// P50 and P90 of planning `PLANS_PER_SAMPLE` copies of a query over the newest
/// 1% of the time range (the dashboard shape: a recent window, most files outside it).
async fn plan_times(ctx: &SessionContext, newest: i64) -> TestResult<(Duration, Duration)> {
    let lower = (newest - ROWS_PER_PROMOTION / 2) * 1_000_000;
    let sql = format!(
        "SELECT verb, SUM(value) FROM {TABLE} WHERE time_unix_nano >= {lower} AND cluster = 'prod-00' GROUP BY verb"
    );
    let mut times = Vec::with_capacity(PLANS_PER_SAMPLE);
    for _ in 0..PLANS_PER_SAMPLE {
        let started = Instant::now();
        ctx.sql(&sql).await?.create_physical_plan().await?;
        times.push(started.elapsed());
    }
    times.sort();
    Ok((times[times.len() / 2], times[times.len() * 9 / 10]))
}

#[test]
#[ignore = "measurement; run explicitly"]
fn test_cold_planning_cost_grows_with_cold_files_sqlite() -> Result<(), String> {
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        test_cold_planning_cost_grows_with_cold_files_impl,
    )
}

async fn test_cold_planning_cost_grows_with_cold_files_impl(fixture: common::TestFixture) -> TestResult<()> {
    let ctx = SessionContext::new();
    let table = create_table(&fixture, &ctx).await?;
    let mut next = 0_i64;
    for target in [20_usize, 80, 160] {
        loop {
            let files = fixture.catalog.list_cold_tier_files(table.table_id()).await?.len();
            if files >= target {
                break;
            }
            common::insert_batch(table.as_ref(), batch(next)?).await?;
            next += ROWS_PER_PROMOTION;
            let _ = table.checkpoint_inlined_data().await;
            let _ = table.checkpoint_mem_tier().await;
            assert!(table.promote_warm_to_cold().await?, "promotion should fire");
        }
        let files = fixture.catalog.list_cold_tier_files(table.table_id()).await?.len();
        let (p50, p90) = plan_times(&ctx, next).await?;
        eprintln!("COLD_PLANNING files={files} plan_p50_us={} plan_p90_us={}", p50.as_micros(), p90.as_micros());
    }
    Ok(())
}
