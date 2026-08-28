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

//! Regression test for #13657: a rapidly refreshing table's on-disk footprint
//! must converge to its live size.
//!
//! Every whole-table replace publishes a new snapshot directory and supersedes
//! the previous one. Reclaiming those used to be deferred by a fixed two-minute
//! grace, with the sweep anchored on the snapshot id captured when it was
//! queued — so a pass could only ever delete dirs older than two minutes, and
//! steady-state garbage was `grace x commit rate`, unbounded in rate. On this
//! test's shape that left every replace's directory on disk.
//!
//! Drives the replaces through the real path (the sweep the provider schedules
//! for itself, not a test-only entry point) and asserts convergence.

#![allow(clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::SessionContext;
use datafusion_catalog::TableProvider;
use tempfile::TempDir;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Replaces to drive. Well above the handful the sweep may legitimately keep,
/// so a regression (retaining one dir per replace) cannot pass by luck.
const REPLACES: usize = 24;

/// Dirs the sweep may legitimately still hold: the live snapshot, plus headroom
/// for one just-published successor.
const MAX_RETAINED_DIRS: usize = 2;

/// Rows each replace writes, so the final read can assert the surviving
/// generation is complete rather than only counting directories.
const ROWS_PER_REPLACE: i64 = 2_000;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Rows big enough that the replace writes Vortex files rather than riding in
/// the metastore inline tier — the file tier is what accumulates.
fn batch(generation: i64) -> RecordBatch {
    let ids: Vec<i64> = (0..ROWS_PER_REPLACE).collect();
    let values: Vec<i64> = ids.iter().map(|id| id + generation).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("valid batch")
}

async fn overwrite(table: &Arc<CayenneTableProvider>, generation: i64) -> TestResult<()> {
    let ctx = SessionContext::new();
    let exec = MemorySourceConfig::try_new_exec(&[vec![batch(generation)]], schema(), None)?;
    let plan = table
        .insert_into(&ctx.state(), exec, InsertOp::Overwrite)
        .await?;
    datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

fn snapshot_dirs(table_dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(table_dir) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect()
}

fn bytes_under(dir: &Path) -> u64 {
    snapshot_dirs(dir)
        .iter()
        .flat_map(|snapshot| std::fs::read_dir(snapshot).into_iter().flatten())
        .filter_map(Result::ok)
        .filter_map(|entry| entry.metadata().ok())
        .map(|meta| meta.len())
        .sum()
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_replaces_converge_to_the_live_footprint() -> TestResult<()> {
    let tmp = TempDir::new()?;
    let db = tmp.path().join("cleanup.db");
    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}",
        db.to_string_lossy()
    ))?);
    catalog.init().await?;

    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "replaced".to_string(),
                schema: schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: tmp.path().to_string_lossy().to_string(),
                partition_column: None,
                // Inlining would keep the rows in the metastore and leave the
                // snapshot dirs empty, which is not the shape under test.
                vortex_config: VortexConfig {
                    inline_max_rows: 0,
                    ..VortexConfig::default()
                },
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    ctx.register_table("replaced", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    let table_dir = tmp.path().join(table.table_id());

    // Drive the replaces as fast as they will go: the whole point is that the
    // retained footprint does not scale with commit rate.
    for generation in 0..REPLACES {
        overwrite(&table, i64::try_from(generation).unwrap_or(0)).await?;
    }

    let live_bytes = {
        let current = snapshot_dirs(&table_dir);
        assert!(
            current.len() > 1,
            "the replaces should have published more than one snapshot dir, saw {}",
            current.len()
        );
        bytes_under(&table_dir) / current.len() as u64
    };
    assert!(live_bytes > 0, "each replace must write Vortex bytes");

    // The sweep defers a dir younger than its grace and re-arms itself once, so
    // convergence lands a grace after the last commit. Poll rather than sleep a
    // fixed span: the assertion is that it converges, not when.
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let dirs = snapshot_dirs(&table_dir);
        // Zero dirs is a failure, not convergence: it would mean the sweep took
        // the live snapshot along with the superseded ones.
        assert!(
            !dirs.is_empty(),
            "the sweep deleted every snapshot dir, including the live one"
        );
        if dirs.len() <= MAX_RETAINED_DIRS {
            let current = table_dir.join(&catalog.get_table("replaced").await?.current_snapshot_id);
            assert!(
                current.is_dir(),
                "the live snapshot dir {} must survive the sweep; {} dir(s) remain",
                current.display(),
                dirs.len()
            );
            let bytes = bytes_under(&table_dir);
            assert!(
                bytes <= live_bytes * MAX_RETAINED_DIRS as u64,
                "footprint {bytes} B should be within {MAX_RETAINED_DIRS}x the live \
                 {live_bytes} B after converging to {} dir(s)",
                dirs.len()
            );
            // Scan the surviving generation: a sweep that unlinked live files
            // would still leave the right directory count.
            let scanned = ctx
                .sql("SELECT count(*) AS n FROM replaced")
                .await?
                .collect()
                .await?;
            let rows = scanned
                .first()
                .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
                .map(|counts| counts.value(0))
                .expect("count(*) returns one Int64 row");
            assert_eq!(
                rows, ROWS_PER_REPLACE,
                "the surviving snapshot must still scan its full generation"
            );
            return Ok(());
        }
        assert!(
            Instant::now() < deadline,
            "on-disk snapshot dirs never converged: {} still present after {REPLACES} \
             replaces ({} B on disk, live is ~{live_bytes} B). Superseded dirs are being \
             retained for a fixed grace instead of being swept against live state.",
            dirs.len(),
            bytes_under(&table_dir)
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
