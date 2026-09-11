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

#![allow(clippy::expect_used)]

//! The byte-size statistics a file scan reports must not depend on which source
//! served them.
//!
//! `CayenneTableProvider::collect_scan_file_statistics` has two sources for the
//! same Vortex file: the file's own footer, and the `cayenne_snapshot_file_statistics`
//! blob the footer path writes. `JoinSelection` compares `total_byte_size` before
//! anything else, so if the two sources disagree the build side of a join is
//! decided by which source happened to serve — which changes across a restart and
//! across concurrent scans of one plan (regression test for #13829).

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, SnapshotFileStatistics, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, Statistics};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

const TABLE: &str = "file_stats_source";

test_with_backends!(file_scan_byte_size_statistics_do_not_depend_on_their_source);

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

async fn insert_rows(table: &CayenneTableProvider, range: std::ops::Range<i64>) -> TestResult<()> {
    let ids: Vec<i64> = range.clone().collect();
    let names: Vec<String> = range.map(|i| format!("name-{i}")).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )?;
    common::insert_batch(table, batch).await?;
    Ok(())
}

/// The `total_byte_size` and per-column `byte_size` a plain scan reports.
async fn scan_statistics(
    table: &Arc<CayenneTableProvider>,
    ctx: &SessionContext,
) -> TestResult<(Precision<usize>, Vec<Precision<usize>>)> {
    let plan = table.scan(&ctx.state(), None, &[], None).await?;
    let stats = plan.partition_statistics(None)?;
    let per_column = stats
        .column_statistics
        .iter()
        .map(|c| c.byte_size)
        .collect();
    Ok((stats.total_byte_size, per_column))
}

async fn file_scan_byte_size_statistics_do_not_depend_on_their_source(
    fixture: common::TestFixture,
) -> TestResult<()> {
    // Session 1 — create the table, land the rows in a durable Vortex file, and
    // scan. No blob exists yet, so this scan reads the file's footer and, on its
    // way out, persists the blob every later scan will be served from.
    let ctx = SessionContext::new();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(
        CayenneTableProvider::create_table(
            catalog,
            CreateTableOptions {
                table_name: TABLE.to_string(),
                schema: schema(),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    insert_rows(&table, 0..512).await?;
    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;
    table.flush_pending_maintenance().await?;

    let (footer_total, footer_columns) = scan_statistics(&table, &ctx).await?;

    // The footer path is only interesting if it reports a size at all — if it
    // did not, the two sources would agree trivially and this test would pass
    // while proving nothing.
    assert!(
        matches!(footer_total, Precision::Exact(_) | Precision::Inexact(_)),
        "the footer path must report a total byte size for this test to mean anything, got {footer_total:?}"
    );

    // Session 2 — a fresh catalog connection and a fresh provider over the same
    // metastore and the same files: the restart case, where every file is served
    // from the persisted blob rather than its footer.
    let catalog = Arc::new(CayenneCatalog::new(fixture.connection_string())?);
    catalog.init().await?;
    let ctx = SessionContext::new();
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .open(TABLE)
        .await?,
    );

    let (blob_total, blob_columns) = scan_statistics(&reopened, &ctx).await?;

    assert_eq!(
        blob_total, footer_total,
        "the same file must report the same total byte size whichever source served it"
    );
    assert_eq!(
        blob_columns, footer_columns,
        "the same file must report the same per-column byte sizes whichever source served it"
    );

    Ok(())
}

test_with_backends!(a_blob_without_byte_sizes_is_re_inferred_from_its_footer);

/// Build a blob shaped like one written before per-column byte sizes were
/// persisted: every column's `byte_size` absent, so the restored total is too.
fn legacy_blob(num_rows: i64) -> Vec<u8> {
    let column_statistics = schema()
        .fields()
        .iter()
        .map(|_| ColumnStatistics {
            null_count: Precision::Absent,
            min_value: Precision::Absent,
            max_value: Precision::Absent,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        })
        .collect();
    let stats = Statistics {
        num_rows: Precision::Exact(usize::try_from(num_rows).unwrap_or(0)),
        total_byte_size: Precision::Absent,
        column_statistics,
    };
    cayenne::stats::statistics_to_persisted_blob(&stats, &schema()).expect("legacy blob serializes")
}

/// The migration path: rows already in `cayenne_snapshot_file_statistics` carry no
/// byte sizes, so serving them would keep reporting a size the footer disagrees
/// with for the whole life of an existing installation. Such a blob has to be
/// re-inferred from the footer *and* rewritten, or the fix reaches only files
/// written after it.
async fn a_blob_without_byte_sizes_is_re_inferred_from_its_footer(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let ctx = SessionContext::new();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(
        CayenneTableProvider::create_table(
            catalog,
            CreateTableOptions {
                table_name: TABLE.to_string(),
                schema: schema(),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    insert_rows(&table, 0..512).await?;
    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;
    table.flush_pending_maintenance().await?;

    let (footer_total, _) = scan_statistics(&table, &ctx).await?;
    assert!(
        matches!(footer_total, Precision::Exact(_) | Precision::Inexact(_)),
        "the footer path must report a total for this test to mean anything, got {footer_total:?}"
    );

    // Overwrite every per-file row with a pre-change blob, which is the state an
    // installation that upgrades into this change is already in.
    let table_id = table.table_id().to_string();
    // The manifest rows are published by the checkpoint/maintenance passes above, and
    // `flush_pending_maintenance` does not guarantee they are committed by the time it
    // returns. Poll for them rather than asserting once: on a loaded runner the first
    // read comes back empty, which is a readiness race in this test and not a missing
    // file (spiceai/spiceai#13906 is the same shape in a neighbouring suite).
    let mut files = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        files = fixture.catalog.get_all_snapshot_files(&table_id).await?;
        if !files.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        !files.is_empty(),
        "the settle must have produced a data file within 30s; \
         `get_all_snapshot_files` still returns no manifest row for table {table_id}"
    );
    let scan_snapshot_id = files[0].snapshot_id.clone();
    // Per-file statistics rows are keyed by the object-store location, which is the
    // store-relative path; the manifest carries only the bare filename.
    let stats_key = |file: &cayenne::metadata::SnapshotFile| {
        format!(
            "{}/{}/{}/{}",
            fixture
                .data_path
                .to_string_lossy()
                .trim_start_matches('/')
                .trim_end_matches('/'),
            table_id,
            file.snapshot_id,
            file.file_path
        )
    };
    for file in &files {
        fixture
            .catalog
            .upsert_snapshot_file_statistics(&SnapshotFileStatistics {
                table_id: table_id.clone(),
                snapshot_id: scan_snapshot_id.clone(),
                file_path: stats_key(file),
                file_size_bytes: file.file_size_bytes,
                num_rows: file.row_count,
                statistics_blob: legacy_blob(file.row_count),
            })
            .await?;
    }

    // The seed really is a legacy row: restoring it yields no total.
    let seeded = fixture
        .catalog
        .get_snapshot_file_statistics(&table_id, &scan_snapshot_id, &stats_key(&files[0]))
        .await?
        .expect("seeded row is present");
    let stored_schema = table.schema();
    let seeded_stats = cayenne::stats::file_statistics_to_df(
        &cayenne::stats::deserialize_file_statistics(&seeded.statistics_blob, &stored_schema)?,
        seeded.num_rows,
    );
    assert_eq!(
        seeded_stats.total_byte_size,
        Precision::Absent,
        "the seeded blob must carry no total, or this test proves nothing"
    );

    // A fresh provider over that metastore must not serve the legacy row.
    let catalog = Arc::new(CayenneCatalog::new(fixture.connection_string())?);
    catalog.init().await?;
    let ctx = SessionContext::new();
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .open(TABLE)
        .await?,
    );

    let (migrated_total, _) = scan_statistics(&reopened, &ctx).await?;
    assert_eq!(
        migrated_total, footer_total,
        "a blob with no total must be re-inferred from the footer, not served as absent"
    );

    // ...and the row must be rewritten, so the next process does not re-infer again.
    // `get_all_snapshot_files` spans every snapshot, so ask which of the seeded rows
    // now carries a total rather than assuming the first row is the live one.
    let mut rewritten_rows = 0;
    for file in &files {
        let row = catalog
            .get_snapshot_file_statistics(&table_id, &scan_snapshot_id, &stats_key(file))
            .await?
            .expect("seeded row is still present");
        let stats = cayenne::stats::file_statistics_to_df(
            &cayenne::stats::deserialize_file_statistics(&row.statistics_blob, &stored_schema)?,
            row.num_rows,
        );
        if stats.total_byte_size != Precision::Absent {
            rewritten_rows += 1;
        }
    }
    assert!(
        rewritten_rows > 0,
        "re-inference must persist the size back for the file it read, or every process re-reads the footer (seeded {} rows, none rewritten)",
        files.len()
    );

    Ok(())
}
