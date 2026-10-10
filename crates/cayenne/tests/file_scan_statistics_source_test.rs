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
    let stats = datafusion::physical_plan::StatisticsContext::new().compute(
        plan.as_ref(),
        &datafusion::physical_plan::StatisticsArgs::new(),
    )?;
    let per_column = stats
        .column_statistics
        .iter()
        .map(|c| c.byte_size)
        .collect();
    Ok((stats.total_byte_size, per_column))
}

/// Compare actual rows only after the statistics-source assertions, so the
/// query cannot populate the blob used as the first footer reference.
async fn assert_input_rows(table: &Arc<CayenneTableProvider>) -> TestResult<()> {
    let ctx = SessionContext::new();
    ctx.register_table("input_rows", Arc::clone(table) as Arc<dyn TableProvider>)?;
    let widened = table.schema().fields().len() == 3;
    let sql = if widened {
        "SELECT id, name, extra FROM input_rows ORDER BY id"
    } else {
        "SELECT id, name FROM input_rows ORDER BY id"
    };
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut actual = Vec::new();
    for batch in batches {
        assert_eq!(
            batch.column(0).null_count(),
            0,
            "input keys must remain non-NULL"
        );
        assert_eq!(
            batch.column(1).null_count(),
            0,
            "input values must remain non-NULL"
        );
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id array");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name array");
        if widened {
            assert_eq!(
                batch.column(2).null_count(),
                batch.num_rows(),
                "the added column is NULL for every original row"
            );
        }
        for row in 0..batch.num_rows() {
            actual.push((ids.value(row), names.value(row).to_string()));
        }
    }
    let expected: Vec<_> = (0..512).map(|id| (id, format!("name-{id}"))).collect();
    assert_eq!(actual, expected, "every input row survives reopen");
    eprintln!(
        "POST_STATISTICS_ROWS count={} first={:?} last={:?} widened_extra_all_null={widened}",
        actual.len(),
        actual.first(),
        actual.last()
    );
    Ok(())
}

/// Wait for the manifest rows needed to author legacy or poisoned blobs.
async fn await_manifest_rows(
    fixture: &common::TestFixture,
    table_id: &str,
) -> TestResult<Vec<cayenne::metadata::SnapshotFile>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let files = fixture.catalog.get_all_snapshot_files(table_id).await?;
        if !files.is_empty() {
            return Ok(files);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the settle must have produced a data file within 30s; \
             `get_all_snapshot_files` still returns no manifest row for table {table_id}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
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
                vortex_config: VortexConfig {
                    inline_max_rows: 0,
                    ..VortexConfig::default()
                },
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    insert_rows(&table, 0..512).await?;
    table.drain_in_flight_maintenance().await?;
    assert_eq!(table.checkpoint_inlined_data().await?, 0);
    assert_eq!(table.checkpoint_mem_tier().await?, 0);
    table.drain_in_flight_maintenance().await?;

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

    assert_input_rows(&reopened).await?;

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
                vortex_config: VortexConfig {
                    inline_max_rows: 0,
                    ..VortexConfig::default()
                },
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    insert_rows(&table, 0..512).await?;
    table.drain_in_flight_maintenance().await?;
    assert_eq!(table.checkpoint_inlined_data().await?, 0);
    assert_eq!(table.checkpoint_mem_tier().await?, 0);
    table.drain_in_flight_maintenance().await?;

    let (footer_total, _) = scan_statistics(&table, &ctx).await?;
    assert!(
        matches!(footer_total, Precision::Exact(_) | Precision::Inexact(_)),
        "the footer path must report a total for this test to mean anything, got {footer_total:?}"
    );

    // Overwrite every per-file row with a pre-change blob, which is the state an
    // installation that upgrades into this change is already in.
    let table_id = table.table_id().to_string();
    let files = await_manifest_rows(&fixture, &table_id).await?;
    let scan_snapshot_id = files[0].snapshot_id.clone();
    let stats_key = |file: &cayenne::metadata::SnapshotFile| {
        common::statistics_row_key(&fixture.data_path, &table_id, file)
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
        &stored_schema,
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
            &stored_schema,
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

    assert_input_rows(&reopened).await?;

    Ok(())
}

test_with_backends!(a_widened_table_still_serves_its_files_from_the_persisted_blob);

const EVOLVED_TABLE: &str = "file_stats_source_evolved";

/// A per-column size no Vortex footer would produce, so a scan reporting it was
/// served from the persisted blob rather than from the file.
const POISON_BYTES: usize = 1_234_567;

fn evolved_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("extra", DataType::Int64, true),
    ]))
}

/// Widening a table leaves every file written before the widening without the new
/// column. The per-file blob those files persist can then never restore a
/// `total_byte_size` — `file_statistics_to_df` sums the per-column sizes and the
/// missing column has none — so a freshness check that reads that total rejects
/// the blob on every cold scan, for the life of the file. The scan still answers
/// correctly, from the footer, but the persisted row it exists to avoid re-reading
/// is re-read and rewritten every time (regression test for #13829).
async fn a_widened_table_still_serves_its_files_from_the_persisted_blob(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let ctx = SessionContext::new();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(
        CayenneTableProvider::create_table(
            catalog,
            CreateTableOptions {
                table_name: EVOLVED_TABLE.to_string(),
                schema: schema(),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig {
                    inline_max_rows: 0,
                    ..VortexConfig::default()
                },
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    insert_rows(&table, 0..512).await?;
    table.drain_in_flight_maintenance().await?;
    assert_eq!(table.checkpoint_inlined_data().await?, 0);
    assert_eq!(table.checkpoint_mem_tier().await?, 0);
    table.drain_in_flight_maintenance().await?;

    let evolution_ctx = arrow_tools::schema_evolution::EvolutionContext {
        constraint_columns: &[],
    };
    let plan =
        match arrow_tools::schema_evolution::classify(&schema(), &evolved_schema(), &evolution_ctx)
        {
            arrow_tools::schema_evolution::SchemaEvolution::Widening(plan) => plan,
            other => panic!("expected a widening classification, got {other:?}"),
        };
    table.evolve_schema_live(&plan).await?;

    // This scan takes the footer path (the widening cleared the per-file rows) and
    // writes the blob every later process is meant to be served from.
    let (footer_total, _) = scan_statistics(&table, &ctx).await?;
    assert!(
        matches!(footer_total, Precision::Exact(_) | Precision::Inexact(_)),
        "the footer path must report a total for this test to mean anything, got {footer_total:?}"
    );

    // Poison every per-file row with a size no footer would produce. A later scan
    // that reports it was served from the blob; one that reports the footer value
    // rejected the blob and re-read the file.
    let table_id = table.table_id().to_string();
    let stored_schema = table.schema();
    let files = await_manifest_rows(&fixture, &table_id).await?;
    let stats_key = |file: &cayenne::metadata::SnapshotFile| {
        common::statistics_row_key(&fixture.data_path, &table_id, file)
    };
    let mut poisoned = 0;
    for file in &files {
        let Some(row) = fixture
            .catalog
            .get_snapshot_file_statistics(&table_id, &file.snapshot_id, &stats_key(file))
            .await?
        else {
            continue;
        };
        let mut restored = cayenne::stats::file_statistics_to_df(
            &cayenne::stats::deserialize_file_statistics(&row.statistics_blob, &stored_schema)?,
            &stored_schema,
            row.num_rows,
        );
        restored.column_statistics[0].byte_size = Precision::Exact(POISON_BYTES);
        let blob = cayenne::stats::statistics_to_persisted_blob(&restored, &stored_schema)
            .expect("poisoned blob serializes");
        fixture
            .catalog
            .upsert_snapshot_file_statistics(&SnapshotFileStatistics {
                table_id: table_id.clone(),
                snapshot_id: file.snapshot_id.clone(),
                file_path: stats_key(file),
                file_size_bytes: row.file_size_bytes,
                num_rows: row.num_rows,
                statistics_blob: blob,
            })
            .await?;
        poisoned += 1;
    }
    assert!(
        poisoned > 0,
        "the footer scan must have persisted a row to poison, or this test proves nothing"
    );

    let catalog = Arc::new(CayenneCatalog::new(fixture.connection_string())?);
    catalog.init().await?;
    let ctx = SessionContext::new();
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .open(EVOLVED_TABLE)
        .await?,
    );
    let (_, blob_columns) = scan_statistics(&reopened, &ctx).await?;

    assert_eq!(
        blob_columns[0],
        Precision::Exact(POISON_BYTES),
        "a widened table's file must still be served from its persisted blob; \
         reporting the footer's size instead means the blob was rejected and the \
         file re-read, which repeats on every cold scan for the life of the file"
    );

    assert_input_rows(&reopened).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Concurrent cold scans share one collection per file (#13829)
// ---------------------------------------------------------------------------
//
// Two scans of a table that miss the statistics cache together — a self-join or
// a CTE read twice in one plan, or two concurrent queries — used to collect every
// file's statistics once each: each read the footer and each upserted the
// persisted row. These tests observe the footer reads through the object store
// and the upserts through triggers on the metastore table, so neither depends on
// the provider's own accounting.

test_with_backends!(concurrent_cold_scans_share_one_footer_read_and_upsert_per_file);
test_with_backends!(scan_file_statistics_counters_match_the_store_and_the_metastore);

const SINGLE_FLIGHT_TABLE: &str = "file_stats_single_flight";
const SINGLE_FLIGHT_FILES: usize = 4;
const ROWS_PER_FILE: i64 = 256;
const CONCURRENT_SCANS: usize = 6;

/// A primary-key-less table whose current snapshot holds `SINGLE_FLIGHT_FILES`
/// Vortex files, with no persisted statistics rows. Returns the table id.
async fn write_multi_file_table(fixture: &common::TestFixture) -> TestResult<String> {
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: SINGLE_FLIGHT_TABLE.to_string(),
                schema: schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig {
                    // One Vortex file per insert, and nothing to merge them.
                    inline_max_rows: 0,
                    inline_max_bytes: 0,
                    inline_max_buffer_bytes: 0,
                    compaction_trigger_files: 64,
                    compaction_background_interval_ms: 0,
                    ..VortexConfig::default()
                },
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    for file in 0..SINGLE_FLIGHT_FILES {
        let start = i64::try_from(file)? * ROWS_PER_FILE;
        insert_rows(&table, start..start + ROWS_PER_FILE).await?;
    }
    table.drain_in_flight_maintenance().await?;
    let table_id = table.table_id().to_string();
    // The manifest rows land with the write's maintenance; poll for them.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let files = fixture.catalog.get_all_snapshot_files(&table_id).await?;
        if files.len() == SINGLE_FLIGHT_FILES {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "each insert must land as its own file within 30s, or the per-file counts \
             below compare nothing; the manifest holds {files:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Ok(table_id)
}

/// Counts the reads of each Vortex file, so a test can see how many times a
/// scan read its footer.
#[derive(Debug, Default)]
struct FooterReadCounter {
    inner: object_store::local::LocalFileSystem,
    reads: parking_lot::Mutex<std::collections::BTreeMap<String, usize>>,
}

impl FooterReadCounter {
    fn reads(&self) -> std::collections::BTreeMap<String, usize> {
        self.reads.lock().clone()
    }
}

impl std::fmt::Display for FooterReadCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FooterReadCounter")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for FooterReadCounter {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if !options.head && location.as_ref().ends_with(".vortex") {
            *self
                .reads
                .lock()
                .entry(location.as_ref().to_string())
                .or_default() += 1;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A provider over the fixture's metastore with every cache cold — a fresh
/// catalog connection, a fresh provider, and a runtime that keeps no footers —
/// whose reads go through a fresh [`FooterReadCounter`].
async fn open_cold(
    fixture: &common::TestFixture,
) -> TestResult<(
    Arc<CayenneTableProvider>,
    SessionContext,
    Arc<FooterReadCounter>,
)> {
    let catalog = Arc::new(CayenneCatalog::new(fixture.connection_string())?);
    catalog.init().await?;
    let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .with_metadata_cache_limit(0)
        .build_arc()?;
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    let table = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .open(SINGLE_FLIGHT_TABLE)
        .await?,
    );
    let store = Arc::new(FooterReadCounter::default());
    ctx.runtime_env().register_object_store(
        &url::Url::parse("file:///")?,
        Arc::clone(&store) as Arc<dyn object_store::ObjectStore>,
    );
    Ok((table, ctx, store))
}

/// The full statistics of a plain scan's plan.
async fn plan_statistics(
    table: &Arc<CayenneTableProvider>,
    ctx: &SessionContext,
) -> TestResult<Statistics> {
    let plan = table.scan(&ctx.state(), None, &[], None).await?;
    let stats = datafusion::physical_plan::StatisticsContext::new().compute(
        plan.as_ref(),
        &datafusion::physical_plan::StatisticsArgs::new(),
    )?;
    Ok(Statistics::clone(&stats))
}

/// Writes to `cayenne_snapshot_file_statistics`, counted by triggers on the
/// `SQLite` metastore itself; `None` on a backend this cannot instrument.
struct StatisticsWriteLog {
    db_path: std::path::PathBuf,
}

impl StatisticsWriteLog {
    async fn install(fixture: &common::TestFixture) -> TestResult<Option<Self>> {
        if fixture.backend_type != common::BackendType::Sqlite {
            return Ok(None);
        }
        let db_path = fixture.db_path();
        let path = db_path.clone();
        tokio::task::spawn_blocking(move || -> rusqlite::Result<()> {
            let conn = rusqlite::Connection::open(path)?;
            conn.busy_timeout(std::time::Duration::from_secs(10))?;
            // An upsert fires the INSERT trigger for a new row and the UPDATE
            // trigger for an existing one — one log row per statement either way.
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS test_statistics_write_log (file_path TEXT NOT NULL);
                 CREATE TRIGGER IF NOT EXISTS test_statistics_write_log_insert
                   AFTER INSERT ON cayenne_snapshot_file_statistics
                   BEGIN INSERT INTO test_statistics_write_log VALUES (NEW.file_path); END;
                 CREATE TRIGGER IF NOT EXISTS test_statistics_write_log_update
                   AFTER UPDATE ON cayenne_snapshot_file_statistics
                   BEGIN INSERT INTO test_statistics_write_log VALUES (NEW.file_path); END;",
            )
        })
        .await??;
        Ok(Some(Self { db_path }))
    }

    /// Writes logged so far, per file.
    async fn writes(&self) -> TestResult<std::collections::BTreeMap<String, usize>> {
        let path = self.db_path.clone();
        let rows = tokio::task::spawn_blocking(
            move || -> rusqlite::Result<std::collections::BTreeMap<String, usize>> {
                let conn = rusqlite::Connection::open(path)?;
                conn.busy_timeout(std::time::Duration::from_secs(10))?;
                let mut statement = conn.prepare(
                    "SELECT file_path, COUNT(*) FROM test_statistics_write_log GROUP BY file_path",
                )?;
                let rows = statement
                    .query_map([], |row| {
                        let count: i64 = row.get(1)?;
                        Ok((
                            row.get::<_, String>(0)?,
                            usize::try_from(count).unwrap_or(0),
                        ))
                    })?
                    .collect::<rusqlite::Result<_>>()?;
                Ok(rows)
            },
        )
        .await??;
        Ok(rows)
    }

    async fn clear(&self) -> TestResult<()> {
        let path = self.db_path.clone();
        tokio::task::spawn_blocking(move || -> rusqlite::Result<()> {
            let conn = rusqlite::Connection::open(path)?;
            conn.busy_timeout(std::time::Duration::from_secs(10))?;
            conn.execute("DELETE FROM test_statistics_write_log", [])?;
            Ok(())
        })
        .await??;
        Ok(())
    }
}

async fn concurrent_cold_scans_share_one_footer_read_and_upsert_per_file(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let table_id = write_multi_file_table(&fixture).await?;
    let write_log = StatisticsWriteLog::install(&fixture).await?;
    let expected_rows = usize::try_from(i64::try_from(SINGLE_FLIGHT_FILES)? * ROWS_PER_FILE)?;

    // Reference: one cold scan alone reads each footer and persists each row.
    fixture
        .catalog
        .clear_snapshot_file_statistics(&table_id)
        .await?;
    if let Some(log) = &write_log {
        log.clear().await?;
    }
    let (table, ctx, store) = open_cold(&fixture).await?;
    let reference = plan_statistics(&table, &ctx).await?;
    let lone_reads = store.reads();
    let lone_writes = match &write_log {
        Some(log) => Some(log.writes().await?),
        None => None,
    };
    assert_eq!(
        lone_reads.len(),
        SINGLE_FLIGHT_FILES,
        "a cold scan reads every file's footer: {lone_reads:?}"
    );
    assert_eq!(reference.num_rows, Precision::Exact(expected_rows));
    assert!(
        matches!(reference.total_byte_size, Precision::Exact(_)),
        "the footer path reports an exact size: {:?}",
        reference.total_byte_size
    );
    if let Some(writes) = &lone_writes {
        assert_eq!(
            writes.values().copied().collect::<Vec<_>>(),
            vec![1; SINGLE_FLIGHT_FILES],
            "a lone cold scan persists each file's statistics once: {writes:?}"
        );
    }

    // The same cold start, scanned `CONCURRENT_SCANS` times at once.
    fixture
        .catalog
        .clear_snapshot_file_statistics(&table_id)
        .await?;
    if let Some(log) = &write_log {
        log.clear().await?;
    }
    let (table, ctx, store) = open_cold(&fixture).await?;
    let started = std::time::Instant::now();
    let concurrent =
        futures::future::try_join_all((0..CONCURRENT_SCANS).map(|_| plan_statistics(&table, &ctx)))
            .await?;
    let elapsed = started.elapsed();
    let concurrent_reads = store.reads();
    eprintln!(
        "SINGLE_FLIGHT {CONCURRENT_SCANS} concurrent cold scans planned in {} us",
        elapsed.as_micros()
    );
    let concurrent_writes = match &write_log {
        Some(log) => Some(log.writes().await?),
        None => None,
    };
    eprintln!(
        "SINGLE_FLIGHT footer reads per file: one scan {lone_reads:?}, \
         {CONCURRENT_SCANS} concurrent scans {concurrent_reads:?}"
    );
    eprintln!(
        "SINGLE_FLIGHT statistics writes per file: one scan {lone_writes:?}, \
         {CONCURRENT_SCANS} concurrent scans {concurrent_writes:?}"
    );
    assert_eq!(
        concurrent_reads, lone_reads,
        "{CONCURRENT_SCANS} concurrent cold scans must read each footer exactly as often as one scan does"
    );
    assert_eq!(
        concurrent_writes, lone_writes,
        "{CONCURRENT_SCANS} concurrent cold scans must persist each file's statistics exactly once"
    );
    for (scan, statistics) in concurrent.iter().enumerate() {
        assert_eq!(
            statistics, &reference,
            "concurrent scan {scan} must report exactly the statistics a lone scan does"
        );
    }

    // One plan that scans the table twice: a self-join plans both scans together.
    fixture
        .catalog
        .clear_snapshot_file_statistics(&table_id)
        .await?;
    if let Some(log) = &write_log {
        log.clear().await?;
    }
    let (table, ctx, _store) = open_cold(&fixture).await?;
    ctx.register_table("t", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    let batches = ctx
        .sql("SELECT COUNT(*) AS n FROM t AS a JOIN t AS b ON a.id = b.id")
        .await?
        .collect()
        .await?;
    let joined = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT(*) is Int64")
        .value(0);
    assert_eq!(
        joined,
        i64::try_from(expected_rows)?,
        "every id joins itself once"
    );
    if let Some(log) = &write_log {
        let writes = log.writes().await?;
        eprintln!("SINGLE_FLIGHT statistics writes per file for one self-join: {writes:?}");
        assert_eq!(
            writes.values().copied().collect::<Vec<_>>(),
            vec![1; SINGLE_FLIGHT_FILES],
            "a self-join's two scans must persist each file's statistics once: {writes:?}"
        );
    }
    Ok(())
}

/// The provider's own accounting agrees with what the store and the metastore
/// observed, and shows the collections were shared rather than merely ordered.
async fn scan_file_statistics_counters_match_the_store_and_the_metastore(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let table_id = write_multi_file_table(&fixture).await?;
    fixture
        .catalog
        .clear_snapshot_file_statistics(&table_id)
        .await?;

    // Cold, no persisted rows: every file goes to its footer, once.
    let (table, ctx, store) = open_cold(&fixture).await?;
    futures::future::try_join_all((0..CONCURRENT_SCANS).map(|_| plan_statistics(&table, &ctx)))
        .await?;
    let counters = table.scan_file_statistics_counters();
    eprintln!("SINGLE_FLIGHT counters, cold without persisted rows: {counters:?}");
    assert_eq!(counters.footer_reads, u64::try_from(SINGLE_FLIGHT_FILES)?);
    assert_eq!(
        counters.persisted_upserts,
        u64::try_from(SINGLE_FLIGHT_FILES)?
    );
    assert_eq!(counters.persisted_hits, 0);
    assert!(
        counters.joined_in_flight > 0,
        "the concurrent scans must have waited on each other's collections, or this \
         proves nothing about coalescing: {counters:?}"
    );
    assert_eq!(
        store.reads().len(),
        SINGLE_FLIGHT_FILES,
        "the store saw a footer read for each file"
    );

    // A restart over those rows: every file is served from its persisted row,
    // read once however many scans asked.
    let (table, ctx, store) = open_cold(&fixture).await?;
    futures::future::try_join_all((0..CONCURRENT_SCANS).map(|_| plan_statistics(&table, &ctx)))
        .await?;
    let counters = table.scan_file_statistics_counters();
    eprintln!("SINGLE_FLIGHT counters, cold over persisted rows: {counters:?}");
    assert_eq!(counters.footer_reads, 0);
    assert_eq!(counters.persisted_hits, u64::try_from(SINGLE_FLIGHT_FILES)?);
    assert_eq!(counters.persisted_upserts, 0);
    assert!(
        store.reads().is_empty(),
        "no footer is read when every row is persisted: {:?}",
        store.reads()
    );
    Ok(())
}
