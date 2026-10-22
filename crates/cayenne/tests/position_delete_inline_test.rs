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

//! Deletes on a table with no primary key, whose rows live both in the inline
//! memtable and in Vortex files.
//!
//! A delete removes the inline rows its predicate matches from the inline corpus,
//! in the same catalog commit as the deletion vectors it writes for file rows, so
//! it writes no data file of its own. Each data file keeps one deletion vector
//! row: a new one holds every deleted position of its file and replaces the
//! earlier ones. Files no row references any more are removed from disk.

use crate::common;

use arrow::array::{Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

test_with_backends!(no_pk_delete_removes_inline_rows_without_writing_a_file);
test_with_backends!(no_pk_delete_spans_inline_and_file_rows);
test_with_backends!(no_pk_position_delete_replaces_the_earlier_vector_of_its_file);
test_with_backends!(no_pk_full_rewrite_removes_the_vector_files_it_clears);
test_with_backends!(no_pk_mutations_match_a_row_model);

/// One stored row: `(id, grp, val)`.
type Row = (i64, Option<String>, Option<i64>);

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("grp", DataType::Utf8, true),
        Field::new("val", DataType::Int64, true),
    ]))
}

fn batch(rows: &[Row]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("rows match the schema")
}

fn row(id: i64, grp: Option<&str>, val: Option<i64>) -> Row {
    (id, grp.map(str::to_string), val)
}

async fn create_table(
    fixture: &common::TestFixture,
    name: &str,
    vortex_config: VortexConfig,
) -> Result<(Arc<CayenneTableProvider>, SessionContext, String), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: name.to_string(),
                schema: schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config,
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    let table_id = fixture.catalog.get_table(name).await?.table_id;
    Ok((table, ctx, table_id))
}

/// Every write goes to a Vortex file, and compaction runs only when driven.
fn files_only_config() -> VortexConfig {
    VortexConfig {
        inline_max_rows: 0,
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    }
}

async fn delete(ctx: &SessionContext, sql: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let results = ctx.sql(sql).await?.collect().await?;
    let batch = results.first().ok_or("delete returned no batches")?;
    Ok(batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or("delete count should be UInt64")?
        .value(0))
}

/// The table's rows, ordered by `id`.
async fn rows(ctx: &SessionContext, table: &str) -> Result<Vec<Row>, Box<dyn std::error::Error>> {
    let batches = ctx
        .sql(&format!("SELECT id, grp, val FROM {table} ORDER BY id"))
        .await?
        .collect()
        .await?;
    let mut out = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id should be Int64")?;
        let grps = arrow::compute::cast(batch.column(1), &DataType::Utf8)?;
        let grps = grps
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("grp should be Utf8")?;
        let vals = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("val should be Int64")?;
        for i in 0..batch.num_rows() {
            out.push((
                ids.value(i),
                grps.is_valid(i).then(|| grps.value(i).to_string()),
                vals.is_valid(i).then(|| vals.value(i)),
            ));
        }
    }
    Ok(out)
}

/// Data files and deletion-vector files under `root`.
fn files_on_disk(root: &Path) -> (Vec<PathBuf>, BTreeSet<PathBuf>) {
    let mut data_files = Vec::new();
    let mut vector_files = BTreeSet::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "vortex") {
                data_files.push(path);
            } else if path.extension().is_some_and(|ext| ext == "arrow")
                && path
                    .components()
                    .any(|component| component.as_os_str() == "deletions")
            {
                vector_files.insert(path);
            }
        }
    }
    (data_files, vector_files)
}

/// The catalog's deletion-vector rows for `table_id`, as file paths.
async fn catalog_vector_files(
    fixture: &common::TestFixture,
    table_id: &str,
) -> Result<BTreeSet<PathBuf>, Box<dyn std::error::Error>> {
    Ok(fixture
        .catalog
        .get_table_delete_files(table_id)
        .await?
        .into_iter()
        .map(|file| PathBuf::from(file.path))
        .collect())
}

/// Wait until the deletion-vector files on disk are exactly the ones the catalog
/// names. Superseded and cleared files are removed in the background.
async fn wait_for_vector_files_to_match_catalog(
    fixture: &common::TestFixture,
    table_id: &str,
) -> TestResult {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let referenced = catalog_vector_files(fixture, table_id).await?;
        let (_, on_disk) = files_on_disk(&fixture.data_path);
        if on_disk == referenced {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "deletion-vector files on disk {on_disk:?} still differ from the catalog's {referenced:?}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn no_pk_delete_removes_inline_rows_without_writing_a_file(
    fixture: common::TestFixture,
) -> TestResult {
    let (table, ctx, table_id) =
        create_table(&fixture, "inline_delete", VortexConfig::default()).await?;
    common::insert_batch(
        &table,
        batch(&[
            row(1, Some("a"), Some(10)),
            row(2, Some("b"), Some(20)),
            row(3, Some("c"), None),
        ]),
    )
    .await?;
    assert_eq!(
        fixture.catalog.get_inlined_data_count(&table_id).await?,
        3,
        "the rows start in the inline memtable"
    );
    assert!(files_on_disk(&fixture.data_path).0.is_empty());

    assert_eq!(
        delete(&ctx, "DELETE FROM inline_delete WHERE id = 2").await?,
        1
    );

    assert_eq!(
        rows(&ctx, "inline_delete").await?,
        vec![row(1, Some("a"), Some(10)), row(3, Some("c"), None)]
    );
    let inlined = fixture.catalog.get_inlined_data(&table_id).await?;
    assert_eq!(inlined.len(), 1, "the entry was rewritten in place");
    assert_eq!(inlined[0].record_count, 2);
    assert!(
        files_on_disk(&fixture.data_path).0.is_empty(),
        "the delete wrote no data file"
    );
    assert!(
        catalog_vector_files(&fixture, &table_id).await?.is_empty(),
        "an inline delete needs no deletion vector"
    );
    Ok(())
}

async fn no_pk_delete_spans_inline_and_file_rows(fixture: common::TestFixture) -> TestResult {
    // A write of more than 5 rows goes to a file; a smaller one stays inline.
    let (table, ctx, table_id) = create_table(
        &fixture,
        "mixed_delete",
        VortexConfig {
            inline_max_rows: 5,
            compaction_background_interval_ms: 0,
            ..VortexConfig::default()
        },
    )
    .await?;
    common::insert_batch(
        &table,
        batch(&[
            row(1, Some("a"), Some(1)),
            row(2, Some("b"), Some(2)),
            row(3, None, Some(3)),
            row(4, Some("a"), None),
            row(5, Some("b"), Some(5)),
            row(6, None, None),
        ]),
    )
    .await?;
    common::insert_batch(
        &table,
        batch(&[
            row(7, Some("a"), Some(7)),
            row(8, None, Some(8)),
            row(9, Some("b"), None),
        ]),
    )
    .await?;
    assert_eq!(files_on_disk(&fixture.data_path).0.len(), 1);
    assert_eq!(fixture.catalog.get_inlined_data_count(&table_id).await?, 3);

    // Rows 1 and 4 are in the file, row 7 is inline.
    assert_eq!(
        delete(&ctx, "DELETE FROM mixed_delete WHERE grp = 'a'").await?,
        3
    );
    assert_eq!(
        rows(&ctx, "mixed_delete").await?,
        vec![
            row(2, Some("b"), Some(2)),
            row(3, None, Some(3)),
            row(5, Some("b"), Some(5)),
            row(6, None, None),
            row(8, None, Some(8)),
            row(9, Some("b"), None),
        ]
    );
    assert_eq!(
        files_on_disk(&fixture.data_path).0.len(),
        1,
        "the delete did not flush the inline rows to a file"
    );
    assert_eq!(fixture.catalog.get_inlined_data_count(&table_id).await?, 2);

    // A NULL predicate keeps the row, inline and in the file alike.
    assert_eq!(
        delete(&ctx, "DELETE FROM mixed_delete WHERE grp <> 'b'").await?,
        0
    );
    assert_eq!(
        delete(&ctx, "DELETE FROM mixed_delete WHERE val > 4").await?,
        2
    );
    assert_eq!(
        rows(&ctx, "mixed_delete").await?,
        vec![
            row(2, Some("b"), Some(2)),
            row(3, None, Some(3)),
            row(6, None, None),
            row(9, Some("b"), None),
        ]
    );
    assert_eq!(
        delete(&ctx, "DELETE FROM mixed_delete WHERE grp IS NULL").await?,
        2
    );
    assert_eq!(
        rows(&ctx, "mixed_delete").await?,
        vec![row(2, Some("b"), Some(2)), row(9, Some("b"), None)]
    );
    assert_eq!(files_on_disk(&fixture.data_path).0.len(), 1);
    Ok(())
}

async fn no_pk_position_delete_replaces_the_earlier_vector_of_its_file(
    fixture: common::TestFixture,
) -> TestResult {
    let (table, ctx, table_id) =
        create_table(&fixture, "vector_replace", files_only_config()).await?;
    let all: Vec<Row> = (1..=10).map(|id| row(id, Some("g"), Some(id))).collect();
    common::insert_batch(&table, batch(&all)).await?;
    let (data_files, _) = files_on_disk(&fixture.data_path);
    assert_eq!(data_files.len(), 1, "the rows are in one file");

    for id in 1..=3 {
        assert_eq!(
            delete(&ctx, &format!("DELETE FROM vector_replace WHERE id = {id}")).await?,
            1
        );
    }

    let vectors = fixture.catalog.get_table_delete_files(&table_id).await?;
    assert_eq!(
        vectors.len(),
        1,
        "one vector row per data file: {vectors:?}"
    );
    assert_eq!(vectors[0].delete_count, 3, "it holds all three deletes");
    let data_file_name = data_files[0].file_name().expect("file name");
    assert!(
        vectors[0]
            .source_data_file_path
            .as_deref()
            .is_some_and(|path| path.ends_with(&*data_file_name.to_string_lossy())),
        "the vector names the data file: {vectors:?}"
    );
    wait_for_vector_files_to_match_catalog(&fixture, &table_id).await?;
    assert_eq!(files_on_disk(&fixture.data_path).1.len(), 1);

    let expected: Vec<Row> = (4..=10).map(|id| row(id, Some("g"), Some(id))).collect();
    assert_eq!(rows(&ctx, "vector_replace").await?, expected);

    // The deletes are durable: a fresh provider reads them back from the one row.
    drop(table);
    let reopened_ctx = SessionContext::new();
    let reopened = CayenneTableProviderBuilder::new(
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
        reopened_ctx.runtime_env(),
    )
    .open("vector_replace")
    .await?;
    reopened_ctx.register_table("vector_replace", Arc::new(reopened))?;
    assert_eq!(rows(&reopened_ctx, "vector_replace").await?, expected);
    Ok(())
}

async fn no_pk_full_rewrite_removes_the_vector_files_it_clears(
    fixture: common::TestFixture,
) -> TestResult {
    let (table, ctx, table_id) = create_table(
        &fixture,
        "rewrite_cleanup",
        VortexConfig {
            compaction_trigger_files: 2,
            compaction_max_files_per_pick: 2,
            ..files_only_config()
        },
    )
    .await?;
    for start in [1_i64, 6, 11] {
        let rows: Vec<Row> = (start..start + 5)
            .map(|id| row(id, Some("g"), Some(id)))
            .collect();
        common::insert_batch(&table, batch(&rows)).await?;
    }
    assert_eq!(files_on_disk(&fixture.data_path).0.len(), 3);
    assert_eq!(
        delete(&ctx, "DELETE FROM rewrite_cleanup WHERE id IN (2, 7, 12)").await?,
        3
    );
    wait_for_vector_files_to_match_catalog(&fixture, &table_id).await?;
    assert_eq!(
        files_on_disk(&fixture.data_path).1.len(),
        3,
        "one vector per data file before the rewrite"
    );

    assert!(
        table.maybe_compact_small_files().await?,
        "the rewrite committed"
    );

    assert!(
        catalog_vector_files(&fixture, &table_id).await?.is_empty(),
        "the rewrite folded every delete into its output"
    );
    wait_for_vector_files_to_match_catalog(&fixture, &table_id).await?;
    let expected: Vec<Row> = (1..=15)
        .filter(|id| ![2, 7, 12].contains(id))
        .map(|id| row(id, Some("g"), Some(id)))
        .collect();
    assert_eq!(rows(&ctx, "rewrite_cleanup").await?, expected);
    Ok(())
}

/// `SplitMix64`: a seeded generator, so a failing sequence replays from its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn small(&mut self, bound: u64) -> i64 {
        i64::try_from(self.below(bound)).expect("bound fits i64")
    }
}

/// Whether a delete removes a row: true where its predicate is true, never where
/// the predicate is NULL.
type RowMatcher = Box<dyn Fn(&Row) -> bool>;

/// A delete predicate in SQL, with the rows SQL deletes for it.
fn random_delete(rng: &mut Rng, max_id: i64) -> (String, RowMatcher) {
    let group = format!("g{}", rng.below(4));
    let threshold = rng.small(100);
    match rng.below(5) {
        0 => (
            format!("grp = '{group}'"),
            Box::new(move |row: &Row| row.1.as_deref() == Some(group.as_str())),
        ),
        1 => {
            let low = rng.small(u64::try_from(max_id.max(1)).expect("positive"));
            let high = low + rng.small(20);
            (
                format!("id >= {low} AND id <= {high}"),
                Box::new(move |row: &Row| (low..=high).contains(&row.0)),
            )
        }
        2 => (
            "val IS NULL".to_string(),
            Box::new(|row: &Row| row.2.is_none()),
        ),
        3 => (
            format!("grp = '{group}' AND val > {threshold}"),
            Box::new(move |row: &Row| {
                row.1.as_deref() == Some(group.as_str()) && row.2.is_some_and(|v| v > threshold)
            }),
        ),
        _ => (
            format!("grp IS NULL OR val < {threshold}"),
            Box::new(move |row: &Row| row.1.is_none() || row.2.is_some_and(|v| v < threshold)),
        ),
    }
}

/// Differential test: random appends (inline and file-sized), deletes and
/// compactions against a table with no primary key, compared after every step
/// with a plain list of rows that applies the same SQL semantics. The delete
/// counts must agree too.
///
/// The inline corpus reaches files only the way it does in production: the
/// write path's checkpoint once the corpus holds a few entries, and the
/// rewrites compaction runs, all of which hold the table's write lock.
async fn no_pk_mutations_match_a_row_model(fixture: common::TestFixture) -> TestResult {
    const SEEDS: u64 = 6;
    const STEPS: usize = 40;
    let mut deletes_meeting_inline_rows = 0_usize;
    let mut deletes_meeting_file_rows = 0_usize;
    for seed in 0..SEEDS {
        let name = format!("model_{seed}");
        let (table, ctx, table_id) = create_table(
            &fixture,
            &name,
            VortexConfig {
                compaction_trigger_files: 2,
                compaction_max_files_per_pick: 2,
                compaction_background_interval_ms: 0,
                inline_flush_max_segments: 4,
                ..VortexConfig::default()
            },
        )
        .await?;
        let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0x5EED);
        let mut model: Vec<Row> = Vec::new();
        let mut next_id = 0_i64;
        let mut deletes_with_effect = 0_usize;
        let mut ops: Vec<String> = Vec::new();
        for step in 0..STEPS {
            let context = format!("seed={seed} step={step}");
            match rng.below(10) {
                0..=4 => {
                    // Mostly inline-sized appends, now and then one past the
                    // inline row bound, which goes to a file.
                    let count = if rng.below(6) == 0 {
                        1_100
                    } else {
                        1 + rng.below(12)
                    };
                    let mut appended = Vec::new();
                    for _ in 0..count {
                        let grp = match rng.below(5) {
                            4 => None,
                            g => Some(format!("g{g}")),
                        };
                        let val = (rng.below(4) != 0).then(|| rng.small(100));
                        appended.push((next_id, grp, val));
                        next_id += 1;
                    }
                    ops.push(format!(
                        "append ids {}..={}",
                        appended[0].0,
                        appended[appended.len() - 1].0
                    ));
                    common::insert_batch(&table, batch(&appended)).await?;
                    model.extend(appended);
                }
                5..=8 => {
                    deletes_meeting_inline_rows +=
                        usize::from(fixture.catalog.get_inlined_data_count(&table_id).await? > 0);
                    deletes_meeting_file_rows += usize::from(
                        !files_on_disk(Path::new(table.table_path_str()))
                            .0
                            .is_empty(),
                    );
                    let (predicate, matches) = random_delete(&mut rng, next_id);
                    let expected = model.iter().filter(|row| matches(row)).count();
                    let deleted = delete(&ctx, &format!("DELETE FROM {name} WHERE {predicate}"))
                        .await
                        .map_err(|e| format!("{context}: DELETE WHERE {predicate}: {e}"))?;
                    assert_eq!(
                        deleted,
                        u64::try_from(expected)?,
                        "{context}: rows deleted by `{predicate}`"
                    );
                    ops.push(format!("delete where {predicate} -> {deleted}"));
                    model.retain(|row| !matches(row));
                    deletes_with_effect += usize::from(expected > 0);
                }
                _ => {
                    let compacted = table.maybe_compact_small_files().await?;
                    ops.push(format!("compact -> {compacted}"));
                }
            }
            let got = rows(&ctx, &name).await?;
            if got != model {
                let mut seen = std::collections::HashSet::new();
                let repeated: Vec<i64> = got
                    .iter()
                    .filter(|row| !seen.insert(row.0))
                    .map(|row| row.0)
                    .collect();
                let missing: Vec<i64> = model
                    .iter()
                    .filter(|row| !got.contains(row))
                    .map(|row| row.0)
                    .collect();
                let extra: Vec<i64> = got
                    .iter()
                    .filter(|row| !model.contains(row))
                    .map(|row| row.0)
                    .collect();
                return Err(format!(
                    "{context}: table holds {} rows, model {}; repeated ids {repeated:?}, missing {missing:?}, extra {extra:?}; operations: {ops:#?}",
                    got.len(),
                    model.len()
                )
                .into());
            }
        }
        assert!(
            deletes_with_effect > 0,
            "seed={seed}: no delete removed a row, so the run compared nothing"
        );
    }
    assert!(
        deletes_meeting_inline_rows > 0 && deletes_meeting_file_rows > 0,
        "the deletes must meet both inline and file rows: {deletes_meeting_inline_rows} met inline rows, {deletes_meeting_file_rows} met files"
    );
    Ok(())
}
