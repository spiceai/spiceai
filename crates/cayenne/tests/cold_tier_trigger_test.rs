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

//! What decides whether a warm→datalake promotion runs at all.
//!
//! Two behaviours registration only validates or warns about, and that no test
//! held to the promotion decision itself:
//!
//! 1. `cayenne_datalake_warm_max_files` is a threshold: a promotion must not fire
//!    below it and must fire at it. Firing on every tick would rewrite the
//!    datalake continuously; never firing would let the warm tier grow unbounded.
//!    Both still pass every existing correctness test.
//! 2. A table with no primary key leaves the tier inert. Registration warns and
//!    carries on, so this pins what inert means: no object written, data still
//!    served from warm.

mod common;

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Warm files required before a promotion may fire, for the threshold test.
const WARM_MAX_FILES: usize = 3;

test_with_backends!(test_promotion_waits_for_the_configured_warm_file_count_impl);
test_with_backends!(test_datalake_tier_is_inert_without_a_primary_key_impl);

/// Recursively count `.vortex` objects under `dir`.
fn count_vortex_files(dir: &std::path::Path) -> usize {
    let mut count = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                count += count_vortex_files(&path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("vortex") {
                count += 1;
            }
        }
    }
    count
}

/// Rows a real scan returns. Projects a column rather than issuing `COUNT(*)`,
/// which can be folded from the maintained statistics instead of reading rows.
async fn row_count(ctx: &SessionContext, table: &str) -> TestResult<i64> {
    let batches = ctx
        .sql(&format!("SELECT id FROM {table}"))
        .await?
        .collect()
        .await?;
    let mut rows = 0;
    for batch in &batches {
        rows += i64::try_from(batch.num_rows())?;
    }
    Ok(rows)
}

/// Append `ids` and checkpoint them into a durable warm file.
///
/// The batch is smaller than the inline threshold, so the checkpoints are what
/// actually produce the file the promotion trigger counts. Their results are
/// propagated rather than discarded: a silently failed checkpoint would leave the
/// warm tier empty, and an empty warm tier declines promotion for a reason that
/// looks exactly like the threshold under test.
async fn append_one_warm_file(
    table: &CayenneTableProvider,
    ids: std::ops::Range<i64>,
) -> TestResult<()> {
    let ids: Vec<i64> = ids.collect();
    let values: Vec<i64> = ids.iter().map(|i| i * 2).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;
    common::insert_batch(table, batch).await?;
    table.checkpoint_inlined_data().await?;
    table.checkpoint_mem_tier().await?;
    Ok(())
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

async fn create_table(
    fixture: &common::TestFixture,
    ctx: &SessionContext,
    name: &str,
    primary_key: Vec<String>,
    cold_dir: &std::path::Path,
    vortex_config: VortexConfig,
) -> TestResult<Arc<CayenneTableProvider>> {
    std::fs::create_dir_all(cold_dir)?;
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key,
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cold_tier_location: Some(format!("file://{}", cold_dir.to_string_lossy())),
            cold_clustering_columns: vec!["id".to_string()],
            ..vortex_config
        },
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok(table)
}

/// Below `cold_tier_warm_max_files` a promotion must decline; at the threshold
/// it must fire, and the whole warm tier graduates.
async fn test_promotion_waits_for_the_configured_warm_file_count_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let cold_dir = fixture.temp_dir.path().join("cold_threshold");
    let ctx = SessionContext::new();
    let table = create_table(
        &fixture,
        &ctx,
        "threshold_t",
        vec!["id".to_string()],
        &cold_dir,
        VortexConfig {
            cold_tier_warm_max_files: WARM_MAX_FILES,
            // The byte trigger stays off (the default) so one knob is measured.
            deletion_mode: DeletionMode::Key,
            ..VortexConfig::default()
        },
    )
    .await?;

    for file in 0..WARM_MAX_FILES - 1 {
        let start = i64::try_from(file)? * 100;
        append_one_warm_file(&table, start..start + 100).await?;
        // The threshold counts warm files, so the test's arithmetic only holds if
        // each append produced exactly one. Asserting it keeps a declined
        // promotion attributable to the threshold rather than to a short warm tier.
        assert_eq!(
            count_vortex_files(&fixture.data_path),
            file + 1,
            "expected one warm file per append"
        );
        assert!(
            !table.promote_warm_to_cold().await?,
            "promotion must decline with {} warm file(s), below the configured {WARM_MAX_FILES}",
            file + 1
        );
    }
    assert!(
        fixture
            .catalog
            .list_cold_tier_files(table.table_id())
            .await?
            .is_empty(),
        "no datalake file may be registered before the threshold is reached"
    );
    assert_eq!(
        count_vortex_files(&cold_dir),
        0,
        "no object may be written to the datalake before the threshold is reached"
    );

    // The file that reaches the threshold graduates the whole warm tier.
    let start = i64::try_from(WARM_MAX_FILES - 1)? * 100;
    append_one_warm_file(&table, start..start + 100).await?;
    assert!(
        table.promote_warm_to_cold().await?,
        "promotion must fire at {WARM_MAX_FILES} warm files"
    );

    let promoted_rows: i64 = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?
        .iter()
        .map(|f| f.row_count)
        .sum();
    let expected = i64::try_from(WARM_MAX_FILES)? * 100;
    assert_eq!(
        promoted_rows, expected,
        "the promotion that fires graduates every warm row, not just the last file"
    );
    assert_eq!(
        row_count(&ctx, "threshold_t").await?,
        expected,
        "all rows remain queryable after the promotion"
    );

    Ok(())
}

/// Without a primary key the tier cannot classify or rewrite by key, so it stays
/// inert: promotion declines, nothing is written, and warm keeps serving.
///
/// The table asks for `DeletionMode::Key`, the same mode the promoting table in
/// this file uses. `Key` resolves to `Key` only when there is a key to record and
/// to `Position` otherwise, and promotion is key-mode only — so the absent
/// primary key is what makes this table decline, and the contrast with its
/// sibling is exactly that one field.
async fn test_datalake_tier_is_inert_without_a_primary_key_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let cold_dir = fixture.temp_dir.path().join("cold_no_pk");
    let ctx = SessionContext::new();
    let table = create_table(
        &fixture,
        &ctx,
        "no_pk_t",
        Vec::new(),
        &cold_dir,
        VortexConfig {
            // Trigger on any warm file — the most permissive setting the tier has.
            cold_tier_warm_max_files: 1,
            deletion_mode: DeletionMode::Key,
            ..VortexConfig::default()
        },
    )
    .await?;

    append_one_warm_file(&table, 0..100).await?;

    // Guard against a vacuous pass: promotion must decline over a warm tier that
    // is over the trigger, not an empty one.
    assert_eq!(
        count_vortex_files(&fixture.data_path),
        1,
        "expected a durable warm file before testing the promotion decision"
    );
    assert!(
        !table.promote_warm_to_cold().await?,
        "promotion must decline on a table with no primary key"
    );
    assert!(
        fixture
            .catalog
            .list_cold_tier_files(table.table_id())
            .await?
            .is_empty(),
        "an inert tier registers no datalake file"
    );
    assert_eq!(
        count_vortex_files(&cold_dir),
        0,
        "an inert tier writes no object to the datalake location"
    );
    assert_eq!(
        row_count(&ctx, "no_pk_t").await?,
        100,
        "the table keeps serving every row from the warm tier"
    );

    Ok(())
}
