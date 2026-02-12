/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Tests covering retention filter application at write time.

mod common;

use arrow::array::{Array, Int64Array, RecordBatch, TimestampMicrosecondArray, UInt64Array};

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use cayenne::metadata::CreateTableOptions;

use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};

use common::TestFixture;

use datafusion::datasource::TableProvider;

use datafusion::prelude::*;

use std::sync::Arc;

test_with_backends!(test_retention_filters_apply_on_insert_impl);
test_with_backends!(test_retention_filters_skip_when_no_matches_impl);
test_with_backends!(test_time_retention_filter_scan_expiry_impl);
test_with_backends!(test_time_retention_with_user_filter_impl);

async fn test_retention_filters_apply_on_insert_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join("retention_apply");
    std::fs::create_dir_all(&table_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "retention_apply".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let retention_expr = col("value").lt(lit(3i64));
    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_provider = Arc::new(
        CayenneTableProvider::create_table_with_retention(
            catalog_arc,
            table_options,
            vec![retention_expr],
        )
        .await?,
    );

    // Insert rows with values 1..=5; retention should mark rows with value < 3 as deleted.
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        ],
    )?;

    let inserted = common::insert_batch(table_provider.as_ref(), batch).await?;
    assert_eq!(inserted, 5, "Should insert all rows");

    // Retention should have created a delete file containing row IDs 0 and 1.
    let delete_files = table_provider
        .catalog()
        .get_table_delete_files(table_provider.metadata().table_id)
        .await?;
    assert_eq!(
        delete_files.len(),
        1,
        "Expected a single deletion file created by retention"
    );

    let delete_file = &delete_files[0];
    assert_eq!(
        delete_file.delete_count, 2,
        "Expected two rows to be marked as deleted"
    );

    let file = std::fs::File::open(&delete_file.path)?;
    let reader = arrow::ipc::reader::FileReader::try_new(file, None)?;
    let mut deleted_row_ids = Vec::new();
    for batch in reader {
        let batch = batch?;
        let row_id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("row_id column (UInt64)");
        for idx in 0..row_id_array.len() {
            deleted_row_ids.push(row_id_array.value(idx));
        }
    }
    assert_eq!(
        deleted_row_ids,
        vec![0u64, 1],
        "Retention should delete the first two logical rows"
    );

    // Query via DataFusion to ensure only rows >= 3 remain.
    let ctx = SessionContext::new();
    ctx.register_table(
        "retention_apply",
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
    )?;
    let df = ctx
        .sql("SELECT value FROM retention_apply ORDER BY value")
        .await?;
    let batches = df.collect().await?;
    let mut remaining_values = Vec::new();
    for batch in &batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column");
        for idx in 0..values.len() {
            remaining_values.push(values.value(idx));
        }
    }
    assert_eq!(remaining_values, vec![3, 4, 5]);

    Ok(())
}

async fn test_retention_filters_skip_when_no_matches_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join("retention_skip");
    std::fs::create_dir_all(&table_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "retention_skip".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let retention_expr = col("value").lt(lit(0i64));
    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_provider = CayenneTableProvider::create_table_with_retention(
        catalog_arc,
        table_options,
        vec![retention_expr],
    )
    .await?;

    // Insert rows that do not match the retention predicate.
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )?;

    common::insert_batch(&table_provider, batch).await?;

    // No delete files should have been created.
    let delete_files = table_provider
        .catalog()
        .get_table_delete_files(table_provider.metadata().table_id)
        .await?;
    assert!(
        delete_files.is_empty(),
        "Retention should not create delete files when no rows match"
    );

    Ok(())
}

/// Test that `time_retention_filter` progressively hides rows as time passes.
///
/// Setup: each row is inserted as a separate batch so it lands in its own
/// Vortex file.  This ensures file-level pruning (via zone-map statistics)
/// is exercised — entire files are skipped when all their rows are expired,
/// not just filtered row-by-row via `FilterExec`.
///
/// - file 1: `event_time` = `now()`       (fresh)
/// - file 2: `event_time` = `now()` - 2s  (within retention)
/// - file 3: `event_time` = `now()` - 4s  (already expired with 3s retention)
///
/// 1. Immediately: files 1 + 2 visible (file 3 pruned)
/// 2. Sleep 2s: file 1 visible (file 2 now expired)
/// 3. Sleep 2s: nothing visible (file 1 now expired)
async fn test_time_retention_filter_scan_expiry_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join("time_retention");
    std::fs::create_dir_all(&table_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));

    let table_options = CreateTableOptions {
        table_name: "time_retention".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let retention_builder = cayenne::TimeRetentionFilterBuilder::try_new("event_time", 3, &schema)
        .expect("to create retention builder");

    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_provider = Arc::new(
        CayenneTableProviderBuilder::new(catalog_arc)
            .with_time_retention_filter_builder(retention_builder)
            .create(table_options)
            .await?,
    );

    // Insert each row as a separate batch → separate Vortex file.
    let now_us = chrono::Utc::now().timestamp_micros();
    let two_sec_ago_us = now_us - 2_000_000;
    let four_sec_ago_us = now_us - 4_000_000;

    for (id, ts) in [(1i64, now_us), (2, two_sec_ago_us), (3, four_sec_ago_us)] {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone("UTC")),
            ],
        )?;
        let inserted = common::insert_batch(table_provider.as_ref(), batch).await?;
        assert_eq!(inserted, 1, "Should insert 1 row for id={id}");
    }

    // Helper: query visible row IDs
    let query_ids = |provider: Arc<dyn TableProvider>| async move {
        let ctx = SessionContext::new();
        ctx.register_table("time_retention", provider)?;
        let df = ctx.sql("SELECT id FROM time_retention ORDER BY id").await?;
        let batches = df.collect().await?;
        let mut ids = Vec::new();
        for batch in &batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column");
            for i in 0..col.len() {
                ids.push(col.value(i));
            }
        }
        Ok::<Vec<i64>, Box<dyn std::error::Error>>(ids)
    };

    // 1. Immediately: id=1 (now) and id=2 (2s ago) are within 3s retention,
    //    id=3 (4s ago) is expired.
    let ids = query_ids(Arc::clone(&table_provider) as Arc<dyn TableProvider>).await?;
    assert_eq!(
        ids,
        vec![1, 2],
        "Initially, only rows within 3s should be visible"
    );

    // 2. Sleep 2s: id=2 is now ~4s old → expired. Only id=1 (~2s old) remains.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let ids = query_ids(Arc::clone(&table_provider) as Arc<dyn TableProvider>).await?;
    assert_eq!(
        ids,
        vec![1],
        "After 2s sleep, only the freshest row should remain"
    );

    // 3. Sleep 2s more: id=1 is now ~4s old → expired. No rows visible.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let ids = query_ids(Arc::clone(&table_provider) as Arc<dyn TableProvider>).await?;
    assert!(ids.is_empty(), "After 4s total, all rows should be expired");

    Ok(())
}

/// Test that a user-supplied `WHERE event_time > X` filter composes correctly
/// with the system retention filter (`event_time >= now() - retention`).
///
/// The tighter (more restrictive) predicate should dominate.
///
/// Setup (`retention_period` = 60s, so no rows expire naturally):
///   - id=1: `event_time` = now - 10s
///   - id=2: `event_time` = now - 30s
///   - id=3: `event_time` = now - 50s
///   - id=4: `event_time` = now - 90s  ← outside retention (expired)
///
/// Test cases (no sleep needed):
///   1. No user filter                          → [1, 2, 3]  (retention prunes id=4)
///   2. WHERE `event_time` > now - 20s            → [1]        (user filter tighter than retention)
///   3. WHERE `event_time` > now - 40s            → [1, 2]     (user filter tighter)
///   4. WHERE `event_time` > now - 120s           → [1, 2, 3]  (retention dominates)
///   5. WHERE `event_time` < now - 20s            → [2, 3]     (upper-bound excludes id=1)
///   6. WHERE `event_time` < now - 55s            → []         (gap: all excluded by one or the other)
///   7. WHERE `event_time` < now - 40s            → [3]        (only id=3 in the window)
///   8. WHERE id > 1                            → [2, 3]     (non-time filter + retention)
///   9. WHERE id > 1 AND `event_time` > now - 40s → [2]        (all three filters intersect)
async fn test_time_retention_with_user_filter_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join("retention_user_filter");
    std::fs::create_dir_all(&table_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));

    let table_options = CreateTableOptions {
        table_name: "retention_user_filter".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    // 60s retention — only id=4 (90s old) is expired
    let retention_builder = cayenne::TimeRetentionFilterBuilder::try_new("event_time", 60, &schema)
        .expect("to create retention builder");

    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_provider = Arc::new(
        CayenneTableProviderBuilder::new(catalog_arc)
            .with_time_retention_filter_builder(retention_builder)
            .create(table_options)
            .await?,
    );

    let now_us = chrono::Utc::now().timestamp_micros();
    let offsets = [
        (1i64, 10), // 10s ago
        (2i64, 30), // 30s ago
        (3i64, 50), // 50s ago
        (4i64, 90), // 90s ago — outside 60s retention
    ];

    // Insert each row as a separate batch (separate Vortex file) for file-level pruning.
    for (id, secs_ago) in offsets {
        let ts = now_us - secs_ago * 1_000_000;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone("UTC")),
            ],
        )?;
        let inserted = common::insert_batch(table_provider.as_ref(), batch).await?;
        assert_eq!(inserted, 1, "Should insert 1 row for id={id}");
    }

    // Helper: query with an optional SQL WHERE clause, returns sorted ids.
    let query_ids = |provider: Arc<dyn TableProvider>, sql: &str| {
        let sql = sql.to_string();
        async move {
            let ctx = SessionContext::new();
            ctx.register_table("t", provider)?;
            let df = ctx.sql(&sql).await?;
            let batches = df.collect().await?;
            let mut ids = Vec::new();
            for batch in &batches {
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id column");
                for i in 0..col.len() {
                    ids.push(col.value(i));
                }
            }
            Ok::<Vec<i64>, Box<dyn std::error::Error>>(ids)
        }
    };

    // Case 1: No user filter — retention alone prunes id=4 (90s > 60s retention).
    let ids = query_ids(
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
        "SELECT id FROM t ORDER BY id",
    )
    .await?;
    assert_eq!(
        ids,
        vec![1, 2, 3],
        "Retention should prune only id=4 (>60s old)"
    );

    // Case 2: User filter tighter than retention (20s < 60s).
    // Only id=1 (10s ago) passes event_time > now - 20s.
    let ids = query_ids(
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
        "SELECT id FROM t WHERE event_time > now() - interval '20 seconds' ORDER BY id",
    )
    .await?;
    assert_eq!(
        ids,
        vec![1],
        "User filter (20s) tighter than retention (60s) — only id=1"
    );

    // Case 3: User filter at 40s — ids 1 and 2 pass.
    let ids = query_ids(
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
        "SELECT id FROM t WHERE event_time > now() - interval '40 seconds' ORDER BY id",
    )
    .await?;
    assert_eq!(
        ids,
        vec![1, 2],
        "User filter (40s) tighter than retention (60s) — ids 1,2"
    );

    // Case 4: User filter looser than retention (120s > 60s).
    // Retention (60s) dominates — id=4 still pruned.
    let ids = query_ids(
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
        "SELECT id FROM t WHERE event_time > now() - interval '120 seconds' ORDER BY id",
    )
    .await?;
    assert_eq!(
        ids,
        vec![1, 2, 3],
        "User filter (120s) looser than retention (60s) — retention dominates, id=4 still pruned"
    );

    // Case 5: Upper-bound filter `event_time < now - 20s` combined with retention.
    // Retention keeps ids [1, 2, 3]. The < filter excludes id=1 (10s ago).
    // Result: ids [2, 3].
    let ids = query_ids(
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
        "SELECT id FROM t WHERE event_time < now() - interval '20 seconds' ORDER BY id",
    )
    .await?;
    assert_eq!(
        ids,
        vec![2, 3],
        "Upper-bound (< 20s ago) + retention (60s) — excludes id=1 (too recent) and id=4 (expired)"
    );

    // Case 6: Upper-bound `event_time < now - 55s`.
    // event_time < (now - 55s) means only rows OLDER than 55s pass.
    // id=1 (10s ago) → no, id=2 (30s ago) → no, id=3 (50s ago) → no (50s < 55s),
    // id=4 (90s ago) → yes but expired by retention.
    // Result: empty.
    let ids = query_ids(
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
        "SELECT id FROM t WHERE event_time < now() - interval '55 seconds' ORDER BY id",
    )
    .await?;
    assert!(
        ids.is_empty(),
        "Upper-bound (< 55s ago) excludes ids 1-3, retention excludes id=4 — nothing visible"
    );

    // Case 7: Upper-bound `event_time < now - 40s` with retention.
    // Rows older than 40s: id=3 (50s), id=4 (90s). Retention prunes id=4.
    // Result: [3].
    let ids = query_ids(
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
        "SELECT id FROM t WHERE event_time < now() - interval '40 seconds' ORDER BY id",
    )
    .await?;
    assert_eq!(
        ids,
        vec![3],
        "Upper-bound (< 40s ago) + retention (60s) — only id=3 in the window"
    );

    // Case 8: Non-time filter (id) combined with retention.
    // Retention keeps [1, 2, 3]. `id > 1` excludes id=1. Result: [2, 3].
    let ids = query_ids(
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
        "SELECT id FROM t WHERE id > 1 ORDER BY id",
    )
    .await?;
    assert_eq!(
        ids,
        vec![2, 3],
        "id > 1 + retention (60s) — id=1 excluded by id filter, id=4 by retention"
    );

    // Case 9: Both id and time filters combined with retention.
    // Retention keeps [1, 2, 3]. `event_time > now()-40s` keeps [1, 2]. `id > 1` keeps [2].
    let ids = query_ids(
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
        "SELECT id FROM t WHERE id > 1 AND event_time > now() - interval '40 seconds' ORDER BY id",
    )
    .await?;
    assert_eq!(
        ids,
        vec![2],
        "id > 1 AND event_time > now()-40s + retention — only id=2 passes all three"
    );

    Ok(())
}
