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

#![allow(clippy::expect_used)]

//! A write and the `retention_sql` delete it is owed publish as one generation:
//! a reader sees the table before the write or after both, never the written rows
//! the delete removes. Every assertion reads right after the write returns, with
//! no `flush_pending_maintenance`, because that is the window a concurrent
//! query lands in.

use crate::common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::*;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

test_with_backends!(append_hides_matching_rows_at_publish_impl);
test_with_backends!(overwrite_hides_matching_rows_at_publish_impl);
test_with_backends!(upsert_to_matching_version_removes_key_at_publish_impl);
test_with_backends!(duplicate_key_upsert_last_matching_removes_key_impl);
test_with_backends!(failing_retention_keeps_previous_generation_impl);
test_with_backends!(concurrent_reader_never_sees_matching_rows_impl);
test_with_backends!(keyed_overwrite_last_copy_decides_at_publish_impl);
test_with_backends!(superseded_copy_is_never_evaluated_impl);
test_with_backends!(memory_overwrite_hides_matching_rows_at_publish_impl);
test_with_backends!(memory_failing_retention_keeps_previous_tier_impl);
test_with_backends!(failing_retention_fails_append_and_keeps_rows_impl);
test_with_backends!(keyed_append_superseded_copy_is_never_evaluated_impl);
test_with_backends!(memory_append_hides_matching_rows_at_publish_impl);
test_with_backends!(volatile_predicate_sees_only_winning_copies_impl);
test_with_backends!(key_without_on_conflict_append_hides_at_publish_impl);
test_with_backends!(retention_deletes_are_durable_with_the_append_impl);
test_with_backends!(memory_cdc_apply_hides_matching_rows_at_publish_impl);
test_with_backends!(durable_cdc_apply_hides_and_failed_batch_replays_impl);
test_with_backends!(partitioned_upsert_hides_matching_rows_at_publish_impl);

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
    ]))
}

/// `status = 'archived'` on `id % 7 == 0`, NULL on `id % 7 == 3`, else `active`.
fn batch(ids: impl IntoIterator<Item = i64>) -> RecordBatch {
    let ids: Vec<i64> = ids.into_iter().collect();
    let status: Vec<Option<&str>> = ids
        .iter()
        .map(|id| match id % 7 {
            0 => Some("archived"),
            3 => None,
            _ => Some("active"),
        })
        .collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(status)),
        ],
    )
    .expect("batch")
}

fn rows(pairs: &[(i64, Option<&str>)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                pairs.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                pairs.iter().map(|(_, s)| *s).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

fn file_tier_config() -> VortexConfig {
    VortexConfig {
        inline_max_rows: 0,
        inline_max_bytes: 0,
        inline_max_buffer_bytes: 0,
        ..VortexConfig::default()
    }
}

async fn table(
    fixture: &common::TestFixture,
    name: &str,
    upsert: bool,
    retention: Expr,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let dir = fixture.data_path.join(name);
    std::fs::create_dir_all(&dir)?;
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: if upsert {
            vec!["id".to_string()]
        } else {
            vec![]
        },
        on_conflict: upsert
            .then(|| OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()]))),
        base_path: dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: file_tier_config(),
    };
    let catalog = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    Ok(Arc::new(
        CayenneTableProvider::create_table_with_retention(
            catalog,
            options,
            vec![retention],
            SessionContext::new().runtime_env(),
        )
        .await?,
    ))
}

fn archived() -> Expr {
    col("status").eq(lit("archived"))
}

/// `(total, archived, null_status)` as a reader sees them now.
async fn counts(
    provider: &Arc<CayenneTableProvider>,
) -> Result<(i64, i64, i64), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(provider) as Arc<dyn TableProvider>)?;
    let batches = ctx
        .sql(
            "SELECT count(*), count(*) FILTER (WHERE status = 'archived'), \
             count(*) FILTER (WHERE status IS NULL) FROM t",
        )
        .await?
        .collect()
        .await?;
    let b = &batches[0];
    let get = |i: usize| {
        b.column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .value(0)
    };
    Ok((get(0), get(1), get(2)))
}

async fn ids(provider: &Arc<CayenneTableProvider>) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(provider) as Arc<dyn TableProvider>)?;
    let batches = ctx
        .sql("SELECT id FROM t ORDER BY id")
        .await?
        .collect()
        .await?;
    let mut out = Vec::new();
    for b in &batches {
        let c = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        out.extend((0..c.len()).map(|i| c.value(i)));
    }
    Ok(out)
}

/// 0..70: 10 archived, 10 NULL (kept: DELETE removes only rows the predicate is
/// TRUE for), 50 active.
async fn append_hides_matching_rows_at_publish_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let t = table(&fixture, "rsql_pub_append", false, archived()).await?;
    // The append still reports every row it inserted, as when the retention
    // delete ran after it.
    assert_eq!(common::insert_batch(t.as_ref(), batch(0..70)).await?, 70);
    assert_eq!(counts(&t).await?, (60, 0, 10), "right after the append");
    common::insert_batch(t.as_ref(), batch(70..140)).await?;
    assert_eq!(
        counts(&t).await?,
        (120, 0, 20),
        "right after a second append"
    );
    t.flush_pending_maintenance().await?;
    assert_eq!(counts(&t).await?, (120, 0, 20), "after maintenance");
    Ok(())
}

async fn overwrite_hides_matching_rows_at_publish_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let t = table(&fixture, "rsql_pub_overwrite", false, archived()).await?;
    common::write_batches(t.as_ref(), vec![batch(0..70)], InsertOp::Overwrite).await?;
    t.flush_pending_maintenance().await?;
    assert_eq!(counts(&t).await?, (60, 0, 10), "after the first load");
    for round in 0..5 {
        common::write_batches(t.as_ref(), vec![batch(0..70)], InsertOp::Overwrite).await?;
        assert_eq!(
            counts(&t).await?,
            (60, 0, 10),
            "right after overwrite {round}"
        );
    }
    Ok(())
}

/// Key 1 goes `active` -> `archived`: the upsert supersedes the old version and
/// retention removes the new one, so key 1 is absent at publish, neither version.
async fn upsert_to_matching_version_removes_key_at_publish_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let t = table(&fixture, "rsql_pub_upsert", true, archived()).await?;
    common::insert_batch(
        t.as_ref(),
        rows(&[(1, Some("active")), (2, Some("active")), (3, None)]),
    )
    .await?;
    t.flush_pending_maintenance().await?;
    assert_eq!(ids(&t).await?, vec![1, 2, 3]);
    assert_eq!(
        common::insert_batch(t.as_ref(), rows(&[(1, Some("archived"))])).await?,
        1,
        "the upsert reports the row it inserted"
    );
    assert_eq!(ids(&t).await?, vec![2, 3], "right after the upsert");
    // A later insert of the removed key is not dropped as a duplicate.
    common::insert_batch(t.as_ref(), rows(&[(1, Some("active"))])).await?;
    assert_eq!(
        ids(&t).await?,
        vec![1, 2, 3],
        "key comes back once it no longer matches"
    );
    Ok(())
}

async fn duplicate_key_upsert_last_matching_removes_key_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let t = table(&fixture, "rsql_pub_dup", true, archived()).await?;
    common::insert_batch(
        t.as_ref(),
        rows(&[(1, Some("active")), (2, Some("active"))]),
    )
    .await?;
    t.flush_pending_maintenance().await?;
    common::insert_batch(
        t.as_ref(),
        rows(&[
            (1, Some("active")),
            (1, Some("archived")),
            (2, Some("archived")),
            (2, Some("active")),
        ]),
    )
    .await?;
    assert_eq!(ids(&t).await?, vec![2], "last version per key decides");
    Ok(())
}

/// A predicate that fails at runtime must fail the write and leave the previous
/// generation readable, instead of publishing rows retention then never deletes.
async fn failing_retention_keeps_previous_generation_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    // `CAST('archived' AS INT)` errors only on the batch that holds an archived row.
    let failing = cast(col("status"), DataType::Int32).eq(lit(1_i32));
    let t = table(&fixture, "rsql_pub_fail", false, failing).await?;
    common::write_batches(
        t.as_ref(),
        vec![rows(&[(1, Some("1")), (2, Some("2"))])],
        InsertOp::Overwrite,
    )
    .await?;
    t.flush_pending_maintenance().await?;
    assert_eq!(ids(&t).await?, vec![2]);
    let result = common::write_batches(t.as_ref(), vec![batch(0..14)], InsertOp::Overwrite).await;
    assert!(
        result.is_err(),
        "the write that retention cannot apply to fails"
    );
    assert_eq!(ids(&t).await?, vec![2], "previous generation still served");
    Ok(())
}

/// Red/green gate: readers run while the table is overwritten and appended to in
/// a loop; not one read may see an archived row.
async fn concurrent_reader_never_sees_matching_rows_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let t = table(&fixture, "rsql_pub_concurrent", false, archived()).await?;
    common::write_batches(t.as_ref(), vec![batch(0..700)], InsertOp::Overwrite).await?;
    t.flush_pending_maintenance().await?;

    let stop = Arc::new(AtomicBool::new(false));
    let hits = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let mut readers = Vec::new();
    for _ in 0..4 {
        let (t, stop, hits, reads) = (
            Arc::clone(&t),
            Arc::clone(&stop),
            Arc::clone(&hits),
            Arc::clone(&reads),
        );
        readers.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let (_, archived, _) = counts(&t).await.expect("read");
                reads.fetch_add(1, Ordering::Relaxed);
                if archived > 0 {
                    hits.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for round in 0..20_i64 {
        if round % 2 == 0 {
            common::write_batches(t.as_ref(), vec![batch(0..700)], InsertOp::Overwrite).await?;
        } else {
            common::insert_batch(t.as_ref(), batch(700 + round * 70..770 + round * 70)).await?;
        }
        tokio::task::yield_now().await;
    }
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.await?;
    }
    assert!(reads.load(Ordering::Relaxed) > 0, "readers ran");
    assert_eq!(
        hits.load(Ordering::Relaxed),
        0,
        "reads that saw archived rows"
    );
    Ok(())
}

/// A keyed overwrite resolves repeated keys first, then retention: key 1's last
/// copy is archived, so key 1 is absent; key 2's last copy is active, so it stays,
/// though an earlier copy of it matched.
async fn keyed_overwrite_last_copy_decides_at_publish_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let t = table(&fixture, "rsql_pub_keyed_overwrite", true, archived()).await?;
    common::write_batches(
        t.as_ref(),
        vec![rows(&[(1, Some("active")), (2, Some("active"))])],
        InsertOp::Overwrite,
    )
    .await?;
    t.flush_pending_maintenance().await?;
    for round in 0..3 {
        common::write_batches(
            t.as_ref(),
            vec![rows(&[
                (1, Some("active")),
                (2, Some("archived")),
                (3, None),
                (1, Some("archived")),
                (2, Some("active")),
            ])],
            InsertOp::Overwrite,
        )
        .await?;
        assert_eq!(
            ids(&t).await?,
            vec![2, 3],
            "right after keyed overwrite {round}"
        );
        assert_eq!(
            count_star(&t).await?,
            2,
            "COUNT(*) after keyed overwrite {round}"
        );
    }
    t.flush_pending_maintenance().await?;
    assert_eq!(ids(&t).await?, vec![2, 3], "after maintenance");
    Ok(())
}

/// Plain `COUNT(*)`, the shape statistics can answer without reading rows.
async fn count_star(
    provider: &Arc<CayenneTableProvider>,
) -> Result<i64, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(provider) as Arc<dyn TableProvider>)?;
    let batches = ctx.sql("SELECT count(*) FROM t").await?.collect().await?;
    Ok(batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0))
}

/// Retention runs on the generation the keys resolved to. A superseded copy whose
/// value the predicate cannot cast must not fail the write: `DELETE` after the
/// publish never sees it. Key 1's winner casts to 1 (deleted), key 2's to 2 (kept).
async fn superseded_copy_is_never_evaluated_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let predicate = cast(col("status"), DataType::Int32).eq(lit(1_i32));
    let t = table(&fixture, "rsql_pub_superseded_eval", true, predicate).await?;
    common::write_batches(
        t.as_ref(),
        vec![rows(&[(5, Some("5"))])],
        InsertOp::Overwrite,
    )
    .await?;
    t.flush_pending_maintenance().await?;
    common::write_batches(
        t.as_ref(),
        vec![rows(&[
            (1, Some("not a number")),
            (2, Some("not a number")),
            (1, Some("1")),
            (2, Some("2")),
        ])],
        InsertOp::Overwrite,
    )
    .await?;
    assert_eq!(ids(&t).await?, vec![2], "right after the overwrite");
    assert_eq!(count_star(&t).await?, 1);
    Ok(())
}

async fn memory_table(
    fixture: &common::TestFixture,
    name: &str,
    retention: Expr,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            memory_mode: true,
            cdc_mem_tier_shards: 1,
            cdc_mem_tier_checkpoint_interval_ms: 0,
            compaction_background_interval_ms: 0,
            inline_max_rows: 0,
            inline_max_bytes: 0,
            cdc_durability: CdcDurability::Memory,
            deletion_mode: DeletionMode::Key,
            ..VortexConfig::default()
        },
    };
    let catalog = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    Ok(Arc::new(
        CayenneTableProvider::create_table_with_retention(
            catalog,
            options,
            vec![retention],
            SessionContext::new().runtime_env(),
        )
        .await?,
    ))
}

async fn memory_overwrite_hides_matching_rows_at_publish_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let t = memory_table(&fixture, "rsql_pub_mem_overwrite", archived()).await?;
    for round in 0..5 {
        common::write_batches(t.as_ref(), vec![batch(0..70)], InsertOp::Overwrite).await?;
        assert_eq!(
            counts(&t).await?,
            (60, 0, 10),
            "right after memory overwrite {round}"
        );
    }
    // Repeats resolve first: key 1's last copy matches, key 2's does not.
    common::write_batches(
        t.as_ref(),
        vec![rows(&[
            (1, Some("active")),
            (2, Some("archived")),
            (1, Some("archived")),
            (2, Some("active")),
        ])],
        InsertOp::Overwrite,
    )
    .await?;
    assert_eq!(ids(&t).await?, vec![2]);
    Ok(())
}

async fn memory_failing_retention_keeps_previous_tier_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let failing = cast(col("status"), DataType::Int32).eq(lit(1_i32));
    let t = memory_table(&fixture, "rsql_pub_mem_fail", failing).await?;
    common::write_batches(
        t.as_ref(),
        vec![rows(&[(1, Some("1")), (2, Some("2"))])],
        InsertOp::Overwrite,
    )
    .await?;
    t.flush_pending_maintenance().await?;
    assert_eq!(ids(&t).await?, vec![2]);
    let result = common::write_batches(t.as_ref(), vec![batch(0..14)], InsertOp::Overwrite).await;
    assert!(
        result.is_err(),
        "the overwrite retention cannot apply to fails"
    );
    assert_eq!(ids(&t).await?, vec![2], "previous tier still served");
    Ok(())
}

/// An append whose rows the predicate cannot evaluate fails, publishing nothing,
/// with or without a primary key.
async fn failing_retention_fails_append_and_keeps_rows_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    for keyed in [false, true] {
        let failing = cast(col("status"), DataType::Int32).eq(lit(1_i32));
        let name = format!("rsql_pub_append_fail_{keyed}");
        let t = table(&fixture, &name, keyed, failing).await?;
        common::insert_batch(t.as_ref(), rows(&[(1, Some("1")), (2, Some("2"))])).await?;
        t.flush_pending_maintenance().await?;
        assert_eq!(ids(&t).await?, vec![2], "keyed={keyed}");
        let result =
            common::insert_batch(t.as_ref(), rows(&[(3, Some("3")), (4, Some("bad"))])).await;
        assert!(
            result.is_err(),
            "keyed={keyed}: the append retention cannot apply to fails"
        );
        assert_eq!(
            ids(&t).await?,
            vec![2],
            "keyed={keyed}: nothing of the failed append is visible"
        );
        common::insert_batch(t.as_ref(), rows(&[(5, Some("5"))])).await?;
        assert_eq!(
            ids(&t).await?,
            vec![2, 5],
            "keyed={keyed}: the table still takes writes"
        );
    }
    Ok(())
}

/// A keyed append resolves its repeats before retention: the losing copy holds a
/// value the predicate cannot cast, and is never evaluated.
async fn keyed_append_superseded_copy_is_never_evaluated_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let predicate = cast(col("status"), DataType::Int32).eq(lit(1_i32));
    let t = table(&fixture, "rsql_pub_append_superseded_eval", true, predicate).await?;
    common::insert_batch(t.as_ref(), rows(&[(9, Some("9"))])).await?;
    t.flush_pending_maintenance().await?;
    common::insert_batch(
        t.as_ref(),
        rows(&[
            (1, Some("bad")),
            (2, Some("bad")),
            (1, Some("1")),
            (2, Some("2")),
        ]),
    )
    .await?;
    assert_eq!(ids(&t).await?, vec![2, 9], "right after the append");
    assert_eq!(count_star(&t).await?, 2);
    Ok(())
}

async fn memory_append_hides_matching_rows_at_publish_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let t = memory_table(&fixture, "rsql_pub_mem_append", archived()).await?;
    assert_eq!(common::insert_batch(t.as_ref(), batch(0..70)).await?, 70);
    assert_eq!(
        counts(&t).await?,
        (60, 0, 10),
        "right after the first append"
    );
    // Key 1 active -> archived: gone at publish; later re-insert is not refused.
    common::insert_batch(t.as_ref(), rows(&[(1, Some("archived"))])).await?;
    assert_eq!(
        counts(&t).await?,
        (59, 0, 10),
        "right after the upsert to archived"
    );
    common::insert_batch(t.as_ref(), rows(&[(1, Some("active"))])).await?;
    assert_eq!(
        counts(&t).await?,
        (60, 0, 10),
        "key 1 back once it no longer matches"
    );
    Ok(())
}

/// A volatile predicate (an instrumented UDF standing in for `random()`) is called
/// on the rows a `DELETE` after the publish would see, and never on a copy the
/// write's repeated keys superseded. Losing copies carry a `loser-` status; the
/// UDF records every value it is called on, so one call on a loser fails this.
async fn volatile_predicate_sees_only_winning_copies_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use std::sync::Mutex;

    let seen: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&seen);
    let probe = create_udf(
        "retention_probe",
        vec![DataType::Utf8],
        DataType::Boolean,
        Volatility::Volatile,
        Arc::new(move |args: &[ColumnarValue]| {
            let array = args[0].clone().into_array(1)?;
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 argument");
            let mut seen = recorder.lock().expect("lock");
            let out: arrow::array::BooleanArray = values
                .iter()
                .map(|v| {
                    seen.push(v.map(str::to_string));
                    Some(v == Some("archived"))
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out)))
        }),
    );
    let predicate = probe.call(vec![col("status")]);
    for (name, op) in [("ow", InsertOp::Overwrite), ("ap", InsertOp::Append)] {
        seen.lock().expect("lock").clear();
        let t = table(
            &fixture,
            &format!("rsql_pub_volatile_{name}"),
            true,
            predicate.clone(),
        )
        .await?;
        common::write_batches(
            t.as_ref(),
            vec![rows(&[
                (1, Some("loser-1")),
                (2, Some("loser-2")),
                (1, Some("active")),
                (2, Some("archived")),
                (3, Some("active")),
            ])],
            op,
        )
        .await?;
        assert_eq!(ids(&t).await?, vec![1, 3], "{name}: right after the write");
        let seen = seen.lock().expect("lock").clone();
        assert!(
            seen.iter()
                .all(|v| !v.as_deref().unwrap_or("").starts_with("loser-")),
            "{name}: predicate called on a superseded copy: {seen:?}"
        );
        for winner in ["active", "archived"] {
            assert!(
                seen.iter().any(|v| v.as_deref() == Some(winner)),
                "{name}: {winner} never evaluated: {seen:?}"
            );
        }
    }
    Ok(())
}

/// A primary key without `on_conflict` resolves no repeats, so the append drops
/// retention's matches from the validated rows before the write. Their keys were
/// validated but are not stored: inserting one again later is not refused.
async fn key_without_on_conflict_append_hides_at_publish_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let name = "rsql_pub_key_no_conflict";
    let dir = fixture.data_path.join(name);
    std::fs::create_dir_all(&dir)?;
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: file_tier_config(),
    };
    let t = Arc::new(
        CayenneTableProvider::create_table_with_retention(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            options,
            vec![archived()],
            SessionContext::new().runtime_env(),
        )
        .await?,
    );
    assert_eq!(common::insert_batch(t.as_ref(), batch(0..70)).await?, 70);
    assert_eq!(counts(&t).await?, (60, 0, 10), "right after the append");
    common::insert_batch(t.as_ref(), rows(&[(1000, Some("archived"))])).await?;
    assert!(
        !ids(&t).await?.contains(&1000),
        "archived key hidden at publish"
    );
    common::insert_batch(t.as_ref(), rows(&[(1000, Some("active"))])).await?;
    assert!(
        ids(&t).await?.contains(&1000),
        "a dropped key can be inserted again"
    );
    assert_eq!(counts(&t).await?, (61, 0, 10));
    Ok(())
}

/// The retention deletes commit with the append, not in a later pass: a provider
/// opened afresh from the catalog, with no retention of its own to apply, never
/// sees the matching rows or the superseded version.
async fn retention_deletes_are_durable_with_the_append_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let name = "rsql_pub_durable";
    {
        let t = table(&fixture, name, true, archived()).await?;
        common::insert_batch(
            t.as_ref(),
            rows(&[(1, Some("active")), (2, Some("active"))]),
        )
        .await?;
        t.flush_pending_maintenance().await?;
        common::insert_batch(
            t.as_ref(),
            rows(&[
                (1, Some("archived")),
                (3, Some("archived")),
                (4, Some("active")),
            ]),
        )
        .await?;
        assert_eq!(ids(&t).await?, vec![2, 4], "right after the upsert");
    }
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            SessionContext::new().runtime_env(),
        )
        .open(name)
        .await?,
    );
    assert_eq!(ids(&reopened).await?, vec![2, 4], "after reopening");
    assert_eq!(count_star(&reopened).await?, 2, "COUNT(*) after reopening");
    Ok(())
}

struct NoopSlotAdvancer;
#[async_trait::async_trait]
impl cayenne::SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

/// A `cdc_durability: memory` CDC apply, unsharded and sharded, publishes each
/// batch with its retention delete: an upsert to a matching version removes the
/// key, a matching new key never appears, and neither waits for a later pass.
async fn memory_cdc_apply_hides_matching_rows_at_publish_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    for shards in [1_usize, 4] {
        let name = format!("rsql_pub_cdc_{shards}");
        let dir = fixture.data_path.join(&name);
        std::fs::create_dir_all(&dir)?;
        let options = CreateTableOptions {
            table_name: name.clone(),
            schema: schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: dir.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                cdc_durability: CdcDurability::Memory,
                deletion_mode: DeletionMode::Key,
                cdc_mem_tier_shards: shards,
                cdc_mem_tier_max_age_ms: 0,
                cdc_mem_tier_checkpoint_interval_ms: 0,
                cdc_mem_tier_seal_age_ms: 0,
                cdc_mem_tier_max_bytes: 0,
                inline_max_rows: 0,
                inline_max_bytes: 0,
                compaction_background_interval_ms: 0,
                ..VortexConfig::default()
            },
        };
        let t = Arc::new(
            CayenneTableProvider::create_table_with_retention(
                Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                options,
                vec![archived()],
                SessionContext::new().runtime_env(),
            )
            .await?,
        );
        t.install_slot_advancer(Arc::new(NoopSlotAdvancer));
        let apply = |batch: RecordBatch| {
            let t = Arc::clone(&t);
            async move {
                let ctx = SessionContext::new();
                let schema = batch.schema();
                let stream: datafusion::execution::SendableRecordBatchStream = Box::pin(
                    datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                        schema,
                        futures::stream::iter([Ok(batch)]),
                    ),
                );
                let write = t.write_cdc_append_stream(stream, &ctx.task_ctx()).await?;
                if write.has_pending_finalize() {
                    write.finish().await?;
                }
                Ok::<_, Box<dyn std::error::Error>>(())
            }
        };
        apply(rows(&[
            (1, Some("active")),
            (2, Some("active")),
            (3, Some("active")),
        ]))
        .await?;
        assert_eq!(
            ids(&t).await?,
            vec![1, 2, 3],
            "{shards} shards: first apply"
        );
        apply(rows(&[
            (1, Some("archived")),
            (2, Some("active")),
            (4, Some("archived")),
            (5, Some("active")),
            (6, None),
        ]))
        .await?;
        assert_eq!(
            ids(&t).await?,
            vec![2, 3, 5, 6],
            "{shards} shards: right after the apply"
        );
        assert_eq!(count_star(&t).await?, 4, "{shards} shards: COUNT(*)");
        apply(rows(&[(1, Some("active"))])).await?;
        assert_eq!(
            ids(&t).await?,
            vec![1, 2, 3, 5, 6],
            "{shards} shards: removed key returns"
        );
    }
    Ok(())
}

async fn cdc_apply(
    t: &Arc<CayenneTableProvider>,
    batch: RecordBatch,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let schema = batch.schema();
    let stream: datafusion::execution::SendableRecordBatchStream = Box::pin(
        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter([Ok(batch)]),
        ),
    );
    let write = t.write_cdc_append_stream(stream, &ctx.task_ctx()).await?;
    if write.has_pending_finalize() {
        write.finish().await?;
    }
    Ok(())
}

async fn durable_cdc_table(
    fixture: &common::TestFixture,
    name: &str,
    retention: Expr,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let dir = fixture.data_path.join(name);
    std::fs::create_dir_all(&dir)?;
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: file_tier_config(),
    };
    Ok(Arc::new(
        CayenneTableProvider::create_table_with_retention(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            options,
            vec![retention],
            SessionContext::new().runtime_env(),
        )
        .await?,
    ))
}

/// A durable CDC apply publishes each batch with its retention delete. A batch
/// retention cannot evaluate fails without publishing anything, so the source
/// never acknowledges it; replaying it fails the same way and leaves the table
/// unchanged, and the next batch the source sends applies normally.
async fn durable_cdc_apply_hides_and_failed_batch_replays_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let t = durable_cdc_table(&fixture, "rsql_pub_cdc_durable", archived()).await?;
    cdc_apply(
        &t,
        rows(&[
            (1, Some("active")),
            (2, Some("active")),
            (3, Some("active")),
        ]),
    )
    .await?;
    cdc_apply(
        &t,
        rows(&[
            (1, Some("archived")),
            (4, Some("active")),
            (4, Some("archived")),
            (5, Some("archived")),
            (5, Some("active")),
            (6, None),
        ]),
    )
    .await?;
    assert_eq!(ids(&t).await?, vec![2, 3, 5, 6], "right after the apply");
    assert_eq!(count_star(&t).await?, 4);

    // `status` values that are not integers make this predicate fail.
    let failing = cast(col("status"), DataType::Int32).eq(lit(1_i32));
    let f = durable_cdc_table(&fixture, "rsql_pub_cdc_durable_fail", failing).await?;
    cdc_apply(&f, rows(&[(1, Some("1")), (2, Some("2"))])).await?;
    assert_eq!(ids(&f).await?, vec![2], "integer statuses apply");
    for attempt in 0..2 {
        let bad = cdc_apply(&f, rows(&[(2, Some("x")), (3, Some("y"))])).await;
        assert!(
            bad.is_err(),
            "attempt {attempt}: the batch retention cannot evaluate fails"
        );
        assert_eq!(
            ids(&f).await?,
            vec![2],
            "attempt {attempt}: nothing of it published"
        );
        assert_eq!(
            count_star(&f).await?,
            1,
            "attempt {attempt}: COUNT(*) unchanged"
        );
    }
    cdc_apply(&f, rows(&[(3, Some("3")), (4, Some("1"))])).await?;
    assert_eq!(ids(&f).await?, vec![2, 3], "the next batch applies");
    Ok(())
}

/// A partitioned upsert table (whose writes do not resolve repeats while they
/// write) still publishes the upsert and its retention delete together, with the
/// last copy of a repeated key deciding.
async fn partitioned_upsert_hides_matching_rows_at_publish_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let name = "rsql_pub_partitioned";
    let dir = fixture.data_path.join(name);
    std::fs::create_dir_all(&dir)?;
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: dir.to_string_lossy().to_string(),
        partition_column: Some("status".to_string()),
        vortex_config: file_tier_config(),
    };
    let t = Arc::new(
        CayenneTableProvider::create_table_with_retention(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            options,
            vec![archived()],
            SessionContext::new().runtime_env(),
        )
        .await?,
    );
    common::insert_batch(
        t.as_ref(),
        rows(&[
            (1, Some("active")),
            (2, Some("active")),
            (3, Some("active")),
        ]),
    )
    .await?;
    t.flush_pending_maintenance().await?;
    let upsert = || {
        rows(&[
            (1, Some("archived")),
            (4, Some("active")),
            (4, Some("archived")),
            (5, Some("archived")),
            (5, Some("active")),
        ])
    };
    // The same table without retention: what the upsert reported before.
    let twin_dir = fixture.data_path.join("rsql_pub_partitioned_twin");
    std::fs::create_dir_all(&twin_dir)?;
    let twin = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "rsql_pub_partitioned_twin".to_string(),
                schema: schema(),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: twin_dir.to_string_lossy().to_string(),
                partition_column: Some("status".to_string()),
                vortex_config: file_tier_config(),
            },
            SessionContext::new().runtime_env(),
        )
        .await?,
    );
    common::insert_batch(
        twin.as_ref(),
        rows(&[
            (1, Some("active")),
            (2, Some("active")),
            (3, Some("active")),
        ]),
    )
    .await?;
    let reported_without_retention = common::insert_batch(twin.as_ref(), upsert()).await?;
    let twin_rows: Vec<(i64, Option<String>)> = {
        let ctx = SessionContext::new();
        ctx.register_table("w", Arc::clone(&twin) as Arc<dyn TableProvider>)?;
        let batches = ctx
            .sql("SELECT id, status FROM w ORDER BY id")
            .await?
            .collect()
            .await?;
        let mut out = Vec::new();
        for b in &batches {
            let i = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id");
            let st = b
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("status");
            for r in 0..b.num_rows() {
                out.push((i.value(r), st.is_valid(r).then(|| st.value(r).to_string())));
            }
        }
        out
    };
    assert_eq!(
        twin_rows,
        vec![
            (1, Some("archived".to_string())),
            (2, Some("active".to_string())),
            (3, Some("active".to_string())),
            (4, Some("archived".to_string())),
            (5, Some("active".to_string())),
        ],
        "without retention the last copy of each key wins"
    );
    assert_eq!(
        common::insert_batch(t.as_ref(), upsert()).await?,
        reported_without_retention,
        "the upsert reports what it reported without retention"
    );
    assert_eq!(ids(&t).await?, vec![2, 3, 5], "right after the upsert");
    assert_eq!(count_star(&t).await?, 3);
    Ok(())
}

/// CTO C's `DELETE` runs over the whole post-conflict generation once per
/// publication, so a resident row that comes to match between writes (a time
/// cutoff passing, `random()`) must be gone when the next append publishes, and
/// a predicate that fails on a resident row must fail that append and keep the
/// old generation.
#[test]
fn resident_row_newly_matching_is_gone_at_next_publish_sqlite() -> Result<(), String> {
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        resident_row_newly_matching_is_gone_at_next_publish_impl,
    )
}

async fn resident_row_newly_matching_is_gone_at_next_publish_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};

    // `aged(status)` is TRUE for "old" once `aged` is set (a cutoff passing),
    // and errors on "old" once `broken` is set.
    // (name, partition column, primary key, on_conflict upsert, deletion mode)
    for (name, partitioned, keyed, upsert, mode) in [
        ("rsql_res_keyed", false, true, true, None),
        ("rsql_res_partitioned", true, true, true, None),
        ("rsql_res_unkeyed", false, false, false, None),
        (
            "rsql_res_key_no_conflict_key",
            false,
            true,
            false,
            Some(DeletionMode::Key),
        ),
        (
            "rsql_res_key_no_conflict_position",
            false,
            true,
            false,
            Some(DeletionMode::Position),
        ),
    ] {
        let aged = Arc::new(AtomicBool::new(false));
        let broken = Arc::new(AtomicBool::new(false));
        let (a, b) = (Arc::clone(&aged), Arc::clone(&broken));
        let udf = create_udf(
            "aged",
            vec![DataType::Utf8],
            DataType::Boolean,
            Volatility::Volatile,
            Arc::new(move |args: &[ColumnarValue]| {
                let array = args[0].clone().into_array(1)?;
                let values = array.as_any().downcast_ref::<StringArray>().expect("utf8");
                let mut out = Vec::with_capacity(values.len());
                for v in values.iter() {
                    let old = v == Some("old");
                    if old && b.load(Ordering::SeqCst) {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "aged() cannot evaluate this row".to_string(),
                        ));
                    }
                    out.push(Some(old && a.load(Ordering::SeqCst)));
                }
                Ok(ColumnarValue::Array(Arc::new(
                    arrow::array::BooleanArray::from(out),
                )))
            }),
        );
        let dir = fixture.data_path.join(name);
        std::fs::create_dir_all(&dir)?;
        let options = CreateTableOptions {
            table_name: name.to_string(),
            schema: schema(),
            primary_key: if keyed {
                vec!["id".to_string()]
            } else {
                vec![]
            },
            on_conflict: upsert
                .then(|| OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()]))),
            base_path: dir.to_string_lossy().to_string(),
            partition_column: partitioned.then(|| "status".to_string()),
            vortex_config: VortexConfig {
                deletion_mode: mode.unwrap_or(file_tier_config().deletion_mode),
                ..file_tier_config()
            },
        };
        let t = Arc::new(
            CayenneTableProvider::create_table_with_retention(
                Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                options,
                vec![udf.call(vec![col("status")])],
                SessionContext::new().runtime_env(),
            )
            .await?,
        );
        common::insert_batch(t.as_ref(), rows(&[(1, Some("old")), (2, Some("new"))])).await?;
        t.flush_pending_maintenance().await?;
        assert_eq!(ids(&t).await?, vec![1, 2], "{name}: nothing matches yet");

        // Failure first: the predicate fails on resident row 1, so the append
        // fails and the previous generation stays.
        broken.store(true, Ordering::SeqCst);
        let failed = common::insert_batch(t.as_ref(), rows(&[(3, Some("new"))])).await;
        assert!(
            failed.is_err(),
            "{name}: a predicate failing on a resident row fails the append"
        );
        assert_eq!(
            ids(&t).await?,
            vec![1, 2],
            "{name}: previous generation kept"
        );
        broken.store(false, Ordering::SeqCst);

        // Row 1 ages into retention; the next append's publication removes it.
        aged.store(true, Ordering::SeqCst);
        common::insert_batch(t.as_ref(), rows(&[(3, Some("new"))])).await?;
        assert_eq!(
            ids(&t).await?,
            vec![2, 3],
            "{name}: resident row gone at the publish"
        );
        assert_eq!(count_star(&t).await?, 2, "{name}: COUNT(*) at the publish");
        drop(t);

        // The resident delete is durable with the append: reopened without any
        // retention filter, the table holds exactly the published generation.
        let reopened = Arc::new(
            CayenneTableProviderBuilder::new(
                Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                SessionContext::new().runtime_env(),
            )
            .open(name)
            .await?,
        );
        assert_eq!(ids(&reopened).await?, vec![2, 3], "{name}: after reopening");
        assert_eq!(
            count_star(&reopened).await?,
            2,
            "{name}: COUNT(*) after reopening"
        );
    }
    Ok(())
}

/// A reader planned against the previous generation keeps reading it through a
/// carried-generation flip (table without a primary key) and the unreferenced
/// snapshot cleanup that follows; a query planned after the flip reads the new
/// generation, with an existing row now matching retention gone.
#[test]
fn old_reader_survives_carried_generation_flip_and_cleanup_sqlite() -> Result<(), String> {
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        old_reader_survives_carried_generation_flip_and_cleanup_impl,
    )
}

async fn old_reader_survives_carried_generation_flip_and_cleanup_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    let gone = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&gone);
    let udf = create_udf(
        "gone",
        vec![DataType::Int64],
        DataType::Boolean,
        Volatility::Volatile,
        Arc::new(move |args: &[ColumnarValue]| {
            let array = args[0].clone().into_array(1)?;
            let ids = array.as_any().downcast_ref::<Int64Array>().expect("int64");
            let on = flag.load(Ordering::SeqCst);
            Ok(ColumnarValue::Array(Arc::new(
                arrow::array::BooleanArray::from_iter(
                    ids.iter().map(|id| Some(on && id == Some(1))),
                ),
            )))
        }),
    );
    let t = table(
        &fixture,
        "rsql_old_reader",
        false,
        udf.call(vec![col("id")]),
    )
    .await?;
    common::insert_batch(
        t.as_ref(),
        rows(&[(1, Some("active")), (2, Some("active"))]),
    )
    .await?;
    t.flush_pending_maintenance().await?;

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&t) as Arc<dyn TableProvider>)?;
    let old_plan = ctx
        .sql("SELECT id FROM t ORDER BY id")
        .await?
        .create_physical_plan()
        .await?;

    gone.store(true, Ordering::SeqCst);
    common::insert_batch(t.as_ref(), rows(&[(3, Some("active"))])).await?;
    t.retire_unreferenced_snapshot_dirs().await?;

    let old = datafusion::physical_plan::collect(old_plan, ctx.task_ctx()).await?;
    let mut old_ids: Vec<i64> = old
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64")
                .values()
                .to_vec()
        })
        .collect();
    old_ids.sort_unstable();
    assert_eq!(
        old_ids,
        vec![1, 2],
        "planned before the flip: the old generation"
    );
    assert_eq!(
        ids(&t).await?,
        vec![2, 3],
        "planned after the flip: the new generation"
    );
    Ok(())
}
