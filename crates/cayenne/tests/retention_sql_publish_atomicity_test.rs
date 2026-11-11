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
test_with_backends!(durable_retention_reaches_a_preexisting_inline_corpus_impl);
test_with_backends!(failing_retention_over_a_preexisting_inline_corpus_publishes_nothing_impl);
test_with_backends!(unkeyed_retention_append_carries_a_preexisting_inline_corpus_impl);

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

/// The resident `retention_sql` delete never evaluates a hidden version: a row
/// an earlier upsert superseded, a row a `DELETE` removed, and a row the
/// current write supersedes are all skipped before the predicate, so a
/// predicate that errors on them does not fail the write. A live resident row
/// the predicate errors on still fails it and keeps the previous state. Run for
/// an Int64 key and a composite (Int64, Utf8) key.
#[test]
fn resident_retention_skips_hidden_versions_before_its_predicate_sqlite() -> Result<(), String> {
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        resident_retention_skips_hidden_versions_before_its_predicate_impl,
    )
}

async fn resident_retention_skips_hidden_versions_before_its_predicate_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    let schema3: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, true),
    ]));
    let batch = |rows: &[(i64, &str)]| -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(&schema3),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec!["a"; rows.len()])),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, s)| Some(*s)).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch")
    };
    for (name, key, mode) in [
        ("rsql_skip_int64", vec!["id"], None),
        ("rsql_skip_composite", vec!["id", "tag"], None),
        // Asking for position deletes on an upsert table still deletes by key.
        (
            "rsql_skip_position_requested",
            vec!["id"],
            Some(DeletionMode::Position),
        ),
    ] {
        let broken = Arc::new(AtomicBool::new(false));
        let b = Arc::clone(&broken);
        let udf = create_udf(
            "boom",
            vec![DataType::Utf8],
            DataType::Boolean,
            Volatility::Volatile,
            Arc::new(move |args: &[ColumnarValue]| {
                let array = args[0].clone().into_array(1)?;
                let values = array.as_any().downcast_ref::<StringArray>().expect("utf8");
                if b.load(Ordering::SeqCst) && values.iter().any(|v| v == Some("boom")) {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "boom() cannot evaluate this row".to_string(),
                    ));
                }
                Ok(ColumnarValue::Array(Arc::new(
                    arrow::array::BooleanArray::from(vec![Some(false); values.len()]),
                )))
            }),
        );
        let key: Vec<String> = key.into_iter().map(String::from).collect();
        let dir = fixture.data_path.join(name);
        std::fs::create_dir_all(&dir)?;
        let options = CreateTableOptions {
            table_name: name.to_string(),
            schema: Arc::clone(&schema3),
            primary_key: key.clone(),
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(key))),
            base_path: dir.to_string_lossy().to_string(),
            partition_column: None,
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
        assert!(
            !t.is_position_based(),
            "{name}: an upsert table deletes by key"
        );
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::clone(&t) as Arc<dyn TableProvider>)?;
        let contents = || async {
            let batches = ctx
                .sql("SELECT id, status FROM t ORDER BY id")
                .await?
                .collect()
                .await?;
            let mut out = Vec::new();
            for b in &batches {
                let ids = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id");
                let st = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("s");
                for i in 0..b.num_rows() {
                    out.push((ids.value(i), st.value(i).to_string()));
                }
            }
            Ok::<_, Box<dyn std::error::Error>>(out)
        };
        let pairs = |v: &[(i64, &str)]| -> Vec<(i64, String)> {
            v.iter().map(|(i, s)| (*i, (*s).to_string())).collect()
        };

        common::insert_batch(t.as_ref(), batch(&[(1, "boom"), (2, "boom"), (3, "boom")])).await?;
        t.flush_pending_maintenance().await?;
        common::insert_batch(t.as_ref(), batch(&[(1, "ok")])).await?;
        t.flush_pending_maintenance().await?;
        ctx.sql("DELETE FROM t WHERE id = 3")
            .await?
            .collect()
            .await?;
        assert_eq!(
            contents().await?,
            pairs(&[(1, "ok"), (2, "boom")]),
            "{name}: setup"
        );

        // Row 1's first version is superseded, row 3 is deleted, and row 2 is
        // superseded by this very write: none may reach the predicate.
        broken.store(true, Ordering::SeqCst);
        common::insert_batch(t.as_ref(), batch(&[(2, "ok"), (4, "new")])).await?;
        assert_eq!(
            contents().await?,
            pairs(&[(1, "ok"), (2, "ok"), (4, "new")]),
            "{name}: hidden versions skipped"
        );

        // Control: a LIVE resident row the predicate errors on fails the write
        // and keeps the previous state.
        broken.store(false, Ordering::SeqCst);
        common::insert_batch(t.as_ref(), batch(&[(6, "boom")])).await?;
        broken.store(true, Ordering::SeqCst);
        let failed = common::insert_batch(t.as_ref(), batch(&[(7, "new")])).await;
        assert!(
            failed.is_err(),
            "{name}: a live erroring row fails the write"
        );
        assert_eq!(
            contents().await?,
            pairs(&[(1, "ok"), (2, "ok"), (4, "new"), (6, "boom")]),
            "{name}: previous state kept"
        );
    }
    Ok(())
}

/// Every evaluation of `retention_sql` in one publication (incoming rows and
/// the resident rows it keeps) sees the same `now()`; the next publication sees
/// a later one. Run for each implemented file route.
#[test]
fn retention_now_is_fixed_once_per_publication_sqlite() -> Result<(), String> {
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        retention_now_is_fixed_once_per_publication_impl,
    )
}

async fn retention_now_is_fixed_once_per_publication_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::TimestampNanosecondArray;
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use std::sync::Mutex;
    for (name, keyed, upsert, mode) in [
        ("rsql_now_keyed", true, true, None),
        ("rsql_now_unkeyed", false, false, None),
        (
            "rsql_now_key_no_conflict",
            true,
            false,
            Some(DeletionMode::Key),
        ),
        (
            "rsql_now_key_no_conflict_position",
            true,
            false,
            Some(DeletionMode::Position),
        ),
    ] {
        let seen: Arc<Mutex<std::collections::BTreeSet<i64>>> = Arc::default();
        let calls = Arc::new(AtomicU64::new(0));
        let (s, c) = (Arc::clone(&seen), Arc::clone(&calls));
        let udf = create_udf(
            "seen",
            vec![DataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some("+00:00".into()),
            )],
            DataType::Boolean,
            Volatility::Volatile,
            Arc::new(move |args: &[ColumnarValue]| {
                let rows = match &args[0] {
                    ColumnarValue::Array(a) => a.len(),
                    ColumnarValue::Scalar(_) => 1,
                };
                let array = args[0].clone().into_array(rows)?;
                let ts = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("timestamp");
                c.fetch_add(1, Ordering::SeqCst);
                let mut set = s.lock().expect("lock");
                set.extend(ts.iter().flatten());
                Ok(ColumnarValue::Array(Arc::new(
                    arrow::array::BooleanArray::from(vec![Some(false); rows]),
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
            partition_column: None,
            vortex_config: VortexConfig {
                deletion_mode: mode.unwrap_or(file_tier_config().deletion_mode),
                ..file_tier_config()
            },
        };
        let t = Arc::new(
            CayenneTableProvider::create_table_with_retention(
                Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                options,
                vec![udf.call(vec![now()])],
                SessionContext::new().runtime_env(),
            )
            .await?,
        );
        common::insert_batch(t.as_ref(), rows(&[(1, Some("a")), (2, Some("b"))])).await?;
        t.flush_pending_maintenance().await?;
        let mut previous = None;
        for id in [3_i64, 4] {
            seen.lock().expect("lock").clear();
            let before = calls.load(Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            common::insert_batch(t.as_ref(), rows(&[(id, Some("c"))])).await?;
            let set = seen.lock().expect("lock").clone();
            let evaluations = calls.load(Ordering::SeqCst) - before;
            t.flush_pending_maintenance().await?;
            let after_maintenance = seen.lock().expect("lock").clone();
            eprintln!(
                "{name} id={id}: publication {set:?} incl. maintenance {after_maintenance:?}"
            );
            assert!(
                evaluations >= 2,
                "{name}: incoming and resident rows were both evaluated ({evaluations})"
            );
            assert_eq!(
                set.len(),
                1,
                "{name}: one now() per publication, saw {set:?}"
            );
            let value = *set.iter().next().expect("one");
            if let Some(previous) = previous {
                assert!(
                    value > previous,
                    "{name}: the next publication sees a later now()"
                );
            }
            previous = Some(value);
        }
    }
    Ok(())
}

/// `mode: memory`: an append's retention delete over the resident tier rows
/// lands in the same tier swap as the incoming rows, with no maintenance pass.
/// A resident row whose predicate errors fails the append and keeps the
/// previous tier; a version an upsert superseded is never evaluated.
#[test]
fn memory_mode_resident_retention_in_the_append_swap_sqlite() -> Result<(), String> {
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        memory_mode_resident_retention_in_the_append_swap_impl,
    )
}

async fn memory_mode_resident_retention_in_the_append_swap_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    for (name, keyed) in [("rsql_mem_keyed", true), ("rsql_mem_unkeyed", false)] {
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
            on_conflict: keyed
                .then(|| OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()]))),
            base_path: dir.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: common::lookup_index::memory_mode_config(),
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
        assert!(t.is_memory_resident_mode(), "{name}: memory mode");
        common::insert_batch(t.as_ref(), rows(&[(1, Some("old")), (2, Some("new"))])).await?;

        broken.store(true, Ordering::SeqCst);
        let failed = common::insert_batch(t.as_ref(), rows(&[(3, Some("new"))])).await;
        assert!(
            failed.is_err(),
            "{name}: an erroring resident row fails the append"
        );
        assert_eq!(ids(&t).await?, vec![1, 2], "{name}: previous tier kept");

        broken.store(false, Ordering::SeqCst);
        aged.store(true, Ordering::SeqCst);
        common::insert_batch(t.as_ref(), rows(&[(3, Some("new"))])).await?;
        assert_eq!(
            ids(&t).await?,
            vec![2, 3],
            "{name}: the aged resident row is gone in the append's own swap"
        );

        if keyed {
            aged.store(false, Ordering::SeqCst);
            common::insert_batch(t.as_ref(), rows(&[(4, Some("old"))])).await?;
            common::insert_batch(t.as_ref(), rows(&[(4, Some("new"))])).await?;
            broken.store(true, Ordering::SeqCst);
            common::insert_batch(t.as_ref(), rows(&[(5, Some("new"))])).await?;
            assert_eq!(
                ids(&t).await?,
                vec![2, 3, 4, 5],
                "{name}: the superseded version is never evaluated"
            );
        }
    }
    Ok(())
}

/// `mode: memory` CDC (`write_cdc_append_stream`): each applied batch applies
/// `retention_sql` to the incoming rows and the resident tier rows in its one
/// tier swap, under one `now()`. A resident row whose predicate errors fails
/// the batch and keeps the previous tier; a superseded version is never
/// evaluated; replaying an applied batch leaves the same rows.
#[test]
fn memory_mode_cdc_resident_retention_in_the_apply_swap_sqlite() -> Result<(), String> {
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        memory_mode_cdc_resident_retention_in_the_apply_swap_impl,
    )
}

async fn memory_mode_cdc_resident_retention_in_the_apply_swap_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::TimestampNanosecondArray;
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use std::sync::Mutex;
    let aged = Arc::new(AtomicBool::new(false));
    let broken = Arc::new(AtomicBool::new(false));
    let nows: Arc<Mutex<std::collections::BTreeSet<i64>>> = Arc::default();
    let calls = Arc::new(AtomicU64::new(0));
    let (a, b, n, c) = (
        Arc::clone(&aged),
        Arc::clone(&broken),
        Arc::clone(&nows),
        Arc::clone(&calls),
    );
    let udf = create_udf(
        "aged_at",
        vec![
            DataType::Utf8,
            DataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some("+00:00".into()),
            ),
        ],
        DataType::Boolean,
        Volatility::Volatile,
        Arc::new(move |args: &[ColumnarValue]| {
            let array = args[0].clone().into_array(1)?;
            let values = array.as_any().downcast_ref::<StringArray>().expect("utf8");
            let ts = args[1].clone().into_array(values.len())?;
            let ts = ts
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts");
            c.fetch_add(1, Ordering::SeqCst);
            n.lock().expect("lock").extend(ts.iter().flatten());
            let mut out = Vec::with_capacity(values.len());
            for v in values.iter() {
                let old = v == Some("old");
                if old && b.load(Ordering::SeqCst) {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "aged_at() cannot evaluate this row".to_string(),
                    ));
                }
                out.push(Some(old && a.load(Ordering::SeqCst)));
            }
            Ok(ColumnarValue::Array(Arc::new(
                arrow::array::BooleanArray::from(out),
            )))
        }),
    );
    let name = "rsql_mem_cdc";
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
        vortex_config: common::lookup_index::memory_mode_config(),
    };
    let t = Arc::new(
        CayenneTableProvider::create_table_with_retention(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            options,
            vec![udf.call(vec![col("status"), now()])],
            SessionContext::new().runtime_env(),
        )
        .await?,
    );
    assert!(t.is_memory_resident_mode());
    let apply = |pairs: Vec<(i64, Option<&'static str>)>| {
        let t = Arc::clone(&t);
        async move {
            let batch = rows(&pairs);
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                batch.schema(),
                futures::stream::iter([Ok(batch)]),
            ));
            let ctx = SessionContext::new();
            let write = t.write_cdc_append_stream(stream, &ctx.task_ctx()).await?;
            if write.has_pending_finalize() {
                write.finish().await?;
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        }
    };

    apply(vec![(1, Some("old")), (2, Some("new"))]).await?;
    assert_eq!(ids(&t).await?, vec![1, 2]);

    broken.store(true, Ordering::SeqCst);
    assert!(
        apply(vec![(3, Some("new"))]).await.is_err(),
        "an erroring resident row fails the batch"
    );
    assert_eq!(ids(&t).await?, vec![1, 2], "previous tier kept");

    broken.store(false, Ordering::SeqCst);
    aged.store(true, Ordering::SeqCst);
    nows.lock().expect("lock").clear();
    let before = calls.load(Ordering::SeqCst);
    apply(vec![(3, Some("new"))]).await?;
    let evaluations = calls.load(Ordering::SeqCst) - before;
    let seen = nows.lock().expect("lock").clone();
    assert!(
        evaluations >= 2,
        "incoming and resident evaluated ({evaluations})"
    );
    assert_eq!(seen.len(), 1, "one now() per applied batch: {seen:?}");
    assert_eq!(
        ids(&t).await?,
        vec![2, 3],
        "aged resident row gone in the apply swap"
    );

    aged.store(false, Ordering::SeqCst);
    apply(vec![(4, Some("old"))]).await?;
    apply(vec![(4, Some("new"))]).await?;
    broken.store(true, Ordering::SeqCst);
    apply(vec![(5, Some("new"))]).await?;
    assert_eq!(
        ids(&t).await?,
        vec![2, 3, 4, 5],
        "superseded version not evaluated"
    );

    // Replay of an applied batch (at-least-once delivery) leaves the same rows.
    apply(vec![(5, Some("new"))]).await?;
    assert_eq!(
        ids(&t).await?,
        vec![2, 3, 4, 5],
        "replay leaves the same rows"
    );
    Ok(())
}

/// A NULL primary key is refused by the existing write validation on a
/// retention table (keyed upsert, file tier), before any retention runs; the
/// table keeps its rows. Pins existing behaviour; no new NULL-key semantics.
#[test]
fn null_primary_key_is_refused_on_a_retention_table_sqlite() -> Result<(), String> {
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        null_primary_key_is_refused_on_a_retention_table_impl,
    )
}

async fn null_primary_key_is_refused_on_a_retention_table_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let nullable_id: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("status", DataType::Utf8, true),
    ]));
    let t = table(&fixture, "rsql_null_pk", true, archived()).await?;
    common::insert_batch(t.as_ref(), rows(&[(1, Some("active"))])).await?;
    let batch = RecordBatch::try_new(
        nullable_id,
        vec![
            Arc::new(Int64Array::from(vec![None, Some(2)])),
            Arc::new(StringArray::from(vec![Some("active"), Some("active")])),
        ],
    )?;
    let result = common::insert_batch(t.as_ref(), batch).await;
    eprintln!("null primary key write: {result:?}");
    assert!(result.is_err(), "a NULL primary key is refused");
    assert_eq!(ids(&t).await?, vec![1], "the table keeps its rows");
    Ok(())
}

/// After a publication's retention delete removes resident rows, a maintained
/// aggregate is not served stale: the query answers over the new generation.
/// File tier (keyed, unkeyed) and `mode: memory` (keyed).
#[test]
fn maintained_aggregate_is_not_served_stale_after_resident_retention_sqlite() -> Result<(), String>
{
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        maintained_aggregate_is_not_served_stale_after_resident_retention_impl,
    )
}

async fn maintained_aggregate_is_not_served_stale_after_resident_retention_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    use cayenne::maintained_aggregate::{
        MaintainedAggregateExpr, MaintainedAggregateFunction, MaintainedAggregateSpec,
    };
    use cayenne::optimizer_rules::CayenneMaintainedAggregateRewriter;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    for (name, keyed, memory) in [
        ("rsql_ivm_keyed", true, false),
        ("rsql_ivm_unkeyed", false, false),
        ("rsql_ivm_memory", true, true),
    ] {
        let aged = Arc::new(AtomicBool::new(false));
        let a = Arc::clone(&aged);
        let udf = create_udf(
            "aged",
            vec![DataType::Utf8],
            DataType::Boolean,
            Volatility::Volatile,
            Arc::new(move |args: &[ColumnarValue]| {
                let array = args[0].clone().into_array(1)?;
                let values = array.as_any().downcast_ref::<StringArray>().expect("utf8");
                let on = a.load(Ordering::SeqCst);
                Ok(ColumnarValue::Array(Arc::new(
                    values
                        .iter()
                        .map(|v| Some(on && v == Some("old")))
                        .collect::<arrow::array::BooleanArray>(),
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
            on_conflict: keyed
                .then(|| OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()]))),
            base_path: dir.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: if memory {
                common::lookup_index::memory_mode_config()
            } else {
                file_tier_config()
            },
        };
        let spec = MaintainedAggregateSpec {
            group_by: vec!["status".to_string()],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Sum,
                column: Some("id".to_string()),
            }],
            filter: None,
        };
        let t = Arc::new(
            CayenneTableProviderBuilder::new(
                Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                SessionContext::new().runtime_env(),
            )
            .with_maintained_aggregates(vec![spec])
            .with_retention_filters(vec![udf.call(vec![col("status")])])
            .create(options)
            .await?,
        );
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(CayenneMaintainedAggregateRewriter::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("t", Arc::clone(&t) as Arc<dyn TableProvider>)?;
        let sums = || async {
            let batches = ctx
                .sql("SELECT status, SUM(id) FROM t GROUP BY status ORDER BY status")
                .await?
                .collect()
                .await?;
            let mut out = Vec::new();
            for b in &batches {
                for i in 0..b.num_rows() {
                    out.push(format!(
                        "{}={}",
                        arrow::util::display::array_value_to_string(b.column(0), i)?,
                        arrow::util::display::array_value_to_string(b.column(1), i)?
                    ));
                }
            }
            Ok::<_, Box<dyn std::error::Error>>(out)
        };
        common::insert_batch(t.as_ref(), rows(&[(1, Some("old")), (2, Some("new"))])).await?;
        assert_eq!(sums().await?, vec!["new=2", "old=1"], "{name}: before");
        aged.store(true, Ordering::SeqCst);
        common::insert_batch(t.as_ref(), rows(&[(3, Some("new"))])).await?;
        assert_eq!(
            sums().await?,
            vec!["new=5"],
            "{name}: the aggregate reflects the resident row retention removed"
        );
    }
    Ok(())
}

/// Ownership of the previous generation across a carried-generation flip: a
/// plan built before the flip pins the previous snapshot's files, cleanup
/// defers them while it lives, the plan reads the previous generation, and
/// once it is dropped cleanup removes them.
#[test]
fn old_reader_pins_its_generation_until_dropped_sqlite() -> Result<(), String> {
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        old_reader_pins_its_generation_until_dropped_impl,
    )
}

fn vortex_files(dir: &std::path::Path) -> std::collections::BTreeSet<std::path::PathBuf> {
    let mut out = std::collections::BTreeSet::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "vortex") {
                out.insert(path);
            }
        }
    }
    out
}

async fn old_reader_pins_its_generation_until_dropped_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    // The sweep keeps a snapshot dir for `SNAPSHOT_CLEANUP_GRACE` (5 s) measured
    // from the dir's uuid7 creation time, which no test hook moves; waiting past
    // it is what separates "pinned by the reader" from "still inside its grace".
    const PAST_GRACE: std::time::Duration = std::time::Duration::from_millis(5_600);
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
                ids.iter()
                    .map(|id| Some(on && id == Some(1)))
                    .collect::<arrow::array::BooleanArray>(),
            )))
        }),
    );
    let name = "rsql_old_reader_pin";
    let t = table(&fixture, name, false, udf.call(vec![col("id")])).await?;
    common::insert_batch(
        t.as_ref(),
        rows(&[(1, Some("active")), (2, Some("active"))]),
    )
    .await?;
    t.flush_pending_maintenance().await?;
    let dir = fixture.data_path.join(name);
    let old_files = vortex_files(&dir);
    assert!(
        !old_files.is_empty(),
        "precondition: the first generation has files"
    );

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&t) as Arc<dyn TableProvider>)?;
    let old_plan = ctx
        .sql("SELECT id FROM t ORDER BY id")
        .await?
        .create_physical_plan()
        .await?;

    gone.store(true, Ordering::SeqCst);
    common::insert_batch(t.as_ref(), rows(&[(3, Some("active"))])).await?;
    let new_files = vortex_files(&dir);
    assert!(
        old_files.iter().all(|f| new_files.contains(f)),
        "precondition: inside the grace nothing is swept"
    );

    // Past the grace, with the plan alive: a sweep (forced, plus the re-arm
    // timer's) must keep the previous generation for the reader.
    tokio::time::sleep(PAST_GRACE).await;
    t.elapse_snapshot_cleanup_grace();
    t.sweep_retired_snapshot_dirs_now().await;
    t.flush_pending_maintenance().await?;
    let pinned_files = vortex_files(&dir);
    let kept_while_pinned = old_files.iter().all(|f| pinned_files.contains(f));
    eprintln!("past grace, plan alive: previous generation kept = {kept_while_pinned}");
    assert!(
        kept_while_pinned,
        "the reader pins its generation past the grace"
    );

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
    assert_eq!(old_ids, vec![1, 2], "the reader reads its own generation");
    assert_eq!(
        ids(&t).await?,
        vec![2, 3],
        "a new reader reads the new generation"
    );

    // Reader gone (its plan was consumed by `collect`). Past the grace of the
    // listing that read just made, the next commit's sweep reclaims it.
    tokio::time::sleep(PAST_GRACE).await;
    gone.store(false, Ordering::SeqCst);
    common::insert_batch(t.as_ref(), rows(&[(4, Some("active"))])).await?;
    t.flush_pending_maintenance().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let after = vortex_files(&dir);
    let remaining: Vec<_> = old_files.iter().filter(|f| after.contains(*f)).collect();
    eprintln!(
        "after reader drop + grace + commit: remaining previous-generation files {remaining:?}"
    );
    assert!(
        remaining.is_empty(),
        "the previous generation is removed once unpinned"
    );
    assert_eq!(
        ids(&t).await?,
        vec![2, 3, 4],
        "cleanup does not touch the new generation"
    );
    Ok(())
}

/// A post-write retention maintenance pass and a publication cannot interleave:
/// the pass is held inside its predicate (on its own thread) while an append is
/// started; the append does not publish until the pass finishes, and the final
/// generation is the same whichever completes first — no resurrected row, no
/// deletion applied to the wrong generation. Run with the pass succeeding and
/// with the pass failing (its failure leaves the append's publication intact).
#[test]
fn maintenance_pass_and_append_are_serialized_sqlite() -> Result<(), String> {
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        maintenance_pass_and_append_are_serialized_impl,
    )
}

async fn maintenance_pass_and_append_are_serialized_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use std::sync::Mutex;
    use std::sync::mpsc;
    const RACER: &str = "rsql-maintenance-racer";
    for (name, fail_pass) in [("rsql_race_ok", false), ("rsql_race_fail", true)] {
        let aged = Arc::new(AtomicBool::new(false));
        let (entered_tx, entered_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let gate = Arc::new(Mutex::new(Some((entered_tx, release_rx))));
        let (a, g) = (Arc::clone(&aged), Arc::clone(&gate));
        let udf = create_udf(
            "aged",
            vec![DataType::Utf8],
            DataType::Boolean,
            Volatility::Volatile,
            Arc::new(move |args: &[ColumnarValue]| {
                let on_racer = std::thread::current().name() == Some(RACER);
                if on_racer && let Some((entered, release)) = g.lock().expect("gate").take() {
                    entered.send(()).expect("entered");
                    release.recv().expect("release");
                    if fail_pass {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "maintenance pass fails".to_string(),
                        ));
                    }
                }
                let array = args[0].clone().into_array(1)?;
                let values = array.as_any().downcast_ref::<StringArray>().expect("utf8");
                let on = a.load(Ordering::SeqCst);
                Ok(ColumnarValue::Array(Arc::new(
                    values
                        .iter()
                        .map(|v| Some(on && v == Some("old")))
                        .collect::<arrow::array::BooleanArray>(),
                )))
            }),
        );
        let t = table(&fixture, name, false, udf.call(vec![col("status")])).await?;
        // This write arms a post-write retention pass; the racer drains it.
        common::insert_batch(t.as_ref(), rows(&[(1, Some("old")), (2, Some("new"))])).await?;
        aged.store(true, Ordering::SeqCst);

        let racer_table = Arc::clone(&t);
        let racer = std::thread::Builder::new()
            .name(RACER.to_string())
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("runtime")
                    .block_on(racer_table.flush_pending_maintenance())
                    .map_err(|e| e.to_string())
            })?;
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .map_err(|_| format!("{name}: the maintenance pass never evaluated retention"))?;

        let appender = Arc::clone(&t);
        let append = tokio::spawn(async move {
            common::insert_batch(appender.as_ref(), rows(&[(3, Some("new"))]))
                .await
                .map_err(|e| e.to_string())
        });
        // No wall-clock wait: the pass is parked at its entry handshake (inside
        // its predicate, holding `write_lock`), so the append cannot publish however
        // long it runs. The lock-level proof is the library test
        // `maintenance_pass_and_hybrid_apply_serialize_by_entry_handshake`.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(
            !append.is_finished(),
            "{name}: the append waits while the maintenance pass holds its generation"
        );
        // No scan here: the pass is parked inside its predicate, mid-delete. The
        // library test reads the rows while its pass is parked at entry.
        release_tx.send(())?;
        // Join off the runtime thread: this test runs on a current-thread
        // runtime, and the append may be parked mid-way (holding a lock while
        // its catalog I/O completes); a blocking join here would starve it and
        // deadlock the pass that waits on that lock.
        let pass = tokio::task::spawn_blocking(move || racer.join())
            .await?
            .expect("racer thread");
        assert_eq!(
            pass.is_err(),
            fail_pass,
            "{name}: maintenance outcome {pass:?}"
        );
        append.await??;
        assert_eq!(ids(&t).await?, vec![2, 3], "{name}: final generation");
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::clone(&t) as Arc<dyn TableProvider>)?;
        let count = ctx.sql("SELECT COUNT(*) FROM t").await?.collect().await?;
        assert_eq!(
            arrow::util::display::array_value_to_string(count[0].column(0), 0)?,
            "2",
            "{name}: COUNT(*)"
        );
    }
    Ok(())
}

struct CountingSlotAdvancer(Arc<AtomicU64>);

#[async_trait::async_trait]
impl cayenne::SlotAdvancer for CountingSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// `cdc_durability: memory` (durable files + RAM tier): each CDC apply applies
/// `retention_sql` over the logical post-batch view of BOTH tiers in its one
/// tier swap, with no maintenance pass:
/// - a durable row matching is gone at the apply (durable archived -> deleted);
/// - a durable version superseded by a tier row is neither evaluated nor
///   allowed to delete the live tier row (durable archived -> memory retained);
/// - a tier row matching is gone (durable retained -> memory archived);
/// - hidden versions (superseded, tombstoned) never reach an erroring predicate,
///   a live erroring row fails the apply and keeps the previous view;
/// - the checkpoint persists the deletion state before the source ack, a later
///   reinsert of a retention-deleted key survives the old tombstone through a
///   checkpoint, and replaying an applied batch leaves the same rows.
#[test]
fn hybrid_cdc_resident_retention_in_the_apply_swap_sqlite() -> Result<(), String> {
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        hybrid_cdc_resident_retention_in_the_apply_swap_impl,
    )
}

async fn hybrid_cdc_resident_retention_in_the_apply_swap_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
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
    let name = "rsql_hybrid_cdc";
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
        vortex_config: VortexConfig {
            cdc_durability: CdcDurability::Memory,
            cdc_mem_tier_checkpoint_interval_ms: 0,
            cdc_mem_tier_max_age_ms: 0,
            cdc_mem_tier_seal_age_ms: 0,
            compaction_background_interval_ms: 3_600_000,
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
    assert!(t.is_cdc_memory_mode() && !t.is_memory_resident_mode());
    let acks = Arc::new(AtomicU64::new(0));
    t.install_slot_advancer(Arc::new(CountingSlotAdvancer(Arc::clone(&acks))));
    let apply = |pairs: Vec<(i64, Option<&'static str>)>| {
        let t = Arc::clone(&t);
        async move {
            let batch = rows(&pairs);
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                batch.schema(),
                futures::stream::iter([Ok(batch)]),
            ));
            let ctx = SessionContext::new();
            let write = t.write_cdc_append_stream(stream, &ctx.task_ctx()).await?;
            if write.has_pending_finalize() {
                write.finish().await?;
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        }
    };

    // Durable: 1 old, 2 new, 3 new, 5 old.
    apply(vec![
        (1, Some("old")),
        (2, Some("new")),
        (3, Some("new")),
        (5, Some("old")),
    ])
    .await?;
    assert!(
        t.checkpoint_mem_tier().await? > 0,
        "precondition: rows are durable"
    );
    // Tier: 2 old (supersedes durable 2 new), 4 old, 5 new (supersedes durable 5 old).
    apply(vec![(2, Some("old")), (4, Some("old")), (5, Some("new"))]).await?;
    assert_eq!(ids(&t).await?, vec![1, 2, 3, 4, 5]);

    // A live erroring row (durable 1, tier 2 and 4) fails the apply; nothing moves.
    broken.store(true, Ordering::SeqCst);
    assert!(
        apply(vec![(6, Some("new"))]).await.is_err(),
        "live erroring row fails"
    );
    assert_eq!(ids(&t).await?, vec![1, 2, 3, 4, 5], "previous view kept");
    broken.store(false, Ordering::SeqCst);

    aged.store(true, Ordering::SeqCst);
    apply(vec![(6, Some("new"))]).await?;
    assert_eq!(
        ids(&t).await?,
        vec![3, 5, 6],
        "durable 1 and tier 2, 4 gone at the apply; 5's tier row kept"
    );

    // Every remaining "old" version is hidden: none may reach the predicate.
    broken.store(true, Ordering::SeqCst);
    apply(vec![(7, Some("new"))]).await?;
    assert_eq!(
        ids(&t).await?,
        vec![3, 5, 6, 7],
        "hidden versions not evaluated"
    );
    broken.store(false, Ordering::SeqCst);

    // Crash before the checkpoint: nothing the applies did (retention included)
    // is durable and nothing was acked, so the source replays them.
    let reopen = || async {
        Ok::<_, Box<dyn std::error::Error>>(Arc::new(
            CayenneTableProviderBuilder::new(
                Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                SessionContext::new().runtime_env(),
            )
            .open(name)
            .await?,
        ))
    };
    let before = acks.load(Ordering::SeqCst);
    assert_eq!(
        ids(&reopen().await?).await?,
        vec![1, 2, 3, 5],
        "crash before checkpoint: only the first checkpoint is durable"
    );

    // Checkpoint: deletion state durable, then the ack.
    t.checkpoint_mem_tier().await?;
    assert!(
        acks.load(Ordering::SeqCst) > before,
        "source acked after the checkpoint"
    );
    assert_eq!(ids(&t).await?, vec![3, 5, 6, 7], "after the checkpoint");
    assert_eq!(
        ids(&reopen().await?).await?,
        vec![3, 5, 6, 7],
        "crash after checkpoint: the retention deletes were durable before the ack"
    );

    // Reinsert of a retention-deleted key survives the old tombstone.
    aged.store(false, Ordering::SeqCst);
    apply(vec![(1, Some("new"))]).await?;
    assert_eq!(ids(&t).await?, vec![1, 3, 5, 6, 7], "reinsert visible");
    t.checkpoint_mem_tier().await?;
    assert_eq!(
        ids(&t).await?,
        vec![1, 3, 5, 6, 7],
        "reinsert survives checkpoint"
    );
    assert_eq!(
        ids(&reopen().await?).await?,
        vec![1, 3, 5, 6, 7],
        "reinsert durable"
    );

    // Replay of an applied batch leaves the same rows.
    apply(vec![(7, Some("new"))]).await?;
    assert_eq!(ids(&t).await?, vec![1, 3, 5, 6, 7], "replay");
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&t) as Arc<dyn TableProvider>)?;
    let count = ctx.sql("SELECT COUNT(*) FROM t").await?.collect().await?;
    assert_eq!(
        arrow::util::display::array_value_to_string(count[0].column(0), 0)?,
        "5",
        "COUNT(*)"
    );
    Ok(())
}

/// `cdc_durability: memory` with a composite `(tenant, id)` key: the retention
/// delete in the apply sees the newest version of every key across the durable
/// files, the inlined corpus and the RAM tier (several tier versions per key),
/// in both directions (durable archived row superseded by a retained tier row is
/// kept; durable retained row superseded by an archived tier row is deleted).
/// Run unsharded and with 4 PK shards (the sharded apply path), with the inline
/// corpus off and on. Then checkpoint, compaction and reinsertion: an old
/// tombstone never hides a later reinsert, and a compaction that drops
/// tombstones never brings a deleted row back, including after reopening.
#[test]
fn hybrid_cdc_composite_retention_across_versions_shards_and_compaction_sqlite()
-> Result<(), String> {
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        hybrid_cdc_composite_retention_across_versions_shards_and_compaction_impl,
    )
}

async fn hybrid_cdc_composite_retention_across_versions_shards_and_compaction_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    let schema3: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
    ]));
    let batch = |rows: &[(i64, i64, &str)]| -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(&schema3),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(t, _, _)| *t).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, i, _)| *i).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, _, s)| Some(*s)).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch")
    };
    let keys = |provider: Arc<CayenneTableProvider>| async move {
        let ctx = SessionContext::new();
        ctx.register_table("t", provider as Arc<dyn TableProvider>)?;
        let batches = ctx
            .sql("SELECT tenant, id, status FROM t ORDER BY tenant, id")
            .await?
            .collect()
            .await?;
        let mut out = Vec::new();
        for b in &batches {
            let t = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("t");
            let i = b
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i");
            let s = b
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("s");
            out.extend((0..b.num_rows()).map(|r| (t.value(r), i.value(r), s.value(r).to_string())));
        }
        Ok::<_, Box<dyn std::error::Error>>(out)
    };
    let k = |rows: &[(i64, i64, &str)]| -> Vec<(i64, i64, String)> {
        rows.iter()
            .map(|(t, i, s)| (*t, *i, (*s).to_string()))
            .collect()
    };

    for (shards, inline) in [(1_usize, false), (1, true), (4, false), (4, true)] {
        let label = format!("shards={shards} inline={inline}");
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
        let name = format!("rsql_hybrid_comp_{shards}_{inline}");
        let dir = fixture.data_path.join(&name);
        std::fs::create_dir_all(&dir)?;
        let key = vec!["tenant".to_string(), "id".to_string()];
        let base = if inline {
            VortexConfig::default()
        } else {
            file_tier_config()
        };
        let vortex_config = VortexConfig {
            cdc_durability: CdcDurability::Memory,
            cdc_mem_tier_shards: shards,
            cdc_mem_tier_checkpoint_interval_ms: 0,
            cdc_mem_tier_max_age_ms: 0,
            cdc_mem_tier_seal_age_ms: 0,
            compaction_background_interval_ms: 3_600_000,
            compaction_trigger_files: 2,
            compaction_trigger_protected_snapshots: 2,
            ..base
        };
        let options = CreateTableOptions {
            table_name: name.clone(),
            schema: Arc::clone(&schema3),
            primary_key: key.clone(),
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(key))),
            base_path: dir.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: vortex_config.clone(),
        };
        let seed = batch(&[(1, 1, "old"), (1, 2, "new"), (2, 1, "old"), (2, 2, "new")]);
        // Inline on: retention tables never inline their own writes, so the
        // inlined corpus is the one a table carries from before `retention_sql`
        // was configured: seed it without retention, then reopen with it.
        let mut inline_gen_before = 0;
        let mut inline_gen_after = 0;
        let t = if inline {
            let plain = CayenneTableProvider::create_table_with_retention(
                Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                options,
                vec![],
                SessionContext::new().runtime_env(),
            )
            .await?;
            inline_gen_before = plain.inlined_generation();
            common::insert_batch(&plain, seed.clone()).await?;
            inline_gen_after = plain.inlined_generation();
            assert!(
                inline_gen_after > inline_gen_before,
                "{label}: the seed is in the inlined corpus"
            );
            drop(plain);
            let runtime_env = SessionContext::new().runtime_env();
            Arc::new(
                CayenneTableProviderBuilder::new(
                    Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                    Arc::clone(&runtime_env),
                )
                .with_context(cayenne::CayenneContext::new(
                    &vortex_config,
                    runtime_env,
                    &name,
                ))
                .with_retention_filters(vec![udf.call(vec![col("status")])])
                .open(&name)
                .await?,
            )
        } else {
            Arc::new(
                CayenneTableProvider::create_table_with_retention(
                    Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                    options,
                    vec![udf.call(vec![col("status")])],
                    SessionContext::new().runtime_env(),
                )
                .await?,
            )
        };
        assert!(
            t.is_cdc_memory_mode() && !t.is_memory_resident_mode(),
            "{label}"
        );
        let acks = Arc::new(AtomicU64::new(0));
        let apply = |rows: Vec<(i64, i64, &'static str)>| {
            let t = Arc::clone(&t);
            let b = batch(&rows);
            async move {
                let stream = Box::pin(RecordBatchStreamAdapter::new(
                    b.schema(),
                    futures::stream::iter([Ok(b)]),
                ));
                let ctx = SessionContext::new();
                let write = t.write_cdc_append_stream(stream, &ctx.task_ctx()).await?;
                if write.has_pending_finalize() {
                    write.finish().await?;
                }
                Ok::<_, Box<dyn std::error::Error>>(())
            }
        };
        let reopen = || async {
            Ok::<_, Box<dyn std::error::Error>>(Arc::new(
                CayenneTableProviderBuilder::new(
                    Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                    SessionContext::new().runtime_env(),
                )
                .open(&name)
                .await?,
            ))
        };

        // Durable generation.
        if !inline {
            t.install_slot_advancer(Arc::new(CountingSlotAdvancer(Arc::clone(&acks))));
            apply(vec![
                (1, 1, "old"),
                (1, 2, "new"),
                (2, 1, "old"),
                (2, 2, "new"),
            ])
            .await?;
            t.checkpoint_mem_tier().await?;
            // The checkpoint armed a background retention pass, which checkpoints
            // the tier itself; let it finish so the "crash before checkpoint"
            // reopen below sees only this durable generation.
            t.drain_in_flight_maintenance().await?;
        }
        t.install_slot_advancer(Arc::new(CountingSlotAdvancer(Arc::clone(&acks))));
        // Two tier versions per key; the newest decides.
        apply(vec![(1, 1, "new"), (1, 2, "old")]).await?;
        apply(vec![(1, 1, "old"), (1, 2, "new"), (2, 2, "new")]).await?;
        let all = k(&[(1, 1, "old"), (1, 2, "new"), (2, 1, "old"), (2, 2, "new")]);
        assert_eq!(
            keys(Arc::clone(&t)).await?,
            all,
            "{label}: before retention"
        );

        // A live erroring row (durable (2,1), tier (1,1)) fails the apply.
        broken.store(true, Ordering::SeqCst);
        assert!(
            apply(vec![(3, 1, "new")]).await.is_err(),
            "{label}: live erroring row fails"
        );
        assert_eq!(keys(Arc::clone(&t)).await?, all, "{label}: view kept");
        broken.store(false, Ordering::SeqCst);

        aged.store(true, Ordering::SeqCst);
        apply(vec![(3, 1, "new")]).await?;
        let retained = k(&[(1, 2, "new"), (2, 2, "new"), (3, 1, "new")]);
        assert_eq!(
            keys(Arc::clone(&t)).await?,
            retained,
            "{label}: tier (1,1) and durable (2,1) deleted in the apply; (1,2) kept"
        );
        // Every remaining "old" version is hidden: none may reach the predicate.
        broken.store(true, Ordering::SeqCst);
        apply(vec![(3, 2, "new")]).await?;
        broken.store(false, Ordering::SeqCst);
        aged.store(false, Ordering::SeqCst);
        let retained = k(&[(1, 2, "new"), (2, 2, "new"), (3, 1, "new"), (3, 2, "new")]);
        assert_eq!(
            keys(Arc::clone(&t)).await?,
            retained,
            "{label}: hidden skipped"
        );

        let before = acks.load(Ordering::SeqCst);
        assert_eq!(
            keys(reopen().await?).await?,
            all,
            "{label}: crash before checkpoint keeps the durable generation"
        );
        t.checkpoint_mem_tier().await?;
        assert!(acks.load(Ordering::SeqCst) > before, "{label}: acked");
        assert_eq!(
            keys(reopen().await?).await?,
            retained,
            "{label}: durable before ack"
        );

        // Reinsert both deleted keys, then checkpoint and compact twice. No
        // post-write compaction may merge the runs before the explicit one.
        let held = t.hold_compaction().await;
        apply(vec![(2, 1, "new"), (1, 1, "new")]).await?;
        let reinserted = k(&[
            (1, 1, "new"),
            (1, 2, "new"),
            (2, 1, "new"),
            (2, 2, "new"),
            (3, 1, "new"),
            (3, 2, "new"),
        ]);
        assert_eq!(
            keys(Arc::clone(&t)).await?,
            reinserted,
            "{label}: reinserted"
        );
        t.checkpoint_mem_tier().await?;
        // Before compacting: (2,2) deleted through its incoming archived version,
        // and a resident tier row (4,1) deleted by a later apply's retention.
        aged.store(true, Ordering::SeqCst);
        apply(vec![(2, 2, "old")]).await?;
        aged.store(false, Ordering::SeqCst);
        apply(vec![(4, 1, "old")]).await?;
        aged.store(true, Ordering::SeqCst);
        apply(vec![(4, 2, "new")]).await?;
        aged.store(false, Ordering::SeqCst);
        let final_rows = k(&[
            (1, 1, "new"),
            (1, 2, "new"),
            (2, 1, "new"),
            (3, 1, "new"),
            (3, 2, "new"),
            (4, 2, "new"),
        ]);
        assert_eq!(
            keys(Arc::clone(&t)).await?,
            final_rows,
            "{label}: (2,2), (4,1) gone"
        );
        t.checkpoint_mem_tier().await?;
        t.drain_in_flight_maintenance_holding_compaction().await?;
        drop(held);
        // Compaction variants (inline off): the merge folds the snapshots holding
        // the deleted rows together with their tombstones: rows physically
        // dropped, tombstones at or below the merged fence no longer applied.
        // Nothing may come back. The merge must really run. The inline variants
        // prove the inlined-corpus arm and do not claim compaction coverage (their
        // folded snapshot sizes need not share a size tier, so no merge is forced).
        if !inline {
            assert!(
                t.compact_protected_snapshots_subset(usize::MAX).await?,
                "{label}: the compaction ran"
            );
        }
        assert_eq!(
            keys(Arc::clone(&t)).await?,
            final_rows,
            "{label}: after compaction"
        );
        assert_eq!(
            keys(reopen().await?).await?,
            final_rows,
            "{label}: compacted, reopened"
        );
        // An old tombstone never hides a reinsert, across the compaction too.
        apply(vec![(2, 2, "new"), (4, 1, "new")]).await?;
        t.checkpoint_mem_tier().await?;
        let mut again = final_rows.clone();
        again.extend(k(&[(2, 2, "new"), (4, 1, "new")]));
        again.sort();
        assert_eq!(
            keys(Arc::clone(&t)).await?,
            again,
            "{label}: reinserted after compaction"
        );
        assert_eq!(
            keys(reopen().await?).await?,
            again,
            "{label}: reinsert durable"
        );
        eprintln!("{label}: inlined_generation {inline_gen_before}->{inline_gen_after}");
    }
    Ok(())
}

/// A keyed table whose rows were inlined before `retention_sql` was configured:
/// the first durable write after it is (a CDC apply, then a SQL insert, each of
/// a distinct key) publishes the retention delete over those inline rows with
/// it. Read immediately and after a reopen, no matching row survives
/// (Codex adversarial review F3: the durable resident scan covered files and
/// protected/cold snapshots but not the catalog's inline corpus).
async fn durable_retention_reaches_a_preexisting_inline_corpus_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    for (name, via_cdc) in [
        ("rsql_pub_inline_corpus_cdc", true),
        ("rsql_pub_inline_corpus_sql", false),
    ] {
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
            vortex_config: VortexConfig::default(),
        };
        let plain = CayenneTableProvider::create_table_with_retention(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            options,
            vec![],
            SessionContext::new().runtime_env(),
        )
        .await?;
        let before = plain.inlined_generation();
        common::insert_batch(
            &plain,
            rows(&[
                (1, Some("archived")),
                (2, Some("active")),
                (3, Some("archived")),
            ]),
        )
        .await?;
        assert!(
            plain.inlined_generation() > before,
            "{name}: precondition: the seed is in the inline corpus"
        );
        drop(plain);
        let open = |retention: Vec<Expr>| {
            let catalog = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
            async move {
                Ok::<_, Box<dyn std::error::Error>>(Arc::new(
                    CayenneTableProviderBuilder::new(catalog, SessionContext::new().runtime_env())
                        .with_retention_filters(retention)
                        .open(name)
                        .await?,
                ))
            }
        };
        let t = open(vec![archived()]).await?;
        let batch = rows(&[(4, Some("active"))]);
        if via_cdc {
            cdc_apply(&t, batch).await?;
        } else {
            common::insert_batch(t.as_ref(), batch).await?;
        }
        assert_eq!(
            ids(&t).await?,
            vec![2, 4],
            "{name}: the write published the delete of the matching inline rows with it"
        );
        assert_eq!(count_star(&t).await?, 2, "{name}: COUNT(*)");
        drop(t);
        let reopened = open(vec![]).await?;
        assert_eq!(ids(&reopened).await?, vec![2, 4], "{name}: durable");
    }
    Ok(())
}

/// Seed `seed` into a table's inline corpus with no `retention_sql`, then reopen
/// it with `retention`.
async fn reopen_inline_seeded(
    fixture: &common::TestFixture,
    name: &str,
    keyed: bool,
    seed: RecordBatch,
    retention: Vec<Expr>,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let dir = fixture.data_path.join(name);
    std::fs::create_dir_all(&dir)?;
    let key = if keyed {
        vec!["id".to_string()]
    } else {
        vec![]
    };
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: key.clone(),
        on_conflict: keyed.then(|| OnConflict::Upsert(ColumnReference::new(key))),
        base_path: dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig::default(),
    };
    let plain = CayenneTableProvider::create_table_with_retention(
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
        options,
        vec![],
        SessionContext::new().runtime_env(),
    )
    .await?;
    let before = plain.inlined_generation();
    common::insert_batch(&plain, seed).await?;
    assert!(
        plain.inlined_generation() > before,
        "{name}: precondition: the seed is in the inline corpus"
    );
    drop(plain);
    Ok(Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            SessionContext::new().runtime_env(),
        )
        .with_retention_filters(retention)
        .open(name)
        .await?,
    ))
}

async fn reopen_plain(
    fixture: &common::TestFixture,
    name: &str,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    Ok(Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            SessionContext::new().runtime_env(),
        )
        .open(name)
        .await?,
    ))
}

/// A predicate error over a preexisting inline corpus fails the write and
/// publishes nothing: the logical rows are the old ones, immediately and after
/// a reopen, whatever storage the corpus was moved to; the next evaluable write
/// applies.
async fn failing_retention_over_a_preexisting_inline_corpus_publishes_nothing_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let name = "rsql_pub_inline_corpus_fails";
    // Non-integer statuses make this predicate fail.
    let failing = cast(col("status"), DataType::Int32).eq(lit(1_i32));
    let t = reopen_inline_seeded(
        &fixture,
        name,
        true,
        rows(&[(1, Some("x")), (2, Some("2"))]),
        vec![failing],
    )
    .await?;
    for attempt in 0..2 {
        assert!(
            cdc_apply(&t, rows(&[(3, Some("3"))])).await.is_err(),
            "attempt {attempt}: retention cannot evaluate the inline row"
        );
        assert_eq!(
            ids(&t).await?,
            vec![1, 2],
            "attempt {attempt}: nothing published"
        );
        assert_eq!(count_star(&t).await?, 2, "attempt {attempt}: COUNT(*)");
    }
    drop(t);
    assert_eq!(
        ids(&reopen_plain(&fixture, name).await?).await?,
        vec![1, 2],
        "durable"
    );
    Ok(())
}

/// A table without a primary key whose rows were inlined before `retention_sql`
/// was configured: its next append moves the corpus into files and carries it,
/// with the retention delete over it, in one generation (previously refused).
async fn unkeyed_retention_append_carries_a_preexisting_inline_corpus_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let name = "rsql_pub_unkeyed_inline_corpus";
    let t = reopen_inline_seeded(
        &fixture,
        name,
        false,
        rows(&[(1, Some("archived")), (2, Some("active"))]),
        vec![archived()],
    )
    .await?;
    common::insert_batch(
        t.as_ref(),
        rows(&[(3, Some("active")), (4, Some("archived"))]),
    )
    .await?;
    assert_eq!(ids(&t).await?, vec![2, 3], "retained at publication");
    assert_eq!(count_star(&t).await?, 2);
    drop(t);
    assert_eq!(
        ids(&reopen_plain(&fixture, name).await?).await?,
        vec![2, 3],
        "durable"
    );
    Ok(())
}
