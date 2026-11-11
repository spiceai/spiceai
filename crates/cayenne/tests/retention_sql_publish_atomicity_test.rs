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

use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};

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
        primary_key: if upsert { vec!["id".to_string()] } else { vec![] },
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
    let batches = ctx.sql("SELECT id FROM t ORDER BY id").await?.collect().await?;
    let mut out = Vec::new();
    for b in &batches {
        let c = b.column(0).as_any().downcast_ref::<Int64Array>().expect("id");
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
    common::insert_batch(t.as_ref(), batch(0..70)).await?;
    assert_eq!(counts(&t).await?, (60, 0, 10), "right after the append");
    common::insert_batch(t.as_ref(), batch(70..140)).await?;
    assert_eq!(counts(&t).await?, (120, 0, 20), "right after a second append");
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
        assert_eq!(counts(&t).await?, (60, 0, 10), "right after overwrite {round}");
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
    common::insert_batch(t.as_ref(), rows(&[(1, Some("archived"))])).await?;
    assert_eq!(ids(&t).await?, vec![2, 3], "right after the upsert");
    // A later insert of the removed key is not dropped as a duplicate.
    common::insert_batch(t.as_ref(), rows(&[(1, Some("active"))])).await?;
    assert_eq!(ids(&t).await?, vec![1, 2, 3], "key comes back once it no longer matches");
    Ok(())
}

async fn duplicate_key_upsert_last_matching_removes_key_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let t = table(&fixture, "rsql_pub_dup", true, archived()).await?;
    common::insert_batch(t.as_ref(), rows(&[(1, Some("active")), (2, Some("active"))])).await?;
    t.flush_pending_maintenance().await?;
    common::insert_batch(
        t.as_ref(),
        rows(&[(1, Some("active")), (1, Some("archived")), (2, Some("archived")), (2, Some("active"))]),
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
    common::write_batches(t.as_ref(), vec![rows(&[(1, Some("1")), (2, Some("2"))])], InsertOp::Overwrite)
        .await?;
    t.flush_pending_maintenance().await?;
    assert_eq!(ids(&t).await?, vec![2]);
    let result =
        common::write_batches(t.as_ref(), vec![batch(0..14)], InsertOp::Overwrite).await;
    assert!(result.is_err(), "the write that retention cannot apply to fails");
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
        let (t, stop, hits, reads) = (Arc::clone(&t), Arc::clone(&stop), Arc::clone(&hits), Arc::clone(&reads));
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
    assert_eq!(hits.load(Ordering::Relaxed), 0, "reads that saw archived rows");
    Ok(())
}
