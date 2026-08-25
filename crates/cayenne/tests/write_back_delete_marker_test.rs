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

#![expect(
    clippy::expect_used,
    reason = "test code asserts with descriptive expect messages"
)]

//! A committed `DELETE` on a durable-write-back table must record its keys as
//! delete markers, only a user `DELETE` may do so, and the markers must be
//! scheduled for delivery without ever being dropped.
//!
//! Delivery removes a row from the federated source when — and only when — a
//! marker says a `DELETE` removed it. That makes the marking itself the safety
//! boundary: a delete that fails to mark leaves the source holding a row the
//! user deleted, while a non-user delete that marks (a retention prune, which
//! only evicts from the accelerator) destroys a source row nobody deleted.
//!
//! Because a marker is only ever retired by delivering it, an undeliverable one
//! stays forever — so the two claim queues (`delivery_attempts <= 1` and the
//! least-recently-attempted rotation above it) are what keep those from
//! starving newer writes. The scheduling tests at the bottom cover that split.

mod common;

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::CreateTableOptions;
use cayenne::row_converter::{RowConverter, SortField};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, WriteBackOp};

use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_expr::{col, lit};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

test_with_backends!(a_user_delete_marks_its_keys_for_write_back);
test_with_backends!(a_retention_delete_marks_nothing);
test_with_backends!(a_delete_on_a_table_without_write_back_marks_nothing);
test_with_backends!(a_retention_shaped_user_delete_still_marks_its_keys);
test_with_backends!(consecutive_deletes_mark_with_strictly_increasing_sequences);
test_with_backends!(a_delete_all_marks_mem_tier_resident_keys);
test_with_backends!(a_marker_reaches_the_deferred_queue_only_after_a_second_failed_attempt);
test_with_backends!(the_deferred_queue_returns_the_least_recently_attempted_first);
test_with_backends!(a_re_mark_returns_a_deferred_marker_to_the_fresh_queue);
test_with_backends!(an_attempt_against_a_superseded_marker_is_not_charged);

async fn create_table(
    fixture: &common::TestFixture,
    table_name: &str,
    durable_write_back: bool,
) -> Result<(CayenneTableProvider, Arc<Schema>), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog = Arc::clone(&fixture.catalog);
    let ctx = SessionContext::new();
    let table = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .with_durable_write_back(durable_write_back)
        .create(options)
        .await?;
    Ok((table, schema))
}

async fn seed_rows(
    table: &CayenneTableProvider,
    schema: &Arc<Schema>,
    ids: &[i64],
) -> Result<(), Box<dyn std::error::Error>> {
    let values: Vec<String> = ids.iter().map(|id| format!("row_{id}")).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(values)),
        ],
    )?;
    common::insert_batch(table, batch).await?;
    Ok(())
}

/// The `RowConverter` `OwnedRow` encoding of an `id`, which is what a marker
/// stores as its `pk_bytes`.
///
/// The expected bytes are built here rather than by decoding the stored ones, so
/// these tests assert what actually landed in the marker table instead of
/// round-tripping through the provider's decoder.
fn marker_key_bytes(id: i64) -> Vec<u8> {
    let converter =
        RowConverter::new(vec![SortField::new(DataType::Int64)]).expect("primary-key RowConverter");
    let column: ArrayRef = Arc::new(Int64Array::from(vec![id]));
    let rows = converter
        .convert_columns(std::slice::from_ref(&column))
        .expect("the key column encodes");
    rows.row(0).as_ref().to_vec()
}

/// Every undelivered marker, from both delivery queues. These tests never run a
/// delivery pass, so in practice all of them sit in the fresh queue — the union
/// is what keeps that an implementation detail rather than something a test
/// silently depends on.
async fn all_markers(table: &CayenneTableProvider) -> Vec<cayenne::PendingWriteBackMarker> {
    let mut markers = table
        .list_fresh_dirty_keys(1024)
        .await
        .expect("listing fresh write-back markers succeeds");
    markers.extend(
        table
            .list_deferred_dirty_keys(1024)
            .await
            .expect("listing deferred write-back markers succeeds"),
    );
    markers
}

/// The marked keys as `(id, op)` pairs, sorted by id. Any marker whose key is
/// not one of `expected_ids` fails the test rather than being ignored, so a
/// delete that marked the wrong key cannot pass unnoticed.
async fn marked_keys(
    table: &CayenneTableProvider,
    expected_ids: &[i64],
) -> Vec<(i64, WriteBackOp)> {
    let markers = all_markers(table).await;

    let known: Vec<(Vec<u8>, i64)> = expected_ids
        .iter()
        .map(|id| (marker_key_bytes(*id), *id))
        .collect();

    let mut pairs: Vec<(i64, WriteBackOp)> = markers
        .iter()
        .map(|marker| {
            let id = known
                .iter()
                .find(|(bytes, _)| *bytes == marker.pk_bytes)
                .map_or_else(
                    || {
                        panic!(
                            "a marker was written for a key outside {expected_ids:?}: {:?}",
                            marker.pk_bytes
                        )
                    },
                    |(_, id)| *id,
                );
            (id, marker.op)
        })
        .collect();
    pairs.sort_by_key(|(id, _)| *id);
    pairs
}

async fn delete_where_id_in(
    table: &CayenneTableProvider,
    ids: &[i64],
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let filter = col("id").in_list(ids.iter().map(|id| lit(*id)).collect(), false);
    let plan = table.delete_from(&ctx.state(), vec![filter]).await?;
    datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

/// The keys a user `DELETE` removed are marked, with the delete op, and no other
/// key is: delivery deletes exactly what the statement deleted.
async fn a_user_delete_marks_its_keys_for_write_back(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, schema) = create_table(&fixture, "wb_delete_marks", true).await?;
    seed_rows(&table, &schema, &[1, 2, 3]).await?;

    // The non-transactional seed does not mark (only a commit transaction does),
    // so anything present afterwards came from the delete.
    let before = marked_keys(&table, &[1, 2, 3]).await;
    assert!(
        before.iter().all(|(_, op)| *op != WriteBackOp::Delete),
        "no delete marker should exist before the delete: {before:?}"
    );

    delete_where_id_in(&table, &[2]).await?;

    let marked = marked_keys(&table, &[1, 2, 3]).await;
    let deletes: Vec<i64> = marked
        .iter()
        .filter(|(_, op)| *op == WriteBackOp::Delete)
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(
        deletes,
        vec![2],
        "exactly the deleted key must be marked for deletion, got {marked:?}"
    );

    Ok(())
}

/// Retention evicts rows from the accelerator; it makes no claim about the
/// federated source. Marking one would delete a live source row.
async fn a_retention_delete_marks_nothing(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, schema) = create_table(&fixture, "wb_retention_marks", true).await?;
    seed_rows(&table, &schema, &[1, 2, 3]).await?;

    let ctx = SessionContext::new();
    let filter = col("id").in_list(vec![lit(2_i64)], false);
    let plan = table.delete_for_retention(vec![filter]).await?;
    datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;

    let marked = marked_keys(&table, &[1, 2, 3]).await;
    assert!(
        marked.iter().all(|(_, op)| *op != WriteBackOp::Delete),
        "a retention delete must never mark a key for deletion at the source, got {marked:?}"
    );

    Ok(())
}

/// A table that does not deliver to a federated source keeps no markers at all,
/// so the delete path costs it nothing.
async fn a_delete_on_a_table_without_write_back_marks_nothing(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, schema) = create_table(&fixture, "wb_disabled_marks", false).await?;
    seed_rows(&table, &schema, &[1, 2, 3]).await?;

    delete_where_id_in(&table, &[2]).await?;

    assert!(
        marked_keys(&table, &[1, 2, 3]).await.is_empty(),
        "a table without durable write-back must not accumulate markers"
    );

    Ok(())
}

/// A user `DELETE` whose predicate happens to have the retention shape
/// (`retention_col < threshold` on a table with time retention configured) is
/// still a user delete: it must mark its keys instead of taking the whole-file
/// delete path, which removes files by their stats without ever enumerating the
/// keys inside them.
async fn a_retention_shaped_user_delete_still_marks_its_keys(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::TimestampMicrosecondArray;
    use arrow::datatypes::TimeUnit;
    use datafusion::scalar::ScalarValue;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    let options = CreateTableOptions {
        table_name: "wb_retention_shaped_delete".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };
    let catalog = Arc::clone(&fixture.catalog);
    let ctx = SessionContext::new();
    let table = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .with_durable_write_back(true)
        .with_time_retention_filter_builder(cayenne::TimeRetentionFilterBuilder::try_new(
            "event_time",
            3600,
            &schema,
        )?)
        .create(options)
        .await?;

    // Every row is older than the threshold below, so the delete removes all of
    // them.
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_000_i64, 2_000, 3_000,
            ])),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    let threshold = ScalarValue::TimestampMicrosecond(Some(1_000_000_000), None);
    let plan = table
        .delete_from(&ctx.state(), vec![col("event_time").lt(lit(threshold))])
        .await?;
    datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;

    let deletes: Vec<i64> = marked_keys(&table, &[1, 2, 3])
        .await
        .into_iter()
        .filter(|(_, op)| *op == WriteBackOp::Delete)
        .map(|(id, _)| id)
        .collect();
    assert_eq!(
        deletes,
        vec![1, 2, 3],
        "a retention-shaped user DELETE must mark every key it removed"
    );

    Ok(())
}

/// Each delete's markers carry a sequence STRICTLY greater than any earlier
/// commit's: the delivery worker's compare-and-clear removes markers at
/// `sequence_number <=` the claimed value, so a delete re-marked at an
/// already-claimed sequence would be cleared by the in-flight delivery and the
/// source would keep the row forever.
async fn consecutive_deletes_mark_with_strictly_increasing_sequences(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, schema) = create_table(&fixture, "wb_delete_sequences", true).await?;
    seed_rows(&table, &schema, &[1, 2, 3]).await?;

    delete_where_id_in(&table, &[2]).await?;
    delete_where_id_in(&table, &[3]).await?;

    let sequence_of = |markers: &[cayenne::PendingWriteBackMarker], id: i64| {
        let key = marker_key_bytes(id);
        markers
            .iter()
            .find(|marker| marker.pk_bytes == key)
            .map_or_else(
                || panic!("a delete marker for id {id} must exist"),
                |marker| marker.sequence_number,
            )
    };
    let markers = all_markers(&table).await;
    let first = sequence_of(&markers, 2);
    let second = sequence_of(&markers, 3);
    assert!(
        second > first,
        "the second delete's marker sequence ({second}) must be strictly greater than the first's ({first})"
    );

    Ok(())
}

/// A delete-all discards uncheckpointed rows still resident in the in-memory
/// mem-tier wholesale, without routing them through the file or inline delete
/// paths — their keys must still be marked, or those rows survive forever at
/// the federated source. `memory_mode` keeps every row in the mem-tier, so it
/// exercises exactly that path.
async fn a_delete_all_marks_mem_tier_resident_keys(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let options = CreateTableOptions {
        table_name: "wb_delete_all_mem_tier".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig {
            memory_mode: true,
            ..Default::default()
        },
    };
    let catalog = Arc::clone(&fixture.catalog);
    let ctx = SessionContext::new();
    let table = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .with_durable_write_back(true)
        .create(options)
        .await?;
    seed_rows(&table, &schema, &[1, 2, 3]).await?;

    let plan = table.delete_from(&ctx.state(), vec![lit(true)]).await?;
    datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;

    let deletes: Vec<i64> = marked_keys(&table, &[1, 2, 3])
        .await
        .into_iter()
        .filter(|(_, op)| *op == WriteBackOp::Delete)
        .map(|(id, _)| id)
        .collect();
    assert_eq!(
        deletes,
        vec![1, 2, 3],
        "a delete-all must mark every mem-tier-resident key it discarded"
    );

    Ok(())
}

/// Timestamps order the deferred rotation, and the metastore stamps them at
/// millisecond resolution — so two attempts recorded back-to-back can land on
/// the same instant and compare equal. Tests that assert an order step past that
/// boundary deliberately; time itself is what is under test here.
const TIMESTAMP_TICK: std::time::Duration = std::time::Duration::from_millis(10);

/// A key's marker, or `None` once nothing pending remains for it.
async fn marker_for(
    table: &CayenneTableProvider,
    id: i64,
) -> Option<cayenne::PendingWriteBackMarker> {
    let key = marker_key_bytes(id);
    all_markers(table)
        .await
        .into_iter()
        .find(|marker| marker.pk_bytes == key)
}

/// Which queue a key's marker is currently claimed from.
async fn queue_of(table: &CayenneTableProvider, id: i64) -> &'static str {
    let key = marker_key_bytes(id);
    let fresh = table
        .list_fresh_dirty_keys(1024)
        .await
        .expect("listing the fresh queue succeeds");
    if fresh.iter().any(|marker| marker.pk_bytes == key) {
        return "fresh";
    }
    let deferred = table
        .list_deferred_dirty_keys(1024)
        .await
        .expect("listing the deferred queue succeeds");
    if deferred.iter().any(|marker| marker.pk_bytes == key) {
        return "deferred";
    }
    "absent"
}

/// Charge one failed delivery attempt against a key's current marker.
async fn record_attempt(table: &CayenneTableProvider, id: i64) {
    let marker = marker_for(table, id)
        .await
        .expect("the key must still have a marker to charge an attempt against");
    table
        .record_delivery_attempts(std::slice::from_ref(&marker))
        .await
        .expect("recording a delivery attempt succeeds");
}

/// The first miss is the common one — a delete whose marker is published before
/// the delete is scan-visible, which resolves within milliseconds — so the
/// marker stays in the fresh queue for one immediate retry. Only a second miss
/// demotes it to the rotation, and neither miss may retire it.
async fn a_marker_reaches_the_deferred_queue_only_after_a_second_failed_attempt(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, schema) = create_table(&fixture, "wb_schedule_promotion", true).await?;
    seed_rows(&table, &schema, &[1]).await?;
    delete_where_id_in(&table, &[1]).await?;

    assert_eq!(queue_of(&table, 1).await, "fresh", "a new marker is fresh");
    assert_eq!(
        marker_for(&table, 1)
            .await
            .expect("marker exists")
            .delivery_attempts,
        0
    );

    record_attempt(&table, 1).await;
    assert_eq!(
        queue_of(&table, 1).await,
        "fresh",
        "one miss keeps the marker in the fresh queue for an immediate retry"
    );
    assert_eq!(
        marker_for(&table, 1)
            .await
            .expect("marker exists")
            .delivery_attempts,
        1
    );

    record_attempt(&table, 1).await;
    assert_eq!(
        queue_of(&table, 1).await,
        "deferred",
        "a second miss demotes the marker to the rotation"
    );
    assert_eq!(
        marker_for(&table, 1)
            .await
            .expect("marker exists")
            .delivery_attempts,
        2,
        "the marker is kept whatever the attempt count: only delivery retires one"
    );

    Ok(())
}

/// The rotation is ordered by when each marker was last tried, so waiting longer
/// monotonically improves a marker's position. Ordering by attempt *count*
/// instead would let a steady trickle of newly-deferred keys jump ahead of the
/// long-suffering ones indefinitely.
async fn the_deferred_queue_returns_the_least_recently_attempted_first(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, schema) = create_table(&fixture, "wb_schedule_rotation", true).await?;
    seed_rows(&table, &schema, &[1, 2, 3]).await?;
    delete_where_id_in(&table, &[1, 2, 3]).await?;

    // Two misses each puts all three in the rotation.
    for _ in 0..2 {
        for id in [1, 2, 3] {
            record_attempt(&table, id).await;
        }
    }

    // Now retry them in a deliberate order; the queue must hand them back in the
    // same order, oldest attempt first.
    for id in [3, 1, 2] {
        record_attempt(&table, id).await;
        tokio::time::sleep(TIMESTAMP_TICK).await;
    }

    let deferred = table.list_deferred_dirty_keys(1024).await?;
    let order: Vec<i64> = deferred
        .iter()
        .map(|marker| {
            [1_i64, 2, 3]
                .into_iter()
                .find(|id| marker_key_bytes(*id) == marker.pk_bytes)
                .expect("every deferred marker is one of the seeded keys")
        })
        .collect();
    assert_eq!(
        order,
        vec![3, 1, 2],
        "the rotation must return the least recently attempted marker first"
    );

    Ok(())
}

/// A newer commit on a stuck key carries data no pass has tried to deliver, so
/// it returns to the fresh queue rather than inheriting the failure history of
/// the commit it superseded — otherwise a brand-new write would wait out a whole
/// rotation before its first attempt.
async fn a_re_mark_returns_a_deferred_marker_to_the_fresh_queue(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, schema) = create_table(&fixture, "wb_schedule_remark", true).await?;
    seed_rows(&table, &schema, &[1]).await?;
    delete_where_id_in(&table, &[1]).await?;
    record_attempt(&table, 1).await;
    record_attempt(&table, 1).await;
    assert_eq!(queue_of(&table, 1).await, "deferred");

    // A newer commit on the same key: re-created, then deleted again.
    seed_rows(&table, &schema, &[1]).await?;
    delete_where_id_in(&table, &[1]).await?;

    assert_eq!(
        queue_of(&table, 1).await,
        "fresh",
        "a re-marked key is fresh work and must be attempted immediately"
    );
    assert_eq!(
        marker_for(&table, 1)
            .await
            .expect("marker exists")
            .delivery_attempts,
        0,
        "the newer commit must not inherit the superseded commit's attempt count"
    );

    Ok(())
}

/// The attempt bump carries the same compare-and-set bound as the clear: a pass
/// that was already in flight when a newer commit re-marked the key must not
/// charge that commit for its own failure, which would demote a write nothing
/// has tried to deliver yet.
async fn an_attempt_against_a_superseded_marker_is_not_charged(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, schema) = create_table(&fixture, "wb_schedule_superseded", true).await?;
    seed_rows(&table, &schema, &[1]).await?;
    delete_where_id_in(&table, &[1]).await?;
    let claimed = marker_for(&table, 1).await.expect("marker exists");

    // A newer commit lands while that claim is still "in flight".
    seed_rows(&table, &schema, &[1]).await?;
    delete_where_id_in(&table, &[1]).await?;
    let re_marked = marker_for(&table, 1).await.expect("marker exists");
    assert!(
        re_marked.sequence_number > claimed.sequence_number,
        "the re-mark must carry a newer sequence for this test to mean anything"
    );

    table
        .record_delivery_attempts(std::slice::from_ref(&claimed))
        .await?;

    assert_eq!(
        marker_for(&table, 1)
            .await
            .expect("marker exists")
            .delivery_attempts,
        0,
        "an attempt bounded by the superseded sequence must not charge the newer commit"
    );
    assert_eq!(queue_of(&table, 1).await, "fresh");

    Ok(())
}
