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

#![expect(
    clippy::expect_used,
    reason = "test code asserts with descriptive expect messages"
)]

//! A committed `DELETE` on a durable-write-back table must record its keys as
//! delete markers, and only a user `DELETE` may do so.
//!
//! Delivery removes a row from the federated source when — and only when — a
//! marker says a `DELETE` removed it. That makes the marking itself the safety
//! boundary: a delete that fails to mark leaves the source holding a row the
//! user deleted, while a non-user delete that marks (a retention prune, which
//! only evicts from the accelerator) destroys a source row nobody deleted.

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
            "id".to_string()
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
    let converter = RowConverter::new(vec![SortField::new(DataType::Int64)])
        .expect("primary-key RowConverter");
    let column: ArrayRef = Arc::new(Int64Array::from(vec![id]));
    let rows = converter
        .convert_columns(std::slice::from_ref(&column))
        .expect("the key column encodes");
    rows.row(0).as_ref().to_vec()
}

/// The marked keys as `(id, op)` pairs, sorted by id. Any marker whose key is
/// not one of `expected_ids` fails the test rather than being ignored, so a
/// delete that marked the wrong key cannot pass unnoticed.
async fn marked_keys(
    table: &CayenneTableProvider,
    expected_ids: &[i64],
) -> Vec<(i64, WriteBackOp)> {
    let markers = table
        .list_dirty_keys(1024)
        .await
        .expect("listing write-back markers succeeds");

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
