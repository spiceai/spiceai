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
    clippy::clone_on_ref_ptr,
    reason = "test code clones Arcs and asserts with expect rather than propagating"
)]

//! Durable write-back marker round trips for single-column primary keys.
//!
//! A committed write on a durable-write-back table marks its primary keys in
//! `cayenne_pending_write_back` as `RowConverter` `OwnedRow` encodings, and the
//! delivery worker's first step decodes them back into key arrays. Encode and
//! decode must agree for a table's key shape, or its writes never reach the
//! federated source (#13396).
//!
//! Each test asserts the full round trip — the decoded array equals the key that
//! was written — not merely that decoding succeeded.

mod common;

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, Date32Array, Decimal128Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProviderBuilder, CayenneTransaction, MetadataCatalog};
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

macro_rules! pk_shape_round_trip {
    ($name:ident, $key:expr) => {
        test_with_backends!($name);

        async fn $name(fixture: common::TestFixture) -> Result<(), Box<dyn std::error::Error>> {
            assert_marker_round_trip(&fixture, Arc::new($key)).await
        }
    };
}

pk_shape_round_trip!(
    a_bigint_primary_key_round_trips_through_a_write_back_marker,
    Int64Array::from(vec![42_i64])
);
pk_shape_round_trip!(
    an_int_primary_key_round_trips_through_a_write_back_marker,
    Int32Array::from(vec![42_i32])
);
pk_shape_round_trip!(
    a_smallint_primary_key_round_trips_through_a_write_back_marker,
    Int16Array::from(vec![42_i16])
);
pk_shape_round_trip!(
    a_text_primary_key_round_trips_through_a_write_back_marker,
    StringArray::from(vec!["order-42"])
);
pk_shape_round_trip!(
    a_large_text_primary_key_round_trips_through_a_write_back_marker,
    LargeStringArray::from(vec!["order-42"])
);
pk_shape_round_trip!(
    a_date_primary_key_round_trips_through_a_write_back_marker,
    Date32Array::from(vec![20_326_i32])
);
pk_shape_round_trip!(
    a_bytea_primary_key_round_trips_through_a_write_back_marker,
    BinaryArray::from_vec(vec![b"order-42".as_slice()])
);
pk_shape_round_trip!(
    a_numeric_primary_key_round_trips_through_a_write_back_marker,
    Decimal128Array::from(vec![4_200_i128])
        .with_precision_and_scale(18, 2)
        .expect("numeric key array")
);
pk_shape_round_trip!(
    a_timestamp_primary_key_round_trips_through_a_write_back_marker,
    TimestampMicrosecondArray::from(vec![1_724_524_800_000_000_i64])
);

/// A durable-write-back table keyed by `key_type`, and its schema.
async fn write_back_table(
    fixture: &common::TestFixture,
    key_type: &DataType,
) -> Result<(cayenne::CayenneTableProvider, Arc<Schema>), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", key_type.clone(), false),
        Field::new("value", DataType::Int64, false),
    ]));

    let ctx = SessionContext::new();
    let catalog = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let provider = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .with_durable_write_back(true)
        .create(CreateTableOptions {
            table_name: "write_back_marker".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        })
        .await?;

    Ok((provider, schema))
}

/// Commit `batch` through a transaction — markers are written only in the
/// commit-publish transaction, so a bare `insert_into` marks nothing.
async fn commit(
    provider: &cayenne::CayenneTableProvider,
    batch: RecordBatch,
) -> Result<(), Box<dyn std::error::Error>> {
    let txn = CayenneTransaction::new();
    let token = provider.transaction_write_token().await;
    txn.register(
        provider.table_id().to_string(),
        token,
        provider.clone_for_write_operations(),
    );
    let staged = provider
        .begin_staged_upsert_occ(token, common::single_batch_stream(batch), 1)
        .await?;
    txn.set_staged(provider.table_id(), staged);
    txn.commit().await?;
    Ok(())
}

/// Commit one upsert of `key` on a durable-write-back table keyed by that shape,
/// then assert the commit left exactly one marker and that decoding it returns
/// the key that was written.
/// The claim cursor for a page's last marker. Markers are `(pk_bytes, sequence)`
/// and the cursor is `(sequence, pk_bytes)`, so the flip is stated once.
fn cursor_after(page: &[(Vec<u8>, i64)]) -> Option<(i64, Vec<u8>)> {
    page.last()
        .map(|(pk_bytes, sequence)| (*sequence, pk_bytes.clone()))
}

/// Every marker the cursor reaches, paging `page_size` at a time from the oldest.
async fn page_all_markers(
    provider: &cayenne::CayenneTableProvider,
    page_size: usize,
) -> Result<Vec<(Vec<u8>, i64)>, Box<dyn std::error::Error>> {
    let mut paged: Vec<(Vec<u8>, i64)> = Vec::new();
    let mut after = None;
    loop {
        let page = provider.list_dirty_keys(page_size, after.as_ref()).await?;
        if page.is_empty() {
            break;
        }
        after = cursor_after(&page);
        paged.extend(page);
    }
    Ok(paged)
}

async fn assert_marker_round_trip(
    fixture: &common::TestFixture,
    key: ArrayRef,
) -> Result<(), Box<dyn std::error::Error>> {
    let (provider, schema) = write_back_table(fixture, key.data_type()).await?;

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::clone(&key), Arc::new(Int64Array::from(vec![7_i64]))],
    )?;
    commit(&provider, batch).await?;

    let markers = provider.list_dirty_keys(16, None).await?;
    assert_eq!(
        markers.len(),
        1,
        "one committed upsert on a {:?}-keyed durable-write-back table must leave exactly one marker",
        key.data_type()
    );

    let decoded = provider.decode_pk_keys(&[markers[0].0.clone()])?;
    assert_eq!(
        decoded.len(),
        1,
        "a single-column primary key decodes to one array"
    );
    assert_eq!(
        &decoded[0],
        &key,
        "the decoded {:?} marker key must equal the key that was written",
        key.data_type()
    );

    Ok(())
}

test_with_backends!(a_claim_cursor_reaches_every_marker_exactly_once);

/// The delivery worker pages the marker set, resuming past markers it could not
/// deliver so one undeliverable key cannot pin the claim. That cursor has to be
/// exact: every key one commit dirties carries that commit's sequence, so without
/// the key breaking the tie two pages could return the same marker twice while
/// never returning another at all — leaving an acknowledged write undelivered.
async fn a_claim_cursor_reaches_every_marker_exactly_once(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (provider, schema) = write_back_table(&fixture, &DataType::Int32).await?;

    // One transaction, five keys — so all five markers share a sequence and only
    // the key can order them.
    commit(
        &provider,
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![7_i64; 5])),
            ],
        )?,
    )
    .await?;

    let all = provider.list_dirty_keys(16, None).await?;
    assert_eq!(all.len(), 5, "one commit of five keys leaves five markers");

    let paged = page_all_markers(&provider, 2).await?;
    assert_eq!(
        paged, all,
        "paging by cursor reaches every marker exactly once, in the same order"
    );

    Ok(())
}

test_with_backends!(a_claim_cursor_reaches_newer_markers_past_an_undeliverable_page);

/// The reason the cursor exists: a marker the worker cannot deliver stays in the
/// table, and the claim is ordered by commit sequence, so re-claiming the oldest
/// page every pass would never reach a newer write. Resuming past that page must
/// return the newer markers — and starting again at the oldest must still return
/// the ones left behind, so nothing is stranded.
async fn a_claim_cursor_reaches_newer_markers_past_an_undeliverable_page(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (provider, schema) = write_back_table(&fixture, &DataType::Int32).await?;

    let commit_keys = |keys: Vec<i32>| {
        let rows = keys.len();
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(Int64Array::from(vec![7_i64; rows])),
            ],
        )
    };

    // An older commit whose keys stand in for a page the worker cannot deliver,
    // then a newer one that must still be reached.
    commit(&provider, commit_keys(vec![1, 2])?).await?;
    commit(&provider, commit_keys(vec![8, 9])?).await?;

    let stuck = provider.list_dirty_keys(2, None).await?;
    assert_eq!(stuck.len(), 2, "the oldest page is the older commit's keys");

    let after = cursor_after(&stuck);
    let newer = provider.list_dirty_keys(2, after.as_ref()).await?;
    assert_eq!(
        newer.len(),
        2,
        "resuming past that page reaches the newer commit's keys"
    );
    assert!(
        newer.iter().all(|marker| !stuck.contains(marker)),
        "and returns none of the page it stepped over"
    );

    // Starting again at the oldest still returns everything, so the keys the
    // worker stepped over are not stranded.
    let restarted = provider.list_dirty_keys(16, None).await?;
    assert_eq!(
        restarted.len(),
        4,
        "a claim from the oldest marker sees every undelivered marker again"
    );

    Ok(())
}

test_with_backends!(a_claim_cursor_pages_a_marker_set_larger_than_one_claim);

/// A claim page is bounded, so the marker set routinely outgrows it. Paging has
/// to cover a set larger than one claim without repeating or skipping a marker.
async fn a_claim_cursor_pages_a_marker_set_larger_than_one_claim(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (provider, schema) = write_back_table(&fixture, &DataType::Int32).await?;

    // More keys than the worker's claim batch, in one commit so they share a
    // sequence and the key alone orders them.
    let rows: usize = 1500;
    let keys: Vec<i32> = (0..i32::try_from(rows)?).collect();
    commit(
        &provider,
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(Int64Array::from(vec![7_i64; rows])),
            ],
        )?,
    )
    .await?;

    let paged = page_all_markers(&provider, 1024).await?;

    assert_eq!(
        paged.len(),
        rows,
        "paging covers a marker set larger than one claim"
    );
    let mut unique = paged.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), paged.len(), "and returns no marker twice");

    Ok(())
}
