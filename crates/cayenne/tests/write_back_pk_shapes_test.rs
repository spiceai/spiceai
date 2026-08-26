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

/// Commit one upsert of `key` on a durable-write-back table keyed by that shape,
/// then assert the commit left exactly one marker and that decoding it returns
/// the key that was written.
async fn assert_marker_round_trip(
    fixture: &common::TestFixture,
    key: ArrayRef,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", key.data_type().clone(), false),
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

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::clone(&key), Arc::new(Int64Array::from(vec![7_i64]))],
    )?;

    // Markers are written only in the commit-publish transaction, so drive the
    // write through a transaction rather than a bare `insert_into`.
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

    let markers = provider.list_dirty_keys(16).await?;
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
