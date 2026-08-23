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

//! `CayenneTableProvider::decode_pk_keys` must decode the write-back marker keys
//! that the write path encodes, for every primary-key shape.
//!
//! A durable-write-back commit marks its primary keys as `RowConverter` `OwnedRow`
//! encodings, produced by an ad-hoc converter built over the key columns. The
//! delivery worker decodes those bytes back into key arrays before it can read the
//! keys' current values or address them at the federated source, so a shape whose
//! keys encode but do not decode never delivers a single write.
//!
//! A single `Int64` primary key is exactly that shape: it runs the converter-free
//! `Int64Pk` deletion strategy, so the provider stores no `pk_row_converter`.

mod common;

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use cayenne::metadata::CreateTableOptions;
use cayenne::row_converter::{RowConverter, SortField};
use cayenne::CayenneTableProvider;

use datafusion::prelude::SessionContext;

use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

test_with_backends!(int64_primary_key_marker_keys_round_trip);
test_with_backends!(composite_primary_key_marker_keys_round_trip);

/// Encode primary-key columns the way a committed write encodes its dirty-key
/// markers: one `RowConverter` `OwnedRow` per row, over the key columns in key
/// order.
fn encode_marker_keys(key_fields: &[Field], key_columns: &[ArrayRef]) -> Vec<Vec<u8>> {
    let sort_fields = key_fields
        .iter()
        .map(|field| SortField::new(field.data_type().clone()))
        .collect();
    let converter = RowConverter::new(sort_fields).expect("primary-key RowConverter");
    let rows = converter
        .convert_columns(key_columns)
        .expect("primary-key columns encode");
    rows.iter().map(|row| row.as_ref().to_vec()).collect()
}

async fn create_table(
    fixture: &common::TestFixture,
    table_name: &str,
    schema: &Arc<Schema>,
    primary_key: Vec<String>,
) -> Result<CayenneTableProvider, Box<dyn std::error::Error>> {
    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(schema),
        primary_key: primary_key.clone(),
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(primary_key))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog = Arc::clone(&fixture.catalog);
    let ctx = SessionContext::new();
    Ok(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?)
}

/// A `BIGINT`-keyed table stores no `pk_row_converter`, so decoding its markers
/// has to rebuild the converter from the key columns. Covers the boundary values
/// whose encoding is order-preserving rather than a plain big-endian copy.
async fn int64_primary_key_marker_keys_round_trip(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let table = create_table(&fixture, "int64_pk", &schema, vec!["id".to_string()]).await?;

    let ids: Vec<i64> = vec![i64::MIN, -1, 0, 1, 42, i64::MAX];
    let key_column: ArrayRef = Arc::new(Int64Array::from(ids.clone()));
    let key_field = Field::new("id", DataType::Int64, false);
    let marker_keys = encode_marker_keys(std::slice::from_ref(&key_field), &[key_column]);

    let decoded = table.decode_pk_keys(&marker_keys)?;

    assert_eq!(
        decoded.len(),
        1,
        "a single-column primary key decodes to exactly one array"
    );
    let decoded_ids = decoded[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("decoded key column is Int64");
    assert_eq!(
        decoded_ids.values(),
        ids.as_slice(),
        "every marker key must decode back to the value that was marked"
    );

    Ok(())
}

/// The stored-converter path (any shape that is not a single `Int64`) must keep
/// decoding as it did — the fallback must not displace it.
async fn composite_primary_key_marker_keys_round_trip(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let table = create_table(
        &fixture,
        "composite_pk",
        &schema,
        vec!["tenant".to_string(), "id".to_string()],
    )
    .await?;

    let tenants = vec!["acme", "", "zeta"];
    let ids: Vec<i64> = vec![1, 2, i64::MAX];
    let key_fields = vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ];
    let key_columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(tenants.clone())),
        Arc::new(Int64Array::from(ids.clone())),
    ];
    let marker_keys = encode_marker_keys(&key_fields, &key_columns);

    let decoded = table.decode_pk_keys(&marker_keys)?;

    assert_eq!(
        decoded.len(),
        2,
        "a two-column primary key decodes to two arrays, in key order"
    );
    let decoded_tenants = decoded[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("decoded first key column is Utf8");
    let decoded_ids = decoded[1]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("decoded second key column is Int64");
    assert_eq!(
        decoded_tenants.iter().flatten().collect::<Vec<_>>(),
        tenants,
        "the first key column must decode back to what was marked"
    );
    assert_eq!(
        decoded_ids.values(),
        ids.as_slice(),
        "the second key column must decode back to what was marked"
    );

    Ok(())
}
