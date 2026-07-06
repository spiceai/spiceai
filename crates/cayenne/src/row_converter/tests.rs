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

//! Tests for the vendored row converter.
//!
//! The central guarantee is **byte-identity with `arrow-row`** for every supported type — this
//! is what lets Cayenne keep reading data it persisted with the old converter. That is proven by
//! [`assert_matches_arrow_row`], which encodes the same columns with both converters and compares
//! the raw bytes of every row. Round-trip decoding is verified by re-encoding the decoded arrays
//! and checking the bytes are unchanged (encoding is injective on logical values).

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    StringViewArray, Time32SecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, i256};
use arrow_schema::SortOptions;

use super::{OwnedRow, RowConverter, RowFormatVersion, SortField};

/// Encode `columns` with both the vendored converter and `arrow-row` using `options` for every
/// field, and assert the encoded bytes are identical row-for-row. Then verify round-trip decode.
fn assert_matches_arrow_row_opts(columns: &[ArrayRef], options: SortOptions) {
    let data_types: Vec<DataType> = columns.iter().map(|c| c.data_type().clone()).collect();

    let ours = RowConverter::new(
        data_types
            .iter()
            .map(|dt| SortField::new_with_options(dt.clone(), options))
            .collect(),
    )
    .expect("vendored converter builds");

    let theirs = arrow_row::RowConverter::new(
        data_types
            .iter()
            .map(|dt| arrow_row::SortField::new_with_options(dt.clone(), options))
            .collect(),
    )
    .expect("arrow-row converter builds");

    let our_rows = ours.convert_columns(columns).expect("vendored encode");
    let their_rows = theirs.convert_columns(columns).expect("arrow-row encode");

    let num_rows = match columns.first() {
        Some(c) => c.len(),
        None => 0,
    };
    assert_eq!(our_rows.num_rows(), num_rows);

    for i in 0..num_rows {
        assert_eq!(
            our_rows.row(i).as_ref(),
            their_rows.row(i).data(),
            "row {i} bytes differ from arrow-row for {data_types:?} with {options:?}"
        );
    }

    // Round-trip: decoding then re-encoding must reproduce the original bytes exactly.
    let decoded = ours.convert_rows(our_rows.iter()).expect("vendored decode");
    assert_eq!(decoded.len(), columns.len());
    let reencoded = ours.convert_columns(&decoded).expect("re-encode decoded");
    for i in 0..num_rows {
        assert_eq!(
            our_rows.row(i).as_ref(),
            reencoded.row(i).as_ref(),
            "round-trip changed row {i} for {data_types:?}"
        );
    }
}

/// [`assert_matches_arrow_row_opts`] with default (ascending, nulls-first) options — the only
/// options Cayenne uses.
fn assert_matches_arrow_row(columns: &[ArrayRef]) {
    assert_matches_arrow_row_opts(columns, SortOptions::default());
}

/// A long value that spans multiple encoding blocks (> 32 bytes).
const LONG: &str = "a value long enough to require multiple 32-byte blocks in the row encoding!!";

#[test]
fn integers_match_arrow_row() {
    assert_matches_arrow_row(&[Arc::new(Int8Array::from(vec![
        Some(0),
        None,
        Some(-1),
        Some(i8::MIN),
        Some(i8::MAX),
    ]))]);
    assert_matches_arrow_row(&[Arc::new(Int16Array::from(vec![
        Some(0),
        None,
        Some(-1),
        Some(i16::MIN),
        Some(i16::MAX),
    ]))]);
    assert_matches_arrow_row(&[Arc::new(Int32Array::from(vec![
        Some(0),
        None,
        Some(-1),
        Some(i32::MIN),
        Some(i32::MAX),
    ]))]);
    assert_matches_arrow_row(&[Arc::new(Int64Array::from(vec![
        Some(0),
        None,
        Some(-1),
        Some(i64::MIN),
        Some(i64::MAX),
    ]))]);
    assert_matches_arrow_row(&[Arc::new(UInt8Array::from(vec![
        Some(0),
        None,
        Some(1),
        Some(u8::MAX),
    ]))]);
    assert_matches_arrow_row(&[Arc::new(UInt16Array::from(vec![
        Some(0),
        None,
        Some(1),
        Some(u16::MAX),
    ]))]);
    assert_matches_arrow_row(&[Arc::new(UInt32Array::from(vec![
        Some(0),
        None,
        Some(1),
        Some(u32::MAX),
    ]))]);
    assert_matches_arrow_row(&[Arc::new(UInt64Array::from(vec![
        Some(0),
        None,
        Some(1),
        Some(u64::MAX),
    ]))]);
}

#[test]
fn floats_match_arrow_row() {
    assert_matches_arrow_row(&[Arc::new(Float32Array::from(vec![
        Some(0.0),
        Some(-0.0),
        None,
        Some(-1.5),
        Some(f32::INFINITY),
        Some(f32::NEG_INFINITY),
        Some(f32::NAN),
    ]))]);
    assert_matches_arrow_row(&[Arc::new(Float64Array::from(vec![
        Some(0.0),
        Some(-0.0),
        None,
        Some(-1.5),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::NAN),
    ]))]);
}

#[test]
fn boolean_matches_arrow_row() {
    assert_matches_arrow_row(&[Arc::new(BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
    ]))]);
}

#[test]
fn strings_and_binary_match_arrow_row() {
    let strings: Vec<Option<&str>> = vec![Some("a"), None, Some(""), Some("hello"), Some(LONG)];
    assert_matches_arrow_row(&[Arc::new(StringArray::from(strings.clone()))]);
    assert_matches_arrow_row(&[Arc::new(LargeStringArray::from(strings.clone()))]);
    assert_matches_arrow_row(&[Arc::new(StringViewArray::from(strings.clone()))]);

    let bytes: Vec<Option<&[u8]>> = vec![
        Some(b"a".as_ref()),
        None,
        Some(b"".as_ref()),
        Some(LONG.as_bytes()),
    ];
    assert_matches_arrow_row(&[Arc::new(BinaryArray::from(bytes.clone()))]);
    assert_matches_arrow_row(&[Arc::new(LargeBinaryArray::from(bytes.clone()))]);
    assert_matches_arrow_row(&[Arc::new(BinaryViewArray::from(bytes))]);
}

#[test]
fn temporal_matches_arrow_row() {
    assert_matches_arrow_row(&[Arc::new(Date32Array::from(vec![
        Some(0),
        None,
        Some(-1),
        Some(19_000),
    ]))]);
    assert_matches_arrow_row(&[Arc::new(Date64Array::from(vec![
        Some(0),
        None,
        Some(-1),
        Some(1_600_000_000_000),
    ]))]);
    assert_matches_arrow_row(&[Arc::new(Time32SecondArray::from(vec![
        Some(0),
        None,
        Some(86_399),
    ]))]);
    assert_matches_arrow_row(&[Arc::new(Time64NanosecondArray::from(vec![
        Some(0),
        None,
        Some(1),
    ]))]);

    let ts =
        TimestampMicrosecondArray::from(vec![Some(0), None, Some(-1), Some(1_600_000_000_000_000)]);
    assert_matches_arrow_row(&[Arc::new(ts.clone())]);
    // With a timezone (the shape Cayenne normalizes timestamps to).
    assert_matches_arrow_row(&[Arc::new(ts.with_timezone("UTC"))]);
}

#[test]
fn decimals_match_arrow_row() {
    let d128 = Decimal128Array::from(vec![Some(0), None, Some(-1), Some(123_456)])
        .with_precision_and_scale(10, 2)
        .expect("valid decimal128 precision/scale");
    assert_matches_arrow_row(&[Arc::new(d128)]);

    let d256 = Decimal256Array::from(vec![
        Some(i256::from_i128(0)),
        None,
        Some(i256::from_i128(-1)),
        Some(i256::from_i128(999)),
    ])
    .with_precision_and_scale(40, 3)
    .expect("valid decimal256 precision/scale");
    assert_matches_arrow_row(&[Arc::new(d256)]);
}

#[test]
fn composite_keys_match_arrow_row() {
    // The exact shape Cayenne exercises: (Int64, Utf8).
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(2)]));
    let names: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        Some(""),
        Some("z"),
        None,
    ]));
    assert_matches_arrow_row(&[Arc::clone(&ids), Arc::clone(&names)]);

    // (Utf8, Int32) and a three-column mix.
    let region: ArrayRef = Arc::new(StringArray::from(vec![Some("us"), Some("eu"), None]));
    let n: ArrayRef = Arc::new(Int32Array::from(vec![Some(-5), None, Some(7)]));
    let flag: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
    assert_matches_arrow_row(&[Arc::clone(&region), Arc::clone(&n)]);
    assert_matches_arrow_row(&[region, n, flag]);
}

#[test]
fn matches_arrow_row_for_all_sort_options() {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(-3)]));
    let names: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        None,
        Some(LONG),
        Some(""),
    ]));
    for descending in [false, true] {
        for nulls_first in [false, true] {
            assert_matches_arrow_row_opts(
                &[Arc::clone(&ids), Arc::clone(&names)],
                SortOptions {
                    descending,
                    nulls_first,
                },
            );
        }
    }
}

#[test]
fn owned_row_equality_and_hashing() {
    let converter = RowConverter::new(vec![
        SortField::new(DataType::Int64),
        SortField::new(DataType::Utf8),
    ])
    .expect("build converter");
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2)]));
    let names: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), Some("x"), Some("x")]));
    let rows = converter
        .convert_columns(&[ids, names])
        .expect("encode rows");

    let owned: Vec<OwnedRow> = rows.iter().map(|r| r.owned()).collect();
    assert_eq!(owned[0], owned[1], "identical keys must be equal");
    assert_ne!(owned[0], owned[2], "different keys must differ");

    let set: HashSet<OwnedRow> = owned.iter().cloned().collect();
    assert_eq!(set.len(), 2, "hash-set dedups identical keys");

    // Borrowed and owned views agree.
    assert_eq!(owned[0].row().as_ref(), rows.row(0).as_ref());
}

#[test]
fn unsupported_primary_key_types_error() {
    for dt in [
        DataType::FixedSizeBinary(4),
        DataType::Interval(arrow::datatypes::IntervalUnit::DayTime),
        DataType::Null,
        DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Int32,
            true,
        ))),
    ] {
        assert!(
            RowConverter::new(vec![SortField::new(dt.clone())]).is_err(),
            "expected {dt:?} to be rejected"
        );
    }
}

#[test]
fn unsupported_type_does_not_reach_arrow_row() {
    // Sanity: FixedSizeBinary is a valid arrow-row type but is not a Cayenne PK type, so we
    // deliberately reject it even though arrow-row would accept it.
    let fsb = FixedSizeBinaryArray::try_from_iter(vec![vec![1u8, 2, 3, 4]].into_iter())
        .expect("build fixed-size-binary array");
    assert!(
        arrow_row::RowConverter::new(vec![arrow_row::SortField::new(fsb.data_type().clone())])
            .is_ok(),
        "arrow-row accepts FixedSizeBinary"
    );
    assert!(
        RowConverter::new(vec![SortField::new(fsb.data_type().clone())]).is_err(),
        "cayenne rejects FixedSizeBinary as a primary key"
    );
}

#[test]
fn version_identifiers_round_trip() {
    assert_eq!(RowFormatVersion::CURRENT, RowFormatVersion::V1);
    assert_eq!(RowFormatVersion::V1.id(), 1);
    assert_eq!(RowFormatVersion::from_id(1), Some(RowFormatVersion::V1));
    assert_eq!(RowFormatVersion::from_id(0), None);
    assert_eq!(RowFormatVersion::from_id(2), None);

    let converter =
        RowConverter::new(vec![SortField::new(DataType::Int64)]).expect("build converter");
    assert_eq!(converter.version(), RowFormatVersion::V1);
}

#[test]
fn empty_input_is_handled() {
    let empty: ArrayRef = Arc::new(Int64Array::from(Vec::<Option<i64>>::new()));
    assert_matches_arrow_row(&[empty]);
}
