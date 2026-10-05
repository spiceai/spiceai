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

use std::cmp::Ordering;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int8Array, Int32Array, Int64Array,
    LargeStringArray, StringArray, StringViewArray, UInt16Array,
};
use arrow_schema::DataType;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use crate::test_support::encode_rows;
use crate::{Error, KeyEncoder, KeyField};

// ---- key encoding --------------------------------------------------------

fn composite_columns(rng: &mut StdRng, rows: usize) -> Vec<ArrayRef> {
    let ints: Int64Array = (0..rows)
        .map(|_| {
            rng.random_bool(0.9)
                .then(|| rng.random_range(-3_i64..=3) * (i64::MAX / 3))
        })
        .collect();
    let strings: StringArray = (0..rows)
        .map(|_| {
            rng.random_bool(0.9).then(|| {
                let len = rng.random_range(0..4);
                (0..len)
                    .map(|_| ['\0', '\u{1}', 'a', 'b', 'é'][rng.random_range(0..5)])
                    .collect::<String>()
            })
        })
        .collect();
    let smalls: UInt16Array = (0..rows)
        .map(|_| [0, 1, 255, 256, 40_000, u16::MAX][rng.random_range(0..6)])
        .map(Some)
        .collect();
    let bools: BooleanArray = (0..rows).map(|_| Some(rng.random_bool(0.5))).collect();
    vec![
        Arc::new(ints),
        Arc::new(strings),
        Arc::new(smalls),
        Arc::new(bools),
    ]
}

fn composite_encoder() -> KeyEncoder {
    KeyEncoder::new(vec![
        KeyField::new(DataType::Int64, true),
        KeyField::new(DataType::Utf8, true),
        KeyField::new(DataType::UInt16, false),
        KeyField::new(DataType::Boolean, false),
    ])
    .expect("supported key types")
}

/// Compare two rows of [`composite_columns`] as SQL orders the tuple, NULLs
/// first — the order the encoding must reproduce.
fn compare_rows(columns: &[ArrayRef], a: usize, b: usize) -> Ordering {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    let ints = columns[0].as_primitive::<arrow_array::types::Int64Type>();
    let strings = columns[1].as_string::<i32>();
    let smalls = columns[2].as_primitive::<arrow_array::types::UInt16Type>();
    let bools = columns[3].as_boolean();
    let int = |i: usize| ints.is_valid(i).then(|| ints.value(i));
    let string = |i: usize| strings.is_valid(i).then(|| strings.value(i).as_bytes());
    int(a)
        .cmp(&int(b))
        .then_with(|| string(a).cmp(&string(b)))
        .then_with(|| smalls.value(a).cmp(&smalls.value(b)))
        .then_with(|| bools.value(a).cmp(&bools.value(b)))
}

#[test]
fn encoding_preserves_tuple_order_and_is_prefix_free() {
    let mut rng = StdRng::seed_from_u64(13);
    let columns = composite_columns(&mut rng, 3_000);
    let encoder = composite_encoder();
    let bound = encoder.bind(&columns).expect("columns match the key");
    let (bytes, offsets) = bound.encode_all();
    let key = |i: usize| &bytes[offsets[i]..offsets[i + 1]];
    for _ in 0..50_000 {
        let (a, b) = (rng.random_range(0..3_000), rng.random_range(0..3_000));
        assert_eq!(
            key(a).cmp(key(b)),
            compare_rows(&columns, a, b),
            "rows {a} and {b}"
        );
        if key(a) != key(b) {
            assert!(
                !key(a).starts_with(key(b)) && !key(b).starts_with(key(a)),
                "rows {a} and {b} encode to a prefix of one another"
            );
        }
    }
}

#[test]
fn string_array_types_encode_identically() {
    let values = [Some("a\0b"), None, Some(""), Some("\u{1}"), Some("zz")];
    let string_columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(values.to_vec())),
        Arc::new(LargeStringArray::from(values.to_vec())),
        Arc::new(StringViewArray::from(values.to_vec())),
    ];
    let mut encodings = Vec::new();
    for column in string_columns {
        let encoder = KeyEncoder::new(vec![KeyField::new(column.data_type().clone(), true)])
            .expect("supported key type");
        let columns = [column];
        let bound = encoder.bind(&columns).expect("column matches the key");
        encodings.push(bound.encode_all());
    }
    assert_eq!(encodings[0], encodings[1]);
    assert_eq!(encodings[0], encodings[2]);
    // NULL, then "", then "\u{1}", then "a\0b", then "zz".
    let (bytes, offsets) = &encodings[0];
    let key = |i: usize| &bytes[offsets[i]..offsets[i + 1]];
    let order = [1, 2, 3, 0, 4];
    for pair in order.windows(2) {
        assert!(key(pair[0]) < key(pair[1]), "{pair:?}");
    }
}

#[test]
fn fixed_width_and_sliced_columns_encode_by_value() {
    let ints = Int32Array::from(vec![i32::MIN, -1, 0, 1, i32::MAX]);
    let small = Int8Array::from(vec![-128_i8, -1, 0, 1, 127]);
    let unsigned = UInt16Array::from(vec![0_u16, 1, 2, 3, u16::MAX]);
    let fixed = FixedSizeBinaryArray::try_from_iter(
        [[0_u8, 0], [0, 1], [1, 0], [1, 1], [255, 255]].into_iter(),
    )
    .expect("fixed size binary");
    let binary = BinaryArray::from(vec![&b""[..], b"\0", b"\0\0", b"\x01", b"\xff"]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(ints),
        Arc::new(small),
        Arc::new(unsigned),
        Arc::new(fixed),
        Arc::new(binary),
    ];
    let encoder = KeyEncoder::new(
        columns
            .iter()
            .map(|c| KeyField::new(c.data_type().clone(), false))
            .collect(),
    )
    .expect("supported key types");
    let bound = encoder.bind(&columns).expect("columns match the key");
    let (bytes, offsets) = bound.encode_all();
    for row in 1..5 {
        assert!(
            bytes[offsets[row - 1]..offsets[row]] < bytes[offsets[row]..offsets[row + 1]],
            "row {row} does not sort after row {}",
            row - 1
        );
    }

    // A sliced array encodes its own rows, not the parent's from offset 0.
    let sliced: Vec<ArrayRef> = columns.iter().map(|c| c.slice(2, 3)).collect();
    let bound_slice = encoder.bind(&sliced).expect("sliced columns match the key");
    let (slice_bytes, _) = bound_slice.encode_all();
    assert_eq!(slice_bytes, bytes[offsets[2]..offsets[5]]);
}

#[test]
fn bind_rejects_mismatched_columns() {
    let encoder = KeyEncoder::new(vec![KeyField::new(DataType::Int64, false)]).expect("int64 key");
    let wrong_type: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1]))];
    let error = encoder.bind(&wrong_type).expect_err("wrong type");
    assert_eq!(
        error,
        Error::ColumnType {
            index: 0,
            found: DataType::Int32,
            declared: DataType::Int64,
        }
    );
    assert_eq!(
        error.to_string(),
        "Failed to encode an index key: key column 0 is Int32 but the key declares Int64"
    );
    let with_null: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(1), None]))];
    let error = encoder.bind(&with_null).expect_err("NULL");
    assert_eq!(error, Error::UnexpectedNull { index: 0 });
    assert_eq!(
        error.to_string(),
        "Failed to encode an index key: key column 0 is declared non-nullable but holds NULL"
    );
    let error = encoder.bind(&[]).expect_err("no columns");
    assert_eq!(
        error,
        Error::ColumnCount {
            expected: 1,
            received: 0,
        }
    );
    assert_eq!(
        error.to_string(),
        "Failed to encode an index key: expected 1 key columns but received 0"
    );
    let pair = KeyEncoder::new(vec![
        KeyField::new(DataType::Int64, false),
        KeyField::new(DataType::Int64, false),
    ])
    .expect("pair key");
    let uneven: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(Int64Array::from(vec![1])),
    ];
    let error = pair.bind(&uneven).expect_err("uneven");
    assert_eq!(
        error,
        Error::ColumnLength {
            index: 1,
            rows: 1,
            expected: 2,
        }
    );
    assert_eq!(
        error.to_string(),
        "Failed to encode an index key: key column 1 has 1 rows but key column 0 has 2"
    );
    assert!(matches!(
        KeyEncoder::new(vec![KeyField::new(
            DataType::List(Arc::new(arrow_schema::Field::new(
                "x",
                DataType::Int8,
                true
            ))),
            false
        )]),
        Err(Error::UnsupportedType { .. })
    ));
}

/// A key's bytes are the byte-by-byte escape (`escape_proof::escape_into`) of
/// its value, behind a nullable column's marker, whether or not the value has
/// a byte to escape: the encoder's whole-value fast path writes the same bytes.
#[test]
fn encoded_values_match_the_byte_by_byte_escape() {
    let mut rng = StdRng::seed_from_u64(0x5afe);
    let values: Vec<Vec<u8>> = (0..5_000)
        .map(|_| {
            let len = rng.random_range(0..12);
            (0..len)
                .map(|_| match rng.random_range(0..4) {
                    0 => 0u8,
                    1 => 1u8,
                    _ => rng.random_range(0..=255_u8),
                })
                .collect()
        })
        .collect();
    let nulls: Vec<bool> = (0..values.len()).map(|_| rng.random_bool(0.1)).collect();
    // Non-nullable binary column: the key is the escape alone.
    let binary: ArrayRef = Arc::new(BinaryArray::from_iter_values(values.iter()));
    let encoder =
        KeyEncoder::new(vec![KeyField::new(DataType::Binary, false)]).expect("binary key");
    let bound = encoder.bind(std::slice::from_ref(&binary)).expect("bind");
    for (row, value) in values.iter().enumerate() {
        let (mut got, mut want) = (Vec::new(), Vec::new());
        bound.encode_row(row, &mut got);
        crate::escape_proof::escape_into(value, &mut want);
        assert_eq!(got, want, "row {row}: {value:?}");
    }
    // Nullable string column: `00` for NULL, `01` then the escape.
    let strings: Vec<Option<String>> = values
        .iter()
        .zip(&nulls)
        .map(|(v, &null)| (!null).then(|| v.iter().map(|&b| char::from(b % 128)).collect()))
        .collect();
    let utf8: ArrayRef = Arc::new(StringArray::from(strings.clone()));
    let encoder = KeyEncoder::new(vec![KeyField::new(DataType::Utf8, true)]).expect("utf8 key");
    let bound = encoder.bind(std::slice::from_ref(&utf8)).expect("bind");
    for (row, value) in strings.iter().enumerate() {
        let (mut got, mut want) = (Vec::new(), Vec::new());
        bound.encode_row(row, &mut got);
        match value {
            None => want.push(0),
            Some(s) => {
                want.push(1);
                crate::escape_proof::escape_into(s.as_bytes(), &mut want);
            }
        }
        assert_eq!(got, want, "row {row}: {value:?}");
    }
}

/// The encoding of fixed keys, byte for byte. Persisted runs and a future
/// range index depend on these exact bytes, so any change to them fails here.
#[test]
fn fixed_keys_encode_to_pinned_bytes() {
    use arrow_array::{Decimal256Array, IntervalDayTimeArray};
    use arrow_buffer::{IntervalDayTime, i256};
    let encode = |fields: Vec<(DataType, bool)>, columns: Vec<ArrayRef>| -> Vec<Vec<u8>> {
        let encoder = KeyEncoder::new(
            fields
                .into_iter()
                .map(|(data_type, nullable)| KeyField::new(data_type, nullable))
                .collect(),
        )
        .expect("supported key types");
        let bound = encoder.bind(&columns).expect("columns match the key");
        (0..bound.num_rows())
            .map(|row| {
                let mut key = Vec::new();
                bound.encode_row(row, &mut key);
                key
            })
            .collect()
    };
    let int64 = |nullable| vec![(DataType::Int64, nullable)];
    assert_eq!(
        encode(
            int64(false),
            vec![Arc::new(Int64Array::from(vec![1, i64::MIN]))]
        ),
        [
            vec![0x80, 0, 0, 0, 0, 0, 0, 1],
            vec![0, 0, 0, 0, 0, 0, 0, 0]
        ]
    );
    assert_eq!(
        encode(
            int64(true),
            vec![Arc::new(Int64Array::from(vec![None, Some(-1)]))]
        ),
        [
            vec![0x00],
            vec![0x01, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
        ]
    );
    assert_eq!(
        encode(
            vec![(DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec![
                Some("a\0b\u{1}"),
                Some(""),
                None
            ]))]
        ),
        [
            vec![0x01, b'a', 0x01, 0x01, b'b', 0x01, 0x02, 0x00],
            vec![0x01, 0x00],
            vec![0x00]
        ]
    );
    assert_eq!(
        encode(
            vec![(DataType::Utf8View, false)],
            vec![Arc::new(StringViewArray::from(vec!["abcdefghijklm\u{1}"]))]
        ),
        [[&b"abcdefghijklm"[..], &[0x01, 0x02, 0x00]].concat()]
    );
    assert_eq!(
        encode(
            vec![(DataType::Binary, false)],
            vec![Arc::new(BinaryArray::from(vec![
                &b"\0"[..],
                b"\x02\xff",
                b""
            ]))]
        ),
        [vec![0x01, 0x01, 0x00], vec![0x02, 0xff, 0x00], vec![0x00]]
    );
    // A compound key: nullable `Int32`, `Utf8`, `Boolean`, `UInt16`.
    assert_eq!(
        encode(
            vec![
                (DataType::Int32, true),
                (DataType::Utf8, false),
                (DataType::Boolean, false),
                (DataType::UInt16, false),
            ],
            vec![
                Arc::new(Int32Array::from(vec![5])),
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(UInt16Array::from(vec![258])),
            ]
        ),
        [vec![0x01, 0x80, 0, 0, 5, b'x', 0x00, 0x01, 0x01, 0x02]]
    );
    assert_eq!(
        encode(
            vec![(DataType::FixedSizeBinary(2), true)],
            vec![Arc::new(
                FixedSizeBinaryArray::try_from_iter([[0_u8, 1]].into_iter()).expect("fixed")
            )]
        ),
        [vec![0x01, 0x00, 0x01]]
    );
    let mut one = vec![0x80];
    one.extend([0; 30]);
    one.push(1);
    assert_eq!(
        encode(
            vec![(DataType::Decimal256(76, 0), false)],
            vec![Arc::new(
                Decimal256Array::from(vec![i256::from(1)])
                    .with_precision_and_scale(76, 0)
                    .expect("decimal")
            )]
        ),
        [one]
    );
    assert_eq!(
        encode(
            vec![(
                DataType::Interval(arrow_schema::IntervalUnit::DayTime),
                false
            )],
            vec![Arc::new(IntervalDayTimeArray::from(vec![
                IntervalDayTime::new(1, -1)
            ]))]
        ),
        [vec![0x80, 0, 0, 1, 0x7f, 0xff, 0xff, 0xff]]
    );
}

/// `Decimal256` and intervals encode in value order (field by field for
/// intervals, as Arrow compares them), equal values identically, nullable
/// columns included.
#[test]
fn wide_decimals_and_intervals_encode_in_order() {
    use arrow_array::{Decimal256Array, IntervalDayTimeArray, IntervalMonthDayNanoArray};
    use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, i256};
    let mut rng = StdRng::seed_from_u64(77);
    let n = 2_000;
    let small = |rng: &mut StdRng| rng.random_range(-3_i32..3);
    let decimals: Vec<Option<i256>> = (0..n)
        .map(|i| {
            (i % 11 != 0).then(|| {
                i256::from_parts(
                    rng.random::<u128>() >> rng.random_range(0..128),
                    i128::from(small(&mut rng)),
                )
            })
        })
        .collect();
    let day_time: Vec<IntervalDayTime> = (0..n)
        .map(|_| IntervalDayTime::new(small(&mut rng), small(&mut rng)))
        .collect();
    let month_day_nano: Vec<IntervalMonthDayNano> = (0..n)
        .map(|_| {
            IntervalMonthDayNano::new(small(&mut rng), small(&mut rng), i64::from(small(&mut rng)))
        })
        .collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(
            Decimal256Array::from(decimals.clone())
                .with_precision_and_scale(76, 4)
                .expect("decimal"),
        ),
        Arc::new(IntervalDayTimeArray::from(day_time.clone())),
        Arc::new(IntervalMonthDayNanoArray::from(month_day_nano.clone())),
    ];
    let fields: Vec<KeyField> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| KeyField::new(c.data_type().clone(), i == 0))
        .collect();
    let encoder = KeyEncoder::new(fields).expect("supported types");
    let bound = encoder.bind(&columns).expect("bind");
    let keys: Vec<Vec<u8>> = (0..n)
        .map(|row| {
            let mut key = Vec::new();
            bound.encode_row(row, &mut key);
            key
        })
        .collect();
    let value = |row: usize| {
        (
            decimals[row],
            (day_time[row].days, day_time[row].milliseconds),
            (
                month_day_nano[row].months,
                month_day_nano[row].days,
                month_day_nano[row].nanoseconds,
            ),
        )
    };
    for _ in 0..20_000 {
        let (a, b) = (rng.random_range(0..n), rng.random_range(0..n));
        // NULL sorts first, as `Option`'s `None` does.
        assert_eq!(
            keys[a].cmp(&keys[b]),
            value(a).cmp(&value(b)),
            "rows {a} and {b}"
        );
    }
    for (a, key) in keys.iter().enumerate() {
        for other in &keys[a + 1..] {
            assert!(!other.starts_with(key) || other == key, "prefix-free");
        }
    }
}

/// A key of fixed-width fields that fit 8 bytes is its own word, so distinct
/// keys have distinct words, in key order, whether or not a field is nullable;
/// any other key is hashed.
#[test]
fn key_words_are_exact_for_keys_that_fit_eight_bytes() {
    use arrow_array::{Int32Array, Int64Array, StringArray};
    let words = |encoder: &KeyEncoder, columns: &[ArrayRef]| -> Vec<u64> {
        encode_rows(encoder, columns)
            .iter()
            .map(|key| encoder.key_word(key))
            .collect()
    };
    let values: Vec<i64> = vec![i64::MIN, -5, -1, 0, 1, 7, i64::MAX];
    for nullable in [false, true] {
        let encoder = KeyEncoder::new(vec![KeyField::new(DataType::Int64, nullable)]).expect("i64");
        assert!(encoder.exact_words());
        let got = words(&encoder, &[Arc::new(Int64Array::from(values.clone()))]);
        assert!(
            got.windows(2).all(|pair| pair[0] < pair[1]),
            "exact words keep key order: {got:?}"
        );
    }
    let pair = KeyEncoder::new(vec![
        KeyField::new(DataType::Int32, false),
        KeyField::new(DataType::Int32, true),
    ])
    .expect("(i32, i32)");
    assert!(pair.exact_words(), "two 4-byte fields fit a word");
    let got = words(
        &pair,
        &[
            Arc::new(Int32Array::from(vec![0, 0, 1, 1])),
            Arc::new(Int32Array::from(vec![0, 1, 0, 1])),
        ],
    );
    assert!(got.windows(2).all(|pair| pair[0] < pair[1]), "{got:?}");
    for fields in [
        vec![KeyField::new(DataType::Utf8, false)],
        vec![
            KeyField::new(DataType::Int64, false),
            KeyField::new(DataType::Int32, false),
        ],
    ] {
        assert!(!KeyEncoder::new(fields).expect("key").exact_words());
    }
    let strings = KeyEncoder::new(vec![KeyField::new(DataType::Utf8, false)]).expect("utf8");
    let got = words(
        &strings,
        &[Arc::new(StringArray::from(vec!["a", "b", "a"]))],
    );
    assert_eq!(got[0], got[2], "equal keys share a word");
    assert_ne!(got[0], got[1]);
}

/// Each row of `column`, encoded alone under a key of its type.
fn encode_each(column: ArrayRef) -> Vec<Vec<u8>> {
    let encoder =
        KeyEncoder::new(vec![KeyField::new(column.data_type().clone(), false)]).expect("key");
    encode_rows(&encoder, &[column])
}

/// `values` as a column of each float width, by their `f64` bits narrowed with
/// `as` (so a NaN payload and the sign of zero carry over).
fn float_columns(values: &[f64]) -> Vec<ArrayRef> {
    use arrow_array::types::Float16Type;
    use arrow_array::{ArrowPrimitiveType, Float32Array, Float64Array, PrimitiveArray};
    type F16 = <Float16Type as ArrowPrimitiveType>::Native;
    #[expect(clippy::cast_possible_truncation, reason = "narrowed on purpose")]
    let narrow = |v: f64| v as f32;
    vec![
        Arc::new(PrimitiveArray::<Float16Type>::from_iter_values(
            values.iter().map(|&v| F16::from_f64(v)),
        )),
        Arc::new(Float32Array::from_iter_values(
            values.iter().map(|&v| narrow(v)),
        )),
        Arc::new(Float64Array::from_iter_values(values.iter().copied())),
    ]
}

/// SQL can hold `-0.0` equal to `0.0`, and one NaN equal to another, so each
/// such pair encodes identically at every width: a lookup for one must find
/// rows holding the other. Infinities stay apart from NaN and from each other.
#[test]
fn floats_sql_holds_equal_encode_identically() {
    let negative_nan = -f64::NAN;
    let payload_nan = f64::from_bits(0x7FF0_0000_0000_0001);
    let values = [
        0.0,
        -0.0,
        f64::NAN,
        negative_nan,
        payload_nan,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    assert!(negative_nan.is_nan() && payload_nan.is_nan());
    for column in float_columns(&values) {
        let data_type = column.data_type().clone();
        let got = encode_each(column);
        assert_eq!(got[0], got[1], "{data_type}: -0.0 and 0.0");
        assert_eq!(got[2], got[3], "{data_type}: NaN and -NaN");
        assert_eq!(got[2], got[4], "{data_type}: NaN payloads");
        assert_ne!(got[2], got[5], "{data_type}: NaN and infinity");
        assert_ne!(got[5], got[6], "{data_type}: the two infinities");
    }
}

/// Distinct floats encode distinctly and in numeric order at every width,
/// from negative infinity through subnormals to positive infinity, with the
/// one NaN above them all.
#[test]
fn floats_encode_in_numeric_order() {
    let ordered = [
        f64::NEG_INFINITY,
        -1.0e300,
        -65_504.0,
        -1.5,
        -1.0,
        -6.0e-8,
        0.0,
        6.0e-8,
        1.0,
        1.5,
        65_504.0,
        1.0e300,
        f64::INFINITY,
        f64::NAN,
    ];
    for column in float_columns(&ordered) {
        let data_type = column.data_type().clone();
        let got = encode_each(column);
        let mut deduped = got.clone();
        // A value too large or too small for the width becomes its infinity
        // or zero when narrowed, so equal neighbours encode equally.
        deduped.dedup();
        assert!(
            deduped.windows(2).all(|pair| pair[0] < pair[1]),
            "{data_type}: not ascending: {got:?}"
        );
        assert!(deduped.len() >= 9, "{data_type}: too few distinct values");
    }
    let mut rng = StdRng::seed_from_u64(31);
    let samples: Vec<f64> = (0..20_000)
        .map(|_| f64::from_bits(rng.random::<u64>()))
        .filter(|v| !v.is_nan())
        .collect();
    let encoded = encode_each(Arc::new(arrow_array::Float64Array::from(samples.clone())));
    for i in 1..samples.len() {
        let (a, b) = (samples[i - 1], samples[i]);
        assert_eq!(
            a.partial_cmp(&b).expect("not NaN"),
            encoded[i - 1].cmp(&encoded[i]),
            "{a} vs {b}"
        );
    }
}

/// An integer column widened to `Float64` (as a schema change can widen
/// `Int32`) keeps exactly the integers' equality and order: two values encode
/// alike only when the integers were equal, and sort as the integers did.
#[test]
fn integers_widened_to_floats_keep_their_equality_and_order() {
    let mut rng = StdRng::seed_from_u64(32);
    let mut ints: Vec<i32> = vec![i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX - 1, i32::MAX];
    ints.extend((0..5_000).map(|_| rng.random::<i32>()));
    ints.extend((0..1_000).map(|_| rng.random_range(-50..50)));
    let widened = encode_each(Arc::new(arrow_array::Float64Array::from_iter_values(
        ints.iter().map(|&v| f64::from(v)),
    )));
    let as_ints = encode_each(Arc::new(Int32Array::from(ints.clone())));
    for i in 0..ints.len() {
        for j in [0, ints.len() / 2, ints.len() - 1, (i + 1) % ints.len()] {
            assert_eq!(
                ints[i].cmp(&ints[j]),
                widened[i].cmp(&widened[j]),
                "{} vs {} widened",
                ints[i],
                ints[j]
            );
            assert_eq!(
                widened[i] == widened[j],
                as_ints[i] == as_ints[j],
                "{} vs {}",
                ints[i],
                ints[j]
            );
        }
    }
}

/// Arrow defines `Time32` only in seconds and milliseconds and `Time64` only in
/// micro- and nanoseconds, and refuses to build an array of any other unit. A
/// key field declared with one is refused when the key is declared, rather
/// than accepted for a type no data can have; every defined unit is accepted.
#[test]
fn time_units_arrow_does_not_define_are_refused() {
    use arrow_schema::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
    let declare = |data_type: DataType| KeyEncoder::new(vec![KeyField::new(data_type, false)]);
    for undefined in [
        DataType::Time32(Microsecond),
        DataType::Time32(Nanosecond),
        DataType::Time64(Second),
        DataType::Time64(Millisecond),
    ] {
        assert!(
            matches!(
                declare(undefined.clone()),
                Err(Error::UnsupportedType { .. })
            ),
            "{undefined:?} must be refused"
        );
    }
    for defined in [
        DataType::Time32(Second),
        DataType::Time32(Millisecond),
        DataType::Time64(Microsecond),
        DataType::Time64(Nanosecond),
    ] {
        assert!(
            declare(defined.clone()).is_ok(),
            "{defined:?} must be accepted"
        );
    }
}

#[test]
fn a_dictionary_column_is_keyed_by_its_values() {
    let dictionary =
        |value: DataType| DataType::Dictionary(Box::new(DataType::Int8), Box::new(value));
    let list = DataType::List(Arc::new(arrow_schema::Field::new_list_field(
        DataType::Int32,
        true,
    )));
    assert_eq!(
        crate::key_type(&dictionary(DataType::Utf8)),
        &DataType::Utf8
    );
    assert_eq!(crate::key_type(&DataType::Int64), &DataType::Int64);
    assert!(crate::can_key(&dictionary(DataType::Utf8)));
    assert!(crate::can_key(&DataType::Float64));
    assert!(!crate::can_key(&list));
    assert!(!crate::can_key(&dictionary(list.clone())));
    for data_type in [DataType::Utf8, DataType::Float64, list] {
        assert_eq!(
            crate::can_key(&data_type),
            KeyEncoder::new(vec![KeyField::new(data_type.clone(), true)]).is_ok(),
            "{data_type:?}: `can_key` agrees with the encoder"
        );
    }
}
