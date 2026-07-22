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

//! Z-order (Morton) multi-column clustering key for the cold object-store tier.
//!
//! The cold-tier promotion stage rewrites settled warm data as read-optimized
//! Vortex files. To make a file's per-column zone maps (footer min/max) tight on
//! *every* clustering dimension at once — so a selective predicate on any of
//! them prunes most cold files — the rows are sorted by a **Z-order curve** over
//! the clustering columns (liquid-clustering-style multi-dimensional locality),
//! rather than a single-column lexicographic sort that only tightens the leading
//! column.
//!
//! ## How it works
//!
//! Each clustering column value is mapped to an **order-preserving** fixed-width
//! (`BYTES_PER_COLUMN`-byte) unsigned key: comparing two keys as big-endian
//! unsigned integers reproduces the column's natural ordering (NULLs sort
//! first). The per-column keys are then **bit-interleaved** most-significant-bit
//! first across the K columns into one `8*K`-byte key. Sorting those interleaved
//! keys ascending (plain lexicographic byte order) walks the Z-order space-
//! filling curve, so adjacent rows are close in all K dimensions.
//!
//! The key is a pure kernel ([`zorder_keys`]). The promotion path appends it as
//! a transient column ([`append_zorder_key_column`]), sorts on it, then strips it
//! ([`strip_zorder_key_column`]) — so the key is never materialized into the
//! written cold file.
//!
//! v1 uses Z-order; a Hilbert curve (better locality, materially more complex
//! encode) is a behind-the-key phase-2 swap that keeps every caller unchanged.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BinaryArray, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::{Float64Type, Int64Type, UInt64Type};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result as DFResult};

/// Number of order-preserving key bytes extracted per clustering column. 8 bytes
/// = 64 bits captures the full precision of every integer/temporal type and a
/// useful prefix of strings, which is what zone-map pruning keys on.
const BYTES_PER_COLUMN: usize = 8;

/// Name of the transient Z-order key column appended before the clustering sort
/// and stripped immediately after — it is NEVER written into a cold file.
pub const ZORDER_COLUMN_NAME: &str = "__cayenne_zorder_key";

/// Map a signed integer to an order-preserving `u64` (flip the sign bit so the
/// most-negative value maps to `0x0000…` and the most-positive to `0xFFFF…`).
#[inline]
fn key_from_i64(v: i64) -> [u8; BYTES_PER_COLUMN] {
    // Reinterpret the two's-complement bits as unsigned (no value change), then
    // flip the sign bit so the ordering becomes unsigned-big-endian monotonic.
    (v.cast_unsigned() ^ (1u64 << 63)).to_be_bytes()
}

/// Unsigned integers are already order-preserving as big-endian bytes.
#[inline]
fn key_from_u64(v: u64) -> [u8; BYTES_PER_COLUMN] {
    v.to_be_bytes()
}

/// Total-ordering transform for IEEE-754 doubles: negatives get all bits
/// flipped, non-negatives get just the sign bit flipped. The result compares as
/// an unsigned big-endian integer in the same order as the floats (NaN sorts at
/// the high end, which is acceptable for clustering).
#[inline]
fn key_from_f64(v: f64) -> [u8; BYTES_PER_COLUMN] {
    let bits = v.to_bits();
    let transformed = if bits >> 63 == 1 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    };
    transformed.to_be_bytes()
}

/// Lexicographic prefix key for variable-length bytes/strings: take the first
/// `BYTES_PER_COLUMN` bytes, right-padded with zeros.
#[inline]
fn key_from_bytes(s: &[u8]) -> [u8; BYTES_PER_COLUMN] {
    let mut key = [0u8; BYTES_PER_COLUMN];
    let n = s.len().min(BYTES_PER_COLUMN);
    key[..n].copy_from_slice(&s[..n]);
    key
}

/// `(0..n)` → per-row key, mapping NULL rows (a `None` from `key`) to the
/// all-zero "sorts-first" key. Owns the null/range/collect boilerplate shared by
/// every [`column_order_keys`] arm so each arm is just its downcast + key fn.
fn keys_with_nulls(
    n: usize,
    key: impl Fn(usize) -> Option<[u8; BYTES_PER_COLUMN]>,
) -> Vec<[u8; BYTES_PER_COLUMN]> {
    (0..n)
        .map(|i| key(i).unwrap_or([0u8; BYTES_PER_COLUMN]))
        .collect()
}

/// Compute one order-preserving `BYTES_PER_COLUMN`-byte key per row of `array`.
///
/// NULLs (and any unsupported column type) map to the all-zero key, which sorts
/// first — the "reserved minimal code". Supported types: all signed/unsigned
/// integers, floats, booleans, dates, times, timestamps, durations, and the
/// utf8/binary families (incl. their `View`/`Large` variants).
fn column_order_keys(array: &dyn Array) -> DFResult<Vec<[u8; BYTES_PER_COLUMN]>> {
    let n = array.len();

    let keys = match array.data_type() {
        DataType::Boolean => {
            let a = array.as_boolean();
            keys_with_nulls(n, |i| {
                (!a.is_null(i)).then(|| key_from_i64(i64::from(a.value(i))))
            })
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => {
            let arr = cast(array, &DataType::Int64)?;
            let a = arr.as_primitive::<Int64Type>();
            keys_with_nulls(n, |i| (!a.is_null(i)).then(|| key_from_i64(a.value(i))))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let arr = cast(array, &DataType::UInt64)?;
            let a = arr.as_primitive::<UInt64Type>();
            keys_with_nulls(n, |i| (!a.is_null(i)).then(|| key_from_u64(a.value(i))))
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let arr = cast(array, &DataType::Float64)?;
            let a = arr.as_primitive::<Float64Type>();
            keys_with_nulls(n, |i| (!a.is_null(i)).then(|| key_from_f64(a.value(i))))
        }
        // String/binary families read the value's byte prefix directly per
        // offset width — no cast to the `i32`-offset variant, which would fail
        // (or truncate) once a `Large`/`View` array's data exceeds `i32::MAX`.
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            keys_with_nulls(n, |i| {
                (!a.is_null(i)).then(|| key_from_bytes(a.value(i).as_bytes()))
            })
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            keys_with_nulls(n, |i| {
                (!a.is_null(i)).then(|| key_from_bytes(a.value(i).as_bytes()))
            })
        }
        DataType::Utf8View => {
            let a = array.as_string_view();
            keys_with_nulls(n, |i| {
                (!a.is_null(i)).then(|| key_from_bytes(a.value(i).as_bytes()))
            })
        }
        DataType::Binary => {
            let a = array.as_binary::<i32>();
            keys_with_nulls(n, |i| (!a.is_null(i)).then(|| key_from_bytes(a.value(i))))
        }
        DataType::LargeBinary => {
            let a = array.as_binary::<i64>();
            keys_with_nulls(n, |i| (!a.is_null(i)).then(|| key_from_bytes(a.value(i))))
        }
        DataType::BinaryView => {
            let a = array.as_binary_view();
            keys_with_nulls(n, |i| (!a.is_null(i)).then(|| key_from_bytes(a.value(i))))
        }
        // Unsupported type: contribute nothing to the curve (all rows equal on
        // this dimension) rather than failing the whole promotion.
        _ => vec![[0u8; BYTES_PER_COLUMN]; n],
    };

    Ok(keys)
}

/// Whether `data_type` has a dedicated value-encoding arm in
/// [`column_order_keys`], so its Z-order keys vary with the column's values and
/// the curve can cluster on it. A type without an arm falls to the catch-all and
/// maps *every* value to the reserved all-zero key — no clustering is ever
/// possible — which is exactly what this guard excludes (notably `Decimal*`,
/// `Map`, and nested types). A supported type may still encode an individual
/// value (e.g. `0` / `false`) to the all-zero key; that is expected and does not
/// change whether the type can cluster. MUST stay in sync with the `match` arms
/// in [`column_order_keys`].
pub(crate) fn is_zorder_clusterable(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
    )
}

/// Compute the interleaved Z-order key for each row across `columns`.
///
/// Returns a [`BinaryArray`] of `8 * columns.len()`-byte keys; sorting it
/// ascending walks the Z-order curve over the columns. All `columns` must be the
/// same length.
///
/// # Errors
///
/// Returns an error if `columns` is empty, the columns differ in length, or a
/// supported column fails to cast to its canonical key type.
pub fn zorder_keys(columns: &[ArrayRef]) -> DFResult<BinaryArray> {
    let k = columns.len();
    if k == 0 {
        return Err(DataFusionError::Internal(
            "zorder_keys requires at least one clustering column".to_string(),
        ));
    }
    let n = columns[0].len();

    let mut col_keys: Vec<Vec<[u8; BYTES_PER_COLUMN]>> = Vec::with_capacity(k);
    for c in columns {
        if c.len() != n {
            return Err(DataFusionError::Internal(
                "zorder_keys columns must all have the same length".to_string(),
            ));
        }
        col_keys.push(column_order_keys(c.as_ref())?);
    }

    let width = BYTES_PER_COLUMN * k;
    let total_bits = BYTES_PER_COLUMN * 8 * k;
    let mut flat = vec![0u8; width * n];

    // `col_keys` is column-major (`[dim][row]`) while the output is row-major, so
    // iterate the output row chunks and gather each row's per-column keys across
    // dimensions. Taking `row` from `enumerate()` (not a range) also keeps this
    // transpose out of `needless_range_loop`. `row_keys` is reused across rows to
    // avoid a per-row heap allocation on multi-million-row cold rewrites.
    let mut row_keys = vec![0u64; k];
    for (row, out) in flat.chunks_mut(width).enumerate() {
        // Pre-decode this row's per-column keys to u64 once.
        for (dim, rk) in row_keys.iter_mut().enumerate() {
            *rk = u64::from_be_bytes(col_keys[dim][row]);
        }
        // Interleave most-significant-bit-first: output bit j takes bit
        // (63 - j/k) of column (j % k). Lexicographic byte order over the
        // packed output then equals Morton (Z-order) order.
        for j in 0..total_bits {
            let round = j / k;
            let dim = j % k;
            let bit = (row_keys[dim] >> (63 - round)) & 1;
            if bit == 1 {
                out[j / 8] |= 1 << (7 - (j % 8));
            }
        }
    }

    Ok(BinaryArray::from_iter_values(
        (0..n).map(|r| &flat[r * width..(r + 1) * width]),
    ))
}

/// The schema produced by [`append_zorder_key_column`] for `base`: `base` plus a
/// trailing `Binary` [`ZORDER_COLUMN_NAME`] column.
#[must_use]
pub fn zorder_augmented_schema(base: &Schema) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = base.fields().iter().map(Arc::clone).collect();
    fields.push(Arc::new(Field::new(
        ZORDER_COLUMN_NAME,
        DataType::Binary,
        false,
    )));
    Arc::new(Schema::new(Fields::from(fields)))
}

/// Append the Z-order clustering key as a trailing `Binary` column
/// ([`ZORDER_COLUMN_NAME`]). Sorting the resulting batches ascending by that
/// column clusters rows in Z-order across `clustering_indices` (the proven
/// `SortExec` path is reused via `util::stream_utils::sort_stream`); the column
/// is stripped right after the sort with [`strip_zorder_key_column`], so it is
/// never written to a cold file.
///
/// # Errors
///
/// Returns an error if the key kernel or batch construction fails.
pub fn append_zorder_key_column(
    batch: &RecordBatch,
    clustering_indices: &[usize],
) -> DFResult<RecordBatch> {
    let cols: Vec<ArrayRef> = clustering_indices
        .iter()
        .map(|&i| Arc::clone(batch.column(i)))
        .collect();
    let key = Arc::new(zorder_keys(&cols)?) as ArrayRef;
    let schema = zorder_augmented_schema(batch.schema_ref());
    let mut arrays = batch.columns().to_vec();
    arrays.push(key);
    RecordBatch::try_new(schema, arrays).map_err(DataFusionError::from)
}

/// Drop the trailing Z-order key column, returning a batch in `original` schema.
///
/// # Errors
///
/// Returns an error if batch construction fails.
pub fn strip_zorder_key_column(batch: &RecordBatch, original: &SchemaRef) -> DFResult<RecordBatch> {
    let n = original.fields().len();
    let arrays = batch.columns()[..n].to_vec();
    RecordBatch::try_new(Arc::clone(original), arrays).map_err(DataFusionError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    /// Argsort: indices that would sort `keys` ascending lexicographically.
    fn argsort(keys: &BinaryArray) -> Vec<usize> {
        let mut idx: Vec<usize> = (0..keys.len()).collect();
        idx.sort_by(|&a, &b| keys.value(a).cmp(keys.value(b)));
        idx
    }

    #[test]
    fn single_column_preserves_order_including_negatives() {
        let col: ArrayRef = Arc::new(Int64Array::from(vec![3, -1, 2, 0, -5]));
        let keys = zorder_keys(&[col]).expect("keys");
        let order = argsort(&keys);
        // Sorted by key == ascending by value: -5, -1, 0, 2, 3 → original idx 4,1,3,2,0
        assert_eq!(order, vec![4, 1, 3, 2, 0]);
    }

    #[test]
    fn two_bit_grid_is_morton_order() {
        // 2x2 grid points (x, y); Morton code = (x<<1)|y → order (0,0),(0,1),(1,0),(1,1).
        let x: ArrayRef = Arc::new(Int64Array::from(vec![1, 0, 1, 0]));
        let y: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 0, 0]));
        let keys = zorder_keys(&[x, y]).expect("keys");
        let order = argsort(&keys);
        // Expected ascending Morton: (0,0)=idx3, (0,1)=idx1, (1,0)=idx2, (1,1)=idx0
        assert_eq!(order, vec![3, 1, 2, 0]);
    }

    #[test]
    fn key_width_is_eight_times_column_count() {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
        let keys = zorder_keys(&[a, b]).expect("keys");
        assert_eq!(keys.len(), 2);
        assert_eq!(keys.value(0).len(), BYTES_PER_COLUMN * 2);
    }

    #[test]
    fn nulls_sort_first() {
        let col: ArrayRef = Arc::new(Int64Array::from(vec![Some(5), None, Some(-3)]));
        let keys = zorder_keys(&[col]).expect("keys");
        let order = argsort(&keys);
        // null → all-zero key → first; then -3, then 5.
        assert_eq!(order, vec![1, 2, 0]);
    }

    #[test]
    fn empty_columns_is_error() {
        let err = zorder_keys(&[]).expect_err("must reject empty");
        assert!(err.to_string().contains("at least one"));
    }

    #[test]
    fn append_then_strip_roundtrips_and_clusters() {
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ]));
        // Same 2x2 grid as the Morton test.
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 0, 1, 0])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 1, 0, 0])) as ArrayRef,
            ],
        )
        .expect("batch");

        let augmented = append_zorder_key_column(&batch, &[0, 1]).expect("append");
        assert_eq!(augmented.num_columns(), 3);
        assert_eq!(
            augmented.schema().field(2).name(),
            ZORDER_COLUMN_NAME,
            "zorder key appended as the trailing column"
        );

        // Sorting by the appended key yields Morton order over (x, y).
        let key = augmented
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary key");
        let mut order: Vec<usize> = (0..key.len()).collect();
        order.sort_by(|&a, &b| key.value(a).cmp(key.value(b)));
        assert_eq!(order, vec![3, 1, 2, 0]);

        // Stripping restores the original schema and column data.
        let stripped = strip_zorder_key_column(&augmented, &schema).expect("strip");
        assert_eq!(stripped.schema(), schema);
        assert_eq!(stripped.num_columns(), 2);
    }
}
