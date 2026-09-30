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

//! Multi-column clustering key (Hilbert curve) for warm and datalake files.
//!
//! Warm rewrites and datalake promotion produce read-optimized Vortex files. To
//! make a file's per-column zone maps (footer min/max) tight on
//! *every* clustering dimension at once — so a selective predicate on any of
//! them prunes most cold files — the rows are sorted along a **space-filling
//! curve** over the clustering columns (liquid-clustering-style multi-dimensional
//! locality), rather than a single-column lexicographic sort that only tightens
//! the leading column.
//!
//! ## How it works
//!
//! 1. **Order-preserving key.** Each clustering column value maps to an
//!    order-preserving unsigned key: comparing two keys as unsigned integers
//!    reproduces the column's natural ordering (NULLs sort first).
//! 2. **Normalized coordinate.** The key is rescaled onto the full
//!    [`CURVE_BITS`]-wide coordinate space using the column's `[min, max]` from
//!    the table's maintained statistics aggregate. This is the step that makes
//!    the curve work at all, and it is worth stating why.
//!
//!    A curve interleaves the columns bit by bit from the most significant end,
//!    so a column only starts discriminating rows once the interleave reaches
//!    its first *varying* bit. Raw values put that bit wherever the column's
//!    magnitude happens to fall: a microsecond timestamp varies around bit 50,
//!    a small tenant id around bit 9. Interleaving those raw bits spends its
//!    first ~40 rounds on the timestamp alone, by which point a microsecond
//!    timestamp has already separated every row — so the key degenerates to the
//!    widest-range column's ordering and clusters nothing else. Rescaling each
//!    column onto the same dense coordinate space puts every column's first
//!    varying bit at the top, which is what makes the interleave a genuine
//!    multi-dimensional layout instead of a disguised single-column sort.
//!
//!    The bounds come from the whole-table aggregate at promotion time rather
//!    than from the rows being promoted, so every file written in that
//!    promotion shares one coordinate space. The aggregate widens as later
//!    writes merge new extrema, and clean cold files are carried forward
//!    without rewrite, so a later promotion may normalize onto a different
//!    scale. Values outside the current bounds clamp to the end cells.
//!    Clustering is a layout-quality property and never a correctness one, so
//!    a stale, widened, or missing bound costs pruning, never rows.
//!
//!    A column with *no* bound falls back to its type's full key domain, i.e.
//!    its raw key. Be clear about what that costs: raw keys are precisely the
//!    case described above, so the curve collapses back onto the widest-range
//!    column and a predicate on a narrow one prunes nothing at all. The fallback
//!    is a floor, not a mild degradation — see
//!    `missing_bounds_leave_the_narrow_column_unclustered`.
//! 3. **Hilbert index.** The per-column coordinates go through Skilling's
//!    transform (*Programming the Hilbert curve*, AIP Conf. Proc. 707, 2004),
//!    then interleave most-significant-bit-first into one `8*K`-byte key.
//!    Sorting those keys ascending as plain bytes walks the Hilbert curve.
//!    Hilbert is preferred over the Morton (Z-order) interleave because
//!    consecutive points on it are always adjacent in space — Morton's curve
//!    jumps across the domain at every quadrant boundary, and each jump is a
//!    zone map stretched over the whole range it jumped.
//!
//! The key is a pure kernel ([`cluster_keys`]). The promotion path appends it as
//! a transient column ([`append_cluster_key_column`]), sorts on it, then strips
//! it ([`strip_cluster_key_column`]) — so the key is never materialized into the
//! written cold file.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BinaryArray, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::{Decimal128Type, Float64Type, Int64Type, UInt64Type};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result as DFResult, ScalarValue};

/// Width, in bits, of the normalized coordinate each clustering column
/// contributes to the curve. 64 bits captures the full precision of every
/// integer/temporal type without rescaling loss, so a column whose statistics
/// are missing degrades to exactly its raw order-preserving key rather than to
/// a truncation of it.
const CURVE_BITS: u32 = u64::BITS;

/// Largest normalized coordinate; the dense code range is `0..=MAX_CODE`.
const MAX_CODE: u64 = u64::MAX;

/// Key bytes contributed per clustering column — the byte width of one
/// [`CURVE_BITS`]-wide coordinate, which is a `u64`.
const BYTES_PER_COLUMN: usize = size_of::<u64>();

/// Inclusive top of the key space for arms whose values fit in 64 bits.
const U64_KEY_DOMAIN: u128 = 0xFFFF_FFFF_FFFF_FFFF;

/// Inclusive top of the key space for arms that use the full 128-bit width
/// (`Decimal128`, and the 16-byte prefix taken from the string/binary family).
const U128_KEY_DOMAIN: u128 = u128::MAX;

/// Name of the transient clustering key column appended before the sort and
/// stripped immediately after — it is NEVER written into a cold file.
pub const CLUSTER_KEY_COLUMN_NAME: &str = "__cayenne_cluster_key";

/// Per-column `[min, max]` in the [`column_order_keys`] key space, or `None`
/// when the table's statistics carry no usable bound for that column.
///
/// `pub` rather than `pub(crate)` because it appears in [`cluster_keys`]'s
/// signature, which the benchmark re-export makes publicly reachable.
pub type ColumnBounds = Option<(u128, u128)>;

/// Map a signed integer to an order-preserving key (flip the sign bit so the
/// most-negative value maps to `0` and the most-positive to `u64::MAX`).
#[inline]
fn key_from_i64(v: i64) -> u128 {
    // Reinterpret the two's-complement bits as unsigned (no value change), then
    // flip the sign bit so the ordering becomes unsigned-monotonic.
    u128::from(v.cast_unsigned() ^ (1u64 << 63))
}

/// Unsigned integers are already order-preserving.
#[inline]
fn key_from_u64(v: u64) -> u128 {
    u128::from(v)
}

/// Total-ordering transform for IEEE-754 doubles: negatives get all bits
/// flipped, non-negatives get just the sign bit flipped. The result compares as
/// an unsigned integer in the same order as the floats (NaN sorts at the high
/// end, which is acceptable for clustering).
#[inline]
fn key_from_f64(v: f64) -> u128 {
    let bits = v.to_bits();
    let transformed = if bits >> 63 == 1 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    };
    u128::from(transformed)
}

/// Sign-bit flip at 128-bit width, for `Decimal128`'s unscaled value. A decimal
/// column has one fixed scale, so ordering the unscaled integers orders the
/// decimals.
#[inline]
fn key_from_i128(v: i128) -> u128 {
    v.cast_unsigned() ^ (1u128 << 127)
}

/// Lexicographic prefix key for variable-length bytes/strings: the first 16
/// bytes, right-padded with zeros.
#[inline]
fn key_from_bytes(s: &[u8]) -> u128 {
    let mut key = [0u8; 16];
    let n = s.len().min(16);
    key[..n].copy_from_slice(&s[..n]);
    u128::from_be_bytes(key)
}

/// `(0..n)` → per-row key, mapping NULL rows (a `None` from `key`) to the zero
/// "sorts-first" key. Owns the null/range/collect boilerplate shared by every
/// [`column_order_keys`] arm so each arm is just its downcast + key fn.
fn keys_with_nulls(n: usize, key: impl Fn(usize) -> Option<u128>) -> Vec<u128> {
    (0..n).map(|i| key(i).unwrap_or(0)).collect()
}

/// Top of the key space [`column_order_keys`] maps `data_type` into, i.e. the
/// normalization range to use when statistics carry no bound for the column.
///
/// Derived from the type alone, so a caller can resolve the range before
/// touching a single row.
fn key_domain_max(data_type: &DataType) -> u128 {
    match data_type {
        // 16-byte keys: the unscaled decimal, and the string/binary prefix.
        DataType::Decimal128(_, _)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView => U128_KEY_DOMAIN,
        // Every other arm with a value encoding fits its key in 64 bits.
        other if is_clusterable(other) => U64_KEY_DOMAIN,
        // No arm: every key is the reserved zero, so the coordinate is constant
        // too and a zero span is the honest range.
        _ => 0,
    }
}

/// Compute one order-preserving key per row of `array`.
///
/// NULLs (and any unsupported column type) map to the zero key, which sorts
/// first — the "reserved minimal code". Supported types: all signed/unsigned
/// integers, floats, booleans, dates, times, timestamps, durations,
/// `Decimal128`, and the utf8/binary families (incl. their `View`/`Large`
/// variants).
fn column_order_keys(array: &dyn Array) -> DFResult<Vec<u128>> {
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
        // Read the unscaled `i128` directly: casting a decimal to `Int64` would
        // divide out the scale, collapsing every value that shares an integer
        // part onto one key.
        DataType::Decimal128(_, _) => {
            let a = array.as_primitive::<Decimal128Type>();
            keys_with_nulls(n, |i| (!a.is_null(i)).then(|| key_from_i128(a.value(i))))
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
        // this dimension) rather than failing the whole promotion. Its
        // [`key_domain_max`] is zero, so the coordinate is constant too.
        _ => vec![0u128; n],
    };

    Ok(keys)
}

/// Whether `data_type` has a dedicated value-encoding arm in
/// [`column_order_keys`], so its clustering keys vary with the column's values
/// and the curve can cluster on it. A type without an arm falls to the catch-all
/// and maps *every* value to the reserved zero key — no clustering is ever
/// possible — which is exactly what this guard excludes (notably `Decimal256`,
/// `Map`, and nested types). A supported type may still encode an individual
/// value (e.g. the column minimum) to the zero key; that is expected and does
/// not change whether the type can cluster. MUST stay in sync with the `match`
/// arms in [`column_order_keys`].
pub(crate) fn is_clusterable(data_type: &DataType) -> bool {
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
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
    )
}

/// The order-preserving key for one statistics bound, in the same space
/// [`column_order_keys`] maps `column_type` into.
///
/// Going through the column kernel rather than re-deriving the transform is what
/// guarantees the bound and the data land in the same space: there is one
/// encoding, not two that must be kept in step. Returns `None` when the scalar
/// is NULL, its type disagrees with the column's, or the type cannot cluster.
pub(crate) fn bound_key(scalar: &ScalarValue, column_type: &DataType) -> Option<u128> {
    if scalar.is_null() || !is_clusterable(column_type) || scalar.data_type() != *column_type {
        return None;
    }
    let array = scalar.to_array().ok()?;
    column_order_keys(array.as_ref()).ok()?.first().copied()
}

/// Rescale an order-preserving key onto the dense `0..=MAX_CODE` coordinate
/// space spanned by `[lo, hi]`, clamping values outside it.
///
/// Runs once per value per clustering column, so it is inlined: `lo`/`hi` are
/// fixed for a whole promotion and the prologue below hoists out of the row
/// loop once the call is inlined.
#[inline]
fn normalize(key: u128, lo: u128, hi: u128) -> u64 {
    let span = hi.saturating_sub(lo);
    if span == 0 {
        // A constant (or inverted) range carries no information to spread.
        return 0;
    }
    let x = key.clamp(lo, hi) - lo;
    // `x * MAX_CODE` has to stay inside `u128`, which needs `span < 2^CURVE_BITS`.
    // When it does not, drop the low bits of `x` and `span` together: they sit
    // below the resolution a CURVE_BITS-wide coordinate can represent anyway, and
    // shifting both preserves the ratio.
    let shift = (u128::BITS - span.leading_zeros()).saturating_sub(CURVE_BITS);
    let (x, span) = (x >> shift, span >> shift);
    u64::try_from(x * u128::from(MAX_CODE) / span).unwrap_or(MAX_CODE)
}

/// Skilling's in-place axes-to-transpose transform for the Hilbert curve
/// (*Programming the Hilbert curve*, AIP Conf. Proc. 707, 2004).
///
/// On entry `x` holds one normalized coordinate per dimension; on return `x[i]`
/// holds every `i`-th bit of the Hilbert index, most significant first, so
/// interleaving `x` MSB-first yields the index itself. In one dimension the
/// transform is the identity, which keeps a single-column clustering key a plain
/// ascending sort.
fn hilbert_transpose(x: &mut [u64]) {
    // `split_first_mut` gives the algorithm's `X[0]` accumulator and the rest
    // without indexing. Its `i == 0` step reduces to the `head ^= p` below:
    // the exchange branch would compute `t = (X[0] ^ X[0]) & p`, i.e. zero.
    let Some((head, tail)) = x.split_first_mut() else {
        return;
    };
    let top = 1u64 << (CURVE_BITS - 1);

    // Undo the excess work: walk the bit planes from the top, reflecting and
    // rotating the sub-cube each plane selects.
    let mut q = top;
    while q > 1 {
        let p = q - 1;
        if *head & q != 0 {
            *head ^= p;
        }
        for xi in tail.iter_mut() {
            if *xi & q != 0 {
                *head ^= p;
            } else {
                let t = (*head ^ *xi) & p;
                *head ^= t;
                *xi ^= t;
            }
        }
        q >>= 1;
    }

    // Gray-encode: each axis takes the running XOR of the ones before it.
    let mut last = *head;
    for xi in tail.iter_mut() {
        *xi ^= last;
        last = *xi;
    }

    let mut t = 0u64;
    let mut q = top;
    while q > 1 {
        if last & q != 0 {
            t ^= q - 1;
        }
        q >>= 1;
    }
    *head ^= t;
    for xi in tail.iter_mut() {
        *xi ^= t;
    }
}

/// Compute the interleaved Hilbert clustering key for each row across `columns`.
///
/// `bounds` carries the per-column `[min, max]` (in [`column_order_keys`] key
/// space) the coordinates are normalized against — see the module docs for why
/// normalizing matters. It must be empty (no statistics for any column) or have
/// one entry per column; an absent entry falls back to the column type's full
/// key domain, which for the fixed-width types is the raw key unchanged — and
/// so gives up multi-dimensional clustering on that column entirely, rather
/// than degrading gently.
///
/// Returns a [`BinaryArray`] of `8 * columns.len()`-byte keys; sorting it
/// ascending walks the Hilbert curve over the columns. All `columns` must be the
/// same length.
///
/// # Errors
///
/// Returns an error if `columns` is empty, `bounds` is neither empty nor
/// column-length, the columns differ in length, or a supported column fails to
/// cast to its canonical key type.
pub fn cluster_keys(columns: &[ArrayRef], bounds: &[ColumnBounds]) -> DFResult<BinaryArray> {
    let k = columns.len();
    if k == 0 {
        return Err(DataFusionError::Internal(
            "cluster_keys requires at least one clustering column".to_string(),
        ));
    }
    if !bounds.is_empty() && bounds.len() != k {
        return Err(DataFusionError::Internal(format!(
            "cluster_keys got {} bounds for {k} clustering columns",
            bounds.len()
        )));
    }
    let n = columns[0].len();

    // Per-column normalized coordinates, column-major (`[dim][row]`).
    let mut coords: Vec<Vec<u64>> = Vec::with_capacity(k);
    for (dim, c) in columns.iter().enumerate() {
        if c.len() != n {
            return Err(DataFusionError::Internal(
                "cluster_keys columns must all have the same length".to_string(),
            ));
        }
        let keys = column_order_keys(c.as_ref())?;
        let (lo, hi) = bounds
            .get(dim)
            .copied()
            .flatten()
            .unwrap_or((0, key_domain_max(c.data_type())));
        coords.push(keys.iter().map(|&key| normalize(key, lo, hi)).collect());
    }

    let width = BYTES_PER_COLUMN * k;
    let mut flat = vec![0u8; width * n];

    // The output is row-major while `coords` is column-major, so iterate the
    // output row chunks and gather each row's coordinates across dimensions.
    // `row` is reused across rows to avoid a per-row heap allocation on
    // multi-million-row cold rewrites.
    let mut row = vec![0u64; k];
    for (r, out) in flat.chunks_mut(width).enumerate() {
        for (dim, x) in row.iter_mut().enumerate() {
            *x = coords[dim][r];
        }
        hilbert_transpose(&mut row);
        // Interleave most-significant-bit-first: output bit `j` takes bit
        // `CURVE_BITS - 1 - j / k` of dimension `j % k`. Lexicographic byte
        // order over the packed output then equals Hilbert index order.
        let mut j = 0usize;
        for round in 0..CURVE_BITS {
            let shift = CURVE_BITS - 1 - round;
            for x in &row {
                if (x >> shift) & 1 == 1 {
                    out[j / 8] |= 1 << (7 - (j % 8));
                }
                j += 1;
            }
        }
    }

    Ok(BinaryArray::from_iter_values(
        (0..n).map(|r| &flat[r * width..(r + 1) * width]),
    ))
}

/// The name for the transient clustering key column of `base`:
/// [`CLUSTER_KEY_COLUMN_NAME`], numbered when a table column already has that
/// name. The sort that follows resolves its key by name, so a shared name
/// would sort by the table's column instead of the key.
#[must_use]
pub fn cluster_key_column_name(base: &Schema) -> String {
    let mut name = CLUSTER_KEY_COLUMN_NAME.to_string();
    let mut suffix = 0_usize;
    while base.column_with_name(&name).is_some() {
        suffix += 1;
        name = format!("{CLUSTER_KEY_COLUMN_NAME}_{suffix}");
    }
    name
}

/// The schema produced by [`append_cluster_key_column`] for `base`: `base` plus
/// a trailing `Binary` column named `key_name` (see
/// [`cluster_key_column_name`]).
#[must_use]
pub fn cluster_augmented_schema(base: &Schema, key_name: &str) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = base.fields().iter().map(Arc::clone).collect();
    fields.push(Arc::new(Field::new(key_name, DataType::Binary, false)));
    Arc::new(Schema::new(Fields::from(fields)))
}

/// Append the clustering key as a trailing `Binary` column named `key_name`
/// (see [`cluster_key_column_name`]). Sorting the resulting batches ascending by
/// that column clusters rows along the Hilbert curve over `clustering_indices`
/// (the proven `SortExec` path is reused via `util::stream_utils::sort_stream`);
/// the column is stripped right after the sort with
/// [`strip_cluster_key_column`], so it is never written to a cold file.
///
/// `bounds` is the per-column normalization range, positionally matching
/// `clustering_indices`; see [`cluster_keys`].
///
/// # Errors
///
/// Returns an error if the key kernel or batch construction fails.
pub fn append_cluster_key_column(
    batch: &RecordBatch,
    clustering_indices: &[usize],
    bounds: &[ColumnBounds],
    key_name: &str,
) -> DFResult<RecordBatch> {
    let cols: Vec<ArrayRef> = clustering_indices
        .iter()
        .map(|&i| Arc::clone(batch.column(i)))
        .collect();
    let key = Arc::new(cluster_keys(&cols, bounds)?) as ArrayRef;
    let schema = cluster_augmented_schema(batch.schema_ref(), key_name);
    let mut arrays = batch.columns().to_vec();
    arrays.push(key);
    RecordBatch::try_new(schema, arrays).map_err(DataFusionError::from)
}

/// Drop the trailing clustering key column, returning a batch in `original`
/// schema.
///
/// # Errors
///
/// Returns an error if batch construction fails.
pub fn strip_cluster_key_column(
    batch: &RecordBatch,
    original: &SchemaRef,
) -> DFResult<RecordBatch> {
    let n = original.fields().len();
    let arrays = batch.columns()[..n].to_vec();
    RecordBatch::try_new(Arc::clone(original), arrays).map_err(DataFusionError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, Int64Array, StringArray, UInt64Array};

    /// Bounds that make [`normalize`] the identity, so a `UInt64` column's
    /// values *are* its curve coordinates and a test can address curve cells
    /// directly.
    const IDENTITY_BOUNDS: ColumnBounds = Some((0, U64_KEY_DOMAIN));

    /// Argsort: indices that would sort `keys` ascending lexicographically.
    fn argsort(keys: &BinaryArray) -> Vec<usize> {
        let mut idx: Vec<usize> = (0..keys.len()).collect();
        idx.sort_by(|&a, &b| keys.value(a).cmp(keys.value(b)));
        idx
    }

    /// `[min, max]` of an `Int64` column in key space, as the engine takes them
    /// from the maintained statistics aggregate.
    fn i64_bounds(values: &[i64]) -> ColumnBounds {
        let lo = values.iter().copied().min()?;
        let hi = values.iter().copied().max()?;
        Some((key_from_i64(lo), key_from_i64(hi)))
    }

    #[test]
    fn single_column_preserves_order_including_negatives() {
        let col: ArrayRef = Arc::new(Int64Array::from(vec![3, -1, 2, 0, -5]));
        let keys = cluster_keys(&[col], &[]).expect("keys");
        let order = argsort(&keys);
        // Sorted by key == ascending by value: -5, -1, 0, 2, 3 → original idx 4,1,3,2,0
        assert_eq!(order, vec![4, 1, 3, 2, 0]);
    }

    #[test]
    fn single_column_preserves_order_under_normalization() {
        // The 1-D Hilbert transform is the identity, and normalization is
        // monotone, so a one-column key stays a plain ascending sort even once
        // statistics rescale it.
        let values = vec![3i64, -1, 2, 0, -5];
        let col: ArrayRef = Arc::new(Int64Array::from(values.clone()));
        let keys = cluster_keys(&[col], &[i64_bounds(&values)]).expect("keys");
        assert_eq!(argsort(&keys), vec![4, 1, 3, 2, 0]);
    }

    #[test]
    fn key_width_is_eight_times_column_count() {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
        let keys = cluster_keys(&[a, b], &[]).expect("keys");
        assert_eq!(keys.len(), 2);
        assert_eq!(keys.value(0).len(), BYTES_PER_COLUMN * 2);
    }

    #[test]
    fn nulls_sort_first() {
        let col: ArrayRef = Arc::new(Int64Array::from(vec![Some(5), None, Some(-3)]));
        let keys = cluster_keys(&[col], &[]).expect("keys");
        let order = argsort(&keys);
        // null → zero key → first; then -3, then 5.
        assert_eq!(order, vec![1, 2, 0]);
    }

    #[test]
    fn empty_columns_is_error() {
        let err = cluster_keys(&[], &[]).expect_err("must reject empty");
        assert!(err.to_string().contains("at least one"));
    }

    #[test]
    fn bounds_length_must_match_columns() {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![3, 4]));
        let err = cluster_keys(&[a, b], &[None]).expect_err("must reject a short bounds list");
        assert!(err.to_string().contains("1 bounds for 2"));
    }

    /// The defining property of a Hilbert curve, and the reason it is preferred
    /// over Morton: consecutive points are always neighbours in space. A Morton
    /// (Z-order) interleave fails this at every quadrant boundary — the curve
    /// jumps the width of the quadrant, and each jump is a zone map stretched
    /// over everything it jumped.
    #[test]
    fn curve_visits_the_grid_in_unit_steps() {
        const SIDE: u64 = 8;
        let (mut xs, mut ys) = (Vec::new(), Vec::new());
        for x in 0..SIDE {
            for y in 0..SIDE {
                xs.push(x);
                ys.push(y);
            }
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(xs.clone())),
            Arc::new(UInt64Array::from(ys.clone())),
        ];
        let keys = cluster_keys(&cols, &[IDENTITY_BOUNDS, IDENTITY_BOUNDS]).expect("keys");
        let order = argsort(&keys);

        assert_eq!(
            order.len(),
            xs.len(),
            "the curve must visit every cell exactly once"
        );
        for pair in order.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            let step = xs[a].abs_diff(xs[b]) + ys[a].abs_diff(ys[b]);
            assert_eq!(
                step, 1,
                "({}, {}) → ({}, {}) is not a unit step",
                xs[a], ys[a], xs[b], ys[b]
            );
        }
    }

    #[test]
    fn curve_index_is_a_bijection() {
        const SIDE: u64 = 16;
        let (mut xs, mut ys) = (Vec::new(), Vec::new());
        for x in 0..SIDE {
            for y in 0..SIDE {
                xs.push(x);
                ys.push(y);
            }
        }
        let cells = xs.len();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(xs)),
            Arc::new(UInt64Array::from(ys)),
        ];
        let keys = cluster_keys(&cols, &[IDENTITY_BOUNDS, IDENTITY_BOUNDS]).expect("keys");
        let distinct: std::collections::HashSet<&[u8]> =
            (0..keys.len()).map(|i| keys.value(i)).collect();
        assert_eq!(
            distinct.len(),
            cells,
            "every grid cell must get its own curve index"
        );
    }

    /// Per-block `[min, max]` on `dim` under `order` — the zone map a
    /// listing-time pruner reads. Computed once so a probe sweep stays cheap.
    fn block_ranges(order: &[usize], dim: &[i64], blocks: usize) -> Vec<(i64, i64)> {
        let per_block = order.len() / blocks;
        (0..blocks)
            .map(|b| {
                let rows = &order[b * per_block..(b + 1) * per_block];
                let lo = rows.iter().map(|&r| dim[r]).min().unwrap_or(i64::MAX);
                let hi = rows.iter().map(|&r| dim[r]).max().unwrap_or(i64::MIN);
                (lo, hi)
            })
            .collect()
    }

    /// Blocks a point query on `probe` has to open.
    fn blocks_touched(ranges: &[(i64, i64)], probe: i64) -> usize {
        ranges
            .iter()
            .filter(|(lo, hi)| probe >= *lo && probe <= *hi)
            .count()
    }

    /// Worst-case blocks opened across every value in `probes`. A single probe
    /// position is a poor summary: how many blocks a point query opens varies
    /// across the range, so one sample can land on an unrepresentative best case.
    fn worst_blocks_touched(ranges: &[(i64, i64)], probes: impl Iterator<Item = i64>) -> usize {
        probes.map(|p| blocks_touched(ranges, p)).max().unwrap_or(0)
    }

    const UNEQUAL_ROWS: i64 = 4096;
    const UNEQUAL_BLOCKS: usize = 64;
    const UNEQUAL_TENANTS: i64 = 16;

    /// The shape this kernel's normalization step exists for: a microsecond
    /// timestamp beside a small, uncorrelated tenant id.
    fn unequal_range_fixture() -> (Vec<i64>, Vec<i64>) {
        // Strictly increasing microseconds — the ingest-order shape that makes
        // a degenerate key indistinguishable from arrival order.
        let base_ts = 1_700_000_000_000_000i64;
        let ts: Vec<i64> = (0..UNEQUAL_ROWS).map(|i| base_ts + i * 1_000).collect();
        // Deterministic and well spread, so a tenant probe is a fair test.
        let tenants: Vec<i64> = (0..UNEQUAL_ROWS)
            .map(|i| (i * 7) % UNEQUAL_TENANTS)
            .collect();
        (ts, tenants)
    }

    fn unequal_range_order(ts: &[i64], tenants: &[i64], bounds: &[ColumnBounds]) -> Vec<usize> {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(ts.to_vec())),
            Arc::new(Int64Array::from(tenants.to_vec())),
        ];
        argsort(&cluster_keys(&cols, bounds).expect("keys"))
    }

    /// Regression guard for the defect the normalization step exists to fix:
    /// with columns of very different magnitude, interleaving the *raw* value
    /// bits spends every high-order round on the wide column, so the key
    /// degenerates to that column's ordering and a predicate on the narrow one
    /// has to read every block. Normalized coordinates give both columns the
    /// same significance, so both prune.
    #[test]
    fn unequal_column_ranges_cluster_on_both_dimensions() {
        let (ts, tenants) = unequal_range_fixture();
        let bounds = [i64_bounds(&ts), i64_bounds(&tenants)];
        let order = unequal_range_order(&ts, &tenants, &bounds);

        // The narrow column is the discriminating one, so hold it to a tight
        // bound: the raw-bit key opens every block here, and improving the
        // layout only drives this number down.
        let tenant_ranges = block_ranges(&order, &tenants, UNEQUAL_BLOCKS);
        let worst_tenant = worst_blocks_touched(&tenant_ranges, 0..UNEQUAL_TENANTS);
        assert!(
            worst_tenant < UNEQUAL_BLOCKS / 4,
            "the worst tenant probe opened {worst_tenant} of {UNEQUAL_BLOCKS} blocks: the \
             clustering key is not separating the narrow column"
        );

        // The wide column must not have been sacrificed to buy that. This is a
        // loose ceiling on purpose. It excludes a layout that clustered only the
        // narrow column, which would open every block here — but it says nothing
        // about the raw-bit key, which orders by `ts` and so prunes a timestamp
        // probe perfectly while failing the assertion above. It is deliberately
        // not tight, because a genuine improvement in multi-dimensional
        // clustering trades some `ts` locality away and drives this number up.
        let ts_ranges = block_ranges(&order, &ts, UNEQUAL_BLOCKS);
        let worst_ts = worst_blocks_touched(&ts_ranges, ts.iter().copied());
        assert!(
            worst_ts < UNEQUAL_BLOCKS / 2,
            "the worst timestamp probe opened {worst_ts} of {UNEQUAL_BLOCKS} blocks"
        );
    }

    /// Pins a known limitation so that removing it is a deliberate act rather
    /// than an unnoticed one. With no statistics there is no range to rescale
    /// against, the coordinates fall back to raw keys, and the curve collapses
    /// onto the wide column exactly as the pre-normalization key did: a tenant
    /// predicate reads every block. Only layout quality is lost, never rows —
    /// but it is lost completely, not gradually.
    #[test]
    fn missing_bounds_leave_the_narrow_column_unclustered() {
        let (ts, tenants) = unequal_range_fixture();
        let order = unequal_range_order(&ts, &tenants, &[None, None]);

        let ranges = block_ranges(&order, &tenants, UNEQUAL_BLOCKS);
        let worst = worst_blocks_touched(&ranges, 0..UNEQUAL_TENANTS);
        assert_eq!(
            worst, UNEQUAL_BLOCKS,
            "without statistics the worst tenant probe opens every block; if this now prunes, \
             the fallback has improved and this test should be updated to match"
        );
    }

    /// A `Decimal128` column used to fall through to the catch-all arm and map
    /// every value to the reserved zero key: configured as a clustering column
    /// it silently produced no clustering at all.
    #[test]
    fn decimal_values_cluster() {
        // Unscaled cents, i.e. Decimal128(10, 2) values -20.48 … 20.47, in a
        // scrambled order so an ordering the input already had proves nothing.
        let unscaled: Vec<i128> = (0..4096i128).map(|i| (i * 1237) % 4096 - 2048).collect();
        let col: ArrayRef = Arc::new(
            Decimal128Array::from(unscaled.clone())
                .with_precision_and_scale(10, 2)
                .expect("decimal array"),
        );
        assert!(
            is_clusterable(col.data_type()),
            "Decimal128 must be reported as clusterable"
        );
        let bounds = Some((
            key_from_i128(*unscaled.iter().min().expect("non-empty")),
            key_from_i128(*unscaled.iter().max().expect("non-empty")),
        ));
        let keys = cluster_keys(&[col], &[bounds]).expect("keys");
        // Single column: the key must reproduce the decimal ordering exactly, and
        // tell every distinct value apart.
        let mut expected: Vec<usize> = (0..unscaled.len()).collect();
        expected.sort_by_key(|&row| unscaled[row]);
        assert_eq!(argsort(&keys), expected);
        let distinct: std::collections::HashSet<&[u8]> =
            (0..keys.len()).map(|row| keys.value(row)).collect();
        assert_eq!(distinct.len(), unscaled.len());
    }

    #[test]
    fn bound_key_rejects_mismatched_and_null_scalars() {
        assert!(
            bound_key(&ScalarValue::Int64(Some(7)), &DataType::Int64).is_some(),
            "a matching, non-null bound is usable"
        );
        assert!(
            bound_key(&ScalarValue::Int64(None), &DataType::Int64).is_none(),
            "a NULL bound carries no range"
        );
        assert!(
            bound_key(&ScalarValue::Int32(Some(7)), &DataType::Int64).is_none(),
            "a bound of a different type would land in a different key space"
        );
    }

    #[test]
    fn bound_key_agrees_with_the_column_encoding() {
        // The bound and the data must be encoded identically, or normalization
        // would rescale against a range the values do not live in.
        let values = vec![-9i64, 0, 41];
        let col: ArrayRef = Arc::new(Int64Array::from(values.clone()));
        let keys = column_order_keys(col.as_ref()).expect("column keys");
        for (value, key) in values.iter().zip(keys) {
            assert_eq!(
                bound_key(&ScalarValue::Int64(Some(*value)), &DataType::Int64),
                Some(key)
            );
        }
    }

    #[test]
    fn out_of_range_values_clamp_rather_than_wrap() {
        // Statistics lag writes, so a promotion can carry values outside the
        // bounds it normalizes against. They must pin to the end cells, keeping
        // the coordinate monotone.
        let lo = key_from_i64(0);
        let hi = key_from_i64(100);
        assert_eq!(normalize(key_from_i64(-5), lo, hi), 0);
        assert_eq!(normalize(key_from_i64(0), lo, hi), 0);
        assert_eq!(normalize(key_from_i64(100), lo, hi), MAX_CODE);
        assert_eq!(normalize(key_from_i64(5_000), lo, hi), MAX_CODE);
        // Monotone in between.
        assert!(normalize(key_from_i64(25), lo, hi) < normalize(key_from_i64(75), lo, hi));
    }

    #[test]
    fn widening_bounds_move_the_same_value_on_the_curve() {
        // The maintained aggregate widens as later writes merge new extrema,
        // and clean cold files keep the layout they were written with. The
        // same value therefore lands on a different coordinate once the
        // bounds change — the space is shared within one promotion, not
        // across them.
        let value = key_from_i64(50);
        let first = normalize(value, key_from_i64(0), key_from_i64(100));
        let widened = normalize(value, key_from_i64(-100), key_from_i64(100));
        assert_eq!(first, 0x7fff_ffff_ffff_ffff);
        assert_eq!(widened, 0xbfff_ffff_ffff_ffff);
        assert_ne!(first, widened);
    }

    #[test]
    fn constant_range_normalizes_to_a_constant_coordinate() {
        let lo = key_from_i64(7);
        assert_eq!(normalize(key_from_i64(7), lo, lo), 0);
        assert_eq!(normalize(key_from_i64(9), lo, lo), 0);
    }

    #[test]
    fn full_width_range_normalizes_without_overflow() {
        // The 128-bit key domain is the widest span `normalize` must handle; it
        // reduces to the top CURVE_BITS of the key.
        assert_eq!(normalize(0, 0, U128_KEY_DOMAIN), 0);
        assert_eq!(normalize(U128_KEY_DOMAIN, 0, U128_KEY_DOMAIN), MAX_CODE);
        assert_eq!(normalize(1u128 << 64, 0, U128_KEY_DOMAIN), 1);
    }

    #[test]
    fn missing_bounds_fall_back_to_the_raw_key() {
        // Without statistics a fixed-width column must keep its exact raw key,
        // so clustering never degrades below what the bounds-free path gave.
        let values = vec![3i64, -1, 2, 0, -5];
        let col: ArrayRef = Arc::new(Int64Array::from(values.clone()));
        let with_none = cluster_keys(&[Arc::clone(&col)], &[None]).expect("keys");
        let without = cluster_keys(&[col], &[]).expect("keys");
        for i in 0..values.len() {
            assert_eq!(with_none.value(i), without.value(i));
        }
    }

    #[test]
    fn append_then_strip_roundtrips_and_clusters() {
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ]));
        let xs = vec![1i64, 0, 1, 0];
        let ys = vec![1i64, 1, 0, 0];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(xs.clone())) as ArrayRef,
                Arc::new(Int64Array::from(ys.clone())) as ArrayRef,
            ],
        )
        .expect("batch");

        let bounds = vec![i64_bounds(&xs), i64_bounds(&ys)];
        let augmented =
            append_cluster_key_column(&batch, &[0, 1], &bounds, &cluster_key_column_name(&schema))
                .expect("append");
        assert_eq!(augmented.num_columns(), 3);
        assert_eq!(
            augmented.schema().field(2).name(),
            CLUSTER_KEY_COLUMN_NAME,
            "clustering key appended as the trailing column"
        );

        // The four corners of the unit cell come back in unit steps.
        let key = augmented
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary key");
        let order = argsort(key);
        for pair in order.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            assert_eq!(xs[a].abs_diff(xs[b]) + ys[a].abs_diff(ys[b]), 1);
        }

        // Stripping restores the original schema and column data.
        let stripped = strip_cluster_key_column(&augmented, &schema).expect("strip");
        assert_eq!(stripped.schema(), schema);
        assert_eq!(stripped.num_columns(), 2);
    }
}
