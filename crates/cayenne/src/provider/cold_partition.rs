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

//! Which durable tombstones a warm-to-datalake promotion must keep.
//!
//! A promotion writes only the warm tier's live rows into new datalake (cold)
//! files and carries every existing cold file forward by manifest reference,
//! never re-reading or rewriting it. A cold file therefore keeps the rows an
//! upsert or delete has since superseded, and the tombstones that superseded
//! them are what hide those rows at read time: the read path applies a
//! tombstone to a cold file only when the tombstone is newer than every row in
//! the file (`delete_sequence > ColdTierFile::max_sequence`), the same rule a
//! protected snapshot applies against its creation threshold.
//!
//! A tombstone can still hide a cold row only when it is newer than that file
//! AND its key may be in the file. Key membership is decided entirely from
//! in-memory manifest data: each cold file's persisted statistics blob yields a
//! per-PK-column min/max hyper-rectangle, and when the file carries a per-file
//! PK bloom (`ColdTierFile::pk_bloom`) the key must ALSO bloom-hit. Both tests
//! individually never miss a hosted key (true bounds contain it; blooms have no
//! false negatives for inserted keys), so their intersection doesn't either —
//! while the bloom rejects ~99% of in-band-but-absent keys, exactly the
//! precision rectangles lose as carried files' PK ranges drift across
//! promotions. The rectangle stays first, so the bloom is only probed for the
//! few tombstones the rectangle cannot exclude.
//!
//! Detection is CONSERVATIVE in the safe direction: a false positive keeps a
//! tombstone that hides nothing; a missed one would resurrect a superseded
//! cold row. Any file whose statistics are absent/undecodable — or any key that
//! cannot be compared — counts as possibly holding the key (bloom-only when a
//! bloom is present).

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow_schema::Schema;
use datafusion_common::ScalarValue;
use datafusion_common::stats::Precision;

use super::pk_index::{PkBloom, PkByteRange};
use crate::metadata::ColdTierFile;
use crate::row_converter::{Row, RowConverter};
use crate::stats::statistics_from_persisted_blob;

/// `SessionConfig` extension supplying the exact cold files the scan branch
/// must read. Attached ONLY by the promotion's private session, which supplies
/// none: a promotion reads the warm tier alone and carries every cold file
/// forward by manifest reference. User-query sessions never carry it, so
/// queries always see the full captured manifest.
///
/// Carries manifest ROWS, not their URLs; see [`cold_files_for_scan`] for why
/// a supplied set replaces the scan's own capture rather than filtering it.
#[derive(Debug)]
pub(crate) struct ColdScanFiles(pub Arc<Vec<ColdTierFile>>);

/// Resolve which cold files a scan branch reads: the rows the promotion's
/// private session supplies, else the caller's fenced manifest capture.
///
/// `promotion` REPLACES `captured` — it is deliberately not intersected with
/// it. The two are independent reads of `cayenne_cold_tier_file` with no lock
/// held across them, so they can disagree, and intersecting silently yields
/// the smaller set (#12708). A promotion supplies an empty set, so its read of
/// the visible rows covers the warm tier and no cold file at all.
pub(crate) fn cold_files_for_scan<'a>(
    captured: &'a [ColdTierFile],
    promotion: Option<&'a ColdScanFiles>,
) -> Vec<&'a ColdTierFile> {
    match promotion {
        Some(promotion) => promotion.0.iter().collect(),
        None => captured.iter().collect(),
    }
}

/// Decode `RowConverter`-encoded tombstone keys into row-major
/// `ScalarValue` tuples (one inner `Vec` per key, one value per PK column).
///
/// # Errors
///
/// Returns an error when the bytes are malformed for this converter's schema
/// — callers treat that as "cannot decide which tombstones to keep" and skip
/// the promotion.
pub(crate) fn decode_tombstone_keys(
    converter: &RowConverter,
    keys: &[Box<[u8]>],
) -> Result<Vec<Vec<ScalarValue>>, arrow_schema::ArrowError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let columns = converter.convert_rows(keys.iter().map(|k| Row::from_encoded(k)))?;
    let mut rows = vec![Vec::with_capacity(columns.len()); keys.len()];
    for column in &columns {
        for (i, row) in rows.iter_mut().enumerate() {
            row.push(ScalarValue::try_from_array(column, i)?);
        }
    }
    Ok(rows)
}

/// Decode raw big-endian `i64` tombstone keys (the Int64-PK fast path's
/// durable encoding) into single-column `ScalarValue` tuples.
///
/// # Errors
///
/// Returns an error when a key is not exactly 8 bytes — callers treat that
/// as "cannot decide which tombstones to keep". Truncating a longer buffer
/// would decode a wrong value and could drop a tombstone that still hides a
/// cold row.
pub(crate) fn decode_int64_tombstone_keys(
    keys: &[Box<[u8]>],
) -> Result<Vec<Vec<ScalarValue>>, arrow_schema::ArrowError> {
    keys.iter()
        .map(|bytes| {
            if let Ok(arr) = <[u8; 8]>::try_from(bytes.as_ref()) {
                Ok(vec![ScalarValue::Int64(Some(i64::from_be_bytes(arr)))])
            } else {
                Err(arrow_schema::ArrowError::InvalidArgumentError(format!(
                    "Int64 tombstone key has {} bytes, expected exactly 8",
                    bytes.len()
                )))
            }
        })
        .collect()
}

/// Re-encode decoded Int64 tombstone keys into the converter row encoding the
/// per-file PK blooms insert (the Int64 fast path's deletion vectors persist
/// raw big-endian bytes — a different representation). `None` when any key is
/// not a single non-null `Int64` or conversion fails — the caller then
/// decides by rectangle only (the safe default, not an error).
pub(crate) fn encode_int64_bloom_probes(
    converter: &RowConverter,
    keys: &[Vec<ScalarValue>],
) -> Option<Vec<Box<[u8]>>> {
    let values = keys
        .iter()
        .map(|k| match k.as_slice() {
            [ScalarValue::Int64(Some(v))] => Some(*v),
            _ => None,
        })
        .collect::<Option<Vec<i64>>>()?;
    let column: ArrayRef = Arc::new(Int64Array::from(values));
    let rows = converter.convert_columns(&[column]).ok()?;
    Some(
        (0..keys.len())
            .map(|i| rows.row(i).data().to_vec().into_boxed_slice())
            .collect(),
    )
}

/// A cold file's PK bloom, parsed on first probe only — files the rectangle
/// excludes never pay the parse. A missing/corrupt bloom probes as "MAY
/// contain" (conservative), degrading that file to the rectangle test alone.
enum LazyBloom<'a> {
    Unparsed(Option<&'a [u8]>),
    Parsed(Option<PkBloom>),
}

impl LazyBloom<'_> {
    fn maybe_contains(&mut self, probe: &[u8]) -> bool {
        if let Self::Unparsed(bytes) = self {
            *self = Self::Parsed(bytes.and_then(PkBloom::from_bytes));
        }
        match self {
            Self::Parsed(Some(bloom)) => bloom.maybe_contains(probe),
            _ => true,
        }
    }
}

/// Which of `tombstones` may each hide a row of some file in `cold_files`,
/// index-aligned with `tombstones`.
///
/// `tombstones` pairs each tombstoned key — row-major, one value per PK column
/// in primary-key order (`decode_tombstone_keys` output) — with the sequence of
/// its newest delete. `pk_indices` are the PK columns' indices in `schema`, in
/// that same order.
///
/// A tombstone may hide a row of a cold file when it is newer than the file
/// (`delete_sequence > max_sequence`, the rule the read path applies) and its
/// key falls inside the file's rectangle and, when the file has a usable bloom,
/// bloom-hits. `bloom_probes` are the same tombstones in the byte encoding the
/// per-file PK blooms insert, index-aligned with `tombstones` (composite DV
/// keys are already that encoding; Int64 keys go through
/// [`encode_int64_bloom_probes`]). `None`, a length mismatch, or a bloom-less
/// file falls back to the rectangle alone.
///
/// A promotion keeps exactly the tombstones marked `true`. One that is `false`
/// can hide no row of any carried file, and none of a file written later,
/// whose rows are all newer than it.
pub(crate) fn cold_relevant_tombstones(
    cold_files: &[ColdTierFile],
    tombstones: &[(Vec<ScalarValue>, i64)],
    bloom_probes: Option<&[Box<[u8]>]>,
    schema: &Arc<Schema>,
    pk_indices: &[usize],
) -> Vec<bool> {
    let probes = bloom_probes.filter(|p| p.len() == tombstones.len());
    let mut files: Vec<ColdFileKeyTest<'_>> = cold_files
        .iter()
        .map(|file| ColdFileKeyTest {
            max_sequence: file.max_sequence,
            bounds: pk_bounds_from_manifest(file, schema, pk_indices),
            bloom: LazyBloom::Unparsed(probes.and(file.pk_bloom.as_deref())),
        })
        .collect();
    tombstones
        .iter()
        .enumerate()
        .map(|(i, (key, delete_sequence))| {
            let probe = probes.and_then(|p| p.get(i)).map(Box::as_ref);
            files
                .iter_mut()
                .any(|file| file.may_hide(key, *delete_sequence, probe))
        })
        .collect()
}

/// One cold file's side of the "may this tombstone hide a row here" test.
struct ColdFileKeyTest<'a> {
    max_sequence: i64,
    bounds: Option<Vec<(ScalarValue, ScalarValue)>>,
    bloom: LazyBloom<'a>,
}

impl ColdFileKeyTest<'_> {
    fn may_hide(
        &mut self,
        key: &[ScalarValue],
        delete_sequence: i64,
        probe: Option<&[u8]>,
    ) -> bool {
        delete_sequence > self.max_sequence
            && self
                .bounds
                .as_ref()
                .is_none_or(|bounds| key_within_bounds(key, bounds))
            && probe.is_none_or(|probe| self.bloom.maybe_contains(probe))
    }
}

/// Extract the per-PK-column `[min, max]` bounds from a cold file's persisted
/// statistics blob. `None` when the blob is missing/undecodable or any PK
/// column lacks concrete, non-null bounds (→ conservatively "may contain").
fn pk_bounds_from_manifest(
    file: &ColdTierFile,
    schema: &Arc<Schema>,
    pk_indices: &[usize],
) -> Option<Vec<(ScalarValue, ScalarValue)>> {
    let stats = statistics_from_persisted_blob(&file.statistics_blob, schema, file.row_count)?;
    let mut bounds = Vec::with_capacity(pk_indices.len());
    for &idx in pk_indices {
        let col = stats.column_statistics.get(idx)?;
        let (
            Precision::Exact(min) | Precision::Inexact(min),
            Precision::Exact(max) | Precision::Inexact(max),
        ) = (&col.min_value, &col.max_value)
        else {
            return None;
        };
        if min.is_null() || max.is_null() {
            return None;
        }
        bounds.push((min.clone(), max.clone()));
    }
    Some(bounds)
}

/// A cold file's single-column integer PK range in the row-converter byte
/// encoding (see [`PkByteRange`]). `None` when the file has no usable
/// statistics, the key is composite or not an integer, or a bound cannot be
/// encoded, so the caller treats the file as always a candidate.
pub(crate) fn pk_byte_range_from_manifest(
    file: &ColdTierFile,
    schema: &Arc<Schema>,
    pk_indices: &[usize],
    converter: &RowConverter,
) -> Option<PkByteRange> {
    let [idx] = pk_indices else {
        return None;
    };
    if !schema.field(*idx).data_type().is_integer() {
        return None;
    }
    let bounds = pk_bounds_from_manifest(file, schema, pk_indices)?;
    let (min, max) = bounds.first()?;
    if min.data_type() != *schema.field(*idx).data_type()
        || max.data_type() != *schema.field(*idx).data_type()
        || min.partial_cmp(max)?.is_gt()
    {
        return None;
    }
    let columns: Vec<ArrayRef> = vec![min.to_array().ok()?, max.to_array().ok()?];
    let merged = arrow::compute::concat(&[columns[0].as_ref(), columns[1].as_ref()]).ok()?;
    let rows = converter.convert_columns(&[merged]).ok()?;
    Some(PkByteRange {
        min: rows.row(0).data().to_vec().into_boxed_slice(),
        max: rows.row(1).data().to_vec().into_boxed_slice(),
    })
}

/// Whether a key tuple falls inside every per-column bound (point-in-
/// hyper-rectangle). Incomparable values (type mismatch, NULL) count as
/// "inside" — conservatively "may contain".
fn key_within_bounds(key: &[ScalarValue], bounds: &[(ScalarValue, ScalarValue)]) -> bool {
    key.iter().zip(bounds.iter()).all(|(v, (min, max))| {
        match (v.partial_cmp(min), v.partial_cmp(max)) {
            (Some(cmp_min), Some(cmp_max)) => cmp_min.is_ge() && cmp_max.is_le(),
            // Incomparable → cannot exclude → treat as contained.
            _ => true,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(vals: &[i64]) -> Vec<ScalarValue> {
        vals.iter().map(|v| ScalarValue::Int64(Some(*v))).collect()
    }

    fn bounds(pairs: &[(i64, i64)]) -> Vec<(ScalarValue, ScalarValue)> {
        pairs
            .iter()
            .map(|(lo, hi)| (ScalarValue::Int64(Some(*lo)), ScalarValue::Int64(Some(*hi))))
            .collect()
    }

    #[test]
    fn key_inside_rectangle_is_contained() {
        assert!(key_within_bounds(
            &key(&[5, 50]),
            &bounds(&[(1, 10), (40, 60)])
        ));
    }

    #[test]
    fn key_outside_any_dimension_is_excluded() {
        // Inside dim 0, outside dim 1.
        assert!(!key_within_bounds(
            &key(&[5, 99]),
            &bounds(&[(1, 10), (40, 60)])
        ));
        // Boundary values are inclusive.
        assert!(key_within_bounds(
            &key(&[1, 60]),
            &bounds(&[(1, 10), (40, 60)])
        ));
    }

    #[test]
    fn incomparable_values_are_conservatively_contained() {
        let mismatched = vec![ScalarValue::Utf8(Some("x".to_string()))];
        assert!(key_within_bounds(&mismatched, &bounds(&[(1, 10)])));
    }

    #[test]
    fn int64_key_decode_roundtrip() {
        let keys: Vec<Box<[u8]>> = vec![
            7_i64.to_be_bytes().to_vec().into_boxed_slice(),
            (-3_i64).to_be_bytes().to_vec().into_boxed_slice(),
        ];
        let decoded = decode_int64_tombstone_keys(&keys).expect("valid 8-byte keys");
        assert_eq!(decoded[0], vec![ScalarValue::Int64(Some(7))]);
        assert_eq!(decoded[1], vec![ScalarValue::Int64(Some(-3))]);
        // Short or long key → cannot classify (truncation could decode a
        // wrong value and misclassify a dirty file as clean).
        let short: Vec<Box<[u8]>> = vec![vec![1, 2].into_boxed_slice()];
        decode_int64_tombstone_keys(&short).expect_err("short key must fail to decode");
        let long: Vec<Box<[u8]>> = vec![vec![0; 9].into_boxed_slice()];
        decode_int64_tombstone_keys(&long).expect_err("long key must fail to decode");
    }
}

#[cfg(test)]
mod composite_key_tests {
    use super::*;
    use crate::row_converter::SortField;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow_schema::{DataType, Field};
    use datafusion_common::ColumnStatistics;
    use datafusion_common::Statistics;

    /// CH-benCHmark `order_line`-shaped schema: composite integer PK
    /// `(ol_w_id, ol_d_id, ol_o_id, ol_number)` plus a payload column.
    fn orderline_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("ol_w_id", DataType::Int64, false),
            Field::new("ol_d_id", DataType::Int64, false),
            Field::new("ol_o_id", DataType::Int64, false),
            Field::new("ol_number", DataType::Int64, false),
            Field::new("ol_amount", DataType::Int64, false),
        ]))
    }

    const PK_INDICES: [usize; 4] = [0, 1, 2, 3];

    /// A cold manifest row whose stats blob (built with the PRODUCTION
    /// serializer) covers the given per-PK-column `[min, max]` ranges —
    /// the clustered shape of real promoted orderline files: a tight `ol_o_id`
    /// band while `w_id`/`d_id`/`number` span their full domains.
    fn cold_file(url: &str, o_id_range: (i64, i64)) -> ColdTierFile {
        let schema = orderline_schema();
        let exact = |lo: i64, hi: i64| ColumnStatistics {
            null_count: datafusion_common::stats::Precision::Exact(0),
            min_value: datafusion_common::stats::Precision::Exact(ScalarValue::Int64(Some(lo))),
            max_value: datafusion_common::stats::Precision::Exact(ScalarValue::Int64(Some(hi))),
            sum_value: datafusion_common::stats::Precision::Absent,
            distinct_count: datafusion_common::stats::Precision::Absent,
            byte_size: datafusion_common::stats::Precision::Absent,
        };
        let stats = Statistics {
            num_rows: datafusion_common::stats::Precision::Exact(1000),
            total_byte_size: datafusion_common::stats::Precision::Absent,
            column_statistics: vec![
                exact(1, 100),                     // ol_w_id: full warehouse domain
                exact(1, 10),                      // ol_d_id: full district domain
                exact(o_id_range.0, o_id_range.1), // ol_o_id: tight band
                exact(1, 15),                      // ol_number: full domain
                exact(-5000, 5000),                // ol_amount (non-PK)
            ],
        };
        let blob = crate::stats::statistics_to_persisted_blob(&stats, &schema)
            .expect("stats blob must serialize");
        ColdTierFile {
            table_id: "t".to_string(),
            file_url: url.to_string(),
            row_count: 1000,
            file_size_bytes: 1_000_000,
            min_sequence: 0,
            max_sequence: 100,
            statistics_blob: blob,
            pk_bloom: None,
        }
    }

    /// Encode composite tombstone keys with the production `RowConverter`
    /// exactly as the durable deletion vectors store them.
    fn encode_keys(keys: &[(i64, i64, i64, i64)]) -> (RowConverter, Vec<Box<[u8]>>) {
        let converter = RowConverter::new(vec![
            SortField::new(DataType::Int64),
            SortField::new(DataType::Int64),
            SortField::new(DataType::Int64),
            SortField::new(DataType::Int64),
        ])
        .expect("converter for 4 int64 columns");
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(
                keys.iter().map(|k| k.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                keys.iter().map(|k| k.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                keys.iter().map(|k| k.2).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                keys.iter().map(|k| k.3).collect::<Vec<_>>(),
            )),
        ];
        let rows = converter
            .convert_columns(&columns)
            .expect("encode composite keys");
        let bytes: Vec<Box<[u8]>> = (0..keys.len())
            .map(|i| rows.row(i).data().to_vec().into_boxed_slice())
            .collect();
        (converter, bytes)
    }

    #[test]
    fn composite_key_encode_decode_roundtrip() {
        let keys = [(7, 3, 1500, 1), (100, 10, 1, 15)];
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        assert_eq!(decoded.len(), 2);
        assert_eq!(
            decoded[0],
            vec![
                ScalarValue::Int64(Some(7)),
                ScalarValue::Int64(Some(3)),
                ScalarValue::Int64(Some(1500)),
                ScalarValue::Int64(Some(1)),
            ]
        );
        assert_eq!(
            decoded[1],
            vec![
                ScalarValue::Int64(Some(100)),
                ScalarValue::Int64(Some(10)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(15)),
            ]
        );
    }

    /// Pair decoded keys with one delete sequence each, as the promotion does.
    fn with_sequences(
        keys: Vec<Vec<ScalarValue>>,
        sequences: &[i64],
    ) -> Vec<(Vec<ScalarValue>, i64)> {
        assert_eq!(keys.len(), sequences.len(), "one sequence per key");
        keys.into_iter().zip(sequences.iter().copied()).collect()
    }

    /// Every file in these tests holds rows up to sequence 100.
    const NEWER: i64 = 200;

    /// The lowest delete sequence among the tombstones
    /// [`cold_relevant_tombstones`] keeps, or `None` when it keeps none: the
    /// retention cutoff a promotion derives from the mask.
    fn lowest_cold_relevant_tombstone_sequence(
        cold_files: &[ColdTierFile],
        tombstones: &[(Vec<ScalarValue>, i64)],
        bloom_probes: Option<&[Box<[u8]>]>,
        schema: &Arc<Schema>,
        pk_indices: &[usize],
    ) -> Option<i64> {
        cold_relevant_tombstones(cold_files, tombstones, bloom_probes, schema, pk_indices)
            .iter()
            .zip(tombstones)
            .filter(|(relevant, _)| **relevant)
            .map(|(_, (_, sequence))| *sequence)
            .min()
    }

    #[test]
    fn only_a_tombstone_inside_a_files_band_keeps_it() {
        // Three cold generations of orderline data, banded by ol_o_id (the
        // realistic clustering outcome for this key shape).
        let files = vec![
            cold_file("f1", (1, 1000)),
            cold_file("f2", (1001, 2000)),
            cold_file("f3", (2001, 3000)),
        ];
        // An o_id beyond every band is newer than every file but can hide
        // nothing; the delivery-like tombstone in the middle band can.
        let keys = [(7, 3, 99_999, 1), (12, 9, 1520, 4)];
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        let tombstones = with_sequences(decoded, &[NEWER, NEWER + 1]);
        assert_eq!(
            lowest_cold_relevant_tombstone_sequence(
                &files,
                &tombstones,
                None,
                &orderline_schema(),
                &PK_INDICES,
            ),
            Some(NEWER + 1),
            "the out-of-band tombstone is older but hides nothing; the in-band one decides"
        );
        assert_eq!(
            cold_relevant_tombstones(&files, &tombstones, None, &orderline_schema(), &PK_INDICES),
            vec![false, true],
            "only the in-band tombstone is kept, though the out-of-band one is older"
        );
    }

    #[test]
    fn a_tombstone_outside_every_pk_dimension_keeps_nothing() {
        let files = vec![cold_file("f1", (1, 1000)), cold_file("f2", (1001, 2000))];
        let keys = [(7, 3, 99_999, 1)];
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        assert_eq!(
            lowest_cold_relevant_tombstone_sequence(
                &files,
                &with_sequences(decoded, &[NEWER]),
                None,
                &orderline_schema(),
                &PK_INDICES,
            ),
            None
        );
    }

    #[test]
    fn a_tombstone_no_newer_than_the_file_never_applies_to_it() {
        // The file holds rows up to sequence 100: a delete at or below that was
        // already applied when the file was written, so it can hide nothing
        // there however well its key matches.
        let files = vec![cold_file("f1", (1, 1000))];
        let keys = [(7, 3, 500, 1), (8, 3, 600, 1)];
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        assert_eq!(
            lowest_cold_relevant_tombstone_sequence(
                &files,
                &with_sequences(decoded.clone(), &[99, 100]),
                None,
                &orderline_schema(),
                &PK_INDICES,
            ),
            None,
            "a delete at or below the file's max sequence is already in the file"
        );
        assert_eq!(
            lowest_cold_relevant_tombstone_sequence(
                &files,
                &with_sequences(decoded, &[100, 101]),
                None,
                &orderline_schema(),
                &PK_INDICES,
            ),
            Some(101),
            "the first delete above the file's max sequence applies to it"
        );
    }

    #[test]
    fn the_lowest_relevant_sequence_is_returned_whatever_the_input_order() {
        let files = vec![cold_file("f1", (1, 1000))];
        let keys = [(1, 1, 10, 1), (2, 2, 20, 2), (3, 3, 30, 3)];
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        assert_eq!(
            lowest_cold_relevant_tombstone_sequence(
                &files,
                &with_sequences(decoded, &[NEWER + 9, NEWER + 3, NEWER + 6]),
                None,
                &orderline_schema(),
                &PK_INDICES,
            ),
            Some(NEWER + 3)
        );
    }

    #[test]
    fn missing_stats_blob_conservatively_keeps_the_tombstone() {
        let mut file = cold_file("f1", (1, 1000));
        file.statistics_blob = Vec::new(); // undecodable
        let keys = [(7, 3, 99_999, 1)]; // outside the band the stats would give
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        assert_eq!(
            lowest_cold_relevant_tombstone_sequence(
                &[file],
                &with_sequences(decoded, &[NEWER]),
                None,
                &orderline_schema(),
                &PK_INDICES,
            ),
            Some(NEWER),
            "no stats -> cannot rule the key out -> keep the tombstone"
        );
    }

    /// Serialize a per-file PK bloom over `keys`, exactly as promotion inserts
    /// them (converter row bytes).
    fn bloom_over(keys: &[(i64, i64, i64, i64)]) -> Vec<u8> {
        let (_, bytes) = encode_keys(keys);
        let mut bloom = PkBloom::with_byte_budget(1024);
        for b in &bytes {
            bloom.insert(b);
        }
        bloom.to_bytes()
    }

    #[test]
    fn bloom_refinement_drops_in_band_but_absent_keys() {
        // Two files with IDENTICAL o_id bands — the clustering-degraded case
        // where rectangles alone cannot tell them apart. Neither bloom holds the
        // tombstoned key.
        let mut f1 = cold_file("f1", (1, 1000));
        f1.pk_bloom = Some(bloom_over(&[(1, 1, 500, 1)]));
        let mut f2 = cold_file("f2", (1, 1000));
        f2.pk_bloom = Some(bloom_over(&[(2, 2, 600, 1)]));
        let files = vec![f1, f2];

        let (converter, bytes) = encode_keys(&[(9, 9, 900, 9)]);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        let tombstones = with_sequences(decoded, &[NEWER]);

        assert_eq!(
            lowest_cold_relevant_tombstone_sequence(
                &files,
                &tombstones,
                None,
                &orderline_schema(),
                &PK_INDICES,
            ),
            Some(NEWER),
            "rectangles alone cannot rule the key out"
        );
        // Composite DV bytes ARE the bloom probe encoding — passed through.
        assert_eq!(
            lowest_cold_relevant_tombstone_sequence(
                &files,
                &tombstones,
                Some(&bytes),
                &orderline_schema(),
                &PK_INDICES,
            ),
            None,
            "both blooms miss the key, so it can hide nothing"
        );
    }

    #[test]
    fn bloom_hit_keeps_the_tombstone() {
        let host_keys = [(1, 1, 500, 1)];
        let mut file = cold_file("f1", (1, 1000));
        file.pk_bloom = Some(bloom_over(&host_keys));
        let (converter, bytes) = encode_keys(&host_keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        assert_eq!(
            lowest_cold_relevant_tombstone_sequence(
                &[file],
                &with_sequences(decoded, &[NEWER]),
                Some(&bytes),
                &orderline_schema(),
                &PK_INDICES,
            ),
            Some(NEWER),
            "a bloom never misses a key the file holds"
        );
    }

    #[test]
    fn bloomless_file_falls_back_to_rectangle_with_probes_present() {
        let keys = [(1, 1, 500, 1)];
        let file = cold_file("f1", (1, 1000)); // pk_bloom: None
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        assert_eq!(
            lowest_cold_relevant_tombstone_sequence(
                &[file],
                &with_sequences(decoded, &[NEWER]),
                Some(&bytes),
                &orderline_schema(),
                &PK_INDICES,
            ),
            Some(NEWER),
            "no bloom -> an in-band tombstone is kept"
        );
    }

    #[test]
    fn missing_stats_with_bloom_decides_by_bloom_alone() {
        let host_keys = [(1, 1, 500, 1)];
        let mut file = cold_file("f1", (1, 1000));
        file.statistics_blob = Vec::new(); // no rectangle
        file.pk_bloom = Some(bloom_over(&host_keys));
        let files = [file];

        let (converter, absent_bytes) = encode_keys(&[(9, 9, 900, 9)]);
        let decoded = decode_tombstone_keys(&converter, &absent_bytes).expect("decode");
        assert_eq!(
            lowest_cold_relevant_tombstone_sequence(
                &files,
                &with_sequences(decoded, &[NEWER]),
                Some(&absent_bytes),
                &orderline_schema(),
                &PK_INDICES,
            ),
            None,
            "a bloom miss rules the key out even without stats"
        );

        let (converter, host_bytes) = encode_keys(&host_keys);
        let decoded = decode_tombstone_keys(&converter, &host_bytes).expect("decode");
        assert_eq!(
            lowest_cold_relevant_tombstone_sequence(
                &files,
                &with_sequences(decoded, &[NEWER]),
                Some(&host_bytes),
                &orderline_schema(),
                &PK_INDICES,
            ),
            Some(NEWER),
            "a bloom hit keeps the tombstone"
        );
    }

    #[test]
    fn misaligned_probes_degrade_to_rectangle_only() {
        let keys = [(1, 1, 500, 1), (2, 2, 600, 1)];
        let mut file = cold_file("f1", (1, 1000));
        // Bloom over NEITHER key: aligned probes would rule both keys out.
        file.pk_bloom = Some(bloom_over(&[(3, 3, 700, 3)]));
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        assert_eq!(
            lowest_cold_relevant_tombstone_sequence(
                &[file],
                &with_sequences(decoded, &[NEWER, NEWER + 1]),
                Some(&bytes[..1]), // wrong length: cannot be trusted
                &orderline_schema(),
                &PK_INDICES,
            ),
            Some(NEWER),
            "misaligned probes must not be used; the rectangle keeps the tombstone"
        );
    }

    #[test]
    fn int64_probe_encoding_matches_bloom_insertion() {
        // The Int64 fast path's DV bytes (raw BE) differ from the bloom's
        // converter-row encoding; `encode_int64_bloom_probes` bridges them.
        let converter =
            RowConverter::new(vec![SortField::new(DataType::Int64)]).expect("int64 converter");
        let column: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20]));
        let rows = converter
            .convert_columns(&[column])
            .expect("encode bloom keys");
        let mut bloom = PkBloom::with_byte_budget(1024);
        bloom.insert(rows.row(0).data());
        bloom.insert(rows.row(1).data());

        let dv_bytes: Vec<Box<[u8]>> = vec![
            10_i64.to_be_bytes().to_vec().into_boxed_slice(),
            99_i64.to_be_bytes().to_vec().into_boxed_slice(),
        ];
        let decoded = decode_int64_tombstone_keys(&dv_bytes).expect("decode");
        let probes = encode_int64_bloom_probes(&converter, &decoded).expect("re-encode probes");
        assert!(
            bloom.maybe_contains(&probes[0]),
            "inserted key must bloom-hit through the re-encode"
        );
        assert!(
            !bloom.maybe_contains(&probes[1]),
            "absent key must bloom-miss through the re-encode"
        );
    }
}

#[cfg(test)]
mod scan_file_selection_tests {
    use super::*;

    /// A manifest row carrying only what the selection reads (its identity);
    /// statistics/bloom are irrelevant to which files are selected.
    fn cold_file(url: &str) -> ColdTierFile {
        ColdTierFile {
            table_id: "t".to_string(),
            file_url: url.to_string(),
            row_count: 1000,
            file_size_bytes: 1_000_000,
            min_sequence: 0,
            max_sequence: 100,
            statistics_blob: Vec::new(),
            pk_bloom: None,
        }
    }

    fn urls(files: &[&ColdTierFile]) -> Vec<String> {
        files.iter().map(|f| f.file_url.clone()).collect()
    }

    #[test]
    fn a_user_query_reads_the_whole_captured_manifest() {
        let captured = vec![cold_file("s3://c/a.vortex"), cold_file("s3://c/b.vortex")];
        assert_eq!(
            urls(&cold_files_for_scan(&captured, None)),
            vec!["s3://c/a.vortex", "s3://c/b.vortex"],
            "a session without the extension must see every captured file"
        );
    }

    #[test]
    fn a_promotion_reads_only_its_classified_files() {
        // `b` is clean — carried forward by manifest reference, so it must stay
        // out of the rewrite stream even though the capture lists it. Guards
        // against "fixing" the intersection by ignoring the extension: that
        // would re-read every clean file and double-write its rows.
        let captured = vec![
            cold_file("s3://c/a.vortex"),
            cold_file("s3://c/b.vortex"),
            cold_file("s3://c/c.vortex"),
        ];
        let dirty = ColdScanFiles(Arc::new(vec![
            cold_file("s3://c/a.vortex"),
            cold_file("s3://c/c.vortex"),
        ]));
        assert_eq!(
            urls(&cold_files_for_scan(&captured, Some(&dirty))),
            vec!["s3://c/a.vortex", "s3://c/c.vortex"],
            "the rewrite must read the dirty files and nothing else"
        );
    }

    /// Regression test for #12708.
    ///
    /// The promotion classified `b` dirty and its commit will retire it. The
    /// scan's own manifest capture no longer lists it — the two reads are
    /// independent, with no lock held across them. Selecting by URL out of the
    /// capture intersects the two sets and drops `b` from the rewrite, so its
    /// live rows are carried forward by neither reference nor rewrite: silent
    /// row loss. The classified rows must win.
    #[test]
    fn a_classified_file_missing_from_the_capture_is_still_rewritten() {
        let captured = vec![cold_file("s3://c/a.vortex")];
        let dirty = ColdScanFiles(Arc::new(vec![
            cold_file("s3://c/a.vortex"),
            cold_file("s3://c/b.vortex"),
        ]));
        let selected = cold_files_for_scan(&captured, Some(&dirty));
        assert_eq!(
            urls(&selected),
            vec!["s3://c/a.vortex", "s3://c/b.vortex"],
            "a dirty file absent from the capture must still be rewritten, not dropped"
        );
    }

    #[test]
    fn a_promotion_with_no_dirty_files_reads_nothing() {
        // A promotion supplies an empty set: it reads the warm tier alone and
        // carries every cold file forward, so the capture must not leak a cold
        // file back into its read.
        let captured = vec![cold_file("s3://c/a.vortex")];
        let dirty = ColdScanFiles(Arc::new(Vec::new()));
        assert!(
            cold_files_for_scan(&captured, Some(&dirty)).is_empty(),
            "an empty dirty set must read no cold files"
        );
    }

    #[test]
    fn an_empty_capture_without_the_extension_reads_nothing() {
        assert!(
            cold_files_for_scan(&[], None).is_empty(),
            "an empty cold manifest must select no files"
        );
    }
}
