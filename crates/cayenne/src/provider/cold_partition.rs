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

//! Dirty/clean partitioning of datalake (cold) files for carry-forward
//! promotion.
//!
//! Incremental promotion rewrites only the cold files a tombstone may touch
//! (**dirty**) and carries every other file (**clean**) forward by manifest
//! reference — never re-reading it from the object store. Classification runs
//! entirely from in-memory manifest data: each cold file's persisted
//! statistics blob yields a per-PK-column min/max hyper-rectangle, and a file
//! is dirty iff any tombstoned key falls inside its rectangle. Cold files are
//! Z-order clustered on the PK by default, so rectangles are tight and a
//! tombstone hits few files.
//!
//! When a file carries a per-file PK bloom (`ColdTierFile::pk_bloom`), a
//! tombstone must ALSO bloom-hit to dirty the file. Both tests individually
//! never miss a hosted key (true bounds contain it; blooms have no false
//! negatives for inserted keys), so their intersection doesn't either — while
//! the bloom rejects ~99% of in-band-but-absent keys, exactly the precision
//! rectangles lose as carried files' PK ranges drift across promotions. The
//! rectangle stays first: bloom false positives compound with tombstone count
//! ((1-p)^T per file), so the bloom is only probed for the few tombstones the
//! rectangle cannot exclude.
//!
//! Detection is CONSERVATIVE in the safe direction: a false positive costs an
//! extra rewrite; a missed tombstone is impossible. Any file whose statistics
//! are absent/undecodable — or any key that cannot be compared — classifies as
//! dirty (bloom-only when a bloom is present). This conservatism is what
//! preserves the promotion commit's invariant that the deletion index fully
//! drains: every tombstone's potential host file is in the rewrite set, so
//! every tombstone is physically applied.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow_schema::Schema;
use datafusion_common::ScalarValue;
use datafusion_common::stats::Precision;

use super::pk_index::PkBloom;
use crate::metadata::ColdTierFile;
use crate::row_converter::{Row, RowConverter};
use crate::stats::statistics_from_persisted_blob;

/// `SessionConfig` extension pinning the cold scan branch to an explicit file
/// set. Attached ONLY by the promotion's private session so the carry-forward
/// rewrite reads the dirty files and nothing else.
///
/// It carries the manifest ROWS the promotion classified, not just their URLs,
/// so the rewrite stream is built from the same listing the classification ran
/// against. Selecting a subset out of whatever listing the scan happened to
/// capture would let a stale capture silently drop a dirty file from the
/// rewrite while the commit still retires it from the manifest.
///
/// User-query sessions never carry it; they read the manifest captured under
/// the listing fence with the rest of the scan's visible state.
#[derive(Debug)]
pub(crate) struct ColdScanFiles(pub Arc<Vec<ColdTierFile>>);

/// The cold manifest split for one promotion pass.
pub(crate) struct ColdFilePartition {
    /// May contain a tombstoned key — must be re-read and rewritten.
    pub dirty: Vec<ColdTierFile>,
    /// Provably untouched by every tombstone — carried forward by manifest
    /// reference.
    pub clean: Vec<ColdTierFile>,
    /// Tombstoned keys the split was computed against (observability only).
    pub tombstones: usize,
    /// Clean files excluded by their min/max rectangle alone (no tombstone
    /// inside the bounds).
    pub cleared_by_min_max: usize,
    /// Clean files a tombstone's rectangle contained but every bloom probe
    /// rejected — rewrites avoided purely by the bloom refinement.
    pub cleared_by_bloom: usize,
}

/// Decode `RowConverter`-encoded tombstone keys into row-major
/// `ScalarValue` tuples (one inner `Vec` per key, one value per PK column).
///
/// # Errors
///
/// Returns an error when the bytes are malformed for this converter's schema
/// — callers treat that as "cannot classify" and fall back to all-dirty.
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
/// as "cannot classify". Truncating a longer buffer would decode a wrong
/// value and could misclassify a truly-dirty file as clean.
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
/// classifies by rectangle only (the safe default, not an error).
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

/// A cold file's PK bloom, parsed on first probe only — clean-by-rectangle
/// files never pay the parse. A missing/corrupt bloom probes as "MAY contain"
/// (conservative), degrading that file to rectangle-only classification.
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

/// Split `cold_files` into dirty/clean against the tombstoned keys.
///
/// `tombstone_keys` is row-major (`decode_tombstone_keys` output); an empty
/// set classifies everything clean. `pk_indices` are the PK columns' indices
/// in `schema`, in primary-key order (matching each key tuple's order).
///
/// `bloom_probes` optionally refines the rectangle test: the same tombstones
/// in the byte encoding the per-file PK blooms insert (index-aligned with
/// `tombstone_keys` — composite DV keys are already that encoding; Int64 keys
/// go through [`encode_int64_bloom_probes`]). A file with a usable bloom is
/// dirty only if a rectangle-contained tombstone ALSO bloom-hits. `None`, a
/// length mismatch, or a bloom-less file falls back to rectangle-only.
pub(crate) fn partition_cold_files(
    cold_files: Vec<ColdTierFile>,
    tombstone_keys: &[Vec<ScalarValue>],
    bloom_probes: Option<&[Box<[u8]>]>,
    schema: &Arc<Schema>,
    pk_indices: &[usize],
) -> ColdFilePartition {
    if tombstone_keys.is_empty() {
        return ColdFilePartition {
            dirty: Vec::new(),
            clean: cold_files,
            tombstones: 0,
            cleared_by_min_max: 0,
            cleared_by_bloom: 0,
        };
    }
    // A misaligned probe set can't be trusted — degrade to rectangle-only
    // rather than probing the wrong bytes.
    let probes = bloom_probes.filter(|p| p.len() == tombstone_keys.len());

    let mut dirty = Vec::new();
    let mut clean = Vec::new();
    let mut cleared_by_min_max = 0usize;
    let mut cleared_by_bloom = 0usize;
    for file in cold_files {
        let bounds = pk_bounds_from_manifest(&file, schema, pk_indices);
        let mut bloom = LazyBloom::Unparsed(probes.and(file.pk_bloom.as_deref()));
        // Which test did the excluding: `rect_hit` records whether any
        // tombstone survived the rectangle, so a clean verdict is attributable
        // to the rectangle (`!rect_hit`) or to the bloom refinement.
        let mut rect_hit = false;
        let is_dirty = match (&bounds, probes) {
            (Some(bounds), Some(probes)) => {
                tombstone_keys
                    .iter()
                    .zip(probes.iter())
                    .any(|(key, probe)| {
                        let contained = key_within_bounds(key, bounds);
                        rect_hit |= contained;
                        contained && bloom.maybe_contains(probe)
                    })
            }
            (Some(bounds), None) => tombstone_keys
                .iter()
                .any(|key| key_within_bounds(key, bounds)),
            // No usable statistics → cannot prove clean by bounds; a bloom
            // (when present) can still clear the file on its own.
            (None, Some(probes)) => {
                rect_hit = true; // no rectangle to credit
                probes.iter().any(|probe| bloom.maybe_contains(probe))
            }
            (None, None) => true,
        };
        if is_dirty {
            dirty.push(file);
        } else {
            if rect_hit {
                cleared_by_bloom += 1;
            } else {
                cleared_by_min_max += 1;
            }
            clean.push(file);
        }
    }
    ColdFilePartition {
        dirty,
        clean,
        tombstones: tombstone_keys.len(),
        cleared_by_min_max,
        cleared_by_bloom,
    }
}

/// Extract the per-PK-column `[min, max]` bounds from a cold file's persisted
/// statistics blob. `None` when the blob is missing/undecodable or any PK
/// column lacks concrete, non-null bounds (→ conservative dirty).
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

/// Whether a key tuple falls inside every per-column bound (point-in-
/// hyper-rectangle). Incomparable values (type mismatch, NULL) count as
/// "inside" — conservative dirty.
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
    /// the Z-order shape of real promoted orderline files: a tight `ol_o_id`
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

    #[test]
    fn orderline_delivery_tombstones_dirty_only_their_o_id_band() {
        // Three cold generations of orderline data, banded by ol_o_id (the
        // realistic Z-order outcome for this key shape).
        let files = vec![
            cold_file("f1", (1, 1000)),
            cold_file("f2", (1001, 2000)),
            cold_file("f3", (2001, 3000)),
        ];
        // Delivery-like updates: tombstones for two orders around o_id 1500,
        // any warehouse/district — should hit ONLY the middle band.
        let keys = [(7, 3, 1500, 1), (12, 9, 1520, 4)];
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");

        let partition =
            partition_cold_files(files, &decoded, None, &orderline_schema(), &PK_INDICES);
        let dirty: Vec<&str> = partition
            .dirty
            .iter()
            .map(|f| f.file_url.as_str())
            .collect();
        let clean: Vec<&str> = partition
            .clean
            .iter()
            .map(|f| f.file_url.as_str())
            .collect();
        assert_eq!(
            dirty,
            vec!["f2"],
            "only the o_id band containing the tombstones is dirty"
        );
        assert_eq!(clean, vec!["f1", "f3"], "sibling bands are provably clean");
        assert_eq!(partition.tombstones, 2);
        assert_eq!(
            partition.cleared_by_min_max, 2,
            "both clean files were excluded by rectangle alone"
        );
        assert_eq!(partition.cleared_by_bloom, 0);
    }

    #[test]
    fn tombstone_outside_every_pk_dimension_leaves_all_clean() {
        let files = vec![cold_file("f1", (1, 1000)), cold_file("f2", (1001, 2000))];
        // o_id beyond every band: nothing is dirty.
        let keys = [(7, 3, 99_999, 1)];
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        let partition =
            partition_cold_files(files, &decoded, None, &orderline_schema(), &PK_INDICES);
        assert!(partition.dirty.is_empty());
        assert_eq!(partition.clean.len(), 2);
    }

    #[test]
    fn missing_stats_blob_is_conservatively_dirty() {
        let mut file = cold_file("f1", (1, 1000));
        file.statistics_blob = Vec::new(); // undecodable
        let keys = [(7, 3, 99_999, 1)]; // would be clean if stats existed
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        let partition =
            partition_cold_files(vec![file], &decoded, None, &orderline_schema(), &PK_INDICES);
        assert_eq!(
            partition.dirty.len(),
            1,
            "no stats -> cannot prove clean -> dirty"
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
    fn bloom_refinement_clears_in_band_but_absent_keys() {
        // Two files with IDENTICAL o_id bands — the clustering-degraded case
        // where rectangles alone cannot tell them apart.
        let host_keys = [(1, 1, 500, 1)];
        let other_keys = [(2, 2, 600, 1)];
        let mut f1 = cold_file("f1", (1, 1000));
        f1.pk_bloom = Some(bloom_over(&host_keys));
        let mut f2 = cold_file("f2", (1, 1000));
        f2.pk_bloom = Some(bloom_over(&other_keys));

        let (converter, bytes) = encode_keys(&host_keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");

        // Rectangle-only: both files dirty (tombstone is in both bands).
        let rect_only = partition_cold_files(
            vec![f1.clone(), f2.clone()],
            &decoded,
            None,
            &orderline_schema(),
            &PK_INDICES,
        );
        assert_eq!(rect_only.dirty.len(), 2, "rectangles alone cannot exclude");

        // Bloom refinement: only the actual host file is dirty. Composite DV
        // bytes ARE the bloom probe encoding — passed through verbatim.
        let refined = partition_cold_files(
            vec![f1, f2],
            &decoded,
            Some(&bytes),
            &orderline_schema(),
            &PK_INDICES,
        );
        let dirty: Vec<&str> = refined.dirty.iter().map(|f| f.file_url.as_str()).collect();
        let clean: Vec<&str> = refined.clean.iter().map(|f| f.file_url.as_str()).collect();
        assert_eq!(dirty, vec!["f1"], "only the bloom-hit host file is dirty");
        assert_eq!(clean, vec!["f2"], "in-band but bloom-missed file is clean");
        assert_eq!(
            refined.cleared_by_bloom, 1,
            "the clean verdict is attributed to the bloom, not the rectangle"
        );
        assert_eq!(refined.cleared_by_min_max, 0);
    }

    #[test]
    fn bloomless_file_falls_back_to_rectangle_with_probes_present() {
        let keys = [(1, 1, 500, 1)];
        let file = cold_file("f1", (1, 1000)); // pk_bloom: None
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        let partition = partition_cold_files(
            vec![file],
            &decoded,
            Some(&bytes),
            &orderline_schema(),
            &PK_INDICES,
        );
        assert_eq!(
            partition.dirty.len(),
            1,
            "no bloom -> in-band tombstone keeps the file dirty"
        );
    }

    #[test]
    fn missing_stats_with_bloom_classifies_by_bloom_alone() {
        let host_keys = [(1, 1, 500, 1)];
        let absent_keys = [(9, 9, 900, 9)];
        let mut file = cold_file("f1", (1, 1000));
        file.statistics_blob = Vec::new(); // no rectangle
        file.pk_bloom = Some(bloom_over(&host_keys));

        let (converter, absent_bytes) = encode_keys(&absent_keys);
        let decoded = decode_tombstone_keys(&converter, &absent_bytes).expect("decode");
        let partition = partition_cold_files(
            vec![file.clone()],
            &decoded,
            Some(&absent_bytes),
            &orderline_schema(),
            &PK_INDICES,
        );
        assert!(
            partition.dirty.is_empty(),
            "bloom miss proves clean even without stats"
        );

        let (converter, host_bytes) = encode_keys(&host_keys);
        let decoded = decode_tombstone_keys(&converter, &host_bytes).expect("decode");
        let partition = partition_cold_files(
            vec![file],
            &decoded,
            Some(&host_bytes),
            &orderline_schema(),
            &PK_INDICES,
        );
        assert_eq!(partition.dirty.len(), 1, "bloom hit keeps the file dirty");
    }

    #[test]
    fn misaligned_probes_degrade_to_rectangle_only() {
        let keys = [(1, 1, 500, 1), (2, 2, 600, 1)];
        let mut file = cold_file("f1", (1, 1000));
        // Bloom over NEITHER key: aligned probes would prove the file clean.
        file.pk_bloom = Some(bloom_over(&[(3, 3, 700, 3)]));
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        let partition = partition_cold_files(
            vec![file],
            &decoded,
            Some(&bytes[..1]), // wrong length: cannot be trusted
            &orderline_schema(),
            &PK_INDICES,
        );
        assert_eq!(
            partition.dirty.len(),
            1,
            "misaligned probes must not be used; rectangle keeps the file dirty"
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
