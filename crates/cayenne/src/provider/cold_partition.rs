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
//! Detection is CONSERVATIVE in the safe direction: a false positive costs an
//! extra rewrite; a missed tombstone is impossible. Any file whose statistics
//! are absent/undecodable — or any key that cannot be compared — classifies as
//! dirty. This conservatism is what preserves the promotion commit's invariant
//! that the deletion index fully drains: every tombstone's potential host file
//! is in the rewrite set, so every tombstone is physically applied.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::Schema;
use datafusion_common::ScalarValue;
use datafusion_common::stats::Precision;

use crate::metadata::ColdTierFile;
use crate::row_converter::{Row, RowConverter};
use crate::stats::statistics_from_persisted_blob;

/// `SessionConfig` extension restricting the cold scan branch to a file
/// subset. Attached ONLY by the promotion's private session so the
/// carry-forward rewrite reads the dirty files and nothing else; user-query
/// sessions never carry it, so queries always see the full manifest.
#[derive(Debug)]
pub(crate) struct ColdScanFileSubset(pub HashSet<String>);

/// The cold manifest split for one promotion pass.
pub(crate) struct ColdFilePartition {
    /// May contain a tombstoned key — must be re-read and rewritten.
    pub dirty: Vec<ColdTierFile>,
    /// Provably untouched by every tombstone — carried forward by manifest
    /// reference.
    pub clean: Vec<ColdTierFile>,
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
/// Returns an error when a key is shorter than 8 bytes — callers treat that
/// as "cannot classify".
pub(crate) fn decode_int64_tombstone_keys(
    keys: &[Box<[u8]>],
) -> Result<Vec<Vec<ScalarValue>>, arrow_schema::ArrowError> {
    keys.iter()
        .map(|bytes| {
            if bytes.len() >= 8 {
                let mut arr = [0_u8; 8];
                arr.copy_from_slice(&bytes[..8]);
                Ok(vec![ScalarValue::Int64(Some(i64::from_be_bytes(arr)))])
            } else {
                Err(arrow_schema::ArrowError::InvalidArgumentError(format!(
                    "Int64 tombstone key has {} bytes, expected at least 8",
                    bytes.len()
                )))
            }
        })
        .collect()
}

/// Split `cold_files` into dirty/clean against the tombstoned keys.
///
/// `tombstone_keys` is row-major (`decode_tombstone_keys` output); an empty
/// set classifies everything clean. `pk_indices` are the PK columns' indices
/// in `schema`, in primary-key order (matching each key tuple's order).
pub(crate) fn partition_cold_files(
    cold_files: Vec<ColdTierFile>,
    tombstone_keys: &[Vec<ScalarValue>],
    schema: &Arc<Schema>,
    pk_indices: &[usize],
) -> ColdFilePartition {
    if tombstone_keys.is_empty() {
        return ColdFilePartition {
            dirty: Vec::new(),
            clean: cold_files,
        };
    }

    let mut dirty = Vec::new();
    let mut clean = Vec::new();
    for file in cold_files {
        let bounds = pk_bounds_from_manifest(&file, schema, pk_indices);
        let is_dirty = match &bounds {
            // No usable statistics → cannot prove clean.
            None => true,
            Some(bounds) => tombstone_keys
                .iter()
                .any(|key| key_within_bounds(key, bounds)),
        };
        if is_dirty {
            dirty.push(file);
        } else {
            clean.push(file);
        }
    }
    ColdFilePartition { dirty, clean }
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
        // Short key → cannot classify.
        let bad: Vec<Box<[u8]>> = vec![vec![1, 2].into_boxed_slice()];
        decode_int64_tombstone_keys(&bad).expect_err("short key must fail to decode");
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

        let partition = partition_cold_files(files, &decoded, &orderline_schema(), &PK_INDICES);
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
    }

    #[test]
    fn tombstone_outside_every_pk_dimension_leaves_all_clean() {
        let files = vec![cold_file("f1", (1, 1000)), cold_file("f2", (1001, 2000))];
        // o_id beyond every band: nothing is dirty.
        let keys = [(7, 3, 99_999, 1)];
        let (converter, bytes) = encode_keys(&keys);
        let decoded = decode_tombstone_keys(&converter, &bytes).expect("decode");
        let partition = partition_cold_files(files, &decoded, &orderline_schema(), &PK_INDICES);
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
            partition_cold_files(vec![file], &decoded, &orderline_schema(), &PK_INDICES);
        assert_eq!(
            partition.dirty.len(),
            1,
            "no stats -> cannot prove clean -> dirty"
        );
    }
}
