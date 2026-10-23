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

//! Query-time pruning of datalake (cold) files by their per-file primary-key
//! bloom filter.
//!
//! Footer min/max statistics cannot skip a file whose primary-key range was
//! widened by clustering on other columns, and every surviving file costs
//! object-store round trips. A point lookup that pins every primary-key column
//! to literals can instead probe each file's bloom (persisted in the cold
//! manifest at promotion): a bloom has no false negatives for the keys it was
//! built from, so a file whose bloom rejects every candidate key holds no
//! matching row.
//!
//! Soundness rests on three rules:
//! - candidate keys are encoded with the same row converter, over the same
//!   column types, that built the blooms;
//! - a literal is used only when it converts to the column type exactly
//!   (identical type, integer to integer or string to string with an exact
//!   round trip); floating-point keys are never probed, since distinct bit
//!   patterns compare equal;
//! - any file without a readable bloom, and any predicate shape that does not
//!   pin every key column to non-NULL literals, is kept.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Schema};
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use parking_lot::Mutex;

use super::lookup_index::key_tuples;
use super::pk_index::PkBloom;
use super::table::bare_column_values_for;
use crate::metadata::ColdTierFile;
use crate::row_converter::{RowConverter, SortField};

/// Bytes of a serialized bloom compared, with its length, to reject changed
/// framing. The immutable, UUID-named cold file URL identifies its key set;
/// this prefix is not a fingerprint of the bloom's contents.
const BLOOM_IDENTITY_PREFIX_BYTES: usize = 48;

struct CachedBloom {
    len: usize,
    prefix: Vec<u8>,
    bloom: Option<Arc<PkBloom>>,
}

/// Parsed per-file blooms, keyed by cold file URL, so a query pays the parse
/// once per file rather than once per scan.
#[derive(Default)]
pub(crate) struct ColdPkBloomCache {
    entries: Mutex<HashMap<String, CachedBloom>>,
}

impl ColdPkBloomCache {
    /// The parsed bloom of `file`; `None` when it has none or it is unreadable
    /// by this build.
    fn bloom_for(&self, file: &ColdTierFile) -> Option<Arc<PkBloom>> {
        let blob = file.pk_bloom.as_deref()?;
        let prefix = &blob[..blob.len().min(BLOOM_IDENTITY_PREFIX_BYTES)];
        if let Some(cached) = self.entries.lock().get(&file.file_url)
            && cached.len == blob.len()
            && cached.prefix == prefix
        {
            return cached.bloom.clone();
        }
        let bloom = PkBloom::from_bytes(blob).map(Arc::new);
        self.entries.lock().insert(
            file.file_url.clone(),
            CachedBloom {
                len: blob.len(),
                prefix: prefix.to_vec(),
                bloom: bloom.clone(),
            },
        );
        bloom
    }

    /// Drops entries for files no longer in `files` once the cache has grown well
    /// past the live set.
    pub(crate) fn trim(&self, files: &[&ColdTierFile]) {
        let mut entries = self.entries.lock();
        if entries.len() > files.len().saturating_mul(2).saturating_add(8) {
            let live: HashSet<&str> = files.iter().map(|file| file.file_url.as_str()).collect();
            entries.retain(|url, _| live.contains(url.as_str()));
        }
    }
}

/// The encoded candidate primary keys a scan's `filters` pin, for probing blooms.
pub(crate) struct PkProbe {
    keys: Vec<Box<[u8]>>,
}

impl PkProbe {
    /// The candidate keys `filters` pin every primary-key column to, or `None`
    /// when they do not (or a value cannot be probed exactly).
    pub(crate) fn from_filters(
        filters: &[Expr],
        table_schema: &Schema,
        pk_indices: &[usize],
    ) -> Option<Self> {
        if pk_indices.is_empty() || filters.is_empty() {
            return None;
        }
        let mut names = Vec::with_capacity(pk_indices.len());
        let mut fields = Vec::with_capacity(pk_indices.len());
        let mut values: HashMap<String, Vec<ScalarValue>> = HashMap::new();
        for &index in pk_indices {
            let field = table_schema.fields().get(index)?;
            let data_type = field.data_type();
            if data_type.is_floating() {
                return None;
            }
            let pinned = filters.iter().find_map(|filter| {
                bare_column_values_for(filter, field.name(), Some(data_type.clone()))
            })?;
            let exact = pinned
                .into_iter()
                .map(|value| exact_key_scalar(value, data_type))
                .collect::<Option<Vec<_>>>()?;
            names.push(field.name().clone());
            fields.push(SortField::new(data_type.clone()));
            values.insert(field.name().clone(), exact);
        }
        let tuples = key_tuples(&names, &|column| values.get(column).cloned())?;
        if tuples.is_empty() {
            return None;
        }
        let converter = RowConverter::new(fields).ok()?;
        let columns = (0..names.len())
            .map(|at| ScalarValue::iter_to_array(tuples.iter().map(|tuple| tuple[at].clone())).ok())
            .collect::<Option<Vec<_>>>()?;
        let rows = converter.convert_columns(&columns).ok()?;
        let keys: Vec<Box<[u8]>> = (0..tuples.len())
            .map(|i| Box::from(rows.row(i).as_ref()))
            .collect();
        Some(Self { keys })
    }

    /// `false` only when `file` has a readable bloom that rejects every
    /// candidate key, so no row of it can match.
    pub(crate) fn may_match(&self, cache: &ColdPkBloomCache, file: &ColdTierFile) -> bool {
        cache
            .bloom_for(file)
            .is_none_or(|bloom| self.keys.iter().any(|key| bloom.maybe_contains(key)))
    }
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// `value` as a `to`-typed key, only when the conversion is exact and the value
/// is not NULL (a NULL key matches no row, and is left to the scan).
fn exact_key_scalar(value: ScalarValue, to: &DataType) -> Option<ScalarValue> {
    if value.is_null() {
        return None;
    }
    let from = value.data_type();
    if &from == to {
        return Some(value);
    }
    let convertible = (from.is_integer() && to.is_integer()) || (is_string(&from) && is_string(to));
    if !convertible {
        return None;
    }
    let converted = value.cast_to(to).ok()?;
    let round_trip = converted.cast_to(&from).ok()?;
    (round_trip == value).then_some(converted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_literals_convert_only_when_exact() {
        assert_eq!(
            exact_key_scalar(ScalarValue::Int32(Some(7)), &DataType::Int64),
            Some(ScalarValue::Int64(Some(7)))
        );
        assert_eq!(
            exact_key_scalar(
                ScalarValue::Int64(Some(i64::from(i32::MAX) + 1)),
                &DataType::Int32
            ),
            None
        );
        assert_eq!(
            exact_key_scalar(ScalarValue::Int64(Some(-1)), &DataType::UInt64),
            None
        );
        assert_eq!(
            exact_key_scalar(ScalarValue::Float64(Some(1.5)), &DataType::Int64),
            None
        );
        assert_eq!(
            exact_key_scalar(ScalarValue::Int64(None), &DataType::Int64),
            None
        );
    }

    #[test]
    fn string_literals_convert_between_string_types() {
        assert_eq!(
            exact_key_scalar(ScalarValue::Utf8(Some("a".into())), &DataType::Utf8View),
            Some(ScalarValue::Utf8View(Some("a".into())))
        );
        assert_eq!(
            exact_key_scalar(ScalarValue::Utf8(Some("1".into())), &DataType::Int64),
            None
        );
    }
}
