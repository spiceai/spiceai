/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Helpers for extracting and hashing Pepper primary-key values.
//!
//! Pepper stores key material in the metastore so `on_conflict` checks can be
//! served without scanning the underlying Vortex data. The utilities in this
//! module provide a streaming-friendly way to serialize key columns into a
//! canonical byte representation and produce stable 128-bit hashes.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, SortField};
use arrow_schema::SchemaRef;
use xxhash_rust::xxh3::xxh3_128;

use crate::catalog::{CatalogError, CatalogResult};

/// Serialized primary-key material for a single row.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyMaterial {
    /// Deterministic 128-bit hash of the key bytes (big-endian).
    pub key_hash: [u8; 16],
    /// Canonical byte representation of the key columns.
    pub key_bytes: Vec<u8>,
    /// Row index within the original [`RecordBatch`].
    pub row_index: usize,
}

impl KeyMaterial {
    /// Returns the key hash as a `Vec<u8>` for convenience when binding SQL parameters.
    #[allow(dead_code)]
    #[must_use]
    pub fn hash_as_vec(&self) -> Vec<u8> {
        self.key_hash.to_vec()
    }
}

/// Serializes primary/unique-key columns into canonical row bytes.
#[allow(dead_code)]
#[derive(Debug)]
pub struct KeySerializer {
    converter: RowConverter,
    key_indices: Vec<usize>,
}

#[allow(dead_code)]
impl KeySerializer {
    /// Create a new serializer for the provided schema and key column names.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidOperation`] if no key columns are supplied,
    /// a column name does not exist, or Arrow row encoding fails for the
    /// requested data types.
    pub fn try_new(schema: &SchemaRef, key_columns: &[String]) -> CatalogResult<Self> {
        if key_columns.is_empty() {
            return Err(CatalogError::InvalidOperation {
                message: "Primary key columns must be provided".to_string(),
            });
        }

        let schema_fields = schema.fields();
        let mut key_indices = Vec::with_capacity(key_columns.len());
        let mut sort_fields = Vec::with_capacity(key_columns.len());

        for column in key_columns {
            let index = schema_fields
                .iter()
                .position(|field| field.name() == column)
                .ok_or_else(|| CatalogError::InvalidOperation {
                    message: format!("Key column '{column}' not found in Pepper table schema"),
                })?;

            key_indices.push(index);
            sort_fields.push(SortField::new(schema_fields[index].data_type().clone()));
        }

        let converter =
            RowConverter::new(sort_fields).map_err(|err| CatalogError::InvalidOperation {
                message: format!("Failed to initialize key row converter: {err}"),
            })?;

        Ok(Self {
            converter,
            key_indices,
        })
    }

    /// Serialize the configured key columns for the provided `RecordBatch`.
    ///
    /// The returned collection preserves row ordering so callers can map key
    /// material back to the original rows. This method allocates only the
    /// per-row key bytes and reuses internal buffers across calls.
    pub fn extract(&mut self, batch: &RecordBatch) -> CatalogResult<Vec<KeyMaterial>> {
        let row_count = batch.num_rows();
        if row_count == 0 {
            return Ok(Vec::new());
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.key_indices.len());
        for &index in &self.key_indices {
            columns.push(Arc::clone(batch.column(index)));
        }

        let rows = self.converter.convert_columns(&columns).map_err(|err| {
            CatalogError::InvalidOperation {
                message: format!("Failed to encode key columns: {err}"),
            }
        })?;

        let mut materials = Vec::with_capacity(row_count);
        for (row_index, row) in rows.iter().enumerate() {
            let key_bytes = row.as_ref().to_vec();
            let key_hash = xxh3_128(&key_bytes).to_be_bytes();
            materials.push(KeyMaterial {
                key_hash,
                key_bytes,
                row_index,
            });
        }

        Ok(materials)
    }

    /// Return the schema indices of the key columns.
    #[must_use]
    pub fn key_indices(&self) -> &[usize] {
        &self.key_indices
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn build_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));

        let ids = Arc::new(Int64Array::from(vec![1, 2, 2]));
        let regions = Arc::new(StringArray::from(vec!["us", "eu", "eu"]));

        RecordBatch::try_new(schema, vec![ids, regions]).expect("valid batch")
    }

    #[test]
    fn extracts_key_material() {
        let batch = build_batch();
        let schema = batch.schema();
        let mut serializer =
            KeySerializer::try_new(&schema, &["id".to_string()]).expect("serializer");

        let keys = serializer.extract(&batch).expect("keys");
        assert_eq!(keys.len(), 3);
        assert!(keys[0].key_bytes != keys[1].key_bytes);
        assert_ne!(keys[0].key_hash, keys[1].key_hash);
    }

    #[test]
    fn identical_rows_share_hash() {
        let batch = build_batch();
        let schema = batch.schema();
        let mut serializer =
            KeySerializer::try_new(&schema, &["id".to_string(), "region".to_string()])
                .expect("serializer");

        let keys = serializer.extract(&batch).expect("keys");
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[1].key_hash, keys[2].key_hash);
        assert_eq!(keys[1].key_bytes, keys[2].key_bytes);
        assert_eq!(keys[1].row_index, 1);
        assert_eq!(keys[2].row_index, 2);
    }

    #[test]
    fn missing_column_errors() {
        let schema = build_batch().schema();
        match KeySerializer::try_new(&schema, &["missing".to_string()]) {
            Err(CatalogError::InvalidOperation { message }) => {
                assert!(message.contains("not found"));
            }
            Err(other) => panic!("unexpected error type: {other:?}"),
            Ok(_) => panic!("expected serializer error"),
        }
    }
}
