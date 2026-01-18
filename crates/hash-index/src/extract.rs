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

//! Key extractors for Arrow arrays.
//!
//! This module provides type-specialized key extractors that efficiently
//! extract keys from Arrow arrays and compute hashes for the hash index.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_row::{OwnedRow, RowConverter, SortField};
use twox_hash::XxHash3_64;

use crate::{Error, KeyColumnNotFoundSnafu, Result};
use snafu::OptionExt as _;

/// Fixed seed for deterministic hashing across instances.
/// Using a fixed seed ensures consistent hash values for the same keys.
const HASH_SEED: u64 = 0x5370_6963_6541_4920; // "SpiceAI " in hex

/// Creates a new hasher with a fixed seed for deterministic hashing.
#[inline]
fn new_hasher() -> XxHash3_64 {
    XxHash3_64::with_seed(HASH_SEED)
}

/// A trait for extracting keys from Arrow arrays and computing hashes.
pub trait KeyExtractor: Send + Sync {
    /// The owned key type stored in the index.
    type Key: Clone + Eq + Hash + Send + Sync;

    /// Returns the number of rows in the current batch.
    fn len(&self) -> usize;

    /// Returns true if there are no rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Extracts the key at the given row index.
    ///
    /// Returns `None` if the key is null.
    fn extract_key(&self, row: usize) -> Option<Self::Key>;

    /// Computes the hash for the key at the given row index.
    ///
    /// Returns `None` if the key is null.
    fn hash_key(&self, row: usize) -> Option<u64>;

    /// Computes the hash for an owned key.
    fn hash_owned_key(key: &Self::Key) -> u64;

    /// Returns the raw bytes representation of the key at the given row.
    ///
    /// This is used for key equality comparison when hash collisions occur.
    /// Returns `None` if the key is null.
    fn key_bytes(&self, row: usize) -> Option<Vec<u8>>;
}

/// Key extractor for primitive integer types.
pub struct PrimitiveKeyExtractor<T: PrimitiveKey> {
    array: T::ArrayType,
}

impl<T: PrimitiveKey> PrimitiveKeyExtractor<T> {
    /// Creates a new primitive key extractor from a record batch and column name.
    ///
    /// # Errors
    ///
    /// Returns an error if the column is not found or has an unsupported type.
    pub fn new(batch: &RecordBatch, column: &str) -> Result<Self> {
        let col_idx = batch
            .schema()
            .index_of(column)
            .ok()
            .context(KeyColumnNotFoundSnafu { column })?;

        let array = batch.column(col_idx);
        let typed_array = T::downcast(array)?;

        Ok(Self { array: typed_array })
    }

    /// Creates from a raw array reference.
    ///
    /// # Errors
    ///
    /// Returns an error if the array type is not supported.
    pub fn from_array(array: &ArrayRef) -> Result<Self> {
        let typed_array = T::downcast(array)?;
        Ok(Self { array: typed_array })
    }
}

impl<T: PrimitiveKey> KeyExtractor for PrimitiveKeyExtractor<T> {
    type Key = T::Native;

    #[inline]
    fn len(&self) -> usize {
        T::array_len(&self.array)
    }

    #[inline]
    fn extract_key(&self, row: usize) -> Option<Self::Key> {
        if T::is_null(&self.array, row) {
            None
        } else {
            Some(T::value(&self.array, row))
        }
    }

    #[inline]
    fn hash_key(&self, row: usize) -> Option<u64> {
        if T::is_null(&self.array, row) {
            None
        } else {
            let val = T::value(&self.array, row);
            Some(Self::hash_owned_key(&val))
        }
    }

    #[inline]
    fn hash_owned_key(key: &Self::Key) -> u64 {
        let mut hasher = new_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[inline]
    fn key_bytes(&self, row: usize) -> Option<Vec<u8>> {
        if T::is_null(&self.array, row) {
            None
        } else {
            Some(T::value_bytes(&self.array, row))
        }
    }
}

/// Trait for primitive key types with optimized array access.
pub trait PrimitiveKey: Clone + Eq + Hash + Send + Sync + 'static {
    /// The Arrow array type for this primitive.
    type ArrayType: Clone + Send + Sync;

    /// The native Rust type.
    type Native: Clone + Eq + Hash + Send + Sync;

    /// Downcasts an `ArrayRef` to the specific array type.
    fn downcast(array: &ArrayRef) -> Result<Self::ArrayType>;

    /// Returns the length of the array.
    fn array_len(array: &Self::ArrayType) -> usize;

    /// Returns true if the value at `row` is null.
    fn is_null(array: &Self::ArrayType, row: usize) -> bool;

    /// Returns the value at `row` (assumes non-null).
    fn value(array: &Self::ArrayType, row: usize) -> Self::Native;

    /// Returns the value at `row` as bytes for comparison.
    fn value_bytes(array: &Self::ArrayType, row: usize) -> Vec<u8>;
}

macro_rules! impl_primitive_key {
    ($native:ty, $array_ty:ty, $data_type:pat) => {
        impl PrimitiveKey for $native {
            type ArrayType = $array_ty;
            type Native = $native;

            fn downcast(array: &ArrayRef) -> Result<Self::ArrayType> {
                array
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .cloned()
                    .ok_or_else(|| Error::UnsupportedKeyType {
                        data_type: format!("{:?}", array.data_type()),
                    })
            }

            #[inline]
            fn array_len(array: &Self::ArrayType) -> usize {
                array.len()
            }

            #[inline]
            fn is_null(array: &Self::ArrayType, row: usize) -> bool {
                array.is_null(row)
            }

            #[inline]
            fn value(array: &Self::ArrayType, row: usize) -> Self::Native {
                array.value(row)
            }

            #[inline]
            fn value_bytes(array: &Self::ArrayType, row: usize) -> Vec<u8> {
                array.value(row).to_le_bytes().to_vec()
            }
        }
    };
}

impl_primitive_key!(i8, Int8Array, DataType::Int8);
impl_primitive_key!(i16, Int16Array, DataType::Int16);
impl_primitive_key!(i32, Int32Array, DataType::Int32);
impl_primitive_key!(i64, Int64Array, DataType::Int64);
impl_primitive_key!(u8, UInt8Array, DataType::UInt8);
impl_primitive_key!(u16, UInt16Array, DataType::UInt16);
impl_primitive_key!(u32, UInt32Array, DataType::UInt32);
impl_primitive_key!(u64, UInt64Array, DataType::UInt64);

/// Key extractor for UTF-8 string columns.
pub struct Utf8KeyExtractor {
    array: StringArray,
}

impl Utf8KeyExtractor {
    /// Creates a new UTF-8 key extractor from a record batch and column name.
    ///
    /// # Errors
    ///
    /// Returns an error if the column is not found or has an unsupported type.
    pub fn new(batch: &RecordBatch, column: &str) -> Result<Self> {
        let col_idx = batch
            .schema()
            .index_of(column)
            .ok()
            .context(KeyColumnNotFoundSnafu { column })?;

        let array = batch.column(col_idx);
        Self::from_array(array)
    }

    /// Creates from a raw array reference.
    ///
    /// # Errors
    ///
    /// Returns an error if the array type is not supported.
    pub fn from_array(array: &ArrayRef) -> Result<Self> {
        if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            return Ok(Self { array: arr.clone() });
        }
        if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
            // Convert LargeStringArray to StringArray (may fail for large offsets)
            let values: Vec<Option<&str>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            let string_array = StringArray::from(values);
            return Ok(Self {
                array: string_array,
            });
        }

        Err(Error::UnsupportedKeyType {
            data_type: format!("{:?}", array.data_type()),
        })
    }
}

impl KeyExtractor for Utf8KeyExtractor {
    type Key = Box<str>;

    #[inline]
    fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    fn extract_key(&self, row: usize) -> Option<Self::Key> {
        if self.array.is_null(row) {
            None
        } else {
            Some(self.array.value(row).into())
        }
    }

    #[inline]
    fn hash_key(&self, row: usize) -> Option<u64> {
        if self.array.is_null(row) {
            None
        } else {
            let val = self.array.value(row);
            let mut hasher = new_hasher();
            val.hash(&mut hasher);
            Some(hasher.finish())
        }
    }

    #[inline]
    fn hash_owned_key(key: &Self::Key) -> u64 {
        let mut hasher = new_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[inline]
    fn key_bytes(&self, row: usize) -> Option<Vec<u8>> {
        if self.array.is_null(row) {
            None
        } else {
            Some(self.array.value(row).as_bytes().to_vec())
        }
    }
}

/// Key extractor for binary columns.
pub struct BinaryKeyExtractor {
    array: BinaryArray,
}

impl BinaryKeyExtractor {
    /// Creates a new binary key extractor from a record batch and column name.
    pub fn new(batch: &RecordBatch, column: &str) -> Result<Self> {
        let col_idx = batch
            .schema()
            .index_of(column)
            .ok()
            .context(KeyColumnNotFoundSnafu { column })?;

        let array = batch.column(col_idx);
        Self::from_array(array)
    }

    /// Creates from a raw array reference.
    pub fn from_array(array: &ArrayRef) -> Result<Self> {
        if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
            return Ok(Self { array: arr.clone() });
        }
        if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
            // Convert LargeBinaryArray to BinaryArray
            let values: Vec<Option<&[u8]>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            let binary_array = BinaryArray::from(values);
            return Ok(Self {
                array: binary_array,
            });
        }

        Err(Error::UnsupportedKeyType {
            data_type: format!("{:?}", array.data_type()),
        })
    }
}

impl KeyExtractor for BinaryKeyExtractor {
    type Key = Box<[u8]>;

    #[inline]
    fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    fn extract_key(&self, row: usize) -> Option<Self::Key> {
        if self.array.is_null(row) {
            None
        } else {
            Some(self.array.value(row).into())
        }
    }

    #[inline]
    fn hash_key(&self, row: usize) -> Option<u64> {
        if self.array.is_null(row) {
            None
        } else {
            let val = self.array.value(row);
            let mut hasher = new_hasher();
            val.hash(&mut hasher);
            Some(hasher.finish())
        }
    }

    #[inline]
    fn hash_owned_key(key: &Self::Key) -> u64 {
        let mut hasher = new_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[inline]
    fn key_bytes(&self, row: usize) -> Option<Vec<u8>> {
        if self.array.is_null(row) {
            None
        } else {
            Some(self.array.value(row).to_vec())
        }
    }
}

/// Key extractor for composite keys using Arrow's `RowConverter`.
///
/// This handles multi-column primary keys by converting them to a
/// comparable byte representation.
pub struct RowConverterKeyExtractor {
    converter: Arc<RowConverter>,
    rows: arrow_row::Rows,
}

impl RowConverterKeyExtractor {
    /// Creates a new composite key extractor from a record batch and column names.
    ///
    /// # Errors
    ///
    /// Returns an error if any column is not found or has an unsupported type.
    pub fn new(batch: &RecordBatch, columns: &[String]) -> Result<Self> {
        let schema = batch.schema();
        let sort_fields: Vec<SortField> = columns
            .iter()
            .map(|col| {
                let field = schema
                    .field_with_name(col)
                    .map_err(|_| Error::KeyColumnNotFound {
                        column: col.clone(),
                    })?;
                Ok(SortField::new(field.data_type().clone()))
            })
            .collect::<Result<Vec<_>>>()?;

        let converter = RowConverter::new(sort_fields).map_err(|e| Error::Arrow { source: e })?;

        let key_arrays: Vec<ArrayRef> = columns
            .iter()
            .map(|col| {
                let idx = schema.index_of(col).map_err(|_| Error::KeyColumnNotFound {
                    column: col.clone(),
                })?;
                Ok(Arc::clone(batch.column(idx)))
            })
            .collect::<Result<Vec<_>>>()?;

        let rows = converter
            .convert_columns(&key_arrays)
            .map_err(|e| Error::Arrow { source: e })?;

        Ok(Self {
            converter: Arc::new(converter),
            rows,
        })
    }

    /// Creates with a pre-existing converter.
    ///
    /// # Errors
    ///
    /// Returns an error if any column is not found.
    pub fn with_converter(
        converter: Arc<RowConverter>,
        batch: &RecordBatch,
        columns: &[String],
    ) -> Result<Self> {
        let schema = batch.schema();
        let key_arrays: Vec<ArrayRef> = columns
            .iter()
            .map(|col| {
                let idx = schema.index_of(col).map_err(|_| Error::KeyColumnNotFound {
                    column: col.clone(),
                })?;
                Ok(Arc::clone(batch.column(idx)))
            })
            .collect::<Result<Vec<_>>>()?;

        let rows = converter
            .convert_columns(&key_arrays)
            .map_err(|e| Error::Arrow { source: e })?;

        Ok(Self { converter, rows })
    }

    /// Returns a reference to the row converter for reuse.
    #[must_use]
    pub fn converter(&self) -> Arc<RowConverter> {
        Arc::clone(&self.converter)
    }
}

impl KeyExtractor for RowConverterKeyExtractor {
    type Key = OwnedRow;

    #[inline]
    fn len(&self) -> usize {
        self.rows.num_rows()
    }

    #[inline]
    fn extract_key(&self, row: usize) -> Option<Self::Key> {
        // RowConverter doesn't track nulls directly, so we return Some
        // The caller should check for nulls in the source columns if needed
        Some(self.rows.row(row).owned())
    }

    #[inline]
    fn hash_key(&self, row: usize) -> Option<u64> {
        let row_bytes = self.rows.row(row);
        let mut hasher = new_hasher();
        row_bytes.as_ref().hash(&mut hasher);
        Some(hasher.finish())
    }

    #[inline]
    fn hash_owned_key(key: &Self::Key) -> u64 {
        let mut hasher = new_hasher();
        key.as_ref().hash(&mut hasher);
        hasher.finish()
    }

    #[inline]
    fn key_bytes(&self, row: usize) -> Option<Vec<u8>> {
        Some(self.rows.row(row).as_ref().to_vec())
    }
}

/// Creates the appropriate key extractor based on the column data type.
pub fn create_key_extractor(
    batch: &RecordBatch,
    columns: &[String],
) -> Result<Box<dyn KeyExtractorDyn>> {
    if columns.is_empty() {
        return Err(Error::KeyColumnNotFound {
            column: "(no columns specified)".to_string(),
        });
    }

    if columns.len() == 1 {
        let column = &columns[0];
        let schema = batch.schema();
        let col_idx = schema
            .index_of(column)
            .ok()
            .context(KeyColumnNotFoundSnafu { column })?;

        let data_type = schema.field(col_idx).data_type().clone();

        match data_type {
            DataType::Int8 => Ok(Box::new(PrimitiveKeyExtractor::<i8>::new(batch, column)?)),
            DataType::Int16 => Ok(Box::new(PrimitiveKeyExtractor::<i16>::new(batch, column)?)),
            DataType::Int32 => Ok(Box::new(PrimitiveKeyExtractor::<i32>::new(batch, column)?)),
            DataType::Int64 => Ok(Box::new(PrimitiveKeyExtractor::<i64>::new(batch, column)?)),
            DataType::UInt8 => Ok(Box::new(PrimitiveKeyExtractor::<u8>::new(batch, column)?)),
            DataType::UInt16 => Ok(Box::new(PrimitiveKeyExtractor::<u16>::new(batch, column)?)),
            DataType::UInt32 => Ok(Box::new(PrimitiveKeyExtractor::<u32>::new(batch, column)?)),
            DataType::UInt64 => Ok(Box::new(PrimitiveKeyExtractor::<u64>::new(batch, column)?)),
            DataType::Utf8 | DataType::LargeUtf8 => {
                Ok(Box::new(Utf8KeyExtractor::new(batch, column)?))
            }
            DataType::Binary | DataType::LargeBinary => {
                Ok(Box::new(BinaryKeyExtractor::new(batch, column)?))
            }
            _ => {
                // Fall back to RowConverter for complex types
                Ok(Box::new(RowConverterKeyExtractor::new(batch, columns)?))
            }
        }
    } else {
        // Composite key - use RowConverter
        Ok(Box::new(RowConverterKeyExtractor::new(batch, columns)?))
    }
}

/// Type-erased key extractor trait for dynamic dispatch.
pub trait KeyExtractorDyn: Send + Sync {
    /// Returns the number of rows.
    fn len(&self) -> usize;

    /// Returns true if empty.
    #[expect(dead_code, reason = "trait method for completeness")]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Computes the hash for the key at the given row.
    fn hash_key(&self, row: usize) -> Option<u64>;

    /// Returns the raw bytes representation of the key at the given row.
    ///
    /// This is used to compare keys for equality when hash collisions occur.
    /// Returns `None` if the key is null.
    fn key_bytes(&self, row: usize) -> Option<Vec<u8>>;
}

impl<E: KeyExtractor> KeyExtractorDyn for E {
    fn len(&self) -> usize {
        KeyExtractor::len(self)
    }

    fn hash_key(&self, row: usize) -> Option<u64> {
        KeyExtractor::hash_key(self, row)
    }

    fn key_bytes(&self, row: usize) -> Option<Vec<u8>> {
        KeyExtractor::key_bytes(self, row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec![
            Some("alice"),
            Some("bob"),
            None,
            Some("dave"),
            Some("eve"),
        ]);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
            .expect("failed to create batch")
    }

    #[test]
    fn test_primitive_key_extractor() {
        let batch = create_test_batch();
        let extractor =
            PrimitiveKeyExtractor::<i64>::new(&batch, "id").expect("failed to create extractor");

        assert_eq!(KeyExtractor::len(&extractor), 5);
        assert_eq!(extractor.extract_key(0), Some(1));
        assert_eq!(extractor.extract_key(4), Some(5));

        // Hash should be consistent
        let hash1 = KeyExtractor::hash_key(&extractor, 0);
        let hash2 = KeyExtractor::hash_key(&extractor, 0);
        assert_eq!(hash1, hash2);

        // Different keys should have different hashes (with high probability)
        let first_hash = KeyExtractor::hash_key(&extractor, 0);
        let second_hash = KeyExtractor::hash_key(&extractor, 1);
        assert_ne!(first_hash, second_hash);
    }

    #[test]
    fn test_utf8_key_extractor() {
        let batch = create_test_batch();
        let extractor = Utf8KeyExtractor::new(&batch, "name").expect("failed to create extractor");

        assert_eq!(KeyExtractor::len(&extractor), 5);
        assert_eq!(
            extractor.extract_key(0),
            Some("alice".to_string().into_boxed_str())
        );
        assert_eq!(extractor.extract_key(2), None); // Null value

        // Hash should return None for null
        assert!(KeyExtractor::hash_key(&extractor, 2).is_none());
        assert!(KeyExtractor::hash_key(&extractor, 0).is_some());
    }

    #[test]
    fn test_row_converter_key_extractor() {
        let batch = create_test_batch();
        let extractor = RowConverterKeyExtractor::new(&batch, &["id".to_string()])
            .expect("failed to create extractor");

        assert_eq!(KeyExtractor::len(&extractor), 5);

        // Keys should be extractable
        let key0 = extractor.extract_key(0);
        let key1 = extractor.extract_key(1);
        assert!(key0.is_some());
        assert!(key1.is_some());

        // Different rows should have different keys
        assert_ne!(key0, key1);
    }

    #[test]
    fn test_create_key_extractor_auto() {
        let batch = create_test_batch();

        // Should create primitive extractor for Int64
        let extractor =
            create_key_extractor(&batch, &["id".to_string()]).expect("failed to create extractor");
        assert_eq!(extractor.len(), 5);
        assert!(extractor.hash_key(0).is_some());

        // Should create Utf8 extractor for string
        let extractor = create_key_extractor(&batch, &["name".to_string()])
            .expect("failed to create extractor");
        assert_eq!(extractor.len(), 5);
    }

    #[test]
    fn test_composite_key() {
        let batch = create_test_batch();

        // Composite key should use RowConverter
        let extractor = create_key_extractor(&batch, &["id".to_string(), "name".to_string()])
            .expect("failed to create extractor");
        assert_eq!(extractor.len(), 5);
    }
}
