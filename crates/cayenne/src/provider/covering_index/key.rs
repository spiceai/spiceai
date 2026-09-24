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

//! Typed source, page, schema, and key identities.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow_schema::{DataType, FieldRef, SchemaRef};

use super::super::lookup_index::{KeyColumn, KeySpec, key_converter};
use super::{Error, Result};
use crate::row_converter::{RowConverter, SortField};

/// The in-process key codec used by every covering-index run.
///
/// It is deliberately separate from the page representation: pages are not
/// durable in this step, but an index definition must still reject keys encoded
/// by a different version in the same process.
pub(crate) const KEY_CODEC_VERSION: u16 = crate::row_converter::RowFormatVersion::CURRENT.id();

/// Immutable identity of one physical source generation in one table.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct SourceId {
    table_id: Arc<str>,
    generation: u64,
}

impl SourceId {
    /// Create an identity for `generation` of `table_id`.
    #[must_use]
    pub(crate) fn new(table_id: impl Into<Arc<str>>, generation: u64) -> Self {
        Self {
            table_id: table_id.into(),
            generation,
        }
    }

    /// The table that owns this source.
    #[must_use]
    pub(crate) fn table_id(&self) -> &str {
        &self.table_id
    }

    /// The immutable source generation.
    #[must_use]
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }
}

/// Generation-qualified identifier of a page containing sorted encoded keys.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct KeyPageId {
    source: SourceId,
    page: u32,
}

impl KeyPageId {
    /// Create page `page` belonging to `source`.
    #[must_use]
    pub(crate) fn new(source: SourceId, page: u32) -> Self {
        Self { source, page }
    }

    /// The source generation this page belongs to.
    #[must_use]
    pub(crate) fn source(&self) -> &SourceId {
        &self.source
    }

    /// The page sequence within its source generation.
    #[must_use]
    pub(crate) const fn page(&self) -> u32 {
        self.page
    }
}

/// Generation-qualified identifier of a page containing covered Arrow rows.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct PayloadPageId {
    source: SourceId,
    page: u32,
}

impl PayloadPageId {
    /// Create page `page` belonging to `source`.
    #[must_use]
    pub(crate) fn new(source: SourceId, page: u32) -> Self {
        Self { source, page }
    }

    /// The source generation this page belongs to.
    #[must_use]
    pub(crate) fn source(&self) -> &SourceId {
        &self.source
    }

    /// The page sequence within its source generation.
    #[must_use]
    pub(crate) const fn page(&self) -> u32 {
        self.page
    }
}

/// Exact schema captured with a source plus the logical-to-physical mapping.
///
/// The schema is retained instead of a hash because structural validation is a
/// correctness boundary for a path that returns stored Arrow values directly.
#[derive(Clone, Debug)]
pub(crate) struct SchemaIdentity {
    schema: SchemaRef,
    column_mapping: Arc<[usize]>,
}

impl SchemaIdentity {
    /// Capture `schema` and validate a complete logical-to-physical mapping.
    pub(crate) fn new(schema: SchemaRef, column_mapping: Vec<usize>) -> Result<Self> {
        let field_count = schema.fields().len();
        if column_mapping.len() != field_count {
            return Err(Error::InvalidContract {
                message: format!(
                    "schema mapping has {} entries for a schema with {field_count} fields",
                    column_mapping.len()
                ),
            });
        }

        let mut seen = vec![false; field_count];
        for physical in &column_mapping {
            if *physical >= field_count {
                return Err(Error::InvalidContract {
                    message: format!(
                        "schema mapping refers to physical column {physical}, but the schema has {field_count} fields"
                    ),
                });
            }
            if std::mem::replace(&mut seen[*physical], true) {
                return Err(Error::InvalidContract {
                    message: format!(
                        "schema mapping refers to physical column {physical} more than once"
                    ),
                });
            }
        }

        Ok(Self {
            schema,
            column_mapping: column_mapping.into(),
        })
    }

    /// Capture an identity mapping for every column of `schema`.
    pub(crate) fn identity(schema: &SchemaRef) -> Result<Self> {
        Self::new(Arc::clone(schema), (0..schema.fields().len()).collect())
    }

    /// The fully retained captured schema.
    #[must_use]
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The logical-to-physical field mapping.
    #[must_use]
    pub(crate) fn column_mapping(&self) -> &[usize] {
        &self.column_mapping
    }

    /// Whether `other` is structurally the same captured schema and mapping.
    #[must_use]
    pub(crate) fn matches(&self, other: &Self) -> bool {
        self.schema.as_ref() == other.schema.as_ref() && self.column_mapping == other.column_mapping
    }
}

/// One canonical key column and its source-schema position.
#[derive(Clone, Debug)]
pub(crate) struct IndexColumn {
    name: String,
    field: FieldRef,
    schema_index: usize,
}

impl IndexColumn {
    /// The exact schema field, including nullability and metadata.
    #[must_use]
    pub(crate) fn field(&self) -> &FieldRef {
        &self.field
    }

    /// The canonical name selected during key resolution.
    #[must_use]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// The source-schema field position.
    #[must_use]
    pub(crate) const fn schema_index(&self) -> usize {
        self.schema_index
    }
}

/// Definition of one ordered covering index over every stored source column.
#[derive(Clone, Debug)]
pub(crate) struct IndexDefinition {
    columns: Arc<[IndexColumn]>,
    schema: SchemaIdentity,
    codec_version: u16,
}

impl IndexDefinition {
    /// Resolve `spec` against `schema` using the same ambiguity and float rules
    /// as Cayenne's existing lookup index.
    pub(crate) fn resolve(schema: SchemaRef, spec: &KeySpec) -> Result<Self> {
        let schema_field_count = schema.fields().len();
        let mut columns = Vec::with_capacity(spec.columns().len());
        let mut key_columns = Vec::with_capacity(spec.columns().len());

        for configured in spec.columns() {
            let resolved = KeyColumn::resolve(schema.as_ref(), configured)
                .map_err(|message| Error::InvalidContract { message })?;
            let schema_index = schema
                .fields()
                .iter()
                .position(|field| field.name() == &resolved.name)
                .ok_or_else(|| Error::InvalidContract {
                    message: format!(
                        "resolved key column '{}' is absent from its captured schema",
                        resolved.name
                    ),
                })?;
            columns.push(IndexColumn {
                name: resolved.name.clone(),
                field: Arc::clone(&schema.fields()[schema_index]),
                schema_index,
            });
            key_columns.push(resolved);
        }

        key_converter(&key_columns).map_err(|message| Error::InvalidContract { message })?;
        Ok(Self {
            columns: columns.into(),
            schema: SchemaIdentity::new(schema, (0..schema_field_count).collect())?,
            codec_version: KEY_CODEC_VERSION,
        })
    }

    /// The ordered canonical key columns.
    #[must_use]
    pub(crate) fn columns(&self) -> &[IndexColumn] {
        &self.columns
    }

    /// The exact schema identity used by the build.
    #[must_use]
    pub(crate) fn schema(&self) -> &SchemaIdentity {
        &self.schema
    }

    /// The in-process codec version required by this definition.
    #[must_use]
    pub(crate) const fn codec_version(&self) -> u16 {
        self.codec_version
    }

    /// Whether another definition has exactly the same key semantics.
    #[must_use]
    pub(crate) fn matches(&self, other: &Self) -> bool {
        self.codec_version == other.codec_version
            && self.schema.matches(&other.schema)
            && self.columns.len() == other.columns.len()
            && self
                .columns
                .iter()
                .zip(other.columns.iter())
                .all(|(left, right)| {
                    left.name == right.name
                        && left.schema_index == right.schema_index
                        && left.field.as_ref() == right.field.as_ref()
                })
    }

    /// Encode row `row` of correlated `columns`, or return `None` when any
    /// equality component is NULL.
    ///
    /// This method never decomposes a composite tuple into scalar key sets. It
    /// validates every expression-domain type before using Arrow's conversion
    /// kernel, so a successful general-purpose cast is not treated as a proof
    /// that equality remains lossless.
    pub(crate) fn encode_probe_row(
        &self,
        columns: &[ArrayRef],
        row: usize,
    ) -> Result<Option<EncodedKey>> {
        if columns.len() != self.columns.len() {
            return Err(Error::InvalidContract {
                message: format!(
                    "probe has {} columns but index key has {}",
                    columns.len(),
                    self.columns.len()
                ),
            });
        }

        let mut adapted = Vec::with_capacity(columns.len());
        for (array, index_column) in columns.iter().zip(self.columns.iter()) {
            if row >= array.len() {
                return Err(Error::InvalidContract {
                    message: format!("probe row {row} is outside a {}-row key array", array.len()),
                });
            }
            if array.is_null(row) {
                return Ok(None);
            }
            adapted.push(adapt_probe_array(array, index_column.field.data_type())?.slice(row, 1));
        }

        let converter = RowConverter::new(
            self.columns
                .iter()
                .map(|column| SortField::new(column.field.data_type().clone()))
                .collect(),
        )?;
        let rows = converter.convert_columns(&adapted)?;
        let key = rows.row(0).as_ref().into();
        Ok(Some(EncodedKey(key)))
    }
}

/// Encoded full key bytes produced only by an [`IndexDefinition`].
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct EncodedKey(Box<[u8]>);

impl EncodedKey {
    /// Build an encoded key from bytes read from an already validated key page.
    #[must_use]
    pub(crate) fn from_page_bytes(bytes: &[u8]) -> Self {
        Self(bytes.into())
    }

    /// The comparable key bytes.
    #[must_use]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// The only expression-domain adaptations currently admitted for equality
/// probes. Numeric casts are intentionally absent: successful `cast_to` alone
/// is not a losslessness proof at signed/unsigned or decimal extrema.
fn equality_compatible(from: &DataType, stored: &DataType) -> bool {
    from == stored
        || matches!(
            (from, stored),
            (DataType::Utf8, DataType::Utf8View)
                | (DataType::Utf8View, DataType::Utf8)
                | (DataType::Binary, DataType::BinaryView)
                | (DataType::BinaryView, DataType::Binary)
        )
}

/// Adapt an already-proven lossless expression domain to the stored key type.
fn adapt_probe_array(array: &ArrayRef, stored: &DataType) -> Result<ArrayRef> {
    if !equality_compatible(array.data_type(), stored) {
        return Err(Error::UnsupportedKeyAdaptation {
            from: array.data_type().to_string(),
            to: stored.to_string(),
        });
    }
    if array.data_type() == stored {
        return Ok(Arc::clone(array));
    }
    arrow::compute::cast(array, stored).map_err(|source| Error::Arrow { source })
}

/// Reference to one physical row retained by a key page.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CoveredRowRef {
    source: SourceId,
    payload_page: PayloadPageId,
    row_in_page: usize,
    source_row_ordinal: u64,
}

impl CoveredRowRef {
    /// Construct a row reference after proving page ownership and row bounds.
    pub(crate) fn new(
        source: SourceId,
        payload_page: PayloadPageId,
        row_in_page: usize,
        source_row_ordinal: u64,
    ) -> Result<Self> {
        if payload_page.source() != &source {
            return Err(Error::InvalidContract {
                message: format!(
                    "payload page {payload_page:?} does not belong to source {source:?}"
                ),
            });
        }
        Ok(Self {
            source,
            payload_page,
            row_in_page,
            source_row_ordinal,
        })
    }

    /// The source generation that owns both this reference and its page.
    #[must_use]
    pub(crate) fn source(&self) -> &SourceId {
        &self.source
    }

    /// The generation-qualified payload page.
    #[must_use]
    pub(crate) fn payload_page(&self) -> &PayloadPageId {
        &self.payload_page
    }

    /// Row position inside the payload page.
    #[must_use]
    pub(crate) const fn row_in_page(&self) -> usize {
        self.row_in_page
    }

    /// Original physical row ordinal, used as a deterministic tie breaker.
    #[must_use]
    pub(crate) const fn source_row_ordinal(&self) -> u64 {
        self.source_row_ordinal
    }
}
