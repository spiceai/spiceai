/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{any::Any, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float16Array,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, UInt8Array, UInt16Array,
        UInt32Array, UInt64Array,
    },
    datatypes::DataType,
    error::ArrowError,
};
use tantivy::{Term, schema::Field};

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use logos::Source;
use runtime_datafusion_index::Index;
use search::generation::CandidateGeneration;
use search::generation::post_apply::PostApplyCandidateGeneration;
use search::generation::text_search::FullTextSearchFieldIndex;
use snafu::ResultExt;
use std::collections::HashSet;
use tantivy::schema::DocParsingError;
use tantivy::{TantivyDocument, TantivyError};

use crate::search::util::get_primary_keys;
use crate::{datafusion::query::write_to_json_string, search::full_text::Error};
use crate::{
    object_store_registry::SpiceObjectStoreRegistry,
    search::full_text::{
        FailedToInsertDataIntoIndexSnafu, IndexCreationSnafu, InvalidIndexingSnafu,
    },
};

/// The minimum number of bytes to support writing to in-memory [`tantivy::Index`].
pub static MINIMUM_MEMORY_BUDGET_FOR_MEMORY_INDEX: usize = 15_000_000;

#[derive(Clone, Debug)]
pub struct FullTextDatabaseIndex {
    pub search_fields: Vec<String>,
    pub primary_key: Vec<String>,
    base_table: Arc<dyn TableProvider>,
    index: Arc<tantivy::Index>,
}

#[async_trait]
impl Index for FullTextDatabaseIndex {
    fn name(&self) -> &'static str {
        "full_text"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn required_columns(&self) -> Vec<String> {
        // Return both the primary key and search fields, deduplicated.
        let mut required_columns = HashSet::new();
        required_columns.extend(self.primary_key.iter().cloned());
        required_columns.extend(self.search_fields.iter().cloned());
        required_columns.into_iter().collect()
    }
    async fn compute_index(&self, batches: Vec<RecordBatch>) {
        if let Err(e) = self.update_index(batches.as_slice()) {
            tracing::error!("Failed to update full text search index: {e}");
        }
    }
}

impl FullTextDatabaseIndex {
    pub async fn try_new(
        inner: Arc<dyn TableProvider>,
        search_fields: Vec<String>,
        primary_key_override: Option<Vec<String>>,
    ) -> Result<Self, Error> {
        // Use 'primary_key_override', fallback to underlying in table.
        let pks = match (primary_key_override, get_primary_keys(&inner).await) {
            (Some(pks), _) => pks,
            (None, Ok(pks)) if !pks.is_empty() => pks,
            (None, _) => {
                return Err(Error::NoPrimaryKey);
            }
        };

        let index = Self::create_index(&inner, search_fields.as_slice(), pks.as_slice())?;

        Ok(Self {
            base_table: inner,
            search_fields,
            index,
            primary_key: pks,
        })
    }

    fn delete_existing_from_primary_key(
        &self,
        writer: &tantivy::IndexWriter,
        rb: &[RecordBatch],
    ) -> Result<(), Error> {
        if let Some(pk) = self.primary_key.first() {
            if self.primary_key.len() == 1 {
                let Some((pk_field, _)) = self.index.schema().find_field(pk.as_str()) else {
                    return Err(Error::FailedToRetrieveDataFromIndex {
                        source: TantivyError::FieldNotFound(pk.clone()),
                    });
                };
                let terms: Vec<tantivy::Term> = rb
                    .iter()
                    .filter_map(|r| r.column_by_name(pk))
                    .map(|arr| array_to_terms(pk_field, arr))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| Error::FailedToRetrieveDataFromSource {
                        source: DataFusionError::ArrowError(e, None),
                    })?
                    .into_iter()
                    .flatten()
                    .collect();
                for t in terms {
                    writer.delete_term(t);
                }
                return Ok(());
            }
        }

        // let column: Vec<&ArrayRef> =
        //     rb.iter().filter_map(|rb| {
        //         let rb_schema = rb.schema();
        //         let proj: Vec<usize> = self.primary_key.iter().filter_map(|p| rb_schema.column_with_name(p.as_str())?.0).collect();
        //         let pks = rb.project(&proj).expect("bad");
        //         arrow_json::writer

        //     }).collect();
        // arrow_json
        // writer.delete_term(term)

        // TODO handle multi column case
        Err(Error::FailedToRetrieveDataFromIndex {
            source: TantivyError::InternalError(
                "currently not handling multiple columns".to_string(),
            ),
        })
    }

    /// Update the underlying [`tantivy::Index`] with new data from [`RecordBatch`]s. Additional
    /// columns present will be ignored.
    ///
    /// If there is a multi-column primary key (as specified by [`Self::primary_key`]), an additional column is used in the [`tantivy::Index`] for unique lookup (required since updates = deletion -> insertion).
    fn update_index(&self, rb: &[RecordBatch]) -> Result<(), Error> {
        let mut index_writer: tantivy::IndexWriter = self
            .index
            .writer(MINIMUM_MEMORY_BUDGET_FOR_MEMORY_INDEX)
            .context(IndexCreationSnafu)?;

        self.delete_existing_from_primary_key(&index_writer, rb)?;

        let doc_json = write_to_json_string(rb).context(InvalidIndexingSnafu {
            context: "Failed to write data to intermediate JSON string for indexing".to_string(),
        })?;
        let docs = parse_json_array(&self.index.schema(), doc_json.as_str())
            .context(FailedToInsertDataIntoIndexSnafu)?;

        for doc in docs {
            index_writer.add_document(doc).context(IndexCreationSnafu)?;
        }
        index_writer
            .commit()
            .context(FailedToInsertDataIntoIndexSnafu)?;

        Ok(())
    }

    #[must_use]
    pub fn underlying_table(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.base_table)
    }

    /// Construct a new [`FullTextDatabaseIndex`] with an updated [`TableProvider`].
    ///
    /// No Checks are done to confirm compatibility between the current index and the provided [`TableProvider`].
    #[must_use]
    pub fn with_new_base(&self, base_table: Arc<dyn TableProvider>) -> Self {
        Self {
            search_fields: self.search_fields.clone(),
            primary_key: self.primary_key.clone(),
            index: Arc::clone(&self.index),
            base_table,
        }
    }

    fn create_index(
        base_table: &Arc<dyn TableProvider>,
        search_fields: &[String],
        primary_key: &[String],
    ) -> Result<Arc<tantivy::Index>, Error> {
        let schema = base_table.schema();
        let mut schema_builder = tantivy::schema::Schema::builder();
        for p in primary_key {
            if search_fields.contains(p) {
                // Added below, tokenized.
                continue;
            }
            let Some((_, field)) = schema.column_with_name(p) else {
                return Err(Error::PrimaryKeyNotFound { column: p.clone() });
            };
            match field.data_type() {
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    schema_builder.add_f64_field(p.as_str(), tantivy::schema::STORED);
                }
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    schema_builder.add_u64_field(p.as_str(), tantivy::schema::STORED);
                }
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    schema_builder.add_i64_field(p.as_str(), tantivy::schema::STORED);
                }
                DataType::Boolean => {
                    schema_builder.add_bool_field(p.as_str(), tantivy::schema::STORED);
                }

                DataType::Date32 | DataType::Date64 => {
                    schema_builder.add_date_field(p.as_str(), tantivy::schema::STORED);
                }
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    schema_builder.add_text_field(p.as_str(), tantivy::schema::STORED);
                }
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                    schema_builder.add_bytes_field(p.as_str(), tantivy::schema::STORED);
                }
                dt => {
                    return Err(Error::PrimaryKeyInvalidType {
                        data_type: dt.clone(),
                        column: p.clone(),
                    });
                }
            }
        }

        for s in search_fields {
            schema_builder.add_text_field(s, tantivy::schema::TEXT | tantivy::schema::STORED);
        }
        let schema = schema_builder.build();
        Ok(Arc::new(tantivy::Index::create_in_ram(schema)))
    }

    fn new_ctx() -> Result<Arc<SessionContext>, DataFusionError> {
        let env = RuntimeEnvBuilder::default()
            .with_object_store_registry(Arc::new(SpiceObjectStoreRegistry::default()))
            .build()?;
        let ctx = SessionContext::new_with_config_rt(SessionConfig::default(), Arc::new(env));

        Ok(Arc::new(ctx))
    }

    pub fn full_text_search_field_index(
        &self,
        search_field: &str,
    ) -> Result<FullTextSearchFieldIndex, search::generation::text_search::Error> {
        let mut search_index = FullTextSearchFieldIndex::try_new(
            Arc::clone(&self.index),
            search_field.to_string(),
            self.primary_key.clone(),
            Some(vec![]), // Explicitly do not return other `self.search_fields` columns in search results.
        )?;
        search_index.add_type_hints(&self.underlying_table().schema());
        Ok(search_index)
    }

    /// Constructs a [`CandidateGeneration`] for full text search on the underlying [`tantivy::Index`] with full filter and column support via the underlying [`TableProvider`].
    pub fn as_candidate_generations(
        &self,
    ) -> Result<Vec<Arc<dyn CandidateGeneration>>, search::generation::Error> {
        let mut generators = vec![];
        for search_field in self.search_fields.as_slice() {
            let base = self
                .full_text_search_field_index(search_field.as_str())
                .map_err(|source| search::generation::Error::TextSearchError { source })?;

            let post_apply = PostApplyCandidateGeneration::new(
                Arc::clone(&self.base_table),
                Arc::new(base),
                self.primary_key.clone(),
            )
            .with_ctx(
                Self::new_ctx()
                    .boxed()
                    .map_err(|source| search::generation::Error::InternalError { source })?,
            );
            generators.push(Arc::new(post_apply) as Arc<dyn CandidateGeneration>);
        }

        Ok(generators)
    }
}

/// An implementation of [`TantivyDocument::parse_json`] that can parse a JSON array of JSON
/// objects that will deserialize to [`TantivyDocument`].
fn parse_json_array(
    schema: &tantivy::schema::Schema,
    doc_json: &str,
) -> Result<Vec<TantivyDocument>, TantivyError> {
    let json_obj: Vec<serde_json::Map<String, serde_json::Value>> = serde_json::from_str(doc_json)
        .map_err(|_| {
            Into::<TantivyError>::into(DocParsingError::InvalidJson(
                doc_json.slice(0..20).unwrap_or_default().to_string(),
            ))
        })?;

    Ok(json_obj
        .into_iter()
        .map(|obj| TantivyDocument::from_json_object(schema, obj))
        .collect::<Result<Vec<_>, _>>()?)
}

/// Macro to downcast an `ArrayRef` to concrete Arrow array type or return Err.
///
/// Users should check type-compatibility beforehand using [`ArrayRef::data_type`].
macro_rules! downcast_array {
    ($ARRAY:expr, $TY:ty) => {
        $ARRAY.as_any().downcast_ref::<$TY>().ok_or_else(|| {
            ArrowError::CastError(format!("Expected arrow array of type {}", stringify!($TY)))
        })?
    };
}

#[allow(clippy::too_many_lines)]
pub fn array_to_terms(field: Field, arr: &ArrayRef) -> Result<Vec<Term>, ArrowError> {
    let mut terms = Vec::with_capacity(arr.len());

    match arr.data_type() {
        // --- Floats → f64
        DataType::Float16 => {
            let a = downcast_array!(arr, Float16Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    let v = f64::from(a.value(i).to_f32());
                    terms.push(Term::from_field_f64(field, v));
                }
            }
        }
        DataType::Float32 => {
            let a = downcast_array!(arr, Float32Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    let v = f64::from(a.value(i));
                    terms.push(Term::from_field_f64(field, v));
                }
            }
        }
        DataType::Float64 => {
            let a = downcast_array!(arr, Float64Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_f64(field, a.value(i)));
                }
            }
        }

        // --- Unsigned ints → u64
        DataType::UInt8 => {
            let a = downcast_array!(arr, UInt8Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_u64(field, u64::from(a.value(i))));
                }
            }
        }
        DataType::UInt16 => {
            let a = downcast_array!(arr, UInt16Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_u64(field, u64::from(a.value(i))));
                }
            }
        }
        DataType::UInt32 => {
            let a = downcast_array!(arr, UInt32Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_u64(field, u64::from(a.value(i))));
                }
            }
        }
        DataType::UInt64 => {
            let a = downcast_array!(arr, UInt64Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_u64(field, a.value(i)));
                }
            }
        }

        // --- Signed ints → i64
        DataType::Int8 => {
            let a = downcast_array!(arr, Int8Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_i64(field, i64::from(a.value(i))));
                }
            }
        }
        DataType::Int16 => {
            let a = downcast_array!(arr, Int16Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_i64(field, i64::from(a.value(i))));
                }
            }
        }
        DataType::Int32 => {
            let a = downcast_array!(arr, Int32Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_i64(field, i64::from(a.value(i))));
                }
            }
        }
        DataType::Int64 => {
            let a = downcast_array!(arr, Int64Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_i64(field, a.value(i)));
                }
            }
        }

        // --- Boolean
        DataType::Boolean => {
            let a = downcast_array!(arr, BooleanArray);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_bool(field, a.value(i)));
                }
            }
        }

        // --- Dates
        DataType::Date32 => {
            let a = downcast_array!(arr, Date32Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_date(
                        field,
                        tantivy::DateTime::from_timestamp_secs(i64::from(a.value(i)) * 86_400),
                    ));
                }
            }
        }
        DataType::Date64 => {
            let a = downcast_array!(arr, Date64Array);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_date(
                        field,
                        tantivy::DateTime::from_timestamp_millis(a.value(i)),
                    ));
                }
            }
        }

        // --- UTF8 text
        DataType::Utf8 => {
            let a = downcast_array!(arr, StringArray);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_text(field, a.value(i)));
                }
            }
        }
        DataType::LargeUtf8 => {
            let a = downcast_array!(arr, LargeStringArray);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_text(field, a.value(i)));
                }
            }
        }

        // --- Binary blobs
        DataType::Binary => {
            let a = downcast_array!(arr, BinaryArray);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_bytes(field, a.value(i)));
                }
            }
        }
        DataType::LargeBinary => {
            let a = downcast_array!(arr, LargeBinaryArray);
            for i in 0..a.len() {
                if a.is_valid(i) {
                    terms.push(Term::from_field_bytes(field, a.value(i)));
                }
            }
        }

        // --- Everything else is unsupported
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Cannot use primary key of arrow type {other:?} for full-text search"
            )));
        }
    }

    Ok(terms)
}
