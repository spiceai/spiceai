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

use arrow_schema::DataType;
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
use snafu::{ResultExt, Snafu};
use std::collections::HashSet;
use std::sync::Arc;
use tantivy::schema::DocParsingError;
use tantivy::{TantivyDocument, TantivyError};

use crate::datafusion::query::write_to_json_string;
use crate::object_store_registry::SpiceObjectStoreRegistry;
use crate::search::util::get_primary_keys;

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

    fn required_columns(&self) -> Vec<String> {
        // Return both the primary key and search fields, deduplicated.
        let mut required_columns = HashSet::new();
        required_columns.extend(self.primary_key.iter().cloned());
        required_columns.extend(self.search_fields.iter().cloned());
        required_columns.into_iter().collect()
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Full text search requires a primary key, and the table did not have one.",))]
    NoPrimaryKey,

    #[snafu(display(
        "Primary key column '{column}' used in search index has unsupported data type: '{data_type}'",
    ))]
    PrimaryKeyInvalidType { column: String, data_type: DataType },

    #[snafu(display("Primary key column '{column}' not found in table.",))]
    PrimaryKeyNotFound { column: String },

    #[snafu(display("Failed to create a full text search index: {source}.",))]
    IndexCreationError { source: TantivyError },

    #[snafu(display("Failed to retrieve the data from the underlying table: {source}.",))]
    FailedToRetrieveDataFromSource { source: DataFusionError },

    #[snafu(display("Failed to insert data into the full text search index: {source}.",))]
    FailedToInsertDataIntoIndex { source: TantivyError },

    #[snafu(display(
        "Failed to create the full text search index. Context: {context}. Error: {source}.",
    ))]
    InvalidIndexingError {
        source: Box<dyn std::error::Error + Send + Sync>,
        context: String,
    },
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

        let index =
            Self::create_index(Arc::clone(&inner), search_fields.as_slice(), pks.as_slice())
                .await?;

        Ok(Self {
            base_table: inner,
            search_fields,
            index,
            primary_key: pks,
        })
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

    async fn create_index(
        base_table: Arc<dyn TableProvider>,
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
        Self::create_and_init_index(base_table, schema).await
    }

    fn new_ctx() -> Result<Arc<SessionContext>, DataFusionError> {
        let env = RuntimeEnvBuilder::default()
            .with_object_store_registry(Arc::new(SpiceObjectStoreRegistry::default()))
            .build()?;
        let ctx = SessionContext::new_with_config_rt(SessionConfig::default(), Arc::new(env));

        Ok(Arc::new(ctx))
    }

    async fn create_and_init_index(
        table: Arc<dyn TableProvider>,
        schema: tantivy::schema::Schema,
    ) -> Result<Arc<tantivy::Index>, Error> {
        let cols: Vec<_> = schema.fields().map(|(_, ent)| ent.name()).collect();
        let ctx = Self::new_ctx().context(FailedToRetrieveDataFromSourceSnafu)?;
        let _ = ctx
            .register_table("temp_table", table)
            .context(FailedToRetrieveDataFromSourceSnafu)?;

        let rbs = ctx
            .table("temp_table")
            .await
            .context(FailedToRetrieveDataFromSourceSnafu)?
            .select_columns(cols.as_slice())
            .context(FailedToRetrieveDataFromSourceSnafu)?
            .collect()
            .await
            .context(FailedToRetrieveDataFromSourceSnafu)?;

        let doc_json = write_to_json_string(rbs.as_slice()).context(InvalidIndexingSnafu {
            context: "Failed to write data to intermediate JSON string for indexing".to_string(),
        })?;
        let docs = parse_json_array(&schema, doc_json.as_str())
            .context(FailedToInsertDataIntoIndexSnafu)?;

        let index = tantivy::Index::create_in_ram(schema);
        let mut index_writer: tantivy::IndexWriter = index
            .writer(15_000_000) // cannot be less than 15_000_000 for in memory
            .context(IndexCreationSnafu)?;

        for doc in docs {
            index_writer.add_document(doc).context(IndexCreationSnafu)?;
        }
        index_writer
            .commit()
            .context(FailedToInsertDataIntoIndexSnafu)?;

        Ok(Arc::new(index))
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
