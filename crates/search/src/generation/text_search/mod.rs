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
use std::{cmp::min, collections::HashMap, sync::Arc};

use crate::{
    SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME,
    generation::text_search::query::FullTextSearchQuery,
};
use arrow::{
    array::RecordBatch,
    datatypes::{Field, FieldRef, Schema, SchemaRef},
    error::ArrowError,
};
use arrow_json::reader::Decoder;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider, error::DataFusionError, execution::SendableRecordBatchStream,
    logical_expr::sqlparser::ast::Expr, physical_plan::stream::RecordBatchStreamAdapter,
};

use futures::{Stream, StreamExt};
use serde_json::{Number, Value};
use snafu::{ResultExt, Snafu};
use tantivy::{
    Searcher, TantivyError,
    collector::TopDocs,
    query::{Occur, QueryParser, QueryParserError},
    query_grammar::{Delimiter, UserInputAst, UserInputLeaf, UserInputLiteral},
    schema::{FieldType, OwnedValue},
    tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer},
};

use super::{
    CandidateGeneration, Error as GenerationError, Result as GenerationResult,
    TextSearchSnafu as GenerationTextSearchSnafu,
};

/// Maximum number of results in a single full-text search request, before any pagination.
/// This size is designated for latency performance on the underlying index.
pub static DEFAULT_BATCH_SIZE: usize = 100;

/// Maximum number of results to return for a given full-text search.
pub static DEFAULT_LIMIT_MAXIMUM: usize = 1000;

pub mod exec;
pub mod index;
pub mod query;
mod util;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Error occurred during text search: {source}"))]
    TextSearchError { source: TantivyError },

    #[snafu(display("Error occurred during indexing text search index: {source}"))]
    TextSearchIndexingError { source: TantivyError },

    #[snafu(display("User provided query '{query}' is invalid: {source}"))]
    InvalidTextSearchQueryError {
        source: QueryParserError,
        query: String,
    },

    #[snafu(display(
        "Search index is missing the column `{missing}`. The index has the following columns: {}", index_columns.join(", ")
    ))]
    TextSearchIndexMissingColummn {
        missing: String,
        index_columns: Vec<String>,
    },

    #[snafu(display("Failed to infer an Arrow schema from JSON format. Error: {source}"))]
    ArrowSchemaError { source: ArrowError },

    #[snafu(display("Failed to convert JSON values to Arrow format. Error: {source}"))]
    ArrowConversionError { source: DataFusionError },

    #[snafu(display("Failed to convert underlying search data into JSON format. Error: {source}"))]
    SerdeJsonConversionError { source: serde_json::Error },

    #[snafu(display("Full text search does not support filters."))]
    UnsupportedFiltersError,

    #[snafu(display("Full text search does not support retrieving additional columns."))]
    UnsupportedAdditionalColumnsError,

    #[snafu(display("Failed to create a full text search index: {source}.",))]
    IndexCreationError { source: TantivyError },

    #[snafu(display("Failed to insert or update data into a full text search index: {source}.",))]
    IndexInsertionError { source: TantivyError },

    #[snafu(display(
        "Failed to create the full text search index. Context: {context}. Error: {source}.",
    ))]
    InvalidIndexingError {
        source: Box<dyn std::error::Error + Send + Sync>,
        context: String,
    },

    #[snafu(display("Failed to retrieve the data from the full text search index: {source}.",))]
    FailedToRetrieveDataFromIndex { source: TantivyError },

    #[snafu(display("Failed to retrieve the data from the underlying table: {source}.",))]
    FailedToRetrieveDataFromSource { source: DataFusionError },

    #[snafu(display("Failed to insert data into the full text search index: {source}.",))]
    FailedToInsertDataIntoIndex { source: TantivyError },

    #[snafu(display("Full text search requires a primary key, and the table did not have one.",))]
    NoPrimaryKey,

    #[snafu(display(
        "Primary key column '{column}' used in search index has unsupported data type: '{data_type}'",
    ))]
    PrimaryKeyInvalidType {
        column: String,
        data_type: arrow::datatypes::DataType,
    },

    #[snafu(display("Primary key column '{column}' used in search index is not allowed.",))]
    PrimaryKeyInvalidName { column: String },

    #[snafu(display("Primary key column '{column}' not found in table.",))]
    PrimaryKeyNotFound { column: String },

    #[snafu(display("Failed to retrieve primary key from the table: {source}."))]
    FailedToRetrievePrimaryKey { source: ArrowError },

    #[snafu(display("Temporarily failed to access full text search index"))]
    TemporarilyFailedToAccessSearchIndex {},
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    #[must_use]
    pub fn is_user_error(&self) -> bool {
        matches!(
            self,
            Error::InvalidTextSearchQueryError { .. } | Error::TextSearchIndexMissingColummn { .. }
        )
    }
}

impl std::fmt::Debug for FullTextSearchFieldIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FullTextSearchFieldIndex")
            .field("schema", self.reader.schema())
            .field("field", &self.field)
            .field("primary_key", &self.primary_key)
            .field("type_hints", &self.type_hints)
            .finish_non_exhaustive()
    }
}

/// A [`FullTextSearchFieldIndex`] performs a search on a [`tantivy::Index`]  for a single field of a table.
#[derive(Clone)]
pub struct FullTextSearchFieldIndex {
    // These are components from a [`tantivy::Index`] required to perform a search on a  [`tantivy::Index`] at a given commit.
    reader: tantivy::Searcher,

    pub field: String,
    pub primary_key: Vec<String>,

    /// Provide hints to the final Arrow datatype for a given column. Keys are column names.
    /// Tantivy [`FieldType`]s are less specific than [`arrow::datatypes::DataType`]s and the Arrow type must be inferred from Tanitvy JSON results (via [`arrow_json::reader::infer_json_schema_from_iterator`]).
    /// For columns present, use the associated [`arrow::datatypes::Field`].
    type_hints: HashMap<String, Arc<arrow::datatypes::Field>>,
}

impl FullTextSearchFieldIndex {
    pub fn try_new(
        index_search: Searcher,
        field: String,
        primary_key: Vec<String>,
    ) -> Result<Self> {
        let fts = Self {
            reader: index_search,
            field,
            primary_key,
            type_hints: HashMap::from([(
                SEARCH_SCORE_COLUMN_NAME.to_string(),
                Arc::new(Field::new(
                    SEARCH_SCORE_COLUMN_NAME,
                    arrow::datatypes::DataType::Float64,
                    false,
                )),
            )]),
        };

        // Ensure that the index has the required primary key columns.
        let cols = fts.all_columns();
        for pk in &fts.primary_key {
            if !cols.contains(pk) {
                return Err(Error::TextSearchIndexMissingColummn {
                    missing: pk.clone(),
                    index_columns: cols,
                });
            }
        }

        Ok(fts)
    }

    ///  Schema is based on the [`tantivy::schema::Schema`] with `self.type_hints` applied.
    fn schema(&self) -> Arc<Schema> {
        let search_schema = self.reader.schema();
        let fields = self
            .all_columns()
            .iter()
            .filter_map(|field_name| {
                let (data_type, nullable) = if let Some(f) = self.get_type_hint(field_name) {
                    (f.data_type().clone(), f.is_nullable())
                } else {
                    let f = search_schema.get_field(field_name).ok()?;
                    let entry = search_schema.get_field_entry(f);
                    (tantivy_to_arrow_type(entry.field_type())?, false)
                };
                Some(Field::new(field_name, data_type, nullable))
            })
            .collect::<Vec<_>>();

        Arc::new(Schema::new(fields))
    }

    /// Add type hints for all fields in [`SchemaRef`].
    ///
    /// Fields in `schema` but not in the underlying [`FullTextSearchIndex::idx`] are added.
    pub fn add_type_hints(&mut self, schema: &SchemaRef) {
        for f in schema.fields() {
            self.add_type_hint(f.name(), Arc::clone(f));
        }
    }

    pub fn add_type_hint(&mut self, name: impl Into<String>, field: impl Into<Arc<Field>>) {
        self.type_hints.insert(name.into(), field.into());
    }

    #[must_use]
    pub fn get_type_hint(&self, name: &String) -> Option<&FieldRef> {
        self.type_hints.get(name)
    }

    #[must_use]
    pub fn all_columns(&self) -> Vec<String> {
        self.reader
            .schema()
            .fields()
            .filter_map(|(_, f)| {
                if f.is_stored() {
                    Some(f.name().to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    fn query_parser(&self) -> QueryParser {
        let default_field = self
            .reader
            .schema()
            .find_field(self.field.as_str())
            .map(|(f, _)| vec![f])
            .unwrap_or_default();
        QueryParser::new(
            self.reader.schema().clone(),
            default_field,
            self.reader.index().tokenizers().clone(),
        )
    }

    pub async fn search(
        &self,
        query: String,
        opt_filters: &[&Expr],
        limit: usize,
    ) -> GenerationResult<SendableRecordBatchStream> {
        if !opt_filters.is_empty() {
            return Err(Error::UnsupportedFiltersError).context(GenerationTextSearchSnafu)?;
        }
        let strm = make_stream(self.clone(), query, limit);
        let mut strm = Box::pin(strm.peekable());
        let schema = match strm.as_mut().peek().await {
            None => Arc::new(Schema::empty()),
            Some(Ok(rb)) => rb.schema(),
            Some(Err(e)) => {
                return Err(GenerationError::internal(
                    format!("Failed to parse schema of full text search results: {e}").as_str(),
                ));
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, strm)) as SendableRecordBatchStream)
    }

    fn search_query_literal(
        &self,
        literal: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Value>> {
        let q = self
            .query_parser()
            .build_query_from_user_input_ast(parse_query_literal(literal))
            .context(InvalidTextSearchQuerySnafu {
                query: literal.to_string(),
            })?;

        let all_cols = self.all_columns();

        let top_docs = self
            .reader
            .search(&q, &TopDocs::with_limit(limit).and_offset(offset))
            .context(TextSearchSnafu)?
            .into_iter()
            .map(|(score, addr)| {
                let doc: HashMap<tantivy::schema::Field, OwnedValue> =
                    self.reader.doc(addr).context(TextSearchSnafu)?;

                let mut doc_w_col_names = doc
                    .into_iter()
                    .map(|(f, v)| (self.reader.schema().get_field_name(f), v))
                    .filter(|(name, _)| all_cols.contains(&(*name).to_string()))
                    .collect::<HashMap<_, _>>();

                // Must rename `self.field` -> `SEARCH_VALUE_COLUMN_NAME` for final result.
                if let Some(value) = doc_w_col_names.remove(self.field.as_str()) {
                    doc_w_col_names.insert(self.field.as_str(), value.clone());
                    doc_w_col_names.insert(SEARCH_VALUE_COLUMN_NAME, value);
                }

                let mut v =
                    serde_json::to_value(&doc_w_col_names).context(SerdeJsonConversionSnafu)?;

                if let Some(num) = Number::from_f64(f64::from(score)) {
                    v[SEARCH_SCORE_COLUMN_NAME] = Value::Number(num);
                }
                Ok(v)
            })
            .collect::<Result<Vec<Value>>>()?;

        Ok(top_docs)
    }

    fn tantivy_json_to_arrow_decoder(
        &self,
        hits: &[Value],
    ) -> std::result::Result<Decoder, ArrowError> {
        let schema = Arc::new(arrow_json::reader::infer_json_schema_from_iterator(
            hits.iter().map(Ok),
        )?);

        let schema = Arc::new(Schema::new(
            schema
                .fields()
                .into_iter()
                .map(|f| {
                    // Use [`Self::type_hints`].
                    if let Some(new_field) = self.type_hints.get(f.name()) {
                        new_field
                    } else {
                        f
                    }
                })
                .cloned()
                .collect::<Vec<_>>(),
        ));

        let mut decoder = arrow_json::ReaderBuilder::new(Arc::clone(&schema)).build_decoder()?;

        decoder.serialize(hits)?;

        Ok(decoder)
    }
}

// Parse a user-provided query to interpret it without terms (e.g. `IN ['foo', 'bar']`) or clauses (foo AND bar).
//
// A query, q, is interpreted as a space-delimited, OR-conjuncted set of string literals.
//
// Examples:
//  - q="'foo and' bar" -> ["foo", "and", "bar"]
//  - q="title:sea^20 body:whale^70" -> ["title", "sea", "20", "body", "whale", "70"]
//  - q="How much (in USD) don't I get?" -> ["how", "much", "in", "usd", "don", "t", "i", "get"]
fn parse_query_literal(q: &str) -> UserInputAst {
    let mut literal = vec![];
    let mut tok = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .build();

    let mut s = tok.token_stream(q);
    while s.advance() {
        literal.push(s.token().text.clone());
    }

    UserInputAst::Clause(
        literal
            .into_iter()
            .map(|phrase| {
                (
                    Some(Occur::Should),
                    UserInputAst::Leaf(Box::new(UserInputLeaf::Literal(UserInputLiteral {
                        field_name: None,
                        phrase,
                        delimiter: Delimiter::None,
                        slop: 0,
                        prefix: false,
                    }))),
                )
            })
            .collect(),
    )
}

impl From<FullTextSearchFieldIndex> for FullTextSearchCandidate {
    fn from(inner: FullTextSearchFieldIndex) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl From<Arc<FullTextSearchFieldIndex>> for FullTextSearchCandidate {
    fn from(inner: Arc<FullTextSearchFieldIndex>) -> Self {
        Self {
            inner: Arc::clone(&inner),
        }
    }
}

pub struct FullTextSearchCandidate {
    inner: Arc<FullTextSearchFieldIndex>,
}

#[async_trait]
impl CandidateGeneration for FullTextSearchCandidate {
    fn search(&self, query: String) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        Ok(Arc::new(FullTextSearchQuery {
            index: Arc::clone(&self.inner),
            query,
            pre_limit: None,
        }))
    }

    /// Returns the name of the column that is used to derive the value in the [`SEARCH_VALUE_COLUMN_NAME`] column.
    fn value_derived_from(&self) -> String {
        self.inner.field.clone()
    }
}

fn make_stream(
    fts: FullTextSearchFieldIndex,
    query: String,
    limit: usize,
) -> impl Stream<Item = std::result::Result<RecordBatch, DataFusionError>> {
    stream! {
        let mut remaining_limit = limit;
        let mut offset = 0;
        while remaining_limit > 0 {
            let limit = min(remaining_limit, DEFAULT_BATCH_SIZE);
            let hits = match fts
                .search_query_literal(query.as_str(), limit, offset)
                .map_err(|e| DataFusionError::Internal(e.to_string())) {
                    Ok(h) => h,
                    Err(e) => {yield Err(e); return}
                };
            offset += limit;
            remaining_limit -= limit;

            let mut decoder = match fts.tantivy_json_to_arrow_decoder(hits.as_slice())
                .map_err(DataFusionError::from) {
                    Ok(h) => h,
                    Err(e) => {
                        yield Err(e);
                        return
                    }
                };

            match decoder.flush() {
                Ok(Some(rb)) => yield Ok(rb),
                Ok(None) => {},
                Err(e) => yield Err(DataFusionError::from(e))
            }
        }
    }
}

#[must_use]
pub fn tantivy_to_arrow_type(t: &FieldType) -> Option<arrow::datatypes::DataType> {
    match t {
        FieldType::Str(_) => Some(arrow::datatypes::DataType::Utf8),
        FieldType::I64(_) => Some(arrow::datatypes::DataType::Int64),
        FieldType::U64(_) => Some(arrow::datatypes::DataType::UInt64),
        FieldType::F64(_) => Some(arrow::datatypes::DataType::Float64),
        FieldType::Date(_) => Some(arrow::datatypes::DataType::Date32),
        FieldType::Bool(_) => Some(arrow::datatypes::DataType::Boolean),
        FieldType::Bytes(_) => Some(arrow::datatypes::DataType::Binary),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::generation::text_search::parse_query_literal;

    #[test]
    fn test_parse_query_literal() {
        insta::assert_json_snapshot!("and_conjunction", parse_query_literal("foo and bar"));
        insta::assert_json_snapshot!("quotes_conjunction", parse_query_literal("'foo and' bar"));
        insta::assert_json_snapshot!(
            "special_characters",
            parse_query_literal("title:sea^20 body:whale^70")
        );
        insta::assert_json_snapshot!(
            "operators",
            parse_query_literal("How much (in USD) don't I get?")
        );
    }
}
