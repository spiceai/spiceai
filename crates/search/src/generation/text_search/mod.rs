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

use crate::{SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME};
use arrow::{
    array::RecordBatch,
    datatypes::{Field, FieldRef, Schema, SchemaRef},
    error::ArrowError,
};
use arrow_json::reader::Decoder;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream,
    logical_expr::sqlparser::ast::Expr, physical_plan::stream::RecordBatchStreamAdapter,
    sql::sqlparser::ast::Ident,
};

use futures::{Stream, StreamExt};
use serde_json::{Number, Value};
use snafu::{ResultExt, Snafu};
use tantivy::{
    Index, ReloadPolicy, TantivyError,
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

pub static DEFAULT_BATCH_SIZE: usize = 100;

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
            .field("schema", &self.search_schema)
            .field("field", &self.field)
            .field("primary_key", &self.primary_key)
            .field("additional_columns", &self.additional_columns)
            .field("type_hints", &self.type_hints)
            .finish_non_exhaustive()
    }
}

/// A [`FullTextSearchFieldIndex`] performs a search on a [`tantivy::Index`]  for a single field of a table.
#[derive(Clone)]
pub struct FullTextSearchFieldIndex {
    // These are components from a [`tantivy::Index`] required to perform a search on a  [`tantivy::Index`] at a given commit.
    pub search_schema: tantivy::schema::Schema,
    reader: tantivy::Searcher,
    tokenizer_manager: tantivy::tokenizer::TokenizerManager,

    pub field: String,
    pub primary_key: Vec<String>,

    /// If provided, will only consider columns in [`Index`] that are in `field`, `primary_key` or `additional_columns`.
    /// This allows for the reuse of a generic `Index` in search.
    pub additional_columns: Option<Vec<String>>,

    /// Provide hints to the final Arrow datatype for a given column. Keys are column names.
    /// Tantivy [`FieldType`]s are less specific than [`arrow::datatypes::DataType`]s and the Arrow type must be inferred from Tanitvy JSON results (via [`arrow_json::reader::infer_json_schema_from_iterator`]).
    /// For columns present, use the associated [`arrow::datatypes::Field`].
    type_hints: HashMap<String, Arc<arrow::datatypes::Field>>,
}

impl FullTextSearchFieldIndex {
    pub fn try_new(
        index: &Index,
        field: String,
        primary_key: Vec<String>,
        additional_columns: Option<Vec<String>>,
    ) -> Result<Self> {
        let fts = Self {
            search_schema: index.schema(),
            reader: index
                .reader_builder()
                .reload_policy(ReloadPolicy::OnCommitWithDelay)
                .try_into()
                .context(TextSearchSnafu)?
                .searcher(),
            tokenizer_manager: index.tokenizers().clone(),
            field,
            primary_key,
            additional_columns,
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
                    index_columns: cols.clone(),
                });
            }
        }

        // Ensure that the index has the field to search on.
        if !cols.contains(&fts.field) {
            return Err(Error::TextSearchIndexMissingColummn {
                missing: fts.field.clone(),
                index_columns: cols,
            });
        }

        Ok(fts)
    }

    ///  Schema is based on the [`tantivy::schema::Schema`] with `self.type_hints` applied.
    fn schema(&self) -> Arc<Schema> {
        let fields = self
            .all_columns()
            .iter()
            .filter_map(|field_name| {
                let (data_type, nullable) = if let Some(f) = self.get_type_hint(field_name) {
                    (f.data_type().clone(), f.is_nullable())
                } else {
                    let f = self.search_schema.get_field(field_name).ok()?;
                    let entry = self.search_schema.get_field_entry(f);
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
    pub fn additional_columns(&self) -> Vec<String> {
        self.all_columns()
            .into_iter()
            .filter(|name| !self.in_base_cols(name))
            .collect()
    }

    fn in_base_cols(&self, name: &String) -> bool {
        *name == self.field || self.primary_key.contains(name)
    }

    #[must_use]
    pub fn all_columns(&self) -> Vec<String> {
        self.search_schema
            .fields()
            .filter_map(|(_, f)| {
                let name = f.name().to_string();
                if self.in_base_cols(&name) {
                    Some(name)
                } else if self
                    // Filter based on [`self.additional_columns`].
                    .additional_columns
                    .as_ref()
                    .is_some_and(|cols| !cols.contains(&name))
                {
                    None
                } else {
                    Some(name)
                }
            })
            .collect()
    }

    fn query_parser(&self) -> QueryParser {
        let default_field = self
            .search_schema
            .find_field(self.field.as_str())
            .map(|(f, _)| vec![f])
            .unwrap_or_default();
        QueryParser::new(
            self.search_schema.clone(),
            default_field,
            self.tokenizer_manager.clone(),
        )
    }

    pub async fn search(
        &self,
        query: String,
        opt_filters: &[&Expr],
        addition_projection: &[&Expr],
        limit: usize,
    ) -> GenerationResult<SendableRecordBatchStream> {
        if !opt_filters.is_empty() {
            return Err(Error::UnsupportedFiltersError).context(GenerationTextSearchSnafu)?;
        }

        // If search field is explicitly request, must keep in Tantivy response (instead of `value`).
        let mut keep_search_field = false;
        let cols = self.all_columns();
        for proj in addition_projection {
            let is_supported = match proj {
                Expr::Identifier(Ident { value, .. }) => {
                    if *value == self.field {
                        keep_search_field = true;
                    }
                    cols.contains(value)
                }
                _ => false,
            };
            if !is_supported {
                return Err(Error::UnsupportedAdditionalColumnsError)
                    .context(GenerationTextSearchSnafu)?;
            }
        }

        for pk in &self.primary_key {
            // keep the field if it is part of the primary key
            if pk == &self.field {
                keep_search_field = true;
                break;
            }
        }

        let strm = make_stream(self.clone(), query, keep_search_field, limit);
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

    /// If `keep_search_field`, `self.field` will be kept in result (as well as [`SEARCH_VALUE_COLUMN_NAME`]).
    fn search_query_literal(
        &self,
        literal: &str,
        keep_search_field: bool,
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
                    .map(|(f, v)| (self.search_schema.get_field_name(f), v))
                    .filter(|(name, _)| all_cols.contains(&(*name).to_string()))
                    .collect::<HashMap<_, _>>();

                // Must rename `self.field` -> `SEARCH_VALUE_COLUMN_NAME` for final result.
                if let Some(value) = doc_w_col_names.remove(self.field.as_str()) {
                    if keep_search_field {
                        doc_w_col_names.insert(self.field.as_str(), value.clone());
                    }
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
    async fn search(
        &self,
        query: String,
        opt_filters: &[&Expr],
        addition_projection: &[&Expr],
        limit: usize,
    ) -> GenerationResult<SendableRecordBatchStream> {
        self.inner
            .search(query, opt_filters, addition_projection, limit)
            .await
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> GenerationResult<Vec<bool>> {
        Ok((0..filters.len()).map(|_| false).collect::<Vec<_>>())
    }

    /// Whether additional columns of the underlying source can also be retrieved during generation.
    fn supports_columns(&self, projection: &[&Expr]) -> GenerationResult<Vec<bool>> {
        let columns = self.inner.all_columns();

        let cols_found = projection
            .iter()
            .map(|expr| {
                if let Expr::Identifier(Ident { value, .. }) = expr {
                    columns.contains(value) || value == SEARCH_SCORE_COLUMN_NAME
                } else {
                    false
                }
            })
            .collect();

        Ok(cols_found)
    }

    /// Returns the name of the column that is used to derive the value in the [`SEARCH_VALUE_COLUMN_NAME`] column.
    fn value_derived_from(&self) -> String {
        self.inner.field.clone()
    }
}

fn make_stream(
    fts: FullTextSearchFieldIndex,
    query: String,
    keep_search_field: bool,
    limit: usize,
) -> impl Stream<Item = std::result::Result<RecordBatch, DataFusionError>> {
    stream! {
        let mut remaining_limit = limit;
        let mut offset = 0;
        while remaining_limit > 0 {
            let limit = min(remaining_limit, DEFAULT_BATCH_SIZE);
            let hits = match fts
                .search_query_literal(query.as_str(), keep_search_field, limit, offset)
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
    use datafusion::{execution::SendableRecordBatchStream, physical_plan::common::collect};
    use serde_json::Value;
    use tantivy::{
        Index, IndexWriter, doc,
        schema::{STORED, Schema, TEXT},
    };

    use crate::{
        aggregation::write_to_json_string,
        generation::{
            CandidateGeneration, Result as GenerationResult,
            text_search::{FullTextSearchCandidate, FullTextSearchFieldIndex, parse_query_literal},
        },
    };

    pub(crate) fn normalise_result(value: &mut serde_json::Value) {
        if let Value::Array(vv) = value {
            for v in vv {
                if let Value::Object(obj) = v {
                    obj.sort_keys();
                    if let Some(Value::Number(n)) = obj.get("score")
                        && let Some(score) = n.as_f64()
                        && let Some(truncated_score) =
                            serde_json::Number::from_f64((1000.0 * score).trunc() / 1000.0)
                    // Keep 2 decimals
                    {
                        obj.insert("score".to_string(), Value::Number(truncated_score));
                    }
                }
            }
        }
    }

    // Keep in sync with `crate::generation::post_apply::tests::create_table_provider`.
    pub(crate) fn create_basic_index() -> Index {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT | STORED);
        let body = schema_builder.add_text_field("body", TEXT | STORED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index
            .writer(15_000_000) // cannot be less than 15_000_000 for in memory
            .expect("Failed to make index writer");
        index_writer.add_document(doc!(
            title => "The Old Man and the Sea",
            body => "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone \
              eighty-four days now without taking a fish.",
        )).expect("failed to add document");

        index_writer.add_document(doc!(
        title => "Of Mice and Men",
        body => "A few miles south of Soledad, the Salinas River drops in close to the hillside \
                bank and runs deep and green. The water is warm too, for it has slipped twinkling \
                over the yellow sands in the sunlight before reaching the narrow pool. On one \
                side of the river the golden foothill slopes curve up to the strong and rocky \
                Gabilan Mountains, but on the valley side the water is lined with fish and trees—willows \
                fresh and green with every spring, carrying in their lower leaf junctures the \
                debris of the winter’s flooding; and sycamores with mottled, white, recumbent \
                limbs and branches that arch over the pool."
        )).expect("failed to add document");

        index_writer.add_document(doc!(
        title => "Frankenstein",
        body => "You will rejoice to hear that no disaster has accompanied the commencement of an \
                 enterprise which you have regarded with such evil forebodings.  I arrived here \
                 yesterday, and my first task is to assure my dear sister of my welfare and \
                 increasing confidence in the success of getting fish."
        )).expect("failed to add document");

        index_writer.commit().expect("failed to commit documents");

        index
    }

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

    #[tokio::test]
    async fn test_basic_index() {
        let fts = FullTextSearchFieldIndex::try_new(
            &create_basic_index(),
            "body".to_string(),
            vec![],
            None,
        )
        .expect("failed to create FullTextSearch");

        let candidate: FullTextSearchCandidate = fts.into();

        let rb_as_value = validate_result(candidate.search("fish".into(), &[], &[], 3).await).await;

        insta::assert_json_snapshot!(rb_as_value);
    }

    /// Validates the result of a search operation by collecting the [`RecordBatch`] results into a JSON value.
    pub(crate) async fn validate_result(
        output: GenerationResult<SendableRecordBatchStream>,
    ) -> Value {
        let output = output.expect("failed to execute search");
        let rbs = collect(output)
            .await
            .expect("failed to collect search results");

        let rb_json =
            write_to_json_string(rbs.as_slice()).expect("failed to write RecordBatch to JSON");

        let mut rb_as_value = serde_json::from_str::<serde_json::Value>(&rb_json)
            .expect("failed to parse JSON string");

        normalise_result(&mut rb_as_value);

        rb_as_value
    }
}
