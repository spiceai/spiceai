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
use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    sync::Arc,
};

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
    catalog::TableProvider,
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::stream::RecordBatchStreamAdapter,
};

use ::util::format_datafusion_error;
use futures::Stream;
use serde_json::{Number, Value};
use snafu::{ResultExt, Snafu};
use tantivy::{
    Searcher, TantivyError, Term,
    collector::TopDocs,
    query::{
        Bm25StatisticsProvider, BooleanQuery, ConstScoreQuery, Occur, Query, QueryParser,
        QueryParserError,
    },
    query_grammar::{Delimiter, UserInputAst, UserInputLeaf, UserInputLiteral},
    schema::{FieldType, OwnedValue},
    tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer},
};

use super::{CandidateGeneration, Result as GenerationResult};

/// Maximum number of results in a single full-text search request, before any pagination.
/// This size is designated for latency performance on the underlying index.
pub static DEFAULT_BATCH_SIZE: usize = 100;

/// Maximum number of results to return for a given full-text search.
pub static DEFAULT_LIMIT_MAXIMUM: usize = 1000;

pub mod bm25_stats;
pub mod exec;
pub mod index;
pub mod query;
mod util;

pub use bm25_stats::{GlobalBm25Provider, GlobalBm25Stats};

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

    #[snafu(display(
        "Failed to convert JSON values to Arrow format. Error: {}",
        format_datafusion_error(source)
    ))]
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

    #[snafu(display(
        "Failed to retrieve the data from the underlying table: {}.",
        format_datafusion_error(source)
    ))]
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

    #[snafu(display(
        "Failed to open the full text search index at '{path}': it was created without the column(s) {} that the index is now configured to hold. \
        Delete '{path}' so the index is rebuilt from the dataset, or set 'index_path' to a new directory. \
        See: https://spiceai.org/docs/features/search/full-text-search",
        columns.join(", ")
    ))]
    PersistedIndexMissingColumns { path: String, columns: Vec<String> },

    #[snafu(display(
        "Failed to open the full text search index at '{path}': column '{column}' is indexed as {persisted} but is now configured as {configured}. \
        Delete '{path}' so the index is rebuilt from the dataset, or set 'index_path' to a new directory. \
        See: https://spiceai.org/docs/features/search/full-text-search"
    ))]
    PersistedIndexColumnChanged {
        path: String,
        column: String,
        persisted: String,
        configured: String,
    },

    #[snafu(display(
        "This partition's full text index changed while a distributed search was gathering \
        collection statistics (generation {expected} then, {actual} now). Retry the search."
    ))]
    IndexGenerationChanged { expected: u64, actual: u64 },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Whether the operator caused the failure and can fix it from their own configuration.
    ///
    /// The persisted-index mismatches qualify: the index on disk and the spicepod disagree,
    /// and the message names the directory to delete or repoint. They surface while the
    /// dataset loads rather than from a query, so nothing maps them to a status code today.
    #[must_use]
    pub fn is_user_error(&self) -> bool {
        matches!(
            self,
            Error::InvalidTextSearchQueryError { .. }
                | Error::TextSearchIndexMissingColummn { .. }
                | Error::PersistedIndexMissingColumns { .. }
                | Error::PersistedIndexColumnChanged { .. }
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

    /// Cached names of stored fields in the underlying index (for O(1) membership checks).
    stored_columns: HashSet<String>,

    /// Provide hints to the final Arrow datatype for a given column. Keys are column names.
    /// Tantivy [`FieldType`]s are less specific than [`arrow::datatypes::DataType`], so source-schema type hints preserve the original Arrow types.
    /// For columns present, use the associated [`arrow::datatypes::Field`].
    type_hints: HashMap<String, Arc<arrow::datatypes::Field>>,

    /// Global BM25 collection statistics to score against, gathered across the
    /// partitions of a distributed index. When `Some`, scoring uses these
    /// statistics instead of the local segments' statistics, so scores are
    /// comparable across executors. When `None`, scoring uses the local
    /// statistics (single-node, or the opt-in `scoring = local` mode).
    global_stats: Option<Arc<GlobalBm25Stats>>,
}

impl FullTextSearchFieldIndex {
    pub fn try_new(
        index_search: Searcher,
        field: String,
        primary_key: Vec<String>,
    ) -> Result<Self> {
        let stored_columns: HashSet<String> = index_search
            .schema()
            .fields()
            .filter(|&(_, f)| f.is_stored())
            .map(|(_, f)| f.name().to_string())
            .collect();

        let fts = Self {
            reader: index_search,
            field,
            primary_key,
            stored_columns,
            type_hints: HashMap::from([(
                SEARCH_SCORE_COLUMN_NAME.to_string(),
                Arc::new(Field::new(
                    SEARCH_SCORE_COLUMN_NAME,
                    arrow::datatypes::DataType::Float64,
                    false,
                )),
            )]),
            global_stats: None,
        };

        // Ensure that the index has the required primary key columns.
        for pk in &fts.primary_key {
            if !fts.stored_columns.contains(pk) {
                return Err(Error::TextSearchIndexMissingColummn {
                    missing: pk.clone(),
                    index_columns: fts.stored_columns.iter().cloned().collect(),
                });
            }
        }

        Ok(fts)
    }

    /// Schema is based on the [`tantivy::schema::Schema`] with `self.type_hints` applied.
    /// Field order follows the underlying tantivy schema (not the `HashSet` cache).
    fn schema(&self) -> Arc<Schema> {
        let search_schema = self.reader.schema();
        let fields = search_schema
            .fields()
            .filter_map(|(_, f)| {
                if !f.is_stored() {
                    return None;
                }
                let field_name = f.name();
                let (data_type, nullable) = if let Some(hint) = self.get_type_hint(field_name) {
                    (hint.data_type().clone(), hint.is_nullable())
                } else {
                    (tantivy_to_arrow_type(f.field_type())?, false)
                };
                Some(Field::new(field_name, data_type, nullable))
            })
            .collect::<Vec<_>>();

        Arc::new(Schema::new(fields))
    }

    /// Schema for every result page produced by [`Self::search`].
    ///
    /// Tantivy omits absent stored values from a document's JSON. Supplying this
    /// fixed schema to the JSON decoder makes those absent nullable values Arrow
    /// nulls instead of removing their columns from a result page.
    fn result_schema(&self) -> Arc<Schema> {
        let schema = self.schema();
        let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();

        if let Some((_, value_field)) = schema.column_with_name(self.field.as_str()) {
            fields.push(Arc::new(Field::new(
                SEARCH_VALUE_COLUMN_NAME,
                value_field.data_type().clone(),
                value_field.is_nullable(),
            )));
        }

        fields.push(Arc::new(Field::new(
            SEARCH_SCORE_COLUMN_NAME,
            arrow::datatypes::DataType::Float64,
            false,
        )));

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
    pub fn get_type_hint(&self, name: &str) -> Option<&FieldRef> {
        self.type_hints.get(name)
    }

    /// Returns the cached set of stored column names in the underlying index.
    #[must_use]
    pub fn all_columns(&self) -> &HashSet<String> {
        &self.stored_columns
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

    /// The Tantivy [`Field`] of the search column, if it exists in the schema.
    fn search_field(&self) -> Option<tantivy::schema::Field> {
        self.reader
            .schema()
            .find_field(self.field.as_str())
            .map(|(f, _)| f)
    }

    /// Score against the given global BM25 collection statistics instead of the
    /// local segments' statistics. Used on an executor to score its partition
    /// with statistics summed across every partition, so scores are comparable
    /// across executors.
    #[must_use]
    pub fn with_global_stats(mut self, global_stats: Option<Arc<GlobalBm25Stats>>) -> Self {
        self.global_stats = global_stats;
        self
    }

    /// This partition's Tantivy reader generation at the moment this index was
    /// opened (each call that opens a fresh searcher — e.g. one `text_search` or
    /// `text_search_stats` UDTF invocation — observes the reader's current
    /// generation; a `reload()` between two calls bumps it). A distributed
    /// search compares this against the generation observed while gathering
    /// statistics, to detect a commit landing between the two rounds.
    #[must_use]
    pub fn generation_id(&self) -> u64 {
        self.reader.generation().generation_id()
    }

    /// Parse `query` with this index's analyzer and gather the local BM25
    /// collection statistics for its terms: the document count `N`, the total
    /// number of tokens in the search field, and the per-term document
    /// frequency. The scheduler sums these across partitions into the global
    /// statistics used for scoring.
    ///
    /// The terms are extracted from the parsed query, so they are tokenized and
    /// stemmed exactly as the scored query's terms are.
    ///
    /// # Errors
    ///
    /// Returns an error when the query cannot be parsed or the index cannot be
    /// read for a term's document frequency.
    pub fn local_bm25_stats(&self, query: &str) -> Result<GlobalBm25Stats> {
        let parser = self.query_parser();
        let parsed = match parser.parse_query(query) {
            Ok(parsed) => parsed,
            Err(_) => parser
                .build_query_from_user_input_ast(parse_query_literal(query))
                .context(InvalidTextSearchQuerySnafu {
                    query: query.to_string(),
                })?,
        };

        // `query_terms` borrows from the parsed query and cannot fail or read
        // the index, so collect the terms first, then compute each frequency.
        let mut terms: Vec<Term> = Vec::new();
        parsed.query_terms(&mut |term, _need_position| terms.push(term.clone()));

        let search_field = self.search_field();
        let mut doc_freq: std::collections::BTreeMap<String, u64> =
            std::collections::BTreeMap::new();
        for term in terms {
            // Only terms on the search field contribute to the gathered
            // statistics; a term on another field is scored locally on the
            // executor (its statistics are not summed).
            if Some(term.field()) != search_field {
                continue;
            }
            // Bind the term value: `as_str` borrows from it, so it must outlive
            // the lookup below (a temporary would be dropped too early).
            let value = term.value();
            let Some(text) = value.as_str() else {
                continue;
            };
            if doc_freq.contains_key(text) {
                continue;
            }
            let df = self.reader.doc_freq(&term).context(TextSearchSnafu)?;
            doc_freq.insert(text.to_string(), df);
        }

        let total_num_docs =
            Bm25StatisticsProvider::total_num_docs(&self.reader).context(TextSearchSnafu)?;
        let total_num_tokens = match search_field {
            Some(field) => Bm25StatisticsProvider::total_num_tokens(&self.reader, field)
                .context(TextSearchSnafu)?,
            None => 0,
        };

        Ok(GlobalBm25Stats {
            total_num_docs,
            total_num_tokens,
            doc_freq,
        })
    }

    /// Classify each pushed-down filter for [`TableProvider::supports_filters_pushdown`].
    ///
    /// Every column is classified against the underlying tantivy schema and field types, so the
    /// classification stays in lockstep with what [`Self::translate_filters`] can actually build.
    #[must_use]
    pub fn classify_filters(&self, filters: &[&Expr]) -> Vec<TableProviderFilterPushDown> {
        let schema = self.reader.schema();
        filters
            .iter()
            .map(|f| tantivy_datafusion_filter::classify_filter(schema, f))
            .collect()
    }

    /// Translate the filters DataFusion pushed into this scan into tantivy queries.
    ///
    /// A filter DataFusion pushes was previously advertised as `Exact`/`Inexact` by
    /// [`Self::classify_filters`], so it must translate; a filter that cannot be translated is a
    /// lockstep violation and is surfaced as an error rather than silently dropped (which would
    /// return wrong results for a filter DataFusion believes was applied).
    pub fn translate_filters(
        &self,
        filters: &[Expr],
    ) -> Result<Vec<Box<dyn Query>>, DataFusionError> {
        let schema = self.reader.schema();
        filters
            .iter()
            .map(|f| {
                tantivy_datafusion_filter::translate_filter(schema, f).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Full text search received a filter it advertised as pushable but cannot translate: {f}"
                    ))
                })
            })
            .collect()
    }

    pub fn search(
        &self,
        query: String,
        filters: Vec<Box<dyn Query>>,
        limit: usize,
    ) -> GenerationResult<SendableRecordBatchStream> {
        let strm = make_stream(self.clone(), query, filters, limit);
        let schema = self.result_schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, strm)) as SendableRecordBatchStream)
    }

    fn search_query_literal(
        &self,
        literal: &str,
        filters: &[Box<dyn Query>],
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Value>> {
        // Prefer Tantivy's full QueryParser so operators (AND/OR/NOT), phrases
        // ("exact match"), field-scoped queries (title:foo) and boosts (term^2)
        // are honored. Fall back to a bag-of-words OR clause for inputs the
        // parser rejects (e.g. unbalanced quotes, lone special characters in
        // conversational queries).
        let parser = self.query_parser();
        let text_query = match parser.parse_query(literal) {
            Ok(parsed) => parsed,
            Err(_) => parser
                .build_query_from_user_input_ast(parse_query_literal(literal))
                .context(InvalidTextSearchQuerySnafu {
                    query: literal.to_string(),
                })?,
        };

        // `Must`-combine each pushed-down SQL filter with the full-text clause so the top-K is
        // computed over the filtered document set (the filter is applied inside the index, not
        // above the candidate cap).
        let q: Box<dyn Query> = if filters.is_empty() {
            text_query
        } else {
            let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(filters.len() + 1);
            clauses.push((Occur::Must, text_query));
            for f in filters {
                clauses.push((
                    Occur::Must,
                    Box::new(ConstScoreQuery::new(f.box_clone(), 0.0)),
                ));
            }
            Box::new(BooleanQuery::new(clauses))
        };

        let collector = TopDocs::with_limit(limit)
            .and_offset(offset)
            .order_by_score();

        // When global statistics are present, score against them instead of the
        // local segments' statistics, so scores are comparable across the
        // partitions of a distributed index. The search still runs over the
        // local segments; only the BM25 collection statistics change.
        let raw_hits = match self.global_stats.as_ref() {
            Some(stats) => {
                let field = self.search_field().ok_or_else(|| Error::TextSearchError {
                    source: TantivyError::FieldNotFound(self.field.clone()),
                })?;
                let provider = GlobalBm25Provider::new(stats.as_ref(), field, &self.reader);
                self.reader
                    .search_with_statistics_provider(&q, &collector, &provider)
                    .context(TextSearchSnafu)?
            }
            None => self
                .reader
                .search(&q, &collector)
                .context(TextSearchSnafu)?,
        };

        let top_docs = raw_hits
            .into_iter()
            .map(|(score, addr)| {
                let doc: HashMap<tantivy::schema::Field, OwnedValue> =
                    self.reader.doc(addr).context(TextSearchSnafu)?;

                let mut doc_w_col_names = doc
                    .into_iter()
                    .map(|(f, v)| (self.reader.schema().get_field_name(f), v))
                    // `HashSet<String>::contains` accepts `&str` via `Borrow<str>`.
                    .filter(|(name, _)| self.stored_columns.contains(*name))
                    .collect::<HashMap<_, _>>();

                // Keep `self.field` and also expose it as `SEARCH_VALUE_COLUMN_NAME`.
                // Both columns are required by the direct-search result schema.
                if let Some(value) = doc_w_col_names.get(self.field.as_str()).cloned() {
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
        let mut decoder = arrow_json::ReaderBuilder::new(self.result_schema()).build_decoder()?;

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
    filters: Vec<Box<dyn Query>>,
    limit: usize,
) -> impl Stream<Item = std::result::Result<RecordBatch, DataFusionError>> {
    stream! {
        // Share the searcher into the blocking task that runs the synchronous
        // tantivy search (mmap/disk reads + scoring + stored-field decode) off
        // the async runtime thread (which also serves `/health`, `/v1/search`).
        let fts = std::sync::Arc::new(fts);
        // Shared across pages; each page `box_clone`s the queries into its own BooleanQuery.
        let filters = std::sync::Arc::new(filters);
        let mut remaining_limit = limit;
        let mut offset = 0;
        while remaining_limit > 0 {
            let page_size = min(remaining_limit, DEFAULT_BATCH_SIZE);
            let hits = {
                let fts = std::sync::Arc::clone(&fts);
                let filters = std::sync::Arc::clone(&filters);
                let query = query.clone();
                match tokio::task::spawn_blocking(move || {
                    fts.search_query_literal(query.as_str(), filters.as_slice(), page_size, offset)
                })
                .await
                {
                    Ok(Ok(h)) => h,
                    Ok(Err(e)) => {
                        yield Err(DataFusionError::Internal(e.to_string()));
                        return;
                    }
                    Err(e) => {
                        yield Err(DataFusionError::Internal(format!(
                            "full text search task failed: {e}"
                        )));
                        return;
                    }
                }
            };

            // Decrement by *actual* hits returned (not the requested page size) so
            // we stop once the index is exhausted instead of issuing further empty
            // queries.
            let returned = hits.len();
            offset += returned;
            remaining_limit = remaining_limit.saturating_sub(returned);

            if !hits.is_empty() {
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

            // Index is exhausted: a partial page (or empty page) means there are
            // no more matching documents.
            if returned < page_size {
                return;
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
