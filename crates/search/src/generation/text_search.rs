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
use std::{collections::HashMap, sync::Arc};

use crate::{SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME};
use arrow::error::ArrowError;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream,
    logical_expr::sqlparser::ast::Expr, physical_plan::stream::RecordBatchStreamAdapter,
    sql::sqlparser::ast::Ident,
};
use serde_json::{Number, Value};
use snafu::{ResultExt, Snafu};
use tantivy::{
    Index, ReloadPolicy, Searcher, TantivyError,
    collector::TopDocs,
    query::{Occur, QueryParser, QueryParserError},
    query_grammar::{Delimiter, UserInputAst, UserInputLeaf, UserInputLiteral},
    schema::{Field, OwnedValue},
    tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer},
};

use super::{
    CandidateGeneration, Result as GenerationResult, TextSearchSnafu as GenerationTextSearchSnafu,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error occurred during text search: {source}"))]
    TextSearchError { source: TantivyError },

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
    ArrowConversionError { source: ArrowError },

    #[snafu(display("Failed to convert underlying search data into JSON format. Error: {source}"))]
    SerdeJsonConversionError { source: serde_json::Error },

    #[snafu(display("Full text search does not support filters."))]
    UnsupportedFiltersError,

    #[snafu(display("Full text search does not support retrieving additional columns."))]
    UnsupportedAdditionalColumnsError,
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

pub struct FullTextSearch {
    idx: Arc<Index>,
    field: String,
    primary_key: Vec<String>,
}

impl FullTextSearch {
    pub fn try_new(index: Arc<Index>, field: String, primary_key: Vec<String>) -> Result<Self> {
        let fts = Self {
            idx: index,
            field,
            primary_key,
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

    #[must_use]
    pub fn additional_columns(&self) -> Vec<String> {
        self.all_columns()
            .into_iter()
            .filter(|name| *name != self.field && !self.primary_key.contains(name))
            .collect()
    }

    #[must_use]
    pub fn all_columns(&self) -> Vec<String> {
        self.idx
            .schema()
            .fields()
            .map(|(_, f)| f.name().to_string())
            .collect()
    }

    fn index_searcher(&self) -> Result<Searcher> {
        Ok(self
            .idx
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .context(TextSearchSnafu)?
            .searcher())
    }

    fn search_query_literal(&self, literal: &str, limit: usize) -> Result<Vec<Value>> {
        // Explicitly create AST to avoid user queries being considered a query language (e.g. `"title:sea^20 body:whale^70"`).
        let default_field = self
            .idx
            .schema()
            .find_field(self.field.as_str())
            .map(|(f, _)| vec![f])
            .unwrap_or_default();
        let q = QueryParser::for_index(&self.idx, default_field)
            .build_query_from_user_input_ast(parse_query_literal(literal))
            .context(InvalidTextSearchQuerySnafu {
                query: literal.to_string(),
            })?;

        let schema = self.idx.schema();
        let searcher = self.index_searcher()?;

        let top_docs = searcher
            .search(&q, &TopDocs::with_limit(limit))
            .context(TextSearchSnafu)?
            .into_iter()
            .map(|(score, addr)| {
                let doc: HashMap<Field, OwnedValue> =
                    searcher.doc(addr).context(TextSearchSnafu)?;

                let mut doc_w_col_names = doc
                    .into_iter()
                    .map(|(f, v)| (schema.get_field_name(f), v))
                    .collect::<HashMap<_, _>>();

                // Must rename `self.field` -> `SEARCH_VALUE_COLUMN_NAME` for final result.
                if let Some(value) = doc_w_col_names.remove(self.field.as_str()) {
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

#[async_trait]
impl CandidateGeneration for FullTextSearch {
    async fn search(
        &self,
        query: String,
        opt_filters: &[&Expr],
        addition_projection: &[&Expr],
        limit: usize,
    ) -> GenerationResult<SendableRecordBatchStream> {
        if !opt_filters.is_empty() {
            return Err(Error::UnsupportedFiltersError).context(GenerationTextSearchSnafu)?;
        }

        let cols = self.all_columns();
        for proj in addition_projection {
            let is_supported = match proj {
                Expr::Identifier(Ident { value, .. }) => cols.contains(value),
                _ => false,
            };
            if !is_supported {
                return Err(Error::UnsupportedAdditionalColumnsError)
                    .context(GenerationTextSearchSnafu)?;
            }
        }

        let hits = self
            .search_query_literal(query.as_str(), limit)
            .context(GenerationTextSearchSnafu)?;

        let schema = Arc::new(
            arrow_json::reader::infer_json_schema_from_iterator(hits.iter().map(Ok))
                .context(ArrowSchemaSnafu)
                .context(GenerationTextSearchSnafu)?,
        );

        let mut decoder = arrow_json::ReaderBuilder::new(Arc::clone(&schema))
            .build_decoder()
            .context(ArrowSchemaSnafu)
            .context(GenerationTextSearchSnafu)?;

        decoder
            .serialize(hits.as_slice())
            .context(ArrowConversionSnafu)
            .context(GenerationTextSearchSnafu)?;

        let strm = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream! {
                match decoder.flush() {
                    Ok(Some(rb)) => yield Ok(rb),
                    Ok(None) => {},
                    Err(e) => yield Err(DataFusionError::ArrowError(e, None))
                }
            },
        )) as SendableRecordBatchStream;

        Ok(strm)
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> GenerationResult<Vec<bool>> {
        Ok((0..filters.len()).map(|_| false).collect::<Vec<_>>())
    }

    /// Whether additional columns of the underlying source can also be retrieved during generation.
    fn supports_columns(&self, projection: &[&Expr]) -> GenerationResult<Vec<bool>> {
        let columns = self.all_columns();

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
        self.field.clone()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

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
            text_search::{FullTextSearch, parse_query_literal},
        },
    };

    pub(crate) fn normalise_result(value: &mut serde_json::Value) {
        if let Value::Array(vv) = value {
            for v in vv {
                if let Value::Object(obj) = v {
                    obj.sort_keys();
                    if let Some(Value::Number(n)) = obj.get("score") {
                        if let Some(score) = n.as_f64() {
                            if let Some(truncated_score) =
                                serde_json::Number::from_f64((1000.0 * score).trunc() / 1000.0)
                            // Keep 2 decimals
                            {
                                obj.insert("score".to_string(), Value::Number(truncated_score));
                            }
                        }
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
        let fts =
            FullTextSearch::try_new(Arc::new(create_basic_index()), "body".to_string(), vec![])
                .expect("failed to create FullTextSearch");

        let rb_as_value = validate_result(fts.search("fish".into(), &[], &[], 3).await).await;

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
